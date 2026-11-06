"""Source acquisition can resume without losing malformed originals or financial values."""

import hashlib
import json

import httpx
import pytest

from spicy_regs.pipelines.congressional_disbursements import acquire_selection, normalize_selection

URL = "https://www.house.gov/sites/default/files/2020q1-summary.csv"
BODY = b"ORGANIZATION,PROGRAM,DESCRIPTION,YTD AMOUNT,QTD AMOUNT\r\nOffice,Program,Expense,-123.45,-23.45\r\n"


def selection(tmp_path, **extra):
    path = tmp_path / "selection.json"
    path.write_text(
        json.dumps(
            {
                "schema": "congressional-disbursement-selection/1",
                "files": [
                    {
                        "id": "house-2020-q1-summary",
                        "chamber": "house",
                        "format": "csv",
                        "kind": "summary",
                        "url": URL,
                        "period_start": "2020-01-01",
                        "period_end": "2020-03-31",
                        **extra,
                    }
                ],
            }
        )
    )
    return path


def capture(tmp_path, path, transport, **kwargs):
    return acquire_selection(path, tmp_path / "run", archive_root=tmp_path / "raw", transport=transport, **kwargs)


def test_resume_reuses_verified_original_and_preserves_exact_money(tmp_path):
    path = selection(tmp_path)
    sent = []

    def serve(request):
        sent.append(str(request.url))
        return httpx.Response(200, content=BODY, headers={"Content-Type": "text/csv"})

    first = capture(tmp_path, path, httpx.MockTransport(serve))
    second = capture(tmp_path, path, httpx.MockTransport(serve))
    assert sent == [URL]
    assert first["files"] == second["files"]
    assert second["pending"] == []
    result = normalize_selection(tmp_path / "run/acquisition.json", tmp_path / "normalized")
    assert result["complete"] and result["rows"] == 1
    output = (tmp_path / "normalized/house-2020-q1-summary.csv").read_text()
    assert "-123.45" in output and "-23.45" in output
    receipt = json.loads((tmp_path / "normalized/house-2020-q1-summary.receipt.json").read_text())
    assert receipt["source_sha256"] == "sha256:" + hashlib.sha256(BODY).hexdigest()


def test_parse_failure_retains_original_and_does_not_refetch_for_offline_repair(tmp_path):
    path = selection(tmp_path)
    malformed = BODY + b"shifted,row\r\n"
    first = capture(
        tmp_path,
        path,
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=malformed, headers={"Content-Type": "text/csv"})
        ),
    )
    assert first["files"][0]["status"] == "captured-unparsed"

    def never(request):
        pytest.fail("Captured source should be replayed offline")

    capture(tmp_path, path, httpx.MockTransport(never))
    result = normalize_selection(tmp_path / "run/acquisition.json", tmp_path / "normalized")
    assert not result["complete"] and result["rows"] == 0
    assert result["files"][0]["status"] == "parse-failed"


def test_unavailable_source_is_retried_on_resume(tmp_path):
    path = selection(tmp_path)
    first = capture(tmp_path, path, httpx.MockTransport(lambda request: httpx.Response(404, content=b"missing")))
    assert first["pending"] == ["house-2020-q1-summary"]
    second = capture(
        tmp_path,
        path,
        httpx.MockTransport(lambda request: httpx.Response(200, content=BODY, headers={"Content-Type": "text/csv"})),
    )
    assert second["files"][0]["status"] == "captured"
    assert second["pending"] == []


def test_credential_refusal_aborts_and_records_pending_source(tmp_path):
    from spicy_docs.transport.credentials import CredentialRefusedError

    path = selection(tmp_path)
    with pytest.raises(CredentialRefusedError):
        capture(tmp_path, path, httpx.MockTransport(lambda request: httpx.Response(403, content=b"access refused")))
    result = json.loads((tmp_path / "run/acquisition.json").read_text())
    assert result["pending"] == ["house-2020-q1-summary"]


def test_reuse_requires_exact_original_and_receipt_pins(tmp_path):
    path = selection(tmp_path)
    original = tmp_path / "original.csv"
    original.write_bytes(BODY)
    reuse = {
        URL: {"path": str(original), "sha256": "sha256:" + hashlib.sha256(BODY).hexdigest(), "byte_size": len(BODY)}
    }

    def never(request):
        pytest.fail("Pinned local original must not be downloaded")

    result = capture(tmp_path, path, httpx.MockTransport(never), reuse=reuse)
    assert result["files"][0]["status"] == "retained"
    changed = json.loads(path.read_text())
    changed["files"][0]["period_end"] = "2020-03-30"
    path.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="identical source selection"):
        capture(tmp_path, path, httpx.MockTransport(never))


def test_normalization_resume_reuses_checked_outputs_and_detects_tampering(tmp_path):
    path = selection(tmp_path)
    capture(
        tmp_path,
        path,
        httpx.MockTransport(lambda request: httpx.Response(200, content=BODY, headers={"Content-Type": "text/csv"})),
    )
    inputs, output = tmp_path / "run/acquisition.json", tmp_path / "normalized"
    first = normalize_selection(inputs, output)
    csv_path = output / "house-2020-q1-summary.csv"
    timestamp = csv_path.stat().st_mtime_ns
    assert normalize_selection(inputs, output) == first
    assert csv_path.stat().st_mtime_ns == timestamp
    csv_path.write_text(csv_path.read_text().replace("-23.45", "-23.46"))
    with pytest.raises(ValueError, match="output changed"):
        normalize_selection(inputs, output)


def test_incomplete_acquisition_cannot_claim_complete_normalization(tmp_path):
    path = selection(tmp_path)
    capture(
        tmp_path,
        path,
        httpx.MockTransport(lambda request: httpx.Response(200, content=BODY, headers={"Content-Type": "text/csv"})),
    )
    inputs = tmp_path / "run/acquisition.json"
    forged = json.loads(inputs.read_text())
    forged["files"] = []
    inputs.write_text(json.dumps(forged))
    with pytest.raises(ValueError, match="pending membership"):
        normalize_selection(inputs, tmp_path / "normalized")


def test_encoding_repair_names_exact_input_and_retains_both_generations(tmp_path):
    path = selection(tmp_path)
    original = BODY.replace(b"Office", b"Caf\xe9")
    result = capture(
        tmp_path,
        path,
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=original, headers={"Content-Type": "text/csv"})
        ),
    )
    inputs, first_dir = tmp_path / "run/acquisition.json", tmp_path / "first"
    first = normalize_selection(inputs, first_dir)
    assert not first["complete"]
    sha = result["files"][0]["source_sha256"]
    options = {sha: {"encoding": "cp1252", "reason": "Reviewed original cafe accent", "evidence": "manual-review"}}
    with pytest.raises(ValueError, match="identical inputs"):
        normalize_selection(inputs, first_dir, source_options=options)
    fixed = normalize_selection(inputs, tmp_path / "fixed", source_options=options)
    assert fixed["complete"]
    assert "Café" in (tmp_path / "fixed/house-2020-q1-summary.csv").read_text()
    assert not json.loads((first_dir / "manifest.json").read_text())["complete"]


def test_pdf_uses_same_capture_resume_and_keeps_full_part_selection(tmp_path):
    url = "https://www.govinfo.gov/content/pkg/GPO-CDOC-112sdoc4/pdf/GPO-CDOC-112sdoc4-1.pdf"
    path = selection(
        tmp_path,
        id="senate-2011-part-1",
        chamber="senate",
        format="pdf",
        kind="part",
        url=url,
        period_start="2011-04-01",
        period_end="2011-09-30",
    )
    # Byte-structure witness only. A PDF format pass is never semantic coverage.
    body = b"%PDF-1.7\nxref\n0 1\n0000000000 65535 f\ntrailer\n<< /Size 1 >>\nstartxref\n9\n%%EOF\n"
    sent = []

    def serve(request):
        sent.append(str(request.url))
        return httpx.Response(200, content=body, headers={"Content-Type": "application/pdf"})

    first = capture(tmp_path, path, httpx.MockTransport(serve))
    second = capture(tmp_path, path, httpx.MockTransport(serve))
    assert sent == [url] and first["files"] == second["files"]
    source = first["files"][0]
    assert source["kind"] == "part" and source["pdf_facts"]["locator"]["chamber"] == "senate"
    assert source["pdf_facts"]["terminal_xref_offset"] == 9
    with pytest.raises(ValueError, match="CSV inputs"):
        normalize_selection(tmp_path / "run/acquisition.json", tmp_path / "normalized")


def test_retained_original_is_exported_in_existing_file_inventory_before_parsing(tmp_path):
    from spicy_docs.storage.raw_archive import iter_acquisition_receipts
    from spicy_regs.source_catalog.exporter import export
    from spicy_regs.source_catalog.file_inventory import validate
    from spicy_regs.source_catalog.importer import build
    from spicy_regs.source_catalog.raw_mapping import REGISTRY

    path = selection(tmp_path)
    # Syntax failure does not erase custody or its declared source period.
    malformed = BODY + b"shifted,row\r\n"
    result = capture(
        tmp_path,
        path,
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=malformed, headers={"Content-Type": "text/csv"})
        ),
    )
    source = result["files"][0]
    assert source["status"] == "captured-unparsed"
    receipt_key = source["inventory_receipt"]["object"]
    _, receipt = next(iter_acquisition_receipts(tmp_path / "raw", receipt_keys=[receipt_key]))
    row = receipt["files"][0]
    assert row["dataset_id"] == "family:house-disbursements"
    assert row["native_id"] == URL
    assert row["source_version"] == source["source_sha256"]
    assert row["covered_period"]["label"] == "2020-01-01/2020-03-31"
    assert "acquired_at" not in row and "http_status" not in row
    assert receipt["receipt"]["acquisition_outcome"] == "captured-unparsed"

    # Exercise the maintained importer and public file codec, not a second inventory.
    registry = json.loads(REGISTRY.read_text())
    collection = next(c for c in registry["collections"] if c["id"] == row["dataset_id"])
    scope = next(c for c in registry["catalog_scope"]["collections"] if c["id"] == row["dataset_id"])
    config = tmp_path / "registry.json"
    config.write_text(
        json.dumps(
            {
                "collections": [collection],
                "catalog_scope": {
                    "format": "spicy-source-scope",
                    "version": 2,
                    "collections": [scope],
                    "excluded_objects": [],
                },
            }
        )
    )
    index = tmp_path / "index.sqlite"
    build(tmp_path / "raw", index, config=config)
    public = tmp_path / "public"
    manifest = export(index, public)
    family = next(c for c in manifest["collections"] if c["id"] == row["dataset_id"])
    detail = json.loads((public / family["detailPath"]).read_text())
    part = detail["files"]["pages"][0]
    rows = validate(
        (public / part["path"]).read_bytes(),
        generation=manifest["generation"],
        dataset_id=row["dataset_id"],
        inventory_kind="active",
    )
    assert len(rows) == 1
    assert rows[0]["source_url"] == URL
    assert rows[0]["source_id"] == URL
    assert rows[0]["content_sha256"] == bytes.fromhex(source["source_sha256"][7:])
    assert rows[0]["period_label"] == "2020-01-01/2020-03-31"
    assert rows[0]["availability"] == "local"


def test_inventory_backfill_preserves_only_matching_acquisition_facts(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts
    from spicy_regs.pipelines.disbursement_inventory import record_original

    path = selection(tmp_path)
    result = capture(
        tmp_path,
        path,
        httpx.MockTransport(lambda request: httpx.Response(200, content=BODY, headers={"Content-Type": "text/csv"})),
    )
    source = result["files"][0]
    documents = [r for _, r in iter_acquisition_receipts(tmp_path / "raw")]
    origin = next(r["files"][0] for r in documents if r["receipt"].get("event") == "capture")
    store = CentralRawArchiveStore(tmp_path / "raw")
    selected = {"path": str(path), "sha256": result["selection_sha256"]}
    first = record_original(store, source, selection=selected, origin=origin)
    assert record_original(store, source, selection=selected, origin=origin) == first
    _, receipt = next(iter_acquisition_receipts(tmp_path / "raw", receipt_keys=[first["object"]]))
    assert receipt["files"][0]["acquired_at"] == origin["acquired_at"]
    assert receipt["files"][0]["http_status"] == 200
    redirected = {**origin, "source_url": "https://download.example.org/report.csv", "requested_url": URL}
    redirect = record_original(store, source, selection=selected, origin=redirected)
    _, redirected_receipt = next(iter_acquisition_receipts(tmp_path / "raw", receipt_keys=[redirect["object"]]))
    assert redirected_receipt["files"][0]["source_url"] == redirected["source_url"]
    assert redirected_receipt["files"][0]["requested_url"] == URL
    assert redirected_receipt["files"][0]["native_id"] == URL
    with pytest.raises(ValueError, match="do not match"):
        record_original(store, source, selection=selected, origin={**origin, "source_url": URL + "wrong"})
    with pytest.raises(ValueError, match="unavailable"):
        record_original(store, {**source, "status": "failed"}, selection=selected)


def test_local_reuse_checks_source_url_before_importing_bytes(tmp_path):
    path = selection(tmp_path, url="https://example.org/untrusted.csv")
    with pytest.raises(ValueError, match="House"):
        capture(tmp_path, path, httpx.MockTransport(lambda request: pytest.fail("Must not request invalid source")))
    assert not (tmp_path / "raw").exists()


def test_new_generation_reuses_only_unchanged_source_rules(tmp_path, monkeypatch):
    from spicy_regs.pipelines import congressional_disbursements as pipeline
    from spicy_docs.sources import house_disbursements_normalize as normalizer

    path = selection(tmp_path)
    acquisition = capture(
        tmp_path,
        path,
        httpx.MockTransport(lambda request: httpx.Response(200, content=BODY, headers={"Content-Type": "text/csv"})),
    )
    inputs, first_dir = tmp_path / "run/acquisition.json", tmp_path / "first"
    first = normalize_selection(inputs, first_dir)
    implementation = pipeline._normalization_implementation()
    implementation["files"]["house_disbursements_corrections.json"] = "changed declarations"
    implementation["source_rules"]["unrelated source"] = "new unrelated repair"
    monkeypatch.setattr(pipeline, "_normalization_implementation", lambda: implementation)
    calls = []
    writer = normalizer.write_normalized_house_disbursements

    def counted(*args, **kwargs):
        calls.append(True)
        return writer(*args, **kwargs)

    monkeypatch.setattr(normalizer, "write_normalized_house_disbursements", counted)
    reused = normalize_selection(inputs, tmp_path / "reused", reuse_manifest=first_dir / "manifest.json")
    assert reused["complete"] and calls == []
    assert reused["files"][0]["receipt_sha256"] == first["files"][0]["receipt_sha256"]
    implementation["source_rules"][acquisition["files"][0]["source_sha256"]] = "changed selected repair"
    rebuilt = normalize_selection(inputs, tmp_path / "rebuilt", reuse_manifest=first_dir / "manifest.json")
    assert rebuilt["complete"] and calls == [True]
    assert "reused_from" not in rebuilt["files"][0]
