"""Maintained CRS file caller: numbered PDFs, unbound HTML, and phase reuse."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import build_crs_report_files as files
from tests.pdf_fixtures import make_pdf

PDF_URL = "https://www.congress.gov/crs_external_products/RA/PDF/RL12345/RL12345.2.pdf"
HTML_URL = "https://www.congress.gov/crs_external_products/RA/HTML/RL12345.html"
PDF = make_pdf(["Cover date 2026-10-01", "Numbered edition two"])
HTML = b'<html><body data-prod-type="RA"><div class="CoverDate">October 1, 2026 (RL12345)</div><p>Later mutable edition three</p></body></html>'


def report(*, update="2026-10-01T00:00:00Z", number=2):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    formats = [{"format": "PDF", "url": PDF_URL.replace(".2.pdf", f".{number}.pdf")},
               {"format": "HTML", "url": HTML_URL}]
    original = json.dumps({"CRSReport": {"id": "RL12345", "updateDate": update, "currentVersion": number,
                                        "formats": formats}}).encode()
    digest = files._sha(original)
    store = CentralRawArchiveStore()
    store.put_blob(digest, len(original), [original])
    store.record_receipt(files=[{"sha256": digest, "bytes": len(original), "family": "crs-reports"}],
                         receipt={"kind": "synthetic-crs-detail-fixture"})
    return {"report_id": "RL12345", "update_date": update, "version": str(number), "detail_state": "ok",
            "detail_witness_json": json.dumps([{"sha256": digest}]), "formats_json": json.dumps(formats)}


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    asked = []
    def handler(request):
        asked.append(str(request.url))
        body = HTML if request.url.path.endswith(".html") else PDF
        return httpx.Response(200, stream=httpx.ByteStream(body), headers={"Content-Type": "text/html" if body == HTML else "application/pdf"})
    return asked, httpx.MockTransport(handler)


def rows(result):
    return pq.read_table(Path(result["bundle"]) / files.KEY).to_pylist()


def test_actual_caller_uses_numbered_pdf_and_keeps_mutable_html_unbound(tmp_path, source):
    asked, transport = source
    result = files.build_crs_report_files(tmp_path / "run", [report()], include_html=True,
                                          transport=transport, allow_network=True)
    pdf, html = rows(result)
    assert asked == [PDF_URL, HTML_URL]
    assert (pdf["publisher_version"], pdf["source_family"], pdf["version_binding"]) == ("2", "RA", "numbered-pdf-locator")
    assert pdf["text_content"] == "Cover date 2026-10-01\n\nNumbered edition two"
    assert html["publisher_version"] is None and html["version_binding"] == "unbound-current-observation"
    assert "edition three" in html["text_content"] and "edition three" not in pdf["text_content"]
    for row, body in ((pdf, PDF), (html, HTML)):
        assert row["source_sha256"] == files._sha(body) == row["text_input_sha256"]
        assert row["text_sha256"] == files._sha(row["text_content"].encode())
        assert row["text_processor_sha256"] == files.text_processor_pin()
        assert row["capture_receipt_key"].endswith(row["capture_receipt_sha256"][7:])
    assert result["requests"] == 2 and result["status"] == "verified_local_candidate_not_published"


def test_unchanged_rerun_and_metadata_only_update_reuse_body_and_text(tmp_path, source, monkeypatch):
    asked, transport = source
    directory = tmp_path / "run"
    first = files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    def unexpected(*_):
        raise AssertionError("completed extraction was repeated")
    monkeypatch.setattr(files, "extract_text", unexpected)
    repeated = files.build_crs_report_files(directory, [report()], prior_bundle=Path(first["bundle"]), transport=transport)
    assert repeated["requests"] == 0 and repeated["reused_text"] == 1 and len(rows(repeated)) == 1
    fresh = files.build_crs_report_files(tmp_path / "metadata-update", [report(update="2026-10-02T00:00:00Z")],
                                        prior_bundle=Path(repeated["bundle"]), transport=transport)
    before, after = rows(fresh)
    assert len(asked) == 1 and fresh["requests"] == 0 and fresh["reused_text"] == 1
    assert before["capture_id"] == after["capture_id"] and before["captured_at"] == after["captured_at"]
    assert before["source_sha256"] == after["source_sha256"]
    assert before["association_receipt_sha256"] != after["association_receipt_sha256"]
    assert after["association_kind"] == "retained-original-reuse"


def test_fresh_output_replay_preserves_one_metadata_association(tmp_path, source):
    asked, transport = source
    first = files.build_crs_report_files(tmp_path / "first", [report()], transport=transport, allow_network=True)
    second = files.build_crs_report_files(tmp_path / "second", [report()], prior_bundle=Path(first["bundle"]), transport=transport)
    assert len(asked) == 1 and second["requests"] == 0
    assert rows(second) == rows(first)


def test_numbered_files_with_same_cover_date_stay_distinct(tmp_path, source):
    asked, transport = source
    first = files.build_crs_report_files(tmp_path / "one", [report()], transport=transport, allow_network=True)
    second = files.build_crs_report_files(tmp_path / "two", [report(update="2026-10-02T00:00:00Z", number=3)],
        prior_bundle=Path(first["bundle"]), transport=transport, allow_network=True)
    assert [row["publisher_version"] for row in rows(second)] == ["2", "3"]
    assert len(asked) == 2


@pytest.mark.parametrize("phase", ["body", "text"])
def test_interrupted_capture_or_extraction_recovers_without_download(tmp_path, source, monkeypatch, phase):
    asked, transport = source
    directory = tmp_path / "run"
    writer, extract = files._write, files.extract_text
    if phase == "body":
        def fail_body(rows, path, generation_id):
            if path.name.endswith("-body"):
                raise RuntimeError("injected body phase interruption")
            return writer(rows, path, generation_id)
        monkeypatch.setattr(files, "_write", fail_body)
    else:
        monkeypatch.setattr(files, "extract_text", lambda *_: (_ for _ in ()).throw(RuntimeError("injected extraction interruption")))
    with pytest.raises(RuntimeError, match="injected"):
        files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    monkeypatch.setattr(files, "_write", writer)
    monkeypatch.setattr(files, "extract_text", extract)
    result = files.build_crs_report_files(directory, [report()], transport=transport)
    assert len(asked) == 1 and result["requests"] == 0 and result["reused_bodies"] == 1
    assert rows(result)[0]["text_state"] == "ok"


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_missing_or_corrupt_retained_body_refuses_without_redownload(tmp_path, source, damage):
    asked, transport = source
    directory = tmp_path / "run"
    first = files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    digest = rows(first)[0]["source_sha256"][7:]
    original = tmp_path / "raw/objects/sha256" / digest[:2] / digest
    if damage == "missing":
        original.unlink()
    else:
        original.write_bytes(b"corrupt retained bytes")
    with pytest.raises((ValueError, FileNotFoundError), match="retained|original|object|blob|No such"):
        files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    assert len(asked) == 1


@pytest.mark.parametrize("status", [404, 200])
def test_unavailable_and_content_mismatch_are_explicit(tmp_path, source, status):
    asked, _ = source
    def handler(request):
        asked.append(str(request.url))
        return httpx.Response(status, stream=httpx.ByteStream(b"<html>unavailable or wrong content</html>"), headers={"Content-Type": "text/html"})
    result = files.build_crs_report_files(tmp_path / "run", [report()], allow_network=True, transport=httpx.MockTransport(handler))
    [row] = rows(result)
    assert row["body_state"] == ("unavailable" if status == 404 else "refused")
    assert row["source_sha256"] is None and row["text_content"] is None and row["text_state"] == "not_requested"
    assert row["association_receipt_key"].endswith(row["association_receipt_sha256"][7:])


@pytest.mark.parametrize("status", [404, 200])
def test_unavailable_and_refused_receipts_witness_only_the_actual_metadata_original(tmp_path, source, status):
    _, _transport = source
    transport = httpx.MockTransport(lambda _: httpx.Response(status,
        stream=httpx.ByteStream(b"<html>missing or mismatched file</html>"), headers={"Content-Type": "text/html"}))
    result = files.build_crs_report_files(tmp_path / "run", [report()], allow_network=True, transport=transport)
    [row] = rows(result)
    [receipt] = pq.read_table(Path(result["bundle"]) / "etl_receipts.parquet").to_pylist()
    assert receipt["witnesses"] == [{"source_id": "congress.gov:crs-detail", "source_uri": None,
        "sha256": row["metadata_source_sha256"], "locator": "/CRSReport", "body_version": None}]


def test_named_html_challenge_counts_retained_bytes_and_stops_at_whole_run_bound(tmp_path, source):
    asked, _ = source
    challenge = b"blocked" + b" " * (4096 - len("blocked"))
    def handler(request):
        asked.append(str(request.url))
        if request.url.path.endswith(".html"):
            return httpx.Response(403, stream=httpx.ByteStream(challenge), headers={"Content-Type": "text/html"})
        return httpx.Response(200, stream=httpx.ByteStream(PDF), headers={"Content-Type": "application/pdf"})
    transport = httpx.MockTransport(handler)
    first = files.build_crs_report_files(tmp_path / "one", [report()], include_html=True,
        allow_network=True, transport=transport, max_file_bytes=4096, max_total_bytes=8192)
    assert first["downloaded_bytes"] == len(PDF) + len(challenge)
    assert first["decoded_body_bytes"] == len(PDF) + len(challenge)
    asked.clear()
    with pytest.raises(ValueError, match="budget exhausted"):
        files.build_crs_report_files(tmp_path / "bounded", [report(), report(update="2026-10-02T00:00:00Z")],
            include_html=True, allow_network=True, transport=transport, max_file_bytes=4096, max_total_bytes=8192)
    assert asked == [PDF_URL, HTML_URL]


@pytest.mark.parametrize("status", [404, 200])
def test_repeated_refusal_replaces_same_association_and_retains_each_observation(tmp_path, source, status):
    from spicy_docs.storage.raw_archive import iter_acquisition_receipts

    asked, _ = source
    def handler(request):
        asked.append(str(request.url))
        return httpx.Response(status, stream=httpx.ByteStream(b"<html>missing or mismatched file</html>"),
                              headers={"Content-Type": "text/html"})
    transport = httpx.MockTransport(handler)
    first = files.build_crs_report_files(tmp_path / "first", [report()], allow_network=True, transport=transport)
    second = files.build_crs_report_files(tmp_path / "second", [report()], prior_bundle=Path(first["bundle"]),
                                          allow_network=True, transport=transport)
    assert len(asked) == 2 and rows(second) == rows(first)
    observations = [document["receipt"] for _, document in iter_acquisition_receipts(tmp_path / "raw")
                    if document["receipt"].get("event") == "capture"
                    and document["receipt"].get("requested_url") == PDF_URL]
    assert len(observations) == 2 and len({observation["capture_id"] for observation in observations}) == 2
    files.verify_generation(Path(first["generation_directory"]))


def test_unavailable_then_success_preserves_failure_and_reuses_recovered_body(tmp_path, source):
    asked, transport = source
    unavailable = httpx.MockTransport(lambda _: httpx.Response(404, stream=httpx.ByteStream(b"missing"),
                                                               headers={"Content-Type": "text/html"}))
    first = files.build_crs_report_files(tmp_path / "first", [report()], allow_network=True, transport=unavailable)
    second = files.build_crs_report_files(tmp_path / "second", [report()], prior_bundle=Path(first["bundle"]),
                                          allow_network=True, transport=transport)
    assert [row["body_state"] for row in rows(second)] == ["unavailable", "ok"]
    third = files.build_crs_report_files(tmp_path / "third", [report()], prior_bundle=Path(second["bundle"]),
                                         transport=transport)
    assert len(asked) == 1 and third["requests"] == 0 and rows(third) == rows(second)


def test_no_unbounded_or_unqualified_acquisition(tmp_path, source):
    asked, transport = source
    with pytest.raises(ValueError, match="explicit bounded network"):
        files.build_crs_report_files(tmp_path / "run", [report()], transport=transport)
    with pytest.raises(ValueError, match="cohort exceeds"):
        files.build_crs_report_files(tmp_path / "too-many", [report(), report()], max_reports=1, transport=transport)
    with pytest.raises(ValueError, match="qualified dated"):
        files.build_crs_report_files(tmp_path / "unqualified", [{**report(), "detail_state": "not_requested"}], transport=transport)
    assert asked == []


def test_returned_extraction_failure_retries_completed_body(tmp_path, source, monkeypatch):
    asked, transport = source
    directory = tmp_path / "run"
    extract = files.extract_text
    monkeypatch.setattr(files, "extract_text", lambda *_: {
        "text_content": None, "text_state": "error", "page_count": 0, "error": "transient reader failure"})
    failed = files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    assert rows(failed)[0]["text_state"] == "error"
    monkeypatch.setattr(files, "extract_text", extract)
    recovered = files.build_crs_report_files(directory, [report()], prior_bundle=Path(failed["bundle"]), transport=transport)
    assert len(asked) == 1 and recovered["requests"] == 0 and rows(recovered)[0]["text_state"] == "ok"


@pytest.mark.parametrize("printed", ["RL12345 · VERSION 3 · UPDATED", "R99999 · VERSION 2 · UPDATED"])
def test_explicit_wrong_footer_withholds_qualified_edition_and_text(tmp_path, source, printed):
    _, _transport = source
    body = make_pdf(["Same cover date", printed])
    transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=httpx.ByteStream(body),
                                                            headers={"Content-Type": "application/pdf"}))
    result = files.build_crs_report_files(tmp_path / "run", [report()], transport=transport, allow_network=True)
    [row] = rows(result)
    assert row["publisher_version"] == "2" and row["qualified_publisher_version"] is None
    assert row["body_state"] == "content_mismatch" and row["text_state"] == "identity_mismatch"
    assert row["content_identity_state"] == "mismatching-explicit-footer" and row["text_content"] is None
    assert row["source_sha256"] == files._sha(body)


def test_selected_locators_must_equal_pinned_metadata_original(tmp_path, source):
    asked, transport = source
    selected = report()
    selected["formats_json"] = selected["formats_json"].replace("/RA/", "/RL/")
    with pytest.raises(ValueError, match="locators differ"):
        files.build_crs_report_files(tmp_path / "run", [selected], transport=transport, allow_network=True)
    assert asked == []


def test_copied_valid_phase_cannot_change_selected_file_identity(tmp_path, source):
    asked, transport = source
    directory = tmp_path / "run"
    files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    [body_phase] = list((directory / ".crs-file-phases").glob("*-body"))
    selected = report(update="2026-10-02T00:00:00Z")
    digest = json.loads(selected["detail_witness_json"])[-1]["sha256"]
    identity = [selected["report_id"], selected["update_date"], "pdf", PDF_URL, digest]
    import hashlib
    phase_key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    shutil.copytree(body_phase, body_phase.parent / (phase_key + "-body"))
    with pytest.raises(ValueError, match="phase differs"):
        files.build_crs_report_files(directory, [selected], transport=transport)
    assert len(asked) == 1


def test_processor_change_reextracts_without_redownloading(tmp_path, source, monkeypatch):
    asked, transport = source
    directory = tmp_path / "run"
    first = files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    changed = "sha256:" + "a" * 64
    monkeypatch.setattr(files, "text_processor_pin", lambda: changed)
    second = files.build_crs_report_files(directory, [report()], prior_bundle=Path(first["bundle"]), transport=transport)
    assert len(asked) == 1 and second["requests"] == 0 and second["reused_text"] == 0
    assert rows(second)[0]["text_processor_sha256"] == changed


def test_native_generation_is_queryable_through_maintained_mcp(tmp_path, source, monkeypatch):
    from spicy_regs import mcp_server
    from tests.test_mcp_server import _records, _tool_data

    _, transport = source
    result = files.build_crs_report_files(tmp_path / "run", [report()], transport=transport, allow_network=True)
    monkeypatch.setattr(mcp_server, "DATA_DIR", Path(result["generation_directory"]))
    mcp_server._reset_connection_cache()
    try:
        response = _tool_data(mcp_server.build_server(), "query_sql", {"sql":
            "SELECT report_id,publisher_version,qualified_publisher_version,body_state,source_url,text_input_sha256 "
            "FROM crs_report_files LIMIT 1"})
        assert _records(response) == [{"report_id": "RL12345", "publisher_version": "2",
            "qualified_publisher_version": "2", "body_state": "ok", "source_url": PDF_URL,
            "text_input_sha256": files._sha(PDF)}]
    finally:
        mcp_server._reset_connection_cache()


def test_historical_catalog_pin_preserves_unknown_http_facts_and_edition_limits(tmp_path, source):
    _, transport = source
    first = files.build_crs_report_files(tmp_path / "capture", [report()], transport=transport, allow_network=True)
    [captured] = rows(first)
    catalog_row = {"collection_id": "family:historical-import", "status": "held",
                   "sha256": captured["source_sha256"], "bytes": captured["source_bytes"], "data": {
        "mapping": {"receipt_evidence_ref": {"manifest": captured["capture_receipt_key"],
                     "sha256": captured["capture_receipt_sha256"], "entry": 1}},
        "receipt_context": {"source_url": PDF_URL, "original_receipts": [{"pointer": "/historical/files/0"}]}}}
    pin = files.retained_file_pin(catalog_row)
    second = files.build_crs_report_files(tmp_path / "retained", [report()], retained_files={PDF_URL: pin}, transport=transport)
    [retained] = rows(second)
    assert second["requests"] == 0 and retained["publisher_version"] == "2"
    assert retained["capture_id"] is None and retained["captured_at"] is None and retained["resolved_url"] is None
    assert retained["qualified_publisher_version"] is None and retained["content_identity_state"] == "unknown"
    assert retained["file_validation_basis"] == "retained-source-association-and-native-shape"
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    association = files._receipt(CentralRawArchiveStore(), retained["association_receipt_key"],
                                 retained["association_receipt_sha256"])
    assert "response_complete" not in association["files"][0]


def test_catalog_ordinal_selects_nonfirst_receipt_file_without_changing_source_pin(tmp_path, source):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    asked, transport = source
    store = CentralRawArchiveStore()
    other = b"unrelated retained original"
    for body in (other, PDF):
        store.put_blob(files._sha(body), len(body), [body])
    receipt = store.record_receipt(files=[
        {"sha256": files._sha(other), "bytes": len(other), "source_url": "https://example.org/other.txt"},
        {"sha256": files._sha(PDF), "bytes": len(PDF), "receipt_context": {"source_url": PDF_URL}},
    ], receipt={"event": "synthetic-retained-source-reconciliation"})
    catalog_row = {"status": "held", "sha256": files._sha(PDF), "bytes": len(PDF), "data": {
        "mapping": {"receipt_evidence_ref": {"manifest": receipt["object"],
                    "sha256": "sha256:" + receipt["sha256"], "entry": 2}},
        "receipt_context": {"source_url": PDF_URL}}}
    pin = files.retained_file_pin(catalog_row)
    assert pin["capture_receipt_entry"] == 1
    assert pin["capture_receipt_key"] == receipt["object"]
    assert pin["capture_receipt_sha256"] == "sha256:" + receipt["sha256"]
    result = files.build_crs_report_files(tmp_path / "run", [report()], retained_files={PDF_URL: pin}, transport=transport)
    [row] = rows(result)
    assert asked == [] and result["requests"] == 0 and row["source_sha256"] == files._sha(PDF)
    assert row["capture_receipt_entry"] == 1 and row["capture_receipt_sha256"] == pin["capture_receipt_sha256"]


@pytest.mark.parametrize("ordinal", [None, 0, -1, True, "2", 2.0])
def test_catalog_pin_refuses_malformed_source_ordinal(ordinal):
    catalog_row = {"status": "held", "sha256": files._sha(PDF), "bytes": len(PDF), "data": {
        "mapping": {"receipt_evidence_ref": {"manifest": "acquisitions/sha256/aa/" + "a" * 64,
                    "sha256": "sha256:" + "a" * 64, "entry": ordinal}},
        "receipt_context": {"source_url": PDF_URL}}}
    with pytest.raises(ValueError, match="positive one-based ordinal"):
        files.retained_file_pin(catalog_row)


def test_corrupt_completed_text_phase_refuses_and_preserves_admitted_generation(tmp_path, source):
    asked, transport = source
    directory = tmp_path / "run"
    first = files.build_crs_report_files(directory, [report()], transport=transport, allow_network=True)
    [text_phase] = [path for path in (directory / ".crs-file-phases").iterdir() if not path.name.endswith("-body")]
    (text_phase / files.KEY).write_bytes(b"corrupt completed text phase")
    with pytest.raises(ValueError):
        files.build_crs_report_files(directory, [report()], transport=transport)
    files.verify_generation(Path(first["generation_directory"]))
    assert len(asked) == 1
