"""House source rows survive the actual typed writer and managed-generation admission."""

import json
import shutil
from decimal import Decimal

import httpx
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines import house_disbursements as house
from spicy_regs.pipelines.congressional_disbursements import acquire_selection, normalize_selection
from spicy_regs.pipelines.house_disbursements import build
from spicy_regs.sources import publication
from tests.generation_fakes import Store, download_generation

ROOT_URL = "https://www.house.gov/sites/default/files/"


def normalized(tmp_path):
    root_url = ROOT_URL
    detail = (
        "ORGANIZATION,PROGRAM,SORT SUBTOTAL DESCRIPTION,SORT SEQUENCE,TRANSACTION DATE,DATA SOURCE,DOCUMENT,"
        "VENDOR NAME,PERFORM START DT,PERFORM END DT,DESCRIPTION,AMOUNT\r\n"
        "Office,Program,Travel,DETAIL,2-Jan-20,AP,00031,Payee,32-Jan-20,3-Jan-20,Travel,-999999999999.91\r\n"
        "Office,Program,Travel,DETAIL,2-Jan-20,AP,00031,Payee,32-Jan-20,3-Jan-20,Travel,-999999999999.91\r\n"
    ).encode()
    summary = b"ORGANIZATION,PROGRAM,DESCRIPTION,YTD AMOUNT,QTD AMOUNT\r\nOffice,Program,Travel,-1999999999999.82,-1999999999999.82\r\n"
    bodies = {root_url + "detail.csv": detail, root_url + "summary.csv": summary}
    selection = tmp_path / "selection.json"
    selection.write_text(
        json.dumps(
            {
                "schema": "congressional-disbursement-selection/1",
                "files": [
                    {
                        "id": "house-2020-q1-" + kind,
                        "chamber": "house",
                        "format": "csv",
                        "kind": kind,
                        "url": root_url + kind + ".csv",
                        "period_start": "2020-01-01",
                        "period_end": "2020-03-31",
                    }
                    for kind in ("detail", "summary")
                ],
            }
        )
    )
    acquire_selection(
        selection,
        tmp_path / "acquired",
        archive_root=tmp_path / "raw",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=bodies[str(request.url)], headers={"Content-Type": "text/csv"})
        ),
    )
    normalize_selection(tmp_path / "acquired/acquisition.json", tmp_path / "normalized")
    return tmp_path / "normalized/manifest.json"


def test_typed_tables_preserve_duplicates_exact_money_and_invalid_date_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    manifest = normalized(tmp_path)
    artifact, evidence = build(manifest, tmp_path / "tables")
    generation = tmp_path / "tables/generation"
    admitted = verify_generation(generation, expected_pin=artifact.pin)
    assert admitted.pin == artifact.pin and evidence.is_dir()
    detail_path = next(generation.rglob("house_disbursements/report_year=2020/report_quarter=1/*.parquet"))
    detail = pq.ParquetFile(detail_path).read().to_pylist()
    assert [row["source_record_number"] for row in detail] == [2, 3]
    assert [row["document"] for row in detail] == ["00031", "00031"]
    assert all(row["amount"] == Decimal("-999999999999.91") for row in detail)
    assert all(row["service_start_date"] is None and "invalid_date" in row["issue_codes"] for row in detail)
    audit = json.loads(detail[0]["audit_json"])
    assert any(issue["input_value"] == "32-Jan-20" for issue in audit["issues"])
    summary_path = next(generation.rglob("house_disbursements_summary/report_year=2020/report_quarter=1/*.parquet"))
    summary = pq.ParquetFile(summary_path).read().to_pylist()
    assert len(summary) == 1 and summary[0]["qtd_amount"] == sum(row["amount"] for row in detail)
    assert "amount" not in summary[0] and "qtd_amount" not in detail[0]
    selected = download_generation(
        monkeypatch, generation, evidence, tmp_path / "selected",
        ("house_disbursements", "house_disbursements_summary"),
    )
    monkeypatch.setattr(mcp_server, "DATA_DIR", selected)
    with mcp_server._build_connection() as connection:
        assert connection.execute(
            "SELECT source_record_number, document, amount, service_start_date, report_year "
            "FROM house_disbursements WHERE row_kind = 'transaction' ORDER BY source_record_number"
        ).fetchall() == [
            (number, "00031", Decimal("-999999999999.91"), None, 2020) for number in (2, 3)
        ]
        assert connection.execute("SELECT qtd_amount FROM house_disbursements_summary").fetchall() == [
            (Decimal("-1999999999999.82"),)
        ]


def test_incomplete_or_tampered_normalization_never_becomes_a_generation(tmp_path):
    manifest = normalized(tmp_path)
    value = json.loads(manifest.read_text())
    value["complete"] = False
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="complete selected normalization"):
        build(manifest, tmp_path / "incomplete")
    assert not (tmp_path / "incomplete/generation").exists()
    value["complete"] = True
    manifest.write_text(json.dumps(value))
    csv_path = tmp_path / "normalized/house-2020-q1-detail.csv"
    csv_path.write_text(csv_path.read_text().replace("-999999999999.91", "999999999999.91"))
    with pytest.raises(ValueError, match="output changed"):
        build(manifest, tmp_path / "tampered")
    assert not (tmp_path / "tampered/generation").exists()


def _listing(monkeypatch, *links):
    """The House pages list ``links`` (url, kind, period start, period end); the current page lists them all."""
    from spicy_docs.sources import house_disbursements_index as index

    def acquire_index(self, *, selection):
        listed = tuple(
            index.HouseDisbursementsCsvLink(
                link_number=number, group_number=1, kind=kind, url=url, href=url, label=kind, report_label="report",
                report_context="report", report_url=None, listing_text=kind, source_path="/a", byte_start=0,
                byte_end=1, period_start=start, period_end=end)
            for number, (url, kind, start, end) in enumerate(links, 1)) if selection == "current" else ()
        return index.HouseDisbursementsIndex(selection, "https://www.house.gov/" + selection, "sha256:" + "0" * 64,
                                             1, listed), None

    monkeypatch.setattr(index.HouseDisbursementsIndexAcquirer, "acquire_index", acquire_index)


def _table_rows(generation, table):
    rows = ds.dataset(generation / table, format="parquet").to_table().to_pylist()
    return sorted((row for row in rows), key=lambda row: (row["source_sha256"], row["source_record_number"]))


def test_a_refresh_selects_the_published_files_plus_listed_quarters_and_rebuilds_them(tmp_path, monkeypatch):
    """``select`` reads the published rows and the House's pages; the published quarters rebuild unchanged."""
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    first, first_evidence = build(normalized(tmp_path), tmp_path / "first")
    store = Store()
    live = publication.publish_generation(tmp_path / "first/generation", client=store, bucket="fixture",
                                          prior_index=publication.empty_index(), evidence_directories=(first_evidence,))
    # What `spicy-regs mirror` leaves: the generation under its prefix and the pointer that selected it.
    held = tmp_path / "mirror" / live["families"][house.FAMILY]["prefix"]
    shutil.copytree(tmp_path / "first/generation", held)
    index = tmp_path / "mirror/publication.v2/snapshot.json"
    index.parent.mkdir()
    index.write_bytes(store.objects[publication.INDEX_V2_KEY])

    q2 = {kind: ROOT_URL + f"q2-{kind}.csv" for kind in ("detail", "summary")}
    _listing(monkeypatch, (ROOT_URL + "detail.csv", "detail", "2020-01-01", "2020-03-31"),
             *((url, kind, "2020-04-01", "2020-06-30") for kind, url in q2.items()))
    added = house.select(held, tmp_path / "select")
    assert added == {"published": 2, "added": ["house-2020-q2-detail", "house-2020-q2-summary"]}
    selection = json.loads((tmp_path / "select/selection.json").read_text())
    assert [(item["id"], item["url"], item["period_start"]) for item in selection["files"]] == [
        ("house-2020-q1-detail", ROOT_URL + "detail.csv", "2020-01-01"),
        ("house-2020-q1-summary", ROOT_URL + "summary.csv", "2020-01-01"),
        ("house-2020-q2-detail", q2["detail"], "2020-04-01"),
        ("house-2020-q2-summary", q2["summary"], "2020-04-01"),
    ]
    reuse = json.loads((tmp_path / "select/reuse.json").read_text())
    assert sorted(reuse) == [ROOT_URL + "detail.csv", ROOT_URL + "summary.csv"]
    assert all(item["encoding"] == "utf-8-sig" for item in reuse.values())

    # The published originals come from the raw archive; only the listed quarter is downloaded.
    detail = (b"ORGANIZATION,PROGRAM,SORT SUBTOTAL DESCRIPTION,SORT SEQUENCE,TRANSACTION DATE,DATA SOURCE,DOCUMENT,"
              b"VENDOR NAME,PERFORM START DT,PERFORM END DT,DESCRIPTION,AMOUNT\r\n"
              b"Office,Program,Travel,DETAIL,2-Apr-20,AP,00032,Payee,2-Apr-20,3-Apr-20,Travel,5.00\r\n")
    summary = b"ORGANIZATION,PROGRAM,DESCRIPTION,YTD AMOUNT,QTD AMOUNT\r\nOffice,Program,Travel,5.00,5.00\r\n"
    fetched = []

    def serve(request):
        fetched.append(str(request.url))
        served: dict[str, bytes] = {q2["detail"]: detail, q2["summary"]: summary}
        return httpx.Response(200, content=served[str(request.url)], headers={"Content-Type": "text/csv"})

    acquire_selection(tmp_path / "select/selection.json", tmp_path / "acquired-2", reuse=reuse,
                      transport=httpx.MockTransport(serve))
    assert fetched == list(q2.values())
    normalize_selection(tmp_path / "acquired-2/acquisition.json", tmp_path / "normalized-2")
    second, _ = build(tmp_path / "normalized-2/manifest.json", tmp_path / "second", read_index=index)
    assert second.root["spec"]["readSnapshot"]["families"][house.FAMILY]["artifactDigest"] == first.pin.artifact_digest
    assert sorted(item["role"] for item in second.root["inputs"]) == ["prior-generation", "source-evidence"]
    for table in house.TABLES.values():
        before, after = _table_rows(tmp_path / "first/generation", table), _table_rows(tmp_path / "second/generation", table)
        # The receipt digest names this run's receipt file; every other value of a published quarter is unchanged.
        published = [{**row, "normalization_receipt_sha256": None} for row in after if row["report_quarter"] == 1]
        assert published == [{**row, "normalization_receipt_sha256": None} for row in before]
        assert {row["report_quarter"] for row in after} == {1, 2}


def test_a_listed_revision_of_a_published_quarter_is_selected_by_hand(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    build(normalized(tmp_path), tmp_path / "first")
    _listing(monkeypatch, (ROOT_URL + "detail-REVISED.csv", "detail", "2020-01-01", "2020-03-31"))
    with pytest.raises(ValueError, match="for house-2020-q1-detail, which is published from .*select the revision"):
        house.select(tmp_path / "first/generation", tmp_path / "select")
