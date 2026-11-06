"""House source rows survive the actual typed writer and managed-generation admission."""

import json
from decimal import Decimal

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.congressional_disbursements import acquire_selection, normalize_selection
from spicy_regs.pipelines.house_disbursements import build
from tests.generation_fakes import download_generation


def normalized(tmp_path):
    root_url = "https://www.house.gov/sites/default/files/"
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
