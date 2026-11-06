"""Source occurrences and page coverage survive the maintained generation/consumer path."""

import gzip
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.congressional_disbursement_tables import build
from tests.disbursement_fixtures import read_pdf_fixtures
from tests.generation_fakes import download_generation

FIXTURE = Path(__file__).parent / "fixtures/senate_disbursements/ledger.pdf"


def reading(tmp_path):
    return read_pdf_fixtures(
        tmp_path,
        "senate",
        [
            {
                "id": "senate-2011-page-excerpt",
                "url": "https://www.govinfo.gov/content/pkg/GPO-CDOC-112sdoc4/pdf/GPO-CDOC-112sdoc4.pdf",
                "period_start": "2011-04-01",
                "period_end": "2011-09-30",
                "fixture": FIXTURE,
            }
        ],
    )


def test_exact_occurrences_and_page_outcomes_survive_managed_download_and_query(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    manifest = reading(tmp_path)
    artifact, evidence = build(manifest, tmp_path / "tables")
    generation = tmp_path / "tables/generation"
    assert verify_generation(generation, expected_pin=artifact.pin).pin == artifact.pin
    table = next(generation.rglob("senate_disbursements/report_year=2011/report_start_month=4/*.parquet"))
    details = pq.ParquetFile(table).read().to_pylist()
    continuation = [row for row in details if row["document"] == "DMEN21100208"]
    assert [row["amount"] for row in continuation] == [Decimal("8.46"), Decimal("290.19")]
    assert all(row["source_page"] == 2 and row["printed_page"] == "B-1484" for row in continuation)
    assert all(
        row["service_start"] == date(2011, 2, 28) and row["service_end"] == date(2011, 3, 27) for row in continuation
    )
    assert json.loads(continuation[1]["raw_fields_json"])["document"] == ""
    assert continuation[0]["group_origin_json"] == continuation[1]["group_origin_json"]
    assert all(row["amount"] is not None for row in details)
    assert len({(row["source_sha256"], row["source_page"], row["source_line"]) for row in details}) == len(details)
    manifest_value = json.loads(manifest.read_text())
    with gzip.open(manifest_value["files"][0]["output"]["path"], "rt") as stream:
        raw_pages = [json.loads(line) for line in stream]
    assert len(details) == sum(len(page["records"]) for page in raw_pages)
    pages = next(generation.rglob("senate_disbursement_pages/report_year=2011/report_start_month=4/*.parquet"))
    page_rows = pq.ParquetFile(pages).read().to_pylist()
    assert [(row["source_page"], row["record_count"]) for row in page_rows] == [
        (page["source_page"], len(page["records"])) for page in raw_pages
    ]
    assert all(row["page_status"] == "ledger_read" for row in page_rows)
    selected = download_generation(
        monkeypatch,
        generation,
        evidence,
        tmp_path / "selected",
        ("senate_disbursements", "senate_disbursement_pages"),
    )
    monkeypatch.setattr(mcp_server, "DATA_DIR", selected)
    with mcp_server._build_connection() as connection:
        assert connection.execute(
            "SELECT amount, service_start, service_end FROM senate_disbursements "
            "WHERE document = 'DMEN21100208' ORDER BY source_page, source_line"
        ).fetchall() == [(Decimal(value), date(2011, 2, 28), date(2011, 3, 27)) for value in ("8.46", "290.19")]
        assert connection.execute("SELECT sum(record_count) FROM senate_disbursement_pages").fetchone() == (
            len(details),
        )


@pytest.mark.parametrize(
    "damage", ["incomplete", "tampered_output", "source_policy", "row_population", "implementation_copy"]
)
def test_unqualified_or_changed_readings_never_become_a_generation(tmp_path, damage):
    manifest = reading(tmp_path)
    value = json.loads(manifest.read_text())
    if damage == "incomplete":
        value["complete"] = False
    elif damage == "tampered_output":
        output = Path(value["files"][0]["output"]["path"])
        output.write_bytes(output.read_bytes() + b"changed")
    elif damage == "source_policy":
        value["files"][0]["source_implementation"] = {}
    elif damage == "implementation_copy":
        Path(value["implementation_copies"][0]["retained"]["path"]).write_bytes(b"changed")
    else:
        value["files"][0]["records"] += 1
        value["records"] += 1
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        build(manifest, tmp_path / "refused")
    assert not (tmp_path / "refused/generation").exists()
