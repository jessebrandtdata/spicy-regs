"""Historical House PDF distinctions survive the ordinary download and query path."""

import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from spicy_regs import mcp_server
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.congressional_disbursement_tables import build
from spicy_regs.schemas.house_disbursement_pages import TABLES
from tests.disbursement_fixtures import read_pdf_fixtures
from tests.generation_fakes import download_generation

FIXTURES = Path(__file__).parent / "fixtures/house_disbursement_pages"


def test_ditto_credits_aggregate_roles_and_page_coverage_survive_download(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    sources = []
    for item in json.loads((FIXTURES / "provenance.json").read_text())["excerpts"]:
        fixture = FIXTURES / item["file"]
        assert "sha256:" + hashlib.sha256(fixture.read_bytes()).hexdigest() == item["sha256"]
        sources.append(
            {
                "id": item["file"].removesuffix(".pdf") + "-excerpt",
                "url": item["original_url"],
                "period_start": item["report_period_start"],
                "period_end": item["report_period_end"],
                "fixture": fixture,
            }
        )
    manifest = read_pdf_fixtures(tmp_path, "house", sources)
    artifact, evidence = build(manifest, tmp_path / "tables")
    generation = tmp_path / "tables/generation"
    assert verify_generation(generation, expected_pin=artifact.pin).pin == artifact.pin
    selected = download_generation(monkeypatch, generation, evidence, tmp_path / "selected", TABLES)
    monkeypatch.setattr(mcp_server, "DATA_DIR", selected)
    with mcp_server._build_connection() as connection:
        vendor = connection.execute(
            "SELECT vendor_name, service_start_date, raw_fields_json, normalizations_json "
            "FROM house_disbursement_pdf_rows WHERE report_year=2009 AND amount=3876.75"
        ).fetchone()
        assert vendor is not None
        assert vendor[:2] == ("STRATTON DESIGN SOURCE, INC.", date(2006, 10, 4))
        assert connection.execute(
            "SELECT transaction_date, vendor_name, document FROM house_disbursement_pdf_rows "
            "WHERE report_year=2015 AND amount=-242.75"
        ).fetchone() == (date(2015, 11, 6), "HALEY, KATHERINE C.", "00822410")
        assert connection.execute(
            "SELECT vendor_name, document, description FROM house_disbursement_pdf_rows "
            "WHERE report_year=2015 AND amount=5960.80"
        ).fetchone() == (None, "PIX0053371", "PHOTOGRAPHIC (TRANSFER)")
        assert connection.execute(
            "SELECT amount, qtd_amount FROM house_disbursement_pdf_rows WHERE report_year=2015 "
            "AND source_page=5 AND row_kind='organization_total'"
        ).fetchone() == (None, Decimal("1705746.19"))
        ditto = connection.execute(
            "SELECT raw_fields_json, normalizations_json FROM house_disbursement_pdf_rows "
            "WHERE report_year=2009 AND amount=3876.75"
        ).fetchone()
        assert ditto is not None
        assert json.loads(ditto[0])["vendor_name"].startswith("DO ")
        assert json.loads(ditto[1])[0]["witness"]["source_page"] == 1
        assert connection.execute(
            "SELECT header_basis, printed_page FROM house_disbursement_pdf_pages "
            "WHERE report_year=2015 AND source_page=3"
        ).fetchone() == ("previous_contiguous_printed_page", "3")
        count = connection.execute("SELECT count(*) FROM house_disbursement_pdf_rows").fetchone()
        assert count is not None
        assert connection.execute(
            "SELECT count(*), sum(record_count) FROM house_disbursement_pdf_pages"
        ).fetchone() == (12, count[0])
