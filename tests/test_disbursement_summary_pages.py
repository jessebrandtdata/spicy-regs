"""Summary profiles share custody and resume while retaining their own page scope."""

import gzip
import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.pipelines.congressional_disbursement_pages import (
    implementation,
    read_pages,
    retained_implementation,
    source_implementation,
)
from spicy_regs import mcp_server
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.congressional_disbursement_tables import build
from spicy_regs.schemas.disbursement_summaries import family_tables
from tests.disbursement_fixtures import read_pdf_fixtures
from tests.generation_fakes import download_generation

FIXTURES = Path(__file__).parent / "fixtures/disbursement_summaries"
CASES = json.loads((FIXTURES / "provenance.json").read_text())["files"]
ALLOWANCE_CASES = json.loads((FIXTURES / "allowance-provenance.json").read_text())["files"]


@pytest.mark.parametrize("approved", [False, True])
def test_declared_financial_repair_resources_are_filtered_to_the_complete_source(approved):
    producer = implementation("house", "financial-summary")
    assert any(item["path"].endswith("house_financial_summaries_corrections.json") for item in producer["files"])
    source = {
        "id": "house-2011-04-01-pdf-full",
        "chamber": "house",
        "url": "https://www.house.gov/sites/default/files/uploads/documents/2011q2_singlevolume.pdf",
        "kind": "full",
        "source_sha256": (
            "sha256:2602450cf4815c529bfc99c3d1c0509ab36a301070b546e5189bcd2dd6834232"
            if approved
            else "sha256:" + "0" * 64
        ),
        "source_byte_size": 9290558,
        "period_start": "2011-04-01",
        "period_end": "2011-06-30",
    }
    selected = source_implementation(source, producer)
    assert all(rule["source_sha256"] == source["source_sha256"] for rule in selected["rules"])
    openings = [
        rule for rule in selected["rules"] if rule.get("code") == "opening_balance_from_cross_quarter_source_evidence"
    ]
    assert len(openings) == (2 if approved else 0)
    assert {rule["normalized_after"] for rule in openings} == ({"6796470.91", "6494340.21"} if approved else set())


@pytest.mark.parametrize("approved", [False, True])
def test_allowance_year_resource_is_retained_and_filtered_by_complete_original(approved):
    producer = implementation("house", "allowance-summary")
    assert any(item["path"].endswith("summary_tables.py") for item in producer["files"])
    assert any(item["path"].endswith("house_allowance_summaries_corrections.json") for item in producer["files"])
    source = {
        "id": "house-2013-01-01-pdf-full",
        "chamber": "house",
        "url": "https://www.house.gov/sites/default/files/uploads/documents/2013q1_singlevolume.pdf",
        "kind": "full",
        "source_sha256": "sha256:a58292c5168c0bdf9ba79918df291fef2000545bad3fc93676c28ea866c6d21e"
        if approved
        else "sha256:" + "0" * 64,
        "source_byte_size": 9293114,
        "period_start": "2013-01-01",
        "period_end": "2013-03-31",
    }
    selected = source_implementation(source, producer)["rules"]
    assert all(rule["source_sha256"] == source["source_sha256"] for rule in selected)
    rules = [rule for rule in selected if rule.get("field") == "allowance_year"]
    assert len(rules) == int(approved)
    if approved:
        assert rules[0]["normalized_after"] == 2013
        assert rules[0]["printed_column_year"] == 2012


@pytest.mark.parametrize("profile", ["ledger", "financial-summary"])
@pytest.mark.parametrize("approved", [False, True])
def test_reviewed_image_context_resource_is_retained_for_both_senate_readers(profile, approved):
    producer = implementation("senate", profile)
    assert any(item["path"].endswith("senate_disbursements_context_corrections.json") for item in producer["files"])
    source = {
        "id": "senate-2023-04-01-pdf-full",
        "chamber": "senate",
        "url": "https://www.govinfo.gov/content/pkg/GPO-CDOC-118sdoc8/pdf/GPO-CDOC-118sdoc8.pdf",
        "kind": "full",
        "source_sha256": (
            "sha256:7dcde3fb4a0367f731a2c5c0f8abbf1da6df8c836a69e26950d3f0477b9eea9a"
            if approved
            else "sha256:" + "0" * 64
        ),
        "source_byte_size": 12738271,
        "period_start": "2023-04-01",
        "period_end": "2023-09-30",
    }
    rules = source_implementation(source, producer)["rules"]
    contexts = [rule for rule in rules if "context_after" in rule]
    assert {rule["source_page"] for rule in contexts} == ({353, 354, 355} if approved else set())
    assert all(rule["source_sha256"] == source["source_sha256"] for rule in rules)
    assert all(rule["image_origin"]["basis"] == "reviewed_page_image" for rule in contexts)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
def test_source_summary_rows_and_partial_page_scope_survive_retention_and_resume(tmp_path, case, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    body = (FIXTURES / case["fixture"]).read_bytes()
    assert "sha256:" + hashlib.sha256(body).hexdigest() == case["fixture_sha256"]
    source = {k: case[k] for k in ("id", "url", "period_start", "period_end", "reading_pages", "reading_scope")}
    source["fixture"] = FIXTURES / case["fixture"]
    path = read_pdf_fixtures(tmp_path, case["chamber"], [source], profile=case["profile"])
    result = json.loads(path.read_text())
    assert result["complete"] and result["pending"] == []
    assert result["reading_profile"] == case["profile"]
    entry = result["files"][0]
    assert entry["selected_pages"] == case["reading_pages"]
    assert entry["pages"] == len(case["reading_pages"])
    assert entry["records"] > 0
    assert entry["physical_pages"] >= max(case["reading_pages"])
    retained = retained_implementation(result)
    assert any(name.endswith("extraction/pages.py") for name in retained) == (case["id"] != "house-financial")
    assert any(name.endswith("house_financial_summaries_corrections.json") for name in retained) == (
        case["id"] == "house-financial"
    )
    assert any(name.endswith("senate_disbursements_context_corrections.json") for name in retained) == (
        case["chamber"] == "senate"
    )
    with gzip.open(entry["output"]["path"], "rt") as stream:
        pages = [json.loads(line) for line in stream]
    assert [page["source_page"] for page in pages] == case["reading_pages"]
    if case["id"] == "house-financial":
        row = next(
            row
            for page in pages
            for row in page["records"]
            if row.get("appropriation_title") == "Net Expenses of Telecommunications revolving fund"
        )
        assert row["values"]["funds_available"] == "2316439.32"
    elif case["id"] == "house-mailing":
        assert entry["pages"] < entry["physical_pages"]
        assert all(row["amended"] and row["period_start"] == "2009-04-01" for page in pages for row in page["records"])
    else:
        first = pages[0]["records"][0]
        assert first["funding_year_literal"] == "X"
        assert first["values"]["net_expenditures"] == "-10374.00"
        assert first["values"]["supplementals"] == "0.00"
    args = {"chamber": case["chamber"], "profile": case["profile"], "archive_root": tmp_path / "raw"}
    assert read_pages(tmp_path / "qualified.json", tmp_path / "reading", **args) == result
    reused = read_pages(tmp_path / "qualified.json", tmp_path / "reused", reuse_manifest=path, **args)
    assert reused["files"][0]["output"] == entry["output"]
    with pytest.raises(ValueError, match="identical selection, reader and runtime"):
        read_pages(
            tmp_path / "qualified.json",
            tmp_path / "reading",
            chamber=case["chamber"],
            profile="ledger",
            archive_root=tmp_path / "raw",
        )
    artifact, evidence = build(path, tmp_path / "tables")
    generation = tmp_path / "tables/generation"
    assert verify_generation(generation, expected_pin=artifact.pin).pin == artifact.pin
    _, tables, _ = family_tables(case["chamber"], case["profile"])
    selected = download_generation(monkeypatch, generation, evidence, tmp_path / "selected", tables)
    monkeypatch.setattr(mcp_server, "DATA_DIR", selected)
    with mcp_server._build_connection() as connection:
        assert connection.execute(f"SELECT count(*) FROM {tables[0]}").fetchone() == (entry["records"],)
        assert connection.execute(f"SELECT count(*), sum(record_count) FROM {tables[2]}").fetchone() == (
            entry["pages"],
            entry["records"],
        )
        restored = connection.execute(
            f"SELECT source_page, summary_row_index, source_row_json FROM {tables[0]} ORDER BY 1, 2"
        ).fetchall()
        assert [json.loads(row[2]) for row in restored] == [row for page in pages for row in page["records"]]
        if case["id"] == "house-financial":
            assert connection.execute(
                f"SELECT value_decimal, value_scale, value_unit FROM {tables[1]} "
                "WHERE appropriation_title='Net Expenses of Telecommunications revolving fund' AND value_name='funds_available'"
            ).fetchone() == (Decimal("2316439.3200"), 2, "USD")
            assert connection.execute(
                f"SELECT count(*) > 0 FROM {tables[1]} WHERE value_status='blank' AND value_decimal IS NULL"
            ).fetchone() == (True,)
            assert connection.execute(
                f"SELECT count(*) > 0 FROM {tables[1]} WHERE value_decimal=0 AND value_status='parsed'"
            ).fetchone() == (True,)
        elif case["id"] == "house-mailing":
            assert connection.execute(
                f"SELECT DISTINCT report_period_start, summary_period_start, summary_period_end, amended FROM {tables[0]}"
            ).fetchall() == [(date(2009, 7, 1), date(2009, 4, 1), date(2009, 6, 30), True)]
            assert connection.execute(
                f"SELECT DISTINCT value_scale, value_unit FROM {tables[1]} WHERE value_name='average_cost_per_household'"
            ).fetchone() == (4, "USD/household")
            assert connection.execute(f"SELECT count(*) FROM {tables[1]}").fetchone() == (entry["records"] * 4,)
        else:
            assert connection.execute(
                f"SELECT value_decimal FROM {tables[1]} WHERE summary_row_index=1 AND value_name='net_expenditures'"
            ).fetchone() == (Decimal("-10374.0000"),)


@pytest.mark.parametrize("case", ALLOWANCE_CASES, ids=lambda case: case["id"])
def test_allowance_pages_use_shared_retention_typed_generation_and_local_consumer(tmp_path, monkeypatch, case):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    body = (FIXTURES / case["fixture"]).read_bytes()
    assert "sha256:" + hashlib.sha256(body).hexdigest() == case["fixture_sha256"]
    source = {key: case[key] for key in ("id", "url", "period_start", "period_end", "reading_pages", "reading_scope")}
    source["fixture"] = FIXTURES / case["fixture"]
    manifest = read_pdf_fixtures(tmp_path, "house", [source], profile="allowance-summary")
    result = json.loads(manifest.read_text())
    entry = result["files"][0]
    assert result["complete"] and not result["pending"]
    assert entry["repairs"] == 0  # These exact excerpts do not borrow a complete original's correction identity.
    with gzip.open(entry["output"]["path"], "rt") as stream:
        pages = [json.loads(line) for line in stream]
    assert any(page["status"] == "allowance_formula_unparsed" for page in pages)
    assert all(not row["issues"] for page in pages for row in page["records"])
    kwargs: dict[str, Any] = {"chamber": "house", "profile": "allowance-summary", "archive_root": tmp_path / "raw"}
    assert read_pages(tmp_path / "qualified.json", tmp_path / "reading", **kwargs) == result
    reused = read_pages(tmp_path / "qualified.json", tmp_path / "reused", reuse_manifest=manifest, **kwargs)
    assert reused["files"][0]["output"] == entry["output"]
    artifact, evidence = build(manifest, tmp_path / "tables")
    generation = tmp_path / "tables/generation"
    assert verify_generation(generation, expected_pin=artifact.pin).pin == artifact.pin
    family, tables, _ = family_tables("house", "allowance-summary")
    assert family == "house-allowance-summary"
    selected = download_generation(monkeypatch, generation, evidence, tmp_path / "selected", tables)
    monkeypatch.setattr(mcp_server, "DATA_DIR", selected)
    with mcp_server._build_connection() as connection:
        assert connection.execute(f"SELECT count(*) FROM {tables[0]}").fetchone() == (entry["records"],)
        assert connection.execute(f"SELECT count(*) FROM {tables[1]}").fetchone() == (entry["records"],)
        assert connection.execute(
            f"SELECT DISTINCT value_name, value_scale, value_unit FROM {tables[1]}"
        ).fetchall() == [("allowance_amount", 2, "USD")]
        assert connection.execute(f"SELECT count(*),sum(record_count) FROM {tables[2]}").fetchone() == (
            entry["pages"],
            entry["records"],
        )
        actual = connection.execute(
            f"SELECT source_row_json FROM {tables[0]} ORDER BY source_page, summary_row_index"
        ).fetchall()
        assert [json.loads(value[0]) for value in actual] == [row for page in pages for row in page["records"]]
        actual_pages = connection.execute(f"SELECT page_reading_json FROM {tables[2]} ORDER BY source_page").fetchall()
        assert [json.loads(value[0]) for value in actual_pages] == pages
        if case["period_start"] == "2009-07-01":
            assert connection.execute(
                f"SELECT label,source_row,value_decimal,allowance_year,state_literal,district_literal FROM {tables[1]} WHERE source_page=3 AND summary_row_index=1"
            ).fetchone() == ("Blumenauer, Earl", 0, Decimal("1538989.0000"), 2009, "OR", "03")
        else:
            assert connection.execute(
                f"SELECT report_period_start,summary_period_start,summary_period_end,amended,allowance_year,value_decimal FROM {tables[1]} WHERE label='Abraham, Ralph' ORDER BY source_page"
            ).fetchall() == [
                (date(2017, 10, 1), None, None, False, 2017, Decimal("1352079.0000")),
                (date(2017, 10, 1), date(2017, 7, 1), date(2017, 9, 30), True, 2017, Decimal("1352079.0000")),
            ]


def test_allowance_coordinates_bind_raw_fields_to_the_retained_table():
    from copy import deepcopy
    from spicy_regs.pipelines.disbursement_page_profiles import validate_page_content

    page: dict[str, Any] = {
        "tables": [{"row_count": 1, "cells": [["Vacant district", "NMI", "00", "$1.00"]]}],
        "records": [
            {
                "table": 0,
                "row": 0,
                "raw_fields": {
                    "name_literal": "Vacant district",
                    "state_literal": "NMI",
                    "district_literal": "00",
                    "allowance_amount": "$1.00",
                },
                "values": {
                    "name_literal": "Vacant district",
                    "state_literal": "NMI",
                    "district_literal": "00",
                    "allowance_amount": "1.00",
                },
            }
        ],
    }
    validate_page_content(page, "allowance-summary")
    changed = deepcopy(page)
    changed["records"][0]["raw_fields"]["allowance_amount"] = "$2.00"
    with pytest.raises(ValueError, match="retained source cells"):
        validate_page_content(changed, "allowance-summary")
    changed = deepcopy(page)
    changed["records"][0]["values"]["extra"] = "1.00"
    with pytest.raises(ValueError, match="retained source cells"):
        validate_page_content(changed, "allowance-summary")
