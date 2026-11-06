"""Formula observations retain units, witnesses and distinct shared-host identity."""

from collections import Counter
from copy import deepcopy
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pytest

from spicy_regs.pipelines.congressional_disbursement_pages import implementation, pin, source_implementation
from spicy_regs.pipelines.disbursement_page_profiles import validate_page_content
from spicy_regs.schemas.disbursement_summaries import family_tables
from spicy_regs.transforms.build_disbursement_summaries import summary_rows

FIXTURES = Path(__file__).parent / "fixtures/disbursement_formulas"
PROVENANCE = json.loads((FIXTURES / "provenance.json").read_text())
BODY = (FIXTURES / PROVENANCE["fixture"]).read_bytes()
assert "sha256:" + hashlib.sha256(BODY).hexdigest() == PROVENANCE["fixture_sha256"]
CASES = json.loads(gzip.decompress(BODY))


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["source_id"])
def test_formula_host_and_typed_cells_preserve_source_facts(tmp_path, case):
    page = case["page"]
    validate_page_content(page, "allowance-formula")
    path = tmp_path / "page.jsonl.gz"
    path.write_bytes(gzip.compress((json.dumps(page) + "\n").encode(), mtime=0))
    source = {
        "id": case["source_id"],
        "source_sha256": page["source_sha256"],
        "url": "https://www.house.gov/sites/default/files/fixture.pdf",
        "period_start": case["source_id"][6:16],
        "period_end": "2009-09-30" if "2009" in case["source_id"] else case["source_id"][6:10] + "-03-31",
    }
    entry = {
        "output": pin(path),
        "selected_pages": [page["source_page"]],
        "physical_pages": page["physical_pages"],
        "reading_profile": "allowance-formula",
        "pages": 1,
        "records": len(page["records"]),
        "repairs": sum(len(row.get("repairs", [])) for row in page["records"]),
        "page_statuses": {page["status"]: 1},
        "row_kinds": dict(Counter(row["row_kind"] for row in page["records"])),
    }
    _, tables, schemas = family_tables("house", "allowance-formula")
    typed = {
        table: pa.Table.from_pylist(
            list(summary_rows(source, entry, table, chamber="house", profile="allowance-formula", tables=tables)),
            schema=schemas[table],
        ).to_pylist()
        for table in tables
    }
    rows, values, pages = [typed[table] for table in tables]
    assert [json.loads(row["source_row_json"]) for row in rows] == page["records"]
    assert json.loads(pages[0]["page_reading_json"]) == page
    assert len(values) == sum(len(row["values"]) for row in page["records"])
    for typed_row in values:
        row = page["records"][typed_row["summary_row_index"] - 1]
        field = typed_row["value_name"]
        assert typed_row["value_decimal"] == Decimal(row["values"][field])
        assert typed_row["raw_value"] == row["raw_fields"][field]
        assert (typed_row["value_scale"], typed_row["value_unit"]) == (
            row["value_specs"][field]["scale"],
            row["value_specs"][field]["unit"],
        )
        assert typed_row["value_status"] == "parsed"
        assert typed_row["formula_parameter"] == row["parameter"]
        assert typed_row["formula_target_year"] == row["target_year"]
        assert typed_row["lower_inclusive"] == row.get("lower_inclusive")
        assert typed_row["upper_inclusive"] == row.get("upper_inclusive")
    assert all(row["source_table"] is None and row["source_row"] is None and row["source_line"] is None for row in rows)


@pytest.mark.parametrize("failure", ["unit", "number", "source_span", "audit", "duplicate"])
def test_formula_host_rejects_corrupted_native_witnesses(failure):
    page = deepcopy(next(case["page"] for case in CASES if "2013" in case["source_id"]))
    row = next(row for row in page["records"] if row["parameter"] == "clerk_hire_base")
    if failure == "unit":
        row["value_specs"]["value"]["unit"] = "days"
    elif failure == "number":
        row["values"]["value"] = "944671.01"
    elif failure == "source_span":
        row["origin"]["start"] += 1
    elif failure == "audit":
        next(row for row in page["records"] if row["parameter"] == "mail_calculation")["repairs"] = []
    else:
        page["records"].append(deepcopy(row))
    with pytest.raises(ValueError):
        validate_page_content(page, "allowance-formula")


def test_formula_repairs_use_existing_source_resource_retention():
    producer = implementation("house", "allowance-formula")
    assert any(item["path"].endswith("house_allowance_formulas_corrections.json") for item in producer["files"])
    page = next(case["page"] for case in CASES if "2013" in case["source_id"])
    source = {
        "id": "house-2013-01-01-pdf-full",
        "chamber": "house",
        "kind": "full",
        "url": "https://www.house.gov/sites/default/files/uploads/documents/2013q1_singlevolume.pdf",
        "source_sha256": page["source_sha256"],
        "source_byte_size": page["source_size_bytes"],
        "period_start": "2013-01-01",
        "period_end": "2013-03-31",
    }
    rules = source_implementation(source, producer)["rules"]
    corrections = [rule for rule in rules if rule.get("parameter") == "mail_calculation"]
    assert len(corrections) == 1
    assert corrections[0]["before"] == "sum" and corrections[0]["after"] == "product"


def test_formula_profile_uses_existing_retention_generation_and_consumer(tmp_path, monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.generations import verify_generation
    from spicy_regs.pipelines.congressional_disbursement_tables import build
    from tests.disbursement_fixtures import read_pdf_fixtures
    from tests.generation_fakes import download_generation
    from tests.test_disbursement_summary_pages import FIXTURES as SUMMARY_FIXTURES

    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    source = {
        "id": "house-formula-excerpt",
        "url": "https://www.house.gov/sites/default/files/uploads/documents/2009q3_singlevolume.pdf",
        "fixture": SUMMARY_FIXTURES / "house-2009-07-01-allowance.pdf",
        "period_start": "2009-07-01",
        "period_end": "2009-09-30",
        "reading_pages": [1],
        "reading_scope": "Retained source formula excerpt; selected physical fixture page only.",
    }
    path = read_pdf_fixtures(tmp_path, "house", [source], profile="allowance-formula")
    manifest = json.loads(path.read_text())
    assert manifest["complete"] and manifest["pages"] == 1
    with gzip.open(manifest["files"][0]["output"]["path"], "rt") as stream:
        page = json.loads(next(stream))
    assert page["status"] == "allowance_formula_facts_read"
    artifact, evidence = build(path, tmp_path / "tables")
    generation = tmp_path / "tables/generation"
    assert verify_generation(generation, expected_pin=artifact.pin).pin == artifact.pin
    _, tables, _ = family_tables("house", "allowance-formula")
    selected = download_generation(monkeypatch, generation, evidence, tmp_path / "selected", tables)
    monkeypatch.setattr(mcp_server, "DATA_DIR", selected)
    with mcp_server._build_connection() as connection:
        restored = connection.execute(f"SELECT source_row_json FROM {tables[0]} ORDER BY summary_row_index").fetchall()
        assert [json.loads(row[0]) for row in restored] == page["records"]
        assert connection.execute(
            f"SELECT count(*) FROM {tables[1]} WHERE formula_parameter='mileage_rate' AND value_name='rate'"
        ).fetchone() == (8,)
        assert connection.execute(
            f"SELECT value_decimal,value_unit FROM {tables[1]} WHERE formula_parameter='mileage_rate' AND value_name='rate' ORDER BY summary_row_index DESC LIMIT 1"
        ).fetchone() == (Decimal("1.3200"), "USD/mile")
        assert connection.execute(f"SELECT count(*) FROM {tables[1]} WHERE value_status<>'parsed'").fetchone() == (0,)
