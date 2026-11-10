"""GAO main lookup keys match the existing resolver without changing source values."""

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.citation_resolution import ROUTES
from spicy_regs.policy_migrations import earlier_policies
from spicy_regs.etl_receipts import ReceiptContext, read_with_receipts, write_dataset
from spicy_regs.explorer_navigation import declarations, published_navigation, scalar, target_keys
from spicy_regs.gao_citation_keys import CITATION_PRODUCT_SQL, citation_product_id
from spicy_regs.transforms.government_receipts import BUILD_METADATA, EARLIER_POLICIES, POLICIES, internal_prior, receipt_builder
from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, SUBJECT_SCHEMAS, map_subject
from tests.test_citation_resolution import occurrence, resolve


def rows():
    return [
        {"report_id": "gao-27-108551", "report_number": None},
        {"report_id": "d23105520", "report_number": "GAO-23-105520"},
        {"report_id": "gao-16-75sp", "report_number": "GAO-16-75SP"},
        {"report_id": "gao-16-75sp-0", "report_number": "GAO-16-75SP"},
        {"report_id": "gao-20-100", "report_number": ""},
        {"report_id": "gao-20-101", "report_number": "  "},
        {"report_id": "gao-20-102", "report_number": "GAO-20-123ß"},
        {"report_id": "gao-20-103", "report_number": "GAO-20-123ﬀ"},
        {"report_id": "gao-20-104", "report_number": "GAO-20-123ı"},
    ]


def gao_target():
    return next(t for s in declarations() if s["id"] == "citation_recorded_targets"
                for t in s["targets"] if t["table"] == "gao_reports")


def test_new_main_keys_produce_the_existing_resolvers_exact_target_sets():
    original = rows()
    subjects = [map_subject("gao_reports", row) for row in original]
    assert original == rows()
    with duckdb.connect() as con:
        con.register("gao_reports", pa.Table.from_pylist(subjects, schema=SUBJECT_SCHEMAS["gao_reports"]))
        assert con.execute(f"SELECT {ROUTES['gao_product_id'].key_sql} FROM gao_reports").fetchall() == [
            (row["citation_product_id"],) for row in subjects]
        for lookup in ("GAO-27-108551", "GAO-23-105520", "GAO-16-75SP", "GAO-20-100", "GAO-20-101"):
            found = resolve(con, [occurrence("gao_product_id", lookup)],
                            {"gao_reports": {"artifact_digest": "fixture"}})["occurrences"][0]
            expected = [row["report_id"] for row in subjects if row["citation_product_id"] == lookup]
            assert [key["report_id"] for key in found.get("candidate_keys", [])] == sorted(expected)
            assert found["target_status"] == ("ambiguous" if len(expected) > 1 else "found" if expected else "missing")
    assert subjects[4]["citation_product_id"] == ""
    assert subjects[5]["citation_product_id"] == "  "
    assert subjects[6]["citation_product_id"] != subjects[6]["report_number"].upper()
    assert CITATION_PRODUCT_SQL == ROUTES["gao_product_id"].key_sql


def test_main_navigation_preserves_aliases_refusals_and_both_directions():
    targets = [map_subject("gao_reports", row) for row in rows()]
    recipe = gao_target()
    assert recipe["columns"] == ["citation_product_id"]
    source = {"target_key": "GAO-16-75SP", "target_resolved": True, "cite_kind": "gao_product_id",
              "target_table": "gao products (product id as GAO prints it)", "target_rule": "gao_product_id"}
    keys = target_keys(recipe, {}, source)
    matches = [r for r in targets if [scalar(r[c]) for c in recipe["columns"]] == keys]
    assert [r["report_id"] for r in matches] == ["gao-16-75sp", "gao-16-75sp-0"]
    assert [row for row in [source, dict(source)] if target_keys(recipe, {}, row) ==
            [matches[0]["citation_product_id"]]] == [source, source]
    for changes in ({"target_resolved": False}, {"target_table": "gao_reports"},
                    {"target_rule": "guessed"}, {"target_key": "gao-16-75sp"}):
        assert target_keys(recipe, {}, {**source, **changes}) is None
    specs = [s for s in declarations() if s["id"] == "citation_recorded_targets"]
    source_schema = [[name, "BOOLEAN" if name == "target_resolved" else "VARCHAR"] for name in source]
    old_schema = [["report_id", "VARCHAR"], ["report_number", "VARCHAR"]]
    old = published_navigation(specs, {"document_citations": source_schema, "gao_reports": old_schema})[0]
    assert not next(t for t in old["targets"] if t["table"] == "gao_reports")["available"]
    new = published_navigation(specs, {"document_citations": source_schema,
                                      "gao_reports": old_schema + [["citation_product_id", "VARCHAR"]]})[0]
    assert next(t for t in new["targets"] if t["table"] == "gao_reports")["available"]


def test_gao_only_policy_change_retains_exact_previous_schemas_and_input_columns():
    current = POLICIES["gao_reports"]
    previous, v2, v1 = EARLIER_POLICIES["gao_reports"]
    assert [p.policy_version for p in (current, previous, v2, v1)] == [
        "government-sources/4", "government-sources/3", "government-sources/2", "government-sources/1"]
    assert current.subject_schema.remove(current.subject_schema.get_field_index("citation_product_id")).equals(
        previous.subject_schema, check_metadata=True)
    assert current.identity_fields == previous.identity_fields == ("report_id",)
    assert "citation_product_id" not in LEGACY_COLUMNS["gao_reports"]
    assert POLICIES["gao_decisions"].policy_version == POLICIES["fcc_filings"].policy_version == "government-sources/3"
    # Only the exact recorded descriptors read their predecessors.
    assert earlier_policies(previous) == (v2, v1)
    assert earlier_policies(replace(previous, subject_schema=previous.subject_schema.append(pa.field("extra", pa.string())))) == ()
    assert earlier_policies(replace(current, identity_fields=("title",))) == ()


def test_v3_native_prior_restores_then_adds_key_without_changing_old_fields(tmp_path):
    previous = EARLIER_POLICIES["gao_reports"][0]
    raw = [{**dict.fromkeys(LEGACY_COLUMNS["gao_reports"]), **row,
            "title": "Same title cannot identify a report", "published_date": "2026-10-01"} for row in rows()]
    originals = deepcopy(raw)
    subjects = [{key: value for key, value in map_subject("gao_reports", row).items()
                 if key in previous.subject_schema.names} for row in raw]
    witness = {"source_id": "test:gao", "source_uri": None, "sha256": "sha256:" + "0" * 64,
               "locator": None, "body_version": None}
    held, receipts = write_dataset([
        ({**subject, "raw_record": source}, ReceiptContext("v3", f"row:{i}", previous.policy_version, [witness]))
        for i, (subject, source) in enumerate(zip(subjects, raw, strict=True))
    ], tmp_path / "v3", previous)
    assert held is not None
    old_receipt_bytes = receipts.read_bytes()
    @receipt_builder(dataset="gao_reports")
    def rebuild():
        restored = internal_prior("gao_reports", held, receipt_path=receipts, generation_id="v3")
        assert pq.read_table(restored).to_pylist() == originals
        output = tmp_path / "gao_reports.parquet"
        pq.write_table(pq.read_table(restored), output)
        return output

    output = rebuild(receipt_generation_id="v4")
    result = json.loads((tmp_path / BUILD_METADATA).read_text())
    new = pq.read_table(output).to_pylist()
    assert [{k: row[k] for k in previous.subject_schema.names} for row in new] == subjects
    assert [row["citation_product_id"] for row in new] == [citation_product_id(source) for source in originals]
    assert receipts.read_bytes() == old_receipt_bytes
    assert [row["raw_record"] for row in read_with_receipts(
        [output], [Path(result["receipt_path"])], POLICIES["gao_reports"], generation_id="v4")] == originals
    assert raw == originals


def test_native_dictionary_has_exact_policy_columns_and_new_key_meaning():
    from spicy_regs.data_dictionary import load_descriptions
    prose = load_descriptions()["gao_reports"]["columns"]
    assert set(prose) == set(POLICIES["gao_reports"].subject_schema.names)
    assert "NULL" in prose["citation_product_id"] and "Blank" in prose["citation_product_id"]
    root = Path(__file__).parents[1] / "src/spicy_regs"
    assert json.loads((root / "etl_policies/gao_reports.json").read_text()) == POLICIES["gao_reports"].descriptor()
