"""Historical and current court keys retain exact receipt-bound identity."""
from collections import Counter
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import legislative_receipts, mcp_server
from spicy_regs.court_receipts import local_receipt_selection, write_court_rows
from spicy_regs.court_subjects import SUBJECT_SCHEMAS, normalize_court_row
from spicy_regs.legislative_documents import map_subject, native_document_key
from spicy_regs.policy_migrations import earlier_policy
from spicy_regs.selected_generations import SelectedDataset, remember_selection
from spicy_regs.transforms.held_citations import NAMESPACE, Selection, build_held_citations, write_citation_reads
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from tests.test_mcp_server import _tool_data


def selected_cohort(root, monkeypatch, *, body_keys, damage=None, mixed_keys=False):
    fixture = json.loads((Path(__file__).parent / "fixtures/court_citations/derived-text-cohort.json").read_text())
    source = [normalize_court_row("court_opinion_pdf_extractions", row) for row in fixture["rows"]]
    legacy = {row["opinion_body_id"]: json.dumps([row["opinion_id"], row["source_sha256"]], separators=(",", ":"))
              for row in source}
    with duckdb.connect() as con:
        con.register("court_opinion_pdf_extractions", pa.Table.from_pylist(
            source, schema=SUBJECT_SCHEMAS["court_opinion_pdf_extractions"]))
        findings = build_held_citations(
            root / "historical", cursor=con,
            selections=[Selection("court_opinion_derived_pdf", (row["opinion_body_id"],)) for row in source],
            input_pins={"court_opinion_pdf_extractions": {"artifactDigest": fixture["publication"]}},
            download_prior=lambda *_: False,
        )
    # Retain the historical capture spelling in both findings and checkpoints.
    checkpoints = [{**row, "document_key": legacy[row["document_key"]]}
                   for row in read_checkpoints(findings, NAMESPACE)]
    table = pq.read_table(findings)
    table = table.set_column(table.schema.get_field_index("document_key"), "document_key",
                             pa.array([legacy[key] for key in table["document_key"].to_pylist()]))
    pq.write_table(table.replace_schema_metadata(checkpoint_metadata(findings, NAMESPACE, checkpoints)), findings)
    if mixed_keys:
        mixed = pq.read_table(findings)
        keys = mixed["document_key"].to_pylist()
        keys[0] = native_document_key("court_opinion_derived_pdf", keys[0])
        pq.write_table(mixed.set_column(mixed.schema.get_field_index("document_key"), "document_key",
                                       pa.array(keys)), findings)
    reads = write_citation_reads(findings.parent, findings)
    historical_policy = earlier_policy("document_citations", "legislative-documents/1")

    def captured_mapper(dataset, raw):
        mapped = map_subject(dataset, raw)
        if mapped is not None and dataset == "document_citations":
            if not body_keys:
                mapped = {name: mapped[name] for name in historical_policy.subject_schema.names}
            mapped["document_key"] = (native_document_key(raw["document_kind"], raw["document_key"])
                                      if body_keys else raw["document_key"])
            if damage == "body":
                mapped["document_key"] = "court-opinion-body:" + "0" * 64
            elif damage == "field":
                mapped["target_rule"] = "contradicted-rule"
        return mapped

    # The writer creates each historically valid layout with real receipts.
    # Only fixture creation overrides spelling; serving uses the current mapper.
    bundle = root / "citations"
    with monkeypatch.context() as producer:
        producer.setattr(legislative_receipts, "map_subject", captured_mapper)
        if not body_keys:
            producer.setattr(legislative_receipts, "_processor", lambda _: "legislative-documents/1")
            current_policy = legislative_receipts.policy
            producer.setattr(legislative_receipts, "policy", lambda dataset:
                             historical_policy if dataset == "document_citations" else current_policy(dataset))
        manifest = legislative_receipts.write_legislative_outputs([findings, reads], bundle, generation_id="citations")
    for dataset in manifest["datasets"]:
        remember_selection(root, [SelectedDataset(
            dataset, tuple(bundle / key for key in manifest["subjects"].get(dataset, ())),
            bundle / manifest["receipt_file"], "citations")])
    pdf = write_court_rows("court_opinion_pdf_extractions", fixture["rows"], root / "pdf", generation_id="pdf",
                           witnesses=[{"source_id": "retained-court-cohort", "sha256": fixture["publication"]}])
    receipt, generation = local_receipt_selection(pdf)
    remember_selection(root, [SelectedDataset("court_opinion_pdf_extractions", (pdf,), receipt, generation)])
    return source, legacy


@pytest.mark.parametrize("body_keys", [False, True])
def test_server_reads_historical_and_current_court_subjects(tmp_path, monkeypatch, body_keys):
    source, legacy = selected_cohort(tmp_path, monkeypatch, body_keys=body_keys)
    restored = legislative_receipts.restore_prior(tmp_path / "citations", tmp_path / "restored")
    original = pq.read_table(tmp_path / "historical/document_citations.parquet")
    assert pq.read_table(restored["document_citations"]).equals(original, check_metadata=True)
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    with mcp_server._build_connection() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        findings = Counter()
        for row in source:
            reply = _tool_data(server, "resolve_document_citations", {
                "document_kind": "court_opinion_derived_pdf", "document_key": row["opinion_body_id"],
                "max_occurrences": 100,
            })
            findings[row["opinion_id"]] = len(reply["occurrences"])
            assert reply["source_read"]["status"] == ("read" if findings[row["opinion_id"]] else "read_none_found")
        assert findings == {"11209378": 6, "11264531": 12, "11264529": 0}
        public_keys = {key for key, in con.execute("SELECT DISTINCT document_key FROM document_citations").fetchall()}
        assert public_keys <= (set(legacy) if body_keys else set(legacy.values()))
    receipt_rows = pq.read_table(tmp_path / "citations/etl_receipts.parquet").to_pylist()
    attempts = [r for r in receipt_rows if r["dataset"] == "document_citations"]
    assert attempts and {r["processor"] for r in attempts} == {f"legislative-documents/{3 if body_keys else 1}"}
    assert {r["policy_version"] for r in attempts} == {f"legislative-documents/{2 if body_keys else 1}"}
    raw = pq.read_table(tmp_path / "historical/document_citations.parquet").to_pylist()[0]
    mapped = map_subject("document_citations", raw)
    assert mapped is not None and mapped["document_key"] == native_document_key(raw["document_kind"], raw["document_key"])


@pytest.mark.parametrize("damage", ["body", "field"])
def test_court_subject_must_match_its_exact_source_body_and_other_fields(tmp_path, monkeypatch, damage):
    source, _ = selected_cohort(tmp_path, monkeypatch, body_keys=True, damage=damage)
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    with mcp_server._build_connection() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        # Exact source admission is lazy; the public tool must refuse before
        # returning any citation evidence, even with valid generic receipt joins.
        server = mcp_server.build_server()
        assert _tool_data(server, "query_sql", {
            "sql": "SELECT count(*) AS count FROM document_citations",
        })["rows"] == [[18]]
        with pytest.raises(ToolError, match="differs from its selected native subject"):
            _tool_data(server, "resolve_document_citations", {
                "document_kind": "court_opinion_derived_pdf", "document_key": source[0]["opinion_body_id"],
            })
        assert con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE starts_with(table_name, '_spicy_citation_')"
        ).fetchone() == (0,)


@pytest.mark.parametrize("damage", ["body", "field"])
def test_incremental_prior_refuses_subject_receipt_disagreement(tmp_path, monkeypatch, damage):
    selected_cohort(tmp_path, monkeypatch, body_keys=True, damage=damage)
    with pytest.raises(ValueError, match="Retained conversion input disagrees"):
        legislative_receipts.restore_prior(tmp_path / "citations", tmp_path / "refused")


def test_serving_refuses_converging_historical_and_native_source_scopes(tmp_path, monkeypatch):
    source, _ = selected_cohort(tmp_path, monkeypatch, body_keys=False, mixed_keys=True)
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    with mcp_server._build_connection() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        with pytest.raises(ToolError, match="converge"):
            _tool_data(mcp_server.build_server(), "resolve_document_citations", {
                "document_kind": "court_opinion_derived_pdf", "document_key": source[0]["opinion_body_id"],
            })
        assert con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE starts_with(table_name, '_spicy_citation_')"
        ).fetchone() == (0,)
