import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, validate_receipt_bundle
from spicy_regs.transforms.regulations_receipts import (
    ReceiptInput,
    build_local_generation,
    policy,
    read_internal,
    write_records,
)
from spicy_regs.transforms.regulations_shape import IDENTITIES, SOURCE_COLUMNS, subject_schema


def context(generation="g1", attempt="row1"):
    return ReceiptContext(
        generation,
        attempt,
        "test-processor",
        [
            {
                "source_id": "captured-source",
                "source_uri": "https://example.gov/source",
                "sha256": "a" * 64,
                "locator": "/records/0",
                "body_version": None,
            }
        ],
    )


def write(tmp_path, dataset, rows, generation="g1", label="input"):
    subjects, receipts = write_records(
        dataset, ((r, context(generation, str(i))) for i, r in enumerate(rows)), tmp_path / label
    )
    return ReceiptInput(dataset, (subjects,), receipts, generation)


def assert_direct_prior_reference(receipt, prior):
    from hashlib import sha256
    from spicy_regs.etl_receipts import decode_exact_json

    diagnostics = decode_exact_json(receipt["diagnostic_json"])
    assert diagnostics["prior_receipt"] == {
        "receipt_id": prior["receipt_id"],
        "generation_id": prior["generation_id"],
        "processing_sha256": "sha256:" + sha256(prior["processing_json"].encode()).hexdigest(),
    }
    assert "prior_receipts" not in diagnostics and "retained_processing" not in diagnostics


@pytest.mark.parametrize("dataset", list(SOURCE_COLUMNS))
def test_every_assigned_policy_can_round_trip_a_qualified_row(tmp_path, dataset):
    row: dict = {k: "1" for k in IDENTITIES[dataset] if k not in ("rule_target_id", "lifecycle_event_id")}
    row.update({k: 1 for k in row if k in ("year", "month", "docket_source_ordinal")})
    if dataset == "lifecycle_events":
        row.update(proceeding_id="P", document_id="D", stage="proposed", event_date=date(2026, 1, 1))
    if dataset == "rule_targets":
        row.update(docket_id="D", source="docket_rin", rin="1000-AA00")
    selected = write(tmp_path, dataset, [row])
    actual = list(read_internal(selected))
    assert len(actual) == 1
    assert all(actual[0][k] == v for k, v in row.items())
    validate_receipt_bundle(
        {dataset: list(selected.subjects)}, [selected.receipts], [policy(dataset)], generation_id="g1"
    )
    assert pq.read_schema(selected.subjects[0]).equals(subject_schema(dataset))


def test_malformed_input_has_diagnosable_receipt_without_fabricated_subject(tmp_path):
    selected = write(
        tmp_path,
        "documents",
        [
            {"document_id": "good", "attachments_json": "[]"},
            {"document_id": "bad", "attachments_json": "{bad"},
            {"document_id": "extra", "undeclared": 7},
            {"document_id": None, "title": "unidentified source observation"},
        ],
    )
    assert [r["document_id"] for r in pq.read_table(selected.subjects[0]).to_pylist()] == ["good"]
    receipts = pq.read_table(selected.receipts).to_pylist()
    assert [r["outcome"] for r in receipts] == ["accepted", "refused", "refused", "refused"]
    assert "{bad" in receipts[1]["processing_json"]
    assert "undeclared" in receipts[2]["processing_json"]
    assert "unidentified source observation" in receipts[3]["processing_json"]
    assert len(list(read_internal(selected))) == 1


def test_wrong_generation_missing_and_wrong_subject_receipts_refuse(tmp_path):
    selected = write(tmp_path, "documents", [{"document_id": "D", "text_extraction_status": "ok"}])
    with pytest.raises(ValueError):
        list(read_internal(ReceiptInput("documents", selected.subjects, selected.receipts, "")))
    pq.write_table(
        pa.Table.from_pylist([{"document_id": "changed"}], schema=subject_schema("documents")), selected.subjects[0]
    )
    with pytest.raises(ValueError):
        list(read_internal(selected))


def test_aggregate_null_coordinates_do_not_merge_with_literal_strings(tmp_path):
    selected = write(
        tmp_path,
        "comments_index",
        [
            {"agency_code": "A", "docket_id": None, "year": None, "month": None, "row_count": 5},
            {"agency_code": "A", "docket_id": "null", "year": None, "month": None, "row_count": 7},
        ],
    )
    assert len(list(read_internal(selected))) == 2
    assert len({r["record_id"] for r in pq.read_table(selected.receipts).to_pylist()}) == 2


def test_local_generation_admits_receipts_separately_from_domain_tables(tmp_path):
    source = tmp_path / "held.parquet"
    pq.write_table(pa.table({"docket_id": ["D"], "modify_date": ["2026-01-01"]}), source)
    artifact = build_local_generation({"dockets": source}, tmp_path / "generation", generation_id="g")
    assert "etl_receipts.parquet" not in artifact.root["spec"]["tables"]
    assert (tmp_path / "generation/etl_receipts.parquet").is_file()
    assert source.is_file()


def test_receipt_pdf_retries_skip_prior_status_and_keep_global_limit(tmp_path):
    from hashlib import sha256

    from spicy_regs.enrich_pdf import enrich_receipted_dataset
    from spicy_regs.etl_receipts import decode_exact_json
    from tests.pdf_fixtures import make_pdf

    selected = write(
        tmp_path,
        "documents",
        [
            {
                "document_id": str(i),
                "attachments_json": json.dumps([{"url": f"https://example.gov/{i}.pdf", "format": "pdf", "size": 1}]),
                "text_extraction_status": "error" if i == 0 else None,
                "text_content": "retained" if i == 0 else None,
            }
            for i in range(4)
        ],
    )
    calls = []

    def fetch(url):
        calls.append(url)
        return make_pdf(["new content"])

    (subject, receipts), stats = enrich_receipted_dataset(
        selected, tmp_path / "filled", generation_id="g2", fetch=fetch, limit=1, batch_size=2, max_workers=1
    )
    assert stats["selected"] == 1
    assert calls == ["https://example.gov/1.pdf"]
    restored = list(read_internal(ReceiptInput("documents", (subject,), receipts, "g2")))
    byid = {r["document_id"]: r for r in restored}
    assert byid["0"]["text_content"] == "retained" and byid["0"]["text_extraction_status"] == "error"
    assert byid["1"]["text_extraction_status"] == "ok"
    assert byid["2"]["text_extraction_status"] is None
    assert len(restored) == 4
    original_byid = {r['record_id']: r for r in pq.read_table(selected.receipts).to_pylist()
                     if r['outcome'] == 'accepted'}
    for receipt in pq.read_table(receipts).to_pylist():
        if receipt['outcome'] == 'accepted':
            prior = original_byid[receipt['record_id']]
            if (receipt['subject_version'], receipt['processing_json']) == (prior['subject_version'], prior['processing_json']):
                assert receipt == prior
            else:
                diagnostics = decode_exact_json(receipt['diagnostic_json'])
                assert diagnostics['prior_receipt'] == {
                    'receipt_id': prior['receipt_id'],
                    'generation_id': prior['generation_id'],
                    'processing_sha256': 'sha256:' + sha256(prior['processing_json'].encode()).hexdigest(),
                }
                assert 'prior_receipts' not in diagnostics and 'retained_processing' not in diagnostics
    assert "text_extraction_status" in pq.read_schema(subject).names


def test_pdf_requires_receipt_validation_before_fetch(tmp_path):
    from spicy_regs.enrich_pdf import enrich_receipted_dataset

    selected = write(tmp_path, "documents", [{"document_id": "D", "attachments_json": "[]"}])
    rows = pq.read_table(selected.receipts).to_pylist()
    rows[0]["processor"] = "changed without matching digest"
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), selected.receipts)
    wrong = selected
    with pytest.raises(ValueError):
        enrich_receipted_dataset(
            wrong, tmp_path / "out", generation_id="next", fetch=lambda _: pytest.fail("fetch before validation")
        )
    assert not (tmp_path / "out").exists()


def test_correction_keeps_text_status_and_diagnostics_together(tmp_path):
    from hashlib import file_digest

    from spicy_regs.transforms.regulations_correction import correct_receipted_dataset

    prior = write(
        tmp_path,
        "documents",
        [
            {
                "document_id": "D",
                "modify_date": "2026-01-01",
                "title": "old",
                "text_content": "retained",
                "text_extraction_status": "ok",
                "pdf_extraction_results_json": '[{"status":"ok"}]',
            }
        ],
        label="prior",
    )
    fresh = write(
        tmp_path,
        "documents",
        [
            {
                "document_id": "D",
                "modify_date": "2026-01-01",
                "title": "corrected",
                "text_content": None,
                "text_extraction_status": None,
            }
        ],
        generation="g2",
        label="fresh",
    )
    subject, receipts = correct_receipted_dataset(prior, fresh, tmp_path / "corrected", generation_id="g3")
    [row] = read_internal(ReceiptInput("documents", (subject,), receipts, "g3"))
    assert row["title"] == "corrected"
    assert (row["text_content"], row["text_extraction_status"], row["pdf_extraction_results_json"]) == (
        "retained",
        "ok",
        '[{"status":"ok"}]',
    )
    assert "pdf_extraction_results_json" in pq.read_schema(subject).names
    [receipt] = [r for r in pq.read_table(receipts).to_pylist() if r['outcome'] == 'accepted']
    [prior_receipt] = [r for r in pq.read_table(prior.receipts).to_pylist() if r['outcome'] == 'accepted']
    assert_direct_prior_reference(receipt, prior_receipt)
    for selected in (prior, fresh):
        for path in (*selected.subjects, selected.receipts):
            with path.open("rb") as body:
                digest = file_digest(body, "sha256").hexdigest()
            assert {"source_id": f"documents:{path.name}", "source_uri": None, "sha256": digest,
                    "locator": None, "body_version": selected.generation_id} in receipt["witnesses"]


def test_correction_keeps_current_failures_and_resolves_only_prior_refusals(tmp_path):
    from spicy_regs.etl_receipts import decode_exact_json
    from spicy_regs.transforms.regulations_correction import correct_receipted_dataset

    held_row = dict.fromkeys(column for column, _ in SOURCE_COLUMNS["documents"])
    held_row.update(document_id="held", title="unchanged")
    prior = write(tmp_path, "documents", [
        held_row,
        {"document_id": "resolved", "attachments_json": "{bad"},
        {"document_id": "prior-unmatched", "attachments_json": "{bad"},
    ], label="prior")
    fresh = write(tmp_path, "documents", [
        {"document_id": "resolved", "attachments_json": "[]"},
        {"document_id": "held", "attachments_json": "{bad"},
        {"document_id": "fresh-unmatched", "attachments_json": "{bad"},
    ], generation="g2", label="fresh")

    def refused(path):
        return {decode_exact_json(r["processing_json"])["raw_conversion_inputs"]["document_id"]: r
                for r in pq.read_table(path).to_pylist() if r["outcome"] == "refused"}

    subject, receipts = correct_receipted_dataset(prior, fresh, tmp_path / "corrected", generation_id="g3")
    corrected = ReceiptInput("documents", (subject,), receipts, "g3")
    assert {r["document_id"] for r in read_internal(corrected)} == {"held", "resolved"}
    prior_failed, fresh_failed = refused(prior.receipts), refused(fresh.receipts)
    expected = {"prior-unmatched": prior_failed["prior-unmatched"], **fresh_failed}
    assert refused(receipts) == expected
    [held] = [r for r in pq.read_table(prior.receipts).to_pylist() if r["outcome"] == "accepted"]
    assert held in pq.read_table(receipts).to_pylist()

    unrelated = write(tmp_path, "documents", [{"document_id": "other", "title": "unrelated correction"}],
                      generation="g4", label="unrelated")
    next_subject, next_receipts = correct_receipted_dataset(corrected, unrelated, tmp_path / "untouched", generation_id="g5")
    assert refused(next_receipts) == expected
    assert held in pq.read_table(next_receipts).to_pylist()

    untouched = ReceiptInput("documents", (next_subject,), next_receipts, "g5")
    successful = write(tmp_path, "documents", [{"document_id": "held", "title": "corrected"}],
                       generation="g6", label="successful")
    _, following = correct_receipted_dataset(untouched, successful, tmp_path / "following", generation_id="g7")
    assert refused(following) == {key: value for key, value in expected.items() if key != "held"}


def test_real_rulemaking_producers_build_native_outputs_from_qualified_inputs(tmp_path):
    from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline

    source_rows = {
        "dockets": [
            {
                "docket_id": "EPA-2026-0001",
                "agency_code": "EPA",
                "docket_type": "Rulemaking",
                "title": "A rule",
                "rin": "1000-AA00",
            }
        ],
        "documents": [
            {
                "document_id": "EPA-2026-0001-0001",
                "docket_id": "EPA-2026-0001",
                "agency_code": "EPA",
                "document_type": "Proposed Rule",
                "posted_date": "2026-01-01",
                "modify_date": "2026-01-01",
                "comment_end_date": "2026-03-01",
                "title": "Proposal",
            }
        ],
        "federal_register": [],
        "unified_agenda": [],
        "fr_docket_links": [],
    }
    inputs = [write(tmp_path, name, rows, label=name) for name, rows in source_rows.items()]
    target = tmp_path / "native-rulemaking"
    artifact = RulemakingDatasetPipeline().build_native(inputs, target, generation_id="rulemaking-g2")
    assert len(artifact.root["spec"]["tables"]) == 8
    [proceeding] = pq.read_table(target / "proceedings.parquet").to_pylist()
    assert proceeding["docket_ids"] == ["EPA-2026-0001"]
    assert proceeding["stage_events"][0]["stage"] == "proposed"
    assert "actor_id" not in proceeding and "stage_events_json" not in proceeding
    [lifecycle] = pq.read_table(target / "rulemaking_lifecycles.parquet").to_pylist()
    assert lifecycle["proposal_date"] == date(2026, 1, 1)
    assert lifecycle["outcome"] == "censored"
    assert pq.read_table(target / "etl_receipts.parquet").num_rows >= 8


def test_comment_period_anchor_is_a_subject_column_and_its_receipt_keeps_the_built_row(tmp_path):
    """Owner decisions 2026-10-05: what anchors a period, and which records state its dates, are columns readers use."""
    from spicy_regs.etl_receipts import _unpack
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from spicy_regs.transforms.regulations_receipts import write_held_dataset
    from tests.test_comment_periods import _build, _document, _notice

    dataset = "comment_periods"
    assert subject_schema(dataset).field("anchor_kind").type == pa.string()
    assert subject_schema(dataset).field("evidence_ids").type == pa.list_(pa.string())
    assert not {"anchor_kind", "evidence_ids", "evidence_ids_json"} & set(policy(dataset).receipt_fields)
    source = tmp_path / "built"
    source.mkdir()
    built = _build(
        source,
        dockets=[("DEA-2026-1519", "Placement of Cipepofol in Schedule IV")],
        documents=[
            _document(
                "DEA-2026-1519-0001", "DEA-2026-1519", "2026-08-27", "2026-09-29T03:59:59Z", fr_doc_num="2026-17536"
            ),
            _document("USCG-X-0001", None, "2026-03-02", "2026-04-02T03:59:59Z", fr_doc_num="2026-04100"),
        ],
        register=[
            _notice("2026-17536", "2026-08-27", "2026-09-28", document_type="Rule"),
            _notice("2026-04100", "2026-03-02", "2026-04-01", document_type="Proposed Rule"),
            _notice("2026-17546", "2026-08-28", "2026-09-28", document_type="Notice"),
        ],
        links=[
            ("2026-17536", "2026-08-27", "Docket No. DEA 1713"),
            ("2026-17546", "2026-08-28", "OMB Control Number 1024-0236"),
        ],
        proceedings=[
            {
                "proceeding_id": "proceeding_uscg",
                "docket_ids_json": "[]",
                "fr_document_ids_json": '["2026-04100@2026-03-02"]',
                "rins_json": '["1625-AC11"]',
            }
        ],
    )
    anchors = {row["comment_period_id"]: row["anchor_kind"] for row in built}
    assert sorted(anchors.values()) == ["docket", "none", "proceeding"]

    subject, receipts = write_held_dataset(
        dataset, source / "comment_periods.parquet", tmp_path / "native", generation_id="g1"
    )
    assert pq.read_schema(subject).equals(subject_schema(dataset))
    stored = pq.read_table(subject).to_pylist()
    assert {row["comment_period_id"]: row["anchor_kind"] for row in stored} == anchors
    evidence = {row["comment_period_id"]: json.loads(row["evidence_ids_json"]) for row in built}
    assert {row["comment_period_id"]: row["evidence_ids"] for row in stored} == evidence and any(evidence.values())
    # The receipt still holds each whole built row, its anchor included, and no second copy of the field.
    held = [
        _unpack(json.loads(receipt["processing_json"]))
        for receipt in pq.read_table(receipts).to_pylist()
        if receipt["outcome"] == "accepted"
    ]
    assert all("anchor_kind" not in fields for fields in held)
    assert [fields["raw_conversion_inputs"] for fields in held] == built
    # A scheduled producer's prior is the builder's own file again.
    remember_selection(tmp_path / "output", [SelectedDataset(dataset, (subject,), receipts, "g1")])
    prior = SelectedPriors(tmp_path / "private", root=tmp_path / "output", public_url="").get(dataset)
    assert pq.read_table(prior).equals(pq.read_table(source / "comment_periods.parquet"))


def test_raw_source_staging_keeps_empty_lists_restrictions_and_failed_count(tmp_path):
    from spicy_regs.transforms.regulations_receipts import write_source_records

    payload = {
        "data": {"id": "C", "attributes": {"modifyDate": "2026-01-01", "duplicateComments": 0}},
        "included": [
            {
                "id": "a",
                "type": "attachments",
                "attributes": {"title": "restricted", "fileFormats": None, "restrictReasonType": "Confidential"},
            },
            {"id": "b", "type": "attachments", "attributes": {"title": "empty", "fileFormats": []}},
        ],
    }
    subject, receipts = write_source_records("comments", [(payload, context())], tmp_path / "source")
    [row] = pq.read_table(subject).to_pylist()
    assert [v["formats"] for v in row["attachments"]] == [None, []]
    assert row["attachments"][0]["restrict_reason_type"] == "Confidential"
    assert row["duplicate_comments"] == 0
    assert "raw_source_record" in pq.read_table(receipts).to_pylist()[0]["processing_json"]
    invalid = {"data": {"id": "bad", "attributes": {"duplicateComments": True}}}
    subject, receipts = write_source_records("comments", [(invalid, context())], tmp_path / "bad")
    assert pq.read_table(subject).num_rows == 0
    assert pq.read_table(receipts)["outcome"].to_pylist() == ["refused"]


def test_native_volume_rollup_keeps_counts_and_omission_metadata_in_receipts(tmp_path):
    from spicy_regs.transforms.regulations_receipts import build_native_rollup

    selected = write(
        tmp_path,
        "documents",
        [
            {"document_id": "D1", "agency_code": "EPA", "document_type": "Rule", "posted_date": "2026-01-01"},
            {"document_id": "D2", "agency_code": "EPA", "document_type": "Rule", "posted_date": "0000-01-01"},
            {"document_id": "D3", "agency_code": "EPA", "document_type": "Rule", "posted_date": None},
        ],
    )
    target = tmp_path / "volume"
    build_native_rollup("agency_monthly_volume", [selected], target, generation_id="volume2")
    assert pq.read_table(target / "agency_monthly_volume.parquet")["document_count"].to_pylist() == [1]
    [receipt] = [r for r in pq.read_table(target / "etl_receipts.parquet").to_pylist() if r["outcome"] == "accepted"]
    assert "omitted_rows" in receipt["processing_json"]
    import hashlib
    retained = {hashlib.sha256(path.read_bytes()).hexdigest() for path in tmp_path.rglob("*") if path.is_file()}
    assert all(w["source_uri"] is None and w["sha256"] in retained for w in receipt["witnesses"])


def test_native_navigation_preserves_occurrences_without_cartesian_products(tmp_path):
    import duckdb
    from spicy_regs.relationship_views.regulations_native import install_native_regulations_views

    selected = write(
        tmp_path,
        "federal_register",
        [
            {
                "document_number": "1",
                "publication_date": "2026-01-01",
                "regulation_id_numbers_json": '["1000-AA00",null,"1000-AA00","malformed-rin"]',
                "docket_ids_json": '["D1","D2"]',
            }
        ],
    )
    with duckdb.connect() as con:
        con.from_parquet(str(selected.subjects[0])).create_view("federal_register")
        installed = install_native_regulations_views(con, ["federal_register"])
        assert con.execute(
            "SELECT source_ordinal,target_key FROM federal_register_rins_occurrences ORDER BY source_ordinal"
        ).fetchall() == [(0, "1000-AA00"), (1, None), (2, "1000-AA00"), (3, None)]
        assert con.execute("SELECT count(*) FROM federal_register_dockets_occurrences").fetchone() == (2,)
        assert con.execute("SELECT count(*) FROM federal_register_rins_pairs").fetchone() == (1,)
        assert "federal_register_rins_field_states" not in installed
        assert "raw_value_json" not in {
            r[0] for r in con.execute("DESCRIBE federal_register_rins_occurrences").fetchall()
        }


def test_pdf_selection_keeps_later_renditions_after_null_members():
    from spicy_regs.enrich_pdf import pdf_urls_for_document, pdf_urls_for_comment

    assert pdf_urls_for_document('[null,{"url":"https://example.gov/a.pdf","format":"pdf"}]', None) == [
        "https://example.gov/a.pdf"
    ]
    assert pdf_urls_for_comment('[null,{"formats":[null,{"url":"https://example.gov/b.pdf","format":"pdf"}]}]') == [
        "https://example.gov/b.pdf"
    ]


def test_cfr_internal_prior_restores_qualified_placement_metadata(tmp_path):
    from spicy_regs.transforms.regulations_receipts import write_held_dataset, materialize_internal
    from spicy_regs.transforms.build_cfr_sections import PLACEMENT_MARKER

    source = tmp_path / "cfr-old.parquet"
    table = pa.table(
        {"granule_id": ["CFR-2025-title1-vol1-sec1-1"], "last_modified": ["2025-01-01"]}
    ).replace_schema_metadata(PLACEMENT_MARKER)
    pq.write_table(table, source)
    subject, receipt = write_held_dataset("cfr_sections", source, tmp_path / "cfr", generation_id="cfr1")
    selected = ReceiptInput("cfr_sections", (subject,), receipt, "cfr1")
    internal = materialize_internal(selected, tmp_path / "prior.parquet")
    assert pq.read_schema(internal).metadata == table.schema.metadata


def test_v1_federal_register_pair_restores_and_rewrites_under_v2_with_new_fields_null(tmp_path):
    """A pair written before the SpicyDocs 0.66.6 projection stays readable; v2 only adds three literal columns."""
    from spicy_regs.earlier_receipt_policies import earlier_policies
    from spicy_regs.etl_receipts import write_dataset
    from spicy_regs.transforms.regulations_shape import shape_record

    current = policy("federal_register")
    (prior,) = earlier_policies(current)
    assert (prior.policy_version, current.policy_version) == ("regulations-native-v1", "regulations-native-v2")
    added = {"president_json", "disposition_notes", "executive_order_notes"}
    assert set(current.subject_schema.names) - set(prior.subject_schema.names) == added
    assert set(prior.subject_schema.names) <= set(current.subject_schema.names)
    raw = {"document_number": "1", "publication_date": "2026-01-01", "title": "old",
           "regulation_id_numbers_json": "[]", "docket_ids_json": "[]", "agencies_json": "[]"}
    shaped = shape_record("federal_register", raw)
    held = {name: shaped.get(name) for name in prior.input_fields}
    subject, receipts = write_dataset([(held, context("old"))], tmp_path / "old", prior)
    assert subject is not None
    original = subject.read_bytes()
    [restored] = read_internal(ReceiptInput("federal_register", (subject,), receipts, "old"))
    assert subject.read_bytes() == original
    new = write(tmp_path, "federal_register", [restored], generation="new", label="new")
    [row] = pq.read_table(new.subjects[0]).to_pylist()
    assert all(row[name] is None for name in added)
    assert row["document_number"] == "1" and row["publication_date"] == "2026-01-01" and row["title"] == "old"


def test_v1_unified_agenda_pair_restores_and_rewrites_under_v2_with_related_rins_null(tmp_path):
    """A pair written before SpicyDocs 0.67.0 read RELATED_RIN_LIST stays readable; NULL there means never read."""
    from spicy_regs.earlier_receipt_policies import earlier_policies
    from spicy_regs.etl_receipts import write_dataset
    from spicy_regs.transforms.regulations_receipts import materialize_internal
    from spicy_regs.transforms.regulations_shape import shape_record

    current = policy("unified_agenda")
    (prior,) = earlier_policies(current)
    assert (prior.policy_version, current.policy_version) == ("regulations-native-v1", "regulations-native-v2")
    assert set(current.subject_schema.names) - set(prior.subject_schema.names) == {"related_rins"}
    assert current.subject_schema.names[:-1] == prior.subject_schema.names  # appended; earlier columns keep places
    raw = {"rin": "1000-AA00", "agenda_edition": "202510", "timetable_json": "[]"}
    shaped = shape_record("unified_agenda", raw)
    held = {name: shaped.get(name) for name in prior.input_fields}
    subject, receipts = write_dataset([(held, context("old"))], tmp_path / "old", prior)
    assert subject is not None
    original = subject.read_bytes()
    selected = ReceiptInput("unified_agenda", (subject,), receipts, "old")
    [restored] = read_internal(selected)
    assert subject.read_bytes() == original
    # The incremental build's prior: SQL NULL, never the JSON text "null", so build_unified_agenda re-reads it.
    [prior_row] = pq.read_table(materialize_internal(selected, tmp_path / "_ua_prior.parquet")).to_pylist()
    assert "related_rins_json" in prior_row and prior_row["related_rins_json"] is None
    new = write(tmp_path, "unified_agenda", [restored], generation="new", label="new")
    [row] = pq.read_table(new.subjects[0]).to_pylist()
    assert row["related_rins"] is None
    assert (row["rin"], row["agenda_edition"], row["timetable"]) == ("1000-AA00", "202510", [])
    # A v2 row whose edition was read and states no list keeps the JSON text, so its edition counts as held.
    read = write(tmp_path, "unified_agenda", [{**raw, "related_rins_json": "null"}], generation="read", label="read")
    [read_prior] = pq.read_table(materialize_internal(read, tmp_path / "_ua_read_prior.parquet")).to_pylist()
    assert read_prior["related_rins_json"] == "null"


def test_unified_agenda_related_rins_refuse_an_unknown_member():
    from spicy_regs.transforms.regulations_shape import RegulationsShapeError, shape_record

    row = {"rin": "1000-AA00", "agenda_edition": "202510",
           "related_rins_json": '[{"rin": "1000-AA01", "rin_relation": "Related to", "note": "x"}]'}
    with pytest.raises(RegulationsShapeError):
        shape_record("unified_agenda", row)
    row["related_rins_json"] = '[{"rin": "1000-AA01", "rin_relation": "Related to"}]'
    assert shape_record("unified_agenda", row)["related_rins"] == [{"rin": "1000-AA01", "rin_relation": "Related to"}]


def test_native_source_ingest_uses_qualified_fr_prior_and_preserves_dated_records(tmp_path):
    from spicy_regs.transforms.regulations_ingest import federal_register_generation

    prior = write(
        tmp_path,
        "federal_register",
        [
            {
                "document_number": "1",
                "publication_date": "2026-01-01",
                "title": "old",
                "regulation_id_numbers_json": "[]",
                "docket_ids_json": "[]",
                "agencies_json": "[]",
            }
        ],
    )
    target = tmp_path / "fr-new"
    federal_register_generation(
        [{"document_number": "1", "publication_date": "2026-02-01", "title": "new"}],
        target,
        generation_id="fr2",
        witnesses=context().witnesses,
        prior=prior,
    )
    assert pq.read_table(target / "federal_register.parquet")["publication_date"].to_pylist() == [
        "2026-02-01",
        "2026-01-01",
    ]
    assert "pdf_url" not in pq.read_schema(target / "federal_register.parquet").names


def test_native_unified_agenda_source_ingest_keeps_partial_dates_and_prior_editions(tmp_path):
    from spicy_regs.transforms.regulations_ingest import unified_agenda_generation

    prior = write(
        tmp_path, "unified_agenda", [{"rin": "1000-AA00", "agenda_edition": "202510", "timetable_json": "[]"}]
    )
    target = tmp_path / "agenda-new"
    unified_agenda_generation(
        {
            "202604": [
                {
                    "rin": "1000-AA00",
                    "agenda_edition": "202604",
                    "rin_status": "Completed",
                    "timetable": [{"action": "Final Rule", "date": "04/00/2026", "fr_citation": None}],
                }
            ]
        },
        target,
        generation_id="agenda2",
        witnesses=context().witnesses,
        prior=prior,
    )
    rows = pq.read_table(target / "unified_agenda.parquet").to_pylist()
    assert len(rows) == 2
    latest = next(r for r in rows if r["agenda_edition"] == "202604")
    assert latest["timetable"][0]["date"] == "04/00/2026"
    assert latest["rin_status"] == "Completed"


def test_native_cfr_source_ingest_keeps_source_modified_date(tmp_path):
    from contextlib import nullcontext
    from spicy_regs.transforms.regulations_ingest import cfr_sections_generation

    target = tmp_path / "cfr-new"
    cfr_sections_generation(
        [
            {
                "granuleId": "CFR-2025-title1-vol1-part1",
                "packageId": "CFR-2025-title1-vol1",
                "lastModified": "2026-01-03",
                "title": "General Provisions",
                "granuleClass": "PART",
            }
        ],
        target,
        acquirer=nullcontext(object()),
        generation_id="cfr2",
        witnesses=context().witnesses,
    )
    [row] = pq.read_table(target / "cfr_sections.parquet").to_pylist()
    assert row["last_modified"] == "2026-01-03"
    assert row["part"] == "1"
    assert "url" not in row


def test_native_staging_merge_keeps_integer_counts_and_source_recency(tmp_path):
    from spicy_regs.transforms.regulations_receipts import merge_native_staging, build_native_rollup

    prior = write(
        tmp_path,
        "comments",
        [
            {
                "comment_id": "C",
                "agency_code": "EPA",
                "modify_date": "2026-01-01",
                "docket_id": None,
                "posted_date": None,
                "duplicate_comments": 2,
            }
        ],
        label="prior",
    )
    fresh = write(
        tmp_path,
        "comments",
        [
            {
                "comment_id": "C",
                "agency_code": "EPA",
                "modify_date": "2026-02-01",
                "docket_id": None,
                "posted_date": None,
                "duplicate_comments": 3,
            }
        ],
        generation="g2",
        label="fresh",
    )
    target = tmp_path / "merged"
    merge_native_staging([fresh], target, generation_id="g3", prior=prior)
    [row] = pq.read_table(target / "comments.parquet").to_pylist()
    assert row["duplicate_comments"] == 3
    assert row["modify_date"] == "2026-02-01"
    selected = ReceiptInput("comments", (target / "comments.parquet",), target / "etl_receipts.parquet", "g3")
    build_native_rollup("comments_index", [selected], tmp_path / "index", generation_id="g4")
    [index] = pq.read_table(tmp_path / "index/comments_index.parquet").to_pylist()
    assert index == {"agency_code": "EPA", "docket_id": None, "year": None, "month": None, "row_count": 1}


def test_successful_empty_output_keeps_metadata_as_observed_receipt(tmp_path):
    from spicy_regs.transforms.regulations_receipts import write_held_dataset, materialize_internal

    source = tmp_path / "empty.parquet"
    metadata = {"spicy_regs.omitted_rows": "9", "spicy_regs.as_of": "2026-01-01"}
    pq.write_table(
        pa.schema(
            [
                ("agency_code", pa.string()),
                ("recent_30d", pa.int64()),
                ("baseline", pa.float64()),
                ("ratio", pa.float64()),
            ]
        )
        .with_metadata(metadata)
        .empty_table(),
        source,
    )
    subject, receipt = write_held_dataset(
        "discovery_signals", source, tmp_path / "empty-native", generation_id="empty1"
    )
    assert pq.read_table(subject).num_rows == 0
    [attempt] = pq.read_table(receipt).to_pylist()
    assert attempt["outcome"] == "observed" and attempt["subject_version"] is None
    selected = ReceiptInput("discovery_signals", (subject,), receipt, "empty1")
    internal = materialize_internal(selected, tmp_path / "empty-internal.parquet")
    assert pq.read_schema(internal).metadata == pq.read_schema(source).metadata


def test_retry_checkpoint_receipts_preserve_empty_retirement_and_wrong_generation(tmp_path):
    from spicy_regs.transforms.regulations_checkpoints import write_checkpoint, read_checkpoint
    from spicy_regs.pipelines.regulations_state import UnresolvedKeys

    row = {
        "agency": "EPA",
        "record_type": "comments",
        "key": "source-key",
        "status": "requested-empty",
        "reason": "zero bytes",
        "attempted_at": "2026-01-01",
        "attempts": 2,
    }
    receipt = write_checkpoint("failed_keys", [row], tmp_path / "checkpoint", context())
    assert not (tmp_path / "checkpoint/failed_keys.parquet").exists()
    restored = UnresolvedKeys.from_receipts(receipt, generation_id="g1", output_dir=tmp_path)
    assert restored.rows == {"source-key": row}
    with pytest.raises(ValueError):
        read_checkpoint("failed_keys", receipt, generation_id="")
    restored.rows.clear()
    cleared = restored.save_receipts(tmp_path / "cleared", context("g2"))
    assert read_checkpoint("failed_keys", cleared, generation_id="g2") == []
    assert pq.read_table(cleared)["outcome"].to_pylist() == ["observed"]


def test_attribute_selection_keeps_every_copy_and_same_volatile_tie_choice(tmp_path):
    from datetime import UTC, datetime
    from spicy_docs.sources.mirrulations import KeyedPayload
    from spicy_regs.etl_receipts import read_attempts
    from spicy_regs.transforms.regulations_attributes import TeeAttributes, merge_attribute_parts
    from spicy_regs.transforms.regulations_attribute_receipts import build_attribute_generation

    copies = [
        {
            "data": {
                "id": "D",
                "type": "documents",
                "attributes": {
                    "modifyDate": "2026-01-01",
                    "openForComment": flag,
                    "pageCount": 4,
                    "topics": ["Air", "Air"],
                    "displayProperties": [{"name": "x", "label": "Display"}],
                },
            }
        }
        for flag in [True, False, False]
    ]
    times = [1_000_000, 1_000_030, 1_004_000]
    keyed = [
        KeyedPayload(str(i), datetime.fromtimestamp(t, UTC), p)
        for i, (p, t) in enumerate(zip(copies, times, strict=True))
    ]
    list(TeeAttributes("document_attributes", tmp_path / "legacy").apply(keyed))
    legacy = tmp_path / "selected.parquet"
    merge_attribute_parts("document_attributes", tmp_path / "legacy/document_attributes", None, legacy)
    prior = write(tmp_path, "document_attributes", [{"document_id": "carried", "page_count": 7}], label="prior")
    invalid = {"data": {"id": "invalid", "attributes": {"pageCount": "wrong"}}}
    inputs: list[tuple[dict, ReceiptContext, int | None]] = [(p, context("g2", str(i)), t) for i, (p, t) in enumerate(zip(copies, times, strict=True))]
    inputs.append((invalid, context("g2", "invalid"), None))
    target = tmp_path / "generation"
    build_attribute_generation("document_attributes", inputs, target, scan_context=context("g2", "scan"), prior=prior)
    selected = ReceiptInput(
        "document_attributes", (target / "document_attributes.parquet",), target / "etl_receipts.parquet", "g2"
    )
    restored = {r["document_id"]: r for r in read_internal(selected)}
    assert restored["D"] == pq.read_table(legacy).to_pylist()[0]
    assert restored["carried"]["page_count"] == 7
    assert restored["D"]["topics"] == ["Air", "Air"]
    attempts = list(read_attempts([selected.receipts], policy("document_attributes"), generation_id="g2"))
    copies_held = [a for a in attempts if a["outcome"] == "observed" and "raw_source_record" in a["processing_fields"]]
    assert [a["processing_fields"]["raw_source_record"] for a in copies_held] == copies
    assert [a["processing_fields"]["raw_conversion_inputs"]["_written_at"] for a in copies_held] == times
    assert sum(a["outcome"] == "refused" for a in attempts) == 1
    assert not any(c.startswith("_") for c in pq.read_schema(selected.subjects[0]).names)


def test_attribute_successful_empty_scan_has_receipt_without_a_subject(tmp_path):
    from spicy_regs.transforms.regulations_attribute_receipts import build_attribute_generation

    target = tmp_path / "generation"
    build_attribute_generation("docket_attributes", [], target, scan_context=context("g2", "empty"))
    assert pq.read_table(target / "docket_attributes.parquet").num_rows == 0
    [attempt] = pq.read_table(target / "etl_receipts.parquet").to_pylist()
    assert attempt["outcome"] == "observed"


@pytest.mark.parametrize('dataset,original,altered', [
    ('documents', {'document_id': 'D', 'title': 'selected'}, {'document_id': 'D'}),
    ('documents', {'document_id': 'D', 'title': 'selected'}, {'document_id': 'D', 'title': 'different'}),
    ('rule_targets', {'docket_id': 'D', 'source': 'docket_rin', 'rin': '1000-AA00'}, {}),
])
def test_resealed_inconsistent_processor_input_refuses_before_processing(tmp_path, dataset, original, altered):
    from hashlib import sha256
    from spicy_regs.etl_receipts import exact_json, RECEIPT_SCHEMA

    originals = ([{"document_id": "early", "title": "safe"}] if dataset == "documents" else []) + [original]
    selected = write(tmp_path, dataset, originals)
    receipts = pq.read_table(selected.receipts).to_pylist()
    receipt = receipts[-1]
    from spicy_regs.etl_receipts import _unpack
    processing = _unpack(json.loads(receipt['processing_json']))
    processing['raw_conversion_inputs'] = altered
    receipt['processing_json'] = exact_json(processing)
    receipt['receipt_id'] = 'sha256:' + sha256(exact_json({k: v for k, v in receipt.items() if k != 'receipt_id'}).encode()).hexdigest()
    pq.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA), selected.receipts)
    # The subject hash, receipt digest and row join are all valid; source replay is not.
    validate_receipt_bundle({dataset: list(selected.subjects)}, [selected.receipts], [policy(dataset)], generation_id='g1')
    with pytest.raises(ValueError, match='retained processor input differs'):
        next(iter(read_internal(selected)))


@pytest.mark.parametrize('loose_prior', [False, True])
def test_scheduled_rulemaking_uses_selected_native_agenda_and_preserves_prior_identity(tmp_path, monkeypatch, loose_prior):
    import shutil
    from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from spicy_regs.sources import r2

    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    source_rows = {
        'dockets': [{'docket_id': 'EPA-2026-0001', 'agency_code': 'EPA', 'docket_type': 'Rulemaking',
                     'title': 'A rule', 'rin': '1000-AA00'}],
        'documents': [{'document_id': 'EPA-2026-0001-0001', 'docket_id': 'EPA-2026-0001',
                       'agency_code': 'EPA', 'document_type': 'Proposed Rule', 'posted_date': '2026-01-01',
                       'modify_date': '2026-01-01', 'comment_end_date': '2026-03-01', 'title': 'Proposal'}],
        'federal_register': [], 'fr_docket_links': [],
        'unified_agenda': [{'rin': '1000-AA00', 'agenda_edition': '202510', 'title': 'A rule',
                           'agency_code': '2000', 'rule_stage': 'Proposed Rule', 'timetable_json': '[]',
                           'legal_authority_json': '["5 U.S.C. 301"]', 'cfr_references_json': '["40 CFR 1"]',
                           'url': 'https://www.reginfo.gov/public/do/eAgendaViewRule?pubId=202510&RIN=1000-AA00'}],
    }
    inputs = [write(tmp_path, name, rows, label=name) for name, rows in source_rows.items()]
    work = tmp_path / 'scheduled'
    work.mkdir()
    remember_selection(work, [SelectedDataset(s.dataset, s.subjects, s.receipts, s.generation_id) for s in inputs])
    monkeypatch.setattr(r2, 'download', lambda *_: False)
    if loose_prior:
        from spicy_regs.transforms.build_proceedings import COLUMNS
        pq.write_table(pa.Table.from_pylist([{'proceeding_id': 'unadmitted-prior-id',
                                             'docket_ids_json': '["EPA-2026-0001"]'}], schema=pa.schema([(name, pa.string()) for name in COLUMNS])),
                       work / 'proceedings.parquet')
        for name in ('_proceedings_prior.parquet', '_proceedings_native_prior.parquet', '_proceedings_receipts.parquet'):
            (work / name).write_bytes(b'unadmitted scratch')
    RulemakingDatasetPipeline(output_dir=work, run_id='rulemaking-first', asserted_at='2026-10-05T00:00:00Z').run()
    native = pq.read_table(work / 'proceedings.parquet').to_pylist()
    assert native[0]['proceeding_id'] != 'unadmitted-prior-id'
    assert not (work / '_proceedings_receipts.parquet').exists()
    assert 'authority_refs' in native[0] and 'authority_refs_json' not in native[0]
    manifest = json.loads((work / 'rulemaking-dataset-manifest.json').read_text())
    assert manifest['inputs']['native_inputs']['unified_agenda']['generationId'] == 'g1'
    assert manifest['etlReceipts']['generationId'] == 'rulemaking-first'
    assert manifest['artifacts']['etl_receipts.parquet']['visibility'] == 'internal'
    # Simulate the exact pinned previous materialized generation, including receipts.
    frozen = tmp_path / 'frozen'
    frozen.mkdir()
    for name in ('proceedings.parquet', 'etl_receipts.parquet'):
        shutil.copyfile(work / name, frozen / name)
    shutil.copyfile(work / 'rulemaking-dataset-latest.json', work / '_rulemaking_latest.json')
    shutil.copyfile(work / 'rulemaking-dataset-manifest.json', work / '_rulemaking_previous_manifest.json')

    def download(key, destination):
        source = frozen / Path(key).name
        if not source.exists():
            return False
        shutil.copyfile(source, destination)
        return True

    monkeypatch.setattr(r2, 'download', download)
    RulemakingDatasetPipeline(output_dir=work, run_id='rulemaking-second', asserted_at='2026-10-06T00:00:00Z').run()
    assert [r['proceeding_id'] for r in pq.read_table(work / 'proceedings.parquet').to_pylist()] == [r['proceeding_id'] for r in native]


def test_scheduled_rulemaking_refuses_unselected_agenda_before_stages(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline

    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    with pytest.raises(ValueError, match='requires selected native input'):
        RulemakingDatasetPipeline(output_dir=tmp_path).run()
    assert not (tmp_path / 'proceedings.parquet').exists()


@pytest.mark.parametrize("attachments_json", [
    None, "[]", "null", "[null]",
    '[{"title":"t","formats":null,"attachment_id":"a"}]',
    '[{"title":"t","formats":[],"restrictReason":"r","restrictReasonType":"x"}]',
    '[{"formats":[{"url":"u","format":"pdf","size":4},null]},{"title":"é😀\\n"}]',
])
@pytest.mark.parametrize("duplicate_comments", [None, 0, 7])
def test_comments_promotion_input_round_trips_its_subject(attachments_json, duplicate_comments):
    from spicy_regs.transforms.regulations_receipts import (
        COMMENTS_PROMOTION_PROCESSOR, _processor_input, comments_promotion_input)
    from spicy_regs.transforms.regulations_shape import shape_record
    source = {"comment_id": "c1", "comment": "<p>one&amp;two</p>", "attachments_json": attachments_json,
              "duplicate_comments": duplicate_comments}
    subject = pa.Table.from_pylist([shape_record("comments", source)], schema=subject_schema("comments")).to_pylist()[0]
    derived = _processor_input("comments", subject, processor=COMMENTS_PROMOTION_PROCESSOR)
    assert list(derived) == [name for name, _ in SOURCE_COLUMNS["comments"]]
    assert derived["duplicate_comments"] == duplicate_comments
    # Canonical, not the original spelling: shaping it again reproduces the same subject and input.
    again = pa.Table.from_pylist([shape_record("comments", derived)], schema=subject_schema("comments")).to_pylist()[0]
    assert again == subject
    assert comments_promotion_input(again, COMMENTS_PROMOTION_PROCESSOR) == derived


def test_comments_missing_input_refuses_unless_its_receipt_is_a_promotion_receipt():
    from spicy_regs.transforms.regulations_receipts import (
        COMMENTS_PROMOTION_PROCESSOR, _retained_processor_input, comments_promotion_input)
    from spicy_regs.transforms.regulations_shape import shape_record
    subject = pa.Table.from_pylist([shape_record("comments", {"comment_id": "c1"})],
                                   schema=subject_schema("comments")).to_pylist()[0]
    for processor in (None, "regulatory-catalog-native-v1", "comments-selected-policy-promotion/2"):
        assert comments_promotion_input(subject, processor) is None
        with pytest.raises(ValueError, match="exact retained processor input is required"):
            _retained_processor_input("comments", subject, processor=processor)
    # A promotion receipt carrying any processing field is not the input-less receipt this rule covers.
    assert comments_promotion_input(subject | {"input_metadata": {}}, COMMENTS_PROMOTION_PROCESSOR) is None
    with pytest.raises(ValueError, match="exact retained processor input is required"):
        _retained_processor_input("documents", {"document_id": "d"}, processor=COMMENTS_PROMOTION_PROCESSOR)
