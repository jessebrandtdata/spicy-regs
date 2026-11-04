"""Government producer/read/generation boundaries retain evidence and fail closed."""

from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import decode_exact_json, read_attempts, read_with_receipts, validate_receipt_bundle
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.native_types import described_schema
from spicy_regs.relationship_views.lobbying_native import LOBBYING_NATIVE_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.transforms.government_receipts import (
    BUILD_METADATA,
    POLICIES,
    _legacy_schema,
    generation_receipt_args,
    internal_prior,
    migrate_outputs,
    receipt_builder,
)
from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS, GovernmentShapeError, map_subject


def literal(directory, dataset, rows):
    directory.mkdir(exist_ok=True)
    path = directory / (dataset + ".parquet")
    pq.write_table(pa.Table.from_pylist(rows, schema=_legacy_schema(dataset)), path)
    return path


def test_exact_processing_values_and_nullable_registration_identity(tmp_path):
    dataset = "sam_entities"
    path = literal(
        tmp_path,
        dataset,
        [
            {
                "uei": "U",
                "entity_eft_indicator": None,
                "registration_date": "2025-01-01",
                "registration_status": "Active",
            },
            {"uei": "U", "entity_eft_indicator": "", "registration_status": "Inactive"},
        ],
    )
    before = pq.read_table(path).to_pylist()
    meta = migrate_outputs((path,), generation_id="sam-first")
    assert pq.read_table(path)["entity_eft_indicator"].to_pylist() == [None, ""]
    assert pq.read_table(internal_prior(dataset, path)).to_pylist() == before
    validate_receipt_bundle(
        {dataset: [path]}, [Path(meta["receipt_path"])], [POLICIES[dataset]], generation_id="sam-first"
    )
    receipts = pq.read_table(meta["receipt_path"]).to_pylist()
    assert len({r["record_id"] for r in receipts}) == 2


def test_native_incremental_read_requires_exact_receipts(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    dataset = "crs_reports"
    path = literal(tmp_path, dataset, [{"report_id": "R1", "version": "1", "url": "https://source.test/r1"}])
    meta = migrate_outputs((path,), generation_id="crs-first")
    receipt = Path(meta["receipt_path"])
    assert pq.read_table(internal_prior(dataset, path, receipt_path=receipt, generation_id="new-publisher")).num_rows == 1
    (tmp_path / BUILD_METADATA).unlink()
    with pytest.raises(ValueError, match="selected receipts"):
        internal_prior(dataset, path)
    changed = pq.read_table(path).to_pylist()
    changed[0]["title"] = "changed without evidence"
    pq.write_table(pa.Table.from_pylist(changed, schema=SUBJECT_SCHEMAS[dataset]), path)
    with pytest.raises(ValueError):
        list(read_with_receipts([path], [receipt], POLICIES[dataset], generation_id="crs-first"))


def test_refused_money_retains_input_without_replacing_original(tmp_path):
    dataset = "usaspending_recipients"
    path = literal(tmp_path, dataset, [{"recipient_id": "R", "total_award_amount": "1.0000001"}])
    before = path.read_bytes()
    with pytest.raises(GovernmentShapeError, match="refused inputs"):
        migrate_outputs((path,), generation_id="refusal")
    assert path.read_bytes() == before
    candidate = tmp_path / ".government-etl/refusal"
    assert pq.ParquetFile(candidate / dataset / (dataset + ".parquet")).metadata.num_rows == 0
    attempts = list(read_attempts([candidate / "etl_receipts.parquet"], POLICIES[dataset], generation_id="refusal"))
    assert attempts[0]["outcome"] == "refused"
    assert attempts[0]["processing_fields"]["raw_record"]["total_award_amount"] == "1.0000001"
    assert not (tmp_path / BUILD_METADATA).exists()


def test_completed_migration_retires_only_its_conversion_work(tmp_path):
    path = literal(tmp_path, "crs_reports", [{"report_id": "R", "version": "1"}])
    original = path.read_bytes()
    failed = tmp_path / ".government-etl/earlier-failed/partial.parquet"
    evidence = tmp_path / "source-evidence/artifact.json"
    for held in (failed, evidence):
        held.parent.mkdir(parents=True)
        held.write_bytes(b"preserved")

    migrate_outputs((path,), generation_id="complete")

    assert not (tmp_path / ".government-etl/complete").exists()
    assert (tmp_path / ".government-inputs/complete" / path.name).read_bytes() == original
    assert failed.read_bytes() == evidence.read_bytes() == b"preserved"
    args = generation_receipt_args((path,))  # Both final subject and receipt pins still verify.
    restored = list(read_with_receipts(
        [path], [args["receipt_path"]], POLICIES[path.stem], generation_id=args["receipt_generation_id"]
    ))
    assert restored[0]["raw_record"]["report_id"] == "R"


@pytest.mark.parametrize("failure", ["subject-copy", "final-hash", "metadata"])
def test_interrupted_migration_keeps_conversion_work(tmp_path, monkeypatch, failure):
    from spicy_regs.transforms import government_receipts as gr

    path = literal(tmp_path, "crs_reports", [{"report_id": "R", "version": "1"}])
    original = path.read_bytes()
    work = tmp_path / ".government-etl/interrupted"
    copyfile, digest, write_text = gr.shutil.copyfile, gr._digest, Path.write_text

    def copy(source, target, **kwargs):
        if failure == "subject-copy" and Path(target).name == "." + path.name + ".native":
            raise InterruptedError("subject-copy")
        return copyfile(source, target, **kwargs)

    def hash_file(source):
        if failure == "final-hash" and Path(source) == path:
            raise InterruptedError("final-hash")
        return digest(source)

    def metadata(target, *args, **kwargs):
        if failure == "metadata" and target.name == BUILD_METADATA:
            raise InterruptedError("metadata")
        return write_text(target, *args, **kwargs)

    monkeypatch.setattr(gr.shutil, "copyfile", copy)
    monkeypatch.setattr(gr, "_digest", hash_file)
    monkeypatch.setattr(Path, "write_text", metadata)
    with pytest.raises(InterruptedError, match=failure):
        migrate_outputs((path,), generation_id="interrupted")
    assert (tmp_path / ".government-inputs/interrupted" / path.name).read_bytes() == original
    assert pq.read_table(work / path.stem / path.name).num_rows == 1
    assert pq.read_table(work / path.stem / "etl_receipts.parquet").num_rows == 1
    assert pq.read_table(work / "etl_receipts.parquet").num_rows == 1
    assert not (tmp_path / BUILD_METADATA).exists()


def test_cleanup_error_stops_before_admission_with_final_pins_intact(tmp_path, monkeypatch):
    from spicy_regs.transforms import government_receipts as gr

    path = literal(tmp_path, "crs_reports", [{"report_id": "R", "version": "1"}])
    work = tmp_path / ".government-etl/complete"
    rmtree = gr.shutil.rmtree

    def refuse(target, *args, **kwargs):
        if Path(target) == work:
            raise PermissionError("cannot retire conversion work")
        return rmtree(target, *args, **kwargs)

    monkeypatch.setattr(gr.shutil, "rmtree", refuse)
    with pytest.raises(PermissionError, match="cannot retire conversion work"):
        migrate_outputs((path,), generation_id="complete")
    assert generation_receipt_args((path,))["receipt_path"] == tmp_path / "etl_receipts.parquet"
    assert (work / "etl_receipts.parquet").exists()


def test_invalid_source_digest_is_a_retained_refusal(tmp_path):
    path = literal(tmp_path, "usaspending_recipients", [{"recipient_id": "R", "source_capture_sha256": "not-a-digest"}])
    with pytest.raises(GovernmentShapeError, match="refused inputs"):
        migrate_outputs((path,), generation_id="bad-digest")
    rows = pq.read_table(tmp_path / ".government-etl/bad-digest/etl_receipts.parquet").to_pylist()
    assert rows[0]["outcome"] == "refused"
    assert len(rows[0]["witnesses"]) == 1  # The valid retained input digest; invalid source claim stays raw.


def test_an_unchanged_row_keeps_its_prior_receipt_verbatim_across_a_rebuild(tmp_path):
    dataset = "usaspending_recipients"
    path = literal(
        tmp_path,
        dataset,
        [
            {
                "recipient_id": "R",
                "total_award_amount": "9007199254740993.12",
                "observed_at": "2026-01-01T00:00:00Z",
                "source_capture_sha256": "a" * 64,
            }
        ],
    )
    migrate_outputs((path,), generation_id="first")
    prior_receipts = pq.read_table(tmp_path / "etl_receipts.parquet").to_pylist()

    @receipt_builder
    def rebuild(output_dir):
        prior = internal_prior(dataset, path)
        table = pq.read_table(prior)
        pq.write_table(table, path)
        return path

    rebuild(tmp_path, receipt_generation_id="second")
    rows = list(
        read_with_receipts([path], [tmp_path / "etl_receipts.parquet"], POLICIES[dataset], generation_id="second")
    )
    assert rows[0]["total_award_amount"] == Decimal("9007199254740993.12")
    assert rows[0]["raw_record"]["observed_at"] == "2026-01-01T00:00:00Z"
    receipts = pq.read_table(tmp_path / "etl_receipts.parquet").to_pylist()
    # The carry rule: an identical attempt keeps the prior receipt as it was (generation, witnesses, id), so
    # a night adds history only for a row that changed.
    assert receipts == prior_receipts
    assert receipts[0]["generation_id"] == "first"


def test_complete_generation_requires_paired_receipts_and_verifies(tmp_path):
    dataset = "crs_reports"
    path = literal(tmp_path / "build", dataset, [{"report_id": "R", "status": "Active", "version": "3"}])
    migrate_outputs((path,), generation_id="complete")
    schemas = {dataset: described_schema(SUBJECT_SCHEMAS[dataset])}
    with pytest.raises(ValueError, match="require ETL receipts"):
        build_generation(
            tmp_path / "unpaired", family="crs-reports", files=[path], expected_keys=[path.name], schemas=schemas
        )
    result = build_generation(
        tmp_path / "generation",
        family="crs-reports",
        files=[path],
        expected_keys=[path.name],
        schemas=schemas,
        **generation_receipt_args((path,)),
    )
    verify_generation(tmp_path / "generation", expected_pin=result.pin)


def test_native_lobbying_relationship_preserves_positions_without_cross_product():
    rows = [
        map_subject(
            "lobbying_activities",
            {"filing_uuid": "F", "activity_index": str(index), "government_entities_json": entities},
        )
        for index, entities in enumerate(['[{"id":2,"name":"HOUSE"},null,{"id":2,"name":"HOUSE"}]', "[]", "null"])
    ]
    con = duckdb.connect()
    con.register("lobbying_activities", pa.Table.from_pylist(rows, schema=SUBJECT_SCHEMAS["lobbying_activities"]))
    assert (
        install_sql_views(con, ["lobbying_activities"], LOBBYING_NATIVE_VIEWS)[
            "lobbying_contacted_entities_occurrences"
        ]["status"]
        == "available"
    )
    assert con.execute(
        "SELECT activity_index,source_ordinal,target_key FROM lobbying_contacted_entities_occurrences ORDER BY source_ordinal"
    ).fetchall() == [(0, 0, "2"), (0, 1, None), (0, 2, "2")]


def test_lobbying_source_mapper_keeps_null_empty_repeats_and_child_positions():
    from spicy_regs.transforms.build_lobbying_filings import _shape, _activity_rows, _lobbyist_rows

    raw = {
        "filing_uuid": "F",
        "lobbying_activities": [
            None,
            {"government_entities": None},
            {"government_entities": []},
            {"government_entities": [None, {"id": 1}, {"id": 1}], "lobbyists": [None, {"new": False}]},
        ],
    }
    row = map_subject("lobbying_filings", _shape(raw))
    assert row["lobbying_activities"][0] is None
    entity = {"id": "1", "name": None}
    assert row["government_entities"] == [None, entity]
    children = [map_subject("lobbying_activities", r) for r in _activity_rows(raw)]
    assert [r["activity_index"] for r in children] == [0, 1, 2, 3]
    assert [r["government_entities"] for r in children[:3]] == [None, None, []]
    assert children[3]["government_entities"] == [None, entity, entity]
    assert [r["lobbyist_index"] for r in _lobbyist_rows(raw)] == ["0", "1"]


def test_empty_processing_observations_survive_incremental_rebuild(tmp_path):
    dataset = "crs_reports"
    path = literal(tmp_path, dataset, [])
    migrate_outputs((path,), generation_id="empty-first")

    @receipt_builder
    def rebuild(directory):
        restored = internal_prior(dataset, path)
        pq.write_table(pq.read_table(restored), path)
        return path

    rebuild(tmp_path, receipt_generation_id="empty-second")
    attempts = list(read_attempts([tmp_path / "etl_receipts.parquet"], POLICIES[dataset], generation_id="empty-second"))
    assert [attempt["outcome"] for attempt in attempts] == ["observed"]
    assert pq.ParquetFile(path).metadata.num_rows == 0
    assert attempts[0]["generation_id"] == "empty-first"  # carried verbatim


def test_later_listing_keeps_held_gao_caption_and_domain_status(tmp_path, monkeypatch):
    from importlib import import_module

    module = import_module("spicy_regs.transforms.build_gao_reports")
    dataset = "gao_decisions"
    path = literal(
        tmp_path,
        dataset,
        [
            {
                "decision_number": "B-1",
                "url": "https://www.gao.gov/products/b-1",
                "title": "Held",
                "released_date": "2026-01-03",
                "decided_date": "2026-01-02",
                "decision_status": "We deny the protest.",
                "outcome": "denied",
                "outcome_rule": "retained-source-rule",
                "b_numbers_json": '["B-1","B-2"]',
                "b_numbers_truncated": "false",
                "source": "gao_listing",
            }
        ],
    )
    migrate_outputs((path,), generation_id="caption")
    import shutil

    shutil.copyfile(path, tmp_path / "_gao_decisions_prior.parquet")
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    from spicy_docs.schemas.gao_decision_tables import GAO_DECISIONS

    fresh = {
        **dict.fromkeys(GAO_DECISIONS.columns),
        "decision_number": "B-1",
        "url": "https://www.gao.gov/products/b-1",
        "title": "Fresh title",
        "released_date": "2026-01-03",
        "decision_status": "We deny the protest.",
        "b_numbers_json": '["B-1"]',
        "source": "gao_listing",
    }
    output = module._build_decisions(tmp_path, [fresh], receipt_generation_id="listing")
    [subject] = pq.read_table(output).to_pylist()
    assert subject["title"] == "Fresh title"
    assert subject["decided_date"].isoformat() == "2026-01-02"
    assert subject["b_numbers"] == ["B-1", "B-2"]
    assert subject["decision_status"] == "We deny the protest." and subject["outcome"] == "denied"
    assert subject["b_numbers_truncated"] is False
    assert "outcome_rule" not in subject
    [joined] = read_with_receipts(
        [output], [tmp_path / "etl_receipts.parquet"], POLICIES[dataset], generation_id="listing"
    )
    from spicy_docs.interpretation.gao_decisions import GAO_OUTCOME_RULE

    assert joined["raw_record"]["outcome_rule"] == GAO_OUTCOME_RULE


def test_gao_prior_published_without_the_cut_flag_is_read_once_and_rewritten_with_it(tmp_path, monkeypatch):
    """gao-reports published gao_decisions under government-sources/1 on 2026-10-04, the flag only in its receipts."""
    import json
    import shutil
    from importlib import import_module

    from spicy_regs.etl_receipts import ReceiptContext, write_dataset
    from spicy_regs.transforms.government_receipts import EARLIER_POLICIES, _digest

    module = import_module("spicy_regs.transforms.build_gao_reports")
    dataset = "gao_decisions"
    earlier = next(p for p in EARLIER_POLICIES[dataset] if p.policy_version == "government-sources/1")
    assert earlier.policy_version != POLICIES[dataset].policy_version
    raw = {
        **dict.fromkeys(_legacy_schema(dataset).names),
        "decision_number": "B-412940",
        "url": "https://www.gao.gov/products/b-412940",
        "released_date": "2016-07-01",
        "b_numbers_json": '["B-412940"]',
        "b_numbers_truncated": "true",
        "source": "gao_listing",
    }
    subject = {name: value for name, value in map_subject(dataset, raw).items() if name in earlier.subject_schema.names}
    witness = {
        "source_id": "test:listing",
        "source_uri": None,
        "sha256": "sha256:" + "0" * 64,
        "locator": None,
        "body_version": None,
    }
    context = ReceiptContext("earlier", dataset + ":0", earlier.policy_version, [witness])
    held, receipts = write_dataset([({**subject, "raw_record": raw}, context)], tmp_path / "published", earlier)
    assert held is not None
    prior = tmp_path / "_gao_decisions_prior.parquet"
    shutil.copyfile(held, prior)
    assert "b_numbers_truncated" not in pq.read_schema(prior).names
    (tmp_path / BUILD_METADATA).write_text(
        json.dumps(
            {
                "generation_id": "earlier",
                "receipt_path": str(receipts),
                "receipt_sha256": _digest(receipts),
                "subjects": {dataset: {"path": str(prior), "sha256": _digest(prior)}},
            }
        )
    )
    monkeypatch.setattr(module.r2, "download", lambda *_: False)

    output = module._build_decisions(tmp_path, [], receipt_generation_id="upgrade")

    assert pq.read_schema(output).equals(SUBJECT_SCHEMAS[dataset])
    [row] = pq.read_table(output).to_pylist()
    assert row["b_numbers_truncated"] is True and row["b_numbers"] == ["B-412940"]
    [joined] = read_with_receipts(
        [output], [tmp_path / "etl_receipts.parquet"], POLICIES[dataset], generation_id="upgrade"
    )
    assert joined["raw_record"]["b_numbers_truncated"] == "true"
    [receipt] = pq.read_table(tmp_path / "etl_receipts.parquet").to_pylist()
    assert receipt["policy_version"] == POLICIES[dataset].policy_version
    # The row's subject changed (it gained columns): a fresh receipt that references only its predecessor.
    assert decode_exact_json(receipt["diagnostic_json"])["prior_receipt"]["generation_id"] == "earlier"
    assert [w["source_id"] for w in receipt["witnesses"]] == ["spicy-regs:producer-output:" + dataset]


def test_gao_recommendations_prior_published_without_its_seen_dates_restores_them(tmp_path):
    """gao-recommendations published under government-sources/1 on 2026-10-04, first_seen and last_seen in receipts."""
    import shutil
    from datetime import date

    from spicy_regs.etl_receipts import ReceiptContext, write_dataset
    from spicy_regs.transforms.government_receipts import EARLIER_POLICIES

    dataset = "gao_recommendations"
    earlier = next(p for p in EARLIER_POLICIES[dataset] if p.policy_version == "government-sources/1")
    raw = {
        **dict.fromkeys(_legacy_schema(dataset).names),
        "recommendation_id": "r-1",
        "report_id": "GAO-26-1",
        "first_seen": "2026-09-30",
        "last_seen": "2026-10-02",
        "listed_open": "true",
    }
    subject = {name: value for name, value in map_subject(dataset, raw).items() if name in earlier.subject_schema.names}
    witness = {
        "source_id": "test:export",
        "source_uri": None,
        "sha256": "sha256:" + "0" * 64,
        "locator": None,
        "body_version": None,
    }
    context = ReceiptContext("earlier", dataset + ":0", earlier.policy_version, [witness])
    held, receipts = write_dataset([({**subject, "raw_record": raw}, context)], tmp_path / "published", earlier)
    assert held is not None and "first_seen" not in pq.read_schema(held).names

    restored = internal_prior(dataset, held, receipt_path=receipts, generation_id="earlier")
    assert pq.read_table(restored).to_pylist() == [raw]
    build = tmp_path / "build"
    build.mkdir()
    output = build / (dataset + ".parquet")
    shutil.copyfile(restored, output)
    migrate_outputs((output,), generation_id="upgrade")
    assert pq.read_schema(output).equals(SUBJECT_SCHEMAS[dataset])
    [row] = pq.read_table(output).to_pylist()
    assert (row["first_seen"], row["last_seen"]) == (date(2026, 9, 30), date(2026, 10, 2))
    [receipt] = pq.read_table(build / "etl_receipts.parquet").to_pylist()
    assert receipt["policy_version"] == POLICIES[dataset].policy_version


@pytest.mark.parametrize("with_prior_refusal", [False, True])
def test_gao_reports_prior_published_before_the_letter_columns_is_read_once_and_gains_them(tmp_path, monkeypatch, with_prior_refusal):
    """gao-reports published gao_reports under government-sources/1 to 2026-10-05: no letter column, and no letter
    field in any receipt's original row either. The daily run reads that prior once and writes government-sources/2
    with the three columns NULL; a run given the letters' capture then fills them."""
    import json
    import shutil
    from importlib import import_module

    from dataclasses import replace
    from spicy_regs.etl_receipts import ReceiptContext, failure_receipt, select_receipts, write_dataset
    from spicy_regs.transforms.government_receipts import EARLIER_POLICIES, _digest
    from tests.test_gao_major_rule_letters import PRODUCTS, capture, page

    module = import_module("spicy_regs.transforms.build_gao_reports")
    dataset = "gao_reports"
    added = ("major_rule_agency", "major_rule_rins", "major_rule_fr_citations")
    earlier = next(p for p in EARLIER_POLICIES[dataset] if p.policy_version == "government-sources/1")
    assert earlier.policy_version == "government-sources/1" != POLICIES[dataset].policy_version
    assert [name for name in SUBJECT_SCHEMAS[dataset].names if name not in earlier.subject_schema.names] == ["url", "source", *added, "citation_product_id"]
    # The row as that generation's receipt holds it: the columns of the time, none of the later ones.
    published = module.COLUMNS[: module.COLUMNS.index("subject_terms_json") + 1]
    raw = {
        **dict.fromkeys(published),
        "report_id": "gao-04-193r",
        "title": "Federal Agency Major Rule Report: Department of Health and Human Services: A Rule",
        "report_type": "Report",
        "published_date": "2003-10-27",
        "topics_json": '["Health Care"]',
        "url": "https://www.gao.gov/products/gao-04-193r",
        "source": "gao_listing",
        "product_type": "Federal Agency Major Rule Report",
        "report_number": "GAO-04-193R",
    }
    subject = {name: value for name, value in map_subject(dataset, raw).items() if name in earlier.subject_schema.names}
    witness = {
        "source_id": "test:listing",
        "source_uri": None,
        "sha256": "sha256:" + "0" * 64,
        "locator": None,
        "body_version": None,
    }
    context = ReceiptContext("earlier", dataset + ":0", earlier.policy_version, [witness])
    refused_raw = {"unclassified_source_cell": "as received"}
    refused = failure_receipt(earlier, replace(context, attempt_id="prior-refusal"),
        outcome="refused", raw_fields={"raw_record": refused_raw})
    held, receipts = write_dataset([({**subject, "raw_record": raw}, context)], tmp_path / "published", earlier,
                                  failures=[refused] if with_prior_refusal else ())
    assert held is not None and not set(added) & set(pq.read_schema(held).names)
    prior = tmp_path / "_gao_prior.parquet"
    shutil.copyfile(held, prior)
    (tmp_path / BUILD_METADATA).write_text(
        json.dumps(
            {
                "generation_id": "earlier",
                "receipt_path": str(receipts),
                "receipt_sha256": _digest(receipts),
                "subjects": {dataset: {"path": str(prior), "sha256": _digest(prior)}},
            }
        )
    )

    class NoFeed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter(())

    monkeypatch.setattr(module, "GaoReportsReader", NoFeed)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)

    output, _ = module.build_gao_reports(tmp_path, receipt_generation_id="upgrade")

    assert pq.read_schema(output).equals(SUBJECT_SCHEMAS[dataset])
    [row] = pq.read_table(output).to_pylist()
    assert all(row[name] is None for name in added)
    assert {name: row[name] for name in earlier.subject_schema.names} == subject
    selected = tmp_path / "selected.parquet"
    select_receipts(tmp_path / "etl_receipts.parquet", selected, dataset=dataset)
    [joined] = read_with_receipts([output], [selected], POLICIES[dataset], generation_id="upgrade")
    assert {name: joined["raw_record"][name] for name in raw} == raw
    assert joined["raw_record"]["major_rule_letter_json"] is None
    receipt_rows = pq.read_table(selected).to_pylist()
    [receipt] = [item for item in receipt_rows if item["outcome"] == "accepted"]
    assert receipt["policy_version"] == POLICIES[dataset].policy_version
    # The row's subject changed (it gained columns): a fresh receipt that references only its predecessor.
    assert decode_exact_json(receipt["diagnostic_json"])["prior_receipt"]["generation_id"] == "earlier"
    assert [w["source_id"] for w in receipt["witnesses"]] == ["spicy-regs:producer-output:" + dataset]
    if with_prior_refusal:
        # No identity can link this raw input to a successful conversion: retain it unchanged.
        assert [item for item in receipt_rows if item["outcome"] == "refused"] == [refused]

    # The run that holds the letters' capture: the same row, now under the current policy, takes its letter.
    output.rename(prior)
    letters = capture(tmp_path / "letters", {PRODUCTS + "gao-04-193r": page("gao-04-193r")})
    output, _ = module.build_gao_reports(tmp_path, receipt_generation_id="letters", major_rule_letters=(letters,))
    [row] = pq.read_table(output).to_pylist()
    assert (row["major_rule_rins"], row["major_rule_fr_citations"]) == (["0910-AC40"], ["68-58894"])
    assert row["major_rule_agency"].startswith("Department of Health and Human Services")
    assert {name: row[name] for name in earlier.subject_schema.names} == subject


def test_acquisition_failure_receipt_pins_shared_evidence_without_exception_text(tmp_path):
    from spicy_regs.pipelines.rollups.government import GovernmentReceiptRollup
    from spicy_regs.transforms.government_receipts import _digest

    class FailedCrs(GovernmentReceiptRollup):
        name = "crs-failure-test"
        output = "crs_reports.parquet"
        inputs = ()
        retain_source_evidence = True

        def build(self, output_dir):
            raise RuntimeError("an arbitrary message that must not enter receipts")

    pipeline = FailedCrs(output_dir=tmp_path, skip_upload=True)
    with pytest.raises(RuntimeError, match="arbitrary message"):
        pipeline.run()
    evidence = pipeline.source_evidence
    assert evidence is not None
    receipts = evidence.directory / "etl-failure/etl_receipts.parquet"
    [attempt] = read_attempts([receipts], POLICIES["crs_reports"], generation_id=pipeline.receipt_generation_id)
    assert attempt["outcome"] == "error"
    assert attempt["diagnostics"] == {"stage": "rollup", "error_type": "RuntimeError"}
    assert attempt["witnesses"][0]["sha256"] == _digest(evidence.artifact_dir / "artifact.json")
    assert b"arbitrary message" not in receipts.read_bytes()
