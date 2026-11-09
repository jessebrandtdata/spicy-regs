"""Receipt joins fail closed while domain values and exact evidence survive."""

from dataclasses import replace
from decimal import Decimal
import json

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_KEY,
    RECEIPT_SCHEMA,
    split_record,
    failure_receipt,
    write_dataset,
    read_with_receipts,
    validate_receipt_bundle,
    combine_receipts,
    subject_identity,
    exact_json,
    read_receipt_bundle,
)
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.sources.publication import empty_index, publish_generation, receipt_members, PublicationError


@pytest.fixture
def policy():
    return DatasetPolicy(
        "payments",
        pa.schema(
            [
                ("id", pa.string()),
                ("body_id", pa.string()),
                ("status", pa.string()),
                ("amount", pa.decimal128(20, 2)),
                ("cycles", pa.list_(pa.int32())),
                ("people", pa.list_(pa.struct([("name", pa.string()), ("roles", pa.list_(pa.string()))]))),
            ]
        ),
        ("id", "body_id"),
        ("parser_status", "source_url", "amount_raw"),
    )


@pytest.fixture
def context():
    witness = {
        "source_id": "retained-response",
        "source_uri": "https://example.test/source",
        "sha256": "a" * 64,
        "locator": "row:2",
        "body_version": None,
    }
    return ReceiptContext("build-1", "attempt-1", "fixture-parser:1", [witness, witness])


@pytest.fixture
def row():
    return {
        "id": "1",
        "body_id": "v1",
        "status": "withdrawn",
        "amount": Decimal("9999999999999999.99"),
        "cycles": [2024, None, 2024],
        "people": [{"name": "A", "roles": ["member", None]}, None],
        "parser_status": "parsed",
        "source_url": "https://example.test/source",
        "amount_raw": "$9,999,999,999,999,999.99",
    }


def test_native_roundtrip_and_internal_read(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    held = pq.read_table(subject).to_pylist()[0]
    assert set(held) == set(policy.subject_schema.names)
    assert held["amount"] == row["amount"]
    assert held["cycles"] == row["cycles"]
    assert held["people"] == row["people"]
    assert held["status"] == "withdrawn"
    receipt_row = pq.read_table(receipt).to_pylist()[0]
    assert receipt_row["witnesses"] == list(context.witnesses)
    assert list(read_with_receipts([subject], [receipt], policy, generation_id="build-1")) == [row]


def test_validation_index_stays_small_while_large_processing_payloads_roundtrip(
    tmp_path, policy, context, row, monkeypatch
):
    from spicy_regs import etl_receipts

    rows = [row | {"id": str(index), "amount_raw": "source payload " * 10000} for index in range(16)]
    pairs = [split_record(policy, record, context) for record in rows]
    subject, receipts = tmp_path / "subjects.parquet", tmp_path / "receipts.parquet"
    pq.write_table(pa.Table.from_pylist([pair[0] for pair in pairs], schema=policy.subject_schema), subject)
    pq.write_table(pa.Table.from_pylist([pair[1] for pair in pairs], schema=RECEIPT_SCHEMA), receipts)
    original_match = etl_receipts._match_subjects

    def bounded_match(connection, subjects, policies):
        size = connection.execute("PRAGMA page_count").fetchone()[0] * connection.execute(
            "PRAGMA page_size"
        ).fetchone()[0]
        assert size < 512 * 1024
        yield from original_match(connection, subjects, policies)

    with monkeypatch.context() as patch:
        patch.setattr(etl_receipts, "_match_subjects", bounded_match)
        validate_receipt_bundle({policy.dataset: [subject]}, [receipts], [policy], generation_id="build-1")
        assert list(read_with_receipts([subject], [receipts], policy, generation_id="build-1")) == rows


@pytest.mark.parametrize("fault", ["digest", "unclassified"])
def test_validation_checks_processing_even_when_it_does_not_keep_a_replay_copy(
    tmp_path, policy, context, row, fault
):
    from spicy_regs import etl_receipts

    subject_row, receipt = split_record(policy, row, context)
    processing = {field: row[field] for field in policy.receipt_fields}
    processing["amount_raw" if fault == "digest" else "unclassified"] = "changed source input"
    receipt["processing_json"] = exact_json(processing)
    if fault == "unclassified":
        receipt["receipt_id"] = etl_receipts._digest({key: value for key, value in receipt.items() if key != "receipt_id"})
    subject, receipts = tmp_path / "subjects.parquet", tmp_path / "receipts.parquet"
    pq.write_table(pa.Table.from_pylist([subject_row], schema=policy.subject_schema), subject)
    pq.write_table(pa.Table.from_pylist([receipt], schema=RECEIPT_SCHEMA), receipts)
    with pytest.raises(ValueError, match="digest" if fault == "digest" else "unclassified"):
        validate_receipt_bundle({policy.dataset: [subject]}, [receipts], [policy], generation_id="build-1")


def test_complete_bundle_read_preserves_exact_values_and_explicit_attempts(tmp_path, policy, context, row):
    reads = DatasetPolicy("parser_reads", pa.schema([]), (), ("checkpoint",), receipt_only=True)
    failed = failure_receipt(policy, replace(context, attempt_id="refused"), outcome="refused", raw_fields=row)
    subject, receipts = write_dataset([(row, context)], tmp_path / "payments", policy, failures=[failed])
    assert subject is not None
    _, observed = write_dataset([({"checkpoint": [None, "", 0]}, context)], tmp_path / "reads", reads)
    shared = combine_receipts([receipts, observed], tmp_path / "shared.parquet")
    subjects, policies = {policy.dataset: [subject], reads.dataset: []}, [policy, reads]
    assert read_receipt_bundle(subjects, [shared], policies, generation_id="build-1") == {
        "payments": [row], "parser_reads": []
    }
    result = read_receipt_bundle(
        subjects, [shared], policies, generation_id="build-1",
        processing_outcomes={"payments": frozenset({"refused"}), "parser_reads": frozenset({"observed"})},
    )
    assert result == {"payments": [row, row], "parser_reads": [{"checkpoint": [None, "", 0]}]}
    from spicy_regs.etl_receipts import visit_receipt_bundle

    visited = {name: [] for name in subjects}
    counts = visit_receipt_bundle(
        subjects, [shared], policies, generation_id="build-1",
        processing_outcomes={"payments": frozenset({"refused"}), "parser_reads": frozenset({"observed"})},
        visit=lambda name, value: visited[name].append(value),
    )
    assert visited == result
    assert counts == {name: len(values) for name, values in result.items()}


@pytest.mark.parametrize("fault", ["late-subject", "orphan", "duplicate", "identity", "publisher", "receipt", "schema"])
def test_complete_bundle_read_refuses_inconsistent_later_dataset(tmp_path, policy, context, row, fault):
    other = replace(policy, dataset="refunds")
    first, first_receipt = write_dataset([(row, context)], tmp_path / "payments", policy)
    second, second_receipt = write_dataset([(row, context)], tmp_path / "refunds", other)
    assert first is not None and second is not None
    shared = combine_receipts([first_receipt, second_receipt], tmp_path / "shared.parquet")
    subjects = {"payments": [first], "refunds": [second]}
    selected_generation = "build-1"
    if fault == "late-subject":
        changed = split_record(other, row | {"amount": Decimal("1.00")}, context)[0]
        pq.write_table(pa.Table.from_pylist([changed], schema=other.subject_schema), second)
    elif fault == "orphan":
        subjects["refunds"] = []
    elif fault in {"duplicate", "identity", "receipt"}:
        receipts = pq.read_table(shared).to_pylist()
        if fault == "duplicate":
            receipts.append(receipts[-1])
        elif fault == "identity":
            duplicate = split_record(other, row, replace(context, attempt_id="other-attempt"))[1]
            assert duplicate["receipt_id"] != receipts[-1]["receipt_id"]
            receipts.append(duplicate)
        else:
            receipts[-1]["processor"] = "altered"
        pq.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA), shared)
    elif fault == "publisher":
        selected_generation = ""
    else:
        pq.write_table(pq.read_table(second).append_column("extra", pa.array(["x"])), second)
    with pytest.raises(ValueError):
        read_receipt_bundle(subjects, [shared], [policy, other], generation_id=selected_generation)
    from spicy_regs.etl_receipts import visit_receipt_bundle

    visited = []
    with pytest.raises(ValueError):
        visit_receipt_bundle(subjects, [shared], [policy, other], generation_id=selected_generation,
                             visit=lambda name, value: visited.append((name, value)))
    if fault in {"late-subject", "orphan", "schema"}:
        assert visited == [("payments", row)]  # Earlier visits are provisional; no counts return.
    else:
        assert visited == []  # Loading and uniqueness checks finish before the first visit.


@pytest.mark.parametrize("outcomes", [{"missing": frozenset({"observed"})}, {"payments": frozenset({"accepted"})}, {"payments": frozenset({"unknown"})}])
def test_complete_bundle_read_requires_explicit_nonaccepted_outcome_selection(tmp_path, policy, context, row, outcomes):
    subject, receipt = write_dataset([(row, context)], tmp_path / "payments", policy)
    assert subject is not None
    with pytest.raises(ValueError, match="nonaccepted"):
        read_receipt_bundle({"payments": [subject]}, [receipt], [policy], generation_id="build-1", processing_outcomes=outcomes)


def test_complete_bundle_read_checks_unselected_failed_receipts(tmp_path, policy, context, row):
    failed = failure_receipt(policy, replace(context, attempt_id="refused"), outcome="refused", raw_fields=row)
    subject, receipt = write_dataset([(row, context)], tmp_path / "payments", policy, failures=[failed])
    assert subject is not None
    receipts = pq.read_table(receipt).to_pylist()
    receipts[-1]["processor"] = "altered"
    pq.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA), receipt)
    with pytest.raises(ValueError, match="digest"):
        read_receipt_bundle({"payments": [subject]}, [receipt], [policy], generation_id="build-1")
    from spicy_regs.etl_receipts import visit_receipt_bundle

    with pytest.raises(ValueError, match="digest"):
        visit_receipt_bundle({"payments": [subject]}, [receipt], [policy], generation_id="build-1",
                             visit=lambda *args: pytest.fail("Invalid receipts must refuse before visits"))


@pytest.mark.parametrize("fault", ["digest", "schema"])
@pytest.mark.parametrize("duplicate_first", [True, False])
def test_receipt_load_preserves_first_refusal_before_visits(tmp_path, policy, context, row, fault, duplicate_first):
    from spicy_regs.etl_receipts import visit_receipt_bundle

    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    duplicate = combine_receipts([receipt, receipt], tmp_path / "duplicate.parquet")
    damaged = tmp_path / "damaged.parquet"
    table = pq.read_table(receipt)
    if fault == "schema":
        table = table.append_column("extra", pa.array(["x"]))
    else:
        rows = table.to_pylist()
        rows[0]["processor"] = "altered"
        table = pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA)
    pq.write_table(table, damaged)
    paths = [duplicate, damaged] if duplicate_first else [damaged, duplicate]
    message = "Duplicate or ambiguous receipt join" if duplicate_first else fault
    with pytest.raises(ValueError, match=message):
        visit_receipt_bundle({"payments": [subject]}, paths, [policy], generation_id="build-1",
                             visit=lambda *args: pytest.fail("Invalid receipts must refuse before visits"))
    # The bulk validator preserves the row reader's first-error boundary too.
    with pytest.raises(ValueError, match=message):
        validate_receipt_bundle({"payments": [subject]}, paths, [policy], generation_id="build-1")


def test_cancelled_receipt_load_does_not_build_indexes_or_visit(tmp_path, policy, context, row, monkeypatch):
    from spicy_regs import etl_receipts as etl
    from spicy_regs.runtime_bounds import RequestBound, checkpoint

    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    bound = RequestBound(None, label="receipt load cancelled")
    original_rows, original_connect = etl._rows, etl.sqlite3.connect
    statements = []

    def cancel_after_load(path):
        yield from original_rows(path)
        bound.cancel()
        checkpoint()

    def tracked_connection(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(etl, "_rows", cancel_after_load)
    monkeypatch.setattr(etl.sqlite3, "connect", tracked_connection)
    with pytest.raises(TimeoutError, match="receipt load cancelled"), bound.scope():
        etl.visit_receipt_bundle({"payments": [subject]}, [receipt], [policy], generation_id="build-1",
                                 visit=lambda *args: pytest.fail("Cancelled receipts must not be visited"))
    assert any(sql.startswith("INSERT INTO receipts") for sql in statements)
    assert not any(sql.startswith("CREATE UNIQUE INDEX") for sql in statements)


@pytest.mark.parametrize(
    "change",
    [
        {"unclassified": 1},
        {"people": [{"name": "A", "extra": 3}]},
        {"cycles": "[2024]"},
        {"cycles": [True]},
        {"amount": 0.1},
    ],
)
def test_no_silent_loss_or_coercion(policy, context, row, change):
    with pytest.raises(ValueError):
        split_record(policy, row | change, context)


def test_null_empty_and_order_are_distinct(policy, context, row):
    identities = [
        split_record(policy, row | {"cycles": values}, context)[1]["subject_version"]
        for values in (None, [], [None], [2024, 2026], [2026, 2024], [2024, 2024])
    ]
    assert len(set(identities)) == len(identities)


def test_decimal_raw_receipt_encoding_does_not_pass_through_float():
    assert exact_json(Decimal("9999999999999999.99")) == '["decimal","9999999999999999.99"]'
    assert exact_json({"__type__": "decimal", "value": "1.00"}) != exact_json(Decimal("1.00"))


def test_failed_attempts_have_no_subject(tmp_path, policy, context, row):
    failed = failure_receipt(
        policy, replace(context, attempt_id="bad"), outcome="refused", raw_fields=row | {"amount": "not money"}
    )
    subject, receipts = write_dataset([(row, context)], tmp_path / "bundle", policy, failures=[failed])
    assert subject is not None
    assert pq.read_table(subject).num_rows == 1
    assert pq.read_table(receipts).num_rows == 2
    assert failed["record_id"] is None and failed["subject_version"] is None


def test_processing_only(tmp_path, context):
    policy = DatasetPolicy("parser_reads", pa.schema([]), (), ("body_sha", "error"), receipt_only=True)
    subject, receipts = write_dataset([({"body_sha": "x", "error": None}, context)], tmp_path / "bundle", policy)
    assert subject is None
    assert pq.read_table(receipts).to_pylist()[0]["outcome"] == "observed"


def test_duplicate_identity_different_attempts_refused_atomically(tmp_path, policy, context, row):
    with pytest.raises(ValueError, match="ambiguous"):
        write_dataset([(row, context), (row, replace(context, attempt_id="attempt-2"))], tmp_path / "bundle", policy)
    assert not (tmp_path / "bundle").exists()


def test_late_failure_leaves_no_partial_bundle(tmp_path, policy, context, row):
    def rows():
        yield row, context
        raise RuntimeError("later source refused")

    with pytest.raises(RuntimeError):
        write_dataset(rows(), tmp_path / "bundle", policy, batch_size=1)
    assert not (tmp_path / "bundle").exists()


def test_generation_and_content_join(tmp_path, policy, context, row):
    subject, receipts = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    assert list(read_with_receipts([subject], [receipts], policy, generation_id="other")) == [row]
    pq.write_table(
        pa.Table.from_pylist(
            [split_record(policy, row | {"amount": Decimal("1.00")}, context)[0]], schema=policy.subject_schema
        ),
        subject,
    )
    with pytest.raises(ValueError, match="Missing"):
        validate_receipt_bundle({"payments": [subject]}, [receipts], [policy], generation_id="build-1")


def test_versioned_body_keys_do_not_collapse(policy, row):
    assert subject_identity(policy, row)[0] != subject_identity(policy, row | {"body_id": "v2"})[0]


def test_direct_writer_extra_subject_column_refuses(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    table = pq.read_table(subject).append_column("parser_status", pa.array(["ok"]))
    pq.write_table(table, subject)
    with pytest.raises(ValueError, match="schema"):
        validate_receipt_bundle({"payments": [subject]}, [receipt], [policy])


def test_polars_native_input(tmp_path, policy, context, row):
    frame = pl.from_arrow(pa.Table.from_pylist([split_record(policy, row, context)[0]], schema=policy.subject_schema))
    assert isinstance(frame, pl.DataFrame)
    records = ((r | {k: row[k] for k in policy.receipt_fields}, context) for r in frame.iter_rows(named=True))
    subject, _ = write_dataset(records, tmp_path / "polars", policy)
    assert subject is not None
    assert pq.read_table(subject).to_pylist()[0]["amount"] == row["amount"]


def test_generation_admission_and_receipt_tampering(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    artifact = build_generation(
        tmp_path / "generation",
        family="payments",
        files=[subject],
        expected_keys=[subject.name],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    assert artifact.root["spec"]["etlReceipts"]["generationId"] == "build-1"
    assert set(artifact.root["spec"]["tables"]) == {"payments.parquet"}
    verify_generation(tmp_path / "generation", expected_pin=artifact.pin)
    pq.write_table(RECEIPT_SCHEMA.empty_table(), tmp_path / "generation" / RECEIPT_KEY)
    with pytest.raises(Exception):
        verify_generation(tmp_path / "generation")


def test_generation_rejects_unclassified_output(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    other = tmp_path / "other.parquet"
    pq.write_table(pa.table({"id": ["1"]}), other)
    with pytest.raises(ValueError, match="classify every subject"):
        build_generation(
            tmp_path / "generation",
            family="payments",
            files=[subject, other],
            expected_keys=[subject.name, other.name],
            receipt_path=receipt,
            receipt_policies=[policy],
            receipt_generation_id="build-1",
        )


def test_publication_shared_receipt_members_do_not_collide(tmp_path, policy, context, row):
    from tests.generation_fakes import Store

    client = Store()
    index = empty_index()
    for dataset in ("payments", "other_payments"):
        p = replace(policy, dataset=dataset)
        subject, receipt = write_dataset([(row, context)], tmp_path / dataset, p)
        assert subject is not None
        generation = tmp_path / (dataset + "-generation")
        build_generation(
            generation,
            family=dataset,
            files=[subject],
            expected_keys=[subject.name],
            receipt_path=receipt,
            receipt_policies=[p],
            receipt_generation_id="build-1",
        )
        index = publish_generation(generation, client=client, bucket="b", prior_index=index)
    assert len(receipt_members(index)) == 2
    assert len(receipt_members(index, dataset="payments")) == 1
    with pytest.raises(PublicationError):
        receipt_members(index, dataset="unknown")


def test_no_witness_or_unpinned_witness_refuses(context):
    with pytest.raises(ValueError):
        replace(context, witnesses=[])
    with pytest.raises(ValueError):
        replace(context, witnesses=[{"source_id": "url-only", "source_uri": "https://example.test"}])


def test_policy_descriptor_exact_native_roundtrip(policy):
    restored = DatasetPolicy.from_descriptor(json.loads(json.dumps(policy.descriptor())))
    assert restored == policy


def test_empty_subject_and_failures_are_valid(tmp_path, policy, context, row):
    failure = failure_receipt(policy, context, outcome="error", raw_fields=row)
    subject, receipt = write_dataset([], tmp_path / "empty", policy, failures=[failure])
    assert subject is not None
    assert pq.read_table(subject).num_rows == 0
    assert pq.read_table(receipt).num_rows == 1


def test_missing_and_orphan_receipts(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    empty = tmp_path / "empty.parquet"
    pq.write_table(RECEIPT_SCHEMA.empty_table(), empty)
    with pytest.raises(ValueError, match="Missing"):
        validate_receipt_bundle({"payments": [subject]}, [empty], [policy])
    pq.write_table(policy.subject_schema.empty_table(), subject)
    with pytest.raises(ValueError, match="no matching subject"):
        validate_receipt_bundle({"payments": [subject]}, [receipt], [policy])


def test_duplicate_receipts_survive_combine_and_refuse(tmp_path, policy, context, row):
    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    combined = combine_receipts([receipt, receipt], tmp_path / "combined.parquet")
    with pytest.raises(ValueError, match="Duplicate"):
        validate_receipt_bundle({"payments": [subject]}, [combined], [policy])


def test_remote_receipt_admission_has_no_local_subject_copy(tmp_path, policy, context, row):
    from hashlib import sha256
    from spicy_regs.native_types import described_schema
    from spicy_regs.remote_generations import prepare_remote_generation, publish_remote_generation
    from spicy_regs.sources.remote_parquet import StoredParquet
    from tests.test_remote_generations import RemoteStore

    subject, receipts = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    store, members = RemoteStore(), []
    for path in (subject, receipts):
        raw, key = path.read_bytes(), "stage/one/" + path.name
        store.objects[key] = raw
        members.append(
            StoredParquet(
                key, len(raw), "sha256:" + sha256(raw).hexdigest(), store.get_object(Bucket="b", Key=key)["ETag"], 1
            )
        )
    directory = tmp_path / "remote"
    artifact = prepare_remote_generation(
        directory,
        family="payments",
        client=store,
        bucket="b",
        staging_prefix="stage/one",
        members=members,
        expected_keys=[subject.name, receipts.name],
        schemas={"payments": described_schema(policy.subject_schema), "etl_receipts": described_schema(RECEIPT_SCHEMA)},
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    assert not list(directory.glob("*.parquet"))
    index = publish_remote_generation(
        directory, client=store, bucket="b", staging_prefix="stage/one", members=members, prior_index=empty_index()
    )
    assert receipt_members(index, dataset="payments")[0].sha256 == members[1].sha256
    assert index["families"]["payments"]["artifactDigest"] == artifact.pin.artifact_digest


def test_carry_rebind_preserves_original_evidence(tmp_path, policy, context, row):
    from spicy_regs.etl_receipts import rebind_receipt

    subject, receipts = write_dataset([(row, context)], tmp_path / "original", policy)
    assert subject is not None
    original = pq.read_table(receipts).to_pylist()[0]
    carried = rebind_receipt(original, generation_id="build-2")
    for name in ("witnesses", "processor", "attempt_id", "record_id", "subject_version", "processing_json"):
        assert carried[name] == original[name]
    assert carried["receipt_id"] != original["receipt_id"]
    pq.write_table(pa.Table.from_pylist([carried], schema=RECEIPT_SCHEMA), receipts)
    assert list(read_with_receipts([subject], [receipts], policy, generation_id="build-2")) == [row]


def test_registered_dataset_cannot_bypass_receipts(tmp_path, policy, context, row, monkeypatch):
    from spicy_regs import etl_policy_registry

    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {policy.dataset: policy})
    subject, _ = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    with pytest.raises(ValueError, match="require ETL receipts"):
        build_generation(tmp_path / "invalid", family="payments", files=[subject], expected_keys=[subject.name])


def test_receipt_only_generation(tmp_path, context):
    from tests.generation_fakes import Store

    policy = DatasetPolicy("failed_fetches", pa.schema([]), (), ("reason",), receipt_only=True)
    _, receipt = write_dataset([({"reason": "no response"}, context)], tmp_path / "bundle", policy)
    directory = tmp_path / "generation"
    build_generation(
        directory,
        family="reads",
        files=[],
        expected_keys=[],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    index = publish_generation(directory, client=Store(), bucket="b", prior_index=empty_index())
    assert index["families"]["reads"]["tables"] == {}
    assert len(receipt_members(index, dataset="failed_fetches")) == 1


def test_successful_empty_read_checkpoint_is_not_a_subject_or_failure(tmp_path, policy, context):
    from spicy_regs.etl_receipts import observation_receipt, read_attempts

    receipt = observation_receipt(policy, context, processing_fields={"parser_status": "requested-empty"})
    subject, receipts = write_dataset([], tmp_path / "bundle", policy, failures=[receipt])
    assert subject is not None
    assert pq.read_table(subject).num_rows == 0
    held = list(read_attempts([receipts], policy, generation_id="build-1"))
    assert held[0]["outcome"] == "observed"
    assert held[0]["processing_fields"] == {"parser_status": "requested-empty"}
    assert held[0]["record_id"] is None


def test_processing_table_moves_only_by_explicit_receipt_policy(tmp_path, context):
    from tests.generation_fakes import Store

    store = Store()
    legacy = tmp_path / "reads.parquet"
    pq.write_table(pa.table({"reason": ["requested-empty"]}), legacy)
    build_generation(tmp_path / "old", family="reads", files=[legacy], expected_keys=[legacy.name])
    index = publish_generation(tmp_path / "old", client=store, bucket="b", prior_index=empty_index())
    policy = DatasetPolicy("reads", pa.schema([]), (), ("reason",), receipt_only=True)
    _, receipt = write_dataset([({"reason": "requested-empty"}, context)], tmp_path / "split", policy)
    build_generation(
        tmp_path / "new",
        family="reads",
        files=[],
        expected_keys=[],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    with pytest.raises(PublicationError, match="membership changed"):
        publish_generation(tmp_path / "new", client=store, bucket="b", prior_index=index)
    updated = publish_generation(
        tmp_path / "new", client=store, bucket="b", prior_index=index, receipt_only_tables=frozenset({"reads.parquet"})
    )
    assert not updated["families"]["reads"]["tables"]
    assert receipt_members(updated, dataset="reads")
    assert any(key.endswith("/reads.parquet") for key in store.objects)


def test_receipt_only_checkpoints_preserve_order_attempts_and_refusals(tmp_path, context):
    from spicy_regs.etl_receipts import read_attempts

    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)
    records = [({"cursor": "same"}, replace(context, attempt_id=f"attempt-{i}")) for i in range(2)]
    failed = failure_receipt(
        policy, replace(context, attempt_id="failed"), outcome="error", raw_fields={"cursor": "unavailable"}
    )
    _, receipt = write_dataset(records, tmp_path / "bundle", policy, failures=[failed])
    attempts = list(read_attempts([receipt], policy, generation_id="build-1"))
    assert [r["attempt_id"] for r in attempts] == ["attempt-0", "attempt-1", "failed"]
    checkpoints = list(read_attempts([receipt], policy, generation_id="build-1", outcomes=frozenset({"observed"})))
    assert [r["processing_fields"] for r in checkpoints] == [{"cursor": "same"}, {"cursor": "same"}]
    assert list(read_attempts([receipt], policy, generation_id="another-build")) == attempts


def test_installed_receipt_only_policy_cannot_be_replaced_or_duplicated(monkeypatch):
    from spicy_regs import etl_policy_registry

    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {policy.dataset: policy})
    etl_policy_registry.require_registered_receipts({}, {"policies": [policy.descriptor()]})
    wrong = replace(policy, policy_version="other").descriptor()
    with pytest.raises(ValueError, match="installed field policy"):
        etl_policy_registry.require_registered_receipts({}, {"policies": [wrong]})
    with pytest.raises(ValueError, match="duplicate datasets"):
        etl_policy_registry.require_registered_receipts({}, {"policies": [policy.descriptor()] * 2})


def test_receipt_dataset_has_one_family_owner_and_cannot_disappear(tmp_path, context):
    from tests.generation_fakes import Store
    from spicy_regs.sources.publication import parse_index

    store = Store()
    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)
    _, receipt = write_dataset([({"cursor": "one"}, context)], tmp_path / "bundle", policy)

    def generation(name, family, policies, receipt_path):
        path = tmp_path / name
        build_generation(
            path,
            family=family,
            files=[],
            expected_keys=[],
            receipt_path=receipt_path,
            receipt_policies=policies,
            receipt_generation_id="build-1",
        )
        return path

    first = generation("first", "checks", [policy], receipt)
    index = publish_generation(first, client=store, bucket="b", prior_index=empty_index())
    second = generation("second", "other", [policy], receipt)
    with pytest.raises(PublicationError, match="already belongs"):
        publish_generation(second, client=store, bucket="b", prior_index=index)
    cloned = json.loads(json.dumps(index))
    other = cloned["families"]["other"] = json.loads(json.dumps(cloned["families"]["checks"]))
    other["prefix"] = other["prefix"].replace("/checks/", "/other/")
    with pytest.raises(PublicationError, match="Invalid publication index"):
        parse_index(json.dumps(cloned).encode())
    other_policy = replace(policy, dataset="other_checks")
    _, other_receipt = write_dataset([({"cursor": "two"}, context)], tmp_path / "other-bundle", other_policy)
    replacement = generation("replacement", "checks", [other_policy], other_receipt)
    with pytest.raises(PublicationError, match="drops receipt dataset ownership"):
        publish_generation(replacement, client=store, bucket="b", prior_index=index)


def test_receipt_only_rollup_has_no_subject_output(tmp_path, monkeypatch, context):
    from spicy_regs.pipelines.rollups.base import RollupPipeline

    policy = DatasetPolicy("resume_checks", pa.schema([]), (), ("cursor",), receipt_only=True)

    class Checks(RollupPipeline):
        name = "checks"
        output = "resume_checks.parquet"
        receipt_only_tables = ("resume_checks.parquet",)
        receipt_policies = (policy,)

        def build(self, output_dir):
            build_context = replace(context, generation_id=self.receipt_generation_id)
            _, receipt = write_dataset([({"cursor": "complete"}, build_context)], output_dir / "checks", policy)
            combine_receipts([receipt], output_dir / RECEIPT_KEY)
            return ()

        def generation_schemas(self):
            return {}

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    Checks(output_dir=tmp_path, skip_upload=True).run()
    (directory,) = (tmp_path / "generations").iterdir()
    artifact = verify_generation(directory)
    assert artifact.root["spec"]["tables"] == {}
    assert artifact.root["spec"]["etlReceipts"]["policies"] == [policy.descriptor()]


def test_receipt_member_descriptor_count_must_match_bytes(tmp_path, policy, context, row, monkeypatch):
    import rulespec_artifacts

    subject, receipt = write_dataset([(row, context)], tmp_path / "bundle", policy)
    assert subject is not None
    directory = tmp_path / "generation"
    build_generation(
        directory,
        family="payments",
        files=[subject],
        expected_keys=[subject.name],
        receipt_path=receipt,
        receipt_policies=[policy],
        receipt_generation_id="build-1",
    )
    original = rulespec_artifacts.iter_member_descriptors

    def altered(*args):
        for member in original(*args):
            yield replace(member, record_count=99) if member.object_key == RECEIPT_KEY else member

    monkeypatch.setattr(rulespec_artifacts, "iter_member_descriptors", altered)
    with pytest.raises(ValueError, match="receipt member count"):
        verify_generation(directory)


@pytest.mark.parametrize("mutation", ["policy", "generation", "missing_receipt"])
def test_materialized_publication_checks_declared_receipt_policy_before_upload(
    tmp_path, policy, context, row, monkeypatch, mutation
):
    from spicy_regs.pipelines.materialized import MaterializedDatasetPipeline
    from spicy_regs.ontology.common import RunContext

    class Example(MaterializedDatasetPipeline):
        name = "receipt-example"
        dataset_name = "receipt-example"
        published_outputs = ("payments.parquet",)
        receipt_policies = (policy,)

        def stages(self):
            return ()

    output = tmp_path / "bundle"
    write_dataset([(row, context)], output, policy)
    runner = Example(output_dir=output)
    manifest, pointer, files = runner._write_publication_files(
        output, context=RunContext("build-1", "2026-10-03T00:00:00Z"), stages=(), input_snapshot={}
    )
    data = json.loads(manifest.read_text())
    if mutation == "policy":
        data["etlReceipts"]["policies"][0]["policy_version"] = "unknown-policy"
    elif mutation == "generation":
        data["etlReceipts"]["generationId"] = "wrong-generation"
    else:
        del files["etl_receipts.parquet"]
    manifest.write_text(json.dumps(data))
    monkeypatch.setattr("spicy_regs.sources.r2.upload_file", lambda *a, **kw: pytest.fail("uploaded before admission"))
    with pytest.raises(ValueError, match="receipt policy|membership"):
        runner._publish(manifest_path=manifest, pointer_path=pointer, artifact_paths=files)


def test_retained_receipt_lineage_resolves_original_values_after_repeated_updates(policy, row, context):
    from spicy_regs.etl_receipts import inherit_receipt, resolve_receipt_witness, retire_receipt, _unpack
    import hashlib

    original = dict(row)
    original["amount_raw"] = "00012.30"
    raw_witness = {
        "source_id": "amount-literal",
        "source_uri": None,
        "sha256": hashlib.sha256(exact_json(original["amount_raw"]).encode()).hexdigest(),
        "locator": "receipt.values.amount_raw (canonical exact_json)",
        "body_version": "build-1",
    }
    first_context = replace(context, witnesses=[raw_witness, raw_witness])
    _, first = split_record(policy, original, first_context)
    second_context = inherit_receipt(replace(context, generation_id="build-2"), first)
    _, second = split_record(policy, row, second_context)
    _, third = split_record(policy, row, inherit_receipt(replace(context, generation_id="build-3"), second))
    assert third["witnesses"][:2] == first["witnesses"]
    assert resolve_receipt_witness(third, raw_witness) == exact_json("00012.30").encode()
    prior_ref = next(w for w in third["witnesses"] if w["body_version"] == first["receipt_id"])
    assert _unpack(json.loads(resolve_receipt_witness(third, prior_ref))) == _unpack(
        json.loads(first["processing_json"])
    )
    identities = _unpack(json.loads(third["diagnostic_json"]))["prior_receipts"]
    assert first["receipt_id"] in {r["receipt_id"] for r in identities}
    retired = retire_receipt(third, generation_id="build-4", reason="explicit deletion")
    assert retired["outcome"] == "observed" and retired["subject_version"] is None
    assert resolve_receipt_witness(retired, raw_witness) == exact_json("00012.30").encode()
    with pytest.raises(ValueError, match="digest differs"):
        inherit_receipt(context, dict(first, processing_json=exact_json({})))


def test_daily_receipt_carries_have_linear_size_and_resolvable_source_values(policy, row, context):
    from spicy_regs.etl_receipts import inherit_receipt, resolve_receipt_witness, _unpack

    original = dict(row, amount_raw="0" * 8000 + "12.30")
    _, current = split_record(policy, original, context)
    first = current
    sizes = []
    for number in range(25):
        _, current = split_record(
            policy,
            original,
            inherit_receipt(replace(context, generation_id=f"day-{number}", attempt_id=f"day-{number}"), current),
        )
        sizes.append(len(exact_json(current).encode()))
    diagnostics = _unpack(json.loads(current["diagnostic_json"]))
    assert len(diagnostics["retained_processing"]) == 1
    assert len(diagnostics["prior_receipts"]) == 25
    assert sizes[24] - sizes[14] < 1.1 * (sizes[14] - sizes[4])
    witness = next(w for w in current["witnesses"] if w["body_version"] == first["receipt_id"])
    assert resolve_receipt_witness(current, witness) == first["processing_json"].encode()


def test_update_and_rebind_preserve_prior_processing_context(policy, row, context):
    from spicy_regs.etl_receipts import inherit_receipt, rebind_receipt, _unpack

    evidence = {"coverage": "partial", "refusal_kind": "source-declared", "counts": {"seen": 2}}
    _, first = split_record(policy, row, replace(context, diagnostics=evidence))
    _, second = split_record(policy, row, inherit_receipt(replace(context, generation_id="update"), first))
    [prior] = _unpack(json.loads(second["diagnostic_json"]))["prior_receipts"]
    assert prior["diagnostics"] == evidence
    assert (prior["processor"], prior["attempt_id"], prior["outcome"]) == (
        context.processor,
        context.attempt_id,
        "accepted",
    )
    carried = rebind_receipt(first, generation_id="carry")
    diagnostics = _unpack(json.loads(carried["diagnostic_json"]))
    assert {key: diagnostics[key] for key in evidence} == evidence
    assert diagnostics["prior_receipts"][0]["diagnostics"] == evidence


def test_member_carries_receipts_from_original_generations_for_all_outcomes(tmp_path, policy, context, row):
    from spicy_regs.etl_receipts import observation_receipt
    accepted = split_record(policy, row, context)[1]
    observed = observation_receipt(policy, replace(context, generation_id="older", attempt_id="observed"),
                                   processing_fields={"source_url": "https://source.test"})
    failures = [failure_receipt(policy, replace(context, generation_id="older-" + outcome, attempt_id=outcome),
                               outcome=outcome, raw_fields={"source_url": "same"})
                for outcome in ("rejected", "refused", "error")]
    subject, receipts = write_dataset([(row, context)], tmp_path / "mixed", policy, failures=[observed, *failures])
    assert subject is not None
    before = receipts.read_bytes()
    validate_receipt_bundle({policy.dataset: [subject]}, [receipts], [policy], generation_id="new-publisher")
    assert list(read_with_receipts([subject], [receipts], policy, generation_id="new-publisher")) == [row]
    assert receipts.read_bytes() == before
    assert pq.read_table(receipts).to_pylist()[0] == accepted


@pytest.mark.parametrize("dataset", ["fcc_proceedings", "gao_decisions", "gao_recommendations", "gao_reports", "lobbying_filings"])
def test_only_declared_earlier_policies_are_admitted(tmp_path, dataset):
    from spicy_regs.etl_receipts import receipt_policies, validate_receipt_row
    from spicy_regs.transforms.government_receipts import POLICIES, EARLIER_POLICIES
    current = POLICIES[dataset]
    assert receipt_policies(current)[1:] == EARLIER_POLICIES[dataset]
    prior = EARLIER_POLICIES[dataset][0]
    witness = {"source_id": "test", "source_uri": "https://source.test", "sha256": "a" * 64, "locator": "/"}
    context = ReceiptContext("old", "attempt", "test", [witness])
    failed = failure_receipt(prior, context, outcome="rejected", raw_fields={"raw_record": {}})
    validate_receipt_row(failed, {current.dataset: current})
    unknown = replace(prior, policy_version="government-sources/unknown")
    failed = failure_receipt(unknown, context, outcome="rejected", raw_fields={"raw_record": {}})
    with pytest.raises(ValueError, match="matching dataset policy"):
        validate_receipt_row(failed, {current.dataset: current})


@pytest.mark.parametrize("backend", [None, True, 1, [], {}, "", "other"])
def test_invalid_write_validation_backend_refuses_before_writes(tmp_path, policy, backend):
    consumed = False

    def records():
        nonlocal consumed
        consumed = True
        yield pytest.fail("invalid backend consumed source records")

    parent = tmp_path / "not-created"
    with pytest.raises(ValueError, match="validation_backend"):
        write_dataset(records(), parent / "bundle", policy, validation_backend=backend)
    assert not consumed
    assert not parent.exists()


def test_write_validation_backends_preserve_exact_subjects_and_receipts(tmp_path, context, monkeypatch):
    from spicy_regs import etl_receipts
    from spicy_regs import etl_bulk

    p = DatasetPolicy(
        "section_fixture",
        pa.schema([("id", pa.string()), ("body", pa.string()), ("note", pa.string())]),
        ("id",),
        ("native_processing",),
    )
    values = [
        {"id": "1", "body": "Native quote: \"hello\"\nUnicode é😀", "note": None, "native_processing": {"raw": "01"}},
        {"id": "2", "body": "", "note": "\x1b", "native_processing": {"raw": "", "items": [None, ""]}},
    ]
    real = etl_receipts.validate_receipt_bundle
    backends = []

    def validation(*args, **kwargs):
        backends.append(kwargs.get("bulk", True))
        return real(*args, **kwargs)

    monkeypatch.setattr(etl_receipts, "validate_receipt_bundle", validation)
    default = write_dataset(((r, context) for r in values), tmp_path / "default", p)

    def unexpected_bulk(*args, **kwargs):
        pytest.fail("reference selection entered the bulk validator")

    monkeypatch.setattr(etl_bulk, "validate_bundle", unexpected_bulk)
    reference = write_dataset(((r, context) for r in values), tmp_path / "reference", p, validation_backend="reference")
    assert backends == [True, False]
    assert default[0] is not None and reference[0] is not None
    assert pq.read_table(default[0]).to_pylist() == pq.read_table(reference[0]).to_pylist()
    assert pq.read_table(default[1]).to_pylist() == pq.read_table(reference[1]).to_pylist()
    assert list(read_with_receipts([reference[0]], [reference[1]], p, generation_id=context.generation_id)) == values


def test_reference_write_rejects_duplicate_bindings_without_installing(tmp_path, policy, context, row, monkeypatch):
    from spicy_regs import etl_bulk

    def unexpected_bulk(*args, **kwargs):
        pytest.fail("reference selection entered the bulk validator")

    monkeypatch.setattr(etl_bulk, "validate_bundle", unexpected_bulk)
    output = tmp_path / "refused"
    with pytest.raises(ValueError, match="Duplicate or ambiguous"):
        write_dataset([(row, context), (row, context)], output, policy, validation_backend="reference")
    assert not output.exists()
    assert not list(tmp_path.glob(".etl-*"))


def test_combine_keeps_receipt_groups_bounded_across_shards(tmp_path):
    from spicy_regs.etl_receipts import RECEIPT_ROW_GROUP

    shards = []
    for number, count in enumerate((2001, 2002)):
        path = tmp_path / f"part{number}.parquet"
        pq.write_table(pa.Table.from_pylist([{"attempt_id": str(i)} for i in range(count)], schema=RECEIPT_SCHEMA), path)
        shards.append(path)
    combined = combine_receipts(shards, tmp_path / "combined.parquet")
    with pq.ParquetFile(combined) as parquet:
        sizes = [parquet.metadata.row_group(i).num_rows for i in range(parquet.metadata.num_row_groups)]
    assert sum(sizes) == 4003 and max(sizes) <= RECEIPT_ROW_GROUP
    assert pq.read_table(combined).equals(pa.concat_tables([pq.read_table(path) for path in shards]))


def test_receipt_groups_end_at_the_byte_budget_before_the_row_cap(tmp_path, monkeypatch):
    from spicy_regs import etl_receipts

    def rows(start, width):
        return pa.Table.from_pylist([{"attempt_id": str(i), "diagnostic_json": "x" * width}
                                     for i in range(start, start + 100)], schema=RECEIPT_SCHEMA)

    monkeypatch.setattr(etl_receipts, "RECEIPT_ROW_GROUP", 300)
    monkeypatch.setattr(etl_receipts, "RECEIPT_ROW_GROUP_BYTES", 200_000)
    path = tmp_path / "receipts.parquet"
    written = []
    with etl_receipts.open_receipt_writer(path) as writer:
        for start in range(0, 1000, 100):  # wide rows: ~5 KB each, so ~40 rows reach the budget
            written.append(rows(start, 5000))
            writer.write_table(written[-1])
        for start in range(1000, 1700, 100):  # narrow rows: the row cap ends the group first
            written.append(rows(start, 10))
            writer.write_table(written[-1])
    with pq.ParquetFile(path) as parquet:
        groups = [parquet.read_row_group(i) for i in range(parquet.metadata.num_row_groups)]
    assert all(group.nbytes <= 200_000 and group.num_rows <= 300 for group in groups)
    assert max(group.num_rows for group in groups) == 300  # narrow rows still fill whole row-cap groups
    assert pa.concat_tables(groups).equals(pa.concat_tables(written))


@pytest.mark.parametrize("uri", ["/home/runner/work/x/.builds/a/input.parquet", "/Users/someone/Work/input.parquet",
                                 "./relative/input.parquet", "~/input.parquet", "file:///tmp/input.parquet",
                                 r"C:\private\input.parquet", "FILE:///x"])
def test_a_new_receipt_refuses_a_local_witness_path(policy, row, context, uri):
    from spicy_regs.etl_receipts import local_witness_uri

    assert local_witness_uri(uri)
    local = replace(context, witnesses=[{**context.witnesses[0], "source_uri": uri}])
    with pytest.raises(ValueError, match="local filesystem path"):
        split_record(policy, row, local)
    with pytest.raises(ValueError, match="local filesystem path"):
        failure_receipt(policy, local, outcome="error", raw_fields={})


@pytest.mark.parametrize("uri", [None, "https://example.test/a", "receipt-processing:" + "b" * 64, "sha256:" + "c" * 64,
                                 "b" * 64, "generations/court-opinions/abc/court_opinions.parquet", "held:fixture"])
def test_addresses_and_digests_are_not_local_paths(uri):
    from spicy_regs.etl_receipts import local_witness_uri

    assert not local_witness_uri(uri)


def test_inheriting_or_retiring_a_path_bearing_receipt_drops_only_the_path(policy, row, context):
    """Already published receipts keep their bytes; a new receipt never re-publishes their machine paths."""
    from spicy_regs.etl_receipts import _digest, inherit_receipt, retire_receipt

    _, legacy = split_record(policy, row, context)
    legacy = dict(legacy, witnesses=[{**w, "source_uri": "/home/runner/work/old.parquet"} for w in legacy["witnesses"]])
    legacy["receipt_id"] = _digest({k: v for k, v in legacy.items() if k != "receipt_id"})
    _, current = split_record(policy, row, inherit_receipt(replace(context, generation_id="build-2"), legacy))
    retired = retire_receipt(legacy, generation_id="build-3", reason="explicit deletion")
    for receipt in (current, retired):
        assert all(w["source_uri"] is None or not w["source_uri"].startswith("/") for w in receipt["witnesses"])
        assert {w["sha256"] for w in legacy["witnesses"]} <= {w["sha256"] for w in receipt["witnesses"]}
