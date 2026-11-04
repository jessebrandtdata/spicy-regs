"""Immutable history uses exact occurrence matching and only direct predecessor references."""

from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_SCHEMA,
    decode_exact_json,
    exact_json,
    split_record,
    failure_receipt,
    observation_receipt,
    _digest,
)
from spicy_regs.receipt_history import RefusalIdentity, carry_receipt_history


@pytest.fixture
def policy():
    return DatasetPolicy(
        "example",
        pa.schema([("id", pa.string()), ("sub", pa.string()), ("text", pa.string())]),
        ("id", "sub"),
        ("raw",),
        nullable_identity_fields=("sub",),
    )


@pytest.fixture
def context():
    return ReceiptContext(
        "old",
        "attempt",
        "test",
        [{"source_id": "test", "source_uri": "https://source.test", "sha256": "a" * 64, "locator": "/"}],
    )


def write(path, rows):
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path, row_group_size=2)
    return path


def accepted(policy, context, identity="1", text="same"):
    return split_record(policy, {"id": identity, "sub": None, "text": text, "raw": "a\x1b\x00"}, context)[1]


def test_all_outcomes_fifo_reorder_delete_and_new_occurrence(tmp_path, policy, context):
    rows = [accepted(policy, context), accepted(policy, replace(context, attempt_id="deleted"), "deleted")]
    for outcome in ("rejected", "refused", "error", "observed"):
        for ordinal in range(2):
            ctx = replace(context, attempt_id=f"{outcome}-{ordinal}", diagnostics={"reason": "same"})
            rows.append(
                observation_receipt(policy, ctx, processing_fields={"raw": "same"})
                if outcome == "observed"
                else failure_receipt(policy, ctx, outcome=outcome, raw_fields={"raw": "same"})
            )
    prior = write(tmp_path / "old.parquet", rows)
    fresh = []
    for old in [rows[0], *rows[2:]]:
        row = dict(old, generation_id="new", attempt_id="new-" + old["attempt_id"])
        row["receipt_id"] = _digest({k: v for k, v in row.items() if k != "receipt_id"})
        fresh.append(row)
    # Reorder outcomes without changing duplicate occurrence order within each match.
    fresh = [fresh[0], *fresh[7:], *fresh[1:7]]
    extra = failure_receipt(
        policy,
        replace(context, generation_id="new", attempt_id="extra", diagnostics={"reason": "same"}),
        outcome="rejected",
        raw_fields={"raw": "same"},
    )
    fresh.append(extra)
    current = write(tmp_path / "current.parquet", fresh)
    result = carry_receipt_history(current, [prior], tmp_path / "result.parquet")
    expected = [rows[0], *rows[8:], *rows[2:8], extra]
    assert pq.read_table(result).to_pylist() == expected
    assert pq.read_table(prior).to_pylist() == rows
    assert pq.read_table(current).to_pylist() == fresh


def test_matching_uses_hash_join_for_mixed_outcomes(tmp_path, policy, context, monkeypatch):
    """Exercise the actual query; outcome-dependent ORs caused a pairwise join."""
    import duckdb

    original_connect = duckdb.connect
    plans = []

    class PlannedConnection:
        def __init__(self, *args, **kwargs):
            self.connection = original_connect(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.connection.close()

        def execute(self, query, *args, **kwargs):
            if query.startswith("CREATE TABLE matches AS"):
                explained = self.connection.execute("EXPLAIN " + query).fetchone()
                assert explained is not None
                plans.append(explained[1])
            return self.connection.execute(query, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", PlannedConnection)
    old = [accepted(policy, context, str(i)) for i in range(17)]
    old += [
        failure_receipt(policy, context, outcome=outcome, raw_fields={"raw": "same"})
        for outcome in ("refused", "rejected", "error")
    ]
    old.append(observation_receipt(policy, context, processing_fields={"raw": "same"}))
    fresh = [dict(row, generation_id="new") for row in reversed(old)]
    result = carry_receipt_history(
        write(tmp_path / "current.parquet", fresh),
        [write(tmp_path / "prior.parquet", old)],
        tmp_path / "result.parquet",
    )
    assert pq.read_table(result).to_pylist() == list(reversed(old))
    assert len(plans) == 1
    assert "HASH_JOIN" in plans[0]
    assert "_NL_JOIN" not in plans[0]
    assert "CROSS_PRODUCT" not in plans[0]


def test_wide_accepted_history_spills_without_changing_ignored_diagnostics(tmp_path, policy, context, monkeypatch):
    """Accepted diagnostics exceeded the raw-check join's memory, despite being ignored."""
    import duckdb

    original_connect = duckdb.connect

    class LimitedConnection:
        def __init__(self, *args, **kwargs):
            self.connection = original_connect(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.connection.close()

        def execute(self, query, *args, **kwargs):
            if query == "SET memory_limit='1GB'":
                query = "SET memory_limit='64MB'"
            elif query == "SET threads=4":
                query = "SET threads=1"
            return self.connection.execute(query, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", LimitedConnection)
    rows = [accepted(policy, context, str(i)) for i in range(32768)]
    for i, row in enumerate(rows):
        row["diagnostic_json"] = exact_json({"legacy": f"{i}:" + "0123456789abcdef" * 256})
        row["receipt_id"] = _digest({k: v for k, v in row.items() if k != "receipt_id"})
    prior = tmp_path / "prior.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), prior, row_group_size=128)
    current = tmp_path / "current.parquet"
    fresh = [dict(row, generation_id="new", diagnostic_json="{}") for row in reversed(rows)]
    pq.write_table(pa.Table.from_pylist(fresh, schema=RECEIPT_SCHEMA), current, row_group_size=128)
    result = carry_receipt_history(current, [prior], tmp_path / "result.parquet")
    assert pq.read_table(result).equals(pa.Table.from_pylist(list(reversed(rows)), schema=RECEIPT_SCHEMA))


def test_nonaccepted_identity_is_not_a_matching_key(tmp_path, policy, context):
    old = [
        failure_receipt(
            policy,
            replace(context, attempt_id=str(i)),
            outcome="refused",
            raw_fields={"raw": "same"},
            identity={"id": str(i), "sub": None},
        )
        for i in range(2)
    ]
    fresh = [
        failure_receipt(
            policy,
            replace(context, generation_id="new", attempt_id=str(i)),
            outcome="refused",
            raw_fields={"raw": "same"},
            identity={"id": str(i), "sub": "different"},
        )
        for i in range(2, 4)
    ]
    result = carry_receipt_history(
        write(tmp_path / "current.parquet", fresh),
        [write(tmp_path / "prior.parquet", old)],
        tmp_path / "result.parquet",
    )
    assert pq.read_table(result).to_pylist() == old


@pytest.mark.parametrize(
    ("outcome", "null_field"),
    [("accepted", "record_id"), ("accepted", "subject_version"), ("refused", "diagnostic_json")],
)
def test_null_matching_key_does_not_authorize_prior_substitution(tmp_path, policy, context, outcome, null_field):
    """The lower-level matcher must not treat missing meaningful keys as equal."""
    old = (
        accepted(policy, context)
        if outcome == "accepted"
        else failure_receipt(policy, context, outcome=outcome, raw_fields={"raw": "same"})
    )
    old[null_field] = None
    fresh = dict(old, generation_id="new")
    result = carry_receipt_history(
        write(tmp_path / "current.parquet", [fresh]),
        [write(tmp_path / "prior.parquet", [old])],
        tmp_path / "result.parquet",
    )
    expected = dict(fresh)
    if null_field == "subject_version":
        # A known record still links to its predecessor, without substituting
        # the old receipt for the current missing subject-version observation.
        expected["diagnostic_json"] = exact_json(
            {
                "prior_receipt": {
                    "receipt_id": old["receipt_id"],
                    "generation_id": old["generation_id"],
                    "processing_sha256": _digest(decode_exact_json(old["processing_json"])),
                }
            }
        )
        expected["receipt_id"] = _digest({k: v for k, v in expected.items() if k != "receipt_id"})
    # Missing diagnostics never authorize substitution. The unresolved old refusal
    # still remains separately; admission, not this matcher, rejects its bad schema.
    assert pq.read_table(result).to_pylist() == [expected, *([old] if outcome == "refused" else [])]


def test_changed_row_direct_predecessor_does_not_copy_history_or_witnesses(tmp_path, policy, context):
    first = accepted(policy, context)
    current = write(
        tmp_path / "current.parquet", [accepted(policy, replace(context, generation_id="new"), text="changed")]
    )
    prior = write(tmp_path / "prior.parquet", [first])
    second = pq.read_table(carry_receipt_history(current, [prior], tmp_path / "second.parquet")).to_pylist()[0]
    assert decode_exact_json(second["diagnostic_json"]) == {
        "prior_receipt": {
            "receipt_id": first["receipt_id"],
            "generation_id": "old",
            "processing_sha256": _digest(decode_exact_json(first["processing_json"])),
        }
    }
    assert second["witnesses"] == pq.read_table(current).to_pylist()[0]["witnesses"]
    third_raw = accepted(policy, replace(context, generation_id="third"), text="again")
    current = write(tmp_path / "third-raw.parquet", [third_raw])
    third = pq.read_table(
        carry_receipt_history(current, [tmp_path / "second.parquet"], tmp_path / "third.parquet")
    ).to_pylist()[0]
    diagnostics = decode_exact_json(third["diagnostic_json"])
    assert set(diagnostics) == {"prior_receipt"}
    assert diagnostics["prior_receipt"]["receipt_id"] == second["receipt_id"]
    assert len(exact_json(diagnostics)) < 400


def test_nonaccepted_diagnostic_change_is_new_attempt(tmp_path, policy, context):
    old = failure_receipt(policy, context, outcome="refused", raw_fields={"raw": "same"})
    fresh = failure_receipt(
        policy,
        replace(context, generation_id="new", diagnostics={"new": True}),
        outcome="refused",
        raw_fields={"raw": "same"},
    )
    result = carry_receipt_history(
        write(tmp_path / "new.parquet", [fresh]), [write(tmp_path / "old.parquet", [old])], tmp_path / "result.parquet"
    )
    assert pq.read_table(result).to_pylist() == [fresh, old]
    again = carry_receipt_history(tmp_path / "new.parquet", [result], tmp_path / "again.parquet")
    assert pq.read_table(again).to_pylist() == [fresh, old]


def test_empty_and_partitioned_prior(tmp_path, policy, context):
    one, two = accepted(policy, context, "1"), accepted(policy, context, "2")
    prior = [write(tmp_path / "part0.parquet", [one]), write(tmp_path / "part1.parquet", [two])]
    current = write(tmp_path / "current.parquet", [two, one])
    assert pq.read_table(carry_receipt_history(current, prior, tmp_path / "result.parquet")).to_pylist() == [two, one]
    empty = write(tmp_path / "empty.parquet", [])
    assert pq.read_table(carry_receipt_history(empty, prior, tmp_path / "gone.parquet")).num_rows == 0
    with pytest.raises(ValueError, match="Conflicting selected prior"):
        carry_receipt_history(current, [prior[0], prior[0]], tmp_path / "refused.parquet")
    assert not (tmp_path / "refused.parquet").exists()


@pytest.mark.parametrize("outcome", ["accepted", "refused"])
def test_candidate_collision_refuses_before_replacing_destination(tmp_path, policy, context, monkeypatch, outcome):
    from spicy_regs import receipt_history

    if outcome == "accepted":
        old = split_record(policy, {"id": "1", "sub": None, "text": "same", "raw": "old"}, context)[1]
        fresh = split_record(
            policy, {"id": "1", "sub": None, "text": "same", "raw": "new"}, replace(context, generation_id="new")
        )[1]
    else:
        old = failure_receipt(policy, context, outcome=outcome, raw_fields={"raw": "same"})
        fresh = failure_receipt(
            policy,
            replace(context, generation_id="new", diagnostics={"different": True}),
            outcome=outcome,
            raw_fields={"raw": "same"},
        )
    prior = write(tmp_path / "prior.parquet", [old])
    current = write(tmp_path / "current.parquet", [fresh])
    destination = write(tmp_path / "destination.parquet", [old])
    before = destination.read_bytes()
    monkeypatch.setattr(receipt_history, "_processing_key", lambda column: "unhex('00')")
    with pytest.raises(ValueError, match="candidate keys differ from exact values"):
        carry_receipt_history(current, [prior], destination)
    assert destination.read_bytes() == before
    assert pq.read_table(current).to_pylist() == [fresh]
    assert not list(tmp_path.glob("receipt-history-*"))


def test_valid_new_attempts_preserve_fifo_across_prior_files_and_output_batches(tmp_path, policy, context):
    old = [accepted(policy, context, str(i), text="value-" + str(i)) for i in range(2011)]
    old += [
        failure_receipt(
            policy, replace(context, attempt_id=f"refused-{i}"), outcome="refused", raw_fields={"raw": "same"}
        )
        for i in range(7)
    ]
    prior = []
    for number, rows in enumerate((old[:1000], old[1000:2014], old[2014:])):
        path = tmp_path / f"prior-{number}.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path, row_group_size=127)
        prior.append(path)
    new_context = replace(
        context, generation_id="new", attempt_id="new", witnesses=[dict(context.witnesses[0], sha256="b" * 64)]
    )
    fresh = [accepted(policy, new_context, str(i), text="value-" + str(i)) for i in reversed(range(2011))]
    fresh += [
        failure_receipt(
            policy, replace(new_context, attempt_id=f"new-refused-{i}"), outcome="refused", raw_fields={"raw": "same"}
        )
        for i in range(8)
    ]
    current = tmp_path / "current.parquet"
    pq.write_table(pa.Table.from_pylist(fresh, schema=RECEIPT_SCHEMA), current, row_group_size=113)
    before = current.read_bytes()
    result = carry_receipt_history(current, prior, current)
    assert pq.read_table(result).to_pylist() == [*reversed(old[:2011]), *old[2011:], fresh[-1]]
    assert current.read_bytes() != before
    assert not list(tmp_path.glob("receipt-history-*"))


def test_sparse_matches_and_predecessors_across_files_and_batches(tmp_path, policy, context):
    old = [accepted(policy, context, str(i)) for i in range(2053)]
    prior = [
        write(tmp_path / "part0.parquet", old[:1003]),
        write(tmp_path / "empty.parquet", []),
        write(tmp_path / "part1.parquet", old[1003:]),
    ]
    fresh, expected = [], []
    for i in reversed(range(2053)):
        identity = "new-" + str(i) if i % 3 == 2 else str(i)
        row = accepted(
            policy, replace(context, generation_id="new"), identity, text="changed" if i % 3 == 1 else "same"
        )
        fresh.append(row)
        if i % 3 == 0:
            expected.append(old[i])
        elif i % 3 == 1:
            row = dict(
                row,
                diagnostic_json=exact_json(
                    {
                        "prior_receipt": {
                            "receipt_id": old[i]["receipt_id"],
                            "generation_id": old[i]["generation_id"],
                            "processing_sha256": _digest(decode_exact_json(old[i]["processing_json"])),
                        }
                    }
                ),
            )
            row["receipt_id"] = _digest({k: v for k, v in row.items() if k != "receipt_id"})
            expected.append(row)
        else:
            expected.append(row)
    result = carry_receipt_history(write(tmp_path / "current.parquet", fresh), prior, tmp_path / "result.parquet")
    assert pq.read_table(result).to_pylist() == expected


def test_collision_after_first_output_batch_preserves_destination(tmp_path, policy, context, monkeypatch):
    from spicy_regs import receipt_history

    old = [accepted(policy, context, str(i)) for i in range(2003)]
    fresh = [dict(row, generation_id="new") for row in old]
    fresh[-1]["processing_json"] = exact_json({"raw": "different"})
    prior = write(tmp_path / "prior.parquet", old)
    current = write(tmp_path / "current.parquet", fresh)
    destination = write(tmp_path / "destination.parquet", old)
    before = destination.read_bytes()
    original_writer = pq.ParquetWriter
    written = []

    class RecordingWriter:
        def __init__(self, *args, **kwargs):
            self.writer = original_writer(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.writer.close()

        def write_table(self, table):
            self.writer.write_table(table)
            written.append(table.num_rows)

    monkeypatch.setattr(pq, "ParquetWriter", RecordingWriter)
    monkeypatch.setattr(receipt_history, "_processing_key", lambda column: "unhex('00')")
    with pytest.raises(ValueError, match="candidate keys differ from exact values"):
        carry_receipt_history(current, [prior], destination)
    assert written == [2000]
    assert destination.read_bytes() == before
    assert not list(tmp_path.glob("receipt-history-*"))


@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_partial_write_or_interruption_preserves_in_place_current(tmp_path, policy, context, monkeypatch, error_type):
    current = write(tmp_path / "current.parquet", [accepted(policy, replace(context, generation_id="new"))])
    prior = write(tmp_path / "prior.parquet", [accepted(policy, context)])
    before = current.read_bytes()

    class FailingWriter:
        def __init__(self, path, *args, **kwargs):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def write_table(self, table):
            self.path.write_bytes(b"partial owned output")
            raise error_type("injected write failure")

    monkeypatch.setattr(pq, "ParquetWriter", FailingWriter)
    with pytest.raises(error_type):
        carry_receipt_history(current, [prior], current)
    assert current.read_bytes() == before
    assert not list(tmp_path.glob("receipt-history-*"))


def test_absent_prior_preserves_every_current_outcome(tmp_path, policy, context):
    rows = [accepted(policy, context)]
    rows += [
        failure_receipt(policy, context, outcome=outcome, raw_fields={"raw": outcome})
        for outcome in ("rejected", "refused", "error")
    ]
    rows.append(observation_receipt(policy, context, processing_fields={"raw": "observed"}))
    current = write(tmp_path / "current.parquet", rows)
    assert pq.read_table(carry_receipt_history(current, [], tmp_path / "result.parquet")).to_pylist() == rows


def test_wide_changed_attempt_keeps_current_witnesses_and_only_direct_predecessor(tmp_path, policy, context):
    old = split_record(policy, {"id": "1", "sub": None, "text": "same", "raw": "a" * 131072}, context)[1]
    new_context = replace(
        context,
        generation_id="new",
        attempt_id="new",
        witnesses=[dict(context.witnesses[0], sha256="b" * 64)],
        diagnostics={
            "current": "kept",
            "prior_receipts": [old],
            "retained_processing": {"old": True},
            "prior_receipt": {"stale": True},
        },
    )
    fresh = split_record(policy, {"id": "1", "sub": None, "text": "same", "raw": "b" * 131072}, new_context)[1]
    result = carry_receipt_history(
        write(tmp_path / "current.parquet", [fresh]),
        [write(tmp_path / "prior.parquet", [old])],
        tmp_path / "result.parquet",
    )
    [actual] = pq.read_table(result).to_pylist()
    expected = dict(
        fresh,
        diagnostic_json=exact_json(
            {
                "current": "kept",
                "prior_receipt": {
                    "receipt_id": old["receipt_id"],
                    "generation_id": old["generation_id"],
                    "processing_sha256": _digest(decode_exact_json(old["processing_json"])),
                },
            }
        ),
    )
    expected["receipt_id"] = _digest({k: v for k, v in expected.items() if k != "receipt_id"})
    assert actual == expected


def test_unanswered_refusals_survive_empty_nights_without_repeating_observations(tmp_path, policy, context):
    refused = [failure_receipt(policy, replace(context, attempt_id=str(i)), outcome="refused",
                               raw_fields={"raw": {"id": "unanswered"}}) for i in range(2)]
    observation = observation_receipt(policy, context, processing_fields={"raw": "empty-file"})
    prior = [write(tmp_path / "part0.parquet", [refused[0], observation]),
             write(tmp_path / "part1.parquet", [refused[1]])]
    empty = write(tmp_path / "empty.parquet", [])
    first = carry_receipt_history(empty, prior, tmp_path / "night1.parquet")
    assert pq.read_table(first).to_pylist() == refused
    second = carry_receipt_history(empty, [first], tmp_path / "night2.parquet")
    assert pq.read_table(second).to_pylist() == refused
    # One repeated attempt matches FIFO; the other old occurrence remains once.
    repeated = failure_receipt(policy, replace(context, generation_id="new"), outcome="refused",
                               raw_fields={"raw": {"id": "unanswered"}})
    third = carry_receipt_history(write(tmp_path / "repeated.parquet", [repeated]), [second],
                                  tmp_path / "night3.parquet")
    assert pq.read_table(third).to_pylist() == refused


def test_success_resolves_declared_identity_only_in_its_dataset(tmp_path, policy, context):
    refused = failure_receipt(policy, context, outcome="refused", raw_fields={"raw": "bad"},
                              identity={"id": "1", "sub": None})
    other = dict(refused, dataset="other")
    current = write(tmp_path / "current.parquet", [accepted(policy, replace(context, generation_id="new"))])
    prior = write(tmp_path / "prior.parquet", [refused, other])
    result = carry_receipt_history(current, [prior], tmp_path / "result.parquet")
    assert pq.read_table(result).to_pylist() == [*pq.read_table(current).to_pylist(), other]


@pytest.mark.parametrize("declared", [True, False], ids=["declared", "raw"])
@pytest.mark.parametrize("fresh_outcome", ["unrelated", "refused", "accepted"])
def test_held_acceptance_requires_a_matching_fresh_success(tmp_path, policy, context, declared, fresh_outcome):
    key = {"id": "1", "sub": None}
    held = split_record(policy, key | {"text": "held", "raw": key}, context)[1]
    refused = failure_receipt(policy, context, outcome="refused", raw_fields={"raw": key | {"text": "bad"}},
                              identity=key if declared else None)
    fresh_context = replace(context, generation_id="fresh", attempt_id="download")
    fresh_key = key | {"id": "2"} if fresh_outcome == "unrelated" else key
    fresh = (failure_receipt(policy, fresh_context, outcome="refused", raw_fields={"raw": fresh_key},
                             identity=fresh_key)
             if fresh_outcome == "refused" else
             split_record(policy, fresh_key | {"text": "fresh", "raw": fresh_key}, fresh_context)[1])
    current = write(tmp_path / "current.parquet", [held])
    prior = write(tmp_path / "prior.parquet", [held, refused])
    resolutions = write(tmp_path / "fresh.parquet", [fresh])
    before = {path: path.read_bytes() for path in (current, prior, resolutions)}
    result = carry_receipt_history(
        current, [prior], tmp_path / "result.parquet", resolution_receipts=resolutions,
        refusal_identities={} if declared else {"example": RefusalIdentity(("id", "sub"), "raw")})
    assert pq.read_table(result).to_pylist() == ([held] if fresh_outcome == "accepted" else [held, refused])
    assert all(path.read_bytes() == contents for path, contents in before.items())


@pytest.mark.parametrize("declared", [True, False], ids=["declared", "raw"])
@pytest.mark.parametrize("output_outcome", ["absent", "refused"])
def test_fresh_success_must_also_be_accepted_in_output(tmp_path, policy, context, declared, output_outcome):
    key = {"id": "1", "sub": None}
    refused = failure_receipt(policy, context, outcome="refused", raw_fields={"raw": key},
                              identity=key if declared else None)
    fresh_context = replace(context, generation_id="fresh", attempt_id="download")
    fresh = split_record(policy, key | {"text": "fresh", "raw": key}, fresh_context)[1]
    current = ([] if output_outcome == "absent" else
               [failure_receipt(policy, fresh_context, outcome="refused", raw_fields={"raw": key | {"retry": True}},
                                identity=key)])
    result = carry_receipt_history(
        write(tmp_path / "current.parquet", current), [write(tmp_path / "prior.parquet", [refused])],
        tmp_path / "result.parquet", resolution_receipts=write(tmp_path / "fresh.parquet", [fresh]),
        refusal_identities={} if declared else {"example": RefusalIdentity(("id", "sub"), "raw")})
    assert pq.read_table(result).to_pylist() == [*current, refused]


@pytest.mark.parametrize("raw", [
    None, "unknown", ["1"],
    {"id": "1", "sub": None},
    {"id": 1, "sub": None},
    {"id": "1"},
    {"id": "2", "sub": None},
    {"id": "1", "sub": ""},
])
@pytest.mark.parametrize("accepted_field", ["raw", None])
def test_raw_resolution_requires_all_exact_typed_natural_key_values(tmp_path, policy, context, raw, accepted_field):
    old = failure_receipt(policy, context, outcome="refused", raw_fields={"raw": raw})
    fresh = split_record(policy, {"id": "1", "sub": None, "text": "fixed",
                                  "raw": {"id": "1", "sub": None, "payload": "repaired"}},
                         replace(context, generation_id="new"))[1]
    result = carry_receipt_history(
        write(tmp_path / "current.parquet", [fresh]), [write(tmp_path / "prior.parquet", [old])],
        tmp_path / "result.parquet", refusal_identities={
            "example": RefusalIdentity(("id", "sub"), accepted_field, "raw")})
    assert pq.read_table(result).to_pylist() == ([fresh] if raw == {"id": "1", "sub": None} else [fresh, old])


def test_raw_resolution_is_explicit_and_supports_different_input_containers(tmp_path, context):
    policy = DatasetPolicy("example", pa.schema([("id", pa.string())]), ("id",), ("original", "failed"))
    key = "quote' and escape\x1b and unicode \u03bb"
    old = failure_receipt(policy, context, outcome="refused", raw_fields={"failed": {"id": key, "data": "bad"}})
    fresh = split_record(policy, {"id": key, "original": {"id": key, "data": "good"}},
                         replace(context, generation_id="new"))[1]
    current, prior = write(tmp_path / "current.parquet", [fresh]), write(tmp_path / "prior.parquet", [old])
    unresolved = carry_receipt_history(current, [prior], tmp_path / "unresolved.parquet")
    assert pq.read_table(unresolved).to_pylist() == [fresh, old]
    result = carry_receipt_history(current, [prior], tmp_path / "resolved.parquet",
                                  refusal_identities={"example": RefusalIdentity(("id",), "original", "failed")})
    assert pq.read_table(result).to_pylist() == [fresh]


def test_family_derivation_visits_only_unmatched_refusals_and_matches_exact_tags(tmp_path, policy, context):
    key = "derived-\x1b-λ"
    prior_rows = [failure_receipt(policy, context, outcome="refused", raw_fields={"raw": {"source": value}})
                  for value in ("repeated", "resolved", "invalid")]
    fresh_context = replace(context, generation_id="new")
    fresh = [accepted(policy, fresh_context, key),
             failure_receipt(policy, fresh_context, outcome="refused", raw_fields={"raw": {"source": "repeated"}}),
             observation_receipt(policy, fresh_context, processing_fields={"raw": {"source": "observed"}})]
    seen = []

    def derive(raw):
        seen.append(raw["source"])
        if raw["source"] == "invalid":
            raise ValueError("No proven key")
        return {"id": key, "sub": None}

    current, prior = write(tmp_path / "current.parquet", fresh), write(tmp_path / "prior.parquet", prior_rows)
    before = prior.read_bytes()
    result = carry_receipt_history(current, [prior], tmp_path / "result.parquet",
                                  refusal_identities={"example": RefusalIdentity(("id", "sub"), None, "raw", derive)})
    assert sorted(seen) == ["invalid", "resolved"]
    assert pq.read_table(result).to_pylist() == [fresh[0], prior_rows[0], fresh[2], prior_rows[2]]
    assert prior.read_bytes() == before


def test_refusal_derivation_operational_failure_preserves_destination(tmp_path, policy, context):
    import duckdb

    old = failure_receipt(policy, context, outcome="refused", raw_fields={"raw": {"source": "bad"}})
    current = write(tmp_path / "current.parquet", [accepted(policy, replace(context, generation_id="new"))])
    prior = write(tmp_path / "prior.parquet", [old])
    destination = write(tmp_path / "destination.parquet", [old])
    before = destination.read_bytes()

    def failed(raw):
        raise OSError("injected operational failure")

    with pytest.raises(duckdb.Error, match="injected operational failure"):
        carry_receipt_history(current, [prior], destination,
                              refusal_identities={"example": RefusalIdentity(("id", "sub"), None, "raw", failed)})
    assert destination.read_bytes() == before


@pytest.mark.parametrize("threads", [None, 2])
def test_history_settings_receipt_matches_the_actual_query(tmp_path, policy, context, monkeypatch, threads):
    import duckdb

    real_connect = duckdb.connect
    effective = []

    class ObservedConnection:
        def __init__(self, *args, **kwargs):
            self.connection = real_connect(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.connection.close()

        def execute(self, query, *args, **kwargs):
            result = self.connection.execute(query, *args, **kwargs)
            if query.startswith("CREATE TABLE matches AS"):
                effective.append(self.connection.execute(
                    "SELECT current_setting('threads'), current_setting('memory_limit'), "
                    "current_setting('max_temp_directory_size')"
                ).fetchone())
            return result

    monkeypatch.setattr(duckdb, "connect", ObservedConnection)
    rows = [accepted(policy, context)]
    prior = write(tmp_path / "prior.parquet", rows)
    current = write(tmp_path / "current.parquet", [dict(rows[0], generation_id="new")])
    originals = [path.read_bytes() for path in (prior, current)]
    reported = []
    kwargs = {} if threads is None else {"threads": threads}
    result = carry_receipt_history(current, [prior], tmp_path / "result.parquet",
                                   report_settings=reported.append, **kwargs)
    assert pq.read_table(result).to_pylist() == rows
    assert [path.read_bytes() for path in (prior, current)] == originals
    assert len(effective) == len(reported) == 1
    assert effective[0][0] == (1 if threads is None else threads)
    assert reported == [dict(zip(
        ["historyQueryThreads", "historyQueryMemory", "historyQuerySpill"], effective[0]
    ))]


@pytest.mark.parametrize("threads", [0, -1, True, 1.5, "2", 2**63])
def test_invalid_history_threads_refuse_before_any_file_or_connection(tmp_path, monkeypatch, threads):
    import duckdb

    def unexpected_connection(*args, **kwargs):
        raise AssertionError("invalid settings must refuse before opening DuckDB")

    monkeypatch.setattr(duckdb, "connect", unexpected_connection)
    destination = tmp_path / "not-created" / "result.parquet"
    reported = []
    with pytest.raises(ValueError, match="History query threads"):
        carry_receipt_history(tmp_path / "missing-current", [], destination,
                              threads=threads, report_settings=reported.append)
    assert not destination.parent.exists()
    assert not reported
