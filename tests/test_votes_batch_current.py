"""A flat Congress dataset's bulk bundle and restored table equal the row writer's, row for row."""

import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal
import pytest
from spicy_regs import etl_bulk

from spicy_regs import congress_bulk
from spicy_regs.congress_receipts import PROCESSOR, CongressInput, policy
from spicy_regs.votes_batch_receipts import restore_processing_input, write_congress_dataset
from spicy_regs.congress_subjects import INPUT_COLUMNS, map_record
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, decode_exact_json, exact_json, failure_receipt

GENERATION = "generation-1"
WITNESS = {"source_id": "capture", "source_uri": "https://example.test/votes", "sha256": "ab" * 32, "locator": "page 1"}


def _member_votes(count: int) -> list[dict]:
    rows = [
        {name: None for name in INPUT_COLUMNS["member_votes"]}
        | {"vote_id": f"101-house-1-{i // 400}", "member_key": f"name:Member {i}", "congress": "101", "chamber": "house",
           "member_name": ["Smith (OR)", "O'Brien", 'Quote "Q"', "Peña"][i % 4], "position": "Yea" if i % 3 else None}
        for i in range(count)
    ]
    for index in range(7, count, 900):
        rows[index]["member_name"] = "Escape \x1b here"  # SQL spells this escape differently: the row code's
    for index in range(11, count, 1300):
        rows[index]["vote_id"] = None                    # no identity: the row code refuses it
    rows[-1] = {name: None for name in INPUT_COLUMNS["member_votes"]} | {"vote_id": "v", "member_key": "k"}
    return rows


def _member_vote_terms(count: int) -> list[dict]:
    rows = []
    for i in range(count):
        matched = i % 3 != 0
        rows.append({"vote_id": f"101-house-1-{i // 400}", "member_key": f"name:Member {i}", "chamber": "house",
                     "bioguide_id": f"B{i:06d}" if matched else None, "vote_day": "1990-01-23",
                     "term_match": "half_open" if matched else "unresolved_member", "term_index": str(i % 7) if matched else None,
                     "term_start": "1989-01-03" if matched else None, "term_end": "1991-01-03" if matched else None})
    rows[4]["term_index"] = "007"       # digits with leading zeros read as 7
    for index in range(5, count, 600):
        rows[index]["term_index"] = "x1"    # not an integer: the row code refuses it
    rows[8]["term_index"] = "9" * 19    # past int64: the row code refuses it
    for index in range(10, count, 750):
        rows[index]["chamber"] = "house\x0b"  # the row code's spelling
    rows[13]["term_match"] = None       # no term and no reason: a subject with a NULL term
    rows[13]["term_index"] = None
    return rows


def _source(tmp_path, dataset, rows, metadata=None):
    schema = pa.schema([(name, pa.string()) for name in INPUT_COLUMNS[dataset]], metadata=metadata)
    path = tmp_path / f"{dataset}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def _taken(monkeypatch, name: str) -> list:
    """Record each call of a bulk function, so an equal result cannot come from the row code running twice."""
    calls, real = [], getattr(congress_bulk, name)

    def spy(*args, **kwargs):
        calls.append(name)
        return real(*args, **kwargs)

    monkeypatch.setattr(congress_bulk, name, spy)
    return calls


def _both(tmp_path, monkeypatch, dataset, rows, **kwargs):
    """The row writer's bundle and the bulk writer's, from one source file."""
    source = _source(tmp_path, dataset, rows, kwargs.pop("metadata", None))
    taken = _taken(monkeypatch, "write_bundle")
    bulk = write_congress_dataset(source, tmp_path / "bulk", dataset=dataset, generation_id=GENERATION, **kwargs)
    assert taken == ["write_bundle"]
    row = write_congress_dataset(source, tmp_path / "row", dataset=dataset, generation_id=GENERATION, bulk=False, **kwargs)
    assert taken == ["write_bundle"]
    return source, row, bulk


def _restored(tmp_path, monkeypatch, dataset, subject, receipts):
    """The row restore's table and the bulk restore's, from one bundle; or the error each raised."""
    def run(name, **kwargs):
        try:
            return pq.read_table(restore_processing_input(
                subject, receipts, tmp_path / name, dataset=dataset, generation_id=GENERATION, **kwargs))
        except Exception as error:  # the two paths are held to the same refusal, whatever it is
            return type(error)

    taken = _taken(monkeypatch, "restore_input")
    bulk = run("bulk.parquet")
    assert taken == ["restore_input"]
    row = run("row.parquet", bulk=False)
    assert taken == ["restore_input"]
    return row, bulk


@pytest.fixture(params=[100_000, 700], ids=["one batch", "several batches"])
def batch(request, monkeypatch) -> int:
    """A scan that spans several batches, each with rows for the row code, as every real table does."""
    monkeypatch.setattr(congress_bulk, "_BATCH", request.param)
    return request.param


@pytest.mark.parametrize(("dataset", "build"), [("member_votes", _member_votes), ("member_vote_terms", _member_vote_terms)])
def test_bulk_bundle_equals_the_row_writers(tmp_path, monkeypatch, batch, dataset, build) -> None:
    rows = build(4500)
    source, (row_subject, row_receipts), (bulk_subject, bulk_receipts) = _both(
        tmp_path, monkeypatch, dataset, rows, witnesses=[WITNESS], metadata={b"scope": b"101"})
    assert congress_bulk.eligible(dataset, pq.read_schema(source))
    expected = pq.read_table(row_receipts).to_pylist()
    assert len(expected) == len(rows) + 1
    assert pq.read_table(bulk_receipts).to_pylist() == expected
    routed = congress_bulk.routed_ordinals(source, dataset=dataset, policy=policy(dataset))
    assert len(routed) > 4500 // 700 and {expected[ordinal + 1]["outcome"] for ordinal in routed} == {"accepted", "refused"}
    assert {receipt["outcome"] for receipt in expected} >= {"accepted", "refused", "observed"}
    assert pq.read_table(bulk_subject).to_pylist() == pq.read_table(row_subject).to_pylist()
    written = pq.ParquetFile(bulk_receipts)
    assert written.schema_arrow.equals(RECEIPT_SCHEMA)
    assert [written.metadata.row_group(i).num_rows for i in range(written.metadata.num_row_groups)] == [2000, 2000, 501]


def test_rows_without_a_subject_are_the_mappers(tmp_path) -> None:
    """NO_SUBJECT restates map_record's rule; hold it to the mapper's answer on every row shape it distinguishes."""
    rows = _member_vote_terms(60)
    source = _source(tmp_path, "member_vote_terms", rows)
    stated = [flag for (flag,) in duckdb.sql(
        f"SELECT ({congress_bulk.NO_SUBJECT['member_vote_terms']}) FROM read_parquet('{source}', file_row_number = true) ORDER BY file_row_number"
    ).fetchall()]
    mapped = []
    for row in rows:
        try:
            mapped.append(map_record("member_vote_terms", row).subject is None)
        except (ValueError, OverflowError):
            mapped.append(False)  # refused, not subjectless: the row code's
    assert stated == mapped and True in stated and False in stated


@pytest.mark.parametrize(("dataset", "build"), [("member_votes", _member_votes), ("member_vote_terms", _member_vote_terms)])
def test_bulk_restore_equals_the_row_restore(tmp_path, monkeypatch, batch, dataset, build) -> None:
    rows = build(4500)
    _, (row_subject, row_receipts), _ = _both(tmp_path, monkeypatch, dataset, rows, metadata={b"scope": b"101"})
    expected, restored = _restored(tmp_path, monkeypatch, dataset, row_subject, row_receipts)
    assert restored.to_pylist() == expected.to_pylist()
    assert restored.schema.equals(expected.schema, check_metadata=True)
    refused = sum(receipt["outcome"] == "refused" for receipt in pq.read_table(row_receipts).to_pylist())
    assert refused > 2 and restored.num_rows == len(rows) - refused  # every row but the refused conversions


def test_wide_restore_stays_bounded_and_ordered_across_receipt_batches(tmp_path) -> None:
    """House communications' many retained columns must fit without a whole-file JSON parse and sort."""
    dataset = "house_communications"
    schema = pa.schema([(name, pa.string()) for name in INPUT_COLUMNS[dataset]], metadata={b"window": b"118"})
    fields = [
        {name: (None if column % 7 == 0 else f"{i}:{name}: Peña \"quoted\" \\ path")
         for column, name in enumerate(schema.names)}
        for i in range(4203)
    ]
    processing = [{"entry_kind": "row", "source_fields": row} for row in fields]
    processing[1999]["extra"] = "another writer's retained context"
    processing[4000] = {"entry_kind": "table_metadata"}
    diagnostics = [{} for _ in fields]
    diagnostics[2000] = {"reason": "conversion_refused"}
    datasets = [dataset] * len(fields)
    datasets[4001] = "another_dataset"
    receipts, destination = tmp_path / "receipts.parquet", tmp_path / "restored.parquet"
    pq.write_table(pa.table({"dataset": datasets, "processing_json": [exact_json(row) for row in processing],
                             "diagnostic_json": [exact_json(row) for row in diagnostics]}), receipts, row_group_size=257)
    routed = []

    def row_fields(processing_text, diagnostic_text):
        row, notes = decode_exact_json(processing_text), decode_exact_json(diagnostic_text)
        routed.append((row, notes))
        if row.get("entry_kind") == "row" and notes.get("reason") != "conversion_refused":
            return row["source_fields"]
        return None

    with etl_bulk.bulk_limits(threads=4, memory_limit="256MB"):
        count = congress_bulk.restore_input(receipts, destination, dataset=dataset, schema=schema, row_fields=row_fields)
    expected = pa.Table.from_pylist([row for i, row in enumerate(fields) if i not in {2000, 4000, 4001}], schema=schema)
    assert pq.read_table(destination).equals(expected, check_metadata=True)
    assert count == 3
    assert routed == [(processing[i], diagnostics[i]) for i in (1999, 2000, 4000)]


def test_refresh_carries_current_receipts_identically_in_both_paths(tmp_path, monkeypatch) -> None:
    source, _, (subject, receipts) = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    prior = CongressInput(subject, receipts, GENERATION)
    fresh = write_congress_dataset(source, tmp_path / "next", dataset="member_votes", generation_id="generation-2", prior=prior)
    row = write_congress_dataset(source, tmp_path / "next-row", dataset="member_votes", generation_id="generation-2", prior=prior, bulk=False)
    assert pq.read_table(fresh[1]).equals(pq.read_table(receipts))  # nothing changed: every receipt is the prior's
    assert pq.read_table(fresh[1]).equals(pq.read_table(row[1]))


def test_a_scan_cut_short_is_refused(tmp_path, monkeypatch) -> None:
    """A stream that ends early leaves a bundle consistent with itself; only the file's own count can tell."""
    source, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    real = congress_bulk._rows_in
    monkeypatch.setattr(congress_bulk, "_rows_in", lambda *args: real(*args) + 1)
    with pytest.raises(RuntimeError, match="read 40 of the source's 41 rows"):
        write_congress_dataset(source, tmp_path / "short", dataset="member_votes", generation_id=GENERATION)
    assert not (tmp_path / "short").exists()
    with pytest.raises(RuntimeError, match="read 41 of the file's 42 receipts"):
        restore_processing_input(subject, receipts, tmp_path / "short.parquet", dataset="member_votes", generation_id=GENERATION)
    assert not (tmp_path / "short.parquet").exists()


def test_the_files_own_count_is_of_the_datasets_receipts(tmp_path, monkeypatch) -> None:
    source, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    assert congress_bulk._rows_in(source) == 40
    assert congress_bulk._rows_in(receipts, "member_votes") == 41 and congress_bulk._rows_in(receipts, "member_vote_terms") == 0


def _with_attempts(receipts, attempts) -> None:
    """Append failed attempts, built by the row code, to a written bundle's receipts."""
    selected = policy("member_votes")
    witness = {"source_id": "another-writer", "source_uri": None, "sha256": "cd" * 32, "locator": None, "body_version": None}
    added = [
        failure_receipt(selected, ReceiptContext(GENERATION, f"other:{i}", PROCESSOR, [witness], diagnostics), outcome="rejected", raw_fields=fields)
        for i, (fields, diagnostics) in enumerate(attempts)
    ]
    pq.write_table(pa.Table.from_pylist(pq.read_table(receipts).to_pylist() + added, schema=RECEIPT_SCHEMA), receipts)


def test_restore_of_receipts_another_writer_could_have_written(tmp_path, monkeypatch) -> None:
    """A valid bundle is wider than this writer's: a failed attempt may hold other keys, fewer columns, other reasons."""
    full = {name: None for name in INPUT_COLUMNS["member_votes"]} | {"vote_id": "v-other", "member_key": "k-other", "position": "Nay"}
    _, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    _with_attempts(receipts, [
        ({"chamber": "house", "entry_kind": "row", "source_fields": full | {"member_name": "a key before entry_kind"}}, {}),
        ({"entry_kind": "row", "source_fields": {"vote_id": "v-few", "member_key": "k-few"}}, {}),
        ({"entry_kind": "row", "source_fields": full | {"member_name": "mentions it"}}, {"reason": "other", "note": "conversion_refused"}),
        ({"entry_kind": "row", "source_fields": full | {"member_name": "refused"}}, {"reason": "conversion_refused"}),
        ({"entry_kind": "note", "source_fields": full | {"member_name": "not a row"}}, {}),
        ({"source_fields": full | {"member_name": "no kind"}}, {}),
    ])
    expected, restored = _restored(tmp_path, monkeypatch, "member_votes", subject, receipts)
    assert restored.to_pylist() == expected.to_pylist()
    names = restored["member_name"].to_pylist()
    assert {"a key before entry_kind", "mentions it"} <= set(names) and not {"refused", "not a row", "no kind"} & set(names)
    assert "v-few" in restored["vote_id"].to_pylist()


_FULL = {name: None for name in INPUT_COLUMNS["member_votes"]} | {"vote_id": "v", "member_key": "k"}


@pytest.mark.parametrize("fields", [
    {"vote_id": "v", "member_key": "k", "not_a_column": "x"},
    {"vote_id": "v", "member_key": 7},
    {name: value for name, value in _FULL.items() if name != "position"} | {"not_a_column": "x"},  # as many keys as columns
    _FULL | {"member_key": 7},
    _FULL | {"position": ["Yea"]},
], ids=["extra key", "integer", "renamed key", "integer in a full row", "list in a full row"])
def test_a_retained_row_the_schema_cannot_hold_is_refused_by_both(tmp_path, monkeypatch, fields) -> None:
    _, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    _with_attempts(receipts, [({"entry_kind": "row", "source_fields": fields}, {})])
    expected, restored = _restored(tmp_path, monkeypatch, "member_votes", subject, receipts)
    assert isinstance(expected, type) and restored is expected


def test_the_decode_reads_every_code_point_the_row_code_writes() -> None:
    points = [p for p in range(sys.maxunicode + 1) if not 0xD800 <= p <= 0xDFFF]
    values = ["a" + chr(p) + "b" for p in points] + ["", "\\u001B", "\\\\", '"', "\x00", "\x00\x00x", "null", " x ", None]
    schema = pa.schema([("k", pa.string()), ("n", pa.string())])
    texts = [exact_json({"entry_kind": "row", "source_fields": {"k": value, "n": None}}) for value in values]
    columns, plain = congress_bulk.source_fields_sql(schema)
    con = duckdb.connect()
    con.register("t", pa.table({"want": pa.array(values, type=pa.string()), "processing_json": pa.array(texts, type=pa.string())}))
    counted = con.execute(
        f"SELECT count(*), count(*) FILTER (k IS NOT DISTINCT FROM want AND n IS NULL AND plain) "
        f"FROM (SELECT want, {columns}, ({plain}) AS plain FROM t)"
    ).fetchone()
    assert counted == (len(values), len(values))


@pytest.mark.parametrize("dataset", ["roll_call_votes", "nominations", "bill_family_archives", "public_activity_events", "unknown"])
def test_a_dataset_with_other_rules_stays_with_the_row_code(dataset) -> None:
    names = INPUT_COLUMNS.get(dataset, ["a"])
    assert not congress_bulk.eligible(dataset, pa.schema([(name, pa.string()) for name in names]))


def test_a_typed_or_unknown_source_column_stays_with_the_row_code() -> None:
    names = INPUT_COLUMNS["member_votes"]
    assert congress_bulk.eligible("member_votes", pa.schema([(name, pa.string()) for name in names[:5]]))
    assert not congress_bulk.eligible("member_votes", pa.schema([(names[0], pa.int64())]))
    assert not congress_bulk.eligible("member_votes", pa.schema([("not_declared", pa.string())]))


@pytest.mark.parametrize("dataset", ["member_votes", "member_vote_terms"])
def test_partial_source_uses_the_same_missing_columns(tmp_path, dataset):
    source = tmp_path / "partial.parquet"
    pq.write_table(pa.table({"vote_id": ["v"], "member_key": ["k"]}), source)
    row = write_congress_dataset(source, tmp_path / "row", dataset=dataset, generation_id=GENERATION, bulk=False)
    bulk = write_congress_dataset(source, tmp_path / "bulk", dataset=dataset, generation_id=GENERATION)
    for reference, actual in zip(row, bulk):
        assert pq.read_table(reference).equals(pq.read_table(actual))


@pytest.mark.parametrize("field", ["locator", "source_uri", "source_id", "body_version"])
def test_control_character_in_receipt_context_routes_to_reference(tmp_path, monkeypatch, field):
    witness = {**WITNESS, field: ("https://example.test/" if field == "source_uri" else "") + "context\x1b"}
    _, row, bulk = _both(tmp_path, monkeypatch, "member_votes", _member_votes(5), witnesses=[witness])
    for reference, actual in zip(row, bulk):
        assert pq.read_table(reference).equals(pq.read_table(actual))


def test_selected_prior_batches_exact_current_restore_and_caches_source(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from spicy_regs.congress_receipts import restore_processing_input as public_restore
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    source = _source(tmp_path, "member_votes", _member_votes(5))
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset="member_votes", generation_id=GENERATION)
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    prior = SelectedPriors(tmp_path / "prior")
    from spicy_regs.selected_generations import SelectedInputs

    member_session = SelectedInputs(tmp_path / "member-session", tmp_path / "member-cache")
    checked_members = []

    def member_pin(path):
        checked_members.append(path)
        return member_session.member_pin(path)

    prior.selected = SimpleNamespace(  # ty: ignore[invalid-assignment]
        select=lambda dataset, ranged=False: SimpleNamespace(subjects=(subject,), receipts=receipts, generation_id=GENERATION),
        member_pin=member_pin,
        ranged={},
    )
    taken = _taken(monkeypatch, "restore_input")
    reference = public_restore(subject, receipts, tmp_path / "reference.parquet",
                               dataset="member_votes", generation_id=GENERATION)
    assert taken == []  # The public API still defaults to the row reader.
    restored = prior.get("member_votes")
    assert taken == ["restore_input"]
    assert pq.read_table(restored).equals(pq.read_table(reference), check_metadata=True)
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)
    assert checked_members == [restored]
    original = restored.read_bytes()
    assert prior.get("member_votes") == restored
    assert restored.read_bytes() == original
    assert checked_members == [restored, restored]
    assert taken == ["restore_input"]


def test_unproven_bundle_dispatch_uses_exact_row_validator(tmp_path, monkeypatch):
    from spicy_regs import votes_batch_receipts as congress_receipts, etl_bulk
    from spicy_regs.etl_receipts import DatasetPolicy, write_dataset

    selected = DatasetPolicy("wide_test", pa.schema([("id", pa.string()), ("n", pa.decimal128(12, 8))]), ("id",), ())
    subject, receipts = write_dataset([({"id": "one", "n": Decimal("1.50000000")}, ReceiptContext(GENERATION, "a", "p", [WITNESS]))],
                                      tmp_path / "bundle", selected)
    assert subject is not None
    calls, real = [], congress_receipts.validate_receipt_bundle
    def counted(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)
    monkeypatch.setattr(congress_receipts, "validate_receipt_bundle", counted)
    congress_receipts._admit({selected.dataset: [subject]}, [receipts], [selected], generation_id=GENERATION)
    assert calls == [1]
    with pytest.raises(etl_bulk.NotBulkEligible):
        etl_bulk.validate_bundle({selected.dataset: [subject]}, [receipts], [selected], generation_id=GENERATION)


@pytest.mark.parametrize("damage", ["utf8", "missing", "truncated"])
def test_bulk_source_decode_failure_uses_exact_row_error(tmp_path, damage):
    source = _source(tmp_path, "member_votes", _member_votes(5))
    if damage == "utf8":
        table = pq.read_table(source)
        values = table["vote_id"].combine_chunks()
        data = bytearray(values.buffers()[2].to_pybytes())
        data[0] = 255
        broken = pa.Array.from_buffers(pa.string(), len(values), [values.buffers()[0], values.buffers()[1], pa.py_buffer(data)])
        pq.write_table(table.set_column(table.schema.get_field_index("vote_id"), "vote_id", broken), source)
    elif damage == "missing":
        source.unlink()
    else:
        source.write_bytes(source.read_bytes()[:40])
    errors = []
    for bulk in (False, True):
        destination = tmp_path / ("bulk" if bulk else "row")
        with pytest.raises(Exception) as caught:
            write_congress_dataset(source, destination, dataset="member_votes", generation_id=GENERATION, bulk=bulk)
        errors.append((type(caught.value), str(caught.value)))
        assert not destination.exists()
    if damage == "utf8":
        # Bulk never redoes a failure by row: the invalid input is reported as bulk input data (the row writer's own
        # first decoding error stays what bulk=False reports).
        assert errors[0][0] is UnicodeDecodeError and errors[1][0] is etl_bulk.BulkInputError
        assert "no row fallback" in errors[1][1]
    else:
        assert errors[0] == errors[1]


@pytest.mark.parametrize('dataset,build', [('member_votes', _member_votes), ('member_vote_terms', _member_vote_terms)])
def test_current_public_row_writer_is_independent_and_batching_requires_opt_in(tmp_path, monkeypatch, dataset, build):
    from spicy_regs import congress_receipts as current
    source = _source(tmp_path, dataset, build(31))
    taken = _taken(monkeypatch, 'write_bundle')
    row = current.write_congress_dataset(source, tmp_path / 'current-row', dataset=dataset, generation_id=GENERATION)
    assert not taken
    bulk = current.write_congress_dataset(source, tmp_path / 'explicit-bulk', dataset=dataset,
                                         generation_id=GENERATION, bulk=True)
    assert taken == ['write_bundle']
    for expected, actual in zip(row, bulk, strict=True):
        assert expected is not None and actual is not None
        assert pq.read_table(expected).equals(pq.read_table(actual), check_metadata=True)
    restored_calls = _taken(monkeypatch, 'restore_input')
    row_restore = current.restore_processing_input(*row, tmp_path / 'current-restored.parquet',
                                                   dataset=dataset, generation_id=GENERATION)
    assert not restored_calls
    batch_restore = current.restore_processing_input(*bulk, tmp_path / 'batch-restored.parquet',
                                                     dataset=dataset, generation_id=GENERATION, bulk=True)
    assert restored_calls == ['restore_input']
    assert pq.read_table(row_restore).equals(pq.read_table(batch_restore), check_metadata=True)


def test_current_public_batch_route_refuses_undeclared_dataset_before_output(tmp_path):
    from spicy_regs.congress_receipts import write_congress_dataset as current
    with pytest.raises(ValueError, match='declared Congress dataset'):
        current(tmp_path / 'absent.parquet', tmp_path / 'absent-output', dataset='not-a-dataset',
                generation_id=GENERATION, bulk=True)
    assert not (tmp_path / 'absent-output').exists()


def test_roll_call_public_batch_fallback_matches_current_prior_and_restore(tmp_path):
    from spicy_regs import congress_receipts as current
    source = _source(tmp_path, 'roll_call_votes', [
        {'vote_id': '119-house-1', 'tallies_json': '{"Yea":123}'},
        {'vote_id': '119-house-2', 'tallies_json': 'invalid JSON'},
    ], metadata={b'owner': b'current-roll-calls'})
    row = current.write_congress_dataset(source, tmp_path/'row-roll', dataset='roll_call_votes',
                                         generation_id=GENERATION)
    bulk = current.write_congress_dataset(source, tmp_path/'batch-roll', dataset='roll_call_votes',
                                          generation_id=GENERATION, bulk=True)
    for expected, actual in zip(row, bulk):
        assert expected is not None and actual is not None
        assert pq.read_table(expected).equals(pq.read_table(actual), check_metadata=True)
    assert row[0] is not None and pq.read_metadata(row[0]).num_rows == 1
    outcomes = [receipt['outcome'] for receipt in pq.read_table(row[1]).to_pylist()]
    assert outcomes == ['observed', 'accepted', 'refused']
    prior = CongressInput(row[0], row[1], GENERATION)
    row_next = current.write_congress_dataset(source, tmp_path/'row-roll-next', dataset='roll_call_votes',
                                              generation_id='next', prior=prior)
    batch_next = current.write_congress_dataset(source, tmp_path/'batch-roll-next', dataset='roll_call_votes',
                                                generation_id='next', prior=prior, bulk=True)
    for expected, actual in zip(row_next, batch_next):
        assert expected is not None and actual is not None
        assert pq.read_table(expected).equals(pq.read_table(actual), check_metadata=True)
    assert pq.read_table(batch_next[1]).to_pylist() == pq.read_table(row[1]).to_pylist()  # carried verbatim
    restored = []
    for name, enabled in [('row', False), ('batch', True)]:
        path = current.restore_processing_input(batch_next[0], batch_next[1], tmp_path/(name+'-roll-restore.parquet'),
                                                 dataset='roll_call_votes', generation_id='next', bulk=enabled)
        restored.append(pq.read_table(path))
    assert restored[0].equals(restored[1], check_metadata=True)


@pytest.mark.parametrize('dataset,build,field,values', [
    ('member_votes', _member_votes, 'position', ('Changed source value', 'Changed again')),
    ('member_vote_terms', _member_vote_terms, 'vote_day', ('1990-01-24', '1990-01-25')),
    ('roll_call_votes', None, 'tallies_json', ('{"Yea":99}', '{"Nay":5}'))])
def test_batch_writer_with_a_prior_equals_the_row_writer_carried_over_it(tmp_path, dataset, build, field, values):
    """The oracle: row writer without a prior, then ``carry_receipt_history``, equals the batch writer with it."""
    from spicy_regs import congress_receipts as current
    from spicy_regs.receipt_history import carry_receipt_history
    if build is None:
        rows = [{'vote_id': f'119-house-{i}', 'tallies_json': '{"Yea":%d}' % i} for i in range(30)]
        rows[3]['tallies_json'] = 'invalid JSON'            # refused, in the prior and in the source
    else:
        rows = build(60)
    (tmp_path / 'a').mkdir()
    first = _source(tmp_path / 'a', dataset, rows)
    subject, receipts = current.write_congress_dataset(first, tmp_path / 'prior', dataset=dataset, generation_id=GENERATION)
    prior = CongressInput(subject, receipts, GENERATION)
    changed = [dict(row) for row in rows]
    changed[1][field], changed[2][field] = values  # accepted rows in every dataset's fixture
    del changed[7]                                           # removed
    changed.append(dict(rows[1]) | {'vote_id': '119-house-new'} if build is None else
                   dict(rows[1]) | {'member_key': 'name:New'})  # added
    (tmp_path / 'b').mkdir()
    updated = _source(tmp_path / 'b', dataset, changed)
    fresh = current.write_congress_dataset(updated, tmp_path / 'fresh', dataset=dataset, generation_id='next')
    expected = carry_receipt_history(fresh[1], [receipts], tmp_path / 'expected.parquet')
    row = current.write_congress_dataset(updated, tmp_path / 'row', dataset=dataset, generation_id='next', prior=prior)
    batch = current.write_congress_dataset(updated, tmp_path / 'batch', dataset=dataset, generation_id='next', prior=prior,
                                           bulk=True)
    assert pq.read_table(batch[1]).to_pylist() == pq.read_table(expected).to_pylist()
    for left, right in zip(row, batch, strict=True):
        assert left is not None and right is not None
        assert pq.read_table(left).equals(pq.read_table(right), check_metadata=True)
    old = {r['record_id']: r for r in pq.read_table(receipts).to_pylist() if r['outcome'] == 'accepted'}
    now = [r for r in pq.read_table(batch[1]).to_pylist() if r['outcome'] == 'accepted']
    referenced = [r for r in now if 'prior_receipt' in decode_exact_json(r['diagnostic_json'])]
    assert len(referenced) >= 1 and all(r['generation_id'] == 'next' for r in referenced)
    assert all(decode_exact_json(r['diagnostic_json'])['prior_receipt']['receipt_id'] == old[r['record_id']]['receipt_id']
               for r in referenced)
    assert sum(r == old.get(r['record_id']) for r in now) >= len(now) - 4


def test_a_prior_receipt_with_unverifiable_retained_history_is_refused_by_carry(tmp_path):
    from spicy_regs import congress_receipts as current
    from spicy_regs.etl_receipts import _digest
    source = _source(tmp_path, 'member_votes', _member_votes(6))
    subject, receipts = current.write_congress_dataset(source, tmp_path / 'prior-with-bad-history',
                                                       dataset='member_votes', generation_id=GENERATION)
    values = pq.read_table(receipts).to_pylist()
    broken = next(row for row in values if row['outcome'] == 'accepted')
    diagnostic = decode_exact_json(broken['diagnostic_json'])
    diagnostic['retained_processing'] = {'sha256:' + '0' * 64: {'unverifiable': 'payload'}}
    broken['diagnostic_json'] = exact_json(diagnostic)
    broken['receipt_id'] = _digest({key: value for key, value in broken.items() if key != 'receipt_id'})
    pq.write_table(pa.Table.from_pylist(values, schema=RECEIPT_SCHEMA), receipts)
    prior = CongressInput(subject, receipts, GENERATION)
    for bulk in (False, True):
        with pytest.raises(ValueError, match='Retained processing digest differs'):
            current.write_congress_dataset(source, tmp_path / f'next-{bulk}', dataset='member_votes',
                                           generation_id='next', prior=prior, bulk=bulk, validate=False)


def test_a_duckdb_failure_in_the_vote_scan_is_not_redone_by_row(tmp_path, monkeypatch):
    import duckdb
    from tests.duckdb_faults import fail_streams_after
    source = _source(tmp_path, 'member_votes', _member_votes(300))
    monkeypatch.setattr(congress_bulk, '_BATCH', 100)
    fail_streams_after(monkeypatch, duckdb.OutOfMemoryException('injected'))
    with pytest.raises(etl_bulk.BulkValidationError, match='no row fallback') as caught:
        write_congress_dataset(source, tmp_path / 'out', dataset='member_votes', generation_id=GENERATION)
    assert not isinstance(caught.value, etl_bulk.BulkInputError) and not (tmp_path / 'out').exists()


def test_validate_false_skips_row_writer_and_row_restore_admission_too(tmp_path, monkeypatch):
    """A non-vote congress dataset: the flag a rollup passes after admitting the pair is honoured by the row path."""
    from spicy_regs import congress_receipts as current
    source = tmp_path / 'source.parquet'
    pq.write_table(pa.Table.from_pylist([{'congress': '119', 'chamber': 'house', 'event_id': '7'}]), source)
    calls = []
    real = current.validate_receipt_bundle
    monkeypatch.setattr(current, 'validate_receipt_bundle', lambda *a, **k: calls.append(1) or real(*a, **k))
    pair = current.write_congress_dataset(source, tmp_path / 'one', dataset='committee_meetings', generation_id='g1')
    assert len(calls) == 1
    current.write_congress_dataset(source, tmp_path / 'two', dataset='committee_meetings', generation_id='g2',
                                   prior=CongressInput(pair[0], pair[1], 'g1'), validate=False)
    current.restore_processing_input(*pair, tmp_path / 'restored.parquet', dataset='committee_meetings',
                                     generation_id='g1', validate=False)
    assert len(calls) == 1
