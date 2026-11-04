"""``native_merge``: prior native pair + fresh pair, equal to carrying the fresh receipts over the whole prior."""

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA, DatasetPolicy, ReceiptContext, decode_exact_json, failure_receipt, validate_receipt_bundle,
    write_dataset,
)
from spicy_regs.native_merge import native_merge
from spicy_regs.receipt_history import carry_receipt_history

POLICY = DatasetPolicy(
    "items",
    pa.schema([("id", pa.string()), ("n", pa.int32()), ("tags", pa.list_(pa.string())), ("note", pa.string())]),
    ("id",), ("raw",))
WITNESS = {"source_id": "s", "source_uri": None, "sha256": "ab" * 32, "locator": "row", "body_version": None}
#: An identity whose text DuckDB spells differently from Python (upper-case hex control escape): the row functions' own.
ESCAPE = "e\x1b[0m"


def _row(id, note="same", n=1):
    return {"id": id, "n": n, "tags": [id, None], "note": note, "raw": f"raw-{id}\x1b"}


def _generation(tmp_path, name, generation, rows, refusal=None):
    records = [(row, ReceiptContext(generation, f"{generation}:{row['id']}", "p", [WITNESS])) for row in rows]
    failures = [] if refusal is None else [failure_receipt(
        POLICY, ReceiptContext(generation, f"{generation}:{refusal}", "p", [WITNESS]), outcome="refused",
        raw_fields={"raw": refusal})]
    return write_dataset(records, tmp_path / name, POLICY, failures=failures, validate=False)


def _merge(tmp_path, prior, fresh, **options):
    return native_merge(POLICY, fresh=fresh, prior=prior, order_by=("id",), subjects=tmp_path / "s.parquet",
                        receipts=tmp_path / "r.parquet", **options)


def _prior_and_fresh(tmp_path):
    prior = _generation(tmp_path, "prior", "g0", [_row(i) for i in "abcdef"] + [_row(ESCAPE), _row("z")], refusal="old")
    fresh = _generation(tmp_path, "fresh", "g1", [
        _row("c"),                     # refetched, unchanged: keeps its prior receipt
        _row("e", note="changed"),     # refetched, changed: references its predecessor
        _row(ESCAPE),                  # refetched, unchanged, identity the row functions spell
        _row("b0"), _row("zz"),        # new
    ], refusal="old")
    return prior, fresh


def test_the_merge_equals_the_fresh_receipts_carried_over_the_whole_prior(tmp_path):
    prior, fresh = _prior_and_fresh(tmp_path)
    merged = _merge(tmp_path, prior, fresh)
    replaced = {"c", "e", ESCAPE}
    subjects = sorted([r for r in pq.read_table(prior[0]).to_pylist() if r["id"] not in replaced]
                      + pq.read_table(fresh[0]).to_pylist(), key=lambda r: r["id"])
    assert pq.read_table(merged.subjects).to_pylist() == subjects
    assert pq.read_table(merged.subjects).schema.equals(POLICY.subject_schema)
    assert (merged.kept, merged.replaced, merged.added) == (5, 3, 2)

    carried = pq.read_table(carry_receipt_history(fresh[1], [prior[1]], tmp_path / "carried.parquet")).to_pylist()
    kept = {r["record_id"]: r for r in pq.read_table(prior[1]).to_pylist() if r["outcome"] == "accepted"}
    by_record = {r["record_id"]: r for r in [*kept.values(), *(r for r in carried if r["outcome"] == "accepted")]}
    got = pq.read_table(merged.receipts).to_pylist()
    accepted = [r for r in got if r["outcome"] == "accepted"]
    assert [decode_exact_json(r["identity_json"])[0][1] for r in accepted] == [row["id"] for row in subjects]
    expected = [by_record[r["record_id"]] for r in accepted]
    assert got == [*expected, *(r for r in carried if r["outcome"] != "accepted")]
    assert pq.read_table(merged.receipts).schema.equals(RECEIPT_SCHEMA)

    by_id = {decode_exact_json(r["identity_json"])[0][1]: r for r in got if r["outcome"] == "accepted"}
    unchanged_prior = {decode_exact_json(r["identity_json"])[0][1]: r for r in pq.read_table(prior[1]).to_pylist()
                       if r["outcome"] == "accepted"}
    for kept_id in ("a", "b", "c", "d", "f", "z", ESCAPE):  # unchanged: the prior receipt, byte for byte
        assert by_id[kept_id] == unchanged_prior[kept_id]
    assert by_id["e"]["generation_id"] == "g1"
    assert decode_exact_json(by_id["e"]["diagnostic_json"])["prior_receipt"]["receipt_id"] == unchanged_prior["e"]["receipt_id"]
    assert by_id["zz"]["generation_id"] == "g1" and "prior_receipt" not in decode_exact_json(by_id["zz"]["diagnostic_json"])
    # The repeated refusal keeps its prior receipt; the merged pair is admitted as any generation is.
    assert [r["generation_id"] for r in got if r["outcome"] == "refused"] == ["g0"]
    validate_receipt_bundle({"items": [merged.subjects]}, [merged.receipts], [POLICY])


def test_a_cold_start_is_the_fresh_pair_sorted(tmp_path):
    fresh = _generation(tmp_path, "fresh", "g1", [_row("b"), _row("a")], refusal="r")
    merged = _merge(tmp_path, None, fresh)
    assert pq.read_table(merged.subjects)["id"].to_pylist() == ["a", "b"]
    assert [r["outcome"] for r in pq.read_table(merged.receipts).to_pylist()] == ["accepted", "accepted", "refused"]
    assert (merged.kept, merged.replaced, merged.added) == (0, 0, 2)
    validate_receipt_bundle({"items": [merged.subjects]}, [merged.receipts], [POLICY])


def test_a_subject_without_an_accepted_receipt_is_refused(tmp_path):
    prior, fresh = _prior_and_fresh(tmp_path)
    table = pq.read_table(prior[1])
    dropped = tmp_path / "dropped.parquet"
    pq.write_table(table.filter(pc.call_function("not_equal", [table["identity_json"], table["identity_json"][0]])), dropped)
    with pytest.raises(ValueError, match="accepted receipts"):
        _merge(tmp_path, (prior[0], dropped), fresh)


def test_receipts_of_another_dataset_are_refused(tmp_path):
    prior, fresh = _prior_and_fresh(tmp_path)
    table = pq.read_table(prior[1])
    other = tmp_path / "other.parquet"
    pq.write_table(table.set_column(table.schema.get_field_index("dataset"), "dataset", pa.array(["x"] * table.num_rows)), other)
    with pytest.raises(ValueError, match="scoped to the dataset"):
        _merge(tmp_path, (prior[0], other), fresh)


def test_a_repeated_identity_is_refused(tmp_path):
    prior, fresh = _prior_and_fresh(tmp_path)
    doubled = tmp_path / "doubled.parquet"
    table = pq.read_table(fresh[0])
    pq.write_table(pa.concat_tables([table, table]), doubled)
    with pytest.raises(ValueError, match="repeat an identity"):
        _merge(tmp_path, prior, (doubled, fresh[1]))


def test_a_descending_order_equals_merge_local_prior_and_only_routed_rows_are_decoded(tmp_path, monkeypatch):
    import duckdb

    from spicy_regs import native_merge as module
    from spicy_regs.transforms.table_merge import merge_local_prior

    prior = _generation(tmp_path, "prior", "g0", [_row(i, n=k % 3) for k, i in enumerate("abcdefgh")] + [_row(ESCAPE)])
    fresh = _generation(tmp_path, "fresh", "g1", [_row("c", n=2), _row("zz", n=1), _row(ESCAPE, note="new")])
    decoded = []
    real = module.subject_identity
    monkeypatch.setattr(module, "subject_identity", lambda policy, row: decoded.append(row["id"]) or real(policy, row))
    merged = native_merge(POLICY, fresh=fresh, prior=prior, order_by=("n DESC", "id"), subjects=tmp_path / "s.parquet",
                          receipts=tmp_path / "r.parquet")
    assert decoded == [ESCAPE, ESCAPE]  # the routed row of each side, not the other 10
    with duckdb.connect() as con:
        merge_local_prior(con, columns=tuple(POLICY.subject_schema.names), identity="id", order_by='"n" DESC, "id"',
                          prior_file=prior[0], new_file=fresh[0], out_file=tmp_path / "expected.parquet")
    assert pq.read_table(merged.subjects)["id"].to_pylist() == pq.read_table(tmp_path / "expected.parquet")["id"].to_pylist()
    validate_receipt_bundle({"items": [merged.subjects]}, [merged.receipts], [POLICY])


def test_native_merge_keeps_unanswered_refusals_and_resolves_only_successes(tmp_path):
    from spicy_regs.receipt_history import RefusalIdentity

    context = ReceiptContext("g0", "refused", "p", [WITNESS])
    refusals = [failure_receipt(POLICY, context, outcome="refused", raw_fields={"raw": {"id": key}})
                for key in ("fixed", "unanswered")]
    prior = write_dataset([], tmp_path / "prior", POLICY, failures=refusals, validate=False)
    fresh = write_dataset([(_row("fixed") | {"raw": {"id": "fixed"}},
                            ReceiptContext("g1", "fixed", "p", [WITNESS]))],
                          tmp_path / "fresh", POLICY, validate=False)
    merged = _merge(tmp_path, prior, fresh, refusal_identity=RefusalIdentity(("id",), "raw"))
    assert pq.read_table(merged.receipts).to_pylist() == [*pq.read_table(fresh[1]).to_pylist(), refusals[1]]
    validate_receipt_bundle({"items": [merged.subjects]}, [merged.receipts], [POLICY])
    empty_subject, empty_receipts = write_dataset([], tmp_path / "empty", POLICY, validate=False)
    assert empty_subject is not None
    again = native_merge(POLICY, fresh=(empty_subject, empty_receipts), prior=(merged.subjects, merged.receipts), order_by=("id",),
                         subjects=tmp_path / "again-s.parquet", receipts=tmp_path / "again-r.parquet",
                         refusal_identity=RefusalIdentity(("id",), "raw"))
    assert pq.read_table(again.receipts).equals(pq.read_table(merged.receipts))


def test_dense_exceptional_identities_have_bounded_python_allocation(tmp_path):
    """A larger routed population must not accumulate Python ordinals or identities."""
    import tracemalloc

    from spicy_regs import etl_bulk
    from spicy_regs.native_merge import _identities
    from spicy_regs.etl_receipts import subject_identity

    def measure(count, name):
        path = tmp_path / f'{name}.parquet'
        rows = [_row(str(i), note='text\x1b') for i in range(count)]
        rows[0]['id'] = ESCAPE
        table = pa.Table.from_pylist(rows, schema=POLICY.subject_schema)
        pq.write_table(table, path, row_group_size=997)
        # Fixture creation and the independent expected values are outside measurement.
        expected = [subject_identity(POLICY, row)[0] for row in table.to_pylist()]
        del rows, table
        with etl_bulk.bulk_connection(tmp_path, threads=1, memory_limit='128MB') as (con, _):
            con.execute(f"CREATE VIEW held AS SELECT *, file_row_number AS _o FROM read_parquet({etl_bulk._literal(str(path))}, file_row_number=true)")
            tracemalloc.start()
            try:
                _identities(con, POLICY, 'held', path)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            got = [value for value, in con.execute('SELECT _rid FROM held_ids ORDER BY _o').fetchall()]
        assert got == expected
        return peak

    small = measure(500, 'small')
    large = measure(8000, 'large')
    assert large - small < 1024 * 1024, (small, large)
