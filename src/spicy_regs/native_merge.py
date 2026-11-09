"""Upsert a fresh native subject/receipt pair over a prior one without restoring rows.

A nightly that fetches only a window (lobbying's posted-date window, a day's regulatory attribute parts) has the prior
generation's subject and receipt files already admitted. :func:`native_merge` is that merge at the native level, in
DuckDB and Arrow:

* subjects are the prior ones whose identity the fresh pair does not carry, plus the fresh ones, in ``order_by`` order
  (what ``table_merge.merge_local_prior`` yields on the same columns, DuckDB's null ordering included);
* accepted receipts are those of the kept prior subjects, verbatim, plus ``carry_receipt_history`` of the fresh receipts
  against the prior receipts they replace: a refetched row that did not change keeps its prior receipt, a changed one
  references only its direct predecessor, a new one is fresh. Accepted receipts follow their subjects' order, as the
  restore readers expect; the fresh pair's other receipts (refusals, observations) come last, in their own order.
  Unresolved prior refusals survive unchanged until a fresh accepted receipt proves the same input converted.
  Other prior non-accepted receipts survive only if repeated: a family that keeps per-file observations for the kept
  rows (regulatory ``input_metadata``) must repeat them in its fresh pair.

Only narrow keys are ever sorted or joined (ordinals and record ids, about 100 bytes a row); wide subject and receipt
rows are gathered by ordinal, one output batch at a time, each source row group read once per batch. Kept prior rows
already sit in ``order_by`` order, so the gather is nearly sequential. Receipt work is O(fresh rows); what stays
O(all rows) is the narrow sort and one pass over each file. The caller admits the result once, as it admits any
generation; nothing here validates it beyond refusing a subject with no receipt. The prior pair must already be
admitted, and its receipts scoped to the dataset.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, open_receipt_writer, DatasetPolicy, subject_identity
from spicy_regs.receipt_history import RefusalIdentity, _take_prior_rows, carry_receipt_history

_BATCH = 50_000


class Merged(NamedTuple):
    subjects: Path
    receipts: Path
    kept: int  # prior subjects the fresh pair did not replace
    replaced: int  # prior subjects it did
    added: int  # fresh subjects with no prior


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _identities(con, policy: DatasetPolicy, view: str, path: Path, *, scratch: Path | None = None) -> None:
    """``<view>_ids(_o, _rid)``: each subject's record id, from SQL where it spells as Python does, else from Python.

    Only the routed rows are decoded: their row groups are read, one at a time, and nothing else.
    """
    record_id, _, _ = etl_bulk.identity_sql(policy)
    reference = etl_bulk._subject_reference_sql(policy)
    con.execute(
        f"CREATE TABLE {view}_ids AS SELECT _o, {record_id} AS _rid, ({reference}) IS NOT FALSE AS _ref FROM {view}"
    )
    # Finish the ordinal query before issuing another statement on this
    # connection. Both the sorted ordinals and corrections live on disk;
    # Python retains only one source batch and its decisions at a time.
    with TemporaryDirectory(prefix="native-identities-", dir=scratch) as temporary:
        root = Path(temporary)
        wanted = iter(etl_bulk._ordinal_file(
            con, f"SELECT _o FROM {view}_ids WHERE _ref ORDER BY _o", [], root / "ordinals.parquet"))
        pending = next(wanted, None)
        if pending is not None:
            schema = pa.schema([("_o", pa.int64()), ("_rid", pa.string())])
            with pq.ParquetWriter(root / "corrections.parquet", schema, compression="zstd") as output:
                with pq.ParquetFile(path) as source:
                    start = 0
                    for group in range(source.metadata.num_row_groups):
                        end = start + source.metadata.row_group(group).num_rows
                        if pending is not None and pending < end:
                            for batch in source.iter_batches(batch_size=etl_bulk._BATCH, row_groups=[group]):
                                stop = start + batch.num_rows
                                decided = []
                                while pending is not None and pending < stop:
                                    row = batch.slice(pending - start, 1).to_pylist()[0]
                                    decided.append({"_o": pending, "_rid": subject_identity(policy, row)[0]})
                                    pending = next(wanted, None)
                                if decided:
                                    output.write_table(pa.Table.from_pylist(decided, schema=schema))
                                start = stop
                        start = end
            if pending is not None:
                raise ValueError(f"{policy.dataset}: routed subject ordinal is outside its source")
            con.execute(f"UPDATE {view}_ids SET _rid = c._rid, _ref = false FROM "
                        f"read_parquet({etl_bulk._literal(str(root / 'corrections.parquet'))}) c "
                        f"WHERE {view}_ids._o = c._o")
    if con.execute(f"SELECT count(*) - count(DISTINCT _rid) FROM {view}_ids").fetchone()[0]:
        raise ValueError(f"{policy.dataset}: {view} subjects repeat an identity")


class _Gatherer:
    """Rows of the concatenated ``files`` by global ordinal, reading each source row group once per call."""

    def __init__(self, stack: ExitStack, files: Sequence[Path], schema: pa.Schema):
        self.schema, self.handles, self.ends, self.groups = schema, [], [], []
        total = 0
        for path in files:
            handle = stack.enter_context(pq.ParquetFile(path))
            self.handles.append(handle)
            cumulative, held = 0, []
            for index in range(handle.metadata.num_row_groups):
                cumulative += handle.metadata.row_group(index).num_rows
                held.append(cumulative)
            self.groups.append(held)
            total += handle.metadata.num_rows
            self.ends.append(total)

    def take(self, ordinals: list[int]) -> pa.Table:
        table, positions = _take_prior_rows(self.handles, self.ends, self.groups, ordinals, self.schema)
        if positions != list(range(len(ordinals))):
            raise ValueError("native merge gathered a different number of rows than it selected")
        return table.select(self.schema.names).cast(self.schema)


def _gather(con, ordinals: str, files: Sequence[Path], schema: pa.Schema, writer, *, group: int | None = None) -> int:
    """Write, in the order ``ordinals`` (SQL of one column) gives, the rows of the concatenated ``files`` at those ordinals."""
    rows = 0
    with ExitStack() as stack:
        gatherer = _Gatherer(stack, files, schema)
        for batch in con.execute(ordinals).to_arrow_reader(_BATCH):
            writer.write_table(gatherer.take(batch.column(0).to_pylist()), row_group_size=group)
            rows += batch.num_rows
    return rows


def _order(order_by: Sequence[str]) -> str:
    """``"col"``, ``"col ASC"`` or ``"col DESC"`` items as an ORDER BY list (DuckDB's null ordering, as the builders' merge)."""
    items = []
    for item in order_by:
        name, _, direction = item.strip().rpartition(" ")
        if not name or direction.upper() not in {"ASC", "DESC"}:
            name, direction = item.strip(), "ASC"
        items.append((name.strip(), direction.upper()))
    return ", ".join(f"{_q(name)} {direction}" for name, direction in items)


def _order_columns(order_by: Sequence[str]) -> list[str]:
    return [item.strip().rsplit(" ", 1)[0] if item.strip().upper().endswith((" ASC", " DESC")) else item.strip()
            for item in order_by]


def native_merge(
    policy: DatasetPolicy,
    *,
    fresh: tuple[Path, Path],
    prior: tuple[Path, Path] | None,
    order_by: Sequence[str],
    subjects: Path,
    receipts: Path,
    refusal_identity: RefusalIdentity | None = None,
) -> Merged:
    """Write the merged pair to ``subjects`` and ``receipts``; ``fresh`` and ``prior`` are ``(subjects, receipts)`` files.

    ``prior=None`` is a cold start: the fresh pair, sorted. ``order_by`` names subject columns, each optionally followed
    by ``ASC`` or ``DESC`` (``("dt_posted DESC", "filing_uuid")``).
    """
    if policy.receipt_only:
        raise ValueError(f"{policy.dataset}: a receipt-only dataset has no native subjects to merge")
    names = policy.subject_schema.names
    if any(name.startswith("_") for name in names) or not order_by or not set(_order_columns(order_by)) <= set(names):
        raise ValueError(f"{policy.dataset}: order_by must name subject columns, and subject columns may not start with _")
    fresh_subjects, fresh_receipts = fresh
    prior_subjects, prior_receipts = prior if prior is not None else (None, None)
    lit = etl_bulk._literal
    keys = _order(order_by)

    with etl_bulk.bulk_connection(Path(subjects).parent) as (con, root):
        con.execute("SET preserve_insertion_order = false")

        def view(name, path, cold=False):
            con.execute(f"CREATE VIEW {name} AS SELECT *, file_row_number AS _o "
                        f"FROM read_parquet({lit(str(path))}, file_row_number = true)" + (" WHERE false" if cold else ""))

        view("fresh_s", fresh_subjects)
        view("fresh_r", fresh_receipts)
        view("prior_s", prior_subjects or fresh_subjects, cold=prior is None)
        view("prior_r", prior_receipts or fresh_receipts, cold=prior is None)
        if con.execute(f"SELECT count(*) FROM prior_r WHERE dataset <> {lit(policy.dataset)}").fetchone()[0]:
            raise ValueError(f"{policy.dataset}: prior receipts must be scoped to the dataset")
        _identities(con, policy, "fresh_s", fresh_subjects, scratch=root)
        if prior is None:
            con.execute("CREATE TABLE prior_s_ids (_o BIGINT, _rid VARCHAR)")
        else:
            assert prior_subjects is not None
            _identities(con, policy, "prior_s", prior_subjects, scratch=root)

        # Receipts the fresh pair replaces, as the history carrier's prior: the prior accepted receipts of replaced
        # identities and every non-accepted one (a refusal or observation repeated verbatim keeps its prior receipt).
        prior_receipt_files = [Path(prior_receipts)] if prior_receipts else []
        replaced = root / "replaced.parquet"
        with open_receipt_writer(replaced) as out:
            replaced_rows = _gather(
                con,
                "SELECT _o FROM prior_r WHERE outcome <> 'accepted' OR record_id IN "
                "(SELECT record_id FROM fresh_r WHERE outcome = 'accepted') ORDER BY _o",
                prior_receipt_files, RECEIPT_SCHEMA, out) if prior_receipts else 0
        carried = carry_receipt_history(
            Path(fresh_receipts), [replaced] if replaced_rows else [], root / "carried.parquet",
            refusal_identities={policy.dataset: refusal_identity} if refusal_identity is not None else None)
        con.execute(f"CREATE VIEW carried_r AS SELECT *, file_row_number AS _o "
                    f"FROM read_parquet({lit(str(carried))}, file_row_number = true)")

        # One narrow sort, streamed: (subject ordinal, accepted-receipt ordinal) of every merged row in order. The joins
        # are plain equi-joins on record ids (a src test inside an ON clause makes DuckDB plan a nested loop); a subject
        # without exactly one accepted receipt drops out or repeats, which the count below refuses.
        prior_rows = pq.ParquetFile(prior_subjects).metadata.num_rows if prior_subjects else 0
        prior_receipt_rows = pq.ParquetFile(prior_receipts).metadata.num_rows if prior_receipts else 0
        fresh_rows = pq.ParquetFile(fresh_subjects).metadata.num_rows
        replaced_rows_n = con.execute(
            "SELECT count(*) FROM prior_s_ids WHERE _rid IN (SELECT _rid FROM fresh_s_ids)").fetchone()[0]
        kept, added = prior_rows - replaced_rows_n, fresh_rows - replaced_rows_n
        total = kept + fresh_rows
        columns = ", ".join(_q(c) for c in dict.fromkeys(_order_columns(order_by)))
        con.execute("CREATE VIEW prior_acc AS SELECT record_id, _o FROM prior_r WHERE outcome = 'accepted'")
        con.execute("CREATE VIEW carried_acc AS SELECT record_id, _o FROM carried_r WHERE outcome = 'accepted'")
        merged = (
            f"SELECT _so, _ro FROM (SELECT 0 AS _src, s._o AS _so, a._o AS _ro, {columns} FROM prior_s s "
            "JOIN prior_s_ids i ON i._o = s._o JOIN prior_acc a ON a.record_id = i._rid "
            "WHERE i._rid NOT IN (SELECT _rid FROM fresh_s_ids) UNION ALL "
            f"SELECT 1, {prior_rows} + s._o, {prior_receipt_rows} + c._o, {columns} FROM fresh_s s "
            "JOIN fresh_s_ids i ON i._o = s._o JOIN carried_acc c ON c.record_id = i._rid) "
            f"ORDER BY {keys}, _src, _so"
        )
        files = [*prior_receipt_files, carried]
        subject_files = [*([Path(prior_subjects)] if prior_subjects else []), Path(fresh_subjects)]
        written = 0
        try:
            with (
                ExitStack() as stack,
                pq.ParquetWriter(subjects, policy.subject_schema, compression="zstd") as subject_out,
                open_receipt_writer(receipts) as out,
            ):
                subject_gather = _Gatherer(stack, subject_files, policy.subject_schema)
                receipt_gather = _Gatherer(stack, files, RECEIPT_SCHEMA)
                for batch in con.execute(merged).to_arrow_reader(_BATCH):
                    subject_out.write_table(subject_gather.take(batch.column(0).to_pylist()))
                    out.write_table(receipt_gather.take(batch.column(1).to_pylist()), row_group_size=etl_bulk._GROUP)
                    written += batch.num_rows
                if written != total:
                    raise ValueError(f"{policy.dataset}: {total} merged subjects but {written} accepted receipts for them")
                _gather(con, f"SELECT {prior_receipt_rows} + _o FROM carried_r WHERE outcome <> 'accepted' ORDER BY _o",
                        files, RECEIPT_SCHEMA, out, group=etl_bulk._GROUP)
        except BaseException:
            Path(subjects).unlink(missing_ok=True)
            Path(receipts).unlink(missing_ok=True)
            raise
    return Merged(Path(subjects), Path(receipts), kept, replaced_rows_n, added)
