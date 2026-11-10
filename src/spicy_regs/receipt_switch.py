"""Prove a family's move off per-row ETL receipts changes nothing a reader sees (ss-vse3, ss-ag1j).

Each family switches once: the receipt route it publishes today is replaced by a plain route that merges the night's
rows into the prior public table. Before the switch both routes build the same night from the same prior and inputs
with upload skipped, and this module says what differs:

- :func:`table_differences`: schema, row count, rows (``EXCEPT ALL`` both ways) and row order on a unique key;
- :func:`promoted_differences`: each promoted column against the copy the receipt route's own receipts hold;
- :func:`receipt_free_prior`: the prior as the plain route is given it, with no receipts file to open at all;
- :func:`receipt_files`: every receipts file a route left in its build or output.

``tests/test_federal_register_switch.py`` is the first user; the full-size replay of a family's switch uses the same
functions on the real prior and the night's retained inputs.

What these cannot see: ``EXCEPT ALL`` compares values, not Parquet encodings or row-group layout; a receipt read by a
native engine is caught only because the plain route is handed none; one night says nothing about a split window.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

RECEIPTS = "etl_receipts.parquet"


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


#: Receipt copies written per Arrow batch while they are read; the comparison itself runs in DuckDB.
BATCH_ROWS = 10_000


def _require_unique(con, relation: str, key: Sequence[str], label: str) -> None:
    """Refuse a key that does not name one row: rows sharing it could trade places and still compare equal."""
    columns = ", ".join(_quoted(name) for name in key)
    [(rows, keys)] = con.sql(f"SELECT count(*), count(DISTINCT row({columns})) FROM {relation}").fetchall()
    if rows != keys:
        raise ValueError(f"{label}: ({', '.join(key)}) names {keys:,} of {rows:,} rows; compare on the whole row "
                         "identity")


def table_differences(actual: Path, expected: Path, *, key: Sequence[str]) -> list[str]:
    """What separates two Parquet tables for a reader; empty when they are the same table.

    Schema (names, types, order), row count, every row both ways (``EXCEPT ALL``), and the key of the row at every
    position (the order a reader paging the file sees). ``key`` must name one row in each table, or the comparison
    refuses: two rows sharing a key could swap places unseen.
    """
    import duckdb
    import pyarrow.parquet as pq

    problems = []
    left, right = pq.read_schema(actual), pq.read_schema(expected)
    if not left.equals(right):
        problems.append(f"schema differs: {left.names} != {right.names}"
                        if left.names != right.names else "schema differs in a column type or nullability")
    names = [name for name in right.names if name in left.names]
    columns = ", ".join(_quoted(name) for name in names)
    one, other = (f"read_parquet({_literal(path)}, file_row_number=true)" for path in (actual, expected))
    with duckdb.connect() as con:
        _require_unique(con, one, key, str(actual))
        _require_unique(con, other, key, str(expected))
        counts = con.sql(f"SELECT (SELECT count(*) FROM {one}), (SELECT count(*) FROM {other})").fetchone()
        assert counts is not None
        if counts[0] != counts[1]:
            problems.append(f"{counts[0]:,} rows against {counts[1]:,}")
        for label, (a, b) in (("only in the first", (one, other)), ("only in the second", (other, one))):
            [(extra,)] = con.sql(f"SELECT count(*) FROM (SELECT {columns} FROM {a} EXCEPT ALL "
                                 f"SELECT {columns} FROM {b})").fetchall()
            if extra:
                problems.append(f"{extra:,} rows {label}")
        moved = " OR ".join(f"a.{_quoted(name)} IS DISTINCT FROM b.{_quoted(name)}" for name in key)
        [(misplaced,)] = con.sql(f"SELECT count(*) FROM {one} a FULL JOIN {other} b USING (file_row_number) "
                                 f"WHERE {moved}").fetchall()
        if misplaced:
            problems.append(f"{misplaced:,} positions hold a different key")
    return problems


def promoted_differences(table: Path, receipt_table: Path, receipts: Path, policy, *, generation_id: str,
                         key: Sequence[str], copies: Mapping[str, Callable[[Mapping], Any]],
                         among: Iterable[tuple] | None = None, shown: int = 20) -> list[str]:
    """Rows of ``table`` whose promoted column differs from the copy the receipt route's receipts hold.

    ``receipt_table`` and ``receipts`` are a pair the receipt route published, read through admission
    (``read_with_receipts``); ``copies`` maps each promoted column to the function reading its copy from one
    admitted row. The copies are written to Parquet in Arrow batches as they are read, typed as ``table``'s columns,
    and compared in DuckDB on ``key``, which must name one row on both sides. Every receipt's row must still be in
    ``table``. ``among`` limits the value comparison to those keys: a row re-read since the pair was published may
    rightly hold a newer value. At most ``shown`` rows of each kind are named.
    """
    import tempfile

    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.etl_receipts import read_with_receipts, select_receipts

    published = pq.read_schema(table)
    copy_schema = pa.schema([published.field(name) for name in (*key, *copies)])
    keys = ", ".join(_quoted(name) for name in key)
    with tempfile.TemporaryDirectory(prefix="promoted-") as work:
        scoped = select_receipts(receipts, Path(work) / "scoped.parquet", dataset=policy.dataset)
        held = Path(work) / "copies.parquet"
        with pq.ParquetWriter(held, copy_schema) as writer:
            batch: list[dict] = []
            for row in read_with_receipts([receipt_table], [scoped], policy, generation_id=generation_id):
                batch.append({**{name: row[name] for name in key}, **{name: read(row) for name, read in copies.items()}})
                if len(batch) == BATCH_ROWS:
                    writer.write_table(pa.Table.from_pylist(batch, schema=copy_schema))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=copy_schema))
        with duckdb.connect() as con:
            rows, copy = f"read_parquet({_literal(table)})", f"read_parquet({_literal(held)})"
            _require_unique(con, rows, key, str(table))
            _require_unique(con, copy, key, "the receipt copies")
            scope = ""
            if among is not None:
                con.register("among", pa.Table.from_pylist([dict(zip(key, item, strict=True)) for item in among],
                                                            schema=pa.schema([published.field(name) for name in key])))
                scope = f" SEMI JOIN among USING ({keys})"
            problems = [f"{found} has a receipt and no row" for found in con.sql(
                f"SELECT {keys} FROM {copy} c ANTI JOIN {rows} t USING ({keys}) ORDER BY ALL LIMIT {shown}").fetchall()]
            for name in copies:
                column = _quoted(name)
                problems += [f"{found[:-2]} {name}: {found[-2]!r} != receipt {found[-1]!r}" for found in con.sql(
                    f"SELECT {keys}, t.{column}, c.{column} FROM {rows} t JOIN {copy} c USING ({keys}){scope} "
                    f"WHERE t.{column} IS DISTINCT FROM c.{column} ORDER BY ALL LIMIT {shown}").fetchall()]
    return problems


def receipt_copy_differences(table: Path | str, receipts: Path | str, *, dataset: str, key: Sequence[str],
                             columns: Mapping[str, str]) -> dict[str, int]:
    """Counts of rows whose promoted column differs from its copy in the receipts, in DuckDB alone.

    The full-size form of :func:`promoted_differences`, for receipts too large to read row by row in Python (the
    court citations' 103 million). Only accepted receipts are read, and only their ``dataset``, ``outcome``,
    ``identity_json`` and ``processing_json``, so a remote receipts file is read by range. ``key`` names the identity in its policy's order, each a text value;
    ``columns`` maps each promoted column to a SQL expression over ``{v}``, the copy as the receipt tags it
    (``["str","..."]``, ``["null",null]``): ``{v}->>'$[1]'`` is a text copy. Returns the receipts read, the receipts
    with no row of their key in ``table``, and per column the rows that differ (``IS DISTINCT FROM``).

    What it cannot see: a row of ``table`` with no receipt (compare the tables for that), or a key repeated in the
    receipts (each repetition is joined and counted).
    """
    import duckdb

    keys = ", ".join(f"json_extract_string(identity_json, '$[1][{at}][1][1][1]') AS {_quoted(name)}"
                     for at, name in enumerate(key))
    counts = ", ".join(f"count(*) FILTER (WHERE ({expression.replace('{v}', 'f[' + _sql_text(name) + ']')}) "
                       f"IS DISTINCT FROM t.{_quoted(name)}) AS {_quoted(name)}"
                       for name, expression in columns.items())
    sql = f"""
        WITH r AS (
          SELECT {keys}, map_from_entries(list_transform(CAST(json_extract(processing_json, '$[1]') AS JSON[]),
                                                         e -> {{'k': e->>'$[0]', 'v': e->'$[1]'}})) AS f
          FROM read_parquet($receipts) WHERE dataset = $dataset AND outcome = 'accepted')
        SELECT count(*) AS receipts, count(*) FILTER (WHERE t._held IS NULL) AS without_row, {counts}
        FROM r LEFT JOIN (SELECT *, true AS _held FROM read_parquet($table)) t USING ({', '.join(map(_quoted, key))})"""
    with duckdb.connect() as con:
        if any(str(path).startswith("https://") for path in (table, receipts)):
            from spicy_regs.duckdb_settings import load_public_http

            load_public_http(con)
        cursor = con.execute(sql, {"receipts": str(receipts), "table": str(table), "dataset": dataset})
        names = [column[0] for column in cursor.description]
        [row] = cursor.fetchall()
    return dict(zip(names, row, strict=True))


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def receipt_free_prior(prior: Path, destination: Path) -> Path:
    """Copy a published generation's tables into ``destination`` without its receipts member.

    The plain route is handed this, so no reader of any kind (Python, DuckDB, Arrow) can open a receipts file
    while it runs: there is none to open.
    """
    destination.mkdir(parents=True, exist_ok=True)
    for path in sorted(prior.glob("*.parquet")):
        if path.name != RECEIPTS:
            shutil.copyfile(path, destination / path.name)
    return destination


def receipt_files(*roots: Path) -> list[Path]:
    """Every receipts file under ``roots``: a plain route that wrote or fetched one left it behind."""
    return sorted(path for root in roots for path in root.rglob("*.parquet")
                  if path.name == RECEIPTS or "receipt" in path.name)
