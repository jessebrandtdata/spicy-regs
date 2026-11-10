"""Prove a family's move off per-row ETL receipts changes nothing a reader sees (ss-vse3, ss-ag1j).

Each family switches once: the receipt route it publishes today is replaced by a plain route that merges the night's
rows into the prior public table. Before the switch both routes build the same night from the same prior and inputs
with upload skipped, and this module says what differs:

- :func:`table_differences`: schema, row count, rows (``EXCEPT ALL`` both ways) and row order on the key;
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


def table_differences(actual: Path, expected: Path, *, key: Sequence[str]) -> list[str]:
    """What separates two Parquet tables for a reader; empty when they are the same table.

    Schema (names, types, order), row count, every row both ways (``EXCEPT ALL``, so a duplicated row counts), and
    the key of the row at every position (the order a reader paging the file sees).
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
                         among: Iterable[tuple] | None = None) -> list[str]:
    """Rows of ``table`` whose promoted column differs from the copy the receipt route's receipts hold.

    ``receipt_table`` and ``receipts`` are a pair the receipt route published, read through admission
    (``read_with_receipts``); ``copies`` maps each promoted column to the function reading its copy from one
    admitted row. Rows are matched on ``key``, compared as text so a date and its spelling agree. Every receipt's
    row must still be in ``table``. ``among`` limits the comparison to those keys: a row re-read since the pair
    was published may rightly hold a newer value.
    """
    import tempfile

    import pyarrow.parquet as pq

    from spicy_regs.etl_receipts import read_with_receipts, select_receipts

    def identity(row):
        return tuple(None if row[name] is None else str(row[name]) for name in key)

    with tempfile.TemporaryDirectory(prefix="promoted-") as work:
        scoped = select_receipts(receipts, Path(work) / "scoped.parquet", dataset=policy.dataset)
        held = {identity(row): {name: read(row) for name, read in copies.items()}
                for row in read_with_receipts([receipt_table], [scoped], policy, generation_id=generation_id)}
    wanted = None if among is None else {tuple(None if v is None else str(v) for v in k) for k in among}
    rows = {identity(row): row for row in pq.read_table(table, columns=[*key, *copies]).to_pylist()}
    problems = [f"{missing} has a receipt and no row" for missing in sorted(set(held) - set(rows), key=str)]
    for found, copy in sorted(held.items(), key=lambda item: str(item[0])):
        if found in rows and (wanted is None or found in wanted):
            problems.extend(f"{found} {name}: {rows[found][name]!r} != receipt {copy[name]!r}"
                            for name in copies if rows[found][name] != copy[name])
    return problems


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
