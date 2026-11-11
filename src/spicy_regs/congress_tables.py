"""Congress tables without per-row receipts (ss-vse3): a builder's rows to the public table, and back again.

The Congress builders write and merge text rows in their spicy-docs contract's shape; the public table is those rows
mapped by ``congress_subjects.map_record``. A family whose installed policies are marked ``receipts_retired`` maps
its outputs here and writes no receipt, and its builders read their prior, and other families' tables, as
:func:`as_builder_input` gives them back: every column the mapping keeps or promotes, in the builder's spelling. A
source field neither kept nor promoted is NULL there, as it is in a row the builder writes without it.

- :func:`to_public` refuses the whole table on a row the mapping refuses, as the receipt route refused its
  generation on a fresh refusal (``has_unmatched_failures``). A row the mapping gives no subject
  (``member_vote_terms`` without a term) is left out, as the receipt route left it out; the count and the first keys
  are returned for the run's log.
- :func:`as_builder_input` reads only the tables of :data:`READABLE`, and refuses one that predates its promoted
  columns: the first night after the switch reads such a prior through the receipts its generation still lists
  (``SelectedPriors``), which is also what back-fills those columns.

What the round trip cannot keep is what the public table cannot hold: an object key the source omitted against one
it stated as null inside ``roll_call_votes``' document, amendment and party-total lists (7,258 roll calls' documents
omitted ``nomination_id`` on 2026-11-11, written before it existed; they read back stating it null, which the votes
builder treats alike), and the key order and spacing of JSON text, which ``json_column`` spells sorted and compact,
as the builders do. Members, terms and affiliations returned their receipts' text byte for byte on 2026-11-11.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from spicy_docs.schemas.tables import json_column, text

from spicy_regs.congress_subjects import (
    BOOLEANS, FLATTEN, IDENTITIES, INPUT_COLUMNS, INTEGERS, JOINED, NATIVE, PROCESSING, PROMOTED, map_record,
    subject_schema, target_name,
)

#: The tables whose published rows are read back as a builder's input: a votes family table, or a reader of one.
READABLE = frozenset({"members", "member_terms", "member_party_affiliations", "roll_call_votes", "member_votes",
                      "member_vote_terms"})
#: A flattened source object read back from public columns: each of its keys and the column holding its value. A key
#: whose value is NULL is left out, as the crosswalk leaves out ``caucus``; every other key it states every time
#: (``end``, ``party``, ``start`` on all 59 rows of 2026-11-11, equal to these columns).
OBJECTS = {
    ("member_party_affiliations", "source_json"): {"caucus": "caucus", "end": "affiliation_end", "party": "party",
                                                   "start": "affiliation_start"},
}
#: Keys of :data:`OBJECTS` stated even when NULL.
_ALWAYS = frozenset({"end", "party", "start"})
#: Rows written per Arrow batch on either path.
_BATCH = 100_000
#: Keys of left-out rows returned for the log.
_SHOWN = 20


def _source_schema(dataset: str) -> pa.Schema:
    return pa.schema([(name, pa.string()) for name in INPUT_COLUMNS[dataset]])


def _pass_through(dataset: str) -> bool:
    """Whether every public column is a source column of the same name, as text or a digit string read as an integer."""
    from spicy_regs.congress_bulk import eligible

    return eligible(dataset) and not PROMOTED.get(dataset)


def to_public(dataset: str, source: Path, destination: Path) -> tuple[int, list[tuple]]:
    """Write ``dataset``'s public table from the builder's ``source`` rows, in their order.

    Returns how many rows the mapping gave no subject (left out) and the first keys of them. Raises ``ValueError``
    naming the first refused row's position, and never its content, when the mapping refuses a row.
    """
    columns = pq.read_schema(source).names
    if extra := set(columns) - set(INPUT_COLUMNS[dataset]):
        raise ValueError(f"{dataset}: unclassified source fields {sorted(extra)}")
    schema = subject_schema(dataset)
    left_out, shown = 0, []
    staged = destination.with_name(f".{destination.name}.partial")
    with pq.ParquetWriter(staged, schema, compression="zstd") as writer:
        if _pass_through(dataset):
            batches = _mapped_in_sql(dataset, source, schema)
        else:
            batches = _mapped_in_python(dataset, source, schema)
        for table, dropped in batches:
            left_out += len(dropped)
            shown.extend(dropped[: _SHOWN - len(shown)])
            if table.num_rows:
                writer.write_table(table, row_group_size=_BATCH)
    staged.replace(destination)
    return left_out, shown


def _mapped_in_python(dataset, source, schema):
    keys = IDENTITIES[dataset]
    ordinal = 0
    with pq.ParquetFile(source) as reader:
        for batch in reader.iter_batches(batch_size=_BATCH):
            rows, dropped = [], []
            for raw in batch.to_pylist():
                try:
                    mapped = map_record(dataset, raw)
                except (ValueError, TypeError, OverflowError, pa.ArrowException) as error:
                    raise ValueError(f"{dataset}: the mapping refuses source row {ordinal} "
                                     f"({type(error).__name__}); nothing is published") from None
                if mapped.subject is None:
                    dropped.append(tuple(raw.get(key) for key in keys))
                else:
                    if any(mapped.subject.get(key) is None for key in keys):
                        raise ValueError(f"{dataset}: source row {ordinal} has no {', '.join(keys)}; nothing is published")
                    rows.append(mapped.subject)
                ordinal += 1
            yield pa.Table.from_pylist(rows, schema=schema), dropped


def _mapped_in_sql(dataset, source, schema):
    """The pass-through mapping in DuckDB: text as it is, a digit string as an integer (``congress_bulk``'s rules)."""
    from spicy_regs import etl_bulk
    from spicy_regs.congress_bulk import _DIGITS, NO_SUBJECT, _text

    names = set(pq.read_schema(source).names)
    integers = [field.name for field in schema if pa.types.is_integer(field.type)]

    def value(field):
        if field.name not in names:
            return f'CAST(NULL AS {"BIGINT" if field.name in integers else "VARCHAR"}) AS "{field.name}"'
        if field.name in integers:
            return f'CAST("{field.name}" AS BIGINT) AS "{field.name}"'
        return f'"{field.name}"'

    no_subject = NO_SUBJECT.get(dataset, "false")
    for name in INPUT_COLUMNS[dataset]:
        if name not in names:
            no_subject = no_subject.replace(f'"{name}"', "CAST(NULL AS VARCHAR)")
    refused = " OR ".join(
        [f'("{name}" IS NOT NULL AND NOT regexp_full_match("{name}", {_DIGITS}))' for name in integers if name in names]
        + [f'"{key}" IS NULL' if key in names else "true" for key in IDENTITIES[dataset]]) or "false"
    relation = f"read_parquet({_text(str(source))}, file_row_number = true)"
    keys = ", ".join(f'"{key}"' for key in IDENTITIES[dataset])
    with etl_bulk.bulk_connection() as (con, _):
        first = con.execute(f"SELECT min(file_row_number) FROM {relation} WHERE NOT ({no_subject}) AND ({refused})").fetchone()
        if first is not None and first[0] is not None:
            raise ValueError(f"{dataset}: the mapping refuses source row {first[0]}; nothing is published")
        dropped = con.execute(f"SELECT {keys} FROM {relation} WHERE {no_subject} ORDER BY file_row_number").fetchall()
        yield schema.empty_table(), dropped
        reader = con.execute(f"SELECT {', '.join(value(field) for field in schema)} FROM {relation} "
                             f"WHERE NOT ({no_subject}) ORDER BY file_row_number").to_arrow_reader(_BATCH)
        for batch in reader:
            yield pa.Table.from_batches([batch]).cast(schema), []


def as_builder_input(dataset: str, tables: Sequence[Path], destination: Path) -> Path:
    """Write ``dataset``'s published ``tables`` (its members, in order) as the builder's text rows, in their order.

    Raises ``ValueError`` for a table outside :data:`READABLE` or one without a column it promotes.
    """
    if dataset not in READABLE:
        raise ValueError(f"{dataset}: no reader of its published table as builder input (congress_tables.READABLE)")
    schema = _source_schema(dataset)
    for table in tables:
        names = set(pq.read_schema(table).names)
        if missing := [name for name in PROMOTED.get(dataset, ()) if name not in names]:
            raise ValueError(f"{dataset}: the published table predates its promoted columns {missing}; read it "
                             "through the receipts its generation lists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
        for table in tables:
            with pq.ParquetFile(table) as reader:
                for batch in reader.iter_batches(batch_size=_BATCH):
                    if _pass_through(dataset):
                        rows = pa.Table.from_batches([batch])
                        rows = pa.table({name: (rows[name].cast(pa.string()) if name in rows.column_names
                                                else pa.nulls(rows.num_rows, pa.string())) for name in schema.names})
                    else:
                        rows = pa.Table.from_pylist([_source_row(dataset, row) for row in batch.to_pylist()], schema=schema)
                    writer.write_table(rows, row_group_size=_BATCH)
    return destination


def _source_row(dataset: str, row: dict) -> dict:
    """One published row in the builder's spelling: the inverse of ``map_record`` for the tables of :data:`READABLE`."""
    source = {}
    promoted = PROMOTED.get(dataset, ())
    for name in INPUT_COLUMNS[dataset]:
        if (dataset, name) in OBJECTS:
            stated = {key: row.get(column) for key, column in OBJECTS[dataset, name].items()}
            source[name] = json_column({key: value for key, value in stated.items()
                                        if value is not None or key in _ALWAYS})
            continue
        if (dataset, name) in FLATTEN or (dataset, name) in JOINED:
            raise ValueError(f"{dataset}.{name}: no inverse of its mapping")
        if name in promoted:
            value = row.get(name)
        elif name in PROCESSING.get(dataset, ()):
            value = None
        elif (dataset, name) in NATIVE:
            value = row.get(target_name(dataset, name))
            if value is not None and dataset == "roll_call_votes" and name == "tallies_json":
                value = {item["choice"]: item["count"] for item in value}
            value = None if value is None else json_column(value)
        else:
            value = row.get(target_name(dataset, name))
        if value is not None and not isinstance(value, str):
            if name not in INTEGERS.get(dataset, ()) and name not in BOOLEANS.get(dataset, ()):
                raise ValueError(f"{dataset}.{name}: unexpected {type(value).__name__} in a text column")
            value = text(value)
        source[name] = value
    return source


def summary(dataset: str, left_out: int, shown: list[tuple]) -> str:
    """The log line for rows the mapping gave no subject."""
    return f"{dataset}: {left_out:,} rows have no subject and are left out, as before; first keys {shown}"


