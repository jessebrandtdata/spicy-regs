"""Court subjects and receipts a column at a time, equal to ``court_receipts.write_court_rows``.

The row writer maps, splits, serializes and validates one row at a time. For the 102M rows of the citations
rollup that was more than a day. :func:`write_bundle` gives the same subject and receipt rows from one DuckDB
query over the staged source, through the shared ``etl_bulk.write_split`` loop; no receipt is validated here
(the generation's admission is the one validation).

SQL owns only the rules ``court_subjects.normalize_court_row`` states for text, integers, booleans and the
publisher's score: a declared column passed through; ``-?[0-9]+`` read as an integer inside BIGINT; ``t``, ``f``,
``true``, ``false`` (any case) read as a boolean; a plain decimal in [0, 1] read as a float. A row with any other
value, a missing identity, or text whose encoding SQL spells differently from Python goes to the row function
(``attempt``), which is ``normalize_court_row`` itself and decides it, refusals included. Which datasets and
source schemas qualify is :func:`eligible`; the rest stay on the row writer.

:func:`write_table` applies the same rules with no receipts, for the tables whose receipts are retired (ss-vse3):
the publisher's record fields are columns, and only a refused or respelled row is recorded, in an exceptions file.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import ExitStack
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.court_subjects import IDENTITIES, LEGACY_COLUMNS, RECEIPT_FIELDS, REFUSED, SUBJECT_SCHEMAS
from spicy_regs.etl_receipts import WITNESS_TYPE, DatasetPolicy, ReceiptContext, exact_json, refuse_local_witnesses
from spicy_regs.runtime_bounds import checkpoint

#: Tables whose mapping is only the rules above (the others convert lists, derive identities or read JSON).
DATASETS = frozenset({
    "court_citation_map", "court_citations", "court_parentheticals", "court_opinion_clusters", "court_opinions",
    "court_opinion_texts",
})
#: Rows per DuckDB batch; clusters carry whole opinions' metadata text, so a batch is smaller, and an opinion's
#: Markdown averages about 16 KB (the first 100,000 of the 2026-06-30 export), so 2,000 hold tens of MB, not GB.
_BATCH = {"court_opinion_clusters": 10_000, "court_opinion_texts": 2_000}
_DEFAULT_BATCH = 100_000

_INTEGER = "'-?[0-9]+'"
#: A decimal, optionally with an exponent: what ``float()`` reads that DuckDB reads the same way.
_DECIMAL = "'(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]{1,3})?'"
_LIST = pa.list_(pa.string())


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_type(dtype: pa.DataType) -> str:
    return ("BOOLEAN" if pa.types.is_boolean(dtype) else "BIGINT" if pa.types.is_integer(dtype)
            else "DOUBLE" if pa.types.is_floating(dtype) else "VARCHAR[]" if pa.types.is_list(dtype) else "VARCHAR")


def eligible(dataset: str, schema: pa.Schema) -> bool:
    """Whether ``dataset`` read from a file of ``schema`` is only the rules this module states in SQL."""
    if dataset not in DATASETS:
        return False
    subject = SUBJECT_SCHEMAS[dataset]
    names = schema.names
    allowed = {*subject.names, *RECEIPT_FIELDS[dataset], *LEGACY_COLUMNS[dataset]}
    prose = {"attorneys"} if dataset == "court_opinion_clusters" else set()
    return (
        len(set(names)) == len(names) and set(names) <= allowed and "conversion_inputs" not in names
        and not set(subject.names) & set(RECEIPT_FIELDS[dataset])
        and set(IDENTITIES[dataset]) <= set(names)
        and all(pa.types.is_string(field.type) or (field.name in prose and field.type.equals(_LIST)) for field in schema)
        and all(field.name in prose or pa.types.is_string(field.type) or pa.types.is_boolean(field.type)
                or pa.types.is_integer(field.type) or pa.types.is_floating(field.type) for field in subject)
    )


def _subject_sql(dataset: str, schema: pa.Schema) -> tuple[list[str], list[str], list[tuple[str, str]]]:
    """Subject columns, the flags that send a row to the row function, and the original-value items of ``conversion_inputs``."""
    present = set(schema.names)
    columns, flags, originals = [], [], []
    for field in SUBJECT_SCHEMAS[dataset]:
        name, column, dtype = field.name, _q(field.name), field.type
        if name not in present:
            columns.append(f"CAST(NULL AS {_sql_type(dtype)}) AS {column}")
            if name == "attorneys":  # ``originals.setdefault`` records the absent value as null
                originals.append((name, '\'["attorneys",["null",null]]\''))
            continue
        if pa.types.is_list(dtype):  # the clusters' prose: one element, or the list as given
            given = schema.field(name).type
            columns.append(f"{column if pa.types.is_list(given) else f'CASE WHEN {column} IS NULL THEN NULL ELSE [{column}] END'} AS {column}")
            originals.append((name, f"'[\"{name}\",' || {etl_bulk.exact_json_sql(column, given)} || ']'"))
            continue
        if pa.types.is_boolean(dtype):
            columns.append(f"CASE WHEN lower({column}) IN ('t', 'true') THEN true WHEN lower({column}) IN ('f', 'false') THEN false END AS {column}")
            flags.append(f"({column} IS NOT NULL AND lower({column}) NOT IN ('t', 'true', 'f', 'false'))")
        elif pa.types.is_integer(dtype):
            columns.append(f"TRY_CAST({column} AS BIGINT) AS {column}")
            flags.append(f"({column} IS NOT NULL AND NOT coalesce(regexp_full_match({column}, {_INTEGER}) "
                         f"AND TRY_CAST({column} AS BIGINT) IS NOT NULL, false))")
        elif pa.types.is_floating(dtype):
            columns.append(f"TRY_CAST({column} AS DOUBLE) AS {column}")
            flags.append(f"({column} IS NOT NULL AND NOT coalesce(regexp_full_match({column}, {_DECIMAL}) "
                         f"AND TRY_CAST({column} AS DOUBLE) BETWEEN 0 AND 1, false))")
        else:
            columns.append(f"{column} AS {column}")
            continue
        # A converted value keeps its source text; a null one has none.
        originals.append((name, f"CASE WHEN {column} IS NOT NULL THEN '[\"{name}\",' || {etl_bulk.exact_json_sql(column, pa.string())} || ']' END"))
    for key in IDENTITIES[dataset]:
        flags.append(f"({_q(key)} IS NULL OR {_q(key)} = '')")
    return columns, flags, originals


def _processing_sql(dataset: str, schema: pa.Schema, originals: list[tuple[str, str]]) -> str:
    """``exact_json`` of ``{receipt fields present, conversion_inputs}``; the originals sorted by key."""
    items = {name: etl_bulk.exact_json_sql(_q(name), schema.field(name).type)
             for name in RECEIPT_FIELDS[dataset] if name in schema.names}
    items["conversion_inputs"] = "'[\"dict\",[' || concat_ws(',', " + ", ".join(item for _, item in sorted(originals)) + ") || ']]'" if originals else "'[\"dict\",[]]'"
    return "'[\"dict\",[' || " + " || ',' || ".join(
        f"'[\"{name}\",' || {value} || ']'" for name, value in sorted(items.items())) + " || ']]'"


#: One row per source row the published table does not hold as the publisher spelled it (ss-vse3): ``refused`` (the
#: row function refused it; it is left out) or ``respelled`` (written, but a converted value's publisher text is not
#: the canonical spelling of the typed value). ``source_record`` is the staged source row as JSON text.
EXCEPTION_SCHEMA = pa.schema([
    ("dataset", pa.string()), ("ordinal", pa.int64()), ("outcome", pa.string()), ("reason", pa.string()),
    ("source_record", pa.string()),
])


#: Exception rows one table may record before its build stops. Every live receipt of the 2026 exports held the
#: canonical spelling (all 10.8M opinions and 85M map and parenthetical rows read on review), so more than this means
#: the export's spelling changed wholesale: refuse rather than send every row through Python, and review it.
MAX_EXCEPTIONS = 1000


def _canonical_sql(column: str, dtype: pa.DataType) -> str | None:
    """Whether a source text is the one spelling of its typed value that the published table implies.

    PostgreSQL's CSV export, the publisher's, writes ``t``/``f``, decimal integers without leading zeros and the
    shortest round-trip double without a trailing ``.0``; those spellings are recoverable from the typed column,
    so only another spelling needs a record of its own.
    """
    if pa.types.is_boolean(dtype):
        return f"{column} IN ('t', 'f')"
    if pa.types.is_integer(dtype):
        return f"regexp_full_match({column}, '0|-?[1-9][0-9]*')"
    if pa.types.is_floating(dtype):
        return f"{column} = regexp_replace(CAST(TRY_CAST({column} AS DOUBLE) AS VARCHAR), '\\.0$', '')"
    return None


def _canonical_text(value) -> str:
    """The canonical spelling :func:`_canonical_sql` accepts, for a typed value the row function returned."""
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float):
        return repr(value).removesuffix(".0")
    return str(value)


def write_table(dataset: str, source: Path, path: Path, *, constants: Mapping[str, str], exceptions: Path,
                attempt: Callable[[Mapping], dict]) -> tuple[int, int]:
    """Write ``dataset``'s published table from a staged source with no receipts; return (rows, exception records).

    The mapping is the receipt route's (:func:`_subject_sql`): the rows SQL decides are typed there, and a row it
    flags goes to ``attempt`` (``court_subjects.normalize_court_row``), which may refuse it. ``constants`` are columns
    every row carries (``source_sha256``: the export file the rows came from). Rows keep the source's order, in
    row groups of the receipt route's batch. Every refused or respelled row is written to ``exceptions``
    (:data:`EXCEPTION_SCHEMA`), which exists only when it holds a row; more than :data:`MAX_EXCEPTIONS` stop the build
    before their batch reaches Python. The written table must hold each identity once (the receipt route's admission
    refused a repeat), or the build stops; the caller keeps the previous table.
    """
    subject = SUBJECT_SCHEMAS[dataset]
    batch_rows = _BATCH.get(dataset, _DEFAULT_BATCH)
    written = recorded = 0
    with pq.ParquetFile(source) as parquet:
        stamped = pa.schema([*parquet.schema_arrow, *((name, pa.string()) for name in constants)])
        if not eligible(dataset, stamped) or set(constants) - set(subject.names):
            raise etl_bulk.NotBulkEligible(f"{dataset}: the source schema has columns the court writer does not map")
        columns, flags, _ = _subject_sql(dataset, stamped)
        checks = [(_q(field.name), _canonical_sql(_q(field.name), field.type))
                  for field in subject if field.name in stamped.names]
        respelled = " OR ".join(f"({column} IS NOT NULL AND NOT coalesce({check}, false))"
                                for column, check in checks if check) or "false"
        # Ordered by the batch's own row numbers: the published order is the source's.
        query = (f"SELECT {', '.join(columns)}, ({' OR '.join(flags)}) AS _bad, ({respelled}) AS _respelled "
                 f"FROM {etl_bulk.SOURCE_BATCH} ORDER BY _o")
        exception_rows: list[dict] = []
        with ExitStack() as stack:
            con, _ = stack.enter_context(etl_bulk.bulk_connection(path.parent))
            writer = stack.enter_context(pq.ParquetWriter(path, subject, compression="zstd"))
            failures: pq.ParquetWriter | None = None

            def flush() -> None:
                nonlocal failures, recorded
                if exception_rows:
                    if failures is None:
                        failures = stack.enter_context(pq.ParquetWriter(exceptions, EXCEPTION_SCHEMA))
                    failures.write_table(pa.Table.from_pylist(exception_rows, schema=EXCEPTION_SCHEMA))
                    recorded += len(exception_rows)
                    exception_rows.clear()

            ordinal = 0
            for batch in parquet.iter_batches(batch_size=batch_rows):
                checkpoint()
                table = pa.Table.from_batches([batch])
                for name, value in constants.items():
                    table = table.append_column(name, pa.array([value] * table.num_rows, pa.string()))
                numbered = table.append_column("_o", pa.array(range(table.num_rows), pa.int64()))
                con.register(etl_bulk.SOURCE_BATCH, numbered)
                try:
                    result = con.execute(query).to_arrow_table()
                finally:
                    con.unregister(etl_bulk.SOURCE_BATCH)
                if result.num_rows != table.num_rows:
                    raise RuntimeError(f"{dataset}: the court writer read {result.num_rows} of {table.num_rows} rows")
                bad = result["_bad"]
                decided: dict[int, dict | None] = {}
                # Only flagged rows are visited in Python (none in the 2026 exports); the rest stay in Arrow.
                flagged = pc.call_function("or", [bad, result["_respelled"]])
                routed = pc.call_function("indices_nonzero", [flagged]).to_pylist()
                if recorded + len(exception_rows) + len(routed) > MAX_EXCEPTIONS:
                    raise RuntimeError(f"{dataset}: more than {MAX_EXCEPTIONS:,} rows the mapping refuses or the "
                                       "publisher spelled other than canonically; review the export before building it")
                for index in routed:
                    flag = bad[index].as_py()
                    raw = table.slice(index, 1).to_pylist()[0]
                    record = {"dataset": dataset, "ordinal": ordinal + index,
                              "source_record": json.dumps(raw, ensure_ascii=False, sort_keys=True)}
                    if not flag:
                        exception_rows.append({**record, "outcome": "respelled",
                                               "reason": "a converted value is not spelled canonically"})
                        continue
                    try:
                        mapped = attempt(raw)
                    except REFUSED as error:
                        decided[index] = None
                        exception_rows.append({**record, "outcome": "refused",
                                               "reason": f"{type(error).__name__}: {error}"})
                        continue
                    decided[index] = {name: mapped[name] for name in subject.names}
                    originals = mapped.get("conversion_inputs") or {}
                    if any(isinstance(text, str) and text != _canonical_text(mapped[name])
                           for name, text in originals.items() if mapped.get(name) is not None
                           and not isinstance(mapped[name], list)):
                        exception_rows.append({**record, "outcome": "respelled",
                                               "reason": "a converted value is not spelled canonically"})
                proven = pc.call_function("invert", [bad])
                kept = result.filter(proven).select(subject.names).cast(subject)
                if decided:
                    kept = etl_bulk.with_insertions(kept, proven.to_pylist(), decided)
                if kept.num_rows:
                    writer.write_table(kept)
                written += kept.num_rows
                ordinal += table.num_rows
                flush()
            if ordinal != parquet.metadata.num_rows:
                raise RuntimeError(f"{dataset}: the court writer read {ordinal:,} of {parquet.metadata.num_rows:,} rows")
            writer.close()
            keys = ", ".join(_q(name) for name in IDENTITIES[dataset])
            repeated = con.execute(
                f"SELECT {keys}, count(*) FROM read_parquet({etl_bulk._literal(str(path))}) GROUP BY ALL "
                f"HAVING count(*) > 1 ORDER BY ALL LIMIT 5").fetchall()
            if repeated:
                raise ValueError(f"{dataset}: the export repeats an identity ({', '.join(IDENTITIES[dataset])}), "
                                 f"for example {repeated}")
    return written, recorded


def write_bundle(
    dataset: str,
    source: Path | pa.Table,
    subject_path: Path,
    receipt_path: Path,
    *,
    policy: DatasetPolicy,
    generation_id: str,
    processor: str,
    witnesses: Sequence[Mapping],
    diagnostics: Mapping,
    attempt_prefix: str,
    attempt: Callable[[Mapping, int], tuple[dict | None, dict]],
    last_receipts: Iterable[Mapping] = (),
    receipt_writer: pq.ParquetWriter | None = None,
    subject_writer: pq.ParquetWriter | None = None,
    segments: Callable[[pa.Table], Iterable[pa.Table]] | None = None,
) -> int:
    """Write ``dataset``'s subject table and receipts to the two paths; return how many rows the row function decided.

    ``attempt(raw_row, ordinal)`` is the row writer's own step for one source row. Receipts that are not accepted
    follow every accepted one, as the row writer orders them. ``receipt_writer``, ``subject_writer`` and ``segments``
    pass through to ``etl_bulk.write_split``.
    """
    lit = etl_bulk._literal
    if isinstance(source, pa.Table):
        schema, expected = source.schema, source.num_rows
    else:
        source = Path(source)
        with pq.ParquetFile(source) as file:
            schema, expected = file.schema_arrow, file.metadata.num_rows
    if not eligible(dataset, schema):
        raise etl_bulk.NotBulkEligible(f"{dataset}: the source schema has columns the court bulk writer does not map")
    # The shared checks on the constants (witness shape and digest, nonempty identities).
    ReceiptContext(generation_id, f"{dataset}:{attempt_prefix}:0", processor, [dict(w) for w in witnesses], diagnostics)
    refuse_local_witnesses(witnesses)  # SQL builds these receipts, so ``etl_receipts._receipt`` never sees them

    columns, flags, originals = _subject_sql(dataset, schema)
    processing = _processing_sql(dataset, schema, originals)
    record_id, _, identity = etl_bulk.identity_sql(policy)
    # ``subject_version`` hashes the subject text that is also checked for escapes: spell it once.
    version = etl_bulk.digest_sql(lit(f'["list",[["str",{json.dumps(dataset)}],') + " || _subject_json || ']]'")
    query = f"""
    WITH read AS (SELECT {', '.join(columns)}, {processing} AS processing_json,
                         ({' OR '.join(flags)}) AS _bad, file_row_number AS _ordinal
                  FROM {etl_bulk.SOURCE_BATCH}),
    mapped AS (SELECT *, {etl_bulk.record_json_sql((field.name, field.type) for field in policy.subject_schema)} AS _subject_json
               FROM read),
    keyed AS (SELECT *, {record_id} AS record_id, {version} AS subject_version, {identity} AS identity_json,
                     {lit(dataset + ':' + attempt_prefix + ':')} || _ordinal AS attempt_id,
                     {lit(dataset)} AS dataset, {lit(policy.policy_version)} AS policy_version,
                     {lit(generation_id)} AS generation_id, 'accepted' AS outcome, {lit(processor)} AS processor,
                     {lit(exact_json(dict(diagnostics)))} AS diagnostic_json
              FROM mapped),
    receipt AS (SELECT * FROM keyed CROSS JOIN court_constants)
    SELECT * EXCLUDE (_bad, _subject_json), {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id,
           (_bad OR {etl_bulk.needs_reference_sql('_subject_json')}
            OR {etl_bulk.needs_reference_sql('processing_json')}
            OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference
    FROM receipt"""
    with etl_bulk.bulk_connection() as (con, _):
        con.register("court_constants", pa.table({"witnesses": pa.array(
            [[dict(w) for w in witnesses]], type=pa.list_(WITNESS_TYPE))}))
        return etl_bulk.write_split(
            con, query, source=source, batch_rows=_BATCH.get(dataset, _DEFAULT_BATCH), expected_rows=expected,
            subject_schema=policy.subject_schema, subject_path=subject_path, receipt_path=receipt_path,
            receipt_writer=receipt_writer, subject_writer=subject_writer, segments=segments,
            label=dataset, refusals_last=True, last_refusals=last_receipts,
            row_attempt=lambda table, index, source_row: attempt(source_row, table["_ordinal"][index].as_py()))
