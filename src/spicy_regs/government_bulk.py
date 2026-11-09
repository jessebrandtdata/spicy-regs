"""Government source tables a column at a time: the receipts ``migrate_outputs`` writes and the rows it restores.

``government_receipts.migrate_outputs`` maps, hashes, serializes and (before this) validated every row in Python:
1.8k rows/s, 82 minutes for lobbying's 15.9M rows. :func:`write_dataset` states the same mapping in one DuckDB query
and hands it to ``etl_bulk.write_split``; :func:`restore_legacy` reads the retained rows back from the receipts the
same way. The row code stays the reference: a row SQL cannot prove (an escape DuckDB spells differently, a value
outside the plain spellings below, a refused conversion) comes back from ``row_attempt``, the family's own function.

Each typed column is gated by the plain spelling SQL owns; any other text of that column routes the row. JSON arrays
go through an Arrow UDF that calls the mapper's own ``list_value`` (no second JSON parser).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import DatasetPolicy, _unpack, exact_json
from spicy_regs.native_types import described_schema, extra_field_checker
from spicy_regs.transforms.government_source_shapes import (
    EDGE_WHITESPACE,
    LEGACY_COLUMNS,
    STRIPPED_TEXT,
    SUBJECT_SCHEMAS,
    GovernmentShapeError,
    list_value,
)

#: Datasets whose mapper does more than pass columns through typed (FCC arrays and extractions, GAO's captions and
#: product ids): they stay on the row path.
SPECIAL = frozenset({"fcc_filings", "gao_decisions", "gao_reports", "usaspending_recipients"})
BULK_DATASETS = frozenset(SUBJECT_SCHEMAS) - SPECIAL
_BATCH = 20_000
_DIGITS = "'-?[0-9]+'"
_MONEY = "'-?[0-9]{1,32}([.][0-9]{1,6})?'"
_DAY = "'[0-9]{4}-[0-9]{2}-[0-9]{2}'"
_STAMP = (
    "'[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])[T ]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    "([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])'"
)


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _text(value) -> str:
    return "CAST(NULL AS VARCHAR)" if value is None else etl_bulk._literal(str(value))


def check_eligible(dataset: str, schema: pa.Schema) -> None:
    """Raise ``NotBulkEligible`` unless every rule the mapper applies to ``dataset`` is one this module states."""
    if dataset in SPECIAL or dataset not in SUBJECT_SCHEMAS:
        raise etl_bulk.NotBulkEligible(f"{dataset}: its mapper has rules SQL does not state here")
    if not all(pa.types.is_string(field.type) for field in schema):
        raise etl_bulk.NotBulkEligible(f"{dataset}: only text source columns are mapped here")
    if extra := set(schema.names) - set(LEGACY_COLUMNS[dataset]):
        raise etl_bulk.NotBulkEligible(f"{dataset}: unclassified fields {sorted(extra)}")
    if "source_capture_sha256" in schema.names:
        raise etl_bulk.NotBulkEligible(f"{dataset}: a captured page digest is a second witness")


def _arrow_parser(dataset: str, name: str, dtype: pa.DataType):
    result = pa.struct([("native", dtype), ("ok", pa.bool_())])

    def parse(values):
        out = []
        for text in values.to_pylist():
            try:
                if dataset == "crs_reports":
                    from spicy_regs.crs_report_detail import detail_array
                    native = detail_array(name, text)
                else:
                    native = list_value(name, text)
                # A control character in any string is spelled by DuckDB as the row code does not: the row decides.
                ok = native is None or "\\u00" not in json.dumps(native, ensure_ascii=False)
            except (GovernmentShapeError, ValueError, TypeError, RecursionError):
                native, ok = None, False
            out.append({"native": native if ok else None, "ok": ok})
        return pa.array(out, type=result)

    return parse


def _column(dataset: str, field: pa.Field, names: Sequence[str]) -> tuple[str, str | None]:
    """SQL for subject column ``field`` over the ``s_``-prefixed source columns, and the condition that routes the row."""
    name, dtype = field.name, field.type
    legacy = name + "_json" if pa.types.is_list(dtype) else name
    s = _q("s_" + legacy) if legacy in names else "CAST(NULL AS VARCHAR)"
    if pa.types.is_list(dtype):
        return f"_p_{name}.native", f"NOT _p_{name}.ok"
    if pa.types.is_integer(dtype):
        value = f"CASE WHEN regexp_full_match({s}, {_DIGITS}) THEN TRY_CAST({s} AS BIGINT) END"
    elif pa.types.is_boolean(dtype):
        value = (f"CASE {s} WHEN 'true' THEN true WHEN 'True' THEN true WHEN '1' THEN true "
                 "WHEN 'false' THEN false WHEN 'False' THEN false WHEN '0' THEN false END")
    elif pa.types.is_decimal(dtype):
        value = f"CASE WHEN regexp_full_match({s}, {_MONEY}) THEN TRY_CAST({s} AS DECIMAL(38,6)) END"
    elif pa.types.is_date(dtype):
        value = f"CASE WHEN regexp_full_match({s}, {_DAY}) AND substr({s}, 1, 4) <> '0000' THEN TRY_CAST({s} AS DATE) END"
    elif pa.types.is_timestamp(dtype):
        value = f"CASE WHEN regexp_full_match({s}, {_STAMP}) THEN TRY_CAST({s} AS TIMESTAMPTZ) END"
    elif pa.types.is_string(dtype):
        if name in STRIPPED_TEXT.get(dataset, ()):
            edges = " || ".join(f"chr({ord(c)})" for c in EDGE_WHITESPACE)
            return f"NULLIF(trim({s}, {edges}), '')", None
        return s, None
    else:
        raise etl_bulk.NotBulkEligible(f"{dataset}.{name}: no SQL mapping for {dtype}")
    flag = f"({s} IS NOT NULL AND ({value}) IS NULL)"
    if pa.types.is_timestamp(dtype):
        # exact_json_sql leaves years outside 1000-9998 (in UTC) to the row code.
        flag = f"({s} IS NOT NULL AND (({value}) IS NULL OR year(timezone('UTC', {value})) NOT BETWEEN 1000 AND 9998))"
    return value, flag


def write_dataset(
    source: Path,
    subject_path: Path,
    receipt_path: Path,
    *,
    dataset: str,
    policy: DatasetPolicy,
    generation_id: str,
    digest: str,
    first_receipts: Sequence[dict] = (),
    last_receipts: Sequence[dict] = (),
    row_attempt: Callable[[dict, int], tuple[dict | None, dict]],
) -> int:
    """Write ``dataset``'s subjects and receipts for the retained producer output ``source``; return how many rows the row code decided.

    ``row_attempt(raw_record, ordinal)`` is ``migrate_outputs``' own step for one row. Nothing is validated here: the
    generation's admission is the one validation.
    """
    source = Path(source)
    schema = pq.read_schema(source)
    check_eligible(dataset, schema)
    names = schema.names
    raw = etl_bulk.record_json_sql((name, pa.string()) for name in names)
    udfs = {field.name: field.type for field in policy.subject_schema if pa.types.is_list(field.type)}
    columns = [_column(dataset, field, names) for field in policy.subject_schema]
    flags = [flag for _, flag in columns if flag]
    if dataset == "crs_reports":
        # Source-role witnesses require the reference receipt writer. Legacy
        # rows without those witnesses keep the existing columnar path.
        flags.extend(f'{_q("s_" + name)} IS NOT NULL' for name in
                     ("list_witness_json", "detail_witness_json", "detail_attempt_witness_json") if name in names)
    # The first identity field must be present and nonempty; the others, unless declared nullable, present.
    for index, key in enumerate(policy.identity_fields):
        if index == 0:
            flags.append(f"({_q(key)} IS NULL OR {_q(key)} = '')")
        elif key not in policy.nullable_identity_fields:
            flags.append(f"{_q(key)} IS NULL")
    parsed = ", ".join(
        f"parse_{name}({_q('s_' + name + '_json') if name + '_json' in names else 'CAST(NULL AS VARCHAR)'}) AS _p_{name}"
        for name in udfs
    )
    subject_select = ", ".join(f"{value} AS {_q(field.name)}" for (value, _), field in zip(columns, policy.subject_schema, strict=True))
    record_id, version, identity = etl_bulk.identity_sql(policy)
    witness = ("[{'source_id': " + _text("spicy-regs:producer-output:" + dataset) + ", 'source_uri': CAST(NULL AS VARCHAR)"
               + ", 'sha256': " + _text(digest) + ", 'locator': 'row:' || _ordinal, 'body_version': CAST(NULL AS VARCHAR)}]")
    diagnostics = exact_json({"source_qualification": "producer-output; original capture coverage unchanged"})
    query = f"""
    WITH read AS (SELECT {''.join(f'{_q(name)} AS {_q("s_" + name)}, ' for name in names)}
                         file_row_number AS _ordinal, {raw} AS _raw FROM {etl_bulk.SOURCE_BATCH}),
    parsed AS (SELECT *{', ' + parsed if parsed else ''} FROM read),
    shaped AS (SELECT {subject_select + ', ' if subject_select else ''}_ordinal, _raw,
                      {' OR '.join(flags) if flags else 'false'} AS _reference FROM parsed),
    keyed AS (SELECT *, {record_id} AS record_id, {version} AS subject_version, {identity} AS identity_json,
                     '["dict",[["raw_record",' || _raw || ']]]' AS processing_json FROM shaped),
    receipt AS (SELECT *, {_text(dataset)} AS dataset, {_text(policy.policy_version)} AS policy_version,
                       {_text(generation_id)} AS generation_id, {_text(dataset + ":")} || _ordinal AS attempt_id,
                       'accepted' AS outcome, {_text(policy.policy_version)} AS processor, {witness} AS witnesses,
                       {_text(diagnostics)} AS diagnostic_json FROM keyed)
    SELECT {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id, * EXCLUDE (_reference),
           (_reference OR {etl_bulk.needs_reference_sql('_raw')} OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference
    FROM receipt"""
    with etl_bulk.bulk_connection() as (con, _):
        for name, dtype in udfs.items():
            sql_type = dict(described_schema(pa.schema([(name, dtype)])))[name]
            con.create_function(f"parse_{name}", _arrow_parser(dataset, name, dtype), ["VARCHAR"],
                                f"STRUCT(native {sql_type}, ok BOOLEAN)", type="arrow", null_handling="special")
        return etl_bulk.write_split(
            con, query, source=source, batch_rows=_BATCH, expected_rows=pq.ParquetFile(source).metadata.num_rows,
            subject_schema=policy.subject_schema, subject_path=subject_path, receipt_path=receipt_path, label=dataset,
            first_receipts=first_receipts, last_receipts=last_receipts, refusals_last=True,
            row_attempt=lambda table, index, source_row: row_attempt(source_row, table["_ordinal"][index].as_py()),
        )


def restore_legacy(
    subject: Path, receipts: Path, destination: Path, *, dataset: str, policy: DatasetPolicy, generation_id: str,
    schema: pa.Schema,
) -> int:
    """Write the retained ``raw_record`` of every accepted receipt, in subject order, as ``schema`` rows.

    One admission of the pair, then one scan of the receipts. Equal to the rows ``read_with_receipts`` yields:
    ``subject | raw_record``'s ``raw_record``. A receipt whose text is not the plain shape (all columns, text or NULL,
    spelled as SQL spells them) is decoded by Python; returns how many were. ``receipts`` hold ``dataset``'s only.
    """
    if not all(pa.types.is_string(field.type) for field in schema):
        raise etl_bulk.NotBulkEligible(f"{dataset}: only text source columns are restored here")
    with etl_bulk._validated_bundle({dataset: [subject]}, [receipts], [policy], generation_id=generation_id) as con:
        [(backwards,)] = con.execute(
            "SELECT count(*) FROM (SELECT receipt, lag(receipt) OVER (ORDER BY n) AS p FROM subject_receipts)"
            " WHERE p IS NOT NULL AND receipt <= p"
        ).fetchall()
    if backwards:
        raise etl_bulk.NotBulkEligible(f"{dataset}: accepted receipts are not in subject order")
    names = sorted(schema.names)
    pairs = "$[1][0][1][1]"
    extracted = ", ".join(f"json_extract_string(_pj, '{pairs}[{i}][1][1]') AS {_q(name)}" for i, name in enumerate(names))
    encoded = etl_bulk.record_json_sql((name, pa.string()) for name in names)
    check_fields = extra_field_checker(schema)
    query = f"""
    SELECT {', '.join(_q(n) for n in schema.names)}, _position, _reference, CASE WHEN _reference THEN _pj END AS _processing
    FROM (SELECT *, NOT coalesce(_pj = '["dict",[["raw_record",' || {encoded} || ']]]', false) AS _reference
          FROM (SELECT *, {extracted} FROM restore_batch)) ORDER BY _position"""
    left, position = 0, 0
    expected = pq.ParquetFile(subject).metadata.num_rows
    with etl_bulk.bulk_connection() as (con, _), TemporaryDirectory(dir=Path(destination).parent) as scratch:
        temporary = Path(scratch) / "rows.parquet"
        written = 0
        with pq.ParquetFile(receipts) as source, pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            for piece in source.iter_batches(batch_size=_BATCH, columns=["outcome", "processing_json"]):
                keep = [o == "accepted" for o in piece.column("outcome").to_pylist()]
                batch = pa.table({
                    "_pj": piece.column("processing_json"),
                    "_position": pa.array(range(position, position + piece.num_rows), pa.int64()),
                }).filter(pa.array(keep))
                position += piece.num_rows
                con.register("restore_batch", batch)
                try:
                    table = con.execute(query).to_arrow_reader(_BATCH).read_all()
                finally:
                    con.unregister("restore_batch")
                flagged = table["_reference"].to_pylist()
                keep = pa.array([not f for f in flagged], pa.bool_())
                rows = table.filter(keep).select(schema.names).cast(schema)
                if any(flagged):
                    decoded = {}
                    for index, text in ((i, t) for i, t in enumerate(table["_processing"].to_pylist()) if flagged[i]):
                        fields = _unpack(json.loads(text))["raw_record"]
                        check_fields(fields)
                        decoded[index] = dict(fields)
                    left += len(decoded)
                    rows = etl_bulk.with_insertions(rows, [not f for f in flagged], decoded)
                if rows.num_rows:
                    writer.write_table(rows)
                    written += rows.num_rows
        if written != expected:
            raise RuntimeError(f"{dataset}: restored {written} of the subject's {expected} rows")
        temporary.replace(destination)
    return left
