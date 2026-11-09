"""Flat Congress datasets a column at a time: the bundle the row writer writes and the table it restores.

``congress_receipts.write_congress_dataset`` maps, splits, serializes and validates one row at a time, and
``restore_processing_input`` decodes one receipt at a time, over several passes. For ``member_votes`` and
``member_vote_terms`` (10.56M rows each on 2026-10-05) that was about 3.3 hours a table against a 60-minute job.
:func:`write_bundle` and :func:`restore_input` give the same receipts and the same restored rows from ordered source
batches. The original flat-table benchmark on the first million rows of both tables checked every receipt at the
same position and the restored table against its source: 5.5 to 8.4 microseconds a row against 577 to 692.

The row code stays the reference and keeps the rows SQL cannot own: a row whose text ``etl_bulk`` spells differently
from Python, and a row the mapper would refuse. Those come back from the ``row_receipt`` and ``row_fields`` callables
the caller passes, which are the row code's own functions, so each rule lives in one place. Such a row's own text
rides along in the one scan. Nothing here runs a second statement on a connection that is still streaming: that ends
the stream without an error, and a bundle cut short that way is consistent with itself (2026-10-05: 199,999 of
1,000,000 rows restored, nothing raised). So each scan also checks that its rows arrive in file order and that it read
as many as the file's own footer counts.

A dataset is taken only when :func:`eligible` says every rule the mapper applies to it is one of the three this
module states in SQL: a column passed through as text, a digit string read as an integer, and a row given no subject.
"""

from __future__ import annotations

import json

from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.congress_subjects import (
    BOOLEANS,
    FLATTEN,
    INPUT_COLUMNS,
    INTEGERS,
    JOINED,
    NATIVE,
    PROCESSING,
    RECEIPT_ONLY,
    subject_schema,
    target_name,
)
from spicy_regs.etl_receipts import WITNESS_TYPE, DatasetPolicy, refuse_local_witnesses

#: ``congress_subjects.integer`` reads an ASCII digit string; 18 digits always fit the subject's int64.
_DIGITS = "'[0-9]{1,18}'"
#: The rows ``congress_subjects.map_record`` gives no subject, as SQL over the source columns. A test holds each to
#: the mapper's own answer.
NO_SUBJECT = {"member_vote_terms": '"term_index" IS NULL AND "term_match" IS NOT NULL'}
#: Rows held in memory at a time; a receipt is about a kilobyte.
_BATCH = 100_000


def eligible(dataset: str, source: pa.Schema | None = None) -> bool:
    """Whether every rule the mapper applies to ``dataset`` is one this module states in SQL.

    Without ``source`` the answer is for the dataset alone, before its source schema is known.
    """
    source = pa.schema([]) if source is None else source
    if dataset not in INPUT_COLUMNS or dataset in RECEIPT_ONLY or dataset == "public_activity_events":
        return False
    special = {name for table, name in (*NATIVE, *FLATTEN, *JOINED) if table == dataset} | set(BOOLEANS.get(dataset, ()))
    inputs = INPUT_COLUMNS[dataset]
    subject = subject_schema(dataset)
    integers = {field.name for field in subject if pa.types.is_integer(field.type)}
    return (
        not special
        and set(source.names) <= set(inputs)
        and all(pa.types.is_string(field.type) for field in source)
        and all(target_name(dataset, name) == name for name in inputs)
        and all(pa.types.is_string(field.type) or pa.types.is_integer(field.type) for field in subject)
        and integers == set(INTEGERS.get(dataset, ())) & set(subject.names)
        and set(subject.names) == set(inputs) - set(PROCESSING.get(dataset, ()))
    )


def _text(value: Any) -> str:
    return "CAST(NULL AS VARCHAR)" if value is None else "'" + str(value).replace("'", "''") + "'"


def _witnesses_sql(first: Mapping[str, Any], rest: Sequence[Mapping[str, Any]]) -> str:
    """A list of witness structs: the row's own, whose locator is its ordinal, then the caller's."""
    refuse_local_witnesses([first, *rest])
    stored = pa.array([list(rest)], type=pa.list_(WITNESS_TYPE)).to_pylist()[0]
    own = ", ".join(f"'{name}': " + ("CAST(_ordinal AS VARCHAR)" if name == "locator" else _text(first.get(name))) for name in WITNESS_TYPE.names)
    others = ["{" + ", ".join(f"'{name}': {_text(witness[name])}" for name in WITNESS_TYPE.names) + "}" for witness in stored]
    return "[" + ", ".join(["{" + own + "}", *others]) + "]"


def _source_sql(source: Path, file_schema: pa.Schema, dataset: str, policy: DatasetPolicy, context_reference: bool = False,
                table: str | None = None) -> str:
    """The source rows, each with its ordinal, its exact text, whether it has no subject, and whether it is the row code's.

    They come from ``table`` (a registered batch of the source) or, without it, from the file.
    """
    integers = [field.name for field in policy.subject_schema if pa.types.is_integer(field.type) and field.name in file_schema.names]
    bad_integer = " OR ".join(f'("{name}" IS NOT NULL AND NOT regexp_full_match("{name}", {_DIGITS}))' for name in integers) or "false"
    no_identity = " OR ".join(
        f'"{name}" IS NULL' if name in file_schema.names else "true" for name in policy.identity_fields if name not in policy.nullable_identity_fields
    ) or "false"
    no_subject = NO_SUBJECT.get(dataset, "false")
    for name in INPUT_COLUMNS[dataset]:
        if name not in file_schema.names:
            no_subject = no_subject.replace('"' + name + '"', 'CAST(NULL AS VARCHAR)')
    raw = etl_bulk.record_json_sql((name, file_schema.field(name).type) for name in file_schema.names)
    return f"""SELECT *, ((NOT _rejected AND (({no_identity}) OR ({bad_integer}))) OR {etl_bulk.needs_reference_sql("_raw")} OR {str(context_reference).lower()}) AS _reference
               FROM (SELECT *, file_row_number AS _ordinal, {raw} AS _raw, ({no_subject}) AS _rejected
                     FROM {table or f"read_parquet({_text(source)}, file_row_number = true)"})"""


def _context_reference(source, dataset, policy, generation_id, processor, witnesses) -> bool:
    """Context text with a control escape SQL spells differently routes every row."""
    controls = "\x0b\x0e\x0f\x1a\x1b\x1c\x1d\x1e\x1f"
    values = [str(source), dataset, policy.policy_version, generation_id, processor,
              *(value for witness in witnesses for value in witness.values())]
    return any(isinstance(value, str) and any(char in value for char in controls) for value in values)


def iter_routed_ordinals(source: Path, *, dataset: str, policy: DatasetPolicy,
                         generation_id: str | None = None, processor: str | None = None,
                         witnesses: Sequence[Mapping[str, Any]] = ()) -> Iterator[int]:
    """Every routed source ordinal in file order, with bounded memory.

    Supply the write's context to include routes caused by witness or publisher text.
    """
    source = Path(source).resolve()
    reference = _context_reference(source, dataset, policy, generation_id, processor, witnesses)
    with etl_bulk.bulk_connection() as (con, _):
        query = f"SELECT _ordinal FROM ({_source_sql(source, pq.read_schema(source), dataset, policy, reference)}) WHERE _reference ORDER BY _ordinal"
        for batch in con.execute(query).to_arrow_reader(_BATCH):
            yield from batch.column(0).to_pylist()


def routed_ordinals(source: Path, *, dataset: str, policy: DatasetPolicy, **context) -> list[int]:
    """List wrapper for bounded samples; large qualification uses ``iter_routed_ordinals``."""
    return list(iter_routed_ordinals(source, dataset=dataset, policy=policy, **context))


def _rows_in(path: Path, dataset: str | None = None) -> int:
    """How many rows the file itself counts, or how many of ``dataset``'s receipts: read apart from the scan it checks."""
    import pyarrow.dataset as ds

    if dataset is None:
        return pq.ParquetFile(path).metadata.num_rows
    return ds.dataset(path, format="parquet").count_rows(filter=ds.field("dataset") == dataset)


def write_bundle(
    source: Path,
    subjects_path: Path,
    receipts_path: Path,
    *,
    dataset: str,
    policy: DatasetPolicy,
    generation_id: str,
    processor: str,
    digest: str,
    first_receipts: Sequence[Mapping[str, Any]],
    row_receipt: Callable[[str, int], tuple[dict | None, dict]],
    witnesses: Sequence[Mapping[str, Any]] = (),
) -> int:
    """Write ``dataset``'s subject table and receipts to the two paths; return how many rows went to the row code.

    ``digest`` is the source file's, as the row writer computes it. ``first_receipts`` are written ahead of the rows
    (the table-metadata receipt). ``row_receipt(processing_json, ordinal)`` is the row writer's own step for one
    source row, which it reads back out of the processing text built here.
    """
    source = Path(source).resolve()
    file_schema = pq.read_schema(source)
    subject = policy.subject_schema
    integers = {field.name for field in subject if pa.types.is_integer(field.type)}
    columns = ", ".join(
        f'CAST(NULL AS VARCHAR) AS "{field.name}"' if field.name not in file_schema.names
        else f'CASE WHEN regexp_full_match("{field.name}", {_DIGITS}) THEN CAST("{field.name}" AS BIGINT) END AS "{field.name}"' if field.name in integers
        else f'"{field.name}"'
        for field in subject
    )
    record_id, version, identity = etl_bulk.identity_sql(policy)
    own_witness = {"source_id": "shaped-observation:" + dataset, "source_uri": None, "sha256": digest, "body_version": None}
    reference = _context_reference(source, dataset, policy, generation_id, processor, witnesses)
    query = f"""
    WITH read AS ({_source_sql(source, file_schema, dataset, policy, reference, etl_bulk.SOURCE_BATCH)}),
    shaped AS (SELECT {columns}, _ordinal, _raw, _rejected, _reference FROM read),
    keyed AS (SELECT *, CASE WHEN NOT _rejected THEN {record_id} END AS record_id, CASE WHEN NOT _rejected THEN {version} END AS subject_version,
                     CASE WHEN NOT _rejected THEN {identity} END AS identity_json,
                     '["dict",[["entry_kind",["str","row"]],["source_fields",' || _raw || ']]]' AS processing_json FROM shaped),
    receipt AS (SELECT * EXCLUDE (_raw), {_text(dataset)} AS dataset, {_text(policy.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
                       {_text(dataset + ":" + digest + ":")} || _ordinal AS attempt_id, CASE WHEN _rejected THEN 'rejected' ELSE 'accepted' END AS outcome,
                       {_text(processor)} AS processor, {_witnesses_sql(own_witness, witnesses)} AS witnesses,
                       CASE WHEN _rejected THEN '["dict",[["reason",["str","no_domain_subject"]]]]' ELSE '["dict",[]]' END AS diagnostic_json FROM keyed)
    SELECT {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id, * EXCLUDE (_reference),
           (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference,
           NOT (_rejected OR _reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _accepted FROM receipt"""
    with etl_bulk.bulk_connection() as (con, _):
        return etl_bulk.write_split(
            con, query, source=source, batch_rows=_BATCH, expected_rows=_rows_in(source), subject_schema=subject,
            subject_path=subjects_path, receipt_path=receipts_path, label=dataset, first_receipts=first_receipts,
            row_attempt=lambda table, index, row: row_receipt(table["processing_json"][index].as_py(),
                                                         table["_ordinal"][index].as_py()),
        )


def mapped_eligible(source: pa.Schema, selected: DatasetPolicy) -> bool:
    """The shared encoder must own both raw and mapped Arrow value types."""
    try:
        etl_bulk.exact_json_sql("raw", pa.struct(source))
        if not selected.receipt_only:
            etl_bulk.identity_sql(selected)
        return bool(source.names)
    except NotImplementedError:
        return False


def write_mapped_bundle(
    source: Path,
    subjects_path: Path,
    receipts_path: Path,
    *,
    policy: DatasetPolicy,
    generation_id: str,
    processor: str,
    mapper: Callable[[dict], dict | None],
    row_attempt: Callable[[dict, int], tuple[dict | None, dict]],
    processing_field: str,
    attempt_prefix: str,
    witness: Mapping[str, Any],
    witnesses: Sequence[Mapping[str, Any]] = (),
    locator_prefix: str = "",
    entry_kind: str | None = None,
    first_receipts: Sequence[Mapping[str, Any]] = (),
    refusals_last: bool = False,
) -> int:
    """Keep the maintained mapper; encode and hash its bounded Arrow batches in SQL.

    Mapping failures and values the encoder cannot prove return to the exact
    row attempt. The source bytes, order, context and refusal order are the
    same as that reference. This uses ``write_split``'s one ordered batch loop.
    """
    source = Path(source).resolve()
    schema = pq.read_schema(source)
    native = policy.subject_schema
    raw_type = pa.struct(schema)
    result_type = pa.struct([*native, pa.field("_reference", pa.bool_())])

    def mapped(values):
        rows = []
        for raw in values.to_pylist():
            try:
                subject = mapper(raw)
                reference = (subject is None and not policy.receipt_only) or (
                    subject is not None and any(subject.get(key) is None for key in policy.identity_fields
                                                if key not in policy.nullable_identity_fields))
                rows.append((subject or dict.fromkeys(native.names)) | {"_reference": reference})
            except (ValueError, TypeError, OverflowError, pa.ArrowException):
                rows.append(dict.fromkeys(native.names) | {"_reference": True})
        return pa.array(rows, type=result_type)

    columns = ", ".join('_mapped."' + field.name + '" AS "' + field.name + '"' for field in native)
    raw = etl_bulk.exact_json_sql("_source", raw_type)
    # Key ordering is exact_json's ordering; these two wrappers are the only
    # processing shapes this writer owns.
    if entry_kind is None:
        processing = _text('["dict",[[' + json.dumps(processing_field) + ',') + " || _raw || ']]]'"
    else:
        processing = _text('["dict",[["entry_kind",["str",' + json.dumps(entry_kind) + ']],["source_fields",') + " || _raw || ']]]'"
    subject_reference = ("false" if policy.receipt_only else etl_bulk.needs_reference_sql(
        etl_bulk.record_json_sql((field.name, field.type) for field in native)))
    keyed = ("NULL AS record_id, NULL AS subject_version, NULL AS identity_json" if policy.receipt_only else
             ", ".join(f"{value} AS {name}" for value, name in zip(etl_bulk.identity_sql(policy),
                         ("record_id", "subject_version", "identity_json"), strict=True)))
    # A native subject may have its own witnesses field; keep that distinct
    # from the receipt's witnesses after computing the subject digest.
    conflicts = set(native.names) & {"witnesses"}
    renamed = ", ".join(f'"{name}" AS "_subject_{name}"' for name in sorted(conflicts))
    keep = "* EXCLUDE (_raw" + "".join(', "' + name + '"' for name in conflicts) + ")"
    own = _witnesses_sql(witness, witnesses).replace("CAST(_ordinal AS VARCHAR)",
                                                    _text(locator_prefix) + " || CAST(_ordinal AS VARCHAR)")
    context_reference = _context_reference(source, policy.dataset, policy, generation_id, processor,
                                           [witness, *witnesses]) or any(
        char in attempt_prefix + locator_prefix for char in "\x0b\x0e\x0f\x1a\x1b\x1c\x1d\x1e\x1f")
    quoted = ['"' + name.replace('"', '""') + '"' for name in schema.names]
    source_columns = ", ".join(f"{name} := {name}" for name in quoted)
    query = f"""WITH input AS (
        SELECT struct_pack({source_columns}) AS _source,
               file_row_number AS _ordinal FROM {etl_bulk.SOURCE_BATCH}),
    mapped AS MATERIALIZED (SELECT *, _map_record(_source) AS _mapped FROM input),
    shaped AS (SELECT {columns + ',' if columns else ''} _ordinal, {raw} AS _raw,
                       (_mapped._reference OR {str(context_reference).lower()} OR {etl_bulk.needs_reference_sql(raw)}) AS _reference FROM mapped),
    keyed AS (SELECT * EXCLUDE (_reference), (_reference OR {subject_reference}) AS _reference,
                     {keyed}, {processing} AS processing_json FROM shaped),
    receipt AS (SELECT {renamed + ',' if renamed else ''} {keep}, {_text(policy.dataset)} AS dataset,
                       {_text(policy.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
                       {_text(attempt_prefix)} || _ordinal AS attempt_id,
                       {_text('observed' if policy.receipt_only else 'accepted')} AS outcome,
                       {_text(processor)} AS processor, {own} AS witnesses, '["dict",[]]' AS diagnostic_json FROM keyed)
    SELECT {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id, * EXCLUDE (_reference),
           (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference,
           {str(not policy.receipt_only).lower()} AND NOT (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _accepted
    FROM receipt"""
    with etl_bulk.bulk_connection() as (con, _):
        def sql_type(dtype):
            return con.from_arrow(pa.table({"value": pa.array([], type=dtype)})).types[0]
        con.create_function("_map_record", mapped, [sql_type(raw_type)], sql_type(result_type), type="arrow")
        return etl_bulk.write_split(
            con, query, source=source, batch_rows=2000, expected_rows=_rows_in(source), subject_schema=native,
            subject_path=subjects_path, receipt_path=receipts_path, label=policy.dataset, first_receipts=first_receipts,
            row_attempt=lambda table, index, raw: row_attempt(raw, table["_ordinal"][index].as_py()),
            refusals_last=refusals_last,
            subject_columns=["_subject_" + name if name in conflicts else name for name in native.names],
        )


#: How ``exact_json`` spells a table-metadata entry, at any depth.
_FOOTER_ENTRY = '["entry_kind",["str","table_metadata"]]'
#: How it opens a row attempt that holds nothing but its source fields.
_ROW_OPENING = '["dict",[["entry_kind",["str","row"]],["source_fields",["dict",['


def footer_candidates(receipts: Path, *, dataset: str) -> list[str]:
    """The processing text of every receipt of ``dataset`` that could be a table-metadata entry, in file order.

    A superset: the text is matched at any depth, so the caller decodes each and keeps the ones that are.
    """
    with etl_bulk.bulk_connection() as (con, _):
        return [text for (text,) in con.execute(
            f"SELECT processing_json FROM read_parquet({_text(receipts)}, file_row_number = true) "
            f"WHERE dataset = {_text(dataset)} AND contains(processing_json, {_text(_FOOTER_ENTRY)}) ORDER BY file_row_number"
        ).fetchall()]


def _source_fields_values_sql(schema: pa.Schema) -> str:
    # One list-of-paths extraction shares the JSON parse across names, type tags and values. Repeated extraction
    # exhausted the configured memory limit on House communications before yielding its first restored batch.
    paths = [f"$[1][1][1][1][{i}]{suffix}" for i in range(len(schema)) for suffix in ("[0]", "[1][0]", "[1][1]")]
    return "json_extract_string(processing_json, [" + ", ".join(_text(path) for path in paths) + "])"


def source_fields_sql(schema: pa.Schema, *, values: str | None = None) -> tuple[str, str]:
    """SQL over ``processing_json``: the retained source columns, and whether the text is the plain shape they read.

    Plain is a row attempt holding only ``source_fields``, with exactly ``schema``'s columns, each text or NULL.
    ``values`` may name the already extracted list from :func:`_source_fields_values_sql`.
    """
    names = sorted(schema.names)
    values = _source_fields_values_sql(schema) if values is None else values
    plain = " AND ".join(
        [f"starts_with(processing_json, {_text(_ROW_OPENING)})", "json_array_length(processing_json, '$[1]') = 2",
         f"json_array_length(processing_json, '$[1][1][1][1]') = {len(names)}"]
        + [f"{values}[{3 * i + 1}] = {_text(name)}" for i, name in enumerate(names)]
        + [f"{values}[{3 * i + 2}] IN ('str', 'null')" for i in range(len(names))]
    )
    columns = ", ".join(f'{values}[{3 * names.index(field.name) + 3}] AS "{field.name}"' for field in schema)
    return columns, plain


def restore_input(
    receipts: Path,
    destination: Path,
    *,
    dataset: str,
    schema: pa.Schema,
    row_fields: Callable[[str, str], Mapping[str, Any] | None],
) -> int:
    """Write the source rows ``dataset``'s receipts retain, in builder order, in the retained ``schema``.

    Equal to the rows ``restore_processing_input`` yields: every ``row`` attempt's ``source_fields`` except a refused
    conversion's. The caller has validated the bundle and read ``schema`` from its table-metadata receipt.
    ``row_fields(processing_json, diagnostic_json)`` is the row code's decoding of one receipt. It decides every
    receipt that is not the plain shape or whose diagnostics mention a refused conversion, and returns None to leave
    the row out. Returns how many receipts went to it.
    """
    from spicy_regs.native_types import extra_field_checker

    if not all(pa.types.is_string(field.type) for field in schema):
        raise NotImplementedError("Only text source columns are decoded here")
    check_fields = extra_field_checker(schema)
    columns, plain = source_fields_sql(schema, values="_source_values")
    query = f"""
    WITH extracted AS MATERIALIZED (
        SELECT *, {_source_fields_values_sql(schema)} AS _source_values
        FROM restore_batch WHERE dataset = {_text(dataset)})
    SELECT {columns}, file_row_number AS _position,
           (NOT coalesce({plain}, false) OR contains(diagnostic_json, 'conversion_refused')) AS _reference,
           CASE WHEN _reference THEN processing_json END AS _processing,
           CASE WHEN _reference THEN diagnostic_json END AS _diagnostic
    FROM extracted ORDER BY _position"""
    expected, seen, last, left = _rows_in(receipts, dataset), 0, -1, 0
    # As ``write_rows`` does: the destination appears only once every row is written.
    with etl_bulk.bulk_connection() as (con, _), TemporaryDirectory(dir=destination.parent) as scratch:
        temporary = Path(scratch) / "rows.parquet"
        # Sort only the current source batch. Sorting the full decoded file holds every wide value before the
        # first result, and a parallel Parquet scan without ORDER BY can reorder healthy receipts.
        with pq.ParquetFile(receipts) as source, pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            position = 0
            for source_batch in source.iter_batches(batch_size=min(_BATCH, 2000),
                                                   columns=["dataset", "processing_json", "diagnostic_json"]):
                etl_bulk.checkpoint()
                source_batch.validate(full=True)
                # Reuse the shared logical-byte slices; one large row and backing Arrow buffers can exceed them.
                for piece in etl_bulk._insert_batches(source_batch):
                    numbered = pa.Table.from_batches([piece]).append_column(
                        "file_row_number", pa.arange(position, position + piece.num_rows))
                    position += piece.num_rows
                    con.register("restore_batch", numbered)
                    try:
                        for batch in con.execute(query).to_arrow_reader(_BATCH):
                            table = pa.Table.from_batches([batch])
                            table.validate(full=True)
                            positions = table["_position"].to_pylist()
                            if positions != sorted(set(positions)) or (positions and positions[0] <= last):
                                raise RuntimeError(f"{dataset}: receipts did not arrive in file order")
                            seen, last = seen + len(positions), positions[-1] if positions else last
                            flagged = table["_reference"].to_pylist()
                            rows = table.filter(pa.array([not flag for flag in flagged])).select(schema.names).cast(schema)
                            if any(flagged):
                                processing, diagnostic, decoded = table["_processing"].to_pylist(), table["_diagnostic"].to_pylist(), {}
                                for index in (index for index, flag in enumerate(flagged) if flag):
                                    fields = row_fields(processing[index], diagnostic[index])
                                    if fields is not None:
                                        check_fields(fields)
                                    decoded[index] = None if fields is None else dict(fields)
                                left += len(decoded)
                                rows = etl_bulk.with_insertions(rows, [not flag for flag in flagged], decoded)
                            if rows.num_rows:
                                writer.write_table(rows)
                    finally:
                        # The current result is exhausted before the connection sees another statement.
                        con.unregister("restore_batch")
        if seen != expected:
            raise RuntimeError(f"{dataset}: read {seen} of the file's {expected} receipts")
        temporary.replace(destination)
    return left
