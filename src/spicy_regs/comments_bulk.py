"""Bulk comments shaping with exact raw rows and row-writer fallback.

The shared regulations mapper remains the authority for attachment parsing and
HTML text. SQL handles pass-through fields, receipt encoding and hashing. Every
unsupported row goes back to the caller's reference writer in its source order.
The catalog retains one original source input per current row. Replacements
write the new input without embedding copies of earlier inputs. Verified
processing reads normalize that input through the maintained catalog mapper.
This module writes private files; catalog commits and publication stay with the
catalog owner.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.congress_bulk import _text
from spicy_regs.etl_receipts import DatasetPolicy
from spicy_regs.native_types import described_schema
from spicy_regs.transforms.html_text import register_html_text
from spicy_regs.transforms.regulations_shape import COMMENT_ATTACHMENTS, SOURCE_COLUMNS, TYPES, _native, _validate

_BATCH = 10_000
# Bound the original strings before repeated SQL receipt encoding. This only
# selects the existing row writer; it never limits the retained source values.
_BULK_ROW_BYTES = 64 * 1024
_ATTACHMENT_RESULT = pa.struct([('native', pa.string()), ('reference', pa.bool_())])


def eligible(schema: pa.Schema) -> bool:
    """Only declared text columns and the original integer count are handled."""
    known = dict(SOURCE_COLUMNS['comments'])
    return all(field.name in known and (
        pa.types.is_string(field.type) or
        field.name == 'duplicate_comments' and pa.types.is_integer(field.type)
    ) for field in schema) and len(set(schema.names)) == len(schema.names)


def _attachments(values):
    """Use the shared strict parser; a refusal belongs to the reference attempt."""
    rows = []
    for value in values.to_pylist():
        try:
            native = _native('comments', 'attachments_json', value)
            _validate(native, COMMENT_ATTACHMENTS, 'comments.attachments')
            stored = pa.array([native], type=COMMENT_ATTACHMENTS).to_pylist()[0]
            rows.append({'native': None if stored is None else json.dumps(stored, ensure_ascii=False, separators=(',', ':')), 'reference': False})
        except (ValueError, TypeError, OverflowError, RecursionError, pa.ArrowException):
            rows.append({'native': None, 'reference': True})
    return pa.array(rows, type=_ATTACHMENT_RESULT)


def _segments(table: pa.Table):
    """Keep each oversized reference attempt out of the batch's insertions."""
    start = 0
    for index, large in enumerate(table['_large'].to_pylist()):
        if large:
            if start < index:
                yield table.slice(start, index - start)
            yield table.slice(index, 1)
            start = index + 1
    if start < len(table):
        yield table.slice(start)


def write_bundle(source: Path, subjects: Path, receipts: Path, *, policy: DatasetPolicy,
                 generation_id: str, source_label: str,
                 row_attempt: Callable[[Mapping, int], tuple[dict | None, dict]]) -> int:
    """Write a private catalog pair, retaining the original input once.

    ``row_attempt`` runs the existing catalog normalizer and regulations writer.
    The returned number counts routed rows. The caller admits the completed pair
    and decides whether any refused row stops the catalog transaction.
    """
    schema = pq.read_schema(source)
    if not eligible(schema):
        raise etl_bulk.NotBulkEligible('Comments bulk writer requires declared text/integer source columns')
    raw = etl_bulk.record_json_sql((field.name, field.type) for field in schema)
    string_bytes = ' + '.join(f'coalesce(strlen("{field.name}"), 0)'
                              for field in schema if pa.types.is_string(field.type)) or '0'
    bounded_columns = ', '.join(f'CASE WHEN _large THEN NULL ELSE "{field.name}" END AS "{field.name}"'
                                for field in schema)
    columns = []
    for name, typename in SOURCE_COLUMNS['comments']:
        expression = '"' + name + '"' if name in schema.names else 'NULL'
        if name == 'duplicate_comments':
            expression = f'TRY_CAST({expression} AS INTEGER)'
        columns.append(f'{expression} AS "{name}"')
    attachment_type = dict(described_schema(policy.subject_schema))['attachments']
    fields = ', '.join('"' + field.name + '"' for field in policy.subject_schema)
    processing = [(name, pa.string()) for name in policy.receipt_fields
                  if name not in {'raw_conversion_inputs', 'input_metadata', 'raw_source_record'}]
    # These field names are explicit policy decisions, and their order is exact_json's.
    pairs = [(name, etl_bulk.exact_json_sql('"' + name + '"', dtype)) for name, dtype in processing]
    pairs.extend([('raw_source_record', '_raw'),
                  ('input_metadata', "'[\"dict\",[]]'" )])
    payload = "'[\"dict\",[' || " + " || ',' || ".join(
        _text('[' + json.dumps(name) + ',') + ' || ' + value + " || ']'" for name, value in sorted(pairs)
    ) + " || ']]'"
    integer = '"duplicate_comments"'
    if 'duplicate_comments' in schema.names:
        integer_bad = f'(_count_raw IS NOT NULL AND (NOT regexp_full_match(CAST(_count_raw AS VARCHAR), \'[+-]?[0-9]+\') OR {integer} IS NULL))'
        count_raw = '"duplicate_comments"'
    else:
        integer_bad, count_raw = 'false', 'NULL'
    record_id, version, identity = etl_bulk.identity_sql(policy)
    subject_text = etl_bulk.record_json_sql((field.name, field.type) for field in policy.subject_schema)
    witness = "[{\"source_id\": " + _text(source_label) + ", 'source_uri': NULL::VARCHAR, 'sha256': sha256(_raw), " + \
              "'locator': 'receipt.values.raw_source_record (canonical exact_json)', 'body_version': " + _text(generation_id) + '}]'
    query = f'''
    WITH sized AS (SELECT *, file_row_number AS _ordinal, ({string_bytes}) > {_BULK_ROW_BYTES} AS _large
                   FROM {etl_bulk.SOURCE_BATCH}),
    bounded AS (SELECT {bounded_columns}, _ordinal, _large FROM sized),
    read AS (SELECT *, {raw} AS _raw, {count_raw} AS _count_raw FROM bounded),
    normalized AS (SELECT {', '.join(columns)}, _ordinal, _large, _raw, _count_raw FROM read),
    shaped AS (SELECT *, comments_attachments(attachments_json) AS _attachment,
               html_text(comment) AS comment_text FROM normalized),
    subjects AS (SELECT *, CAST(CAST(_attachment.native AS JSON) AS {attachment_type}) AS attachments FROM shaped),
    receipt AS (SELECT {fields}, _ordinal, _large, _raw,
               (_large OR _attachment.reference OR {integer_bad} OR comment_id IS NULL OR NOT comments_identity(comment_id)
                OR {etl_bulk.needs_reference_sql("_raw")} OR {etl_bulk.needs_reference_sql(subject_text)}) AS _reference,
               {record_id} AS record_id, {version} AS subject_version, {identity} AS identity_json,
               {payload} AS processing_json, {_text(policy.dataset)} AS dataset,
               {_text(policy.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
               'row:' || _ordinal AS attempt_id, 'accepted' AS outcome, 'regulatory-catalog-native-v1' AS processor,
               {witness} AS witnesses, '["dict",[]]' AS diagnostic_json FROM subjects)
    SELECT * EXCLUDE (_reference), (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference,
           {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id FROM receipt'''
    with etl_bulk.bulk_connection() as (con, _):
        # Wide comment receipts multiply each worker's live strings. Keep this
        # ordered writer within the connection's memory target with one worker.
        con.execute('SET threads = 1')
        register_html_text(con)
        con.create_function('comments_attachments', _attachments, ['VARCHAR'], 'STRUCT(native VARCHAR, reference BOOLEAN)',
                            type='arrow', null_handling='special')
        con.create_function('comments_identity', lambda values: pa.array([isinstance(v, str) and bool(v.strip())
                            for v in values.to_pylist()], type=pa.bool_()), ['VARCHAR'], 'BOOLEAN',
                            type='arrow', null_handling='special')

        # Refused oversized originals need the same one-row flush bound (write_split reads its refusals one at a time).
        return etl_bulk.write_split(
            con, query, source=source, batch_rows=_BATCH, expected_rows=pq.ParquetFile(source).metadata.num_rows,
            subject_schema=policy.subject_schema, subject_path=subjects, receipt_path=receipts, label='comments',
            row_attempt=lambda table, index, row: row_attempt(row, table['_ordinal'][index].as_py()),
            refusals_last=True, segments=_segments, isolate='_large')


def _accepted_join(subjects: Path, receipts: Path, columns: str) -> str:
    """Admitted subjects joined to their accepted receipts, in subject file order.

    Admission already checked the exact canonical identity and accepted joins.
    Decode its sole comment_id value to join; SQL spelling of control escapes
    never becomes a second identity calculation here.
    """
    identity = "json_extract_string(r.identity_json, '$[1][0][1][1][1]')"
    return f"""SELECT {columns}
        FROM read_parquet({_text(subjects)}, file_row_number=true) n
        JOIN read_parquet({_text(receipts)}) r ON n.comment_id={identity}
        WHERE r.dataset='comments' AND r.outcome='accepted' ORDER BY n.file_row_number"""


def receipt_processors(subjects: Path, receipts: Path, *, directory: Path | None = None):
    """``(comment_id, processor)`` per admitted subject in file order, for the row reader's promotion rule.

    It runs beside the row reader's own validator, so it takes one thread and a small memory target and
    spills under the caller's ``directory``.
    """
    with etl_bulk.bulk_connection(directory, threads=1, memory_limit='1GB') as (con, _):
        for batch in con.execute(_accepted_join(subjects, receipts, 'n.comment_id, r.processor')).to_arrow_reader(_BATCH):
            yield from zip(batch.column(0).to_pylist(), batch.column(1).to_pylist())


def restore_catalog(subjects: Path, receipts: Path, destination: Path, *, generation_id: str) -> int:
    """Restore catalog comments after complete admission and shared mapper proof.

    The catalog retains one original row; readers normalize it when needed.
    Earlier receipts with explicit normalized inputs remain readable, and
    carried-over promotion receipts take theirs from the subject row
    (``comments_promotion_input``) without a row-reader fallback.
    Batch shaping uses the same mapper as _processor_input, followed by one Arrow
    conversion per batch. Any unproven value returns the whole pair to that row
    reader so it decides the exact first error. A DuckDB or Arrow failure does
    not: it raises ``BulkValidationError`` (``BulkInputError`` when the data
    caused it) and the pair is never redone by row. No destination appears
    until all joins, input values, and source-order checks pass.
    """
    from tempfile import TemporaryDirectory
    from spicy_regs.etl_receipts import decode_exact_json, exact_json
    from spicy_regs.transforms.regulations_shape import RECEIPT_COLUMNS, shape_record
    from spicy_regs.transforms.regulations_receipts import (
        COMMENTS_PROMOTION_PROCESSOR, comments_promotion_input, policy as selected_policy, _retained_processor_input)

    selected = selected_policy('comments')
    etl_bulk.validate_bundle({'comments': [subjects]}, [receipts], [selected], generation_id=generation_id)
    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS['comments']])
    expected = pq.ParquetFile(subjects).metadata.num_rows
    query = _accepted_join(subjects, receipts, 'n.* EXCLUDE(file_row_number), n.file_row_number AS _ordinal, '
                                               'r.processing_json AS _processing, r.processor AS _processor')
    destination.parent.mkdir(parents=True, exist_ok=True)
    seen = 0
    try:
        with etl_bulk.bulk_connection() as (con, _), TemporaryDirectory(prefix='comments-restore-', dir=destination.parent) as scratch:
            temporary = Path(scratch)/'processing.parquet'
            with pq.ParquetWriter(temporary, schema, compression='zstd') as writer:
                for batch in con.execute(query).to_arrow_reader(min(_BATCH, 1000)):
                    table = pa.Table.from_batches([batch])
                    table.validate(full=True)
                    if table['_ordinal'].to_pylist() != list(range(seen, seen+batch.num_rows)):
                        raise RuntimeError('comments: restored subject order or membership changed')
                    held = [decode_exact_json(value) for value in table['_processing'].to_pylist()]
                    processors = table['_processor'].to_pylist()
                    # Promotion receipts hold no input; their subject rows supply it (see comments_promotion_input).
                    subject_rows = (table.select(selected.subject_schema.names).to_pylist()
                                    if COMMENTS_PROMOTION_PROCESSOR in processors else None)
                    raw = []
                    for index, values in enumerate(held):
                        promoted = (None if subject_rows is None else
                                    comments_promotion_input(subject_rows[index] | values, processors[index]))
                        if promoted is None and values.get('input_metadata') != {}:
                            raise etl_bulk.NotBulkEligible('Comments catalog restore requires empty processor metadata')
                        raw.append(promoted if promoted is not None else _retained_processor_input('comments', values))
                    if any(not isinstance(value, Mapping) for value in raw):
                        raise etl_bulk.NotBulkEligible('Comments exact retained processor input is required')
                    shaped = [shape_record('comments', value) for value in raw]
                    reproduced = etl_bulk.mapper_table(shaped, selected.subject_schema, 'Comments reproduction')
                    actual = table.select(selected.subject_schema.names).cast(selected.subject_schema)
                    if not reproduced.equals(actual):
                        raise etl_bulk.NotBulkEligible('Comments processor input differs from selected native subjects')
                    names = RECEIPT_COLUMNS['comments']
                    if any(exact_json({name: value.get(name) for name in names}) !=
                           exact_json({name: values.get(name) for name in names})
                           for value, values in zip(shaped, held)):
                        raise etl_bulk.NotBulkEligible('Comments processor input differs from selected receipt values')
                    writer.write_table(etl_bulk.mapper_table(raw, schema, 'Comments retained input'))
                    seen += batch.num_rows
            if seen != expected:
                raise RuntimeError(f'comments: restored {seen} of {expected} native subjects')
            temporary.replace(destination)
    except (etl_bulk.duckdb.Error, pa.ArrowException) as error:
        raise etl_bulk.bulk_failure('Comments batch processing proof failed', error) from error
    except (ValueError, TypeError, OverflowError, RecursionError) as error:
        # The shared mapper rejected a retained input: the row reader decides the exact first error.
        raise etl_bulk.NotBulkEligible('Comments batch processing proof requires the row reader') from error
    return seen
