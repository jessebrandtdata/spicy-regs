"""Private batch adapters for the original dockets and documents inputs.

Shared parsers and row attempts decide unproven values. SQL handles declared
strings and exact receipt hashing. Completed pairs undergo public admission
before becoming visible; restoration proves every retained processor input.
Only unsupported semantics raise ``NotBulkEligible`` for the caller's row path;
a DuckDB or Arrow failure raises ``BulkValidationError`` (``BulkInputError``
when the data caused it) and is never redone by row.
"""
from collections.abc import Mapping
from dataclasses import replace
from hashlib import sha256
import json
from pathlib import Path
from typing import Any
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import (
    WITNESS_TYPE, ReceiptContext,
    decode_exact_json, exact_json, observation_receipt, select_receipts, selected_subject_policy,
    validate_receipt_bundle,
)
from spicy_regs.native_types import described_schema
from spicy_regs.transforms.regulations_shape import NATIVE_FIELDS, RECEIPT_COLUMNS, SOURCE_COLUMNS, TYPES, _native, _validate

def _text(value: Any) -> str:
    return "CAST(NULL AS VARCHAR)" if value is None else "'" + str(value).replace("'", "''") + "'"


_BATCH = 10_000
_PARSED = pa.struct([('native', pa.string()), ('reference', pa.bool_())])
ATTRIBUTES = frozenset({'comment_attributes', 'docket_attributes', 'document_attributes'})
DATASETS = frozenset({'dockets', 'documents'}) | ATTRIBUTES


def eligible(dataset, schema):
    """Declared columns held at their declared source types (text for the base tables, typed for the attribute ones)."""
    known = dict(SOURCE_COLUMNS.get(dataset, ()))
    return dataset in DATASETS and len(set(schema.names)) == len(schema.names) and all(
        field.name in known and field.type == TYPES[known[field.name]] for field in schema)


def _parser(dataset, name, dtype):
    held = {}  # a column's values repeat heavily (display properties are a handful of spellings over millions of rows)

    def decide(value):
        try:
            native = _native(dataset, name, value)
            _validate(native, dtype, f'{dataset}.{name}')
            stored = pa.array([native], type=dtype).to_pylist()[0]
            return {'native': None if stored is None else json.dumps(stored, ensure_ascii=False, separators=(',', ':')),
                    'reference': False}
        except (ValueError, TypeError, OverflowError, pa.ArrowException):
            return {'native': None, 'reference': True}

    def parse(values):
        if len(held) > 100_000:
            held.clear()
        result = []
        for value in values.to_pylist():
            if value not in held:
                held[value] = decide(value)
            result.append(held[value])
        return pa.array(result, type=_PARSED)
    return parse


def _subject_sql(dataset, declared, source):
    """How the subject derives from the source columns: ``(parsed, exprs, flags)``.

    ``parsed`` are the JSON columns ``(source name, type)`` the ``parse_<name>`` function must be registered for,
    ``exprs`` each subject column's SQL ``(name, expression)`` over the source columns and ``_parsed_<name>``, and
    ``flags`` the SQL conditions under which the row function must decide the row instead. The writer selects the
    expressions; the restore compares them to the stored subject.
    """
    sql_types = dict(described_schema(declared.subject_schema))
    parsed, exprs, flags = [], [], []
    for field in declared.subject_schema:
        name = field.name
        mapping = next(((source_name, dtype) for source_name, (target, dtype) in NATIVE_FIELDS.get(dataset, {}).items()
                        if target == name), None)
        if mapping is not None:
            source_name, dtype = mapping
            parsed.append((source_name, dtype))
            exprs.append((name, f'CAST(CAST(_parsed_{source_name}.native AS JSON) AS {sql_types[name]})'))
            flags.append(f'_parsed_{source_name}.reference')
        elif pa.types.is_boolean(field.type) and name in source.names and pa.types.is_string(source.field(name).type):
            exprs.append((name, f'CASE WHEN "{name}" IN (\'true\',\'True\') THEN true WHEN "{name}" IN (\'false\',\'False\') THEN false END'))
            flags.append(f"(\"{name}\" IS NOT NULL AND \"{name}\" NOT IN ('true','True','false','False'))")
        else:
            exprs.append((name, f'"{name}"'))
    return parsed, exprs, flags


def write_held_dataset(dataset, source, destination, *, generation_id, processor, witnesses,
                       include_source_witness, prior_receipts, validate=True):
    from spicy_regs.transforms.regulations_receipts import carry_regulatory_receipts, map_regulations_attempt, policy
    from rulespec_artifacts import publish_directory_no_replace

    schema = pq.read_schema(source)
    if not eligible(dataset, schema):
        raise etl_bulk.NotBulkEligible('Regulatory base bulk requires input columns at their declared types')
    declared = policy(dataset)
    metadata = {key.decode('utf-8'): value for key, value in (schema.metadata or {}).items()}
    label = f'retained:{dataset}'
    def own_witness(raw, field):
        return {'source_id': label, 'source_uri': None, 'sha256': sha256(exact_json(raw).encode()).hexdigest(),
                'locator': f'receipt.values.{field} (canonical exact_json)', 'body_version': None}
    observed = ReceiptContext(generation_id, f'{dataset}:input-file', processor,
                              ([own_witness(metadata, 'input_metadata')] if include_source_witness else []) + list(witnesses),
                              {'kind': 'input_file_metadata', 'rows': pq.ParquetFile(source).metadata.num_rows})
    def attempt(raw, ordinal):
        context = ReceiptContext(generation_id, f'{dataset}:row:{ordinal}', processor,
                                 ([own_witness(raw, 'raw_conversion_inputs')] if include_source_witness else []) + list(witnesses),
                                 {} if include_source_witness else {'output_row_ordinal': ordinal})
        return map_regulations_attempt(dataset, raw, context, input_metadata=metadata)

    raw = etl_bulk.record_json_sql((field.name, field.type) for field in schema)
    source_columns = ', '.join((f'"{name}"' if name in schema.names else f'CAST(NULL AS {kind})') + f' AS "{name}"'
                               for name, kind in SOURCE_COLUMNS[dataset])
    processing = [(name, etl_bulk.exact_json_sql(f'"{name}"', pa.string()))
                  for name in RECEIPT_COLUMNS[dataset] if name in schema.names]
    processing.extend([('raw_conversion_inputs', '_raw'), ('input_metadata', _text(exact_json(metadata)))])
    payload = "'[\"dict\",[' || " + " || ',' || ".join(
        _text('[' + json.dumps(name) + ',') + ' || ' + value + " || ']'" for name, value in sorted(processing)
    ) + " || ']]'"
    fields = [f'"{field.name}"' for field in declared.subject_schema]
    parsed, native, flags = _subject_sql(dataset, declared, schema)
    key = declared.identity_fields[0]
    flags.append(f'"{key}" IS NULL')
    own = "[{'source_id': " + _text(label) + ", 'source_uri': NULL::VARCHAR, 'sha256': sha256(_raw), " + \
          "'locator': 'receipt.values.raw_conversion_inputs (canonical exact_json)', 'body_version': NULL::VARCHAR}]"
    witness_sql = f'list_concat({own}, _extra)' if include_source_witness else '_extra'
    diagnostics = "'[\"dict\",[]]'" if include_source_witness else "'[\"dict\",[[\"output_row_ordinal\",[\"int\",' || _ordinal || ']]]]'"
    record_id, version, identity = etl_bulk.identity_sql(declared)
    subject_json = etl_bulk.record_json_sql((field.name, field.type) for field in declared.subject_schema)
    parse_sql = ', '.join(f'parse_{name}("{name}") AS _parsed_{name}' for name, _ in parsed)
    reference = ' OR '.join(flags)
    # Keep raw evidence and scalar processing fields separately from native fields.
    retained = ', '.join(f'"{name}"' for name in RECEIPT_COLUMNS[dataset])
    native_select = ', '.join([f'{expr} AS "{name}"' for name, expr in native] + ([retained] if retained else []))
    query = f'''
    WITH original AS (SELECT *, file_row_number AS _ordinal, {raw} AS _raw
        FROM {etl_bulk.SOURCE_BATCH}),
    input AS (SELECT {source_columns}, _ordinal, _raw FROM original),
    parsed AS (SELECT * {', ' + parse_sql if parse_sql else ''} FROM input),
    native AS (SELECT {native_select}, _ordinal, _raw,
        ({reference}) AS _reference FROM parsed),
    receipts AS (SELECT {', '.join(fields)}, _ordinal, _raw,
        (_reference OR {etl_bulk.needs_reference_sql('_raw')} OR {etl_bulk.needs_reference_sql(subject_json)}) AS _reference,
        {record_id} AS record_id, {version} AS subject_version, {identity} AS identity_json,
        {payload} AS processing_json, {_text(dataset)} AS dataset,
        {_text(declared.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
        {_text(dataset + ':row:')} || _ordinal AS attempt_id, 'accepted' AS outcome,
        {_text(processor)} AS processor, {witness_sql} AS witnesses, {diagnostics} AS diagnostic_json
        FROM native CROSS JOIN held_constants)
    SELECT * EXCLUDE (_reference), (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference,
        {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id FROM receipts'''
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix='.regulatory-base-', dir=destination.parent) as temporary:
        stage = Path(temporary)/'bundle'
        stage.mkdir()
        subject_path, receipt_path = stage/f'{dataset}.parquet', stage/'etl_receipts.parquet'
        with etl_bulk.bulk_connection() as (con, _):
            con.register('held_constants', pa.table({'_extra': pa.array([list(witnesses)], type=pa.list_(WITNESS_TYPE))}))
            for name, dtype in parsed:
                con.create_function(f'parse_{name}', _parser(dataset, name, dtype), ['VARCHAR'],
                                    'STRUCT(native VARCHAR, reference BOOLEAN)', type='arrow', null_handling='special')
            etl_bulk.write_split(
                con, query, source=source, batch_rows=_BATCH, expected_rows=pq.ParquetFile(source).metadata.num_rows,
                subject_schema=declared.subject_schema, subject_path=subject_path, receipt_path=receipt_path,
                label=dataset, refusals_last=True,
                last_receipts=[observation_receipt(declared, observed, processing_fields={'input_metadata': metadata})],
                row_attempt=lambda table, index, row: attempt(row, table['_ordinal'][index].as_py()))
        carry_regulatory_receipts(dataset, receipt_path, prior_receipts)
        if validate:
            validate_receipt_bundle({dataset: [subject_path]}, [receipt_path], [declared], generation_id=generation_id)
        publish_directory_no_replace(stage, destination)
    return destination/subject_path.name, destination/receipt_path.name


def materialize_internal(selected, destination, *, source_schema=None, scoped=False):
    """Restore one selected member; ``scoped`` says ``selected.receipts`` is already its dataset's receipts."""
    from spicy_regs.transforms.regulations_receipts import BATCH_RESTORE_DATASETS
    if selected.dataset not in BATCH_RESTORE_DATASETS or len(selected.subjects) != 1:
        raise etl_bulk.NotBulkEligible('Regulatory base restore requires one selected member')
    if scoped:
        return _materialize_selected(selected, destination, source_schema=source_schema)
    with TemporaryDirectory(prefix='regulatory-base-selected-') as temporary:
        receipts = select_receipts(selected.receipts, Path(temporary)/'receipts.parquet', dataset=selected.dataset)
        return _materialize_selected(replace(selected, receipts=receipts), destination, source_schema=source_schema)


def _rechunk(tables, size):
    """The same rows as ``tables`` in tables of ``size`` (the last may be shorter)."""
    held = []
    count = 0
    for table in tables:
        held.append(table)
        count += table.num_rows
        while count >= size:
            whole = pa.concat_tables(held)
            yield whole.slice(0, size)
            held, count = [whole.slice(size)], whole.num_rows - size
    if count:
        yield pa.concat_tables(held)


def _accepted_receipts(receipts, dataset):
    """The dataset's accepted receipts' ``identity_json`` and ``processing_json``, in file order."""
    columns = ['dataset', 'outcome', 'identity_json', 'processing_json']
    with pq.ParquetFile(receipts) as source:
        for batch in source.iter_batches(batch_size=_BATCH, columns=columns):
            table = pa.Table.from_batches([batch])
            yield table.filter((pc.field('dataset') == dataset) & (pc.field('outcome') == 'accepted')).select(['identity_json', 'processing_json'])


#: How each restorable source type reads back from a tagged value ``pair`` (``[name, [tag, body]]``); the exact
#: re-encoding of the result is compared with the retained text, so a wrong tag or body only routes the row.
def _restore_sql(pair, dtype):
    body = f"json_extract_string({pair}, '$[1][1]')"
    if pa.types.is_string(dtype):
        return body
    if pa.types.is_int32(dtype) or pa.types.is_int64(dtype):
        return f"TRY_CAST({body} AS {'INTEGER' if pa.types.is_int32(dtype) else 'BIGINT'})"
    if pa.types.is_boolean(dtype):
        return f"TRY_CAST({body} AS BOOLEAN)"
    if pa.types.is_timestamp(dtype) and dtype.unit == 'us' and dtype.tz == 'UTC':
        return f"TRY_CAST({body} AS TIMESTAMPTZ)"
    if pa.types.is_list(dtype) and pa.types.is_string(dtype.value_type):
        return (f"list_transform(from_json(json_extract({pair}, '$[1][1]'), '[\"JSON\"]'), "
                "item -> json_extract_string(item, '$[1]'))")
    return None


def _plain_query(dataset, declared, schema):
    """SQL over registered ``s`` (subject, ``_s_``-prefixed columns) and ``r`` (receipts), joined on ``_i``, or None.

    Per row: the source row read back from the retained ``raw_conversion_inputs`` by position, ``_plain`` (it re-encodes
    to the retained text, the subject derives from it, and the layout is the writer's), ``_identity`` (the receipt
    names the subject's identity), and the retained input metadata. A row that is not plain is decided in Python.
    """
    if RECEIPT_COLUMNS[dataset] or not {name for name, _ in SOURCE_COLUMNS[dataset]} <= set(schema.names):
        return None
    # A subject column the mapper derives from another field (the Register's regulations.gov statement) has no
    # source column for the SQL to compare, so the mapper proves those rows.
    native = {target for target, _ in NATIVE_FIELDS.get(dataset, {}).values()}
    if not set(declared.subject_schema.names) <= native | set(schema.names):
        return None
    names = sorted(schema.names)
    restored = [_restore_sql(f"json_extract(_processing, '$[1][1][1][1][{i}]')", schema.field(name).type)
                for i, name in enumerate(names)]
    if any(expression is None for expression in restored):
        return None
    parsed, exprs, flags = _subject_sql(dataset, declared, schema)
    same = [f'{expr} IS NOT DISTINCT FROM "_s_{name}"' for name, expr in exprs]
    key = declared.identity_fields[0]
    if len(declared.identity_fields) == 1:
        identity = f'"_s_{key}" = json_extract_string(_identity, \'$[1][0][1][1][1]\')'
    else:
        identity = ' AND '.join(
            etl_bulk.exact_json_sql(f'"_s_{name}"', declared.subject_schema.field(name).type)
            + f"=CAST(json_extract(_identity, '$[1][{i}][1][1]') AS VARCHAR)"
            for i, name in enumerate(declared.identity_fields))
    columns = ', '.join(f'{expression} AS "{name}"' for name, expression in zip(names, restored))
    parse_sql = ''.join(f', parse_{name}("{name}") AS _parsed_{name}' for name, _ in parsed)
    reencoded = etl_bulk.record_json_sql((field.name, field.type) for field in schema)
    return f'''
    WITH x AS (SELECT s._i, r.identity_json AS _identity, r.processing_json AS _processing, s.* EXCLUDE (_i),
                   {columns} FROM s JOIN r USING (_i)),
    p AS (SELECT * {parse_sql} FROM x)
    SELECT _i, coalesce({identity}, false) AS _identity_ok,
        coalesce(json_extract_string(_processing, '$[1][0][0]') = 'input_metadata'
         AND json_extract_string(_processing, '$[1][1][0]') = 'raw_conversion_inputs'
         AND {reencoded} = CAST(json_extract(_processing, '$[1][1][1]') AS VARCHAR)
         AND {' AND '.join(same)} AND NOT ({' OR '.join(flags) or 'false'}), false) AS _plain,
        CASE WHEN _plain THEN CAST(json_extract(_processing, '$[1][0][1]') AS VARCHAR) END AS _meta,
        CASE WHEN _plain THEN NULL ELSE _processing END AS _processing,
        {', '.join(f'"{name}"' for name in names)}
    FROM p ORDER BY _i'''


def _prove_rows(dataset, declared, subjects, held, source_schema):
    """The exact retained inputs of ``subjects`` rows (a table) from their decoded processing, proving each one."""
    from spicy_regs.transforms.regulations_shape import shape_records

    raw = [values.get('raw_conversion_inputs') for values in held]
    if any(not isinstance(value, Mapping) for value in raw):
        raise etl_bulk.NotBulkEligible('Regulatory base exact processor input is required')
    if source_schema is not None and any(set(value) != set(source_schema.names) for value in raw):
        raise etl_bulk.NotBulkEligible("Exact regulatory source field presence differs from retained input")
    shaped = shape_records(dataset, raw)
    reproduced = etl_bulk.mapper_table(shaped, declared.subject_schema, 'Regulatory base reproduction')
    if not reproduced.equals(subjects.select(declared.subject_schema.names).cast(declared.subject_schema)):
        raise etl_bulk.NotBulkEligible('Regulatory base retained processor input differs from subject')
    names = RECEIPT_COLUMNS[dataset]
    if any(exact_json({name: row.get(name) for name in names}) != exact_json({name: values.get(name) for name in names})
           for row, values in zip(shaped, held)):
        raise etl_bulk.NotBulkEligible('Regulatory base retained processing evidence differs')
    return raw


def _materialize_selected(selected, destination, *, source_schema=None):
    from spicy_regs.transforms.regulations_receipts import policy

    declared = policy(selected.dataset)
    subject, receipts = selected.subjects[0], selected.receipts
    # Admit under the policy the subject's schema selects, as the selected-prior readers do.
    validate_receipt_bundle({selected.dataset: [subject]}, [receipts], [selected_subject_policy(declared, [subject])],
                            generation_id=selected.generation_id)
    expected = pq.ParquetFile(subject).metadata.num_rows
    if not expected:
        raise etl_bulk.NotBulkEligible('Empty observed input metadata stays with the row reader')
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    schema = (source_schema.remove_metadata() if source_schema is not None else
              pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[selected.dataset]]))
    plain = _plain_query(selected.dataset, declared, schema)
    metadata, seen = None, 0
    try:
        with etl_bulk.bulk_connection() as (con, _), TemporaryDirectory(prefix='.regulatory-restore-', dir=destination.parent) as temporary:
            output = Path(temporary)/'processing.parquet'
            writer = None
            for name, dtype in _subject_sql(selected.dataset, declared, schema)[0]:
                con.create_function(f'parse_{name}', _parser(selected.dataset, name, dtype), ['VARCHAR'],
                                    'STRUCT(native VARCHAR, reference BOOLEAN)', type='arrow', null_handling='special')
            try:
                # The writers keep accepted receipts in subject order; the identity check below proves it per row.
                subjects = (pa.Table.from_batches([b]) for b in pq.ParquetFile(subject).iter_batches(batch_size=_BATCH))
                for table, held_receipts in zip(_rechunk(subjects, _BATCH),
                                                _rechunk(_accepted_receipts(receipts, selected.dataset), _BATCH)):
                    table.validate(full=True)
                    if table.num_rows != held_receipts.num_rows:
                        raise etl_bulk.NotBulkEligible('Regulatory base restored membership or order differs')
                    index = pa.array(range(table.num_rows), pa.int64())
                    names = table.column_names
                    con.register('s', table.rename_columns([f'_s_{name}' for name in names]).append_column('_i', index))
                    con.register('r', held_receipts.append_column('_i', index))
                    try:
                        if plain is None:
                            result = pa.table({'_i': index, '_identity_ok': pa.array([True] * table.num_rows),
                                               '_plain': pa.array([False] * table.num_rows),
                                               '_meta': pa.array([None] * table.num_rows, pa.string()),
                                               '_processing': held_receipts['processing_json']})
                        else:
                            result = pa.Table.from_batches(list(con.execute(plain).to_arrow_reader(_BATCH)))
                    finally:
                        con.unregister('s')
                        con.unregister('r')
                    if not result['_i'].combine_chunks().equals(index):
                        raise etl_bulk.BulkValidationError('Regulatory restore changed source order or population')
                    if not all(result['_identity_ok'].to_pylist()):
                        raise etl_bulk.NotBulkEligible('Regulatory base restored membership or order differs')
                    flags = result['_plain'].to_pylist()
                    insertions, texts = {}, set(value for value in result['_meta'].to_pylist() if value is not None)
                    metadatas = [decode_exact_json(text) for text in texts]
                    rest = [index for index, flag in enumerate(flags) if not flag]
                    if rest:
                        held = [decode_exact_json(result['_processing'][index].as_py()) for index in rest]
                        raw = _prove_rows(selected.dataset, declared, table.take(rest), held, source_schema)
                        insertions = dict(zip(rest, raw))
                        metadatas.extend(values.get('input_metadata', {}) for values in held)
                    if metadata is None:
                        metadata = metadatas[0]
                        writer = pq.ParquetWriter(output, schema.with_metadata(metadata) if metadata else schema, compression='zstd')
                    if any(value != metadata for value in metadatas):
                        raise etl_bulk.NotBulkEligible('Regulatory base input metadata differs across receipts')
                    if plain is None:
                        kept = etl_bulk.mapper_table(list(insertions.values()), schema, 'Regulatory base retained input')
                    else:
                        kept = etl_bulk.with_insertions(
                            result.filter(pa.array(flags)).select(schema.names).cast(schema), flags, insertions)
                    assert writer is not None
                    writer.write_table(kept)
                    seen += table.num_rows
            finally:
                if writer is not None:
                    writer.close()
            if seen != expected:
                raise etl_bulk.NotBulkEligible('Regulatory base restored population differs')
            output.replace(destination)
    except (etl_bulk.duckdb.Error, pa.ArrowException) as error:
        raise etl_bulk.bulk_failure('Regulatory base processing proof failed', error) from error
    except (ValueError, TypeError, OverflowError, RecursionError) as error:
        # The shared mapper rejected a retained input: the row reader decides the exact first error.
        raise etl_bulk.NotBulkEligible('Regulatory base processing proof requires the original row reader') from error
    return destination
