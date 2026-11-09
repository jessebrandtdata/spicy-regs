"""Restore court mapper inputs from one admitted subject/receipt selection."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import court_bulk, etl_bulk
from spicy_regs.etl_receipts import decode_exact_json, subject_identity
from spicy_regs.runtime_bounds import checkpoint


def _entries(text):
    return ("map_from_entries(list_transform(CAST(json_extract(" + text + ", '$[1]') AS JSON[]),"
            " e -> {'k': e->>'$[0]', 'v': CAST(json_extract(e, '$[1]') AS VARCHAR)}))")


def _query(dataset, policy, schema):
    if not court_bulk.eligible(dataset, schema):
        raise etl_bulk.NotBulkEligible('Court restore source schema has no bulk mapping')
    subject_types = {field.name: field.type for field in policy.subject_schema}
    record_id, version, identity = etl_bulk.identity_sql(policy)
    fields, proven = [], []
    for index, field in enumerate(schema):
        key = etl_bulk._literal(field.name)
        original = (etl_bulk.exact_json_sql(court_bulk._q(field.name), subject_types[field.name])
                    if field.name in subject_types else "'[\"null\",null]'")
        tagged = f'coalesce(_ci[{key}], _top[{key}], {original})'
        tag = f"json_extract_string({tagged}, '$[0]')"
        if pa.types.is_list(field.type):
            # processing_rows wraps clusters' bare attorney prose in a list.
            value = (f"CASE WHEN {tag}='str' THEN [json_extract_string({tagged}, '$[1]')] "
                     f"WHEN {tag}='list' THEN list_transform(json_extract({tagged}, '$[1]')::JSON[],"
                     " item -> json_extract_string(item, '$[1]')) END")
            tagged = f"CASE WHEN {tag}='str' THEN '[\"list\",[' || {tagged} || ']]' ELSE {tagged} END"
        else:
            value = f"CASE WHEN {tag}='str' THEN json_extract_string({tagged}, '$[1]') END"
        fields.append(f'{value} AS "_out_{index}"')
        proven.append(f"coalesce({etl_bulk.exact_json_sql(value, field.type)} = {tagged}, false)")
        if dataset == 'court_opinion_clusters' and field.name == 'attorneys' and pa.types.is_string(field.type):
            # The maintained decoder serializes the wrapped list with Python
            # json.dumps; it owns spacing and Unicode escapes for this format.
            proven.append(f"{tag}='null'")
    ready = ("NOT _receipt_unproven AND _reference IS FALSE AND _record_id=_held_id "
             "AND _version=_held_version AND _identity=_held_identity")
    return f'''
        WITH s AS (SELECT *, {record_id} AS _record_id, {version} AS _version,
                   {identity} AS _identity, {etl_bulk._subject_reference_sql(policy)} AS _reference
                   FROM court_restore_source),
        joined AS (SELECT s.*, r.processing_json AS _processing, r.unproven AS _receipt_unproven,
                          d.record_id AS _held_id, d.version AS _held_version, d.identity AS _held_identity
                   FROM s JOIN (SELECT * FROM subjects WHERE NOT unproven AND n>=? AND n<?) d ON d.n=s._ordinal
                   JOIN subject_receipts m ON m.n=d.n JOIN receipts r ON r.n=m.receipt),
        top AS (SELECT *, {_entries('_processing')} AS _top FROM joined),
        inputs AS (SELECT *, CASE WHEN _top['conversion_inputs'] LIKE '["dict",%' THEN
                   {_entries("_top['conversion_inputs']")} ELSE map_from_entries([]::STRUCT(k VARCHAR,v VARCHAR)[])
                   END AS _ci FROM top)
        SELECT _ordinal, _held_id, _held_version, _held_identity,
               coalesce({ready} AND (_top['conversion_inputs'] IS NULL OR
                   _top['conversion_inputs']='["null",null]' OR _top['conversion_inputs'] LIKE '["dict",%')
                   AND {' AND '.join(proven)}, false) AS _plain,
               CASE WHEN _plain THEN NULL ELSE _processing END AS _processing,
               {', '.join(fields)} FROM inputs ORDER BY _ordinal'''


@contextmanager
def _one_thread(connection):
    """Run the per-batch join on one DuckDB thread, then restore the caller's setting.

    The join is serial JSON work over 10k wide rows. With 4 threads the live 10.2M-row
    clusters exceeded the 4GB engine limit by the second or third of 1,018 batches
    (2026-11-09); one thread ran 120 batches at the same ~1 s each.
    """
    threads = connection.execute("SELECT current_setting('threads')").fetchone()[0]
    connection.execute('SET threads = 1')
    try:
        yield
    finally:
        connection.execute(f'SET threads = {int(threads)}')


def materialize(reader, dataset: str, schema: pa.Schema, destination: Path) -> Path:
    """Use the reader's admitted match; only unproven rows use the maintained decoder."""
    from spicy_regs.court_receipts import processing_rows

    if reader.row_reference or len(reader.subjects[dataset]) != 1:
        raise etl_bulk.NotBulkEligible('Court restore needs one bulk-admitted subject')
    policy = reader.policies[dataset]
    query = _query(dataset, policy, schema)
    first = reader.connection.execute('SELECT coalesce(min(n),0) FROM subjects WHERE dataset=?', [dataset]).fetchone()[0]
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    seen = 0
    with (reader._stream(), _one_thread(reader.connection),
          TemporaryDirectory(prefix='.court-restore-', dir=destination.parent) as temporary):
        staged = Path(temporary) / 'processing.parquet'
        with pq.ParquetFile(reader.subjects[dataset][0]) as source, pq.ParquetWriter(staged, schema, compression='zstd') as writer:
            for batch in source.iter_batches(batch_size=10_000):
                checkpoint()
                table = pa.Table.from_batches([batch])
                table.validate(full=True)
                low, high = first + seen, first + seen + table.num_rows
                reader.connection.register('court_restore_source', table.append_column(
                    '_ordinal', pa.array(range(low, high), pa.int64())))
                try:
                    result = reader.connection.execute(query, [low, high]).to_arrow_table()
                finally:
                    reader.connection.unregister('court_restore_source')
                if result['_ordinal'].to_pylist() != list(range(low, high)):
                    raise ValueError('Court restore differs from admitted subject membership')
                flags = result['_plain'].to_pylist()
                rest = [i for i, plain in enumerate(flags) if not plain]
                insertions = {}
                for i, subject in zip(rest, table.take(pa.array(rest, pa.int64())).to_pylist(), strict=True):
                    expected = tuple(result[name][i].as_py() for name in ('_held_id', '_held_version', '_held_identity'))
                    if subject_identity(policy, subject) != expected:
                        raise ValueError('Court restore subject differs from admitted identity')
                    row = subject | decode_exact_json(result['_processing'][i].as_py())
                    insertions[i] = next(processing_rows([row], dataset=dataset, schema=schema))
                kept = result.filter(pa.array(flags)).select([f'_out_{i}' for i in range(len(schema))])
                kept = kept.rename_columns(schema.names).cast(schema)
                writer.write_table(etl_bulk.with_insertions(kept, flags, insertions))
                seen += table.num_rows
            if seen != source.metadata.num_rows:
                raise ValueError('Court restore did not read every admitted subject')
        reader.check()
        staged.replace(destination)
    return destination
