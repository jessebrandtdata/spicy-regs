"""Native regulatory catalog rows and their receipts commit in one transaction.

Native subjects and shared receipts are the only catalog authority. Processing
reads return the exact retained source observations after validating their
current native subject joins; they never reconstruct an older storage format.
"""
from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, rebind_receipt, read_with_receipts, subject_identity
from spicy_regs.native_types import described_schema
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.transforms.regulations_receipts import policy, write_records
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, TYPES

DATASETS = frozenset({'dockets', 'documents', 'comments'})


class CatalogConversionRefused(ValueError):
    """Conversion failed; the separately staged attempts survive transaction rollback."""
    def __init__(self, receipt_path):
        import shutil
        with NamedTemporaryFile(prefix='catalog-refusal-', suffix='.parquet', delete=False) as retained:
            self.receipt_path = Path(retained.name)
        shutil.copyfile(receipt_path, self.receipt_path)
        super().__init__('Catalog conversion refused source rows; source state was not changed')


def retain_refusal(con, error):
    """After rollback, persist only failed attempts; no subject becomes accepted."""
    while error is not None and not isinstance(error, CatalogConversionRefused):
        error = error.__cause__
    if error is None:
        return
    from . import iceberg
    try:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {iceberg._CATALOG_ALIAS}."{namespace()}"')
        with _transaction(con):
            con.execute(f'CREATE TABLE IF NOT EXISTS {receipts_table()} ({_ddl(RECEIPT_SCHEMA)})')
            con.execute(f"INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?) WHERE outcome<>'accepted'",
                        [str(error.receipt_path)])
    except BaseException as persistence_error:
        error.add_note(f'Refusal receipts remain recoverable at {error.receipt_path}')
        raise error from persistence_error
    else:
        error.receipt_path.unlink(missing_ok=True)


def rejected_attempts(con, source, dataset, work, generation, *, reason):
    """Keep unselected exact source observations as processing evidence."""
    from spicy_regs.etl_receipts import ReceiptContext, failure_receipt, exact_json
    path = work / ('rejected-source-' + uuid4().hex + '.parquet')
    _copy(con, f'SELECT * FROM {source}', path)
    def records():
        for i, row in enumerate(_rows(path)):
            witness = {'source_id': source, 'source_uri': None,
                       'sha256': sha256(exact_json(row).encode()).hexdigest(),
                       'locator': 'receipt.values.raw_source_record (canonical exact_json)',
                       'body_version': generation}
            context = ReceiptContext(generation, f'unselected-row:{i}', 'regulatory-catalog-native-v1',
                                     [witness], {'reason': reason})
            yield failure_receipt(policy(dataset), context, outcome='rejected', raw_fields={'raw_source_record': row})
    receipts = work / ('rejected-receipts-' + uuid4().hex + '.parquet')
    write_rows(records(), receipts, RECEIPT_SCHEMA)
    con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?)', [str(receipts)])


def supports(record_type):
    return record_type.name in DATASETS


def namespace():
    from . import iceberg
    return iceberg._namespace() + '_native'


def qualified(record_type):
    from . import iceberg
    return f'{iceberg._CATALOG_ALIAS}."{namespace()}"."{record_type.name}"'


def receipts_table():
    from . import iceberg
    return f'{iceberg._CATALOG_ALIAS}."{namespace()}"."etl_receipts"'


def _exists(con, name):
    # A failed SELECT on an absent Iceberg namespace caches that negative lookup
    # for the connection. Inspect metadata instead so a preview can precede DDL.
    import re
    parts = [part[1:-1].replace('""', '"') if part.startswith('"') else part
             for part in re.findall(r'"(?:[^"]|"")*"|[^.]+', name)]
    if len(parts) != 3:
        raise ValueError('Catalog existence lookup requires a qualified table name')
    return con.execute('SELECT 1 FROM information_schema.tables WHERE table_catalog=? '
                       'AND table_schema=? AND table_name=? LIMIT 1', parts).fetchone() is not None


def _ddl(schema):
    return ', '.join('"' + name.replace('"', '""') + '" ' + dtype for name, dtype in described_schema(schema))


def _copy(con, sql, path, *, resources: ExportResources | None = None, parameters=None):
    from .iceberg import _sql_str
    options = 'FORMAT PARQUET, COMPRESSION ZSTD'
    if resources is not None:
        options += f', ROW_GROUP_SIZE {resources.row_group_rows}'
        # DuckDB permits the byte target only when the caller allows reordering.
        if not con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]:
            options += f", ROW_GROUP_SIZE_BYTES '{_sql_str(resources.row_group_bytes)}'"
    statement = f"COPY ({sql}) TO '{_sql_str(str(path))}' ({options})"
    if parameters is None:
        con.execute(statement)
    else:
        con.execute(statement, parameters)


def _rows(path):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=2000):
        yield from batch.to_pylist()


def _validate_source_columns(con, source, dataset):
    """Require declared mapper input columns before retaining a source observation."""
    types = {r[0]: r[1] for r in con.execute(f'DESCRIBE SELECT * FROM {source}').fetchall()}
    held = set(types)
    wrong = [name for name, dtype in SOURCE_COLUMNS[dataset]
             if dtype == 'VARCHAR' and name in held and types[name] != 'VARCHAR']
    if wrong:
        raise ValueError(f'{dataset}: source string columns have incompatible types: {wrong}')
    unknown = held - {c for c, _ in SOURCE_COLUMNS[dataset]}
    if unknown:
        raise ValueError(f'{dataset}: unclassified source columns {sorted(unknown)}')
    return ', '.join(f'CAST("{name}" AS {dtype}) AS "{name}"' if name in held
                     else f'CAST(NULL AS {dtype}) AS "{name}"' for name, dtype in SOURCE_COLUMNS[dataset])


def normalize_source_record(dataset, row):
    """Validate and normalize the declared source fields without losing raw values."""
    key = policy(dataset).identity_fields[0]
    from datetime import date, datetime
    shaped = {}
    identity = row.get(key)
    if not isinstance(identity, str) or not identity.strip():
        raise ValueError(f'{dataset}: source identity must be nonblank text')
    for name, dtype in SOURCE_COLUMNS[dataset]:
        value = row.get(name)
        if isinstance(value, str) and dtype in {'INTEGER', 'BIGINT'}:
            import re
            if not re.fullmatch(r'[+-]?[0-9]+', value):
                raise ValueError(f'{dataset}.{name}: invalid integer')
            value = int(value)
        elif isinstance(value, str) and dtype == 'BOOLEAN':
            if value.lower() not in {'true', 'false'}:
                raise ValueError(f'{dataset}.{name}: invalid boolean')
            value = value.lower() == 'true'
        elif isinstance(value, str) and dtype == 'DATE':
            value = date.fromisoformat(value)
        elif isinstance(value, str) and dtype.startswith('TIMESTAMP'):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif isinstance(value, str) and dtype == 'DOUBLE':
            value = float(value)
        shaped[name] = value
    return shaped


def _stage(con, source, dataset, work, generation, *, prior_receipts=None, bulk=True):
    from spicy_regs.etl_receipts import ReceiptContext
    source_path = work / 'source.parquet'
    _validate_source_columns(con, source, dataset)  # Refuse unclassified columns before conversion.
    key = policy(dataset).identity_fields[0]
    source_query = f'SELECT * FROM {source}'
    has_prior = prior_receipts is not None and con.execute(f'SELECT 1 FROM {prior_receipts} LIMIT 1').fetchone() is not None
    if has_prior:
        source_query = f'SELECT s.*, w.receipt AS _prior_receipt FROM {source} s LEFT JOIN {prior_receipts} w USING ("{key}")'
    # Reuse comments layout targets before its private receipt writer reads
    # these wide strings. Preserve the caller's connection and ordering settings.
    _copy(con, source_query, source_path, resources=ExportResources() if dataset == 'comments' else None)
    def observations():
        from spicy_regs.etl_receipts import exact_json
        for i, row in enumerate(_rows(source_path)):
            prior = row.pop('_prior_receipt', None)
            # The exact source record is retained in this receipt; its canonical bytes
            # remain reconstructable after temporary staging files are removed.
            witness = {'source_id': source, 'source_uri': None,
                       'sha256': sha256(exact_json(row).encode()).hexdigest(),
                       'locator': 'receipt.values.raw_source_record (canonical exact_json)',
                       'body_version': generation}
            context = ReceiptContext(generation, f'row:{i}', 'regulatory-catalog-native-v1', [witness])
            if prior is not None:
                from spicy_regs.etl_receipts import inherit_receipt
                context = inherit_receipt(context, prior)
            yield row, context
    records = observations()
    def normalized(row):
        return normalize_source_record(dataset, row)
    subject = receipts = None
    # Fresh Comments keeps one current original input and uses the batch mapper.
    # Replacements carry exact prior evidence through the shared row writer.
    if dataset == 'comments' and bulk and not has_prior:
        from spicy_regs import comments_bulk, etl_bulk
        from spicy_regs.etl_receipts import exact_json
        from spicy_regs.transforms.regulations_receipts import map_regulations_attempt
        def row_attempt(row, ordinal):
            witness = {'source_id': source, 'source_uri': None,
                       'sha256': sha256(exact_json(row).encode()).hexdigest(),
                       'locator': 'receipt.values.raw_source_record (canonical exact_json)',
                       'body_version': generation}
            context = ReceiptContext(generation, f'row:{ordinal}', 'regulatory-catalog-native-v1', [witness])
            return map_regulations_attempt(dataset, row, context, project=normalized)
        mapped = work / 'mapped'
        mapped.mkdir(parents=True, exist_ok=True)
        subject, receipts = mapped / 'comments.parquet', mapped / 'receipts.parquet'
        try:
            comments_bulk.write_bundle(source_path, subject, receipts, policy=policy(dataset),
                generation_id=generation, source_label=source, row_attempt=row_attempt)
        except etl_bulk.NotBulkEligible as error:
            etl_bulk.log_row_fallback("comments write", error, dataset=dataset)
            subject = receipts = None
    if subject is None:
        subject, receipts = write_records(dataset, records, work / 'mapped', project=normalized)
    if pq.ParquetFile(subject).metadata.num_rows != pq.ParquetFile(source_path).metadata.num_rows:
        raise CatalogConversionRefused(receipts)
    return subject, receipts


@contextmanager
def _transaction(con, *, retain_failed=False):
    con.execute('BEGIN')
    try:
        yield
        con.execute('COMMIT')
    except BaseException as error:
        try:
            con.execute('ROLLBACK')
        except Exception:
            pass
        if retain_failed:
            retain_refusal(con, error)
        raise


def initialized(con, dataset):
    """Selection requires one checked shared receipt for completed initialization."""
    if not _exists(con, receipts_table()):
        return False
    rows = con.execute(f"SELECT * FROM {receipts_table()} WHERE dataset=? "
                       "AND processor='regulatory-catalog-initialization-v1'",
                       [dataset]).to_arrow_table().to_pylist()
    if not rows:
        return False
    if len(rows) != 1:
        raise ValueError('Native catalog has ambiguous initialization receipts')
    row = rows[0]
    from spicy_regs.etl_receipts import exact_json, ReceiptContext, receipt_policies
    ReceiptContext(row['generation_id'], row['attempt_id'], row['processor'], row['witnesses'])
    rebind_receipt(row, generation_id=row['generation_id'])  # Verify the exact receipt digest.
    # The marker states that an empty catalog was initialized, once. A later field-policy version does not undo that:
    # comments moved to regulations-native-v2 on 2026-10-06 and the live marker is still v1 (every nightly check failed).
    # Any declared version of the dataset's policy lineage qualifies it.
    if (row['outcome'] != 'observed'
            or row['policy_version'] not in {p.policy_version for p in receipt_policies(policy(dataset))}
            or row['record_id'] is not None or row['subject_version'] is not None
            or row['processing_json'] != exact_json({'input_metadata': {'catalog_initialized': True}})):
        raise ValueError('Native catalog initialization receipt differs from field policy')
    return True


def _schema_difference(observed, declared) -> str:
    """Name what differs, so a refusal says which columns and types to migrate (it once said only "differs")."""
    held, wanted = dict(observed), dict(declared)
    parts = []
    if missing := [n for n, _ in declared if n not in held]:
        parts.append(f'missing {missing}')
    if extra := [n for n, _ in observed if n not in wanted]:
        parts.append(f'unexpected {extra}')
    if retyped := [f'{n}: {held[n]} != {wanted[n]}' for n, _ in declared if n in held and held[n] != wanted[n]]:
        parts.append(f'retyped {retyped}')
    if not parts:
        parts.append('same columns in a different order')
    return '; '.join(parts)


def require_initialized(con, record_type):
    """Refuse an absent or merely prepared catalog; physical existence is not selection."""
    if not supports(record_type):
        raise ValueError(f'Unsupported regulatory catalog dataset: {record_type.name}')
    target = qualified(record_type)
    if not _exists(con, target) or not initialized(con, record_type.name):
        raise ValueError('Native catalog has no qualified initialization receipt')
    observed = [(r[0], r[1]) for r in con.execute(f'DESCRIBE {target}').fetchall()]
    declared = described_schema(policy(record_type.name).subject_schema)
    if observed != declared:
        raise ValueError('Native catalog schema differs from declared policy: ' + _schema_difference(observed, declared))


def ensure_native(con, record_type):
    """Initialize an empty native dataset; never read or migrate another namespace.

    Storage preparation and initialization are separate transactions because the
    Iceberg engine does not retain manifests from a multi-table CREATE+data commit.
    Readers require the checked initialization receipt, including after interruption.
    """
    from . import iceberg
    if not supports(record_type):
        raise ValueError(f'Unsupported regulatory catalog dataset: {record_type.name}')
    dataset = record_type.name
    declared = policy(dataset)
    target = qualified(record_type)
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {iceberg._CATALOG_ALIAS}."{namespace()}"')
    if _exists(con, target) and initialized(con, dataset):
        require_initialized(con, record_type)
        return
    with _transaction(con):
        con.execute(f'CREATE TABLE IF NOT EXISTS {receipts_table()} ({_ddl(RECEIPT_SCHEMA)})')
        con.execute(f'CREATE TABLE IF NOT EXISTS {target} ({_ddl(declared.subject_schema)})')
    with TemporaryDirectory(prefix='catalog-initialize-') as temporary, _transaction(con):
        if initialized(con, dataset):
            return
        if con.execute(f'SELECT count(*) FROM {target}').fetchone()[0]:
            raise ValueError('Uninitialized native catalog contains unselected rows')
        marker_path = _initialization_marker(dataset, Path(temporary) / 'initialization.parquet')
        con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?)', [str(marker_path)])
        require_initialized(con, record_type)


def _initialization_marker(dataset, path):
    """Write the one checked initialization receipt of an empty native dataset to ``path``."""
    from spicy_regs.etl_receipts import ReceiptContext, observation_receipt
    generation = 'initialize-' + uuid4().hex
    context = ReceiptContext(generation, 'initialize', 'regulatory-catalog-initialization-v1',
        [{'source_id': dataset, 'source_uri': None, 'sha256': None,
          'locator': 'empty native catalog initialization', 'body_version': generation}])
    marker = observation_receipt(policy(dataset), context, processing_fields={
        'input_metadata': {'catalog_initialized': True}})
    write_rows([marker], path, RECEIPT_SCHEMA)
    return path


def processing_table(con, record_type, *, where: str | None = None, in_transaction=False,
                     selected_receipts: Path | None = None):
    """Return exact retained mapper observations after checking native subject joins.

    The receipt owns the original processing input. Native business columns are
    not converted back into strings or JSON to manufacture an older table shape.
    A caller-owned ``selected_receipts`` file retains the original accepted
    receipts from this same read for history inheritance, before rebinding.
    """
    from . import iceberg
    from contextlib import nullcontext
    dataset = record_type.name
    target = qualified(record_type)
    temporary_name = '_processing_' + dataset + '_' + uuid4().hex
    predicate = f' WHERE {where}' if where else ''
    with TemporaryDirectory(prefix='catalog-read-') as temporary:
        work = Path(temporary)
        with (nullcontext() if in_transaction else _transaction(con)):
            generation = 'read-' + uuid4().hex
            require_initialized(con, record_type)
            subject = work / 'subjects.parquet'
            _copy(con, f'SELECT * FROM {target}{predicate}', subject)
            ids = work / 'identities.parquet'
            if where:
                write_rows(({'record_id': subject_identity(policy(dataset), row)[0]} for row in _rows(subject)),
                           ids, pa.schema([('record_id', pa.string())]))
            raw_receipts = selected_receipts if selected_receipts is not None else work / 'selected-receipts.parquet'
            # Full reads also detect orphan receipts (for example an unpaired DELETE).
            # Scoped reads join only selected identities and avoid restoring unrelated agencies.
            selected_ids = (f" SEMI JOIN read_parquet('{iceberg._sql_str(str(ids))}') i USING (record_id)"
                            if where else '')
            receipt_predicate = f"r.dataset='{dataset}' AND r.outcome='accepted'"
            parameters = None
            if where:
                # Hashed identities span nearly every file's bounds. Read only
                # narrow headers first; generation bounds can then prune payload
                # files. Both reads retain the same transaction and exact IDs.
                generations = con.execute(
                    f'SELECT DISTINCT r.generation_id FROM {receipts_table()} r{selected_ids} '
                    f'WHERE {receipt_predicate}').fetchall()
                parameters = [value for (value,) in generations if value is not None]
                choices = []
                if parameters:
                    choices.append('r.generation_id IN (' + ','.join('?' for _ in parameters) + ')')
                if any(value is None for (value,) in generations):
                    choices.append('r.generation_id IS NULL')
                # Malformed generations remain selected for the reference reader
                # to refuse; duplicate accepted receipts also remain visible.
                receipt_predicate += ' AND (' + (' OR '.join(choices) or 'FALSE') + ')'
            _copy(con, f"SELECT r.* FROM {receipts_table()} r{selected_ids} "
                       f'WHERE {receipt_predicate}', raw_receipts, parameters=parameters)
            receipts = work / 'receipts.parquet'
            write_rows((rebind_receipt(row, generation_id=generation) for row in _rows(raw_receipts)),
                       receipts, RECEIPT_SCHEMA)
            restored = work / 'processing.parquet'
            def source_rows():
                from spicy_regs.transforms.regulations_receipts import _processor_input
                # Comments' promotion receipts need their processor (comments_promotion_input).
                processors = None
                if dataset == 'comments':
                    from spicy_regs.comments_bulk import receipt_processors
                    processors = receipt_processors(subject, receipts)
                for row in read_with_receipts([subject], [receipts], policy(dataset), generation_id=generation):
                    processor = None
                    if processors is not None:
                        comment_id, processor = next(processors, (None, None))
                        if comment_id != row['comment_id']:
                            raise ValueError('comments: receipt processors differ from the admitted subject order')
                    yield _processor_input(dataset, row, processor=processor)
            batch_restored = False
            if dataset == 'comments':
                from spicy_regs import comments_bulk, etl_bulk
                try:
                    comments_bulk.restore_catalog(subject, receipts, restored, generation_id=generation)
                    batch_restored = True
                except etl_bulk.NotBulkEligible as error:
                    etl_bulk.log_row_fallback("comments restore", error, dataset=dataset)  # The row reader decides the first error.
            if not batch_restored:
                write_rows(source_rows(), restored, pa.schema([(n, TYPES[t]) for n, t in SOURCE_COLUMNS[dataset]]))
            con.execute(f'CREATE TEMP TABLE "{temporary_name}" AS SELECT * FROM read_parquet(?)', [str(restored)])
    return '"' + temporary_name + '"'


def replace_native(con, record_type, source, *, expected_prior=None, scope=None, expected_snapshot=None, in_transaction=False, delete_scope=False, rejected_source=None):
    """Replace selected rows and current receipts in one multi-table catalog commit."""
    from . import iceberg
    from contextlib import nullcontext
    if not in_transaction:
        ensure_native(con, record_type)
    key = record_type.dedup_key
    key_sql = '"' + key.replace('"', '""') + '"'
    scope_sql = iceberg._scope_predicate(record_type, scope)
    count, distinct, missing, outside = con.execute(f'''SELECT count(*), count(DISTINCT {key_sql}),
        count(*) FILTER (WHERE {key_sql} IS NULL OR trim({key_sql})=''),
        count(*) FILTER (WHERE ({scope_sql}) IS NOT TRUE) FROM {source}''').fetchone()
    if missing or count != distinct or outside:
        raise ValueError('Catalog replacements require distinct, nonblank identities within scope')
    if delete_scope and not scope:
        raise ValueError('Replacing a whole scope requires an explicit scope')
    if not count and not delete_scope and rejected_source is None:
        return
    with TemporaryDirectory(prefix='catalog-replace-') as temporary:
        work = Path(temporary)
        generation = 'write-' + uuid4().hex
        with (nullcontext() if in_transaction else _transaction(con, retain_failed=True)):
            if expected_snapshot is not None and iceberg._read_snapshot(con, record_type) != expected_snapshot:
                raise RuntimeError('Catalog snapshot changed after preparation; rerun the operation')
            prior_where = scope_sql if delete_scope else f'{key_sql} IN (SELECT {key_sql} FROM {source}) AND {scope_sql}'
            selected_receipts = work / 'prior-receipts.parquet'
            prior = processing_table(con, record_type, where=prior_where, in_transaction=True,
                                     selected_receipts=selected_receipts)
            if expected_prior is not None:
                expected_columns = _validate_source_columns(con, expected_prior, record_type.name)
                expected_prior = f'(SELECT {expected_columns} FROM {expected_prior} WHERE {prior_where})'
                delta = con.execute(f'''SELECT count(*) FROM ((SELECT * FROM {prior} EXCEPT ALL SELECT * FROM {expected_prior})
                    UNION ALL (SELECT * FROM {expected_prior} EXCEPT ALL SELECT * FROM {prior}))''').fetchone()[0]
                if delta:
                    raise RuntimeError('Catalog changed after preparation; rerun the operation')
            if scope and con.execute(f'SELECT 1 FROM {qualified(record_type)} WHERE {key_sql} IN '
                                     f'(SELECT {key_sql} FROM {source}) AND ({scope_sql}) IS NOT TRUE LIMIT 1').fetchone():
                raise ValueError('Catalog replacement identity belongs to another scope')
            identities = work / 'prior-identities.parquet'
            reader = con.execute(f'SELECT {key_sql} FROM {prior}').to_arrow_reader(2000)
            write_rows(({'record_id': subject_identity(policy(record_type.name), row)[0], key: row[key]}
                        for batch in reader for row in batch.to_pylist()), identities,
                       pa.schema([('record_id', pa.string()), (key, pa.string())]))
            prior_receipts = '_prior_receipts_' + uuid4().hex
            # Reuse the exact originals already selected and admitted above.
            # Rebound read receipts would manufacture an extra history entry.
            con.execute(f'CREATE TEMP TABLE {prior_receipts} AS SELECT i."{key}", r AS receipt '
                        'FROM read_parquet(?) r JOIN read_parquet(?) i USING (record_id) '
                        "WHERE r.dataset=? AND r.outcome='accepted'",
                        [str(selected_receipts), str(identities), record_type.name])
            selected_receipts.unlink()  # The local table now owns these prior values.
            subject, receipts = _stage(con, source, record_type.name, work, generation,
                                      prior_receipts=prior_receipts)
            native = '_native_replacement_' + uuid4().hex
            pending = '_receipt_replacement_' + uuid4().hex
            con.execute(f'CREATE TEMP TABLE {native} AS SELECT * FROM read_parquet(?)', [str(subject)])
            con.execute(f'CREATE TEMP TABLE {pending} AS SELECT * FROM read_parquet(?)', [str(receipts)])
            if delete_scope:
                removed = work / 'removed-identities.parquet'
                from spicy_regs.transforms.regulations_shape import shape_record
                write_rows(({'record_id': subject_identity(policy(record_type.name), shape_record(record_type.name, row))[0]}
                            for batch in con.execute(f'SELECT * FROM {prior} WHERE {key_sql} NOT IN '
                                                   f'(SELECT {key_sql} FROM {source})').to_arrow_reader(2000)
                            for row in batch.to_pylist()),
                           removed, pa.schema([('record_id', pa.string())]))
                from spicy_regs.etl_receipts import retire_receipt
                retired = work / 'retired-receipts.parquet'
                prior_rows = con.execute(f'SELECT receipt FROM {prior_receipts} WHERE "{key}" NOT IN '
                                         f'(SELECT {key_sql} FROM {source})').to_arrow_reader(2000)
                write_rows((retire_receipt(row['receipt'], generation_id=generation,
                                          reason='explicit scope replacement')
                            for batch in prior_rows for row in batch.to_pylist()), retired, RECEIPT_SCHEMA)
                con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?)', [str(retired)])
                con.execute(f'DELETE FROM {receipts_table()} WHERE dataset=? AND outcome=\'accepted\' AND record_id IN '
                            '(SELECT record_id FROM read_parquet(?))', [record_type.name, str(removed)])
                con.execute(f'DELETE FROM {qualified(record_type)} WHERE {scope_sql} AND {key_sql} NOT IN '
                            f'(SELECT {key_sql} FROM {source})')
            columns = policy(record_type.name).subject_schema.names
            assignments = ', '.join(f'"{c}"=s."{c}"' for c in columns)
            names = ', '.join(f'"{c}"' for c in columns)
            values = ', '.join(f's."{c}"' for c in columns)
            changed = con.execute(f'''MERGE INTO {qualified(record_type)} t USING {native} s ON t.{key_sql}=s.{key_sql}
                WHEN MATCHED THEN UPDATE SET {assignments}
                WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})''').fetchone()[0]
            if changed != count:
                raise RuntimeError('Catalog replacement changed an unexpected number of rows')
            con.execute(f'''DELETE FROM {receipts_table()} WHERE dataset=? AND outcome='accepted'
                AND record_id IN (SELECT record_id FROM {pending})''', [record_type.name])
            con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM {pending}')
            if rejected_source is not None:
                rejected_attempts(con, rejected_source, record_type.name, work, generation,
                                  reason='not selected: duplicate or not newer than current subject')
            verified = processing_table(con, record_type, where=f'{key_sql} IN (SELECT {key_sql} FROM {source})',
                                        in_transaction=True)
            source_columns = _validate_source_columns(con, source, record_type.name)
            comparison = f'(SELECT {source_columns} FROM {source})'
            delta = con.execute(f'''SELECT count(*) FROM ((SELECT * FROM {verified} EXCEPT ALL SELECT * FROM {comparison})
                UNION ALL (SELECT * FROM {comparison} EXCEPT ALL SELECT * FROM {verified}))''').fetchone()[0]
            if delta:
                raise RuntimeError('Catalog replacement changed source values')
            for temporary_table in (prior, verified, prior_receipts, native, pending):
                con.execute(f'DROP TABLE {temporary_table}')


#: Columns regulations-native-v2 inserted into the v1 comments table (6th, 20th, 21st of 24).
V2_COMMENT_COLUMNS = frozenset({'comment_reference_values_json', 'text_extraction_status', 'pdf_extraction_results_json'})


#: A published receipt file may carry its own catalog's marker; the rebuild writes exactly one fresh marker instead.
_NOT_MARKER = "processor <> 'regulatory-catalog-initialization-v1'"


def rebuild_state(con, record_type):
    """Classify the live comments catalog for a rebuild; refuse any state a rerun cannot safely continue.

    ``v1`` (the 21-column table), ``absent`` (dropped, receipts gone), ``empty`` (v2 table, nothing in it) are
    writable states; ``done`` is a finished v2 catalog. Rows or receipts without the marker are a partial copy.
    """
    dataset = record_type.name
    target = qualified(record_type)
    declared = described_schema(policy(dataset).subject_schema)
    receipts = (con.execute(f'SELECT count(*) FROM {receipts_table()} WHERE dataset=?', [dataset]).fetchone()[0]
                if _exists(con, receipts_table()) else 0)
    if not _exists(con, target):
        if receipts:
            raise ValueError(f'{dataset} receipts exist without a catalog table; refusing a partial state')
        return 'absent'
    observed = [(r[0], r[1]) for r in con.execute(f'DESCRIBE {target}').fetchall()]
    if observed == declared:
        if not con.execute(f'SELECT count(*) FROM {target}').fetchone()[0] and not receipts:
            return 'empty'
        if initialized(con, dataset):
            return 'done'
        raise ValueError('Native catalog holds rows or receipts without an initialization marker (partial copy)')
    if observed == [column for column in declared if column[0] not in V2_COMMENT_COLUMNS]:
        return 'v1'
    raise ValueError(f'Live {dataset} columns are neither the v1 nor the declared v2 shape: {observed}')


def rebuild_native(con, record_type, pair, *, expected_snapshot, expected_prior, dry_run=False):
    """Recreate the comments catalog in the declared (v2) shape from the published generation, in bulk.

    ``pair`` is the verified published subject and receipt files; ``expected_prior`` is the published subject
    Parquet the live v1 catalog must equal on the columns they share. Guards, in order: published pair shape;
    live state; snapshot pin; per-key SHA-256 equality of the shared columns. Then transaction A drops the v1
    table and its receipts (snapshot rechecked), B creates the v2 table, C inserts rows, their receipts verbatim
    and one initialization marker. A rerun resumes from ``absent`` or ``empty``. ``dry_run`` does every read and
    check and writes nothing. Returns the plan.
    """
    from . import iceberg
    dataset = record_type.name
    if dataset != 'comments':
        raise ValueError('rebuild_native rebuilds only the comments catalog')
    declared = policy(dataset)
    columns = declared.subject_schema.names
    key = '"' + record_type.dedup_key + '"'
    if pair.dataset != dataset:
        raise ValueError('Published pair belongs to another dataset')
    (subjects,) = pair.subjects
    if not Path(subjects).samefile(expected_prior):
        raise ValueError('Expected prior must be the same published subject that will be copied')
    # Published pair: exactly the declared columns, one accepted receipt per subject row.
    if not pq.read_schema(subjects).equals(declared.subject_schema, check_metadata=False):
        raise ValueError('Published subject schema differs from the declared policy')
    if not pq.read_schema(pair.receipts).equals(RECEIPT_SCHEMA, check_metadata=False):
        raise ValueError('Published receipt schema differs from the declared schema')
    rows = pq.ParquetFile(subjects).metadata.num_rows
    receipt_rows, accepted = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE outcome='accepted') FROM read_parquet(?) WHERE dataset=? AND {_NOT_MARKER}",
        [str(pair.receipts), dataset]).fetchone()
    if accepted != rows:
        raise ValueError(f'Published receipts hold {accepted} accepted {dataset} rows for {rows} subjects')
    state = rebuild_state(con, record_type)
    if state == 'done':
        raise ValueError('Native catalog is already rebuilt; use the readback to verify it')
    plan = {'state': state, 'target': qualified(record_type), 'receipts_table': receipts_table(), 'rows': rows,
            'receipt_rows': receipt_rows, 'dry_run': dry_run}
    if state == 'v1':
        if expected_snapshot is None:
            raise ValueError('A v1 rebuild requires the catalog snapshot captured at preparation')
        namespace_name = iceberg._catalog_namespace(record_type)
        if iceberg._read_pair_snapshot(con, record_type, namespace=namespace_name) != expected_snapshot:
            raise RuntimeError('Catalog snapshot changed after preparation; rerun the operation')
        shared = [c for c in columns if c not in V2_COMMENT_COLUMNS]
        row = 'ROW(' + ', '.join('"' + c.replace('"', '""') + '"' for c in shared) + ')'
        live_count, live_keys, pub_count, pub_keys, mismatched = con.execute(f'''
            WITH l AS (SELECT {key} AS k, sha256(CAST({row} AS VARCHAR)) AS h FROM {plan['target']}),
                 p AS (SELECT {key} AS k, sha256(CAST({row} AS VARCHAR)) AS h FROM read_parquet(?))
            SELECT (SELECT count(*) FROM l), (SELECT count(DISTINCT k) FROM l),
                   (SELECT count(*) FROM p), (SELECT count(DISTINCT k) FROM p),
                   (SELECT count(*) FROM l FULL JOIN p ON l.k=p.k
                    WHERE l.k IS NULL OR p.k IS NULL OR l.h IS DISTINCT FROM p.h)''',
            [str(expected_prior)]).fetchone()
        if not (live_count == live_keys == pub_count == pub_keys) or mismatched:
            raise RuntimeError(f'Live catalog differs from the published subject: live {live_count} rows '
                               f'({live_keys} keys), published {pub_count} ({pub_keys}), {mismatched} mismatched keys')
        plan['live_rows'] = live_count
    if dry_run:
        return plan
    from loguru import logger
    logger.info('Rebuilding {}: {} rows, {} receipts (+1 marker); state {}', plan['target'], rows, accepted, state)
    names = ', '.join(f'"{c}"' for c in columns)
    receipt_names = ', '.join(f'"{c}"' for c in RECEIPT_SCHEMA.names)
    if state == 'v1':
        with _transaction(con):
            if iceberg._read_pair_snapshot(con, record_type, namespace=namespace()) != expected_snapshot:
                raise RuntimeError('Catalog snapshot changed after preparation; rerun the operation')
            con.execute(f'DELETE FROM {receipts_table()} WHERE dataset=?', [dataset])
            con.execute(f'DROP TABLE {plan["target"]}')
    if state in ('v1', 'absent'):
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {iceberg._CATALOG_ALIAS}."{namespace()}"')
        with _transaction(con):  # The engine drops manifests of a CREATE+data commit.
            con.execute(f'CREATE TABLE IF NOT EXISTS {receipts_table()} ({_ddl(RECEIPT_SCHEMA)})')
            con.execute(f'CREATE TABLE {plan["target"]} ({_ddl(declared.subject_schema)})')
    with TemporaryDirectory(prefix='catalog-rebuild-') as temporary, _transaction(con):
        if (con.execute(f'SELECT count(*) FROM {plan["target"]}').fetchone()[0]
                or con.execute(f'SELECT count(*) FROM {receipts_table()} WHERE dataset=?', [dataset]).fetchone()[0]):
            raise ValueError('Rebuild target is not empty; refusing to append to a partial copy')
        marker = _initialization_marker(dataset, Path(temporary) / 'initialization.parquet')
        inserted = con.execute(f'INSERT INTO {plan["target"]} ({names}) SELECT {names} FROM read_parquet(?)',
                               [str(subjects)]).fetchone()[0]
        carried = con.execute(f'INSERT INTO {receipts_table()} ({receipt_names}) SELECT {receipt_names} '
                              f'FROM read_parquet(?) WHERE dataset=? AND {_NOT_MARKER}', [str(pair.receipts), dataset]).fetchone()[0]
        if inserted != rows or carried != receipt_rows:
            raise RuntimeError(f'Rebuild copied {inserted} rows and {carried} receipts, expected {rows} and {receipt_rows}')
        con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?)', [str(marker)])
        require_initialized(con, record_type)
    return plan


def verify_rebuilt(con, record_type, pair, *, smoke=True):
    """Check schemas/counts and a small deterministic sample against the verified published files.

    Compare every field of the first/middle/last subjects and their accepted receipts, plus the first
    nonaccepted receipt when present. This is sampled readback, not full-corpus validation. No full catalog
    export or second full-corpus validation database is built. The smallest-docket processing smoke uses a
    small validator and is capped at 100 rows within one catalog transaction.
    """
    dataset = record_type.name
    declared = policy(dataset)
    if dataset != 'comments' or pair.dataset != dataset:
        raise ValueError('Rebuild readback requires the comments pair')
    (subjects,) = pair.subjects
    rows = pq.ParquetFile(subjects).metadata.num_rows
    target = qualified(record_type)
    with _transaction(con):
        require_initialized(con, record_type)
        if [(r[0], r[1]) for r in con.execute(f'DESCRIBE {receipts_table()}').fetchall()] != described_schema(RECEIPT_SCHEMA):
            raise ValueError('Rebuilt receipt schema differs from the declared schema')
        counted = con.execute(f'SELECT count(*) FROM {target}').fetchone()[0]
        if counted != rows:
            raise RuntimeError(f'Rebuilt catalog holds {counted} rows, expected {rows}')
        receipt_rows, accepted, other_position = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE outcome='accepted'), "
            "min(file_row_number) FILTER (WHERE outcome<>'accepted') "
            f"FROM read_parquet(?, file_row_number=true) WHERE dataset=? AND {_NOT_MARKER}",
            [str(pair.receipts), dataset]).fetchone()
        copied, copied_accepted = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE outcome='accepted') FROM {receipts_table()} "
            f"WHERE dataset=? AND {_NOT_MARKER}", [dataset]).fetchone()
        if copied != receipt_rows or copied_accepted != accepted or accepted != rows:
            raise RuntimeError(f'Rebuilt comments receipts hold {copied} rows ({copied_accepted} accepted), '
                               f'expected {receipt_rows} ({rows} accepted), excluding the initialization marker')
        positions = sorted({0, rows // 2, rows - 1}) if rows else []
        sample = con.execute('SELECT * EXCLUDE (file_row_number) FROM read_parquet(?, file_row_number=true) '
                             'WHERE file_row_number IN (SELECT unnest(?))', [str(subjects), positions]).to_arrow_table()
        key = record_type.dedup_key
        actual = con.execute(f'SELECT * FROM {target} WHERE "{key}" IN (SELECT unnest(?)) LIMIT ?',
                             [sample[key].to_pylist(), len(positions) + 1]).to_arrow_table()
        if not sample.sort_by(key).equals(actual.sort_by(key)):
            raise RuntimeError('Sampled subjects differ from the published generation')
        identities = [subject_identity(declared, row)[0] for row in sample.to_pylist()]
        expected = con.execute('SELECT * EXCLUDE (file_row_number) FROM read_parquet(?, file_row_number=true) '
                               f"WHERE dataset=? AND {_NOT_MARKER} AND ((outcome='accepted' "
                               'AND record_id IN (SELECT unnest(?))) OR file_row_number=?) LIMIT ?',
                               [str(pair.receipts), dataset, identities, other_position, len(positions) + 2]).to_arrow_table()
        accepted_ids = [row['record_id'] for row in expected.to_pylist() if row['outcome'] == 'accepted']
        if sorted(accepted_ids) != sorted(identities):
            raise RuntimeError('Published sample lacks one accepted receipt per subject')
        other_ids = [row['receipt_id'] for row in expected.to_pylist() if row['outcome'] != 'accepted']
        actual = con.execute(f'SELECT * FROM {receipts_table()} WHERE dataset=? AND '
                             "((outcome='accepted' AND record_id IN (SELECT unnest(?))) "
                             'OR receipt_id IN (SELECT unnest(?))) LIMIT ?',
                             [dataset, identities, other_ids, len(expected) + 1]).to_arrow_table()
        if not expected.sort_by('receipt_id').equals(actual.sort_by('receipt_id')):
            raise RuntimeError('Sampled receipts differ from the published generation, including provenance')
    result = {'rows': counted, 'receipts': copied + 1, 'verification': 'sampled',
              'sampled_subjects': len(sample), 'sampled_receipts': len(expected)}
    if smoke and counted:
        with _transaction(con):
            docket, expected_rows = con.execute(f'SELECT docket_id, count(*) FROM {target} GROUP BY 1 ORDER BY 2, 1 LIMIT 1').fetchone()
            if expected_rows > 100:
                result['smoke_skipped'] = 'smallest docket exceeds 100 rows'
            else:
                predicate = 'docket_id IS NULL' if docket is None else f"docket_id = '{docket.replace(chr(39), chr(39) * 2)}'"
                restored = processing_table(con, record_type, where=predicate, in_transaction=True)
                try:
                    if con.execute(f'SELECT count(*) FROM {restored}').fetchone()[0] != expected_rows:
                        raise RuntimeError(f'Processing read of docket {docket} returned a different row count')
                finally:
                    con.execute(f'DROP TABLE {restored}')
                result['smoke_docket'] = docket
    return result


def export_pair(con, record_type, output_dir, *, generation_id, snapshot=None):
    """Export and qualify one native catalog read, preserving its source witnesses."""
    import json
    from dataclasses import asdict
    from spicy_regs.transforms.regulations_receipts import ReceiptInput
    from . import iceberg
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset = record_type.name
    subjects = output_dir / f'{dataset}.parquet'
    receipts = output_dir / 'etl_receipts.parquet'
    with TemporaryDirectory(prefix='catalog-export-') as temporary, _transaction(con):
        if not initialized(con, dataset):
            raise ValueError('Native catalog has no qualified initialization receipt')
        if snapshot is not None and iceberg._read_pair_snapshot(con, record_type) != snapshot:
            raise RuntimeError('Catalog snapshot changed before paired export; retry')
        _copy(con, f'SELECT * FROM {qualified(record_type)}', subjects)
        raw_receipts = Path(temporary) / 'receipts.parquet'
        _copy(con, f"SELECT * FROM {receipts_table()} WHERE dataset='{dataset}'", raw_receipts)
        write_rows((rebind_receipt(row, generation_id=generation_id) for row in _rows(raw_receipts)),
                   receipts, RECEIPT_SCHEMA)
        for _ in read_with_receipts([subjects], [receipts], policy(dataset), generation_id=generation_id):
            pass
    metadata = {'generation_id': generation_id, 'dataset': dataset}
    if snapshot is not None:
        metadata['snapshot'] = asdict(snapshot)
    (output_dir / 'generation.json').write_text(json.dumps(metadata, sort_keys=True) + '\n')
    return ReceiptInput(dataset, (subjects,), receipts, generation_id)
