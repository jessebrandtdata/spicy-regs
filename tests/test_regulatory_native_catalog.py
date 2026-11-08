"""Native catalog writes admit subject/receipt pairs with rollback and exact reads."""

import dataclasses
import duckdb
import polars as pl
import pytest

from spicy_regs import etl_bulk
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.sources import regulatory_catalog as native
from tests.duckdb_faults import FAILURES, assert_raised_as, fail_streams_after


@pytest.fixture
def con():
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        yield connection


def source(con, *, identity='c1', text='first', attachments='[]'):
    row = {c: None for c in COMMENT.schema}
    row.update(comment_id=identity, docket_id='EPA-1', agency_code='EPA', modify_date='2026-10-03',
               text_content=text, text_extraction_status='complete', attachments_json=attachments)
    con.register('incoming', pl.DataFrame([row], schema=COMMENT.schema).to_arrow())
    con.execute('CREATE OR REPLACE TEMP TABLE source AS SELECT * FROM incoming')
    return row


@pytest.mark.parametrize('ordered', [True, False])
def test_comments_copy_layout_preserves_values_and_caller_ordering(con, tmp_path, ordered):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.duckdb_settings import ExportResources

    size = 25_001
    columns = {name: pa.nulls(size, type=pa.string()) for name in COMMENT.schema}
    columns.update(comment_id=pa.array([f'c{i}' for i in range(size)]),
                   agency_code=pa.array(['01'] * size),
                   comment=pa.array([None if i % 7 == 0 else f' literal\u00a0é😀 {i} ' for i in range(size)]),
                   duplicate_comments=pa.array(['007'] * size))
    expected = pa.table(columns)
    con.register('copy_source', expected)
    con.execute('SET preserve_insertion_order=?', [ordered])
    target = tmp_path / 'comments.parquet'
    native._copy(con, 'SELECT * FROM copy_source', target, resources=ExportResources())
    assert pq.ParquetFile(target).metadata.num_row_groups > 1
    actual = pq.read_table(target)
    assert actual.sort_by('comment_id').equals(expected.sort_by('comment_id'))
    if ordered:
        assert actual.equals(expected)
    assert con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0] is ordered


def test_native_pair_and_processing_read(con):
    expected = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    columns = {r[0] for r in con.execute(f'DESCRIBE {iceberg._qualified(COMMENT)}').fetchall()}
    assert 'attachments' in columns and 'attachments_json' not in columns
    assert 'text_extraction_status' in columns
    assert con.execute(f'SELECT text_extraction_status FROM {iceberg._qualified(COMMENT)}').fetchone() == (expected['text_extraction_status'],)
    assert con.execute(f"SELECT outcome,count(*) FROM {native.receipts_table()} GROUP BY 1 ORDER BY 1").fetchall() == [('accepted', 1), ('observed', 1)]
    processing = native.processing_table(con, COMMENT)
    actual = con.execute(f'SELECT * FROM {processing}').to_arrow_table().to_pylist()[0]
    assert actual == expected


def test_replacement_retires_prior_receipt_and_checks_stale_prior(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    prior = native.processing_table(con, COMMENT)
    source(con, text='second')
    iceberg.replace_rows(con, COMMENT, 'source', expected_prior=prior)
    assert con.execute(f"SELECT count(*) FROM {native.receipts_table()} WHERE outcome='accepted'").fetchone() == (1,)
    source(con, text='third')
    with pytest.raises(RuntimeError, match='changed after preparation'):
        iceberg.replace_rows(con, COMMENT, 'source', expected_prior=prior)
    assert con.execute(f'SELECT text_content FROM {iceberg._qualified(COMMENT)}').fetchone() == ('second',)


def test_scoped_receipts_match_unfiltered_path_with_multiple_generations(con, monkeypatch, tmp_path):
    import pyarrow.parquet as pq

    first = source(con, text=' literal\u00a0é😀 ')
    iceberg.replace_rows(con, COMMENT, 'source')
    second = source(con, identity='c2', text='second observation')
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, identity='unselected', text='unrelated')
    iceberg.replace_rows(con, COMMENT, 'source')
    copy = native._copy
    selected = []
    def compare(connection, sql, path, **kwargs):
        copy(connection, sql, path, **kwargs)
        if path.name == 'selected-receipts.parquet':
            # This is the previously maintained full-payload SQL selection,
            # exercised only on the constructed local fixture.
            original = sql.split(' AND (r.generation_id', 1)[0]
            reference = tmp_path / 'reference.parquet'
            copy(connection, original, reference)
            actual = pq.read_table(path).sort_by('record_id')
            assert actual.equals(pq.read_table(reference).sort_by('record_id'))
            selected.append(actual.to_pylist())
    monkeypatch.setattr(native, '_copy', compare)
    restored = native.processing_table(con, COMMENT, where="comment_id IN ('c1','c2')")
    assert con.execute(f'SELECT * FROM {restored} ORDER BY comment_id').to_arrow_table().to_pylist() == [first, second]
    assert len(selected) == 1 and len(selected[0]) == 2
    assert len({row['generation_id'] for row in selected[0]}) == 2


@pytest.mark.parametrize('damage', ['duplicate', 'null-generation', 'mixed-null', 'blank-generation', 'bad-digest'])
def test_scoped_receipt_filter_keeps_invalid_selected_evidence(con, monkeypatch, damage):
    import pyarrow.parquet as pq

    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    if damage == 'duplicate':
        con.execute(f"INSERT INTO {native.receipts_table()} SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'")
    elif damage == 'mixed-null':
        con.execute(f"INSERT INTO {native.receipts_table()} SELECT * REPLACE (NULL AS generation_id) "
                    f"FROM {native.receipts_table()} WHERE outcome='accepted'")
    else:
        field, value = ('subject_version', 'wrong') if damage == 'bad-digest' else (
            'generation_id', None if damage == 'null-generation' else '')
        con.execute(f"UPDATE {native.receipts_table()} SET {field}=? WHERE outcome='accepted'", [value])
    expected = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'").to_arrow_table()
    copy = native._copy
    observed = []
    def observe(connection, sql, path, **kwargs):
        copy(connection, sql, path, **kwargs)
        if path.name == 'selected-receipts.parquet':
            actual = pq.read_table(path)
            assert actual.equals(expected)
            observed.append(actual.num_rows)
    monkeypatch.setattr(native, '_copy', observe)
    with pytest.raises(ValueError, match='[Dd]uplicate|[Dd]igest'):
        native.processing_table(con, COMMENT, where="comment_id='c1'")
    assert observed == [expected.num_rows]


def test_scoped_receipt_headers_see_transaction_local_replacements(con):
    first = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, identity='c2')
    iceberg.replace_rows(con, COMMENT, 'source')
    con.execute('BEGIN')
    try:
        second = source(con, text='transaction local')
        native.replace_native(con, COMMENT, 'source', in_transaction=True)
        con.execute(f"DELETE FROM {native.qualified(COMMENT)} WHERE comment_id='c2'")
        from spicy_regs.etl_receipts import subject_identity
        record_id = subject_identity(native.policy('comments'), {'comment_id': 'c2'})[0]
        con.execute(f"DELETE FROM {native.receipts_table()} WHERE record_id=? AND outcome='accepted'", [record_id])
        restored = native.processing_table(con, COMMENT, where="agency_code='EPA'", in_transaction=True)
        assert con.execute(f'SELECT * FROM {restored}').to_arrow_table().to_pylist() == [second]
    finally:
        con.execute('ROLLBACK')
    restored = native.processing_table(con, COMMENT, where="comment_id='c1'")
    assert con.execute(f'SELECT * FROM {restored}').to_arrow_table().to_pylist() == [first]


def test_scoped_receipt_generations_are_bound_and_do_not_replace_id_selection(con):
    import pyarrow as pa
    from spicy_regs.etl_receipts import _digest

    first = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, identity='unselected')
    iceberg.replace_rows(con, COMMENT, 'source')
    rows = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'").to_arrow_table().to_pylist()
    for generation in ["quoted'é😀", 'new-generation']:
        for row in rows:
            row['generation_id'] = generation
            row['receipt_id'] = _digest({k: v for k, v in row.items() if k != 'receipt_id'})
        con.register('resealed', pa.Table.from_pylist(rows, schema=native.RECEIPT_SCHEMA))
        con.execute(f"DELETE FROM {native.receipts_table()} WHERE outcome='accepted'")
        con.execute(f'INSERT INTO {native.receipts_table()} SELECT * FROM resealed')
        restored = native.processing_table(con, COMMENT, where="comment_id='c1'")
        assert con.execute(f'SELECT * FROM {restored}').to_arrow_table().to_pylist() == [first]


def test_scoped_receipt_selection_preserves_empty_and_missing_pair_behavior(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    empty = native.processing_table(con, COMMENT, where="comment_id='absent'")
    assert con.execute(f'SELECT count(*) FROM {empty}').fetchone() == (0,)
    assert {row[0] for row in con.execute(f'DESCRIBE {empty}').fetchall()} == set(COMMENT.schema)
    con.execute(f"DELETE FROM {native.receipts_table()} WHERE outcome='accepted'")
    with pytest.raises(ValueError, match='receipt|Receipt'):
        native.processing_table(con, COMMENT, where="comment_id='c1'")


def test_bad_receipt_refuses_processing_read(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    con.execute(f"UPDATE {native.receipts_table()} SET subject_version='wrong'")
    with pytest.raises(ValueError, match='digest'):
        native.processing_table(con, COMMENT)


def test_only_initialized_native_storage_is_read_authority(con):
    source(con)
    con.execute('CREATE SCHEMA reg_catalog."default"')
    con.execute('CREATE TABLE reg_catalog."default".comments AS SELECT * FROM source')
    with pytest.raises(ValueError, match='initialization receipt'):
        native.processing_table(con, COMMENT)
    assert not native._exists(con, native.qualified(COMMENT))
    native.ensure_native(con, COMMENT)
    assert con.execute(f'SELECT count(*) FROM {native.processing_table(con, COMMENT)}').fetchone() == (0,)
    assert con.execute('SELECT count(*) FROM reg_catalog."default".comments').fetchone() == (1,)


def test_receipt_insert_failure_rolls_back_subject_update(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    before = con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall()
    source(con, text='second')

    class Fault:
        def execute(self, sql, *args):
            if sql.startswith(f'INSERT INTO {native.receipts_table()} SELECT * FROM _receipt_replacement_'):
                raise RuntimeError('injected receipt write failure')
            return con.execute(sql, *args)

    with pytest.raises(RuntimeError, match='injected receipt'):
        iceberg.replace_rows(Fault(), COMMENT, 'source')
    assert con.execute(f'SELECT text_content FROM {native.qualified(COMMENT)}').fetchone() == ('first',)
    assert con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall() == before


def test_pair_export_preserves_generation_and_processing(con, tmp_path):
    from spicy_regs.transforms.regulations_receipts import read_internal
    expected = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    selected = native.export_pair(con, COMMENT, tmp_path, generation_id='export-1')
    assert list(read_internal(selected)) == [expected]
    assert 'export-1' in (tmp_path / 'generation.json').read_text()


def test_explicit_scope_replacement_retains_removed_evidence(con):
    from spicy_regs.etl_receipts import resolve_receipt_witness
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, identity='c2', text='replacement')
    native.replace_native(con, COMMENT, 'source', scope={'agency_code': 'EPA'}, delete_scope=True)
    assert con.execute(f'SELECT comment_id FROM {native.qualified(COMMENT)}').fetchall() == [('c2',)]
    retired = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE attempt_id LIKE '%:retired'").to_arrow_table().to_pylist()
    assert len(retired) == 1 and retired[0]['outcome'] == 'observed'
    references = [w for w in retired[0]['witnesses'] if (w['source_uri'] or '').startswith('receipt-processing:')]
    assert references and all(resolve_receipt_witness(retired[0], w) for w in references)
    native.processing_table(con, COMMENT)


def test_prepared_storage_is_not_a_clean_selected_catalog(con):
    con.execute(f'CREATE SCHEMA reg_catalog."{native.namespace()}"')
    con.execute(f'CREATE TABLE {native.qualified(COMMENT)} ({native._ddl(native.policy("comments").subject_schema)})')
    with pytest.raises(ValueError, match='initialization receipt'):
        iceberg.audit_duplicates(con, COMMENT)
    native.ensure_native(con, COMMENT)
    assert iceberg.audit_duplicates(con, COMMENT) == []


def test_failed_conversion_keeps_refusal_and_prior_subject(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, attachments='not-json')
    with pytest.raises(ValueError, match='conversion refused'):
        iceberg.replace_rows(con, COMMENT, 'source')
    assert con.execute(f'SELECT outcome,count(*) FROM {native.receipts_table()} GROUP BY 1 ORDER BY 1').fetchall() == [('accepted', 1), ('observed', 1), ('refused', 1)]
    assert con.execute(f'SELECT text_content FROM {native.qualified(COMMENT)}').fetchone() == ('first',)
    native.processing_table(con, COMMENT)


def test_merge_keeps_unselected_observations_without_full_prior_scan(con, tmp_path):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    other = source(con, identity='other-agency')
    other['agency_code'] = 'FAA'
    con.register('other', pl.DataFrame([other | {'comment_id': f'other-{i}'} for i in range(257)], schema=COMMENT.schema).to_arrow())
    iceberg.replace_rows(con, COMMENT, 'other')
    older = source(con, text='older')
    older['modify_date'] = '2020-01-01'
    path = tmp_path / 'staged.parquet'
    pl.DataFrame([older, older], schema=COMMENT.schema).write_parquet(path)
    reads = []
    copied_subject_counts = []
    class Observe:
        def execute(self, sql, *args):
            reads.append(sql)
            result = con.execute(sql, *args)
            if sql.startswith(f'COPY (SELECT * FROM {native.qualified(COMMENT)}'):
                import re
                import pyarrow.parquet as pq
                destination = re.search(r" TO '([^']+)'", sql)
                assert destination is not None
                copied_subject_counts.append(pq.ParquetFile(destination.group(1)).metadata.num_rows)
            return result
    assert iceberg._merge(Observe(), [path], COMMENT) == 0
    assert con.execute(f'SELECT outcome,count(*) FROM {native.receipts_table()} GROUP BY 1 ORDER BY 1').fetchall() == [('accepted', 258), ('observed', 1), ('rejected', 2)]
    payload_reads = [sql for sql in reads if f'SELECT * FROM {native.qualified(COMMENT)}' in sql and 'COPY' in sql]
    assert payload_reads and all('WHERE' in sql for sql in payload_reads)
    assert max(copied_subject_counts) == 1  # 257 unrelated agency rows stay unread.
    assert not any('SELECT witnesses' in sql for sql in reads)  # no per-row catalog lookups


def test_unpaired_subject_delete_refuses_whole_dataset_read(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    con.execute(f'DELETE FROM {native.qualified(COMMENT)}')
    with pytest.raises(ValueError, match='Accepted receipt has no matching subject'):
        native.processing_table(con, COMMENT)


def test_replaced_and_exported_witnesses_resolve_exact_retained_payloads(con, tmp_path):
    from hashlib import sha256
    import pyarrow.parquet as pq
    from spicy_regs.etl_receipts import resolve_receipt_witness
    for text in ('first', 'second', 'third'):
        source(con, text=text)
        iceberg.replace_rows(con, COMMENT, 'source')
    selected = native.export_pair(con, COMMENT, tmp_path, generation_id='exported')
    receipt = next(r for r in pq.read_table(selected.receipts).to_pylist() if r['outcome'] == 'accepted')
    assert len(receipt['witnesses']) >= 3
    for witness in receipt['witnesses']:
        payload = resolve_receipt_witness(receipt, witness)
        assert sha256(payload).hexdigest() == witness['sha256'].removeprefix('sha256:')


def test_refusal_persistence_failure_preserves_recoverable_spool(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, attachments='not-json')
    class Unavailable:
        def execute(self, sql, *args):
            if sql.startswith(f'INSERT INTO {native.receipts_table()} SELECT * FROM read_parquet'):
                raise RuntimeError('catalog unavailable while retaining refusal')
            return con.execute(sql, *args)
    with pytest.raises(native.CatalogConversionRefused) as refusal:
        iceberg.replace_rows(Unavailable(), COMMENT, 'source')
    path = refusal.value.receipt_path
    try:
        import pyarrow.parquet as pq
        assert path.is_file()
        assert any(r['outcome'] == 'refused' for r in pq.read_table(path).to_pylist())
        assert str(path) in ' '.join(refusal.value.__notes__)
        assert con.execute(f'SELECT text_content FROM {native.qualified(COMMENT)}').fetchone() == ('first',)
        native.retain_refusal(con, refusal.value)
        assert not path.exists()
    finally:
        path.unlink(missing_ok=True)


def test_historical_refusal_and_stale_attempt_do_not_poison_later_export(con, tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_dataset
    row = source(con, text='current')
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, attachments='not-json')
    with pytest.raises(native.CatalogConversionRefused):
        iceberg.replace_rows(con, COMMENT, 'source')
    # A later valid write remains selectable while the failed attempt is retained.
    row = source(con, text='recovered')
    iceberg.replace_rows(con, COMMENT, 'source')
    staged = tmp_path / 'stale.parquet'
    pl.DataFrame([{**row, 'modify_date': '2020-01-01'}], schema=COMMENT.schema).write_parquet(staged)
    assert iceberg._merge(con, [staged], COMMENT) == 0
    source_path = iceberg._export_parquet(con, COMMENT, tmp_path / 'export')
    result = finish_dataset(tmp_path, 'comments', source_path, publish=False)
    assert pl.read_parquet(result)['text_content'].to_list() == ['recovered']



def test_comments_rebind_compacts_current_input_without_losing_legacy_witness(con):
    from hashlib import sha256
    from spicy_regs.etl_receipts import decode_exact_json, exact_json, rebind_receipt, resolve_receipt_witness, _digest

    original = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    [receipt] = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'").to_arrow_table().to_pylist()
    values = decode_exact_json(receipt['processing_json'])
    normalized = native.normalize_source_record('comments', original)
    values['raw_conversion_inputs'] = normalized
    witness = {'source_id': 'legacy-normalized-input', 'source_uri': None,
               'sha256': sha256(exact_json(normalized).encode()).hexdigest(),
               'locator': 'receipt.values.raw_conversion_inputs (canonical exact_json)',
               'body_version': receipt['generation_id']}
    receipt['witnesses'].append(witness)
    receipt['processing_json'] = exact_json(values)
    receipt['receipt_id'] = _digest({k: v for k, v in receipt.items() if k != 'receipt_id'})
    carried = rebind_receipt(receipt, generation_id='compact-export')
    current = decode_exact_json(carried['processing_json'])
    assert 'raw_conversion_inputs' not in current
    assert current['raw_source_record'] == original
    assert carried['witnesses'] == receipt['witnesses']
    assert resolve_receipt_witness(carried, witness) == exact_json(normalized).encode()
    assert all(resolve_receipt_witness(carried, item) for item in carried['witnesses'])


def test_resealed_catalog_receipt_cannot_restore_inconsistent_source_input(con):
    import json
    from hashlib import sha256
    import pyarrow as pa
    from spicy_regs.etl_receipts import exact_json

    source(con, text='selected')
    iceberg.replace_rows(con, COMMENT, 'source')
    [receipt] = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'").to_arrow_table().to_pylist()
    from spicy_regs.etl_receipts import _unpack
    processing = _unpack(json.loads(receipt['processing_json']))
    processing['raw_source_record'].pop('text_content')
    receipt['processing_json'] = exact_json(processing)
    receipt['receipt_id'] = 'sha256:' + sha256(exact_json({k: v for k, v in receipt.items() if k != 'receipt_id'}).encode()).hexdigest()
    con.register('altered_receipt', pa.Table.from_pylist([receipt], schema=native.RECEIPT_SCHEMA))
    con.execute(f"DELETE FROM {native.receipts_table()} WHERE outcome='accepted'")
    con.execute(f'INSERT INTO {native.receipts_table()} SELECT * FROM altered_receipt')
    with pytest.raises(ValueError, match='retained processor input differs'):
        native.processing_table(con, COMMENT)


def test_full_comments_processing_uses_batch_proof_without_unused_identity_scan(con, monkeypatch):
    expected = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    monkeypatch.setattr(native, 'subject_identity', lambda *a, **k: pytest.fail('unused full-read identity scan'))
    monkeypatch.setattr(native, 'read_with_receipts', lambda *a, **k: pytest.fail('unproven row fallback'))
    restored = native.processing_table(con, COMMENT)
    [actual] = con.execute(f'SELECT * FROM {restored}').to_arrow_table().to_pylist()
    assert all(actual[name] == value for name, value in expected.items())


@pytest.mark.parametrize('later_attachment', ['{bad', '['*20000+']'*20000], ids=['invalid-json', 'excessive-depth'])
def test_comments_batch_refusal_restores_exact_row_first_error(con, monkeypatch, later_attachment):
    import pyarrow as pa
    from spicy_regs import comments_bulk
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, decode_exact_json, exact_json, _digest
    source(con)
    con.execute("INSERT INTO source SELECT * REPLACE ('c2' AS comment_id, 'second' AS text_content) FROM source")
    iceberg.replace_rows(con, COMMENT, 'source')
    rows = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted' ORDER BY record_id").to_arrow_table().to_pylist()
    # Native subject file order selects c1 first, then c2. Both payloads remain
    # valid receipt data, but their retained source inputs fail mapper proof.
    for row in rows:
        values = decode_exact_json(row['processing_json'])
        raw = values['raw_source_record']
        if raw['comment_id']=='c1':
            raw['text_content'] = 'different'
        if raw['comment_id']=='c2':
            raw['attachments_json'] = later_attachment
        old_id = row['receipt_id']
        row['processing_json'] = exact_json(values)
        row['receipt_id'] = _digest({k: v for k,v in row.items() if k!='receipt_id'})
        con.register('_damaged', pa.Table.from_pylist([row], schema=RECEIPT_SCHEMA))
        con.execute(f'DELETE FROM {native.receipts_table()} WHERE receipt_id=?', [old_id])
        con.execute(f'INSERT INTO {native.receipts_table()} SELECT * FROM _damaged')
        con.unregister('_damaged')
    monkeypatch.setattr(comments_bulk, '_BATCH', 2)
    with pytest.raises(ValueError, match='retained processor input differs from selected native subject'):
        native.processing_table(con, COMMENT)


@pytest.mark.parametrize('failure, invalid', FAILURES)
def test_comments_processing_duckdb_failure_surfaces_without_row_fallback(con, monkeypatch, failure, invalid):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    monkeypatch.setattr(native, 'read_with_receipts', lambda *a, **k: pytest.fail('row fallback'))
    delivered = fail_streams_after(monkeypatch, failure)
    with pytest.raises(etl_bulk.BulkValidationError) as caught:
        native.processing_table(con, COMMENT)
    assert delivered == [1]
    assert_raised_as(caught, failure, invalid)


def test_empty_comments_processing_batch_preserves_declared_schema(con, monkeypatch):
    native.ensure_native(con, COMMENT)
    monkeypatch.setattr(native, 'read_with_receipts', lambda *a, **k: pytest.fail('empty batch fell back'))
    restored = native.processing_table(con, COMMENT)
    assert con.execute(f'SELECT count(*) FROM {restored}').fetchone() == (0,)
    assert {row[0] for row in con.execute(f'DESCRIBE {restored}').fetchall()} == set(COMMENT.schema)


def test_initial_comments_stage_batches_with_empty_prior_and_matches_row_writer(con, tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    from spicy_regs import comments_bulk

    source(con)
    con.execute("INSERT INTO source SELECT * REPLACE ('c2' AS comment_id, 'second' AS text_content) FROM source")
    con.execute("INSERT INTO source SELECT * REPLACE ('c3' AS comment_id, 'third' AS text_content) FROM source LIMIT 1")
    native.ensure_native(con, COMMENT)
    con.execute(f'CREATE TEMP TABLE empty_prior AS SELECT r.record_id AS comment_id, r AS receipt '
                f'FROM {native.receipts_table()} r WHERE false')
    calls = []
    real = comments_bulk.write_bundle
    def batch(*args, **kwargs):
        calls.append(kwargs['generation_id'])
        return real(*args, **kwargs)
    monkeypatch.setattr(comments_bulk, 'write_bundle', batch)
    monkeypatch.setattr(comments_bulk, '_BATCH', 2)
    batch_dir, row_dir = tmp_path / 'batch', tmp_path / 'row'
    batch_dir.mkdir()
    row_dir.mkdir()
    actual = native._stage(con, 'source', 'comments', batch_dir, 'same-generation', prior_receipts='empty_prior')
    expected = native._stage(con, 'source', 'comments', row_dir, 'same-generation', prior_receipts='empty_prior', bulk=False)
    assert calls == ['same-generation']
    for got, reference in zip(actual, expected):
        assert pq.read_table(got).equals(pq.read_table(reference))
    assert pq.read_table(actual[0])['comment_id'].to_pylist() == ['c1', 'c2', 'c3']


def test_repeated_comments_updates_keep_all_prior_witnesses_and_payloads(con, monkeypatch):
    from spicy_regs import comments_bulk
    from spicy_regs.etl_receipts import decode_exact_json, resolve_receipt_witness

    first_source = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    def selected_receipt():
        rows = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'").to_arrow_table().to_pylist()
        assert len(rows) == 1
        return rows[0]
    first = selected_receipt()
    monkeypatch.setattr(comments_bulk, 'write_bundle', lambda *a, **k: pytest.fail('prior history routed to fresh writer'))
    second_source = source(con, text='second')
    iceberg.replace_rows(con, COMMENT, 'source')
    second = selected_receipt()
    third_source = source(con, text='third')
    iceberg.replace_rows(con, COMMENT, 'source')
    third = selected_receipt()
    assert third['witnesses'][:len(second['witnesses'])] == second['witnesses']
    assert second['witnesses'][:len(first['witnesses'])] == first['witnesses']
    history = decode_exact_json(third['diagnostic_json'])
    assert {row['receipt_id'] for row in history['prior_receipts']} == {first['receipt_id'], second['receipt_id']}
    assert {values['raw_source_record']['text_content'] for values in history['retained_processing'].values()} == {'first', 'second'}
    raw_witnesses = [w for w in third['witnesses'] if w['source_uri'] is None]
    assert [decode_exact_json(resolve_receipt_witness(third, w).decode()) for w in raw_witnesses] == [first_source, second_source, third_source]
    assert all(resolve_receipt_witness(third, w) for w in third['witnesses'])
    processing = native.processing_table(con, COMMENT)
    assert con.execute(f'SELECT * FROM {processing}').to_arrow_table().to_pylist() == [third_source]


def test_fresh_batch_refusal_commits_no_partial_subjects(con, monkeypatch):
    from spicy_regs import comments_bulk
    from spicy_regs.etl_receipts import decode_exact_json

    source(con)
    con.execute("INSERT INTO source SELECT * REPLACE ('refused' AS comment_id, 'not-json' AS attachments_json) FROM source")
    calls = []
    real = comments_bulk.write_bundle
    def batch(*args, **kwargs):
        calls.append(True)
        return real(*args, **kwargs)
    monkeypatch.setattr(comments_bulk, 'write_bundle', batch)
    monkeypatch.setattr(comments_bulk, '_BATCH', 1)
    with pytest.raises(native.CatalogConversionRefused):
        iceberg.replace_rows(con, COMMENT, 'source')
    assert calls == [True]
    assert con.execute(f'SELECT count(*) FROM {native.qualified(COMMENT)}').fetchone() == (0,)
    assert con.execute(f"SELECT count(*) FROM {native.receipts_table()} WHERE outcome='accepted'").fetchone() == (0,)
    [refused] = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='refused'").to_arrow_table().to_pylist()
    assert decode_exact_json(refused['processing_json'])['raw_source_record']['attachments_json'] == 'not-json'


def test_an_initialization_marker_written_under_an_earlier_policy_version_still_qualifies(con):
    """The live comments catalog was initialized under regulations-native-v1; the policy has been v2 since 2026-10-06."""
    from spicy_regs.etl_receipts import ReceiptContext, observation_receipt, receipt_policies
    from spicy_regs.transforms.regulations_receipts import policy

    native.ensure_native(con, COMMENT)
    current, earlier = receipt_policies(policy('comments'))[:2]
    assert earlier.policy_version == 'regulations-native-v1' != current.policy_version
    old = con.execute(f"SELECT generation_id, attempt_id, processor, witnesses FROM {native.receipts_table()}").fetchone()
    context = ReceiptContext(old[0], old[1], old[2], old[3])
    marker = observation_receipt(earlier, context, processing_fields={'input_metadata': {'catalog_initialized': True}})
    con.execute(f"DELETE FROM {native.receipts_table()}")
    con.register('marker', pl.DataFrame([marker]).to_arrow())
    con.execute(f"INSERT INTO {native.receipts_table()} SELECT * FROM marker")
    assert native.initialized(con, 'comments') is True
    native.require_initialized(con, COMMENT)
    # The exact initialization value under a version outside the lineage is refused by the version check alone.
    undeclared = dataclasses.replace(current, policy_version='regulations-native-v0')
    marker = observation_receipt(undeclared, context, processing_fields={'input_metadata': {'catalog_initialized': True}})
    con.execute(f"DELETE FROM {native.receipts_table()}")
    con.register('bad', pl.DataFrame([marker]).to_arrow())
    con.execute(f"INSERT INTO {native.receipts_table()} SELECT * FROM bad")
    with pytest.raises(ValueError, match='differs from field policy'):
        native.initialized(con, 'comments')


def test_a_catalog_in_an_earlier_shape_is_refused_naming_its_columns(con):
    """The live comments catalog is still the 21-column regulations-native-v1 table; the policy has 24."""
    native.ensure_native(con, COMMENT)
    con.execute(f'ALTER TABLE {native.qualified(COMMENT)} DROP COLUMN pdf_extraction_results_json')
    with pytest.raises(ValueError, match=r"differs from declared policy: missing \['pdf_extraction_results_json'\]"):
        native.require_initialized(con, COMMENT)


def _rewrite_accepted_receipts(con, processor):
    """Make every accepted comments receipt input-less, as the 2026-11-08 rebuild copied them."""
    import pyarrow as pa
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, exact_json, _digest
    rows = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'").to_arrow_table().to_pylist()
    for row in rows:
        old_id = row['receipt_id']
        row.update(processor=processor, processing_json=exact_json({}))
        row['receipt_id'] = _digest({k: v for k, v in row.items() if k != 'receipt_id'})
        con.register('_promoted', pa.Table.from_pylist([row], schema=RECEIPT_SCHEMA))
        con.execute(f'DELETE FROM {native.receipts_table()} WHERE receipt_id=?', [old_id])
        con.execute(f'INSERT INTO {native.receipts_table()} SELECT * FROM _promoted')
        con.unregister('_promoted')


_RESTRICTED = '[{"title":"t","formats":[{"url":"u","format":"pdf","size":4}],"restrictReason":"r"}]'
_CANONICAL = ('[{"title":"t","formats":[{"url":"u","format":"pdf","size":4}],"attachment_id":null,'
              '"restrict_reason":"r","restrict_reason_type":null}]')


@pytest.mark.parametrize('route', ['batch', 'row'])
def test_promotion_receipts_restore_from_subjects_and_allow_replacement(con, monkeypatch, route):
    from spicy_regs import comments_bulk
    from spicy_regs.transforms.regulations_receipts import COMMENTS_PROMOTION_PROCESSOR
    expected = source(con, attachments=_RESTRICTED)
    con.execute("INSERT INTO source SELECT * REPLACE ('c2' AS comment_id, NULL AS attachments_json) FROM source")
    iceberg.replace_rows(con, COMMENT, 'source')
    _rewrite_accepted_receipts(con, COMMENTS_PROMOTION_PROCESSOR)
    if route == 'batch':
        monkeypatch.setattr(native, 'read_with_receipts', lambda *a, **k: pytest.fail('row fallback'))
    else:
        def unproven(*args, **kwargs):
            raise etl_bulk.NotBulkEligible('forced row reader')
        monkeypatch.setattr(comments_bulk, 'restore_catalog', unproven)
    restored = native.processing_table(con, COMMENT)
    actual = {row['comment_id']: row for row in con.execute(f'SELECT * FROM {restored}').to_arrow_table().to_pylist()}
    assert actual['c1'] == expected | {'attachments_json': _CANONICAL}
    assert actual['c2'] == expected | {'comment_id': 'c2', 'attachments_json': None}
    # A nightly replacement restores the prior rows, commits, and reads its own rows back.
    prior = native.processing_table(con, COMMENT, where="comment_id='c1'")
    source(con, text='second', attachments=_RESTRICTED)
    iceberg.replace_rows(con, COMMENT, 'source', expected_prior=prior)
    assert con.execute(f'SELECT comment_id, text_content FROM {iceberg._qualified(COMMENT)} ORDER BY 1').fetchall() == [
        ('c1', 'second'), ('c2', 'first')]
    # The rewritten row now holds a normal receipt with its input; the untouched one keeps its promotion receipt.
    assert con.execute(f"SELECT processor FROM {native.receipts_table()} WHERE outcome='accepted' "
                       'ORDER BY 1').fetchall() == [(COMMENTS_PROMOTION_PROCESSOR,), ('regulatory-catalog-native-v1',)]


def test_input_less_receipt_from_another_processor_still_refuses(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    _rewrite_accepted_receipts(con, 'regulatory-catalog-native-v1')
    with pytest.raises(ValueError, match='exact retained processor input is required'):
        native.processing_table(con, COMMENT)


@pytest.mark.parametrize('route', ['batch', 'row'])
def test_mixed_promotion_and_normal_receipts_restore_in_one_batch(con, monkeypatch, route):
    from spicy_regs import comments_bulk
    from spicy_regs.transforms.regulations_receipts import COMMENTS_PROMOTION_PROCESSOR
    expected = source(con)
    assert expected['comment'] is None  # A promotion row with a null comment.
    con.execute("INSERT INTO source SELECT * REPLACE ('c2' AS comment_id, '<p>one&amp;two</p>' AS comment) FROM source")
    iceberg.replace_rows(con, COMMENT, 'source')
    _rewrite_accepted_receipts(con, COMMENTS_PROMOTION_PROCESSOR)
    source(con, text='second')  # c1 gets a normal receipt; c2 keeps its promotion receipt.
    iceberg.replace_rows(con, COMMENT, 'source')
    assert sorted(con.execute(f"SELECT processor FROM {native.receipts_table()} WHERE outcome='accepted'").fetchall()) == [
        (COMMENTS_PROMOTION_PROCESSOR,), ('regulatory-catalog-native-v1',)]
    if route == 'batch':
        monkeypatch.setattr(native, 'read_with_receipts', lambda *a, **k: pytest.fail('row fallback'))
    else:
        def unproven(*args, **kwargs):
            raise etl_bulk.NotBulkEligible('forced row reader')
        monkeypatch.setattr(comments_bulk, 'restore_catalog', unproven)
    restored = native.processing_table(con, COMMENT)
    actual = {row['comment_id']: row for row in con.execute(f'SELECT * FROM {restored}').to_arrow_table().to_pylist()}
    assert actual == {'c1': expected | {'text_content': 'second'},
                      'c2': expected | {'comment_id': 'c2', 'comment': '<p>one&amp;two</p>'}}


def test_row_reader_skips_processor_pairing_without_promotion_receipts(con, monkeypatch):
    from spicy_regs import comments_bulk
    expected = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    def unproven(*args, **kwargs):
        raise etl_bulk.NotBulkEligible('forced row reader')
    monkeypatch.setattr(comments_bulk, 'restore_catalog', unproven)
    monkeypatch.setattr(comments_bulk, 'receipt_processors', lambda *a, **k: pytest.fail('unneeded processor join'))
    restored = native.processing_table(con, COMMENT)
    assert con.execute(f'SELECT * FROM {restored}').to_arrow_table().to_pylist() == [expected]
