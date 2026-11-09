"""Bulk court restore exercises the maintained mapper on exceptional rows only."""
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import court_receipts, court_restore
from spicy_regs.court_receipts import local_receipt_selection, processing_rows, read_court_rows, restore_processing_input
from tests.test_court_bulk import FAMILIES, WITNESSES, _table, _write


@pytest.mark.parametrize('dataset', sorted(FAMILIES))
def test_restore_equals_maintained_decoder_with_refusals_and_escaped_values(tmp_path, dataset):
    columns, rows = FAMILIES[dataset](random.Random(17), 120)
    source = _write(tmp_path / 'source.parquet', _table(columns, rows))
    native = court_receipts.write_court_bulk(dataset, source, tmp_path / 'native',
                                            witnesses=WITNESSES, generation_id='selected')
    schema = pq.read_schema(source)
    expected = pa.Table.from_pylist(list(processing_rows(read_court_rows(native, dataset=dataset),
                                                       dataset=dataset, schema=schema)), schema=schema)
    restored = restore_processing_input(native, tmp_path / 'restored.parquet', dataset=dataset, schema=schema)
    assert pq.read_table(restored).equals(expected, check_metadata=True)


def test_restore_replays_only_exceptional_rows_and_admits_once(tmp_path, monkeypatch):
    dataset = 'court_citation_map'
    rows: list[dict] = [{'citing_opinion_id': str(i), 'cited_opinion_id': '2', 'depth': '007',
             'dump_date': '2026-06-30' if i != 27 else 'control\x1b'} for i in range(70)]
    rows[27]['depth'] = '7'
    source = _write(tmp_path / 'source.parquet', pa.Table.from_pylist(rows))
    rows[27]['depth'] = 7  # The maintained decoder owns integer-to-string restoration.
    native = court_receipts.write_court_rows(dataset, rows, tmp_path / 'native',
                                            witnesses=WITNESSES, generation_id='selected')
    seen, admissions = [], []
    original = court_receipts.processing_rows
    from spicy_regs import etl_bulk
    validate = etl_bulk._validated_bundle

    def decoded(rows, **kwargs):
        rows = list(rows)
        seen.extend(row['citing_opinion_id'] for row in rows)
        yield from original(rows, **kwargs)

    def admitted(*args, **kwargs):
        admissions.append(1)
        return validate(*args, **kwargs)

    monkeypatch.setattr(court_receipts, 'processing_rows', decoded)
    monkeypatch.setattr(etl_bulk, '_validated_bundle', admitted)
    restored = restore_processing_input(native, tmp_path / 'restored.parquet', dataset=dataset,
                                        schema=pq.read_schema(source))
    assert pq.read_table(restored).equals(pq.read_table(source))
    assert seen == ['27']
    assert admissions == [1]


def test_restore_batches_run_on_one_thread_then_restore_the_caller_setting(tmp_path, monkeypatch):
    # With 4 threads the live 10.2M-row cluster batches exceeded the 4GB engine limit (2026-11-09).
    from spicy_regs import etl_bulk

    dataset = 'court_citation_map'
    rows = [{'citing_opinion_id': str(i), 'cited_opinion_id': '2', 'depth': '1'} for i in range(30)]
    native = court_receipts.write_court_rows(dataset, rows, tmp_path / 'native',
                                            witnesses=WITNESSES, generation_id='selected')
    settings = []
    materialize = court_restore.materialize

    def observed(reader, *args):
        setting = "SELECT current_setting('threads')"
        monkeypatch.setattr(court_restore, 'checkpoint',
                            lambda: settings.append(reader.connection.execute(setting).fetchone()[0]))
        try:
            return materialize(reader, *args)
        finally:
            settings.append(reader.connection.execute(setting).fetchone()[0])

    monkeypatch.setattr(court_restore, 'materialize', observed)
    with etl_bulk.bulk_limits(threads=3, memory_limit='1GB', require_bulk=True):
        restore_processing_input(native, tmp_path / 'restored.parquet', dataset=dataset,
                                 schema=pa.schema([(name, pa.string()) for name in rows[0]]))
    assert settings == [1, 3]


def test_one_thread_restores_on_failure_and_keeps_the_original_error():
    import duckdb

    connection = duckdb.connect(config={'threads': 3})
    with pytest.raises(KeyError, match='original'):
        with court_restore._one_thread(connection):
            raise KeyError('original')
    assert connection.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    with pytest.raises(KeyError, match='original'):
        with court_restore._one_thread(connection):
            connection.close()  # The restore cannot run; the caller still sees its own failure.
            raise KeyError('original')


def test_restore_refuses_corrupt_selected_receipts_without_replacing_output(tmp_path):
    dataset = 'court_citation_map'
    source = _write(tmp_path / 'source.parquet', pa.table({'citing_opinion_id': ['1'], 'cited_opinion_id': ['2']}))
    native = court_receipts.write_court_bulk(dataset, source, tmp_path / 'native',
                                            witnesses=WITNESSES, generation_id='selected')
    receipts, _ = local_receipt_selection(native)
    table = pq.read_table(receipts)
    rows = table.to_pylist()
    rows[0]['receipt_id'] = 'sha256:' + '0' * 64
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), receipts)
    destination = tmp_path / 'existing.parquet'
    destination.write_bytes(b'preserved')
    with pytest.raises(ValueError):
        restore_processing_input(native, destination, dataset=dataset, schema=pq.read_schema(source))
    assert destination.read_bytes() == b'preserved'


@pytest.mark.parametrize('reference', [False, True])
def test_scoped_restore_uses_original_receipts_and_selects_only_for_reference(tmp_path, monkeypatch, reference):
    from spicy_regs import conversion_reads, etl_bulk

    dataset = 'court_citation_map'
    source = _write(tmp_path / 'source.parquet', pa.table({
        'citing_opinion_id': ['1'], 'cited_opinion_id': ['2'], 'depth': ['007'],
    }))
    native = court_receipts.write_court_bulk(dataset, source, tmp_path / 'native',
                                            witnesses=WITNESSES, generation_id='selected')
    receipts, _ = local_receipt_selection(native)
    table = pq.read_table(receipts)
    excluded = table.to_pylist()[0] | {'dataset': 'unselected', 'receipt_id': 'invalid'}
    # A semantically invalid excluded receipt is still decoded, then left out.
    # Its leading position also exercises noncontiguous selected receipt ordinals.
    pq.write_table(pa.concat_tables([pa.Table.from_pylist([excluded], schema=table.schema), table]), receipts)
    validate, select = etl_bulk._validated_bundle, conversion_reads.select_receipts
    admissions, copies = [], []

    def admitted(subjects, paths, policies, **kwargs):
        admissions.append((paths, kwargs['scoped']))
        if reference:
            raise etl_bulk.NotBulkEligible('exercise the maintained reference admission')
        return validate(subjects, paths, policies, **kwargs)

    def selected(path, destination, **kwargs):
        copies.append(path)
        return select(path, destination, **kwargs)

    monkeypatch.setattr(etl_bulk, '_validated_bundle', admitted)
    monkeypatch.setattr(conversion_reads, 'select_receipts', selected)
    monkeypatch.setattr(court_receipts, 'select_receipts',
                        lambda *a, **kw: pytest.fail('court restore copied receipts before admission'))
    restored = restore_processing_input(native, tmp_path / 'restored.parquet', dataset=dataset,
                                        schema=pq.read_schema(source))
    assert pq.read_table(restored).equals(pq.read_table(source))
    assert admissions == [([receipts], True)]
    assert copies == ([receipts] if reference else [])


def test_scoped_restore_refuses_changed_original_receipt_before_replacing_output(tmp_path, monkeypatch):
    dataset = 'court_citation_map'
    source = _write(tmp_path / 'source.parquet', pa.table({'citing_opinion_id': ['1'], 'cited_opinion_id': ['2']}))
    native = court_receipts.write_court_bulk(dataset, source, tmp_path / 'native',
                                            witnesses=WITNESSES, generation_id='selected')
    receipts, _ = local_receipt_selection(native)
    materialize = court_restore.materialize

    def changed(*args):
        with receipts.open('ab') as stream:
            stream.write(b'changed after admission')
        return materialize(*args)

    monkeypatch.setattr(court_restore, 'materialize', changed)
    destination = tmp_path / 'existing.parquet'
    destination.write_bytes(b'preserved')
    with pytest.raises(ValueError, match='Selected court input changed'):
        restore_processing_input(native, destination, dataset=dataset, schema=pq.read_schema(source))
    assert destination.read_bytes() == b'preserved'


def test_held_export_uses_restored_values_with_unicode_and_large_keys(tmp_path):
    from spicy_regs.transforms.build_court_opinion_clusters import _held_restored_export

    big = '9' * 50
    table = pa.table({'cluster_id': ['7', '١٢', big, 'not-id', '99'],
                      'ingest_source': ['bulk'] * 4 + ['search'],
                      'date_created': ['2025-01-01', '2025-01-02', None, '2025-02-03T00:00', '2026-01-01']})
    source = _write(tmp_path / 'restored.parquet', table)
    assert _held_restored_export(source) == (int(big), '2025-02-03')


def test_unknown_schema_has_no_bulk_restore_query():
    with pytest.raises(court_restore.etl_bulk.NotBulkEligible):
        court_restore._query('court_citation_map', court_receipts.POLICIES['court_citation_map'],
                             pa.schema([('unknown', pa.string())]))


def test_mapped_restore_schema_drift_refuses_under_require_bulk(tmp_path):
    from spicy_regs import etl_bulk

    dataset = 'court_citation_map'
    native = court_receipts.write_court_rows(dataset, [{'citing_opinion_id': '1', 'cited_opinion_id': '2', 'depth': '3'}],
                                            tmp_path / 'native', witnesses=WITNESSES, generation_id='selected')
    schema = pa.schema([('citing_opinion_id', pa.string()), ('cited_opinion_id', pa.string()), ('depth', pa.int64())])
    destination = tmp_path / 'existing.parquet'
    destination.write_bytes(b'preserved')
    with etl_bulk.bulk_limits(threads=1, memory_limit='1GB', require_bulk=True):
        with pytest.raises(ValueError, match='reference fallback disabled'):
            restore_processing_input(native, destination, dataset=dataset, schema=schema)
    assert destination.read_bytes() == b'preserved'


def test_unmapped_restore_remains_eligible_with_count_under_require_bulk(tmp_path):
    from loguru import logger
    from spicy_regs import etl_bulk

    dataset = 'court_docket_groups'
    native = court_receipts.write_court_rows(dataset, [{'parent_cl_docket_id': 'group', 'cl_docket_id': '1'}],
                                            tmp_path / 'native', witnesses=WITNESSES, generation_id='selected')
    schema = pa.schema([('parent_cl_docket_id', pa.string()), ('cl_docket_id', pa.string())])
    messages = []
    sink = logger.add(messages.append, format='{message}')
    try:
        with etl_bulk.bulk_limits(threads=1, memory_limit='1GB', require_bulk=True):
            destination = restore_processing_input(native, tmp_path / 'restored.parquet', dataset=dataset, schema=schema)
    finally:
        logger.remove(sink)
    assert pq.read_table(destination).to_pylist() == [{'parent_cl_docket_id': 'group', 'cl_docket_id': '1'}]
    assert any('dataset=court_docket_groups rows=1' in message for message in messages)
