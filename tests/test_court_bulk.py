"""The court bulk writer gives the bundle the row writer gives, row for row."""
import random
import re
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import court_bulk, etl_bulk
from spicy_regs.court_receipts import (
    POLICIES, finish_court_output, local_receipt_selection, parquet_rows, write_court_bulk, write_court_rows,
)
from spicy_regs.etl_receipts import validate_receipt_bundle

WITNESSES = [{'source_id': 'export.csv.bz2', 'source_uri': 'https://example.test/export.csv.bz2', 'sha256': 'sha256:' + 'a' * 64,
              'locator': None, 'body_version': None},
             {'source_id': 'staged.parquet', 'source_uri': None, 'sha256': 'b' * 64, 'locator': None,
              'body_version': None}]
EDGE_TEXT = ['', 'plain', 'quote " and \\ backslash', 'café 中文', 'astral \U0001f600', 'tab\tnewline\n',
             'bell\x07', 'vt\x0b', 'esc\x1b', 'unit\x1f', 'del\x7f', 'line sep', 'nbsp ', "it's", '[1,2]', 'null']


def _table(columns, rows, schema=None):
    schema = schema or pa.schema([(name, pa.string()) for name in columns])
    return pa.Table.from_pylist([dict(zip(columns, row)) for row in rows], schema=schema)


def _write(path, table):
    pq.write_table(table, path, row_group_size=700)
    return path


def citations(rng, count):
    columns = ('citation_id', 'cluster_id', 'volume', 'reporter', 'page', 'citation_type', 'date_created',
               'date_modified', 'dump_date')
    rows: list = [(str(i + 1), str(rng.randrange(1, 10**7)), str(rng.randrange(1, 900)), rng.choice(['U.S.', 'F.3d', 'S. Ct.']),
             str(rng.randrange(1, 2000)), rng.choice(['1', '2', None]), '2024-01-02T03:04:05.123456-08:00',
             rng.choice(['2024-05-06T07:08:09Z', None]), '2026-06-30') for i in range(count)]
    for n, text in enumerate(EDGE_TEXT):
        rows.append((f'e{n}', '7', text, text, text, None, text, text, text))
    rows += [('', '1', 'v', 'r', 'p', 't', None, None, None), (None, '1', 'v', 'r', 'p', 't', None, None, None)]
    return columns, rows


def citation_map(rng, count):
    columns = ('citing_opinion_id', 'cited_opinion_id', 'depth', 'dump_date')
    rows: list = [(str(rng.randrange(1, 10**8)), str(rng.randrange(1, 10**8)), str(rng.randrange(1, 40)), '2026-06-30')
            for _ in range(count)]
    depths = ['0', '007', '-0', '', 'abc', '99999999999999999999', '9223372036854775807', '9223372036854775808',
              '-9223372036854775808', '-9223372036854775809', None, '+5', ' 5', '5 ', '1.0', '٥', '1\n']
    rows += [(f'x{n}', str(n), depth, None) for n, depth in enumerate(depths)]
    rows += [('', '1', '1', None), ('1', None, '1', None)]
    return columns, rows


def parentheticals(rng, count):
    columns = ('parenthetical_id', 'described_opinion_id', 'describing_opinion_id', 'text', 'score', 'group_id',
               'dump_date')
    scores = [str(round(rng.random(), rng.randrange(1, 17))) for _ in range(count)]
    scores += [repr(rng.random()) for _ in range(count // 2)]
    rows: list = [(str(n), '1', '2', rng.choice(EDGE_TEXT), score, rng.choice([None, '9']), '2026-06-30')
            for n, score in enumerate(scores)]
    edge = ['0', '1', '0.5', '.5', '5.', '1e-300', '1e-310', '4.9e-324', '1E-3', '1.0000000000000002', 'nan', 'inf',
            '-0', '-0.0', '0.0', '1.0', '', None, '0_5', ' 0.5', '0.5 ', '1e5', '1e-400', '0.1e1', '+0.5', '٥',
            '2.2250738585072014e-308', '2.225073858507201e-308', '0.99999999999999989', '0.30000000000000004']
    rows += [(f'e{n}', '1', '2', 't', score, None, None) for n, score in enumerate(edge)]
    return columns, rows


CLUSTER_COLUMNS = ('cluster_id', 'cl_docket_id', 'court_id', 'court_is_federal', 'case_name', 'date_filed',
                   'date_filed_is_approximate', 'citation_count', 'scdb_votes_majority', 'attorneys', 'syllabus',
                   'blocked', 'slug', 'date_created', 'ingest_source')


def clusters(rng, count):
    rows: list = [(str(n + 1), str(rng.randrange(1, 10**6)), 'scotus', rng.choice(['t', 'f', 'T', 'FALSE', 'True', None]),
             'A v. B ' * rng.randrange(1, 5), '2001-01-01', rng.choice(['t', 'f']), str(rng.randrange(0, 5000)),
             rng.choice(['9', None, '0']), rng.choice(['A; B', None, '', 'Smith']), 'text ' * rng.randrange(0, 200),
             'f', f'slug-{n}', '2024-01-01T00:00:00Z', rng.choice(['bulk', None])) for n in range(count)]
    rows += [(f'e{n}', '1', 'c', flag, text, text, flag, count_, count_, text, text, flag, text, text, text)
             for n, (text, flag, count_) in enumerate(zip(EDGE_TEXT * 2, ['t', 'x', '', 'yes', '1', 'tRuE', 'f', None] * 4,
                                                          ['1', 'x', '', None, '-3', '12345678901234567890', '007', '+1'] * 4))]
    rows += [('', '1', 'c', 't', 'n', 'd', 't', '1', '1', 'a', 's', 't', 's', 'd', 'i')]
    return CLUSTER_COLUMNS, rows


OPINIONS = ('opinion_id', 'cluster_id', 'opinion_type', 'per_curiam', 'page_count', 'sha1', 'extracted_by_ocr',
            'local_path', 'dump_date')


def opinions(rng, count):
    rows: list = [(str(n + 1), str(rng.randrange(1, 100)), '010combined', rng.choice(['t', 'f', None]),
             rng.choice(['3', None, '0']), '%040x' % rng.getrandbits(160), 'f', f'pdf/{n}.pdf', '2026-06-30')
            for n in range(count)]
    rows += [('x1', '1', 'o', 'maybe', 'many', 's', 'f', None, None), ('', '1', 'o', 't', '1', 's', 'f', None, None)]
    return OPINIONS, rows


FAMILIES = {'court_citations': citations, 'court_citation_map': citation_map, 'court_parentheticals': parentheticals,
            'court_opinion_clusters': clusters, 'court_opinions': opinions}


def both(tmp_path, dataset, table, **options):
    source = _write(tmp_path / 'source.parquet', table)
    options: dict = {'witnesses': WITNESSES, 'generation_id': 'g-1', 'diagnostics': {'dump_date': '2026-06-30'}, **options}
    expected = write_court_rows(dataset, parquet_rows(source), tmp_path / 'row', **options)
    routed = []
    original = court_bulk.write_bundle

    def spy(*args, **kwargs):
        routed.append(original(*args, **kwargs))
        return routed[-1]
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(court_bulk, 'write_bundle', spy)
        actual = write_court_bulk(dataset, source, tmp_path / 'bulk', **options)
    return expected, actual, routed[0]


@pytest.mark.parametrize('dataset', sorted(FAMILIES))
def test_bulk_bundle_equals_row_bundle(tmp_path, dataset):
    columns, rows = FAMILIES[dataset](random.Random(7), 1500)
    expected, actual, routed = both(tmp_path, dataset, _table(columns, rows))
    assert pq.read_table(actual).equals(pq.read_table(expected))
    assert pq.read_schema(actual).equals(pq.read_schema(expected), check_metadata=True)
    receipts, generation = local_receipt_selection(actual)
    row_receipts, _ = local_receipt_selection(expected)
    assert generation == 'g-1'
    assert pq.read_table(receipts).equals(pq.read_table(row_receipts))
    outcomes = [row['outcome'] for row in parquet_rows(receipts)]
    assert outcomes == sorted(outcomes, key=lambda o: o != 'accepted')  # refusals follow every accepted receipt
    assert 'refused' in outcomes and 0 < routed < len(outcomes) // 5
    # The whole bundle, floats and escape-routed rows included, validates in SQL: a row-validator route would raise.
    with etl_bulk.bulk_limits(threads=2, memory_limit='1GB', require_bulk=True):
        validate_receipt_bundle({dataset: [actual]}, [receipts], [POLICIES[dataset]], generation_id=generation)


def test_shared_family_keeps_exact_rows_and_sources_and_adopts_one_receipt(tmp_path, monkeypatch):
    from spicy_regs import court_receipts
    from spicy_regs.court_receipts import build_court_generation, court_bulk_family, generation_options, read_court_rows
    from spicy_regs.etl_receipts import combine_receipts
    from spicy_regs.generations import verify_generation
    from spicy_regs.local_data import local_selection
    from spicy_regs.selected_generations import remember_generation
    from spicy_regs.sources import publication
    from tests.generation_fakes import Store

    datasets = ('court_citations', 'court_citation_map', 'court_parentheticals')
    baseline, shared, sources = [], [], []
    for dataset in datasets:
        columns, rows = FAMILIES[dataset](random.Random(7), 2)
        source = _write(tmp_path / (dataset + '-source.parquet'), _table(columns, rows))
        sources.append(source)
        baseline.append(write_court_bulk(dataset, source, tmp_path / 'standalone',
                                        witnesses=WITNESSES, generation_id='shared-build'))
    shards = [local_receipt_selection(path)[0] for path in baseline]
    combined = combine_receipts(shards, tmp_path / 'combined.parquet')
    with court_bulk_family(tmp_path / 'shared', datasets, generation_id='shared-build') as write:
        for dataset, source in zip(datasets, sources):
            shared.append(write(dataset, source, witnesses=WITNESSES))
            assert not shared[-1].exists()
    receipt, generation = local_receipt_selection(shared[0])
    assert generation == 'shared-build'
    assert list((tmp_path / 'shared').rglob('etl_receipts.parquet')) == [receipt]
    assert pq.read_table(receipt).equals(pq.read_table(combined))
    for dataset, expected, actual in zip(datasets, baseline, shared):
        assert pq.read_table(actual).equals(pq.read_table(expected), check_metadata=True)
        assert list(read_court_rows(actual, dataset=dataset)) == list(read_court_rows(expected, dataset=dataset))
    old_files = [*shards, combined]
    before = sum(path.stat().st_blocks * 512 for path in old_files)
    after = receipt.stat().st_blocks * 512
    print(f'Receipt fixture allocation: shards+combined={before}B; shared={after}B; saved={before - after}B')
    assert after < before
    foreign = write_court_bulk(datasets[-1], sources[-1], tmp_path / 'foreign',
                               witnesses=WITNESSES, generation_id='shared-build')
    for selection in (shared[:-1], [*shared, shared[-1]], [*shared[:-1], foreign], list(reversed(shared))):
        with pytest.raises(ValueError, match='complete receipt family'):
            generation_options(selection)
    monkeypatch.setattr(court_receipts, 'combine_receipts', lambda *args: pytest.fail('shared receipt must not copy'))
    inode = receipt.stat().st_ino
    directory = tmp_path / 'generation'
    artifact = build_court_generation(directory, family='court-citations', files=shared)
    assert not receipt.exists() and (directory / 'etl_receipts.parquet').stat().st_ino == inode
    verify_generation(directory, expected_pin=artifact.pin)
    remember_generation(tmp_path, directory, artifact)
    selected = local_selection(tmp_path)
    for dataset in datasets:
        assert selected.native[dataset].receipts == directory / 'etl_receipts.parquet'
        assert selected.paths(dataset) == (directory / (dataset + '.parquet'),)
    store = Store()
    index = publication.publish_generation(directory, client=store, bucket='test', prior_index=publication.empty_index())
    for dataset in datasets:
        member, = publication.receipt_members(index, dataset=dataset)
        assert store.objects[member.path] == (directory / 'etl_receipts.parquet').read_bytes()


def test_attorneys_as_a_native_list_and_missing_columns(tmp_path):
    columns, rows = clusters(random.Random(3), 200)
    index = columns.index('attorneys')
    lists = [[None, 'A', '', 'A'], None, [], ['B']]
    rows: list = [(*row[:index], lists[n % 4], *row[index + 1:]) for n, row in enumerate(rows)]
    schema = pa.schema([(name, pa.list_(pa.string()) if name == 'attorneys' else pa.string()) for name in columns])
    expected, actual, _ = both(tmp_path, 'court_opinion_clusters', _table(columns, rows, schema))
    assert pq.read_table(actual).equals(pq.read_table(expected))
    assert pq.read_table(local_receipt_selection(actual)[0]).equals(pq.read_table(local_receipt_selection(expected)[0]))
    # A source without the converted columns, and without attorneys, keeps the same shape.
    narrow = ('cluster_id', 'case_name', 'slug')
    table = _table(narrow, [(str(n), 'x', 's') for n in range(50)])
    (tmp_path / 'narrow').mkdir()
    expected, actual, _ = both(tmp_path / 'narrow', 'court_opinion_clusters', table)
    assert pq.read_table(local_receipt_selection(actual)[0]).equals(pq.read_table(local_receipt_selection(expected)[0]))


def test_context_text_the_sql_spells_differently_routes_every_row(tmp_path):
    columns, rows = citation_map(random.Random(1), 40)
    expected, actual, routed = both(tmp_path, 'court_citation_map', _table(columns, rows),
                                    diagnostics={'note': 'esc\x1b'}, witnesses=[{**WITNESSES[0], 'source_uri': 'https://example.test/x\x1b'}])
    assert routed == len(rows)
    assert pq.read_table(local_receipt_selection(actual)[0]).equals(pq.read_table(local_receipt_selection(expected)[0]))


@pytest.mark.parametrize('dataset', sorted(FAMILIES))
def test_control_character_only_in_receipt_field_uses_exact_row_encoding(tmp_path, dataset):
    from spicy_regs.court_subjects import IDENTITIES, RECEIPT_FIELDS
    from spicy_regs.etl_receipts import decode_exact_json

    receipt_field = next(name for name in RECEIPT_FIELDS[dataset] if name not in
                         {'conversion_inputs', 'raw_source_record', 'source_observation'})
    row = {name: '1' for name in IDENTITIES[dataset]}
    row[receipt_field] = 'receipt-only\x1b'
    expected, actual, routed = both(tmp_path, dataset, pa.Table.from_pylist([row]))
    assert routed == 1
    actual_receipts = pq.read_table(local_receipt_selection(actual)[0])
    assert actual_receipts.equals(pq.read_table(local_receipt_selection(expected)[0]))
    assert decode_exact_json(actual_receipts['processing_json'][0].as_py())[receipt_field] == row[receipt_field]


@pytest.mark.parametrize('bulk', [False, True])
def test_unresolved_refusal_survives_then_exact_success_resolves_it(tmp_path, bulk):
    def run(name, rows, prior=None):
        options: dict = dict(witnesses=WITNESSES, generation_id=name, prior_receipts=prior)
        if bulk:
            schema = pa.schema([(name, pa.string()) for name in ('citing_opinion_id', 'cited_opinion_id', 'depth')])
            source = _write(tmp_path / (name + '.parquet'), pa.Table.from_pylist(rows, schema=schema))
            path = write_court_bulk('court_citation_map', source, tmp_path / name, **options)
        else:
            path = write_court_rows('court_citation_map', rows, tmp_path / name, **options)
        return local_receipt_selection(path)[0]

    bad = {'citing_opinion_id': '01', 'cited_opinion_id': '2', 'depth': 'bad'}
    first = run('first', [bad])
    refused, = list(parquet_rows(first))
    second = run('second', [], first)
    assert list(parquet_rows(second)) == [refused]
    third = run('third', [{**bad, 'citing_opinion_id': '1', 'depth': '7'}], second)
    assert list(parquet_rows(third))[-1] == refused
    fourth = run('fourth', [{**bad, 'depth': '7'}], third)
    accepted, = list(parquet_rows(fourth))
    assert accepted['outcome'] == 'accepted'


def test_refused_updates_follow_every_receipt_and_must_match_the_build(tmp_path):
    columns, rows = citation_map(random.Random(2), 60)
    source = _write(tmp_path / 'incoming.parquet', _table(columns, rows))
    updates = write_court_rows('court_citation_map', parquet_rows(source), tmp_path / 'updates', witnesses=WITNESSES,
                               generation_id='g-1', attempt_prefix='incoming')
    refused = local_receipt_selection(updates)[0]
    columns, rows = citation_map(random.Random(5), 80)
    expected, actual, _ = both(tmp_path, 'court_citation_map', _table(columns, rows), refused_receipts=refused)
    assert pq.read_table(local_receipt_selection(actual)[0]).equals(pq.read_table(local_receipt_selection(expected)[0]))
    with pytest.raises(ValueError, match='differ from the selected court build'):
        write_court_bulk('court_citation_map', tmp_path / 'source.parquet', tmp_path / 'other', witnesses=WITNESSES,
                         generation_id='g-2', refused_receipts=refused)
    assert not (tmp_path / 'other' / 'court_citation_map.parquet').exists()
    assert list((tmp_path / 'other').glob('.court-etl/failed-*'))  # the failed attempt is retained as a receipt


def test_prior_receipts_are_carried_not_inherited(tmp_path):
    columns, rows = citation_map(random.Random(4), 300)
    table = _table(columns, rows)
    source = _write(tmp_path / 'source.parquet', table)
    first = write_court_bulk('court_citation_map', source, tmp_path / 'one', witnesses=WITNESSES, generation_id='g-1')
    prior = local_receipt_selection(first)[0]
    changed = [(*row[:2], '30' if n == 5 else row[2], *row[3:]) for n, row in enumerate(rows)]
    _write(tmp_path / 'next.parquet', _table(columns, changed))
    second = write_court_bulk('court_citation_map', tmp_path / 'next.parquet', tmp_path / 'two', witnesses=WITNESSES,
                              generation_id='g-2', prior_receipts=prior)
    before = {r['record_id']: r for r in parquet_rows(prior) if r['outcome'] == 'accepted'}
    after = list(parquet_rows(local_receipt_selection(second)[0]))
    kept = [r for r in after if r['outcome'] == 'accepted' and before.get(r['record_id']) == r]
    assert len(kept) == len(before) - 1  # one changed row has a fresh receipt; every other keeps its prior one verbatim
    assert {r['generation_id'] for r in after} == {'g-1', 'g-2'}
    validate_receipt_bundle({'court_citation_map': [second]}, [local_receipt_selection(second)[0]],
                            [POLICIES['court_citation_map']], generation_id='g-2')


def test_ineligible_sources_stay_on_the_row_writer_loudly(tmp_path, monkeypatch):
    # A source column the mapper would refuse row by row is not mapped here.
    table = pa.table({'citing_opinion_id': ['1'], 'cited_opinion_id': ['2'], 'depth': pa.array([3], pa.int64())})
    source = _write(tmp_path / 'int.parquet', table)
    assert not court_bulk.eligible('court_citation_map', table.schema)
    assert not court_bulk.eligible('court_dockets', pa.schema([('cl_docket_id', pa.string())]))
    with pytest.raises(etl_bulk.NotBulkEligible):
        court_bulk.write_bundle('court_citation_map', source, tmp_path / 's.parquet', tmp_path / 'r.parquet',
                                policy=POLICIES['court_citation_map'], generation_id='g', processor='p',
                                witnesses=WITNESSES, diagnostics={}, attempt_prefix='row', attempt=lambda raw, ordinal: (None, {}))
    with etl_bulk.bulk_limits(threads=1, memory_limit='1GB', require_bulk=True):
        with pytest.raises(ValueError, match='reference fallback disabled'):
            finish_court_output('court_citation_map', source, tmp_path / 'out', witnesses=WITNESSES)
    # Outside require_bulk the row writer takes it and its answer stands.
    path = finish_court_output('court_citation_map', source, tmp_path / 'out', witnesses=WITNESSES, generation_id='g')
    assert pq.read_table(path).to_pylist()[0]['depth'] == 3


def test_a_local_witness_path_is_refused_before_any_receipt_is_written(tmp_path):
    columns, rows = citation_map(random.Random(7), 3)
    source = _write(tmp_path / 'source.parquet', _table(columns, rows))
    local = [WITNESSES[0] | {'source_uri': '/Users/builder/export.csv.bz2'}]
    with pytest.raises(ValueError, match='local filesystem path'):
        court_bulk.write_bundle('court_citation_map', source, tmp_path / 's.parquet', tmp_path / 'r.parquet',
                                policy=POLICIES['court_citation_map'], generation_id='g', processor='p',
                                witnesses=local, diagnostics={}, attempt_prefix='row', attempt=lambda raw, ordinal: (None, {}))
    assert not (tmp_path / 'r.parquet').exists()


def test_a_cut_short_read_or_duckdb_fault_is_not_retried_by_row(tmp_path, monkeypatch):
    columns, rows = citation_map(random.Random(9), 50)
    source = _write(tmp_path / 'source.parquet', _table(columns, rows))
    monkeypatch.setattr(court_bulk.etl_bulk, 'write_split', lambda *a, **k: (_ for _ in ()).throw(
        etl_bulk.bulk_failure('Bulk court_citation_map write failed', RuntimeError('disk full'))))
    with pytest.raises(etl_bulk.BulkValidationError, match='no row fallback'):
        finish_court_output('court_citation_map', source, tmp_path / 'out', witnesses=WITNESSES, generation_id='g')
    assert not (tmp_path / 'out' / 'court_citation_map.parquet').exists()
    assert list((tmp_path / 'out').glob('.court-etl/failed-*'))


def test_decimal_text_parses_to_the_same_double_as_python():
    import duckdb

    rng = random.Random(11)
    texts = [repr(rng.random()) for _ in range(20000)] + [str(rng.randrange(0, 10**17)) + 'e-17' for _ in range(5000)]
    texts += [format(rng.random() * 10 ** rng.randrange(-30, 0), '.17g') for _ in range(5000)]
    got = duckdb.connect().execute("SELECT TRY_CAST(t AS DOUBLE) FROM (SELECT unnest(?) AS t)", [texts]).fetchall()
    assert [struct.pack('<d', g[0]) for g in got] == [struct.pack('<d', float(t)) for t in texts]
    assert all(re.fullmatch(court_bulk._DECIMAL.strip("'"), t) for t in texts)


def test_the_pipelines_own_source_schemas_are_mapped():
    import importlib

    tables = importlib.import_module('spicy_regs.transforms.build_court_bulk_tables')
    _SCHEMA = importlib.import_module('spicy_regs.transforms.build_court_opinion_clusters')._SCHEMA

    for table in (tables.CITATIONS, tables.CITATION_MAP, tables.PARENTHETICALS, tables.OPINIONS):
        assert court_bulk.eligible(table.output.removesuffix('.parquet'), table.input_schema)
    assert court_bulk.eligible('court_opinion_clusters', _SCHEMA)
    # The retained all-text clusters table a conversion starts from has the prose column as one string.
    assert court_bulk.eligible('court_opinion_clusters', pa.schema(
        [(name, pa.string()) for name in _SCHEMA.names]))
