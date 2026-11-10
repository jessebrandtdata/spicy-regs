"""The maintained service-term key bridges exact native index spellings."""
from dataclasses import replace
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import congress_receipts
from spicy_regs.congress_subjects import subject_schema
from spicy_regs.etl_receipts import receipt_policies, validate_receipt_bundle, validate_receipt_row
from spicy_regs.explorer_navigation import declarations, published_navigation, target_keys
from spicy_regs.navigation_measurement_runner import canonical_routes
from spicy_regs.navigation_measurements import MeasurementCache
from spicy_regs.table_joins import JOINS, RETIRED_PROCESSING_JOINS, SOURCE_JOINS
from spicy_regs.transforms.build_member_vote_terms import HALF_OPEN, INCLUSIVE_END
from tests.test_remaining_main_navigation import selected

NAME = 'member_vote_terms.bioguide_id+term_index -> member_terms.bioguide_id+term_index'


def recipe():
    return next(spec for spec in declarations(RETIRED_PROCESSING_JOINS) if spec['id'] == NAME)


def test_service_term_uses_one_recipe_derived_from_the_original_measured_relationship():
    original = next(join for join in SOURCE_JOINS if join.name == NAME)
    assert (original.child_columns, original.parent_columns, original.kind) == (
        ('bioguide_id', 'term_index'), ('bioguide_id', 'term_index'), 'complete')
    assert original in RETIRED_PROCESSING_JOINS
    assert original not in JOINS
    assert len([spec for spec in canonical_routes() if spec['id'] == NAME]) == 1
    spec = recipe()
    target = spec['targets'][0]
    assert (spec['source'], target['table'], target['columns']) == (
        original.child, original.parent, list(original.parent_columns))
    assert target['guards'][0]['values'] == [HALF_OPEN, INCLUSIVE_END]
    assert not spec['receiptFields']


@pytest.mark.parametrize('index', ['0', '1', '29', 0, 1, 29])
@pytest.mark.parametrize('status', [HALF_OPEN, INCLUSIVE_END])
def test_canonical_string_and_native_integer_indices_share_exact_words(index, status):
    row = {'bioguide_id': 'X000001', 'term_index': index, 'term_match': status}
    assert target_keys(recipe()['targets'][0], {}, row) == ['X000001', str(index)]
    assert row['term_index'] == index


@pytest.mark.parametrize('index', [None, '', '00', '01', '-1', '+1', ' 1', '1 ', '1\n', '1.0',
                                  '1e0', '１', True, False, 1.0, Decimal('1'), {}, []])
def test_noncanonical_indices_remain_unsupported_without_casting(index):
    assert target_keys(recipe()['targets'][0], {}, {
        'bioguide_id': 'X000001', 'term_index': index, 'term_match': HALF_OPEN}) is None


@pytest.mark.parametrize('status', [None, '', 'matched', 'unmatched', 'ambiguous', 'unresolved_member', 'undated'])
def test_a_present_index_does_not_replace_the_writer_success_status(status):
    assert target_keys(recipe()['targets'][0], {}, {
        'bioguide_id': 'X000001', 'term_index': '0', 'term_match': status}) is None


@pytest.mark.parametrize('field', ['bioguide_id', 'term_index', 'term_match'])
def test_missing_main_source_fields_refuse_both_directions(field):
    schemas = {'member_vote_terms': [('vote_id', 'VARCHAR'), ('member_key', 'VARCHAR'),
        ('bioguide_id', 'VARCHAR'), ('term_index', 'VARCHAR'), ('term_match', 'VARCHAR')],
        'member_terms': [('bioguide_id', 'VARCHAR'), ('term_index', 'BIGINT')]}
    schemas['member_vote_terms'] = [entry for entry in schemas['member_vote_terms'] if entry[0] != field]
    target = published_navigation([recipe()], schemas)[0]['targets'][0]
    assert not target['directions']['forward']['available']
    assert not target['directions']['reverse']['available']


@pytest.mark.parametrize('copies', [1, 2])
@pytest.mark.parametrize('duplicate_source', [False, True])
def test_guarded_flat_rows_match_independent_native_sql_counts_and_keep_ambiguity(tmp_path, copies, duplicate_source):
    rows = [
        {'vote_id': 'v1', 'member_key': 'one', 'bioguide_id': 'X', 'term_index': '0', 'term_match': HALF_OPEN},
        {'vote_id': 'v1' if duplicate_source else 'v2', 'member_key': 'one', 'bioguide_id': 'X', 'term_index': '0', 'term_match': INCLUSIVE_END},
        {'vote_id': 'v3', 'member_key': 'one', 'bioguide_id': 'Y', 'term_index': '1', 'term_match': HALF_OPEN},
        {'vote_id': 'v4', 'member_key': 'one', 'bioguide_id': 'Z', 'term_index': '0', 'term_match': 'ambiguous'},
        {'vote_id': 'v5', 'member_key': 'one', 'bioguide_id': 'X', 'term_index': '00', 'term_match': HALF_OPEN},
        {'vote_id': 'v6', 'member_key': 'one', 'bioguide_id': 'X', 'term_index': None, 'term_match': 'unmatched'},
    ]
    targets = [{'bioguide_id': 'X', 'term_index': 0}] * copies + [{'bioguide_id': 'unused', 'term_index': 0}]
    index, paths = selected(tmp_path, [('member_vote_terms', rows, None), ('member_terms', targets, None)])
    result = MeasurementCache(tmp_path / 'cache').measure(index, paths, recipe(), source_identity=('vote_id', 'member_key'))
    with duckdb.connect() as con:
        con.register('source', pa.Table.from_pylist(rows))
        con.register('target', pa.Table.from_pylist(targets))
        counts = con.sql("""WITH eligible AS (SELECT * FROM source WHERE term_match IN ('half_open','inclusive_end')
            AND regexp_full_match(term_index,'0|[1-9][0-9]*')), keys AS (
            SELECT bioguide_id,CAST(term_index AS VARCHAR) term_index,count(*) n FROM target GROUP BY ALL)
            SELECT count(*),count(*) FILTER(WHERE k.n IS NOT NULL),count(*) FILTER(WHERE k.n IS NULL),
            count(*) FILTER(WHERE k.n>1),count(DISTINCT ROW(s.vote_id,s.member_key)) FILTER(WHERE k.n IS NOT NULL)
            FROM eligible s LEFT JOIN keys k USING(bioguide_id,term_index)""").fetchone()
    assert counts is not None
    assert tuple(result[field] for field in ('eligible', 'matched', 'missing', 'ambiguous')) == counts[:4]
    assert result['unsupportedReferences'] == 3
    assert result['reverse']['maximumReferencesPerTarget'] == 2
    assert result['reverse']['maximumPhysicalSourceRowsPerTarget'] == 2
    assert result['aggregationMethods']['targetKeys'] == 'grouped_keys'
    assert result['aggregationMethods']['reverseSourceKeys'] == 'grouped_keys'
    if duplicate_source:
        assert result['distinctMatchedSourceRecords'] is None
        assert result['aggregationMethods']['sourceRecords'] == 'grouped_occurrences'
    else:
        assert result['distinctMatchedSourceRecords'] == counts[4] == 2
        assert result['reverse']['maximumDistinctSourceRecordsPerTarget'] == 2
        assert result['aggregationMethods']['sourceRecords'] == 'qualified_flat_row'


def test_native_policy_promotes_only_match_status_and_restores_exact_processing_values(tmp_path):
    raw = {'vote_id': 'v1', 'member_key': 'one', 'bioguide_id': 'X', 'term_index': '0', 'term_match': HALF_OPEN}
    source = tmp_path / 'processing.parquet'
    pq.write_table(pa.Table.from_pylist([raw]), source)
    current = congress_receipts.policy('member_vote_terms')
    assert current.policy_version == 'congress-subjects/2'
    earlier, = receipt_policies(current)[1:]
    assert earlier.policy_version == 'congress-subjects/1'
    assert current.identity_fields == earlier.identity_fields == ('vote_id', 'member_key')
    assert current.subject_schema.remove(current.subject_schema.get_field_index('term_match')).equals(earlier.subject_schema)
    assert subject_schema('member_vote_terms').field('term_index').type == pa.int64()
    subject, receipts = congress_receipts.write_congress_dataset(source, tmp_path / 'new', dataset='member_vote_terms', generation_id='g')
    assert subject is not None
    assert pq.read_table(subject)['term_match'].to_pylist() == [HALF_OPEN]
    schemas = {'member_vote_terms': [('vote_id', 'VARCHAR'), ('member_key', 'VARCHAR'), ('bioguide_id', 'VARCHAR'),
        ('term_index', 'BIGINT'), ('term_match', 'VARCHAR')], 'member_terms': [('bioguide_id', 'VARCHAR'), ('term_index', 'BIGINT')]}
    target = published_navigation([recipe()], schemas)[0]['targets'][0]
    assert target['directions']['forward']['available'] and target['directions']['reverse']['available']
    restored = congress_receipts.restore_processing_input(subject, receipts, tmp_path / 'restored.parquet', dataset='member_vote_terms', generation_id='g')
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)


def test_old_exact_native_policy_and_receipts_remain_readable_without_main_status(tmp_path, monkeypatch):
    raw = {'vote_id': 'v1', 'member_key': 'one', 'bioguide_id': 'X', 'term_index': '0', 'term_match': HALF_OPEN}
    source = tmp_path / 'processing.parquet'
    pq.write_table(pa.Table.from_pylist([raw]), source)
    from spicy_regs.policy_migrations import earlier_policy
    earlier = earlier_policy('member_vote_terms', 'congress-subjects/1')
    current_map = congress_receipts.map_record
    def old_map(dataset, row):
        mapped = current_map(dataset, row)
        assert mapped.subject is not None
        return replace(mapped, subject={key: value for key, value in mapped.subject.items() if key != 'term_match'})
    with monkeypatch.context() as old:
        old.setattr(congress_receipts, 'policy', lambda _dataset: earlier)
        old.setattr(congress_receipts, 'map_record', old_map)
        subject, receipts = congress_receipts.write_congress_dataset(source, tmp_path / 'old', dataset='member_vote_terms', generation_id='old')
    assert subject is not None and 'term_match' not in pq.read_schema(subject).names
    assert congress_receipts.input_policy('member_vote_terms', subject).descriptor() == earlier.descriptor()
    validate_receipt_bundle({'member_vote_terms': [subject]}, [receipts],
        [congress_receipts.input_policy('member_vote_terms', subject)], generation_id='old')
    for row in pq.read_table(receipts).to_pylist():
        validate_receipt_row(row, {'member_vote_terms': congress_receipts.policy('member_vote_terms')})
    restored = congress_receipts.restore_processing_input(subject, receipts, tmp_path / 'restored-old.parquet', dataset='member_vote_terms', generation_id='old')
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)
    # The old receipt still restores the exact original status; it never supplies
    # a missing main field to the public navigation recipe.
    schemas = {'member_vote_terms': [('name', 'VARCHAR') for name in earlier.subject_schema.names],
        'member_terms': [('bioguide_id', 'VARCHAR'), ('term_index', 'BIGINT')]}
    assert not published_navigation([recipe()], schemas)[0]['targets'][0]['sourceAvailable']


def test_flat_transformed_foreign_keys_keep_collisions_guards_and_duplicate_targets(tmp_path):
    from spicy_regs.explorer_navigation import array, guard, key, part, route
    rows = [{'id': 'one', 'fk': 'A', 'allow': 'yes'}, {'id': 'two', 'fk': 'a', 'allow': 'yes'},
            {'id': 'three', 'fk': 'missing', 'allow': 'yes'}, {'id': 'four', 'fk': 'A', 'allow': 'no'},
            {'id': 'five', 'fk': None, 'allow': 'yes'}]
    targets = [{'id': 'a'}, {'id': 'a'}, {'id': 'unused'}]
    index, paths = selected(tmp_path, [('source', rows, None), ('target', targets, None)])
    spec = array('transformed-flat', 'source', (), (
        route('target', ('id',), (key(part('fk', row=True, transform='lower')),),
              guard('allow', row=True, values=('yes',))),), meaning='Fixture', mode='row')
    result = MeasurementCache(tmp_path / 'cache').measure(index, paths, spec, source_identity=('id',))
    with duckdb.connect() as con:
        con.register('source', pa.Table.from_pylist(rows))
        con.register('target', pa.Table.from_pylist(targets))
        oracle = con.sql("""WITH keys AS (SELECT id,count(*) n FROM target GROUP BY id)
            SELECT count(*),count(*) FILTER(WHERE k.n IS NOT NULL),count(*) FILTER(WHERE k.n IS NULL),
            count(*) FILTER(WHERE k.n>1),count(DISTINCT s.id) FILTER(WHERE k.n IS NOT NULL)
            FROM source s LEFT JOIN keys k ON lower(s.fk)=k.id WHERE s.allow='yes' AND s.fk IS NOT NULL""").fetchone()
    assert oracle is not None
    assert tuple(result[field] for field in ('eligible', 'matched', 'missing', 'ambiguous', 'distinctMatchedSourceRecords')) == oracle
    assert result['unsupportedReferences'] == 2
    assert result['aggregationMethods'] == {'sourceRecords': 'qualified_flat_row', 'targetKeys': 'grouped_keys', 'reverseSourceKeys': 'grouped_keys'}
    assert result['reverse']['matchedTargetRows'] == 2
    assert result['reverse']['maximumReferencesPerTarget'] == 2
    assert result['reverse']['maximumPhysicalSourceRowsPerTarget'] == 2
    assert result['reverse']['maximumDistinctSourceRecordsPerTarget'] == 2
