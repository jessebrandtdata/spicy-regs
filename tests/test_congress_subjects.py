"""Subject semantics and conservative conversion boundaries for Congress datasets."""
import json
from typing import Any

import pyarrow as pa
import pytest

from spicy_regs.congress_subjects import (
    INPUT_COLUMNS, NATIVE, PROCESSING, PROMOTED, RECEIPT_ONLY, map_record, subject_schema,
)


@pytest.mark.parametrize('dataset', INPUT_COLUMNS)
def test_every_declared_field_has_a_destination_and_unknown_fields_refuse(dataset):
    raw: dict[str, Any] = dict.fromkeys(INPUT_COLUMNS[dataset])
    mapped = map_record(dataset, raw)
    assert mapped.source_fields == raw
    if dataset in RECEIPT_ONLY:
        assert mapped.subject is None
    else:
        assert mapped.subject is not None
        assert set(mapped.subject) == set(subject_schema(dataset).names)
        # A field the receipts alone held stays out, unless its table publishes it since its receipts retired.
        assert set(mapped.subject) & PROCESSING[dataset] == set(PROMOTED.get(dataset, ())) & PROCESSING[dataset]
    with pytest.raises(ValueError, match='unclassified source fields'):
        map_record(dataset, raw | {'unreviewed_property': 'new fact'})


@pytest.mark.parametrize('dataset,column', NATIVE)
def test_native_lists_keep_unknown_null_and_empty_separate(dataset, column):
    assert map_record(dataset, {column: None}).subject is not None
    if (dataset, column) in {('record_issues', 'chambers'), ('record_issues', 'section_names')}:
        raw = ''
    elif column == 'tallies_json':
        raw = '{}'
    else:
        raw = '[]'
    result = map_record(dataset, {column: raw})
    assert result.subject is not None
    target = 'rins' if column == 'rin_occurrences_json' else column.removesuffix('_json')
    assert result.subject[target] == []
    assert result.source_fields[column] == raw


def test_list_order_repetition_and_null_items_survive_arrow_round_trip():
    raw = {'fec_ids_json': '["H0VA00001",null,"H0VA00001"]', 'other_names_json': '[{"last":"A","middle":null},{"last":"A"}]'}
    mapped = map_record('members', raw)
    assert mapped.subject is not None
    table = pa.Table.from_pylist([mapped.subject], schema=subject_schema('members'))
    row = table.to_pylist()[0]
    assert row['fec_ids'] == ['H0VA00001', None, 'H0VA00001']
    assert len(row['other_names']) == 2
    assert mapped.source_fields == raw  # absent middle vs explicit null stays exact in receipt input
    rins = map_record('house_communications', {'rin_occurrences_json': '[{"rin":"1234-AA01"},null,{"rin":"1234-AA01"}]'})
    assert rins.subject is not None
    assert rins.subject['rins'] == ['1234-AA01', None, '1234-AA01']


def test_business_status_and_withdrawal_survive_processing_split():
    meeting = map_record('committee_meetings', {'meeting_status': 'Canceled'}).subject
    assert meeting is not None and meeting['meeting_status'] == 'Canceled'
    bill = map_record('congress_bills', {
        'stage': 'vetoed', 'stage_rule': 'code', 'stage_matcher': 'E20000', 'cosponsors_outcome': 'empty'})
    assert bill.subject is not None
    # The rule that gave the stage is a column beside it; its matcher and the read's outcome stay in the receipt.
    assert bill.subject['stage'] == 'vetoed' and bill.subject['stage_rule'] == 'code'
    assert not {'stage_matcher', 'cosponsors_outcome'} & set(bill.subject)
    assert bill.source_fields['stage_matcher'] == 'E20000' and bill.source_fields['cosponsors_outcome'] == 'empty'
    row = map_record('bill_cosponsors', {'is_original_raw': 'True', 'sponsorship_withdrawn_date': '2026-01-01'})
    assert row.subject is not None
    assert row.subject['is_original'] is True
    assert row.subject['sponsorship_withdrawn_date'] == '2026-01-01'


def test_nomination_and_affiliation_scalars_are_not_hidden_in_raw_json():
    n = map_record('nominations', {'nomination_type_json': '{"isMilitary":true}'})
    assert n.subject is not None
    assert n.subject['is_military'] is True and n.subject['is_civilian'] is None
    a = map_record('member_party_affiliations', {'source_json': '{"party":"Independent","caucus":"Democrat"}'})
    assert a.subject is not None
    assert a.subject['caucus'] == 'Democrat'


def test_named_candidates_are_tallies_and_empty_amendment_ids_do_not_collapse():
    mapped = map_record('roll_call_votes', {
        'tallies_json': '{"Jeffries":212,"Johnson (LA)":218,"Not Voting":0}',
        'amendments_json': '[{"number":null,"purpose":"A"},{"number":null,"purpose":"B"}]',
        'result': 'Passed',
    })
    assert mapped.subject is not None
    assert mapped.subject['tallies'] == [
        {'choice': 'Jeffries', 'count': 212}, {'choice': 'Johnson (LA)', 'count': 218},
        {'choice': 'Not Voting', 'count': 0},
    ]
    assert [a['purpose'] for a in mapped.subject['amendments']] == ['A', 'B']
    assert mapped.subject['result'] == 'Passed'


@pytest.mark.parametrize('raw', ['[', '{}', '[{"name":"A","unreviewed":"B"}]'])
def test_malformed_and_unreviewed_nested_values_refuse(raw):
    with pytest.raises((ValueError, TypeError)):
        map_record('committee_meetings', {'witnesses_json': raw})


def test_repeated_json_keys_do_not_silently_disappear():
    with pytest.raises(ValueError, match='Repeated JSON object key'):
        map_record('roll_call_votes', {'tallies_json': '{"Yea":3,"Yea":4}'})


@pytest.mark.parametrize('raw', ['many', '2.5', '-1', '', True])
def test_bad_count_does_not_turn_into_null_or_truncate(raw):
    with pytest.raises(ValueError):
        map_record('congress_bills', {'action_count': raw})


def test_public_activity_domain_payload_is_flat_and_model_events_are_receipt_only():
    event = map_record('public_activity_events', {
        'event_type': 'stage_changed',
        'event_data_json': json.dumps({'from': 'introduced', 'to': 'enacted', 'rule': 'code', 'matcher': 'E40000'}),
    })
    assert event.subject is not None
    assert event.subject['from_stage'] == 'introduced' and event.subject['to_stage'] == 'enacted'
    assert 'rule' not in event.subject
    summary = {'event_type': 'summary_generated', 'event_data_json': '{"model":"m","prompt_version":"v"}'}
    assert map_record('public_activity_events', summary).subject is None
    assert map_record('public_activity_events', summary).source_fields == summary


def test_native_relationship_reads_keep_independent_occurrences_and_qualified_pairs():
    import duckdb
    from spicy_regs.relationship_views.core import install_arrays
    from spicy_regs.relationship_views.congress import NATIVE_CONGRESS_RELATIONSHIPS
    con = duckdb.connect()
    meeting = map_record('committee_meetings', {
        'congress': '119', 'chamber': 'house', 'event_id': '7', 'meeting_status': 'Canceled',
        'bill_ids_json': '["119-hr-1","119-hr-1",null]',
        'hearing_jackets_json': '["12345","23456"]',
    }).subject
    con.register('committee_meetings', pa.Table.from_pylist([meeting], schema=subject_schema('committee_meetings')))
    specs = [s for s in NATIVE_CONGRESS_RELATIONSHIPS if s.name in {'meeting_bills', 'meeting_hearing_jackets'}]
    install_arrays(con, ['committee_meetings'], specs)
    assert con.sql('select count(*) from meeting_bills_occurrences').fetchone() == (3,)
    assert con.sql('select count(*) from meeting_bills_pairs').fetchone() == (1,)
    assert con.sql('select count(*) from meeting_hearing_jackets_occurrences').fetchone() == (2,)
    assert con.sql('select distinct meeting_status from meeting_bills_occurrences').fetchone() == ('Canceled',)
    bill = map_record('congress_bills', {'bill_id': '119-hr-1', 'related_bills_json': json.dumps([
        {'bill_type': 's', 'congress': '119', 'number': '2',
         'relationship_details': [{'identifiedBy': 'CRS', 'type': 'Related bill'}]},
    ])}).subject
    con.register('congress_bills', pa.Table.from_pylist([bill], schema=subject_schema('congress_bills')))
    install_arrays(con, ['congress_bills'], [s for s in NATIVE_CONGRESS_RELATIONSHIPS if s.name == 'bill_related_bills'])
    details = con.sql('select relationship_details from bill_related_bills_occurrences').arrow().read_all()
    assert pa.types.is_list(details.schema.field('relationship_details').type)
    assert details.to_pylist()[0]['relationship_details'] == [{'identifiedBy': 'CRS', 'type': 'Related bill'}]
    con.close()


@pytest.mark.parametrize('intervals,expected,unusable', [
    ([('2025-01-01', '2026-01-01')], 'found', 0),
    ([('2025-01-01', '2025-06-01')], 'missing', 0),
    ([('2025-1-01', '2026-01-01')], 'missing', 1),
    ([('2025-01-01', None)], 'missing', 1),
    ([('0000-01-01', '2026-01-01')], 'missing', 1),
    ([('2025-01-01', '2026-01-01'), ('2025-02-01', '2026-01-01')], 'ambiguous', 0),
])
def test_native_party_intervals_keep_literal_dates_gaps_and_overlaps(intervals, expected, unusable):
    import duckdb
    from spicy_regs.relationship_views.affiliations import NATIVE_AFFILIATION_VIEWS
    from spicy_regs.relationship_views.sql_views import install_sql_views
    con = duckdb.connect()
    vote = map_record('member_vote_terms', {'vote_id': '119-senate-1-1', 'member_key': 'X',
        'bioguide_id': 'X', 'term_index': '0', 'vote_day': '2025-06-01'}).subject
    parties = [map_record('member_party_affiliations', {'bioguide_id': 'X', 'term_index': '0',
        'affiliation_index': str(i), 'party': 'Independent', 'affiliation_start': start,
        'affiliation_end': end, 'term_party': 'Democrat', 'source_json': '{"caucus":"Democrat"}'}).subject
        for i, (start, end) in enumerate(intervals)]
    con.register('member_vote_terms', pa.Table.from_pylist([vote], schema=subject_schema('member_vote_terms')))
    con.register('member_party_affiliations', pa.Table.from_pylist(parties, schema=subject_schema('member_party_affiliations')))
    installed = install_sql_views(con, ['member_vote_terms', 'member_party_affiliations'], NATIVE_AFFILIATION_VIEWS)
    assert installed['member_vote_party_affiliations']['status'] == 'available'
    row = con.sql('SELECT * FROM member_vote_party_affiliations').arrow().read_all().to_pylist()[0]
    assert row['target_status'] == expected and row['unusable_interval_count'] == unusable
    assert row['dated_party'] == ('Independent' if expected == 'found' else None)
    if row['affiliation_candidates']:
        assert row['affiliation_candidates'][0]['caucus'] == 'Democrat'
        assert [c['affiliation_index'] for c in row['affiliation_candidates']] == list(range(len(intervals)))
    con.close()
