"""Published keys and recorded context stay distinct from evidence and current lookup."""
from datetime import date
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext
from spicy_regs.transforms.regulations_receipts import ReceiptInput, read_internal, write_records
from spicy_regs.transforms.regulations_shape import shape_record, subject_schema


def test_period_occurrences_preserve_repeats_typed_namespace_and_dates(tmp_path):
    from tests.test_comment_periods import _build, _document, _notice
    docket = 'EPA-HQ-OW-2026-2509'
    periods = _build(tmp_path, dockets=[(docket, 'Water')],
        documents=[_document(docket+'-0001', docket, '2026-08-05', '2026-09-05T03:59:59Z',
            fr_doc_num='2026-15000', additional_rins='["2040-AG20","2040-AG20"]')],
        register=[_notice('2026-15000', '2026-08-05', '2026-09-04',
            regulation_id_numbers_json='["2040-AG20","2040-AG20"]')], links=[])
    [row] = periods
    occurrences = shape_record('comment_periods', row)['evidence_occurrences']
    assert [o['source'] for o in occurrences] == ['documents.comment_end_date','federal_register.comments_close_on']
    assert [o['source_ordinal'] for o in occurrences] == [0,0]
    assert [o['rins'] for o in occurrences] == [['2040-AG20','2040-AG20']]*2
    assert occurrences[0]['document_id'] == docket+'-0001'
    assert occurrences[0]['publication_date'] is None
    assert occurrences[1]['document_number'] == '2026-15000'
    assert occurrences[1]['publication_date'] == '2026-08-05'
    assert json.loads(row['rins_json']) == ['2040-AG20']


def test_old_period_without_occurrences_is_unknown_not_reconstructed_from_ids():
    row = shape_record('comment_periods', {'comment_period_id':'p', 'evidence_ids_json':'["opaque@2020-01-01"]'})
    assert row['evidence_occurrences'] is None


def test_lifecycle_main_source_facts_and_exact_receipt_readback(tmp_path):
    row = dict(proceeding_id='p',document_id='opaque',stage='proposed',event_date=date(2026,1,1),
               source='regulations_gov',dated_by=None,evidence_id='native-document-id',joined_by='document_docket')
    subject, receipts = write_records('lifecycle_events', [(row,ReceiptContext('g','row','fixture',[{'source_id':'fixture','body_version':'fixture-v1'}]))], tmp_path/'native')
    [main] = pq.read_table(subject).to_pylist()
    assert {k:main[k] for k in ('source','dated_by','evidence_id','joined_by')} == {k:row[k] for k in ('source','dated_by','evidence_id','joined_by')}
    assert list(read_internal(ReceiptInput('lifecycle_events',(subject,),receipts,'g'))) == [row]
    assert main['dated_by'] is None  # Never a fabricated Register publication date.


def test_capture_and_comment_reference_statuses_are_main_and_not_inferred():
    row = shape_record('comments',dict(comment_id='c',comment_reference_values_json='[]',
        text_extraction_status='failed',pdf_extraction_results_json='[{"status":"error"}]'))
    assert row['text_extraction_status']=='failed'
    assert row['comment_reference_values_json']=='[]'
    assert row['comment_on_document_id'] is None
    assert {'text_extraction_status','pdf_extraction_results_json'} <= set(subject_schema('documents').names)


def test_exact_parent_members_refuse_missing_changed_conflicting_and_wrong_native_context(tmp_path):
    from spicy_regs.transforms.build_court_pdf_extractions import qualify_parent_opinions
    opinion=dict(opinion_id='1',cluster_id='2',download_url='https://court.example/1.pdf',sha1='a'*40)
    path=tmp_path/'opinions.parquet'
    def selected(rows):
        pq.write_table(pa.Table.from_pylist(rows),path)
        return {'tables':{'court_opinions.parquet':dict(rows=len(rows),byteSize=path.stat().st_size,
            sha256='sha256:'+hashlib.sha256(path.read_bytes()).hexdigest())}}
    parent=selected([opinion])
    qualify_parent_opinions(parent,[path],[opinion])
    with pytest.raises(ValueError,match='Exact selected'):
        qualify_parent_opinions(parent,[],[opinion])
    with pytest.raises(ValueError,match='differs'):
        qualify_parent_opinions(parent,[path],[dict(opinion,cluster_id='3')])
    parent=selected([opinion,opinion])
    with pytest.raises(ValueError,match='ambiguous'):
        qualify_parent_opinions(parent,[path],[opinion])
    path.write_bytes(path.read_bytes()+b'changed')
    with pytest.raises(ValueError,match='bytes differ'):
        qualify_parent_opinions(parent,[path],[opinion])


def test_retained_court_cohort_exposes_parent_and_body_facts_without_current_lookup(tmp_path):
    from spicy_regs.court_receipts import write_court_rows, read_court_rows
    fixture=json.loads((Path(__file__).parent/'fixtures/court_citations/derived-text-cohort.json').read_text())
    path=write_court_rows('court_opinion_pdf_extractions',fixture['rows'],tmp_path,generation_id='g',witnesses=[{'source_id':'retained-cohort','sha256':fixture['publication']}])
    main=pq.read_table(path).to_pylist()
    assert len(main)==3 and len({r['opinion_body_id'] for r in main})==3
    for original,row in zip(fixture['rows'],main):
        parent=json.loads(original['parent_opinion_publication_json'])
        assert row['parent_artifact_digest']==parent['artifactDigest']
        assert row['source_sha256']==original['source_sha256']
        assert row['native_sha1']==row['actual_sha1'] and row['sha1_matches'] is True
        assert row['parent_member_sha256']==parent['tables']['court_opinions.parquet']['sha256']
    assert len(list(read_court_rows(path,dataset='court_opinion_pdf_extractions')))==3


def test_scorecard_recorded_snapshot_is_association_not_business_identity(tmp_path):
    from tests.test_scorecard_refresh import tables, edition
    from spicy_regs.scorecards.etl import SOURCE_NAMES, write_family, read_family
    from spicy_regs.scorecards.subject_shapes import IDENTITIES, subject_schema
    from spicy_regs.explorer_navigation import declarations, target_keys
    from spicy_regs.table_joins import JOINS, RETIRED_PROCESSING_JOINS
    raw=tables(edition('2025'))
    write_family(tmp_path,raw)
    assert read_family(tmp_path,SOURCE_NAMES)==raw
    assert IDENTITIES['scorecard_items']==('scorecard_id','item_id')
    [card]=pq.read_table(tmp_path/'scorecards.parquet').to_pylist()
    [snapshot]=pq.read_table(tmp_path/'scorecard_snapshots.parquet').to_pylist()
    assert card['snapshot_id']==snapshot['snapshot_id']
    assert {'source_snapshot_id','resolution_status','input_pins_json','candidates_json'} <= set(subject_schema('scorecard_item_links').names)
    assert not any(j.child=='scorecard_items' and j.parent=='scorecard_snapshots' for j in JOINS)
    routes=[s for s in declarations(RETIRED_PROCESSING_JOINS)
            if s['id']=='recorded_snapshot_scorecard_items_scorecard_snapshots']
    assert len(routes)==1
    target=routes[0]['targets'][0]
    assert target['columns']==['snapshot_id','scorecard_id']
    [item]=pq.read_table(tmp_path/'scorecard_items.parquet').to_pylist()
    assert target_keys(target, {}, item)==[snapshot['snapshot_id'], snapshot['scorecard_id']]
    assert target_keys(target, {}, dict(item,scorecard_id=None)) is None


def test_gao_standalone_identifiers_and_source_route_do_not_infer_reports():
    from spicy_regs.transforms.government_source_shapes import map_subject
    row=map_subject('gao_decisions',dict(decision_number='B-1',b_numbers_json='["B-1","B-1","B-2"]',
        b_numbers_truncated='true',source='gao_listing',listing_page='https://gao.gov/legal'))
    assert row['b_numbers']==['B-1','B-1','B-2']
    assert row['b_numbers_truncated'] is True and row['source']=='gao_listing'
    assert 'report_id' not in row


def test_unresolved_scorecard_decision_is_a_main_row_with_all_candidates_and_no_edge(tmp_path):
    from spicy_regs.scorecards.etl import write_family, read_family
    from spicy_regs.scorecards.subject_shapes import SOURCE_COLUMNS
    name='scorecard_item_links'
    row=dict.fromkeys(SOURCE_COLUMNS[name])
    row.update(scorecard_id='publisher:edition',item_id='item',reference_id='ref',
               source_snapshot_id='accepted-snapshot',capture_id='capture',resolution_status='ambiguous',
               candidate_count='2',candidates_json='[{"bill_id":"119-hr-1"},{"bill_id":"119-hr-2"}]',
               reason='two qualifying candidates',rule_version='rule-v1')
    write_family(tmp_path/'association',{name:[row]})
    [main]=pq.read_table(tmp_path/'association'/f'{name}.parquet').to_pylist()
    assert main['resolution_status']=='ambiguous' and main['candidate_count']==2
    assert main['bill_id'] is None and main['vote_id'] is None
    assert main['candidates_json']==row['candidates_json']
    assert read_family(tmp_path/'association',(name,))=={name:[row]}
    with pytest.raises(ValueError,match='conversion refused'):
        write_family(tmp_path/'refused',{name:[dict(row,bill_id='119-hr-1')]})


def test_derived_court_parent_fields_refuse_contradictory_or_malformed_declarations():
    from spicy_regs.court_subjects import normalize_court_row
    fixture=json.loads((Path(__file__).parent/'fixtures/court_citations/derived-text-cohort.json').read_text())
    row=fixture['rows'][0]
    with pytest.raises(ValueError,match='differs from the recorded'):
        normalize_court_row('court_opinion_pdf_extractions',dict(row,parent_member_sha256='sha256:'+'0'*64))
    with pytest.raises(ValueError,match='must be objects'):
        normalize_court_row('court_opinion_pdf_extractions',dict(row,parent_opinion_publication_json='{"tables": []}'))


def test_exact_old_regulations_pair_rebuilds_promoted_context_without_relabeling(tmp_path):
    from spicy_regs.etl_receipts import write_dataset
    from spicy_regs.policy_migrations import earlier_policy
    prior=earlier_policy('lifecycle_events','regulations-native-v1')
    raw=dict(proceeding_id='p',document_id='doc',stage='proposed',event_date=date(2026,1,1),
             source='federal_register',dated_by='publication_date',evidence_id='2026-00001@2026-01-01',joined_by='official_docket')
    shaped=shape_record('lifecycle_events',raw)
    held={name:shaped.get(name) for name in prior.input_fields}
    subject,receipts=write_dataset([(held,ReceiptContext('old','row','old',[{'source_id':'selected','body_version':'old'}]))],tmp_path/'old',prior)
    assert subject is not None
    original=subject.read_bytes()
    [restored]=read_internal(ReceiptInput('lifecycle_events',(subject,),receipts,'old'))
    assert restored==raw and subject.read_bytes()==original
    new,_=write_records('lifecycle_events',[(restored,ReceiptContext('new','row','new',[{'source_id':'selected','body_version':'old'}]))],tmp_path/'new')
    [main]=pq.read_table(new).to_pylist()
    assert main['source']=='federal_register' and main['dated_by']=='publication_date'
    assert main['evidence_id']=='2026-00001@2026-01-01' and main['event_date']==date(2026,1,1)


def test_exact_old_opinion_policy_remains_readable_before_parent_context_cutover(tmp_path):
    from spicy_regs.etl_receipts import write_dataset
    from spicy_regs.court_subjects import normalize_court_row
    from spicy_regs.court_receipts import read_court_rows
    from spicy_regs.policy_migrations import earlier_policy
    prior=earlier_policy('court_opinions','courts/1')
    raw=dict(opinion_id='1',cluster_id='2',sha1='a'*40,download_url='https://court.example/1.pdf')
    shaped=normalize_court_row('court_opinions',raw)
    mapped={name:shaped.get(name) for name in prior.input_fields}
    subject,receipt=write_dataset([(mapped,ReceiptContext('frozen-parent','row','prior',[{'source_id':'selected-opinion','body_version':'frozen-parent'}]))],tmp_path/'old',prior)
    assert subject is not None
    before=subject.read_bytes()
    [held]=read_court_rows(subject,dataset='court_opinions',receipt_path=receipt,generation_id='frozen-parent')
    assert held['opinion_id']=='1' and held['cluster_id']=='2'
    assert held['sha1']==raw['sha1'] and held['download_url']==raw['download_url']
    assert subject.read_bytes()==before and pq.read_schema(subject)==prior.subject_schema
