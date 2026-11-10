"""Published source namespaces and exact historical citation replay."""
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext, read_with_receipts, selected_subject_policy, write_dataset
from spicy_regs.explorer_navigation import declarations, published_navigation, target_keys
from spicy_regs.legislative_documents import map_subject, recorded_subject
from spicy_regs.legislative_receipts import policy, write_legislative_outputs, restore_prior
from spicy_regs.navigation_measurements import MeasurementCache
from tests.test_legislative_receipts import retained, row
from tests.test_remaining_main_navigation import selected


def recipe(identifier):
    return next(spec for spec in declarations() if spec['id'] == identifier)


def test_comment_literal_document_and_reverse_keep_repeated_sources(tmp_path):
    spec = recipe('comment_document')
    index, paths = selected(tmp_path, [
        ('comments', [{'comment_id': 'a', 'comment_on_document_id': 'doc'},
                      {'comment_id': 'b', 'comment_on_document_id': 'doc'},
                      {'comment_id': 'c', 'comment_on_document_id': None, 'comment_on_object_id': 'doc'}],
         pa.schema([('comment_id', pa.string()), ('comment_on_document_id', pa.string()), ('comment_on_object_id', pa.string())])),
        ('documents', [{'document_id': 'doc'}], None),
    ])
    proof = MeasurementCache(tmp_path/'cache').measure(index, paths, spec, source_identity=('comment_id',))
    assert (proof['eligible'], proof['matched'], proof['missing']) == (2, 2, 0)
    assert proof['reverse']['maximumDistinctSourceRecordsPerTarget'] == 2
    assert target_keys(spec['targets'][0], {}, {'comment_on_object_id': 'doc', 'original_document_id': 'doc'}) is None


def test_register_supporting_occurrences_and_dated_rule_keys():
    target = recipe('register_supporting_documents')['targets'][0]
    assert target_keys(target, {'document_id': 'doc'}, {}) == ['doc']
    assert target_keys(target, {'title': 'doc'}, {}) is None
    target = recipe('rule_target_register')['targets'][0]
    assert target_keys(target, '2026-12@2026-02-03', {'final_date': '2026-05-01'}) == ['2026-12', '2026-02-03']
    assert target_keys(target, '2026-12', {'final_date': '2026-05-01'}) is None


def test_gao_rin_routes_return_all_editions(tmp_path):
    index, paths = selected(tmp_path, [
        ('gao_reports', [{'report_id': 'r', 'major_rule_rins': ['1234-AB12', '1234-AB12']}], None),
        ('unified_agenda', [{'rin': '1234-AB12', 'agenda_edition': e} for e in ['202504', '202510']], None),
    ])
    proof = MeasurementCache(tmp_path/'cache').measure(index, paths, recipe('gao_major_rule_agenda'), source_identity=('report_id',))
    assert proof['matched'] == 2 and proof['ambiguous'] == 2 and proof['repeatedReferences'] == 1
    assert proof['reverse']['matchedTargetRows'] == 2


def citation(**values):
    return row('document_citations', document_kind='govinfo_package', document_key='CDOC-119hdoc1',
               cite_kind='bill_number', target_key='119-hr-5', target_table='congress_bills',
               target_resolved='true', target_rule='bill_number:inline_congress',
               span_start='1', span_end='7', matched_text='H.R. 5', text_sha256='a'*64, **values)


def test_citation_namespace_is_main_and_original_remains_exact(tmp_path):
    source = citation()
    original = retained(tmp_path/'input', 'document_citations', [source], {'original': 'fixture'})
    write_legislative_outputs([original], tmp_path/'native', generation_id='g')
    [subject] = pq.read_table(tmp_path/'native/document_citations.parquet').to_pylist()
    assert subject['target_table'] == source['target_table']
    assert target_keys(recipe('citation_recorded_targets')['targets'][0], {}, subject) == ['119-hr-5']
    restored = restore_prior(tmp_path/'native', tmp_path/'restored')
    assert pq.read_table(restored['document_citations']).equals(pq.read_table(original), check_metadata=True)


@pytest.mark.parametrize('change', [
    {'target_resolved': False}, {'target_resolved': 'true'}, {'target_table': 'members'},
    {'cite_kind': 'bioguide_id'}, {'target_rule': 'bill_number:document_fallback'}, {'target_key': 'HR5'},
])
def test_citation_refuses_wrong_scope_unqualified_rule_and_unresolved(change):
    source = map_subject('document_citations', citation())
    assert target_keys(recipe('citation_recorded_targets')['targets'][0], {}, source | change) is None


def test_old_citation_policy_stays_readable_without_relabeling(tmp_path):
    from spicy_regs.policy_migrations import earlier_policy

    source = citation()
    old = earlier_policy('document_citations', 'legislative-documents/1')
    mapped = map_subject('document_citations', source)
    assert mapped is not None
    original_subject = {name: mapped[name] for name in old.subject_schema.names}
    subject, receipt = write_dataset([(original_subject | {'raw_source_row': source},
        ReceiptContext('old', 'one', 'fixture', [{'source_id': 'fixture', 'sha256': 'a'*64}]))], tmp_path/'old', old)
    assert subject is not None
    digest = hashlib.sha256(receipt.read_bytes()).hexdigest()
    selected_policy = selected_subject_policy(policy('document_citations'), [subject])
    assert selected_policy == old
    [replayed] = read_with_receipts([subject], [receipt], selected_policy, generation_id='old')
    assert replayed['raw_source_row'] == source
    assert recorded_subject('document_citations', source, original_subject) == original_subject
    assert 'target_table' not in original_subject
    assert hashlib.sha256(receipt.read_bytes()).hexdigest() == digest


def test_selected_old_citations_do_not_recover_namespace_from_receipts():
    spec = recipe('citation_recorded_targets')
    source_schema = [(name, 'VARCHAR') for name in ('target_key', 'target_rule', 'cite_kind', 'target_resolved')]
    [published] = published_navigation([spec], {'document_citations': source_schema, 'congress_bills': [('bill_id', 'VARCHAR')]})
    assert not published['targets'][0]['sourceAvailable']
    assert any(field['path'] == 'target_table' and field['status'] == 'missing'
               for field in published['targets'][0]['requiredMainFields'])
