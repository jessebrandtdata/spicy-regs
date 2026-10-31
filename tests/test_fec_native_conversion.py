"""Complete FEC conversion and staged-parent controls with tiny original families."""
import base64
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import native_conversion as conversion
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.fec_native_conversion import (
    REBUILT_TABLES, RETIRED_TABLES, build_retained_family, staged_snapshot, family_tables)
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication
from spicy_regs.transforms.build_fec_observations import RECORD_SCHEMA, COLLECTION_SCHEMA, RELATIONSHIP_SCHEMA
from tests.test_native_conversion import bucket as _bucket, BASE, MAIN, WHEEL, STATE


@pytest.fixture(autouse=True, name="fec_originals")
def selected_fec_originals(tmp_path, monkeypatch):
    """Fixture: one admitted original per rebuilt table, plus a failed page capture, in the test raw archive."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_regs.transforms import fec_original_tables
    from tests.test_fec_original_tables import DETAIL, TOTAL, admit, ie_workbook, rss
    store = CentralRawArchiveStore(tmp_path / 'central-raw')
    meetings = (b'<html><body><h2>Open meetings</h2><table>'
                b'<tr><td>January 5, 2026</td><td><a href="/agenda.pdf">Agenda</a></td></tr></table></body></html>')
    banner = b'<p>Federal Election Commission | United States of America</p>'
    page = (b'<html><head><title>MUR 1</title></head><body>' + banner * 2 + b'<h1>MUR 1</h1><p>Text</p>'
            b'<p>About\nCareers\nPress\nContact\nPrivacy and security policy</p></body></html>')
    entries = [
        admit(store, 'fec_historical_ie_statistics', ie_workbook([DETAIL, TOTAL])),
        admit(store, 'fec_research_meeting_observations', meetings, source_url='https://www.fec.gov/meetings/'),
        admit(store, 'fec_reference_filing_feed_items', rss(('A', '101'), ('B', '102'))),
        admit(store, 'fec_reference_source_pages', page, page_type='official_legal_document_reference'),
        admit(store, 'fec_reference_source_pages', b'<html>error</html>', status=500,
              page_type='official_legal_document_reference'),
    ]
    monkeypatch.setattr(fec_original_tables, 'selection', lambda path=None: entries)
    # Rows these fixture originals hold; qualification refuses any other count.
    monkeypatch.setattr(fec_original_tables, 'expected_rows', lambda table, path=None: FIXTURE_ROWS[table])
    return entries


FIXTURE_ROWS = {'fec_historical_ie_statistics': 2, 'fec_research_meeting_observations': 1,
                'fec_reference_filing_feed_items': 2, 'fec_reference_source_pages': 1}
# The captured name of each rebuilt table: a captured prior (live 784b23da) holds these.
CAPTURED_NAMES = {table + '.parquet': key for key, table in REBUILT_TABLES['fec-query'].items()}


@pytest.fixture(name="bucket")
def fec_bucket(monkeypatch):
    factory=getattr(_bucket,"__wrapped__",None)
    if not callable(factory):
        raise RuntimeError("The shared publication fixture has no callable factory")
    return factory(monkeypatch)


def old_family(tmp, monkeypatch, store, family, *, populated=False, rows_by_table=None, legacy_tables=(), split=()):
    """A published captured prior; a table in ``split`` is a source_namespace member directory of its rows
    (first row in part-000000, an empty part-000001 after it)."""
    from spicy_regs import etl_policy_registry
    if family == 'fec-observations':
        schemas = dict(zip(('fec_source_records', 'fec_collections', 'fec_relationships'),
                           (RECORD_SCHEMA, COLLECTION_SCHEMA, RELATIONSHIP_SCHEMA), strict=True))
    else:
        declared = json.loads((conversion.Path(conversion.__file__).resolve().parents[2] / 'data_dictionary/fec_typed_schemas.json').read_text())
        schemas = {CAPTURED_NAMES.get(name + '.parquet', name + '.parquet').removesuffix('.parquet'):
                   pa.ipc.read_schema(pa.BufferReader(base64.b64decode(v['arrow_schema_ipc_base64'])))
                   for name, v in declared['tables'].items()}
    legacy=json.loads((conversion.Path(conversion.__file__).with_name('fec_retained_legacy_schemas.json')).read_text())
    for name in legacy_tables:
        schemas[name]=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(legacy[name])))
    files = []
    for name, schema in schemas.items():
        path = tmp / 'original' / (name + '.parquet')
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        if populated and name == 'fec_source_records':
            rows = [dict(collection_id='collection', source_record_id='record', source_sha256='sha256:' + 'a'*64,
                         source_family='fec-test', source_record_json='{"amount":"5"}')]*2
        rows = (rows_by_table or {}).get(name, rows)
        table = pa.Table.from_pylist(rows, schema=schema.with_metadata({'retained': 'fixture'}))
        if name in split:
            path = path.with_suffix('')
            for part, piece in enumerate((table, table.slice(0, 0))):
                member = path / f'source_namespace={rows[0]["source_namespace"]}' / f'part-{part:06d}.parquet'
                member.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(piece, member)
        else:
            pq.write_table(table, path)
        files.append(path)
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, 'require_registered_receipts', lambda *_: None)
        build_generation(tmp / 'old-generation', family=family, files=files,
                         expected_keys=[p.with_suffix('.parquet').name for p in files],
                         partitioned={name + '.parquet': ('source_namespace',) for name in split})
        snapshot = publication.publish_generation(tmp / 'old-generation', client=store, bucket='spicy-regs',
                                                   prior_index=publication.current_index(BASE))
    return snapshot, {p.with_suffix('.parquet').name: p for p in files}


def test_observations_converter_complete_family_restores_duplicate_rows_and_metadata(tmp_path, monkeypatch, bucket):
    snapshot, retained = old_family(tmp_path, monkeypatch, bucket, 'fec-observations', populated=True)
    receipt = conversion.convert('fec-observations', allowed=['fec-observations'], work=tmp_path / 'conversion',
        expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))
    assert set(receipt['tables']) == {key.removesuffix('.parquet') for key in family_tables('fec-observations')}
    assert receipt['tables']['fec_source_records']['retained_rows'] == 2
    assert receipt['prepared'] and receipt['published'] is None
    assert snapshot == publication.current_index(BASE)


def test_incomplete_observations_membership_refused(tmp_path, monkeypatch, bucket):
    snapshot, retained = old_family(tmp_path, monkeypatch, bucket, 'fec-observations')
    old = snapshot['families']['fec-observations']
    with ConversionReadOperation() as operation:
        with pytest.raises(ValueError, match='retained table membership'):
            build_retained_family('fec-observations', old, snapshot, {next(iter(retained)): next(iter(retained.values()))},
                                  tmp_path / 'incomplete', operation=operation)
    wrong = {**old, 'tables': {next(iter(old['tables'])): next(iter(old['tables'].values()))}}
    with pytest.raises(ValueError, match='complete registered'):
        conversion._check_prepared_membership('fec-observations', wrong, {})


def test_query_requires_explicit_filing_inputs_and_exact_native_parent(tmp_path, monkeypatch, bucket):
    source_snapshot, source = old_family(tmp_path / 'source', monkeypatch, bucket, 'fec-observations')
    snapshot, retained = old_family(tmp_path / 'query', monkeypatch, bucket, 'fec-query')
    with ConversionReadOperation() as operation:
        native = build_retained_family('fec-observations', snapshot['families']['fec-observations'], snapshot,
                                      source, tmp_path / 'native-source', operation=operation)
        staged = staged_snapshot(snapshot, native, operation)
        assert staged['families']['fec-query'] == snapshot['families']['fec-query']
        assert staged['families']['fec-observations']['artifactDigest'] != snapshot['families']['fec-observations']['artifactDigest']
        assert 'publishedAt' not in staged['families']['fec-observations']
        from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
        from spicy_regs.transforms.fec_identity_receipts import dataset_policy
        target = tmp_path / 'empty-filings.parquet'
        pq.write_table(pa.Table.from_batches([], schema=dataset_policy('fec_filings').subject_schema), target)
        selection = FilingAssociationInputs(staged['families']['fec-observations']['artifactDigest'], None,
            [TypedInput(target, _sha(target), 0)], {}, filing_generation_scope='self')
        complete = build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
            tmp_path / 'complete-query', operation=operation, read_snapshot=staged, filing_associations=selection,
            observation_generation=native)
        artifact, reader, _ = operation.admit_generation(complete)
        assert set(artifact.root['spec']['tables']) == family_tables('fec-query')
        # The moved tables publish under their reference names only (ss-j4vz Q1).
        assert {'fec_reference_filing_feed_items.parquet', 'fec_reference_source_pages.parquet'} <= set(artifact.root['spec']['tables'])
        assert not {'fec_research_filing_feed_items.parquet', 'fec_research_source_pages.parquet'} & set(artifact.root['spec']['tables'])
        assert artifact.root['spec']['readSnapshot'] == staged
        assert set(artifact.root['spec']['parents']) == family_tables('fec-observations')
        for key, original in retained.items():
            if key not in REBUILT_TABLES['fec-query']:
                assert reader.compare_original(key.removesuffix('.parquet'), original)['rows_only_in_restored'] == 0
        # Rebuilt tables come from the originals, not the (empty) captured rows; the failed capture is refused.
        from spicy_regs.transforms.fec_original_tables import rebuild
        for table in REBUILT_TABLES['fec-query'].values():
            published = list(reader.read_subjects(table))
            expected = rebuild(table).rows
            assert published and len(published) == len(expected) == FIXTURE_ROWS[table]
            assert all(row[name] == want[name] for row, want in zip(published, expected, strict=True)
                       for name in row.keys() & want.keys())
            assert not {'context_sha256', 'source_context_pointer'} & set(published[0])
        refusals = json.loads((tmp_path / 'complete-query/build/rebuilt/fec_reference_source_pages'
                               / 'fec_reference_source_pages.refusals.json').read_text())
        assert [reason for _, reason in refusals] == ['http_status_500']
        conversion._check_prepared_membership('fec-query', snapshot['families']['fec-query'], artifact.root['spec'])
        with pytest.raises(ValueError, match='admitted filing association'):
            build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
                tmp_path / 'native-query', operation=operation, read_snapshot=staged)
        with pytest.raises(ValueError, match='selected native observations'):
            build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
                tmp_path / 'legacy-query', operation=operation)


def test_source_occurrence_ordinals_allow_sample_gaps_and_refuse_resets(tmp_path):
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS
    with IdentityReceiptWriter(tmp_path / 'source', generation_id='sample', tables=['fec_source_records']) as writer:
        row = dict(collection_id='c', source_record_id='r', source_sha256=WITNESS['sha256'])
        writer.emit('fec_source_records', row, input_witness=WITNESS, observation_ordinal=32)
        writer.emit('fec_source_records', row, input_witness=WITNESS, observation_ordinal=4096)
        with pytest.raises(ValueError, match='must increase'):
            writer.emit('fec_source_records', row, input_witness=WITNESS, observation_ordinal=32)
    actual = pq.ParquetFile(tmp_path / 'source/fec_source_records.parquet').read().to_pylist()
    assert [r['observation_ordinal'] for r in actual] == [32, 4096]


def test_changed_fec_policies_preserve_exact_earlier_admission():
    from spicy_regs.subject_catalog import policies
    from spicy_regs.etl_receipts import receipt_policies
    from dataclasses import replace
    for name in ('fec_source_records', 'fec_receipts', 'fec_agency_mapping_dispositions'):
        current = policies()[name]
        versions = receipt_policies(current)
        assert [p.policy_version.rsplit('/', 1)[1] for p in versions] == ['3', '2', '1']
        changed = replace(current, subject_schema=current.subject_schema.append(pa.field('unadmitted', pa.string())))
        assert receipt_policies(changed) == (changed,)


def test_production_query_prepares_with_official_staged_parent_and_rejects_stale_publication(tmp_path, monkeypatch, bucket):
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    from tests.test_native_conversion import BUCKET
    old_family(tmp_path / 'source', monkeypatch, bucket, 'fec-observations')
    snapshot, retained = old_family(tmp_path / 'query', monkeypatch, bucket, 'fec-query')
    source_work = tmp_path / 'source-conversion'
    source = conversion.convert('fec-observations', allowed=['fec-observations'], work=source_work,
        expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))
    target = tmp_path / 'filing-selection.parquet'
    pq.write_table(pa.Table.from_batches([], schema=dataset_policy('fec_filings').subject_schema), target)
    selection = FilingAssociationInputs(source['generation']['artifactDigest'], None,
        [TypedInput(target, _sha(target), 0)], {}, filing_generation_scope='self')
    work = tmp_path / 'query-conversion'
    done = conversion.convert('fec-query', allowed=['fec-query'], work=work, staged_observations=source_work / 'build/generation',
        filing_associations=selection, expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))
    assert done['prepared'] and done['staged_observations']['preparation'] == source
    assert done['captured']['entry'] == snapshot['families']['fec-query']
    # Candidate staging is not pointer publication: query cannot publish until its exact parent is selected.
    with pytest.raises(conversion.ConversionRefused, match='parent|Parent'):
        conversion.publish_prepared(work / 'conversion.json', allowed=['fec-query'], expected_main=MAIN,
            expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE),
            expected_receipt_digest=done['prepared']['receiptDigest'])
    assert publication.current_index(BASE) == snapshot
    # The parent's admission and restoration are not in this seal: staged FEC query always verifies in full,
    # although the same receipt without its staged parent would qualify for the sealed path.
    sealed = json.loads((work / 'conversion.json').read_bytes())
    assert 'staged_observations' in sealed and not conversion._sealed_admission(sealed, False, False)
    assert conversion._sealed_admission({k: v for k, v in sealed.items() if k != 'staged_observations'}, False, False)


@pytest.mark.parametrize('table', sorted(FIXTURE_ROWS))
@pytest.mark.parametrize('expected', ['more', 'fewer'])
def test_query_qualification_refuses_a_rebuilt_table_whose_row_count_differs_from_its_selection(
        tmp_path, monkeypatch, bucket, table, expected):
    """A short, long or empty rebuilt table never qualifies: the selection, not the build, states the count."""
    from spicy_regs.transforms import fec_original_tables
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    stated = FIXTURE_ROWS[table] + (1 if expected == 'more' else -1)
    monkeypatch.setattr(fec_original_tables, 'expected_rows',
                        lambda name, path=None: stated if name == table else FIXTURE_ROWS[name])
    old_family(tmp_path / 'source', monkeypatch, bucket, 'fec-observations')
    old_family(tmp_path / 'query', monkeypatch, bucket, 'fec-query')
    source_work = tmp_path / 'source-conversion'
    source = conversion.convert('fec-observations', allowed=['fec-observations'], work=source_work,
        expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))
    target = tmp_path / 'filing-selection.parquet'
    pq.write_table(pa.Table.from_batches([], schema=dataset_policy('fec_filings').subject_schema), target)
    selection = FilingAssociationInputs(source['generation']['artifactDigest'], None,
        [TypedInput(target, _sha(target), 0)], {}, filing_generation_scope='self')
    with pytest.raises(conversion.ConversionRefused,
                       match=f'{table} holds {FIXTURE_ROWS[table]} rows; its selected originals hold {stated}'):
        conversion.convert('fec-query', allowed=['fec-query'], work=tmp_path / 'query-conversion',
            staged_observations=source_work / 'build/generation', filing_associations=selection,
            expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))


@pytest.mark.parametrize('split', [(), ('fec_receipts',)])
def test_nonempty_complete_query_links_filing_source_and_retained_history(tmp_path, monkeypatch, bucket, split):
    from decimal import Decimal
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
    from spicy_regs.transforms.fec_identity_context_fields import normalize_record
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    from tests.test_fec_filing_associations import financial, filing, SHA, NAMESPACE, EVIDENCE, pin
    from spicy_docs.sources.fec.committee_master import COMMITTEE_MASTER_FIELDS
    from spicy_docs.schemas.fec_committee_history import project_committee_master_row
    record=dict(collection_id='c',source_record_id='native-row',source_sha256=SHA,source_family='fixture',source_record_json='{}')
    observations,_=old_family(tmp_path/'source',monkeypatch,bucket,'fec-observations',rows_by_table={'fec_source_records':[record]})
    original_filing={**filing(), 'collection_id':'filings', 'source_record_id':'metadata', 'source_sha256':SHA,
                     'source_locator_json':'{"pointer":"/results/0"}'}
    native=dict.fromkeys(COMMITTEE_MASTER_FIELDS,'')
    native.update(CMTE_ID='C00000001',CMTE_NM='TEST COMMITTEE')
    business=project_committee_master_row({'fields':native,'cycle':2024})
    history={**business,'cycle':'2024','source_cycle':2024,'native_fields':native,'record_id':pin('history'),
             'mapping_version':'fec-retained-committee-history/1'}
    old_header=dict(record_id=pin('old-header'),policy_version='fec-retained-filing-association/1',
        target_table='fec_source_records',target_record_id='native-row',source_generation_pin=observations['families']['fec-observations']['artifactDigest'],
        collection_id='c',source_record_id='native-row',source_sha256=SHA,source_authority='official-fec',
        source_locator_json=financial()['source_locator_json'],association_basis='fixture-native-header',
        association_status='resolved_native_filing_key',referenced_filing_key=filing()['filing_key'],filing_key=filing()['filing_key'],
        selection_evidence_sha256=EVIDENCE,namespace_evidence_sha256=EVIDENCE,header_record_id='header-record',
        header_locator_json='{"pointer":"/header"}',filing_observation_count=1,filing_observation_ids=['metadata-1'])
    snapshot,retained=old_family(tmp_path/'query',monkeypatch,bucket,'fec-query',rows_by_table={
        'fec_filings':[original_filing], 'fec_filing_header_associations':[old_header], 'fec_receipts':[{**financial(),'amount':Decimal('5')}],
        'fec_agency_mapping_dispositions':[{'collection_id':'c','source_record_id':'native-row','mapping_status':'mapped'}],
        'fec_committee_master_observations':[history],
        'fec_record_evidence':[dict(target_table='fec_receipts',target_record_id=financial()['record_id'],
            target_generation_scope='self',witness_generation_scope='external',
            witness_generation_pin=observations['families']['fec-observations']['artifactDigest'],
            endpoint_kind='source_record',collection_id='c',source_record_id='native-row',witness_sha256=SHA,
            witness_locator_json=financial()['source_locator_json']),
            # Receipt-context evidence of a rebuilt table is not carried (ss-j4vz B7-B9).
            dict(target_table='fec_research_source_pages',target_record_id='old-page',role='primary',
                 endpoint_kind='collection_context',collection_id='c',witness_sha256=SHA)]},
        legacy_tables=['fec_committee_master_observations'],split=split)
    source_work=tmp_path/'prepared-source'
    source=conversion.convert('fec-observations',allowed=['fec-observations'],work=source_work,
        expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    original=pq.ParquetFile(retained['fec_filings.parquet']).read().to_pylist()[0]
    mapped=normalize_record('fec_filings',original)
    target=tmp_path/'admitted-filings.parquet'
    pq.write_table(pa.Table.from_pylist([mapped],schema=dataset_policy('fec_filings').subject_schema),target)
    headers=tmp_path/'current-headers.parquet'
    old_schema=pq.read_schema(retained['fec_filing_header_associations.parquet'])
    from spicy_regs.transforms.fec_filing_associations import _base
    current_header={**old_header,'policy_version':'fec-retained-filing-association/2',
                    'selection_evidence_sha256':pin('current-header-selection')}
    current_header['record_id']=_base({**current_header,'record_id':'native-row'},'fec_source_records',
        current_header['source_generation_pin'],current_header['selection_evidence_sha256'],current_header['association_basis'])['record_id']
    pq.write_table(pa.Table.from_pylist([current_header],schema=old_schema),headers)
    selection=FilingAssociationInputs(observations['families']['fec-observations']['artifactDigest'],None,
        [TypedInput(target,_sha(target),1)],{NAMESPACE:EVIDENCE},[TypedInput(headers,_sha(headers),1)],filing_generation_scope='self')
    work=tmp_path/'prepared-query'
    result=conversion.convert('fec-query',allowed=['fec-query'],work=work,staged_observations=source_work/'build/generation',
        filing_associations=selection,expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    generation=work/'build/generation'
    if split:  # ss-3bkk: assembly wrote the captured member layout, the empty member included, once
        members=[p.relative_to(generation/'fec_receipts').as_posix() for p in sorted((generation/'fec_receipts').rglob('*.parquet'))]
        assert members==[p.relative_to(retained['fec_receipts.parquet']).as_posix()
                         for p in sorted(retained['fec_receipts.parquet'].rglob('*.parquet'))]
        assert not (work/'build/partitioned').exists()
    [money]=pa.concat_tables(pq.ParquetFile(p).read() for p in sorted(generation.glob('fec_receipts*/**/*.parquet'))
                             ).to_pylist() if split else pq.ParquetFile(generation/'fec_receipts.parquet').read().to_pylist()
    assert money['amount']==Decimal('5') and money['filing_key']==filing()['filing_key']
    assert money['filing_association_target_generation_scope']=='self' and money['filing_association_target_generation_pin'] is None
    [header]=pq.ParquetFile(generation/'fec_filing_header_associations.parquet').read().to_pylist()
    assert header['filing_key']==filing()['filing_key'] and header['policy_version']=='fec-retained-filing-association/2'
    assert header['source_generation_pin']==source['generation']['artifactDigest']
    assert header['record_id']==_base({**current_header,'record_id':'native-row'},'fec_source_records',
        source['generation']['artifactDigest'],current_header['selection_evidence_sha256'],current_header['association_basis'])['record_id']
    [agency]=pq.ParquetFile(generation/'fec_agency_mapping_dispositions.parquet').read().to_pylist()
    assert agency['source_sha256']==SHA
    [witness]=pq.ParquetFile(generation/'fec_record_evidence.parquet').read().to_pylist()
    assert witness['witness_generation_pin']==source['generation']['artifactDigest']
    [committee]=pq.ParquetFile(generation/'fec_committee_master_observations.parquet').read().to_pylist()
    assert committee['name']=='TEST COMMITTEE' and committee['cycle']==2024
    assert result['tables']['fec_receipts']['retained_rows']==1
    assert result['tables']['fec_record_evidence']['unreferenced_evidence_rows']==1
    assert result['tables']['fec_reference_source_pages']['rebuilt_from_originals']
    assert _sha(headers) == selection.header_associations[0].sha256
    assert _sha(target) == selection.filings[0].sha256
    with ConversionReadOperation() as operation:
        _,reader,_=operation.admit_generation(generation)
        for key,original in retained.items():
            if key in REBUILT_TABLES['fec-query'] or key == 'fec_record_evidence.parquet':
                continue
            compared = reader.compare_original(key.removesuffix('.parquet'),original)
            assert compared['rows_only_in_restored'] == compared['rows_only_in_retained'] == 0
    assert all(path.is_relative_to(generation) for path in (generation.parent).rglob('*.parquet'))


def test_resource_callback_reaches_fec_inner_table_loops(tmp_path,monkeypatch,bucket):
    snapshot,retained=old_family(tmp_path,monkeypatch,bucket,'fec-observations',populated=True)
    calls=[]
    def guard():
        calls.append(True)
        if len(calls)==4:
            raise TimeoutError('bounded fixture stop')
    with pytest.raises(TimeoutError,match='bounded fixture stop'):
        conversion.convert('fec-observations',allowed=['fec-observations'],work=tmp_path/'stopped',
            expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE),check_resources=guard)
    assert len(calls)==4 and publication.current_index(BASE)==snapshot
    assert not (tmp_path/'stopped/build/generation/artifact.json').exists()


def test_complete_fec_observations_resume_reuses_writer_without_rebuild(tmp_path,monkeypatch,bucket):
    from spicy_regs.generations import implementation_id
    from tests.test_native_conversion import PINNED
    old_family(tmp_path,monkeypatch,bucket,'fec-observations',populated=True)
    work=tmp_path/'work'
    original=conversion.convert('fec-observations',allowed=['fec-observations'],work=work,
        expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    interrupted=dict(original)
    interrupted.pop('prepared')
    (work/'conversion.json').write_text(json.dumps(interrupted))
    identity=implementation_id()
    monkeypatch.setattr(conversion,'_historical_producer',lambda *_:(identity,{'version':WHEEL,'wheels':[{'hash':PINNED}]}))
    def rebuild(*args,**kwargs):
        pytest.fail('Resume must never call a FEC writer')
    monkeypatch.setattr(conversion,'_convert_fec',rebuild)
    resumed=conversion.resume_prepared(work/'build/generation',work=work,allowed=['fec-observations'],
        expected_main=MAIN,expected_spicy_docs=WHEEL,expected_producer=MAIN,state=lambda _:dict(STATE))
    assert resumed['prepared'] and resumed['tables']==original['tables'] and resumed['published'] is None


@pytest.mark.parametrize('raw, native, status', [
    ('0202', 202, 'parsed'), ('2024', 2024, 'parsed'), (2024, 2024, 'parsed'),
    (None, None, 'source_null'), ('', None, 'source_empty'),
    ('24', None, 'unsupported_year'), ('20245', None, 'unsupported_year'),
    (' 2024 ', None, 'unsupported_year'), ('+2024', None, 'unsupported_year'),
    ('-2024', None, 'unsupported_year'), (str(2**63), None, 'unsupported_year'),
    (2**63, None, 'unsupported_year'), (True, None, 'unsupported_year'),
    (1.0, None, 'unsupported_year'), ('2.5', None, 'unsupported_year'),
])
def test_retained_registration_legacy_years_preserve_exact_receipt_values(tmp_path, raw, native, status):
    from spicy_regs.fec_native_conversion import retained_identity_row
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, read_identity_rows
    from tests.test_fec_identity_receipts import WITNESS
    original = dict(record_id='legacy-statement', mapping_version='fec-identity-observations/1',
                    mapping_status='original-mapping', mapping_reason_json='{"original":true}',
                    election_year=raw, report_year='2025', source_namespace='fec-bulk-form2')
    before = dict(original)
    row, overrides = retained_identity_row('fec_registration_statements', original)
    assert original == before and type(original['election_year']) is type(raw)
    assert row['election_year'] == native and type(row['election_year']) is type(native)
    assert row['election_year_status'] == status
    assert row['report_year'] == 2025 and row['report_year_status'] == 'parsed'
    # The mapping version remains the actual retained producer's version.
    assert row['mapping_version'] == original['mapping_version']
    with IdentityReceiptWriter(tmp_path/'statement', generation_id='legacy', tables=['fec_registration_statements']) as writer:
        subject, _ = writer.emit('fec_registration_statements', row, input_witness=WITNESS,
                                source_input={'fec_retained_overrides': overrides})
    assert subject['election_year_status'] == status
    [internal] = read_identity_rows(tmp_path/'statement', 'fec_registration_statements', generation_id='legacy')
    moved = internal['conversion_inputs']
    held = internal['source_input']['fec_retained_overrides']
    restored = {name: held.get(name, moved.get(name, internal.get(name))) for name in original}
    assert restored == original
    assert all(type(restored[name]) is type(value) for name, value in original.items())


def test_retained_registration_current_refusal_evidence_is_not_reinterpreted(tmp_path):
    from spicy_regs.fec_native_conversion import retained_identity_row
    from spicy_regs.transforms import fec_identity_observations as identity
    from tests.test_fec_identity_observations import SELECTION, registry
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, read_identity_rows
    from tests.test_fec_identity_receipts import WITNESS
    for number, value in enumerate(('0202', '2.5', '', None)):
        row, _ = identity.map_registry(
            registry(identity.FORM2, ELECTION_YEAR=value, REPORT_YEAR='2025'), SELECTION, identity.FORM2)
        retained, overrides = retained_identity_row('fec_registration_statements', row)
        assert retained == row and overrides == {}
        assert all(type(retained[name]) is type(original) for name, original in row.items())
        destination = tmp_path/str(number)
        with IdentityReceiptWriter(destination, generation_id='native', tables=['fec_registration_statements']) as writer:
            writer.emit('fec_registration_statements', retained, input_witness=WITNESS)
        [internal] = read_identity_rows(destination, 'fec_registration_statements', generation_id='native')
        assert internal['election_year_raw'] == row['election_year_raw']
        assert internal['election_year_status'] == row['election_year_status']


@pytest.mark.parametrize('drop_evidence', [False, True])
def test_retained_registration_current_malformed_type_still_refuses(tmp_path, drop_evidence):
    from spicy_regs.fec_native_conversion import retained_identity_row
    from spicy_regs.transforms import fec_identity_observations as identity
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_observations import SELECTION, registry
    from tests.test_fec_identity_receipts import WITNESS
    current, _ = identity.map_registry(
        registry(identity.FORM2, ELECTION_YEAR='2024', REPORT_YEAR='2025'), SELECTION, identity.FORM2)
    malformed = {**current, 'election_year': '2024'}
    if drop_evidence:
        for name in ('election_year','report_year'):
            malformed.pop(name+'_raw')
            malformed.pop(name+'_status')
    row, overrides = retained_identity_row('fec_registration_statements', malformed)
    assert row == malformed and overrides == {}
    with IdentityReceiptWriter(tmp_path/'bad-current', generation_id='current', tables=['fec_registration_statements']) as writer:
        with pytest.raises(ValueError, match='election_year: native integer required'):
            writer.emit('fec_registration_statements', row, input_witness=WITNESS)


def test_retained_legacy_context_bridges_reach_production_admission(tmp_path, monkeypatch, bucket):
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    original = dict(record_id='legacy-form2', mapping_version='fec-identity-observations/1',
                    election_year='0202', report_year='2025', source_namespace='fec-bulk-form2')
    rows_by_table = {'fec_registration_statements':[original]}
    _, source_paths = old_family(tmp_path/'source', monkeypatch, bucket, 'fec-observations')
    snapshot, retained = old_family(tmp_path/'query', monkeypatch, bucket, 'fec-query',
        rows_by_table=rows_by_table, legacy_tables=tuple(rows_by_table))
    with ConversionReadOperation() as operation:
        native = build_retained_family('fec-observations', snapshot['families']['fec-observations'], snapshot,
                                      source_paths, tmp_path/'native-source', operation=operation)
        staged = staged_snapshot(snapshot, native, operation)
        target = tmp_path/'empty-native-filings.parquet'
        pq.write_table(pa.Table.from_batches([], schema=dataset_policy('fec_filings').subject_schema), target)
        selection = FilingAssociationInputs(staged['families']['fec-observations']['artifactDigest'], None,
            [TypedInput(target, _sha(target), 0)], {}, filing_generation_scope='self')
        complete = build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
            tmp_path/'native-query', operation=operation, read_snapshot=staged, filing_associations=selection,
            observation_generation=native)
        _, reader, _ = operation.admit_generation(complete)
        for table in rows_by_table:
            comparison = reader.compare_original(table, retained[table+'.parquet'])
            assert comparison['rows_only_in_restored'] == comparison['rows_only_in_retained'] == 0
            assert comparison['type_changes'] == {}
        [statement] = reader.read_subjects('fec_registration_statements')
        assert statement['election_year']==202 and statement['report_year']==2025


def test_retired_query_tables_may_remain_in_a_captured_prior_but_nothing_else_may_be_missing():
    from spicy_regs.fec_native_conversion import check_membership
    registered = family_tables('fec-query')
    retired = RETIRED_TABLES['fec-query']
    rebuilt = REBUILT_TABLES['fec-query']
    assert not retired & registered
    # The captured prior (live 784b23da) names the moved tables by their earlier names (ss-j4vz Q1).
    assert {'fec_research_filing_feed_items.parquet', 'fec_research_source_pages.parquet'} < rebuilt.keys()
    prior = (registered - {table + '.parquet' for table in rebuilt.values()}) | rebuilt.keys()
    check_membership('fec-query', {'tables': {key: {} for key in prior | retired}})
    with pytest.raises(ValueError, match='complete registered family membership'):
        check_membership('fec-query', {'tables': {key: {} for key in prior | {'fec_unknown.parquet'}}})
    check_membership('fec-query', {'tables': {key: {} for key in registered}})

    with pytest.raises(ValueError, match='complete registered family membership'):
        check_membership('fec-query', {'tables': {key: {} for key in sorted(registered)[1:]}})


def test_foreign_generation_pin_is_never_rebound():
    from spicy_regs.fec_native_conversion import ObservationRebinding,rebind_row
    proof=ObservationRebinding('sha256:'+'a'*64,'sha256:'+'b'*64)
    foreign={'witness_generation_pin':'sha256:'+'c'*64}
    assert rebind_row('fec_record_evidence',foreign,proof)==foreign


def test_adapters_refuse_unexplained_columns_and_third_schema():
    from spicy_regs.fec_native_conversion import retained_identity_row,validate_identity_schema
    with pytest.raises(ValueError,match='unexplained'):
        retained_identity_row('fec_filing_links',{'unreviewed_business_data':'value'})
    with pytest.raises(ValueError,match='neither supported current nor captured legacy'):
        validate_identity_schema('fec_filing_links',pa.schema([('record_id',pa.int64())]))


def test_noncanonical_partition_order_refuses_before_family_writes(tmp_path):
    from spicy_regs.fec_native_conversion import check_membership
    tables={key:{} for key in family_tables('fec-observations')}
    tables['fec_source_records.parquet']={'partitionColumns':['source_namespace'],'members':[
        {'key':'fec_source_records/source_namespace=fixture/part-000001.parquet'},
        {'key':'fec_source_records/source_namespace=fixture/part-000000.parquet'}]}
    with pytest.raises(ValueError,match='Noncanonical captured FEC member order'):
        check_membership('fec-observations',{'tables':tables})
    assert not (tmp_path/'build').exists()


def test_already_current_header_selection_rejects_wrong_generation_bound_identity(tmp_path):
    from spicy_regs.fec_native_conversion import header_subjects
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs,TypedInput,_sha
    from tests.test_fec_filing_associations import pin,SHA
    row=dict(record_id=pin('old'),policy_version='fec-retained-filing-association/1',target_table='fec_source_records',
        target_record_id='r',source_generation_pin=pin('current-source'),collection_id='c',source_record_id='r',
        source_sha256=SHA,source_authority='official-fec',source_locator_json='{"collection_id":"c","source_record_id":"r"}',
        association_basis='native-api-original-url',selection_evidence_sha256=pin('selection'))
    original=tmp_path/'original.parquet'
    pq.write_table(pa.Table.from_pylist([row]),original)
    current=tmp_path/'current.parquet'
    pq.write_table(pa.Table.from_pylist([{**row,'record_id':pin('wrong-current-id'),'policy_version':'fec-retained-filing-association/2'}]),current)
    selection=FilingAssociationInputs(row['source_generation_pin'],None,(),{},[TypedInput(current,_sha(current),1)],
        filing_generation_scope='self')
    with pytest.raises(ValueError,match='maintained source-generation rule'):
        header_subjects(selection,{'fec_filing_header_associations.parquet':original},None,lambda:None)


def test_conversion_reuses_exact_local_originals_without_fetch(tmp_path,monkeypatch,bucket):
    snapshot,retained=old_family(tmp_path,monkeypatch,bucket,'fec-observations',populated=True)
    selected={member.path:str(retained[key]) for key in retained for member in publication.table_members(snapshot,key)}
    def fetch(*args,**kwargs):
        pytest.fail('Explicit complete local original selection must never download')
    monkeypatch.setattr(publication,'fetch_member',fetch)
    work=tmp_path/'reused'
    result=conversion.convert('fec-observations',allowed=['fec-observations'],work=work,retained_members=selected,
        expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    assert result['prepared']
    for key,path in retained.items():
        assert path.stat().st_ino==(work/'retained'/key).stat().st_ino


def test_observations_consolidation_has_only_sealed_parquet_outputs(tmp_path, monkeypatch, bucket):
    from spicy_regs.fec_native_conversion import build_retained_family
    from spicy_regs.conversion_reads import ConversionReadOperation
    snapshot, retained = old_family(tmp_path, monkeypatch, bucket, 'fec-observations', populated=True)
    originals = {key: path.read_bytes() for key, path in retained.items()}
    work = tmp_path / 'minimal'
    with ConversionReadOperation() as operation:
        generation = build_retained_family('fec-observations', snapshot['families']['fec-observations'],
            snapshot, retained, work, operation=operation)
    owned = list((work / 'build').rglob('*.parquet'))
    assert owned and all(path.is_relative_to(generation) for path in owned)
    assert len([path for path in owned if path.name == 'etl_receipts.parquet']) == 1
    assert all(path.read_bytes() == originals[key] for key, path in retained.items())


def test_partition_flat_removed_only_after_exact_readback(tmp_path, monkeypatch):
    import spicy_regs.fec_native_conversion as module
    original = tmp_path / 'original'
    part = original / 'source_namespace=n/part-000000.parquet'
    part.parent.mkdir(parents=True)
    table = pa.table({'source_namespace': ['n'], 'value': ['exact']})
    pq.write_table(table, part)
    flat = tmp_path / 'flat.parquet'
    pq.write_table(table, flat)
    module._partition_subject(flat, original, tmp_path / 'partitioned', lambda: None)
    assert not flat.exists() and part.exists()
    pq.write_table(table, flat)
    def refusal(*args):
        raise ValueError('readback mismatch')
    monkeypatch.setattr(module, '_equal_streams', refusal)
    with pytest.raises(ValueError, match='readback mismatch'):
        module._partition_subject(flat, original, tmp_path / 'failed', lambda: None)
    assert flat.exists() and not (tmp_path / 'failed/artifact.json').exists()


def test_receipt_consolidation_closes_recoverable_partial_on_interruption(tmp_path):
    from spicy_regs.fec_native_conversion import _consume_receipt
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA
    first, second, shared = [tmp_path / name for name in ('first.parquet', 'second.parquet', 'shared.parquet')]
    table = pa.Table.from_pylist([{}], schema=RECEIPT_SCHEMA)
    pq.write_table(table, first)
    pq.write_table(table, second)
    def stop():
        raise RuntimeError('resource stop')
    with pytest.raises(RuntimeError, match='resource stop'):
        with pq.ParquetWriter(shared, RECEIPT_SCHEMA) as writer:
            _consume_receipt(first, writer, lambda: None)
            _consume_receipt(second, writer, stop)
    assert not first.exists() and second.exists()
    assert pq.ParquetFile(shared).read().equals(table)
    assert not (tmp_path / 'artifact.json').exists()


def test_receipt_mutation_retains_local_source(tmp_path, monkeypatch):
    import spicy_regs.fec_native_conversion as module
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA
    receipt, shared = tmp_path / 'receipt.parquet', tmp_path / 'shared.parquet'
    pq.write_table(pa.Table.from_pylist([{}], schema=RECEIPT_SCHEMA), receipt)
    answers = iter(['original', 'changed'])
    monkeypatch.setattr(module, '_sha', lambda _: next(answers))
    with pq.ParquetWriter(shared, RECEIPT_SCHEMA) as writer:
        with pytest.raises(ValueError, match='changed during consolidation'):
            module._consume_receipt(receipt, writer, lambda: None)
    assert receipt.exists() and not (tmp_path / 'artifact.json').exists()


# Exact metadata-only captured selected schemas; no publisher row fixtures.
CAPTURED_REMAINING_SCHEMAS_011 = {'fec_registration_statements': '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', 'fec_report_metrics': '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', 'fec_reported_financial_summaries': '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yP3//wT+//8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABzb3VyY2VfYXV0aG9yaXR5AAAAAPz9//84/v//AAABBRAAAAAkAAAABAAAAAAAAAATAAAAc291cmNlX2xvY2F0b3JfanNvbgAw/v//bP7//wAAAQUQAAAAIAAAAAQAAAAAAAAADQAAAHNvdXJjZV9zaGEyNTYAAABg/v//nP7//wAAAQUQAAAAJAAAAAQAAAAAAAAAEAAAAHNvdXJjZV9yZWNvcmRfaWQAAAAAlP7//9D+//8AAAEFEAAAACAAAAAEAAAAAAAAAA0AAABjb2xsZWN0aW9uX2lkAAAAxP7//wD///8AAAEFEAAAACQAAAAEAAAAAAAAABMAAABtYXBwaW5nX3JlYXNvbl9qc29uAPj+//80////AAABBRAAAAAgAAAABAAAAAAAAAAOAAAAbWFwcGluZ19zdGF0dXMAACj///9k////AAABBRAAAAAoAAAABAAAAAAAAAAVAAAAdmFsdWVfbWFwcGluZ192ZXJzaW9uAAAAYP///5z///8AAAEFEAAAACAAAAAEAAAAAAAAAA8AAABtYXBwaW5nX3ZlcnNpb24AkP///8z///8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABpZGVudGl0eV92ZXJzaW9uAAAAAMT///8QABQACAAGAAcADAAAABAAEAAAAAAAAQUQAAAAIAAAAAQAAAAAAAAACQAAAHJlY29yZF9pZAAAAAQABAAEAAAA', 'fec_research_context_dispositions': '/////7ABAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAcAAABMAQAACAEAANQAAACkAAAAcAAAADgAAAAEAAAA4P7//wAAAQUQAAAAIAAAAAQAAAAAAAAADAAAAG91dHB1dHNfanNvbgAAAADQ/v//EP///wAAAQUQAAAAJAAAAAQAAAAAAAAAEQAAAHNvdXJjZV9mYWN0X2NvdW50AAAABP///0T///8AAAEFEAAAACAAAAAEAAAAAAAAAA4AAABjb250ZXh0X3NoYTI1NgAANP///3T///8AAAEFEAAAABwAAAAEAAAAAAAAAAoAAABzb3VyY2VfdXJsAABg////oP///wAAAQUQAAAAIAAAAAQAAAAAAAAADgAAAG1hcHBpbmdfcmVhc29uAACQ////0P///wAAAQUQAAAAIAAAAAQAAAAAAAAADgAAAG1hcHBpbmdfc3RhdHVzAADA////EAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAACQAAAAEAAAAAAAAAA0AAABjb2xsZWN0aW9uX2lkAAAABAAEAAQAAAA=', 'fec_research_document_observations': '/////3AEAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAABQAAAAQBAAAzAMAAJgDAABgAwAAMAMAAPwCAADIAgAAjAIAAFQCAAAcAgAA8AEAAMABAACMAQAAVAEAABwBAADkAAAArAAAAHwAAABAAAAABAAAAFD8//8AAAEFEAAAACgAAAAEAAAAAAAAABQAAABuYXRpdmVfbWV0YWRhdGFfanNvbgAAAABM/P//iPz//wAAAQUQAAAAKAAAAAQAAAAAAAAAFwAAAGZlY19yZWxhdGlvbnNoaXBfc3RhdHVzAIT8///A/P//AAABBRAAAAAcAAAABAAAAAAAAAALAAAAYm9keV9zdGF0dXMAsPz//+z8//8AAAEFEAAAACQAAAAEAAAAAAAAABIAAABvcmlnaW5hbF9leHRlbnNpb24AAOT8//8g/f//AAABBRAAAAAkAAAABAAAAAAAAAARAAAAc291cmNlX3VwZGF0ZWRfYXQAAAAY/f//VP3//wAAAQUQAAAAJAAAAAQAAAAAAAAAEQAAAHNvdXJjZV9jcmVhdGVkX2F0AAAATP3//4j9//8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABzb3VyY2VfcHVibGlzaGVyAAAAAID9//+8/f//AAABBRAAAAAgAAAABAAAAAAAAAANAAAAY2Fub25pY2FsX3VybAAAALD9///s/f//AAABBRAAAAAcAAAABAAAAAAAAAALAAAAZGVzY3JpcHRpb24A3P3//xj+//8AAAEFEAAAABgAAAAEAAAAAAAAAAUAAAB0aXRsZQAAAAT+//9A/v//AAABBRAAAAAkAAAABAAAAAAAAAASAAAAZG9jdW1lbnRfbmF0aXZlX2lkAAA4/v//dP7//wAAAQUQAAAAJAAAAAQAAAAAAAAAEAAAAG9ic2VydmVkX2F0X2pzb24AAAAAbP7//6j+//8AAAEFEAAAACgAAAAEAAAAAAAAABYAAABzb3VyY2VfY29udGV4dF9wb2ludGVyAACk/v//4P7//wAAAQUQAAAAIAAAAAQAAAAAAAAADgAAAGNvbnRleHRfc2hhMjU2AADU/v//EP///wAAAQUQAAAAIAAAAAQAAAAAAAAADQAAAHNvdXJjZV9zaGEyNTYAAAAE////QP///wAAAQUQAAAAHAAAAAQAAAAAAAAACgAAAHNvdXJjZV91cmwAADD///9s////AAABBRAAAAAkAAAABAAAAAAAAAAQAAAAc291cmNlX2F1dGhvcml0eQAAAABk////oP///wAAAQUQAAAAIAAAAAQAAAAAAAAADQAAAGNvbGxlY3Rpb25faWQAAACU////0P///wAAAQUQAAAAIAAAAAQAAAAAAAAADwAAAG1hcHBpbmdfdmVyc2lvbgDE////EAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAACAAAAAEAAAAAAAAAAkAAAByZWNvcmRfaWQAAAAEAAQABAAAAA==', 'fec_research_filing_feed_items': '/////7gDAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAABEAAABYAwAAFAMAAOACAACoAgAAeAIAAEQCAAAQAgAA1AEAAJwBAABwAQAARAEAABgBAADgAAAAsAAAAHgAAABAAAAABAAAAPz8//8AAAEFEAAAACgAAAAEAAAAAAAAABUAAABjdXJyZW50X3JlY29yZF9zdGF0dXMAAAD4/P//NP3//wAAAQUQAAAAJAAAAAQAAAAAAAAAEQAAAHNvdXJjZV9wYXJ0c19qc29uAAAALP3//2j9//8AAAEFEAAAACQAAAAEAAAAAAAAABIAAABuYXRpdmVfZmllbGRzX2pzb24AAGD9//+c/f//AAABBRAAAAAcAAAABAAAAAAAAAALAAAAZGVzY3JpcHRpb24AjP3//8j9//8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABwdWJsaXNoZWRfYXRfcmF3AAAAAMD9///8/f//AAABBRAAAAAYAAAABAAAAAAAAAAEAAAAZ3VpZAAAAADo/f//JP7//wAAAQUQAAAAGAAAAAQAAAAAAAAABAAAAGxpbmsAAAAAEP7//0z+//8AAAEFEAAAABgAAAAEAAAAAAAAAAUAAAB0aXRsZQAAADj+//90/v//AAABBRAAAAAkAAAABAAAAAAAAAAQAAAAb2JzZXJ2ZWRfYXRfanNvbgAAAABs/v//qP7//wAAAQUQAAAAKAAAAAQAAAAAAAAAFgAAAHNvdXJjZV9jb250ZXh0X3BvaW50ZXIAAKT+///g/v//AAABBRAAAAAgAAAABAAAAAAAAAAOAAAAY29udGV4dF9zaGEyNTYAANT+//8Q////AAABBRAAAAAgAAAABAAAAAAAAAANAAAAc291cmNlX3NoYTI1NgAAAAT///9A////AAABBRAAAAAcAAAABAAAAAAAAAAKAAAAc291cmNlX3VybAAAMP///2z///8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABzb3VyY2VfYXV0aG9yaXR5AAAAAGT///+g////AAABBRAAAAAgAAAABAAAAAAAAAANAAAAY29sbGVjdGlvbl9pZAAAAJT////Q////AAABBRAAAAAgAAAABAAAAAAAAAAPAAAAbWFwcGluZ192ZXJzaW9uAMT///8QABQACAAGAAcADAAAABAAEAAAAAAAAQUQAAAAIAAAAAQAAAAAAAAACQAAAHJlY29yZF9pZAAAAAQABAAEAAAA', 'fec_research_meeting_observations': '/////wAEAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAABIAAACcAwAAWAMAACQDAADsAgAAvAIAAIgCAABUAgAAGAIAAOABAACsAQAAfAEAAEgBAAAYAQAA6AAAALgAAACAAAAAPAAAAAQAAAC8/P//AAABAhAAAAAcAAAABAAAAAAAAAAJAAAAdGFibGVfcm93AAAAyP///wAAAAEgAAAA8Pz//wAAAQIQAAAAKAAAAAQAAAAAAAAADQAAAHRhYmxlX29yZGluYWwAAAAIAAwACAAHAAgAAAAAAAABIAAAADD9//8AAAEFEAAAACQAAAAEAAAAAAAAABEAAABuYXRpdmVfY2VsbHNfanNvbgAAACj9//9k/f//AAABBRAAAAAcAAAABAAAAAAAAAAKAAAAbGlua3NfanNvbgAAVP3//5D9//8AAAEFEAAAABwAAAAEAAAAAAAAAAsAAABkYXRlX3N0YXR1cwCA/f//vP3//wAAAQUQAAAAHAAAAAQAAAAAAAAACgAAAGRhdGVzX2pzb24AAKz9///o/f//AAABBRAAAAAgAAAABAAAAAAAAAAPAAAAcmVwb3J0ZWRfc3RhdHVzANz9//8Y/v//AAABBRAAAAAcAAAABAAAAAAAAAAJAAAAdGl0bGVfcmF3AAAACP7//0T+//8AAAEFEAAAACAAAAAEAAAAAAAAAAwAAABtZWV0aW5nX3R5cGUAAAAAOP7//3T+//8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABvYnNlcnZlZF9hdF9qc29uAAAAAGz+//+o/v//AAABBRAAAAAoAAAABAAAAAAAAAAWAAAAc291cmNlX2NvbnRleHRfcG9pbnRlcgAApP7//+D+//8AAAEFEAAAACAAAAAEAAAAAAAAAA4AAABjb250ZXh0X3NoYTI1NgAA1P7//xD///8AAAEFEAAAACAAAAAEAAAAAAAAAA0AAABzb3VyY2Vfc2hhMjU2AAAABP///0D///8AAAEFEAAAABwAAAAEAAAAAAAAAAoAAABzb3VyY2VfdXJsAAAw////bP///wAAAQUQAAAAJAAAAAQAAAAAAAAAEAAAAHNvdXJjZV9hdXRob3JpdHkAAAAAZP///6D///8AAAEFEAAAACAAAAAEAAAAAAAAAA0AAABjb2xsZWN0aW9uX2lkAAAAlP///9D///8AAAEFEAAAACAAAAAEAAAAAAAAAA8AAABtYXBwaW5nX3ZlcnNpb24AxP///xAAFAAIAAYABwAMAAAAEAAQAAAAAAABBRAAAAAgAAAABAAAAAAAAAAJAAAAcmVjb3JkX2lkAAAABAAEAAQAAAAAAAAA', 'fec_research_response_outcomes': '/////4ADAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAA8AAAAcAwAA2AIAAKQCAABsAgAAPAIAAAgCAADUAQAAmAEAAGABAAAsAQAA9AAAAMAAAACMAAAAVAAAAAQAAAAw/f//AAABAhAAAAA0AAAABAAAAAAAAAAYAAAAb2JzZXJ2ZWRfcGF5bG9hZF9yZWNvcmRzAAAAAAgADAAIAAcACAAAAAAAAAEgAAAAfP3//wAAAQUQAAAAJAAAAAQAAAAAAAAAEwAAAG5hdGl2ZV9wYXlsb2FkX2pzb24AdP3//7D9//8AAAEFEAAAACAAAAAEAAAAAAAAAA4AAABvdXRjb21lX3N0YXR1cwAApP3//+D9//8AAAEFEAAAACAAAAAEAAAAAAAAAA0AAABwYXlsb2FkX3NoYXBlAAAA1P3//xD+//8AAAEFEAAAACQAAAAEAAAAAAAAABIAAABxdWVyeV9jb21wbGV0ZW5lc3MAAAj+//9E/v//AAABBRAAAAAgAAAABAAAAAAAAAAPAAAAcHJvZmlsZV9yZWZ1c2FsADj+//90/v//AAABBRAAAAAkAAAABAAAAAAAAAAQAAAAb2JzZXJ2ZWRfYXRfanNvbgAAAABs/v//qP7//wAAAQUQAAAAKAAAAAQAAAAAAAAAFgAAAHNvdXJjZV9jb250ZXh0X3BvaW50ZXIAAKT+///g/v//AAABBRAAAAAgAAAABAAAAAAAAAAOAAAAY29udGV4dF9zaGEyNTYAANT+//8Q////AAABBRAAAAAgAAAABAAAAAAAAAANAAAAc291cmNlX3NoYTI1NgAAAAT///9A////AAABBRAAAAAcAAAABAAAAAAAAAAKAAAAc291cmNlX3VybAAAMP///2z///8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABzb3VyY2VfYXV0aG9yaXR5AAAAAGT///+g////AAABBRAAAAAgAAAABAAAAAAAAAANAAAAY29sbGVjdGlvbl9pZAAAAJT////Q////AAABBRAAAAAgAAAABAAAAAAAAAAPAAAAbWFwcGluZ192ZXJzaW9uAMT///8QABQACAAGAAcADAAAABAAEAAAAAAAAQUQAAAAIAAAAAQAAAAAAAAACQAAAHJlY29yZF9pZAAAAAQABAAEAAAAAAAAAA==', 'fec_research_source_pages': '/////3ADAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAABAAAAAQAwAAzAIAAJgCAABgAgAAMAIAAPwBAADIAQAAjAEAAFQBAAAkAQAA+AAAAMwAAACYAAAAaAAAADgAAAAEAAAAQP3//wAAAQUQAAAAIAAAAAQAAAAAAAAADQAAAGhlYWRpbmdzX2pzb24AAAA0/f//cP3//wAAAQUQAAAAHAAAAAQAAAAAAAAACgAAAHRpbWVzX2pzb24AAGD9//+c/f//AAABBRAAAAAcAAAABAAAAAAAAAAKAAAAbGlua3NfanNvbgAAjP3//8j9//8AAAEFEAAAACAAAAAEAAAAAAAAAA0AAABjb250ZW50X3Njb3BlAAAAvP3///j9//8AAAEFEAAAABgAAAAEAAAAAAAAAAQAAAB0ZXh0AAAAAOT9//8g/v//AAABBRAAAAAYAAAABAAAAAAAAAAFAAAAdGl0bGUAAAAM/v//SP7//wAAAQUQAAAAHAAAAAQAAAAAAAAACQAAAHBhZ2VfdHlwZQAAADj+//90/v//AAABBRAAAAAkAAAABAAAAAAAAAAQAAAAb2JzZXJ2ZWRfYXRfanNvbgAAAABs/v//qP7//wAAAQUQAAAAKAAAAAQAAAAAAAAAFgAAAHNvdXJjZV9jb250ZXh0X3BvaW50ZXIAAKT+///g/v//AAABBRAAAAAgAAAABAAAAAAAAAAOAAAAY29udGV4dF9zaGEyNTYAANT+//8Q////AAABBRAAAAAgAAAABAAAAAAAAAANAAAAc291cmNlX3NoYTI1NgAAAAT///9A////AAABBRAAAAAcAAAABAAAAAAAAAAKAAAAc291cmNlX3VybAAAMP///2z///8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABzb3VyY2VfYXV0aG9yaXR5AAAAAGT///+g////AAABBRAAAAAgAAAABAAAAAAAAAANAAAAY29sbGVjdGlvbl9pZAAAAJT////Q////AAABBRAAAAAgAAAABAAAAAAAAAAPAAAAbWFwcGluZ192ZXJzaW9uAMT///8QABQACAAGAAcADAAAABAAEAAAAAAAAQUQAAAAIAAAAAQAAAAAAAAACQAAAHJlY29yZF9pZAAAAAQABAAEAAAA', 'fec_retained_csv_observations': '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', 'fec_collections': '/////9ACAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAwAAABsAgAAKAIAAPwBAADEAQAAjAEAAFgBAAAgAQAA7AAAALAAAAB0AAAAOAAAAAQAAADU/f//AAABBRAAAAAgAAAABAAAAAAAAAAPAAAAYXJ0aWZhY3Rfc2hhMjU2AMT9//8E/v//AAABBRAAAAAoAAAABAAAAAAAAAAXAAAAY29sbGVjdGlvbl9vdXRjb21lX2pzb24A/P3//zz+//8AAAEFEAAAACgAAAAEAAAAAAAAABQAAABjb3ZlcmFnZV9saW1pdHNfanNvbgAAAAA0/v//dP7//wAAAQUQAAAAKAAAAAQAAAAAAAAAFAAAAHJlcXVlc3RlZF9zY29wZV9qc29uAAAAAGz+//+s/v//AAABBRAAAAAgAAAABAAAAAAAAAAOAAAAcmVjb3JkX291dGNvbWUAAJz+///c/v//AAABBRAAAAAkAAAABAAAAAAAAAASAAAAcmVsYXRpb25zaGlwX2NvdW50AADQ/v//EP///wAAAQUQAAAAIAAAAAQAAAAAAAAADAAAAHJlY29yZF9jb3VudAAAAAAA////QP///wAAAQUQAAAAJAAAAAQAAAAAAAAAEgAAAHNvdXJjZV9zdGF0ZV9zY29wZQAANP///3T///8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABzb3VyY2Vfc3lzdGVtX2lkAAAAAGj///+o////AAABBRAAAAAYAAAABAAAAAAAAAAHAAAAcHJvZmlsZQCQ////0P///wAAAQUQAAAAIAAAAAQAAAAAAAAADQAAAHNvdXJjZV9mYW1pbHkAAADA////EAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAACQAAAAEAAAAAAAAAA0AAABjb2xsZWN0aW9uX2lkAAAABAAEAAQAAAA=', 'fec_relationships': '/////7gDAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAABEAAABUAwAAEAMAANgCAACgAgAAcAIAAEACAAAIAgAA2AEAAKQBAABwAQAARAEAAAgBAADUAAAApAAAAHQAAAA8AAAABAAAAAD9//8AAAEFEAAAACQAAAAEAAAAAAAAABIAAABzb3VyY2VfZmllbGRzX2pzb24AAPj8//80/f//AAABBRAAAAAkAAAABAAAAAAAAAATAAAAc291cmNlX2xvY2F0b3JfanNvbgAs/f//aP3//wAAAQUQAAAAHAAAAAQAAAAAAAAACwAAAG9ic2VydmVkX2F0AFj9//+U/f//AAABBRAAAAAcAAAABAAAAAAAAAAKAAAAc291cmNlX3VybAAAhP3//8D9//8AAAEFEAAAACAAAAAEAAAAAAAAAA0AAABzb3VyY2Vfc2hhMjU2AAAAtP3///D9//8AAAEFEAAAACgAAAAEAAAAAAAAABcAAABjYW5kaWRhdGVfZWxlY3Rpb25feWVhcgDs/f//KP7//wAAAQUQAAAAGAAAAAQAAAAAAAAABQAAAGN5Y2xlAAAAFP7//1D+//8AAAEFEAAAACAAAAAEAAAAAAAAAA0AAABzb3VyY2VfZmFtaWx5AAAARP7//4D+//8AAAEFEAAAACAAAAAEAAAAAAAAAAwAAAB2YWx1ZV9zdGF0dXMAAAAAdP7//7D+//8AAAEFEAAAABwAAAAEAAAAAAAAAAsAAABvYmplY3RfbmFtZQCg/v//3P7//wAAAQUQAAAAJAAAAAQAAAAAAAAAEAAAAG9iamVjdF9pZF9zdGF0dXMAAAAA1P7//xD///8AAAEFEAAAABwAAAAEAAAAAAAAAAsAAABvYmplY3RfdHlwZQAA////PP///wAAAQUQAAAAHAAAAAQAAAAAAAAACQAAAG9iamVjdF9pZAAAACz///9o////AAABBRAAAAAkAAAABAAAAAAAAAARAAAAcmVsYXRpb25zaGlwX3R5cGUAAABg////nP///wAAAQUQAAAAJAAAAAQAAAAAAAAAEQAAAHN1YmplY3RfaWRfc3RhdHVzAAAAlP///9D///8AAAEFEAAAACAAAAAEAAAAAAAAAAwAAABzdWJqZWN0X3R5cGUAAAAAxP///xAAFAAIAAYABwAMAAAAEAAQAAAAAAABBRAAAAAgAAAABAAAAAAAAAAKAAAAc3ViamVjdF9pZAAABAAEAAQAAAAAAAAA', 'fec_source_records': '/////7gDAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAABEAAABQAwAADAMAAOACAACoAgAAdAIAAEACAAAQAgAA3AEAAKgBAAB0AQAARAEAABQBAADcAAAAqAAAAHgAAAA8AAAABAAAAAT9//8AAAEFEAAAACQAAAAEAAAAAAAAABIAAABzb3VyY2VfcmVjb3JkX2pzb24AAPj8//84/f//AAABBRAAAAAoAAAABAAAAAAAAAAUAAAAZW1iZWRkZWRfYm9kaWVzX2pzb24AAAAAMP3//3D9//8AAAEFEAAAABwAAAAEAAAAAAAAAAsAAABhc3NldHNfanNvbgBc/f//nP3//wAAAQUQAAAAIAAAAAQAAAAAAAAADQAAAG1ldGFkYXRhX2pzb24AAACM/f//zP3//wAAAQUQAAAAJAAAAAQAAAAAAAAAEwAAAHNvdXJjZV9sb2NhdG9yX2pzb24AwP3//wD+//8AAAEFEAAAABwAAAAEAAAAAAAAAAsAAABvYnNlcnZlZF9hdADs/f//LP7//wAAAQUQAAAAHAAAAAQAAAAAAAAACgAAAHNvdXJjZV91cmwAABj+//9Y/v//AAABBRAAAAAgAAAABAAAAAAAAAANAAAAc291cmNlX3NoYTI1NgAAAEj+//+I/v//AAABBRAAAAAgAAAABAAAAAAAAAANAAAAYXVkaXRfY2FzZV9pZAAAAHj+//+4/v//AAABBRAAAAAgAAAABAAAAAAAAAAMAAAAbGVnYWxfZG9jX2lkAAAAAKj+///o/v//AAABBRAAAAAcAAAABAAAAAAAAAAJAAAAZmlsaW5nX2lkAAAA1P7//xT///8AAAEFEAAAACAAAAAEAAAAAAAAAAwAAABjYW5kaWRhdGVfaWQAAAAABP///0T///8AAAEFEAAAACAAAAAEAAAAAAAAAAwAAABjb21taXR0ZWVfaWQAAAAANP///3T///8AAAEFEAAAACQAAAAEAAAAAAAAABAAAABzb3VyY2VfcmVjb3JkX2lkAAAAAGj///+o////AAABBRAAAAAYAAAABAAAAAAAAAAHAAAAcHJvZmlsZQCQ////0P///wAAAQUQAAAAIAAAAAQAAAAAAAAADQAAAHNvdXJjZV9mYW1pbHkAAADA////EAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAACQAAAAEAAAAAAAAAA0AAABjb2xsZWN0aW9uX2lkAAAABAAEAAQAAAAAAAAA'}

# Retired tables (ss-j4vz B10) are omitted before any schema check, so their captured shapes are no longer admitted.
@pytest.mark.parametrize('table', sorted(set(CAPTURED_REMAINING_SCHEMAS_011)
                                         - {key.removesuffix('.parquet') for key in RETIRED_TABLES['fec-query'] | REBUILT_TABLES['fec-query'].keys()}))
def test_all_remaining_captured_schemas_admit_and_unknown_columns_refuse(table):
    from spicy_regs.fec_native_conversion import validate_identity_schema
    schema=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(CAPTURED_REMAINING_SCHEMAS_011[table])))
    validate_identity_schema(table,schema)
    bad=schema.append(pa.field('unknown_source_column',pa.string()))
    with pytest.raises(ValueError,match='neither supported current nor captured legacy'):
        validate_identity_schema(table,bad)
    for malformed in (schema.remove(0),pa.schema(list(reversed(schema))),
                      schema.set(0,pa.field(schema[0].name,pa.bool_())),
                      schema.set(0,pa.field(schema[0].name,schema[0].type,nullable=False))):
        with pytest.raises(ValueError,match='neither supported current nor captured legacy'):
            validate_identity_schema(table,malformed)


@pytest.mark.parametrize('table,field', [('fec_collections','record_count')])
@pytest.mark.parametrize('raw,native,status', [('0171',171,'parsed'),('0',0,'parsed'),
    ('9223372036854775807',2**63-1,'parsed'),('9223372036854775808',None,'unsupported_integer'),
    (None,None,'source_null'),('',None,'source_empty'),('+1',None,'unsupported_integer'),
    ('-1',None,'unsupported_integer'),(' 1',None,'unsupported_integer'),
    (1.0,None,'unsupported_integer'),(False,None,'unsupported_integer'),(True,None,'unsupported_integer')])
def test_captured_legacy_counts_restore_raw_types_and_refusals(tmp_path,table,field,raw,native,status):
    from spicy_regs.fec_native_conversion import retained_identity_row
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter,read_identity_rows
    from spicy_regs.etl_receipts import exact_json
    from tests.test_fec_identity_receipts import WITNESS
    schema=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(CAPTURED_REMAINING_SCHEMAS_011[table])))
    original=dict.fromkeys(schema.names)
    original.update(collection_id='synthetic',**{field:raw})
    original['relationship_count']='0002'
    diagnostic={}
    row,overrides=retained_identity_row(table,original,diagnostics=diagnostic,source_schema=schema)
    assert type(row[field]) is type(native) and row[field]==native
    assert diagnostic['field_statuses'][field]==status
    assert diagnostic['refusals'].get(field)==('unsupported_integer' if status=='unsupported_integer' else None)
    with IdentityReceiptWriter(tmp_path/'native',generation_id='count',tables=[table]) as writer:
        writer.emit(table,row,input_witness=WITNESS,source_input={'fec_retained_overrides':overrides,'fec_retained_interpretation':diagnostic})
    [internal]=read_identity_rows(tmp_path/'native',table,generation_id='count')
    held=internal['source_input']['fec_retained_overrides']
    moved=internal['conversion_inputs']
    restored={name:held.get(name,moved.get(name,internal.get(name))) for name in original}
    assert exact_json(restored)==exact_json(original)
    assert internal[field]==native and type(internal[field]) is type(native)
    assert internal['source_input']['fec_retained_interpretation']==diagnostic


@pytest.mark.parametrize('table,field', [('fec_collections','record_count')])
def test_current_count_schemas_keep_strict_badtype_refusal(tmp_path,table,field):
    from spicy_regs.fec_native_conversion import retained_identity_row,validate_identity_schema
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS
    schema=COLLECTION_SCHEMA
    validate_identity_schema(table,schema)
    original=dict.fromkeys(schema.names)
    original.update(collection_id='current',**{field:'0171'})
    diagnostic={}
    row,overrides=retained_identity_row(table,original,diagnostics=diagnostic,source_schema=schema)
    assert row==original and overrides=={} and diagnostic=={}
    with IdentityReceiptWriter(tmp_path/'strict',generation_id='current',tables=[table]) as writer:
        with pytest.raises(ValueError,match=field+': native integer required'):
            writer.emit(table,row,input_witness=WITNESS)
    row,overrides=retained_identity_row(table,{**original,field:None},diagnostics=diagnostic,source_schema=schema)
    assert row[field] is None and overrides=={} and diagnostic=={}


def test_legacy_count_bridges_reach_full_production_admission(tmp_path,monkeypatch,bucket):
    collection=dict(collection_id='legacy-count',source_family='fec',profile='synthetic',record_count='0171',relationship_count='0002')
    _,source=old_family(tmp_path/'source',monkeypatch,bucket,'fec-observations',rows_by_table={'fec_collections':[collection]},legacy_tables=['fec_collections'])
    snapshot,_=old_family(tmp_path/'query',monkeypatch,bucket,'fec-query')
    with ConversionReadOperation() as operation:
        native=build_retained_family('fec-observations',snapshot['families']['fec-observations'],snapshot,source,tmp_path/'native-source',operation=operation)
        _,source_reader,_=operation.admit_generation(native)
        comparison=source_reader.compare_original('fec_collections',source['fec_collections.parquet'])
        assert comparison['rows_only_in_restored']==comparison['rows_only_in_retained']==0 and comparison['type_changes']=={}
        [held]=source_reader.read_subjects('fec_collections')
        assert held['record_count']==171 and held['relationship_count']==2

@pytest.mark.parametrize('table,field', [('fec_collections','record_count')])
def test_captured_legacy_count_missing_field_has_explicit_diagnostics(table,field):
    # Synthetic missing key is a helper diagnostic, not a decoded Arrow row.
    from spicy_regs.fec_native_conversion import retained_identity_row
    schema=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(CAPTURED_REMAINING_SCHEMAS_011[table])))
    original=dict.fromkeys(schema.names)
    original.pop(field)
    diagnostics={}
    row,_=retained_identity_row(table,original,diagnostics=diagnostics,source_schema=schema)
    assert row[field] is None
    assert diagnostics['field_statuses'][field]=='source_missing'
    assert diagnostics['raw_values'][field] is None and field not in diagnostics['refusals']


def test_agency_source_lookup_semi_joins_the_admitted_parent_subjects(tmp_path, monkeypatch, bucket):
    """ss-3bkk: the DuckDB semi-join keeps the row replay's answers and refusals."""
    from spicy_regs.fec_native_conversion import agency_source_lookup
    a, b = 'sha256:' + 'a' * 64, 'sha256:' + 'b' * 64
    def record(collection, source_record, digest):
        return dict(collection_id=collection, source_record_id=source_record, source_sha256=digest,
                    source_family='fec-test', source_record_json='{}')
    rows = [record('c1', 'r1', a), record('c1', 'r1', a), record('c2', 'r2', b),
            record('c3', 'r3', a), record('c3', 'r3', b)]
    snapshot, retained = old_family(tmp_path, monkeypatch, bucket, 'fec-observations',
                                    rows_by_table={'fec_source_records': rows})
    with ConversionReadOperation() as operation:
        generation = build_retained_family('fec-observations', snapshot['families']['fec-observations'],
                                           snapshot, retained, tmp_path / 'work', operation=operation)
        pin = operation.admit_generation(generation)[0].pin.artifact_digest
        def lookup(coordinates, expected=pin):
            return agency_source_lookup(generation, operation, coordinates, expected, lambda: None)
        found = lookup({('c1', 'r1'), ('c2', 'r2')})
        assert [found(dict(collection_id=c, source_record_id=r)) for c, r in (('c1', 'r1'), ('c2', 'r2'))] == [a, b]
        assert lookup(set())(dict(collection_id='c1', source_record_id='r1')) is None
        for coordinates, refusal in (({('c3', 'r3')}, 'conflicting digest'), ({('c1', 'r1'), ('c9', 'r9')}, 'absent')):
            with pytest.raises(ValueError, match=refusal):
                lookup(coordinates)
        with pytest.raises(ValueError, match='differs from selected parent'):
            lookup({('c1', 'r1')}, 'sha256:' + 'f' * 64)
