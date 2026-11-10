"""New FEC readers admit exact shipped /1 pairs without relabeling old subjects."""

import duckdb
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, write_dataset, selected_subject_policy
from spicy_regs.fec_receipt_adapter import ReceiptAdapter
from spicy_regs.policy_migrations import earlier_policies, earlier_policy, ledger
from spicy_regs.subject_catalog import policies

#: Each FEC dataset's exact /1, the shape every FEC generation was first published under.
FIRST=('fec-subject-receipts/1','fec-identity-context-receipts/1')
HISTORY={name:p.descriptor() for name,entry in ledger().items() for p in [earlier_policy(name,e['policy_version'])
         for e in entry.get('earlier',()) if e['policy_version'] in FIRST]}


@pytest.mark.parametrize('dataset',sorted(HISTORY))
def test_every_exact_pre_migration_policy_is_supported(dataset):
    priors=earlier_policies(policies()[dataset])
    assert priors[-1].descriptor()==HISTORY[dataset]
    # A /3 dataset also reads the /2 its conversion shipped; the rest read only their /1.
    assert [p.policy_version.rsplit('/',1)[1] for p in priors]==(
        ['2','1'] if policies()[dataset].policy_version.endswith('/3') else ['1'])


def old_pair(tmp_path, table, row):
    policy=DatasetPolicy.from_descriptor(HISTORY[table])
    context=ReceiptContext('historical','row','old-writer',[{'source_id':'retained','body_version':'selected-original'}])
    subject,receipt=write_dataset([(row,context)],tmp_path,policy)
    return policy,subject,receipt


def test_adapter_restores_historical_receipt_only_collection_without_new_main_file(tmp_path):
    row={'collection_id':'old-collection','source_family':'bulk','record_count':1,
         'conversion_inputs':{'collection_id':'old-collection','source_family':'bulk','record_count':1}}
    prior,subject,receipt=old_pair(tmp_path/'bundle','fec_collections',row)
    assert subject is None and prior.receipt_only
    native={'fec_collections':dict(subjects=[],receipts=receipt,generation_id='historical')}
    with duckdb.connect() as con:
        adapter=ReceiptAdapter(con,{'families':{}},'',local_native=native)
        [restored]=adapter.selected_rows('fec_collections')
        assert restored==row
        target=adapter.restore('fec_collections')
        assert con.execute(f'SELECT collection_id,source_family,record_count FROM "{target}"').fetchall()==[('old-collection','bulk',1)]


def test_adapter_and_scalar_reader_keep_old_financial_shape_and_source_literals(tmp_path):
    from tests.test_fec_subject_receipts import source_row
    from spicy_regs.transforms.fec_subject_receipts import mapped_record
    from spicy_regs.transforms.fec_subject_receipts import read_fec_with_receipts
    row=source_row()
    current=policies()['fec_receipts']
    prior=DatasetPolicy.from_descriptor(HISTORY['fec_receipts'])
    mapped=mapped_record('fec_receipts',row,current)
    held={name:mapped.get(name) for name in prior.input_fields}
    held["fec_conversion_inputs"] = {**mapped["fec_conversion_inputs"],
        **{name:value for name,value in row.items() if name not in prior.subject_schema.names}}
    prior,subject,receipt=old_pair(tmp_path/'bundle','fec_receipts',held)
    before=subject.read_bytes()
    assert selected_subject_policy(current,[subject]).descriptor()==prior.descriptor()
    [restored]=read_fec_with_receipts([subject],[receipt],current,generation_id='historical')
    assert restored==row
    with duckdb.connect() as con:
        adapter=ReceiptAdapter(con,{'families':{}},'',local_native={
            'fec_receipts':dict(subjects=[subject],receipts=receipt,generation_id='historical')})
        target=adapter.restore('fec_receipts')
        assert con.execute(f'SELECT source_namespace FROM "{target}"').fetchone()==(row['source_namespace'],)
    assert subject.read_bytes()==before and pq.read_schema(subject)==prior.subject_schema


def test_selected_priors_restores_old_committee_policy_before_new_context_cutover(tmp_path):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from spicy_regs.transforms.build_fec_committees import COLUMNS
    original=dict.fromkeys(COLUMNS)
    original.update(committee_id='C00000001',name='Retained',cycles_json='[2026]',candidate_ids_json='[]')
    prior=DatasetPolicy.from_descriptor(HISTORY['fec_committees'])
    row={name:None for name in prior.subject_schema.names}
    row.update(committee_id='C00000001',name='Retained',cycles=[2026],candidate_ids=[],
               cycles_json=original['cycles_json'],candidate_ids_json=original['candidate_ids_json'],
               conversion_inputs=original)
    prior,subject,receipt=old_pair(tmp_path/'bundle','fec_committees',row)
    root=tmp_path/'root'
    remember_selection(root,[SelectedDataset('fec_committees',(subject,),receipt,'historical')])
    selected=SelectedPriors(tmp_path/'read',root=root,public_url='')
    output=selected.get('fec_committees')
    assert pq.read_table(output).to_pylist()==[original]


def test_additive_fcc_history_preserves_both_exact_old_declarations():
    from spicy_regs.transforms.government_receipts import POLICIES
    assert earlier_policies(POLICIES['fcc_filings'])==tuple(
        earlier_policy('fcc_filings',f'government-sources/{n}') for n in (2,1))


def test_shared_schema_does_not_admit_a_changed_complete_identity(tmp_path, monkeypatch):
    from dataclasses import replace
    import spicy_regs.etl_receipts as receipts
    from tests.test_fec_subject_receipts import source_row
    from spicy_regs.transforms.fec_subject_receipts import mapped_record
    policy=policies()['fec_receipts']
    alternate=replace(policy,identity_fields=('collection_id',),policy_version='unrelated-history')
    subject,_=write_dataset([(mapped_record('fec_receipts',source_row(),policy),
                            ReceiptContext('same','row','writer',[{'source_id':'fixture','body_version':'selected'}]))],tmp_path/'bundle',policy)
    assert subject is not None
    monkeypatch.setattr(receipts,'receipt_policies',lambda current: (current,alternate))
    with pytest.raises(ValueError,match='Subject schema differs'):
        selected_subject_policy(policy,[subject])


def test_historical_receipt_only_selection_has_no_public_main_or_queryable_empty_table(tmp_path, monkeypatch):
    from spicy_regs.local_data import local_selection
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from spicy_regs import mcp_server
    from tests.test_mcp_server import _tool_data
    from mcp.server.mcpserver.exceptions import ToolError
    from tests.test_local_native_selection import connection
    row={'collection_id':'old-collection','source_family':'bulk','record_count':1,
         'conversion_inputs':{'collection_id':'old-collection','source_family':'bulk','record_count':1}}
    _,subject,receipt=old_pair(tmp_path/'bundle','fec_collections',row)
    assert subject is None
    root=tmp_path/'state'
    remember_selection(root,[SelectedDataset('fec_collections',(),receipt,'historical')])
    selection=local_selection(root)
    assert 'fec_collections' not in selection.files and 'fec_collections' in selection.native
    con=connection(root,monkeypatch)
    monkeypatch.setattr(mcp_server,'_get_connection',lambda: con)
    try:
        with pytest.raises(ToolError,match='Unknown table'):
            _tool_data(mcp_server.build_server(),'describe_table',{'table':'fec_collections'})
        with pytest.raises(ToolError,match='fec_collections'):
            _tool_data(mcp_server.build_server(),'query_sql',{'sql':'SELECT * FROM fec_collections'})
        adapter=ReceiptAdapter(con,{'families':{}},'',local_native={
            'fec_collections':dict(subjects=[],receipts=receipt,generation_id='historical')})
        assert list(adapter.selected_rows('fec_collections'))==[row]
    finally:
        con.close()


def test_current_empty_promoted_fec_main_is_a_real_visible_file(tmp_path):
    from spicy_regs.local_data import local_selection
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    subject,receipt=write_dataset([],tmp_path/'empty',policies()['fec_collections'])
    assert subject is not None and pq.ParquetFile(subject).metadata.num_rows==0
    root=tmp_path/'state'
    remember_selection(root,[SelectedDataset('fec_collections',(subject,),receipt,'current-empty')])
    selected=local_selection(root)
    assert selected.paths('fec_collections')==(subject,) and 'fec_collections' in selected.files


@pytest.mark.parametrize('damage',['unknown','mixed','accepted-current-without-subject'])
def test_no_subject_selection_refuses_unknown_mixed_or_missing_current_rows(tmp_path, damage):
    import pyarrow as pa
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest
    from spicy_regs.local_data import local_selection
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    row={'collection_id':'held','source_family':'bulk','record_count':1,'conversion_inputs':{'collection_id':'held'}}
    if damage=='accepted-current-without-subject':
        policy=policies()['fec_collections']
        current={name:None for name in policy.input_fields}
        current.update(row)
        subject,receipt=write_dataset([(current,ReceiptContext('historical','row','new-writer',[{'source_id':'held','body_version':'held'}]))],tmp_path/'bundle',policy)
        assert subject is not None
    else:
        _,_,receipt=old_pair(tmp_path/'bundle','fec_collections',row)
        records=pq.read_table(receipt).to_pylist()
        wrong=dict(records[0],policy_version='undeclared')
        wrong['receipt_id']=_digest({k:v for k,v in wrong.items() if k!='receipt_id'})
        pq.write_table(pa.Table.from_pylist([wrong] if damage=='unknown' else [*records,wrong],schema=RECEIPT_SCHEMA),receipt)
    root=tmp_path/'state'
    remember_selection(root,[SelectedDataset('fec_collections',(),receipt,'historical')])
    with pytest.raises(ValueError,match='policy|receipt|subject|Subject'):
        local_selection(root)
