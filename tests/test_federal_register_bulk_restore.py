"""Retained Register editions use their full identity in the maintained batch reader."""
import copy
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, regulations_bulk
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest, decode_exact_json, exact_json
from spicy_regs.transforms import regulations_receipts
from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal, write_held_dataset
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS


def _source(tmp_path, rows):
    schema = pa.schema([(name, pa.string()) for name, _ in SOURCE_COLUMNS['federal_register']],
                       metadata={b'dump_date':b'2026-06-30'})
    complete = []
    for row in rows:
        raw = dict.fromkeys(schema.names)
        raw.update(volume='0091', significant='True', regulations_dot_gov_comments_count='0007',
                   docket_ids_json='[ "EPA-2026-0001", "EPA-2026-0001" ]',
                   cfr_references_json='[ {"title":7,"part":"01","chapter":"I"},'
                                       ' {"title":7,"part":"01","chapter":"I"} ]',
                   html_url='https://example.gov/register')
        raw.update(row)
        complete.append(raw)
    source = tmp_path / 'source.parquet'
    pq.write_table(pa.Table.from_pylist(complete, schema=schema), source)
    subject, receipts = write_held_dataset('federal_register', source, tmp_path / 'native', generation_id='g')
    return ReceiptInput('federal_register', (subject,), receipts, 'g'), source, schema


@pytest.mark.parametrize('repeated_numbers', [1, 483])
def test_complete_composite_editions_match_row_authority_without_fallback(tmp_path, monkeypatch, repeated_numbers):
    rows = [{'document_number':f'2026-{i:05}', 'publication_date':'2025-06-30', 'title':f'first-{i}'}
            for i in range(repeated_numbers)]
    rows += [{'document_number':f'2026-{i:05}', 'publication_date':'2026-06-30', 'title':f'second-{i}'}
             for i in reversed(range(repeated_numbers))]
    selected, source, schema = _source(tmp_path, rows)
    reference = materialize_internal(selected, tmp_path / 'reference.parquet', source_schema=schema)
    monkeypatch.setattr(regulations_bulk, '_BATCH', 37)
    def no_row_fallback(*args, **kwargs):
        raise AssertionError('A complete composite-key publication must finish in the batch reader')
    monkeypatch.setattr(regulations_receipts, '_qualified_rows', no_row_fallback)
    actual = materialize_internal(selected, tmp_path / 'bulk.parquet', bulk=True, source_schema=schema)
    held = pq.read_table(actual)
    assert held.equals(pq.read_table(reference), check_metadata=True)
    assert held.equals(pq.read_table(source), check_metadata=True)
    assert len(held) == 2 * repeated_numbers
    assert held['publication_date'].to_pylist() == [row['publication_date'] for row in rows]
    assert regulations_bulk.DATASETS == frozenset({'dockets', 'documents', *regulations_bulk.ATTRIBUTES})
    assert not regulations_bulk.eligible('federal_register', schema)


@pytest.mark.parametrize('change', ['missing', 'ambiguous', 'digest', 'version', 'raw', 'metadata'])
def test_late_corruption_or_ambiguous_receipts_refuse_without_replacing_destination(tmp_path, change):
    selected, _, schema = _source(tmp_path, [
        {'document_number':'2026-00001', 'publication_date':'2025-06-30'},
        {'document_number':'2026-00001', 'publication_date':'2026-06-30'},
    ])
    rows = pq.read_table(selected.receipts).to_pylist()
    last = next(row for row in reversed(rows) if row['outcome'] == 'accepted')
    if change == 'missing':
        rows.remove(last)
    elif change == 'ambiguous':
        extra = copy.deepcopy(last)
        extra['attempt_id'] += '-another'
        extra['receipt_id'] = _digest({key:value for key,value in extra.items() if key != 'receipt_id'})
        rows.append(extra)
    elif change == 'digest':
        last['receipt_id'] = 'sha256:' + '0' * 64
    else:
        if change == 'version':
            last['subject_version'] = 'sha256:' + '0' * 64
        else:
            values = decode_exact_json(last['processing_json'])
            if change == 'raw':
                values['raw_conversion_inputs']['publication_date'] = '2024-06-30'
            else:
                values['input_metadata'] = {'dump_date':'different'}
            last['processing_json'] = exact_json(values)
        last['receipt_id'] = _digest({key:value for key,value in last.items() if key != 'receipt_id'})
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), selected.receipts)
    destination = tmp_path / 'existing.parquet'
    destination.write_bytes(b'preserve')
    with pytest.raises((ValueError, etl_bulk.NotBulkEligible)):
        materialize_internal(selected, destination, bulk=True, source_schema=schema)
    assert destination.read_bytes() == b'preserve'


@pytest.mark.parametrize('schema', [
    pa.schema([('document_number', pa.int64()), ('publication_date', pa.string())]),
    pa.schema([('document_number', pa.string()), ('undeclared_source_field', pa.string())]),
])
def test_explicit_source_schema_stays_declared_and_typed(tmp_path, schema):
    selected, _, _ = _source(tmp_path, [{'document_number':'2026-00001', 'publication_date':'2026-06-30'}])
    with pytest.raises(ValueError, match='source schema differs'):
        materialize_internal(selected, tmp_path / 'invalid.parquet', bulk=True, source_schema=schema)
    assert not (tmp_path / 'invalid.parquet').exists()


def test_missing_identity_remains_a_refused_attempt_with_empty_source_metadata(tmp_path):
    selected, _, schema = _source(tmp_path, [{'document_number':'2026-00001', 'publication_date':None}])
    assert pq.read_table(selected.subjects[0]).num_rows == 0
    assert pq.read_table(selected.receipts)['outcome'].to_pylist() == ['observed', 'refused']
    reference = materialize_internal(selected, tmp_path / 'reference.parquet', source_schema=schema)
    actual = materialize_internal(selected, tmp_path / 'bulk.parquet', bulk=True, source_schema=schema)
    assert pq.read_table(actual).equals(pq.read_table(reference), check_metadata=True)
    assert pq.read_table(actual).num_rows == 0


def test_unproven_batch_proof_keeps_exact_row_fallback(tmp_path, monkeypatch):
    selected, source, schema = _source(tmp_path, [
        {'document_number':'2026-00001', 'publication_date':'2026-06-30\x1b'},
    ])
    calls = []
    original = regulations_receipts._qualified_rows
    def reference(*args, **kwargs):
        calls.append('row')
        yield from original(*args, **kwargs)
    def unavailable_batch_proof(*args, **kwargs):
        raise etl_bulk.NotBulkEligible('Batch reproduction cannot establish this input')
    monkeypatch.setattr(regulations_bulk, '_materialize_selected', unavailable_batch_proof)
    monkeypatch.setattr(regulations_receipts, '_qualified_rows', reference)
    actual = materialize_internal(selected, tmp_path / 'bulk.parquet', bulk=True, source_schema=schema)
    assert calls == ['row']
    assert pq.read_table(actual).equals(pq.read_table(source), check_metadata=True)


@pytest.mark.parametrize('fallback', [False, True])
def test_selected_processing_prior_preserves_editions_and_exact_fallback(tmp_path, monkeypatch, fallback):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.sources.publication import empty_index

    selected, source, _ = _source(tmp_path, [
        {'document_number': '2026-00001', 'publication_date': '2025-06-30', 'title': 'first'},
        {'document_number': '2026-00001', 'publication_date': '2026-06-30', 'title': 'second'},
    ])
    prior = SelectedPriors(tmp_path / 'selected', index=empty_index(), public_url='https://example.invalid')
    monkeypatch.setattr(prior.selected, 'select', lambda _, ranged=False: SimpleNamespace(
        subjects=selected.subjects, receipts=selected.receipts, generation_id=selected.generation_id,
    ))
    calls = []
    reference = regulations_receipts._qualified_rows

    def row_reader(*args, **kwargs):
        calls.append('row')
        if not fallback:
            raise AssertionError('A proven selected prior must use the batch reader')
        yield from reference(*args, **kwargs)

    monkeypatch.setattr(regulations_receipts, '_qualified_rows', row_reader)
    if fallback:
        def unproven(*args, **kwargs):
            raise etl_bulk.NotBulkEligible('Retained input has no batch proof')
        monkeypatch.setattr(regulations_bulk, '_materialize_selected', unproven)
    output = prior.get('federal_register')
    assert pq.read_table(output).equals(pq.read_table(source), check_metadata=True)
    assert calls == (['row'] if fallback else [])
    assert prior.get('federal_register') == output
    assert calls == (['row'] if fallback else [])
