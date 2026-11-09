"""Retained docket references keep edition, source position and literal target."""
import copy
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, regulations_bulk
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest, decode_exact_json, exact_json
from spicy_regs.transforms import regulations_receipts
from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal, write_held_dataset
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, TYPES


def _source(tmp_path):
    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS['fr_docket_links']],
                       metadata={b'dump_date': b'2026-06-30'})
    rows = []
    for date, ordinal, docket in [
        ('2026-06-30', 0, 'EPA-2026-0001'),
        ('2026-06-30', 1, 'EPA-2026-0001'),
        ('2026-06-30', None, 'EPA-2026-0001'),
        ('2026-06-30', None, 'EPA-2026-0002'),
        ('2025-06-30', 0, 'EPA-2026-0001'),
    ]:
        raw = dict.fromkeys(schema.names)
        raw.update(document_number='2026-00001', publication_date=date,
                   docket_source_ordinal=ordinal, docket_id=docket,
                   docket_ids_json='[ "EPA-2026-0001", "EPA-2026-0001" ]',
                   normalized_docket_candidates_json='[ "EPA-2026-0001", "EPA-2026-0001" ]',
                   docket_normalization_rule='literal', agency_slugs='[ "epa" ]',
                   html_url='https://example.gov/register')
        rows.append(raw)
    source = tmp_path / 'source.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source)
    subject, receipts = write_held_dataset('fr_docket_links', source, tmp_path / 'native', generation_id='g')
    return ReceiptInput('fr_docket_links', (subject,), receipts, 'g'), source, schema


def test_complete_nullable_identity_preserves_source_positions_order_and_originals(tmp_path, monkeypatch):
    selected, source, schema = _source(tmp_path)
    reference = materialize_internal(selected, tmp_path / 'reference.parquet', source_schema=schema)
    monkeypatch.setattr(regulations_bulk, '_BATCH', 2)
    def no_row_fallback(*args, **kwargs):
        raise AssertionError('Complete nullable identities must finish in the batch reader')
    monkeypatch.setattr(regulations_receipts, '_qualified_rows', no_row_fallback)
    actual = materialize_internal(selected, tmp_path / 'bulk.parquet', bulk=True, source_schema=schema)
    restored = pq.read_table(actual)
    assert restored.equals(pq.read_table(reference), check_metadata=True)
    assert restored.equals(pq.read_table(source), check_metadata=True)
    assert restored['docket_source_ordinal'].to_pylist() == [0, 1, None, None, 0]
    assert regulations_bulk.DATASETS == frozenset({'dockets', 'documents', *regulations_bulk.ATTRIBUTES})
    assert not regulations_bulk.eligible('fr_docket_links', schema)


@pytest.mark.parametrize('change', ['missing', 'ambiguous', 'digest', 'version', 'raw', 'metadata', 'generation'])
def test_invalid_selected_receipts_preserve_destination(tmp_path, change):
    selected, _, schema = _source(tmp_path)
    rows = pq.read_table(selected.receipts).to_pylist()
    last = next(row for row in reversed(rows) if row['outcome'] == 'accepted')
    if change == 'missing':
        rows.remove(last)
    elif change == 'ambiguous':
        extra = copy.deepcopy(last)
        extra['attempt_id'] += '-another'
        extra['receipt_id'] = _digest({key: value for key, value in extra.items() if key != 'receipt_id'})
        rows.append(extra)
    elif change == 'digest':
        last['receipt_id'] = 'sha256:' + '0' * 64
    else:
        if change == 'generation':
            last['generation_id'] = ''
        elif change == 'version':
            last['subject_version'] = 'sha256:' + '0' * 64
        else:
            processing = decode_exact_json(last['processing_json'])
            if change == 'raw':
                processing['raw_conversion_inputs']['docket_source_ordinal'] = 9
            else:
                processing['input_metadata'] = {'dump_date': 'different'}
            last['processing_json'] = exact_json(processing)
        last['receipt_id'] = _digest({key: value for key, value in last.items() if key != 'receipt_id'})
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), selected.receipts)
    destination = tmp_path / 'existing.parquet'
    destination.write_bytes(b'preserve')
    with pytest.raises((ValueError, etl_bulk.NotBulkEligible)):
        materialize_internal(selected, destination, bulk=True, source_schema=schema)
    assert destination.read_bytes() == b'preserve'


def test_nullable_identity_field_still_requires_its_declared_type(tmp_path):
    selected, _, _ = _source(tmp_path)
    schema = pa.schema([('document_number', pa.string()), ('docket_source_ordinal', pa.string())])
    with pytest.raises(ValueError, match='source schema differs'):
        materialize_internal(selected, tmp_path / 'invalid.parquet', bulk=True, source_schema=schema)
    assert not (tmp_path / 'invalid.parquet').exists()


@pytest.mark.parametrize('fallback', [False, True])
def test_selected_processing_prior_preserves_nullable_positions_and_exact_fallback(tmp_path, monkeypatch, fallback):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.sources.publication import empty_index

    selected, source, _ = _source(tmp_path)
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
    output = prior.get('fr_docket_links')
    assert pq.read_table(output).equals(pq.read_table(source), check_metadata=True)
    assert calls == (['row'] if fallback else [])
    assert prior.get('fr_docket_links') == output
    assert calls == (['row'] if fallback else [])
