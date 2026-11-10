"""Regulatory carry keeps current bytes, direct predecessors and unresolved source attempts."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, regulations_bulk
from spicy_regs.etl_receipts import ReceiptContext, decode_exact_json
from spicy_regs.transforms.regulations_receipts import (
    ReceiptInput, materialize_internal, policy, write_held_dataset, write_records,
)
from tests.test_regulatory_attributes_bulk import _table


def _seed(tmp_path, dataset):
    key = policy(dataset).identity_fields[0]
    field = 'title' if dataset in {'dockets', 'documents'} else 'city' if dataset == 'comment_attributes' else 'category'
    rows = _table(dataset, [{key: name, field: 'before'} for name in ('same', 'changed', 'pending', 'resolved')]).to_pylist()
    for row in rows[2:]:
        row['unknown'] = 'refuse and retain'
    witness = {'source_id': 'retained', 'source_uri': None, 'sha256': 'a' * 64, 'locator': 'row', 'body_version': None}
    pair = write_records(dataset, [(row, ReceiptContext('old', str(i), 'test', [witness])) for i, row in enumerate(rows)],
                         tmp_path / 'seed')
    return key, field, rows, pair


def _by_key(receipts, key):
    accepted, refused = {}, {}
    for row in pq.read_table(receipts).to_pylist():
        if row['outcome'] in {'accepted', 'refused'}:
            raw = decode_exact_json(row['processing_json'])['raw_conversion_inputs']
            (accepted if row['outcome'] == 'accepted' else refused)[raw[key]] = row
    return accepted, refused


@pytest.mark.parametrize('dataset', ['dockets', 'documents', 'comment_attributes', 'docket_attributes', 'document_attributes'])
@pytest.mark.parametrize('bulk', [False, True])
def test_both_writer_routes_carry_exact_receipts_and_resolve_only_matching_refusals(tmp_path, dataset, bulk):
    key, field, rows, prior = _seed(tmp_path, dataset)
    before, refused = _by_key(prior[1], key)
    current = _table(dataset, [rows[0], rows[1] | {field: 'after'}, {key: 'new'}, {key: 'resolved'}])
    source = tmp_path / 'current.parquet'
    pq.write_table(current, source)
    first = write_held_dataset(dataset, source, tmp_path / 'first', generation_id='new', prior_receipts=[prior[1]], bulk=bulk)
    after, pending = _by_key(first[1], key)
    assert after['same'] == before['same']
    assert decode_exact_json(after['changed']['diagnostic_json'])['prior_receipt']['receipt_id'] == before['changed']['receipt_id']
    assert 'prior_receipt' not in decode_exact_json(after['new']['diagnostic_json'])
    assert pending == {'pending': refused['pending']}
    second = write_held_dataset(dataset, source, tmp_path / 'second', generation_id='again', prior_receipts=[first[1]], bulk=bulk)
    assert _by_key(second[1], key) == (after, pending)
    restored = materialize_internal(ReceiptInput(dataset, (second[0],), second[1], 'again'),
                                    tmp_path / 'restored.parquet', bulk=bulk, source_schema=current.schema)
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)


def test_attribute_nightly_keeps_historical_refusals_and_refuses_only_new_failures(tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset
    from spicy_regs.selected_generations import SelectedDataset, SelectedInputs, remember_selection
    dataset = 'document_attributes'  # comment_attributes publishes without receipts (ss-vse3)
    key, _, _, prior = _seed(tmp_path, dataset)
    root = tmp_path / 'root'
    remember_selection(root, [SelectedDataset(dataset, (prior[0],), prior[1], 'old')])
    inputs = SelectedInputs(root, root / 'selected', public_url='')
    fresh = tmp_path / 'fresh.parquet'
    pq.write_table(_table(dataset, [{key: 'new'}, {key: 'resolved'}]), fresh)
    result = finish_attribute_dataset(root, dataset, fresh, publish=False, inputs=inputs)
    _, pending = _by_key(result.parent / 'etl_receipts.parquet', key)
    assert set(pending) == {'pending'}
    selected = inputs.select(dataset)
    pq.write_table(_table(dataset, [{key: 'bad', 'display_properties_json': '[bad'}]), fresh)
    with pytest.raises(ValueError, match='native conversion refused'):
        finish_attribute_dataset(root, dataset, fresh, publish=False, inputs=inputs)
    assert inputs.select(dataset) == selected


def test_empty_attribute_prior_preserves_metadata_and_replaces_fresh_file_metadata(tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset, finish_dataset
    from spicy_regs.selected_generations import SelectedInputs
    dataset = 'docket_attributes'
    root = tmp_path / 'root'
    source = tmp_path / 'seed.parquet'
    pq.write_table(_table(dataset, []).replace_schema_metadata({'placement': 'original'}), source)
    finish_dataset(root, dataset, source, publish=False)
    inputs = SelectedInputs(root, root / 'selected', public_url='')
    pq.write_table(_table(dataset, [{'docket_id': 'new'}]).replace_schema_metadata({'placement': 'different'}), source)
    result = finish_attribute_dataset(root, dataset, source, publish=False, inputs=inputs)
    restored = materialize_internal(ReceiptInput(dataset, (result,), result.parent / 'etl_receipts.parquet', 'selected'),
                                    tmp_path / 'restored.parquet', bulk=True)
    assert pq.read_schema(restored).metadata == {b'placement': b'original'}


def test_attribute_nightly_admits_the_prior_before_copy_or_carry(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from spicy_regs import native_merge
    from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset
    from spicy_regs.selected_generations import SelectedDataset
    dataset = 'document_attributes'  # comment_attributes publishes without receipts (ss-vse3)
    key, _, _, prior = _seed(tmp_path, dataset)
    rows = pq.read_table(prior[1]).to_pylist()
    rows[0]['processor'] = 'changed without a new receipt digest'
    pq.write_table(pa.Table.from_pylist(rows, schema=pq.read_schema(prior[1])), prior[1])
    selected = SelectedDataset(dataset, (prior[0],), prior[1], 'old')
    inputs = SimpleNamespace(select=lambda _: selected, index=None)
    source = tmp_path / 'fresh.parquet'
    pq.write_table(_table(dataset, [{key: 'same'}]), source)
    monkeypatch.setattr(native_merge, 'native_merge', lambda *a, **k: pytest.fail('unadmitted prior reached merge'))
    with pytest.raises(ValueError):
        finish_attribute_dataset(tmp_path / 'root', dataset, source, publish=False, inputs=inputs)


def test_mapped_schema_fallback_obeys_required_bulk_before_row_reader(tmp_path, monkeypatch):
    dataset = 'comment_attributes'
    source = tmp_path / 'source.parquet'
    pq.write_table(_table(dataset, [{'comment_id': 'one'}]), source)
    pair = write_held_dataset(dataset, source, tmp_path / 'native', generation_id='g', bulk=True)
    table = pq.read_table(pair[0])
    schema = pa.schema([f.with_type(pa.large_string()) if f.name == 'comment_id' else f for f in table.schema])
    pq.write_table(table.cast(schema), pair[0])
    from spicy_regs.transforms import regulations_receipts
    monkeypatch.setattr(regulations_receipts, '_qualified_rows', lambda *a, **k: pytest.fail('row reader bypassed required bulk'))
    with etl_bulk.bulk_limits(threads=1, memory_limit='512MB', require_bulk=True), pytest.raises(ValueError, match='reference fallback disabled'):
        materialize_internal(ReceiptInput(dataset, (pair[0],), pair[1], 'g'), tmp_path / 'out.parquet', bulk=True)


def test_restore_order_fault_refuses_without_retry_or_output(tmp_path, monkeypatch):
    dataset = 'comment_attributes'
    source = tmp_path / 'source.parquet'
    pq.write_table(_table(dataset, [{'comment_id': 'one'}, {'comment_id': 'two'}]), source)
    pair = write_held_dataset(dataset, source, tmp_path / 'native', generation_id='g', bulk=True)
    original = regulations_bulk._plain_query
    monkeypatch.setattr(regulations_bulk, '_plain_query', lambda *a: original(*a).replace('ORDER BY _i', 'ORDER BY _i DESC'))
    from spicy_regs.transforms import regulations_receipts
    monkeypatch.setattr(regulations_receipts, '_qualified_rows', lambda *a, **k: pytest.fail('row retry after order fault'))
    destination = tmp_path / 'out.parquet'
    with pytest.raises(etl_bulk.BulkValidationError, match='source order or population'):
        materialize_internal(ReceiptInput(dataset, (pair[0],), pair[1], 'g'), destination, bulk=True)
    assert not destination.exists()


def test_a_formerly_inheriting_dataset_carries_instead_of_growing_history(tmp_path):
    """fr_docket_links once inherited every matched prior; refreshes now keep unchanged rows and do not accumulate."""
    from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS

    def row(number, title):
        values = dict.fromkeys(name for name, _ in SOURCE_COLUMNS['fr_docket_links'])
        return values | {'docket_id': f'EPA-HQ-OAR-2020-000{number}', 'docket_source_ordinal': 0,
                         'document_number': f'2020-0000{number}', 'publication_date': '2020-01-02', 'title': title}

    witness = {'source_id': 'retained', 'source_uri': None, 'sha256': 'a' * 64, 'locator': 'row', 'body_version': None}

    def write(name, generation, rows, priors=()):
        records = [(r, ReceiptContext(generation, str(i), 'test', [witness])) for i, r in enumerate(rows)]
        return write_records('fr_docket_links', records, tmp_path / name, prior_receipts=priors)

    def accepted(receipts):
        return {decode_exact_json(r['processing_json'])['raw_conversion_inputs']['title']: r
                for r in pq.read_table(receipts).to_pylist() if r['outcome'] == 'accepted'}

    receipts = write('v1', 'g1', [row(1, 'same'), row(2, 'v1')])[1]
    first = accepted(receipts)
    predecessor = first['v1']
    for version in ('v2', 'v3', 'v4'):
        receipts = write(version, version, [row(1, 'same'), row(2, version)], [receipts])[1]
        current = accepted(receipts)
        assert current['same'] == first['same']
        changed = decode_exact_json(current[version]['diagnostic_json'])
        assert changed['prior_receipt']['receipt_id'] == predecessor['receipt_id']
        assert current[version]['witnesses'] == [witness]
        predecessor = current[version]
