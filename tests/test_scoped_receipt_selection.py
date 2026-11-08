"""A Congress reader selects one dataset's receipts from a shared remote member by row group, not by whole download."""
import hashlib
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, select_receipts
from spicy_regs.selected_generations import SelectedInputs
from spicy_regs.sources import publication

PREFIX = 'generations/bills/' + 'b' * 64
DATASETS = ('congress_bills', 'bill_vote_references', 'section_diffs')


def receipts(path, rows_each=5):
    rows = [{name: f'{dataset}-{n}-{name}' for name in RECEIPT_SCHEMA.names if name != 'witnesses'}
            | {'dataset': dataset, 'witnesses': []} for dataset in DATASETS for n in range(rows_each)]
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path, row_group_size=rows_each)
    return path


def reader(tmp_path, monkeypatch, *, rows=None, scoped_reads=None):
    member = receipts(tmp_path / 'member.parquet')
    subject = tmp_path / 'subject.parquet'
    subject.write_bytes(b'subject')
    data, subject_bytes = member.read_bytes(), subject.read_bytes()

    def pin(raw, count):
        return {'sha256': 'sha256:' + hashlib.sha256(raw).hexdigest(), 'byteSize': len(raw), 'rows': count}

    index = publication.empty_index()
    index['families']['bills'] = {
        'prefix': PREFIX, 'tables': {'bill_vote_references.parquet': pin(subject_bytes, 1)},
        'etlReceipts': {'key': 'etl_receipts.parquet', **pin(data, len(DATASETS) * 5 if rows is None else rows),
                        'generationId': 'g1', 'datasets': list(DATASETS)},
    }
    whole = []

    def fetch(base_url, m, target, label=None, **kwargs):
        whole.append(m.path)
        target.write_bytes(subject_bytes if m.path.endswith('bill_vote_references.parquet') else data)
        return True

    monkeypatch.setattr(publication, 'fetch_member', fetch)

    @contextmanager
    def open_member(base_url, m):
        assert (base_url, m.path, m.byte_size) == ('https://fixture.invalid', f'{PREFIX}/etl_receipts.parquet', len(data))
        with member.open('rb') as stream:
            (scoped_reads if scoped_reads is not None else []).append(m.path)
            yield stream

    monkeypatch.setattr(publication, 'open_member', open_member)
    selected = SelectedInputs(tmp_path, tmp_path / 'selected', index=index, public_url='https://fixture.invalid')
    return selected, member, whole, index


def test_scoped_selection_reads_only_its_groups_and_keeps_rows_order_and_published_pin(tmp_path, monkeypatch):
    opened = []
    selected, member, whole, index = reader(tmp_path, monkeypatch, scoped_reads=opened)
    reads = []
    real = pq.ParquetFile.iter_batches

    def iter_batches(self, *args, row_groups=None, **kwargs):
        reads.append(row_groups)
        return real(self, *args, row_groups=row_groups, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, 'iter_batches', iter_batches)
    selection = selected.select('bill_vote_references', scoped=True)
    assert reads == [[1]] and len(opened) == 1
    assert whole == [f'{PREFIX}/bill_vote_references.parquet']  # the receipt member is never fetched whole
    expected = select_receipts(member, tmp_path / 'expected.parquet', dataset='bill_vote_references')
    assert pq.read_table(selection.receipts).equals(pq.read_table(expected))
    published = index['families']['bills']['etlReceipts']
    assert selected.receipts_pin('bill_vote_references', selection.receipts) == {
        'sha256': published['sha256'], 'byteSize': published['byteSize']}
    assert selected.select('bill_vote_references', scoped=True) == selection
    with pytest.raises(ValueError, match='for this dataset alone'):
        selected.select('bill_vote_references')


def test_unscoped_selection_still_fetches_the_member_whole(tmp_path, monkeypatch):
    selected, _, whole, index = reader(tmp_path, monkeypatch)
    selection = selected.select('bill_vote_references')
    assert f'{PREFIX}/etl_receipts.parquet' in whole
    assert selected.receipts_pin('bill_vote_references', selection.receipts)['sha256'] == (
        index['families']['bills']['etlReceipts']['sha256'])


def test_scoped_selection_refuses_a_member_whose_rows_differ_from_its_pin(tmp_path, monkeypatch):
    selected, _, _, _ = reader(tmp_path, monkeypatch, rows=14)
    with pytest.raises(ValueError, match='pinned row count'):
        selected.select('bill_vote_references', scoped=True)
    assert not selected.cache and not selected.scoped


def ranges(data, *, etag='"v1"', changed_after=None):
    import httpx
    asked = []

    def handle(request):
        tag = etag if changed_after is None or len(asked) < changed_after else '"v2"'
        if request.method == 'HEAD':
            return httpx.Response(200, headers={'ETag': tag, 'Content-Length': str(len(data))})
        asked.append(request.headers['Range'])
        start, end = map(int, request.headers['Range'].removeprefix('bytes=').split('-'))
        return httpx.Response(206, content=data[start:end + 1],
                              headers={'ETag': tag, 'Content-Range': f'bytes {start}-{end}/{len(data)}'})
    return httpx.Client(transport=httpx.MockTransport(handle)), asked


def test_published_ranges_ask_only_for_stated_lengths_and_refuse_a_changed_object():
    from spicy_regs.sources.remote_parquet import _PublishedRanges
    data = bytes(range(256)) * 4
    client, asked = ranges(data)
    reader = _PublishedRanges(client, 'https://fixture.invalid/m', byte_size=len(data))
    reader.seek(-10, 2)
    assert reader.read(100) == data[-10:] and asked == [f'bytes={len(data) - 10}-{len(data) - 1}']
    with pytest.raises(ValueError, match='pinned byte size'):
        _PublishedRanges(client, 'https://fixture.invalid/m', byte_size=len(data) + 1)
    client, _ = ranges(data, changed_after=1)
    reader = _PublishedRanges(client, 'https://fixture.invalid/m', byte_size=len(data))
    reader.read(4)
    with pytest.raises(ValueError, match='changed'):
        reader.read(4)
