"""A soft Congress input's receipts are read by row group from a shared remote member, not downloaded whole."""
import hashlib
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import selected_generations
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, dataset_row_groups, select_receipts
from spicy_regs.selected_generations import SelectedInputs
from spicy_regs.sources import publication

PREFIX = 'generations/bills/' + 'b' * 64
DATASETS = ('congress_bills', 'bill_vote_references', 'section_diffs')


def rows(datasets, each=5):
    return [{name: f'{dataset}-{n}-{name}' for name in RECEIPT_SCHEMA.names if name != 'witnesses'}
            | {'dataset': dataset, 'witnesses': []} for dataset in datasets for n in range(each)]


def receipts(path, *, row_group_size=5, **options):
    pq.write_table(pa.Table.from_pylist(rows(DATASETS), schema=RECEIPT_SCHEMA), path,
                   row_group_size=row_group_size, **options)
    return path


def reader(tmp_path, monkeypatch, *, count=None, share=1.0, key_index=False):
    monkeypatch.setattr(selected_generations, 'RANGED_MAX_SHARE', share)
    member = receipts(tmp_path / 'member.parquet')
    subject = tmp_path / 'subject.parquet'
    subject.write_bytes(b'subject')
    data, subject_bytes = member.read_bytes(), subject.read_bytes()

    def pin(raw, n):
        return {'sha256': 'sha256:' + hashlib.sha256(raw).hexdigest(), 'byteSize': len(raw), 'rows': n}

    index = publication.empty_index()
    index['families']['bills'] = {
        'prefix': PREFIX, 'tables': {f'{d}.parquet': pin(subject_bytes, 1) for d in DATASETS},
        'etlReceipts': {'key': 'etl_receipts.parquet', **pin(data, len(DATASETS) * 5 if count is None else count),
                        'generationId': 'g1', 'datasets': list(DATASETS),
                        **({'keyIndex': {'key': 'etl_receipts.keys.parquet', 'format': 'spicy-receipt-keys/1',
                                         **pin(b'keys', 1), 'receiptSha256': pin(data, 1)['sha256']}}
                           if key_index else {})},
    }
    log = {'whole': [], 'opened': [], 'read': []}

    def fetch(base_url, m, target, label=None, **kwargs):
        log['whole'].append(m.path)
        target.write_bytes(data if m.path.endswith('etl_receipts.parquet') else subject_bytes)
        return True

    @contextmanager
    def open_member(base_url, m):
        assert (base_url, m.path, m.byte_size) == ('https://fixture.invalid', f'{PREFIX}/etl_receipts.parquet', len(data))
        log['opened'].append(m.path)
        with member.open('rb') as stream:
            read = stream.read
            log['read'].append(0)

            def counted(n=-1):
                chunk = read(n)
                log['read'][-1] += len(chunk)
                return chunk
            stream.read = counted
            yield stream

    monkeypatch.setattr(publication, 'fetch_member', fetch)
    monkeypatch.setattr(publication, 'open_member', open_member)
    selected = SelectedInputs(tmp_path, tmp_path / 'selected', index=index, public_url='https://fixture.invalid')
    return selected, member, log, index


def batches_read(monkeypatch):
    reads, real = [], pq.ParquetFile.iter_batches

    def iter_batches(self, *args, row_groups=None, **kwargs):
        reads.append(row_groups)
        return real(self, *args, row_groups=row_groups, **kwargs)
    monkeypatch.setattr(pq.ParquetFile, 'iter_batches', iter_batches)
    return reads


RECEIPT_KEY = f'{PREFIX}/etl_receipts.parquet'


def test_ranged_selection_reads_only_its_groups_and_keeps_rows_order_and_published_pin(tmp_path, monkeypatch):
    selected, member, log, index = reader(tmp_path, monkeypatch)
    reads = batches_read(monkeypatch)
    selection = selected.select('bill_vote_references', ranged=True)
    assert reads == [[1]] and len(log['opened']) == 1 and RECEIPT_KEY not in log['whole']
    expected = select_receipts(member, tmp_path / 'expected.parquet', dataset='bill_vote_references')
    assert pq.read_table(selection.receipts).equals(pq.read_table(expected))
    published = index['families']['bills']['etlReceipts']
    assert selected.receipts_pin('bill_vote_references', selection.receipts) == {
        'sha256': published['sha256'], 'byteSize': published['byteSize']}
    assert selected.receipts_verification('bill_vote_references') == 'ranged'
    assert selected.select('bill_vote_references', ranged=True) == selection
    with pytest.raises(ValueError, match='for this dataset alone'):
        selected.select('bill_vote_references')
    with pytest.raises(ValueError, match='no single file'):
        selected.receipts_pin('bill_vote_references', (selection.receipts,))


def test_whole_selection_still_fetches_the_member_whole(tmp_path, monkeypatch):
    selected, _, log, index = reader(tmp_path, monkeypatch)
    selection = selected.select('bill_vote_references')
    assert RECEIPT_KEY in log['whole'] and not log['opened']
    assert selected.receipts_verification('bill_vote_references') is None
    assert selected.receipts_pin('bill_vote_references', selection.receipts)['sha256'] == (
        index['families']['bills']['etlReceipts']['sha256'])


def test_mixed_session_reuses_a_whole_member_and_parses_one_footer(tmp_path, monkeypatch):
    selected, _, log, _ = reader(tmp_path, monkeypatch)
    first = selected.select('congress_bills', ranged=True)
    second = selected.select('section_diffs', ranged=True)
    assert len(log['opened']) == 2 and len(selected.footers) == 1 and log['read'][1] < log['read'][0]
    assert first.receipts != second.receipts and RECEIPT_KEY not in log['whole']
    whole = selected.select('bill_vote_references')  # this caller needs the member itself
    assert log['whole'].count(RECEIPT_KEY) == 1 and len(log['opened']) == 2
    # Once the session holds the whole member, a ranged caller reads it rather than the network.
    selected.cache.pop('congress_bills')
    selected.ranged.pop('congress_bills')
    again = selected.select('congress_bills', ranged=True)
    assert again.receipts == whole.receipts and len(log['opened']) == 2 and 'congress_bills' not in selected.ranged


def test_large_share_falls_back_to_one_whole_download(tmp_path, monkeypatch):
    selected, _, log, _ = reader(tmp_path, monkeypatch, share=0.1)
    selection = selected.select('bill_vote_references', ranged=True)
    assert log['whole'].count(RECEIPT_KEY) == 1 and 'bill_vote_references' not in selected.ranged
    assert selected.receipts_pin('bill_vote_references', selection.receipts)['byteSize'] == selection.receipts.stat().st_size


def test_ranged_read_buffers_bounded_runs_of_groups(tmp_path, monkeypatch):
    monkeypatch.setattr(selected_generations, 'RANGED_CHUNK_BYTES', 1)
    selected, member, _, _ = reader(tmp_path, monkeypatch)
    pq.write_table(pa.Table.from_pylist(rows(['bill_vote_references'] * 3), schema=RECEIPT_SCHEMA), member,
                   row_group_size=5)
    selected.index['families']['bills']['etlReceipts'].update(rows=15)
    data = member.read_bytes()
    selected.index['families']['bills']['etlReceipts'].update(byteSize=len(data))
    monkeypatch.setattr(publication, 'open_member', contextmanager(lambda base, m: (yield member.open('rb'))))
    reads = batches_read(monkeypatch)
    selection = selected.select('bill_vote_references', ranged=True)
    assert reads == [[0], [1], [2]] and pq.read_metadata(selection.receipts).num_rows == 15


def test_ranged_selection_refuses_a_member_whose_rows_differ_from_its_pin(tmp_path, monkeypatch):
    selected, _, _, _ = reader(tmp_path, monkeypatch, count=14)
    with pytest.raises(ValueError, match='pinned row count'):
        selected.select('bill_vote_references', ranged=True)
    assert not selected.cache and not selected.ranged


def test_row_groups_keep_a_straddling_group_and_one_without_statistics(tmp_path):
    straddling = receipts(tmp_path / 'straddling.parquet', row_group_size=7)  # groups: 0-6, 7-13, 14
    with pq.ParquetFile(straddling) as parquet:
        assert dataset_row_groups(parquet, 'bill_vote_references') == [0, 1]
        assert dataset_row_groups(parquet, 'absent') == []
    bare = receipts(tmp_path / 'bare.parquet', write_statistics=False)
    with pq.ParquetFile(bare) as parquet:
        assert dataset_row_groups(parquet, 'absent') == [0, 1, 2]


def ranges(data, *, etag='"v1"', changed_after=None, failures=0):
    import httpx
    asked = []

    def handle(request):
        tag = etag if changed_after is None or len(asked) < changed_after else '"v2"'
        if request.method == 'HEAD':
            return httpx.Response(200, headers={'ETag': tag, 'Content-Length': str(len(data))})
        asked.append(request.headers['Range'])
        if len(asked) <= failures:
            return httpx.Response(503)
        start, end = map(int, request.headers['Range'].removeprefix('bytes=').split('-'))
        return httpx.Response(206, content=data[start:end + 1],
                              headers={'ETag': tag, 'Content-Range': f'bytes {start}-{end}/{len(data)}'})
    return httpx.Client(transport=httpx.MockTransport(handle)), asked


def test_published_ranges_ask_only_for_stated_lengths_and_refuse_a_changed_object():
    from spicy_regs.sources.remote_parquet import _PublishedRanges
    data = bytes(range(256)) * 4
    client, asked = ranges(data)
    ranged = _PublishedRanges(client, 'https://fixture.invalid/m', byte_size=len(data))
    ranged.seek(-10, 2)
    assert ranged.read(100) == data[-10:] and asked == [f'bytes={len(data) - 10}-{len(data) - 1}']
    with pytest.raises(ValueError, match='pinned byte size'):
        _PublishedRanges(client, 'https://fixture.invalid/m', byte_size=len(data) + 1)
    client, _ = ranges(data, changed_after=1)
    ranged = _PublishedRanges(client, 'https://fixture.invalid/m', byte_size=len(data))
    ranged.read(4)
    with pytest.raises(ValueError, match='changed'):
        ranged.read(4)


def test_published_ranges_retry_a_transient_failure_then_give_up(monkeypatch):
    import httpx
    from spicy_regs.sources import remote_parquet
    monkeypatch.setattr('time.sleep', lambda _: None)
    data = bytes(range(64))
    client, asked = ranges(data, failures=2)
    assert remote_parquet._PublishedRanges(client, 'https://fixture.invalid/m', byte_size=64).read(8) == data[:8]
    assert len(asked) == 3
    client, asked = ranges(data, failures=99)
    with pytest.raises(httpx.HTTPStatusError):
        remote_parquet._PublishedRanges(client, 'https://fixture.invalid/m', byte_size=64).read(8)
    assert len(asked) == remote_parquet.RANGE_ATTEMPTS


def test_only_another_familys_soft_inputs_are_ranged():
    from spicy_regs.pipelines.rollups.roll_call_votes import RollCallVotesRollup
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    rollup = RollCallVotesRollup.__new__(RollCallVotesRollup)
    rollup.output_dir = None
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as scratch:
        _, prior = rollup._new_receipt_work(Path(scratch))
    assert isinstance(prior, SelectedPriors) and prior.ranged == {'bill_vote_references'}


def test_published_ranges_refuse_a_full_answer_before_reading_its_body():
    import httpx
    from spicy_regs.sources.remote_parquet import _PublishedRanges

    def body():
        pytest.fail('the body of a non-range answer was read')
        yield b''

    def handle(request):
        if request.method == 'HEAD':
            return httpx.Response(200, headers={'ETag': '"v1"', 'Content-Length': '64'})
        return httpx.Response(200, content=body(), headers={'ETag': '"v1"'})
    client = httpx.Client(transport=httpx.MockTransport(handle))
    with pytest.raises(ValueError, match='answered 200 to a range request'):
        _PublishedRanges(client, 'https://fixture.invalid/m', byte_size=64).read(8)


@pytest.mark.parametrize('ranged', [True, False])
def test_pipeline_selection_never_fetches_or_checks_a_published_key_index(tmp_path, monkeypatch, ranged):
    selected, _, log, _ = reader(tmp_path, monkeypatch, key_index=True)
    selection = selected.select('bill_vote_references', ranged=ranged)
    assert not any(path.endswith('etl_receipts.keys.parquet') for path in log['whole'])
    assert selection.key_index is None and selection.key_index_descriptor is None
    assert ('bill_vote_references' in selected.ranged) == ranged and bool(log['opened']) == ranged
