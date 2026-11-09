"""A pinned local member is selected by row group, with exactly the full scan's rows, order and refusals."""
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, select_pinned_receipts, select_receipts
from .test_ranged_receipt_selection import batches_read, rows
from .test_receipt_selection_batch import observed

DATASETS = ('bill_actions', 'congress_bills', 'section_diffs', 'legislative_document_file_states')


def member(path, order, **options):
    table = pa.Table.from_pylist([row | {'receipt_id': f'{row["receipt_id"]}-{n}'} for n, row in enumerate(order)],
                                 schema=RECEIPT_SCHEMA)
    pq.write_table(table, path, **options)
    return path


def same_as_full_scan(path, tmp_path, dataset):
    expected = select_receipts(path, tmp_path / f'full-{dataset}.parquet', dataset=dataset)
    actual = select_pinned_receipts(path, tmp_path / f'pinned-{dataset}.parquet', dataset=dataset)
    assert pq.read_table(actual).equals(pq.read_table(expected), check_metadata=True)
    return pq.read_table(actual)


@pytest.mark.parametrize('seed', range(6))
@pytest.mark.parametrize('group', [1, 3, 7, 50])
def test_interleaved_and_straddling_datasets_match_the_full_scan(tmp_path, seed, group):
    # Clustered runs whose boundaries fall inside groups, then a shuffled tail that interleaves every dataset.
    order = rows(DATASETS[:3], each=6)
    tail = rows(DATASETS, each=4)
    random.Random(seed).shuffle(tail)
    path = member(tmp_path / 'member.parquet', order + tail, row_group_size=group)
    for dataset in (*DATASETS, 'absent', 'aaa', 'zzz'):
        selected = same_as_full_scan(path, tmp_path, dataset)
        assert set(selected['dataset'].to_pylist()) <= {dataset}


def test_reads_only_the_groups_that_can_hold_the_dataset(tmp_path, monkeypatch):
    path = member(tmp_path / 'member.parquet', rows(DATASETS[:3], each=5), row_group_size=4)  # 0-3 .. 12-14
    reads = batches_read(monkeypatch)
    assert same_as_full_scan(path, tmp_path, 'congress_bills').num_rows == 5
    # The full scan reads every group; the pinned selection reads the two that straddle congress_bills' rows.
    assert reads == [None, [1, 2]]


@pytest.mark.parametrize('statistics', [False, ['receipt_id']])
def test_groups_without_dataset_statistics_are_read(tmp_path, monkeypatch, statistics):
    path = member(tmp_path / 'member.parquet', rows(DATASETS[:3], each=5), row_group_size=5,
                  write_statistics=statistics)
    reads = batches_read(monkeypatch)
    for dataset in ('section_diffs', 'absent'):
        same_as_full_scan(path, tmp_path, dataset)
    assert reads[1] == reads[3] == [0, 1, 2]


def test_a_group_of_null_datasets_has_no_bounds_and_is_read(tmp_path, monkeypatch):
    nulls = [row | {'dataset': None} for row in rows(['x'], each=3)]
    path = member(tmp_path / 'member.parquet', rows(['congress_bills'], each=3) + nulls, row_group_size=3)
    reads = batches_read(monkeypatch)
    assert same_as_full_scan(path, tmp_path, 'section_diffs').num_rows == 0
    assert reads[1] == [1]


def test_another_schema_or_unreadable_member_keeps_the_full_selector_outcome(tmp_path):
    extra = tmp_path / 'extra.parquet'
    pq.write_table(pa.Table.from_pylist([r | {'extra': 'x'} for r in rows(['congress_bills'])],
                                        schema=RECEIPT_SCHEMA.append(pa.field('extra', pa.string()))), extra)
    broken = tmp_path / 'broken.parquet'
    broken.write_bytes(b'not parquet')
    for path in (extra, broken):
        expected = observed(lambda: select_receipts(path, tmp_path / 'full.parquet', dataset='congress_bills'))
        actual = observed(lambda: select_pinned_receipts(path, tmp_path / 'pinned.parquet', dataset='congress_bills'))
        assert actual == expected
