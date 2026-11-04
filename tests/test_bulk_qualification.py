"""Qualification must fail if a private source snapshot changes after pinning."""
import importlib.util
import json
import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.congress_subjects import INPUT_COLUMNS


def test_full_qualifier_rejects_source_changed_after_pin(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location('qualifier', Path(__file__).parents[1] / 'scripts/qualify_congress_bulk.py')
    assert spec is not None and spec.loader is not None
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    source = tmp_path / 'source.parquet'
    schema = pa.schema([(name, pa.string()) for name in INPUT_COLUMNS['member_votes']])
    pq.write_table(pa.Table.from_pylist([{'vote_id':'101-house-1-1', 'member_key':'M', 'congress':'101', 'chamber':'house'}], schema=schema), source)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    original = source.read_bytes()
    real = runner.write_congress_dataset
    def changed(path, *args, **kwargs):
        rows = pq.read_table(path).to_pylist()
        rows[0]['member_name'] = 'changed after pin'
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        return real(path, *args, **kwargs)
    monkeypatch.setattr(runner, 'write_congress_dataset', changed)
    output = tmp_path / 'qualification'
    monkeypatch.setattr('sys.argv', ['qualifier',str(source),str(output),'--dataset','member_votes','--sha256',digest,'--generation','g'])
    with pytest.raises(ValueError, match='Pinned snapshot changed'):
        runner.main()
    assert source.read_bytes() == original
    phases = [json.loads(line) for line in (output / 'phases.jsonl').read_text().splitlines()]
    assert phases[-1]['phase'] == 'final-input-pin' and phases[-1]['status'] == 'failed'
    assert not (output / 'LIMITATIONS.json').exists()


@pytest.mark.parametrize("threads", [None, 2])
def test_tiny_qualifier_reports_default_and_overridden_query_threads(tmp_path, monkeypatch, threads):
    spec = importlib.util.spec_from_file_location('qualifier', Path(__file__).parents[1] / 'scripts/qualify_congress_bulk.py')
    assert spec is not None and spec.loader is not None
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    source = tmp_path / 'source.parquet'
    schema = pa.schema([(name, pa.string()) for name in INPUT_COLUMNS['member_votes']])
    pq.write_table(pa.Table.from_pylist([
        {'vote_id': '101-house-1-1', 'member_key': 'M', 'congress': '101', 'chamber': 'house'}
    ], schema=schema), source)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    output = tmp_path / 'qualification'
    arguments = ['qualifier', str(source), str(output), '--dataset', 'member_votes', '--sha256', digest,
                 '--generation', 'g']
    if threads is not None:
        arguments.extend(['--history-query-threads', str(threads)])
    monkeypatch.setattr('sys.argv', arguments)
    runner.main()
    phases = [json.loads(line) for line in (output / 'phases.jsonl').read_text().splitlines()]
    history = next(phase for phase in phases if phase['phase'] == 'unchanged-history-full')
    assert history['status'] == 'passed'
    assert history['historyQueryThreads'] == (1 if threads is None else threads)
    assert runner.compare_files(output / 'selected-prior/etl_receipts.parquet', output / 'carried-receipts.parquet') == 2
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest


@pytest.mark.parametrize("threads", ['0', '-1', 'invalid', str(2**63)])
def test_qualifier_invalid_threads_refuse_before_creating_output(tmp_path, monkeypatch, threads):
    spec = importlib.util.spec_from_file_location('qualifier', Path(__file__).parents[1] / 'scripts/qualify_congress_bulk.py')
    assert spec is not None and spec.loader is not None
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    output = tmp_path / 'not-created'
    monkeypatch.setattr('sys.argv', [
        'qualifier', str(tmp_path / 'missing-source'), str(output), '--dataset', 'member_votes',
        '--sha256', 'a' * 64, '--generation', 'g', '--history-query-threads', threads,
    ])
    with pytest.raises(SystemExit) as error:
        runner.main()
    assert error.value.code == 2
    assert not output.exists()
