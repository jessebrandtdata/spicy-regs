"""Refuse changed selected source bytes before any conversion or publication."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import qualify_votes_workflow as workflow
from scripts import retained_qualification_support as support


@pytest.mark.parametrize('key', ['codeHead', 'dirty', 'implementationId', 'qualificationScriptsSha256',
                               'spicyDocsCodeSha256', 'rulespecCodeSha256', 'lockSha256', 'packages'])
def test_final_code_drift_refuses_with_retained_phase(tmp_path, monkeypatch, key):
    frozen = workflow.capture_code_pins()
    changed = dict(frozen, **{key: 'changed'})
    monkeypatch.setattr(support, 'capture_code_pins', lambda: changed)
    log = tmp_path / 'phases.jsonl'
    with pytest.raises(ValueError, match=key):
        workflow.verify_code_pins(log, frozen)
    phases = [json.loads(line) for line in log.read_text().splitlines()]
    assert phases[-1]['phase'] == 'final-code-and-dependency-pins'
    assert phases[-1]['status'] == 'failed'
    assert phases[-1][key] == 'changed'


def test_unchanged_code_pins_pass(tmp_path):
    log = tmp_path / 'phases.jsonl'
    workflow.verify_code_pins(log, workflow.capture_code_pins())
    assert json.loads(log.read_text().splitlines()[-1])['status'] == 'passed'


def test_wrong_original_pin_retains_failure_and_never_starts_private_publication(tmp_path):
    plan: dict[str, Any] = {'sources': []}
    before = {}
    for dataset in ('member_votes', 'member_vote_terms', 'roll_call_votes'):
        source = tmp_path / (dataset + '.parquet')
        pq.write_table(pa.table({'vote_id': ['v']}), source)
        raw = source.read_bytes()
        before[source] = raw
        entry = {'dataset': dataset, 'localInput': str(source), 'bytes': len(raw), 'rows': 1,
                 'sha256': 'sha256:' + hashlib.sha256(raw).hexdigest()}
        if dataset == 'roll_call_votes':
            plan['completeFamilySibling'] = entry
        else:
            plan['sources'].append(entry)
    plan['sources'][0]['sha256'] = 'sha256:' + '0' * 64
    pin = tmp_path / 'plan.json'
    pin.write_text(json.dumps(plan))
    output = tmp_path / 'output'
    result = subprocess.run([sys.executable, '-m', 'scripts.qualify_votes_workflow', str(pin), str(output),
                             '--fixture'], cwd=Path(__file__).resolve().parent.parent,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    phases = [json.loads(line) for line in (output / 'phases.jsonl').read_text().splitlines()]
    assert phases[-1]['phase'] == 'all-original-source-pins' and phases[-1]['status'] == 'failed'
    assert (output / 'plan-pinned.json').read_bytes() == pin.read_bytes()
    assert not (output / 'member_votes').exists()
    assert not (output / 'private-store').exists()
    assert {path: path.read_bytes() for path in before} == before
