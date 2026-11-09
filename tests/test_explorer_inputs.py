"""The scheduled explorer refresh skips only when the revision and every recorded input match exactly."""

import json
import re
from pathlib import Path

import pytest
import yaml

from spicy_regs.explorer_inputs import FORMAT, input_digest, unchanged

ROOT = Path(__file__).resolve().parents[1]
REVISION = "b" * 40
STORE = {"publication.v2.json": b'{"index": 1}', "materialized/rulemaking/latest.json": b'{"snapshot_id": "s1"}',
         "comments-publication.json": None, "navigation-measurements.v1.json": b'{"results": []}'}


def _live(store=STORE, *, revision=REVISION, **changes):
    value = {"format": FORMAT, "version": 1, "generatedAt": "2026-11-09T00:00:00Z", "sourceRevision": revision,
             "inputs": {key: input_digest(body) for key, body in store.items()}, "tables": {}}
    return json.dumps(value | changes).encode()


def _reader(store):
    def read(key):
        value = store[key]
        if isinstance(value, Exception):
            raise value
        return value
    return read


def test_exact_revision_and_inputs_skip_with_an_unchanged_line():
    same, reason = unchanged(_live(), revision=REVISION, read=_reader(STORE))
    assert same and reason.startswith("unchanged:")


@pytest.mark.parametrize(("store", "previous", "revision", "why"), [
    (STORE | {"publication.v2.json": b'{"index": 2}'}, None, REVISION, "publication.v2.json changed"),
    (STORE | {"comments-publication.json": b"{}"}, None, REVISION, "comments-publication.json changed"),
    (STORE | {"navigation-measurements.v1.json": None}, None, REVISION, "navigation-measurements.v1.json changed"),
    (STORE | {"publication.v2.json": OSError("timeout")}, None, REVISION, "could not be read"),
    (STORE, None, "c" * 40, "revision differs"),
    (STORE, _live(inputs={}), REVISION, "records no inputs"),
    (STORE, _live(inputs=None), REVISION, "records no inputs"),
    (STORE, _live(version=2), REVISION, "another format"),
    (STORE, b"not json", REVISION, "unreadable"),
    (STORE, b"", REVISION, "absent"),
    (STORE, None, "", "revision differs"),
])
def test_any_change_absence_or_doubt_runs_the_full_refresh(store, previous, revision, why):
    same, reason = unchanged(_live() if previous is None else previous, revision=revision, read=_reader(store))
    assert not same and why in reason


def test_the_workflow_runs_the_check_before_any_install_and_gates_everything_after_it():
    steps = yaml.safe_load((ROOT / ".github/workflows/publish-explorer-metadata.yml").read_text())["jobs"]["publish"]["steps"]
    names = [step.get("name") or step.get("uses") for step in steps]
    quick = names.index("Exit early when no input changed")
    check = steps[quick]
    assert check["continue-on-error"] is True and check["if"] == "github.event_name != 'workflow_dispatch'"
    assert "scripts/explorer_metadata_unchanged.py" in check["run"]
    assert all(step.get("if") == "steps.quick.outputs.unchanged != 'true'" for step in steps[quick + 1:])
    assert all("uv sync" not in step.get("run", "") for step in steps[:quick])
    # The check's boto3 is the locked one.
    pin = re.search(r"boto3==([\d.]+)", check["run"])
    assert pin is not None
    pinned = pin.group(1)
    assert f'name = "boto3"\nversion = "{pinned}"' in (ROOT / "uv.lock").read_text()
