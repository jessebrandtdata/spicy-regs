"""Publication uses a pinned main revision only after its complete push CI passes."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/publish-explorer-metadata.yml"
MAIN_SHA = "a" * 40


def _workflow():
    return yaml.safe_load(WORKFLOW.read_text())


def _run(*, sha=MAIN_SHA, status="completed", conclusion="success", number=1, event="push", branch="main"):
    return {
        "id": number,
        "run_number": number,
        "run_attempt": 1,
        "head_sha": sha,
        "head_branch": branch,
        "event": event,
        "status": status,
        "conclusion": conclusion,
    }


def _preflight(runs):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to execute the GitHub Actions preflight script")
    script = _workflow()["jobs"]["preflight"]["steps"][0]["with"]["script"]
    # Run the actual workflow script with a small API adapter; nothing contacts GitHub.
    harness = r"""
const payload = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const outputs = {}, messages = [], requests = [];
const context = {repo: {owner: 'example', repo: 'spicy-regs'}};
const core = {
  setOutput: (key, value) => { outputs[key] = value; },
  notice: message => messages.push(message),
  info: message => messages.push(message)
};
const github = {rest: {
  git: {getRef: async request => {
    requests.push(request);
    return {data: {object: {sha: payload.sha}}};
  }},
  actions: {listWorkflowRuns: async request => {
    requests.push(request);
    return {data: {workflow_runs: payload.runs}};
  }}
}};
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
new AsyncFunction('github', 'context', 'core', payload.script)(github, context, core)
  .then(() => process.stdout.write(JSON.stringify({outputs, messages, requests})))
  .catch(error => { process.stderr.write(String(error)); process.exitCode = 1; });
"""
    result = subprocess.run(
        [node, "-e", harness],
        input=json.dumps({"sha": MAIN_SHA, "runs": runs, "script": script}),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("runs", [
    [],
    [_run(status="queued", conclusion=None)],
    [_run(status="in_progress", conclusion=None)],
    [_run(conclusion="failure")],
    [_run(conclusion="cancelled")],
    [_run(sha="b" * 40)],
    [_run(event="pull_request")],
    [_run(branch="feature")],
    [_run(number=1), _run(number=2, conclusion="failure")],
    [_run(number=1), _run(number=2, status="in_progress", conclusion=None)],
])
def test_publication_waits_for_successful_push_ci_for_exact_main(runs):
    result = _preflight(runs)
    assert result["outputs"] == {"sha": MAIN_SHA, "publish": "false"}
    assert "Skipping publication" in result["messages"][0]


def test_success_publishes_the_exact_verified_main_revision():
    result = _preflight([_run(number=2), _run(number=1, conclusion="failure")])
    assert result["outputs"] == {"sha": MAIN_SHA, "publish": "true"}
    assert result["requests"] == [
        {"owner": "example", "repo": "spicy-regs", "ref": "heads/main"},
        {
            "owner": "example", "repo": "spicy-regs", "workflow_id": "ci.yml",
            "branch": "main", "event": "push", "head_sha": MAIN_SHA, "per_page": 100,
        },
    ]


def test_an_hourly_schedule_is_the_one_automatic_trigger_and_shares_the_gate():
    """Hourly catches hosted and workstation publishes and CI-passed main revisions alike; no list to maintain."""
    workflow = _workflow()
    # PyYAML's YAML 1.1 parser treats the unquoted Actions key "on" as True.
    triggers = workflow[True]
    assert triggers.keys() == {"schedule", "workflow_dispatch"}
    assert triggers["schedule"] == [{"cron": "7 * * * *"}]
    preflight = workflow["jobs"]["preflight"]
    assert "if" not in preflight
    assert preflight["permissions"] == {"contents": "read", "actions": "read"}
    publish = workflow["jobs"]["publish"]
    assert publish["needs"] == "preflight"
    assert publish["if"] == "needs.preflight.outputs.publish == 'true'"
    assert publish["steps"][0]["with"]["ref"] == "${{ needs.preflight.outputs.sha }}"
