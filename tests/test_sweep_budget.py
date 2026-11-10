"""A scheduled sweep stops starting batches in time to exit cleanly and publish (ss-ag8i).

Run 42674437011 (2026-11-09) was killed at the hosted 360-minute limit inside its second batch, so the
consumers that wait for the sweep published nothing. A fake clock stands in for the batches' durations.
"""

from pathlib import Path

import pytest
import yaml

import spicy_regs.pipelines.regulations as regulations
from spicy_regs.manifest import Manifest
from spicy_regs.pipelines import RegulationsPipeline

WORKFLOW = Path(__file__).parents[1] / ".github/workflows/etl-new-pipeline.yml"
MINUTE = 60.0


@pytest.fixture
def sweep(tmp_path, monkeypatch):
    """Run a five-batch sweep whose manifest load and batches take the given minutes on a fake clock."""
    now = [0.0]
    monkeypatch.setattr(regulations, "monotonic", lambda: now[0])
    monkeypatch.setenv("AGENCIES", "A,B,C,D,E")

    def run(minutes, *, budget, load=5.0):
        started = []

        def load_manifest(*args, **kwargs):
            now[0] += load * MINUTE
            return Manifest.empty()

        def batch(self, *, manifest):
            started.append(self.batch_number)
            now[0] += minutes[self.batch_number] * MINUTE

        monkeypatch.setattr(Manifest, "load", load_manifest)
        monkeypatch.setattr(RegulationsPipeline, "run", batch)
        pipeline = RegulationsPipeline(output_dir=tmp_path, batch_count=5, use_iceberg=True)
        stopped = pipeline.run_sweep(budget_minutes=budget)
        assert pipeline.batch_number is None
        marker = tmp_path / regulations.SWEEP_STOPPED_NAME
        return stopped, started, marker.read_text() if marker.exists() else None, now[0] / MINUTE

    return run


def test_sweep_stops_before_the_batch_its_longest_batch_says_will_not_fit(sweep):
    # Starts at 5, 55 and 155 minutes. At 225 one more batch is estimated at the longest so far, 100: past 300.
    stopped, started, marker, ended = sweep([50, 100, 70, 10, 10], budget=300)
    assert (stopped, started, marker, ended) == (3, [0, 1, 2], "3\n", 225)


def test_the_estimate_is_never_below_the_floor(sweep):
    assert regulations.SWEEP_BATCH_FLOOR_SECONDS == 60 * MINUTE
    # One-minute batches: the fifth would start at 9 minutes, and 9 + 60 is past 68.
    assert sweep([1, 1, 1, 1, 1], budget=68)[:3] == (4, [0, 1, 2, 3], "4\n")
    assert sweep([1, 1, 1, 1, 1], budget=69)[:3] == (None, [0, 1, 2, 3, 4], None)


def test_a_budget_too_small_for_one_batch_starts_none(sweep):
    assert sweep([1, 1, 1, 1, 1], budget=64)[:3] == (0, [], "0\n")


def test_a_sweep_without_a_budget_runs_every_batch_and_clears_an_old_marker(sweep, tmp_path):
    (tmp_path / regulations.SWEEP_STOPPED_NAME).write_text("2\n")
    assert sweep([200, 200, 200, 200, 200], budget=None)[:3] == (None, [0, 1, 2, 3, 4], None)


def test_a_failed_batch_still_stops_the_sweep_with_no_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENCIES", "A,B,C")
    monkeypatch.setattr(Manifest, "load", lambda *a, **kw: Manifest.empty())

    def fail(self, *, manifest):
        raise OSError("checkpoint publication failed")

    monkeypatch.setattr(RegulationsPipeline, "run", fail)
    with pytest.raises(OSError):
        RegulationsPipeline(output_dir=tmp_path, batch_count=3, use_iceberg=True).run_sweep(budget_minutes=300)
    assert not (tmp_path / regulations.SWEEP_STOPPED_NAME).exists()


def test_cli_passes_the_budget_to_the_sweep_and_refuses_it_elsewhere(monkeypatch):
    seen = {}

    class Pipeline:
        def __init__(self, **kwargs):
            pass

        def run_sweep(self, *, budget_minutes):
            seen["budget"] = budget_minutes

        def run(self):
            seen["ran"] = True

    monkeypatch.setattr(regulations, "RegulationsPipeline", Pipeline)
    monkeypatch.setattr(regulations, "load_dotenv", lambda *a, **k: None)
    with pytest.raises(SystemExit) as exited:
        regulations.app(["--sweep", "--batch-count", "15", "--sweep-budget-minutes", "300", "--skip-upload"])
    assert exited.value.code == 0 and seen == {"budget": 300}
    with pytest.raises(ValueError, match="only to --sweep"):
        regulations.main(sweep_budget_minutes=300)
    assert "ran" not in seen


def setup_outputs(tmp_path, *, event, schedule="", gh="", batch="", timeout=""):
    """Run the setup step's shell as written, with ``gh`` replaced by a script; its outputs and the calls it made."""
    import subprocess

    workflow = yaml.safe_load(WORKFLOW.read_text())
    bin_dir, calls, output = tmp_path / "bin", tmp_path / "gh-calls", tmp_path / "github_output"
    bin_dir.mkdir()
    (bin_dir / "gh").write_text(f'#!/bin/sh\necho "$*" >> "{calls}"\n{gh}\n')
    (bin_dir / "gh").chmod(0o755)
    output.touch()
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin", "EVENT": event, "SCHEDULE": schedule, "GH_REPO": "owner/repo",
        "BATCH_NUMBER": batch, "TIMEOUT_MINUTES": timeout, "SKIP_UPLOAD": "false",
        "BATCH_COUNT": str(workflow["env"]["BATCH_COUNT"]), "GITHUB_OUTPUT": str(output),
    }
    [step] = workflow["jobs"]["setup"]["steps"]
    result = subprocess.run(["bash", "-c", step["run"]], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return dict(line.split("=", 1) for line in output.read_text().splitlines()), calls.read_text() if calls.exists() else ""


def test_only_a_scheduled_sweep_gets_a_budget_and_it_ends_an_hour_inside_the_limit(tmp_path):
    (tmp_path / "a").mkdir(), (tmp_path / "b").mkdir()
    scheduled, _ = setup_outputs(tmp_path / "a", event="schedule", schedule="25 6 * * *")
    assert (scheduled["timeout_minutes"], scheduled["sweep_budget_minutes"], scheduled["batch"]) == ("360", "300", "all")
    manual, _ = setup_outputs(tmp_path / "b", event="workflow_dispatch", batch="all", timeout="240")
    assert manual["sweep_budget_minutes"] == ""

    workflow = yaml.safe_load(WORKFLOW.read_text())
    assert workflow["jobs"]["setup"]["outputs"]["sweep_budget_minutes"] == "${{ steps.gen.outputs.sweep_budget_minutes }}"
    steps = workflow["jobs"]["etl-new"]["steps"]
    run = next(step for step in steps if step.get("name") == "Run new pipeline")
    assert run["env"]["SWEEP_BUDGET_MINUTES"] == "${{ needs.setup.outputs.sweep_budget_minutes }}"
    assert 'ARGS+=(--sweep-budget-minutes "$SWEEP_BUDGET_MINUTES")' in run["run"]
    _command, bound, *_ = regulations.app.parse_args(["--sweep", "--batch-count", "15", "--sweep-budget-minutes", "300"])
    assert bound.arguments["sweep_budget_minutes"] == 300


def test_a_stopped_sweep_leaves_an_artifact_and_still_lets_the_consumers_publish():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["etl-new"]["steps"]
    names = [step.get("name") for step in steps]
    stopped = next(step for step in steps if step.get("name") == "Record a sweep that stopped at its budget")
    assert names.index("Record a sweep that stopped at its budget") > names.index("Run new pipeline")
    assert stopped["if"] == f"hashFiles('output/{regulations.SWEEP_STOPPED_NAME}') != ''"
    assert stopped["with"] == {"name": "sweep-stopped", "path": f"output/{regulations.SWEEP_STOPPED_NAME}",
                               "retention-days": 7}
    # The stop is a clean exit, so the job succeeds and the consumers that need it run.
    assert workflow["jobs"]["refresh-consumers"]["needs"] == ["setup", "etl-new"]


@pytest.mark.parametrize(
    ("schedule", "succeeded", "stopped", "run"),
    [
        ("25 6 * * *", [11], {11: 0}, "true"),  # the daily sweep never asks
        ("25 18 * * *", [], {}, "true"),  # no scheduled sweep succeeded today
        ("25 18 * * *", [11], {11: 0}, "false"),  # one covered every batch
        ("25 18 * * *", [11], {11: 1}, "true"),  # the only success stopped at its budget: batches remain
        ("25 18 * * *", [11, 12], {11: 1, 12: 0}, "false"),
    ],
)
def test_retry_sweep_is_skipped_only_after_a_sweep_that_covered_every_batch(
    tmp_path, schedule, succeeded, stopped, run
):
    """The 18:25 sweep re-lists everything, so it exits once a scheduled sweep has covered every batch today."""
    workflow = yaml.safe_load(WORKFLOW.read_text())
    assert workflow["jobs"]["etl-new"]["if"] == "${{ needs.setup.outputs.run == 'true' }}"
    assert workflow["jobs"]["setup"]["permissions"] == {"actions": "read"}
    listed = "\\n".join(map(str, succeeded))
    cases = " ".join(f"*runs/{run_id}/artifacts*) echo {count};;" for run_id, count in stopped.items())
    gh = f'case "$*" in "run list"*) printf "{listed}";; {cases} *) exit 1;; esac'
    outputs, calls = setup_outputs(tmp_path, event="schedule", schedule=schedule, gh=gh)
    assert outputs["run"] == run
    if schedule == "25 6 * * *":
        assert not calls
        return
    assert "--event schedule --status success" in calls
    covered = next((index for index, run_id in enumerate(succeeded) if stopped[run_id] == 0), len(succeeded))
    asked = [run_id for run_id in succeeded if f"repos/owner/repo/actions/runs/{run_id}/artifacts" in calls]
    assert asked == succeeded[: covered + 1]  # each success is asked until one covered every batch
