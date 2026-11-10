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

    def run(minutes, *, budget, load=5.0, start=0):
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
        stopped = pipeline.run_sweep(budget_minutes=budget, start_batch=start)
        assert pipeline.batch_number is None
        marker = tmp_path / regulations.SWEEP_STOPPED_NAME
        return stopped, started, marker.read_text() if marker.exists() else None, now[0] / MINUTE

    return run


def test_sweep_stops_before_the_batch_its_longest_batch_says_will_not_fit(sweep):
    # Starts at 5, 55 and 155 minutes. At 225 one more batch is estimated at the longest so far, 100: past 300.
    stopped, started, marker, ended = sweep([50, 100, 70, 10, 10], budget=300)
    assert (stopped, started, marker, ended) == (3, [0, 1, 2], "3\n", 225)


def test_the_estimate_is_never_below_the_floor(sweep):
    assert regulations.SWEEP_BATCH_FLOOR_SECONDS == 80 * MINUTE  # the measured merging batch: 79.4 minutes
    # One-minute batches: the fifth would start at 9 minutes, and 9 + 80 is past 88.
    assert sweep([1, 1, 1, 1, 1], budget=88)[:3] == (4, [0, 1, 2, 3], "4\n")
    assert sweep([1, 1, 1, 1, 1], budget=89)[:3] == (None, [0, 1, 2, 3, 4], None)


def test_a_budget_too_small_for_one_batch_starts_none(sweep):
    assert sweep([1, 1, 1, 1, 1], budget=84)[:3] == (0, [], "0\n")


def test_a_sweep_without_a_budget_runs_every_batch_and_clears_an_old_marker(sweep, tmp_path):
    (tmp_path / regulations.SWEEP_STOPPED_NAME).write_text("2\n")
    assert sweep([200, 200, 200, 200, 200], budget=None)[:3] == (None, [0, 1, 2, 3, 4], None)


def test_a_resumed_sweep_begins_at_its_start_batch_and_wraps_to_the_first(sweep, tmp_path):
    assert sweep([1, 1, 1, 1, 1], budget=None, start=3)[:3] == (None, [3, 4, 0, 1, 2], None)
    # Starts at 5 and 105 minutes; at 205 one more 100-minute batch is past 300: batch 0 is next in the round.
    assert sweep([100, 100, 100, 100, 100], budget=300, start=3)[:3] == (0, [3, 4], "0\n")
    for outside in (-1, 5):
        with pytest.raises(ValueError, match="start_batch"):
            RegulationsPipeline(output_dir=tmp_path, batch_count=5, use_iceberg=True).run_sweep(start_batch=outside)


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

        def run_sweep(self, *, budget_minutes, start_batch):
            seen["sweep"] = (budget_minutes, start_batch)

        def run(self):
            seen["ran"] = True

    monkeypatch.setattr(regulations, "RegulationsPipeline", Pipeline)
    monkeypatch.setattr(regulations, "load_dotenv", lambda *a, **k: None)
    with pytest.raises(SystemExit) as exited:
        regulations.app(["--sweep", "--batch-count", "15", "--sweep-budget-minutes", "300", "--sweep-start-batch", "7",
                         "--skip-upload"])
    assert exited.value.code == 0 and seen == {"sweep": (300, 7)}
    with pytest.raises(ValueError, match="only to --sweep"):
        regulations.main(sweep_budget_minutes=300)
    with pytest.raises(ValueError, match="only to --sweep"):
        regulations.main(sweep_start_batch=7)
    assert "ran" not in seen


def setup_outputs(tmp_path, *, event, schedule="", batch="", timeout="", succeeded=(), stopped=None, latest="",
                  stopped_before="", artifact_file="sweep-stopped.txt"):
    """Run the setup step's shell as written against a stub ``gh``; its outputs and the calls it made.

    ``succeeded``: today's successful scheduled runs. ``stopped``: sweep-stopped artifacts per run id, or
    ``"fail"`` for a question ``gh`` cannot answer. ``latest``: the latest completed scheduled run, or ``"fail"``
    for a list that fails. Its sweep-stopped artifact holds ``artifact_file`` naming batch ``stopped_before``;
    with none, the download exits 1 as ``gh`` does.
    """
    import subprocess

    workflow = yaml.safe_load(WORKFLOW.read_text())
    bin_dir, calls, output = tmp_path / "bin", tmp_path / "gh-calls", tmp_path / "github_output"
    bin_dir.mkdir()
    artifacts = " ".join(f"*runs/{run_id}/artifacts*) {'exit 1' if count == 'fail' else f'echo {count}'};;"
                         for run_id, count in (stopped or {}).items())
    listed = "".join(f"{run_id}\\n" for run_id in succeeded)
    (bin_dir / "gh").write_text(f"""#!/bin/sh
echo "$*" >> "{calls}"
case "$*" in
  *"--status success"*) printf "{listed}";;
  *"--status completed"*) {'echo partial; exit 1' if latest == "fail" else f'echo "{latest}"'};;
  {artifacts}
  "run download"*) [ -n "{stopped_before}" ] || exit 1
    while [ "$1" != "--dir" ]; do shift; done; echo "{stopped_before}" > "$2/{artifact_file}";;
  *) exit 1;;
esac
""")
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


def test_only_a_scheduled_sweep_gets_a_budget_and_its_last_batch_may_run_twice_the_floor(tmp_path):
    (tmp_path / "a").mkdir(), (tmp_path / "b").mkdir()
    scheduled, _ = setup_outputs(tmp_path / "a", event="schedule", schedule="25 6 * * *")
    assert (scheduled["timeout_minutes"], scheduled["sweep_budget_minutes"], scheduled["batch"]) == ("360", "270", "all")
    limit, budget, floor = 360, int(scheduled["sweep_budget_minutes"]), regulations.SWEEP_BATCH_FLOOR_SECONDS / 60
    other_steps = 10  # setup before the sweep, the warning and uploads after it
    assert limit - other_steps - (budget - floor) >= 2 * floor  # the last batch to start has twice the floor
    manual, calls = setup_outputs(tmp_path / "b", event="workflow_dispatch", batch="all", timeout="240")
    assert (manual["sweep_budget_minutes"], manual["sweep_start_batch"], calls) == ("", "", "")

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
                               "retention-days": 7, "overwrite": True}
    # The stop is a clean exit, so the job succeeds and the consumers that need it run.
    assert workflow["jobs"]["refresh-consumers"]["needs"] == ["setup", "etl-new"]


@pytest.mark.parametrize("stopped_before", ["2", None])
def test_a_stopped_sweep_is_a_warning_on_the_run_page(tmp_path, stopped_before):
    """A budget stop concludes "success"; the lines after the pipeline command, run as written, announce it."""
    import subprocess

    workflow = yaml.safe_load(WORKFLOW.read_text())
    run = next(step for step in workflow["jobs"]["etl-new"]["steps"] if step.get("name") == "Run new pipeline")
    command = 'uv run run-pipeline "${ARGS[@]}"\n'
    assert run["run"].count(command) == 1
    after = "set -euo pipefail\n" + run["run"].split(command)[1]
    (tmp_path / "output").mkdir()
    if stopped_before is not None:
        (tmp_path / "output" / regulations.SWEEP_STOPPED_NAME).write_text(f"{stopped_before}\n")
    result = subprocess.run(["bash", "-c", after], cwd=tmp_path, capture_output=True, text=True,
                            env={"PATH": "/usr/bin:/bin", "BATCH_COUNT": "15"})
    assert result.returncode == 0, result.stderr
    if stopped_before is None:
        assert result.stdout == ""
    else:
        assert result.stdout.startswith("::warning title=Sweep stopped at its time budget::Stopped before batch 2 ")
        assert "(0-indexed) of 15" in result.stdout and result.stdout.count("\n") == 1


@pytest.mark.parametrize(
    ("schedule", "succeeded", "stopped", "run"),
    [
        ("25 6 * * *", [11], {11: 0}, "true"),  # the daily sweep never asks
        ("25 18 * * *", [], {}, "true"),  # no scheduled sweep succeeded today
        ("25 18 * * *", [11], {11: 0}, "false"),  # one covered every batch
        ("25 18 * * *", [11], {11: 1}, "true"),  # the only success stopped at its budget: batches remain
        ("25 18 * * *", [11, 12], {11: 1, 12: 0}, "false"),
        ("25 18 * * *", [11], {11: "fail"}, "true"),  # an unanswered question does not cancel the retry
    ],
)
def test_retry_sweep_is_skipped_only_after_a_sweep_that_covered_every_batch(
    tmp_path, schedule, succeeded, stopped, run
):
    """The 18:25 sweep re-lists everything, so it exits once a scheduled sweep has covered every batch today."""
    workflow = yaml.safe_load(WORKFLOW.read_text())
    assert workflow["jobs"]["etl-new"]["if"] == "${{ needs.setup.outputs.run == 'true' }}"
    assert workflow["jobs"]["setup"]["permissions"] == {"actions": "read"}
    outputs, calls = setup_outputs(tmp_path, event="schedule", schedule=schedule, succeeded=succeeded, stopped=stopped)
    assert outputs["run"] == run
    if schedule == "25 6 * * *":
        assert "--status success" not in calls
        return
    assert "--event schedule --status success" in calls
    covered = next((index for index, run_id in enumerate(succeeded) if stopped[run_id] == 0), len(succeeded))
    assert outputs["sweep_start_batch"] == ("0" if run == "true" else "")
    asked = [run_id for run_id in succeeded if f"repos/owner/repo/actions/runs/{run_id}/artifacts" in calls]
    assert asked == succeeded[: covered + 1]  # each success is asked until one covered every batch


def test_only_the_comment_attributes_family_gets_more_than_thirty_minutes():
    """Its per-row receipt rewrite is estimated at about 121 minutes on 26.4M rows; the other four are small."""
    refresh = yaml.safe_load((WORKFLOW.parent / "_regulations-refresh.yml").read_text())
    families = refresh["jobs"]["base-families"]
    assert "run-rollup-comment-attributes" in families["strategy"]["matrix"]["command"]
    assert families["with"]["timeout_minutes"] == (
        "${{ matrix.command == 'run-rollup-comment-attributes' && 180 || 30 }}"
    )
    rollup = yaml.safe_load((WORKFLOW.parent / "_rollup.yml").read_text())
    assert rollup[True]["workflow_call"]["inputs"]["timeout_minutes"]["type"] == "number"
    assert rollup["jobs"]["rollup"]["timeout-minutes"] == "${{ inputs.timeout_minutes }}"
    # Three copies of its 4.2 GiB receipt member do not fit the 14 GB a hosted runner guarantees.
    assert families["with"]["free_disk"] == "${{ matrix.command == 'run-rollup-comment-attributes' }}"
    assert rollup[True]["workflow_call"]["inputs"]["free_disk"]["type"] == "boolean"
    freeing = [step for step in rollup["jobs"]["rollup"]["steps"] if "inputs.free_disk" in str(step.get("if"))]
    assert freeing and "sudo rm -rf" in freeing[0]["run"]


@pytest.mark.parametrize(
    ("latest", "stopped_before", "artifact_file", "start"),
    [
        ("", "", "sweep-stopped.txt", "0"),  # no earlier scheduled sweep
        # The latest covered every batch, failed, was killed or was a skipped retry: no artifact, and the
        # download exits 1. Run 42674437011, the latest completed scheduled run on 2026-11-10, is this case.
        ("77", "", "sweep-stopped.txt", "0"),
        ("77", "7", "sweep-stopped.txt", "7"),  # the latest stopped before batch 7
        ("77", "15", "sweep-stopped.txt", "0"),  # an artifact naming no batch begins at the first
        ("77", "x", "sweep-stopped.txt", "0"),
        ("77", "7", "another-name.txt", "0"),  # an artifact without the expected file
        ("fail", "7", "sweep-stopped.txt", "0"),  # the run list fails, having printed something
    ],
)
def test_a_scheduled_sweep_begins_where_the_latest_one_stopped_and_no_lookup_failure_stops_it(
    tmp_path, latest, stopped_before, artifact_file, start
):
    """Sweeps that always began at the first batch would never reach the later ones (ss-ag8i).

    ``setup_outputs`` asserts the step exits 0: whatever the lookup meets, the daily sweep still starts.
    """
    outputs, calls = setup_outputs(tmp_path, event="schedule", schedule="25 6 * * *", latest=latest,
                                   stopped_before=stopped_before, artifact_file=artifact_file)
    assert (outputs["run"], outputs["sweep_start_batch"], outputs["batch"]) == ("true", start, "all")
    assert "--event schedule --status completed --limit 1" in calls
    assert ("run download" in calls) is (latest == "77") and ("run download 77 --name sweep-stopped" in calls) is (
        latest == "77"
    )

    workflow = yaml.safe_load(WORKFLOW.read_text())
    run = next(step for step in workflow["jobs"]["etl-new"]["steps"] if step.get("name") == "Run new pipeline")
    assert run["env"]["SWEEP_START_BATCH"] == "${{ needs.setup.outputs.sweep_start_batch }}"
    assert 'ARGS+=(--sweep-start-batch "$SWEEP_START_BATCH")' in run["run"]


def test_a_skipped_retry_asks_for_no_start_batch(tmp_path):
    outputs, calls = setup_outputs(tmp_path, event="schedule", schedule="25 18 * * *", succeeded=[11], stopped={11: 0})
    assert (outputs["run"], outputs["sweep_start_batch"]) == ("false", "")
    assert "--status completed" not in calls
