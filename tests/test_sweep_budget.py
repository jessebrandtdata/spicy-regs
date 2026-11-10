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
from spicy_regs.pipelines.mirrulations_originals import RetentionHalted

WORKFLOW = Path(__file__).parents[1] / ".github/workflows/etl-new-pipeline.yml"
MINUTE = 60.0


@pytest.fixture
def sweep(tmp_path, monkeypatch):
    """Run a five-batch sweep whose manifest load, first pass and batches take the given minutes on a fake clock.

    ``first`` is the pass over every agency's dockets and documents; ``started`` lists the comment batches. With
    ``first_error`` that pass raises it after ``first`` minutes, having recorded a key when ``first_records`` and
    saved it locally when ``first_saves``. A sweep that raises returns its error in place of the batch it stopped
    before.
    """
    now = [0.0]
    monkeypatch.setattr(regulations, "monotonic", lambda: now[0])
    monkeypatch.setenv("AGENCIES", "A,B,C,D,E")

    def run(minutes, *, budget, load=5.0, start=0, first=0.0, first_error=None, first_records=False, first_saves=False):
        started, firsts, begun = [], [], now[0]

        def load_manifest(*args, **kwargs):
            now[0] += load * MINUTE
            return Manifest.empty()

        def one_pass(self, *, manifest, record_types):
            names = [record_type.name for record_type in record_types]
            if self.batch_number is None:
                assert not started and names == ["dockets", "documents"] and self._agencies() == list("ABCDE")
                firsts.append(now[0])
                now[0] += first * MINUTE
                if first_records:
                    manifest.record(["raw-data/A/A-1/text-A-1/documents/A-1-0001.json"])
                if first_saves:  # the local save succeeds; the error stands for the refused upload after it
                    manifest.save(tmp_path)
                if first_error is not None:
                    raise first_error
                return
            assert firsts and names == ["comments"]
            started.append(self.batch_number)
            now[0] += minutes[self.batch_number] * MINUTE

        monkeypatch.setattr(Manifest, "load", load_manifest)
        monkeypatch.setattr(RegulationsPipeline, "run", one_pass)
        pipeline = RegulationsPipeline(output_dir=tmp_path, batch_count=5, use_iceberg=True)
        try:
            stopped = pipeline.run_sweep(budget_minutes=budget, start_batch=start)
        except Exception as error:
            stopped = error
        assert firsts == [begun + load * MINUTE]  # right after the manifest, before any batch
        assert (pipeline.batch_number, pipeline.skip_comments, pipeline.only_comments) == (None, False, False)
        marker = tmp_path / regulations.SWEEP_STOPPED_NAME
        return stopped, started, marker.read_text() if marker.exists() else None, now[0] / MINUTE

    return run


def test_sweep_stops_before_the_batch_its_longest_batch_says_will_not_fit(sweep):
    # Starts at 5, 55 and 155 minutes. At 225 one more batch is estimated at the longest so far, 100: past 300.
    stopped, started, marker, ended = sweep([50, 100, 70, 10, 10], budget=300)
    assert (stopped, started, marker, ended) == (3, [0, 1, 2], "3\n", 225)


def test_every_agencys_documents_come_first_and_spend_the_budget_without_raising_the_estimate(sweep):
    """On 2026-11-10 the documents of 12 of the 15 batches still ended on 10-03, waiting behind comment batches."""
    # The first pass runs 5-105. Batches start at 105, 155 and 205, each estimated at the 80-minute floor; the
    # 100-minute pass is not a batch, or the third would be estimated past 300. At 255, 255 + 80 is past 300.
    assert sweep([50, 50, 50, 50, 50], budget=300, first=100) == (3, [0, 1, 2], "3\n", 255)
    # A first pass that leaves less than the floor starts no batch; its documents are still committed.
    assert sweep([1, 1, 1, 1, 1], budget=300, first=220)[:3] == (0, [], "0\n")


def test_a_failed_first_pass_that_recorded_nothing_lets_the_comment_batches_run_then_fails(sweep, capsys):
    """One agency failing in the dockets and documents pass must not starve comments, and must stay visible."""
    refused = OSError("listing raw-data/FAA/ refused")
    # The pass fails at 105. Batches start at 105, 155 and 205 under the same budget; at 255 the sweep stops before
    # batch 3, writes its marker for the next sweep, and only then raises.
    error, started, marker, ended = sweep([50] * 5, budget=300, first=100, first_error=refused)
    assert (started, marker, ended) == ([0, 1, 2], "3\n", 255)
    assert isinstance(error, RuntimeError) and error.__cause__ is refused
    assert "::error title=Dockets and documents pass failed" in capsys.readouterr().out
    # Every batch ran: no marker, and the run still fails.
    error, started, marker, _ = sweep([1] * 5, budget=None, first_error=refused)
    assert (started, marker) == ([0, 1, 2, 3, 4], None) and error.__cause__ is refused


def test_a_failed_first_pass_that_recorded_keys_stops_the_sweep(sweep):
    """A later batch's manifest would retire the keys it recorded, so it fails like any batch."""
    refused = OSError("manifest upload refused")
    assert sweep([1] * 5, budget=300, first_error=refused, first_records=True)[:3] == (refused, [], None)
    # The realistic case: _run saved the manifest locally, then its upload was refused. The keys are no longer
    # pending, but a later batch's upload of that local file would still retire them.
    assert sweep([1] * 5, budget=300, first_error=refused, first_records=True, first_saves=True)[:3] == (
        refused, [], None)


def test_an_evidence_failure_in_the_first_pass_stops_the_sweep(sweep):
    """A refused commit of originals (SourceEvidenceError) is an evidence failure: never caught, like a halt."""
    from spicy_regs.source_evidence import SourceEvidenceError

    refused = SourceEvidenceError("Mirrulations original retention failed: ClientError publishing originals")
    assert sweep([1] * 5, budget=300, first_error=refused)[:3] == (refused, [], None)


def test_an_original_retention_halt_in_the_first_pass_stops_the_sweep(sweep):
    """No object is used without its kept original: a halted retention is never caught."""
    with pytest.raises(RetentionHalted):
        sweep([1] * 5, budget=300, first_error=RetentionHalted("R2 refused the evidence write"))


@pytest.mark.parametrize(
    ("scope", "passes"),
    [({}, [None, 0, 1]), ({"only_comments": True}, [0, 1]), ({"skip_comments": True}, [None])],
)
def test_only_comments_skips_the_first_pass_and_skip_comments_is_that_pass_alone(tmp_path, monkeypatch, scope,
                                                                                 passes):
    monkeypatch.setenv("AGENCIES", "A,B")
    monkeypatch.setattr(Manifest, "load", lambda *a, **kw: Manifest.empty())
    seen = []
    monkeypatch.setattr(RegulationsPipeline, "run", lambda self, *, manifest, record_types: seen.append(
        (self.batch_number, [record_type.name for record_type in record_types])))
    pipeline = RegulationsPipeline(output_dir=tmp_path, batch_count=2, use_iceberg=True, **scope)
    assert pipeline.run_sweep() is None
    assert seen == [(batch, ["dockets", "documents"] if batch is None else ["comments"]) for batch in passes]
    assert (pipeline.skip_comments, pipeline.only_comments) == (
        scope.get("skip_comments", False), scope.get("only_comments", False))
    # The chunked path stages no attribute rows, so comment batches never take it.
    with pytest.raises(ValueError, match="chunk_size"):
        RegulationsPipeline(output_dir=tmp_path, batch_count=2, use_iceberg=True, chunk_size=10).run_sweep()


def test_a_failed_first_pass_with_no_comment_batches_raises_its_own_error(tmp_path, monkeypatch):
    """With --skip-comments the pass is the whole sweep: nothing runs after it, so its error is not wrapped."""
    monkeypatch.setenv("AGENCIES", "A,B")
    monkeypatch.setattr(Manifest, "load", lambda *a, **kw: Manifest.empty())
    refused = OSError("listing raw-data/A/ refused")

    def fail(self, *, manifest, record_types):
        raise refused

    monkeypatch.setattr(RegulationsPipeline, "run", fail)
    with pytest.raises(OSError) as raised:
        RegulationsPipeline(output_dir=tmp_path, batch_count=2, skip_comments=True).run_sweep()
    assert raised.value is refused


def test_an_empty_list_of_record_types_is_not_every_type(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(RegulationsPipeline, "_run", lambda self, manifest, record_types: seen.append(record_types))
    RegulationsPipeline(output_dir=tmp_path, agency="A", skip_comments=True, enrich_text=False).run(
        manifest=Manifest.empty(), record_types=[])
    assert seen == [[]]


def test_skip_and_only_comments_together_are_refused():
    """The sweep ran no pass at all and exited 0 for this combination; a single run read no comments."""
    with pytest.raises(ValueError, match="exclude each other"):
        RegulationsPipeline(batch_count=2, use_iceberg=True, skip_comments=True, only_comments=True)


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
    passes = []

    def fail(self, *, manifest, record_types):
        passes.append(self.batch_number)
        raise OSError("checkpoint publication failed")

    monkeypatch.setattr(RegulationsPipeline, "run", fail)
    with pytest.raises(OSError):
        RegulationsPipeline(output_dir=tmp_path, batch_count=3, use_iceberg=True).run_sweep(budget_minutes=300)
    # The first pass recorded nothing, so the first comment batch ran; its failure ends the sweep.
    assert passes == [None, 0]
    assert not (tmp_path / regulations.SWEEP_STOPPED_NAME).exists()


def test_cli_passes_the_budget_to_the_sweep_and_refuses_it_elsewhere(monkeypatch):
    seen = {}

    class Pipeline:
        def __init__(self, **kwargs):
            pass

        def run_sweep(self, *, budget_minutes, start_batch, resume):
            seen["sweep"] = (budget_minutes, start_batch, resume)

        def run(self):
            seen["ran"] = True

    monkeypatch.setattr(regulations, "RegulationsPipeline", Pipeline)
    monkeypatch.setattr(regulations, "load_dotenv", lambda *a, **k: None)
    with pytest.raises(SystemExit) as exited:
        regulations.app(["--sweep", "--batch-count", "15", "--sweep-budget-minutes", "300", "--sweep-start-batch", "7",
                         "--sweep-resume", "--skip-upload"])
    assert exited.value.code == 0 and seen == {"sweep": (300, 7, True)}
    with pytest.raises(ValueError, match="only to --sweep"):
        regulations.main(sweep_budget_minutes=300)
    with pytest.raises(ValueError, match="only to --sweep"):
        regulations.main(sweep_start_batch=7)
    with pytest.raises(ValueError, match="only to --sweep"):
        regulations.main(sweep_resume=True)
    assert "ran" not in seen


def setup_outputs(tmp_path, *, event, schedule="", batch="", timeout="", succeeded=(), stopped=None, latest="",
                  stopped_before="", artifact_file="sweep-stopped.txt"):
    """Run the setup step's shell as written against a stub ``gh``; its outputs and the calls it made.

    ``succeeded``: today's successful scheduled runs, or ``"fail"`` for a list that fails. ``stopped``:
    sweep-stopped artifacts per run id, or ``"fail"`` for a question ``gh`` cannot answer. ``latest``: the latest
    completed scheduled run, or ``"fail"`` for a list that fails. Its sweep-stopped artifact holds
    ``artifact_file`` naming batch ``stopped_before``; with none, the download exits 1 as ``gh`` does.
    """
    import subprocess

    workflow = yaml.safe_load(WORKFLOW.read_text())
    bin_dir, calls, output = tmp_path / "bin", tmp_path / "gh-calls", tmp_path / "github_output"
    bin_dir.mkdir()
    artifacts = " ".join(f"*runs/{run_id}/artifacts*) {'exit 1' if count == 'fail' else f'echo {count}'};;"
                         for run_id, count in (stopped or {}).items())
    listed = ("echo partial; exit 1" if succeeded == "fail"
              else 'printf "' + "".join(f"{run_id}\\n" for run_id in succeeded) + '"')
    (bin_dir / "gh").write_text(f"""#!/bin/sh
echo "$*" >> "{calls}"
case "$*" in
  *"--status success"*) {listed};;
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
    # Only a scheduled (budgeted) sweep resumes from its R2 state.
    assert 'ARGS+=(--sweep-budget-minutes "$SWEEP_BUDGET_MINUTES" --sweep-resume)' in run["run"]
    _command, bound, *_ = regulations.app.parse_args(["--sweep", "--batch-count", "15", "--sweep-budget-minutes", "300"])
    assert bound.arguments["sweep_budget_minutes"] == 300


def test_a_stopped_sweep_leaves_an_artifact_and_still_lets_the_consumers_publish():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["etl-new"]["steps"]
    names = [step.get("name") for step in steps]
    stopped = next(step for step in steps if step.get("name") == "Record a sweep that stopped at its budget")
    assert names.index("Record a sweep that stopped at its budget") > names.index("Run new pipeline")
    # Also after a failed dockets and documents pass, which fails the job once the batches stopped at the budget.
    assert stopped["if"] == f"${{{{ !cancelled() && hashFiles('output/{regulations.SWEEP_STOPPED_NAME}') != '' }}}}"
    assert stopped["with"] == {"name": "sweep-stopped", "path": f"output/{regulations.SWEEP_STOPPED_NAME}",
                               "retention-days": 7, "overwrite": True}
    # The stop is a clean exit, so the job succeeds and the consumers that need it run; a failed job skips them.
    consumers = workflow["jobs"]["refresh-consumers"]
    assert consumers["needs"] == ["setup", "etl-new"] and "always()" not in consumers["if"]


@pytest.mark.parametrize("status", [0, 1])
@pytest.mark.parametrize("stopped_before", ["2", None])
def test_a_stopped_sweep_is_a_warning_on_the_run_page(tmp_path, stopped_before, status):
    """A budget stop is announced, also when a failed first pass then fails the step with the pipeline's status.

    The lines from the pipeline command on run as written, against a stub ``uv`` that exits with ``status``.
    """
    import subprocess

    workflow = yaml.safe_load(WORKFLOW.read_text())
    run = next(step for step in workflow["jobs"]["etl-new"]["steps"] if step.get("name") == "Run new pipeline")
    assert run["run"].count('uv run run-pipeline "${ARGS[@]}" || status=$?\n') == 1
    after = "set -euo pipefail\nARGS=(--sweep)\n" + run["run"][run["run"].index("status=0\n"):]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "uv").write_text(f"#!/bin/sh\nexit {status}\n")
    (bin_dir / "uv").chmod(0o755)
    (tmp_path / "output").mkdir()
    if stopped_before is not None:
        (tmp_path / "output" / regulations.SWEEP_STOPPED_NAME).write_text(f"{stopped_before}\n")
    result = subprocess.run(["bash", "-c", after], cwd=tmp_path, capture_output=True, text=True,
                            env={"PATH": f"{bin_dir}:/usr/bin:/bin", "BATCH_COUNT": "15"})
    assert result.returncode == status, result.stderr
    if stopped_before is None:
        assert result.stdout == ""
    else:
        assert result.stdout.startswith("::warning title=Sweep stopped at its time budget::Stopped before batch 2 ")
        assert "(0-indexed) of 15" in result.stdout and result.stdout.count("\n") == 1
        # A red run's consumers are skipped, so the line must not promise they publish.
        assert ("this run failed, so its consumers do not publish them" in result.stdout) is (status != 0)
        assert ("this run's consumers publish them" in result.stdout) is (status == 0)


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


def test_a_failed_list_of_todays_sweeps_does_not_cancel_the_retry(tmp_path):
    """An API error on the list exited the setup step, so the 18:25 retry never started (ss-ag8i)."""
    outputs, calls = setup_outputs(tmp_path, event="schedule", schedule="25 18 * * *", succeeded="fail")
    assert (outputs["run"], outputs["sweep_start_batch"]) == ("true", "0")
    assert "--event schedule --status success" in calls


def test_only_the_comment_attributes_family_gets_more_than_thirty_minutes():
    """Its per-row receipt rewrite is estimated at about 121 minutes on 26.4M rows; the other four are small."""
    refresh = yaml.safe_load((WORKFLOW.parent / "_regulations-refresh.yml").read_text())
    families = refresh["jobs"]["base-families"]
    assert "run-rollup-comment-attributes" in families["strategy"]["matrix"]["command"]
    assert families["with"]["timeout_minutes"] == (
        "${{ matrix.command == 'run-rollup-comment-attributes' && 180 || matrix.command == 'run-rollup-document-attributes' && 90 || 30 }}"
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


@pytest.fixture
def resumable(tmp_path, monkeypatch):
    """Sweeps over five one-agency batches that publish to a fake R2; ``kill`` makes that batch die as a hosted kill.

    Each call is a new run (a new pipeline and process), sharing only the fake R2. It returns the batches started
    and the R2 state left behind.
    """
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    monkeypatch.setenv("AGENCIES", "A,B,C,D,E")
    monkeypatch.setattr(Manifest, "load", lambda *a, **kw: Manifest.empty())

    class Killed(BaseException):
        """Stands in for the runner's kill: nothing after it runs, as no job step runs after a timeout."""

    def run(*, kill=None, start=0, budget=None):
        started = []

        def one_pass(self, *, manifest, record_types):
            if self.batch_number is None:
                return
            started.append(self.batch_number)
            if self.batch_number == kill:
                raise Killed

        monkeypatch.setattr(RegulationsPipeline, "run", one_pass)
        pipeline = RegulationsPipeline(output_dir=tmp_path, batch_count=5, use_iceberg=True, skip_upload=False)
        try:
            pipeline.run_sweep(budget_minutes=budget, start_batch=start, resume=True)
        except Killed:
            pass
        raw = remote.objects.get(regulations.SWEEP_STATE_KEY)
        import json

        return started, json.loads(raw) if raw else None

    class Sweeps:
        """Call to run one sweep; ``remote`` is the fake R2 every run shares."""

        def __init__(self) -> None:
            self.remote = remote

        def __call__(self, **kwargs):
            return run(**kwargs)

    return Sweeps()


def test_a_killed_run_resumes_at_the_batch_it_was_in(resumable):
    """Run 42875578221 died inside a batch and left no artifact: the next sweep would have begun at batch 0."""
    assert resumable(kill=2) == ([0, 1, 2], {"batch_count": 5, "next": 2, "starts": 1})
    # The artifact-derived start (0 here, as after a kill) is overridden by the state the killed process wrote.
    assert resumable(start=0) == ([2, 3, 4, 0, 1], {"batch_count": 5, "next": 2, "starts": 0})


def test_a_batch_that_never_finishes_is_skipped_once_and_reported(resumable, capsys):
    resumable(kill=2)
    assert resumable(kill=2) == ([2], {"batch_count": 5, "next": 2, "starts": 2})
    # Started by two runs in a row without finishing: this sweep begins after it, says so, and reaches it last.
    started, state = resumable(kill=2)
    assert started == [3, 4, 0, 1, 2] and state == {"batch_count": 5, "next": 2, "starts": 1}
    assert "::error title=Sweep batch skipped once::RuntimeError: batch 2 (0-indexed)" in capsys.readouterr().out


def test_a_budget_stop_does_not_count_as_a_start(resumable, monkeypatch):
    resumable(kill=3)
    # A sweep with no time for a batch starts none; the state still names batch 3, started once.
    assert resumable(budget=1) == ([], {"batch_count": 5, "next": 3, "starts": 1})
    assert resumable()[0] == [3, 4, 0, 1, 2]


def test_an_unreadable_state_falls_back_to_the_given_batch(resumable, monkeypatch):
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "get_r2_client", lambda: (_ for _ in ()).throw(OSError("R2 unavailable")))
    assert resumable(start=3) == ([3, 4, 0, 1, 2], None)


def test_a_sweep_that_does_not_publish_neither_reads_nor_writes_the_state(tmp_path):
    with pytest.raises(ValueError, match="drop --skip-upload"):
        RegulationsPipeline(output_dir=tmp_path, batch_count=5, use_iceberg=True).run_sweep(resume=True)


@pytest.mark.parametrize("held", [{"next": 3.0, "starts": 0}, {"next": True, "starts": 0}, {"next": "3", "starts": 0},
                                  {"next": 3, "starts": True}, {"next": 3, "starts": 1.0}])
def test_a_state_that_names_no_integer_batch_is_not_used(resumable, held):
    """A hand-edited 3.0 passed the range check, then range() raised after the first pass, on every run."""
    import json

    resumable.remote.objects[regulations.SWEEP_STATE_KEY] = json.dumps({"batch_count": 5, **held}).encode()
    # The given start is used instead (not 1, which true would also name), and the sweep writes a sound state.
    assert resumable(start=2) == ([2, 3, 4, 0, 1], {"batch_count": 5, "next": 2, "starts": 0})
