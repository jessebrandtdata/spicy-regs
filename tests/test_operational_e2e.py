"""Prepared H/D and qualification routes through the real `spicy-regs-run` CLI.

Heavy: each test builds a runtime inventory by observing the real phase code,
prepares a request, and runs it under an owner, guardian and payload. Run these
under the shared heavy-check lock.
"""

import json
from pathlib import Path
import subprocess
import sys

from spicy_regs import operational_runner as runner
from spicy_regs.sources import uscode_editions as builder, uscode_editions_audit as auditor
from tests.operational_helpers import ENV, FIRST_PARTY, REPO, cfr_capture, grant, inventory, observed_files
from tests.test_retained_population_qualification import selected_plan
from tests.test_uscode_editions_audit import fixture

SOURCES = "spicy_regs.operational_sources"


def cli(*args, timeout=900):
    return subprocess.run(
        [sys.executable, "-B", "-m", "spicy_regs.operational_runner", *map(str, args)],
        cwd=REPO,
        env=ENV,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def run_request(request_path):
    result = cli("run", "--request", request_path, "--grant", grant(request_path))
    root = Path(json.loads(Path(request_path).read_text())["run_root"])
    logs = "\n".join(path.read_text() for path in sorted(root.glob("*.log")))
    return result, runner.read_json(root / "result.json"), logs


def continuation(request_path, tmp_path, root, name):
    request = json.loads(Path(request_path).read_text())
    request.update(run_root=str(tmp_path / name), continue_from=runner.file_pin(root / "build-completion.json"))
    path = tmp_path / (name + "-request.json")
    path.write_text(json.dumps(request))
    return path


def observe_sources(tmp_path, argvs):
    """Inventory of everything the adapter commands open or import, observed under the payload's launcher flags."""
    code = f"""
import runpy, sys
for argv in {argvs!r}:
    sys.argv = [{SOURCES!r}, *argv]
    try:
        runpy.run_module({SOURCES!r}, run_name='__main__', alter_sys=True)
    except SystemExit as stop:
        assert stop.code in (0, None), stop.code
"""
    files = observed_files(code, [REPO / "src"], tmp_path / "observe")
    path = tmp_path / "runtime-inventory.json"
    path.write_text(json.dumps(inventory(files, [tmp_path, *FIRST_PARTY])))
    return path


def prepare(tmp_path, kind, historical, inventory_path, run_root):
    request = tmp_path / (run_root + "-request.json")
    result = cli(
        "prepare",
        kind,
        "--preparation",
        historical,
        "--runtime-inventory",
        inventory_path,
        "--python",
        sys.executable,
        "--target",
        REPO / "src",
        "--run-root",
        tmp_path / run_root,
        "--output",
        request,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return request


def test_prepared_h_annual_runs_offline_and_continues(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    _, selection, _ = fixture(source, year=1994, build_candidate=False)
    original = source / "original.zip"
    reference = tmp_path / "reference"
    builder.build(selection, reference)
    expected = auditor.audit(reference, selection, annual_year=1994)
    conservation = {
        "expected_rows": expected["rows"],
        "expected_content_categories": expected["content_categories"],
        "expected_complete_members": expected["complete_original_members"],
    }
    pins = builder._code_pins()
    generation = builder._identity("uscode-editions-public-v2", builder._digest(selection.read_bytes()), pins)
    observation = tmp_path / "observe-source.json"
    observation.write_text(
        json.dumps(
            {
                "selection": runner.file_pin(selection),
                "original_input": runner.file_pin(original),
                "expected_reader_pins": pins,
                "expected_generation": generation,
                "annual_year": 1994,
                "conservation": conservation,
            }
        )
    )
    candidate, output = tmp_path / "observed-candidate", tmp_path / "observed-audit.json"
    inventory_path = observe_sources(
        tmp_path,
        [
            ["h-build", "--config", str(observation), "--candidate", str(candidate)],
            ["h-audit", "--config", str(observation), "--candidate", str(candidate), "--output", str(output)],
        ],
    )
    historical = tmp_path / "historical-request.json"
    historical.write_text(
        json.dumps(
            {
                "format": "H-maintained-annual1994-source-replay-request-v1",
                "selection": runner.file_pin(selection),
                "original_input": {**runner.file_pin(original), "year": 1994},
                "conservation": conservation,
                "metadata_pins": [],
                "expected_reader_pins": pins,
                "expected_generation": generation,
                "resource_request": {
                    "wall_seconds": 300,
                    "sample_interval_seconds": 0.1,
                    "process_tree_rss_bytes": 4 * 1024**3,
                    "owned_output_scratch_bytes": 1024**3,
                    "admission_free_bytes": 1,
                    "stop_free_bytes": 1,
                },
            }
        )
    )
    request_path = prepare(tmp_path, "h-annual", historical, inventory_path, "h-run1")
    request = json.loads(request_path.read_text())
    assert request["offline_single_process"] is True
    result, record, logs = run_request(request_path)
    assert result.returncode == 0 and record["state"] == "built-and-audited-awaiting-review", (
        result.stderr + json.dumps(record) + logs
    )
    root = Path(request["run_root"])
    assert record["phases_launched"] == ["build", "audit"]
    assert runner.read_json(root / "output/audit.json")["verdict"] == "APPROVE"
    # Continue from the sealed build: the audit alone reads the previous run's candidate under the offline guard.
    before = runner.census(root)
    second = continuation(request_path, tmp_path, root, "h-run2")
    result, record, logs = run_request(second)
    fresh = Path(json.loads(second.read_text())["run_root"])
    assert result.returncode == 0 and record["state"] == "built-and-audited-awaiting-review", (
        result.stderr + json.dumps(record) + logs
    )
    assert record["phases_launched"] == ["audit"] and not (fresh / "build-launch.json").exists()
    assert runner.read_json(fresh / "output/audit.json")["verdict"] == "APPROVE"
    assert runner.census(root) == before


def test_prepared_d_cfr_runs_preflight_then_semantic_offline(tmp_path):
    config = cfr_capture(tmp_path / "capture", lambda i: b'<CFRDOC><X a="1">%d<X/>tail</X></CFRDOC>' % i)
    capture = tmp_path / "capture-config.json"
    capture.write_text(json.dumps(config))
    pin = runner.file_pin(capture)
    observation = tmp_path / "observe-source.json"
    observation.write_text(json.dumps({"capture_config": pin}))
    candidate, output = tmp_path / "observed-candidate", tmp_path / "observed-audit.json"
    inventory_path = observe_sources(
        tmp_path,
        [
            ["d-preflight", "--config", str(observation), "--candidate", str(candidate)],
            ["d-semantic", "--config", str(observation), "--candidate", str(candidate), "--output", str(output)],
        ],
    )
    assert json.loads(output.read_text())["status"] == "native-observations-and-comparisons-complete"
    historical = tmp_path / "preflight-config.json"
    historical.write_text(
        json.dumps(
            {
                "phase": "preflight",
                "capture_config": pin,
                "limits": {
                    "start_free_bytes": 1,
                    "stop_free_bytes": 1,
                    "rss_bytes": 4 * 1024**3,
                    "wall_seconds": 300,
                    "aggregate_bytes": 256 * 1024**2,
                    "scratch_bytes": 64 * 1024**2,
                    "output_bytes": 64 * 1024**2,
                    "evidence_bytes": 64 * 1024**2,
                    "interval_seconds": 0.1,
                },
            }
        )
    )
    request_path = prepare(tmp_path, "d-cfr", historical, inventory_path, "d-run1")
    request = json.loads(request_path.read_text())
    result, record, logs = run_request(request_path)
    assert result.returncode == 0 and record["state"] == "built-and-audited-awaiting-review", (
        result.stderr + json.dumps(record) + logs
    )
    root = Path(request["run_root"])
    assert runner.read_json(root / "candidate/preflight.json")["state"] == "tiny-maintained-preflight-passed"
    audit = runner.read_json(root / "output/audit.json")
    assert audit["status"] == "native-observations-and-comparisons-complete" and len(audit["observations"]) == 6
    # One grant covered preflight then semantic; the semantic phase used this run's own sealed preflight.
    assert record["phases_launched"] == ["build", "audit"]


def test_court_qualification_under_runner_then_explicit_continuation(tmp_path, installed_httpfs):
    plan, _, source = selected_plan(tmp_path, "court_opinions")
    code = f"""
from pathlib import Path
from scripts import qualify_retained_population as q
q.build_candidates(Path({str(plan)!r}), Path({str(tmp_path / "observed/candidate")!r}), fixture=True)
q.audit_candidates(Path({str(plan)!r}), Path({str(tmp_path / "observed/candidate")!r}),
                   Path({str(tmp_path / "observed/audit")!r}), fixture=True,
                   httpfs_extension=Path({str(installed_httpfs)!r}))
"""
    (tmp_path / "observed").mkdir()
    files = observed_files(code, [REPO / "src", REPO], tmp_path / "observe")
    runtime = [*inventory(files, [tmp_path, *FIRST_PARTY])["files"], runner.file_pin(Path(sys.executable).resolve())]
    runtime.append(runner.file_pin(installed_httpfs))
    first_party = [
        runner.file_pin(path)
        for base in (REPO / "src/spicy_regs", REPO / "scripts")
        for path in sorted(base.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
    ]
    base_paths = json.loads(
        subprocess.check_output(
            [
                sys.executable,
                "-I",
                "-B",
                "-c",
                "import json,sys; from pathlib import Path; print(json.dumps([str(Path(p).resolve()) for p in sys.path]))",
            ],
            text=True,
        )
    )
    module = "scripts.qualify_retained_population"
    request = {
        "version": 1,
        "run_root": str(tmp_path / "q-run1"),
        "python": sys.executable,
        "inputs": [runner.file_pin(plan), runner.file_pin(source)],
        "code": first_party,
        "runtime": runtime,
        "activation": {
            "prefix": str(Path(sys.prefix).resolve()),
            "paths": [str(REPO / "src"), str(REPO)],
            "base_paths": base_paths,
            "origins": {"spicy_regs": str(REPO / "src/spicy_regs/__init__.py")},
        },
        "offline_single_process": False,
        "limits": {
            "wall_seconds": 1200,
            "interval_seconds": 0.5,
            "rss_bytes": 16 * 1024**3,
            "output_bytes": 4 * 1024**3,
            "start_free_bytes": 1,
            "stop_free_bytes": 1,
        },
        "commands": {
            "build": {"module": module, "args": [str(plan), "{candidate}", "--fixture", "--stage", "build"]},
            "audit": {
                "module": module,
                "args": [
                    str(plan),
                    "{run}/output/audit",
                    "--fixture",
                    "--stage",
                    "audit",
                    "--candidate",
                    "{candidate}",
                    "--httpfs-extension",
                    str(installed_httpfs),
                ],
            },
        },
    }
    request_path = tmp_path / "q-run1-request.json"
    request_path.write_text(json.dumps(request))
    result, record, logs = run_request(request_path)
    assert result.returncode == 0 and record["state"] == "built-and-audited-awaiting-review", (
        result.stderr + json.dumps(record) + logs
    )
    root = Path(request["run_root"])
    completion = runner.read_json(root / "build-completion.json")
    links = sorted(row["path"] for row in completion["files"] if "link" in row)
    assert links == ["current/court_opinions.parquet", "selected-prior/court_opinions.parquet"]
    assert runner.read_json(root / "output/audit/RESULT.json")["status"] == "passed"
    before = runner.census(root)
    second = continuation(request_path, tmp_path, root, "q-run2")
    result, record, logs = run_request(second)
    fresh = Path(json.loads(second.read_text())["run_root"])
    assert result.returncode == 0 and record["state"] == "built-and-audited-awaiting-review", (
        result.stderr + json.dumps(record) + logs
    )
    assert record["phases_launched"] == ["audit"] and not (fresh / "build-launch.json").exists()
    assert runner.read_json(fresh / "output/audit/RESULT.json")["status"] == "passed"
    assert runner.census(root) == before
    pinned = runner.file_pin(installed_httpfs)
    assert pinned in request["runtime"]
    for completed in (root, fresh):
        for cache in ("output/audit/index/scratch/duckdb-extensions", "output/audit/mcp-home"):
            extensions = list((completed / cache).rglob("httpfs.duckdb_extension"))
            assert len(extensions) == 1
            assert runner.file_pin(extensions[0])["sha256"] == pinned["sha256"]
