"""Fork readers share one selected host; hosted checks require an explicit one."""

import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import MagicMock

import pytest
import yaml

from spicy_regs import cli, generate_analytics, mcp_server
from spicy_regs.public_url import DEFAULT_R2_BASE_URL, resolve_r2_base_url


@pytest.mark.parametrize(
    ("reader", "publisher", "expected"),
    [
        (None, None, DEFAULT_R2_BASE_URL),
        ("", "", DEFAULT_R2_BASE_URL),
        (None, "https://fork.example/", "https://fork.example"),
        ("", "https://fork.example", "https://fork.example"),
        ("https://reader.example/", "https://publisher.example", "https://reader.example"),
    ],
)
def test_resolver_precedence_and_mcp_fallback(monkeypatch, reader, publisher, expected):
    for name, value in (("SPICY_REGS_R2_URL", reader), ("R2_PUBLIC_URL", publisher)):
        if value is not None:
            monkeypatch.setenv(name, value)
    assert resolve_r2_base_url() == expected
    assert mcp_server._resolve_r2_base_url() == expected
    assert resolve_r2_base_url("https://explicit.example/") == "https://explicit.example"


@pytest.mark.parametrize("url", ["http://fork.example", "https://fork.example/'); DROP", "https://fork.example/\n"])
def test_publisher_url_gets_same_validation(monkeypatch, url):
    monkeypatch.setenv("R2_PUBLIC_URL", url)
    with pytest.raises(RuntimeError):
        resolve_r2_base_url()


def test_cli_reads_url_after_loading_dotenv(tmp_path, monkeypatch):
    # Inject dotenv's effect without opening any local secret file.
    monkeypatch.setattr(cli, "load_dotenv", lambda: monkeypatch.setenv("R2_PUBLIC_URL", "https://fork.example/"))
    from spicy_regs.sources import publication

    bases = []
    monkeypatch.setattr(publication, "load_index", lambda base: bases.append(base) or publication.empty_index())
    captured = []

    def download(name, output_dir, force=False, *, base_url=None):
        captured.append((name, base_url))
        return output_dir / f"{name}.parquet"

    monkeypatch.setattr(cli, "download_file", download)
    monkeypatch.setattr(sys, "argv", ["spicy-regs", "--output-dir", str(tmp_path), "download", "--types", "dockets"])
    cli.main()
    assert bases == ["https://fork.example"]
    assert captured == [("dockets", "https://fork.example")]


def test_analytics_remote_queries_use_selected_host(tmp_path, monkeypatch):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fork.example/")
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = []
    monkeypatch.setattr(generate_analytics.duckdb, "connect", lambda: connection)
    outputs = generate_analytics.generate_analytics(output_dir=tmp_path)
    queries = "\n".join(call.args[0] for call in connection.execute.call_args_list)
    for name in ("comments", "documents", "dockets"):
        assert f"https://fork.example/{name}.parquet" in queries
    assert DEFAULT_R2_BASE_URL not in queries
    assert outputs and all(path.is_file() for path in outputs.values())


def test_freshness_uses_publisher_url_and_explicit_argument(tmp_path, monkeypatch):
    from scripts import check_rollup_freshness
    from spicy_regs.sources import publication

    monkeypatch.setenv("R2_PUBLIC_URL", "https://fork.example/")
    snapshots = []
    monkeypatch.setattr(publication, "load_index", lambda base: snapshots.append(base) or publication.empty_index())
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = []
    monkeypatch.setattr(check_rollup_freshness.duckdb, "connect", lambda: connection)
    args = ["check", "--state-file", str(tmp_path / "state.json")]
    for extra, expected in (
        ([], "https://fork.example"),
        (["--base-url", "https://explicit.example"], "https://explicit.example"),
    ):
        monkeypatch.setattr(sys, "argv", args + extra)
        assert check_rollup_freshness.main() == 0
        query = connection.execute.call_args.args[0]
        assert expected in query and DEFAULT_R2_BASE_URL not in query
        assert snapshots[-1] == expected
    assert len(snapshots) == 2


@pytest.mark.parametrize(
    ("workflow", "step_name"),
    [
        ("check-comments-freshness.yml", "Check comments freshness"),
        ("check-rollup-freshness.yml", "Check published tables"),
        ("deploy-docs.yml", "Verify every active published schema"),
        ("_comments-mirror.yml", "Publish comments mirror from the catalog"),
        ("_comments-mirror.yml", "Capture published base versions for dependent jobs"),
        ("_regulations-refresh.yml", "Confirm dependent jobs used unchanged base versions"),
        ("_regulations-refresh.yml", "Bind public comments and the catalog to the publication receipt"),
    ],
)
def test_workflow_refuses_missing_public_url_before_any_reader(tmp_path, workflow, step_name):
    root = Path(__file__).resolve().parents[1]
    config = yaml.safe_load((root / ".github" / "workflows" / workflow).read_text())
    step = next(step for job in config["jobs"].values() for step in job.get("steps", ()) if step.get("name") == step_name)
    assert step["env"]["R2_PUBLIC_URL"] == (
        "${{ secrets.R2_PUBLIC_URL || format('https://data.{0}', vars.SPICYREGS_DOMAIN || 'spicy-regs.dev') }}"
    )
    # Execute the actual guard and first read boundary; no GitHub expressions,
    # tools or network are evaluated. A missing URL must prevent reaching it.
    guard = step["run"].splitlines()[0]
    script = guard + "\nprintf 'READER_REACHED'\n"
    for value in (None, "", "https://fork.example"):
        env = {"PATH": os.defpath}
        if value is not None:
            env["R2_PUBLIC_URL"] = value
        result = subprocess.run(
            ["/bin/bash", "-e", "-c", script], env=env, cwd=tmp_path, capture_output=True, text=True
        )
        assert (result.returncode == 0) == bool(value)
        assert ("READER_REACHED" in result.stdout) == bool(value)
    if workflow == "deploy-docs.yml":
        assert '--base "$R2_PUBLIC_URL"' in step["run"]


def test_mcp_connection_without_etl_dependencies(tmp_path):
    import duckdb

    # Real local Parquet plus a fresh Python process: no already-imported
    # botocore module can hide the minimal container's missing dependency.
    connection = duckdb.connect()
    connection.execute(
        "COPY (SELECT 'fork-docket' AS docket_id) TO ? (FORMAT PARQUET)", [str(tmp_path / "dockets.parquet")]
    )
    connection.close()
    script = """
import importlib.abc
import sys
class MinimalReader(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'boto3', 'botocore', 'spicy_docs', 'rulespec_artifacts', 'polars', 'pyarrow'}:
            raise ImportError('ETL dependency unavailable: ' + fullname)
sys.meta_path.insert(0, MinimalReader())
from spicy_regs import mcp_server
from spicy_regs.sources.publication import Member, empty_index, parse_index, table_members
import json
assert parse_index(json.dumps(empty_index()).encode()) == empty_index()
assert table_members(empty_index(), 'dockets.parquet') == (Member('dockets.parquet', None, None, None),)
con = mcp_server._build_connection()
assert con.execute('SELECT docket_id FROM dockets').fetchall() == [('fork-docket',)]
con.close()
print('MINIMAL_READER_OK')
"""
    env = dict(os.environ, SPICY_REGS_DATA_DIR=str(tmp_path), R2_PUBLIC_URL="https://fork.example")
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert "MINIMAL_READER_OK" in result.stdout
