"""PR checks read every changed join; scheduled checks still read the whole registry."""

from dataclasses import replace
import http.server
import json
import threading

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import check_table_joins as checker
from spicy_regs import table_joins


def join():
    return table_joins.Join("child", ("id",), "parent", ("id",), 1, 0, expected_cardinality="one")


def record(*joins):
    return {"format": table_joins.RECORD_FORMAT, "version": 1,
            "joins": [table_joins.record(j) for j in joins]}


@pytest.mark.parametrize("change", [
    {"baseline_keys": 2}, {"baseline_missing": 1}, {"kind": "scope", "reason": "Selected parent"},
    {"expected_cardinality": "unspecified"}, {"measured_via": "other_child"},
    {"measurement": {"source": "new receipt"}}, {"parent_columns": ("other_id",)},
])
def test_any_changed_declaration_or_baseline_is_selected(change):
    original = join()
    changed = replace(original, **change)
    selected, removed = checker.changed_joins([changed], record(original))
    assert selected == [changed]
    assert removed == ([] if changed.name == original.name else [original.name])


def test_additions_are_checked_and_removals_are_reported():
    original, added = join(), replace(join(), child="another_child")
    assert checker.changed_joins([original, added], record(original)) == ([added], [])
    assert checker.changed_joins([added], record(original)) == ([added], [original.name])


@pytest.mark.parametrize("previous", [{}, {"format": table_joins.RECORD_FORMAT, "version": 1, "joins": [{}]},
                                      record(join(), join())])
def test_invalid_comparison_registry_fails_instead_of_skipping(previous):
    with pytest.raises(ValueError):
        checker.changed_joins([join()], previous)


def test_comparison_revision_is_data_not_shell_code():
    for invalid in ("main", "HEAD; echo wrong", "--all", "a" * 39):
        with pytest.raises(ValueError, match="commit SHA"):
            checker.previous_record(invalid)


def test_unchanged_pr_needs_no_database_or_network(monkeypatch, tmp_path):
    monkeypatch.setattr(table_joins, "JOINS", (join(),))
    monkeypatch.setattr(checker, "previous_record", lambda _: record(join()))
    monkeypatch.setattr(checker, "connect", lambda: pytest.fail("Unchanged PR opened a database"))
    monkeypatch.setattr(checker, "table_urls", lambda _: pytest.fail("Unchanged PR read public metadata"))
    receipt = tmp_path / "receipt.json"
    assert checker.main(["--changed-since", "a" * 40, "--receipt", str(receipt)]) == 0
    assert json.loads(receipt.read_text())["selected"] == []


def test_failed_comparison_is_not_a_successful_no_change_run(monkeypatch):
    def unavailable(_):
        raise ValueError("base commit unavailable")
    monkeypatch.setattr(checker, "previous_record", unavailable)
    assert checker.main(["--changed-since", "a" * 40]) == 2


def test_default_scheduled_run_does_not_filter_any_declaration(monkeypatch):
    declarations = (join(), replace(join(), child="other_child", baseline_keys=0, kind="empty"))
    monkeypatch.setattr(table_joins, "JOINS", declarations)
    monkeypatch.setattr(checker, "connect", duckdb.connect)
    monkeypatch.setattr(checker, "table_urls", lambda _: lambda name: [name])
    seen = []
    def check(_con, joins, _urls):
        seen.extend(joins)
        return [checker.verdict(j, 0, 0) for j in joins]
    monkeypatch.setattr(checker, "check", check)
    assert checker.main(["--index-url", "https://example.org"]) == 1  # populated child became empty
    assert seen == list(declarations)


@pytest.mark.parametrize("declared,children,parents,expected_status", [
    (replace(join(), baseline_keys=0, kind="empty"), ["new"], ["new"], "UNBASELINED"),
    (replace(join(), baseline_keys=10, baseline_missing=5, kind="scope", reason="Partial parent"),
     [str(i) for i in range(10)], ["0"], "BELOW"),
    (join(), ["one"], ["one", "one"], "MULTIPLICITY"),
    (replace(join(), baseline_keys=0, kind="empty"), [], [], "EMPTY"),
])
def test_changed_join_ci_enforces_empty_partial_and_cardinality_rules(
        monkeypatch, tmp_path, declared, children, parents, expected_status):
    urls = {}
    for name, values in (("child", children), ("parent", parents)):
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({"id": pa.array(values, type=pa.string())}), path)
        urls[name] = [str(path)]
    monkeypatch.setattr(table_joins, "JOINS", (declared,))
    monkeypatch.setattr(checker, "previous_record", lambda _: record())
    monkeypatch.setattr(checker, "connect", duckdb.connect)
    monkeypatch.setattr(checker, "table_urls", lambda _: urls.__getitem__)
    receipt = tmp_path / "receipt.json"
    code = checker.main(["--changed-since", "a" * 40, "--index-url", "https://example.org",
                         "--receipt", str(receipt)])
    assert code == (1 if expected_status in checker.FAILING else 0)
    assert json.loads(receipt.read_text())["results"][0]["status"] == expected_status


def test_unreadable_public_input_fails_with_a_receipt(monkeypatch, tmp_path):
    monkeypatch.setattr(table_joins, "JOINS", (join(),))
    monkeypatch.setattr(checker, "connect", duckdb.connect)
    def unavailable(_):
        raise checker.publication.PublicationError("public index unavailable")
    monkeypatch.setattr(checker, "table_urls", unavailable)
    receipt = tmp_path / "receipt.json"
    assert checker.main(["--index-url", "https://example.org", "--receipt", str(receipt)]) == 3
    assert json.loads(receipt.read_text())["status"] == "UNREACHABLE"
    assert json.loads(receipt.read_text())["selected"] == [join().name]


def test_record_writes_measured_baselines_that_the_registry_then_enforces(monkeypatch, tmp_path):
    for name, values in (("child", ["a", "b", "b"]), ("parent", ["a"])):
        pq.write_table(pa.table({"id": pa.array(values, type=pa.string())}), tmp_path / f"{name}.parquet")
    unmeasured = replace(join(), baseline_keys=0, kind="unmeasured")
    empty = replace(join(), child="other_child", baseline_keys=0, kind="empty")
    pq.write_table(pa.table({"id": pa.array([], type=pa.string())}), tmp_path / "other_child.parquet")
    measurements = tmp_path / "join_measurements.json"
    previous = {"join": "earlier", "keys": 1, "missing": 0}
    measurements.write_text(json.dumps({"measured_on": "2026-09-27", "results": [previous]}))
    monkeypatch.setattr(checker, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(checker, "MEASUREMENTS", measurements)
    monkeypatch.setattr(table_joins, "JOINS", (unmeasured, empty))
    monkeypatch.setattr(checker, "connect", duckdb.connect)
    monkeypatch.setattr(checker, "table_urls", lambda _: lambda name: [str(tmp_path / f"{name}.parquet")])
    receipt = tmp_path / "evidence.json"
    args = ["--index-url", "https://example.org", "--receipt", str(receipt), "--record", unmeasured.name]
    assert checker.main(args) == 0
    earlier, recorded = json.loads(measurements.read_text())["results"]
    assert earlier == previous
    assert (recorded["join"], recorded["keys"], recorded["missing"], recorded["kind"]) == (unmeasured.name, 2, 1, "scope")
    assert recorded["receipt"] == "evidence.json" and "status" not in recorded
    with pytest.raises(SystemExit):
        checker.main([*args[:-1], "not a declared join"])
    # A join with no key has nothing to baseline, and the file stays as it was.
    before = measurements.read_text()
    assert checker.main([*args[:-1], empty.name]) == 1
    assert measurements.read_text() == before

@pytest.mark.integration
def test_only_a_declared_empty_child_may_be_unpublished(monkeypatch, tmp_path):
    """Live httpfs over a local server: a 404 child of a declared-empty join is EMPTY; elsewhere it is unreadable."""
    pq.write_table(pa.table({"id": pa.array(["one"], type=pa.string())}), tmp_path / "parent.parquet")

    class Files(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(tmp_path), **kwargs)

        def log_message(self, format, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Files)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setattr(checker, "table_urls", lambda _: lambda name: [f"{base}/{name}.parquet"])
    receipt = tmp_path / "receipt.json"
    try:
        for declared, code, status in ((replace(join(), baseline_keys=0, kind="empty"), 0, "EMPTY"),
                                       (join(), 3, "UNREACHABLE")):
            monkeypatch.setattr(table_joins, "JOINS", (declared,))
            assert checker.main(["--index-url", base, "--receipt", str(receipt)]) == code
            written = json.loads(receipt.read_text())
            assert (written["results"][0]["status"] if code == 0 else written["status"]) == status
    finally:
        server.shutdown()
