#!/usr/bin/env python3
"""Hold the live published tables to their declared cross-table joins (``spicy_regs.table_joins``).

Each declared join counts its distinct non-null child keys and those absent from
the parent. A join fails below its floor, the baseline rate truncated to four
decimals, so a new orphan or a narrowed parent shows as a failing number. A
join reports a few orphans beyond those its floor admits as LAG
(``Join.lag_allowance``) rather than failing, since a growing child can name a
key its parent publishes a run later. A declared-empty child that now publishes keys fails too, until its
baseline is recorded. Reads the publisher the output ledger names, or an explicit
``--index-url``, never a default. It downloads column pages and writes nothing
but an optional receipt, except that ``--record`` also records each named join's
measurement as its baseline in ``join_measurements.json``.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Sequence
from datetime import date
from pathlib import Path

import duckdb
import httpx

from spicy_regs import output_ledger, table_joins
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.sources import publication

EXIT_UNREACHABLE = 3
FAILING = ("BELOW", "UNBASELINED", "EMPTIED", "MULTIPLICITY")
REPO_ROOT = Path(__file__).resolve().parents[1]
MEASUREMENTS = REPO_ROOT / "src/spicy_regs/join_measurements.json"
#: Fields describing the declaration before this measurement, not the measurement itself.
_DECLARATION_FIELDS = ("status", "floor_pct", "baseline_keys", "baseline_missing")


def previous_record(commit: str) -> dict:
    """Read the comparison commit's generated registry, never execute its code."""
    if not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", commit):
        raise ValueError("--changed-since requires a full Git commit SHA")
    result = subprocess.run(
        ["git", "show", f"{commit}:src/spicy_regs/table_joins.json"], cwd=REPO_ROOT,
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise ValueError("Cannot read the comparison commit's table_joins.json; fetch the base commit first")
    return json.loads(result.stdout)


def changed_joins(joins: Iterable[table_joins.Join], previous: dict) -> tuple[list[table_joins.Join], list[str]]:
    """Select every new or changed declaration, including baseline and evidence changes.

    This bounds PR reads without changing the scheduled full-registry check.
    Removed declarations are reported separately rather than treated as checks.
    """
    if (previous.get("format") != table_joins.RECORD_FORMAT or previous.get("version") != 1
            or not isinstance(previous.get("joins"), list)):
        raise ValueError("Comparison registry has an unsupported format")
    before = {}
    for record in previous["joins"]:
        if not isinstance(record, dict):
            raise ValueError("Comparison registry has an invalid join")
        for side in ("child", "parent"):
            columns = record.get(f"{side}_columns")
            if (not isinstance(record.get(side), str) or not isinstance(columns, list) or not columns
                    or not all(isinstance(column, str) and column for column in columns)):
                raise ValueError("Comparison registry has an invalid join identity")
        name = (f"{record['child']}.{'+'.join(record['child_columns'])} -> "
                f"{record['parent']}.{'+'.join(record['parent_columns'])}")
        if name in before:
            raise ValueError("Comparison registry repeats a join identity")
        before[name] = record
    current = list(joins)
    selected = [join for join in current if before.get(join.name) != table_joins.record(join)]
    removed = sorted(set(before) - {join.name for join in current})
    return selected, removed


def table_urls(base_url: str) -> Callable[[str], list[str]]:
    """Resolve each table once: through the publication index, then the rulemaking pointer, else its legacy key."""
    base = base_url.rstrip("/")
    urls = publication.published_urls(base)
    return lambda table: urls.get(table, [f"{base}/{table}.parquet"])


def connect() -> duckdb.DuckDBPyConnection:
    """Modest concurrency and patient retries: the public r2.dev endpoint answers 429 under load."""
    con = duckdb.connect()
    load_public_http(con)
    con.execute("SET threads = 2")
    return con


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _unpublished(con: duckdb.DuckDBPyConnection, urls: list[str]) -> bool:
    """Whether the publisher serves no file for a table (HTTP 404), as distinct from one it fails to serve."""
    try:
        con.execute(f"SELECT 1 FROM {publication.parquet_scan(urls)} LIMIT 0")
    except duckdb.HTTPException as exc:
        if exc.status_code == 404:
            return True
        raise
    return False


def measure(con: duckdb.DuckDBPyConnection, join: table_joins.Join, url_of: Callable[[str], list[str]]) -> dict:
    """Measure full selected inputs, preserving parent multiplicity and raw-row amplification.

    A declared-empty child that no generation publishes yet has no key to read: it
    reports EMPTY without a parent read. Any other unpublished input stays unreadable.
    """
    child = [_quote(column) for column in join.child_columns]
    parent = [_quote(column) for column in join.parent_columns]
    child_urls, parent_urls = url_of(join.measured_via or join.child), url_of(join.parent)
    if join.kind == "empty" and _unpublished(con, child_urls):
        result = verdict(join, 0, 0)
        result.update({"scope": "child_not_published", "child_urls": child_urls, "parent_urls": parent_urls,
                       "expected_cardinality": join.expected_cardinality})
        return result
    keys_sql = ", ".join(f"c{i}" for i in range(len(child)))
    on = " AND ".join(f"c.c{i} = p.p{i}" for i in range(len(child)))
    sql = f"""
        WITH child_input AS (SELECT {', '.join(child)} FROM {publication.parquet_scan(child_urls)}),
             parent_input AS (SELECT {', '.join(parent)} FROM {publication.parquet_scan(parent_urls)}),
             c AS (SELECT {', '.join(f'{col} AS c{i}' for i, col in enumerate(child))}, count(*) AS child_rows
                   FROM child_input WHERE {' AND '.join(f'{col} IS NOT NULL' for col in child)}
                   GROUP BY ALL),
             p AS (SELECT {', '.join(f'{col} AS p{i}' for i, col in enumerate(parent))}, count(*) AS parent_rows
                   FROM parent_input WHERE {' AND '.join(f'{col} IS NOT NULL' for col in parent)}
                   GROUP BY ALL),
             j AS (SELECT {keys_sql}, child_rows, coalesce(parent_rows, 0) AS parent_rows
                   FROM c LEFT JOIN p ON {on})
        SELECT count(*) AS keys, count(*) FILTER (WHERE parent_rows=0) AS missing,
               (list(concat_ws('|', {keys_sql}) ORDER BY {keys_sql}) FILTER (WHERE parent_rows=0))[1:3] AS examples,
               (SELECT count(*) FROM child_input) AS child_input_rows,
               coalesce(sum(child_rows),0) AS child_nonnull_rows,
               (SELECT count(*) FROM parent_input) AS parent_input_rows,
               (SELECT count(*) FROM p) AS parent_distinct_keys,
               (SELECT count(*) FROM p WHERE parent_rows>1) AS parent_duplicate_keys,
               coalesce((SELECT max(parent_rows) FROM p),0) AS max_parent_multiplicity,
               coalesce(max(parent_rows),0) AS max_matched_parent_multiplicity,
               coalesce(sum(child_rows*parent_rows),0) AS inner_join_rows,
               coalesce(sum(child_rows*greatest(parent_rows,1)),0) AS left_join_nonnull_rows
        FROM j"""
    row = con.execute(sql).fetchone()
    assert row is not None
    result = verdict(join, row[0], row[1], row[2] or [])
    names = ("child_input_rows", "child_nonnull_rows", "parent_input_rows", "parent_distinct_keys",
             "parent_duplicate_keys", "max_parent_multiplicity", "max_matched_parent_multiplicity",
             "inner_join_rows", "left_join_nonnull_rows")
    result.update(zip(names, row[3:]))
    result.update({"scope": "full_selected_inputs", "child_urls": child_urls, "parent_urls": parent_urls,
                   "sql": sql, "expected_cardinality": join.expected_cardinality})
    if join.expected_cardinality == "one" and result["max_parent_multiplicity"] > 1:
        result["status"] = "MULTIPLICITY"
    return result


def verdict(join: table_joins.Join, keys: int, missing: int, examples: Sequence[str] = ()) -> dict:
    floor = join.floor_pct
    pct = None if not keys else 100 * (keys - missing) / keys
    if not keys:
        status = "EMPTY" if floor is None else "EMPTIED"
    elif floor is None:
        status = "UNBASELINED"
    elif pct is not None and pct >= floor:
        status = "OK"
    else:
        status = "LAG" if missing <= join.admitted_missing(keys) + join.lag_allowance(keys) else "BELOW"
    return {"join": join.name, "kind": join.kind, "status": status, "keys": keys, "missing": missing,
            "resolved_pct": None if pct is None else round(pct, 4), "floor_pct": floor,
            "baseline_keys": join.baseline_keys, "baseline_missing": join.baseline_missing,
            "examples": list(examples)}


def check(con: duckdb.DuckDBPyConnection, joins: Iterable[table_joins.Join],
          url_of: Callable[[str], list[str]]) -> list[dict]:
    return [measure(con, join, url_of) for join in joins]


def record_baselines(joins: Sequence[table_joins.Join], results: Sequence[dict], receipt: Path,
                     measured_on: str, path: Path) -> None:
    """Record each measurement as its join's baseline, replacing one recorded under the same name.

    An ``unmeasured`` join becomes ``complete`` when every key resolves and ``scope``
    otherwise; other kinds keep their declaration. A join with no key, or with more
    parent rows than its declared cardinality admits, has no baseline to record.
    """
    record = json.loads(path.read_text())
    entries = {entry["join"]: entry for entry in record["results"]}
    for join, result in zip(joins, results, strict=True):
        if result["status"] in ("EMPTY", "EMPTIED", "MULTIPLICITY"):
            raise ValueError(f"{join.name}: {result['status']} has no baseline to record")
        kind = join.kind if join.kind != "unmeasured" else ("scope" if result["missing"] else "complete")
        entries[join.name] = {
            **{key: value for key, value in result.items() if key not in _DECLARATION_FIELDS},
            "kind": kind, "measured_on": measured_on, "receipt": receipt.resolve().relative_to(REPO_ROOT).as_posix(),
            "measured_expected_cardinality": "one" if result["max_parent_multiplicity"] <= 1 else "many",
        }
    record["results"] = list(entries.values())
    path.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")


def _line(result: dict) -> str:
    pct = "n/a" if result["resolved_pct"] is None else f"{result['resolved_pct']:.4f}%"
    floor = "none" if result["floor_pct"] is None else f"{result['floor_pct']:.4f}%"
    detail = f"  e.g. {result['examples']}" if result["examples"] else ""
    return (f"{result['status']:<11} {result['join']}  {pct} (floor {floor}, {result['kind']})  "
            f"keys={result['keys']:,} missing={result['missing']:,}{detail}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=output_ledger.LEDGER)
    parser.add_argument("--index-url", help="Public base URL to check instead of the ledger's stated destination")
    parser.add_argument("--receipt", type=Path, help="Write every measurement to this JSON file")
    parser.add_argument("--changed-since", help="Check only declarations added or changed since this full Git commit SHA")
    parser.add_argument("--aggregate", action="append", help="Run this named aggregate check instead of join checks; repeatable")
    parser.add_argument("--timeout-seconds", type=float, default=90, help="Aggregate query timeout, at most 90 seconds")
    parser.add_argument("--record", action="append", metavar="JOIN",
                        help="Measure this declared join (its full name) and record the result as its baseline in "
                             "join_measurements.json, citing --receipt; repeatable")
    args = parser.parse_args(argv)
    if args.changed_since and args.aggregate:
        parser.error("--changed-since selects joins and cannot be used with --aggregate")
    if args.record and (args.changed_since or args.aggregate or not args.receipt
                        or not args.receipt.resolve().is_relative_to(REPO_ROOT)):
        parser.error("--record needs a --receipt inside the repository it cites and cannot be combined with "
                     "--changed-since or --aggregate")
    joins, removed = list(table_joins.JOINS), []
    if args.record:
        declared = {join.name: join for join in joins}
        if unknown := [name for name in args.record if name not in declared]:
            parser.error(f"Not a declared join: {unknown[0]}")
        joins = [declared[name] for name in dict.fromkeys(args.record)]
    if args.changed_since:
        try:
            joins, removed = changed_joins(joins, previous_record(args.changed_since))
        except (ValueError, OSError) as exc:
            print(f"Cannot select changed joins: {exc}", file=sys.stderr)
            return 2
        print(f"Selected {len(joins)} added or changed joins against {args.changed_since}")
        for name in removed:
            print(f"REMOVED     {name}")
        if not joins:
            if args.receipt:
                args.receipt.write_text(json.dumps({"changed_since": args.changed_since, "selected": [],
                                                  "removed": removed, "results": []}, indent=2) + "\n")
            print("No added or changed join declarations; no public data reads needed.")
            return 0
    if args.index_url:
        base, source = args.index_url.rstrip("/"), "an explicit --index-url, not the ledger's destination"
    else:
        try:
            base = output_ledger.ledger_destination(args.ledger.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(f"Cannot name the publisher to check: {exc}", file=sys.stderr)
            return 2
        source = "the ledger's stated destination"
    if args.aggregate:
        from spicy_regs.aggregate_checks import check_public
        try:
            with connect() as con:
                receipt = check_public(con, base, args.aggregate, timeout_seconds=args.timeout_seconds)
        except (httpx.HTTPError, duckdb.Error, publication.PublicationError, ValueError) as exc:
            print(f"Aggregate selection failed: {exc}", file=sys.stderr)
            return EXIT_UNREACHABLE
        if args.receipt:
            args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
        for result in receipt["results"]:
            print(f"{result['status']:<14} {result['check']}: {result['reason']}")
        if any(r["status"] == "MISMATCH" for r in receipt["results"]):
            return 1
        return 0 if all(r["status"] in ("OK", "EMPTY") for r in receipt["results"]) else EXIT_UNREACHABLE
    print(f"Checking {len(joins)} declared joins against {base} ({source}); baseline {table_joins.BASELINE_DATE}")
    try:
        with connect() as con:
            results = check(con, joins, table_urls(base))
    except (httpx.HTTPError, duckdb.Error, publication.PublicationError) as exc:
        if args.receipt:
            args.receipt.write_text(json.dumps({"base": base, "changed_since": args.changed_since,
                                              "selected": [join.name for join in joins], "removed": removed,
                                              "error": str(exc), "status": "UNREACHABLE"}, indent=2) + "\n")
        print(f"Live tables could not be read; joins were NOT checked: {exc}", file=sys.stderr)
        return EXIT_UNREACHABLE
    for result in results:
        print(_line(result))
    if args.receipt:
        args.receipt.write_text(json.dumps({"base": base, "changed_since": args.changed_since,
                                          "selected": [join.name for join in joins], "removed": removed,
                                          "results": results}, indent=1) + "\n")
    if args.record:
        try:
            record_baselines(joins, results, args.receipt, date.today().isoformat(), MEASUREMENTS)
        except ValueError as exc:
            print(f"Baselines NOT recorded: {exc}", file=sys.stderr)
            return 1
        print(f"Recorded {len(joins)} baselines in {MEASUREMENTS.relative_to(REPO_ROOT)}; "
              "run `spicy-regs-dict generate` to refresh table_joins.json")
        return 0
    failed = [result for result in results if result["status"] in FAILING]
    counts = {status: sum(r["status"] == status for r in results) for status in dict.fromkeys(r["status"] for r in results)}
    print(" ".join(f"{status}={count}" for status, count in counts.items()))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
