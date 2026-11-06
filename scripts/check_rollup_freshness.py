#!/usr/bin/env python3
"""Check published tables for stale watermarks and stalled growth.

Covers both the base tables the daily ETL publishes (``dockets``, ``documents``)
and the external-source rollups. Date-backed sources are checked against
source-appropriate age budgets. Sources without a meaningful update date are
tracked by row-count change (``usaspending_recipients``) or checked against their
publication metadata without a date-age rule. The state file lets the daily
workflow remember when a row count last changed.

One publication snapshot selects all inputs. Remote column scans check managed
members against their declared schemas and row counts. This monitor does not
download or hash whole files: the publisher and CLI own byte-pin verification.
Tables outside the index keep the legacy remote column-read path.

The comment corpus gets a coverage check instead of a date watermark: per-agency
row counts from ``comments_index.parquet``, compared with the last known-good
counts in the state file (see :func:`evaluate_comment_coverage`).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import httpx

from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.sources import publication

FreshnessRow = tuple[str, str, str | None, int]
FreshnessState = dict[str, dict]


@dataclass(frozen=True)
class DateCheck:
    table: str
    column: str
    budget_days: int
    label: str | None = None


# Base tables published by the daily ETL (etl-new-pipeline.yml). They are not
# external rollups, but they share this machinery — a max watermark over the
# published Parquet on R2 — and nothing else was watching them. A silently
# swallowed upload failure froze `dockets` at 2026-07-02 for ~8 weeks without a
# single alert, while `comments` (covered by check-comments-freshness.yml) and
# every external rollup stayed green.
#
# `modify_date` is the watermark for both, NOT `posted_date`: documents carry
# future effective dates (months ahead of today), so max(posted_date) reads as
# fresh forever and can never detect a stall.
#
# 4-day budget: enough slack to ride out a three-day federal holiday weekend
# without paging, tight enough that a real break surfaces the same week.
BASE_TABLE_CHECKS = (
    DateCheck("dockets", "modify_date", 4),
    DateCheck("documents", "modify_date", 4),
)

EXTERNAL_DATE_CHECKS = (
    DateCheck("federal_register", "publication_date", 3),
    DateCheck("lobbying_filings", "dt_posted", 3),
    # Both metrics matter: update_date alone can stay green while recent
    # legislative actions are missing from an incompletely seeded congress.
    DateCheck("congress_bills", "update_date", 7, "update watermark"),
    DateCheck("congress_bills", "latest_action_date", 7, "latest action"),
    DateCheck("cfr_sections", "last_modified", 45),
    DateCheck("fec_committees", "last_file_date", 14),
    DateCheck("court_dockets", "date_filed", 14),
    DateCheck("gao_reports", "published_date", 14),
    DateCheck("crs_reports", "published_date", 14),
    DateCheck("fcc_proceedings", "date_created", 14),
    DateCheck("fcc_filings", "date_received", 7),
)

DATE_CHECKS = BASE_TABLE_CHECKS + EXTERNAL_DATE_CHECKS

ROW_CHANGE_BUDGETS = {"usaspending_recipients": 14}
SKIPPED = {
    "hearing_bill_links": "derived cover links; recall unmeasured, no daily watermark",
    "cbo_cost_estimates": "sparse publication index; no daily publication guarantee",
    "committee_report_reads": "processing receipts; integrity checked, no source-date age rule",
    "unified_agenda": "semiannual edition, not a daily date watermark",
    "sam_entities": "registration_date does not change when an existing entity is refreshed",
    # Derived from comments + fec_committees, so it carries no date watermark of
    # its own; both of its sources are already watched above. Its row count is a
    # better signal — promote it to ROW_CHANGE_BUDGETS once the first publish
    # lands and a steady-state row count is known.
    "org_committee_links": "derived link table; no date watermark of its own",
    # The two PDF-only families. None of the five is a daily-watermark table:
    # House activity reports are published once a Congress, budget volumes once
    # a fiscal year, and the Secretary of the Senate reports twice a year, so
    # any age budget a daily check could use would page for months at a time on
    # a source behaving normally. The three derived tables carry no watermark of
    # their own at all. Promote the two document tables to ROW_CHANGE_BUDGETS
    # once a first publish lands and a steady-state row count is known — a
    # stalled rollup shows there, where a date budget cannot show it.
    "house_activity_reports": "published once a Congress; no daily watermark",
    "budget_volumes": "published once a fiscal year; no daily watermark",
    "bill_committee_actions": "derived from house_activity_reports, which is watched with it",
    "document_citations": "derived link table; both of its source families are watched with it",
    "senate_expenditures": "semiannual report; no daily watermark",
}


def _query(sources: Mapping[str, list[str]]) -> str:
    branches = []
    for check in DATE_CHECKS:
        if check.table not in sources:
            continue
        label = check.label or check.column
        target = publication.parquet_scan(sources[check.table])
        branches.append(
            f"""SELECT '{check.table}' AS table_name, '{label}' AS metric,
                       CAST(MAX(TRY_CAST({check.column} AS TIMESTAMP)) AS VARCHAR) AS latest,
                       COUNT(*)::BIGINT AS row_count
                FROM {target}"""
        )
    for table in ROW_CHANGE_BUDGETS:
        if table not in sources:
            continue
        target = publication.parquet_scan(sources[table])
        branches.append(
            f"""SELECT '{table}' AS table_name, 'row count' AS metric,
                       NULL::VARCHAR AS latest, COUNT(*)::BIGINT AS row_count
                FROM {target}"""
        )
    dated = {check.table for check in DATE_CHECKS}
    for table in sorted(set(sources) - dated - set(ROW_CHANGE_BUDGETS)):
        target = publication.parquet_scan(sources[table])
        branches.append(
            f"SELECT '{table}' AS table_name, 'publication rows' AS metric, "
            f"NULL::VARCHAR AS latest, COUNT(*)::BIGINT AS row_count FROM {target}"
        )
    return "\nUNION ALL\n".join(branches)


def read_freshness_rows(base_url: str, selected_tables: Sequence[str] | None = None) -> list[FreshnessRow]:
    """Measure selected members; a broken managed target never falls back."""
    tables = dict.fromkeys(
        selected_tables if selected_tables is not None else [check.table for check in DATE_CHECKS] + list(ROW_CHANGE_BUDGETS)
    )
    rows: list[FreshnessRow] = []
    with publication.snapshot(base_url) as index:
        if selected_tables is None:
            tables.update(dict.fromkeys(publication.parquet_tables(index)))
        con = duckdb.connect()
        try:
            for table in tables:
                descriptor = publication.table_descriptor(index, f"{table}.parquet")
                keys = [member.path for member in publication.table_members(index, f"{table}.parquet")]
                target = [f"{base_url.rstrip('/')}/{key}" for key in keys]
                if descriptor is not None:
                    actual = con.execute(f"DESCRIBE SELECT * FROM {publication.parquet_scan(target)}").fetchall()
                    if [[row[0], row[1]] for row in actual] != descriptor["columns"]:
                        raise publication.PublicationError(f"Published freshness schema differs from its pin: {table}")
                    family = next(
                        entry for entry in index["families"].values() if f"{table}.parquet" in entry["tables"]
                    )
                    print(
                        f"SOURCE: {table} managed artifact={family['artifactDigest']} members={','.join(keys)} "
                        "(byte digest not rechecked)"
                    )
                else:
                    print(f"SOURCE: {table} legacy url={target} (no publication pin)")
                measured = con.execute(_query({table: target})).fetchall()
                if descriptor is not None and any(row[3] != descriptor["rows"] for row in measured):
                    raise publication.PublicationError(f"Published freshness row count differs from its pin: {table}")
                rows.extend(measured)
        finally:
            con.close()
    return rows


# Comment coverage. A dedupe swap that died mid-refill left `comments` with 103 of
# 179 agencies (#197), and nothing alerted for weeks: check-comments-freshness
# only compares agencies present in both the index and the rows, and the lost
# agencies were gone from both, so it stayed green. Knowing an agency *should*
# exist takes memory, which is what the state file is for. It holds the last
# known-good per-agency counts, and flags an agency that vanished, one that lost
# at least half its rows (and at least COVERAGE_MIN_AGENCY_DROP rows, so small
# agencies and the dedupe trickle don't page), or a corpus-wide drop beyond
# COVERAGE_MAX_TOTAL_DROP. On a failure the baseline is left alone, so the alert
# keeps firing until the rows come back or --accept-comment-coverage rebaselines.
COMMENTS_INDEX = "comments_index"
COVERAGE_STATE_KEY = "comments_coverage"
COVERAGE_MAX_AGENCY_DROP = 0.5
COVERAGE_MIN_AGENCY_DROP = 1_000
COVERAGE_MAX_TOTAL_DROP = 0.02


def read_comment_counts(base_url: str) -> dict[str, int]:
    """Per-agency comment rows from the published ``comments_index`` (small: one row per group)."""
    target = publication.parquet_scan([f"{base_url.rstrip('/')}/{COMMENTS_INDEX}.parquet"])
    with duckdb.connect() as con:
        return dict(con.execute(f"""SELECT agency_code, CAST(SUM(row_count) AS BIGINT) FROM {target}
                                    WHERE agency_code IS NOT NULL GROUP BY agency_code""").fetchall())


def evaluate_comment_coverage(
    counts: dict[str, int],
    state: FreshnessState,
    today: date,
    accept: bool = False,
) -> list[str]:
    """Flag agencies that vanished or lost most of their comments since the baseline.

    Updates the baseline in ``state`` only when nothing is flagged, or when
    ``accept`` is set (a deliberate change being rebaselined).
    """
    baseline = state.get(COVERAGE_STATE_KEY, {}).get("agencies", {})
    failures: list[str] = []
    for agency, before in sorted(baseline.items(), key=lambda kv: -kv[1]):
        after = counts.get(agency)
        if after is None:
            failures.append(f"comments: agency {agency} vanished (had {before:,} rows)")
        elif before - after >= COVERAGE_MIN_AGENCY_DROP and after < before * (1 - COVERAGE_MAX_AGENCY_DROP):
            failures.append(f"comments: agency {agency} fell {before:,} -> {after:,} rows")
    before_total, after_total = sum(baseline.values()), sum(counts.values())
    if before_total and after_total < before_total * (1 - COVERAGE_MAX_TOTAL_DROP):
        failures.append(f"comments: total fell {before_total:,} -> {after_total:,} rows")

    if failures and not accept:
        print(f"FAIL: comments coverage — {len(failures)} problem(s); baseline kept")
    else:
        if failures:
            print(f"ACCEPTED: comments coverage rebaselined over {len(failures)} problem(s)")
            failures = []
        state[COVERAGE_STATE_KEY] = {"agencies": dict(counts), "as_of": today.isoformat()}
        print(f"OK: comments coverage agencies={len(counts)} rows={after_total:,}")
    return failures


def _parse_latest(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None


def evaluate_date_rows(rows: Sequence[FreshnessRow], today: date) -> list[str]:
    """Return human-readable failures for the date-backed result rows."""
    budgets = {(c.table, c.label or c.column): c.budget_days for c in DATE_CHECKS}
    failures: list[str] = []
    for table, metric, raw_latest, row_count in rows:
        budget = budgets.get((table, metric))
        if budget is None:
            continue
        latest = _parse_latest(raw_latest)
        if latest is None:
            failures.append(f"{table} ({metric}): no parseable watermark across {row_count:,} rows")
            continue
        age = (today - latest).days
        status = "OK" if age <= budget else "STALE"
        print(f"{status}: {table} ({metric}) latest={latest} age={age}d budget={budget}d rows={row_count:,}")
        if age > budget:
            failures.append(f"{table} ({metric}) is {age}d old (budget {budget}d)")
    return failures


def evaluate_row_changes(
    rows: Sequence[FreshnessRow],
    state: FreshnessState,
    today: date,
) -> list[str]:
    """Update row-count state and flag tables unchanged beyond their budget."""
    failures: list[str] = []
    for table, metric, _latest, row_count in rows:
        if metric != "row count" or table not in ROW_CHANGE_BUDGETS:
            continue
        previous = state.get(table, {})
        previous_count = previous.get("count")
        if previous_count != row_count:
            state[table] = {"count": row_count, "last_changed": today.isoformat()}
            print(f"OK: {table} row count changed {previous_count!r} -> {row_count:,}")
            continue
        raw_changed = str(previous.get("last_changed", today.isoformat()))
        try:
            last_changed = date.fromisoformat(raw_changed)
        except ValueError:
            last_changed = today
        age = (today - last_changed).days
        budget = ROW_CHANGE_BUDGETS[table]
        status = "OK" if age <= budget else "STALE"
        print(f"{status}: {table} rows={row_count:,} unchanged={age}d budget={budget}d")
        if age > budget:
            failures.append(f"{table} row count has not changed for {age}d (budget {budget}d)")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url")
    parser.add_argument(
        "--table",
        action="append",
        choices=sorted({check.table for check in DATE_CHECKS} | set(ROW_CHANGE_BUDGETS)),
        help="Check only this monitored table; repeat to select several. Omits unrelated tables and comment coverage.",
    )
    parser.add_argument("--state-file", type=Path, default=Path(".rollup-freshness-state.json"))
    parser.add_argument("--today", type=date.fromisoformat, default=date.today(), help="Testing override (YYYY-MM-DD)")
    parser.add_argument(
        "--accept-comment-coverage",
        action="store_true",
        help="Rebaseline comment coverage to today's counts (after a deliberate removal)",
    )
    args = parser.parse_args()
    if args.table and args.accept_comment_coverage:
        parser.error("--accept-comment-coverage cannot be combined with --table")
    args.base_url = resolve_r2_base_url(args.base_url)

    try:
        state = json.loads(args.state_file.read_text()) if args.state_file.exists() else {}
    except (OSError, json.JSONDecodeError):
        state = {}

    try:
        rows = read_freshness_rows(args.base_url, args.table)
        comment_counts = read_comment_counts(args.base_url) if args.table is None else None
    except (duckdb.Error, httpx.HTTPError, publication.PublicationError) as exc:
        print(f"Freshness inputs could not be verified: {exc}", file=sys.stderr)
        return 1

    failures = evaluate_date_rows(rows, args.today)
    failures.extend(evaluate_row_changes(rows, state, args.today))
    if comment_counts is not None:
        failures.extend(evaluate_comment_coverage(comment_counts, state, args.today, args.accept_comment_coverage))
    args.state_file.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")

    if args.table is None:
        for table, reason in SKIPPED.items():
            print(f"NO DATE BUDGET: {table} — {reason}")
    if failures:
        print("\nFreshness failures:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    if args.table:
        print("\nSelected monitored tables are within budget.")
    else:
        print("\nAll monitored base tables and external-source rollups are within budget.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
