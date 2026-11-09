"""The bill family's BILLSUM supplement: CRS summaries BILLSTATUS has not caught up with.

BILLSTATUS is the family's summary source and stays authoritative. GovInfo does not regenerate an ended
Congress's BILLSTATUS file when CRS later publishes a summary for it, while the BILLSUM collection carries that
summary (:mod:`spicy_regs.sources.billsum` states the measurement). This step reads the BILLSUM folder zips of
the scoped Congresses and adds a ``bill_publisher_summaries`` row for each (bill, summary version) no BILLSTATUS
row states. When a later BILLSTATUS read states that pair, the BILLSUM rows for it are retired at the merge.

A BILLSUM row keeps BILLSUM's own values: ``update_date`` is the summary's ``update-date``, a day with no time;
``last_summary_update_date`` is NULL, because BILLSUM states no such field; ``source_schema_version`` is
``BILLSUM`` (:data:`~spicy_regs.sources.billsum.BILLSUM_SCHEMA_CONTEXT`), so a reader can tell which clock
rules apply. No first-publication claim follows from either source's date.

Scope, budget and resume:

- Congresses: ``BILL_FAMILY_BILLSUM_CONGRESSES`` (the scope syntax of ``BILL_FAMILY_CONGRESSES``), by default the
  current Congress and the :data:`BILLSUM_TRAILING` before it, never below ``BILLSUM_FLOOR``; bill types: the
  family's own scope. A replay of the summary prior reads every BILLSUM Congress up to the newest bulk one, so
  the rebuilt table restates every BILLSUM row.
- One listing per folder each run; the zip only when its listing entry moved since the entry retained on
  ``bill_family_archives`` under :data:`BILLSUM_ARCHIVES_KEY` (``zip_entry_unchanged``, as the BILLSTATUS and
  BILLS folders compare). At most ``BILL_FAMILY_BILLSUM_MAX_ZIPS`` zips a run (:data:`DEFAULT_MAX_ZIPS`); a
  folder past the cap keeps its retained entry and is read on a later run.
- A folder whose listing or zip is refused keeps its retained entry and its published rows. A folder that held a
  summary for a bill with no ``congress_bills`` row retains no entry, so it is read again next run.
"""

from __future__ import annotations

import json
import os
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from loguru import logger
from spicy_docs.schemas.tables import bill_id as bill_key
from spicy_docs.schemas.tables import text
from spicy_docs.sources.congress.bulk_status import BulkListingEntry, zip_entry_unchanged
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.source_evidence import SourceEvidenceError
from spicy_regs.sources.billsum import BILLSUM_FLOOR, BILLSUM_SCHEMA_CONTEXT, billsum_locator
from spicy_regs.transforms.congress_scope import congresses_from_env

#: The ``bill_family_archives`` metadata key holding each BILLSUM folder's retained zip entry.
BILLSUM_ARCHIVES_KEY = "spicy_regs.bill_family.billsum_archives.v1"

#: Ended Congresses kept in the default scope beside the current one. On 2026-11-09 CRS was still publishing
#: 118th summaries (82 since February 2026) and the 117th differed by 2; two earlier Congresses cover both.
BILLSUM_TRAILING = 2

#: Zips one run may read. The default scope is 24 folders; the first run reads them all (about 46 MB for the
#: 117th-119th, measured 2026-11-09), later runs only the folders whose zip moved.
DEFAULT_MAX_ZIPS = 32

#: The summary identity: a bill and its two-digit summary version.
Pair = tuple[str, str]


class BillSumSource(Protocol):
    """What this step needs of a BILLSUM acquirer: the real ``BillSumAcquirer`` and the tests' stubs."""

    def list_folder(self, congress: int, bill_type: str) -> Any: ...

    def acquire(self, listing: Any) -> Any: ...


def billsum_congresses(bulk_congresses: Sequence[int], *, replay: bool) -> tuple[int, ...]:
    """The Congresses this run reads BILLSUM for, newest first."""
    if replay:
        newest = max(
            [*bulk_congresses, *congresses_from_env("BILL_FAMILY_BILLSUM_CONGRESSES", trailing=BILLSUM_TRAILING)]
        )
        return tuple(range(newest, BILLSUM_FLOOR - 1, -1))
    scoped = congresses_from_env("BILL_FAMILY_BILLSUM_CONGRESSES", trailing=BILLSUM_TRAILING)
    return tuple(sorted({congress for congress in scoped if congress >= BILLSUM_FLOOR}, reverse=True))


def max_zips() -> int:
    raw = os.environ.get("BILL_FAMILY_BILLSUM_MAX_ZIPS", "").strip()
    if not raw:
        return DEFAULT_MAX_ZIPS
    value = int(raw)
    if value < 0:
        raise ValueError("BILL_FAMILY_BILLSUM_MAX_ZIPS must be a nonnegative integer; zero reads no zip")
    return value


def _entry_record(entry: BulkListingEntry, observed_at: str | None) -> dict[str, str | None]:
    return {
        "name": entry.name,
        "link": entry.link,
        "formatted_last_modified_time": entry.formatted_last_modified_time,
        "modified_at": entry.modified_at.isoformat(),
        "size": None if entry.size is None else str(entry.size),
        "observed_at": observed_at,
    }


def _entry_from(record: Mapping[str, Any]) -> BulkListingEntry | None:
    """The comparison value a retained record states, or ``None`` when it states too little to compare."""
    try:
        name, link = str(record["name"]), str(record["link"])
        instant = datetime.fromisoformat(str(record["modified_at"]))
        size = None if record.get("size") is None else int(str(record["size"]))
    except (KeyError, TypeError, ValueError):
        return None
    return BulkListingEntry(
        name=name,
        display_label="",
        just_file_name="",
        link=link,
        folder=False,
        formatted_last_modified_time=str(record.get("formatted_last_modified_time") or ""),
        modified_at=instant,
        mime_type=None,
        file_extension=None,
        formatted_size=None,
        size=size,
    )


def held_billsum_archives(path: Path | None) -> dict[str, dict[str, Any]]:
    """``"{congress}-{type}"`` -> the record the last run retained, read off ``bill_family_archives``' metadata."""
    if path is None:
        return {}
    import pyarrow.parquet as pq

    raw = (pq.read_schema(path).metadata or {}).get(BILLSUM_ARCHIVES_KEY.encode())
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except ValueError:
        logger.warning("Bill family BILLSUM: retained folder entries are unreadable — every scoped zip is read")
        return {}
    if not isinstance(value, dict):
        return {}
    return {str(key): record for key, record in value.items() if isinstance(record, dict)}


def billsum_metadata(held: Mapping[str, Mapping[str, Any]]) -> str:
    return json.dumps({key: dict(held[key]) for key in sorted(held)}, sort_keys=True)


def _has(path: Path | None, columns: Iterable[str]) -> bool:
    if path is None:
        return False
    import pyarrow.parquet as pq

    return set(columns) <= set(pq.read_schema(path).names)


def prior_summary_pairs(path: Path | None, congresses: Collection[int]) -> tuple[set[Pair], set[Pair]]:
    """The published summary pairs of the scoped Congresses: (BILLSTATUS pairs, BILLSUM pairs).

    A prior written before ``source_schema_version`` existed holds BILLSTATUS rows only, so every pair it
    states is BILLSTATUS's; reading none would make each of its summaries look missing.
    """
    if not congresses or not _has(path, ("bill_id", "summary_version_code")):
        return set(), set()
    import duckdb

    status: set[Pair] = set()
    billsum: set[Pair] = set()
    wanted = [str(congress) for congress in congresses]
    schema_column = "source_schema_version" if _has(path, ("source_schema_version",)) else "NULL"
    for bill, version, schema in duckdb.execute(
        f"SELECT DISTINCT bill_id, summary_version_code, {schema_column} FROM read_parquet('{path}') "
        "WHERE split_part(bill_id, '-', 1) IN (SELECT UNNEST(?::VARCHAR[]))",
        [wanted],
    ).fetchall():
        (billsum if schema == BILLSUM_SCHEMA_CONTEXT else status).add((bill, version))
    return status, billsum


def known_bills(path: Path | None, fresh: Iterable[Mapping[str, Any]], congresses: Collection[int]) -> set[str]:
    """The bill ids with a ``congress_bills`` row, published or this run's, in the scoped Congresses."""
    bills = {str(row["bill_id"]) for row in fresh if row.get("bill_id")}
    if congresses and _has(path, ("bill_id",)):
        import duckdb

        wanted = [str(congress) for congress in congresses]
        bills.update(
            str(row[0])
            for row in duckdb.execute(
                f"SELECT bill_id FROM read_parquet('{path}') WHERE split_part(bill_id, '-', 1) "
                "IN (SELECT UNNEST(?::VARCHAR[]))",
                [wanted],
            ).fetchall()
        )
    return bills


def summary_row(summary: Any) -> dict[str, str | None]:
    """One ``bill_publisher_summaries`` row from one BILLSUM summary, every value as BILLSUM states it."""
    return {
        "bill_id": bill_key(summary.identity),
        "summary_version_code": text(summary.version_code),
        "action_date": text(summary.action_date),
        "action_desc": text(summary.action_desc),
        "update_date": text(summary.update_date),
        "summary_html": text(summary.text),
        "summary_chars": text(len(summary.text)),
        "last_summary_update_date": None,
        "source_schema_version": BILLSUM_SCHEMA_CONTEXT,
    }


@dataclass
class BillSumOutcome:
    """What one BILLSUM pass produced: rows to publish, rows to retire, and the entries to retain."""

    rows: list[dict[str, str | None]] = field(default_factory=list)
    #: ``(bill_id, summary_version_code, source_schema_version)`` scopes whose published BILLSUM rows retire.
    superseded: set[tuple[str, str, str]] = field(default_factory=set)
    held: dict[str, dict[str, Any]] = field(default_factory=dict)
    listings: int = 0
    zips: int = 0
    unchanged: int = 0
    capped: int = 0
    refused_folders: int = 0
    refused_files: int = 0
    refused_summaries: int = 0
    stated_by_status: int = 0
    orphans: int = 0


def run_billsum_supplement(
    source: BillSumSource,
    congresses: Sequence[int],
    bill_types: Sequence[str],
    *,
    held: Mapping[str, Mapping[str, Any]],
    status_pairs: Collection[Pair],
    prior_billsum: Collection[Pair],
    bills: Collection[str],
    zip_cap: int,
    replay: bool,
) -> BillSumOutcome:
    """Read each scoped BILLSUM folder whose zip moved, and keep the summaries BILLSTATUS does not state.

    ``status_pairs`` is every (bill, summary version) a BILLSTATUS row states, published or this run's;
    ``prior_billsum`` the pairs published BILLSUM rows state. A replay ignores the retained entries and the cap,
    because the rebuilt table must restate every BILLSUM row.
    """
    outcome = BillSumOutcome(held={key: dict(record) for key, record in held.items()})
    status = set(status_pairs)
    outcome.superseded = {
        (bill, version, BILLSUM_SCHEMA_CONTEXT) for bill, version in prior_billsum if (bill, version) in status
    }
    seen: set[tuple[str | None, ...]] = set()
    for congress in congresses:
        for bill_type in bill_types:
            folder = f"{congress}-{bill_type}"
            retained = None if replay else _entry_from(outcome.held.get(folder, {}))
            locator = billsum_locator(congress, bill_type)
            if retained is not None and (retained.name, retained.link) != (locator.rsplit("/", 1)[-1], locator):
                retained = None
            try:
                listing = source.list_folder(congress, bill_type)
                outcome.listings += 1
                entry = listing.listing.zip_entry
                if retained is not None and zip_entry_unchanged(entry, retained):
                    outcome.unchanged += 1
                    continue
                if not replay and outcome.zips >= zip_cap:
                    outcome.capped += 1
                    continue
                acquisition = source.acquire(listing.listing)
                outcome.zips += 1
            except SourceEvidenceError:
                raise
            except Exception as error:  # noqa: BLE001 — a refused folder keeps its entry and rows; the run goes on
                outcome.refused_folders += 1
                logger.warning(
                    "Bill family BILLSUM: {} {} refused — its published rows stand: {}",
                    congress,
                    bill_type,
                    scrub_credential(str(error), ""),
                )
                continue
            orphans = 0
            for member in acquisition.archive.members:
                if member.refusal is not None:
                    outcome.refused_files += 1
                    continue
                outcome.refused_summaries += len(member.refused_summaries)
                for summary in member.summaries:
                    row = summary_row(summary)
                    pair = (row["bill_id"], row["summary_version_code"])
                    if pair in status:
                        outcome.stated_by_status += 1
                        continue
                    if row["bill_id"] not in bills:
                        orphans += 1
                        continue
                    identity = (row["bill_id"], row["summary_version_code"], row["action_date"], row["update_date"])
                    if identity in seen:
                        continue
                    seen.add(identity)
                    outcome.rows.append(row)
            outcome.orphans += orphans
            if orphans:
                # Read again next run: the bill's congress_bills row may arrive with a later BILLSTATUS read.
                outcome.held.pop(folder, None)
            else:
                outcome.held[folder] = _entry_record(entry, acquisition.capture.observed_at)
    return outcome


def log_outcome(outcome: BillSumOutcome, congresses: Sequence[int]) -> None:
    logger.info(
        "Bill family BILLSUM: Congresses {} — {:,} listings, {:,} zips read, {:,} unchanged, {:,} past the cap; "
        "{:,} summaries BILLSTATUS does not state, {:,} it does; {:,} BILLSUM rows retired as BILLSTATUS now states "
        "them",
        list(congresses),
        outcome.listings,
        outcome.zips,
        outcome.unchanged,
        outcome.capped,
        len(outcome.rows),
        outcome.stated_by_status,
        len(outcome.superseded),
    )
    if outcome.refused_folders or outcome.refused_files or outcome.refused_summaries:
        logger.warning(
            "Bill family BILLSUM: {:,} folders, {:,} files and {:,} single summaries refused (an empty text is one)",
            outcome.refused_folders,
            outcome.refused_files,
            outcome.refused_summaries,
        )
    if outcome.orphans:
        logger.warning(
            "Bill family BILLSUM: {:,} summaries name a bill with no congress_bills row — their folders "
            "are read again next run",
            outcome.orphans,
        )
    if outcome.capped:
        logger.warning(
            "Bill family BILLSUM: {:,} folders past the zip cap keep their entries for a later run", outcome.capped
        )
