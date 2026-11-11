"""Transform: build ``unified_agenda.parquet`` from the reginfo.gov export.

Produces the pinned 18 all-VARCHAR columns keyed by (``rin``, ``agenda_edition``).
The Unified Agenda is a Tier-1 rulemaking-lifecycle source: keyed by RIN, it is
the upstream, forward-looking companion to ``federal_register`` (also RIN-keyed)
and the regulations.gov ``dockets``/``documents`` view — it lists actions
agencies *plan* to take, long before they publish.

Incremental by design. Each semiannual edition is a full re-statement of every
agency's active agenda, so re-fetching all historical editions every run would be
wasteful *and* would trip the R2 catastrophic-shrink guard on any short run.
Instead we:

1. Best-effort download the prior ``unified_agenda.parquet`` from R2.
2. Fetch only the requested edition(s), by default the newest edition the
   publisher's report page names (``newest_edition``), so a new edition is
   fetched the day it is listed without a code change.
3. Union prior + new and dedup on (``rin``, ``agenda_edition``), preferring the
   freshly fetched row so a re-run of an edition refreshes it in place.

With no prior table (first run) step 3 keeps only the freshly fetched editions.

Step 2 also backfills: each run fetches, newest first, up to
``BACKFILL_EDITIONS_PER_RUN`` editions of ``edition_series`` that the prior table
lacks, so historical RINs cited by ``dockets`` and ``federal_register`` resolve.
Measured 2026-09-26: the 58 readable editions from 1995 hold 233,250 rows over
46,247 RINs and take about 15 s each to fetch; with them, 93.1% of distinct
``dockets.rin`` and 83.0% of ``federal_register.rin`` values name a held RIN, up
from 11.5% and 5.7% with the Fall 2025 edition alone.

The reginfo.gov XML export the reader parses is documented in
:mod:`spicy_regs.sources.unified_agenda`; :func:`_shape` maps the reader's
normalized per-RIN dict onto the pinned 18 columns.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from collections.abc import Callable, Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.parquet_rows import str_or_none
from spicy_regs.sources import r2
from spicy_regs.sources.unified_agenda import DEFAULT_EDITION, UnifiedAgendaReader, newest_edition

OUTPUT = "unified_agenda.parquet"

#: Unpublished spring editions. Exact retained 2004 originals are readable
#: through the shared, hash-qualified repair; changed malformed originals refuse.
UNREADABLE_EDITIONS = frozenset({"199504", "201204"})

#: Missing editions fetched per run, newest first. At least the 60 editions of
#: ``edition_series``, so the first generation under a new column is complete
#: rather than 37% filled for up to three days (run 41237549923). The runner took
#: 121 s for 21 editions, so 60 take about 6 minutes of the job's 60-minute timeout.
BACKFILL_EDITIONS_PER_RUN = 60

# The raw input schema: 18 columns, all VARCHAR; array-valued fields serialized
# as JSON strings. Primary / dedup key is (rin, agenda_edition). related_rins_json
# (SpicyDocs 0.67.0) is appended last so the earlier columns keep their positions.
COLUMNS = (
    "rin",
    "agency_code",
    "agency_name",
    "title",
    "abstract",
    "rin_status",
    "rule_stage",
    "priority_category",
    "agenda_edition",
    "major",
    "publication_id",
    "timetable_json",
    "cfr_references_json",
    "legal_authority_json",
    "first_action_date",
    "next_action_date",
    "url",
    "related_rins_json",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


# reginfo timetable dates are ``MM/DD/YYYY``; anything else (``To Be Determined``)
# never matches. ``strptime`` cannot replace this regex: ``%d`` rejects reginfo's
# ``00`` day at the format level, so the swap would silently drop the 2,922
# month-only dates in the published table.
_MDY = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def timetable_date(raw: object) -> date | None:
    """One timetable action's date, or ``None``.

    A date the calendar rejects (``02/30/2024``) is dropped, never clamped into a
    *different* real date; ``00`` stays reginfo's month-only marker for the 1st.
    """
    if not isinstance(raw, str) or not (m := _MDY.match(raw.strip())):
        return None
    month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(year, month, day or 1)
    except ValueError:
        # Warn only here. The regex miss above fires 538 times per edition on
        # reginfo's own "To Be Determined" and would bury this.
        logger.warning("Unified Agenda: dropped impossible timetable date {!r}", raw.strip())
        return None


def _iso_dates(timetable: list) -> list[str]:
    """Return the timetable's real action dates (:func:`timetable_date`) as sorted ISO ``YYYY-MM-DD``."""
    return sorted({day.isoformat() for entry in timetable if (day := timetable_date((entry or {}).get("date")))})


def _shape(doc: dict) -> dict:
    """Map one normalized Unified Agenda RIN dict onto the published column shape.

    The reader (:mod:`spicy_regs.sources.unified_agenda`) already normalizes the
    XML tags onto these keys. ``first_action_date`` / ``next_action_date`` are
    derived from the timetable (earliest action, then the earliest later action);
    ``url`` is the deterministic per-RIN reginfo detail page.
    """
    timetable = doc.get("timetable") or []
    dates = _iso_dates(timetable)
    first_action = dates[0] if dates else None
    next_action = None
    if dates:
        earliest = dates[0]  # str, not str | None — keeps the comparison well-typed
        next_action = next((d for d in dates if d > earliest), None)

    rin = str_or_none(doc.get("rin"))
    edition = str_or_none(doc.get("agenda_edition"))
    url = None
    if rin and edition:
        url = f"https://www.reginfo.gov/public/do/eAgendaViewRule?pubId={edition}&RIN={rin}"

    return {
        "rin": rin,
        "agency_code": str_or_none(doc.get("agency_code")),
        "agency_name": str_or_none(doc.get("agency_name")),
        "title": str_or_none(doc.get("title")),
        "abstract": str_or_none(doc.get("abstract")),
        "rin_status": str_or_none(doc.get("rin_status")),
        "rule_stage": str_or_none(doc.get("rule_stage")),
        "priority_category": str_or_none(doc.get("priority_category")),
        "agenda_edition": edition,
        "major": str_or_none(doc.get("major")),
        "publication_id": str_or_none(doc.get("publication_id")),
        "timetable_json": json.dumps(timetable),
        "cfr_references_json": json.dumps(doc.get("cfr_references") or []),
        "legal_authority_json": json.dumps(doc.get("legal_authority") or []),
        "first_action_date": first_action,
        "next_action_date": next_action,
        "url": url,
        # NULL, not [], when the edition states no RELATED_RIN_LIST (owner rule: absent lists are NULL).
        "related_rins_json": json.dumps(doc.get("related_rins")),
    }


def edition_series(latest: str = DEFAULT_EDITION) -> tuple[str, ...]:
    """The file stem of every readable edition from 1995 through ``latest``, oldest first."""
    from spicy_docs.sources.unified_agenda import LEGACY_FILE_STEMS

    stem_of = {publication: stem for stem, publication in LEGACY_FILE_STEMS.items()}
    return tuple(
        stem_of.get(edition, edition)
        for year in range(1995, int(latest[:4]) + 1)
        for edition in (f"{year}04", f"{year}10")
        if edition <= latest and edition not in UNREADABLE_EDITIONS
    )


def editions_to_fetch(held: set[str], latest: str = DEFAULT_EDITION) -> tuple[str, ...]:
    """The current edition, refreshed every run, then the newest editions the prior table lacks."""
    from spicy_docs.sources.unified_agenda import LEGACY_FILE_STEMS

    missing = [
        stem
        for stem in reversed(edition_series(latest))
        if stem != latest and LEGACY_FILE_STEMS.get(stem, stem) not in held
    ]
    return (latest, *missing[:BACKFILL_EDITIONS_PER_RUN])


def build_unified_agenda(
    output_dir: Path,
    *,
    editions: tuple[str, ...] | None = None,
    records: Callable[[str], Iterable[dict]] | None = None,
    download_prior: Callable[[str, Path], bool] | None = None,
    source_only: bool = False,
    source_receipts: list[dict] | None = None,
    evidence=None,
) -> Path:
    """Build incremental editions, or replay explicit offline editions without a prior.

    ``source_only`` requires a caller-selected offline reader. Success receipts
    supplied by that reader retain original pins and any exact historical repair.
    ``evidence`` (a ``CaptureEvidence``) retains each edition file the default
    reader fetches, and journals the editions' manifest as
    ``unified-agenda-editions``.
    """
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_ua_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means no merge base).
    if source_only and (editions is None or records is None):
        raise ValueError("Source-only Unified Agenda rebuild requires explicit editions and an offline records reader")
    if source_only and out_file.exists():
        raise ValueError("Source-only Unified Agenda rebuild refuses an existing output")
    if source_only and prior_file.exists():
        raise ValueError("Source-only Unified Agenda rebuild refuses a prior merge file")
    have_prior = False if source_only else prior_file.exists() or (download_prior or r2.download)(OUTPUT, prior_file)
    if have_prior:
        logger.info("Unified Agenda: merging against prior table {}", prior_file)
    else:
        logger.info("Unified Agenda: no prior table found — starting fresh")

    # 2. Fetch + shape the requested edition(s) into a "new rows" parquet: the
    # current edition plus a bounded backfill of editions the prior table lacks.
    if editions is None:
        # An edition is held once any of its rows was read for related_rins (SpicyDocs 0.67.0). Rows restored
        # from a regulations-native-v1 generation carry SQL NULL there (a read edition that states no list
        # carries the JSON text "null"), so the bounded backfill re-reads editions that are entirely v1 instead
        # of republishing them with the column empty. One read row suffices: a v2 re-read that no longer lists
        # a v1 row leaves that row NULL, and requiring every row would re-fetch the edition every day (ss-cdaa).
        held = set()
        if have_prior and "related_rins_json" in pq.read_schema(prior_file).names:
            held = {
                row[0]
                for row in duckdb.sql(
                    f"SELECT agenda_edition FROM read_parquet('{prior_file}') "
                    "GROUP BY 1 HAVING count(related_rins_json) > 0"
                ).fetchall()
            }
        editions = editions_to_fetch(held, newest_edition())
    logger.info("Unified Agenda: fetching editions {}", ", ".join(editions))
    # One reader per edition: each acquisition carries its own request budget.
    source_receipts = [] if source_receipts is None else source_receipts
    read = (
        records
        if records is not None
        else (lambda edition: UnifiedAgendaReader(editions=(edition,), on_source=source_receipts.append,
                                                  evidence=evidence).iter_records())
    )
    new_file = output_dir / "_ua_new.parquet"
    total_new = 0
    with pq.ParquetWriter(new_file, _SCHEMA, compression="zstd") as writer:
        rows = []
        for edition in editions:
            for doc in read(edition):
                rows.append(_shape(doc))
                if len(rows) == 1000:
                    writer.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA))
                    total_new += len(rows)
                    rows.clear()
        if rows:
            writer.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA))
            total_new += len(rows)
    logger.info("Unified Agenda: fetched {:,} entries this run", total_new)

    # 3. Merge prior + new, dedup on (rin, agenda_edition) preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if source_only:
        invalid_row = con.execute(
            "SELECT count(*) FROM (SELECT rin, agenda_edition FROM read_parquet(?) "
            "GROUP BY rin, agenda_edition HAVING count(*) > 1 OR rin IS NULL OR agenda_edition IS NULL)",
            [str(new_file)],
        ).fetchone()
        assert invalid_row is not None
        if invalid_row[0]:
            con.close()
            raise ValueError("Source-only Unified Agenda rebuild requires unique non-null native identities")
    if have_prior:
        # A prior written before a column existed (a 17-column table) merges with that column NULL.
        prior_names = set(pq.read_schema(prior_file).names)
        prior_cols = ", ".join(c if c in prior_names else f"NULL AS {c}" for c in COLUMNS)
        union = (
            f"SELECT {prior_cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY rin, agenda_edition ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE rin IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY agenda_edition DESC, rin
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    unread = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_file}') "
        "WHERE related_rins_json IS NULL AND agenda_edition IN "
        f"(SELECT DISTINCT agenda_edition FROM read_parquet('{new_file}'))"
    ).fetchone()
    con.close()
    if unread and unread[0]:
        logger.warning(
            "Unified Agenda: {:,} restored rows in editions read this run were not in the re-read; "
            "their related_rins stay unread (NULL)", unread[0])

    if evidence is not None:
        evidence.event("unified-agenda-editions", editions=source_receipts)
    if source_receipts:
        # The published table's file metadata pins this manifest and carries its repairs.
        encoded = (json.dumps(source_receipts, sort_keys=True, separators=(",", ":")) + "\n").encode()
        (output_dir / "unified_agenda_source_inputs.json").write_bytes(encoded)
        metadata = {
            b"unified_agenda_source_inputs_sha256": hashlib.sha256(encoded).hexdigest().encode(),
            b"unified_agenda_source_only": json.dumps(source_only).encode(),
            b"unified_agenda_repairs": json.dumps(
                [item["repair"] for item in source_receipts if item.get("repair")],
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
        }
        # DuckDB COPY drops key/value metadata. Rewrite in bounded batches,
        # preserving every value and adding compact provenance.
        parsed = pq.ParquetFile(out_file)
        annotated = output_dir / "_ua_annotated.parquet"
        with pq.ParquetWriter(annotated, parsed.schema_arrow.with_metadata(metadata), compression="zstd") as writer:
            for batch in parsed.iter_batches(batch_size=1000):
                writer.write_batch(batch)
        annotated.replace(out_file)

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (new_file,) if source_only else (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Unified Agenda: {:,} rows", total)
    return out_file
