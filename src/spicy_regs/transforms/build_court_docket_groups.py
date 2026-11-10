"""Build ``court_docket_groups.parquet``: which published ``court_dockets`` rows are records of one case.

CourtListener can hold one case as several docket objects, and leaves its own ``parent_docket_id`` blank in every
row of the bulk editions. Two kinds are the publisher's own: a criminal case's per-defendant sub-dockets, which PACER
opens together under one docket number with consecutive case ids (its "doppeldocket" problem, FLP wiki, issue
#2185), and duplicate records of one filing, made when a record arrives with a different PACER case id for the same
number (its Docket is unique on docket number, PACER case id and court), an older docket object reused for a new case
among them.

Rule version 2, over one native bulk edition:

* Candidate groups are the native records sharing a published row's ``(court_id, docket_number)``; only records in
  the published selection are members, so every representative resolves in ``court_dockets``.
* Every member must be the same case as the member with the longest caption, under :func:`same_case`, the test the
  APA enrichment admitted sibling records with. A key that fails it holds a reused number and stays ungrouped.
* A member's PACER case id is *in sequence* when it lies within 10% of the median id of its court's dockets
  numbered within five serials of it (same office, year and type): CM/ECF assigns the serial and the case id
  together. Appellate numbers have no serial, so their keys have no sequence.
* The representative row has the lowest in-sequence id; with none in sequence, the lowest numeric id (rule 1's
  choice). Ties go to the lower ``cl_docket_id``.
* The tier is :data:`PER_DEFENDANT_DOCKETS` when a member carries ``federal_defendant_number`` or the ids the
  representative was chosen from run consecutively with at most :data:`MAX_MISSING_IDS` missing (rule 1's "spread at
  most five" for a pair); otherwise :data:`DUPLICATE_RECORDS`.

Rule 1 (2026-09-23) grouped identical captions only, took the lowest numeric id even where it was a reused or foreign
object's, and called a wide id spread ``refiled``, which a key of court and number cannot observe: 54 of its 55
``refiled`` groups had one filing date, and 11 parents lay outside their court's sequence (round-6 audit, M5).

Membership and neighbour ids come from one scan of the native edition with Python-side set logic, the pattern the
enrichment used after a DuckDB parallel-join anomaly. The edition is CourtListener's own ``dockets-<date>.csv.bz2``,
read by SpicyDocs' ``CourtListenerLocalDump`` (about 5 GB compressed; every record is decoded, seven columns are
kept, and only the published keys' courts reach Python), so the raw archive's copy is the only input it needs.
"""

from __future__ import annotations

import json
import re
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from loguru import logger
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.court_receipts import file_witness, write_court_rows

RULE_VERSION = "2"

#: What the group's rows are.
PER_DEFENDANT_DOCKETS = "per_defendant_dockets"
DUPLICATE_RECORDS = "duplicate_records"
#: What showed the rows are one case: every caption reads the same, or (on some member) one caption is contained in
#: another filed the same day.
SAME_CAPTION = "same_caption"
SAME_DATE_CONTAINED_CAPTION = "same_date_contained_caption"

#: Consecutive ids may skip this many and still be one PACER opening: rule 1's spread of five for a pair.
MAX_MISSING_IDS = 4
#: Dockets numbered within this many serials of a key are its court's sequence there.
NEIGHBOUR_SERIALS = 5
#: A real CM/ECF case id has at most nine digits; RECAP's 14-15 digit values are not one.
MAX_CASE_ID_DIGITS = 9

INPUT_SCHEMA = pa.schema([
    ("cl_docket_id", pa.string()),
    ("parent_cl_docket_id", pa.string()),
    ("confidence_tier", pa.string()),
    ("group_size", pa.int64()),
    ("edition", pa.string()),
    ("rule_version", pa.string()),
    ("match_basis", pa.string()),
])
SCHEMA = SUBJECT_SCHEMAS['court_docket_groups']

_NATIVE_COLUMNS = ["id", "court_id", "docket_number", "pacer_case_id", "case_name", "date_filed",
                   "federal_defendant_number"]
#: A district docket number: office, two-digit year, case type, serial (``1:20-cv-01104``).
_DISTRICT_NUMBER = re.compile(r"(\d+):(\d{2})-([a-z]+)-(\d+)")
_ABBREVIATIONS = (("u.s.", "united states"), ("u.s", "united states"), ("dept.", "department"), ("dept", "department"),
                  ("&", "and"))


def caption_key(caption: str | None) -> str:
    """A caption as :func:`same_case` compares it: lowercase, U.S. and Dept. spelled out, ``&`` as ``and``, every run
    of other characters one space."""
    value = (caption or "").lower()
    for short, long in _ABBREVIATIONS:
        value = value.replace(short, long)
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def same_case(a: Mapping, b: Mapping) -> str | None:
    """How two records of one court and docket number show one case, or ``None``: the same caption, or the same filing
    date with one caption contained in the other. A record with no caption shows nothing."""
    first, second = caption_key(a["case_name"]), caption_key(b["case_name"])
    if not first or not second:
        return None
    if first == second:
        return SAME_CAPTION
    if a["date_filed"] and a["date_filed"] == b["date_filed"] and (first in second or second in first):
        return SAME_DATE_CONTAINED_CAPTION
    return None


def group_members(members: Sequence[Mapping], sequence: float | None = None) -> tuple[str, str, str] | None:
    """(representative id, tier, match basis) for one candidate group, or ``None`` to leave it ungrouped.

    ``sequence`` is the median PACER id of the court's neighbouring dockets, or ``None`` where there are none.
    """
    if len(members) < 2:
        return None
    anchor = max(members, key=lambda member: (len(caption_key(member["case_name"])), member["id"]))
    bases = {same_case(member, anchor) for member in members}
    if None in bases:
        return None
    numeric = sorted((int(member["pacer_case_id"]), member["id"]) for member in members
                     if (member["pacer_case_id"] or "").isdigit())
    if not numeric:
        return None
    ids = [pair for pair in numeric if sequence is not None and 9 * sequence <= 10 * pair[0] <= 11 * sequence] or numeric
    consecutive = len(ids) >= 2 and ids[-1][0] - ids[0][0] - (len(ids) - 1) <= MAX_MISSING_IDS
    defendants = any(member.get("federal_defendant_number") for member in members)
    basis = SAME_DATE_CONTAINED_CAPTION if SAME_DATE_CONTAINED_CAPTION in bases else SAME_CAPTION
    return ids[0][1], PER_DEFENDANT_DOCKETS if defendants or consecutive else DUPLICATE_RECORDS, basis


def _neighbour_numbers(number: str) -> list[str]:
    """The district docket numbers within :data:`NEIGHBOUR_SERIALS` serials of ``number``; none for another shape."""
    match = _DISTRICT_NUMBER.match(number)
    if match is None:
        return []
    office, year, kind, serial = match.groups()
    return [f"{office}:{year}-{kind}-{int(serial) + step:0{len(serial)}d}"
            for step in range(-NEIGHBOUR_SERIALS, NEIGHBOUR_SERIALS + 1) if step and int(serial) + step > 0]


def build_court_docket_groups(output_dir: Path, *, dockets_file: Path, native_file: Path, edition: str) -> Path:
    """Write the side table for the published ``dockets_file`` from one native bulk edition's ``.csv.bz2``."""
    published = pq.read_table(dockets_file, columns=["cl_docket_id", "court_id", "docket_number"]).to_pylist()
    published_ids = {row["cl_docket_id"] for row in published}
    per_key: dict[tuple[str, str], int] = defaultdict(int)
    for row in published:
        if row["docket_number"]:
            per_key[(row["court_id"], row["docket_number"])] += 1
    keys = {key for key, count in per_key.items() if count > 1}
    wanted: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for court, number in keys:
        for neighbour in _neighbour_numbers(number):
            wanted[(court, neighbour)].append((court, number))

    candidates: dict[tuple[str, str], list[dict]] = defaultdict(list)
    neighbour_ids: dict[tuple[str, str], list[int]] = defaultdict(list)
    from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

    scanned = 0
    output_dir.mkdir(parents=True, exist_ok=True)
    dump = CourtListenerLocalDump(native_file, columns=_NATIVE_COLUMNS, work_dir=output_dir)
    courts = pa.array(sorted({court for court, _ in keys}), pa.string())
    # Only a published key's records and its neighbours' reach Python: a few thousand of the courts' millions.
    looked_up = pa.array(sorted(f"{court}\x1f{number}" for court, number in keys | wanted.keys()), pa.string())
    # call_function: the type checker cannot see pyarrow.compute's generated wrappers.
    in_courts, looked_up_keys = pc.SetLookupOptions(value_set=courts), pc.SetLookupOptions(value_set=looked_up)
    for batch in dump.iter_batches():
        batch = batch.filter(pc.call_function("is_in", [batch.column("court_id")], in_courts))
        scanned += batch.num_rows
        key_text = pc.call_function("binary_join_element_wise",
                                    [batch.column("court_id"), batch.column("docket_number"), pa.scalar("\x1f")])
        for row in batch.filter(pc.call_function("is_in", [key_text], looked_up_keys)).to_pylist():
            key = (row["court_id"], row["docket_number"])
            if key in keys and row["id"] in published_ids:
                candidates[key].append(row)
            pacer = row["pacer_case_id"] or ""
            if key in wanted and pacer.isdigit() and len(pacer) <= MAX_CASE_ID_DIGITS:
                for owner in wanted[key]:
                    neighbour_ids[owner].append(int(pacer))

    if not dump.completed:
        raise RuntimeError(f"CourtListener dockets: {native_file.name} was not read to its end")

    rows, stats = [], {"groups": 0, PER_DEFENDANT_DOCKETS: 0, DUPLICATE_RECORDS: 0, SAME_CAPTION: 0,
                       SAME_DATE_CONTAINED_CAPTION: 0, "ungrouped": 0}
    for key, members in sorted(candidates.items()):
        sequence = statistics.median(neighbour_ids[key]) if neighbour_ids.get(key) else None
        decision = group_members(members, sequence)
        if decision is None:
            stats["ungrouped"] += 1
            continue
        representative, tier, basis = decision
        stats["groups"] += 1
        stats[tier] += 1
        stats[basis] += 1
        rows.extend({"cl_docket_id": member["id"], "parent_cl_docket_id": representative, "confidence_tier": tier,
                     "group_size": len(members), "edition": edition, "rule_version": RULE_VERSION,
                     "match_basis": basis}
                    for member in members)
    rows.sort(key=lambda row: row["cl_docket_id"])
    if {row["parent_cl_docket_id"] for row in rows} - published_ids:
        raise RuntimeError("a group representative is missing from the published court_dockets selection")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = write_court_rows('court_docket_groups', rows, output_dir,
                                witnesses=[file_witness(dockets_file), file_witness(native_file)])
    receipt = {"edition": edition, "rule_version": RULE_VERSION, "dockets_file": str(dockets_file),
               "native_file": str(native_file), "published_rows": len(published), "native_rows_read": dump.rows,
               "native_rows_scanned": scanned,
               "keys_with_neighbours": sum(1 for key in keys if neighbour_ids.get(key)),
               "grouped_rows": len(rows), **stats}
    (output_dir / "court_docket_groups.parquet.receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    logger.info("Court docket groups: {:,} groups over {:,} rows ({:,} per-defendant, {:,} duplicate records)",
                stats["groups"], len(rows), stats[PER_DEFENDANT_DOCKETS], stats[DUPLICATE_RECORDS])
    return out_file
