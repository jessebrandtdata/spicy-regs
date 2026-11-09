"""Transform: build ``cfr_sections.parquet`` from the GovInfo CFR API and annual volume XML.

All-VARCHAR section metadata, one row per CFR granule (a section-level unit
within a title's annual edition) keyed on ``granule_id``. ``cfr_ref`` is the
join key back to Federal Register ``cfr_references_json``; ``title``, ``part``
and ``section`` describe structure. SECTION METADATA ONLY — the full
regulatory text is deliberately out of scope (see ``sources/cfr_sections.py``).

Incremental by design: best-effort prior from R2, fetch bounded by
``since_year``, dedup on ``granule_id`` preferring the fresh row, so a short
run never trips the R2 catastrophic-shrink guard. With no prior table the
output covers only the requested year window, not a full historical backfill.
GovInfo's annual index package (``GPO-CFR-INDEX-2025``) is not a title volume:
the walk skips it and its prior rows are dropped.

Neither a granule id nor a printed section number establishes the enclosing
part, so section granules are placed from their package's annual volume XML:
one download and one streaming SpicyDocs scan per package, then a lookup per
granule token among the scan's canonical sections, O(volume bytes + granules).
``part`` is the innermost PART heading. Title 43 numbers sections by subpart:
§ 1601.0-1 sits in Part 1600, so ``part`` is 1600 (structure) while only
``cfr_ref`` (``43-1601.0-1``, the printed citation) joins the Federal Register
side. Title 41's compound parts keep their hyphen (``50-201``) and Title 14
Part 241 prints ``19-8.1``. ``cfr_ref`` is NULL for ranges, publisher typos,
unprefixed numbers and parenthesized citations (SpicyDocs'
``split_annual_cfr_section``). The volume passes SpicyDocs' annual identity
validator before it is scanned; an empty scan places nothing.

Placement changes only the rows the scan holds, plus the part of a section's
appendix or TOC (``sec746-10-app1``) whose host section (``746-10``) it holds.
Every other granule keeps its identifier-derived values: TOC, NODE and
appendix part tokens, and for an appendix or TOC of a section the scan does
not hold, the token's leading number, as published before cac7615. A plain
section token the scan does not hold keeps an unknown part. Title 41's part
granule ids name only the chapter (``part60``, ``part60-id334``), so its
structural rows take the compound part their part granule's heading states
(:func:`compound_parts`: "2-AFFIRMATIVE ACTION PROGRAMS" is ``60-2``), on
fresh rows and on the prior alike, with no volume read.

``part_granule`` is ``'true'`` on the granule that is a part itself (``…-part200``, ``…-part60-id334``) and
``'false'`` on every other row, read from the granule id by SpicyDocs' grammar, so a part citation (``2-200``) can
pick the part out of its subparts, appendices and tables of contents. It depends on the id alone: held rows take
it from their ids on every run (:func:`_read_prior`), so a prior published before the column gains it without a
re-read.

A package is re-placed only when it is new to the prior table or any of its
granules is new or carries a different ``last_modified``. GovInfo's granule
listing carries no ``lastModified``, so every row's stamp is its package's and
the check is per package in practice. An unchanged package keeps its prior
rows without a download. That is safe only when the prior rows were placed by
this rule, which the output records in its Parquet metadata
(``PLACEMENT_MARKER``): a prior without the marker (generation ``8fb97150`` and
earlier) re-places every package, and the output is marked only when no prior
package kept unplaced rows. ``replace_all`` forces a full re-placement.

A volume that cannot be downloaded, validated or scanned keeps that package's prior rows
and is annotated in the run log (an error when the package has no prior rows).
With no prior table at all, any failed volume refuses the run. A 401/403
refusal aborts the run.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.phase_log import annotate
from spicy_regs.transforms.parquet_rows import str_or_none
from spicy_regs.sources import r2
from spicy_regs.sources.cfr_sections import INDEX_PACKAGE_RE, CfrSectionsError, CfrSectionsReader
from spicy_regs.transforms.table_merge import merge_local_prior

if TYPE_CHECKING:
    from spicy_docs.sources.cfr.acquisition import CfrAcquirer
    from spicy_docs.sources.cfr.annual import AnnualCfrGranuleId

OUTPUT = "cfr_sections.parquet"

#: Byte cap for one annual volume's download and scan. The largest retained
#: 2025 volume (40 CFR vol 20) is 12,576,481 bytes; SpicyDocs caps any CFR
#: capture at 256 MiB (spicy-docs docs/sources/cfr.md).
MAX_VOLUME_BYTES = 64 * 1024 * 1024
_VOLUME_REQUESTS = 3  # per volume, retries of a 429/5xx/transport failure included
_VOLUME_TIMEOUT_SECONDS = 60.0
_VOLUME_REQUEST_INTERVAL_SECONDS = 1.0

#: Parquet key-value metadata recording that every section row was placed by
#: the annual-volume PART heading rule; bump the value when that rule changes.
#: A change in SpicyDocs' scan or split (0.31.0's single-section ``§§`` read) is
#: applied by a ``replace_all`` run recorded in the ledger, not by a marker bump.
PLACEMENT_MARKER = {"spicy_regs.cfr_sections.placement": "annual-volume-part-heading/1"}

# The published schema: all VARCHAR, in a fixed order. ``granule_id`` is the
# primary/dedup key.
COLUMNS = (
    "granule_id",
    "package_id",
    "cfr_ref",
    "title",
    "part",
    "section",
    "heading",
    "structure_level",
    "edition_year",
    "last_modified",
    "url",
    "part_granule",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


#: Title 41 numbers parts by chapter and part (60-1, 60-2), and GovInfo's part granule ids keep only the chapter
#: (``part60``, ``part60-id334``): the part survives as the leading ``N-`` of the granule's heading ("2-AFFIRMATIVE
#: ACTION PROGRAMS" is Part 60-2). The 2025 edition holds 190 such granules, the same 190 parts the volume scan places
#: Title 41's sections in. No other title numbers parts so; elsewhere that heading shape is prose (7 CFR 8 "4-H CLUB
#: NAME AND EMBLEM", 31 CFR 82 "5-CENT AND ONE-CENT COIN REGULATIONS", 49 CFR 1240 "1259-REPORTS"). The title, the
#: chapter's place in a granule id and the heading rule are spicy-docs' (``sources.cfr``, round 5 X3).


def _ids(granule_id: str, package_id: str | None = None) -> AnnualCfrGranuleId:
    """A GovInfo CFR granule id's tokens, read by spicy-docs' id grammar (edition, title, part, section, ...)."""
    from spicy_docs.sources.cfr import split_annual_cfr_granule_id

    return split_annual_cfr_granule_id(granule_id, package_id=package_id)


def _part_granule(ids: AnnualCfrGranuleId) -> str:
    """``'true'`` when the id names a part's own granule (not a subpart, appendix, TOC or section of it)."""
    return "true" if ids.part_granule is not None else "false"


def compound_parts(rows: Iterable[dict]) -> list[dict]:
    """Title 41's structural rows with the compound part their part granule's heading states; other rows unchanged.

    A part granule's own row and every non-section granule nested under its id (``…-part60-id334-subpartA``: the
    longest part-granule id prefixing it) take ``<chapter>-<N>`` and the ``cfr_ref`` that composes. A part granule of
    such a chapter whose heading states no number (the 35 "Reserved" ones of the 2025 edition) names no part: its
    rows take NULL rather than the chapter, which is no part and matched the Register's truncated chapter keys. The
    chapter is read from the id, never from ``part``, so a row already compound comes back unchanged. Section
    granules are the volume scan's (:func:`place_sections`). O(rows x id segments).
    """
    from spicy_docs.interpretation.citation_grammar import cfr_key
    from spicy_docs.sources.cfr import COMPOUND_PART_TITLE, compound_part

    rows = list(rows)
    stated: dict[str, tuple[str, str | None]] = {}
    for row in rows:
        chapter = _ids(row["granule_id"]).part_granule if row["title"] == COMPOUND_PART_TITLE else None
        if chapter:
            stated[row["granule_id"]] = (chapter, compound_part(chapter, row["heading"]))
    compound = {chapter for chapter, part in stated.values() if part}
    parts = {granule_id: part for granule_id, (chapter, part) in stated.items() if chapter in compound}
    if not parts:
        return rows
    compounded = []
    for row in rows:
        prefix = row["granule_id"]
        if row["title"] == COMPOUND_PART_TITLE and not _ids(prefix).section_token:
            while prefix and prefix not in parts:
                prefix = prefix.rpartition("-")[0]
        if not prefix or prefix not in parts:
            compounded.append(row)
            continue
        part = parts[prefix]
        compounded.append({**row, "part": part, "cfr_ref": cfr_key(row["title"], part, row["section"])})
    return compounded


def _shape(granule: dict) -> dict:
    """Map one raw GovInfo CFR granule onto the published all-VARCHAR shape, from its identifiers.

    Section granules are placed afterwards by ``place_sections``. Missing
    optional facts remain NULL; a missing source identity refuses.
    """
    granule_id = granule.get("granuleId")
    if not isinstance(granule_id, str) or not granule_id.strip():
        raise CfrSectionsError("CFR row requires a nonempty granuleId")
    from spicy_docs.interpretation.citation_grammar import cfr_key

    package_id = str_or_none(granule.get("_package_id") or granule.get("packageId"))
    # Title and edition from the package id where one is stated (always well formed), else the granule id; part and
    # section from the granule id (both nullable -- see the module docstring). The edition falls back to dateIssued.
    ids = _ids(granule_id, package_id)
    edition_year = ids.edition_year
    if edition_year is None:
        date_issued = str_or_none(granule.get("dateIssued"))
        edition_year = date_issued[:4] if date_issued else None

    return {
        "granule_id": granule_id,
        "package_id": package_id,
        "cfr_ref": cfr_key(ids.title, ids.part, ids.section),
        "title": ids.title,
        "part": ids.part,
        "section": ids.section,
        # The granule list-level ``title`` field is the heading text.
        "heading": str_or_none(granule.get("title")),
        "structure_level": str_or_none(granule.get("granuleClass")),
        "edition_year": edition_year,
        "last_modified": str_or_none(granule.get("lastModified") or granule.get("_package_last_modified")),
        "url": f"https://www.govinfo.gov/app/details/{package_id}/{granule_id}" if package_id and granule_id else None,
        "part_granule": _part_granule(ids),
    }


def place_sections(rows: Iterable[dict], xml: bytes) -> list[dict]:
    """Place each section granule of one package under its PART heading in the package's volume XML.

    The scan refuses a non-volume root and unsafe or oversized XML. A granule
    is looked up by token among the ``canonical`` sections only; nested,
    wrapped and revised copies are current text too. An appendix or TOC of a
    held section takes that section's part and keeps its other values; every
    other row the scan does not hold is returned unchanged.
    """
    from spicy_docs.sources.cfr.annual import scan_annual_cfr_sections, split_annual_cfr_section

    sections = {s.granule: s for s in scan_annual_cfr_sections(xml, max_bytes=MAX_VOLUME_BYTES) if s.canonical}

    placed = []
    for row in rows:
        ids = _ids(row["granule_id"])
        section = sections.get(ids.section_token) if ids.section_token else None
        if section is None:
            held = sections.get(ids.host_section) if ids.host_section else None
            placed.append(row if held is None else {**row, "part": held.part})
            continue
        number = split_annual_cfr_section(section.number, section.part, section.subpart)
        joins = number.citation is not None and number.citation_joins and row["title"] is not None
        cfr_ref = f"{row['title']}-{number.citation}" if joins else None
        placed.append({**row, "part": section.part, "section": number.section, "cfr_ref": cfr_ref})
    return placed


def prepare_retained_cfr_rows(
    mods: bytes,
    xml: bytes,
    *,
    selection,
    mods_url: str,
    xml_url: str,
    expected_constituents: int,
    expected_sections: int,
    max_bytes: int = MAX_VOLUME_BYTES,
) -> dict:
    """Map one explicitly selected annual MODS inventory using maintained readers.

    Caller owns original/capture digest admission and receipt writing. Native
    edition and XML identity stay separate; no API modification date is inferred.
    Complete inventory expectations come from the selected manifest, not this code.
    """
    from dataclasses import asdict

    from spicy_docs.sources.cfr.annual import scan_annual_cfr_sections, validate_annual_cfr_xml
    from spicy_docs.sources.cfr.edition import _EditionFields, _parse_annual_cfr_metadata

    if (
        type(expected_constituents) is not int
        or expected_constituents < 1
        or type(expected_sections) is not int
        or expected_sections < 0
    ):
        raise ValueError("retained CFR requires explicit complete inventory counts")
    edition, metadata = _parse_annual_cfr_metadata(mods, selection=selection, final_url=mods_url, max_bytes=max_bytes)
    native = validate_annual_cfr_xml(xml, identity=selection, final_url=xml_url, max_bytes=max_bytes)
    if len(metadata.constituents) != expected_constituents:
        raise ValueError("complete retained MODS inventory differs")
    package = selection.package_id
    raw, contexts, seen = [], [], set()
    for position, record in enumerate(metadata.constituents):
        fields = _EditionFields(record)
        # The maintained reader raises for a missing required field.
        gid = cast(str, fields.value("extension", "accessId", required=True)).strip()
        if gid == package:
            if not (
                position == 0
                and fields.value("extension", "granuleClass", required=True) == "NODE"
                and fields.value("extension", "granuleLabel", required=True) == "title"
                and cast(str, fields.value("extension", "granuleNumber", required=True)).strip() == str(selection.title)
                and fields.value("extension", "sequenceNumber", required=True) == "1"
            ):
                raise ValueError("selected package title node identity differs")
        elif not gid.startswith(package + "-"):
            raise ValueError("outside selected CFR package")
        if gid in seen:
            raise ValueError("duplicate retained CFR constituent")
        seen.add(gid)
        raw.append(
            {
                "granuleId": gid,
                "_package_id": package,
                "title": fields.value("extension", "heading"),
                "granuleClass": fields.value("extension", "granuleClass"),
            }
        )
        contexts.append(
            {
                "constituent_position": position,
                "granule_id": gid,
                "native_selectors": ["extension/accessId", "extension/heading", "extension/granuleClass"],
            }
        )
    rows = compound_parts([_shape(row) for row in raw])
    canonical_sections = [
        (section.granule, position)
        for position, section in enumerate(scan_annual_cfr_sections(xml, max_bytes=max_bytes), 1)
        if section.canonical
    ]
    canonical = dict(canonical_sections)
    tokens = [_ids(row["granule_id"]).section_token for row in rows]
    section_tokens = [token for token in tokens if token is not None]
    if (
        len(canonical_sections) != len(canonical)
        or len(canonical) != expected_sections
        or len(section_tokens) != len(set(section_tokens))
        or len(section_tokens) != len(canonical)
        or set(section_tokens) != set(canonical)
    ):
        raise ValueError("complete unique XML-to-destination section coverage differs")
    placed = place_sections(rows, xml)
    if [row["granule_id"] for row in placed] != [row["granuleId"] for row in raw]:
        raise ValueError("retained destination inventory/order changed")
    for row, context in zip(placed, contexts, strict=True):
        if (
            set(row) != set(COLUMNS)
            or row["last_modified"] is not None
            or any(value is not None and not isinstance(value, str) for value in row.values())
        ):
            raise ValueError("retained destination schema/modification semantics differ")
        ids = _ids(row["granule_id"])
        token = ids.section_token or ids.host_section
        context.update(
            placement_class=(
                "canonical-section"
                if ids.section_token in canonical
                else "host-section-attachment"
                if ids.host_section in canonical
                else "structural-unplaced"
            ),
            annual_position=canonical.get(token) if token is not None else None,
        )
    return {"rows": placed, "contexts": contexts, "edition": asdict(edition), "native_identity": asdict(native)}


def _placed_package(acquirer: CfrAcquirer, package_id: str | None, rows: list[dict]) -> list[dict] | None:
    """The package's rows with section granules placed; ``None`` when its volume cannot be read.

    One download per package that has section granules, through
    ``acquire_annual``, whose identity validator is a second streaming pass
    before the scan: over the 262 retained volumes (236 from the 2025 edition,
    26 from 2026; 1.29 GB) it cost 14.7 s against the scan's 18.2 s, small
    beside the download (measured 2026-09-23), and SpicyDocs 0.31.0 admits all
    262, the combined Title 34/35 and appendix-only Title 40 vol 9 included. A
    failure, a refused volume among them, leaves the prior table's rows for the
    package in place (the merge keeps them); a 401/403
    (``CredentialRefusedError``) is not a volume failure and aborts.
    """
    from spicy_docs.sources.cfr.models import AnnualCfrSelection, CfrSourceError

    # A volume package names an annual volume; another CFR package (GPO-CFR-INDEX-2025) does not.
    volume = AnnualCfrSelection.from_package_id(package_id)
    if volume is None or not any(_ids(row["granule_id"]).section_token for row in rows):
        return rows

    try:
        return place_sections(rows, acquirer.acquire_annual(volume, max_bytes=MAX_VOLUME_BYTES).xml)
    except (CfrSourceError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("CFR: {} volume could not be placed ({})", package_id, error)
        return None


@dataclass(frozen=True)
class _Prior:
    """What the build needs from the prior table: its placement marker, row stamps and packages."""

    placed: bool
    modified: dict[str, str | None]
    packages: frozenset[str]


def _read_prior(prior_file: Path) -> _Prior:
    """Read the prior table, first rewriting it without the index package's rows, with Title 41's compound parts
    and with every row's ``part_granule`` read from its id.

    An unchanged package keeps its prior rows unread by this run, so the prior carries :func:`compound_parts` and
    ``part_granule`` itself: one id parse per held row, no volume read.
    """
    [(key, value)] = PLACEMENT_MARKER.items()
    placed = (pq.read_schema(prior_file).metadata or {}).get(key.encode()) == value.encode()
    table = pq.read_table(prior_file)
    rewrite = False
    packages = table["package_id"].to_pylist()
    keep = [package_id is None or not INDEX_PACKAGE_RE.fullmatch(package_id) for package_id in packages]
    if not all(keep):
        table = table.filter(pa.array(keep))
        packages = table["package_id"].to_pylist()
        rewrite = True
        logger.info("CFR: dropped {} prior index-package row(s); they are not CFR sections", keep.count(False))
    from spicy_docs.sources.cfr import COMPOUND_PART_TITLE

    compound = [title == COMPOUND_PART_TITLE for title in table["title"].to_pylist()]
    held = table.filter(pa.array(compound)).to_pylist()
    if (fixed := compound_parts(held)) != held:
        others = table.filter(pa.array([not row for row in compound]))
        table = pa.concat_tables([others, pa.Table.from_pylist(fixed, schema=table.schema)])
        packages = table["package_id"].to_pylist()
        rewrite = True
        logger.info(
            "CFR: {} prior Title {} row(s) took their compound part",
            sum(a != b for a, b in zip(fixed, held)),
            COMPOUND_PART_TITLE,
        )
    flags = pa.array([_part_granule(_ids(granule_id)) for granule_id in table["granule_id"].to_pylist()], pa.string())
    if "part_granule" not in table.column_names:
        table = table.append_column("part_granule", flags)
        rewrite = True
    elif not table["part_granule"].combine_chunks().equals(flags):
        table = table.set_column(table.column_names.index("part_granule"), "part_granule", flags)
        rewrite = True
    if rewrite:
        pq.write_table(table, prior_file, compression="zstd")
    return _Prior(
        placed,
        dict(zip(table["granule_id"].to_pylist(), table["last_modified"].to_pylist(), strict=True)),
        frozenset(package_id for package_id in packages if package_id is not None),
    )


def _unchanged(rows: list[dict], prior: dict[str, str | None]) -> bool:
    """Every granule is already in the prior table with the same ``last_modified`` (its package's stamp)."""
    return all(row["granule_id"] in prior and prior[row["granule_id"]] == row["last_modified"] for row in rows)


def _volume_acquirer() -> CfrAcquirer:
    """A paced, bounded GovInfo client for annual volume XML."""
    from spicy_docs.sources.cfr.acquisition import CfrAcquirer, CfrAcquisitionBudget

    budget = CfrAcquisitionBudget(
        max_requests=_VOLUME_REQUESTS,
        max_bytes=MAX_VOLUME_BYTES,
        timeout_seconds=_VOLUME_TIMEOUT_SECONDS,
        min_request_interval_seconds=_VOLUME_REQUEST_INTERVAL_SECONDS,
    )
    return CfrAcquirer(budget=budget)


def build_cfr_sections(
    output_dir: Path,
    *,
    since_year: int | None = None,
    replace_all: bool = False,
    acquirer: CfrAcquirer | None = None,
    granules: Iterable[dict] | None = None,
    download_prior=None,
) -> Path:
    """Build ``cfr_sections.parquet`` (incremental merge with the prior table).

    ``replace_all`` re-places every listed package whatever the prior table
    holds; ``acquirer`` replaces the default volume client (for hermetic replay).
    """
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_cfr_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or (download_prior or r2.download)(OUTPUT, prior_file)
    if have_prior:
        logger.info("CFR: merging against prior table {}", prior_file)
    else:
        logger.info("CFR: no prior table found — output covers the selected year window")

    # 2. Complete the selected traversal before downloading any volume or writing output.
    records = CfrSectionsReader(since_year=since_year).iter_records() if granules is None else granules
    packages: dict[str | None, list[dict]] = {}
    for granule in records:
        row = _shape(granule)
        packages.setdefault(row["package_id"], []).append(row)
    packages = {package_id: compound_parts(rows) for package_id, rows in packages.items()}

    # 3. Re-place only changed packages, and only against a prior this rule placed
    # (see module docstring); an unchanged package or a failed volume keeps its prior rows.
    prior = _read_prior(prior_file) if have_prior else None
    stamps = prior.modified if prior is not None and prior.placed and not replace_all else {}
    if prior is not None and (replace_all or not prior.placed):
        reason = "replace_all was requested" if replace_all else "the prior table lacks the placement marker"
        logger.info("CFR: re-placing every listed package: {}", reason)
    rows: list[dict] = []
    replaced: set[str | None] = set()
    failed: list[str | None] = []
    unchanged = 0
    with acquirer or _volume_acquirer() as volumes:
        for package_id, shaped in packages.items():
            if _unchanged(shaped, stamps):
                unchanged += 1
                continue
            placed = _placed_package(volumes, package_id, shaped)
            if placed is None:
                failed.append(package_id)
            else:
                rows.extend(placed)
                replaced.add(package_id)
    if failed and prior is None:
        raise CfrSectionsError(
            f"CFR: no prior table to keep, and {len(failed)} volume(s) failed: {', '.join(map(str, failed))}"
        )
    for package_id in failed:
        assert prior is not None
        if package_id in prior.packages:
            annotate("warning", "cfr_sections", f"{package_id} volume failed; its prior rows stand")
        else:
            annotate("error", "cfr_sections", f"{package_id} volume failed and it has no prior rows; its granules are absent")
    # Marked output promises every row was placed by this rule: true when the prior
    # was, or there was none, or every prior package was re-placed this run.
    marked = prior is None or prior.placed or (not failed and prior.packages <= replaced)
    if not marked:
        logger.warning("CFR: output left unmarked; the next run re-places every package")
    new_file = output_dir / "_cfr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info(
        "CFR: {:,} granules re-placed this run; prior rows kept for {} unchanged and {} failed packages",
        len(rows),
        unchanged,
        len(failed),
    )

    # 4. Merge prior + new, dedup on granule_id preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    merge_local_prior(
        con,
        columns=COLUMNS,
        identity="granule_id",
        order_by="edition_year DESC, granule_id",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
        kv_metadata=PLACEMENT_MARKER if marked else None,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("CFR sections: {:,} rows", total)
    return out_file
