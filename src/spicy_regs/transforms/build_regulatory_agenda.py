"""Transform: build ``regulatory_agenda_items.parquet`` and ``agenda_item_proceedings.parquet``.

Materializes durable agenda items (one per RIN) and provenance-bearing action
links from Unified Agenda observations and independently assembled proceedings.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import normalize_regsgov_identifier, normalize_rin
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    eastern_day_text,
    iter_parquet_rows,
    parse_json_list,
    stable_id,
    write_parquet_rows,
)

from spicy_regs.ontology.federal_register import FederalRegisterIndex, record_url, references_json
from spicy_regs.ontology.rins import proceeding_rins

ITEMS_OUTPUT = "regulatory_agenda_items.parquet"
RELATIONSHIPS_OUTPUT = "agenda_item_proceedings.parquet"
# v3: first_seen/last_seen and evidence_date are the Eastern day of a Regulations.gov
# instant, not its UTC day. Still v3 under proceedings v6 (listed FR dockets, decisions 32
# and 33): this code did not change, though its rows move with the proceedings they link
# (1,834 relationship rows and 496 items on the 2026-09-23 parents from the list join alone).
# v4: its FR references resolve through the index, and its rows move with the proceedings
# they link: SpicyDocs 0.35.0 reads a docket named after prose (D1) and folds Regulations.gov's
# typed FR-number separators (D2). A rulemaking snapshot records no package version, so the
# actor carries the reader change.
# v5 (one bump over published v4): an item tracks a proceeding only through a RIN the
# proceeding holds, so a Federal Register feed docket tracks none of the rules it posts; and its
# rows move with proceedings v9, whose feed shells go with their links (two, from FAA-2007-0004
# to 2120-AA64). Items a feed tracked keep their other links and count fewer (decision 32 as
# amended 2026-09-26).
# v6 (one bump over published v5): code unchanged; its rows move with proceedings v10. A
# proceeding held only through X-pattern codes is no longer formed (owner decision 61, NOAA's
# 0648-X… and as extended every agency's), so its RIN links go; and a Register document that
# joins a docketed proceeding (decision 56) takes its RIN links along. On the 2026-09-26
# parents decisions 58-61 alone took 6,648 relationships on 0648-X… codes from the published
# snapshot and left 1,658 items counting fewer or unresolved. From there, this code removes
# 25,465 relationships (22,204 on RINs, 2,750 on 0648-X… and 511 on other agencies' codes) and
# adds 24,977 on the proceedings that now hold them, 488 fewer in all; 8,097 items that linked
# several proceedings link one (unresolved to single_observed), and 119 that linked one link
# none (single_observed to unresolved).
ITEM_ACTOR_ID = "spicy-regs:regulatory-agenda-items:v6"
RELATIONSHIP_ACTOR_ID = "spicy-regs:agenda-item-proceedings:v6"

ITEM_COLUMNS = (
    "agenda_item_id",
    "rin",
    "scope_status",
    "scope_basis",
    "linked_proceeding_count",
    "observation_count",
    "latest_agenda_edition",
    "first_seen",
    "last_seen",
    *ATTESTATION_COLUMNS,
    "unresolved_fr_references_json",
)

RELATIONSHIP_COLUMNS = (
    "relationship_id",
    "agenda_item_id",
    "rin",
    "proceeding_id",
    "relationship_role",
    "source",
    "evidence_id",
    "evidence_uri",
    "evidence_date",
    *ATTESTATION_COLUMNS,
)

SOURCES = frozenset({"docket_rin", "document_rin", "federal_register_rin"})
SCOPE_STATUSES = frozenset({"recurring", "single_observed", "unresolved"})


def _agenda_date(edition: object) -> str | None:
    text = str(edition or "").strip()
    if re.fullmatch(r"\d{6}", text) and 1 <= int(text[4:]) <= 12:
        return f"{text[:4]}-{text[4:]}-01"
    return None


def _source_date(*values: object) -> str | None:
    """The latest Eastern day among the values (``eastern_day_text``), or ``None``."""
    dates = sorted(day for value in values if (day := eastern_day_text(value)))
    return dates[-1] if dates else None


def agenda_item_id(rin: str) -> str:
    """The durable agenda item a RIN identifies (``urn:rkaf:us:rin:<rin>``)."""
    return f"urn:rkaf:us:rin:{rin}"


def _source_uri(source: str, evidence_id: str) -> str:
    escaped = quote(evidence_id, safe="-._~")
    if source == "docket_rin":
        return f"https://www.regulations.gov/docket/{escaped}"
    if source == "document_rin":
        return f"https://www.regulations.gov/document/{escaped}"
    if source == "federal_register_rin":
        return record_url(evidence_id)
    raise ValueError(f"unknown agenda relationship evidence source: {source}")


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"regulatory agenda inputs missing from {output_dir}: {', '.join(missing)}")
    return paths


def build_regulatory_agenda(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
    fr_index: FederalRegisterIndex | None = None,
) -> tuple[Path, Path]:
    """Build one agenda item per RIN and provenance-bearing action links.

    Unified Agenda equality creates the item and its editioned observations,
    but never an action relationship: only a docket, regulations.gov document,
    or Federal Register artifact that directly reports the RIN can link the
    agenda item to an independently assembled Proceeding. Every day is
    ``eastern_day_text``. ``fr_index`` is the generation's shared index of
    ``federal_register.parquet``; it is built here when not supplied.
    """
    paths = _require_inputs(
        output_dir,
        (
            "dockets",
            "documents",
            "federal_register",
            "unified_agenda",
            "proceedings",
        ),
    )
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="regulatory-agenda",
    )
    item_provenance = context.provenance(
        method="deterministic",
        actor_id=ITEM_ACTOR_ID,
    )
    relationship_provenance = context.provenance(
        method="deterministic",
        actor_id=RELATIONSHIP_ACTOR_ID,
    )
    json_stats = JsonReadStats()
    fr_index = fr_index or FederalRegisterIndex(paths["federal_register"])
    unresolved_by_fr: dict[str, list[dict]] = defaultdict(list)
    unresolved_by_rin: dict[str, list[dict]] = defaultdict(list)

    proceedings_by_docket: dict[str, set[str]] = defaultdict(set)
    proceedings_by_fr_document: dict[str, set[str]] = defaultdict(set)
    rins_by_proceeding: dict[str, set[str]] = {}
    for row in iter_parquet_rows(
        paths["proceedings"],
        # Every column FederalRegisterIndex.proceeding_ids and proceeding_rins read.
        columns=(
            "proceeding_id",
            "docket_ids_json",
            "fr_document_ids_json",
            "fr_document_numbers_json",
            "unresolved_fr_references_json",
            "rins_json",
            "rin",
        ),
    ):
        proceeding_id = str(row.get("proceeding_id") or "").strip()
        if not proceeding_id:
            continue
        rins_by_proceeding[proceeding_id] = proceeding_rins(row, json_stats)
        dockets = parse_json_list(
            row.get("docket_ids_json"),
            stats=json_stats,
            table="proceedings",
            row_id=proceeding_id,
            column="docket_ids_json",
        )
        if dockets is not None:
            for value in dockets:
                if docket := normalize_regsgov_identifier(value):
                    proceedings_by_docket[docket].add(proceeding_id)
        fr_documents, unresolved = fr_index.proceeding_ids(row, json_stats)
        for identity in fr_documents:
            proceedings_by_fr_document[identity].add(proceeding_id)
        for reference in unresolved:
            for candidate in reference["candidate_ids"]:
                unresolved_by_fr[candidate].append(reference)

    observed_rins: set[str] = set()
    seen_dates: dict[str, set[str]] = defaultdict(set)
    observation_keys: dict[str, set[str]] = defaultdict(set)
    latest_agenda: dict[str, dict] = {}
    relationships: dict[tuple[str, str, str, str], dict] = {}

    def observe(rin: str, *dates: object) -> None:
        observed_rins.add(rin)
        seen_dates[rin].update(day for value in dates if (day := eastern_day_text(value)))

    def add_relationship(
        *,
        rin: str,
        proceeding_id: str,
        source: str,
        evidence_id: object,
        evidence_date: object,
    ) -> None:
        evidence = str(evidence_id or "").strip()
        # An item tracks a proceeding only through a RIN the proceeding holds: a catch-all
        # docket's documents report other rulemakings' RINs, which its proceeding does not take.
        if source not in SOURCES or not evidence or rin not in rins_by_proceeding.get(proceeding_id, ()):
            return
        key = (rin, proceeding_id, source, evidence)
        date = _source_date(evidence_date)
        relationships.setdefault(
            key,
            {
                "relationship_id": stable_id(
                    "agenda_proceeding_relationship",
                    *key,
                ),
                "agenda_item_id": agenda_item_id(rin),
                "rin": rin,
                "proceeding_id": proceeding_id,
                "relationship_role": "agenda_tracks_proceeding",
                "source": source,
                "evidence_id": evidence,
                "evidence_uri": _source_uri(source, evidence),
                "evidence_date": date,
                **relationship_provenance,
            },
        )
        if date:
            seen_dates[rin].add(date)

    for row in iter_parquet_rows(paths["dockets"], columns=("docket_id", "rin", "modify_date")):
        rin = normalize_rin(row.get("rin"))
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if not rin:
            continue
        observe(rin, row.get("modify_date"))
        if docket is None:
            continue
        targets = proceedings_by_docket.get(docket, set())
        if len(targets) == 1:
            add_relationship(
                rin=rin,
                proceeding_id=next(iter(targets)),
                source="docket_rin",
                evidence_id=docket,
                evidence_date=row.get("modify_date"),
            )

    for row in iter_parquet_rows(
        paths["documents"], columns=("document_id", "docket_id", "additional_rins", "posted_date", "modify_date")
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        if raw_rins is None:
            continue
        for value in raw_rins:
            rin = normalize_rin(value)
            if not rin:
                continue
            evidence_date = _source_date(
                row.get("modify_date"),
                row.get("posted_date"),
            )
            observe(rin, evidence_date)
            if docket is None:
                continue
            targets = proceedings_by_docket.get(docket, set())
            if len(targets) == 1:
                add_relationship(
                    rin=rin,
                    proceeding_id=next(iter(targets)),
                    source="document_rin",
                    evidence_id=row.get("document_id"),
                    evidence_date=evidence_date,
                )

    for row in iter_parquet_rows(
        paths["federal_register"], columns=("document_number", "publication_date", "regulation_id_numbers_json")
    ):
        document_number = str(row.get("document_number") or "").strip()
        identity = fr_index.record_id(row)
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        if raw_rins is None:
            continue
        for value in raw_rins:
            rin = normalize_rin(value)
            if not rin:
                continue
            observe(rin, row.get("publication_date"))
            unresolved_by_rin[rin].extend(unresolved_by_fr[identity])
            targets = proceedings_by_fr_document.get(identity, set())
            if len(targets) == 1:
                add_relationship(
                    rin=rin,
                    proceeding_id=next(iter(targets)),
                    source="federal_register_rin",
                    evidence_id=identity,
                    evidence_date=row.get("publication_date"),
                )

    for row in iter_parquet_rows(
        paths["unified_agenda"],
        columns=("rin", "agenda_edition", "priority_category", "first_action_date", "next_action_date"),
    ):
        rin = normalize_rin(row.get("rin"))
        if not rin:
            continue
        edition = str(row.get("agenda_edition") or "").strip()
        edition_date = _agenda_date(edition)
        observe(
            rin,
            edition_date,
            row.get("first_action_date"),
            row.get("next_action_date"),
        )
        # One observation per edition: the page it was read from is a function of RIN and edition (ss-vse3; the
        # former (edition, url) key counted the same on all 46,562 RINs of 2026-11-10).
        observation_keys[rin].add(edition)
        if rin not in latest_agenda or edition > str(latest_agenda[rin].get("agenda_edition") or ""):
            latest_agenda[rin] = row

    linked_proceedings: dict[str, set[str]] = defaultdict(set)
    for row in relationships.values():
        linked_proceedings[row["rin"]].add(row["proceeding_id"])

    item_rows: list[dict] = []
    for rin in sorted(observed_rins):
        linked_count = len(linked_proceedings.get(rin, ()))
        latest = latest_agenda.get(rin, {})
        priority = " ".join(str(latest.get("priority_category") or "").casefold().split())
        if priority == "routine and frequent":
            scope_status = "recurring"
            scope_basis = "latest_agenda_priority_routine_and_frequent"
        elif linked_count == 1:
            scope_status = "single_observed"
            scope_basis = "one_evidence_linked_proceeding"
        elif linked_count == 0:
            scope_status = "unresolved"
            scope_basis = "zero_evidence_linked_proceedings"
        else:
            scope_status = "unresolved"
            scope_basis = "multiple_evidence_linked_proceedings"
        dates = sorted(seen_dates.get(rin, ()))
        item_rows.append(
            {
                "agenda_item_id": agenda_item_id(rin),
                "rin": rin,
                "scope_status": scope_status,
                "scope_basis": scope_basis,
                "linked_proceeding_count": str(linked_count),
                "observation_count": str(len(observation_keys.get(rin, ()))),
                "latest_agenda_edition": latest.get("agenda_edition"),
                "first_seen": dates[0] if dates else None,
                "last_seen": dates[-1] if dates else None,
                **item_provenance,
                "unresolved_fr_references_json": references_json(unresolved_by_rin[rin]),
            }
        )

    relationship_rows = sorted(
        relationships.values(),
        key=lambda row: (
            row["rin"],
            row["proceeding_id"],
            row["source"],
            row["evidence_id"],
        ),
    )
    items_file = write_parquet_rows(
        output_dir / ITEMS_OUTPUT,
        columns=ITEM_COLUMNS,
        rows=item_rows,
    )
    relationships_file = write_parquet_rows(
        output_dir / RELATIONSHIPS_OUTPUT,
        columns=RELATIONSHIP_COLUMNS,
        rows=relationship_rows,
    )
    json_stats.log("regulatory_agenda")
    logger.info(
        "Regulatory agenda: {:,} items ({:,} recurring; {:,} unresolved), {:,} action relationships",
        len(item_rows),
        sum(row["scope_status"] == "recurring" for row in item_rows),
        sum(row["scope_status"] == "unresolved" for row in item_rows),
        len(relationship_rows),
    )
    assert pq.ParquetFile(items_file).schema_arrow.names == list(ITEM_COLUMNS)
    assert pq.ParquetFile(relationships_file).schema_arrow.names == list(RELATIONSHIP_COLUMNS)
    assert all(row["scope_status"] in SCOPE_STATUSES for row in item_rows)
    return items_file, relationships_file
