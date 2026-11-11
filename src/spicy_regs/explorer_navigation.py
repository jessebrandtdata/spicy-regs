"""Portable navigation over explicitly named source fields, never inferred column names.

The registry ships with table_joins.json. Python validation/measurement and the
browser consume the same key recipes. Installation and metadata rendering do
not read source rows. Existing source-occurrence SQL views remain available.
"""
from __future__ import annotations

import re
import json
from datetime import date, datetime
from pathlib import Path
from collections.abc import Mapping
from copy import deepcopy
from functools import lru_cache


def part(path: str, *, row: bool = False, transform: str | None = None) -> dict:
    value = {"path": path.split(".") if path else [], "from": "row" if row else "element"}
    if transform:
        value["transform"] = transform
    return value


def key(*parts: dict, separator: str = "", pattern: str = ".+") -> dict:
    return {"parts": list(parts), "separator": separator, "pattern": pattern}


def guard(path: str, *, row: bool = False, values: tuple[str, ...] = (), pattern: str | None = None) -> dict:
    return {**part(path, row=row), **({"values": list(values)} if values else {}),
            **({"pattern": pattern} if pattern else {})}


def route(table: str, columns: tuple[str, ...], keys: tuple[dict, ...], *guards: dict) -> dict:
    return {"table": table, "columns": list(columns), "keys": list(keys), "guards": list(guards)}


def array(identifier: str, source: str, fields: tuple[str, ...], targets: tuple[dict, ...], *,
          meaning: str, element_path: tuple[str, ...] = (),
          mode: str = "array", candidates: bool = False) -> dict:
    # receiptFields named what the site's receipt lookup read beside a reference; the lookup is retired
    # (ss-vse3, 2026-11-10), and the key stays, empty, because the deployed site refuses a recipe without it.
    return {"id": identifier, "source": source, "fields": list(fields), "targets": list(targets),
            "mode": mode, "candidates": candidates, "meaning": meaning, "receiptFields": [],
            "elementPath": list(element_path), "ruleVersion": "source-navigation/1"}


BILL_TYPES = ("hr", "s", "hres", "sres", "hjres", "sjres", "hconres", "sconres")
NUM = r"[1-9][0-9]*"
CONGRESS = key(part("congress"), pattern=NUM)
BILL_ID = key(part(""), pattern=r"[0-9]+-(hr|s|hres|sres|hjres|sjres|hconres|sconres)-[0-9]+")
NOMINATION = route("nominations", ("congress", "citation"), (
    CONGRESS, key(part("number", transform="nomination-citation"), part("part", transform="partition"))),
    guard("number", pattern=NUM), guard("congress", pattern=NUM), guard("part", pattern=r"00|0*[1-9][0-9]*"))
TREATY = route("treaties", ("congress_received", "number", "suffix"), (
    CONGRESS, key(part("number"), pattern=NUM), key(part("part", transform="partition-value"), pattern=r"[0-9]*")),
    guard("number", pattern=NUM), guard("congress", pattern=NUM),
    {**part("part", transform="partition-value"), "pattern": r"[0-9]*"})



def _regulatory_navigation() -> list[dict]:
    """Reuse native occurrence definitions; never pair independent memberships."""
    from spicy_regs.relationship_views.regulations_native import ARRAYS

    targets = {
        "proceeding": ("proceedings", "proceeding_id", ".+"),
        "regulations_docket": ("dockets", "docket_id", ".+"),
        "rin": ("unified_agenda", "rin", r"[0-9]{4}-[A-Z0-9]{4}"),
    }
    selected = {"comment_period_proceedings", "comment_period_dockets", "comment_period_rins",
                "document_additional_rins", "proceeding_dockets", "proceeding_rins",
                "proceeding_federal_register", "federal_register_rins"}
    specs = []
    for name, source, _identity, field, kind, value in ARRAYS:
        if name not in selected:
            continue
        if value != "item":
            raise ValueError("Portable regulatory membership requires its native item")
        if kind == "dated_federal_register":
            target = _dated_register_target("", row=False)
        else:
            table, column, pattern = targets[kind]
            target = route(table, (column,), (key(part(""), pattern=pattern),))
        specs.append(array(name, source, (field,), (target,),
                           meaning="Each independent native membership. Repeated occurrences remain visible; RINs return every selected agenda edition, with no latest choice or positional pairing."))
    specs.append(array("comment_period_evidence", "comment_periods", ("evidence_occurrences",), (
        route("documents", ("document_id",), (key(part("document_id")),),
              guard("source", values=("documents.comment_end_date",)),
              {**part("evidence_id"), "sameAs": part("document_id")}),
        route("federal_register", ("document_number", "publication_date"), (
            key(part("document_number"), pattern=r"[A-Za-z0-9._-]+"),
            key(part("publication_date", transform="canonical-date"))),
              guard("source", values=("federal_register.comments_close_on",)),
              {**part("evidence_id", transform="fr-document-number"), "sameAs": part("document_number")},
              {**part("evidence_id", transform="fr-publication-date"), "sameAs": part("publication_date")}),
    ), meaning="Each retained window or quiet notice follows its own source namespace. A Register reference requires its native number and publication date; period opening and closing dates do not supply that identity."))
    specs.append(array("lifecycle_date_evidence", "lifecycle_events", (), (
        route("documents", ("document_id",), (key(part("document_id", row=True)),),
              guard("dated_by", row=True, values=("regulations_gov",))),
        _dated_register_target("document_id", row=True,
                               guards=(guard("dated_by", row=True, values=("federal_register",)),)),
        route("regulatory_agenda_items", ("agenda_item_id",), (key(part("document_id", row=True)),),
              guard("dated_by", row=True, values=("unified_agenda",))),
    ), mode="row", meaning="The source that supplied the event date routes document_id, independently of the source that supplied its stage. The event date and evidence_id never manufacture a target key."))
    for name, source, field, target, target_field in (
        ("comment_document", "comments", "comment_on_document_id", "documents", "document_id"),
        ("register_document", "federal_register", "regulations_dot_gov_document_id", "documents", "document_id"),
        ("register_docket", "federal_register", "regulations_dot_gov_docket_id", "dockets", "docket_id"),
    ):
        specs.append(array(name, source, (), (
            route(target, (target_field,), (key(part(field, row=True)),)),
        ), mode="row", meaning="The publisher's explicit Regulations.gov reference. Object IDs, printed docket labels and dates do not supply this key."))
    specs.append(array("register_supporting_documents", "federal_register", ("regulations_dot_gov_supporting_documents",), (
        route("documents", ("document_id",), (key(part("document_id")),)),
    ), meaning="Each source-listed supporting document, including repeated occurrences. Listing a document does not establish that its file was retained."))
    specs.append(array("rule_target_register", "rule_targets", ("fr_document_ids", "fr_document_ids_json"), (
        _dated_register_target("", row=False),
    ), meaning="Each retained Register reference uses its own document number and publication date. Other rule dates never supply the target identity."))
    specs.append(array("gao_major_rule_agenda", "gao_reports", ("major_rule_rins", "major_rule_rins_json"), (
        route("unified_agenda", ("rin",), (key(part(""), pattern=r"[0-9]{4}-[A-Z0-9]{4}"),)),
    ), meaning="Each RIN explicitly listed for the major rule returns all held agenda editions. No latest edition or printed-title match is chosen."))
    return specs


def _dated_register_target(path: str, *, row: bool, guards: tuple = ()) -> dict:
    return route("federal_register", ("document_number", "publication_date"), (
        key(part(path, row=row, transform="fr-document-number")),
        key(part(path, row=row, transform="fr-publication-date"))), *guards)


def _citation_navigation() -> list[dict]:
    """Literal source-rule namespaces, never a target recovered from receipts."""
    resolved = {**part("target_resolved", row=True, transform="native-boolean"), "values": ["true"]}
    targets = []
    for kind, namespace, table, column, pattern, rules in (
        ("bill_number", "congress_bills", "congress_bills", "bill_id",
         r"[1-9][0-9]*-(hr|s|hres|sres|hjres|sjres|hconres|sconres)-[1-9][0-9]*",
         ("bill_number:inline_congress", "bill_number:congress_subheading")),
        ("public_law", "laws", "laws", "law_id", r"[1-9][0-9]*-public-[1-9][0-9]*", ("public_law",)),
        ("gao_product_id", "gao products (product id as GAO prints it)", "gao_reports", "citation_product_id", r"GAO-[A-Z0-9-]+", ("gao_product_id",)),
        ("crs_report_id", "crs reports (Congress.gov crsreport)", "crs_reports", "report_id", r"(R|RL|RS|IF|IN|LSB|IG|MM)[0-9]{4,6}", ("crs_report_id",)),
        ("bioguide_id", "members", "members", "bioguide_id", r"[A-Z][0-9]{6}", ("bioguide_id",)),
        ("docket_number", "dockets", "dockets", "docket_id", r"[A-Z0-9]+-[0-9]{4}-[A-Z0-9-]+", ("docket_number",)),
        ("committee_name", "committees", "committees", "system_code", r"[hs][hs][a-z0-9]+", ("exact", "roster_prefix")),
    ):
        targets.append(route(table, (column,), (key(part("target_key", row=True), pattern=pattern),),
                             resolved, guard("cite_kind", row=True, values=(kind,)),
                             guard("target_table", row=True, values=(namespace,)),
                             guard("target_rule", row=True, values=rules)))
    return [array("citation_recorded_targets", "document_citations", (), tuple(targets), mode="row",
                  meaning="The source rule's qualified, typed main reference. Target presence and uniqueness are checked separately; unresolved references, inferred committee names and unsupported namespaces remain inspectable without a link. Code, reporter and volume/page citations require complete typed identities and remain unsupported here.")]


def _recorded_source_navigation() -> list[dict]:
    # The court writer admits this exact native tuple against the recorded
    # selected parent. Lookup compares current main fields, not publication pins.
    captured = (
        {**part("sha1_matches", row=True, transform="native-boolean"), "values": ["true"]},
        {**part("actual_sha1", row=True), "sameAs": part("native_sha1", row=True)},
        guard("native_sha1", row=True, pattern=r"[a-f0-9]{40}"),
        guard("source_sha256", row=True, pattern=r"sha256:[a-f0-9]{64}"),
        guard("opinion_body_id", row=True, pattern=r"court-opinion-body:[a-f0-9]{64}"),
        guard("opinion_id", row=True, pattern=NUM),
        guard("cluster_id", row=True, pattern=NUM),
        guard("source_url", row=True, pattern=r"https://[^\s]+"),
        guard("parent_artifact_digest", row=True, pattern=r"(?:sha256:)?[a-f0-9]{64}"),
        guard("parent_member_sha256", row=True, pattern=r"(?:sha256:)?[a-f0-9]{64}"),
        guard("parent_member_byte_size", row=True, pattern=NUM),
    )
    specs = [array("court_captured_opinion", "court_opinion_pdf_extractions", (), (
        route("court_opinions", ("opinion_id", "cluster_id", "sha1", "download_url"),
              tuple(key(part(field, row=True)) for field in
                    ("opinion_id", "cluster_id", "native_sha1", "source_url")), *captured),
    ), mode="row", meaning="Recorded capture and parent data qualify this text. A match requires the opinion ID, opinion group, fingerprint and offered URL together. A match in current data does not prove it was the original parent publication."),
        array("court_captured_source", "court_opinion_pdf_extractions", (), (
            route("@url", ("url",), (key(part("source_url", row=True)),), *captured),
        ), mode="row", meaning="The literal source URL recorded for this qualified body. Following the publisher URL does not retrieve or verify the retained capture.")]
    # Derive the table set from maintained policies. Availability still requires
    # the selected main schema to publish each association field.
    for policy in sorted(Path(__file__).with_name("etl_policies").glob("scorecard*.json")):
        source = json.loads(policy.read_text())["dataset"]
        specs.append(array(source + "_recorded_source", source, (), (
            route("@url", ("url",), (key(part("source_url", row=True), pattern=r"https://[^\s]+"),),
                  guard("capture_id", row=True), guard("source_path", row=True)),
        ), mode="row", meaning="The publisher URL associated with the recorded capture ID and source path. Capture IDs are opaque; this URL does not imply a public capture or extracted document target."))
    return specs


# These retained context mappers have no individual source-record identity.
_FEC_CONTEXT_SOURCES = frozenset({
    "fec_collection_selection", "fec_historical_ie_statistics",
    "fec_reference_filing_feed_items", "fec_research_meeting_observations", "fec_reference_source_pages",
    "fec_filing_definition_evidence",
})
_FEC_DIGEST = r"sha256:[a-f0-9]{64}"


def _fec_context_route(source):
    """Return collection-anchor evidence guards, not fictional record keys."""
    if source in {"fec_record_evidence", "fec_filing_definition_evidence"}:
        return (
            guard("endpoint_kind", row=True, values=("collection_context",)),
            *_fec_witness_scope(),
            guard("context_column", row=True, values=("collection_outcome_json",)),
            guard("context_pointer", row=True, pattern="/.*"),
            guard("witness_sha256", row=True, pattern=_FEC_DIGEST),
        ), ("The collection containing this context or workbook witness, with its retained context column, JSON pointer "
            "and witness digest. Target scope is self and witness scope is the exact external generation pin. This is "
            "a collection anchor; release qualification must check the selected generation and the witness pointer/digest.")
    if source == "fec_collection_selection":
        return (guard("source_generation_pin", row=True, pattern=_FEC_DIGEST),
                guard("source_sha256", row=True, pattern=_FEC_DIGEST)), (
            "The retained source collection selected by this policy decision, under its source-generation pin and "
            "source digest; selection is not an individual source-record identity.")
    # Tables rebuilt from their retained originals (ss-j4vz B7-B9): each row names its original's digest.
    return (guard("source_sha256", row=True, pattern=_FEC_DIGEST),), (
        "The capture collection that retained the original this row was read from, with that original's digest. "
        "This collection anchor does not assert an individual source-record identity or qualify financial totals.")


def _fec_witness_scope():
    # Literal guards preserve scope labels. Selected-generation equality and
    # pointer/digest validation remain mandatory release qualification checks.
    return (
        guard("target_generation_scope", row=True, values=("self",)),
        guard("target_table", row=True, pattern="fec_[a-z_]+"),
        guard("target_record_id", row=True, pattern=_FEC_DIGEST),
        guard("witness_generation_scope", row=True, values=("external",)),
        guard("witness_generation_pin", row=True, pattern=_FEC_DIGEST),
    )


def declarations(processing_joins: tuple = ()) -> list[dict]:
    """Explicit source relationships, over published main fields only."""
    specs = [
        array("meeting_nominations", "committee_meetings", ("nomination_references_json",), (NOMINATION,),
              meaning="Nominations explicitly listed for this meeting; nomination partitions stay distinct."),
        array("meeting_treaties", "committee_meetings", ("treaty_references_json",), (TREATY,),
              meaning="Treaties explicitly listed for this meeting, with their own Congress and partition."),
        array("meeting_bills", "committee_meetings", ("bill_ids", "bill_ids_json"),
              (route("congress_bills", ("bill_id",), (BILL_ID,)),), meaning="Bills explicitly listed for this meeting."),
        array("meeting_committees", "committee_meetings", ("committees", "committees_json"),
              (route("committees", ("system_code",), (key(part("systemCode")),)),),
              meaning="Every publisher-listed committee; independent of meeting bills and witnesses."),
        array("nomination_committees", "nominations", ("committees_json",),
              (route("committees", ("system_code",), (key(part("systemCode")),)),),
              meaning="Committees whose nomination relationship list was read completely."),
        array("nomination_hearings", "nominations", ("hearings_json",),
              (route("hearing_transcripts", ("congress", "chamber", "jacket_number"), (
                  key(part("citation", transform="hearing-congress"), pattern=NUM), key(part("chamber", transform="lower")),
                  key(part("jacketNumber"), pattern=NUM))),),
              meaning="Printed hearings the nomination lists; no meeting or witness pairing is inferred."),
        array("fcc_filing_proceedings", "fcc_filings", ("proceedings",),
              (route("fcc_proceedings", ("name", "id_proceeding"), (key(part("name")), key(part("id_proceeding")))),),
              meaning="Match both the FCC proceeding name and native numeric ID; missing IDs remain unsupported."),
        array("fcc_filing_documents", "fcc_filings", ("documents",),
              (route("@url", ("url",), (key(part("src"), pattern=r"https://[^\s]+"),)),),
              meaning="Document URLs offered by the FCC; offering a URL does not establish capture or extraction."),
        array("fcc_extraction_results", "fcc_filings", ("extraction_results",),
              (route("@url", ("url",), (key(part("url"), pattern=r"https://[^\s]+"),), guard("url_status", values=("usable",))),),
              meaning="Recorded extraction URLs. Status and digest describe retained results; public capture and text access remain unverified."),
        array("house_communication_record", "house_communications", (),
              (route("record_issues", ("package_id",), (key(part("record_package_id", row=True)),)),),
              meaning="The exact Congressional Record package that supplied this communication.", mode="row"),
        array("member_fec_ids", "members", ("fec_ids", "fec_ids_json"),
              (route("fec_candidate_history", ("candidate_id",), (key(part(""), pattern=r"[HSP][0-9][A-Z0-9]{7}"),)),),
              meaning="Source-asserted candidate IDs; returns retained cycle observations, not an adjudicated person identity."),
    ]
    for source, identity in (("nominations", ("congress", "citation")),
                             ("committee_meetings", ("congress", "chamber", "event_id")),
                             ("house_communications", ("communication_id",))):
        specs.append(array(source + "_detail_attempts", source, (),
                           (route(source + "_detail_reads", identity,
                                  tuple(key(part(column, row=True)) for column in identity)),),
                           meaning="Detail read attempts for this exact record. Read and failed attempts stay visible; no recorded attempt does not mean an empty response.",
                           mode="row"))
    specs.append(array("house_record_enrichment_outcomes", "house_communications", (),
                       (route("house_record_enrichment_results", ("communication_id",),
                              (key(part("communication_id", row=True)),)),), mode="row",
                       meaning="Recorded enrichment checks for this communication, including incomplete reads, conflicts and exact passage witnesses."))
    specs += [
        array("bill_related_bills", "congress_bills", ("related_bills", "related_bills_json"),
              (route("congress_bills", ("bill_id",),
                     (key(part("congress"), part("bill_type", transform="lower"), part("number"), separator="-", pattern=r"[0-9]+-(hr|s|hres|sres|hjres|sjres|hconres|sconres)-[0-9]+"),)),),
              meaning="Publisher-listed related bills. No reciprocal relationship or identical text is inferred."),
        array("meeting_hearing_jackets", "committee_meetings", ("hearing_jackets", "hearing_jackets_json"),
              (route("hearing_transcripts", ("congress", "chamber", "jacket_number"),
                     (key(part("congress", row=True), pattern=NUM), key(part("chamber", row=True)), key(part(""), pattern=NUM))),),
              meaning="Meeting-listed hearing jackets within this meeting's own Congress and chamber."),
        array("meeting_documents", "committee_meetings", ("document_urls", "document_urls_json"),
              (route("@url", ("url",), (key(part(""), pattern=r"https://[^\s]+"),)),),
              meaning="Documents offered for this meeting; no capture, extraction or witness pairing is implied."),
        array("meeting_witness_documents", "committee_meetings", ("witness_documents", "witness_documents_json"),
              (route("@url", ("url",), (key(part("url"), pattern=r"https://[^\s]+"),)),),
              meaning="Publisher-listed witness documents in their source order; a URL does not prove capture."),
    ]
    vote_bill = route("congress_bills", ("bill_id",), (
        key(part("congress"), part("type", transform="bill-type"), part("number"), separator="-"),),
        guard("type", values=("HR", "S", "HRES", "SRES", "HJRES", "SJRES", "HCONRES", "SCONRES", "H.R.", "S.", "H.Res.", "S.Res.", "H.J.Res.", "S.J.Res.", "H.Con.Res.", "S.Con.Res.")),
        guard("congress", pattern=NUM), guard("number", pattern=NUM))
    vote_nomination = route("nominations", ("congress", "citation"), (
        CONGRESS, key(part("number", transform="nomination-citation"), pattern=r"PN[1-9][0-9]*(?:-[1-9][0-9]*)?")),
        guard("type", values=("PN",)))
    vote_treaty = route("treaties", ("treaty_id",), (key(part("number"), pattern=r"[1-9][0-9]*-[1-9][0-9]*"),),
                        guard("type", values=("Treaty Doc.",)))
    specs += [
        array("vote_documents", "roll_call_votes", ("documents", "documents_json"),
              (vote_bill, vote_nomination, vote_treaty), meaning="Each independent source document reference; its own Congress and nomination partition are retained."),
        array("vote_amendments", "roll_call_votes", ("amendments", "amendments_json"),
              (route("amendments", ("amendment_id",), (key(part("congress", row=True), part("number", transform="senate-amendment"), separator="-"),),
                     guard("chamber", row=True, values=("senate",)), guard("congress", row=True, pattern=NUM),
                     {**part("vote_id", row=True, transform="vote-congress"), "sameAs": part("congress", row=True)}, guard("number", pattern=r"S\.Amdt\. [1-9][0-9]*")),),
              meaning="Source-stated Senate amendment numbers; no positional pairing with vote document blocks."),
    ]
    # Resolver-owned routes qualify candidate identities, including ranges and ambiguous targets.
    # Navigate each recorded candidate key rather than reconstructing its original citation. Native rows record
    # candidates under the native routes (a usc_section candidate names law_code_sections rows).
    from spicy_regs.citation_resolution import NATIVE_ROUTES
    legal_targets = [route(r.table, r.identity, tuple(key(part(c)) for c in r.identity),
                           guard("target_table_selected", values=(r.table,)),
                           guard("target_status", values=("found", "ambiguous")))
                     for r in {(r.table, r.identity): r for r in NATIVE_ROUTES.values()}.values()]
    specs.append(array("native_legal_targets", "native_legal_references", ("target_candidates", "target_candidates_json"), tuple(legal_targets),
                       candidates=True,
                       meaning="Recorded candidate identities, including ambiguous matches. Links check the currently selected target table; recorded resolution and pins do not prove a current match. Missing, unsupported and unchecked outcomes remain visible."))
    specs.append(array("native_legal_read", "native_legal_references", (),
                       (route("native_legal_reference_reads", ("scope_id",), (key(part("scope_id", row=True)),)),),
                       meaning="The complete-read scope for this observed legal reference.", mode="row"))
    for source, target, columns in (
        ("committee_reports", "committee_report_read_outcomes", ("package_id",)),
        ("document_citations", "document_citation_read_outcomes", ("document_kind", "document_key", "body_version_id")),
    ):
        specs.append(array(source + "_read_outcomes", source, (),
                           (route(target, columns, tuple(key(part(column, row=True)) for column in columns)),),
                           mode="row", meaning="Recorded reads for the exact document and body scope; no recorded row does not mean a successful empty read."))
    for source, field, target, column in (("fec_committee_observations", "candidate_ids_json", "fec_candidate_history", "candidate_id"),
                                           ("fec_committee_observations", "sponsor_candidate_ids_json", "fec_candidate_history", "candidate_id")):
        specs.append(array(source + "_" + field, source, (field, field.removesuffix("_json")),
                           (route(target, (column,), (key(part(""), pattern=r"[HSP][0-9A-Z]{8}"),)),),
                           meaning="Source-reported candidate relationship; all retained cycle observations remain available."))
    field_rules = json.loads(Path(__file__).with_name("fec_subject_fields.json").read_text())
    for join in processing_joins:
        if (join.child, join.child_columns, join.parent, join.parent_columns) == (
            'member_vote_terms', ('bioguide_id', 'term_index'), 'member_terms', ('bioguide_id', 'term_index')
        ):
            specs.append(array(join.name, join.child, (), (
                route(join.parent, join.parent_columns,
                      tuple(key(part(column, row=True), pattern=r'0|[1-9][0-9]*' if column == 'term_index' else '.+')
                            for column in join.child_columns),
                      guard('term_match', row=True, values=('half_open', 'inclusive_end'))),
            ), mode='row', meaning='The service term selected by the vote date and chamber. Only successful half-open or inclusive-end matches link; missing, ambiguous and noncanonical indices stay unsupported.'))
            continue
        if join.child == "scorecard_snapshots" or join.parent == "scorecard_snapshots":
            columns = join.parent_columns
            fields = join.child_columns
            # A snapshot is qualified by its edition as well as its literal ID.
            if join.parent == "scorecard_snapshots":
                columns += ("scorecard_id",)
                fields += ("scorecard_id",)
            specs.append(array("recorded_snapshot_" + join.child + "_" + join.parent,
                               join.child, (), (route(join.parent, columns,
                                   tuple(key(part(c, row=True)) for c in fields)),),
                               meaning="Recorded association between this scorecard edition and its snapshot. Snapshot records retain capture roles and source order; these identifiers do not identify an official record.",
                               mode="row"))
            continue
        if join.child in {"scorecard_member_links", "scorecard_item_links"} and join.parent in {
            "members", "congress_bills", "roll_call_votes", "amendments",
        }:
            specs.append(array("resolved_" + join.child + "_" + join.parent, join.child, (),
                               (route(join.parent, join.parent_columns,
                                      tuple(key(part(c, row=True)) for c in join.child_columns),
                                      guard("resolution_status", row=True, values=("resolved",))),),
                               meaning="Recorded resolved identifier. This link checks current published data; the resolver's recorded input pins describe its original target.",
                               mode="row"))
            continue
        if not join.child.startswith("fec_"):
            continue
        # These are existing canonical declarations, not inferred matching names.
        guards = ()
        meaning = join.reason or "Retained source-record relationship; no current-record or financial-total qualification."
        if join.parent == "fec_collections" and join.child in _FEC_CONTEXT_SOURCES | {"fec_record_evidence"}:
            guards, meaning = _fec_context_route(join.child)
        if join.child == "fec_record_evidence" and "target_record_id" in join.child_columns:
            guards = (guard("target_table", row=True, values=(join.parent,)),
                      guard("target_generation_scope", row=True, values=("self",)))
        if join.parent == "fec_filings" and "filing_key" in field_rules.get(join.child, {}).get("keep", ()):
            guards += (guard("filing_association_status", row=True, values=("resolved_native_filing_key",)),)
        specs.append(array("receipt_" + join.child + "_" + "_".join(join.child_columns) + "_" + join.parent,
                           join.child, (), (route(join.parent, join.parent_columns,
                                                tuple(key(part(c, row=True)) for c in join.child_columns), *guards),),
                           meaning=meaning,
                           mode="row"))
    # These canonical collection relationships enumerate the typed observations.
    # A stored native filing_key is already the retained mapper's decision; browser
    # navigation keeps every metadata observation and makes no latest/current selection.
    filing_sources = {j.child for j in processing_joins if j.parent == "fec_collections"}
    for source in sorted(filing_sources - {"fec_source_records"} - _FEC_CONTEXT_SOURCES):
        digest_field = "witness_sha256" if source == "fec_record_evidence" else "source_sha256"
        guards = (guard("endpoint_kind", row=True, values=("source_record",)), *_fec_witness_scope()) if source == "fec_record_evidence" else ()
        meaning = ("An individual source witness identified by collection, source ID and witness content digest. "
                   "Target scope is self and witness scope is the exact external generation pin; release qualification "
                   "must bind that pin to the selected source generation before serving this route.") if source == "fec_record_evidence" else (
                   "The recorded source row under its exact collection, source ID and source-content hash. "
                   "This does not qualify a current filing or financial total.")
        specs.append(array("fec_source_row_" + source, source, (),
                           (route("fec_source_records", ("collection_id", "source_record_id", "source_sha256"),
                                  (key(part("collection_id", row=True)), key(part("source_record_id", row=True)),
                                   key(part(digest_field, row=True), pattern=_FEC_DIGEST)), *guards),),
                           meaning=meaning,
                           mode="row"))
    for source in sorted(filing_sources):
        if "filing_key" not in field_rules.get(source, {}).get("keep", ()):
            continue
        targets = tuple(route("fec_filings", ("filing_key",),
                              (key(part("filing_key", row=True), pattern=r"urn:fec:filing:official-fec:openfec-file-number:-?[1-9][0-9]*"),),
                              guard("filing_association_status", row=True, values=("resolved_native_filing_key",)),
                              guard("filing_association_target_generation_scope", row=True, values=(scope,)))
                        for scope in ("external", "self"))
        specs.append(array("native_filing_" + source, source, (),
                           targets,
                           meaning="All retained filing metadata with this mapper-qualified native key. This does not select a current amendment or combine financial amounts.", mode="row"))
    for side in ("subject", "object"):
        targets = tuple(route(table, (column, "cycle"),
                              (key(part(side + "_id", row=True), pattern=pattern), key(part("cycle", row=True), pattern=NUM)),
                              guard(side + "_type", row=True, values=(kind,)),
                              guard(side + "_endpoint_status", row=True, values=("lookup_eligible",)),
                              guard(side + "_id_status", row=True, values=("source_id_shape",)),
                              *((guard("value_status", row=True, values=("reported",)),) if side == "object" else ()))
                        for kind, table, column, pattern in (
                            ("candidate", "fec_candidate_history", "candidate_id", r"[HPS][A-Za-z0-9]{8}"),
                            ("committee", "fec_committee_history", "committee_id", r"C[0-9]{8}")))
        specs.append(array("fec_relationship_" + side, "fec_relationships", (), targets,
                           mode="row", meaning="The retained entity type, native ID and stated cycle qualify this endpoint. Name-only, absent and unavailable-cycle results create no edge."))
    specs.append(array("fec_relationship_source_record", "fec_relationships", (),
                       (route("fec_source_records", ("collection_id", "source_record_id", "source_sha256"),
                              (key(part("collection_id", row=True)), key(part("source_record_id", row=True)),
                               key(part("source_sha256", row=True), pattern=r"sha256:[a-f0-9]{64}")),
                              guard("companion_status", row=True, values=("locator_coordinates_available",))),),
                       mode="row", meaning="Literal retained source coordinates and digest; target existence does not establish verified source bytes."))
    from spicy_regs.explorer_retained_navigation import declarations as retained_navigation
    from spicy_regs.explorer_domain_navigation import declarations as domain_navigation

    specs += _regulatory_navigation() + _citation_navigation() + _recorded_source_navigation() + retained_navigation() + domain_navigation()
    return validate_navigation(deepcopy(specs))


def scalar(value):
    # DuckDB JSON retains signed/unsigned 64-bit integer tokens exactly.
    # Larger numeric tokens are doubles there; digit strings stay literal.
    if isinstance(value, str):
        return value
    if isinstance(value, int) and not isinstance(value, bool) and -(2**63) <= value < 2**64:
        return str(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    return None


def at(value, path):
    for name in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(name)
    return value


def word(recipe, element, row):
    if "literal" in recipe:
        return recipe["literal"]
    value = at(row if recipe.get("from") == "row" else element, recipe["path"])
    transform = recipe.get("transform")
    if transform == "native-boolean":
        return str(value).lower() if type(value) is bool else None
    if transform in {"fr-document-number", "fr-publication-date", "canonical-date"}:
        if not isinstance(value, str):
            return None
        if transform == "canonical-date":
            day, number = value, None
        else:
            match = re.fullmatch(r"([A-Za-z0-9._-]+)@([0-9]{4}-[0-9]{2}-[0-9]{2})", value)
            if not match:
                return None
            number, day = match.groups()
        try:
            valid = date.fromisoformat(day).isoformat() == day
        except ValueError:
            return None
        if not valid:
            return None
        return number if transform == "fr-document-number" else day
    if transform in {"partition", "partition-value"}:
        if value is not None and scalar(value) is None:
            return None
        if value in (None, "", "00", 0):
            return ""
        value = scalar(value)
        if transform == "partition":
            # The publisher retains partNumber "07" beside citation "PN1272-7".
            return "-" + value.lstrip("0") if value and re.fullmatch(r"0*[1-9][0-9]*", value) else None
        return value if value and re.fullmatch(r"[0-9]+", value) else None
    value = scalar(value)
    if value is None:
        return None
    if transform == "vote-congress":
        match = re.fullmatch(r"([1-9][0-9]*)-senate-[12]-[1-9][0-9]*", value)
        return match[1] if match else None
    if transform == "hearing-congress":
        match = re.fullmatch(r"[SH]\.Hrg\. ?([1-9][0-9]*)-[0-9]+", value)
        return match[1] if match else None
    if transform == "lower":
        return value.lower()
    if transform == "bill-type":
        return value.lower().replace(".", "")
    if transform == "nomination-citation":
        return "PN" + value
    if transform == "senate-amendment":
        return "samdt-" + value.removeprefix("S.Amdt. ")
    return value


def target_keys(target, element, row):
    for check in target["guards"]:
        value = word(check, element, row)
        if value is None or "sameAs" in check and value != word(check["sameAs"], element, row) or "values" in check and value not in check["values"] or "pattern" in check and not re.fullmatch(check["pattern"], value):
            return None
    values = []
    for recipe in target["keys"]:
        parts = [word(p, element, row) for p in recipe["parts"]]
        if None in parts:
            return None
        value = recipe["separator"].join(parts)
        if not re.fullmatch(recipe["pattern"], value):
            return None
        values.append(value)
    return values


def _recipes(target):
    """Include equality guard dependencies as well as transformed key parts."""
    return [*target["guards"],
            *(g["sameAs"] for g in target["guards"] if "sameAs" in g),
            *(p for k in target["keys"] for p in k["parts"])]


@lru_cache(maxsize=256)
def _struct_fields(typ):
    """Reuse DuckDB's native type parser, without Arrow or reading source rows."""
    from duckdb import InvalidInputException, sqltype

    try:
        parsed = sqltype(typ)
    except InvalidInputException:
        return None
    return {name: str(dtype) for name, dtype in parsed.children} if parsed.id == "struct" else None


def _path_status(typ, path):
    """Typed paths are schema checked; JSON paths are checked on each occurrence."""
    for name in path:
        if typ.upper() in {"VARCHAR", "JSON"}:
            return "runtime_checked"
        fields = _struct_fields(typ)
        if fields is None or name not in fields:
            return "missing"
        typ = fields[name]
    return "published"


def _dependencies(target, fields, selected_field, mode, *, candidates=False):
    main, nested = {}, {}
    if mode == "array":
        main[selected_field or "<reference array>"] = (
            "published" if selected_field else "missing")
    for recipe in _recipes(target):
        if "literal" in recipe:
            continue
        path = recipe["path"]
        if recipe["from"] == "row":
            name = ".".join(path)
            main[name] = (_path_status(fields[path[0]], path[1:])
                          if path and path[0] in fields else "missing")
        elif mode == "array":
            typ = fields.get(selected_field, "")
            if typ.endswith("[]"):
                element_type = typ[:-2]
                if candidates and any(recipe is p for k in target["keys"] for p in k["parts"]):
                    candidate_fields = _struct_fields(element_type)
                    inner_type = (candidate_fields or {}).get("candidate_keys", "")
                    status = _path_status(inner_type[:-2], path) if inner_type.endswith("[]") else "missing"
                    path = ["candidate_keys", *path]
                else:
                    status = _path_status(element_type, path)
            elif typ.upper() in {"VARCHAR", "JSON"}:
                status = "runtime_checked"
            else:
                status = "missing"
            nested[".".join(path) or "<element>"] = status
    return ([{"path": name, "status": status} for name, status in sorted(main.items())],
            [{"path": name, "status": status} for name, status in sorted(nested.items())])


def directions(available, *, external=False):
    """A declaration or historical baseline never proves current target uniqueness."""
    return {direction: {
        "available": available and not (external and direction == "reverse"),
        "measurement": {"status": "unknown", "scope": "unknown"},
        "lookup": {"status": ("external" if external else "scan")
                   if available and not (external and direction == "reverse") else "unsupported",
                   "scope": "selected_publication" if not external else "external_url",
                   "requiresExactMatch": True},
    } for direction in ("forward", "reverse")}


_FEC_WITNESS_ROUTES = frozenset({
    "fec_source_row_fec_record_evidence",
    "receipt_fec_record_evidence_collection_id_fec_collections",
    "receipt_fec_filing_definition_evidence_collection_id_fec_collections",
})


def selected_generation_pins(index):
    """One table-to-generation map shared by metadata and measurements."""
    return {filename.removesuffix(".parquet"): entry.get("artifactDigest")
            for entry in index.get("families", {}).values()
            for filename in entry.get("tables", {}) if filename.endswith(".parquet")}


def _bind_fec_witness_generation(spec, target, generation_pins):
    """Bind literal FEC witness guards to this selected metadata snapshot."""
    filing = spec["id"].startswith("native_filing_fec_")
    logical_target = spec["id"].startswith("receipt_fec_record_evidence_target_record_id_")
    if spec["id"] not in _FEC_WITNESS_ROUTES and not filing and not logical_target:
        return True
    # An earlier metadata snapshot contributes no generation authority here.
    target["guards"] = [g for g in target["guards"] if not g.get("selectedGenerationGuard")]
    target.pop("generationScope", None)
    source_pin = generation_pins.get(spec["source"])
    witness_pin = generation_pins.get(target["table"])
    def valid(pin):
        return isinstance(pin, str) and re.fullmatch(_FEC_DIGEST, pin) is not None
    if logical_target:
        if not valid(source_pin) or not valid(witness_pin) or source_pin != witness_pin:
            return False
        target["generationScope"] = {"target": witness_pin, "scope": "self"}
        return True
    if filing:
        evidence_pin = generation_pins.get("fec_source_records")
        scope = next(g["values"][0] for g in target["guards"]
                     if g["path"] == ["filing_association_target_generation_scope"])
        if not all(valid(pin) for pin in (source_pin, witness_pin, evidence_pin)):
            return False
        if scope == "self" and source_pin != witness_pin:
            return False
        target["guards"].append(
            {**part("filing_association_source_generation_pin", row=True),
             "sameAs": {**part("", row=True), "literal": evidence_pin}, "selectedGenerationGuard": True})
        if scope == "external":
            target["guards"].append(
                {**part("filing_association_target_generation_pin", row=True),
                 "sameAs": {**part("", row=True), "literal": witness_pin}, "selectedGenerationGuard": True})
        target["generationScope"] = {"sourceEvidence": evidence_pin, "target": witness_pin, "scope": scope}
        return True
    target_tables = sorted(name for name, pin in generation_pins.items()
                           if name.startswith("fec_") and name != spec["source"] and pin == source_pin)
    if not valid(source_pin) or not valid(witness_pin) or not target_tables:
        return False
    # Remove prior snapshot bindings before fresh resolution. An old metadata
    # recipe can never impose its pin on a new selected publication.
    target["guards"] += [
        {**part("witness_generation_pin", row=True), "sameAs": {**part("", row=True), "literal": witness_pin},
         "selectedGenerationGuard": True},
        {**guard("target_table", row=True, values=tuple(target_tables)), "selectedGenerationGuard": True},
    ]
    target["generationScope"] = {"target": source_pin, "witness": witness_pin,
                                 "targetTables": target_tables}
    return True


def published_navigation(specs, schemas, identities=None, *, generation_pins=None):
    """Admit only main-field routes and retain dispositions for unpublished sources.

    Only published fields recover keys or make a source route available. Native nested fields bind
    against selected schemas; JSON paths remain guarded per source occurrence.
    """
    identities = identities or {}
    generation_pins = generation_pins or {}
    result = []
    for original in specs:
        spec = deepcopy(original)
        fields = dict(schemas.get(spec["source"], []))
        spec["field"] = next((name for name in spec["fields"] if name in fields), None)
        for target in spec["targets"]:
            generation_available = _bind_fec_witness_generation(spec, target, generation_pins)
            main, nested = _dependencies(target, fields, spec["field"], spec["mode"], candidates=spec["candidates"])
            target["requiredMainFields"], target["requiredElementFields"] = main, nested
            target["sourceAvailable"] = spec["source"] in schemas and all(
                item["status"] != "missing" for item in [*main, *nested]) and generation_available
            external = target["table"] == "@url"
            target["requiredTargetFields"] = [
                {"path": column, "status": "published" if external or column in
                 dict(schemas.get(target["table"], [])) else "missing"}
                for column in target["columns"]]
            target["available"] = external or (target["table"] in schemas and all(
                item["status"] == "published" for item in target["requiredTargetFields"]))
            target["completeKey"] = bool(identities.get(target["table"])) and (
                set(target["columns"]) == set(identities[target["table"]]))
            target["directions"] = directions(target["available"] and target["sourceAvailable"], external=external)
            if not target["sourceAvailable"]:
                missing = [item["path"] for item in [*main, *nested] if item["status"] == "missing"]
                target["sourceUnavailableReason"] = (
                    "Required main source fields are not published: " + ", ".join(missing)
                    if spec["source"] in schemas else "The source table is not published.")
                if not generation_available:
                    target["sourceUnavailableReason"] = "Exact selected FEC target and witness generation scope is unavailable."
            if not target["available"]:
                target["unavailableReason"] = "The target table or its complete key is not published."
        spec["available"] = any(t["sourceAvailable"] for t in spec["targets"])
        if not spec["available"]:
            spec["unavailableReason"] = "Required main source references are not published."
        result.append(spec)
    return result


def validate_navigation(specs):
    """Reject malformed keys at generation time, before clients receive them."""
    ids = set()
    for spec in specs:
        if spec["id"] in ids or spec["mode"] not in {"row", "array"}:
            raise ValueError("Invalid or duplicate navigation declaration")
        ids.add(spec["id"])
        for target in spec["targets"]:
            if not target["columns"] or len(target["columns"]) != len(target["keys"]):
                raise ValueError("Navigation requires every component of the target key")
            for recipe in [*target["guards"], *(g["sameAs"] for g in target["guards"] if "sameAs" in g), *(p for k in target["keys"] for p in k["parts"])]:
                if recipe["from"] not in {"row", "element"} or not all(isinstance(p, str) for p in recipe["path"]):
                    raise ValueError("Invalid navigation field path")
                if recipe.get("transform") not in {None, "lower", "bill-type", "nomination-citation", "partition", "partition-value", "senate-amendment", "hearing-congress", "vote-congress", "native-boolean", "fr-document-number", "fr-publication-date", "canonical-date"}:
                    raise ValueError("Unknown navigation transform")
                if "literal" in recipe and (not isinstance(recipe["literal"], str) or not 0 < len(recipe["literal"]) <= 4096 or recipe.get("transform")):
                    raise ValueError("Invalid navigation literal")
            for check in [*target["guards"], *target["keys"]]:
                if "pattern" in check:
                    re.compile(check["pattern"])
    return specs


def array_sql_relationships():
    """Compile new Congress list relationships from the same recipes shipped to browsers."""
    from spicy_regs.relationship_views.core import ArrayRelationship
    # Keep base MCP imports free of the ETL-only Arrow dependency.
    identities = {"committee_meetings": ("congress", "chamber", "event_id"), "nominations": ("congress", "citation")}

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    def expression(p):
        if p["from"] == "row":
            if len(p["path"]) != 1:
                raise ValueError("SQL row navigation requires a scalar field")
            result = 's."' + p["path"][0] + '"'
        else:
            path = literal("$." + ".".join(p["path"]))
            kind = "json_type(e.value, " + path + ")"
            raw = "json_extract_string(e.value, " + path + ")"
            result = f"CASE WHEN {kind} IN ('VARCHAR','BIGINT','UBIGINT') THEN {raw} END"
        transform = p.get("transform")
        if transform == "nomination-citation":
            return "'PN' || " + result
        if transform in {"partition", "partition-value"}:
            kind = "json_type(e.value, " + literal("$." + ".".join(p["path"])) + ")"
            value = (f"CASE WHEN regexp_full_match({result}, '0*[1-9][0-9]*') "
                     f"THEN '-' || ltrim({result}, '0') END" if transform == "partition" else
                     f"CASE WHEN regexp_full_match({result}, '[0-9]+') THEN {result} END")
            return (f"CASE WHEN {kind} IS NULL OR {kind} = 'NULL' THEN '' "
                    f"WHEN {kind} NOT IN ('VARCHAR','BIGINT','UBIGINT') THEN NULL "
                    f"WHEN {result} IN ('','00') OR ({kind} IN ('BIGINT','UBIGINT') AND {result} = '0') THEN '' "
                    f"ELSE {value} END")
        if transform == "lower":
            return f"lower({result})"
        if transform == "hearing-congress":
            return f"regexp_extract({result}, '^[SH]\\.Hrg\\. ?([1-9][0-9]*)-[0-9]+$', 1)"
        if transform:
            raise ValueError("Unsupported SQL navigation transform")
        return result

    specs = []
    for spec in declarations():
        if spec["id"] not in {"meeting_nominations", "meeting_treaties", "nomination_committees", "nomination_hearings"}:
            continue
        target = spec["targets"][0]
        values, checks = [], ["e.type = 'OBJECT'"]
        for check in target["guards"]:
            expr = expression(check)
            if "pattern" in check:
                checks.append(f"regexp_full_match({expr}, {literal(check['pattern'])})")
            if "values" in check:
                checks.append(expr + " IN (" + ",".join(map(literal, check["values"])) + ")")
        for recipe in target["keys"]:
            expr = (" || " + literal(recipe["separator"]) + " || ").join(expression(p) for p in recipe["parts"])
            checks.append(f"regexp_full_match({expr}, {literal(recipe['pattern'])})")
            values.append(expr)
        target_expression = " || ':' || ".join(values)
        specs.append(ArrayRelationship(spec["id"], spec["source"], identities[spec["source"]], spec["fields"][0],
                                       target["table"], target_expression, " AND ".join(checks), spec["meaning"],
                                       rule_version=spec["ruleVersion"]))
    return tuple(specs)
