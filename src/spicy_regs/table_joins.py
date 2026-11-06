"""The published tables' cross-table joins: declared once, with the measured baseline each is held to.

A join names a child table and columns and the parent table and columns they
reference. It resolves when a distinct non-null child key occurs in the parent.
Each carries a measured rate (``BASELINE_RECEIPTS``, with later dates in each
reason or measurement); its floor is that rate truncated to four decimals, and
``scripts/check_table_joins.py`` fails a join that falls below it. Joins that
are low by design, or by a table's current selection, are declared with that
rate and the reason, so the check still covers them. ``spicy-regs-dict
generate`` bundles them as ``table_joins.json`` beside ``table_metadata.json``
and ``check`` refuses a stale copy. ``references`` is the measurement-free
shape; SpicyDocs 0.36.0 contracts state the joins onto a parent's identity as
``TableContract.references``, and ``tests/test_table_joins.py`` holds each of
those to a join declared here. Standard library only: the MCP image imports it.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, replace
from pathlib import Path

from spicy_regs.subject_catalog import descriptors as _policy_descriptors

RECORD = Path(__file__).with_name("table_joins.json")
RECORD_FORMAT = "spicy-regs-table-joins"
#: A path on a maintainer's machine (``~/…`` or an absolute ``/dir/…``), which means nothing to a reader of a
#: record the server ships. A repository-relative path, a URL and a token such as ``/002`` do not match.
_MAINTAINER_PATH = re.compile(r"(?<![\w/.~])(?:~|/[A-Za-z][\w.-]*)/[^`\s]*")
BASELINE_DATE = "2026-09-26"
BASELINE_RECEIPTS = (
    "src/spicy_regs/join_measurements.json",
    "docs/evidence/column-navigation-2026-10-06.json",
    "docs/evidence/explorer-joins-2026-10-03.json",
    "docs/evidence/scorecard-navigation-2026-10-03.json",
    "docs/evidence/scorecard-source-joins-2026-10-04.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/results.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/rule-targets-baseline.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/unified-agenda-rin-after-backfill.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/dockets-after-gap-fill.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/sam-entities-after-backfill.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/live-check-after-fixes.json",
    "~/Work/corpora/fork-execution-2026-09-21/committee-fixes-2026-09-26/3-rosters-118/runs.json",
    "~/Work/corpora/fork-execution-2026-09-21/rulemaking-exposure-2026-09-27/proposed-joins.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/rulemaking-exposure-independent.json",
    "~/Work/corpora/fork-execution-2026-09-21/join-map-2026-09-26/rulemaking-exposure-independent-2.json",
    "~/.codex/artifacts/spicy-regs-mcp-repair-20260927/01-fr-composite-baseline.json",
)

#: What the declared joins in a description establish, bundled as the record's ``basis`` so the server states
#: the meaning beside the record it describes rather than holding a second copy.
BASIS = (
    "Declared cross-table joins, bundled when the dictionary was generated. baseline_keys and "
    "baseline_missing count distinct non-null child keys and those absent from the parent on the baseline "
    "date; floor_pct is the resolution rate scripts/check_table_joins.py holds the live tables to. "
    "'complete' says every non-null child key names a parent row; it does not check that the publisher paired "
    "them correctly. A 'scope' or 'design' join may resolve partly; its reason documents the intended scope "
    "or known gap. A measured baseline does not establish that missing matches are correct. "
    "This is not an exhaustive relationship catalog: JSON-array joins and other undeclared relationships "
    "may be described in the column meanings. An empty join list does not establish that no relationship exists."
)

#: Expected resolution. ``complete``: every key should resolve and an orphan is
#: a defect. ``unmeasured``: native keys are declared but have no measured baseline. ``scope``: the parent holds a narrower selection than the child
#: references. ``design``: the columns deliberately hold values the parent does
#: not key. ``empty``: the child publishes no key yet.
KINDS = ("complete", "scope", "design", "empty", "unmeasured")

#: A join reports up to this many orphans beyond those its floor admits, or
#: this share of its keys if larger, as LAG instead of failing (see
#: ``Join.lag_allowance``).
LAG_MIN = 3
LAG_SHARE = 0.0001


@dataclass(frozen=True)
class Join:
    child: str
    child_columns: tuple[str, ...]
    parent: str
    parent_columns: tuple[str, ...]
    baseline_keys: int
    baseline_missing: int
    kind: str = "complete"
    reason: str = ""
    #: A table read in the child's place when it carries the same distinct keys
    #: more cheaply (the comments index for the 26M-row comments table).
    measured_via: str | None = None
    expected_cardinality: str = "unspecified"
    measurement: dict | None = None

    @property
    def name(self) -> str:
        return f"{self.child}.{'+'.join(self.child_columns)} -> {self.parent}.{'+'.join(self.parent_columns)}"

    @property
    def baseline_pct(self) -> float | None:
        if not self.baseline_keys:
            return None
        return 100 * (self.baseline_keys - self.baseline_missing) / self.baseline_keys

    @property
    def floor_pct(self) -> float | None:
        """The baseline truncated to four decimals; ``None`` when the child had no keys to resolve."""
        pct = self.baseline_pct
        return None if pct is None else math.floor(pct * 10_000) / 10_000

    def admitted_missing(self, keys: int) -> int:
        """The most orphans ``keys`` distinct keys can hold and still meet the floor."""
        floor = self.floor_pct
        return 0 if floor is None else keys - math.ceil(keys * floor / 100)

    def lag_allowance(self, keys: int) -> int:
        """Orphans a join tolerates as lag, beyond those its floor admits, before it fails.

        A growing child can name a key its parent publishes a run later (a
        report citing a bill BILLSTATUS has not served yet, a document naming a
        docket before the gap fill reaches it, a new PACER docket whose opinion
        comes later), and failing on each such orphan trains people to ignore
        the check. The allowance sits on top of ``admitted_missing`` rather than
        the baseline count, so a scope or design join, whose missing count grows
        with the child by definition, gets the same few orphans of lag.
        """
        return max(LAG_MIN, math.ceil(keys * LAG_SHARE))


def _join(child: str, child_columns: str | tuple[str, ...], parent: str, parent_columns: str | tuple[str, ...],
          keys: int, missing: int, kind: str = "complete", reason: str = "", measured_via: str | None = None,
          expected_cardinality: str = "unspecified", measurement: dict | None = None) -> Join:
    as_tuple = lambda columns: (columns,) if isinstance(columns, str) else columns  # noqa: E731
    return Join(child, as_tuple(child_columns), parent, as_tuple(parent_columns), keys, missing, kind, reason,
                measured_via, expected_cardinality, measurement)


#: Why the model-written bill tables publish no rows: an owner decision, not a missing key. The financial pairing has
#: its own reason below. Neither names a deployment setting: a join reason is public metadata.
_MODEL_OUTPUT = ("Kept empty by owner decision 34 (fork-delivery-decisions-2026-09-22): the model output that would fill "
                 "it is not produced; publishes no rows")
_AMOUNT_PAIRING = "Disabled by design in spicy-docs (pair_amounts=False), owner decision 34; publishes no rows"
_BILL = "congress_bills"
#: Where the rulemaking dataset's joins were measured, twice independently (the pointer's snapshot, not a table pin).
_RULEMAKING = ("Measured 2026-09-27 on snapshot_f31a4045…; receipts rulemaking-exposure-2026-09-27/proposed-joins.json "
               "and join-map-2026-09-26/rulemaking-exposure-independent.json, -2.json.")


#: Where the round-6 FEC joins were measured: check_table_joins.measure over the full columns, 2026-10-03.
_FEC_R6 = ("Measured 2026-10-03 through check_table_joins on fec-query 81453ac6…, fec-observations 9c289dfe…, "
           "fec-committees ce80d98e… and fec-candidate-history d9e9c233…; receipt "
           "mcp-chaos-2026-10-02/round6/impl-B/joins-measured-2026-10-03.json.")
#: Distinct collection_id values per FEC typed table on that date (_FEC_R6), each naming exactly one fec_collections
#: row; fec_filing_definitions is the one typed table without the column. The joins carry no reason: a complete join
#: needs none, and fec_collections receives every one of them, so a reason each would repeat in its describe_table.
_FEC_COLLECTION_KEYS = {
    "fec_account_transfers": 20, "fec_agency_mapping_dispositions": 124, "fec_agency_report_documents": 98,
    "fec_agency_report_text": 60, "fec_agency_reports": 124, "fec_allocated_disbursements": 32,
    "fec_allocation_bases": 15, "fec_api_response_controls": 280, "fec_audit_findings": 2,
    "fec_bundled_contributions": 9, "fec_candidate_api_observations": 1, "fec_collection_selection": 19,
    "fec_committee_master_observations": 26, "fec_committee_observations": 277, "fec_communication_costs": 9,
    "fec_contribution_aggregates": 3, "fec_coordinated_party_expenditures": 4, "fec_debts": 174,
    "fec_disbursements": 657, "fec_electioneering_communications": 9, "fec_filing_definition_evidence": 14,
    "fec_filing_header_associations": 1_357, "fec_filing_links": 23, "fec_filing_report_observations": 1_357,
    "fec_filing_text_observations": 223, "fec_filings": 26, "fec_historical_ie_statistics": 4,
    "fec_inaugural_donations": 23, "fec_independent_expenditures": 54, "fec_intercommittee_transactions": 2,
    "fec_legal_documents": 25, "fec_legal_events": 25, "fec_legal_matters": 25, "fec_legal_parties": 25,
    "fec_loan_guarantors": 19, "fec_loan_terms": 9, "fec_loans": 127, "fec_lobbyist_registrations": 1,
    "fec_oversight_recommendations": 6, "fec_postgres_committee_history_observations": 1, "fec_quality_notices": 1,
    "fec_receipts": 824, "fec_record_evidence": 712, "fec_registration_statements": 4, "fec_report_metrics": 107,
    "fec_reported_financial_summaries": 14, "fec_research_meeting_observations": 1,
    # fec_record_evidence: 712 once rows targeting retired or rebuilt tables are dropped (live 784b23da: 1,181).
    # Rebuilt from their originals (ss-j4vz B7-B9): one collection per original, the first capture part, measured
    # 2026-10-31 on fec_original_tables.rebuild over the committed selection (live held 9 and 30).
    "fec_reference_filing_feed_items": 1, "fec_reference_source_pages": 14,
}
#: Why a summary's literal ids are a scope join: the file's ids are copied, never looked up (entity_reference_status).
_LITERAL_IDS = "The ids are copied as each summary file states them and are never looked up when a row is mapped. "


def _unlisted_rins(x_pattern: int, well_formed: int, placeholder: int, detail: str = "") -> str:
    """A RIN join's scope reason: the three kinds of RIN no Unified Agenda edition lists (receipt rin_orphans.json)."""
    return (f"unified_agenda holds every readable edition, Fall 1995 to the newest (202510). Of the RINs no edition "
            f"lists, {x_pattern:,} are X-pattern codes (NOAA's 0648-X in-season series and other agencies', never on "
            f"the agenda; decision 61), {well_formed:,} are well formed but unlisted{detail}, and {placeholder} is the "
            f"placeholder 0000-AA00. {_RULEMAKING}")


JOINS: tuple[Join, ...] = (
    # The bill family and the tables that name its bills.
    _join("bill_actions", "bill_id", _BILL, "bill_id", 38_559, 0),
    _join("bill_cosponsors", "bill_id", _BILL, "bill_id", 0, 0, "empty",
          "New source-occurrence output; public population not yet baselined."),
    _join("bill_cosponsors", "bioguide_id", "members", "bioguide_id", 0, 0, "empty",
          "Source-listed member identifier; public cosponsor output not yet baselined."),
    _join("bill_committee_actions", "bill_id", _BILL, "bill_id", 2_956, 1,
          reason="1 (118-hr-14106) is the publisher's misprint. The 16 of 2026-09-26 are resolved: the "
                 "status-only bill-family backfill of the 108th-117th filled the list walk's 1,299-bill gap, the "
                 "print re-keys moved Senate reports to their covered Congress and subheaded bills to theirs. "
                 "Receipts join-gaps-2026-09-26/d/, congress-bills-backfill-2026-09-26/."),
    _join("bill_committees", "bill_id", _BILL, "bill_id", 37_473, 0),
    _join("bill_committee_activities", "bill_id", _BILL, "bill_id", 0, 0, "empty",
          "New in spicy-docs 0.54.0: filled as each Congress's BILLSTATUS is re-read; not yet baselined."),
    _join("bill_committee_activities", ("bill_id", "system_code"), "bill_committees", ("bill_id", "system_code"),
          0, 0, "empty", "Read from the same committee walk as bill_committees; not yet baselined."),
    _join("bill_publisher_summaries", "bill_id", _BILL, "bill_id", 17_839, 0),
    _join("bill_sections", "bill_id", _BILL, "bill_id", 1_849, 0),
    _join("bill_subjects", "bill_id", _BILL, "bill_id", 149_261, 0),
    _join("bill_summaries", "bill_id", _BILL, "bill_id", 0, 0, "empty", _MODEL_OUTPUT),
    _join("bill_versions", "bill_id", _BILL, "bill_id", 38_230, 0),
    _join("bill_vote_references", "bill_id", _BILL, "bill_id", 7_513, 0),
    _join("cbo_cost_estimates", "bill_id", _BILL, "bill_id", 2_488, 0),
    _join("committee_reports", "bill_id", _BILL, "bill_id", 137, 0),
    _join("diff_summaries", "bill_id", _BILL, "bill_id", 0, 0, "empty", _MODEL_OUTPUT),
    _join("financial_changes", "bill_id", _BILL, "bill_id", 0, 0, "empty", _AMOUNT_PAIRING),
    _join("hearing_bill_links", "bill_id", _BILL, "bill_id", 85, 0),
    _join("hearing_transcripts", "bill_id", _BILL, "bill_id", 0, 0, "design",
          "Always NULL: a hearing covers a list of bills, which hearing_bill_links hosts one row per bill."),
    _join("laws", "bill_id", _BILL, "bill_id", 113, 0),
    _join("press_releases", "bill_id", _BILL, "bill_id", 3, 0),
    _join("public_activity_events", "bill_id", _BILL, "bill_id", 38_564, 0),
    _join("roll_call_votes", "bill_id", _BILL, "bill_id", 7_515, 0),
    _join("section_classifications", "bill_id", _BILL, "bill_id", 0, 0, "empty", _MODEL_OUTPUT),
    _join("section_diff_items", "bill_id", _BILL, "bill_id", 223, 0),
    _join("section_diffs", "bill_id", _BILL, "bill_id", 223, 0),
    _join("bill_sections", ("bill_id", "version_code", "source"), "bill_versions", ("bill_id", "version_code", "source"), 2_397, 0),
    _join("section_diffs", ("bill_id", "from_version_code", "from_source"), "bill_versions", ("bill_id", "version_code", "source"), 548, 0),
    _join("section_diffs", ("bill_id", "to_version_code", "to_source"), "bill_versions", ("bill_id", "version_code", "source"), 548, 0),
    # People, votes and committees.
    _join("amendments", "sponsor_bioguide_id", "members", "bioguide_id", 187, 0),
    _join("congress_bills", "sponsor_bioguide_id", "members", "bioguide_id", 636, 0),
    _join("member_terms", "bioguide_id", "members", "bioguide_id", 12_770, 0),
    _join("member_party_affiliations", ("bioguide_id", "term_index"), "member_terms", ("bioguide_id", "term_index"),
          0, 0, "empty", "New source interval occurrences; current roster backfill and public baseline pending."),
    _join("member_votes", "bioguide_id", "members", "bioguide_id", 1_261, 1, "complete",
          "L000555 is Luke Letlow, elected in 2020, who died on 2020-12-29 before taking the seat. The Clerk's "
          "117-house-1-1 roll lists him Not Voting; Congress.gov lists only members who served. Measured after the "
          "108th-119th votes chain; receipt join-map-2026-09-26/vote-joins-after-votes-chain.json."),
    _join("committee_assignments", "bioguide_id", "members", "bioguide_id", 532, 0),
    _join("member_votes", "vote_id", "roll_call_votes", "vote_id", 23_358, 0),
    _join("committee_assignments", "system_code", "committees", "system_code", 163, 5, "design",
          "28 seats have no Congress.gov code to join and are never matched by name: 25 House seats on the "
          "Clerk's joint committees EC00, IT00, JL00 and JP00 (typed joint with no Congress.gov code, so they keep "
          "the legacy hs spelling; the Joint Economic Committee has three Congress.gov codes) and 3 Senate seats on "
          "JSIK00, the 2024 inaugural committee, which Congress.gov lists in no Congress. Receipt "
          "join-gaps-2026-09-26/e/."),
    _join("bill_committees", "system_code", "committees", "system_code", 255, 0,
          reason="Complete once committees lists every Congress: the 108th-117th backfill named 78 codes on no 119th "
                 "list. Measured on a local committee-rosters run over the live children (receipt "
                 "committee-fixes-2026-09-26/3-rosters-118/runs.json); the published table misses them until it runs."),
    _join("committee_meetings", "committee_system_code", "committees", "system_code", 217, 0,
          reason="Complete once committees lists every Congress: meetings keep the previous Congress, whose House "
                 "bodies hlfd00, hlvc00 and htzt00 are on no 119th list. Same local run and receipt."),
    _join("hearing_bill_links", "committee_system_code", "committees", "system_code", 4, 0),
    _join("hearing_bill_links", ("package_id", "last_modified"), "hearing_transcripts", ("package_id", "last_modified"), 0, 0, "unmeasured",
          "A link names its exact dated hearing revision; the rule digest and held date are not revision dates.",
          expected_cardinality="one"),
    _join("hearing_transcripts", ("congress", "chamber", "event_id"), "committee_meetings", ("congress", "chamber", "event_id"), 16, 0,
          reason="Complete since committee_meetings keeps the previous Congress: the 7 orphans of the first "
                 "baseline were 118th-Congress meetings it had not listed (join-gaps-2026-09-26/f/). Receipt "
                 "join-map-2026-09-26/live-check-after-fixes.json."),
    _join("hearing_transcripts", ("congress", "chamber", "mods_event_id"), "committee_meetings",
          ("congress", "chamber", "event_id"), 43, 1,
          reason="The meeting GovInfo's MODS names (spicy-docs 0.54.0), filled as each held hearing is read again. Baseline "
          "from the reader over the 185 retained MODS (round 5, implementer E, 2026-10-03): 42 of 43 keys name a held "
          "meeting; CHRG-119hhrg60746's 417109 names none. Where Congress.gov's event_id also names one they agree "
          "21 of 21."),
    _join("report_sections", ("package_id", "part_id", "last_modified"), "committee_reports", ("package_id", "part_id", "last_modified"), 0, 0, "unmeasured",
          "A section belongs to exactly one report part at its publisher revision.", expected_cardinality="one"),
    _join("committee_report_cbo_letters", ("package_id", "part_id", "last_modified"), "committee_reports", ("package_id", "part_id", "last_modified"),
          0, 0, "empty", "Written by the committee-reports family from the spicy-docs reader that reads every CBO "
          "letter (ss-uep5); no generation publishes it yet."),
    _join("law_sections", "law_id", "laws", "law_id", 0, 0, "empty",
          "Sections from the law's own XML; public population not yet baselined."),
    _join("law_code_sections", "law_id", "laws", "law_id", 70, 0),
    # The contract's own sentence: both OLRC tables key the bare section, so they meet on title, key and place.
    _join("law_code_sections", ("law_number", "usc_title", "usc_section_key", "usc_place"),
          "table3_records", ("act_key", "usc_title", "usc_section_key", "usc_place"), 2_778, 557, "scope",
          "Table III lags enactment (release point 119-73 on 2026-10-03): 517 of the 557 lines are of laws it does not "
          "hold yet. The other 40 are lines of held acts the two tables state differently (119-65: 20, 119-60: 18). "
          "Without usc_place 569 note or preceding lines met a record of another place; with it, 9. Measured on the "
          "published rows of 2026-10-03, both keys and places derived as the laws build now derives them (receipt "
          "round6/impl-W/m7/measure.out)."),
    # Regulations.gov and the Federal Register.
    _join("documents", "docket_id", "dockets", "docket_id", 278_651, 114,
          reason="The mirror lacks these dockets' records, and fill-docket-gaps asked the Regulations.gov API for "
                 "each (docket_gap_outcomes.parquet, re-asked after 30 days): 47 answer 404, which the publisher "
                 "does not publish, and 67 answer 400 Invalid ID, legacy -RULEMAKING/-NONRULEMAKING ids whose "
                 "unsuffixed forms it does not serve either. It served 23 more, merged 2026-09-26. Receipt "
                 "join-map-2026-09-26/dockets-after-gap-fill.json."),
    _join("comments", "docket_id", "dockets", "docket_id", 60_221, 28, measured_via="comments_index",
          reason="Asked of the Regulations.gov API by fill-docket-gaps like documents' orphans: 6 answer 404 and 22 "
                 "answer 400 Invalid ID (legacy -RULEMAKING/-NONRULEMAKING ids)."),
    _join("fr_docket_links", "docket_id", "dockets", "docket_id", 612_342, 592_102, "design",
          "Printed rows carry raw publisher docket labels (many are agency docket numbers); the normalized bridge is "
          "rule_targets.docket_id -> dockets. A row whose link_source is regulations_dot_gov_info carries the "
          "Register's own regulations.gov docket id (from 2026-10-03), which names a regulations.gov docket; the "
          "baseline predates those rows."),
    _join("rule_targets", "docket_id", "dockets", "docket_id", 143_012, 83,
          reason="Normalized FR-to-Regulations.gov bridge from the materialized rulemaking snapshot. Each orphan "
                 "is one fill-docket-gaps asked for: 16 answer 404 and 67 answer 400 Invalid ID."),
    _join("fr_docket_links", ("document_number", "publication_date"),
          "federal_register", ("document_number", "publication_date"), 603_935, 0,
          reason="Measured 2026-09-27 on the live fork. A document number can occur on distinct publication dates; "
                 "both fields identify the referenced record. Receipt 01-fr-composite-baseline.json."),
    _join("documents", "fr_doc_num", "federal_register", "document_number", 454_231, 64_761, "design",
          "fr_doc_num is Regulations.gov's spelling, which zero-pads where the Register did not (every "
          "2010-2012 number: 2011-01234 is the Register's 2011-1234) and carries errata prefixes, en dashes "
          "and citations. Compared on spicy-docs' unpadded_federal_register_document_number key, 447,929 of "
          "the 454,231 values resolve (98.6%); the rest are citations and placeholders ('91 FR 13845', "
          "'none'), pre-1994 numbers the table does not reach and malformed values. The resolved link is "
          "interpretation and belongs in a typed layer over both tables, not here. Receipt "
          "join-map-2026-09-26/fr-link-classify.txt."),
    _join("dockets", "rin", "unified_agenda", "rin", 14_421, 994, "scope",
          "unified_agenda holds every readable edition, Fall 1995 to the newest (not Spring 1995 or Spring 2012, "
          "never published, nor the two 2004 editions SpicyDocs refuses). Of the 994 RINs no edition lists, 536 "
          "are NMFS's 0648-X series, which it assigns to in-season actions and never puts on the agenda, 115 name "
          "2026 dockets newer than the newest edition, and 9 are malformed. Receipt "
          "join-map-2026-09-26/unified-agenda-rin-after-backfill.json."),
    _join("federal_register", "rin", "unified_agenda", "rin", 35_859, 6_090, "scope",
          "unified_agenda holds every readable edition, Fall 1995 to the newest. Of the 6,090 RINs no edition "
          "lists, 2,211 are NMFS's 0648-X in-season series (every one), 1,409 first appear before Fall 1995, 179 "
          "first appear in 2026, after the newest edition, and 460 are malformed as printed (3206-XXXX, "
          "7100 AG80). Receipt join-map-2026-09-26/unified-agenda-rin-after-backfill.json."),
    # The materialized rulemaking dataset (rule_targets' docket join is above), read through its snapshot pointer.
    _join("agenda_item_proceedings", "proceeding_id", "proceedings", "proceeding_id", 80_688, 0, reason=_RULEMAKING),
    _join("agenda_item_proceedings", "agenda_item_id", "regulatory_agenda_items", "agenda_item_id", 35_451, 0,
          reason=_RULEMAKING),
    _join("rulemaking_lifecycles", "proceeding_id", "proceedings", "proceeding_id", 75_270, 0, reason=_RULEMAKING),
    _join("lifecycle_events", "proceeding_id", "rulemaking_lifecycles", "proceeding_id", 65_278, 0,
          reason=_RULEMAKING),
    *(_join("rulemaking_lifecycles", ("proceeding_id", f"{role}_document_id"), "lifecycle_events",
            ("proceeding_id", "document_id"), keys, 0,
            reason=f"The {role} anchor is one of its proceeding's events. {_RULEMAKING}")
      for role, keys in (("proposal", 36_697), ("final", 55_640), ("withdrawal", 721))),
    _join("lifecycle_events", "document_id", "documents", "document_id", 131_476, 105_768, "design",
          "Three id kinds, told apart by dated_by, each resolving 100% in its own table: regulations_gov a "
          "Regulations.gov document id (25,708 keys, all in documents), federal_register a dated Register record id "
          "number@YYYY-MM-DD (104,995, all in federal_register), unified_agenda an agenda item id (773, all in "
          "regulatory_agenda_items). source does not tell them apart: 72 events Regulations.gov typed are copies of "
          f"untyped Register rows and carry Register ids. {_RULEMAKING}"),
    _join("regulatory_agenda_items", ("rin", "latest_agenda_edition"), "unified_agenda", ("rin", "agenda_edition"),
          46_247, 0, reason=f"An item with no latest edition is one no edition lists. {_RULEMAKING}"),
    _join("rule_targets", "rin", "unified_agenda", "rin", 22_290, 2_299, "scope",
          _unlisted_rins(1_269, 1_029, 1, " (854 first dated 1995-2025, 174 in 2026, after the newest edition, "
                                          "and 1 before Fall 1995)")),
    _join("proceedings", "rin", "unified_agenda", "rin", 30_536, 3_833, "scope", _unlisted_rins(802, 3_030, 1)),
    _join("regulatory_agenda_items", "rin", "unified_agenda", "rin", 52_092, 5_845, "scope",
          _unlisted_rins(2_410, 3_434, 1, " (an item exists for every RIN any source states)")),
    # Declared, not measured into join_measurements.json: its overlay reads a measured join as complete with one
    # parent per key, and this one is a scope join over many section rows per part. The baseline counts the 13
    # Title 41 chapter keys missing although they still resolve on 2026-10-03, so the compound-part fix in
    # build_cfr_sections, released with this declaration, does not drop the join below its floor.
    _join("rule_targets", ("cfr_title", "cfr_part"), "cfr_sections", ("title", "part"), 6_146, 812, "scope",
          "rule_targets holds part-level keys, so the join is on title and part, which a part held only as section "
          "rows also has; the printed cfr_ref missed 12 such parts (50 CFR 622 among them). cfr_sections holds "
          "the 2025 edition of every title and the 2026 edition of titles 5, 7, 9-12 and 14-16 only, so a part can "
          "be missing either way: 679 of the 799 missing keys were last cited before 2025, which fits a part removed "
          "or redesignated since; a key first cited later can name a part created after the held edition (41 CFR "
          "105-9 and 105-10 are new in 2026) or one a proposed rule would create, which no edition holds (2 CFR "
          "6100, 10 CFR 57, 21 CFR 1108 and 43 CFR 1700 are in no eCFR version of 2025-01-01 or 2026-09-30). "
          "Title 41 matches at chapter level only: the Federal Register cuts a compound part at the hyphen (41 CFR "
          "60-1.4 is part 60), so 13 of its 14 Title 41 keys (chapters 50, 51, 60, 61, 101, 102, 105, 128, 201 and "
          "300-303) name chapters, which no cfr_sections part matches once its Title 41 structural rows carry their "
          "compound parts; they are counted missing (the 14th, 41-74, names no Title 41 chapter and is among the "
          "799). A Register reference that names a chapter and no part (FR 2026-00929, chapters 300-304) yields "
          "no edge. Measured 2026-10-03 on rule_targets snapshot_448e6d9a… and cfr_sections eab08a3f…: 6,146 "
          "keys, 799 missing, plus those 13; the same counts on snapshot_55d396ee…. Receipts "
          "mcp-chaos-2026-10-02/round4/joins-measured-2026-10-03.json and "
          "mcp-chaos-2026-10-02/round5/scout-A/m1b_rt_missing.out."),
    # FEC.
    _join("org_committee_links", "committee_id", "fec_committees", "committee_id", 6_693, 0,
          reason="The committee's status is here, not copied into the links: filing_frequency T or A means "
          "terminated, and last_file_date is its latest filing. Measured 2026-10-03 on the rebuild of "
          "org-committee-links 8487b62e's inputs (fec-committees ce80d98e, fec-committee-history 4ed93047) after "
          "links to candidate committees and leadership PACs left: every committee resolves. Receipt "
          "mcp-chaos-2026-10-02/round6/impl-C2/m6/measure.out."),
    # The Regulations.gov attribute tables (decisions 65-67): every row is a record the thin tables also hold.
    # First measured on the published attribute families (document-attributes 30f9aa29, docket-attributes ba5aba75;
    # join check 36360197318, 2026-09-27): every attribute row names a thin-table row.
    _join("document_attributes", "document_id", "documents", "document_id", 2_002_888, 0),
    _join("docket_attributes", "docket_id", "dockets", "docket_id", 279_406, 0),
    _join("comment_attributes", "comment_id", "comments", "comment_id", 26_381_105, 0,
          reason="First public generation 46061b9d, measured 2026-09-30: every attribute identity resolves to "
          "the comments mirror, with no missing or extra identities. Receipt "
          "docs/evidence/new-source-publication-2026-09-30.json."),
    _join("fec_committee_history", "committee_id", "fec_committees", "committee_id", 89_710, 21, "scope",
          "fec_committees is OpenFEC's registry; the bulk committee master also names 21 committees, newest cycle "
          "2000-2020 (15 in 2014), that the API does not serve (C00428599 and C00317453 answer an empty result). "
          "Every registry committee appears in the history. Receipt join-map-2026-09-26/fec-committee-history-first-run.json."),
    _join("fec_source_records", "collection_id", "fec_collections", "collection_id", 647, 0),
    *(_join(table, "collection_id", "fec_collections", "collection_id", keys, 0, expected_cardinality="one")
      for table, keys in _FEC_COLLECTION_KEYS.items()),
    _join("fec_agency_report_documents", "report_id", "fec_agency_reports", "report_id", 98, 0,
          reason="Report edition explicitly named by this document reference. Repeated links remain separate "
                 "observations; a link does not establish that its body was captured or extracted.",
          expected_cardinality="one"),
    _join("fec_agency_report_text", "report_id", "fec_agency_reports", "report_id", 60, 0,
          reason="Report edition explicitly named by this text observation. Each retained passage keeps its "
                 "source location; report titles do not merge editions.",
          expected_cardinality="one"),
    _join("fec_report_metrics", "report_id", "fec_agency_reports", "report_id", 107, 0,
          reason="Report edition explicitly named by this metric observation. The relationship supplies "
                 "report context; it does not combine metric values or qualify financial totals.",
          expected_cardinality="one"),
    _join("fec_independent_expenditures", "collection_id", "fec_filing_report_observations", "collection_id", 54, 1,
          "scope", "An original-filing row's cover: each electronic collection holds one fec_filing_report_observations "
          "row, its report_record_role reported-form (F24N or F24A, F3XN or F3XA, F5N). The bulk file "
          "(bulk-independent-expenditure-2024) has no cover row. " + _FEC_R6),
    _join("fec_reported_financial_summaries", "committee_native_id", "fec_committees", "committee_id", 21_633, 331,
          "scope", _LITERAL_IDS + "Most of those OpenFEC's registry does not serve are filers of FEC's committee web "
          "summaries (2024 and 2026 cycles) that the bulk committee master does not list either; the rest are a few "
          "bundling, leadership and committee-summary filers and the empty string. " + _FEC_R6),
    _join("fec_reported_financial_summaries", "candidate_native_id", "fec_candidate_history", "candidate_id",
          10_405, 21, "scope", _LITERAL_IDS + "The 21 not in the bulk candidate master are FEC's aggregate codes "
          "P00000001-P00000003, committee ids the file puts in its candidate column, the empty string and ten "
          "candidate ids no cycle's master lists. A candidate has one row per cycle, so a key meets several. "
          + _FEC_R6),
    _join("fec_contribution_aggregates", "candidate_native_id", "fec_candidate_history", "candidate_id", 27, 3,
          "scope", _LITERAL_IDS + "The 3 missing are FEC's aggregate codes P00000001 (all candidates), P00000002 "
          "(Democrats) and P00000003 (Republicans). " + _FEC_R6),
    _join("fec_loan_guarantors", ("collection_id", "back_reference_transaction_id"), "fec_loans",
          ("collection_id", "transaction_id"), 22, 0,
          reason="A Schedule C2 guarantor names its Schedule C loan by transaction id within one filing; each of the "
                 "22 met exactly one loan, though a few other loan keys repeat. " + _FEC_R6),
    _join("fec_legal_parties", "matter_record_id", "fec_legal_matters", "record_id", 184, 0,
          expected_cardinality="one"),
    _join("fec_legal_events", "matter_record_id", "fec_legal_matters", "record_id", 184, 0,
          expected_cardinality="one"),
    _join("fec_legal_documents", "matter_record_id", "fec_legal_matters", "record_id", 184, 0,
          expected_cardinality="one"),
    # GAO.
    _join("gao_recommendations", "report_id", "gao_reports", "report_id", 1_811, 0,
          reason="First public generation 14761739, measured 2026-09-30: every distinct report key resolves "
          "against gao_reports b9c9c974. Receipt docs/evidence/new-source-publication-2026-09-30.json."),
    # Courts.
    _join("court_opinions", "cluster_id", "court_opinion_clusters", "cluster_id", 10_069_107, 21, "scope",
          "The publisher cuts each export at a different hour, clusters first (2026-06-30: 08:16 UTC, opinions "
          "09:56), so 21 opinions name newer clusters. All 21 are RECAP trial-court opinions that opinion search, "
          "the clusters' catch-up, does not index, so they resolve only with the next export. Receipt "
          "join-gaps-2026-09-26/i/."),
    _join("court_citations", "cluster_id", "court_opinion_clusters", "cluster_id", 7_844_636, 63, "scope",
          "The publisher cuts citations hours after clusters (2026-06-30: 19:14 vs 08:16 UTC), so 63 name newer "
          "clusters. The clusters' id-keyed catch-up adds the 43 search indexes; the other 20 were merged away "
          "since or are not indexed. Re-record at 20 once that catch-up is published. Receipt "
          "join-gaps-2026-09-26/i/."),
    _join("court_dockets", "cl_docket_id", "court_opinion_clusters", "cl_docket_id", 11_475, 9_214, "design",
          "court_dockets is a PACER (RECAP) docket selection: most PACER dockets have no published opinion, and for "
          "an appellate case the publisher often keeps two docket objects, the court-website scraper's, which the "
          "cluster names, and RECAP's, which this table holds, so a decision of a case this table holds may not join "
          "by id (30 of the 100 cadc cases present on 2026-10-02 did not). The docket number, in "
          "court_opinions.download_url or court_dockets.docket_number, is the fallback key."),
    # Federal spending and registration.
    _join("usaspending_recipients", "uei", "sam_entities", "uei", 140_849, 20_417, "scope",
          "sam_entities holds SAM's public active registrations, every registration year from 1996. The recipients "
          "are every one funded in the trailing 12 months (decision 48; 231,700 rows on 2026-09-26), and a smaller "
          "recipient more often has no public active registration: the top 10,000 resolved 87.8%, all funded "
          "recipients 85.5%, with 120,432 UEIs resolving against 6,390 before. Of a 40-UEI sample of the orphans, "
          "the entity API held no public record for 38. Receipts join-map-2026-09-26/sam-entities-after-backfill.json "
          "and usaspending-unresolved-uei-sample-2026-09-26.txt."),
    # Lobbying disclosure: the activity tables joined the family at run 36264742453 (added_tables). Receipt
    # join-map-2026-09-26/lobbying-activities-after-migration.json. Those first-migration populations (247, 522)
    # were served as baselines until join_measurements.json measured both in full (2026-10-03). A static count is a
    # population at its date: check_table_joins compares rates, so it never sees one go stale (recorded debt).
    _join("lobbying_activities", "filing_uuid", "lobbying_filings", "filing_uuid", 247, 0),
    _join("lobbying_activity_lobbyists", ("filing_uuid", "activity_index"),
          "lobbying_activities", ("filing_uuid", "activity_index"), 522, 0),
    _join("lobbying_activity_lobbyists", "filing_uuid", "lobbying_filings", "filing_uuid", 1_696_361, 0),
    # Profiles as last captured (ss-hqoi C3): a filing's registrant_id and client_id are the ids of the records it
    # embeds, and the rebuild keeps one profile per embedded id, so every key resolves by construction. Measured on
    # the no-upload rebuild of all 80,523 retained pages (2026-10-30, 1,965,568 filings; the live eb88eb81 holds
    # 129,462 client ids, the rebuild adds the 481 filings captured 10-19): distinct keys and missing below.
    _join("lobbying_filings", "registrant_id", "lobbying_registrants", "registrant_id", 14_881, 0,
          expected_cardinality="one"),
    _join("lobbying_filings", "client_id", "lobbying_clients", "client_id", 129_495, 0, expected_cardinality="one"),
    _join("lobbying_clients", "registrant_id", "lobbying_registrants", "registrant_id", 14_881, 0,
          expected_cardinality="one"),
    # The filing's other source lists (round 8 Q1): every entry is shaped from its filing's record. Distinct filings
    # holding one, counted over all 80,523 retained pages with the latest capture per filing (2026-10-31).
    _join("lobbying_foreign_entities", "filing_uuid", "lobbying_filings", "filing_uuid", 11_168, 0),
    _join("lobbying_affiliated_organizations", "filing_uuid", "lobbying_filings", "filing_uuid", 12_327, 0),
    _join("lobbying_conviction_disclosures", "filing_uuid", "lobbying_filings", "filing_uuid", 1_214, 0),
    # Native legal references: each observation names the complete read of its input (spicy-docs 0.52.0's
    # contract reference). Measured 2026-09-28 on native-legal-references 53755e3e…: both scopes resolve.
    _join("native_legal_references", "scope_id", "native_legal_reference_reads", "scope_id", 2, 0),
    # Scorecard source joins, measured over every selected input row.
    _join('scorecards', 'publisher_id',
          'scorecard_publishers', 'publisher_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecards', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_snapshots', 'scorecard_id',
          'scorecards', 'scorecard_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_methodologies', 'scorecard_id',
          'scorecards', 'scorecard_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_methodologies', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metrics', 'scorecard_id',
          'scorecards', 'scorecard_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metrics', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metrics', ('scorecard_id', 'methodology_id'),
          'scorecard_methodologies', ('scorecard_id', 'methodology_id'),
          2, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_items', 'scorecard_id',
          'scorecards', 'scorecard_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_items', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metric_items', 'scorecard_id',
          'scorecards', 'scorecard_id',
          3, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metric_items', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          3, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metric_items', ('scorecard_id', 'metric_id'),
          'scorecard_metrics', ('scorecard_id', 'metric_id'),
          3, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metric_items', ('scorecard_id', 'item_id'),
          'scorecard_items', ('scorecard_id', 'item_id'),
          169, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_metric_items', ('scorecard_id', 'methodology_id'),
          'scorecard_methodologies', ('scorecard_id', 'methodology_id'),
          0, 0, 'empty',
          'Full selected-input measurement on 2026-10-04; all child methodology keys are NULL.',
          expected_cardinality="one"),
    _join('scorecard_metric_components', 'scorecard_id',
          'scorecards', 'scorecard_id',
          0, 0, 'empty',
          'Full selected-input measurement on 2026-10-04; child table contains no rows.',
          expected_cardinality="one"),
    _join('scorecard_metric_components', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          0, 0, 'empty',
          'Full selected-input measurement on 2026-10-04; child table contains no rows.',
          expected_cardinality="one"),
    _join('scorecard_metric_components', ('scorecard_id', 'parent_metric_id'),
          'scorecard_metrics', ('scorecard_id', 'metric_id'),
          0, 0, 'empty',
          'Full selected-input measurement on 2026-10-04; child table contains no rows.',
          expected_cardinality="one"),
    _join('scorecard_metric_components', ('scorecard_id', 'component_metric_id'),
          'scorecard_metrics', ('scorecard_id', 'metric_id'),
          0, 0, 'empty',
          'Full selected-input measurement on 2026-10-04; child table contains no rows.',
          expected_cardinality="one"),
    _join('scorecard_members', 'scorecard_id',
          'scorecards', 'scorecard_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_members', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_ratings', 'scorecard_id',
          'scorecards', 'scorecard_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_ratings', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_ratings', ('scorecard_id', 'metric_id'),
          'scorecard_metrics', ('scorecard_id', 'metric_id'),
          16, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_ratings', ('scorecard_id', 'publisher_member_key'),
          'scorecard_members', ('scorecard_id', 'publisher_member_key'),
          2_099, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_item_results', 'scorecard_id',
          'scorecards', 'scorecard_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_item_results', 'snapshot_id',
          'scorecard_snapshots', 'snapshot_id',
          4, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_item_results', ('scorecard_id', 'publisher_member_key'),
          'scorecard_members', ('scorecard_id', 'publisher_member_key'),
          2_095, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_item_results', ('scorecard_id', 'item_id'),
          'scorecard_items', ('scorecard_id', 'item_id'),
          196, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),
    _join('scorecard_member_item_results', ('scorecard_id', 'metric_id', 'item_id', 'participation_id'),
          'scorecard_metric_items', ('scorecard_id', 'metric_id', 'item_id', 'participation_id'),
          140, 0, 'complete',
          'Full selected-input measurement on 2026-10-04; every non-null key resolves to one parent row.',
          expected_cardinality="one"),

    # Relationships measured across the complete published inputs on 2026-10-03.
    # These use the same coverage and parent-cardinality checks as every other join.
    _join('scorecard_member_links', ('scorecard_id', 'publisher_member_key', 'source_snapshot_id'),
          'scorecard_members', ('scorecard_id', 'publisher_member_key', 'snapshot_id'),
          2_102, 0, 'scope',
          'Matches the publisher identity and the exact source snapshot used for resolution. A newer source snapshot '
          'is not interchangeable.',
          expected_cardinality="one"),
    _join('scorecard_item_links', ('scorecard_id', 'item_id', 'source_snapshot_id'),
          'scorecard_items', ('scorecard_id', 'item_id', 'snapshot_id'),
          225, 0, 'scope',
          'Matches the publisher identity and the exact source snapshot used for resolution. A newer source snapshot '
          'is not interchangeable.',
          expected_cardinality="one"),
    *(_join(child, 'source_snapshot_id', 'scorecard_snapshots', 'snapshot_id',
            0, 0, 'unmeasured',
            'The resolver recorded this literal source snapshot. Main navigation also requires the same '
            'scorecard edition; this association remains separate from the current business entity and '
            'the recorded official-target resolution. No capture ID is interpreted as a snapshot ID.',
            expected_cardinality="one")
      for child in ('scorecard_member_links', 'scorecard_item_links')),
    _join('scorecard_member_links', 'bioguide_id',
          'members', 'bioguide_id',
          565, 0, 'scope',
          'Exact identifier emitted by the scorecard resolver. Unresolved or ambiguous identifiers remain NULL. The '
          'explorer follows current published records; input_pins_json identifies the historical input used by the '
          'resolver.',
          expected_cardinality="one"),
    _join('scorecard_item_links', 'bill_id',
          'congress_bills', 'bill_id',
          95, 0, 'scope',
          'Exact identifier emitted by the scorecard resolver. Unresolved or ambiguous identifiers remain NULL. The '
          'explorer follows current published records; input_pins_json identifies the historical input used by the '
          'resolver.',
          expected_cardinality="one"),
    _join('scorecard_item_links', 'vote_id',
          'roll_call_votes', 'vote_id',
          112, 0, 'scope',
          'Exact identifier emitted by the scorecard resolver. Unresolved or ambiguous identifiers remain NULL. The '
          'explorer follows current published records; input_pins_json identifies the historical input used by the '
          'resolver.',
          expected_cardinality="one"),
    _join('scorecard_item_links', 'amendment_id',
          'amendments', 'amendment_id',
          0, 0, 'empty',
          'Exact identifier emitted by the scorecard resolver. Unresolved or ambiguous identifiers remain NULL. The '
          'explorer follows current published records; input_pins_json identifies the historical input used by the '
          'resolver.',
          expected_cardinality="one"),
    _join('fec_account_transfers', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          10, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_allocated_disbursements', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          10, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_allocation_bases', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          8, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_bundled_contributions', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          1, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_coordinated_party_expenditures', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          3, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_debts', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          11, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_disbursements', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          13, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_filing_report_observations', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          64, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_filing_text_observations', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          9, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_inaugural_donations', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          11, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_independent_expenditures', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          2, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_loan_guarantors', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          2, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_loan_terms', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          2, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_loans', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          8, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_receipts', 'definition_set_id',
          'fec_filing_definitions', 'record_id',
          18, 0, 'complete',
          'The normalized original-filing mapper supplies the exact source layout identity. Rows from other source '
          'formats can have no definition key; this does not prove filing conformance. Parent keys were unique in '
          'the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_collections', 'source_family',
          'fec_source_catalog', 'source_family',
          26, 0, 'complete',
          'The collection records the official FEC inventory family. A catalog route identifies available source '
          'paths, not acquired coverage. Parent keys were unique in the complete selected inputs checked on '
          '2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('feed_summary', 'docket_id',
          'dockets', 'docket_id',
          279_719, 0, 'complete',
          'The rollup retains each input docket identity. Input generations can advance independently. Parent keys '
          'were unique in the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('agency_monthly_volume', 'agency_code',
          'agency_stats', 'agency_code',
          316, 0, 'complete',
          'Both aggregates retain the Regulations.gov agency code; counts and date windows remain distinct. Parent '
          'keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('discovery_signals', 'agency_code',
          'agency_stats', 'agency_code',
          11, 0, 'complete',
          'Both aggregates retain the Regulations.gov agency code; counts and date windows remain distinct. Parent '
          'keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('cbo_cost_estimates', 'publication_id',
          'cbo_feed_items', 'publication_id',
          14_800, 3, 'scope',
          'CBO publication identity connects each estimate to its feed observation. A feed publication may name '
          'several bills or no bill. Three publication IDs were absent from the selected feed; the cause is not '
          'established by this join check. Parent keys were unique in the complete selected inputs checked on '
          '2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('committee_reports', 'package_id',
          'committee_report_reads', 'package_id',
          160, 0, 'complete',
          'Each report or hearing belongs to its GovInfo package read checkpoint. The checkpoint describes a read, '
          'not a positive bill relationship. Parent keys were unique in the complete selected inputs checked on '
          '2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('hearing_transcripts', 'package_id',
          'committee_report_reads', 'package_id',
          185, 0, 'complete',
          'Each report or hearing belongs to its GovInfo package read checkpoint. The checkpoint describes a read, '
          'not a positive bill relationship. Parent keys were unique in the complete selected inputs checked on '
          '2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('court_docket_groups', 'cl_docket_id',
          'court_dockets', 'cl_docket_id',
          914, 0, 'complete',
          'This is an inferred, edition-scoped grouping of held docket IDs. The parent is a representative record, '
          'not a publisher-designated main case; independent publication changes can leave a missing docket. Parent '
          'keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('court_docket_groups', 'parent_cl_docket_id',
          'court_dockets', 'cl_docket_id',
          407, 0, 'complete',
          'This is an inferred, edition-scoped grouping of held docket IDs. The parent is a representative record, '
          'not a publisher-designated main case; independent publication changes can leave a missing docket. Parent '
          'keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('court_opinion_pdf_extractions', 'cluster_id',
          'court_opinion_clusters', 'cluster_id',
          0, 0, 'unmeasured',
          'The native cluster ID recorded with the extraction identifies its opinion group. The guarded '
          'opinion relationship separately identifies the exact captured input. This direct group relationship '
          'has no complete selected-input baseline yet.', expected_cardinality='one'),
    _join('court_opinion_pdf_extractions', ('opinion_id', 'native_sha1'),
          'court_opinions', ('opinion_id', 'sha1'),
          3, 0, 'complete',
          'The original opinion ID and its held native body digest identify the input. Preserve both fields so a '
          'later changed source body cannot be presented as the extraction input. Parent keys were unique in the '
          'complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    *(_join('court_opinion_texts', key, parent, key,
            0, 0, 'unmeasured',
            'Both rows come from the same CourtListener opinions export; only a bounded build of its head exists, so '
            'the relationship has no complete baseline yet.', expected_cardinality='one')
      for key, parent in (('opinion_id', 'court_opinions'), ('cluster_id', 'court_opinion_clusters'))),
    # The pointer from an opinion without text (text_source 'none') to its extracted PDF (owner, 2026-10-31).
    _join('court_opinion_pdf_extractions', 'opinion_id',
          'court_opinion_texts', 'opinion_id',
          0, 0, 'unmeasured',
          'An opinion the export holds no text for has a court_opinion_texts row with text_source none; its '
          'extracted PDF, where one exists, is this row. Only a bounded build of the texts exists, so the '
          'relationship has no complete baseline yet.', expected_cardinality='one'),
    _join('court_citation_map', ('citing_opinion_id', 'dump_date'),
          'court_opinions', ('opinion_id', 'dump_date'),
          7_511_929, 262, 'scope',
          'CourtListener opinion identity is matched within the same exported edition. Publisher exports can contain '
          'a small residue of absent opinion IDs; a newer independent export does not supply a same-edition match. '
          'Parent keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that '
          'cardinality.',
          expected_cardinality="one"),
    _join('court_citation_map', ('cited_opinion_id', 'dump_date'),
          'court_opinions', ('opinion_id', 'dump_date'),
          4_519_538, 1, 'scope',
          'CourtListener opinion identity is matched within the same exported edition. Publisher exports can contain '
          'a small residue of absent opinion IDs; a newer independent export does not supply a same-edition match. '
          'Parent keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that '
          'cardinality.',
          expected_cardinality="one"),
    _join('court_parentheticals', ('described_opinion_id', 'dump_date'),
          'court_opinions', ('opinion_id', 'dump_date'),
          1_203_305, 0, 'complete',
          'CourtListener opinion identity is matched within the same exported edition. Publisher exports can contain '
          'a small residue of absent opinion IDs; a newer independent export does not supply a same-edition match. '
          'Parent keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that '
          'cardinality.',
          expected_cardinality="one"),
    _join('court_parentheticals', ('describing_opinion_id', 'dump_date'),
          'court_opinions', ('opinion_id', 'dump_date'),
          1_852_737, 176, 'scope',
          'CourtListener opinion identity is matched within the same exported edition. Publisher exports can contain '
          'a small residue of absent opinion IDs; a newer independent export does not supply a same-edition match. '
          'Parent keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces that '
          'cardinality.',
          expected_cardinality="one"),
    _join('member_vote_terms', ('vote_id', 'member_key'),
          'member_votes', ('vote_id', 'member_key'),
          10_561_417, 0, 'complete',
          'The derived term row retains the complete member-vote key; member_key alone identifies a row only within '
          'its roll call. Parent keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces '
          'that cardinality.',
          expected_cardinality="one"),
    _join('member_vote_terms', ('bioguide_id', 'term_index'),
          'member_terms', ('bioguide_id', 'term_index'),
          5_884, 0, 'complete',
          'The builder selects one service term by the vote date and chamber. Unresolved or ambiguous terms remain '
          'NULL and are not joined. Parent keys were unique in the complete selected inputs checked on 2026-10-03; '
          'CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('document_citations', ('document_kind', 'document_key', 'text_sha256'),
          'document_citation_reads', ('document_kind', 'document_key', 'text_sha256'),
          79, 67, 'scope',
          'The exact document kind, identity and text digest bind a citation to its held-field read. The reads table '
          'covers held-field extraction only; print-derived citations have separate package metadata. Parent keys '
          'were unique in the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('document_citations', ('document_key', 'text_sha256'),
          'budget_volumes', ('package_id', 'text_sha256'),
          0, 0, 'unmeasured',
          'A body can occur in several dated metadata revisions. Historical navigation returns each matching '
          'parent revision; current citation reads first select the latest package revision.',
          expected_cardinality="many"),
    _join('document_citations', ('document_key', 'text_sha256'),
          'house_activity_reports', ('package_id', 'text_sha256'),
          0, 0, 'unmeasured',
          'A body can occur in several dated metadata revisions. Historical navigation returns each matching '
          'parent revision; current citation reads first select the latest package revision.',
          expected_cardinality="many"),
    _join('house_activity_reports', 'committee_system_code',
          'committees', 'system_code',
          27, 0, 'complete',
          'The report carries Congress.gov committee identity. The committee table holds its history; a current name '
          'alone is not the historical name for the report. Parent keys were unique in the complete selected inputs '
          'checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('house_activity_reports', 'submitted_by_bioguide_id',
          'members', 'bioguide_id',
          33, 0, 'complete',
          'The publisher-supplied Bioguide ID identifies the member who submitted the report; it does not infer '
          'service dates. Parent keys were unique in the complete selected inputs checked on 2026-10-03; CI enforces '
          'that cardinality.',
          expected_cardinality="one"),
    _join('house_communications', 'referral_system_code',
          'committees', 'system_code',
          22, 0, 'complete',
          'The scalar referral is the first source-stated committee referral. Further referrals remain in '
          'committees_json and require array expansion. Parent keys were unique in the complete selected inputs '
          'checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('house_communications', 'record_package_id',
          'record_issues', 'package_id',
          0, 0, 'empty',
          'The communication reconstruction retains the exact Congressional Record package. Only held issues can '
          'resolve; a package ID does not imply article text was acquired. No non-null composite keys were present '
          'in the measured child; no successful traversal is claimed. Parent keys were unique in the complete '
          'selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
    _join('bill_family_backfills', ('congress', 'bill_type', 'number'),
          'congress_bills', ('congress', 'bill_type', 'bill_number'),
          0, 0, 'empty',
          'A backfill attempt names a bill by Congress, type and number, including refused attempts. The currently '
          'published child is empty; this is a supported identity path, not proof that every future attempted bill '
          'will have an output. No non-null composite keys were present in the measured child; no successful '
          'traversal is claimed. Parent keys were unique in the complete selected inputs checked on 2026-10-03; CI '
          'enforces that cardinality.',
          expected_cardinality="one"),
    _join('fec_filing_definition_evidence', 'target_record_id',
          'fec_filing_definitions', 'record_id',
          174, 0, 'complete',
          'The definition binder writes these witnesses with target_table=fec_filing_definitions and the exact '
          'layout digest as target_record_id. Each definition may have several evidence rows. Parent keys were '
          'unique in the complete selected inputs checked on 2026-10-03; CI enforces that cardinality.',
          expected_cardinality="one"),
)


# Column inventory candidates checked against all selected key columns, not row samples.
JOINS += (
    _join('amendments', ('amended_bill_id',), 'congress_bills', ('bill_id',),
          203, 0, 'complete', 'Bill that the amendment targets. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('amendments', ('amended_amendment_id',), 'amendments', ('amendment_id',),
          73, 0, 'complete', 'Amendment that this amendment modifies; null means no such target is supplied. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('committees', ('parent_system_code',), 'committees', ('system_code',),
          40, 0, 'complete', 'Parent of this committee or subcommittee. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('committee_assignments', ('parent_system_code',), 'committees', ('system_code',),
          19, 0, 'complete', 'Parent committee of the assigned committee. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('bill_committee_activities', ('parent_system_code',), 'committees', ('system_code',),
          29, 0, 'complete', 'Parent committee of the committee recording activity. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('bill_committee_activities', ('system_code',), 'committees', ('system_code',),
          255, 0, 'complete', 'Committee recording activity on the bill. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('committee_reports', ('recital_bill_id',), 'congress_bills', ('bill_id',),
          153, 0, 'complete', 'Bill the report cover says it accompanies; retain the recital separately from other attachments. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('cbo_cost_estimates', ('title_bill_id',), 'congress_bills', ('bill_id',),
          13128, 0, 'complete', 'Bill identified by the title reader; retain its documented extraction caveats. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('bill_vote_references', ('congress', 'chamber', 'session', 'roll_number'), 'roll_call_votes', ('congress', 'chamber', 'session', 'roll_number'),
          11726, 0, 'complete', 'All four fields identify the vote. Congress alone is a filter. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('agency_lifecycle_stats', ('agency_code',), 'agency_stats', ('agency_code',),
          163, 0, 'complete', 'Null agency codes represent all agencies together and are excluded. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('rulemaking_lifecycles', ('agency_code',), 'agency_stats', ('agency_code',),
          170, 0, 'complete', 'Agency assigned to the proceeding; preserve mapping provenance. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('comments_index', ('docket_id',), 'dockets', ('docket_id',),
          60296, 28, 'scope', 'Partial navigation: 28 selected docket IDs have no destination. Do not claim complete coverage. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
    _join('comments_index', ('agency_code',), 'agency_stats', ('agency_code',),
          180, 0, 'complete', 'Receiving agency of the counted comments. Full selected keys and unique parent keys checked on 2026-10-06; CI checks resolution and parent uniqueness.',
          expected_cardinality='one'),
)

# New full-key/attribute baselines include raw-row cardinality and immutable input
# URLs. Keep historical declarations above as lineage; replace their measured
# values only from the replayable complete-input receipt.
_MEASUREMENTS = json.loads(Path(__file__).with_name("join_measurements.json").read_text())
_MEASURED_BY_NAME = {item["join"]: item for item in _MEASUREMENTS["results"]}
JOINS = tuple(
    replace(join, baseline_keys=measurement["keys"], baseline_missing=measurement["missing"],
            kind=join.kind if measurement.get("receipt") else "complete",
            expected_cardinality="one", measurement=measurement,
            reason=join.reason if measurement.get("receipt") else f"Full selected-input measurement on {measurement.get('measured_on', _MEASUREMENTS['measured_on'])}; "
                   "immutable URLs, SQL and cardinality in join_measurements.json. "
                   + measurement.get("reason", "Full-key hardening does not assert prior production amplification."))
    if (measurement := _MEASURED_BY_NAME.get(join.name)) is not None else join
    for join in JOINS
)


# Source joins retain their measured keys, including processing observations.
# Public joins use only native subject keys; changed keys require a new baseline.
SOURCE_JOINS = JOINS
_PROCESSING_TABLES = {name for name, spec in _policy_descriptors().items() if spec["receipt_only"]}
_NATIVE_JOIN_FIELDS = {
    ("bill_sections", "source"): "printing_id", ("bill_versions", "source"): "printing_id",
    ("section_diffs", "from_source"): "from_printing_id", ("section_diffs", "to_source"): "to_printing_id",
    **{(name, "text_sha256"): "body_version_id" for name in
       ("document_citations", "budget_volumes", "house_activity_reports")},
}
# These tables now have one native row per logical subject. The captured source
# snapshot still qualifies the original relationship in the corresponding receipt.
_NATIVE_SCOPE_FIELDS = {
    ("scorecard_member_links", "source_snapshot_id"), ("scorecard_members", "snapshot_id"),
    ("scorecard_item_links", "source_snapshot_id"), ("scorecard_items", "snapshot_id"),
    ("court_citation_map", "dump_date"), ("court_parentheticals", "dump_date"), ("court_opinions", "dump_date"),
}
_PROCESSING_JOIN_FIELDS = {
    ("court_opinion_pdf_extractions", "native_sha1"), ("court_opinions", "sha1"),
    ("house_communications", "record_package_id"),
}


def _processing_join(join):
    # The original snapshot baselines describe receipt-era inputs. Portable
    # main recipes expose these associations only when both schemas publish them.
    if join.child == "scorecard_snapshots" or join.parent == "scorecard_snapshots":
        return True
    # Date evidence belongs to its dated_by namespace; bare ID equality would
    # route Register and agenda IDs into Regulations.gov documents.
    if join.child == "lifecycle_events" and join.parent == "documents":
        return True
    # Main read scopes use the maintained native_legal_read recipe. Keep the
    # historical scalar baseline as lineage instead of publishing a second route.
    if join.child == 'native_legal_references' and join.parent == 'native_legal_reference_reads':
        return True
    # The selected service term uses a guarded portable row recipe: processing
    # keeps the native index as text while native subjects keep it as BIGINT.
    if (join.child, join.child_columns, join.parent, join.parent_columns) == (
        'member_vote_terms', ('bioguide_id', 'term_index'), 'member_terms', ('bioguide_id', 'term_index')
    ):
        return True
    # Resolver outputs require the recorded resolution status, not bare equality.
    # The same declarations supply guarded main-row navigation recipes.
    if join.child in {'scorecard_member_links', 'scorecard_item_links'} and join.parent in {
        'members', 'congress_bills', 'roll_call_votes', 'amendments',
    }:
        return True
    if join.child in _PROCESSING_TABLES or join.parent in _PROCESSING_TABLES:
        return True
    if join.child == "hearing_transcripts" and join.child_columns == ("bill_id",):
        return True
    return any((table, column) in _PROCESSING_JOIN_FIELDS or
               (table.startswith("fec_") and column in {"collection_id", "definition_set_id"})
               for table, columns in ((join.child, join.child_columns), (join.parent, join.parent_columns))
               for column in columns)


def _native_join(join):
    def columns(table, names):
        return tuple(_NATIVE_JOIN_FIELDS.get((table, name), name) for name in names
                     if (table, name) not in _NATIVE_SCOPE_FIELDS or join.parent == "scorecard_snapshots")
    child, parent = columns(join.child, join.child_columns), columns(join.parent, join.parent_columns)
    if (child, parent) == (join.child_columns, join.parent_columns):
        return join
    if not child or len(child) != len(parent):
        raise ValueError(f"Native join loses its declared key: {join.name}")
    return replace(join, child_columns=child, parent_columns=parent,
                   baseline_keys=0, baseline_missing=0, kind="unmeasured", measurement=None,
                   reason=(join.reason if join.parent in {"budget_volumes", "house_activity_reports"} else
                           "Native subject keys replace source capture keys. The original measured relationship "
                           "remains in processing_joins; this subject-key relationship has no measured baseline yet."))


JOINS = tuple(_native_join(join) for join in SOURCE_JOINS if not _processing_join(join))
RETIRED_PROCESSING_JOINS = tuple(join for join in SOURCE_JOINS
                               if _processing_join(join) or _native_join(join) != join)


def joins_for(table: str) -> dict[str, list[dict]]:
    """The declared joins where ``table`` is the child (``outgoing``) or the parent (``incoming``)."""
    return {
        "outgoing": [record(join) for join in JOINS if join.child == table],
        "incoming": [record(join) for join in JOINS if join.parent == table],
    }


def record(join: Join) -> dict:
    """One join as the bundled record states it."""
    return {
        "child": join.child,
        "child_columns": list(join.child_columns),
        "parent": join.parent,
        "parent_columns": list(join.parent_columns),
        "kind": join.kind,
        "reason": join.reason,
        "measured_via": join.measured_via,
        "baseline_keys": join.baseline_keys,
        "baseline_missing": join.baseline_missing,
        "floor_pct": join.floor_pct,
        "expected_cardinality": join.expected_cardinality,
        "measurement": join.measurement,
    }


def references() -> dict[str, list[dict]]:
    """Per child table, ``(child_columns, parent_table, parent_columns)`` with no measurement: the contract shape."""
    shaped: dict[str, list[dict]] = {}
    for join in JOINS:
        shaped.setdefault(join.child, []).append({
            "child_columns": list(join.child_columns),
            "parent_table": join.parent,
            "parent_columns": list(join.parent_columns),
        })
    return shaped


def maintainer_path(text: str) -> str | None:
    """The first path on a maintainer's machine ``text`` names, or None; a served record must name none."""
    found = _MAINTAINER_PATH.search(text)
    return found.group(0) if found else None


def joins_record() -> dict:
    """The bundled ``table_joins.json`` document."""
    from spicy_regs.explorer_navigation import declarations
    return {
        "format": RECORD_FORMAT,
        "version": 1,
        "basis": BASIS,
        # The server ships this record to the public; receipts kept on a maintainer's machine
        # mean nothing there, so only repository-relative receipts are bundled.
        "baseline": {
            "date": BASELINE_DATE,
            "receipts": [receipt for receipt in BASELINE_RECEIPTS if not maintainer_path(receipt)],
        },
        "kinds": list(KINDS),
        "joins": [record(join) for join in JOINS],
        "processing_joins": [record(join) for join in RETIRED_PROCESSING_JOINS],
        "references": references(),
        "navigation": declarations(RETIRED_PROCESSING_JOINS),
    }


def declaration_errors(schemas: dict[str, list[tuple[str, str]]]) -> list[str]:
    """Each join names known tables and columns, a known kind, and no duplicate."""
    errors: list[str] = []
    seen: set[str] = set()
    for join in JOINS:
        if join.name in seen:
            errors.append(f"duplicate join declaration: {join.name}")
        seen.add(join.name)
        if join.kind not in KINDS:
            errors.append(f"{join.name}: unknown kind {join.kind!r}")
        if len(join.child_columns) != len(join.parent_columns):
            errors.append(f"{join.name}: child and parent column counts differ")
        if join.kind != "complete" and not join.reason:
            errors.append(f"{join.name}: a {join.kind} join must state its reason")
        if not 0 <= join.baseline_missing <= join.baseline_keys:
            errors.append(f"{join.name}: baseline missing count outside 0..keys")
        if (join.kind in {"empty", "unmeasured"}) != (join.baseline_keys == 0) and join.kind != "design":
            errors.append(f"{join.name}: an empty or unmeasured join must have no baseline keys")
        for table, columns in ((join.child, join.child_columns), (join.parent, join.parent_columns)):
            known = {name for name, _ in schemas.get(table, [])}
            if not known:
                errors.append(f"{join.name}: {table} is not a dictionary table")
            errors.extend(f"{join.name}: {table}.{column} is not a declared column"
                          for column in columns if known and column not in known)
        if join.measured_via and join.measured_via not in schemas:
            errors.append(f"{join.name}: measured_via {join.measured_via} is not a dictionary table")
    return errors
