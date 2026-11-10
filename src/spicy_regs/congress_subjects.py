"""Explicit subject schemas for congressional people, events and relations.

SpicyDocs continues to shape retained source observations. This adapter checks
every incoming field before converting business data; raw conversion inputs and
processing fields are supplied separately to the shared ETL receipt writer.
"""
from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import pyarrow as pa

TEXT, INTEGER, BOOLEAN = pa.string(), pa.int64(), pa.bool_()

def struct(names: str, **types: pa.DataType) -> pa.DataType:
    return pa.struct([(name, types.get(name, TEXT)) for name in names.split()])

def objects(names: str, **types: pa.DataType) -> pa.DataType:
    return pa.list_(struct(names, **types))

# Source spellings inside structs remain visible; no positional pairing is inferred.
NATIVE = {
    ("bill_subjects", "subjects_json"): pa.list_(TEXT),
    ("congress_bills", "subjects_json"): pa.list_(TEXT),
    ("congress_bills", "related_bills_json"): objects(
        "bill_type congress latest_action_date latest_action_text number relationship_details title",
        relationship_details=objects("identifiedBy type")),
    ("committee_meetings", "committees_json"): objects("name systemCode url"),
    ("committee_meetings", "hearing_jackets_json"): pa.list_(TEXT),
    ("committee_meetings", "bill_ids_json"): pa.list_(TEXT),
    ("committee_meetings", "witnesses_json"): objects("name position organization"),
    ("committee_meetings", "witness_documents_json"): objects("documentType format url"),
    ("committee_meetings", "meeting_documents_json"): objects("description documentType format name url"),
    ("committee_meetings", "document_urls_json"): pa.list_(TEXT),
    ("committee_meetings", "videos_json"): objects("name url"),
    ("committees", "subcommittees_json"): pa.list_(TEXT),
    ("committees", "history_json"): objects(
        "committeeTypeCode establishingAuthority libraryOfCongressName locLinkedDataId naraId "
        "officialName startDate superintendentDocumentNumber updateDate endDate"),
    ("house_communications", "committees_json"): objects("name referralDate systemCode url"),
    ("house_communications", "matching_requirements_json"): pa.list_(INTEGER),
    ("house_communications", "rin_occurrences_json"): pa.list_(TEXT),
    ("members", "bioguide_previous_json"): pa.list_(TEXT),
    ("members", "fec_ids_json"): pa.list_(TEXT),
    ("members", "other_names_json"): objects("first middle last nickname suffix official_full start end"),
    ("press_releases", "categories_json"): pa.list_(TEXT),
    ("record_issues", "chambers"): pa.list_(TEXT),
    ("record_issues", "section_names"): pa.list_(TEXT),
    ("record_issues", "sections_json"): objects("endPage name startPage text", text=objects("type url part")),
    ("record_issues", "entire_issue_json"): objects("part type url"),
    ("roll_call_votes", "tallies_json"): objects("choice count", count=INTEGER),
    ("roll_call_votes", "documents_json"): objects(
        "congress name nomination_id number short_title title type", congress=INTEGER
    ),
    ("roll_call_votes", "amendments_json"): objects(
        "number purpose to_amendment_number to_amendment_to_amendment_number to_document_number to_document_short_title"),
    ("roll_call_votes", "party_totals_json"): objects("party yea-total nay-total present-total not-voting-total",
        **{name: INTEGER for name in ("yea-total", "nay-total", "present-total", "not-voting-total")}),
    ("treaties", "titles_json"): objects("title titleType"),
    ("treaties", "countries_json"): pa.list_(TEXT),
    ("treaties", "index_terms_json"): pa.list_(TEXT),
    ("treaties", "related_docs_json"): objects("congress congressReceived number suffix title url"),
}

# Complete source field inventories. A new producer field must be classified explicitly.
INPUT_COLUMNS = {
    'amendments': tuple(
        'amendment_id congress amendment_type amendment_number purpose description proposed_date '
        'submitted_date chamber update_date latest_action_date latest_action_text sponsor_bioguide_id '
        'sponsor_full_name sponsor_party amended_bill_id amended_amendment_id url'
    .split()),
    'bill_actions': tuple(
        'bill_id action_index action_date action_time action_text action_code action_type '
        'source_system_code source_system_name recorded_vote_count is_latest_action stage stage_rule '
        'stage_matcher'
    .split()),
    'bill_committee_activities': tuple('bill_id system_code activity_name activity_date occurrence parent_system_code snapshot_update_date'.split()),
    'cbo_feed_items': tuple('publication_id congress pub_date title link description bill_number feed_url'.split()),
    'bill_committees': tuple(
        'bill_id system_code name chamber committee_type parent_system_code is_subcommittee '
        'snapshot_update_date referral_signal referral_rule'
    .split()),
    'bill_cosponsors': tuple(
        'bill_id input_sha256 cosponsor_index bioguide_id full_name sponsorship_date '
        'sponsorship_date_status is_original_raw sponsorship_withdrawn_date '
        'sponsorship_withdrawn_date_status party state district source_path source_xml'
    .split()),
    'bill_family_archives': tuple('name link formatted_last_modified_time modified_at size congress bill_type observed_at'.split()),
    'bill_family_backfill_walks': tuple(
        'congress bill_type declared_count records_walked pages_walked list_completed unwalkable_count '
        'repeated_count backfilled_count observed_at'
    .split()),
    'bill_family_backfills': tuple('congress bill_type number list_update_date_including_text refusal observed_at'.split()),
    'bill_subjects': tuple('bill_id policy_area subjects_json subject_count carrier enriched_at'.split()),
    'bill_vote_references': tuple('bill_id chamber congress session roll_number action_index url date full_action_name observed_at'.split()),
    'committee_assignments': tuple(
        'congress congress_basis session chamber system_code committee_code is_subcommittee '
        'parent_system_code committee_name bioguide_id lis_id name_last name_first party state district '
        'rank position file_date observed_at source_granule source_line edition_date position_basis printed_entry '
        'entry_label'
    .split()),
    'committee_directory_entries': tuple(
        'congress congress_basis source_granule source_line side edition_date kind committee_name subcommittee_name '
        'printed_text note observed_at'
    .split()),
    'committee_meetings': tuple(
        'congress chamber event_id title meeting_type meeting_status meeting_date location_building '
        'location_room committee_system_code committee_count committees_json hearing_jacket '
        'hearing_jacket_count hearing_jackets_json bill_count bill_ids_json witness_count witnesses_json '
        'witness_document_count witness_documents_json meeting_document_count meeting_documents_json '
        'document_urls_json videos_json update_date url detail_read nomination_references_json treaty_references_json'
    .split()),
    'committees': tuple(
        'system_code chamber name committee_type parent_system_code parent_name is_subcommittee '
        'subcommittee_count subcommittees_json update_date url detail_captured is_current detail_type '
        'website_url history_count history_json bill_count report_count communication_count '
        'detail_update_date'
    .split()),
    'congress_bills': tuple(
        'bill_id congress bill_type bill_number title origin_chamber latest_action_date '
        'latest_action_text update_date url schema_version update_date_including_text introduced_date '
        'policy_area subjects_json subject_count sponsor_bioguide_id sponsor_full_name cosponsor_count '
        'latest_action_code latest_action_time latest_action_source_system_code '
        'latest_action_source_system_name action_count committee_count version_count public_law_number '
        'law_type statutes_at_large_cite stage stage_rule stage_matcher stage_action_index '
        'stage_action_date stage_source_text signed_date signed_date_rule signed_date_action_index '
        'signed_date_action_code money_bill_kind money_bill_rule money_bill_reason_codes fiscal_year '
        'appropriations_subcommittee referral_signals short_title related_bills_json related_bill_count '
        'cbo_cost_estimates_outcome url_source cosponsors_outcome'
    .split()),
    'house_communications': tuple(
        'communication_id congress communication_type communication_type_name number chamber session '
        'abstract report_nature legal_authority submitting_agency submitting_official '
        'congressional_record_date is_rulemaking referral_system_code referral_committee_name '
        'referral_date referral_count committees_json matching_requirement_number '
        'matching_requirement_count matching_requirements_json rin rin_occurrences_json rin_rule '
        'rin_matched_text update_date url source_route record_package_id record_granule_id '
        'record_entry_text reconstruction_rule_version detail_read'
    .split()),
    'member_party_affiliations': tuple(
        'bioguide_id input_sha256 term_index affiliation_index party affiliation_start affiliation_end '
        'start_status end_status term_party source_path source_json observed_at'
    .split()),
    'member_terms': tuple(
        'bioguide_id term_index term_type term_start term_end term_state term_party term_district '
        'observed_at party_affiliations_state'
    .split()),
    'member_vote_terms': tuple('vote_id member_key chamber bioguide_id vote_day term_match term_index term_start term_end'.split()),
    'member_votes': tuple(
        'vote_id congress chamber session roll_number member_key bioguide_id lis_id member_name party '
        'state position position_normalized vote_date'
    .split()),
    'members': tuple(
        'bioguide_id bioguide_previous_json lis_id fec_ids_json icpsr_id govtrack_id votesmart_id '
        'opensecrets_id wikidata_id name_first name_last other_names_json term_count first_term_start '
        'last_term_end current_term_type current_term_state current_term_party current_term_district '
        'roster observed_at name_nickname name_middle name_suffix name_official_full'
    .split()),
    'nominations': tuple(
        'nomination_id citation congress number part_number description organization received_date is_civilian '
        'nomination_type_json latest_action_date latest_action_text update_date url committees_json hearings_json detail_read'
    .split()),
    'press_releases': tuple(
        'release_id chamber feed_url channel_title channel_link channel_description channel_language '
        'channel_copyright channel_docs channel_last_build_date channel_ttl channel_skip_days '
        'channel_skip_hours item_index title link guid guid_is_permalink description description_text '
        'description_chars pub_date pub_date_instant author creator categories_json enclosure_url '
        'enclosure_length enclosure_type observed_at bill_id match_rule matched_field matched_text'
    .split()),
    'public_activity_events': tuple('bill_id event_type subject_id occurred_at detected_at event_data_json'.split()),
    'record_issues': tuple(
        'volume issue congress session issue_date chambers chambers_rule section_count section_names '
        'sections_json entire_issue_json package_id package_id_rule article_count articles_url '
        'update_date url detail_read'
    .split()),
    'roll_call_votes': tuple(
        'vote_id congress chamber session roll_number vote_date source_url question result yea nay '
        'present not_voting tallies_json member_vote_count bill_id match_rule match_action_index '
        'match_url conflict_count tally_kind documents_json amendments_json vote_day legis_num '
        'clerk_body_element vote_desc vote_question_text vote_title majority_requirement modify_date '
        'tie_breaker_by_whom tie_breaker_vote party_totals_json'
    .split()),
    'treaties': tuple(
        'treaty_id congress_received congress_considered number suffix old_number '
        'old_number_display_name topic transmitted_date in_force_date resolution_text formal_title '
        'short_title titles_json countries_json index_terms_json related_docs_json parts_json '
        'action_count actions_url package_id package_id_rule update_date url detail_read'
    .split()),
}

RECEIPT_ONLY = frozenset(("bill_family_archives", "bill_family_backfills", "bill_family_backfill_walks"))

# Fields kept only in the receipt. Five a reader needs to judge a row are subject columns instead (owner decision,
# 2026-10-05): the rule that gave congress_bills its stage and the action text it matched (stage_rule,
# stage_source_text), the rule that gave its signed date (signed_date_rule), the system that entered a bill action
# (source_system_name), and whether the roster file or the caller stated an assignment's Congress (congress_basis).
PROCESSING = {
    'amendments': frozenset('url'.split()),
    'bill_actions': frozenset('source_system_code stage_rule stage_matcher'.split()),
    'bill_committees': frozenset('referral_rule referral_signal'.split()),
    'bill_committee_activities': frozenset({'snapshot_update_date'}),
    'cbo_feed_items': frozenset({'feed_url'}),
    'bill_cosponsors': frozenset('input_sha256 sponsorship_date_status sponsorship_withdrawn_date_status source_path source_xml'.split()),
    'bill_subjects': frozenset('carrier enriched_at'.split()),
    'bill_vote_references': frozenset('url observed_at'.split()),
    'committee_assignments': frozenset('file_date observed_at'.split()),
    'committee_directory_entries': frozenset('observed_at'.split()),
    'committee_meetings': frozenset('url detail_read'.split()),
    'committees': frozenset('url detail_captured'.split()),
    'congress_bills': frozenset(
        'url schema_version latest_action_source_system_code latest_action_source_system_name '
        'stage_matcher stage_action_index stage_action_date signed_date_action_index '
        'signed_date_action_code money_bill_rule money_bill_reason_codes '
        'referral_signals cbo_cost_estimates_outcome url_source cosponsors_outcome'
    .split()),
    'house_communications': frozenset(
        'url rin_rule rin_matched_text reconstruction_rule_version detail_read'
    .split()),
    'member_party_affiliations': frozenset('input_sha256 start_status end_status source_path observed_at'.split()),
    'member_terms': frozenset('observed_at party_affiliations_state'.split()),
    # Navigation requires the writer's explicit successful/unresolved outcome.
    'member_vote_terms': frozenset(),
    'member_votes': frozenset(''.split()),
    'members': frozenset('roster observed_at'.split()),
    'nominations': frozenset('url detail_read'.split()),
    'press_releases': frozenset(
        'feed_url channel_title channel_link channel_description channel_language channel_copyright '
        'channel_docs channel_last_build_date channel_ttl channel_skip_days channel_skip_hours '
        'item_index observed_at match_rule matched_field matched_text'
    .split()),
    'public_activity_events': frozenset('detected_at'.split()),
    'record_issues': frozenset('url chambers_rule package_id_rule articles_url detail_read'.split()),
    'roll_call_votes': frozenset('source_url match_rule match_action_index match_url conflict_count'.split()),
    'treaties': frozenset('url package_id_rule actions_url detail_read'.split()),
    'bill_family_archives': frozenset('name link formatted_last_modified_time modified_at size congress bill_type observed_at'.split()),
    'bill_family_backfills': frozenset('congress bill_type number list_update_date_including_text refusal observed_at'.split()),
    'bill_family_backfill_walks': frozenset(
        'congress bill_type declared_count records_walked pages_walked list_completed unwalkable_count '
        'repeated_count backfilled_count observed_at'
    .split()),
}

INTEGERS = {
    'bill_committee_activities': frozenset({'occurrence'}),
    'cbo_feed_items': frozenset({'congress'}),
    'bill_actions': frozenset('action_index recorded_vote_count'.split()),
    'bill_cosponsors': frozenset('cosponsor_index'.split()),
    'bill_subjects': frozenset('subject_count'.split()),
    'bill_vote_references': frozenset('action_index'.split()),
    'committee_meetings': frozenset(
        'committee_count hearing_jacket_count bill_count witness_count witness_document_count '
        'meeting_document_count'
    .split()),
    'committees': frozenset('subcommittee_count history_count bill_count report_count communication_count'.split()),
    'congress_bills': frozenset('subject_count cosponsor_count action_count committee_count version_count related_bill_count'.split()),
    'house_communications': frozenset('referral_count matching_requirement_count'.split()),
    'member_party_affiliations': frozenset('term_index affiliation_index'.split()),
    'member_terms': frozenset('term_index'.split()),
    'member_vote_terms': frozenset('term_index'.split()),
    'members': frozenset('term_count'.split()),
    'press_releases': frozenset('description_chars enclosure_length'.split()),
    'record_issues': frozenset('section_count article_count'.split()),
    'roll_call_votes': frozenset('yea nay present not_voting member_vote_count'.split()),
    'treaties': frozenset('action_count'.split()),
}

BOOLEANS = {
    'bill_actions': frozenset('is_latest_action'.split()),
    'bill_committees': frozenset('is_subcommittee'.split()),
    'bill_cosponsors': frozenset('is_original_raw'.split()),
    'committee_assignments': frozenset('is_subcommittee'.split()),
    'committees': frozenset('is_subcommittee is_current'.split()),
    'house_communications': frozenset('is_rulemaking'.split()),
    'nominations': frozenset('is_civilian'.split()),
    'press_releases': frozenset('guid_is_permalink'.split()),
}

FLATTEN = {
    ("nominations", "nomination_type_json"): {"isCivilian": ("is_civilian", BOOLEAN), "isMilitary": ("is_military", BOOLEAN)},
    ("treaties", "parts_json"): {"count": ("parts_count", INTEGER)},
    ("member_party_affiliations", "source_json"): {"caucus": ("caucus", TEXT)},
    ("public_activity_events", "event_data_json"): {
        "title": ("title", TEXT), "stage": ("stage", TEXT), "sponsor_bioguide_id": ("sponsor_bioguide_id", TEXT),
        "from": ("from_stage", TEXT), "to": ("to_stage", TEXT), "version_code": ("version_code", TEXT), "kind": ("kind", TEXT),
    },
}
# Non-domain members of these source objects stay exclusively in the receipt.
OBJECT_PROCESSING = {
    ("nominations", "nomination_type_json"): frozenset(),
    ("treaties", "parts_json"): frozenset(("urls", "url")),
    ("member_party_affiliations", "source_json"): frozenset(("party", "start", "end")),
    ("public_activity_events", "event_data_json"): frozenset(
        ("rule", "matcher", "kind_rule", "source", "model", "prompt_version", "content_hash", "regenerated")),
}
IDENTITIES = {
    "bill_committee_activities": ("bill_id", "system_code", "activity_name", "activity_date", "occurrence"),
    "cbo_feed_items": ("publication_id",),
    "amendments": ("amendment_id",), "bill_actions": ("bill_id", "action_index"),
    "bill_committees": ("bill_id", "system_code"), "bill_cosponsors": ("bill_id", "cosponsor_index"),
    "bill_subjects": ("bill_id",),
    "bill_vote_references": ("bill_id", "chamber", "congress", "session", "roll_number", "action_index"),
    "committee_assignments": ("congress", "system_code", "bioguide_id"),
    "committee_directory_entries": ("congress", "source_granule", "source_line", "side"),
    "committee_meetings": ("congress", "chamber", "event_id"), "committees": ("system_code",),
    "congress_bills": ("bill_id",), "house_communications": ("communication_id",),
    "member_party_affiliations": ("bioguide_id", "term_index", "affiliation_index"),
    "member_terms": ("bioguide_id", "term_index"), "member_vote_terms": ("vote_id", "member_key"),
    "member_votes": ("vote_id", "member_key"), "members": ("bioguide_id",),
    "nominations": ("congress", "citation"), "press_releases": ("release_id",),
    "public_activity_events": ("bill_id", "event_type", "subject_id", "occurred_at"),
    "record_issues": ("volume", "issue"), "roll_call_votes": ("vote_id",), "treaties": ("treaty_id",),
}
RENAMED = {("bill_cosponsors", "is_original_raw"): "is_original", ("house_communications", "rin_occurrences_json"): "rins"}
JOINED = frozenset((("record_issues", "chambers"), ("record_issues", "section_names")))


def target_name(dataset: str, name: str) -> str:
    if (dataset, name) in RENAMED:
        return RENAMED[dataset, name]
    return name.removesuffix("_json") if (dataset, name) in NATIVE else name


@lru_cache(maxsize=None)
def subject_schema(dataset: str) -> pa.Schema:
    if dataset in RECEIPT_ONLY:
        return pa.schema([])
    fields = {}
    for name in INPUT_COLUMNS[dataset]:
        if name in PROCESSING[dataset]:
            continue
        if (dataset, name) in FLATTEN:
            fields.update({target: dtype for target, dtype in FLATTEN[dataset, name].values()})
        else:
            dtype = NATIVE.get((dataset, name), INTEGER if name in INTEGERS.get(dataset, ())
                               else BOOLEAN if name in BOOLEANS.get(dataset, ()) else TEXT)
            fields[target_name(dataset, name)] = dtype
    return pa.schema(list(fields.items()))


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError(f"Repeated JSON object key {key!r}")
        result[key] = value
    return result


def parsed(value):
    return None if value is None else json.loads(value, object_pairs_hook=_pairs)


def integer(value):
    if value is None or type(value) is int:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        return int(value)
    raise ValueError("Expected a nonnegative integer spelling")


def boolean(value):
    if value is None or type(value) is bool:
        return value
    if value in ("true", "True"):
        return True
    if value in ("false", "False"):
        return False
    raise ValueError("Expected the publisher's declared boolean spelling")


def _check_native(value, dtype, path):
    if value is None:
        return
    if pa.types.is_list(dtype):
        if not isinstance(value, list):
            raise ValueError(f"{path}: expected an ordered list")
        for i, item in enumerate(value):
            _check_native(item, dtype.value_type, f"{path}[{i}]")
    elif pa.types.is_struct(dtype):
        if not isinstance(value, Mapping) or set(value) - set(f.name for f in dtype):
            raise ValueError(f"{path}: unclassified nested fields or non-object")
        for field in dtype:
            _check_native(value.get(field.name), field.type, f"{path}.{field.name}")
    elif pa.types.is_integer(dtype):
        if type(value) is not int or value < 0:
            raise ValueError(f"{path}: expected a nonnegative integer")
    elif pa.types.is_boolean(dtype):
        if type(value) is not bool:
            raise ValueError(f"{path}: expected a boolean")
    elif not isinstance(value, str):
        raise ValueError(f"{path}: expected a string")


@dataclass(frozen=True)
class MappedRecord:
    subject: dict[str, Any] | None
    source_fields: dict[str, Any]


def map_record(dataset: str, row: Mapping[str, Any]) -> MappedRecord:
    """Map source observations without losing unknowns, repeats, nulls or raw inputs.

    Unknown fields or malformed values raise before any output is emitted. The
    receipt integration retains that whole original input as a refused attempt.
    Missing legacy columns remain null; they do not establish an empty list.
    """
    if extra := set(row) - set(INPUT_COLUMNS[dataset]):
        raise ValueError(f"{dataset}: unclassified source fields {sorted(extra)}")
    raw = dict(row)
    if (
        dataset in RECEIPT_ONLY
        or (dataset == "public_activity_events" and row.get("event_type") == "summary_generated")
        or (dataset == "member_vote_terms" and row.get("term_index") is None and row.get("term_match") is not None)
    ):
        return MappedRecord(None, raw)
    subject = dict.fromkeys(subject_schema(dataset).names)
    for name in INPUT_COLUMNS[dataset]:
        if name in PROCESSING[dataset]:
            continue
        value = row.get(name)
        if (dataset, name) in FLATTEN:
            obj = parsed(value)
            if obj is None:
                continue
            if not isinstance(obj, dict) or set(obj) - (set(FLATTEN[dataset, name]) | OBJECT_PROCESSING[dataset, name]):
                raise ValueError(f"{dataset}.{name}: unclassified object properties")
            for key, (target, dtype) in FLATTEN[dataset, name].items():
                if key in obj:
                    v = obj[key]
                    v = integer(v) if pa.types.is_integer(dtype) else boolean(v) if pa.types.is_boolean(dtype) else v
                    if subject.get(target) is not None and subject[target] != v:
                        raise ValueError(f"{dataset}.{name}: conflicting duplicate scalar {target}")
                    subject[target] = v
            continue
        if (dataset, name) in JOINED:
            value = None if value is None else [] if value == "" else value.split("\x1f")
        elif (dataset, name) in NATIVE:
            value = parsed(value)
            if dataset == "roll_call_votes" and name == "tallies_json" and value is not None:
                if not isinstance(value, dict):
                    raise ValueError("roll_call_votes.tallies_json: expected choice-count object")
                value = [{"choice": key, "count": count} for key, count in value.items()]
            elif dataset == "house_communications" and name == "rin_occurrences_json" and value is not None:
                known = {"rin", "field_sha256", "matched_text", "ordinal", "rule", "rule_version", "span_end", "span_start"}
                if not isinstance(value, list) or any(
                    v is not None and (not isinstance(v, dict) or "rin" not in v or set(v) - known) for v in value
                ):
                    raise ValueError("house_communications.rin_occurrences_json: expected RIN occurrences")
                value = [None if v is None else v["rin"] for v in value]
        elif name in INTEGERS.get(dataset, ()):
            value = integer(value)
        elif name in BOOLEANS.get(dataset, ()):
            value = boolean(value)
        subject[target_name(dataset, name)] = value
    schema = subject_schema(dataset)
    for field in schema:
        _check_native(subject[field.name], field.type, f"{dataset}.{field.name}")
    # Arrow checks integer width and materializes missing struct members as null.
    subject = pa.Table.from_pylist([subject], schema=schema).to_pylist()[0]
    return MappedRecord(subject, raw)
