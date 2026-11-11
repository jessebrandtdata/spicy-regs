"""Narrow publisher targets versus pinned official actions; credit stays literal."""

from collections import defaultdict
from datetime import date, datetime
from hashlib import file_digest
import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

RULE_VERSION = "lcv-2025-roll-call-comparison-v1"
SPONSORSHIP_RULE_VERSION = "common-cause-2022-house-disclose-cutoff-v1"
SPONSORSHIP_SCOPE = "common_cause:2022-archived"
SPONSORSHIP_ITEM = "House:column:15"
SPONSORSHIP_BILL = "117-hr-1334"
TABLE = "scorecard_action_comparisons"
COLUMNS = (
    "scorecard_id", "result_id", "source_snapshot_id", "item_id", "publisher_member_key", "metric_id",
    "reference_id", "vote_id", "bioguide_id", "official_member_key",
    "publisher_target_text", "publisher_action_text", "publisher_credit_text", "publisher_eligibility_text",
    "official_position_text", "official_position_normalized", "comparison_status", "reason",
    "rule_version", "input_pins_json", "capture_id", "source_url", "source_path",
)
KEYS = ("scorecard_id", "item_id", "publisher_member_key", "result_id")
RESULT_COLUMNS = (
    "scorecard_id", "result_id", "snapshot_id", "item_id", "publisher_member_key", "metric_id",
    "participation_id", "action_text", "result_text", "eligibility_text", "capture_id", "source_url", "source_path",
)
METRIC_COLUMNS = ("scorecard_id", "snapshot_id", "metric_id", "item_id", "participation_id", "publisher_position_text")
VOTE_COLUMNS = ("vote_id", "member_key", "bioguide_id", "lis_id", "position", "position_normalized")
METHODOLOGY_COLUMNS = ("scorecard_id", "snapshot_id", "methodology_id", "chamber_text", "methodology_text",
                       "capture_id", "source_url", "source_path")
COSPONSOR_COLUMNS = ("bill_id", "cosponsor_index", "bioguide_id", "sponsorship_date", "sponsorship_withdrawn_date")
STATUSES = frozenset({"same", "different", "not_voting", "present", "unresolved", "not_applicable"})


def selected_native_rows(snapshot, name, paths, columns, filter_column, values):
    """Verify complete native members; convert only the requested Arrow rows to Python.

    Results and votes need literal main fields, not source reconstruction. Their
    selected native schema, digest and complete population remain mandatory.
    """
    from spicy_regs.congress_receipts import policy as congress_policy
    from spicy_regs.local_data import file_signature, file_state_from_stat
    from spicy_regs.scorecards.analysis_inputs import published_input
    from spicy_regs.scorecards.etl import POLICIES
    from spicy_regs.sources import publication

    if name not in {"scorecard_member_item_results", "scorecard_metric_items", "member_votes",
                    "scorecard_methodologies", "bill_cosponsors"}:
        raise ValueError("Unsupported action-comparison native input")
    owner = publication.table_owner(snapshot, name + ".parquet")
    if owner is None or (name not in owner[1].get("etlReceipts", {}).get("datasets", ())
                         and not published_input(owner[1], name)):  # member_votes publishes none (ss-vse3)
        raise ValueError("Action comparison requires selected native inputs")
    members = publication.table_members(snapshot, name + ".parquet")
    if not paths or len(paths) != len(members):
        raise ValueError("Action comparison requires every selected native member")
    policy = POLICIES[name] if name in POLICIES else congress_policy(name)
    rows, population = [], 0
    selected = pa.array(sorted(values), type=pa.string())
    for path, member in zip(paths, members, strict=True):
        staged = file_state_from_stat(path.lstat())
        retained = path.resolve(strict=True)
        before = file_signature(retained)
        with retained.open("rb") as stream:
            digest = "sha256:" + file_digest(stream, "sha256").hexdigest()
        if digest != member.sha256 or retained.stat().st_size != member.byte_size:
            raise ValueError("Action-comparison input differs from its immutable pin")
        with pq.ParquetFile(retained) as source:
            if not source.schema_arrow.equals(policy.subject_schema, check_metadata=False):
                raise ValueError("Action-comparison input differs from its native schema")
            for batch in source.iter_batches(batch_size=2000, columns=list(columns)):
                population += batch.num_rows
                mask = pc.call_function("is_in", [batch.column(filter_column)], pc.SetLookupOptions(selected))
                rows.extend(batch.filter(mask).to_pylist())
        if (file_state_from_stat(path.lstat()) != staged or path.resolve(strict=True) != retained
                or file_signature(retained) != before):
            raise ValueError("Action-comparison input changed during its read")
    if population != owner[1]["tables"][name + ".parquet"]["rows"]:
        raise ValueError("Action-comparison input differs from its complete selected population")
    return rows


def compare_lcv_2025(source, links, results, votes, official_members, input_pins, metric_items):
    """One descriptive outcome per original LCV result, without changing credit."""
    from spicy_regs.scorecards.resolution import _pins

    pins = _pins(input_pins)
    editions = {r["scorecard_id"]: r for r in source["scorecards"]
                if r.get("publisher_id") == "lcv" and r.get("year_text") == "2025"}
    items = {(r["scorecard_id"], r["item_id"]): r for r in source["scorecard_items"]}
    members = defaultdict(list)
    references = defaultdict(list)
    for row in links["scorecard_member_links"]:
        members[row["scorecard_id"], row["publisher_member_key"]].append(row)
    for row in links["scorecard_item_links"]:
        references[row["scorecard_id"], row["item_id"]].append(row)
    official = {r["bioguide_id"]: r for r in official_members}
    lis_people = defaultdict(set)
    for person in official_members:
        if person.get("lis_id"):
            lis_people[person["lis_id"]].add(person["bioguide_id"])
    targets = defaultdict(list)
    for row in metric_items:
        targets[row["scorecard_id"], row["metric_id"], row["item_id"], row["participation_id"]].append(row)
    by_vote = defaultdict(list)
    for row in votes:
        by_vote[row["vote_id"]].append(row)
    output, seen = [], set()
    for result in results:
        scope = result["scorecard_id"]
        if scope not in editions:
            raise ValueError("Comparison selection includes a non-LCV-2025 result")
        identity = tuple(result[k] for k in KEYS)
        if not all(identity) or identity in seen:
            raise ValueError("Comparison results require distinct source identities")
        seen.add(identity)
        item = items.get((scope, result["item_id"]))
        edition = editions[scope]
        if (not edition.get("snapshot_id") or result["snapshot_id"] != edition["snapshot_id"]
                or item is None or item["snapshot_id"] != edition["snapshot_id"]):
            raise ValueError("Comparison source facts differ from the selected snapshot")
        row = dict.fromkeys(COLUMNS)
        row.update({key: result[key] for key in
                    ("scorecard_id", "result_id", "item_id", "publisher_member_key", "metric_id",
                     "capture_id", "source_url", "source_path")})
        row.update(source_snapshot_id=result["snapshot_id"], publisher_target_text=item.get("publisher_position_text"),
                   publisher_action_text=result["action_text"], publisher_credit_text=result["result_text"],
                   publisher_eligibility_text=result["eligibility_text"], rule_version=RULE_VERSION,
                   input_pins_json=pins, comparison_status="unresolved")
        output.append(row)
        # Non-floor actions need historical sponsorship/cutoff evidence, deferred here.
        if item.get("item_kind_text") != "Scorecard Vote":
            row.update(comparison_status="not_applicable", reason="outside_lcv_floor_vote_method")
            continue
        target = {"Yes": "yea", "No": "nay"}.get(item.get("publisher_position_text"))
        if target is None:
            row["reason"] = "unsupported_publisher_target"
            continue
        if result["metric_id"]:
            contexts = targets[scope, result["metric_id"], result["item_id"], result["participation_id"]]
            if (len(contexts) != 1 or contexts[0]["snapshot_id"] != result["snapshot_id"]
                    or contexts[0]["publisher_position_text"] != item["publisher_position_text"]):
                row["reason"] = "missing_or_conflicting_metric_target"
                continue
        selected_members = members[scope, result["publisher_member_key"]]
        selected_refs = references[scope, result["item_id"]]
        if len(selected_members) != 1 or len(selected_refs) != 1:
            row["reason"] = "missing_or_repeated_exact_links"
            continue
        member, reference = selected_members[0], selected_refs[0]
        if any(r.get("source_snapshot_id") != result["snapshot_id"] for r in (member, reference)):
            row["reason"] = "link_snapshot_mismatch"
            continue
        if (member.get("resolution_status") != "resolved" or not member.get("bioguide_id")
                or reference.get("resolution_status") != "resolved" or not reference.get("vote_id")):
            row["reason"] = "unresolved_member_or_roll_call"
            continue
        row.update(reference_id=reference["reference_id"], vote_id=reference["vote_id"],
                   bioguide_id=member["bioguide_id"])
        person = official.get(member["bioguide_id"])
        if person is None:
            row["reason"] = "member_absent_from_pinned_crosswalk"
            continue
        lis = person.get("lis_id")
        candidates = [v for v in by_vote[reference["vote_id"]]
                      if (v.get("bioguide_id") == member["bioguide_id"]
                          or (not v.get("bioguide_id") and lis and lis_people[lis] == {member["bioguide_id"]}
                              and v.get("lis_id") == lis))]
        if len(candidates) != 1:
            row["reason"] = "missing_or_ambiguous_official_vote"
            continue
        vote = candidates[0]
        if lis and vote.get("lis_id") and vote["lis_id"] != lis:
            row["reason"] = "conflicting_official_member_ids"
            continue
        row.update(official_member_key=vote["member_key"], official_position_text=vote["position"],
                   official_position_normalized=vote["position_normalized"])
        position = vote["position_normalized"]
        if position in {"yea", "nay"}:
            row.update(comparison_status="same" if position == target else "different", reason="exact_roll_call_position")
        elif position in {"not_voting", "present"}:
            row.update(comparison_status=position, reason="explicit_official_position")
        else:
            row["reason"] = "unsupported_official_position"
    return output


def sponsorship_method(source, methodologies):
    """Bind this one qualified item to the retained publisher's explicit cutoff."""
    editions = [r for r in source["scorecards"] if r["scorecard_id"] == SPONSORSHIP_SCOPE]
    if not editions:
        return None
    if len(editions) != 1 or any(editions[0].get(k) != v for k, v in
                                {"publisher_id": "common_cause", "year_text": "2022", "congress_text": "117"}.items()):
        raise ValueError("Historical sponsorship edition differs from the qualified scope")
    edition = editions[0]
    items = [r for r in source["scorecard_items"]
             if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM]
    expected = {"item_kind_text": "BILLS TO COSPONSOR", "publisher_position_text": "BILLS TO COSPONSOR",
                "congress_text": "117", "chamber_text": "House", "bill_citation_text": "HR 1334",
                "title": "HR 1334, DISCLOSE Act", "snapshot_id": edition.get("snapshot_id")}
    if len(items) != 1 or not edition.get("snapshot_id") or any(items[0].get(k) != v for k, v in expected.items()):
        raise ValueError("Historical sponsorship item differs from the qualified scope")
    selected = [r for r in methodologies if r["scorecard_id"] == SPONSORSHIP_SCOPE
                and r["methodology_id"] == "House:methodology"]
    if (len(selected) != 1 or selected[0].get("snapshot_id") != edition["snapshot_id"]
            or selected[0].get("chamber_text") != "House"
            or not all(selected[0].get(k) for k in ("capture_id", "source_url", "source_path"))):
        raise ValueError("Historical sponsorship requires its pinned House methodology")
    methodology = selected[0]
    try:
        text = json.loads(methodology["methodology_text"])
        literal = text["source_cutoff_date_text"]
        statement = " ".join(text["source_methodology_text"].split())
        cutoff = datetime.strptime(literal, "%B %d, %Y").date()
    except (TypeError, KeyError, ValueError, AttributeError) as exc:
        raise ValueError("Historical sponsorship requires its explicit source cutoff") from exc
    if (literal != "September 6, 2022" or
            "All cosponsors listed in this Democracy Scorecard were taken from the Congress.gov website as of "
            + literal + "." not in statement):
        raise ValueError("Historical sponsorship source cutoff changed or is unsupported")
    return {"rule_version": SPONSORSHIP_RULE_VERSION, "scorecard_id": SPONSORSHIP_SCOPE,
            "item_id": SPONSORSHIP_ITEM, "bill_id": SPONSORSHIP_BILL,
            "cutoff_date": cutoff.isoformat(), "source_cutoff_date_text": literal,
            "methodology": {k: methodology[k] for k in METHODOLOGY_COLUMNS},
            "reference_ids": ["item:direct", "reference:header-citation-0"]}


def compare_historical_sponsorship(source, links, results, cosponsors, input_pins, method):
    """One selected bill; dates on the cutoff day and missing rows stay unresolved.

    The existing pins identify the exact methodology and official date rows.
    All reference occurrences remain in item links; item:direct is the primary
    comparison reference only after every occurrence resolves to the same bill.
    """
    from spicy_regs.scorecards.resolution import _pins

    if method is None:
        if results:
            raise ValueError("Historical sponsorship results lack a qualified method")
        return []
    pins = _pins(input_pins)
    cutoff = date.fromisoformat(method["cutoff_date"])
    item = next(r for r in source["scorecard_items"]
                if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM)
    references = [r for r in links["scorecard_item_links"]
                  if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM]
    members, official = defaultdict(list), defaultdict(list)
    for row in links["scorecard_member_links"]:
        if row["scorecard_id"] == SPONSORSHIP_SCOPE:
            members[row["publisher_member_key"]].append(row)
    for row in cosponsors:
        if row["bill_id"] != SPONSORSHIP_BILL:
            raise ValueError("Historical sponsorship includes an unselected official bill")
        official[row["bioguide_id"]].append(row)
    output, seen = [], set()
    for result in results:
        identity = tuple(result[k] for k in KEYS)
        if (result["scorecard_id"] != SPONSORSHIP_SCOPE or result["item_id"] != SPONSORSHIP_ITEM
                or not all(identity) or identity in seen or result["snapshot_id"] != item["snapshot_id"]):
            raise ValueError("Historical sponsorship result differs from the selected source identity")
        seen.add(identity)
        row = dict.fromkeys(COLUMNS)
        row.update({k: result[k] for k in ("scorecard_id", "result_id", "item_id", "publisher_member_key", "metric_id",
                                         "capture_id", "source_url", "source_path")})
        row.update(source_snapshot_id=result["snapshot_id"], publisher_target_text=item["publisher_position_text"],
                   publisher_action_text=result["action_text"], publisher_credit_text=result["result_text"],
                   publisher_eligibility_text=result["eligibility_text"], rule_version=SPONSORSHIP_RULE_VERSION,
                   input_pins_json=pins, comparison_status="unresolved")
        output.append(row)
        if (sorted(r["reference_id"] for r in references) != method["reference_ids"]
                or any(r.get("resolution_status") != "resolved" or r.get("bill_id") != SPONSORSHIP_BILL
                       or r.get("vote_id") or r.get("amendment_id")
                       or r.get("source_snapshot_id") != result["snapshot_id"] for r in references)):
            row["reason"] = "missing_or_conflicting_exact_bill_references"
            continue
        row["reference_id"] = "item:direct"
        candidates = members[result["publisher_member_key"]]
        if (len(candidates) != 1 or candidates[0].get("resolution_status") != "resolved"
                or not candidates[0].get("bioguide_id")
                or candidates[0].get("source_snapshot_id") != result["snapshot_id"]):
            row["reason"] = "missing_or_ambiguous_exact_member"
            continue
        row["bioguide_id"] = candidates[0]["bioguide_id"]
        records = official[row["bioguide_id"]]
        if len(records) != 1:
            row["reason"] = "missing_or_ambiguous_official_cosponsorship"
            continue
        record = records[0]
        row["official_member_key"] = f"{SPONSORSHIP_BILL}/cosponsor_index:{record['cosponsor_index']}"
        try:
            start = date.fromisoformat(record["sponsorship_date"])
            end_text = record["sponsorship_withdrawn_date"]
            end = date.fromisoformat(end_text) if end_text is not None else None
            if (start.isoformat() != record["sponsorship_date"] or
                    (end is not None and (end.isoformat() != end_text or end < start))):
                raise ValueError("Invalid official date interval")
        except (TypeError, ValueError):
            row["reason"] = "missing_or_invalid_official_sponsorship_dates"
            continue
        if start == cutoff or end == cutoff:
            row["reason"] = "same_day_cutoff_time_unknown"
            continue
        active = start < cutoff and (end is None or end > cutoff)
        row.update(official_position_normalized="cosponsor_at_cutoff" if active else "not_cosponsor_at_cutoff",
                   comparison_status="same" if active else "different",
                   reason="dated_official_cosponsorship_at_source_cutoff")
    return output


def build_comparisons(snapshot, paths, source, links, official_members, input_pins):
    scopes = {r["scorecard_id"] for r in source["scorecards"]
              if r.get("publisher_id") == "lcv" and r.get("year_text") == "2025"}
    selected_scopes = scopes | {r["scorecard_id"] for r in source["scorecards"] if r["scorecard_id"] == SPONSORSHIP_SCOPE}
    results = selected_native_rows(snapshot, "scorecard_member_item_results", paths["scorecard_member_item_results"],
                                   RESULT_COLUMNS, "scorecard_id", selected_scopes)
    vote_ids = {r["vote_id"] for r in links["scorecard_item_links"]
                if r["scorecard_id"] in scopes and r.get("resolution_status") == "resolved" and r.get("vote_id")}
    votes = selected_native_rows(snapshot, "member_votes", paths["member_votes"], VOTE_COLUMNS, "vote_id", vote_ids)
    metrics = selected_native_rows(snapshot, "scorecard_metric_items", paths["scorecard_metric_items"],
                                  METRIC_COLUMNS, "scorecard_id", scopes)
    methodologies = selected_native_rows(snapshot, "scorecard_methodologies", paths["scorecard_methodologies"],
                                         METHODOLOGY_COLUMNS, "scorecard_id", {SPONSORSHIP_SCOPE} & selected_scopes)
    method = sponsorship_method(source, methodologies)
    cosponsors = selected_native_rows(snapshot, "bill_cosponsors", paths["bill_cosponsors"],
                                     COSPONSOR_COLUMNS, "bill_id", {SPONSORSHIP_BILL} if method else set())
    rows = compare_lcv_2025(source, links, [r for r in results if r["scorecard_id"] in scopes],
                            votes, official_members, input_pins, metrics)
    rows.extend(compare_historical_sponsorship(source, links,
                [r for r in results if r["scorecard_id"] == SPONSORSHIP_SCOPE and r["item_id"] == SPONSORSHIP_ITEM],
                cosponsors, input_pins, method))
    return rows, method
