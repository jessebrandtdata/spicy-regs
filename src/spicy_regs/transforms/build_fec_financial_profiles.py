"""Named profiles from an explicit pair of captured FEC summary CSVs.

Each CSV is read as fec-observations reads it and mapped by the fec-query summary mapper, so a witness keeps the
locator and record identity a rebuilt fec-query would give the same bytes. The financial rule owns amount
eligibility. This owner only groups equivalent witnesses and exposes unresolved conflicts; it never sums or
selects a latest row.
"""

from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import UTC, date, datetime
from decimal import Decimal
import hashlib
from itertools import groupby
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.fec_financial_policy import reported_summary_measure
from spicy_regs.transforms.fec_query import FIRST_FEC_CYCLE, CollectionSelection
from spicy_regs.transforms.fec_summaries import CANDIDATE_CSV, COMMITTEE_CSV, SUMMARY_SCHEMA, map_summary

TABLE = "fec_financial_profiles"
OUTPUT = TABLE + ".parquet"
POLICY = "fec-reported-profile-selection/1"
LAYOUTS = {"candidate": CANDIDATE_CSV, "committee": COMMITTEE_CSV}
#: A runaway bound on one summary CSV: the 2026 committee summary was 8.3 MB on 2026-11-10.
MAX_SUMMARY_BYTES = 256 * 1024**2
#: The share of one CSV's rows a refresh may lose against the published profiles before it refuses, so a header-only
#: or truncated download never publishes. FEC dropped 32 of 18,594 rows between 2026-09-12 and 2026-11-10.
MAX_ROW_LOSS = 0.10
MEASURES = {
    "candidate": {
        "receipts": "Total_Receipt",
        "disbursements": "Total_Disbursement",
        "cash_on_hand": "Cash_On_Hand_COP",
        "debts_owed": "Debt_Owed_By_Committee",
    },
    "committee": {
        "receipts": "TTL_RECEIPTS",
        "disbursements": "TTL_DISB",
        "cash_on_hand": "COH_COP",
        "debts_owed": "DEBTS_OWED_BY_CMTE",
    },
}
TEXT_FIELDS = (
    "profile_id scope_id entity_kind entity_id entity_name selection_status selection_reason "
    "coverage_status coverage_reasons_json period_start_raw period_start_status period_end_raw period_end_status "
    "amount_basis captured_at source_collection_id source_sha256 source_table_generation "
    "witnesses_json source_url publisher_profile_url selection_policy"
).split()
SCHEMA = pa.schema(
    [(name, pa.string()) for name in TEXT_FIELDS]
    + [
        (name + suffix, pa.string())
        for name in MEASURES["candidate"]
        for suffix in ("", "_status", "_raw", "_native_field")
    ]
    + [
        ("source_cycle", pa.int32()),
        ("witness_count", pa.int32()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
    ]
)
_PIN = re.compile(r"sha256:[0-9a-f]{64}\Z")


def _json_default(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Unsupported source value: {type(value).__name__}")


def json_bytes(value):
    return json.dumps(
        value, default=_json_default, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def collection_id(kind: str, cycle: int) -> str:
    """The collection identity fec-observations gave one cycle's summary CSV."""
    return f"bulk-{kind}_summary_{cycle}-csv"


def select(captures: list[dict]) -> dict:
    """The selection of one candidate and one committee capture of one cycle, as ``acquire`` records them.

    The scope names the cycle and the capture days, so two captures of one day share a scope and a profile
    identity. Anything but exactly one complete capture of each kind refuses.
    """
    if len(captures) != 2 or {item["entity_kind"] for item in captures} != set(MEASURES):
        raise ValueError("Select exactly one candidate CSV and one committee CSV")
    cycles = {item["cycle"] for item in captures}
    cycle = next(iter(cycles))
    if len(cycles) != 1 or type(cycle) is not int or not FIRST_FEC_CYCLE <= cycle <= 9998 or cycle % 2:
        raise ValueError("Profile selection requires both CSVs of one even FEC election cycle")
    days = set()
    for item in captures:
        if item["collection_id"] != collection_id(item["entity_kind"], cycle):
            raise ValueError("Profile collection does not identify the declared CSV cycle")
        if not _PIN.fullmatch(str(item["response_sha256"])) or type(item["byte_size"]) is not int:
            raise ValueError("Profile source requires an exact digest and size")
        observed = datetime.fromisoformat(item["observed_at"])
        if observed.tzinfo is None or not item["request_url"].startswith("https://"):
            raise ValueError("Profile capture requires an aware time and source URL")
        days.add(observed.astimezone(UTC).date().isoformat())
    return {
        "scope_id": f"fec-summary-csv-{cycle}-captured-" + "-and-".join(sorted(days)),
        "cycle": cycle,
        "captures": sorted(captures, key=lambda item: item["entity_kind"]),
    }


def _summaries(capture, cycle, store, selection_sha256, counts):
    """Map each data row of one captured CSV as fec-query maps it; the header and blank rows carry no record."""
    from spicy_docs.sources.fec.row_profile import positional_row_scope

    from spicy_regs.transforms.build_fec_observations import header_mapped_records

    kind, collection, digest = capture["entity_kind"], capture["collection_id"], capture["response_sha256"]
    original = {"requestUrl": capture["request_url"], "observedAt": capture["observed_at"],
                "responseSha256": digest, "byteSize": capture["byte_size"], "representation": "opaque"}
    item = {
        "collection_id": collection,
        "source_family": "fec_summaries",
        "profile": "positional",
        "scope": positional_row_scope(original, format="delimited", encoding="utf-8", delimiter=",", quoting="csv",
                                      max_decoded_bytes=MAX_SUMMARY_BYTES),
        "field_mapping": {"header_collection_id": collection, "header_row_ordinal": 0, "data_has_header": True},
    }
    # The witnesses are the original's own rows, so its digest is their source pin; no evidence row is kept.
    selection = CollectionSelection(collection, digest, digest, "official-fec", cycle, "snapshot", selection_sha256)
    for row, named in header_mapped_records(item, store):
        if named is None:
            counts[collection + ":unmapped"] += 1
            continue
        counts[collection] += 1
        yield map_summary(row, selection, LAYOUTS[kind])[0]


def _coverage(row, cycle, captured_at):
    reasons = []
    for field in ("period_start", "period_end"):
        if row[field] is None or row[field + "_status"] != "exact":
            reasons.append(field + ":" + row[field + "_status"])
    start, end = row["period_start"], row["period_end"]
    if start and end and start > end:
        reasons.append("period_start_after_end")
    if end and end > datetime.fromisoformat(captured_at).date():
        reasons.append("period_end_after_capture")
    if start and start < date(cycle - 1, 1, 1) or end and end > date(cycle, 12, 31):
        reasons.append("period_outside_declared_cycle")
    return "unqualified" if reasons else "reported", json_bytes(reasons).decode()


def _equivalence(row, kind):
    # Linked candidates explain duplicate committee witnesses, not distinct
    # committee money. All measures and date spellings still must agree.
    fields = (
        "identity_version mapping_version value_mapping_version mapping_status "
        "source_authority source_representation_role correction_operation correction_applicability_status "
        "current_record_status summary_type source_namespace currency period_basis "
        "period_start period_start_raw period_start_status period_end period_end_raw period_end_status measures"
    ).split()
    return json_bytes({**{key: row[key] for key in fields}, "name": row[kind + "_name"]})


def profile(rows, capture, selection):
    """Keep all witnesses; equal financial facts select once, conflicts select none."""
    kind = capture["entity_kind"]
    first = rows[0]
    entity_id = first[kind + "_native_id"]
    if not isinstance(entity_id, str) or not entity_id:
        raise ValueError("Selected summary lacks its entity identity; population cannot be qualified")
    first_facts = _equivalence(first, kind)
    equivalent = all(_equivalence(row, kind) == first_facts for row in rows[1:])
    selected = first if equivalent else None
    result = dict.fromkeys(SCHEMA.names)
    result.update(
        # One entity of one kind and cycle keeps its identity across captures, so a link to it survives a refresh;
        # scope_id names the capture. Generations selected from fec-query also hashed their scope (one per capture).
        profile_id="sha256:"
        + hashlib.sha256(json_bytes([POLICY, kind, entity_id, selection["cycle"]])).hexdigest(),
        scope_id=selection["scope_id"],
        entity_kind=kind,
        entity_id=entity_id,
        entity_name=first[kind + "_name"]
        if all(row[kind + "_name"] == first[kind + "_name"] for row in rows)
        else None,
        source_cycle=selection["cycle"],
        selection_status="selected" if equivalent else "ambiguous",
        selection_reason=("unique_reported_snapshot" if len(rows) == 1 else "equivalent_witnesses")
        if equivalent
        else "conflicting_reported_snapshots",
        amount_basis="source_reported",
        captured_at=capture["observed_at"],
        source_collection_id=capture["collection_id"],
        source_sha256=capture["response_sha256"],
        # Generations selected from fec-query named its generation here; these come from the CSV itself.
        source_table_generation=None,
        witness_count=len(rows),
        witnesses_json=json_bytes(rows).decode(),
        source_url=capture["request_url"],
        publisher_profile_url=f"https://www.fec.gov/data/{kind}/{entity_id}/?cycle={selection['cycle']}",
        selection_policy=POLICY,
    )
    if selected:
        result["coverage_status"], result["coverage_reasons_json"] = _coverage(
            selected, selection["cycle"], capture["observed_at"]
        )
        for field in ("period_start", "period_end"):
            for suffix in ("", "_raw", "_status"):
                result[field + suffix] = selected[field + suffix]
    else:
        result.update(coverage_status="ambiguous", coverage_reasons_json='["conflicting_reported_snapshots"]')
        result["period_start_status"] = result["period_end_status"] = "ambiguous"
    for name, native in MEASURES[kind].items():
        result[name + "_native_field"] = native
        result[name + "_status"] = "ambiguous"
        if selected is None:
            continue
        matches = [measure for measure in selected["measures"] if measure["native_field"] == native]
        decision = reported_summary_measure(selected, native)
        raw = matches[0]["raw_value"] if len(matches) == 1 else None
        result[name + "_raw"] = raw
        if decision.status == "eligible":
            result[name] = format(decision.value, "f")
            result[name + "_status"] = "exact"
        elif len(matches) == 1 and matches[0]["value_status"] in {"source_null", "source_empty"}:
            result[name + "_status"] = matches[0]["value_status"]
        else:
            result[name + "_status"] = "refused" if matches else "not_supplied"
    return result


def _profiles(summaries: Path, selection, outcomes):
    """One profile per entity of the staged summaries, read in one bounded ordered pass."""
    captures = {item["collection_id"]: item for item in selection["captures"]}
    # No corpus-sized Python list or spill. Sorting is bounded by DuckDB's
    # memory refusal, and only one entity's witnesses stay in Python.
    with duckdb.connect(config={"threads": 1, "memory_limit": "512MB", "max_temp_directory_size": "0B"}) as db:
        reader = db.execute(
            """SELECT * FROM read_parquet(?)
               ORDER BY collection_id,
                 CASE WHEN summary_type='candidate-summary-csv/1'
                      THEN candidate_native_id ELSE committee_native_id END, record_id""",
            [str(summaries)],
        ).to_arrow_reader(batch_size=256)
        try:
            rows = (row for batch in reader for row in batch.to_pylist())
            for (collection, _entity), group in groupby(
                rows, key=lambda row: (row["collection_id"], row[captures[row["collection_id"]]["entity_kind"] + "_native_id"])
            ):
                result = profile(list(group), captures[collection], selection)
                outcomes[result["selection_status"]] += 1
                outcomes["witnesses"] += result["witness_count"]
                yield result
        finally:
            reader.close()


def published_rows(table: Path) -> dict[str, int]:
    """The CSV rows behind a published profiles table, by entity kind: the witnesses its profiles record."""
    import pyarrow.parquet as pq

    totals = pq.read_table(table, columns=["entity_kind", "witness_count"]).group_by("entity_kind").aggregate(
        [("witness_count", "sum")])
    return dict(zip(totals["entity_kind"].to_pylist(), totals["witness_count_sum"].to_pylist(), strict=True))


def _refuse_lost_rows(selection, counts, prior_rows, accept_shrink):
    """Refuse a CSV that lost more than ``MAX_ROW_LOSS`` of the rows the published profiles record for its kind."""
    for capture in selection["captures"]:
        kind, held = capture["entity_kind"], counts[capture["collection_id"]]
        prior = prior_rows.get(kind)
        if prior and kind not in accept_shrink and held < prior * (1 - MAX_ROW_LOSS):
            raise ValueError(
                f"The {kind} summary CSV holds {held:,} rows, more than {MAX_ROW_LOSS:.0%} fewer than the {prior:,} "
                f"the published profiles record; a header-only or cut download looks like this. Accept a genuine "
                f"loss by hand with build --accept-shrink {kind}")


def build_profiles(selection, store, destination, *, selection_sha256, prior_rows=None, accept_shrink=()):
    """Map both captured CSVs from ``store`` into a staged summary table, then write the profiles atomically.

    ``selection_sha256`` is the digest of the retained selection, recorded in every witness. ``prior_rows`` are
    the published rows by kind (``published_rows``); a CSV that lost more than ``MAX_ROW_LOSS`` of them refuses
    unless its kind is in ``accept_shrink``. The staged table lives beside ``destination`` only for this call;
    ``destination`` is replaced only after every witness of every mapped row has been written into a profile.
    """
    from spicy_docs.sources.fec.delimited import verified_archive_session

    destination = Path(destination)
    counts, outcomes = Counter(), Counter()
    with TemporaryDirectory(dir=destination.parent, prefix=".fec-summaries-") as scratch:
        staged = Path(scratch) / "summaries.parquet"
        with verified_archive_session():
            rows = (row for capture in selection["captures"]
                    for row in _summaries(capture, selection["cycle"], store, selection_sha256, counts))
            write_rows(rows, staged, SUMMARY_SCHEMA, batch_size=256)
        _refuse_lost_rows(selection, counts, prior_rows or {}, frozenset(accept_shrink))
        mapped = sum(counts[item["collection_id"]] for item in selection["captures"])

        def checked():
            yield from _profiles(staged, selection, outcomes)
            if outcomes.pop("witnesses", 0) != mapped:
                raise ValueError("Profile witnesses differ from the mapped summary rows")

        with closing(checked()) as profiles:
            write_rows(profiles, destination, SCHEMA, batch_size=128)
    return {"source_rows": dict(counts), "profiles": dict(outcomes), "selection_policy": POLICY,
            "published_rows": prior_rows, "accepted_shrink": sorted(accept_shrink)}
