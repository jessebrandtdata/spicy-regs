"""Enrich selected bill-version rows without reopening or extracting their bodies."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from spicy_docs.sources.govinfo.bill_publication import bill_publication_evidence


def _instant(value: str) -> datetime:
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("Publication evidence observation must include a timezone")
    return instant


def enrich_bill_publication(
    rows: Iterable[Mapping[str, Any]], *, metadata_source: Any, max_packages: int,
) -> tuple[list[dict], list[dict]]:
    """Return complete updated source rows and explicit dispositions for a bounded selection.

    Callers select rows before this boundary. Refused, unstable, unchanged and
    older metadata leave the selected row intact. Raw capture retention belongs
    to the source's existing CaptureEvidence transport, including refusals.
    """
    if isinstance(max_packages, bool) or not isinstance(max_packages, int) or max_packages < 0:
        raise ValueError("max_packages must be a nonnegative integer")
    selected = list(rows)
    if len(selected) > max_packages:
        raise ValueError("Bill publication selection exceeds max_packages")
    updated, outcomes = [], []
    for original in selected:
        row = dict(original)
        package_id = row.get("package_id")
        if not package_id or not package_id.startswith("BILLS-") or not row.get("sha256"):
            outcomes.append({"package_id": package_id, "status": "body_identity_unavailable"})
            continue
        captures = metadata_source.acquire_bill_metadata(package_id)
        if len(captures) != 3:
            raise ValueError("Bill metadata refresh requires the complete SUMMARY/MODS/SUMMARY capture set")
        result = bill_publication_evidence(
            package_id, body_sha256=row["sha256"], body_url=row["resolved_url"],
            body_observed_at=row.get("observed_at"), summary_capture=captures[0], mods_capture=captures[1],
            closing_summary_capture=captures[2],
        )
        status = result["status"]
        prior = json.loads(row["publication_evidence_json"]) if row.get("publication_evidence_json") else None
        evidence = result["evidence"]
        if prior and any(prior.get(key) != evidence[key] for key in ("package_id", "body_sha256", "body_url")):
            raise ValueError("Selected bill publication evidence names another body")
        if status == "summary_changed_during_acquisition":
            disposition = status
        elif prior and _instant(evidence["observed_at"]) < _instant(prior["observed_at"]):
            disposition = "older_observation_preserved"
        elif prior and ([item["sha256"] for item in prior["captures"][:2]]
                        == [item["sha256"] for item in evidence["captures"][:2]]):
            disposition = "unchanged_evidence_reused"
        elif prior and _instant(evidence["observed_at"]) == _instant(prior["observed_at"]):
            raise ValueError("Conflicting bill metadata at the same capture observation")
        else:
            row.update(publication_dates_json=json.dumps(result["dates"], sort_keys=True),
                       publication_evidence_status=status,
                       publication_evidence_json=json.dumps(evidence, sort_keys=True))
            updated.append(row)
            disposition = "enriched_body_reused"
        outcomes.append({"package_id": package_id, "status": disposition, "evidence": evidence})
    return updated, outcomes


def refresh_bill_publication(
    path: Path, *, package_ids: tuple[str, ...], metadata_source: Any, evidence: Any = None,
) -> list[dict]:
    """Update an explicit package selection in a candidate, using the shared table merge.

    The caller admits the full candidate rewrite as well as the small metadata
    request scope. Scratch derivatives stay beside the candidate, outside raw.
    The previous candidate remains in place until the replacement completes.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import _duckdb_session, merge_local_prior

    if not package_ids or len(set(package_ids)) != len(package_ids):
        raise ValueError("Bill publication refresh requires distinct explicit package ids")
    selected = pq.read_table(path, filters=[("package_id", "in", package_ids)]).to_pylist()
    if {row["package_id"] for row in selected} != set(package_ids):
        raise ValueError("Bill publication refresh selection contains an unavailable body")
    rows, outcomes = enrich_bill_publication(selected, metadata_source=metadata_source, max_packages=len(package_ids))
    if evidence is not None:
        for outcome in outcomes:
            evidence.event("bill-publication-metadata", **outcome)
    if not rows:
        return outcomes
    contract = TABLE_CONTRACTS["bill_versions"]
    fresh = path.with_name("_bill_publication_new.parquet")
    candidate = path.with_name("_bill_publication_candidate.parquet")
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(name, pa.string()) for name in contract.columns])), fresh)
    connection = _duckdb_session(path.parent)
    try:
        merge_local_prior(connection, columns=contract.columns, identity=contract.identity,
                          order_by=", ".join(contract.identity), prior_file=path, new_file=fresh, out_file=candidate)
        candidate.replace(path)
    finally:
        connection.close()
        fresh.unlink(missing_ok=True)
        candidate.unlink(missing_ok=True)
    return outcomes
