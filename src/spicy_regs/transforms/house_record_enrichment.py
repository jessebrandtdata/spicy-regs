"""Fill API communications' missing Record locators from exact printed entries."""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pyarrow.parquet as pq
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.sources.congress.record_communications import (
    RECORD_COMMUNICATION_RULE_VERSION, executive_communication_granules, parse_granule_body,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyBudget
from spicy_docs.sources.govinfo.bodies import parse_package_id, parse_granule_identity, granule_body_locator, validate_granule_body
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.sources.govinfo.discovery import package_granules_url
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.sources import publication
from spicy_regs.sources.congress_bills import _resolve_api_key
from spicy_regs.sources.retained import RetainedGovInfoBodyAcquirer, RetainedGovInfoDiscoveryReader
from spicy_regs.source_evidence import SourceEvidenceError
from spicy_regs.transforms.table_merge import set_column, write_in_place

FIELDS = ("record_package_id", "record_granule_id", "record_entry_text")
EVENT = "house-record-package"
RULE = "house-record-enrichment-001:" + RECORD_COMMUNICATION_RULE_VERSION
BODY_LIMIT = 2 ** 20


def _calendar_day(value):
    """Validate the entire stated ISO date/time and keep its local calendar day."""
    if not isinstance(value, str):
        raise ValueError("Record date must be a stated ISO date or datetime")
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        return date.fromisoformat(value).isoformat()
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
                    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-5][0-9])?", value):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    raise ValueError("Record date must be a stated ISO date or datetime")


def _digest(value):
    return "sha256:" + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _retain_body(evidence, body, locator):
    with TemporaryDirectory(prefix="house-record-witness-") as folder:
        path = Path(folder) / "record-body.htm"
        path.write_bytes(body)
        evidence.retain_file(path, stage="house-record-body-witness", source_locator=locator)


def _retained_bodies(evidence, checkpoint, fetch):
    """Check the prior journal's cached interpretation against declared raw blobs."""
    if evidence._prior_evidence is None:
        raise ValueError("Record checkpoint has no selected retained evidence")
    origin = checkpoint.get("body_evidence")
    base, pin = (origin["base_url"], origin["pin"]) if origin else evidence._prior_evidence
    prefix = publication.EVIDENCE_PREFIX + "/" + pin["artifactDigest"].removeprefix("sha256:")
    root = publication.family_root(fetch(base + "/" + prefix + "/artifact.json", publication.EVIDENCE_CONTROL_LIMIT),
                                   {**pin, "prefix": prefix})
    members = {}
    for descriptor in root["memberManifests"]:
        raw = fetch(base + "/" + prefix + "/" + descriptor["objectKey"], publication.EVIDENCE_CONTROL_LIMIT)
        if "sha256:" + hashlib.sha256(raw).hexdigest() != descriptor["sha256"]:
            raise ValueError("Record retained member manifest changed")
        members.update((m["objectKey"], m) for m in json.loads(raw)["members"])
    size, entries = 0, []
    for witness in checkpoint["bodies"]:
        identity = parse_granule_identity(checkpoint["package_id"], witness["granule_id"])
        if (witness["package_id"] != checkpoint["package_id"]
                or witness["locator"] != granule_body_locator(identity.package, identity.granule_id, "htm")):
            raise ValueError("Retained Record witness has a different package/granule locator")
        key = "blobs/sha256/" + witness["sha256"].removeprefix("sha256:")
        member = members.get(key, {})
        if (member.get("role") != "source-body" or member.get("sha256") != witness["sha256"]
                or member.get("byteSize") != witness["byte_size"]):
            raise ValueError("Record retained body is not declared with its exact SHA and size")
        # Blobs are content-addressed once under the shared prefix (publish_evidence), not per artifact.
        body = fetch(base + "/" + publication.EVIDENCE_PREFIX + "/" + key, BODY_LIMIT)
        if len(body) != witness["byte_size"] or "sha256:" + hashlib.sha256(body).hexdigest() != witness["sha256"]:
            raise ValueError("Record retained body changed")
        size += len(body)
        _retain_body(evidence, body, witness["locator"])
        capture = CapturedBodyResponse(witness["locator"], witness["locator"], 200, "text/html", witness["observed_at"], body)
        validated = validate_granule_body(body, package=identity.package, granule_id=identity.granule_id,
                                         format="htm", content_type="text/html", final_url=witness["locator"], max_bytes=BODY_LIMIT)
        source = SimpleNamespace(identity=identity, format="htm", body_capture=capture,
                                 body=validated)
        entries.extend(asdict(entry) for entry in parse_granule_body(source))
    if checkpoint.get("rule") != RULE or _digest(entries) != _digest(checkpoint["entries"]):
        raise ValueError("Retained Record interpretation differs from exact body bytes or current rule")
    return size


def enrich_house_record(output: Path, *, download_prior, selected_input, evidence,
                        reader=None, acquirer=None, max_packages=8, max_granules=24,
                        max_bytes=16 * 2 ** 20, fetch_retained=None):
    """Append three empty fields only after every selected package granule is read.

    Publisher-stated Congress comes from exact selected Record issue inputs. The
    API's explicit Record date narrows acquisition; equality on the complete
    Congress/type/number establishes identity. A partial scope never supplies a
    scalar locator, and repeated printed occurrences are ambiguous.
    """
    if min(max_packages, max_granules, max_bytes) < 0:
        raise ValueError("Record acquisition bounds must be nonnegative")
    if evidence is None:
        return
    table = pq.read_table(output)
    rows = table.to_pylist()
    wanted = defaultdict(list)
    for position, row in enumerate(rows):
        if (row.get("source_route") == "congress-gov-detail" and row.get("communication_type") == "ec"
                and row.get("congress") and str(row.get("number") or "").isdecimal()):
            date = row.get("congressional_record_date")
            if not date:
                evidence.event("house-record-result", congress=row["congress"], communication_type="ec",
                               communication_id=row.get("communication_id"), number=row["number"],
                               rule_version=RULE, outcome="unread", reason="API states no Record date")
                continue
            try:
                day = _calendar_day(date)
            except ValueError:
                evidence.event("house-record-result", congress=row["congress"], communication_type="ec",
                               communication_id=row.get("communication_id"), number=row["number"], rule_version=RULE,
                               outcome="refused", congressional_record_date=date,
                               reason="API Record date is not a valid ISO date or datetime")
                continue
            wanted[(str(row["congress"]), day)].append(position)
    if not wanted:
        return
    issue_path = output.parent / "record-enrichment-issues.parquet"
    if selected_input is None or not download_prior("record_issues.parquet", issue_path):
        evidence.event("house-record-selection", outcome="unread", reason="No exact selected Record issues")
        for scope, positions in wanted.items():
            for position in positions:
                row = rows[position]
                evidence.event("house-record-result", communication_id=row.get("communication_id"),
                    congress=row["congress"], communication_type="ec", number=row["number"], rule_version=RULE,
                    congressional_record_date=row["congressional_record_date"], record_calendar_day=scope[1],
                    outcome="unread", complete_scope=False, reason="No exact selected Record issues")
        return
    pin = selected_input("record_issues")
    if (not isinstance(pin, dict) or pin.get("dataset") != "record_issues" or not pin.get("generationId")
            or pin.get("processing", {}).get("byteSize") != issue_path.stat().st_size
            or pin.get("processing", {}).get("sha256") != "sha256:" + hashlib.sha256(issue_path.read_bytes()).hexdigest()):
        raise ValueError("Record issue processing input differs from selected receipts")
    def package_result(**fields):
        evidence.event(EVENT, **{"input": pin, "rule": RULE, **fields})

    issues, invalid_congresses = defaultdict(list), set()
    for issue in pq.read_table(issue_path).to_pylist():
        congress = str(issue.get("congress"))
        if not any(scope[0] == congress for scope in wanted):
            continue
        try:
            scope = (congress, _calendar_day(issue.get("issue_date")))
        except ValueError:
            package_result( package_id=issue.get("package_id"), outcome="refused",
                           reason="Selected Record issue date is not a valid ISO date or datetime", issue=issue, input=pin)
            invalid_congresses.add(congress)
            continue
        if scope in wanted:
            issues[scope].append(issue)
    fetched = granules_used = bytes_used = replayed_bytes = 0
    fetch = fetch_retained or (lambda url, limit: publication._bounded_get(url, allow_missing=False, limit=limit))
    for scope, positions in sorted(wanted.items()):
        candidates, complete = [], bool(issues[scope]) and scope[0] not in invalid_congresses
        for issue in issues[scope]:
            package = issue.get("package_id")
            if package in (None, ""):
                package_result( package_id=package, outcome="unread",
                               reason="Selected Record issue has no package identity", issue=issue, input=pin)
                complete = False
                continue
            try:
                stated = parse_package_id(package)
                if stated.collection != "CREC" or stated.issue_date != scope[1]:
                    raise ValueError("Issue/package identity differs")
            except (ValueError, TypeError):
                package_result( package_id=package, outcome="refused", reason="Issue/package identity differs", issue=issue, input=pin)
                complete = False
                continue
            marker = _digest({"rule": RULE, "issue": issue})
            prior = evidence.inherited_event(EVENT, package_id=package, marker=marker)
            checkpoint = None
            if prior is not None and prior.get("outcome") in ("read", "empty"):
                try:
                    checkpoint = {k: v for k, v in prior.items() if k not in ("event", "observed_at")}
                    if (checkpoint.get("rule") != RULE or checkpoint.get("issue") != issue
                            or checkpoint.get("package_id") != package or checkpoint.get("marker") != marker):
                        raise ValueError("Record checkpoint differs from exact issue context or current rule")
                    needs_replay = any(not all(rows[position].get(field) for field in FIELDS) for position in positions)
                    if needs_replay:
                        if replayed_bytes + sum(w["byte_size"] for w in prior["bodies"]) > 64 * 2 ** 20:
                            raise ValueError("Retained Record replay exceeds the separate bounded replay allowance")
                        replayed_bytes += _retained_bodies(evidence, prior, fetch)
                    elif not checkpoint.get("body_evidence"):
                        base, source_pin = evidence._prior_evidence
                        checkpoint["body_evidence"] = {"base_url":base, "pin":source_pin}
                    checkpoint["acquisition"] = "retained-source-reuse" if needs_replay else "qualified-slots-preserved"
                    checkpoint["retained_input"] = checkpoint["input"]
                    checkpoint["input"] = pin
                    checkpoint["issue"] = issue
                except (ValueError, OSError) as error:
                    package_result( package_id=package, marker=marker, outcome="refused", reason=str(error))
                    complete = False
                    continue
            elif fetched >= max_packages:
                package_result( package_id=package, marker=marker, outcome="unread", reason="Package bound")
                complete = False
                continue
            if checkpoint is None:
                fetched += 1
                if reader is None or acquirer is None:
                    key = _resolve_api_key()
                    if not key:
                        package_result( package_id=package, marker=marker, outcome="unread", reason="No GovInfo credential")
                        complete = False
                        continue
                    reader = reader or RetainedGovInfoDiscoveryReader(evidence=evidence, api_key=key,
                        budget=PagedJsonBudget(48, 2 ** 20, 30., .2))
                    acquirer = acquirer or RetainedGovInfoBodyAcquirer(evidence=evidence, api_key=key,
                        budget=GovInfoBodyBudget(8, BODY_LIMIT, 2 ** 20, 30., .2))
                try:
                    records = [r for p in reader.granules(package_granules_url(package), max_pages=4) for r in p.records]
                    ids = executive_communication_granules(records)
                    if len(ids) + granules_used > max_granules or len(ids) != len(set(ids)):
                        raise ValueError("Record granule bound or repeated listed identity")
                    entries, bodies = [], []
                    for granule in ids:
                        remaining = min(BODY_LIMIT, max_bytes - bytes_used)
                        if remaining <= 0:
                            raise ValueError("Record body byte allowance exhausted")
                        granules_used += 1
                        body = acquirer.acquire_granule(package, granule, prefer=("htm",), max_bytes=remaining)
                        capture = body.body_capture
                        if len(capture.body) > remaining:
                            raise ValueError("Acquired Record body exceeds the selected byte allowance")
                        if body.identity.package.package_id != package or body.identity.granule_id != granule:
                            raise ValueError("Acquired Record identity differs from selected granule")
                        bytes_used += len(capture.body)
                        _retain_body(evidence, capture.body, capture.resolved_url)
                        bodies.append({"package_id":package, "granule_id":granule, "sha256":capture.sha256,
                                       "byte_size":capture.byte_size, "locator":capture.resolved_url,
                                       "observed_at":capture.observed_at})
                        entries.extend(asdict(entry) for entry in parse_granule_body(body))
                    checkpoint = {"package_id":package, "marker":marker, "rule":RULE,
                                  "outcome":"read" if entries else "empty", "entries":entries, "bodies":bodies,
                                  "issue":issue, "input":pin, "acquisition":"new-source-read"}
                except (CredentialRefusedError, SourceEvidenceError):
                    raise
                except Exception as error:
                    evidence.refusal(error, stage="house-record-enrichment")
                    package_result( package_id=package, marker=marker, outcome="failed", error_type=type(error).__name__)
                    complete = False
                    continue
            package_result( **checkpoint)
            candidates.extend((entry, checkpoint) for entry in checkpoint["entries"])
        for position in positions:
            row = rows[position]
            matched = [(entry, checkpoint) for entry, checkpoint in candidates
                       if entry["communication_type"] == "ec" and str(entry["number"]) == str(row["number"])]
            outcome = "unread" if not complete else "empty" if not matched else "ambiguous" if len(matched) != 1 else "matched"
            if outcome == "matched":
                entry, checkpoint = matched[0]
                values = (entry["record_package_id"], entry["record_granule_id"], entry["entry_text"])
                if any(row.get(field) not in (None, "", value) for field, value in zip(FIELDS, values, strict=True)):
                    outcome = "ambiguous"
                else:
                    for field, value in zip(FIELDS, values, strict=True):
                        if not row.get(field):
                            row[field] = value
            evidence.event("house-record-result", congress=row["congress"], communication_type="ec", number=row["number"],
                           communication_id=row.get("communication_id"), rule_version=RULE,
                           congressional_record_date=row["congressional_record_date"], record_calendar_day=scope[1],
                           outcome=outcome, qualified_occurrences=len(matched), complete_scope=complete,
                           conflict_status="conflict" if outcome == "ambiguous" else "no-conflict" if complete else "not-checked",
                           scope_packages=[issue.get("package_id") for issue in issues[scope]],
                           witnesses=[{"entry":entry, "marker":cp["marker"], "input":cp["input"],
                                       "bodies":cp["bodies"]} for entry, cp in matched])
    for field in FIELDS:
        table = set_column(table, field, [row.get(field) for row in rows])
    write_in_place(table, output)
