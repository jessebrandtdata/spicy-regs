"""Rebuild source identities from retained provenance, without reading payloads.

The catalog associates a file with a dataset; it does not claim that the file
completes that dataset. Periods describe an explicit request, native Congress,
edition, or declared coverage. Dates in local run directories never imply data
coverage. Original receipts and locations remain the authority for each mapping.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

REGISTRY = Path(__file__).with_name("config") / "registry.v1.json"
_DATE = r"\d{4}-\d{2}-\d{2}"
_PACKAGE = re.compile(
    r"(?<![A-Za-z0-9])(?=(?:GAOREPORTS-[A-Za-z0-9]|[A-Z]+-\d))"
    r"(?P<id>(?P<kind>BILLSTATUS|BILLSUM|BILLS|CHRG|CRPT|CPRT|PLAW|CDOC|TDOC|CREC|FR|CFR|GAOREPORTS)"
    r"-[A-Za-z0-9][A-Za-z0-9_.-]*)", re.I,
)
_PACKAGE_FAMILIES = {
    "BILLSTATUS": "bill-family", "BILLSUM": "bill-family", "BILLS": "bill-family", "CHRG": "committee-reports",
    "CRPT": "committee-reports", "CPRT": "print-citations", "PLAW": "laws", "CDOC": "committee-reports",
    "TDOC": "treaties", "CREC": "record-issues", "FR": "federal-register", "CFR": "cfr-sections",
    "GAOREPORTS": "gao-reports",
}
_CONGRESS_KINDS = {"BILLSTATUS", "BILLSUM", "BILLS", "CHRG", "CRPT", "CPRT", "PLAW", "CDOC", "TDOC"}
_NAMED_PERIODS = {"congress", "meeting-date", "document-date", "schedule-week"}

# These are explicit acquisition-directory identities, not fuzzy title matching.
# Keep dates out: a capture campaign's date is not the period covered by its data.
_PATH_FAMILIES = (
    (r"/full-comments/(?:acf|source)-campaign/originals/", "comments-mirror"),
    (r"/regs-documents-[A-Z]+(?:/|$)", "documents"),
    (r"/committee_meetings/(?:detail|list)/", "committee-meetings"),
    (r"/(?:presidential-bodies(?:-recovered)?|rule-bodies-pre2000)-", "federal-register"),
    (r"/(?:scorecards?[-/]|.*?/scorecards-expansion-)", "scorecards"),
    (r"/(?:fec[-/]|.*?/fec-research)", "fec-source-catalog"),
    (r"/amendment-details-", "amendments"),
    (r"/lobbying-initial-load-", "lobbying-filings"),
    (r"/gao-(?:decision|listing|product|requester|cra|month|recommend)", "gao-reports"),
    (r"/(?:bill-family|bill-text|vote-links)-", "bill-family"),
    (r"/(?:report-family/|reports-requalification-|report-bill-linkage-)", "committee-reports"),
    (r"/congress-legislators-", "members"),
    (r"/usaspending-", "usaspending-recipients"),
    (r"/executive-orders(?:-publication-|/publication-)", "executive-orders"),
    (r"/fr-audit-|/inv-frvol/", "federal-register"),
    (r"/inspectors-general/", "inspectors-general-archive"),
)
_PATH_RULES = tuple((re.compile(pattern, re.I), family) for pattern, family in _PATH_FAMILIES)


def _text(value: Any) -> str | None:
    return str(value) if isinstance(value, (str, int)) and not isinstance(value, bool) and str(value) else None


def _url(value: Any) -> str | None:
    """Keep identifying query fields, but never emit access credentials."""
    if not isinstance(value, str) or not value.startswith(("https://", "http://")):
        return None
    from urllib.parse import parse_qsl, urlencode

    try:
        parsed = urlsplit(value)
        query = [(key, item) for key, item in parse_qsl(parsed.query, keep_blank_values=True)
                 if not re.search(r"api.?key|token|signature|credential|password|secret", key, re.I)]
        host = parsed.hostname or ""
        if parsed.port:
            host += f":{parsed.port}"
        return urlunsplit((parsed.scheme, host, parsed.path, urlencode(query), ""))
    except ValueError:
        return None


def _evidence(field: str, value: Any, rule: str, target: str, **extra) -> dict:
    """Locate a claim relative to the enclosing association's pinned receipt input."""
    return {
        "origin": "receipt",
        "field": field, "locator": "/" + field.removeprefix("record.").replace(".", "/"),
        "value": value, "rule": rule, "target": target, **extra,
    }


def _timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or not re.fullmatch(
        rf"{_DATE}T\d{{2}}:\d{{2}}:\d{{2}}(?:\.\d+)?(?:Z|[+-]\d{{2}}:\d{{2}})", value,
    ):
        return None
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value


def _date(value: str) -> bool:
    if not re.fullmatch(_DATE, value):
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _receipt_fields(record: dict) -> dict:
    """Normalize only named receipt facts, independently of source interpretation.

    Captured/observed timestamps mean acquisition, never coverage. HTTP response
    completeness, source-file completeness and source validation stay separate.
    Unknown and conflicting values remain visible rather than selecting the first.
    """
    aliases = {
        "native_id": ("native_id", "source_record_key"),
        "source_version": ("source_version",),
        "acquired_at": ("acquired_at", "observed_at", "captured_at"),
        "http_status": ("http_status", "status_code", "source_http_status", "status"),
        "response_complete": ("response_complete", "capture_complete", "complete_http_response"),
        "source_file_complete": ("source_file_complete",),
        "source_validation": ("source_validation",),
        "transport": ("transport",),
    }
    context = record.get("receipt_context")
    context = context if isinstance(context, dict) else {}
    claims: dict[str, list] = {key: [] for key in aliases}
    evidence = []

    def claim(target: str, field: str, raw: Any, value: Any) -> None:
        evidence.append(_evidence(field, raw, "receipt-field", target, valid=value is not None))
        if value is not None and value not in claims[target]:
            claims[target].append(value)

    for owner, label in ((record, "record"), (context, "receipt_context")):
        for target, keys in aliases.items():
            for key in keys:
                raw = owner.get(key)
                if raw is None or (key == "status" and not isinstance(raw, int)):
                    continue
                if target == "acquired_at":
                    value = _timestamp(raw)
                elif target == "http_status":
                    value = raw if isinstance(raw, int) and not isinstance(raw, bool) and 100 <= raw <= 599 else None
                elif target in {"response_complete", "source_file_complete"}:
                    value = raw if isinstance(raw, bool) else None
                elif target == "source_validation":
                    value = raw if isinstance(raw, str) and raw in {"accepted", "refused"} else None
                else:
                    value = _text(raw) if target in {"native_id", "source_version"} else raw
                    value = value if isinstance(value, str) and value.strip() else None
                claim(target, f"{label}.{key}", raw, value)
        # Refusal is affirmative evidence; refused:false does not establish validation.
        for key in ("status", "outcome", "original_receipt_status", "refused"):
            if owner.get(key) == "refused" or (key == "refused" and owner.get(key) is True):
                claim("source_validation", f"{label}.{key}", owner[key], "refused")
        if owner.get("partial") is True:
            claim("source_file_complete", f"{label}.partial", True, False)
    normalized = {}
    conflicts = {}
    for target, values in claims.items():
        # Different timezone spellings of the same instant are not a conflict.
        distinct = {datetime.fromisoformat(v.replace("Z", "+00:00")) for v in values} if target == "acquired_at" else set(values)
        normalized[target] = values[0] if len(distinct) == 1 else None
        if len(distinct) > 1:
            conflicts[target] = values
    return {**normalized, "field_conflicts": conflicts, "evidence": evidence}


class SourceMapper:
    """Reusable compiled registry lookup for a streaming catalog rebuild."""

    def __init__(self, registry: dict | None = None):
        if registry is None:
            registry = json.loads(REGISTRY.read_text())
        self.names: dict[str, set[str]] = {}
        self.prefixes: list[tuple[str, str]] = []
        self.families: dict[str, str] = {}
        # (family, sha256) -> dataset: exact originals of one family that the registry assigns to a narrower
        # dataset than the family a receipt already declares (receipts are immutable; ss-wfop Q4).
        self.objects: dict[tuple[str, str], str] = {}
        for binding in registry.get("object_datasets", []):
            for digest in binding["sha256"]:
                key = (binding["family"], digest)
                if key in self.objects:
                    raise ValueError(f"Ambiguous object dataset binding: {digest}")
                self.objects[key] = binding["dataset_id"]
        for collection in registry.get("collections", []):
            identity = collection["id"]
            for family in collection.get("families", []):
                self.families[family] = identity
            for name in collection.get("source_names", []):
                self.names.setdefault(name, set()).add(identity)
                if name.startswith(("http://", "https://")):
                    self.prefixes.append((name.rstrip("/"), identity))
        self.prefixes.sort(key=lambda item: (-len(item[0]), item))

    def family_id(self, family: str) -> str:
        if family == "inspectors-general-archive":
            return "declared:inspectors-general-archive"
        if family == "administration-policy":
            return "declared:administration-policy"
        return self.families.get(family, f"family:{family}")

    def map_source(self, record: dict) -> dict:
        """Return a conservative mapping; nulls mean the receipt did not establish it.

        New writers can supply dataset_id, native_id, covered_period, source_version,
        and acquired_at in receipt_context. Legacy family and URL bindings remain
        readable. A declared family outranks a general publisher route because the
        same original can serve multiple application families.
        """
        context = record.get("receipt_context")
        context = context if isinstance(context, dict) else {}
        facts = _receipt_fields(record)
        evidence: list[dict] = facts.pop("evidence")
        candidates: dict[int, set[str]] = {}
        periods: list[dict] = []
        url_claims: dict[str, list[str]] = {"native_id": [], "source_version": []}
        role = record.get("role") or context.get("original_role") or context.get("declared_role") or "original-response"
        source_name = str(context.get("member") or record.get("original_location") or "")
        if re.search(r"\.receipt\.json(?:\.gz)?$", source_name):
            role = "acquisition-receipt"
        original_location = record.get("original_location")
        if isinstance(original_location, str) and re.search(
            r"/source-evidence/[0-9a-f]{64}/journal\.jsonl$", original_location,
        ):
            # SourceEvidence.seal assigns this artifact member the journal role.
            # A generic journal.jsonl elsewhere does not establish that identity.
            role = "acquisition-receipt"
            evidence.append(_evidence("original_location", original_location,
                                      "source-evidence-acquisition-journal", "role", role=role))

        def bind(identity: str, field: str, value: str, rule: str, priority: int) -> None:
            candidates.setdefault(priority, set()).add(identity)
            evidence.append(_evidence(field, value, rule, "dataset_id", dataset_id=identity))

        def period(kind: str, label: str, basis: str, field: str | None = None) -> None:
            item = {"kind": kind, "label": label, "basis": basis}
            if item not in periods:
                periods.append(item)
                evidence.append(_evidence(field or basis, {"kind": kind, "label": label},
                                          "receipt-period", "periods"))

        def native(target: str, value: str, field: str) -> None:
            if value not in url_claims[target]:
                url_claims[target].append(value)
            evidence.append(_evidence(field, value, "source-url-identity", target))

        for owner, label in ((record, "record"), (context, "receipt_context")):
            if identity := _text(owner.get("dataset_id")):
                bind(identity, f"{label}.dataset_id", identity, "explicit-dataset", 0)
            if family := _text(owner.get("family")):
                bind(self.family_id(family), f"{label}.family", family, "explicit-family", 1)
                digest = str(record.get("sha256", "")).removeprefix("sha256:")
                if identity := self.objects.get((family, digest)):
                    bind(identity, f"{label}.family", family, "registry-family-object", 0)
            for key in ("source_name", "source_system", "collection"):
                if name := _text(owner.get(key)):
                    for identity in sorted(self.names.get(name, set())):
                        bind(identity, f"{label}.{key}", name, "registry-source-name", 1)
            for key in ("covered_period", "period", "requested_period"):
                declared = owner.get(key)
                if declared is not None:
                    if isinstance(declared, bool) or not isinstance(declared, (str, int, list, dict)) or not declared:
                        evidence.append(_evidence(f"{label}.{key}", declared,
                                                  "receipt-period", "periods", valid=False))
                        continue
                    # Structured receipts already name the period kind and label.
                    # Keep their searchable value, without promoting a request or
                    # declared coverage into an observed native period.
                    structured = declared if isinstance(declared, list) else [declared]
                    named = isinstance(declared, dict) and bool(declared.keys() & _NAMED_PERIODS) and (
                        declared.keys() <= _NAMED_PERIODS | {"basis", "title"}
                    ) and all(
                        (value := _text(declared[name])) is not None and (
                            value.isdecimal() and int(value) > 0 if name == "congress" else _date(value)
                        ) for name in declared.keys() & _NAMED_PERIODS
                    )
                    if named:
                        # Named native fields are another retained receipt shape.
                        # Title/basis describe its evidence, never extra periods.
                        evidence.append(_evidence(f"{label}.{key}", declared,
                                                  "receipt-period-declaration", "period_declaration"))
                        structured = [
                            {"kind": name, "label": value,
                             "basis": _text(declared.get("basis")) or _text(owner.get("period_basis"))}
                            for name, value in declared.items() if name in _NAMED_PERIODS
                        ]
                    if structured and all(
                        isinstance(item, dict) and isinstance(item.get("kind"), str) and item["kind"]
                        and _text(item.get("label")) for item in structured
                    ):
                        for i, item in enumerate(structured):
                            kind = item["kind"]
                            assert isinstance(kind, str)
                            if key == "requested_period" and not kind.startswith(("requested-", "declared-requested-")):
                                kind = "requested-" + kind
                            elif key == "covered_period" and not kind.startswith("declared-covered-"):
                                kind = "declared-covered-" + kind
                            basis = f"{label}.{key}; declared scope; completeness unknown"
                            if detail := _text(item.get("basis")):
                                basis += "; " + detail
                            field = f"{label}.{key}" + (f".{item['kind']}" if named else
                                                       f".{i}" if isinstance(declared, list) else "")
                            period(kind, str(item["label"]), basis, field)
                        continue
                    # Unstructured values remain explicit declarations.
                    kind = {"covered_period": "declared-covered-period", "period": "declared-period",
                            "requested_period": "declared-requested-period"}[key]
                    period(kind,
                           json.dumps(declared, sort_keys=True) if not isinstance(declared, str) else declared,
                           f"{label}.{key}")

        locations: list[tuple[str, str]] = []
        for field, value in (
            ("source_url", record.get("source_url")),
            ("receipt_source_url", record.get("receipt_source_url")),
            ("requested_url", record.get("requested_url")),
            ("resolved_url", record.get("resolved_url")),
            ("receipt_context.source_url", context.get("source_url")),
            ("receipt_context.requested_url", context.get("requested_url")),
            ("receipt_context.resolved_url", context.get("resolved_url")),
            ("receipt_context.url", context.get("url")),
            ("original_location", record.get("original_location")),
            ("receipt_context.member", context.get("member")),
            ("receipt_context.collection", "/" + context["collection"] + "/" if isinstance(context.get("collection"), str) else None),
        ):
            if isinstance(value, str) and value:
                # Invalid URLs must not fall back to an unsanitized credential URL.
                location = _url(value) if value.startswith(("https://", "http://")) else value
                if location:
                    locations.append((field, location))

        for field, location in locations:
            parsed = urlsplit(location) if location.startswith(("https://", "http://")) else None
            native_path = unquote(parsed.path) if parsed else location
            host = (parsed.hostname or "").lower() if parsed else ""
            query = parse_qs(parsed.query) if parsed else {}
            match = _PACKAGE.search(native_path)
            if match:
                kind = match["kind"].upper()
                identity = match["id"].rstrip(".")
                identity = re.sub(r"(?:\.(?:xml|json|pdf|zip|html?|txt|gz))+$", "", identity, flags=re.I)
                identity = re.sub(r"\.(?:premis|mods)$", "", identity, flags=re.I)
                # Retained issue metadata uses both .mods.xml and _mods.xml.
                # This suffix labels the file; it is not the publisher package.
                identity = re.sub(rf"^(FR-{_DATE})_mods$", r"\1", identity, flags=re.I)
                bind(self.family_id(_PACKAGE_FAMILIES[kind]), field, location, "publisher-package-id", 2)
                native("native_id", identity, field)
                if kind in _CONGRESS_KINDS:
                    congress = re.match(r"[A-Z]+-(\d{1,3})(?!\d)", identity, re.I)
                    if congress:
                        period("congress", congress[1], f"native package {identity}", field)
                elif kind == "CFR":
                    edition = re.match(r"CFR-(\d{4})-", identity, re.I)
                    if edition:
                        period("requested-edition", edition[1], f"native package {identity}; body edition not inspected", field)
                elif kind in {"FR", "CREC"}:
                    issue = re.match(rf"(?:FR|CREC)-({_DATE})", identity, re.I)
                    if issue and _date(issue[1]):
                        period("publication-date", issue[1], f"native package {identity}", field)

            if host:
                url_family = self._url_family(host, native_path)
                if url_family:
                    bind(self.family_id(url_family), field, location, "publisher-source-route", 3)
                for prefix, identity in self.prefixes:
                    if location == prefix or location.startswith(prefix + "/") or location.startswith(prefix + "?"):
                        bind(identity, field, location, "registry-url-prefix", 3)
                        break
                if host in {"www.ecfr.gov", "ecfr.gov"}:
                    ecfr = re.search(rf"/full/({_DATE})/(title-\d+)\.xml", native_path)
                    if ecfr and _date(ecfr[1]):
                        native("native_id", ecfr[2], field)
                        native("source_version", ecfr[1], field)
                        period("requested-issue-date", ecfr[1], location, field)
                if "fec.gov" in host or host == "cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com":
                    cycle = re.search(r"/bulk-downloads/(\d{4})/", native_path)
                    if cycle:
                        period("election-cycle", cycle[1], location, field)
                    for name in ("cycle", "two_year_transaction_period", "election_full"):
                        for value in query.get(name, []):
                            if re.fullmatch(r"\d{4}", value):
                                period("requested-election-cycle", value, f"{field} query {name}", field)
                if host.endswith("federalregister.gov"):
                    self._query_periods(query, period, field, "conditions[publication_date][gte]",
                                        "conditions[publication_date][lte]", "requested-publication-range")
                if host == "lda.gov" or host.endswith(".lda.gov"):
                    self._query_periods(query, period, field, "filing_dt_posted_after",
                                        "filing_dt_posted_before", "requested-filing-posted-range")
                    for value in query.get("filing_year", []):
                        if re.fullmatch(r"\d{4}", value):
                            period("requested-filing-year", value, f"{field} query filing_year", field)
                if host == "raw.githubusercontent.com":
                    parts = native_path.strip("/").split("/")
                    if len(parts) > 2 and re.fullmatch(r"[0-9a-f]{40}", parts[2]):
                        native("source_version", parts[2], field)
                if host == "api.congress.gov":
                    congress = re.search(r"/v3/(?:bill|amendment|committee-report|committee-meeting|hearing|nomination|treaty|house-communication)/(\d{1,3})(?:/|$)", native_path)
                    if congress:
                        period("congress", congress[1], location, field)
            else:
                for pattern, family in _PATH_RULES:
                    if pattern.search(location):
                        bind(self.family_id(family), field, location, "retained-source-directory", 4)
                        break

        selected = candidates[min(candidates)] if candidates else set()
        identity = next(iter(selected)) if len(selected) == 1 else None
        conflict = len(selected) > 1
        for target, values in url_claims.items():
            if facts[target] is None and target not in facts["field_conflicts"]:
                facts[target] = values[0] if len(values) == 1 else None
                if len(values) > 1:
                    facts["field_conflicts"][target] = values
        return {
            **facts,
            "dataset_id": identity,
            "mapping_status": "mapped" if identity else "conflict" if selected else "unmapped",
            "native_id": None if conflict else facts["native_id"],
            "periods": [] if conflict else periods,
            "candidate_periods": periods if conflict else [],
            "source_version": None if conflict else facts["source_version"],
            "role": role,
            "candidate_dataset_ids": sorted(selected),
            "evidence": evidence,
        }

    @staticmethod
    def _query_periods(query, add, field, lower, upper, kind):
        start, end = query.get(lower, []), query.get(upper, [])
        if len(start) == len(end) == 1 and _date(start[0]) and _date(end[0]) and start[0] <= end[0]:
            add(kind, f"{start[0]}/{end[0]}", f"{field} query {lower} and {upper}; pagination completeness unknown", field)

    @staticmethod
    def _url_family(host: str, path: str) -> str | None:
        if host in {"www-senate-gov-media-srs.akamaized.net", "www-senate-gov-msl3archive.akamaized.net"}:
            return "committee-meetings"
        if host.endswith(".senate.gov") and any(part in path.lower() for part in ("/download/", "/hearings/", "/services/files", "/public/")):
            return "committee-meetings"
        if host == "docs.house.gov" and path.startswith("/meetings/"):
            return "committee-meetings"
        if host in {"www.federalregister.gov", "federalregister.gov"}:
            return "federal-register"
        if host in {"www.ecfr.gov", "ecfr.gov"}:
            return "cfr-sections"
        if host in {"www.gao.gov", "gao.gov"}:
            return "gao-reports"
        if host == "api.open.fec.gov":
            return "fec-query"
        if host == "docquery.fec.gov" or host == "cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com":
            return "fec-observations"
        if host in {"www.fec.gov", "fec.gov", "beta.fec.gov"}:
            return "fec-source-catalog"
        if host == "lda.gov" or host.endswith(".lda.gov"):
            return "lobbying-filings"
        if host in {"www.courtlistener.com", "courtlistener.com", "storage.courtlistener.com"}:
            return "courtlistener"
        if host in {"api.regulations.gov", "www.regulations.gov"}:
            if "/comments" in path:
                return "comments-mirror"
            if "/dockets" in path:
                return "dockets"
            if "/documents" in path:
                return "documents"
        if host == "raw.githubusercontent.com" and "/unitedstates/statements-of-administration-policy/" in path:
            return "administration-policy"
        if host == "raw.githubusercontent.com" and "/unitedstates/reports/" in path and "/inspectors-general/" in path:
            return "inspectors-general-archive"
        if host == "api.congress.gov":
            endpoint = path.strip("/").split("/")
            families = {"bill": "bill-family", "amendment": "amendments", "member": "members",
                        "committee-report": "committee-reports", "hearing": "committee-reports",
                        "committee-meeting": "committee-meetings", "nomination": "nominations", "treaty": "treaties",
                        "house-communication": "house-communications"}
            return families.get(endpoint[1]) if len(endpoint) > 1 else None
        return None


@lru_cache(maxsize=1)
def _default_mapper() -> SourceMapper:
    return SourceMapper()


def map_source(record: dict, registry: dict | None = None) -> dict:
    """Convenience form; use SourceMapper once when supplying a custom registry."""
    return (SourceMapper(registry) if registry is not None else _default_mapper()).map_source(record)
