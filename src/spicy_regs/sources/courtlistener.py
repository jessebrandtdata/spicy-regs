"""CourtListener docket and opinion searches through the SpicyDocs page reader.

The provider owns requests, exact response parsing and cursor validation. Failed
or incomplete walks raise before consumers replace their prior tables. Docket
searches select APA/review-of-agency cases (nature of suit 899) or named docket
ids; opinion searches provide cluster metadata for catch-up, not full opinion
bodies, selected by filing date or by cluster id. COURTLISTENER_API_TOKEN is
required for live API dispatch (SpicyDocs 0.66.6 refuses keyless search) and
travels only in a request header. Every request is paced to the published
per-minute limit.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator, Mapping
from contextlib import nullcontext
from datetime import date
from decimal import Decimal
from typing import Any, Literal

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.sources.courtlistener_resume import RetainedCourtSearch

import httpx

from spicy_regs.sources.base import Reader

API_BASE = "https://www.courtlistener.com/api/rest/v4"
APA_NATURE_OF_SUIT = "899"
API_TOKEN_ENV_VAR = "COURTLISTENER_API_TOKEN"
_MAX_PAGES = 5_000
_MAX_REQUESTS_PER_PAGE = 6
#: CourtListener's API help page states authenticated limits of 5 requests a
#: minute, 50 an hour and 125 a day (checked 2026-09-26; doubled through
#: October 1). A token's budget is shared by every run that uses it.
MIN_REQUEST_INTERVAL_SECONDS = 12.0
#: The search API refuses (and spicy-docs' search_url refuses) a longer ``q``.
QUERY_LIMIT = 512


class CourtListenerError(ValueError):
    """The selected CourtListener search cannot be established."""


def record_json(value: object) -> str:
    """A selected record as the JSON text ``json.dumps(value, ensure_ascii=False)`` writes, numbers kept exact.

    The search reader parses publisher numbers as ``Decimal`` (every RECAP result carries ``meta.score.bm25``),
    which ``json.dumps`` refuses; each is written as its decimal literal rather than rounded through float.
    """
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise CourtListenerError("CourtListener record holds a non-finite number")
        return str(value)
    if isinstance(value, dict):
        return "{" + ", ".join(f"{json.dumps(key, ensure_ascii=False)}: {record_json(item)}"
                               for key, item in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(record_json(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=False)


def _resolve_api_token() -> str | None:
    return os.environ.get(API_TOKEN_ENV_VAR, "").strip() or None


def require_api_token(purpose: str) -> str:
    """Refuse before any work when live search would run keyless, which SpicyDocs refuses at dispatch."""
    token = _resolve_api_token()
    if token is None:
        raise CourtListenerError(
            f"{purpose} requires {API_TOKEN_ENV_VAR}: CourtListener's live search API refuses keyless requests. "
            "Set the token for this job, or build offline from pinned originals without the search catch-up."
        )
    return token


class CourtListenerReader(Reader):
    """Select RECAP dockets; a record cap is an explicit prefix, not a full walk.

    Every record needs a distinct positive integer identity; an omitted count, a terminal page
    that disagrees with an exact declared count, or a continuation after an empty page raises
    ``CourtListenerError`` rather than truncating the selection.
    """

    kind: Literal["r", "o"] = "r"
    identity_field = "docket_id"
    court: str | None = None
    query: str | None = None

    def __init__(
        self,
        *,
        since: date | None = None,
        nature_of_suit: str | None = APA_NATURE_OF_SUIT,
        max_records: int | None = None,
        api_token: str | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
        evidence: CaptureEvidence | None = None,
        traversal_budget: CourtListenerTraversalBudget | None = None,
        retained_search: RetainedCourtSearch | None = None,
    ) -> None:
        if max_records is not None and (type(max_records) is not int or max_records < 1):
            raise ValueError("max_records must be a positive integer")
        self.since = since
        self.nature_of_suit = nature_of_suit
        self.max_records = max_records
        self.api_token = api_token if api_token is not None else _resolve_api_token()
        self.verbose = verbose
        self.transport = transport
        self.evidence = evidence
        self.traversal_budget = traversal_budget
        self.retained_search = retained_search
        if retained_search is not None and (evidence is None or max_records is not None):
            raise ValueError("Retained CourtListener search requires source evidence and an uncapped walk")

    def _validate_record(self, record: Mapping[str, Any], seen: set[int]) -> None:
        identity = record.get(self.identity_field)
        if type(identity) is not int or identity <= 0 or identity in seen:
            raise CourtListenerError("CourtListener search has a missing, invalid or repeated identity")

    def iter_records(self) -> Iterator[dict]:
        try:
            from spicy_docs.reading.paged_json import PagedJsonBudget
            from spicy_docs.sources.courtlistener.search import (
                OPINION_STATUSES, CourtListenerSearchReader, CourtListenerTraversalBudget, is_exact_count, search_url,
            )
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "CourtListener search requires spicy-regs[source-readers]. "
                    "Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        url = search_url(
            kind=self.kind,
            filed_after=self.since.isoformat() if self.since else None,
            court=self.court,
            nature_of_suit=self.nature_of_suit if self.kind == "r" else None,
            q=self.query,
            opinion_statuses=OPINION_STATUSES if self.kind == "o" else None,
        )
        budget = PagedJsonBudget(
            max_requests=_MAX_REQUESTS_PER_PAGE,
            max_page_bytes=16 * 1024 * 1024,
            timeout_seconds=60,
            min_request_interval_seconds=MIN_REQUEST_INTERVAL_SECONDS,
        )
        seen = set()
        yielded = 0
        declared = None
        transport = self.transport
        if self.evidence:
            self.evidence.credential = self.api_token or ""
            self.evidence.event("selection", stage="courtlistener", url=url, max_records=self.max_records)
            transport = self.evidence.transport(transport, stage="courtlistener-response", max_bytes=budget.max_page_bytes)
        # One caller's allocation; a retained continuation also fits it to the account's usage.
        traversal = self.traversal_budget or CourtListenerTraversalBudget(max_requests=50, max_wait_seconds=600)
        recovery = (
            self.retained_search.open(
                url=url, page_budget=budget, traversal_budget=traversal, live_transport=transport,
                usage_transport=self.transport, evidence=self.evidence, api_token=self.api_token,
            ) if self.retained_search else nullcontext((transport, iter, traversal))
        )
        try:
            with recovery as (selected_transport, track, traversal), CourtListenerSearchReader(
                budget=budget, traversal_budget=traversal, api_key=self.api_token, transport=selected_transport,
            ) as reader:
                # Commit only after the existing whole-page native checks.
                # The same checks run again on a retained prefix before live continuation.
                def checked_pages():
                    nonlocal declared
                    for page in reader.search(url, max_pages=_MAX_PAGES):
                        if page.declared_count is None:
                            raise CourtListenerError("CourtListener search omitted its declared count")
                        if declared is None:
                            declared = page.declared_count
                        for record in page.records:
                            self._validate_record(record, seen)
                            seen.add(record[self.identity_field])
                        if is_exact_count(self.kind, declared) and page.next_url is None and len(seen) != declared:
                            raise CourtListenerError("CourtListener terminal page disagrees with its declared count")
                        # Large RECAP counts can be estimates; the terminal cursor
                        # ends a nonempty docket walk, not equality with that estimate.
                        if page.next_url is None and not seen and page.declared_count:
                            raise CourtListenerError("CourtListener empty search has a positive declared count")
                        if page.next_url is not None and not page.records:
                            raise CourtListenerError("CourtListener continuation follows an empty page")
                        yield page

                for page in track(checked_pages()):
                    for record in page.records:
                        yield dict(record)
                        yielded += 1
                        if self.max_records is not None and yielded >= self.max_records:
                            return
        except Exception as error:
            if self.evidence:
                allocation = getattr(error, "traversal_allocation", None)
                if allocation is not None:
                    # The evidence JSON forbids binary floats; keep exact diagnostic text.
                    allocation = {name: str(value) if isinstance(value, float) else value
                                  for name, value in allocation.items()}
                self.evidence.event(
                    "refusal", stage="courtlistener-traversal", error=str(error),
                    traversal=getattr(error, "courtlistener_traversal", None),
                    allocation=allocation,
                )
            raise


class CourtListenerOpinionSearchReader(CourtListenerReader):
    """Select opinion cluster metadata; full text remains a bulk-source task.

    ``above`` selects indexed clusters of every publisher-defined status whose
    id is greater, whatever their filing date. ``through`` bounds the selection
    by a sibling export's ceiling. Search can omit unindexed RECAP clusters;
    exhaustion is not complete publisher coverage or citation membership proof.
    Historical corrections arrive with the next bulk export.
    """

    kind: Literal["r", "o"] = "o"
    identity_field = "cluster_id"

    def __init__(
        self,
        *,
        since: date | None = None,
        above: int | None = None,
        through: int | None = None,
        court: str | None = None,
        max_records: int | None = None,
        api_token: str | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
        evidence: CaptureEvidence | None = None,
        traversal_budget: CourtListenerTraversalBudget | None = None,
        retained_search: RetainedCourtSearch | None = None,
    ) -> None:
        if above is not None and (type(above) is not int or above < 0):
            raise ValueError("above must be a non-negative integer cluster id")
        if through is not None and (type(through) is not int or above is None or through <= above):
            raise ValueError("through must be an integer cluster id greater than above")
        super().__init__(
            since=since, max_records=max_records, api_token=api_token, verbose=verbose, transport=transport,
            evidence=evidence, traversal_budget=traversal_budget, retained_search=retained_search,
        )
        self.court = court
        self.above = above
        self.through = through
        self.query = None if above is None else f"cluster_id:[{above + 1} TO {through if through is not None else '*'}]"

    def _validate_record(self, record: Mapping[str, Any], seen: set[int]) -> None:
        super()._validate_record(record, seen)
        identity = record[self.identity_field]
        if ((self.above is not None and identity <= self.above) or
                (self.through is not None and identity > self.through)):
            raise CourtListenerError("CourtListener opinion lies outside the selected cluster range")


def docket_id_queries(docket_ids: Iterable[str]) -> list[str]:
    """Pack docket ids, in order, into as few ``docket_id:(a OR b ...)`` queries as ``QUERY_LIMIT`` allows."""
    queries: list[str] = []
    batch: list[str] = []
    length = 0
    for docket_id in docket_ids:
        added = len(docket_id) + (len(" OR ") if batch else len("docket_id:()"))
        if batch and length + added > QUERY_LIMIT:
            queries.append(f"docket_id:({' OR '.join(batch)})")
            batch, length = [], 0
            added = len(docket_id) + len("docket_id:()")
        if added > QUERY_LIMIT:
            raise ValueError(f"Docket id {docket_id[:20]} does not fit one query")
        batch.append(docket_id)
        length += added
    if batch:
        queries.append(f"docket_id:({' OR '.join(batch)})")
    return queries


class CourtListenerDocketIdReader(CourtListenerReader):
    """Re-read named RECAP dockets, whatever their filing date or nature of suit, from one packed query."""

    def __init__(self, *, query: str, api_token: str | None = None, transport: httpx.BaseTransport | None = None,
                 evidence: CaptureEvidence | None = None) -> None:
        super().__init__(nature_of_suit=None, api_token=api_token, transport=transport, evidence=evidence)
        self.query = query
