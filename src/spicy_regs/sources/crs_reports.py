"""CRS report lists through the SpicyDocs Congress.gov reader.

SpicyDocs owns requests, page parsing, declared counts and continuations. CRS
ignores the requested sort, so the source window is explicit and a locally old
row never ends the walk. The list can shift while it is read (the 2026-09-22
scheduled run met a repeated report), so whole walks are pooled by report id
(``CongressListingReader.pooled``, which varies the page size per walk) until
one is clean or the pool holds exactly the declared count. Failed or incomplete
walks raise before the transform can replace its prior table. Successful empty
windows remain valid.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import date
from operator import itemgetter
from pathlib import Path
import hashlib

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import httpx

from spicy_regs.env_values import first_env
from spicy_regs.sources.base import Reader

API_BASE = "https://api.congress.gov/v3"
PER_PAGE = 250
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "CONGRESS_GOV_API_KEY", "REGULATIONS_GOV_API_KEY")
_MAX_PAGES = 400
_MAX_REQUESTS_PER_PAGE = 5


class CrsReportsError(ValueError):
    """The selected CRS report list cannot be established."""


def _resolve_api_key() -> str | None:
    """Return the first nonempty configured api.data.gov key."""
    return first_env(API_KEY_ENV_VARS)


class CrsReportsReader(Reader):
    """Yield raw CRS report dicts, filtering locally to ``since`` or later.

    Requires an api.data.gov key (``CrsReportsError`` without one). A walk stating no
    declared count, a report with no ``id`` or ``updateDate``, or a query spicy-docs'
    pooled walks cannot settle raises ``PagedJsonSourceError``. ``per_page`` is clamped
    to ``PER_PAGE``.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        per_page: int = PER_PAGE,
        api_key: str | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
        evidence: CaptureEvidence | None = None,
    ) -> None:
        if isinstance(per_page, bool) or not isinstance(per_page, int) or per_page < 1:
            raise ValueError("per_page must be a positive integer")
        self.since = since
        self.per_page = min(per_page, PER_PAGE)
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.verbose = verbose
        self.transport = transport
        self.evidence = evidence
        self.witnesses: dict[tuple, list[dict]] = {}

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            raise CrsReportsError("CRS reports require an api.data.gov key")
        try:
            from spicy_docs.reading.paged_json import PagedJsonBudget
            from spicy_docs.sources.congress.listing import LIST_ROUTES, CongressListingReader, crs_report_list_url
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "CRS reports require spicy-regs[source-readers]. Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        url = crs_report_list_url(
            from_datetime=f"{self.since.isoformat()}T00:00:00Z" if self.since else None,
            limit=self.per_page,
        )
        budget = PagedJsonBudget(
            max_requests=_MAX_REQUESTS_PER_PAGE,
            max_page_bytes=16 * 1024 * 1024,
            timeout_seconds=60,
            min_request_interval_seconds=0,
        )
        transport = self.transport
        if self.evidence:
            self.evidence.credential = self.api_key
            self.evidence.event("selection", stage="crs", url=url, since=str(self.since), max_pages=_MAX_PAGES)
            transport = self.evidence.transport(transport, stage="crs-response", max_bytes=budget.max_page_bytes)
        witnesses = self.witnesses
        class WitnessedListingReader(CongressListingReader):
            def records(self, *args, **kwargs):
                for page in super().records(*args, **kwargs):
                    for ordinal, report in enumerate(page.records):
                        witness = {"source_id": "congress.gov:crs-list", "source_uri": page.capture.requested_url,
                                   "sha256": page.sha256, "locator": f"/CRSReports/{ordinal}",
                                   "body_version": str(report.get("version"))}
                        key = (report.get("id"), report.get("updateDate"))
                        held = witnesses.setdefault(key, [])
                        if witness not in held:
                            held.append(witness)
                    yield page
        with WitnessedListingReader(budget=budget, api_key=self.api_key, transport=transport) as reader:
            pooled = reader.pooled(
                LIST_ROUTES["crsreport"],
                url,
                key=lambda report: report.get("id"),  # a missing id refuses the walk
                version=itemgetter("updateDate"),
                max_pages=_MAX_PAGES,
            )
        for report in pooled.records:
            # Do not rely on the server's ignored sort parameter.
            if self.since is None or not _older_than(report, self.since):
                yield dict(report)


class CrsReportDetailReader:
    """One bounded, captured official detail read for one selected metadata revision."""

    def __init__(self, *, evidence, api_key=None, transport=None, retained: dict[str, tuple[Path, str]] | None = None):
        self.evidence = evidence
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.transport = transport
        self.retained = retained

    def read(self, selected: dict) -> dict:
        from spicy_docs.sources.congress.crs_summaries import CrsRevisionMismatchError, fetch_one, read_report, report_version
        from spicy_docs.transport.credentials import CredentialRefusedError, failure_reason

        if self.retained is not None:
            if selected["report_id"] not in self.retained:
                raise CrsReportsError("Selected CRS detail has no explicitly supplied retained original")
            path, digest = self.retained[selected["report_id"]]
            with path.open("rb") as stream:
                body = stream.read(2 * 1024 * 1024 + 1)
            if len(body) > 2 * 1024 * 1024 or "sha256:" + hashlib.sha256(body).hexdigest() != digest:
                raise CrsReportsError("Retained CRS detail differs from its pinned original")
            self.evidence.retain_bytes(body, stage="crs-detail-retained", raw_source=True,
                                       original_location=str(path), source_sha256=digest)
            witnesses = [{"source_id": "congress.gov:crs-detail", "source_uri": str(path),
                          "sha256": digest, "locator": "/CRSReport", "body_version": None}]
            try:
                report = read_report(body, selected["report_id"], update_date=selected["update_date"],
                                     version=selected.get("version"))
            except CrsRevisionMismatchError as error:
                return {"state": "revision_mismatch", "witnesses": witnesses, "error": str(error)}
            witnesses[-1]["body_version"] = str(report_version(report))
            return {"state": "ok", "report": report, "witnesses": witnesses, "error": None}
        if not self.api_key:
            raise CrsReportsError("CRS details require an api.data.gov key")
        self.evidence.credential = self.api_key
        witnesses = []
        def capture(response):
            if response.byte_size > 2 * 1024 * 1024:
                raise CrsReportsError("CRS detail exceeds 2 MiB byte bound")
            witnesses.append({"source_id": "congress.gov:crs-detail", "source_uri": response.requested_url,
                              "sha256": response.sha256, "locator": "/CRSReport" if response.status_code == 200 else None,
                              "body_version": None})
        transport = self.evidence.transport(self.transport, stage="crs-detail-response", max_bytes=2 * 1024 * 1024)
        with httpx.Client(timeout=60, transport=transport, headers={"Accept-Encoding": "identity"}) as client:
            try:
                report = fetch_one(client, selected["report_id"], self.api_key,
                                   update_date=selected["update_date"], version=selected.get("version"), capture=capture)
            except CredentialRefusedError:
                raise
            except CrsRevisionMismatchError as error:
                return {"state": "revision_mismatch", "witnesses": witnesses, "error": str(error)}
            except (ValueError, TypeError, httpx.HTTPError) as error:
                return {"state": "failed", "witnesses": witnesses, "error": failure_reason(error, self.api_key)}
        witnesses[-1]["body_version"] = str(report_version(report))
        return {"state": "ok", "report": report, "witnesses": witnesses, "error": None}


def _older_than(report: Mapping, since: date) -> bool:
    """True when ``updateDate`` parses to a date before ``since``; missing or invalid dates are not older."""
    raw = report.get("updateDate")
    if not raw:
        return False
    try:
        return date.fromisoformat(str(raw)[:10]) < since
    except ValueError:
        return False
