"""One bill's CRS policy area and legislative subjects, and the Congress.gov fetch for bills BILLSTATUS cannot reach.

``congress_bills`` rows from the list route carry no subject signal, so
:mod:`spicy_regs.transforms.enrich_bill_subjects` publishes the assignment as a
sibling table. The Library of Congress assigns each bill one ``policyArea`` (a
~33-term controlled list) and any number of legislative subjects, and two
publishers serve those same assignments; the Congress decides which one answers:

``govinfo-billstatus``
    From the 108th Congress on, BILLSTATUS states both fields. The transform
    reads them from the bill family's own rows or from the folder's bulk zip,
    one request per Congress and bill type, never one per bill.

``congress-api``
    Below the 108th, only ``GET /v3/bill/{congress}/{type}/{number}/subjects``
    holds them, back to the 93rd Congress, when CRS began indexing legislation.
    It returns **both** fields in one response, so a bill costs one request
    until its subjects overflow a page. Requires an api.data.gov key, resolved
    through the same fallback chain as :mod:`spicy_regs.sources.congress_bills`
    and sent, like there, only as the ``X-Api-Key`` header, never in the URL.

**Three outcomes, not two.** :meth:`BillSubjectsFetcher.subjects_for` returns a
:class:`BillSubjects` when the carrier *answered* (including an answer of "no
assignments"), and ``None`` when it did not (timeout, 5xx, exhausted retries, or
a subject list longer than the page walk may read). The transform writes a row
only for an answer, so a network wobble leaves the bill un-enriched for the next
run instead of pinning an empty or truncated answer to it. Credential refusal
(401/403) aborts the run on the first answer, without retry. The subject table
retains carrier and time, not the response bytes.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from spicy_regs.sources.congress_bills import API_BASE, API_KEY_ENV_VARS, _resolve_api_key

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

#: Carrier names, stored verbatim in ``bill_subjects.carrier`` so a reader can
#: tell which publisher supplied a row.
CARRIER_API = "congress-api"
CARRIER_BULKDATA = "govinfo-billstatus"

#: The earliest Congress the API's subject assignments reach: the 93rd, when
#: CRS began indexing legislation.
API_FIRST_CONGRESS = 93

#: An honest identifying User-Agent on every request, per the repo's other
#: scraped sources (``uscode_olrc``, ``gao_reports``, ``courtlistener_bulk``).
_USER_AGENT = "spicy-regs-etl/1.0 (+https://github.com/civictechdc/spicy-regs)"

# Transport hygiene, matching ``sources.congress_bills``: bound every request and
# retry transient failures with backoff.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 4

#: Max subjects the API returns per page (the ``limit`` ceiling), and how many
#: pages one bill may cost. 4 x 250 = 1,000 subjects is far past the fattest
#: bill observed; a bill stating more is refused whole, never published short.
_SUBJECTS_PER_PAGE = 250
_MAX_SUBJECT_PAGES = 4

#: Requests an hour Congress.gov documents for a keyed client. It is a stated
#: budget, not a guess, and every attempt spends from it — retries included.
API_HOURLY_BUDGET = 5_000

#: Minimum seconds between the starts of two API requests: at most 1.33 a
#: second, under the 1.39/s the hourly budget allows, so a run of retries
#: cannot walk into 429s. It paces the start of each request rather than adding
#: a sleep to it: measured 2026-09-23 over 30 bills, one ``/subjects`` round
#: trip took 1.36 s on average, so a sleep on top made a bill cost 2.1 s.
DELAY_SECONDS = 0.75


class _Absent:
    """Sentinel: the carrier answered, and does not hold this bill."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<absent>"


_ABSENT = _Absent()


class _PageCapExceeded(Exception):
    """The bill states more subjects than :data:`_MAX_SUBJECT_PAGES` pages hold."""


@dataclass(frozen=True)
class BillSubjects:
    """One bill's subject assignment, as a carrier actually answered it.

    An answer of ``policy_area=None, subjects=()`` is a real answer, and it has
    two distinguishable causes: the carrier holds the bill and no terms were
    assigned to it (``held=True``), or the carrier has no record of the bill at
    all (``held=False`` — a 404, or a bulk folder that does not list it). Both
    publish the same null, but a coverage number that cannot tell them apart is
    a coverage number nobody can read, so the run report counts them separately.
    A carrier that never answered is ``None`` instead, and is left for the next
    run.
    """

    policy_area: str | None
    subjects: tuple[str | None, ...]
    carrier: str
    held: bool = True


def assignment(policy_area: object, subjects: Iterable[object], carrier: str) -> BillSubjects:
    """Retain literal names, including repeated, blank and null occurrences."""
    names = tuple(subjects)
    if any(value is not None and not isinstance(value, str) for value in (policy_area, *names)):
        raise ValueError("Subject assignments require text or null names")
    from typing import cast
    return BillSubjects(cast(str | None, policy_area), cast(tuple[str | None, ...], names), carrier)


@dataclass
class FetchCounts:
    """Per-run tally of how each bill landed; every recorded result lands in exactly one bucket."""

    answered: int = 0
    with_policy_area: int = 0
    subjects_only: int = 0
    unassigned: int = 0
    not_held: int = 0
    failed: int = 0
    policy_areas: dict[str, int] = field(default_factory=dict)

    def record(self, result: BillSubjects | None) -> None:
        if result is None:
            self.failed += 1
            return
        self.answered += 1
        if result.policy_area:
            self.with_policy_area += 1
            self.policy_areas[result.policy_area] = self.policy_areas.get(result.policy_area, 0) + 1
        elif result.subjects:
            self.subjects_only += 1
        elif result.held:
            self.unassigned += 1
        else:
            self.not_held += 1


class BillSubjectsFetcher:
    """Fetches one bill's policy area + legislative subjects from Congress.gov.

    Build one per run and reuse it: the HTTP client is opened lazily on the
    first fetch and held for connection reuse. Not shared-safe across threads.
    ``deadline`` is an instant on ``clock``: no request attempt — first page,
    later page or retry — starts at or after it, and the bill is then no
    answer. One already started ends within its 60-second timeout.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        delay: float = DELAY_SECONDS,
        client: httpx.Client | None = None,
        deadline: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        evidence: CaptureEvidence | None = None,
    ) -> None:
        key = api_key if api_key is not None else _resolve_api_key()
        if not key:
            raise ValueError(
                f"carrier {CARRIER_API!r} needs an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})"
            )
        self.api_key: str = key
        self.delay = delay
        self.deadline = deadline
        self._clock = clock
        self._last_start = float("-inf")
        self._client = client
        self._owns_client = client is None
        self.evidence = evidence
        if evidence is not None:
            evidence.credential = key

    def __enter__(self) -> BillSubjectsFetcher:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None and self._owns_client:
            self._client.close()
            self._client = None

    def subjects_for(self, congress: str, bill_type: str, bill_number: str) -> BillSubjects | None:
        """One ``/subjects`` call (plus offset pages) for both fields, or None if there was no whole answer."""
        url = f"{API_BASE}/bill/{congress}/{str(bill_type).lower()}/{bill_number}/subjects"
        try:
            return self._walk(url)
        except _PageCapExceeded as refusal:
            logger.error(
                "Bill subjects: refusing a truncated list for {}-{}-{}: {}", congress, bill_type, bill_number, refusal
            )
            return None

    def _walk(self, url: str) -> BillSubjects | None:
        policy_area: object = None
        names: list[object] = []
        seen = 0
        total: object = None
        for page in range(_MAX_SUBJECT_PAGES):
            params = {"offset": seen, "limit": _SUBJECTS_PER_PAGE, "format": "json"}
            payload = self._get_json(url, params=params)
            if isinstance(payload, _Absent):
                return BillSubjects(None, (), CARRIER_API, held=False)
            if payload is None:
                # A later page failing must not publish a truncated subject list.
                return None
            block = payload.get("subjects") or {}
            policy_area = policy_area or (block.get("policyArea") or {}).get("name")
            listed = block.get("legislativeSubjects") or []
            names.extend(item.get("name") for item in listed if isinstance(item, dict))
            seen += len(listed)
            total = (payload.get("pagination") or {}).get("count")
            if not isinstance(total, int) or seen >= total:
                return assignment(policy_area, names, CARRIER_API)
            if total > _MAX_SUBJECT_PAGES * _SUBJECTS_PER_PAGE:
                # Every later page would be read only to refuse the bill; slow pages cost minutes each.
                raise _PageCapExceeded(
                    f"{total} subjects stated, {_MAX_SUBJECT_PAGES} pages of {_SUBJECTS_PER_PAGE} allowed"
                )
            if not listed:
                raise _PageCapExceeded(f"page {page + 1} is empty with {seen} of {total} subjects read")
        raise _PageCapExceeded(f"{total} subjects stated, {_MAX_SUBJECT_PAGES} pages of {_SUBJECTS_PER_PAGE} allowed")

    # -- transport -----------------------------------------------------------

    def _get_json(self, url: str, *, params: dict) -> dict | _Absent | None:
        """Fetch a JSON body. A body that will not parse counts as no answer."""
        response = self._request(url, params=params)
        if response is None or isinstance(response, _Absent):
            return response
        try:
            payload = response.json()
        except ValueError as exc:
            logger.warning("Bill subjects: unparseable JSON from {}: {}", url, exc)
            return None
        return payload if isinstance(payload, dict) else None

    def _request(self, url: str, *, params: dict | None) -> httpx.Response | _Absent | None:
        """GET with bounded retries + exponential backoff.

        Returns the response, :data:`_ABSENT` on a definitive 404 (the carrier
        does not hold this bill), or ``None`` when no answer was obtained,
        including when the run's deadline arrives before an attempt starts. A
        401/403 raises ``CredentialRefusedError`` on the first answer: a refused
        key is not transient, and every retry spends the hourly budget.

        The key travels only as the ``X-Api-Key`` header, and redirects are not
        followed because httpx forwards custom headers to a cross-origin
        redirect. Logs name method, host, path and status, never the exception
        text, which renders the whole request URL.
        """
        from spicy_docs.transport.credentials import ACCESS_REFUSED_STATUSES, CredentialRefusedError, refusal_message

        if self._client is None:
            self._client = httpx.Client(
                timeout=_TIMEOUT,
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
                transport=None if self.evidence is None else self.evidence.transport(
                    stage="bill-subject-response", max_bytes=8 * 1024 * 1024,
                ),
            )
        target = httpx.URL(url)
        where = f"GET {target.host}{target.path}"
        key_header = {"X-Api-Key": self.api_key}
        for attempt in range(1, _MAX_RETRIES + 1):
            wait = self._last_start + self.delay - self._clock()
            if wait > 0:
                time.sleep(wait)
            if self.deadline is not None and self._clock() >= self.deadline:
                logger.warning("Bill subjects: run deadline reached before {} (attempt {})", where, attempt)
                return None
            self._last_start = self._clock()
            try:
                resp = self._client.get(url, params=params, headers=key_header, follow_redirects=False)
                if resp.status_code in ACCESS_REFUSED_STATUSES:
                    raise CredentialRefusedError(refusal_message("congress.gov", resp.status_code, target.path))
                if resp.status_code == 404:
                    return _ABSENT
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp
            except httpx.HTTPError as exc:
                reason = (
                    f"HTTP {exc.response.status_code}" if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
                )
                if attempt == _MAX_RETRIES:
                    logger.error("Bill subjects: giving up on {} after {} attempts: {}", where, attempt, reason)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Bill subjects: {} {} (attempt {}/{}), retrying in {}s",
                    where,
                    reason,
                    attempt,
                    _MAX_RETRIES,
                    backoff,
                )
                time.sleep(backoff)
        return None
