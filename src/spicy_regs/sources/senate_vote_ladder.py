"""Senate roll-call files through the walled-fetch ladder, then the Internet Archive, when senate.gov refuses (ss-bruk).

www.senate.gov answers the LIS vote menu (``vote_menu_119_1.xml``) with HTTP 403 to GitHub's runners and to this
Mac (measured 2026-11-08), so the votes rollup published only the House half. Owner decision 2026-11-09: read the
Senate through Zyte, with Firecrawl only as a hedge, and then from the Internet Archive. Measured the same day, neither
provider reaches it: Zyte answers HTTP 520 "Website Ban" (plain, US-geolocated and browser requests; bans are
unbilled) and Firecrawl's engines all fail (stealth proxy, US); Zyte's residential IPs need account KYC.

:class:`LadderVoteAcquirer` is spicy-docs' ``VoteAcquirer`` with one change: the Senate menu and each Senate vote
file go through ``SourceAcquirer.capture_walled``, spicy-docs' ladder. It tries one direct attempt on this
acquirer's own client first, so a free answer is always taken and a proxy is paid only after a refusal; then one
Zyte ``httpResponseBody`` call (the publisher's bytes, never a rendered page), then one Firecrawl ``rawBase64`` call
when ``FIRECRAWL_API_KEY`` is set. A provider without a credential makes no request. Every answer is held to the
budget's byte bound, the exact locator, the file's XML media types, ``publisher_page`` (an HTML page is an error page,
so the ladder escalates past it) and the reader's own validation, which proves the file's congress, session and roll
number. A proxy that fails one Senate file is not asked again in the run: senate.gov bans a provider's whole pool,
not one file, and every further request would cost a timeout. House reads are unchanged.

**The Internet Archive is the last rung**, read only when every ladder rung refused (``VoteRefusedError``; a
publisher 404 is absence and never reaches it). One CDX query per file family -- the menu's own URL, or a session's
whole ``vote<congress><session>/`` directory, read once per acquirer, a failed read included -- names the newest
200 ``text/xml`` capture of each file. Its ``id_`` replay (the archived bytes, not the rewritten page) is read on this acquirer's own client and
user agent, paced at :data:`ARCHIVE_INTERVAL_SECONDS`, and must hash (SHA-1, as served) to the digest the CDX row
states, pass the same XML media-type check and the same reader. Measured 2026-11-09: the replay of
``vote_119_1_00001.xml`` is byte-identical to the direct capture of 2026-09-19 (SHA-256 ``9d71d78a...``). A file the
Archive holds no capture of keeps the ladder's refusal. An Archive that does not answer (a transport error or a
refusal) is not asked again in the run, so a hanging Archive costs one file's timeouts, not every file's.

Retention and provenance: a direct or archive answer is retained by the evidence transport as served (the archive's
gzip transfer wrapper included), so its central capture receipt names the replay URL and with it the capture time. A
proxy answer is retained here as a ``vote-source`` capture naming the serving ``transport`` and the provider's
request id. An archive answer is also recorded as a ``senate-archive-capture`` event, which the central receipt
archive keeps beside the captures: route ``internet_archive``, the senate.gov URL, the replay URL, ``archived_at``,
its age at the fetch in whole seconds (the journal admits no floats), and the fetch time. ``LadderVoteAcquirer.archived``
holds the same for the run, which ``build_roll_call_votes`` raises as a CI warning (:func:`senate_warnings`): an
Archive copy of any age passes, so its age must be seen where the run is read.
"""

from __future__ import annotations

import json
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlencode, urlsplit

import httpx
from loguru import logger
from spicy_docs.sources.congress.committee_rosters import archived_url
from spicy_docs.sources.congress.votes import (
    MEDIA_TYPES,
    SenateVoteMenuAcquisition,
    VoteAcquirer,
    VoteAcquisition,
    VoteLocator,
    VoteRefusedError,
    VoteSourceError,
    VoteUnavailableError,
    parse_senate_vote,
    parse_senate_vote_menu,
    senate_vote_menu_url,
)
from spicy_docs.sources.walled_fetch import Transport
from spicy_docs.sources.web_archive import payload_sha1
from spicy_docs.transport.source_acquirer import named_challenge

from spicy_regs.sources.senate_roster_ladder import is_senate_xml

if TYPE_CHECKING:
    from spicy_docs.transport.captured import CapturedBodyResponse

    from spicy_regs.source_evidence import CaptureEvidence

#: The route an archive answer is recorded under, beside the ladder's own transports.
ARCHIVE: Final = "internet_archive"
CDX_URL: Final = "https://web.archive.org/cdx/search/cdx"
#: The least time between two requests to web.archive.org; the votes budget's own pacing is twice as fast.
ARCHIVE_INTERVAL_SECONDS: Final = 1.0
#: The ``ProxyFetchers`` field of each proxy rung.
_PROXY_FIELDS: Final = {Transport.ZYTE_HTTP: "zyte", Transport.FIRECRAWL_RAW: "firecrawl"}


@dataclass(frozen=True, slots=True)
class ArchiveCapture:
    """One CDX row: the capture's 14-digit time, the URL as archived, and the archive's SHA-1 (base32) of its payload."""

    timestamp: str
    original: str
    digest: str

    @property
    def replay_url(self) -> str:
        return archived_url(self.original, self.timestamp)

    @property
    def captured_at(self) -> datetime:
        return datetime.strptime(self.timestamp, "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def _url_key(url: str) -> str:
    """Host and path, scheme and default port dropped: the CDX's ``original`` spellings of one file compare equal."""
    parts = urlsplit(url)
    return (parts.hostname or "").lower() + parts.path


def newest_captures(body: bytes) -> dict[str, ArchiveCapture]:
    """The newest capture per file from a CDX ``output=json`` answer of ``timestamp,original,digest`` rows."""
    try:
        rows = json.loads(body)
    except ValueError as error:
        raise VoteSourceError("Internet Archive CDX answer is not JSON") from error
    if not isinstance(rows, list) or (rows and rows[0] != ["timestamp", "original", "digest"]):
        raise VoteSourceError("Internet Archive CDX answer does not have the requested fields")
    newest: dict[str, ArchiveCapture] = {}
    for row in rows[1:]:
        try:
            if not isinstance(row, list) or not all(isinstance(value, str) for value in row):
                raise TypeError("a CDX row is a list of strings")
            capture = ArchiveCapture(*row)
            capture.captured_at, capture.replay_url  # noqa: B018 -- each refuses a malformed timestamp
        except (TypeError, ValueError) as error:
            # One file's refusal, never a crashed run: the run's other chamber must still publish.
            raise VoteSourceError(f"Internet Archive CDX row is malformed: {row!r}") from error
        key = _url_key(capture.original)
        if key not in newest or capture.timestamp > newest[key].timestamp:
            newest[key] = capture
    return newest


def _archived_body(response: CapturedBodyResponse, capture: ArchiveCapture, limit: int) -> bytes:
    """The archived file: the served bytes must be the payload the CDX row hashed, then the gzip wrapper is removed."""
    if payload_sha1(response.body) != capture.digest:
        raise VoteSourceError(f"Internet Archive replay of {capture.original} does not hash to its CDX digest")
    if response.content_encoding != "gzip":
        return response.body
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        body = decoder.decompress(response.body, limit + 1)
    except zlib.error as error:
        raise VoteSourceError("Internet Archive gzip body is malformed") from error
    if len(body) > limit or decoder.unconsumed_tail:
        raise VoteSourceError("Internet Archive decoded body exceeds its byte bound")
    return body


@dataclass(frozen=True, slots=True)
class ArchiveServe:
    """One Senate file this run read from the Internet Archive: its senate.gov URL, capture time and age at the fetch."""

    url: str
    archived_at: datetime
    age_seconds: int

    @property
    def menu(self) -> bool:
        return "/roll_call_lists/" in self.url


class LadderVoteAcquirer(VoteAcquirer):
    """The roll-call acquirer, reading Senate files through the walled-fetch ladder.

    ``rungs`` counts what served each Senate file and ``archived`` lists each one the Internet Archive served.
    """

    def __init__(self, *, evidence: CaptureEvidence | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.evidence = evidence
        self.rungs: Counter[str] = Counter()
        self.archived: list[ArchiveServe] = []
        #: Each CDX scope read this run; ``None`` once its read failed, so it is not repeated.
        self._archive_indexes: dict[str, dict[str, ArchiveCapture] | None] = {}
        #: Why the Archive is not asked again this run, once it failed to answer at all.
        self._archive_down: BaseException | None = None
        self._archive_last = 0.0

    def acquire(self, locator: VoteLocator, *, crosswalk: Any = None) -> VoteAcquisition:
        if not isinstance(locator, VoteLocator) or locator.chamber != "senate":
            return super().acquire(locator, crosswalk=crosswalk)
        vote, capture = self._walled(
            locator.url(), lambda body: parse_senate_vote(body, locator, crosswalk), operation="roll-call-vote"
        )
        return VoteAcquisition(vote, capture, self.request_count, self.budget)

    def list_senate_votes(self, congress: int, session: int) -> SenateVoteMenuAcquisition:
        url = senate_vote_menu_url(congress, session)
        menu, capture = self._walled(
            url, lambda body: parse_senate_vote_menu(body, congress=congress, session=session), operation="senate-vote-menu"
        )
        return SenateVoteMenuAcquisition(menu, capture, self.request_count, self.budget)

    def _walled[Result](self, url: str, read: Callable[[bytes], Result], *, operation: str) -> tuple[Result, Any]:
        try:
            result, capture, answer = self.capture_walled(
                url,
                media_types=MEDIA_TYPES,
                parse=lambda response, _allowance: read(response.body),
                max_bytes=self.budget.max_bytes,
                unavailable=VoteUnavailableError,
                context={"operation": operation, "chamber": "senate", "url": url},
                publisher_page=is_senate_xml,
                refusal=VoteRefusedError,
            )
        except VoteSourceError as error:
            outcomes = error.__dict__.get("rung_outcomes", ())
            self._skip_proxies(outcome.transport for outcome in outcomes if outcome.kind != "credential-error")
            if not isinstance(error, VoteRefusedError):
                raise
            if self._archive_down is not None:
                raise
            try:
                with named_challenge(url, error_type=VoteRefusedError, context_key=self.context_key):
                    archived = self._archived(url, read, operation=operation)
            except (httpx.HTTPError, ConnectionError, TimeoutError, VoteRefusedError) as down:
                # The archive is a fallback: its outage is this file's refusal, never a crashed run, and it is not
                # asked again this run, so a hanging archive cannot spend every file's timeouts.
                self._archive_down = down
                logger.warning("Roll-call votes: the Internet Archive did not answer, so it is skipped this run: {}", down)
                raise VoteRefusedError(url) from down
            if archived is None:
                raise
            return archived
        if answer.transport is Transport.FIRECRAWL_RAW:
            # Zyte, the rung before it, failed this file.
            self._skip_proxies((Transport.ZYTE_HTTP,))
        rung = answer.transport.value
        self.rungs[rung] += 1
        if self.evidence is not None and answer.transport is not Transport.DIRECT:
            self.evidence.capture(capture, stage="vote-source", transport=rung, provider_request_id=answer.request_id)
        return result, capture

    def _skip_proxies(self, transports: Iterable[Transport]) -> None:
        """Each proxy in ``transports`` failed a Senate file: it makes no further request this run."""
        for transport in transports:
            field = _PROXY_FIELDS.get(transport)
            if field is None or self._proxies is None or isinstance(getattr(self._proxies, field), str):
                continue
            # A string is a provider the ladder records as unusable without making a request.
            self._proxies = replace(self._proxies, **{field: f"{transport.value} failed a Senate file earlier this run"})
            logger.warning("Roll-call votes: {} failed a Senate file, so it is not asked again this run", transport.value)

    def _archive_request[Result](self, url: str, **kwargs: Any) -> tuple[Result, CapturedBodyResponse]:
        wait = self._archive_last + ARCHIVE_INTERVAL_SECONDS - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            return self.capture_validated(url, max_bytes=self.budget.max_bytes, unavailable=VoteUnavailableError, **kwargs)
        finally:
            self._archive_last = time.monotonic()

    def _archive_index(self, url: str) -> dict[str, ArchiveCapture] | None:
        """The newest captures of ``url`` (a menu) or of its session directory (a vote file), one CDX read each.

        ``None`` once that read failed: its error was the first file's, and the scope is not read again this run.
        """
        menu = "/roll_call_lists/" in url
        scope = url if menu else url.rsplit("/", 1)[0] + "/"
        if scope not in self._archive_indexes:
            query = [("url", scope.split("://", 1)[1]), ("output", "json"), ("fl", "timestamp,original,digest"),
                     ("filter", "statuscode:200"), ("filter", "mimetype:text/xml")]  # fmt: skip
            if not menu:
                query.append(("matchType", "prefix"))
            index_url = f"{CDX_URL}?{urlencode(query)}"
            self._archive_indexes[scope] = None
            self._archive_indexes[scope], _capture = self._archive_request(
                index_url,
                media_types=("application/json",),
                parse=lambda response, _limit: newest_captures(response.body),
                context={"operation": "senate-archive-index", "chamber": "senate", "url": index_url},
            )
        return self._archive_indexes[scope]

    def _archived[Result](
        self, url: str, read: Callable[[bytes], Result], *, operation: str
    ) -> tuple[Result, CapturedBodyResponse] | None:
        """The newest archive capture of ``url`` read and validated, or ``None`` when the Archive holds none."""
        index = self._archive_index(url)
        archived = None if index is None else index.get(_url_key(url))
        if archived is None:
            return None
        result, capture = self._archive_request(
            archived.replay_url,
            media_types=MEDIA_TYPES,
            allow_gzip=True,
            parse=lambda response, limit: read(_archived_body(response, archived, limit)),
            context={"operation": operation, "chamber": "senate", "url": archived.replay_url, "original_url": url},
        )
        self.rungs[ARCHIVE] += 1
        age_seconds = int((self._clock() - archived.captured_at).total_seconds())
        self.archived.append(ArchiveServe(url, archived.captured_at, age_seconds))
        if self.evidence is not None:
            self.evidence.event(
                "senate-archive-capture", route=ARCHIVE, original_url=url, archive_url=archived.replay_url,
                archived_at=archived.captured_at.isoformat(), age_seconds=age_seconds,
                fetched_at=capture.observed_at, sha256=capture.sha256, cdx_digest=archived.digest,
                content_encoding=capture.content_encoding,
            )  # fmt: skip
        return result, capture


def senate_warnings(archived: Iterable[ArchiveServe], refused_files: int) -> list[str]:
    """What a run must say aloud about its Senate half: Archive copies (with their age) and refused files.

    Either passes the run green -- an Archive copy of any age is the file, and a refused file keeps its held row --
    so each is a CI warning, read where the run is read; the published freshness check budgets the Senate's
    newest ``vote_day`` (``scripts/check_rollup_freshness.py``).
    """
    messages: list[str] = []
    files: list[ArchiveServe] = []
    for serve in archived:
        if serve.menu:
            messages.append(f"Senate vote menu {serve.url} read from an Internet Archive capture of "
                            f"{serve.archived_at:%Y-%m-%d %H:%M} UTC, {serve.age_seconds // 86400} day(s) old")  # fmt: skip
        else:
            files.append(serve)
    if files:
        ages = sorted({serve.age_seconds // 86400 for serve in files})
        span = f"{ages[0]}" if len(ages) == 1 else f"{ages[0]}-{ages[-1]}"
        messages.append(f"{len(files)} Senate vote file(s) read from Internet Archive captures {span} day(s) old")
    if refused_files:
        messages.append(f"{refused_files} Senate vote file(s) refused; their held rows stay as published")
    return messages


__all__ = [
    "ARCHIVE",
    "ARCHIVE_INTERVAL_SECONDS",
    "ArchiveCapture",
    "ArchiveServe",
    "LadderVoteAcquirer",
    "newest_captures",
    "senate_warnings",
]
