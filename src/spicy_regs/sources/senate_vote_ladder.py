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
budget's byte bound, the exact locator, the file's XML media types and the reader's own validation, which proves the
file's congress, session and roll number. House reads are unchanged.

**The Internet Archive is the last rung**, read only when every ladder rung refused (``VoteRefusedError``; a
publisher 404 is absence and never reaches it). One CDX query per file family -- the menu's own URL, or a session's
whole ``vote<congress><session>/`` directory, read once per acquirer -- names the newest 200 ``text/xml`` capture of
each file. Its ``id_`` replay (the archived bytes, not the rewritten page) is read on this acquirer's own client and
user agent, paced at :data:`ARCHIVE_INTERVAL_SECONDS`, and must hash (SHA-1, as served) to the digest the CDX row
states, pass the same XML media-type check and the same reader. Measured 2026-11-09: the replay of
``vote_119_1_00001.xml`` is byte-identical to the direct capture of 2026-09-19 (SHA-256 ``9d71d78a...``). A file the
Archive holds no capture of keeps the ladder's refusal.

Retention and provenance: a direct or archive answer is retained by the evidence transport as served (the archive's
gzip transfer wrapper included). A proxy answer is retained here as a ``vote-source`` capture naming the serving
``transport`` and the provider's request id. An archive answer is journaled as ``senate-archive-capture``:
route ``internet_archive``, the senate.gov URL, the replay URL, the capture time and its age at the fetch in whole seconds (the journal admits no floats), and the
fetch time, so the run's acquisition receipts say which rows rest on an archive capture of which date.
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
import zlib
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlencode, urlsplit

import httpx

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
from spicy_docs.transport.source_acquirer import named_challenge

if TYPE_CHECKING:
    from spicy_docs.transport.captured import CapturedBodyResponse

    from spicy_regs.source_evidence import CaptureEvidence

#: The route an archive answer is recorded under, beside the ladder's own transports.
ARCHIVE: Final = "internet_archive"
CDX_URL: Final = "https://web.archive.org/cdx/search/cdx"
#: The least time between two requests to web.archive.org; the votes budget's own pacing is twice as fast.
ARCHIVE_INTERVAL_SECONDS: Final = 1.0


@dataclass(frozen=True, slots=True)
class ArchiveCapture:
    """One CDX row: the capture's 14-digit time, the URL as archived, and the archive's SHA-1 (base32) of its payload."""

    timestamp: str
    original: str
    digest: str

    @property
    def replay_url(self) -> str:
        return f"https://web.archive.org/web/{self.timestamp}id_/{self.original}"

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
        capture = ArchiveCapture(*row)
        key = _url_key(capture.original)
        if key not in newest or capture.timestamp > newest[key].timestamp:
            newest[key] = capture
    return newest


def _archived_body(response: CapturedBodyResponse, capture: ArchiveCapture, limit: int) -> bytes:
    """The archived file: the served bytes must be the payload the CDX row hashed, then the gzip wrapper is removed."""
    served = base64.b32encode(hashlib.sha1(response.body).digest()).decode()  # noqa: S324 -- the archive's own digest
    if served != capture.digest:
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


class LadderVoteAcquirer(VoteAcquirer):
    """The roll-call acquirer, reading Senate files through the walled-fetch ladder; ``rungs`` counts what served."""

    def __init__(self, *, evidence: CaptureEvidence | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.evidence = evidence
        self.rungs: Counter[str] = Counter()
        self._archive_indexes: dict[str, dict[str, ArchiveCapture]] = {}
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
                refusal=VoteRefusedError,
            )
        except VoteRefusedError:
            try:
                with named_challenge(url, error_type=VoteRefusedError, context_key=self.context_key):
                    archived = self._archived(url, read, operation=operation)
            except (httpx.HTTPError, ConnectionError, TimeoutError) as error:
                # The archive is a fallback: its outage is this file's refusal, never a crashed run.
                raise VoteRefusedError(url) from error
            if archived is None:
                raise
            return archived
        rung = answer.transport.value
        self.rungs[rung] += 1
        if self.evidence is not None and answer.transport is not Transport.DIRECT:
            self.evidence.capture(capture, stage="vote-source", transport=rung, provider_request_id=answer.request_id)
        return result, capture

    def _archive_request[Result](self, url: str, **kwargs: Any) -> tuple[Result, CapturedBodyResponse]:
        wait = self._archive_last + ARCHIVE_INTERVAL_SECONDS - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            return self.capture_validated(url, max_bytes=self.budget.max_bytes, unavailable=VoteUnavailableError, **kwargs)
        finally:
            self._archive_last = time.monotonic()

    def _archive_index(self, url: str) -> dict[str, ArchiveCapture]:
        """The newest captures of ``url`` (a menu) or of its session directory (a vote file), one CDX read each."""
        menu = "/roll_call_lists/" in url
        scope = url if menu else url.rsplit("/", 1)[0] + "/"
        if scope not in self._archive_indexes:
            query = [("url", scope.split("://", 1)[1]), ("output", "json"), ("fl", "timestamp,original,digest"),
                     ("filter", "statuscode:200"), ("filter", "mimetype:text/xml")]  # fmt: skip
            if not menu:
                query.append(("matchType", "prefix"))
            index_url = f"{CDX_URL}?{urlencode(query)}"
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
        archived = self._archive_index(url).get(_url_key(url))
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
        if self.evidence is not None:
            fetched = self._clock()
            self.evidence.event(
                "senate-archive-capture", route=ARCHIVE, original_url=url, archive_url=archived.replay_url,
                archived_at=archived.captured_at.isoformat(), age_seconds=int((fetched - archived.captured_at).total_seconds()),
                fetched_at=capture.observed_at, sha256=capture.sha256, cdx_digest=archived.digest,
                content_encoding=capture.content_encoding,
            )  # fmt: skip
        return result, capture


__all__ = ["ARCHIVE", "ARCHIVE_INTERVAL_SECONDS", "ArchiveCapture", "LadderVoteAcquirer", "newest_captures"]
