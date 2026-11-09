"""Senate roll-call files through the shared walled-fetch ladder when senate.gov refuses a direct read (ss-bruk).

www.senate.gov answers the LIS vote menu (``vote_menu_119_1.xml``) with HTTP 403 to GitHub's runners and to this
Mac (measured 2026-11-08), so the votes rollup published only the House half. Owner decision 2026-11-09: read the
Senate through Zyte, with Firecrawl only as a hedge. Measured the same day, neither reaches it yet: Zyte answers
HTTP 520 "Website Ban" (plain, US-geolocated and browser requests; bans are unbilled) and Firecrawl's engines all fail
(stealth proxy, US); Zyte's residential IPs need account KYC. Until one does, the ladder ends in the chamber refusal
the run always reported, about 2.4 s later.

:class:`LadderVoteAcquirer` is spicy-docs' ``VoteAcquirer`` with one change: the Senate menu and each Senate vote
file go through ``SourceAcquirer.capture_walled``, spicy-docs' ladder. It tries one direct attempt on this
acquirer's own client first, so a free answer is always taken and a proxy is paid only after a refusal; then one
Zyte ``httpResponseBody`` call (the publisher's bytes, never a rendered page), then one Firecrawl ``rawBase64`` call
when ``FIRECRAWL_API_KEY`` is set. A provider without a credential makes no request. Every answer is held to the
budget's byte bound, the exact locator, the file's XML media types and the reader's own validation, which proves the
file's congress, session and roll number. When every rung fails, ``VoteRefusedError`` carries each rung's outcome, so
the caller refuses the chamber or counts the vote exactly as it did a direct refusal. House reads are unchanged.

Retention: a direct answer is already retained by the evidence transport. A proxy answer is retained here, exactly
as received, as a ``vote-source`` capture naming the serving ``transport`` and the provider's request id, beside the
requested URL and the time it was observed. A proxy answer the reader then refuses is retained by the caller's
refusal path with the error.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from spicy_docs.sources.congress.votes import (
    MEDIA_TYPES,
    SenateVoteMenuAcquisition,
    VoteAcquirer,
    VoteAcquisition,
    VoteLocator,
    VoteRefusedError,
    VoteUnavailableError,
    parse_senate_vote,
    parse_senate_vote_menu,
    senate_vote_menu_url,
)
from spicy_docs.sources.walled_fetch import Transport

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence


class LadderVoteAcquirer(VoteAcquirer):
    """The roll-call acquirer, reading Senate files through the walled-fetch ladder; ``rungs`` counts what served."""

    def __init__(self, *, evidence: CaptureEvidence | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.evidence = evidence
        self.rungs: Counter[str] = Counter()

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
        result, capture, answer = self.capture_walled(
            url,
            media_types=MEDIA_TYPES,
            parse=lambda response, _allowance: read(response.body),
            max_bytes=self.budget.max_bytes,
            unavailable=VoteUnavailableError,
            context={"operation": operation, "chamber": "senate", "url": url},
            refusal=VoteRefusedError,
        )
        rung = answer.transport.value
        self.rungs[rung] += 1
        if self.evidence is not None and answer.transport is not Transport.DIRECT:
            self.evidence.capture(capture, stage="vote-source", transport=rung, provider_request_id=answer.request_id)
        return result, capture


__all__ = ["LadderVoteAcquirer"]
