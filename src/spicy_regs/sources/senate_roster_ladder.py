"""Today's Senate committee file through spicy-docs' walled-fetch ladder, when a run opts in (ss-ehci).

www.senate.gov answers ``cvc_member_data.xml`` with HTTP 403 to this Mac and to GitHub's runners (measured
2026-11-01 and 2026-11-08). Owner decision 2026-11-08: read it through the paid ladder, Zyte first and Firecrawl as
the fallback. :func:`spicy_docs.sources.walled_fetch.walled_fetch` is that ladder: one bounded direct attempt, then
one Zyte ``httpResponseBody`` call, then one Firecrawl ``rawBase64`` call, never a rendered page, never more than
:data:`MAX_REQUESTS` requests or :data:`MAX_BYTES` bytes.

The answer must be the publisher's own XML: a body that is not the file (an HTML page, a rendered DOM) is refused
before the reader sees it, and the reader still validates it exactly as it validates a direct capture. The capture
is retained like any source with the rung that served it in its stage (``senate-cvc:<rung>``) and journaled
(``senate-roster-rung``). When every rung fails the run stops, as a direct refusal always has.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from spicy_docs.sources.congress.committee_rosters import (
    SENATE_CVC_URL,
    CommitteeRosterRefusedError,
    SenateCvc,
    parse_senate_cvc,
)
from spicy_docs.sources.walled_fetch import ProxyFetchers, WalledFetchError, walled_fetch
from spicy_docs.transport.captured import CapturedBodyResponse, observed_instant
from spicy_docs.transport.credentials import scrub_credential
from spicy_docs.transport.source_acquirer import utc_now

#: Direct, Zyte, Firecrawl: one attempt each, no rendered-browser rung.
MAX_REQUESTS = 3
#: The file was 66,846 bytes on 2024-09-11 and 68 KB on 2026-09-19; the bound leaves room without being open-ended.
MAX_BYTES = 4 * 1024 * 1024
TIMEOUT_SECONDS = 90.0
USER_AGENT = "spicy-docs-committee-rosters/1.0"


@dataclass(frozen=True, slots=True)
class LadderAcquisition:
    """The shape the committee-rosters transform reads from an acquisition, plus the rung that served it."""

    roster: SenateCvc
    capture: CapturedBodyResponse
    rung: str


def _is_cvc_xml(body: bytes) -> bool:
    """Whether the bytes are the Senate's XML file rather than a page about it."""
    head = body.lstrip()[:512].lower()
    return (head.startswith(b"<?xml") or head.startswith(b"<senators")) and b"<html" not in head


def acquire_senate_via_ladder(
    evidence: Any = None, *, proxies: ProxyFetchers | None = None, direct: Any = None
) -> LadderAcquisition:
    """``cvc_member_data.xml`` through the ladder, refused unless it is the publisher's XML; retained when evidenced."""
    try:
        result = walled_fetch(
            SENATE_CVC_URL,
            max_bytes=MAX_BYTES,
            timeout_seconds=TIMEOUT_SECONDS,
            user_agent=USER_AGENT,
            allow_browser=False,
            max_requests=MAX_REQUESTS,
            publisher_page=_is_cvc_xml,
            direct=direct,
            proxies=proxies,
        )
    except WalledFetchError as error:
        outcomes = "; ".join(f"{o.transport.value}: {o.kind}" for o in error.rung_outcomes)
        raise CommitteeRosterRefusedError(SENATE_CVC_URL) from WalledFetchError(
            scrub_credential(f"{error} ({outcomes})")
        )
    rung = result.transport.value
    if not (200 <= result.status_code < 300) or not _is_cvc_xml(result.body):
        raise CommitteeRosterRefusedError(SENATE_CVC_URL) from ValueError(
            f"the {rung} rung answered HTTP {result.status_code} with something other than the Senate's XML file"
        )
    capture = CapturedBodyResponse(
        requested_url=SENATE_CVC_URL,
        resolved_url=result.final_url,
        status_code=result.status_code,
        content_type=result.content_type,
        observed_at=observed_instant(utc_now),
        body=result.body,
        response_headers=(("x-spicy-acquisition-rung", rung),)
        + ((("x-spicy-provider-request-id", result.request_id),) if result.request_id else ()),
    )
    roster = parse_senate_cvc(capture.body)
    if evidence is not None:
        evidence.capture(capture, stage=f"senate-cvc:{rung}")
        evidence.event("senate-roster-rung", rung=rung, provider_request_id=result.request_id,
                       sha256=capture.sha256, bytes=capture.byte_size)  # fmt: skip
    return LadderAcquisition(roster, capture, rung)
