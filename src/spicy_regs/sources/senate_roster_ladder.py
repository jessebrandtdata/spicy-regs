"""Today's Senate committee file through the shared walled-fetch ladder, when a run opts in (ss-ehci).

www.senate.gov answers ``cvc_member_data.xml`` with HTTP 403 to this Mac and to GitHub's runners (measured
2026-11-01 and 2026-11-08). Owner decision 2026-11-08: read it through the paid ladder, Zyte first and Firecrawl as
the fallback.

:class:`LadderCommitteeRosterAcquirer` is the chamber-file acquirer with one change: today's Senate file goes
through ``SourceAcquirer.capture_walled``, spicy-docs' ladder (one direct attempt on this acquirer's own client,
then one Zyte ``httpResponseBody`` call, then one Firecrawl ``rawBase64`` call, then the opt-in Zyte session rung:
Zyte's browser opens ``https://www.senate.gov/`` and fetches the file same-origin, returning the captured response
bytes; never a rendered page). The session rung is the one that served the file when measured on 2026-11-09; the
others answered 403, 520 and 500. It is the fourth request, so the budget must allow four (``ROSTER_BUDGET`` does)
or the ladder refuses before any paid call. Every rung is
charged and paced on the same ``CommitteeRosterBudget`` as a direct capture, and each answer is held to the budget's
byte bound, the exact locator (``check_final_url``), the file's media types (``text/xml``/``application/xml``) and
the reader's own validation. A 2xx error or interstitial page (spicy-docs' one classifier, plus
:func:`is_senate_xml`) escalates to the next rung; the file under a wrong media type or locator stops the ladder. The timeout is the budget's, per rung: the worst case is four timeouts in a row
(4 x 120 s with the rollup's ``ROSTER_BUDGET``). When every rung fails, ``CommitteeRosterRefusedError`` carries the
ladder's ``rung_outcomes`` and the run stops, as a direct refusal always has. The House file, and any archived
capture (``archived_at``), are read exactly as before.

The serving rung is retained: the capture is kept with stage ``senate-cvc:<rung>`` and journaled as
``senate-roster-rung`` with the provider's request id.
"""

from __future__ import annotations

from typing import Any

from spicy_docs.sources.congress.committee_rosters import (
    MEDIA_TYPES,
    SENATE_CVC_URL,
    CommitteeRosterAcquirer,
    CommitteeRosterRefusedError,
    CommitteeRosterUnavailableError,
    SenateRosterAcquisition,
    parse_senate_cvc,
)
from spicy_docs.reading.error_pages import sniff
from spicy_docs.transport.source_acquirer import narrow_byte_limit


def is_senate_xml(body: bytes) -> bool:
    """Whether a 2xx body is an XML file, not an (X)HTML page about it: the ``publisher_page`` of every senate.gov
    file read through the ladder, this committee file and the roll-call files (``senate_vote_ladder``) alike.

    spicy-docs' :func:`sniff` looks past a byte-order mark, an XML declaration, comments and a DOCTYPE to the root.
    """
    return sniff(body[:512]) == "xml"


class LadderCommitteeRosterAcquirer(CommitteeRosterAcquirer):
    """The chamber-file acquirer, reading today's Senate file through the walled-fetch ladder."""

    def __init__(self, *args: Any, evidence: Any = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.evidence = evidence

    def acquire_senate(
        self, *, max_bytes: int | None = None, archived_at: str | None = None
    ) -> SenateRosterAcquisition:
        if archived_at is not None:
            return super().acquire_senate(max_bytes=max_bytes, archived_at=archived_at)

        def parse(response, _limit):
            if not is_senate_xml(response.body):
                raise CommitteeRosterRefusedError(SENATE_CVC_URL) from ValueError(
                    "the ladder's answer is not the Senate's XML file"
                )
            return parse_senate_cvc(response.body)

        roster, capture, answer = self.capture_walled(
            SENATE_CVC_URL,
            media_types=MEDIA_TYPES,
            parse=parse,
            max_bytes=narrow_byte_limit(self.budget.max_bytes, max_bytes),
            unavailable=CommitteeRosterUnavailableError,
            context={"operation": "senate-cvc", "url": SENATE_CVC_URL},
            publisher_page=is_senate_xml,
            refusal=CommitteeRosterRefusedError,
            session_rung=True,
        )
        rung = answer.transport.value
        if self.evidence is not None:
            self.evidence.capture(capture, stage=f"senate-cvc:{rung}")
            self.evidence.event("senate-roster-rung", rung=rung, provider_request_id=answer.request_id,
                                sha256=capture.sha256, bytes=capture.byte_size)  # fmt: skip
        return SenateRosterAcquisition(roster, capture, self.request_count, self.budget)
