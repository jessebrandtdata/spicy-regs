"""Senate roll-call files through the shared walled-fetch ladder (ss-bruk): fake rungs, no network or keys."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from spicy_docs.sources.congress.votes import VoteBudget, VoteLocator, VoteRefusedError, VoteSourceError
from spicy_docs.sources.walled_fetch import ProxyFetchers

from spicy_regs.sources.senate_vote_ladder import LadderVoteAcquirer

FIXTURES = Path(__file__).parent / "fixtures" / "congress_votes"
VOTE = (FIXTURES / "senate-vote-119-1-00001.xml").read_bytes()
MENU = (FIXTURES / "senate-vote-menu-119-1.xml").read_bytes()
CLERK = (FIXTURES / "clerk-roll240.xml").read_bytes()
SENATE_1 = VoteLocator("senate", 119, 1, 1)
BUDGET = VoteBudget(max_requests=4, max_bytes=4 << 20, timeout_seconds=30, min_request_interval_seconds=0)


class Fetcher:
    """One provider rung: answers ``body`` and counts its calls."""

    def __init__(self, body: bytes, content_type: str = "text/xml") -> None:
        self.body, self.content_type, self.calls = body, content_type, 0

    def fetch(self, url, *, timeout_seconds, max_bytes, mode=None):
        self.calls += 1
        return SimpleNamespace(resolved_url=url, status_code=200, content_type=self.content_type, body=self.body,
                               request_id="req-1")  # fmt: skip


class Evidence:
    def __init__(self) -> None:
        self.captures: list[tuple[str, Any, dict]] = []

    def capture(self, capture, *, stage, **fields):
        self.captures.append((stage, capture, fields))


def refusing(request: httpx.Request) -> httpx.Response:
    """The publishers as measured 2026-11-08: the Clerk answers, the Senate refuses direct clients."""
    if request.url.host == "clerk.house.gov":
        return httpx.Response(200, headers={"content-type": "text/xml"}, stream=httpx.ByteStream(CLERK))
    return httpx.Response(403, headers={"content-type": "text/html"}, stream=httpx.ByteStream(b"Forbidden"))


def serving(request: httpx.Request) -> httpx.Response:
    body = MENU if "vote_menu" in str(request.url) else VOTE
    return httpx.Response(200, headers={"content-type": "application/xml"}, stream=httpx.ByteStream(body))


def acquirer(publisher, zyte, firecrawl, evidence=None) -> LadderVoteAcquirer:
    source = LadderVoteAcquirer(budget=BUDGET, transport=httpx.MockTransport(publisher), evidence=evidence)
    source._proxies = ProxyFetchers(cast(Any, zyte), cast(Any, firecrawl))
    return source


def test_a_refused_senate_menu_and_vote_are_read_through_zyte_and_retained_with_their_route():
    zyte, firecrawl, evidence = Fetcher(MENU), Fetcher(MENU), Evidence()
    source = acquirer(refusing, zyte, firecrawl, evidence)
    menu = source.list_senate_votes(119, 1)
    assert menu.menu.votes[0].vote_number == 659 and menu.capture.body == MENU
    zyte.body = VOTE
    vote = source.acquire(SENATE_1)
    assert vote.vote.roll_number == 1 and vote.capture.body == VOTE
    assert (zyte.calls, firecrawl.calls) == (2, 0), "Firecrawl is only a hedge"
    assert source.rungs == {"zyte_http": 2}
    assert [(stage, capture.body, fields) for stage, capture, fields in evidence.captures] == [
        ("vote-source", MENU, {"transport": "zyte_http", "provider_request_id": "req-1"}),
        ("vote-source", VOTE, {"transport": "zyte_http", "provider_request_id": "req-1"}),
    ]
    assert evidence.captures[1][1].requested_url == SENATE_1.url()


def test_a_direct_answer_pays_no_proxy_and_is_left_to_the_evidence_transport():
    zyte, firecrawl, evidence = Fetcher(VOTE), Fetcher(VOTE), Evidence()
    source = acquirer(serving, zyte, firecrawl, evidence)
    assert source.acquire(SENATE_1).vote.roll_number == 1
    assert (zyte.calls, firecrawl.calls, evidence.captures) == (0, 0, [])
    assert source.rungs == {"direct": 1}


def test_firecrawl_serves_when_zyte_has_no_credential():
    firecrawl = Fetcher(VOTE)
    source = acquirer(refusing, "ZYTE_TOKEN is not set", firecrawl)
    assert source.acquire(SENATE_1).capture.body == VOTE
    assert firecrawl.calls == 1 and source.rungs == {"firecrawl_raw": 1}


def test_every_rung_failing_is_a_refusal_naming_each_rung():
    source = acquirer(refusing, "ZYTE_TOKEN is not set", "FIRECRAWL_API_KEY is not set")
    with pytest.raises(VoteRefusedError) as refused:
        source.list_senate_votes(119, 1)
    kinds = [(outcome.transport.value, outcome.kind) for outcome in refused.value.__dict__["rung_outcomes"]]
    assert kinds == [("direct", "publisher-refused"), ("zyte_http", "credential-error"),
                     ("firecrawl_raw", "credential-error")]  # fmt: skip
    assert not source.rungs


def test_a_rendered_page_from_a_proxy_is_refused_before_the_reader_trusts_it():
    """A proxy answering an HTML page (or the wrong file) is not the vote: the reader refuses it; nothing is counted."""
    page = b"<!DOCTYPE html><html><body>Roll Call Vote 119th Congress</body></html>"
    for body, content_type in ((page, "text/html"), (page, "text/xml"), (MENU, "text/xml")):
        zyte, firecrawl = Fetcher(body, content_type), Fetcher(VOTE)
        source = acquirer(refusing, zyte, firecrawl)
        with pytest.raises(VoteSourceError):
            source.acquire(SENATE_1)
        assert firecrawl.calls == 0 and not source.rungs


def test_house_reads_never_reach_the_ladder():
    zyte, firecrawl = Fetcher(VOTE), Fetcher(VOTE)
    source = acquirer(refusing, zyte, firecrawl)
    assert source.acquire(VoteLocator("house", 119, 1, 240)).vote.roll_number == 240
    assert (zyte.calls, firecrawl.calls) == (0, 0) and not source.rungs
