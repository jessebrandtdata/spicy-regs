"""Senate roll-call files through the walled-fetch ladder and the Internet Archive (ss-bruk): fakes, no network or keys."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from spicy_docs.sources.congress.votes import VoteBudget, VoteLocator, VoteRefusedError, VoteSourceError
from spicy_docs.sources.walled_fetch import ProxyFetchers

from spicy_regs.sources import senate_vote_ladder
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
        self.events: list[tuple[str, dict]] = []

    def capture(self, capture, *, stage, **fields):
        self.captures.append((stage, capture, fields))

    def event(self, name, **fields):
        self.events.append((name, fields))


CDX_HEADER = ["timestamp", "original", "digest"]


def refusing(request: httpx.Request) -> httpx.Response:
    """The publishers as measured 2026-11-08: the Clerk answers, the Senate refuses direct clients; no archive capture."""
    if request.url.host == "clerk.house.gov":
        return httpx.Response(200, headers={"content-type": "text/xml"}, stream=httpx.ByteStream(CLERK))
    if request.url.host == "web.archive.org":
        return httpx.Response(200, headers={"content-type": "application/json"},
                              stream=httpx.ByteStream(json.dumps([CDX_HEADER]).encode()))  # fmt: skip
    return httpx.Response(403, headers={"content-type": "text/html"}, stream=httpx.ByteStream(b"Forbidden"))


def digest(body: bytes) -> str:
    return base64.b32encode(hashlib.sha1(body).digest()).decode()


class Archive:
    """senate.gov refuses; web.archive.org answers CDX rows and ``id_`` replays as measured 2026-11-09."""

    def __init__(self, replays: dict[str, tuple[bytes, dict[str, str]]], rows: list[list[str]]) -> None:
        self.replays, self.rows, self.requests = replays, rows, []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if request.url.host != "web.archive.org":
            return refusing(request)
        if request.url.path == "/cdx/search/cdx":
            body = json.dumps([CDX_HEADER, *self.rows]).encode()
            return httpx.Response(200, headers={"content-type": "application/json"}, stream=httpx.ByteStream(body))
        body, headers = self.replays[str(request.url)]
        return httpx.Response(200, headers=headers, stream=httpx.ByteStream(body))


def replay(timestamp: str, original: str) -> str:
    return f"https://web.archive.org/web/{timestamp}id_/{original}"


@pytest.fixture(autouse=True)
def unpaced(monkeypatch):
    monkeypatch.setattr(senate_vote_ladder, "ARCHIVE_INTERVAL_SECONDS", 0.0)


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


def no_proxies(publisher, evidence=None) -> LadderVoteAcquirer:
    return acquirer(publisher, "ZYTE_TOKEN is not set", "FIRECRAWL_API_KEY is not set", evidence)


def test_the_archive_serves_the_newest_capture_when_every_rung_refuses_and_says_so():
    original = SENATE_1.url()
    gz = gzip.compress(VOTE, mtime=0)
    old, new = "20260610042145", "20260719010444"
    archive = Archive(
        {replay(new, original): (gz, {"content-type": "text/xml", "content-encoding": "gzip"})},
        [[old, original.replace("https://", "http://"), digest(VOTE)], [new, original, digest(gz)]],
    )
    evidence = Evidence()
    source = no_proxies(archive, evidence)
    got = source.acquire(SENATE_1)
    assert got.vote.roll_number == 1 and len(got.vote.member_votes) == 99
    assert got.capture.requested_url == replay(new, original) and got.capture.body == gz, "retained as served"
    assert source.rungs == {"internet_archive": 1}
    [(name, fields)] = evidence.events
    assert name == "senate-archive-capture"
    assert fields["route"] == "internet_archive" and fields["original_url"] == original
    assert fields["archive_url"] == replay(new, original) and fields["archived_at"] == "2026-07-19T01:04:44+00:00"
    assert fields["cdx_digest"] == digest(gz) and fields["fetched_at"] == got.capture.observed_at
    assert isinstance(fields["age_seconds"], int) and fields["age_seconds"] > 0, "the journal admits no floats"
    assert all(not isinstance(value, float) for value in fields.values())


def test_one_cdx_read_serves_a_whole_session_directory():
    second = VoteLocator("senate", 119, 1, 2)
    ts = "20260719010444"
    archive = Archive(
        {replay(ts, SENATE_1.url()): (VOTE, {"content-type": "text/xml"}),
         replay(ts, second.url()): (VOTE, {"content-type": "text/xml"})},
        [[ts, SENATE_1.url(), digest(VOTE)], [ts, second.url(), digest(VOTE)]],
    )  # fmt: skip
    source = no_proxies(archive)
    source.acquire(SENATE_1)
    with pytest.raises(VoteSourceError):
        source.acquire(second), "the reader still proves the roll number: this file is vote 1"
    assert sum("/cdx/" in url for url in archive.requests) == 1


def test_a_rewritten_or_wrong_archive_answer_is_refused():
    """A rewritten replay page, or bytes that are not the payload the CDX row hashed, never reach the reader."""
    ts, original = "20260719010444", SENATE_1.url()
    page = b"<!DOCTYPE html><html><body><div id='wm-ipp'>Wayback Machine</div></body></html>"
    for body, content_type, stated in ((page, "text/html", digest(page)), (page, "text/xml", digest(VOTE)),
                                       (VOTE + b" ", "text/xml", digest(VOTE))):  # fmt: skip
        archive = Archive({replay(ts, original): (body, {"content-type": content_type})}, [[ts, original, stated]])
        source = no_proxies(archive)
        with pytest.raises(VoteSourceError):
            source.acquire(SENATE_1)
        assert not source.rungs


def test_no_archive_capture_keeps_the_ladder_refusal():
    source = no_proxies(refusing)
    with pytest.raises(VoteRefusedError) as refused:
        source.acquire(SENATE_1)
    assert [o.transport.value for o in refused.value.__dict__["rung_outcomes"]] == ["direct", "zyte_http", "firecrawl_raw"]


def test_an_unreachable_archive_is_the_file_s_refusal_not_a_crash(monkeypatch):
    import spicy_docs.transport.retry as retry

    monkeypatch.setattr(retry.time, "sleep", lambda _seconds: None)

    def down(request: httpx.Request) -> httpx.Response:
        if request.url.host == "web.archive.org":
            raise httpx.ConnectError("archive down")
        return refusing(request)

    with pytest.raises(VoteRefusedError):
        no_proxies(down).list_senate_votes(119, 1)


def test_a_menu_reads_its_own_capture_exactly():
    ts, original = "20261106060122", "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_119_1.xml"
    archive = Archive({replay(ts, original): (MENU, {"content-type": "text/xml"})}, [[ts, original, digest(MENU)]])
    menu = no_proxies(archive).list_senate_votes(119, 1)
    assert menu.menu.votes[0].vote_number == 659
    [cdx] = [url for url in archive.requests if "/cdx/" in url]
    assert "matchType" not in cdx and "vote_menu_119_1.xml" in cdx
