"""Today's Senate committee file through the shared walled-fetch ladder (ss-ehci): fake rungs, no network or keys."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.congress.committee_rosters import (
    HOUSE_MEMBER_DATA_URL,
    CommitteeRosterBudget,
    CommitteeRosterError,
    CommitteeRosterRefusedError,
)
from spicy_docs.sources.firecrawl import FirecrawlTransportError
from spicy_docs.sources.walled_fetch import ProxyFetchers
from spicy_docs.sources.zyte import ZyteTransportError

from spicy_regs.sources.senate_roster_ladder import LadderCommitteeRosterAcquirer, is_senate_xml
from spicy_regs.transforms import build_committee_rosters as bcr
from tests.test_committee_rosters import StubListingReader
from tests.test_incremental_rollups import no_download

FIXTURES = Path(__file__).parent / "fixtures" / "congress_rosters"
XML = (FIXTURES / "cvc-member-data-excerpt.xml").read_bytes()
HOUSE_XML = (FIXTURES / "memberdata-119-excerpt.xml").read_bytes()
HTML = b"<!DOCTYPE html><html><head><title>U.S. Senate</title></head><body>Committee membership</body></html>"
BUDGET = CommitteeRosterBudget(max_requests=4, max_bytes=4 << 20, timeout_seconds=30, min_request_interval_seconds=0)


class Fetcher:
    """One provider rung: answers ``body`` (or raises ``error``) and counts its calls."""

    def __init__(self, body=None, error=None, content_type="text/xml", resolved_url=None, session=None):
        self.body, self.error, self.content_type, self.resolved_url, self.calls = (
            body, error, content_type, resolved_url, 0,
        )  # fmt: skip
        self.session = session  # Zyte's browser-session answer; None fails as the provider has (HTTP 520)

    def fetch_in_session(self, url, *, session_url, timeout_seconds, max_bytes):
        assert session_url == "https://www.senate.gov/"  # the origin root, then the file fetched same-origin
        if self.session is None:
            raise ZyteTransportError("Zyte acquisition failed with HTTP 520 (/download/temporary-error)")
        return self.session.fetch(url, timeout_seconds=timeout_seconds, max_bytes=max_bytes)

    def fetch(self, url, *, timeout_seconds, max_bytes, mode=None):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return SimpleNamespace(resolved_url=self.resolved_url or url, status_code=200,
                               content_type=self.content_type, body=self.body, request_id="req-1")  # fmt: skip


class Evidence:
    def __init__(self):
        self.captures, self.events = [], []

    def capture(self, capture, *, stage):
        self.captures.append((stage, capture))

    def event(self, name, **fields):
        self.events.append((name, fields))


def publisher(request: httpx.Request) -> httpx.Response:
    """The publishers as measured: the Clerk answers, the Senate refuses direct clients."""
    if str(request.url) == HOUSE_MEMBER_DATA_URL:
        return httpx.Response(200, headers={"content-type": "text/xml"}, stream=httpx.ByteStream(HOUSE_XML))
    return httpx.Response(403, headers={"content-type": "text/html"}, stream=httpx.ByteStream(b"Forbidden"))


def acquirer(zyte, firecrawl, evidence=None, budget=BUDGET) -> LadderCommitteeRosterAcquirer:
    source = LadderCommitteeRosterAcquirer(budget=budget, transport=httpx.MockTransport(publisher), evidence=evidence)
    source._proxies = ProxyFetchers(cast(Any, zyte), cast(Any, firecrawl))
    return source


def test_zyte_serves_the_file_and_the_rung_is_recorded():
    zyte, firecrawl, evidence = Fetcher(XML), Fetcher(XML), Evidence()
    got = acquirer(zyte, firecrawl, evidence).acquire_senate()
    assert got.roster.senators and got.capture.body == XML and (zyte.calls, firecrawl.calls) == (1, 0)
    assert [stage for stage, _ in evidence.captures] == ["senate-cvc:zyte_http"]
    assert evidence.events == [("senate-roster-rung", {"rung": "zyte_http", "provider_request_id": "req-1",
                                                       "sha256": got.capture.sha256, "bytes": len(XML)})]  # fmt: skip


def test_firecrawl_serves_when_zyte_fails():
    zyte, firecrawl, evidence = Fetcher(error=ZyteTransportError("Zyte acquisition failed")), Fetcher(XML), Evidence()
    acquirer(zyte, firecrawl, evidence).acquire_senate()
    assert (zyte.calls, firecrawl.calls) == (1, 1)
    assert [stage for stage, _ in evidence.captures] == ["senate-cvc:firecrawl_raw"]


def test_every_rung_failing_stops_the_run_with_each_rungs_outcome():
    zyte = Fetcher(error=ZyteTransportError("Zyte acquisition failed"))
    firecrawl = Fetcher(error=FirecrawlTransportError("Firecrawl acquisition failed with HTTP 500"))
    with pytest.raises(CommitteeRosterRefusedError) as refused:
        acquirer(zyte, firecrawl).acquire_senate()
    outcomes = refused.value.__dict__["rung_outcomes"]
    assert [(o.transport.value, o.kind) for o in outcomes] == [
        ("direct", "publisher-refused"), ("zyte_http", "transport-error"), ("firecrawl_raw", "transport-error"),
        ("zyte_session", "transport-error"),
    ]  # fmt: skip


def test_the_session_rung_serves_when_the_other_three_fail():
    """As measured 2026-11-09: direct 403, Zyte HTTP 520, Firecrawl 500; Zyte's same-origin session served the file."""
    session = Fetcher(XML)
    zyte = Fetcher(error=ZyteTransportError("Zyte acquisition failed with HTTP 520"), session=session)
    firecrawl, evidence = (
        Fetcher(error=FirecrawlTransportError("Firecrawl acquisition failed with HTTP 500")),
        Evidence(),
    )
    got = acquirer(zyte, firecrawl, evidence).acquire_senate()
    assert got.capture.body == XML and (zyte.calls, firecrawl.calls, session.calls) == (1, 1, 1)
    assert [stage for stage, _ in evidence.captures] == ["senate-cvc:zyte_session"]
    assert evidence.events[0][1]["rung"] == "zyte_session"


def test_a_budget_short_of_the_session_rung_is_refused_before_any_paid_call():
    session = Fetcher(XML)
    zyte, firecrawl = Fetcher(XML, session=session), Fetcher(XML)
    short = CommitteeRosterBudget(max_requests=3, max_bytes=4 << 20, timeout_seconds=30, min_request_interval_seconds=0)
    source = acquirer(zyte, firecrawl, budget=short)
    with pytest.raises(CommitteeRosterRefusedError) as refused:
        source.acquire_senate()
    assert "stops before the session rung, which is attempt 4" in str(refused.value.__cause__)
    assert source.request_count == 0
    assert (zyte.calls, firecrawl.calls, session.calls) == (0, 0, 0)
    assert bcr.ROSTER_BUDGET.max_requests >= 4


@pytest.mark.parametrize("content_type", ["text/html", "text/xml"])
def test_an_html_page_escalates_to_the_next_rung(content_type):
    """A 2xx page about the file, however labeled, is an error page: the ladder asks Firecrawl (spicy-docs 0.80.0)."""
    zyte, firecrawl, evidence = Fetcher(HTML, content_type=content_type), Fetcher(XML), Evidence()
    got = acquirer(zyte, firecrawl, evidence).acquire_senate()
    assert got.capture.body == XML and (zyte.calls, firecrawl.calls) == (1, 1)
    assert [stage for stage, _ in evidence.captures] == ["senate-cvc:firecrawl_raw"]


def test_error_pages_on_every_rung_stop_the_run_unread():
    evidence = Evidence()
    zyte, firecrawl = Fetcher(HTML, content_type="text/html"), Fetcher(HTML, content_type="text/xml")
    with pytest.raises(CommitteeRosterRefusedError) as refused:
        acquirer(zyte, firecrawl, evidence).acquire_senate()
    assert [(o.transport.value, o.kind) for o in refused.value.__dict__["rung_outcomes"]] == [
        ("direct", "publisher-refused"), ("zyte_http", "error-page"), ("firecrawl_raw", "error-page"),
        ("zyte_session", "transport-error"),
    ]  # fmt: skip
    assert evidence.captures == []


@pytest.mark.parametrize(("content_type", "resolved_url", "reason"), [
    ("text/html", None, "Content-Type"),  # the file itself, under another media type
    ("text/xml", "https://www.senate.gov/legislative/LIS_MEMBER/other.xml", "final URL"),
])  # fmt: skip
def test_the_file_with_a_wrong_label_stops_the_ladder_unread(content_type, resolved_url, reason):
    """Not an error page, so no escalation: the capture's own checks refuse it and Firecrawl is never asked."""
    zyte, firecrawl, evidence = (
        Fetcher(XML, content_type=content_type, resolved_url=resolved_url),
        Fetcher(XML),
        Evidence(),
    )
    with pytest.raises(CommitteeRosterError, match=reason):
        acquirer(zyte, firecrawl, evidence).acquire_senate()
    assert firecrawl.calls == 0 and evidence.captures == []


def test_a_byte_order_mark_before_the_xml_is_the_file():
    assert is_senate_xml(b"\xef\xbb\xbf" + XML) and not is_senate_xml(b"\xef\xbb\xbf" + HTML)
    got = acquirer(Fetcher(b"\xef\xbb\xbf" + XML), Fetcher(XML)).acquire_senate()
    assert got.roster.senators


@pytest.fixture(autouse=True)
def scoped(monkeypatch):
    monkeypatch.setattr(bcr, "current_congress", lambda: 119)


def test_the_ladder_acquirer_changes_only_the_senate_request(tmp_path):
    _, assignments, _ = bcr.build_committee_rosters(
        tmp_path, reader=StubListingReader(), rosters=acquirer(Fetcher(XML), Fetcher(XML)), download_prior=no_download
    )
    assert {row["chamber"] for row in pq.read_table(assignments).to_pylist()} == {"house", "senate"}


def test_the_run_still_stops_when_every_rung_fails(tmp_path):
    zyte = Fetcher(error=ZyteTransportError("Zyte acquisition failed"))
    firecrawl = Fetcher(error=FirecrawlTransportError("Firecrawl acquisition failed with HTTP 500"))
    with pytest.raises(CommitteeRosterRefusedError):
        bcr.build_committee_rosters(
            tmp_path, reader=StubListingReader(), rosters=acquirer(zyte, firecrawl), download_prior=no_download
        )


def test_the_opt_in_builds_the_ladder_acquirer(monkeypatch, tmp_path):
    built = {}

    class Recording(LadderCommitteeRosterAcquirer):
        def __init__(self, **kwargs):
            built.update(kwargs)
            super().__init__(**kwargs, transport=httpx.MockTransport(publisher))
            self._proxies = ProxyFetchers(cast(Any, Fetcher(XML)), cast(Any, Fetcher(XML)))

    monkeypatch.setattr("spicy_regs.sources.senate_roster_ladder.LadderCommitteeRosterAcquirer", Recording)
    bcr.build_committee_rosters(tmp_path, reader=StubListingReader(), download_prior=no_download,
                                senate_paid_ladder=True)  # fmt: skip
    assert built["budget"] is bcr.ROSTER_BUDGET and "evidence" in built


def test_the_hosted_rollup_passes_the_ladder_flag_and_its_keys_to_committee_rosters_only():
    shared = (Path(__file__).parents[1] / ".github/workflows/_rollup.yml").read_text()
    assert (
        "if [ \"$COMMAND\" = 'run-rollup-committee-rosters' ]; then\n            ARGS+=(--senate-paid-ladder)" in shared
    )
    assert "inputs.command == 'run-rollup-committee-rosters' && secrets.FIRECRAWL_API_KEY || ''" in shared
    zyte = next(line for line in shared.splitlines() if line.strip().startswith("ZYTE_TOKEN:"))
    assert "inputs.command == 'run-rollup-committee-rosters'" in zyte
