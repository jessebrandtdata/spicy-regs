"""Today's Senate committee file through the walled-fetch ladder (ss-ehci): fake rungs, no network, no credentials."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from spicy_docs.sources.congress.committee_rosters import SENATE_CVC_URL, CommitteeRosterRefusedError
from spicy_docs.sources.firecrawl import FirecrawlTransportError
from spicy_docs.sources.walled_fetch import ProxyFetchers
from spicy_docs.sources.zyte import ZyteTransportError
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.sources.senate_roster_ladder import acquire_senate_via_ladder
from spicy_regs.transforms import build_committee_rosters as bcr
from tests.test_committee_rosters import StubListingReader, StubRosters
from tests.test_incremental_rollups import no_download

XML = (Path(__file__).parent / "fixtures" / "congress_rosters" / "cvc-member-data-excerpt.xml").read_bytes()
HTML = b"<!DOCTYPE html><html><head><title>U.S. Senate</title></head><body>Committee membership</body></html>"


def fakes(zyte, firecrawl) -> ProxyFetchers:
    """Fake provider rungs where the ladder takes its resolved clients."""
    return ProxyFetchers(cast(Any, zyte), cast(Any, firecrawl))


def refused_directly(url, max_bytes):
    return CapturedBodyResponse(url, url, 403, "text/html", "2026-11-08T00:00:00Z", b"Forbidden")


class Fetcher:
    """One provider rung: answers ``body`` (or raises ``error``) and counts its calls."""

    def __init__(self, body=None, error=None, content_type="text/xml"):
        self.body, self.error, self.content_type, self.calls = body, error, content_type, 0

    def fetch(self, url, *, timeout_seconds, max_bytes, mode=None):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return SimpleNamespace(resolved_url=url, status_code=200, content_type=self.content_type, body=self.body,
                               request_id="req-1")  # fmt: skip


class Evidence:
    def __init__(self):
        self.captures, self.events = [], []

    def capture(self, capture, *, stage):
        self.captures.append((stage, capture))

    def event(self, name, **fields):
        self.events.append((name, fields))


def test_zyte_serves_the_file_and_the_rung_is_recorded():
    zyte, firecrawl, evidence = Fetcher(XML), Fetcher(XML), Evidence()
    got = acquire_senate_via_ladder(evidence, proxies=fakes(zyte, firecrawl), direct=refused_directly)
    assert got.rung == "zyte_http" and (zyte.calls, firecrawl.calls) == (1, 0)
    assert got.roster.senators and got.capture.body == XML
    assert [stage for stage, _ in evidence.captures] == ["senate-cvc:zyte_http"]
    assert evidence.events[0][0] == "senate-roster-rung" and evidence.events[0][1]["rung"] == "zyte_http"
    assert ("x-spicy-acquisition-rung", "zyte_http") in got.capture.response_headers


def test_firecrawl_serves_when_zyte_fails():
    zyte, firecrawl = Fetcher(error=ZyteTransportError("Zyte acquisition failed")), Fetcher(XML)
    got = acquire_senate_via_ladder(Evidence(), proxies=fakes(zyte, firecrawl), direct=refused_directly)
    assert got.rung == "firecrawl_raw" and (zyte.calls, firecrawl.calls) == (1, 1)


def test_every_rung_failing_stops_the_run():
    zyte = Fetcher(error=ZyteTransportError("Zyte acquisition failed"))
    firecrawl = Fetcher(error=FirecrawlTransportError("Firecrawl acquisition failed with HTTP 500"))
    with pytest.raises(CommitteeRosterRefusedError):
        acquire_senate_via_ladder(Evidence(), proxies=fakes(zyte, firecrawl), direct=refused_directly)


def test_an_html_page_is_refused_not_read():
    zyte, firecrawl = Fetcher(HTML, content_type="text/html"), Fetcher(XML)
    evidence = Evidence()
    with pytest.raises(CommitteeRosterRefusedError):
        acquire_senate_via_ladder(evidence, proxies=fakes(zyte, firecrawl), direct=refused_directly)
    assert evidence.captures == []


def test_a_missing_credential_is_a_rung_outcome_not_a_request():
    firecrawl = Fetcher(XML)
    got = acquire_senate_via_ladder(None, proxies=fakes("ZYTE_TOKEN is not set", firecrawl),
                                    direct=refused_directly)  # fmt: skip
    assert got.rung == "firecrawl_raw"


@pytest.fixture(autouse=True)
def scoped(monkeypatch):
    monkeypatch.setattr(bcr, "current_congress", lambda: 119)


def _build(tmp_path, ladder):
    return bcr.build_committee_rosters(
        tmp_path, reader=StubListingReader(), rosters=StubRosters(senate_error=AssertionError("direct not asked")),
        download_prior=no_download, senate_paid_ladder=True, senate_ladder=ladder,
    )  # fmt: skip


def test_the_opt_in_replaces_only_the_senate_request(tmp_path):
    import pyarrow.parquet as pq

    def ladder():
        return acquire_senate_via_ladder(None, proxies=fakes(Fetcher(XML), Fetcher(XML)), direct=refused_directly)

    _, assignments, _ = _build(tmp_path, ladder)
    chambers = {row["chamber"] for row in pq.read_table(assignments).to_pylist()}
    assert chambers == {"house", "senate"}


def test_the_opt_in_still_stops_the_run_when_every_rung_fails(tmp_path):
    def ladder():
        raise CommitteeRosterRefusedError(SENATE_CVC_URL)

    with pytest.raises(CommitteeRosterRefusedError):
        _build(tmp_path, ladder)
