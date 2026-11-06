"""Hermetic tests that each incremental rollup actually re-fetches less.

The claim these pin is "a steady-state run does not pay for what it already
published". A docstring saying so is not evidence; each case here seeds a prior
published table, runs the transform with a counting stub, and asserts on what
was requested.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import (
    DEFAULT_POOL_PASSES,
    IncompleteWalkError,
    PagedJsonBudget,
    PagedJsonSourceError,
)
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.listing import CongressListingReader

from spicy_regs.transforms.table_merge import prior_scratch_path


def seed(output_dir: Path, table: str, rows: list[dict]) -> None:
    """Write a prior published table where the transform will look for it."""
    contract = TABLE_CONTRACTS[table]
    filled = [{c: None for c in contract.columns} | row for row in rows]
    pq.write_table(
        pa.Table.from_pylist(filled, schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(output_dir, table),
    )


def no_download(remote_key: str, local_path: Path) -> bool:
    return False


# --------------------------------------------------------------------------- #
# Amendments: a window, not a full walk.
# --------------------------------------------------------------------------- #
class PageStubReader(CongressListingReader):
    """spicy-docs' reader with its pages stubbed, so ``pooled`` (walk order, page size, settling) runs for real."""

    def __init__(self):
        super().__init__(
            budget=PagedJsonBudget(
                max_requests=1, max_page_bytes=1024, timeout_seconds=1, min_request_interval_seconds=0
            ),
            api_key="stub-key",
        )
        self.urls: list[str] = []


class StubListingReader(PageStubReader):
    """Records the URLs asked for and serves an empty window."""

    def records(self, route, url, *, max_pages=100):
        self.urls.append(url)
        return iter((SimpleNamespace(records=(), declared_count=0),))


def test_amendments_windows_from_the_prior_watermark(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.delenv("AMENDMENTS_SINCE", raising=False)
    monkeypatch.delenv("AMENDMENTS_UNTIL", raising=False)
    seed(
        tmp_path,
        "amendments",
        [
            {
                "congress": "119",
                "amendment_type": "samdt",
                "amendment_number": "1",
                "update_date": "2026-09-10T00:00:00Z",
            }
        ],
    )

    reader = StubListingReader()
    build_amendments(tmp_path, reader=reader, download_prior=no_download)

    assert len(reader.urls) == 1
    url = reader.urls[0]
    # Watermark 2026-09-10 minus the 3-day overlap.
    assert "fromDateTime=2026-09-07" in url, url
    assert "toDateTime=" in url


def test_amendments_walks_everything_with_no_prior(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.delenv("AMENDMENTS_SINCE", raising=False)
    monkeypatch.delenv("AMENDMENTS_UNTIL", raising=False)

    reader = StubListingReader()
    build_amendments(tmp_path, reader=reader, download_prior=no_download)
    assert "fromDateTime" not in reader.urls[0], "a cold start must not bound the walk"


def test_amendments_caps_the_window(tmp_path, monkeypatch):
    """A deep backfill converges over runs instead of timing out in one."""
    from spicy_regs.transforms.build_amendments import MAX_WINDOW_DAYS, build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = StubListingReader()
    build_amendments(
        tmp_path, reader=reader, since=date(2020, 1, 1), until=date(2026, 1, 1), download_prior=no_download
    )
    url = reader.urls[0]
    assert "fromDateTime=2020-01-01" in url
    capped = date(2020, 1, 1).toordinal() + MAX_WINDOW_DAYS
    assert f"toDateTime={date.fromordinal(capped).isoformat()}" in url, url


def _amendment(number: int, update: str = "2026-01-01T00:00:00Z") -> dict:
    return {
        "congress": 119,
        "type": "SAMDT",
        "number": str(number),
        "updateDate": update,
        "url": f"https://api.congress.gov/v3/amendment/119/samdt/{number}?format=json",
    }


class ShiftingListingReader(PageStubReader):
    """Each pass returns the declared count of rows, but repeats one amendment and skips another."""

    def __init__(self, passes):
        super().__init__()
        self.passes = list(passes)

    def records(self, route, url, *, max_pages=100):
        if route.name == "amendment-detail":
            number = url.split("?")[0].rsplit("/", 1)[1]
            detail = {
                "sponsors": [{"bioguideId": f"S{number}", "fullName": f"Sen. {number}", "party": "D"}],
                "amendedBill": {"congress": 119, "type": "HR", "number": "1"},
                "chamber": "Senate",
            }
            return iter((SimpleNamespace(records=(detail,) if number != "404" else (), declared_count=None),))
        self.urls.append(url)
        records, declared = self.passes.pop(0)
        return iter((SimpleNamespace(records=tuple(records), declared_count=declared),))


def test_amendments_pool_opposite_sort_passes_until_the_declared_count(tmp_path, monkeypatch):
    """A single pass that repeats #2 and skips #3 still matches the declared count by rows; pooling catches it.

    Neither walk is clean, so the pool of both settles the query; each amendment keeps its newest ``updateDate``.
    """
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader(
        [
            ([_amendment(1), _amendment(2), _amendment(2)], 3),
            ([_amendment(3), _amendment(3), _amendment(2, "2026-02-01T00:00:00Z")], 3),
        ]
    )
    out = build_amendments(tmp_path, reader=reader, download_prior=no_download)
    rows = {row["amendment_number"]: row for row in pq.read_table(out).to_pylist()}
    assert sorted(rows) == ["1", "2", "3"]
    assert rows["2"]["update_date"] == "2026-02-01T00:00:00Z", "the later observation wins"
    assert "sort=updateDate+desc" in reader.urls[0] and "sort=updateDate+asc" in reader.urls[1]
    assert "limit=250" in reader.urls[0] and "limit=237" in reader.urls[1], "each walk moves its page boundaries"


def test_amendments_refuse_a_walk_that_never_reaches_its_declared_count(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader([([_amendment(1), _amendment(1)], 2)] * DEFAULT_POOL_PASSES)
    with pytest.raises(IncompleteWalkError, match="pooled 1 of 2 declared"):
        build_amendments(tmp_path, reader=reader, download_prior=no_download)
    assert len(reader.urls) == DEFAULT_POOL_PASSES


def test_amendments_refuse_a_record_without_an_update_date(tmp_path, monkeypatch):
    """The version ranks two observations of one amendment; a missing one refuses rather than ranking first."""
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    undated = {key: value for key, value in _amendment(1).items() if key != "updateDate"}
    reader = ShiftingListingReader([([undated], 1)])
    with pytest.raises(PagedJsonSourceError, match="could not read a record's version"):
        build_amendments(tmp_path, reader=reader, download_prior=no_download)


def test_amendments_refuse_a_pool_holding_more_than_the_declared_count(tmp_path, monkeypatch):
    """A record replaced under an unchanged total overfills the pool; it refuses rather than settling on a surplus."""
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    first = [_amendment(1), _amendment(1), _amendment(2)]
    reader = ShiftingListingReader(
        [(first, 3), ([_amendment(3), _amendment(4), _amendment(4)], 3)] + [(first, 3)] * (DEFAULT_POOL_PASSES - 2)
    )
    with pytest.raises(IncompleteWalkError, match="pooled 4 records, more than the 3 declared"):
        build_amendments(tmp_path, reader=reader, download_prior=no_download)


# --------------------------------------------------------------------------- #
# Roll-call votes: held roll calls are not re-read from the Clerk.
# --------------------------------------------------------------------------- #
class CountingVoteAcquirer:
    def __init__(self):
        self.requested: list[int] = []

    def acquire(self, locator, *, crosswalk=None):
        self.requested.append(locator.roll_number)
        raise _Unavailable("stub: no Clerk file in a hermetic test")

    def list_house_votes(self, congress, session, *, start_roll=1):
        """The Clerk's roll files for the 119th's first session: rolls 1-40, newest first."""
        from types import SimpleNamespace

        from spicy_docs.sources.congress.votes import ClerkVoteIndex, ClerkVoteIndexEntry

        rolls = range(40, 0, -1) if session == 1 else ()
        entries = tuple(ClerkVoteIndexEntry(n) for n in rolls)
        return SimpleNamespace(index=ClerkVoteIndex(congress, session, 2024 + session, entries))

    def list_senate_votes(self, congress, session):
        from types import SimpleNamespace

        return SimpleNamespace(menu=SimpleNamespace(votes=()))


class _Unavailable(Exception):
    pass


@pytest.fixture(autouse=True)
def _vote_error(monkeypatch):
    """Make the stub's refusal the type the transform counts rather than raises."""
    import spicy_regs.transforms.build_roll_call_votes as module

    monkeypatch.setattr(module, "VoteSourceError", _Unavailable)


def test_roll_call_votes_skips_what_it_already_published(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    # Every listed roll call is already published with a real tally.
    seed(
        tmp_path,
        "roll_call_votes",
        [
            {
                "vote_id": f"119-house-1-{n}",
                "congress": "119",
                "chamber": "house",
                "session": "1",
                "roll_number": str(n),
                "yea": "220",
                "legis_num": "H R 3424",
                "clerk_body_element": "chamber",
                "party_totals_json": "[]",
            }
            for n in range(1, 41)
        ],
    )

    acquirer = CountingVoteAcquirer()
    build_roll_call_votes(
        tmp_path,
        acquirer=acquirer,
        overlap=5,
        download_prior=no_download,
        open_congresses=(119,),
    )
    # Only the overlap is re-read; the other 35 are not fetched again.
    assert len(acquirer.requested) == 5
    assert sorted(acquirer.requested, reverse=True) == [40, 39, 38, 37, 36]


def test_roll_call_votes_fetches_everything_with_no_prior(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    acquirer = CountingVoteAcquirer()
    build_roll_call_votes(
        tmp_path, acquirer=acquirer, overlap=5, download_prior=no_download
    )
    assert len(acquirer.requested) == 40


def test_a_linkage_only_row_is_not_treated_as_held(tmp_path, monkeypatch):
    """A row with no tally means the Clerk file was never read; try it again."""
    from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    seed(
        tmp_path,
        "roll_call_votes",
        [
            {
                "vote_id": f"119-house-1-{n}",
                "congress": "119",
                "chamber": "house",
                "session": "1",
                "roll_number": str(n),
                "yea": None,
            }
            for n in range(1, 41)
        ],
    )
    acquirer = CountingVoteAcquirer()
    build_roll_call_votes(
        tmp_path, acquirer=acquirer, overlap=5, download_prior=no_download
    )
    assert len(acquirer.requested) == 40


# --------------------------------------------------------------------------- #
# Committee reports: watermark, and held packages are not re-fetched.
# --------------------------------------------------------------------------- #
def test_committee_reports_history_does_not_establish_a_live_watermark(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_committee_reports import _since

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    seed(tmp_path, "committee_reports", [{"package_id": "CRPT-119hrpt1", "last_modified": "2026-09-10T12:00:00Z"}])
    from datetime import UTC, datetime
    since = _since(prior_scratch_path(tmp_path, "committee_reports"), now=datetime(2026, 11, 6, tzinfo=UTC))
    assert since == "2026-10-07T00:00:00Z"


def test_committee_reports_cold_start_uses_the_default_window(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_committee_reports import _since

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    since = _since(None)
    assert since.endswith("Z")
    # Thirty days back, not a watermark.
    assert since < date.today().isoformat()


def test_committee_reports_env_override_wins(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_committee_reports import _since

    monkeypatch.setenv("COMMITTEE_REPORTS_SINCE", "2020-01-01")
    seed(tmp_path, "committee_reports", [{"package_id": "CRPT-119hrpt1", "last_modified": "2026-09-10T12:00:00Z"}])
    assert _since(prior_scratch_path(tmp_path, "committee_reports")) == "2020-01-01T00:00:00Z"


def test_committee_reports_skips_packages_it_holds(tmp_path, monkeypatch):
    from spicy_regs.transforms import build_committee_reports as module

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    seed(
        tmp_path,
        "committee_reports",
        [{"package_id": "CRPT-119hrpt1", "last_modified": "2026-09-10T12:00:00Z"}],
    )

    from spicy_regs.transforms.committee_report_reads import READS_TABLE, RULE_VERSIONS
    from spicy_regs.transforms.table_merge import prior_scratch_path

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "package_id": "CRPT-119hrpt1",
                    "last_modified": "2026-09-10T12:00:00Z",
                    "outcome": "complete",
                    "rule_version": RULE_VERSIONS["CRPT"],
                    "observed_at": "2026-09-20",
                }
            ]
        ),
        prior_scratch_path(tmp_path, READS_TABLE),
    )

    class Reader:
        """Serves each collection its own packages, as GovInfo does."""

        def packages(self, url, *, max_pages=40):
            collection = "CRPT" if "CRPT" in url else "CHRG"

            class Page:
                records = (
                    {"packageId": f"{collection}-119hrpt1"},
                    {"packageId": f"{collection}-119hrpt2"},
                )

            return iter((Page(),))

    class CountingAcquirer:
        def __init__(self):
            self.requested: list[str] = []

        def acquire(self, package_id, *, prefer=(), max_bytes=None):
            self.requested.append(package_id)
            raise LookupError("stub: no body in a hermetic test")

        acquire_parts = acquire

    acquirer = CountingAcquirer()

    class NoHearings:
        def records(self, route, url, *, max_pages=1):
            raise AssertionError("no package body was read, so no hearing detail may be asked for")

    module.build_committee_reports(
        tmp_path, reader=Reader(), acquirer=acquirer, hearings=NoHearings(), download_prior=no_download
    )
    assert "CRPT-119hrpt1" not in acquirer.requested, "a held package must not be re-fetched"
    assert "CRPT-119hrpt2" in acquirer.requested


@pytest.mark.parametrize("change", ["release", "code"])
def test_a_report_is_re_read_for_a_code_change_not_a_release_string(tmp_path, monkeypatch, change):
    """The token held ``spicy-docs=<version>``, so every version-only release re-read all 141 reports.

    708f2bf4 against 95810b26 differed in ``observed_at`` alone. A release string moving with the
    code unchanged re-reads nothing; the code moving re-reads what it derives.
    """
    import importlib
    import importlib.metadata

    from spicy_regs import generations
    from spicy_regs.transforms import build_committee_reports as module
    from spicy_regs.transforms import committee_report_reads as reads
    from spicy_regs.transforms.table_merge import prior_scratch_path

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    held = {"package_id": "CRPT-119hrpt1", "last_modified": "2026-09-10T12:00:00Z"}
    seed(tmp_path, "committee_reports", [held])
    checkpoint = held | {"outcome": "complete", "rule_version": reads.RULE_VERSIONS["CRPT"], "observed_at": "2026-09-20"}
    pq.write_table(pa.Table.from_pylist([checkpoint]), prior_scratch_path(tmp_path, reads.READS_TABLE))
    if change == "release":
        real = importlib.metadata.version
        monkeypatch.setattr(importlib.metadata, "version", lambda name: "9.9.9" if name == "spicy-docs" else real(name))
    else:
        monkeypatch.setattr(generations, "spicy_docs_code", lambda: "0" * 64)
    importlib.reload(reads)
    try:
        assert (reads.RULE_VERSIONS["CRPT"] == checkpoint["rule_version"]) is (change == "release")

        class Reader:
            def packages(self, url, *, max_pages=40):
                return iter(())

        class Acquirer:
            requested: list[str] = []

            def acquire_parts(self, package_id, *, prefer=(), max_bytes=None):
                self.requested.append(package_id)
                raise LookupError("stub: no body in a hermetic test")

            acquire = acquire_parts

        class NoHearings:
            def records(self, route, url, *, max_pages=1):
                raise AssertionError("no hearing is pending")

        acquirer = Acquirer()
        module.build_committee_reports(tmp_path, reader=Reader(), acquirer=acquirer, hearings=NoHearings(),
                                       download_prior=no_download)
        assert acquirer.requested == ([] if change == "release" else ["CRPT-119hrpt1"])
    finally:
        monkeypatch.undo()
        importlib.reload(reads)


def test_amendments_carry_the_detail_routes_sponsor_and_amended_bill(tmp_path, monkeypatch):
    """The list route states neither; each amendment's detail record does."""
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader([([_amendment(7)], 1)])
    rows = pq.read_table(build_amendments(tmp_path, reader=reader, download_prior=no_download)).to_pylist()
    assert [(r["sponsor_bioguide_id"], r["amended_bill_id"], r["chamber"], r["url"]) for r in rows] == [
        ("S7", "119-hr-1", "Senate", "https://api.congress.gov/v3/amendment/119/samdt/7?format=json")
    ]


def test_amendments_refuse_a_detail_that_answers_nothing(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    reader = ShiftingListingReader([([_amendment(404)], 1)])
    with pytest.raises(RuntimeError, match="answered 0 records"):
        build_amendments(tmp_path, reader=reader, download_prior=no_download)


def test_amendments_per_congress_watermark_and_bounded_max_details_drain_across_runs(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_amendments import build_amendments

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118-119")
    monkeypatch.delenv("AMENDMENTS_SINCE", raising=False)
    monkeypatch.delenv("AMENDMENTS_UNTIL", raising=False)
    seed(
        tmp_path,
        "amendments",
        [
            {
                "amendment_id": "119-samdt-1",
                "congress": "119",
                "amendment_type": "samdt",
                "amendment_number": "1",
                "update_date": "2026-09-10T00:00:00Z",
                "sponsor_bioguide_id": "S1",
                "amended_bill_id": "119-hr-1",
            }
        ],
    )
    a118_old = {**_amendment(10, "2024-03-01T00:00:00Z"), "congress": 118}
    a118_new = {**_amendment(20, "2024-04-01T00:00:00Z"), "congress": 118}

    class MultiCongressReader(PageStubReader):
        def __init__(self, c118_records):
            super().__init__()
            self.c118_records = c118_records
            self.detail_urls: list[str] = []

        def records(self, route, url, *, max_pages=100):
            if route.name == "amendment-detail":
                self.detail_urls.append(url)
                number = url.split("?")[0].rsplit("/", 1)[1]
                detail = {
                    "sponsors": [{"bioguideId": f"S{number}", "fullName": f"Sen. {number}", "party": "D"}],
                    "amendedBill": {"congress": 118, "type": "S", "number": "1"},
                    "chamber": "Senate",
                }
                return iter((SimpleNamespace(records=(detail,), declared_count=None),))
            self.urls.append(url)
            if "/amendment/118" in url:
                return iter((SimpleNamespace(records=tuple(self.c118_records), declared_count=len(self.c118_records)),))
            return iter((SimpleNamespace(records=(), declared_count=0),))

    r1 = MultiCongressReader([a118_old, a118_new])
    out1 = build_amendments(tmp_path, reader=r1, max_details=1, download_prior=no_download)
    assert any("/amendment/118" in u and "fromDateTime" not in u for u in r1.urls), (
        "cold 118th Congress walks from the beginning even when 119th has a 2026 watermark"
    )
    assert any("/amendment/119" in u and "fromDateTime=2026-09-07" in u for u in r1.urls)
    assert len(r1.detail_urls) == 1 and r1.detail_urls[0].split("?")[0].endswith("/118/samdt/20")
    rows1 = {row["amendment_id"]: row for row in pq.read_table(out1).to_pylist()}
    assert set(rows1) == {"119-samdt-1", "118-samdt-10", "118-samdt-20"}
    assert rows1["118-samdt-20"]["sponsor_bioguide_id"] == "S20"
    assert rows1["118-samdt-10"]["sponsor_bioguide_id"] is None

    seed(tmp_path, "amendments", list(rows1.values()))
    r2 = MultiCongressReader([])
    out2 = build_amendments(tmp_path, reader=r2, max_details=1, download_prior=no_download)
    assert any("/amendment/118" in u and "fromDateTime=2024-03-29" in u for u in r2.urls)
    assert len(r2.detail_urls) == 1 and r2.detail_urls[0].split("?")[0].endswith("/118/samdt/10")
    rows2 = {row["amendment_id"]: row for row in pq.read_table(out2).to_pylist()}
    assert rows2["118-samdt-10"]["sponsor_bioguide_id"] == "S10"
    assert rows2["118-samdt-10"]["amended_bill_id"] == "118-s-1"

    # When an already-enriched row is re-listed with a newer updateDate but deferred by max_details,
    # its held enriched fields (sponsor_bioguide_id, amended_bill_id) must not be overwritten by a list-only row.
    seed(tmp_path, "amendments", list(rows2.values()))
    r3 = MultiCongressReader(
        [
            {**_amendment(30, "2024-05-02T00:00:00Z"), "congress": 118},
            {**_amendment(20, "2024-05-01T00:00:00Z"), "congress": 118},
        ]
    )
    out3 = build_amendments(tmp_path, reader=r3, max_details=1, download_prior=no_download)
    rows3 = {row["amendment_id"]: row for row in pq.read_table(out3).to_pylist()}
    assert rows3["118-samdt-30"]["sponsor_bioguide_id"] == "S30"
    assert rows3["118-samdt-20"]["sponsor_bioguide_id"] == "S20"
    assert rows3["118-samdt-20"]["amended_bill_id"] == "118-s-1"
