"""Hermetic tests for the committee-rosters transform: the walk, the detail fold, the seats, and what a run re-asks.

No network. The list reader and the chamber-file acquirer are stubbed over the
wheel's own fixtures (``tests/fixtures/congress_rosters/``), parsed through the
wheel's own readers, so what is proved here is this repository's seam — which
details are asked for, in what order, how the over-declared route is read, and
how a capture replaces its chamber's seats — and never a shaper rule.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import DeclaredCountMismatch, PagedJsonSourceError
from spicy_docs.sources.congress.committee_rosters import (
    CommitteeRosterError,
    CommitteeRosterRefusedError,
    parse_house_member_data,
    parse_senate_cvc,
)
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms import build_committee_rosters as bcr
from tests.test_incremental_rollups import no_download, seed

FIXTURES = Path(__file__).parent / "fixtures" / "congress_rosters"
LIST_PAGE = json.loads((FIXTURES / "congress-committee-list.json").read_text())
HSJU00 = json.loads((FIXTURES / "congress-committee-hsju00-list-row.json").read_text())
DETAIL = json.loads((FIXTURES / "congress-committee-detail.json").read_text())["committee"]
HOUSE_XML = (FIXTURES / "memberdata-119-excerpt.xml").read_bytes()
SENATE_XML = (FIXTURES / "cvc-member-data-excerpt.xml").read_bytes()
OBSERVED_AT = "2026-09-19T00:00:00Z"
#: The three listed rows plus the Judiciary row the detail fixture folds onto.
LISTED = [*LIST_PAGE["committees"], HSJU00]
#: Newest ``updateDate`` first, which is the order details are asked for.
NEWEST_FIRST = ["hsbu00", "slet00", "sssb00", "hsju00"]
#: The measured over-declaration, at the stub's size: ``committee/119`` declared
#: 238 and served 236, two entries the count includes and the list omits.
DECLARED = len(LISTED) + (LIST_PAGE["pagination"]["count"] - 236)


class _Page:
    """One list page: its records and the route's declared total."""

    def __init__(self, records, declared):
        self.records = tuple(records)
        self.declared_count = declared


class StubListingReader:
    """The committee route as measured: one terminal page that serves fewer than it declares."""

    def __init__(self, listed=LISTED, details=None, *, walk_refusal=None):
        self.listed = listed
        self.details = {"hsju00": DETAIL} if details is None else details
        self.walk_refusal = walk_refusal or DeclaredCountMismatch(
            "Congress.gov declared and observed record counts differ", declared=DECLARED, observed=len(listed)
        )
        self.detail_codes: list[str] = []
        self.list_urls: list[str] = []

    def _walk(self):
        yield _Page(self.listed, DECLARED)
        raise self.walk_refusal

    def records(self, route, url, *, max_pages=100):
        if route.name == "committee":
            self.list_urls.append(url)
            return self._walk()
        assert route.name == "committee-detail"
        code = url.rsplit("/", 1)[-1].split("?")[0]
        self.detail_codes.append(code)
        detail = self.details.get(code)
        if detail is None:
            raise PagedJsonSourceError("stub: detail refused")
        return iter((_Page([detail], None),))


class StubRosters:
    """Serves both chamber files, raising ``house_error``/``senate_error`` when set."""

    def __init__(self, *, house_error=None, senate_error=None):
        self.house_error = house_error
        self.senate_error = senate_error
        self.calls: list[str] = []

    def acquire_house(self, *, congress, session=None, max_bytes=None, archived_at=None):
        self.calls.append("house")
        if self.house_error is not None:
            raise self.house_error
        roster = parse_house_member_data(HOUSE_XML, congress=congress, session=session)
        return SimpleNamespace(roster=roster, capture=SimpleNamespace(observed_at=OBSERVED_AT))

    def acquire_senate(self, *, max_bytes=None, archived_at=None):
        self.calls.append("senate")
        if self.senate_error is not None:
            raise self.senate_error
        return SimpleNamespace(roster=parse_senate_cvc(SENATE_XML), capture=SimpleNamespace(observed_at=OBSERVED_AT))


def _rows(path: Path) -> list[dict]:
    """Read a published Parquet table as a list of dicts."""
    return pq.read_table(path).to_pylist()


def _by_code(path: Path) -> dict[str, dict]:
    """Index a published table's rows by ``system_code``."""
    return {row["system_code"]: row for row in _rows(path)}


@pytest.fixture(autouse=True)
def scoped(monkeypatch):
    # The House fixture states the 119th; the roster leg always asks for the sitting Congress.
    monkeypatch.setattr(bcr, "current_congress", lambda: 119)


def _build(tmp_path, *, reader=None, rosters=None, **kw):
    """Run the rosters transform over the stubs and no prior download."""
    return bcr.build_committee_rosters(
        tmp_path,
        reader=reader or StubListingReader(),
        rosters=rosters or StubRosters(),
        download_prior=no_download,
        **kw,
    )


def test_a_cold_start_folds_each_detail_newest_first_and_publishes_what_the_route_served(tmp_path):
    reader = StubListingReader()
    committees, *_ = _build(tmp_path, reader=reader)

    assert reader.detail_codes == NEWEST_FIRST
    rows = _by_code(committees)
    assert set(rows) == set(NEWEST_FIRST), "the over-declared route's served rows are published"
    judiciary = rows["hsju00"]
    assert judiciary["detail_captured"] == "true" and judiciary["subcommittee_count"] == "15"
    assert judiciary["history_count"] == "1" and judiciary["bill_count"] == "74362"
    budget = rows["hsbu00"]
    assert budget["detail_captured"] == "false" and budget["subcommittee_count"] == "0"
    assert budget["history_count"] is None and budget["chamber"] == "House"


def test_the_list_is_every_congress_in_one_walk_whatever_the_bill_scope(tmp_path, monkeypatch):
    """committee_meetings keeps the 118th and bill_committees reaches the 108th; a Congress-scoped list orphaned both."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    older = {**HSJU00, "updateDate": "2020-01-01T00:00:00Z", "name": "an older listing of the same committee"}
    reader = StubListingReader(listed=[*LISTED, older])
    committees, *_ = _build(tmp_path, reader=reader)
    [url] = reader.list_urls
    assert url.split("?", 1)[0].endswith("/v3/committee"), "no Congress in the list path"
    rows = _by_code(committees)
    assert len(rows) == len(LISTED) and rows["hsju00"]["update_date"] == HSJU00["updateDate"]


def test_a_code_the_detail_route_cannot_spell_is_listed_without_spending_the_cap(tmp_path):
    """A code outside the reader's grammar (a name-authority prefix no list uses) would refuse every run."""
    unlisted = {**LIST_PAGE["committees"][1], "systemCode": "nb79043125", "name": "Mines and Mining",
                "updateDate": "2099-01-01T00:00:00Z"}
    reader = StubListingReader(listed=[unlisted, *LISTED])
    committees, *_ = _build(tmp_path, reader=reader, max_details=1)
    assert reader.detail_codes == ["hsbu00"], "the one detail of the cap goes to the newest code it can spell"
    row = _by_code(committees)["nb79043125"]
    assert (row["detail_captured"], row["name"]) == ("false", "Mines and Mining")


def test_a_committee_keyed_on_its_name_authority_id_folds_its_detail(tmp_path):
    """The 95 historical committees the unscoped list keys on a Library of Congress id have details to fold."""
    detail = {"systemCode": "n79043125", "isCurrent": False, "type": "Standing",
              "history": [{"officialName": "Committee on Indian Affairs", "locLinkedDataId": "n79043125"}]}
    historical = {"chamber": "Senate", "committeeTypeCode": "Standing", "name": "Indian Affairs",
                  "systemCode": "n79043125", "updateDate": "2025-06-20T19:17:19Z",
                  "url": "https://api.congress.gov/v3/committee/senate/n79043125?format=json"}
    reader = StubListingReader(listed=[historical], details={"n79043125": detail})
    committees, *_ = _build(tmp_path, reader=reader)
    assert reader.detail_codes == ["n79043125"]
    row = _by_code(committees)["n79043125"]
    assert (row["detail_captured"], row["is_current"], row["history_count"]) == ("true", "false", "1")


def test_every_other_walk_refusal_fails_the_run(tmp_path):
    with pytest.raises(PagedJsonSourceError, match="repeated its continuation"):
        _build(
            tmp_path,
            reader=StubListingReader(walk_refusal=PagedJsonSourceError("Congress.gov repeated its continuation")),
        )


def test_a_folded_committee_is_not_re_read_unless_its_list_row_moved(tmp_path):
    seed(
        tmp_path,
        "committees",
        [
            # Folded, list row unchanged: left standing.
            {
                "system_code": "hsju00",
                "update_date": HSJU00["updateDate"],
                "detail_captured": "true",
                "website_url": "the prior row",
            },
            # Folded, but the list row moved: asked for again.
            {"system_code": "slet00", "update_date": "2020-01-01T00:00:00Z", "detail_captured": "true"},
            # Listed last run without a detail: asked for again.
            {
                "system_code": "sssb00",
                "update_date": LIST_PAGE["committees"][2]["updateDate"],
                "detail_captured": "false",
            },
        ],
    )
    reader = StubListingReader()
    committees, *_ = _build(tmp_path, reader=reader)
    assert reader.detail_codes == ["hsbu00", "slet00", "sssb00"]
    rows = _by_code(committees)
    assert rows["hsju00"]["website_url"] == "the prior row"
    # slet00's detail was refused this run: the held row — its detail included — stands.
    assert rows["slet00"]["detail_captured"] == "true" and rows["slet00"]["update_date"] == "2020-01-01T00:00:00Z"
    # sssb00 had no detail to lose: the fresh list row is published, still without one.
    assert rows["sssb00"]["detail_captured"] == "false"


def test_the_cap_leaves_held_rows_standing_and_lists_the_rest_without_a_detail(tmp_path):
    seed(
        tmp_path,
        "committees",
        [
            {
                "system_code": "hsju00",
                "update_date": "2020-01-01T00:00:00Z",
                "detail_captured": "true",
                "website_url": "held",
            }
        ],
    )
    reader = StubListingReader()
    committees, *_ = _build(tmp_path, reader=reader, max_details=1)
    assert reader.detail_codes == ["hsbu00"]
    rows = _by_code(committees)
    assert rows["hsju00"]["website_url"] == "held" and rows["hsju00"]["update_date"] == "2020-01-01T00:00:00Z"
    assert {rows[code]["detail_captured"] for code in ("hsbu00", "slet00", "sssb00")} == {"false"}


def test_todays_seats_come_from_both_files_and_carry_each_files_own_statement(tmp_path):
    rosters = StubRosters()
    _, assignments, _ = _build(tmp_path, rosters=rosters)
    assert rosters.calls == ["house", "senate"]
    rows = _rows(assignments)
    house = [r for r in rows if r["chamber"] == "house"]
    senate = [r for r in rows if r["chamber"] == "senate"]

    roster = parse_house_member_data(HOUSE_XML, congress=119)
    seated = [m for m in roster.members if not m.vacant]
    assert len(house) == sum(len(m.assignments) for m in seated) and len(house) > 0
    assert {r["bioguide_id"] for r in house} == {m.bioguide_id for m in seated if m.assignments}
    begich = next(r for r in house if r["bioguide_id"] == "B001323" and r["system_code"] == "hsii00")
    assert (begich["congress"], begich["congress_basis"], begich["session"]) == ("119", "file", "2")
    assert begich["committee_name"] == "Committee on Natural Resources" and begich["file_date"] == "September 2, 2026"

    cvc = parse_senate_cvc(SENATE_XML)
    assert len(senate) == sum(len(s.committees) for s in cvc.senators)
    aging = next(r for r in senate if r["bioguide_id"] == "A000382" and r["system_code"] == "spag00")
    assert (aging["congress"], aging["congress_basis"], aging["lis_id"]) == ("119", "caller", "S428")
    assert aging["observed_at"] == OBSERVED_AT


def test_a_capture_replaces_its_chambers_seats_and_leaves_earlier_congresses(tmp_path):
    seed(
        tmp_path,
        "committee_assignments",
        [
            {
                "congress": "119",
                "system_code": "hsxx00",
                "bioguide_id": "X000001",
                "chamber": "house",
                "observed_at": "2026-01-01",
            },
            {
                "congress": "119",
                "system_code": "ssxx00",
                "bioguide_id": "X000002",
                "chamber": "senate",
                "observed_at": "2026-01-01",
            },
            {
                "congress": "118",
                "system_code": "hsxx00",
                "bioguide_id": "X000003",
                "chamber": "house",
                "observed_at": "2024-01-01",
            },
        ],
    )
    _, assignments, _ = _build(tmp_path)
    rows = _rows(assignments)
    assert not [r for r in rows if r["bioguide_id"] in ("X000001", "X000002")], (
        "seats the files no longer list are gone"
    )
    assert [r["congress"] for r in rows if r["bioguide_id"] == "X000003"] == ["118"], (
        "an earlier Congress keeps its last capture"
    )


def test_a_file_not_established_keeps_its_chambers_prior_seats(tmp_path):
    seed(
        tmp_path,
        "committee_assignments",
        [
            {
                "congress": "119",
                "system_code": "hsxx00",
                "bioguide_id": "X000001",
                "chamber": "house",
                "observed_at": "2026-01-01",
            },
            {
                "congress": "119",
                "system_code": "ssxx00",
                "bioguide_id": "X000002",
                "chamber": "senate",
                "observed_at": "2026-01-01",
            },
        ],
    )
    _, assignments, _ = _build(tmp_path, rosters=StubRosters(house_error=CommitteeRosterError("stub: truncated")))
    rows = _rows(assignments)
    assert [r["bioguide_id"] for r in rows if r["chamber"] == "house" and r["system_code"] == "hsxx00"] == ["X000001"]
    assert not [r for r in rows if r["bioguide_id"] == "X000002"], "the Senate capture still replaced its seats"


@pytest.mark.parametrize(
    "rosters",
    [
        StubRosters(house_error=CommitteeRosterRefusedError("https://clerk.house.gov/xml/lists/MemberData.xml")),
        StubRosters(senate_error=CredentialRefusedError("stub: 403")),
    ],
)
def test_a_refusal_of_access_aborts_the_run(tmp_path, rosters):
    with pytest.raises((CommitteeRosterRefusedError, CredentialRefusedError)):
        _build(tmp_path, rosters=rosters)


def test_the_published_shapes_are_the_contracts(tmp_path):
    from spicy_docs.schemas import TABLE_CONTRACTS

    names = ("committees", "committee_assignments", "committee_directory_entries")
    for name, path in zip(names, _build(tmp_path), strict=True):
        assert path.name == f"{name}.parquet"
        assert pq.read_table(path).schema.names == list(TABLE_CONTRACTS[name].columns)
