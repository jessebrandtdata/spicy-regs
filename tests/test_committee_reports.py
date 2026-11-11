"""Hermetic test for the committee-report transform's body reading.

No network: the discovery reader and the body acquirer are stubbed, and the
``htm`` body is the shape GovInfo actually serves for a CRPT package — GPO's
plain text inside ``<html><title>..</title><body><pre>``, measured
2026-09-19 in spicy-docs ``docs/sources/govinfo-bodies.md``.

What this establishes is the two things the 0.21.1 adoption changed here — the
text comes from ``extraction.body_text`` rather than a local decoder, and a
rendition that states no page boundary publishes a NULL ``page_count`` instead
of a count invented from a separator that never occurs — and the bill linkage:
``bill_id`` is read from the package's own MODS, the real one for each package
(``tests/fixtures/govinfo_bodies/README.md``) parsed by the same
``validate_package_mods`` the acquirer runs, and only the bill spicy-docs'
``PackageModsIdentity.primary_bill`` names fills it. The window arithmetic is
covered in ``test_incremental_rollups.py``.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.govinfo.bodies import (
    PackageBodyIdentity,
    PackageSummary,
    ReportPart,
    package_body_locator,
    parse_package_id,
    validate_package_mods,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyBudget, GovInfoPackageBody
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.transforms.build_committee_reports import build_committee_reports
from tests.pdf_fixtures import make_pdf

FIXTURES = Path(__file__).parent / "fixtures" / "govinfo_bodies"
HEARINGS = Path(__file__).parent / "fixtures" / "congress_hearings"
OBSERVED_AT = "2026-09-19T00:00:00Z"

#: The CRPT package's report accompanies H. Res. 53 (`PRIMARY`); the CHRG
#: package's transcript merely mentions H.R. 1 and H.R. 5371 (`BODY`).
CRPT_ID = "CRPT-119hrpt1"
CHRG_ID = "CHRG-119hhrg63127"

#: One agency header the report parser matches, so `report_sections` is reached.
REPORT_HTML = b"""<html><title>Committee Report</title><body><pre>
  DEPARTMENT OF THE TREASURY

  The Committee recommends an appropriation of $1,000,000 for salaries
  and expenses.
</pre></body></html>
"""

BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=1 << 20,
    max_metadata_bytes=1 << 18,
    timeout_seconds=10.0,
    min_request_interval_seconds=0.0,
)


def _package(
    package_id: str,
    *,
    fmt: str = "htm",
    media_type: str = "text/html",
    body: bytes = REPORT_HTML,
    mods_bytes: bytes | None = None,
    part: ReportPart | None = None,
):
    """One acquired body: ``part``'s, or the one ``acquire`` reads, the part the root's renditions sit at."""
    identity = parse_package_id(package_id)
    mods_url = f"https://api.govinfo.gov/packages/{package_id}/mods"
    mods_capture = CapturedBodyResponse(
        requested_url=mods_url,
        resolved_url=mods_url,
        status_code=200,
        content_type="application/xml",
        observed_at=OBSERVED_AT,
        body=(FIXTURES / f"mods-{package_id}.xml").read_bytes() if mods_bytes is None else mods_bytes,
    )
    # The parse the acquirer itself runs on these bytes, so the bills and the
    # parts come from the real MODS rather than a hand-typed list, and each
    # part is read at its own stem, as the acquirer reads it.
    mods = validate_package_mods(
        mods_capture.body, package=identity, final_url=mods_url, max_bytes=BUDGET.max_metadata_bytes
    )
    if part is None:
        part = next((each for each in mods.parts if each.part_id == (mods.part_id or package_id)), None)
    part_id = None if part is None else part.part_id
    url = package_body_locator(identity, fmt, part_id=part_id)
    capture = CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type=media_type,
        observed_at=OBSERVED_AT,
        body=body,
    )
    return GovInfoPackageBody(
        identity=identity,
        format=fmt,
        preference=("xml", "htm", "txt", "pdf"),
        offered_formats=("htm", "pdf"),
        summary=PackageSummary(
            identity=identity,
            collection_code=identity.collection,
            date_issued="2026-01-05",
            last_modified="2026-09-18T12:00:00Z",
            title=f"Report {package_id}",
            download_links=(),
        ),
        mods=mods,
        body=PackageBodyIdentity(
            identity=identity,
            format=fmt,
            media_type=media_type,
            final_url=url,
            byte_size=len(body),
            part_id=part_id,
        ),
        summary_capture=capture,
        mods_capture=mods_capture,
        body_capture=capture,
        request_count=3,
        budget=BUDGET,
        part=part,
    )


def _parts(package_id: str, *, bodies: dict[str, bytes] | None = None, **kwargs):
    """Every part the record states, each at its own stem, as ``acquire_parts`` returns them.

    ``bodies`` maps a part id to its bytes; any other part is ``body`` (default ``REPORT_HTML``).
    """
    record = _package(package_id, **kwargs).mods
    default = kwargs.pop("body", REPORT_HTML)
    return tuple(
        _package(package_id, part=part, body=(bodies or {}).get(part.part_id, default), **kwargs)
        for part in record.parts
    )


class _Page:
    def __init__(self, records):
        self.records = records


class StubDiscovery:
    """One CRPT package and one CHRG package, the smallest input reaching all three tables."""

    IDS = {"CRPT": [CRPT_ID], "CHRG": [CHRG_ID]}

    def packages(self, url: str, *, max_pages: int = 1):
        collection = next(name for name in self.IDS if f"/{name}/" in url or url.endswith(name))
        yield _Page([{"packageId": package_id} for package_id in self.IDS[collection]])


class CrptOnlyDiscovery(StubDiscovery):
    """The CRPT package alone, for the cases that hand-build its MODS."""

    IDS = {"CRPT": [CRPT_ID], "CHRG": []}


class NoBodies:
    """An acquirer that serves nothing; a stub overrides the read its collection makes.

    The transform asks ``acquire_parts`` of a report and ``acquire`` of a hearing
    (``PackageBodySource``), so a stub for one collection still states both.
    """

    def acquire(self, package_id: str, *, max_bytes=None, prefer=()):
        raise LookupError(f"stub: no hearing body for {package_id}")

    def acquire_parts(self, package_id: str, *, max_bytes=None, prefer=()):
        raise LookupError(f"stub: no report body for {package_id}")


class StubBodyAcquirer:
    """Serves the htm rendition, and records that no preference was asked for.

    `mods_bytes` stands in for every package's fixture MODS, for the hand-built
    edge cases; the default is each package's real one.
    """

    def __init__(self, mods_bytes: bytes | None = None):
        self.requested: list[str] = []
        self.mods_bytes = mods_bytes

    def acquire(self, package_id: str, *, max_bytes=None, prefer=()):
        assert prefer == (), "only a publisher placeholder asks for a rendition"
        self.requested.append(package_id)
        return _package(package_id, mods_bytes=self.mods_bytes)

    def acquire_parts(self, package_id: str, *, max_bytes=None, prefer=()):
        assert prefer == (), "only a publisher placeholder asks for a rendition"
        self.requested.append(package_id)
        return _parts(package_id, mods_bytes=self.mods_bytes)


class StubHearings:
    """Serves the two retained Congress.gov hearing details by jacket; anything else is a 404-shaped refusal.

    Jacket 63127 (the CHRG MODS fixture's) states no `associatedMeeting`;
    64431 states event 119003 (`tests/fixtures/congress_hearings/README.md`).
    """

    DETAILS = {"63127": "hearing-119-house-63127.json", "64431": "hearing-119-house-64431.json"}

    def __init__(self, details: dict[str, str] | None = None):
        self.requested: list[str] = []
        self.details = self.DETAILS if details is None else details

    def records(self, route, url: str, *, max_pages: int = 1):
        self.requested.append(url)
        jacket = url.rsplit("/", 1)[-1].split("?", 1)[0]
        if jacket not in self.details:
            from spicy_docs.reading.paged_json import PagedJsonSourceError

            raise PagedJsonSourceError("stub: list source answered HTTP 404 for the requested page")
        record = json.loads((HEARINGS / self.details[jacket]).read_text())["hearing"]
        yield _Page((record,))


def _no_prior(remote_key: str, local_path: Path) -> bool:
    return False


def _build_with_log(tmp_path: Path, *, reader, acquirer) -> tuple[dict[str, Path], list[str]]:
    """Run the transform and return its tables by name with every INFO-and-up log line."""
    from loguru import logger

    messages: list[str] = []
    sink = logger.add(messages.append, level="INFO", format="{message}")
    try:
        paths = build_committee_reports(
            tmp_path, reader=reader, acquirer=acquirer, hearings=StubHearings(), download_prior=_no_prior
        )
    finally:
        logger.remove(sink)
    return {path.stem: path for path in paths}, messages


def _mentions_line(messages: list[str]) -> tuple[int, dict[str, int]]:
    """The linked count and the by-context mention counter the run log states."""
    line = next(m for m in messages if "PRIMARY bill" in m)
    linked = int(line.removeprefix("Committee reports: ").split(" ", 1)[0].replace(",", ""))
    return linked, ast.literal_eval(line.split("— ", 1)[1])


@pytest.fixture
def reports(tmp_path, monkeypatch):
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    paths = build_committee_reports(
        tmp_path, reader=StubDiscovery(), acquirer=StubBodyAcquirer(), hearings=StubHearings(), download_prior=_no_prior
    )
    return {path.stem: path for path in paths}


def test_a_non_pdf_rendition_publishes_no_page_count(reports):
    """No GovInfo body carries a form feed or a `[[Page N]]` marker, so htm states no pages.

    The old reader split on a form feed that never occurred and published
    `page_count = 1` for every report — a count of the separator's absence,
    not of pages.
    """
    for table in ("committee_reports", "hearing_transcripts"):
        rows = pq.read_table(reports[table]).to_pylist()
        assert len(rows) == 1, table
        assert rows[0]["page_count"] is None, table
        assert rows[0]["format"] == "htm", table
        assert rows[0]["media_type"] == "text/html"


def test_the_text_is_the_markup_readers_text_not_the_raw_html(reports):
    """`body_text` reads the htm body structurally; the wrapper never reaches the digest."""
    rows = pq.read_table(reports["committee_reports"]).to_pylist()
    assert rows[0]["text_sha256"].startswith("sha256:")
    assert rows[0]["byte_size"] == str(len(REPORT_HTML))

    sections = pq.read_table(reports["report_sections"]).to_pylist()
    assert sections, "the agency header must produce a block"
    assert all("<pre>" not in (row["body"] or "") for row in sections)
    assert all(row["package_id"] == CRPT_ID for row in sections)


def test_a_report_is_linked_to_the_bill_its_mods_marks_primary(reports):
    """The MODS the acquirer already fetched names four bills; the one marked PRIMARY is the linkage.

    H. Res. 53 appears twice in the fixture, once as OTHER and once as
    PRIMARY, beside S. 5 and H.R. 471 as OTHER, so "first bill listed" would
    have answered S. 5 — the resolution the report provides for considering,
    not the one it accompanies. Measured live 2026-09-19, the PRIMARY bill
    agreed with Congress.gov's own `associatedBill[0]` on 12 of 12 reports.
    """
    rows = pq.read_table(reports["committee_reports"]).to_pylist()
    assert rows[0]["package_id"] == CRPT_ID
    assert rows[0]["bill_id"] == "119-hres-53"


def test_a_report_in_one_part_is_keyed_on_its_package_id(reports):
    """Decision 29: a report published in one part is that part, and its record numbers none."""
    [row] = pq.read_table(reports["committee_reports"]).to_pylist()
    assert (row["package_id"], row["part_id"], row["part_number"]) == (CRPT_ID, CRPT_ID, None)
    assert {row["part_id"] for row in pq.read_table(reports["report_sections"]).to_pylist()} == {CRPT_ID}


def test_a_hearing_that_only_mentions_bills_is_not_linked_to_one(reports):
    """`BODY` is a mention, not a subject: the Worldwide Threats hearing is not "about" H.R. 1."""
    rows = pq.read_table(reports["hearing_transcripts"]).to_pylist()
    assert rows[0]["package_id"] == CHRG_ID
    assert rows[0]["bill_id"] is None


def test_the_mentions_are_counted_by_context_in_the_run_log(tmp_path, monkeypatch):
    """What the column does not carry is still stated: the OTHER and BODY mentions, by context."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    _, messages = _build_with_log(tmp_path, reader=StubDiscovery(), acquirer=StubBodyAcquirer())
    assert _mentions_line(messages) == (1, {"OTHER": 3, "BODY": 2})


def _mods(bills: str) -> bytes:
    """The smallest package MODS `validate_package_mods` accepts for the CRPT package, with these `<bill>`s."""
    return (
        '<mods xmlns="http://www.loc.gov/mods/v3"><extension>'
        f"<accessId>{CRPT_ID}</accessId><collectionCode>CRPT</collectionCode>{bills}"
        "</extension></mods>"
    ).encode()


#: (the MODS `<bill>` elements, the published bill_id, the mention counter, whether a type warning is logged)
MODS_EDGES = {
    "no bills": ("", None, {}, False),
    "a contextless bill is a mention": ('<bill congress="119" number="7" type="HR"/>', None, {"": 1}, False),
    "two PRIMARY: the first in document order": (
        '<bill congress="119" context="PRIMARY" number="5" type="S"/>'
        '<bill congress="119" context="PRIMARY" number="53" type="HRES"/>',
        "119-s-5",
        {},
        False,
    ),
    "a type outside the vocabulary is not linked but still counted": (
        '<bill congress="119" context="PRIMARY" number="9" type="HDOC"/>'
        '<bill congress="119" context="OTHER" number="10" type="HDOC"/>',
        None,
        {"OTHER": 1},
        True,
    ),
    "a zero-padded number spells the int key": (
        '<bill congress="119" context="PRIMARY" number="0053" type="HRES"/>',
        "119-hres-53",
        {},
        False,
    ),
}


@pytest.mark.parametrize(("bills", "bill_id", "mentions", "warns"), MODS_EDGES.values(), ids=MODS_EDGES.keys())
def test_the_mods_bill_edges(tmp_path, monkeypatch, bills, bill_id, mentions, warns):
    """Hand-built MODS through the acquirer's own `validate_package_mods`, one edge per case.

    The key is spelled from an int because `congress_bills.bill_id` is, so a
    publisher's zero-padded number joins; two `PRIMARY` entries resolve to the
    first in document order, spicy-docs' rule; a type outside `BILL_TYPES`
    is refused as a linkage with a warning yet counted as the mention it is;
    and an entry stating no `context` is a mention under `""`, not dropped.
    """
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    paths, messages = _build_with_log(tmp_path, reader=CrptOnlyDiscovery(), acquirer=StubBodyAcquirer(_mods(bills)))
    rows = pq.read_table(paths["committee_reports"]).to_pylist()
    assert [row["bill_id"] for row in rows] == [bill_id]
    assert _mentions_line(messages) == (int(bill_id is not None), mentions)
    assert any("not a supported bill type" in m for m in messages) is warns


def test_the_acquirer_is_asked_with_no_preference(tmp_path, monkeypatch):
    """The rendition order is the acquirer's sealed default; passing one here would fork it."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    acquirer = StubBodyAcquirer()
    build_committee_reports(
        tmp_path, reader=StubDiscovery(), acquirer=acquirer, hearings=StubHearings(), download_prior=_no_prior
    )
    assert acquirer.requested == [CRPT_ID, CHRG_ID]


class StubPdfAcquirer:
    """The fallback: a package offered only as PDF, which the sealed preference now reaches."""

    def acquire(self, package_id: str, *, max_bytes=None, prefer=()):
        body = make_pdf([f"{package_id} page one", "page two"])
        return _package(package_id, fmt="pdf", media_type="application/pdf", body=body)

    def acquire_parts(self, package_id: str, *, max_bytes=None, prefer=()):
        return (self.acquire(package_id),)


def test_the_pdf_fallback_is_extracted_and_states_its_page_count(tmp_path, monkeypatch):
    """The one rendition that does state page boundaries.

    `body_text` reads this through its default extractor
    (`DocumentExtractor(NativeText())`, PyMuPDF) — the pipeline
    `gpo_normalize` was derived on, and the one this repository now installs
    for the GovInfo body path (`vendor/README.md`).
    """
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    paths = {
        path.stem: path
        for path in build_committee_reports(
            tmp_path,
            reader=StubDiscovery(),
            acquirer=StubPdfAcquirer(),
            hearings=StubHearings(),
            download_prior=_no_prior,
        )
    }
    for table in ("committee_reports", "hearing_transcripts"):
        rows = pq.read_table(paths[table]).to_pylist()
        assert len(rows) == 1, f"{table}: the PDF fallback must publish a row, not a refusal"
        assert rows[0]["format"] == "pdf"
        assert rows[0]["page_count"] == "2", "a paginated rendition states its pages"
        assert rows[0]["text_sha256"].startswith("sha256:")


# --------------------------------------------------------------------------- #
# hearing_transcripts.event_id, from the Congress.gov hearing detail (A7).
# --------------------------------------------------------------------------- #
JACKET_64431 = "CHRG-119hhrg64431"


def _hearing_mods(package_id: str) -> bytes:
    """The smallest package MODS `validate_package_mods` accepts for a CHRG package."""
    return (
        '<mods xmlns="http://www.loc.gov/mods/v3"><extension>'
        f"<accessId>{package_id}</accessId><collectionCode>CHRG</collectionCode>"
        "</extension></mods>"
    ).encode()


class TwoHearingsDiscovery(StubDiscovery):
    """The CHRG package whose MODS is on disk, and one whose hearing detail names a meeting."""

    IDS = {"CRPT": [], "CHRG": [CHRG_ID, JACKET_64431]}


class HearingBodyAcquirer(StubBodyAcquirer):
    """The real MODS for 63127, a minimal one for 64431, both through `validate_package_mods`."""

    def acquire(self, package_id: str, *, max_bytes=None, prefer=()):
        self.requested.append(package_id)
        mods = None if package_id == CHRG_ID else _hearing_mods(package_id)
        return _package(package_id, mods_bytes=mods)


def _hearing_rows(tmp_path, hearings: StubHearings) -> tuple[dict[str, dict], list[str], StubHearings]:
    from loguru import logger

    messages: list[str] = []
    sink = logger.add(messages.append, level="INFO", format="{message}")
    try:
        paths = build_committee_reports(
            tmp_path,
            reader=TwoHearingsDiscovery(),
            acquirer=HearingBodyAcquirer(),
            hearings=hearings,
            download_prior=_no_prior,
        )
    finally:
        logger.remove(sink)
    rows = {row["package_id"]: row for row in pq.read_table(paths[2]).to_pylist()}
    return rows, messages, hearings


def test_event_id_is_the_meeting_the_hearing_detail_names_and_null_when_it_names_none(tmp_path, monkeypatch):
    """Two real details: 64431 names event 119003, 63127 names no meeting at all."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    rows, messages, hearings = _hearing_rows(tmp_path, StubHearings())
    assert rows[JACKET_64431]["event_id"] == "119003"
    assert rows[CHRG_ID]["event_id"] is None
    # One keyed request per CHRG package, addressed by the package id's own chamber and jacket.
    assert [url.split("/v3/", 1)[1].split("?", 1)[0] for url in hearings.requested] == [
        "hearing/119/house/63127",
        "hearing/119/house/64431",
    ]
    assert any("hearing details by outcome — {'no_meeting': 1, 'meeting': 1}" in m for m in messages)


def test_a_refused_hearing_detail_leaves_event_id_null_and_is_counted(tmp_path, monkeypatch):
    """A jacket Congress.gov does not hold is a refusal, counted apart from a detail naming no meeting."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    rows, messages, _ = _hearing_rows(tmp_path, StubHearings(details={}))
    assert rows[JACKET_64431]["event_id"] is None
    assert rows[CHRG_ID]["event_id"] is None
    assert any("hearing details by outcome — {'refused': 2}" in m for m in messages)
    assert sum("hearing detail refused" in m for m in messages) == 2


def test_a_hearing_detail_for_another_jacket_is_refused_not_read(tmp_path, monkeypatch):
    """The detail must name the jacket asked for: 63127's record served under 64431 is not 64431's event."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    swapped = StubHearings(details={"64431": "hearing-119-house-63127.json", "63127": "hearing-119-house-64431.json"})
    rows, messages, _ = _hearing_rows(tmp_path, swapped)
    assert rows[JACKET_64431]["event_id"] is None
    assert rows[CHRG_ID]["event_id"] is None
    assert any("identity differs from the requested jacket" in m for m in messages)


def test_a_transport_failure_on_the_hearing_detail_leaves_event_id_null_and_the_run_completes(tmp_path, monkeypatch):
    """``ConnectionError`` is what the reader raises once its retries are spent: this row's refusal, not the run's."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)

    class Down(StubHearings):
        def records(self, route, url, *, max_pages=1):
            self.requested.append(url)
            raise ConnectionError("down")
            yield  # pragma: no cover

    rows, messages, hearings = _hearing_rows(tmp_path, Down())
    assert len(rows) == 2, "both transcripts still publish"
    assert all(row["event_id"] is None for row in rows.values())
    assert len(hearings.requested) == 2
    assert any("hearing details by outcome — {'refused': 2}" in m for m in messages)


def test_a_credential_refusal_on_the_hearing_detail_aborts_the_run(tmp_path, monkeypatch):
    from spicy_docs.transport.credentials import CredentialRefusedError

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)

    class Refusing:
        def records(self, route, url, *, max_pages=1):
            raise CredentialRefusedError("stub: 403")
            yield  # pragma: no cover

    with pytest.raises(CredentialRefusedError):
        build_committee_reports(
            tmp_path,
            reader=TwoHearingsDiscovery(),
            acquirer=HearingBodyAcquirer(),
            hearings=Refusing(),
            download_prior=_no_prior,
        )


@pytest.mark.parametrize(
    ("issued", "today", "settles"),
    [
        ("2026-01-05", "2026-02-01", False),  # 27 days after GovInfo issued it: asked again, quietly
        ("2026-01-05", "2026-04-05", False),  # 90 days: still inside the window
        ("2026-01-05", "2026-04-06", True),  # 91 days: settled
        # Issued in December, run after the 120th Congress convenes: the 119th has ended, but age decides.
        ("2026-12-20", "2027-01-10", False),
    ],
    ids=["27-days", "90-days", "91-days", "after-the-congress-ends"],
)
def test_an_unavailable_hearing_detail_is_awaited_for_90_days_then_settled(
    tmp_path, monkeypatch, capsys, issued, today, settles
):
    """Congress.gov answers 404 for every 87th-90th Congress jacket (run 42558022791), and adds a new hearing some
    time after GovInfo publishes it (owner decision 2026-11-09). Within 90 days of the package's GovInfo dateIssued
    such a hearing is asked again every run but is not pending, and a warning names it; past them it is settled and
    published with a NULL event_id. Any other detail refusal (a 500, a wrong record) is retried and pending."""
    import dataclasses
    import shutil
    from datetime import date
    from types import SimpleNamespace
    from typing import Any, cast

    from spicy_docs.reading.paged_json import PagedJsonSourceError, PagedJsonUnavailableError

    from spicy_regs.transforms import build_committee_reports as module
    from spicy_regs.transforms.committee_report_reads import READS_TABLE

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    monkeypatch.setattr(module, "_today", lambda: date.fromisoformat(today))

    class Issued(HearingBodyAcquirer):
        def acquire(self, package_id: str, *, max_bytes=None, prefer=()):
            body = super().acquire(package_id, max_bytes=max_bytes, prefer=prefer)
            return dataclasses.replace(body, summary=dataclasses.replace(body.summary, date_issued=issued))

    class Mixed(StubHearings):
        """63127 is absent from Congress.gov; 64431 fails transiently."""

        def records(self, route, url, *, max_pages=1):
            self.requested.append(url)
            if url.split("?", 1)[0].endswith("/63127"):
                raise PagedJsonUnavailableError(cast(Any, SimpleNamespace(status_code=404)))
            raise PagedJsonSourceError("stub: list source answered HTTP 500")
            yield  # pragma: no cover

    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir(), second.mkdir()
    coverage: dict = {}
    tables = _tables(build_committee_reports(first, reader=TwoHearingsDiscovery(), acquirer=Issued(),
                                             hearings=Mixed(), download_prior=_no_prior, coverage=coverage))
    assert {row["package_id"]: row["event_id"] for row in tables["hearing_transcripts"]} == {
        CHRG_ID: None, JACKET_64431: None}, "both bodies publish"
    reads = {row["package_id"]: row for row in tables[READS_TABLE]}
    assert {key: row["outcome"] for key, row in reads.items()} == {CHRG_ID: "detail_refused",
                                                                  JACKET_64431: "detail_refused"}
    basis = "hearing-detail-unavailable" if settles else "hearing-detail-awaited"
    assert reads[CHRG_ID]["rule_version"].endswith(f";refused-under={basis}")
    assert not reads[JACKET_64431]["rule_version"].count("refused-under"), "a transient refusal keeps the plain rule"
    assert coverage["pending"] == 1, "only the transient refusal is pending"
    assert coverage["awaiting_detail"] == (0 if settles else 1)
    warnings = [line for line in capsys.readouterr().out.splitlines() if line.startswith("::warning")]
    awaited = [line for line in warnings if "hearing details awaited" in line]
    assert bool(awaited) is not settles and all(CHRG_ID in line for line in awaited)
    # The transient refusal warns and is retried; 63127 read, so the collection did not fail.
    assert [line for line in warnings if "acquisition pending" in line] == [
        "::warning title=Committee reports: acquisition pending::1 package(s) pending, retried next run: "
        f"1 refused ({JACKET_64431}), 0 deferred by the 200-package cap (none)"]
    assert coverage["nothing_read"] == []

    def from_first(remote_key: str, local_path: Path) -> bool:
        source = first / remote_key
        return source.exists() and bool(shutil.copy(source, local_path))

    acquirer, hearings = Issued(), Mixed()
    build_committee_reports(second, reader=TwoHearingsDiscovery(), acquirer=acquirer, hearings=hearings,
                            download_prior=from_first)
    retried = [JACKET_64431] if settles else sorted([CHRG_ID, JACKET_64431])
    assert sorted(acquirer.requested) == retried, "only a settled detail is not asked for again"
    assert sorted(url.split("?", 1)[0].rsplit("/", 1)[-1] for url in hearings.requested) == sorted(
        key.removeprefix("CHRG-119hhrg") for key in retried)


@pytest.mark.parametrize(("served", "expected"), [("senate", "meeting"), (None, "unavailable")])
def test_a_joint_hearing_detail_is_asked_under_each_chamber_before_it_is_unavailable(served, expected):
    """Congress.gov files some joint hearings under a chamber: GovInfo's CHRG-119jhrg61392 answers 404 under
    ``joint`` and 200 under ``senate`` (probed 2026-11-09). Settling after ``joint`` alone would hide that route; a
    detail is unavailable only when every chamber answers 404."""
    from types import SimpleNamespace
    from typing import Any, cast

    from spicy_docs.reading.paged_json import PagedJsonUnavailableError

    from spicy_regs.transforms.build_committee_reports import _event_id

    class ByChamber:
        def __init__(self):
            self.chambers: list[str] = []

        def records(self, route, url, *, max_pages=1):
            chamber = url.split("/hearing/119/", 1)[1].split("/", 1)[0]
            self.chambers.append(chamber)
            if chamber != served:
                raise PagedJsonUnavailableError(cast(Any, SimpleNamespace(status_code=404)))
            yield _Page(({"jacketNumber": 61392, "congress": 119, "associatedMeeting": {"eventId": "118000"}},))

    package = SimpleNamespace(identity=SimpleNamespace(package_id="CHRG-119jhrg61392", document_type="jhrg",
                                                       congress=119, number="61392"))
    hearings = ByChamber()
    event_id, outcome = _event_id(cast(Any, hearings), package)
    assert outcome == expected and event_id == ("118000" if served else None)
    assert hearings.chambers == (["joint", "senate"] if served else ["joint", "senate", "house"])


def test_retries_queue_behind_packages_never_attempted_oldest_attempt_first():
    """A fixed order let the same first 200 unsettled hearings take the cap every night (run 42558022791): a package
    not yet attempted at its stamp goes first, then each retry by its last attempt, oldest first."""
    from spicy_regs.transforms.build_committee_reports import _retry_order

    stamp = "2026-09-18T12:00:00Z"
    reads = {
        "CHRG-87a": {"outcome": "detail_refused", "last_modified": stamp, "observed_at": "2026-11-09T03:00:00Z"},
        "CHRG-88b": {"outcome": "refused", "last_modified": stamp, "observed_at": "2026-11-07T03:00:00Z"},
        "CHRG-89c": {"outcome": "pending", "last_modified": stamp, "observed_at": "2026-11-09T03:00:00Z"},  # deferred
        "CHRG-90d": {"outcome": "detail_refused", "last_modified": "2026-01-01T00:00:00Z",
                     "observed_at": "2026-11-01T03:00:00Z"},  # changed since its attempt
    }
    pending = {"CHRG-87a": stamp, "CHRG-88b": stamp, "CHRG-89c": stamp, "CHRG-90d": stamp, "CHRG-91new": stamp}
    order = [key for key, _ in sorted(pending.items(), key=lambda item: _retry_order(reads.get(item[0], {}), item[1]))]
    assert order == ["CHRG-89c", "CHRG-90d", "CHRG-91new", "CHRG-88b", "CHRG-87a"]


def test_a_hearing_read_before_the_mods_columns_is_read_once_more():
    """spicy-docs 0.54.0 reads a hearing's MODS event ids, witnesses and held date (E's 7db044f) into three appended
    hearing_transcripts columns; a hearing read under the rule before them is not complete, so each held hearing is
    read once more and gains them. A report's rule is untouched: CRPT reads gain nothing from the MODS columns."""
    from spicy_regs.transforms.committee_report_reads import HEARING_BILL_LINK_RULE_VERSION, RULE_VERSIONS, complete

    # The CHRG rule every held hearing was read under before the adoption (d2b5f53).
    before = f"{HEARING_BILL_LINK_RULE_VERSION};body=placeholder-pdf-002"
    assert not complete({"outcome": "complete", "rule_version": before}, "CHRG")
    assert complete({"outcome": "complete", "rule_version": RULE_VERSIONS["CHRG"]}, "CHRG")


def test_a_hearing_read_before_the_committee_codes_column_is_read_once_more():
    """The round-6 spicy-docs build reads the committee codes a hearing's MODS states into
    hearing_transcripts.committee_system_codes_json (GovInfo names the committee there; Congress.gov's hearing
    record often names none). A hearing read under the rule before that column is not complete, so each held
    hearing is read once more and gains it; a report's rule is untouched."""
    from spicy_regs.transforms.committee_report_reads import HEARING_BILL_LINK_RULE_VERSION, RULE_VERSIONS, complete

    before = f"{HEARING_BILL_LINK_RULE_VERSION};body=placeholder-pdf-002;mods=hearing-mods-001"
    assert not complete({"outcome": "complete", "rule_version": before}, "CHRG")
    assert complete({"outcome": "complete", "rule_version": RULE_VERSIONS["CHRG"]}, "CHRG")
    assert "mods" not in RULE_VERSIONS["CRPT"]


# --------------------------------------------------------------------------- #
# hearing_bill_links.committee_system_code, keyed as committees.system_code is (round 6, L16).
# --------------------------------------------------------------------------- #
#: House Armed Services, 2025-04-09: the one held hearing MODS whose committee authorityId is upper-case (`HSAS00`;
#: the committees table keys `hsas00`). It states no cover bill, so the tests add the first COVER element
#: CHRG-119hhrg60951 states, verbatim, for a link row to exist; the rest is the publisher's record.
JACKET_61487 = "CHRG-119hhrg61487"
_HASC = b'<congHASC congress="119" number="11"></congHASC>'
COVERED_61487 = (FIXTURES / f"mods-{JACKET_61487}.xml").read_bytes().replace(
    _HASC, _HASC + b'<bill congress="119" context="COVER" number="1676" type="HR"></bill>', 1
)


class Hearing61487(StubDiscovery):
    IDS = {"CRPT": [], "CHRG": [JACKET_61487]}


def _tables(paths) -> dict[str, list[dict]]:
    return {path.stem: pq.read_table(path).to_pylist() for path in paths}


def test_a_hearing_link_keys_its_committee_the_way_the_committees_table_does(tmp_path, monkeypatch):
    """The link takes its hearing's first committee code as hearing_transcripts states it (spicy-docs' shaper)."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    assert COVERED_61487.count(b'context="COVER"') == 1
    tables = _tables(build_committee_reports(
        tmp_path, reader=Hearing61487(), acquirer=StubBodyAcquirer(mods_bytes=COVERED_61487),
        hearings=StubHearings(details={}), download_prior=_no_prior,
    ))
    assert [row["committee_system_codes_json"] for row in tables["hearing_transcripts"]] == ['["hsas00"]']
    assert [(row["bill_id"], row["committee_system_code"]) for row in tables["hearing_bill_links"]] == [
        ("119-hr-1676", "hsas00")]


def test_a_held_link_takes_its_hearings_code_without_a_read_and_one_without_codes_keeps_its_own(tmp_path, monkeypatch):
    """The second run reads nothing: its held 61487 link, published unfolded before this change, takes `hsas00` from
    the held transcript row; a link whose hearing row predates the codes column (NULL) keeps the code it holds."""
    import pyarrow as pa
    from spicy_docs.schemas.committee_report_tables import HEARING_TRANSCRIPTS
    from spicy_docs.schemas.hearing_bill_link_tables import HEARING_BILL_LINKS

    from spicy_regs.transforms.committee_report_reads import READ_COLUMNS, READS_TABLE
    from spicy_regs.transforms.table_merge import prior_scratch_path

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    (tmp_path / "first").mkdir()
    first = _tables(build_committee_reports(
        tmp_path / "first", reader=Hearing61487(), acquirer=StubBodyAcquirer(mods_bytes=COVERED_61487),
        hearings=StubHearings(details={}), download_prior=_no_prior,
    ))
    (link,) = first["hearing_bill_links"]
    (hearing,) = first["hearing_transcripts"]
    (read,) = first[READS_TABLE]
    older = {"package_id": CHRG_ID, "last_modified": read["last_modified"]}
    held = {
        "hearing_transcripts": (HEARING_TRANSCRIPTS.columns, [hearing, older | {"committee_system_codes_json": None}]),
        "hearing_bill_links": (HEARING_BILL_LINKS.columns, [link | {"committee_system_code": "HSAS00"},
                                                           link | {"package_id": CHRG_ID, "committee_system_code": "HSHM00"}]),
        # Both read completely under the running rule (the first run's detail read was refused, which it retries).
        READS_TABLE: (READ_COLUMNS, [read | {"outcome": "complete"}, read | {"package_id": CHRG_ID, "outcome": "complete"}]),
    }
    second = tmp_path / "second"
    second.mkdir()
    for name, (columns, rows) in held.items():
        schema = pa.schema([(column, pa.string()) for column in columns])
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), prior_scratch_path(second, name))
    acquirer = StubBodyAcquirer()
    tables = _tables(build_committee_reports(
        second, reader=CrptOnlyDiscovery(), acquirer=acquirer, hearings=StubHearings(details={}),
        download_prior=_no_prior,
    ))
    assert acquirer.requested == [CRPT_ID], "neither hearing is read again"
    assert sorted((row["package_id"], row["committee_system_code"]) for row in tables["hearing_bill_links"]) == [
        (JACKET_61487, "hsas00"), (CHRG_ID, "HSHM00")]


def test_cap_deferral_alone_warns_and_names_refusals_first(tmp_path, monkeypatch, capsys):
    """A package left behind by the cap is retried next run: a warning, never a failure."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    coverage: dict = {}
    build_committee_reports(tmp_path, reader=TwoHearingsDiscovery(), acquirer=HearingBodyAcquirer(),
                            hearings=StubHearings(), download_prior=_no_prior, max_packages=1, coverage=coverage)
    assert (coverage["deferred"], coverage["pending"], coverage["nothing_read"]) == (1, 1, [])
    warning = [line for line in capsys.readouterr().out.splitlines() if "acquisition pending" in line]
    assert len(warning) == 1 and "0 refused (none), 1 deferred by the 1-package cap (CHRG-" in warning[0]


@pytest.mark.parametrize("broken", ["CRPT", "CHRG"])
def test_a_collection_that_reads_nothing_it_attempted_is_named_while_the_other_reads(tmp_path, monkeypatch, broken):
    """A reader regression that refuses every package of one collection must not hide behind the other's reads."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)

    class Broken(StubBodyAcquirer):
        def acquire(self, package_id: str, *, max_bytes=None, prefer=()):
            if broken == "CHRG":
                raise ValueError("stub: reader regression")
            return super().acquire(package_id, max_bytes=max_bytes, prefer=prefer)

        def acquire_parts(self, package_id: str, *, max_bytes=None, prefer=()):
            if broken == "CRPT":
                raise ValueError("stub: reader regression")
            return super().acquire_parts(package_id, max_bytes=max_bytes, prefer=prefer)

    coverage: dict = {}
    build_committee_reports(tmp_path, reader=StubDiscovery(), acquirer=Broken(), hearings=StubHearings(),
                            download_prior=_no_prior, coverage=coverage)
    assert coverage["nothing_read"] == [broken] and coverage["refused"] == 1


@pytest.mark.parametrize(("nothing_read", "fails"), [([], False), (["CRPT"], True)])
def test_the_rollup_fails_after_publishing_only_when_a_collection_read_nothing(tmp_path, monkeypatch,
                                                                               nothing_read, fails):
    from spicy_regs.pipelines.rollups.committee_reports import CommitteeReportsRollup

    def build_receipts(self, output_dir, builder, **kwargs):
        kwargs["coverage"].update(pending=57, deferred=56, nothing_read=nothing_read)
        return ()

    monkeypatch.setattr(CommitteeReportsRollup, "build_receipts", build_receipts)
    monkeypatch.delenv("COMMITTEE_REPORTS_REREAD", raising=False)
    rollup = CommitteeReportsRollup(output_dir=tmp_path)
    rollup.build(tmp_path)
    assert (rollup.deferred_failure is not None) is fails
    if fails:
        assert "read none of the packages attempted in CRPT" in str(rollup.deferred_failure)
