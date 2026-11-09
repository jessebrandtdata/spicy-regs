"""The BILLSUM supplement: its offline reader, its plan, and one bill-family run end to end.

No network: the archives are built here from BILLSUM files shaped like the publisher's (118-hr-4291's file,
2026-11-09, trimmed), and the bill family runs over the fixture bill 119 HR 6028, whose BILLSTATUS states
summary versions 00 and 53.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.congress.bill_status import BillIdentity, BillSourceError
from spicy_docs.sources.congress.bulk_status import BulkListingEntry
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.sources.billsum import (
    BILLSUM_BULKDATA,
    BILLSUM_SCHEMA_CONTEXT,
    billsum_locator,
    read_billsum_archive,
    read_billsum_file,
    read_billsum_listing,
)
from spicy_regs.transforms.bill_family_billsum import (
    BILLSUM_ARCHIVES_KEY,
    billsum_congresses,
    prior_summary_pairs,
    run_billsum_supplement,
)
from spicy_regs.transforms.build_bill_family import ARCHIVES_TABLE, build_bill_family
from tests.test_bill_family import (
    StubBodyAcquirer,
    StubBulkAcquirer,
    _priors,
)

OBSERVED_AT = "2026-11-09T12:00:00Z"


def billsum_xml(congress: int, bill_type: str, number: int, summaries: list[tuple[str, str, str, str, str]]) -> bytes:
    """One BILLSUM file: ``summaries`` are (version code, update-date, action-date, action-desc, text)."""
    items = "".join(
        f'<summary summary-id="id{congress}{bill_type}{number}v{code}" currentChamber="HOUSE" update-date="{updated}">'
        f"<action-date>{action_date}</action-date><action-desc>{desc}</action-desc>"
        f"<summary-text><![CDATA[{body}]]></summary-text></summary>"
        for code, updated, action_date, desc, body in summaries
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?><BillSummaries>'
        f'<item congress="{congress}" measure-type="{bill_type}" measure-number="{number}" '
        f'measure-id="id{congress}{bill_type}{number}" originChamber="HOUSE" update-date="2026-10-26">'
        f"<title>A bill.</title>{items}</item>"
        '<dublinCore xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:format>text/xml</dc:format></dublinCore>'
        "</BillSummaries>"
    ).encode()


def zip_of(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in files.items():
            archive.writestr(name, body)
    return buffer.getvalue()


def locator(congress: int, bill_type: str) -> str:
    return f"{BILLSUM_BULKDATA}/{congress}/{bill_type}/BILLSUM-{congress}-{bill_type}.zip"


def capture(url: str, body: bytes, content_type: str = "application/zip") -> CapturedBodyResponse:
    return CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type=content_type,
        observed_at=OBSERVED_AT,
        body=body,
    )


def entry(
    congress: int, bill_type: str, *, stamp: str = "26-Oct-2026 21:15", size: int = 4_429_913
) -> BulkListingEntry:
    name = f"BILLSUM-{congress}-{bill_type}.zip"
    day, month, rest = stamp.split("-")
    year, clock = rest.split(" ")
    hour, minute = clock.split(":")
    months = {"Oct": 10, "Nov": 11}
    return BulkListingEntry(
        name=name,
        display_label=name,
        just_file_name=name,
        link=locator(congress, bill_type),
        folder=False,
        formatted_last_modified_time=stamp,
        modified_at=datetime(int(year), months[month], int(day), int(hour), int(minute), tzinfo=UTC),
        mime_type="application/zip",
        file_extension="zip",
        formatted_size="4.2 MB",
        size=size,
    )


# --- the offline reader -----------------------------------------------------------------------------------


def test_a_file_states_each_summary_literally_and_refuses_an_empty_text_alone():
    body = billsum_xml(
        118,
        "hr",
        4291,
        [
            (
                "00",
                "2026-10-26",
                "2023-06-22",
                "Introduced in House",
                "<p>This bill directs the U.S. Postal Service.</p>",
            ),
            ("07", "2026-10-27", "2023-09-01", "Reported to House", "  "),
        ],
    )
    summaries, refused = read_billsum_file(body, identity=BillIdentity(118, "hr", 4291))
    assert [(s.version_code, s.update_date, s.action_date, s.action_desc) for s in summaries] == [
        ("00", "2026-10-26", "2023-06-22", "Introduced in House")
    ]
    assert summaries[0].text == "<p>This bill directs the U.S. Postal Service.</p>"
    assert refused == ("BILLSUM summary states no text",)


def test_a_file_naming_another_bill_refuses_whole():
    body = billsum_xml(118, "hr", 4292, [("00", "2026-10-26", "2023-06-22", "Introduced in House", "<p>x</p>")])
    with pytest.raises(BillSourceError, match="another bill"):
        read_billsum_file(body, identity=BillIdentity(118, "hr", 4291))


def test_the_archive_reads_every_member_and_files_a_bad_one_as_its_own_refusal():
    files = {
        "BILLSUM-118hr4291.xml": billsum_xml(
            118, "hr", 4291, [("00", "2026-10-26", "2023-06-22", "Introduced in House", "<p>a</p>")]
        ),
        "BILLSUM-118s17.xml": billsum_xml(118, "s", 17, [("00", "2023-01-26", "2023-01-23", "Introduced", "<p>b</p>")]),
    }
    archive = read_billsum_archive(capture(locator(118, "hr"), zip_of(files)), congress=118, bill_type="hr")
    assert archive.summary_count == 1
    assert archive.refused_count == 1
    refused = next(member for member in archive.members if member.refusal)
    assert refused.refusal == "BILLSUM archive entry belongs to another Congress or bill type"


def test_an_archive_captured_from_another_address_refuses():
    with pytest.raises(BillSourceError, match="not the listed folder"):
        read_billsum_archive(capture(locator(118, "s"), zip_of({})), congress=118, bill_type="hr")


def test_the_listing_proves_its_zip_entry_and_refuses_another_folders_entry():
    zip_name = "BILLSUM-118-hr.zip"
    good = {
        "name": zip_name,
        "displayLabel": zip_name,
        "justFileName": zip_name,
        "folder": False,
        "link": locator(118, "hr"),
        "formattedLastModifiedTime": "26-Oct-2026 21:14",
        "size": 8_816_705,
        "mimeType": "application/zip",
        "fileExtension": "zip",
        "formattedSize": "8.4 MB",
    }
    listing = read_billsum_listing(json.dumps({"files": [good]}).encode(), congress=118, bill_type="hr")
    assert listing.zip_entry.size == 8_816_705
    stray = dict(good, name="BILLSUM-118s17.xml", link=f"{BILLSUM_BULKDATA}/118/s/BILLSUM-118s17.xml")
    with pytest.raises(BillSourceError):
        read_billsum_listing(json.dumps({"files": [good, stray]}).encode(), congress=118, bill_type="hr")


def test_bulk_begins_with_the_113th():
    with pytest.raises(BillSourceError, match="113"):
        billsum_locator(112, "hr")


# --- the plan -----------------------------------------------------------------------------------------------


class StubBillSum:
    """Serves one built zip per folder; records listings and zip downloads."""

    def __init__(self, folders: dict[tuple[int, str], dict[str, bytes]], *, entries=entry, refuse=()):
        self.folders = folders
        self.entries = entries
        self.refuse = set(refuse)
        self.listings: list[tuple[int, str]] = []
        self.zips: list[tuple[int, str]] = []

    def list_folder(self, congress: int, bill_type: str):
        from types import SimpleNamespace

        self.listings.append((congress, bill_type))
        if (congress, bill_type) in self.refuse:
            raise BillSourceError("stub: refused")
        listing = SimpleNamespace(congress=congress, bill_type=bill_type, zip_entry=self.entries(congress, bill_type))
        return SimpleNamespace(listing=listing)

    def acquire(self, listing):
        from types import SimpleNamespace

        key = (listing.congress, listing.bill_type)
        self.zips.append(key)
        captured = capture(locator(*key), zip_of(self.folders.get(key, {})))
        return SimpleNamespace(
            archive=read_billsum_archive(captured, congress=key[0], bill_type=key[1]), capture=captured
        )


HR_4291 = {
    "BILLSUM-118hr4291.xml": billsum_xml(
        118,
        "hr",
        4291,
        [
            ("00", "2026-10-26", "2023-06-22", "Introduced in House", "<p>a</p>"),
            ("07", "2026-10-30", "2023-09-01", "Reported to House", "<p>b</p>"),
        ],
    )
}


def _plan(source, **overrides: Any):
    arguments: dict[str, Any] = {
        "held": {},
        "status_pairs": {("118-hr-4291", "07")},
        "prior_billsum": set(),
        "bills": {"118-hr-4291"},
        "zip_cap": 10,
        "replay": False,
    }
    arguments.update(overrides)
    return run_billsum_supplement(source, (118,), ("hr",), **arguments)


def test_only_the_pairs_billstatus_does_not_state_become_rows():
    outcome = _plan(StubBillSum({(118, "hr"): HR_4291}))
    assert outcome.rows == [
        {
            "bill_id": "118-hr-4291",
            "summary_version_code": "00",
            "action_date": "2023-06-22",
            "action_desc": "Introduced in House",
            "update_date": "2026-10-26",
            "summary_html": "<p>a</p>",
            "summary_chars": "8",
            "last_summary_update_date": None,
            "source_schema_version": BILLSUM_SCHEMA_CONTEXT,
        }
    ]
    assert outcome.stated_by_status == 1
    assert outcome.held["118-hr"]["size"] == "4429913"


def test_an_unchanged_zip_is_not_downloaded():
    first = StubBillSum({(118, "hr"): HR_4291})
    held = _plan(first).held
    second = StubBillSum({(118, "hr"): HR_4291})
    outcome = _plan(second, held=held)
    assert second.zips == [] and outcome.unchanged == 1 and outcome.rows == []


def test_a_moved_zip_is_read_again():
    held = _plan(StubBillSum({(118, "hr"): HR_4291})).held
    moved = StubBillSum({(118, "hr"): HR_4291}, entries=lambda c, t: entry(c, t, stamp="06-Nov-2026 21:13"))
    assert _plan(moved, held=held).zips == 1


def test_a_replay_reads_every_zip_whatever_was_retained():
    held = _plan(StubBillSum({(118, "hr"): HR_4291})).held
    replayed = StubBillSum({(118, "hr"): HR_4291})
    assert len(_plan(replayed, held=held, replay=True, zip_cap=0).rows) == 1


def test_past_the_cap_a_folder_keeps_no_new_entry_and_is_read_later():
    source = StubBillSum({(118, "hr"): HR_4291})
    outcome = _plan(source, zip_cap=0)
    assert source.zips == [] and outcome.capped == 1 and "118-hr" not in outcome.held


def test_a_refused_folder_keeps_its_retained_entry():
    held = _plan(StubBillSum({(118, "hr"): HR_4291})).held
    outcome = _plan(StubBillSum({}, refuse={(118, "hr")}), held=held)
    assert outcome.refused_folders == 1 and outcome.held == held


def test_a_summary_of_an_unknown_bill_is_held_back_and_its_folder_read_again():
    outcome = _plan(StubBillSum({(118, "hr"): HR_4291}), bills=set())
    assert outcome.rows == [] and outcome.orphans == 1 and "118-hr" not in outcome.held


def test_a_billsum_pair_billstatus_now_states_is_retired():
    outcome = _plan(StubBillSum({}), prior_billsum={("118-hr-4291", "07"), ("118-hr-4291", "00")})
    assert outcome.superseded == {("118-hr-4291", "07", BILLSUM_SCHEMA_CONTEXT)}


def test_the_default_scope_is_the_current_congress_and_two_before(monkeypatch):
    monkeypatch.delenv("BILL_FAMILY_BILLSUM_CONGRESSES", raising=False)
    scope = billsum_congresses((119,), replay=False)
    assert len(scope) == 3 and scope == tuple(range(scope[0], scope[0] - 3, -1))
    monkeypatch.setenv("BILL_FAMILY_BILLSUM_CONGRESSES", "110-118")
    assert billsum_congresses((119,), replay=False) == tuple(range(118, 112, -1))
    assert billsum_congresses((119,), replay=True) == tuple(range(119, 112, -1))


# --- one bill-family run end to end ---------------------------------------------------------------------------

HR_6028 = {
    "BILLSUM-119hr6028.xml": billsum_xml(
        119,
        "hr",
        6028,
        [
            ("00", "2025-11-19", "2025-11-12", "Introduced in House", "<p>introduced</p>"),
            ("07", "2026-07-01", "2026-06-30", "Reported to House", "<p>reported</p>"),
        ],
    )
}


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.setenv("BILL_FAMILY_BILLSUM_CONGRESSES", "119")
    for name in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(name, raising=False)


def _run(output_dir: Path, billsum: StubBillSum, published: Path | None = None) -> dict[str, Path]:
    paths = build_bill_family(
        output_dir,
        bulk_acquirer=StubBulkAcquirer(),
        body_acquirer=StubBodyAcquirer(),
        summary_acquirer=billsum,
        **_priors(published),
    )
    return {path.stem: path for path in paths}


def _summaries(path: Path) -> list[dict]:
    rows = pq.read_table(path).to_pylist()
    return sorted(rows, key=lambda row: (row["summary_version_code"], row["update_date"]))


def test_a_run_publishes_the_summary_billstatus_lacks_beside_its_own(tmp_path, scoped):
    out = _run(tmp_path, StubBillSum({(119, "hr"): HR_6028}))
    rows = _summaries(out["bill_publisher_summaries"])
    assert [(row["summary_version_code"], row["source_schema_version"]) for row in rows] == [
        ("00", "3.0.0"),
        ("07", BILLSUM_SCHEMA_CONTEXT),
        ("53", "3.0.0"),
    ]
    added = rows[1]
    assert added["update_date"] == "2026-07-01" and added["last_summary_update_date"] is None
    metadata = pq.read_schema(out[ARCHIVES_TABLE]).metadata
    assert json.loads(metadata[BILLSUM_ARCHIVES_KEY.encode()])["119-hr"]["observed_at"] == OBSERVED_AT


def test_a_second_run_over_an_unchanged_zip_downloads_nothing_and_keeps_the_row(tmp_path, scoped):
    first = tmp_path / "first"
    first.mkdir()
    _run(first, StubBillSum({(119, "hr"): HR_6028}))
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    second = StubBillSum({(119, "hr"): HR_6028})
    out = _run(second_dir, second, published=first)
    assert second.zips == [] and second.listings == [(119, "hr")]
    assert ("07", BILLSUM_SCHEMA_CONTEXT) in {
        (row["summary_version_code"], row["source_schema_version"])
        for row in _summaries(out["bill_publisher_summaries"])
    }


def test_a_billsum_row_retires_once_billstatus_states_its_pair(tmp_path, scoped):
    first = tmp_path / "first"
    first.mkdir()
    # A BILLSUM 00 row published before BILLSTATUS stated 00 (seeded): this run's status read states 00.
    out = _run(first, StubBillSum({(119, "hr"): HR_6028}))
    path = out["bill_publisher_summaries"]
    table = pq.read_table(path)
    seeded = dict(
        table.to_pylist()[0],
        summary_version_code="00",
        update_date="2025-11-19",
        source_schema_version=BILLSUM_SCHEMA_CONTEXT,
        last_summary_update_date=None,
    )
    pq.write_table(table.from_pylist([*table.to_pylist(), seeded], schema=table.schema), path)
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    out = _run(second_dir, StubBillSum({(119, "hr"): HR_6028}), published=first)
    pairs = [
        (row["summary_version_code"], row["source_schema_version"])
        for row in _summaries(out["bill_publisher_summaries"])
    ]
    assert ("00", BILLSUM_SCHEMA_CONTEXT) not in pairs
    assert ("07", BILLSUM_SCHEMA_CONTEXT) in pairs and ("00", "3.0.0") in pairs


def test_a_prior_without_schema_context_counts_every_pair_as_billstatus(tmp_path):
    import pyarrow as pa

    path = tmp_path / "bill_publisher_summaries.parquet"
    pq.write_table(pa.table({"bill_id": ["118-hr-1", "117-hr-2"], "summary_version_code": ["00", "00"]}), path)
    assert prior_summary_pairs(path, (118,)) == ({("118-hr-1", "00")}, set())
