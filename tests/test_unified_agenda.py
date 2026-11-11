"""Hermetic tests for the Unified Agenda ingest (no network).

Covers the two pieces with real logic: parsing the reginfo.gov
``REGINFO_RIN_DATA`` XML export into normalized RIN dicts
(``UnifiedAgendaReader.iter_records`` / ``_normalize``) and mapping those onto
the 18-column raw input schema (``_shape``, including timetable-date derivation
and the deterministic per-RIN URL). The HTTP transport returns fixtures without network access.
"""

from __future__ import annotations

import json

import httpx
import pytest
from spicy_docs.sources.unified_agenda import UnifiedAgendaSourceError
from loguru import logger

from spicy_regs.sources import unified_agenda
from spicy_regs.sources.unified_agenda import UnifiedAgendaReader
from spicy_regs.transforms.build_unified_agenda import COLUMNS, _iso_dates, _shape

# A two-record slice of the real export, matching the observed tag structure:
# <REGINFO_RIN_DATA> root, repeated <RIN_INFO> records, nested AGENCY / CFR_LIST /
# LEGAL_AUTHORITY_LIST / TIMETABLE_LIST. The second record exercises empty lists
# and an unparseable ("To Be Determined") timetable date.
_FIXTURE_XML = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<REGINFO_RIN_DATA RUN_DATE="2025-11-01-05:00">
    <RIN_INFO>
        <RIN>2060-AV12</RIN>
        <PUBLICATION>
            <PUBLICATION_ID>202510</PUBLICATION_ID>
            <PUBLICATION_TITLE>The Unified Agenda</PUBLICATION_TITLE>
        </PUBLICATION>
        <AGENCY>
            <CODE>2060</CODE>
            <NAME>Environmental Protection Agency</NAME>
            <ACRONYM>EPA</ACRONYM>
        </AGENCY>
        <PARENT_AGENCY>
            <CODE>2000</CODE>
            <NAME>Environmental Protection Agency</NAME>
            <ACRONYM>EPA</ACRONYM>
        </PARENT_AGENCY>
        <RULE_TITLE>A Planned Rule</RULE_TITLE>
        <ABSTRACT>Plans to do a thing.</ABSTRACT>
        <PRIORITY_CATEGORY>Other Significant</PRIORITY_CATEGORY>
        <RIN_STATUS>Active</RIN_STATUS>
        <RULE_STAGE>Proposed Rule Stage</RULE_STAGE>
        <MAJOR>Yes</MAJOR>
        <CFR_LIST>
            <CFR>40 CFR 60</CFR>
            <CFR>40 CFR 63</CFR>
        </CFR_LIST>
        <LEGAL_AUTHORITY_LIST>
            <LEGAL_AUTHORITY>42 U.S.C. 7401</LEGAL_AUTHORITY>
        </LEGAL_AUTHORITY_LIST>
        <TIMETABLE_LIST>
            <TIMETABLE>
                <TTBL_ACTION>NPRM</TTBL_ACTION>
                <TTBL_DATE>06/15/2024</TTBL_DATE>
                <FR_CITATION>89 FR 12345</FR_CITATION>
            </TIMETABLE>
            <TIMETABLE>
                <TTBL_ACTION>NPRM Comment Period End</TTBL_ACTION>
                <TTBL_DATE>08/14/2024</TTBL_DATE>
            </TIMETABLE>
            <TIMETABLE>
                <TTBL_ACTION>Final Rule</TTBL_ACTION>
                <TTBL_DATE>03/01/2025</TTBL_DATE>
            </TIMETABLE>
        </TIMETABLE_LIST>
    </RIN_INFO>
    <RIN_INFO>
        <RIN>1234-AB56</RIN>
        <PUBLICATION>
            <PUBLICATION_ID>202510</PUBLICATION_ID>
        </PUBLICATION>
        <AGENCY>
            <CODE>1234</CODE>
            <NAME>Some Agency</NAME>
        </AGENCY>
        <RULE_TITLE>Another Rule</RULE_TITLE>
        <RULE_STAGE>Long-Term Actions</RULE_STAGE>
        <CFR_LIST/>
        <LEGAL_AUTHORITY_LIST/>
        <TIMETABLE_LIST>
            <TIMETABLE>
                <TTBL_ACTION>Interim Final Rule</TTBL_ACTION>
                <TTBL_DATE>To Be Determined</TTBL_DATE>
            </TIMETABLE>
        </TIMETABLE_LIST>
    </RIN_INFO>
</REGINFO_RIN_DATA>
"""


def _read_fixture(edition: str = "202510") -> list[dict]:
    """Read the two-record fixture through a MockTransport serving ``edition``'s XML."""

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.params["f"] == f"REGINFO_RIN_DATA_{edition}.xml"
        return httpx.Response(200, stream=httpx.ByteStream(_FIXTURE_XML), headers={"content-type": "application/xml"})

    reader = UnifiedAgendaReader(editions=(edition,), transport=httpx.MockTransport(respond))
    return list(reader.iter_records())


def test_reader_parses_records_and_stamps_edition():
    records = _read_fixture("202510")
    assert [r["rin"] for r in records] == ["2060-AV12", "1234-AB56"]
    # Edition is stamped on every record (half the dedup key).
    assert all(r["agenda_edition"] == "202510" for r in records)
    # AGENCY/ACRONYM wins for agency_code, falling back to CODE when absent.
    assert records[0]["agency_code"] == "EPA"
    assert records[1]["agency_code"] == "1234"
    assert records[0]["agency_name"] == "Environmental Protection Agency"


def test_reader_normalizes_lists_and_timetable():
    rec = _read_fixture("202510")[0]
    assert rec["cfr_references"] == ["40 CFR 60", "40 CFR 63"]
    assert rec["legal_authority"] == ["42 U.S.C. 7401"]
    assert rec["timetable"][0] == {
        "action": "NPRM",
        "date": "06/15/2024",
        "fr_citation": "89 FR 12345",
    }
    # Optional child (FR_CITATION) absent -> None.
    assert rec["timetable"][1]["fr_citation"] is None


def test_shape_produces_exact_schema():
    row = _shape(_read_fixture("202510")[0])
    assert set(row) == set(COLUMNS)


def test_shape_maps_serializes_and_derives():
    row = _shape(_read_fixture("202510")[0])
    assert row["rin"] == "2060-AV12"
    assert row["agenda_edition"] == "202510"
    assert row["rule_stage"] == "Proposed Rule Stage"
    assert row["priority_category"] == "Other Significant"
    assert row["major"] == "Yes"
    assert row["publication_id"] == "202510"
    # Array fields serialize to JSON strings.
    assert json.loads(row["timetable_json"])[0]["action"] == "NPRM"
    assert json.loads(row["cfr_references_json"]) == ["40 CFR 60", "40 CFR 63"]
    assert json.loads(row["legal_authority_json"]) == ["42 U.S.C. 7401"]
    # Timetable dates -> ISO; first = earliest, next = earliest later action.
    assert row["first_action_date"] == "2024-06-15"
    assert row["next_action_date"] == "2024-08-14"
    # Deterministic per-RIN reginfo detail URL.
    assert row["url"] == "https://www.reginfo.gov/public/do/eAgendaViewRule?pubId=202510&RIN=2060-AV12"


def test_shape_handles_missing_and_unparseable_dates():
    row = _shape(_read_fixture("202510")[1])
    # No parseable timetable dates -> both action dates null.
    assert row["first_action_date"] is None
    assert row["next_action_date"] is None
    # Empty lists still serialize to "[]".
    assert row["cfr_references_json"] == "[]"
    assert row["legal_authority_json"] == "[]"
    # Absent fields are null.
    assert row["abstract"] is None
    assert row["priority_category"] is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("06/15/2024", ["2024-06-15"]),
        ("02/29/2024", ["2024-02-29"]),  # a real leap day survives
        ("06/00/2024", ["2024-06-01"]),  # reginfo's month-only marker
        ("02/30/2024", []),  # never existed — dropped, not moved to the 29th
        ("02/29/2023", []),  # not a leap year
        ("06/32/2024", []),  # the old clamp made this 2024-06-31
        ("13/01/2024", []),  # ``date()`` now owns the month range; the hand check is gone
        ("00/15/2024", []),  # the ``00`` marker is the day's alone — month 0 stays invalid
        ("To Be Determined", []),
    ],
)
def test_iso_dates_rejects_impossible_calendar_dates(raw, expected):
    assert _iso_dates([{"action": "NPRM", "date": raw}]) == expected


def _warnings_from(timetable: list) -> list[str]:
    """Collect WARNING records emitted while ``_iso_dates`` runs."""
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        assert _iso_dates(timetable) == []
    finally:
        logger.remove(sink)
    return messages


def test_an_impossible_date_says_so_in_the_run_log():
    """Dropping is otherwise silent, so an edition that starts emitting impossible
    dates would shrink the published table with nothing in the log to explain it."""
    assert any("02/30/2024" in m for m in _warnings_from([{"date": "02/30/2024"}]))


def test_an_unparseable_date_stays_quiet():
    """``To Be Determined`` is reginfo's own placeholder and appears 538 times in a
    single published edition; warning on it would bury the impossible-date case."""
    assert _warnings_from([{"date": "To Be Determined"}]) == []


def test_shape_drops_impossible_dates_without_fabricating_a_neighbour():
    doc = _read_fixture("202510")[0]
    doc["timetable"] = [
        {"action": "NPRM", "date": "02/30/2024"},
        {"action": "Final Rule", "date": "07/04/2024"},
    ]
    row = _shape(doc)
    # The impossible date is absent, so the real one becomes the first action.
    assert row["first_action_date"] == "2024-07-04"
    assert row["next_action_date"] is None
    # ``timetable_json`` still carries the raw date verbatim.
    assert json.loads(row["timetable_json"])[0]["date"] == "02/30/2024"


def test_download_rejects_non_xml_body():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, stream=httpx.ByteStream(b"<html>listing page</html>"), headers={"content-type": "text/html"}
        )
    )
    with pytest.raises(UnifiedAgendaSourceError):
        list(UnifiedAgendaReader(editions=("202510",), transport=transport).iter_records())


def test_default_edition_is_yyyymm():
    ed = unified_agenda.DEFAULT_EDITION
    assert len(ed) == 6 and ed.isdigit()
    assert ed[4:] in {"04", "10"}


def test_the_series_runs_from_fall_1995_to_the_current_edition_without_unreadable_ones():
    from spicy_regs.transforms.build_unified_agenda import UNREADABLE_EDITIONS, edition_series

    series = edition_series("202510")
    assert series[0] == "199510" and series[-1] == "202510"
    assert "2012" in series and "201210" not in series  # the publisher's one off-pattern file name
    assert not UNREADABLE_EDITIONS & set(series)
    assert len(series) == 60  # retained census with exact qualified 2004 repairs, 2026-10-10
    assert {"200404", "200410"} <= set(series)


def test_each_run_refreshes_the_current_edition_then_backfills_the_newest_missing(monkeypatch):
    from spicy_regs.transforms import build_unified_agenda as module

    fetch = module.editions_to_fetch({"202510", "202504", "201210"}, "202510")
    assert fetch[:3] == ("202510", "202410", "202404")
    assert "2012" not in fetch  # held as its stated edition, 201210
    assert len(fetch) == len(module.edition_series()) - 2  # every edition but the two other held ones
    monkeypatch.setattr(module, "BACKFILL_EDITIONS_PER_RUN", 20)
    assert len(module.editions_to_fetch({"202510", "202504", "201210"}, "202510")) == 21  # the bound holds


def _report(status: int, body: str) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(status, text=body, headers={"content-type": "text/html"}))


def test_the_newest_edition_is_the_newest_the_publisher_lists():
    """The report page (2026-09-26) links each edition's XML; a Spring 2026 link would move the series."""
    page = '<a href="XMLViewFileAction?f=REGINFO_RIN_DATA_2012.xml">'
    page += "".join(f'<a href="XMLViewFileAction?f=REGINFO_RIN_DATA_{e}.xml">' for e in ("202504", "202510"))
    assert unified_agenda.newest_edition(transport=_report(200, page)) == "202510"
    newer = page + '<a href="XMLViewFileAction?f=REGINFO_RIN_DATA_202604.xml">'
    assert unified_agenda.newest_edition(transport=_report(200, newer)) == "202604"


@pytest.mark.parametrize(
    ("status", "body"), [(503, "down"), (200, "<html>redesigned</html>"), (200, "REGINFO_RIN_DATA_202410.xml")]
)
def test_an_unreadable_or_older_edition_report_keeps_the_known_edition(status, body):
    assert unified_agenda.newest_edition(transport=_report(status, body)) == unified_agenda.DEFAULT_EDITION


def test_the_legacy_2012_file_is_keyed_by_the_edition_its_records_state():
    body = _FIXTURE_XML.replace(b"<PUBLICATION_ID>202510</PUBLICATION_ID>", b"<PUBLICATION_ID>201210</PUBLICATION_ID>")

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.params["f"] == "REGINFO_RIN_DATA_2012.xml"
        return httpx.Response(200, stream=httpx.ByteStream(body), headers={"content-type": "application/xml"})

    rows = list(UnifiedAgendaReader(editions=("2012",), transport=httpx.MockTransport(respond)).iter_records())
    assert {row["agenda_edition"] for row in rows} == {"201210"}


def test_the_first_run_is_complete_and_a_smaller_bound_completes_over_runs(tmp_path, monkeypatch):
    import sys

    import pyarrow.parquet as pq

    import spicy_regs.transforms.build_unified_agenda  # noqa: F401 -- the package re-exports the function by this name

    module = sys.modules["spicy_regs.transforms.build_unified_agenda"]

    class Reader:
        def __init__(self, *, editions, on_source=None, evidence=None):
            self.editions = editions

        def iter_records(self):
            for stem in self.editions:
                edition = "201210" if stem == "2012" else stem
                yield {"rin": f"0000-{edition}", "agenda_edition": edition, "timetable": []}

    monkeypatch.setattr(module, "UnifiedAgendaReader", Reader)
    monkeypatch.setattr(module.r2, "download", lambda *args: False)
    monkeypatch.setattr(module, "newest_edition", lambda: "202510")
    every = {"201210" if stem == "2012" else stem for stem in module.edition_series()}

    def runs(number):
        counts = []
        for _ in range(number):
            out = module.build_unified_agenda(tmp_path)
            editions = set(pq.read_table(out)["agenda_edition"].to_pylist())
            counts.append(len(editions))
            out.replace(tmp_path / "_ua_prior.parquet")  # the next run's prior table
        (tmp_path / "_ua_prior.parquet").unlink()
        return counts, editions

    assert runs(2) == ([60, 60], every)  # a first publication under a new column holds it for every edition
    monkeypatch.setattr(module, "BACKFILL_EDITIONS_PER_RUN", 20)
    assert runs(4) == ([21, 41, 60, 60], every)  # and a smaller bound keeps every prior row as it converges


def _agenda(*records: tuple[str, str]) -> bytes:
    return (
        "<REGINFO_RIN_DATA>"
        + "".join(
            f"<RIN_INFO><RIN>{rin}</RIN><PUBLICATION><PUBLICATION_ID>202004</PUBLICATION_ID></PUBLICATION>"
            f"{fields}</RIN_INFO>"
            for rin, fields in records
        )
        + "</REGINFO_RIN_DATA>"
    ).encode()


def _related(*pairs: tuple[str, str]) -> str:
    return (
        "<RELATED_RIN_LIST>"
        + "".join(f"<RELATED_RIN><RIN>{rin}</RIN><RIN_RELATION>{relation}</RIN_RELATION></RELATED_RIN>" for rin, relation in pairs)
        + "</RELATED_RIN_LIST>"
    )


# Literal publisher values: 1125-AB08 (202004) states " 1615-NYD" with the cut-off "Related t";
# 1904-AF61 (202404) lists 1904-AF53 first and third; 80 related RINs across the editions name their own row.
_RELATED_EDITION = _agenda(
    ("1125-AB08", _related((" 1615-NYD", "Related t"))),
    ("1904-AF61", _related(("1904-AF53", "Related to"), ("1904-AF54", "Related to"), ("1904-AF53", "Previously reported as"))),
    ("1545-BL70", _related(("1545-BL70", "Related to"))),
    ("1545-BL71", "<RELATED_RIN_LIST/>"),
    ("1545-BL72", ""),
)


def test_related_rins_are_stripped_but_never_shape_checked_or_deduplicated():
    from spicy_regs.sources.unified_agenda import normalize_retained_edition

    rows = {row["rin"]: row for row in normalize_retained_edition(_RELATED_EDITION, edition="202004")}
    assert rows["1125-AB08"]["related_rins"] == [{"rin": "1615-NYD", "rin_relation": "Related t"}]
    assert [(item["rin"], item["rin_relation"]) for item in rows["1904-AF61"]["related_rins"]] == [
        ("1904-AF53", "Related to"),
        ("1904-AF54", "Related to"),
        ("1904-AF53", "Previously reported as"),
    ]
    assert rows["1545-BL70"]["related_rins"] == [{"rin": "1545-BL70", "rin_relation": "Related to"}]
    # A stated empty list and an absent one stay distinct: [] and NULL.
    assert rows["1545-BL71"]["related_rins"] == []
    assert rows["1545-BL72"]["related_rins"] is None
    assert _shape(rows["1545-BL71"])["related_rins_json"] == "[]"
    assert _shape(rows["1545-BL72"])["related_rins_json"] == "null"
    assert json.loads(_shape(rows["1125-AB08"])["related_rins_json"]) == rows["1125-AB08"]["related_rins"]


def test_related_rins_strip_agrees_with_the_spicy_docs_projection_collapse():
    """The table strips; SpicyDocs' projection collapses. They agree only while no value holds an internal run."""
    from spicy_docs.sources.unified_agenda import UnifiedAgendaEdition
    from spicy_docs.sources.unified_agenda.projection import project_unified_agenda_edition

    from spicy_regs.sources.unified_agenda import normalize_retained_edition

    projected: list = []
    project_unified_agenda_edition(_RELATED_EDITION, edition=UnifiedAgendaEdition("202004"), on_record=projected.append)
    table = normalize_retained_edition(_RELATED_EDITION, edition="202004")
    assert [[(item.rin, item.rin_relation) for item in row.related_rins] for row in projected] == [
        [(item["rin"], item["rin_relation"]) for item in row["related_rins"] or ()] for row in table
    ]


def test_the_backfill_rereads_editions_restored_before_related_rins_were_read(tmp_path, monkeypatch):
    """A v1-restored edition (related_rins_json SQL NULL) is not held; a read edition with no list ("null") is."""
    import sys

    import pyarrow as pa
    import pyarrow.parquet as pq

    import spicy_regs.transforms.build_unified_agenda  # noqa: F401 -- the package re-exports the function by this name

    module = sys.modules["spicy_regs.transforms.build_unified_agenda"]
    requested: list[str] = []

    class Reader:
        def __init__(self, *, editions, on_source=None, evidence=None):
            requested.extend(editions)
            self.editions = editions

        def iter_records(self):
            for stem in self.editions:
                edition = "201210" if stem == "2012" else stem
                yield {"rin": f"0000-{edition}", "agenda_edition": edition, "timetable": [], "related_rins": []}

    unread = {"202404", "201210"}
    prior = [
        {**dict.fromkeys(COLUMNS), "rin": f"0000-{edition}", "agenda_edition": edition,
         "related_rins_json": None if edition in unread else "null"}
        for edition in ("201210" if stem == "2012" else stem for stem in module.edition_series())
    ]
    pq.write_table(pa.Table.from_pylist(prior, schema=module._SCHEMA), tmp_path / "_ua_prior.parquet")
    monkeypatch.setattr(module, "UnifiedAgendaReader", Reader)
    monkeypatch.setattr(module, "newest_edition", lambda: "202510")
    out = module.build_unified_agenda(tmp_path, download_prior=lambda *args: False)
    assert requested == ["202510", "202404", "2012"]
    rows = {row["agenda_edition"]: row["related_rins_json"] for row in pq.read_table(out).to_pylist()}
    assert len(rows) == 60 and None not in rows.values()
    assert rows["202404"] == rows["201210"] == "[]" and rows["202310"] == "null"


def test_a_reread_that_drops_a_restored_row_holds_the_edition_afterwards(tmp_path, monkeypatch):
    """A v1 row the v2 re-read no longer lists stays NULL, and the edition is held, not re-fetched daily."""
    import sys

    import pyarrow as pa
    import pyarrow.parquet as pq

    import spicy_regs.transforms.build_unified_agenda  # noqa: F401 -- the package re-exports the function by this name

    module = sys.modules["spicy_regs.transforms.build_unified_agenda"]
    requested: list[str] = []
    warnings: list[str] = []

    class Reader:
        def __init__(self, *, editions, on_source=None, evidence=None):
            requested.extend(editions)
            self.editions = editions

        def iter_records(self):
            for stem in self.editions:
                edition = "201210" if stem == "2012" else stem
                yield {"rin": f"0000-{edition}", "agenda_edition": edition, "timetable": [], "related_rins": []}

    prior = [
        {**dict.fromkeys(COLUMNS), "rin": f"0000-{edition}", "agenda_edition": edition,
         "related_rins_json": None if edition == "202404" else "null"}
        for edition in ("201210" if stem == "2012" else stem for stem in module.edition_series())
    ]
    prior.append({**dict.fromkeys(COLUMNS), "rin": "9999-AA00", "agenda_edition": "202404"})
    pq.write_table(pa.Table.from_pylist(prior, schema=module._SCHEMA), tmp_path / "_ua_prior.parquet")
    monkeypatch.setattr(module, "UnifiedAgendaReader", Reader)
    monkeypatch.setattr(module, "newest_edition", lambda: "202510")
    monkeypatch.setattr(module.logger, "warning", lambda message, *args: warnings.append(message.format(*args)))
    out = module.build_unified_agenda(tmp_path, download_prior=lambda *args: False)
    assert requested == ["202510", "202404"]
    assert [w for w in warnings if "not in the re-read" in w] == [
        "Unified Agenda: 1 restored rows in editions read this run were not in the re-read; "
        "their related_rins stay unread (NULL)"]
    dropped = {(row["rin"], row["related_rins_json"]) for row in pq.read_table(out).to_pylist()
               if row["agenda_edition"] == "202404"}
    assert dropped == {("0000-202404", "[]"), ("9999-AA00", None)}

    out.rename(tmp_path / "_ua_prior.parquet")
    requested.clear()
    module.build_unified_agenda(tmp_path, download_prior=lambda *args: False)
    assert requested == ["202510"]


def test_a_prior_without_the_related_rins_column_is_reread_from_the_newest_edition(tmp_path, monkeypatch):
    import sys

    import pyarrow as pa
    import pyarrow.parquet as pq

    import spicy_regs.transforms.build_unified_agenda  # noqa: F401 -- the package re-exports the function by this name

    module = sys.modules["spicy_regs.transforms.build_unified_agenda"]
    requested: list[str] = []

    class Reader:
        def __init__(self, *, editions, on_source=None, evidence=None):
            requested.extend(editions)

        def iter_records(self):
            return iter(())

    seventeen = pa.schema([(name, pa.string()) for name in COLUMNS if name != "related_rins_json"])
    pq.write_table(pa.Table.from_pylist([{"rin": "0000-AA00", "agenda_edition": "202504"}], schema=seventeen),
                   tmp_path / "_ua_prior.parquet")
    monkeypatch.setattr(module, "UnifiedAgendaReader", Reader)
    monkeypatch.setattr(module, "newest_edition", lambda: "202510")
    module.build_unified_agenda(tmp_path, download_prior=lambda *args: False)
    assert requested[:3] == ["202510", "202504", "202410"] and len(requested) == len(module.edition_series())
