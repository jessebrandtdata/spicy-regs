"""Committee seats from a Congressional Directory edition: mapping, members, ranks, the independent check, the build.

Hermetic. The printed facts come from spicy-docs' reader over retained originals
(``tests/fixtures/govinfo_directory/README.md``); this repository's seam is the
interpretation (committee codes, members, ranks and labels) and when an edition is read.
"""

from __future__ import annotations

import json
from pathlib import Path

from dataclasses import replace

import pyarrow.parquet as pq
import pytest
import yaml
from spicy_docs.sources.govinfo.directory_rosters import PrintedSeat, parse_directory_granule, read_directory_mods

from spicy_regs.transforms import build_committee_rosters as bcr
from spicy_regs.transforms import directory_rosters as dr
from tests.test_committee_rosters import StubListingReader, StubRosters
from tests.test_incremental_rollups import no_download, seed

FIXTURES = Path(__file__).parent / "fixtures" / "govinfo_directory"
JOINT = "CDIR-2014-02-18-HOUSECOMMITTEES-4"
REFERENCES = json.loads((FIXTURES / "references-113-joint.json").read_text())
EDITION = read_directory_mods((FIXTURES / "CDIR-2014-02-18.mods.xml").read_bytes())
OBSERVED_AT = "2026-11-08T00:00:00Z"


def joint_granule():
    return parse_directory_granule((FIXTURES / f"{JOINT}.htm").read_bytes(), JOINT, "joint")


def interpret(granules=None, *, terms=None, edition=EDITION):
    members = {row["bioguide_id"]: row for row in REFERENCES["members"]}
    return dr.interpret_edition(
        edition,
        granules or [joint_granule()],
        committees=REFERENCES["committees"],
        terms=REFERENCES["terms"] if terms is None else terms,
        members=members,
        observed_at=OBSERVED_AT,
    )


# --------------------------------------------------------------------------- committee codes


def _committee(code, name, history=(), parent=None, chamber="Senate"):
    return {"system_code": code, "chamber": chamber, "name": name, "parent_system_code": parent,
            "history": [dict(entry) for entry in history]}  # fmt: skip


def _named(name, start, end=None):
    return {"libraryOfCongressName": name, "officialName": None, "startDate": start, "endDate": end}


def test_a_committee_title_maps_through_its_names_in_force_and_never_by_overlap():
    committees = [
        _committee("ssxx00", "Example Committee"),
        _committee("ssxx01", "Water Subcommittee", [_named("Water", "2015-01-01"), _named("Rivers", "2001-01-01",
                   "2014-12-31")], parent="ssxx00"),  # fmt: skip
        _committee("ssxx02", "Old Rivers Subcommittee", [_named("Rivers", "1990-01-01", "2000-12-31")], parent="ssxx00"),
        _committee("ssxx03", "Space, Science, and Industry Subcommittee",
                   [_named("Space, Science, and Industry", "2007-01-01", "2019-01-01")], parent="ssxx00"),
    ]  # fmt: skip
    names = dr.CommitteeNames(committees, "2014-02-18")
    assert names.resolve("senate", None, "Example") == ("ssxx00", "name of an active code")
    # Two codes once held "Rivers"; only one held it on the date.
    assert names.resolve("senate", "ssxx00", "Rivers") == ("ssxx01", "name in force")
    assert names.resolve("senate", "ssxx00", "Science and Space") == ("ssxx03", "subset of an active code's name")
    # Sharing words is not enough.
    assert names.resolve("senate", "ssxx00", "Water and Industry")[0] is None
    # The same title under another parent is another committee.
    assert names.resolve("senate", None, "Rivers")[0] is None


def test_reviewed_titles_and_the_joint_codes_congressional_tech_keeps():
    result = interpret()
    codes = {code for _, title, code, _ in result.mappings}
    assert codes == {"jsec00", "jslc00", "jspr00", "jstx00"}
    reviewed = dr.CommitteeNames([_committee("ssfr01", "Europe and Regional Security Cooperation Subcommittee",
                                             parent="ssfr00")], "2014-02-18")  # fmt: skip
    assert reviewed.resolve("senate", "ssfr00", "European Affairs") == ("ssfr01", "reviewed title")


# --------------------------------------------------------------------------- members, ranks and roles


def _seat(name, state, side, order, role=None, line=None):
    return PrintedSeat(JOINT, line or order, "Example Committee", None, name, state, role, role, side, order, None)


def test_the_first_minority_entry_is_the_inferred_ranking_member_unless_a_minority_leader_is_printed():
    seats = [_seat("Lead", "TX", "chair", 1, "Chair"), _seat("Aide", "TX", "chair", 2, "Vice Chair"),
             _seat("Left", "OH", "majority", 3), _seat("Right", "IL", "minority", 4)]  # fmt: skip
    lead, aide = dr._Person("A", "H", "TX", "R", (), "lead"), dr._Person("B", "H", "TX", "R", (), "aide")
    same = [(lead, "", None), (aide, "", None), (None, "", None), (None, "", None)]
    # A vice chair of the chair's party (House style) leaves the minority's lead unprinted.
    assert dr._inferred_ranking(seats, same, set()) == 3
    # A vice chair of the other party (Senate Ethics style) is the minority's lead.
    other = [same[0], (dr._Person("B", "S", "TX", "D", (), "aide"), "", None), *same[2:]]
    assert dr._inferred_ranking(seats, other, set()) is None
    printed = [*seats[:3], _seat("Right", "IL", "minority", 4, "Ranking Member")]
    assert dr._inferred_ranking(printed, same, set()) is None
    # So is any role printed in the minority column (the 118th Senate Ethics committee's Vice Chairman).
    assert dr._inferred_ranking([*seats[:3], _seat("Right", "IL", "minority", 4, "Vice Chair")], same, set()) is None


def test_an_unmatched_entry_keeps_its_rank_place_and_its_inference_is_not_shifted():
    taxation = {row["bioguide_id"]: row for row in interpret().rows if row["system_code"] == "jstx00"}
    assert (taxation["L000263"]["position"], taxation["L000263"]["position_basis"]) == ("Ranking Member", "inferred")
    result = interpret(terms=[t for t in REFERENCES["terms"] if t["bioguide_id"] != "L000263"])  # Sander Levin
    levin = [d for d in result.dispositions if d.kind == "unmatched_member" and "Levin" in d.text]
    assert len(levin) == 1 and "no Ranking Member is inferred" in levin[0].note
    taxation = {row["bioguide_id"]: row for row in result.rows if row["system_code"] == "jstx00"}
    rangel = taxation["R000053"]  # Charles B. Rangel, printed second in the minority column
    assert (rangel["rank"], rangel["position"]) == ("2", None)
    # The Joint Economic Committee prints its minority's lead as Vice Chair: nothing is inferred there.
    assert not any(r["position_basis"] == "inferred" for r in result.rows if r["system_code"] == "jsec00")


def test_every_printed_entry_is_a_row_or_a_labeled_disposition():
    granule = joint_granule()
    result = interpret([granule])
    seated = [d for d in result.dispositions if d.kind not in ("vacancy", "unread_text")]
    assert len(result.rows) + len(seated) == len(granule.seats)
    row = next(r for r in result.rows if r["system_code"] == "jspr00" and r["position"] == "Chair")
    assert row["bioguide_id"] == "S000148"  # Schumer, from the MODS
    assert (row["congress"], row["congress_basis"], row["edition_date"], row["position_basis"]) == (
        "113",
        "directory",
        "2014-02-18",
        "printed",
    )
    assert row["source_granule"] == JOINT and row["source_line"] == "98"
    assert {r["chamber"] for r in result.rows if r["system_code"] == "jspr00"} == {"house", "senate"}


# --------------------------------------------------------------------------- the independent record


def _independent():
    """congress-legislators at 53547039 (2014-02-13), the joint committees: another publisher's own record."""
    data = yaml.safe_load((FIXTURES / "legislators-113-joint.yaml").read_text())
    seats = {f"j{code[1:].lower()}00": {m["bioguide"] for m in members} for code, members in data.items()}
    chairs = {f"j{code[1:].lower()}00": m["bioguide"] for code, members in data.items() for m in members
              if (m.get("title") or "").lower() in {"chair", "chairman"}}  # fmt: skip
    return seats, chairs


def _compare(rows):
    seats, chairs = _independent()
    ours: dict[str, set[str]] = {}
    for row in rows:
        ours.setdefault(row["system_code"], set()).add(row["bioguide_id"])
    our_chairs = {r["system_code"]: r["bioguide_id"] for r in rows if r["position"] == "Chair"}
    return (
        {code: (sorted(ours.get(code, set()) - seats[code]), sorted(seats[code] - ours.get(code, set())))
         for code in seats | ours if ours.get(code, set()) != seats.get(code, set())},
        {code: (our_chairs.get(code), chairs[code]) for code in chairs if our_chairs.get(code) != chairs[code]},
    )  # fmt: skip


def test_the_113th_joint_committees_agree_with_an_independent_record_both_ways():
    differences, chair_mismatches = _compare(interpret().rows)
    assert chair_mismatches == {}
    # The one difference: legislators lists Gregg Harper on the Library committee, whose CDIR list prints him as
    # its chair and on Printing as vice chair; CDIR prints the Library chair line as "Gregg Harper, Representative
    # from Mississippi, Chair", which is read, so any difference here is a regression.
    assert differences == {}


def test_a_planted_wrong_chair_and_a_dropped_seat_are_flagged():
    rows = [dict(row) for row in interpret().rows]
    chair = next(r for r in rows if r["system_code"] == "jstx00" and r["position"] == "Chair")
    other = next(r for r in rows if r["system_code"] == "jstx00" and r["position"] is None)
    chair["position"], other["position"] = None, "Chair"
    dropped = next(r for r in rows if r["system_code"] == "jslc00" and r["position"] is None)
    rows.remove(dropped)
    differences, chair_mismatches = _compare(rows)
    assert chair_mismatches == {"jstx00": (other["bioguide_id"], chair["bioguide_id"])}
    assert differences == {"jslc00": ([], [dropped["bioguide_id"]])}


# --------------------------------------------------------------------------- when an edition is read


class StubDirectory:
    def __init__(self):
        self.read: list[str] = []

    def edition(self, package_id):
        self.read.append(package_id)
        edition = EDITION if package_id == "CDIR-2014-02-18" else None
        if edition is None:
            raise dr.DirectoryRosterError(f"{package_id}: stub has no fixture")
        return dr.DirectoryCapture(edition, (joint_granule(),), OBSERVED_AT)


def _references_download(remote_key, local_path):
    import pyarrow as pa

    table = {"members.parquet": REFERENCES["members"], "member_terms.parquet": REFERENCES["terms"]}.get(remote_key)
    if table is None:
        return False
    pq.write_table(pa.Table.from_pylist(table), local_path)
    return True


@pytest.fixture(autouse=True)
def scoped(monkeypatch):
    monkeypatch.setattr(bcr, "current_congress", lambda: 119)


def _build(tmp_path, directory, monkeypatch, reread=""):
    monkeypatch.setenv(bcr.DIRECTORY_REREAD_ENV, reread)
    seed(tmp_path, "committees", [
        {"system_code": c["system_code"], "chamber": c["chamber"], "name": c["name"],
         "history_json": json.dumps(c["history"]), "update_date": "2026-01-01", "detail_captured": "true"}
        for c in REFERENCES["committees"]
    ])  # fmt: skip
    return bcr.build_committee_rosters(
        tmp_path, reader=StubListingReader(), rosters=StubRosters(), directory=directory,
        download_prior=lambda key, path: _references_download(key, path) or no_download(key, path),
    )  # fmt: skip


def test_an_edition_is_read_once_kept_after_and_re_read_only_when_named(tmp_path, monkeypatch):
    first = StubDirectory()
    _, assignments, entries = _build(tmp_path, first, monkeypatch)
    # The stub refuses all but the 113th; the 118th is the manual edition.
    assert first.read == [*dr.DIRECTORY_EDITIONS.values(), dr.MANUAL_EDITIONS[118].package_id]
    rows = pq.read_table(assignments).to_pylist()
    directory_rows = [r for r in rows if r["congress_basis"] == "directory"]
    assert {r["congress"] for r in directory_rows} == {"113"} and {r["congress"] for r in rows} == {"113", "119"}

    # The next run's prior holds the 113th: it is not asked for again; the 114th-117th still are.
    seed(tmp_path, "committee_assignments", rows)
    again = StubDirectory()
    _build(tmp_path, again, monkeypatch)
    assert "CDIR-2014-02-18" not in again.read and len(again.read) == 5

    seed(tmp_path, "committee_assignments", rows)
    named = StubDirectory()
    _, reread, _ = _build(tmp_path, named, monkeypatch, reread="113")
    assert named.read[0] == "CDIR-2014-02-18"
    assert len([r for r in pq.read_table(reread).to_pylist() if r["congress_basis"] == "directory"]) == len(
        directory_rows
    )


def test_without_evidence_or_a_directory_reader_no_edition_is_read(tmp_path, monkeypatch):
    monkeypatch.delenv(bcr.DIRECTORY_REREAD_ENV, raising=False)
    _, assignments, _ = bcr.build_committee_rosters(
        tmp_path, reader=StubListingReader(), rosters=StubRosters(), download_prior=no_download
    )
    assert {r["congress_basis"] for r in pq.read_table(assignments).to_pylist()} == {"file", "caller"}


# --------------------------------------------------------------------------- corrections and labels


def _person(bioguide, chamber, state, party, name):
    return dr._Person(bioguide, chamber, state, party, (dr.normalize_name(name),), dr.normalize_name(name).split()[-1])


def test_a_misprinted_name_or_state_is_corrected_only_when_one_member_fits():
    index = dr.MemberIndex([
        _person("T1", "S", "NC", "R", "Thom Tillis"), _person("R1", "H", "OH", "R", "James B. Renacci"),
        _person("G1", "H", "TX", "R", "Tony Gonzales"), _person("G2", "H", "TX", "D", "Vicente Gonzalez"),
        _person("B1", "H", "NY", "D", "Jamaal Bowman"),
    ])  # fmt: skip
    assert index.resolve_labeled("Thom Tills", "NC", ("S",))[0::2] == (_person("T1", "S", "NC", "R", "Thom Tillis"),
                                                                       "corrected")  # fmt: skip
    renacci = index.resolve_labeled("James B. Renacci", "SC", ("H",))[0]
    assert renacci is not None and renacci.bioguide_id == "R1"
    assert index.resolve_labeled("Jamaal Bowman, NY", "NY", ("H",))[2] == "corrected"
    # The printed surname is Tony Gonzales's; the first name is Vicente Gonzalez's.
    person, _, label = index.resolve_labeled("Vicente Gonzales", "TX", ("H",))
    assert person is not None and (person.bioguide_id, label) == ("G2", "corrected")
    assert index.resolve_labeled("Nobody Atall", "TX", ("H",))[0] is None


def test_a_member_printed_after_leaving_is_kept_and_labeled():
    terms = [*REFERENCES["terms"], {"bioguide_id": "Z000001", "term_type": "sen", "term_start": "2007-01-04",
                                    "term_end": "2013-01-03", "term_state": "NM", "term_party": "Democrat"}]  # fmt: skip
    members = {row["bioguide_id"]: row for row in REFERENCES["members"]} | {
        "Z000001": {"bioguide_id": "Z000001", "name_first": "Zed", "name_last": "Former"}
    }
    granule = joint_granule()
    seats = [replace(granule.seats[-1], name="Zed Former", state="NM", member_chamber="S")]
    result = dr.interpret_edition(EDITION, [replace(granule, seats=seats, dispositions=[])],
                                  committees=REFERENCES["committees"], terms=terms, members=members,
                                  observed_at=OBSERVED_AT)  # fmt: skip
    assert [(r["bioguide_id"], r["entry_label"]) for r in result.rows] == [("Z000001", "after_departure")]


def test_an_entry_in_the_other_partys_column_is_labeled_and_nothing_is_inferred():
    seats = [_seat("Lead", "TX", "chair", 1, "Chair"), _seat("Left", "OH", "majority", 2),
             _seat("Wrong", "MI", "minority", 3), _seat("Right", "IL", "minority", 4)]  # fmt: skip
    people = [(dr._Person("A", "S", "TX", "D", (), "lead"), "", None), (dr._Person("B", "S", "OH", "D", (), "l"), "", None),
              (dr._Person("C", "S", "MI", "D", (), "w"), "", None), (dr._Person("D", "S", "IL", "R", (), "r"), "", None)]  # fmt: skip
    crossed = dr._crossed_columns(seats, people)
    assert crossed == {2}
    assert dr._inferred_ranking(seats, people, crossed) is None
    assert dr._ranks(seats, crossed) == {0: 1, 1: 2, 2: None, 3: 1}


def test_a_body_without_a_congress_gov_code_is_published_under_its_directory_code():
    names = dr.CommitteeNames([], "2016-02-12")
    assert names.resolve("house", "hsba00", "Task Force to Investigate Terrorism Financing") == (
        "hsba00-tf-terrorism-financing", "no Congress.gov code",
    )  # fmt: skip


def test_every_disposition_is_a_labeled_entry_row():
    result = interpret()
    granule = joint_granule()
    assert len(result.entries) == len(result.dispositions)
    assert len(result.rows) + sum(e["kind"] == "unresolved_seat" for e in result.entries) == len(granule.seats)
    assert {e["kind"] for e in result.entries} <= {"vacancy", "not_a_committee", "unread_text", "unresolved_seat",
                                                   "duplicate_seat"}  # fmt: skip


# --------------------------------------------------------------------------- the 118th


def test_the_manual_edition_is_read_only_as_pinned(monkeypatch):
    edition, granules = dr.manual_edition(dr.MANUAL_EDITIONS[118])
    assert (edition.package_id, edition.congress, edition.date_issued) == ("CDIR-2024-04-25", "118", "2024-04-25")
    assert sum(len(g.seats) for g in granules) == 3722
    assert sum(len(g.dispositions) for g in granules) == 73  # vacancies
    first = granules[0].seats[0]
    assert (first.line, first.committee, first.name, first.role, first.side) == (
        "612:12", "Agriculture, Nutrition, and Forestry", "Debbie Stabenow", "Chair", "chair",
    )  # fmt: skip
    with pytest.raises(dr.DirectoryRosterError, match="does not match its pin"):
        dr.manual_edition(replace(dr.MANUAL_EDITIONS[118], entries_sha256="0" * 64))


def test_reconciliation_keeps_the_chamber_files_row_and_reports_each_difference():
    def row(code, bioguide, basis, position=None):
        return {"system_code": code, "bioguide_id": bioguide, "congress_basis": basis, "position": position}

    directory = [row("hsru00", "C1", "directory-manual", "Chair"), row("hsru00", "B1", "directory-manual"),
                 row("hsru00", "X1", "directory-manual"), row("ssap01", "S1", "directory-manual")]  # fmt: skip
    files = [row("hsru00", "B1", "file", "Chairman"), row("hsru00", "C1", "file"), row("hsru00", "F1", "file")]
    result = dr.reconcile(directory, files)
    assert sorted((r["system_code"], r["bioguide_id"], r["congress_basis"]) for r in result.rows) == [
        ("hsru00", "B1", "file"), ("hsru00", "C1", "file"), ("hsru00", "F1", "file"), ("hsru00", "X1", "directory-manual"),
        ("ssap01", "S1", "directory-manual"),
    ]  # fmt: skip
    report = result.report
    assert report["seat_differences"] == {"hsru00": {"directory_only": ["X1"], "chamber_file_only": ["F1"]}}
    assert report["leader_differences"]["Chair"] == {"hsru00": {"directory": "C1", "chamber_file": "B1"}}
    assert report["committees_directory_only"] == ["ssap01"]
