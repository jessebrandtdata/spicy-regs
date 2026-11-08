"""Committee seats from a Congressional Directory edition: mapping, members, ranks, the independent check, the build.

Hermetic. The printed facts come from spicy-docs' reader over retained originals
(``tests/fixtures/govinfo_directory/README.md``); this repository's seam is the
interpretation (committee codes, members, ranks and labels) and when an edition is read.
"""

from __future__ import annotations

import json
from pathlib import Path

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
    same = [(dr._Person("A", "H", "TX", "R", (), "lead"), ""), (dr._Person("B", "H", "TX", "R", (), "aide"), ""),
            (None, ""), (None, "")]  # fmt: skip
    # A vice chair of the chair's party (House style) leaves the minority's lead unprinted.
    assert dr._inferred_ranking(seats, same) == 3
    # A vice chair of the other party (Senate Ethics style) is the minority's lead.
    other = [same[0], (dr._Person("B", "S", "TX", "D", (), "aide"), ""), *same[2:]]
    assert dr._inferred_ranking(seats, other) is None
    printed = [*seats[:3], _seat("Right", "IL", "minority", 4, "Ranking Member")]
    assert dr._inferred_ranking(printed, same) is None


def test_an_unmatched_entry_keeps_its_rank_place_and_its_inference_is_not_shifted():
    taxation = {row["bioguide_id"]: row for row in interpret().rows if row["system_code"] == "jstx00"}
    assert (taxation["L000263"]["position"], taxation["L000263"]["position_basis"]) == ("Ranking Member", "inferred")
    result = interpret(terms=[t for t in REFERENCES["terms"] if t["bioguide_id"] != "L000263"])  # Sander Levin
    levin = [d for d in result.dispositions if d.kind == "unmatched_member" and "Levin" in d.text]
    assert len(levin) == 1 and "Ranking Member is not inferred" in levin[0].text
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
    _, assignments = _build(tmp_path, first, monkeypatch)
    assert first.read == list(dr.DIRECTORY_EDITIONS.values())  # the stub refuses all but the 113th
    rows = pq.read_table(assignments).to_pylist()
    directory_rows = [r for r in rows if r["congress_basis"] == "directory"]
    assert {r["congress"] for r in directory_rows} == {"113"} and {r["congress"] for r in rows} == {"113", "119"}

    # The next run's prior holds the 113th: it is not asked for again; the 114th-117th still are.
    seed(tmp_path, "committee_assignments", rows)
    again = StubDirectory()
    _build(tmp_path, again, monkeypatch)
    assert "CDIR-2014-02-18" not in again.read and len(again.read) == 4

    seed(tmp_path, "committee_assignments", rows)
    named = StubDirectory()
    _, reread = _build(tmp_path, named, monkeypatch, reread="113")
    assert named.read[0] == "CDIR-2014-02-18"
    assert len([r for r in pq.read_table(reread).to_pylist() if r["congress_basis"] == "directory"]) == len(
        directory_rows
    )


def test_without_evidence_or_a_directory_reader_no_edition_is_read(tmp_path, monkeypatch):
    monkeypatch.delenv(bcr.DIRECTORY_REREAD_ENV, raising=False)
    _, assignments = bcr.build_committee_rosters(
        tmp_path, reader=StubListingReader(), rosters=StubRosters(), download_prior=no_download
    )
    assert {r["congress_basis"] for r in pq.read_table(assignments).to_pylist()} == {"file", "caller"}
