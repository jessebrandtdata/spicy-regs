"""Hermetic tests for the same-case docket groups side table, on real rows of CourtListener's 2026-06-30 edition.

Each fixture is a published key's native records plus the same court's dockets numbered within five serials of it,
copied from the native edition (``round6/impl-C2/m5/fixture_rows.parquet``), written back in the export's own
dialect so the build reads them through the same reader as the real ``dockets-<date>.csv.bz2``.
"""
from __future__ import annotations

import bz2

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.build_court_docket_groups import SCHEMA, build_court_docket_groups

# The published words, spelled out so a change to them fails here.
PER_DEFENDANT_DOCKETS, DUPLICATE_RECORDS = "per_defendant_dockets", "duplicate_records"
SAME_CAPTION, SAME_DATE_CONTAINED_CAPTION = "same_caption", "same_date_contained_caption"

_NATIVE = ("id", "court_id", "docket_number", "pacer_case_id", "case_name", "date_filed", "federal_defendant_number")


def _row(court, number, docket_id, pacer, name, filed, defendant=None):
    return dict(zip(_NATIVE, (docket_id, court, number, pacer, name, filed, defendant)))


def _neighbours(court, numbers_and_ids):
    return [_row(court, number, f"n-{court}-{number}", pacer, "Neighbour v. Case", "2020-01-01")
            for number, pacer in numbers_and_ids]


def _export(rows) -> bytes:
    """``rows`` as CourtListener writes an export: every value quoted with backslash escapes, NULL unquoted empty.

    The reader finds records by a newline, a quote and a digit, which the real export's leading numeric ``id`` gives;
    these fixtures' ids are not all numeric, so a leading row number stands in for it.
    """
    def value(text):
        return "" if text is None else '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'

    lines = ["seq," + ",".join(_NATIVE)]
    lines += [",".join([value(str(number)), *(value(row[column]) for column in _NATIVE)])
              for number, row in enumerate(rows, 1)]
    return bz2.compress(("\n".join(lines) + "\n").encode())


# almd 2:24-cv-00567: a docket object CourtListener created in 2020 with PACER id "16", reused for this 2024 case.
CLEMENTS = [_row("almd", "2:24-cv-00567", "18766389", "16", "Clements v. Becerra", "2024-09-04"),
            _row("almd", "2:24-cv-00567", "69130490", "84126", "Clements v. Becerra", "2024-09-04")]
ALMD = _neighbours("almd", [("2:24-cv-00562", "84059"), ("2:24-cv-00563", "84060"), ("2:24-cv-00565", "84124"),
                            ("2:24-cv-00566", "84125"), ("2:24-cv-00568", "84127"), ("2:24-cv-00572", "84130")])
# dcd 1:20-cv-01104: five of the case's twenty records; only 217431 lies among its court's neighbouring ids.
SAMMA = [_row("dcd", "1:20-cv-01104", docket_id, pacer, "SAMMA v. U.S. DEPARTMENT OF DEFENSE", "2020-04-28")
         for docket_id, pacer in (("10086531", "45170"), ("4203564", "118188"), ("17106856", "217431"),
                                  ("60654941", "281271"), ("61595056", "1142280"))]
DCD = _neighbours("dcd", [("1:20-cv-01099", "217411"), ("1:20-cv-01100", "217417"), ("1:20-cv-01101", "217418"),
                          ("1:20-cv-01102", "217419"), ("1:20-cv-01103", "217420"), ("1:20-cv-01105", "217434"),
                          ("1:20-cv-01106", "217897"), ("1:20-cv-01107", "217459"), ("1:20-cv-01108", "217453"),
                          ("1:20-cv-01109", "217441")])
# casd 3:13-cr-00492: seventeen defendants' dockets with consecutive PACER ids 406701-406717.
MILLAN = [_row("casd", "3:13-cr-00492", docket_id, str(406701 + i), "United States v. Millan", "2013-02-08")
          for i, docket_id in enumerate(("14396048", "49322664", "49322670", "49322720", "8479038", "49322820",
                                         "49322797", "49322848", "49323229", "6015069", "6269493", "4192508",
                                         "7737228", "49323227", "49323072", "14547125", "49323128"))]
CASD = _neighbours("casd", [("3:13-cr-00490", "406563"), ("3:13-cr-00491", "406674"), ("3:13-cr-00493", "406722"),
                            ("3:13-cr-00494", "406911"), ("3:13-cr-00496", "406825")])
# nysd 1:25-cv-01510: one case CourtListener holds as five records, captioned two ways on one filing date.
TRUMP = [_row("nysd", "1:25-cv-01510", docket_id, pacer, name, "2025-02-21") for docket_id, pacer, name in (
    ("69663206", "11421001348837", "v. Trump"), ("69713359", "179823212758882", "City of New York v. Trump"),
    ("69663207", "637338", "City of New York v. Trump"), ("69664101", "665119140952121", "v. Trump"),
    ("69663202", "961835826161651", "v. Trump"))]
NYSD = _neighbours("nysd", [("1:25-cv-01505", "637327"), ("1:25-cv-01506", "637330"), ("1:25-cv-01509", "637335"),
                            ("1:25-cv-01512", "637346"), ("1:25-cv-01515", "637349")])
# ca6 24-5119: an appellate case held as a RECAP record and an id-less one; appellate numbers have no serial sequence.
BLACK_FARMERS = [_row("ca6", "24-5119", docket_id, pacer, "Black Farmers Agriculturalists Association, Inc. v. "
                      "Brooke Rollins", "2024-02-07") for docket_id, pacer in (("68922994", "150283"), ("68372045", None))]


def _build(tmp_path, published, neighbours=()):
    """Run the real build: ``published`` rows are the selection, ``neighbours`` only live in the native edition."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    string = pa.string()
    dockets, native = tmp_path / "court_dockets.parquet", tmp_path / "dockets-2026-06-30.csv.bz2"
    pq.write_table(pa.Table.from_pylist(
        [{"cl_docket_id": r["id"], "court_id": r["court_id"], "docket_number": r["docket_number"]} for r in published],
        schema=pa.schema([("cl_docket_id", string), ("court_id", string), ("docket_number", string)])), dockets)
    # Another court's dockets: the build reads the whole export and keeps only the published keys' courts.
    native.write_bytes(_export([*published, *neighbours, _row("zzz", "1:20-cv-00001", "77", "1", "Other v. Court",
                                                                "2020-01-01")]))
    out = build_court_docket_groups(tmp_path / "out", dockets_file=dockets, native_file=native, edition="2026-06-30")
    table = pq.read_table(out)
    assert table.schema == SCHEMA
    from spicy_regs.court_receipts import read_court_rows

    return {row["cl_docket_id"]: row for row in read_court_rows(out, dataset="court_docket_groups")}


def _groups(rows):
    """{representative: (tier, match basis, sorted member ids)} for a build's rows."""
    groups = {}
    for docket_id, row in sorted(rows.items()):
        rep = row["parent_cl_docket_id"]
        groups.setdefault(rep, (row["confidence_tier"], row["match_basis"], []))[2].append(docket_id)
        assert (row["confidence_tier"], row["match_basis"]) == groups[rep][:2]
        assert row["rule_version"] == "2" and row["edition"] == "2026-06-30"
    return groups


def test_the_representative_is_the_courts_own_id_not_a_reused_object(tmp_path):
    assert _groups(_build(tmp_path, CLEMENTS, ALMD)) == {
        "69130490": (DUPLICATE_RECORDS, SAME_CAPTION, ["18766389", "69130490"])}


def test_the_representative_is_the_id_in_its_courts_sequence_not_the_lowest(tmp_path):
    """Rule 1 picked 45170 for SAMMA v. DoD; its neighbours' ids run 217411-217897."""
    assert _groups(_build(tmp_path, SAMMA, DCD)) == {
        "17106856": (DUPLICATE_RECORDS, SAME_CAPTION, sorted(r["id"] for r in SAMMA))}


def test_consecutive_criminal_ids_are_per_defendant_dockets(tmp_path):
    """Rule 1 called this case `refiled` (a spread of 16); PACER opened all seventeen together."""
    assert _groups(_build(tmp_path, MILLAN, CASD)) == {
        "14396048": (PER_DEFENDANT_DOCKETS, SAME_CAPTION, sorted(r["id"] for r in MILLAN))}


def test_one_case_captioned_two_ways_on_one_date_is_one_group(tmp_path):
    """Rule 1 left these five ungrouped; four of the ids are RECAP's 14-15 digit values, out of sequence."""
    assert _groups(_build(tmp_path, TRUMP, NYSD)) == {
        "69663207": (DUPLICATE_RECORDS, SAME_DATE_CONTAINED_CAPTION, sorted(r["id"] for r in TRUMP))}


def test_an_appellate_case_held_twice_is_duplicate_records_under_rule_1s_choice(tmp_path):
    """No serial sequence to test: the representative is the lowest numeric id, and one id is no consecutive run."""
    assert _groups(_build(tmp_path, BLACK_FARMERS)) == {
        "68922994": (DUPLICATE_RECORDS, SAME_CAPTION, ["68372045", "68922994"])}


# --- edges -------------------------------------------------------------------


def _pair(pacers, names=("A v. B", "A v. B"), dates=("2020-01-01", "2020-01-01"), defendants=(None, None),
          number="1:20-cv-00001"):
    return [_row("dcd", number, f"d{i}", pacer, name, filed, defendant)
            for i, (pacer, name, filed, defendant) in enumerate(zip(pacers, names, dates, defendants))]


def test_ids_five_apart_without_a_sequence_stay_per_defendant_dockets_as_in_rule_1(tmp_path):
    assert _groups(_build(tmp_path, _pair(["616724", "616729"]))) == {
        "d0": (PER_DEFENDANT_DOCKETS, SAME_CAPTION, ["d0", "d1"])}
    assert _groups(_build(tmp_path / "wider", _pair(["616724", "616730"]))) == {
        "d0": (DUPLICATE_RECORDS, SAME_CAPTION, ["d0", "d1"])}


def test_a_defendant_number_makes_per_defendant_dockets_whatever_the_ids(tmp_path):
    assert _groups(_build(tmp_path, _pair(["100", "900000"], defendants=(None, "2")))) == {
        "d0": (PER_DEFENDANT_DOCKETS, SAME_CAPTION, ["d0", "d1"])}


def test_with_no_member_in_sequence_the_representative_is_the_lowest_numeric_id(tmp_path):
    rows = _build(tmp_path, _pair(["5", "7000000"]), _neighbours("dcd", [("1:20-cv-00002", "210000")]))
    assert _groups(rows) == {"d0": (DUPLICATE_RECORDS, SAME_CAPTION, ["d0", "d1"])}


def test_a_neighbour_of_two_published_keys_counts_for_both(tmp_path):
    first = _pair(["16", "210001"], number="1:20-cv-00001")
    second = [_row("dcd", "1:20-cv-00003", f"e{i}", pacer, "C v. D", "2020-01-01")
              for i, pacer in enumerate(["3", "210003"])]
    groups = _groups(_build(tmp_path, first + second, _neighbours("dcd", [("1:20-cv-00002", "210002")])))
    assert set(groups) == {"d1", "e1"}


def test_reused_numbers_missing_ids_and_singletons_stay_ungrouped(tmp_path):
    reused = _pair(["1", "2"], names=("Healy v. Dukes", "G.C.P. v. DHS"), dates=("2019-05-01", "2021-03-02"))
    contained_other_day = _pair(["1", "2"], names=("v. Trump", "City of New York v. Trump"),
                                dates=("2025-02-21", "2025-02-24"), number="1:20-cv-00002")
    no_ids = _pair(["", None], number="1:20-cv-00003")
    no_captions = _pair(["1", "2"], names=(None, ""), number="1:20-cv-00004")
    single = _pair(["1"], number="1:20-cv-00005")
    assert _build(tmp_path, reused + contained_other_day + no_ids + no_captions + single) == {}


def test_the_same_case_test_reads_captions_the_way_the_enrichment_did():
    """The APA enrichment admitted sibling records with this test (court-dockets-qualification, 2026-09-22)."""
    from spicy_regs.transforms.build_court_docket_groups import caption_key, same_case

    assert caption_key("Weise v. U.S. Dept. of Labor & Industry") == "weise v united states department of labor and industry"
    weise = {"case_name": "Weise v. United States", "date_filed": "2020-01-01"}
    assert same_case(weise, {"case_name": "WEISE v. U.S.", "date_filed": "2021-06-30"}) == SAME_CAPTION
    assert same_case({"case_name": "v. Trump", "date_filed": "2025-02-21"},
                     {"case_name": "City of New York v. Trump", "date_filed": "2025-02-21"}) == SAME_DATE_CONTAINED_CAPTION
    assert same_case({"case_name": "", "date_filed": "2025-02-21"},
                     {"case_name": "City of New York v. Trump", "date_filed": "2025-02-21"}) is None


# --- the build's place in the repository -----------------------------------


def test_the_rollup_writes_a_generation_whose_parent_is_the_court_dockets_it_read(tmp_path):
    """A local run (no bucket): the root records the selection's bytes as its parent through the standard writer."""
    import hashlib
    import json
    import tomllib
    from pathlib import Path

    from spicy_regs.pipelines.rollups.court_docket_groups import CourtDocketGroupsRollup

    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    _build(tmp_path / "inputs", CLEMENTS, ALMD)
    out = tmp_path / "run"
    out.mkdir()
    dockets = (tmp_path / "inputs" / "court_dockets.parquet").read_bytes()
    (out / "court_dockets.parquet").write_bytes(dockets)
    # The export is read where the raw archive (conftest's SPICYGOV_RAW_ROOT) keeps it, by its digest.
    export = (tmp_path / "inputs" / "dockets-2026-06-30.csv.bz2").read_bytes()
    digest = "sha256:" + hashlib.sha256(export).hexdigest()
    CentralRawArchiveStore(tmp_path / "central-raw").put_blob(digest, len(export), [export])
    CourtDocketGroupsRollup(native_sha256=digest, edition="2026-06-30", output_dir=out).run()
    (root,) = (out / "generations").glob("*/artifact.json")
    spec = json.loads(root.read_text())["spec"]
    assert spec["family"] == "court-docket-groups"
    assert spec["tables"]["court_docket_groups.parquet"]["rows"] == 2
    assert spec["parents"] == {"court_dockets.parquet": {"sha256": "sha256:" + hashlib.sha256(dockets).hexdigest(),
                                                         "byteSize": len(dockets)}}
    scripts = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["run-rollup-court-docket-groups"] == "spicy_regs.pipelines.rollups.court_docket_groups:app"


def test_the_rollup_refuses_a_missing_edition_or_an_undated_one(tmp_path):
    import pytest

    from spicy_regs.pipelines.rollups.court_docket_groups import CourtDocketGroupsRollup

    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    CentralRawArchiveStore(tmp_path / "central-raw")  # an archive without that export
    with pytest.raises(FileNotFoundError):
        CourtDocketGroupsRollup(native_sha256="sha256:" + "0" * 64, edition="2026-06-30")
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        CourtDocketGroupsRollup(native_sha256="sha256:" + "0" * 64, edition="june")
