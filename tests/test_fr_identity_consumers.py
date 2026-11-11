"""Dated FR joins, labelled and listed dockets, unpadded numbers and explicit ambiguity across the materialized consumers.

These controlled source rows exercise joins; the unmodified native collision
fixture and its byte provenance are tested in test_federal_register.py.
"""

import json
import shutil
import sys

import pyarrow.parquet as pq
import pytest
from loguru import logger

from spicy_docs.interpretation.identifier_shapes import normalize_docket_reference

from spicy_regs.ontology.common import RunContext, stable_id, write_parquet_rows
from spicy_regs.ontology import federal_register
from spicy_regs.ontology.federal_register import FederalRegisterIndex, linked_docket_ids
from spicy_regs.pipelines import rulemaking_dataset
from spicy_regs.transforms.build_comment_periods import build_comment_periods
from spicy_regs.pipelines.rulemaking_dataset import stage_text
from spicy_regs.transforms.build_federal_register import merge_federal_register
from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links
from spicy_regs.transforms.build_proceedings import build_proceedings
from spicy_regs.transforms.build_regulatory_agenda import build_regulatory_agenda
from spicy_regs.transforms.build_rule_targets import build_rule_targets


def _write(root, name, columns, rows):
    write_parquet_rows(root / f"{name}.parquet", columns=columns, rows=rows)


def _inputs(root):
    records = [
        {
            "document_number": "00-111",
            "publication_date": "2000-01-14",
            "type": "Proposed Rule",
            "title": "First controlled rule",
            "docket_ids": ["EPA-HQ-OAR-2000-0001"],
            "cfr_references": [{"title": 40, "part": 60}],
            "regulation_id_numbers": ["1111-AA11"],
            "comments_close_on": "2000-02-01",
        },
        {
            "document_number": "00-111",
            "publication_date": "2000-01-18",
            "type": "Proposed Rule",
            "title": "Second controlled rule",
            "docket_ids": ["FAA-2000-0002"],
            "cfr_references": [{"title": 49, "part": 71}],
            "regulation_id_numbers": ["2222-BB22"],
            "comments_close_on": "2000-02-01",
        },
    ]
    merge_federal_register(root, documents=lambda start: iter(records), download_prior=lambda key, path: False)
    build_fr_docket_links(root)
    stage_text("federal_register", root / "federal_register.parquet")  # as the materializer reads it
    _write(
        root,
        "dockets",
        ("docket_id", "docket_type", "rin"),
        [
            {"docket_id": docket, "docket_type": "Rulemaking"}
            for docket in ("EPA-HQ-OAR-2000-0001", "FAA-2000-0002", "FDA-UNKNOWN")
        ],
    )
    # A Regulations.gov posting date is not an FR publication-date qualifier.
    _write(
        root,
        "documents",
        ("document_id", "docket_id", "fr_doc_num", "additional_rins", "posted_date"),
        [
            {
                "document_id": "FDA-UNKNOWN-0001",
                "docket_id": "FDA-UNKNOWN",
                "fr_doc_num": "00-111",
                "additional_rins": "[]",
                "posted_date": "2000-01-14",
            }
        ],
    )
    _write(root, "unified_agenda", ("rin", "agenda_edition"), [])


def test_dated_bridge_never_cross_wires_colliding_documents(tmp_path):
    _inputs(tmp_path)
    links = pq.read_table(tmp_path / "fr_docket_links.parquet").to_pylist()
    assert {(r["docket_id"], r["publication_date"]) for r in links} == {
        ("EPA-HQ-OAR-2000-0001", "2000-01-14"),
        ("FAA-2000-0002", "2000-01-18"),
    }
    targets = pq.read_table(build_rule_targets(tmp_path)).to_pylist()
    assert {(r["docket_id"], r["cfr_ref"], r["rin"]) for r in targets if r["source"] == "fr_cfr_ref"} == {
        ("EPA-HQ-OAR-2000-0001", "40-60", "1111-AA11"),
        ("FAA-2000-0002", "49-71", "2222-BB22"),
    }
    unknown = next(r for r in targets if r["source"] == "document_fr_doc")
    assert unknown["cfr_ref"] is None and unknown["rin"] is None
    (reference,) = json.loads(unknown["fr_references_json"])
    assert reference == {
        "source": "documents.fr_doc_num",
        "evidence_id": "FDA-UNKNOWN-0001",
        "document_number": "00-111",
        "publication_date": None,
        "status": "ambiguous",
        "candidate_ids": ["00-111@2000-01-14", "00-111@2000-01-18"],
    }

    # An old collapsed FR-only row is a candidate predecessor of either date,
    # so neither date may silently inherit its identity.
    _write(
        tmp_path,
        "_proceedings_prior",
        ("proceeding_id", "docket_ids_json", "fr_document_numbers_json"),
        [{"proceeding_id": "legacy-collapsed", "docket_ids_json": "[]", "fr_document_numbers_json": '["00-111"]'}],
    )
    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    by_docket = {json.loads(r["docket_ids_json"])[0]: r for r in proceedings}
    assert len(proceedings) == 3 and "legacy-collapsed" not in {r["proceeding_id"] for r in proceedings}
    for docket, day in (("EPA-HQ-OAR-2000-0001", "2000-01-14"), ("FAA-2000-0002", "2000-01-18")):
        row = by_docket[docket]
        assert json.loads(row["fr_document_numbers_json"]) == ["00-111"]
        assert json.loads(row["fr_document_ids_json"]) == [f"00-111@{day}"]
        assert json.loads(row["unresolved_fr_references_json"])[0]["status"] == "ambiguous"
        assert json.loads(row["stage_events_json"])[0]["evidence_id"] == f"00-111@{day}"

    _, relationships_path = build_regulatory_agenda(tmp_path)
    relationships = pq.read_table(relationships_path).to_pylist()
    assert {(r["rin"], r["proceeding_id"], r["evidence_id"]) for r in relationships} == {
        ("1111-AA11", by_docket["EPA-HQ-OAR-2000-0001"]["proceeding_id"], "00-111@2000-01-14"),
        ("2222-BB22", by_docket["FAA-2000-0002"]["proceeding_id"], "00-111@2000-01-18"),
    }
    assert all("/documents/2000/01/" in r["evidence_uri"] for r in relationships)
    periods = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    assert len(periods) == 2
    for period in periods:
        (docket,) = json.loads(period["docket_ids_json"])
        assert json.loads(period["proceeding_ids_json"]) == [by_docket[docket]["proceeding_id"]]
        assert json.loads(period["evidence_ids_json"]) == json.loads(by_docket[docket]["fr_document_ids_json"])

    shutil.copyfile(tmp_path / "proceedings.parquet", tmp_path / "_proceedings_prior.parquet")
    repeated = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    assert {r["proceeding_id"] for r in repeated} == {r["proceeding_id"] for r in proceedings}
    assert {r["proceeding_id"]: r["unresolved_fr_references_json"] for r in repeated} == {
        r["proceeding_id"]: r["unresolved_fr_references_json"] for r in proceedings
    }


def test_legacy_ambiguous_proceeding_is_unresolved_in_agenda_and_periods(tmp_path):
    _inputs(tmp_path)
    _write(
        tmp_path,
        "proceedings",
        ("proceeding_id", "docket_ids_json", "fr_document_numbers_json"),
        [{"proceeding_id": "legacy-collapsed", "docket_ids_json": "[]", "fr_document_numbers_json": '["00-111"]'}],
    )
    # An old bridge lacks date qualifiers, too. No whole-output refusal and no
    # selection of a colliding FR row are permitted.
    _write(
        tmp_path,
        "fr_docket_links",
        ("document_number", "docket_id"),
        [{"document_number": "00-111", "docket_id": "EPA-HQ-OAR-2000-0001"}],
    )
    items_path, relationships_path = build_regulatory_agenda(tmp_path)
    assert pq.read_table(relationships_path).num_rows == 0
    items = pq.read_table(items_path).to_pylist()
    assert len(items) == 2
    assert all(r["scope_status"] == "unresolved" for r in items)
    assert all(json.loads(r["unresolved_fr_references_json"])[0]["status"] == "ambiguous" for r in items)
    periods = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    assert len(periods) == 2
    assert len({r["comment_period_id"] for r in periods}) == 2
    assert all(r["proceeding_ids_json"] == r["docket_ids_json"] == "[]" for r in periods)
    assert all(json.loads(r["unresolved_fr_references_json"]) for r in periods)


def _index(root, rows):
    _write(root, "federal_register", ("document_number", "publication_date"), rows)
    return FederalRegisterIndex(root / "federal_register.parquet")


def test_a_number_resolves_on_its_comparison_key_only_when_the_key_names_one_document(tmp_path):
    index = _index(
        tmp_path,
        [
            {"document_number": "2010-2394", "publication_date": "2010-02-05"},
            {"document_number": "2013-00123", "publication_date": "2013-01-04"},
            {"document_number": "E9-9366", "publication_date": "2009-04-24"},
            # Two documents the Register itself numbers apart, one key once unpadded.
            {"document_number": "94-0190", "publication_date": "1994-04-26"},
            {"document_number": "94-190", "publication_date": "1994-01-05"},
            {"document_number": "2018-28359", "publication_date": "2018-12-31"},
            {"document_number": "2015-00674", "publication_date": "2015-01-14"},
            # Corrections: a five-digit tail keys, and so does the Register's short-tail form.
            {"document_number": "C1-2013-00123", "publication_date": "2013-02-01"},
            {"document_number": "C1-2012-9978", "publication_date": "2012-05-01"},
        ],
    )

    def resolve(number, day=None):
        reference = index.reference(number, day)
        return reference["status"], reference["candidate_ids"]

    # Regulations.gov pads where the Register did not.
    assert resolve("2010-02394") == ("unpadded_single_candidate_in_input", ["2010-2394@2010-02-05"])
    assert resolve("E9-09366") == ("unpadded_single_candidate_in_input", ["E9-9366@2009-04-24"])
    assert resolve("2010-02394", "2010-02-05") == ("unpadded_dated", ["2010-2394@2010-02-05"])
    assert resolve("2010-02394", "2010-02-06") == ("missing", [])
    # Both sides reduce: an unpadded reference reaches the Register's padded number.
    assert resolve("2013-123") == ("unpadded_single_candidate_in_input", ["2013-00123@2013-01-04"])
    # The exact spelling is tried first, so a held number never goes through the key.
    assert resolve("2013-00123") == ("single_candidate_in_input", ["2013-00123@2013-01-04"])
    assert resolve("94-190") == ("single_candidate_in_input", ["94-190@1994-01-05"])
    assert resolve("94-190", "1994-04-26") == ("missing", [])
    # A key two held numbers share is refused, not chosen between.
    assert resolve("94-00190") == ("ambiguous", ["94-0190@1994-04-26", "94-190@1994-01-05"])
    assert resolve("94-00190", "1994-04-26") == ("ambiguous", ["94-0190@1994-04-26", "94-190@1994-01-05"])
    assert resolve("2010-09999") == ("missing", [])
    assert resolve("SSA-2010-0037") == ("missing", [])
    # Only a dash or case differs: a fold, not an unpadding, and the status says so.
    assert resolve("2018–28359") == ("folded_single_candidate_in_input", ["2018-28359@2018-12-31"])
    assert resolve("2018–28359", "2018-12-31") == ("folded_dated", ["2018-28359@2018-12-31"])
    assert resolve("c1-2013-00123") == ("folded_single_candidate_in_input", ["C1-2013-00123@2013-02-01"])
    # Both sides padded, to different widths: neither is the unpadded number, so no match.
    assert resolve("2015-0674") == ("missing", [])
    assert resolve("2015–0674") == ("missing", [])
    assert resolve("2015-674") == ("unpadded_single_candidate_in_input", ["2015-00674@2015-01-14"])
    # A correction's short tail keys to itself since SpicyDocs 0.31.0 (the key is a fixed
    # point, because C1-2012-09978 is read), so the padded spelling reaches it by unpadding.
    assert resolve("C1-2012-09978") == ("unpadded_single_candidate_in_input", ["C1-2012-9978@2012-05-01"])
    assert resolve("C1-2012-9978") == ("single_candidate_in_input", ["C1-2012-9978@2012-05-01"])


def test_the_index_answers_its_own_rows_record_ids(tmp_path):
    index = _index(tmp_path, [{"document_number": "00-111", "publication_date": "2000-01-14"}])
    assert index.record_id({"document_number": "00-111", "publication_date": "2000-01-14"}) == "00-111@2000-01-14"
    # A row the index does not hold is validated by its owner, as before.
    assert index.record_id({"document_number": "00-112", "publication_date": "2000-01-14"}) == "00-112@2000-01-14"


@pytest.mark.parametrize(
    ("stated", "dockets"),
    [
        ("Docket No. SSA-2010-0037", ("SSA-2010-0037",)),
        ("DHS Docket No. USCIS-2025-0004", ("USCIS-2025-0004",)),
        ("epa-hq-oar-2021-0317", ("EPA-HQ-OAR-2021-0317",)),
        # The -RULE-family suffix is part of the docket (SpicyDocs 0.31.0).
        ("GIPSA-2010-FGIS-0014-NONRULEMAKING", ("GIPSA-2010-FGIS-0014-NONRULEMAKING",)),
        # Lists and longer labels, as the Register states them (fr_docket_links, 2026-09-23).
        (
            "Docket No. FDA-2000-P-0102, FDA-2000-P-0133, and FDA-2006-P-0033",
            ("FDA-2000-P-0102", "FDA-2000-P-0133", "FDA-2006-P-0033"),
        ),
        ("Docket Nos. FMCSA-2001-9709 and FMCSA-00-7382", ("FMCSA-2001-9709", "FMCSA-00-7382")),
        ("Docket ID Number: DOT-OST-2010-0054", ("DOT-OST-2010-0054",)),
        # A docket named after prose is read (owner ruling D1, SpicyDocs 0.35.0); a former one is not.
        ("Public Notice: EIB-2023-0012", ("EIB-2023-0012",)),
        ("Formerly Docket Nos. 2000P-1275, 2000P-1276, and 2006P-0316, Respectively", ()),
        # The one held id the reader refuses: it states no sequence, and no link names it.
        ("GSA-NA-2005", ()),
        # A Commerce case number, the commonest value only the removed syntax-only fallback kept.
        ("A-570-831", ()),
        ("Docket No. RM98-1-000", ()),  # a FERC docket, not a Regulations.gov one
        ("MM Docket No. 98-213", ()),
        ("Sequence No. 1", ()),
        ("PPWOCRADI0, PCU00RP14.R50000", ()),
        (None, ()),
    ],
)
def test_a_federal_register_docket_value_is_read_through_its_label_and_list(stated, dockets):
    assert linked_docket_ids(stated) == dockets


@pytest.mark.parametrize(
    ("document_type", "title", "expected"),
    [
        # A stage comes from the document's own type, never the title alone (decision 56).
        ("Supporting & Related Material", "Final Rule Response to Comments", None),
        ("Other", "Withdrawal as attorney", None),
        ("Notice", "Notice of Filing of Proposed Rule Change", None),
        (None, "Final Rule", None),
        # Within Rule and Proposed Rule the title refines the stage.
        ("Proposed Rule", "Airworthiness Directives; Withdrawal", "withdrawn"),
        ("Proposed Rule", "Supplemental notice of proposed rulemaking", "supplemental"),
        # "withdraw" outranks "supplement": a withdrawn supplement withdraws.
        ("Proposed Rule", "Withdrawal of Supplemental Proposed Rulemaking", "withdrawn"),
        ("Rule", "Guidance Documents; Withdrawal", "withdrawn"),
        ("Rule", "Energy Conservation Standards", "final"),
        ("Proposed Rule", "Energy Conservation Standards", "proposed"),
        # The source spells its type with either case.
        ("PROPOSED RULE", "Standards proposal", "proposed"),
    ],
)
def test_rule_stage_is_carried_only_by_a_rule_or_proposed_rule(document_type, title, expected):
    assert federal_register.rule_stage(document_type, title) == expected


@pytest.mark.parametrize(
    ("document_type", "title", "expected"),
    [
        # Real 1994 titles (snapshot_7eafd657): the suffix types the row (decision 59).
        (
            "Uncategorized Document",
            "Federal Family Education Loan Program; Final Rule DEPARTMENT OF EDUCATION",
            "final",
        ),
        (
            "Uncategorized Document",
            "Overflights of Units of the National Park System; Proposed Rule DEPARTMENT OF TRANSPORTATION",
            "proposed",
        ),
        # Its section-marker variants (decision 59 as amended): interim rules are Rules, and
        # plurals and a comma-led marker are the same sections.
        (
            "Uncategorized Document",
            "Financial Assistance Rules; Implementation of OMB Circular A-110; Interim Final Rule DEPARTMENT OF ENERGY",
            "final",
        ),
        (
            "Uncategorized Document",
            "Career and Career-Conditional Employment; Interim Rule OFFICE OF PERSONNEL MANAGEMENT",
            "final",
        ),
        (
            "Uncategorized Document",
            "Rulemaking for EDGAR System and Adoption of Updated EDGAR Filer Manual; Final Rules SECURITIES AND EXCHANGE COMMISSION",
            "final",
        ),
        (
            "Uncategorized Document",
            "Revisions to Committee Regulations, Proposed Rule COMMITTEE FOR PURCHASE FROM PEOPLE WHO ARE BLIND OR SEVERELY DISABLED",
            "proposed",
        ),
        # No agency appended, or prose where the agency would be: no typing.
        ("Uncategorized Document", "Acid Rain Program: Permits; Final Rule", None),
        ("Uncategorized Document", "Pesticides; Proposed Rule Revoking Certain Food Additive Regulations", None),
        ("Uncategorized Document", "Lead Fishing Sinkers; Response to Citizens' Petition; Proposed Rule -", None),
        # Only the Register's Uncategorized rows read this way; other types never re-type.
        ("Notice", "Student Assistance General Provisions; Final Rule DEPARTMENT OF EDUCATION", None),
    ],
)
def test_an_uncategorized_register_row_types_from_its_title_suffix(document_type, title, expected):
    assert federal_register.register_rule_stage(document_type, title) == expected
    # The plain typed reading never types an Uncategorized row: Regulations.gov documents
    # with that type carry no stage even when their title carries the suffix.
    if document_type == "Uncategorized Document" and expected is not None:
        assert federal_register.rule_stage(document_type, title) is None


def _labelled_inputs(root):
    records = [
        {
            "document_number": "2010-2394",
            "publication_date": "2010-02-05",
            "type": "Proposed Rule",
            "title": "Labelled docket rule",
            "docket_ids": ["Docket No. SSA-2010-0037", "Docket No. FAA-2010-0001"],
            "cfr_references": [{"title": 20, "part": 404}],
            "regulation_id_numbers": ["0960-AG21"],
            "comments_close_on": "2010-04-06",
        },
        {
            "document_number": "2010-2400",
            "publication_date": "2010-02-05",
            "type": "Rule",
            "title": "Literal docket rule",
            "docket_ids": ["GIPSA-2010-FGIS-0014-NONRULEMAKING"],
            "cfr_references": [{"title": 7, "part": 800}],
            "regulation_id_numbers": [],
        },
    ]
    merge_federal_register(root, documents=lambda start: iter(records), download_prior=lambda key, path: False)
    build_fr_docket_links(root)
    stage_text("federal_register", root / "federal_register.parquet")  # as the materializer reads it
    _write(
        root,
        "dockets",
        ("docket_id", "docket_type", "rin", "modify_date"),
        [
            # 22:30 Eastern on the 10th is the 11th in UTC.
            {
                "docket_id": "SSA-2010-0037",
                "docket_type": "Rulemaking",
                "rin": "0960-AG21",
                "modify_date": "2010-03-11T03:30:00Z",
            },
            {"docket_id": "GIPSA-2010-FGIS-0014-NONRULEMAKING", "docket_type": "Nonrulemaking"},
        ],
    )
    _write(
        root,
        "documents",
        ("document_id", "docket_id", "fr_doc_num", "additional_rins", "document_type", "posted_date", "modify_date"),
        [
            {
                "document_id": "SSA-2010-0037-0001",
                "docket_id": "SSA-2010-0037",
                "fr_doc_num": "2010-02394",  # Regulations.gov pads; the Register did not
                "additional_rins": "[]",
                "document_type": "Proposed Rule",
                "posted_date": "2010-02-05T05:00:00Z",
                "modify_date": "2010-02-06T02:00:00Z",
            }
        ],
    )
    _write(root, "unified_agenda", ("rin", "agenda_edition"), [])


def test_labelled_dockets_and_padded_numbers_join_every_rulemaking_table(tmp_path):
    _labelled_inputs(tmp_path)

    targets = pq.read_table(build_rule_targets(tmp_path)).to_pylist()
    edges = {(r["docket_id"], r["source"], r["cfr_ref"], r["rin"]): r for r in targets}
    assert set(edges) == {
        ("SSA-2010-0037", "docket_rin", None, "0960-AG21"),
        ("SSA-2010-0037", "fr_cfr_ref", "20-404", "0960-AG21"),
        ("SSA-2010-0037", "document_fr_doc", "20-404", "0960-AG21"),
        ("SSA-2010-0037", "docket_document_cites_action_notice", None, None),
        ("GIPSA-2010-FGIS-0014-NONRULEMAKING", "fr_cfr_ref", "7-800", None),
    }, "FAA-2010-0001 is named but no Regulations.gov record asserts it"
    corroboration = edges[("SSA-2010-0037", "document_fr_doc", "20-404", "0960-AG21")]
    (reference,) = json.loads(corroboration["fr_references_json"])
    assert reference["document_number"] == "2010-02394"
    assert reference["status"] == "unpadded_single_candidate_in_input"
    assert reference["candidate_ids"] == ["2010-2394@2010-02-05"]
    # Days, not instants: the Eastern day of each Regulations.gov stamp.
    assert (corroboration["first_seen"], corroboration["last_seen"]) == ("2010-02-05", "2010-02-05")
    docket_rin = edges[("SSA-2010-0037", "docket_rin", None, "0960-AG21")]
    assert (docket_rin["first_seen"], docket_rin["last_seen"]) == ("2010-03-10", "2010-03-10")
    assert {r["actor_id"] for r in targets} == {"spicy-regs:rule-targets:v7"}

    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    ssa = next(r for r in proceedings if "SSA-2010-0037" in json.loads(r["docket_ids_json"]))
    assert json.loads(ssa["fr_document_ids_json"]) == ["2010-2394@2010-02-05"], "not an FR-only proceeding"
    assert json.loads(ssa["cfr_refs_json"]) == ["20-404"]
    assert {(e["source"], e["effective_date"]) for e in json.loads(ssa["stage_events_json"])} == {
        ("documents.document_type", "2010-02-05"),
        ("federal_register.document_type", "2010-02-05"),
    }
    assert not any(json.loads(r["docket_ids_json"]) == [] for r in proceedings)
    assert {r["actor_id"] for r in proceedings} == {"spicy-regs:proceedings:v13"}

    periods = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    (period,) = [r for r in periods if "federal_register.comments_close_on" in r["source"]]
    assert json.loads(period["docket_ids_json"]) == ["SSA-2010-0037"]
    assert json.loads(period["proceeding_ids_json"]) == [ssa["proceeding_id"]]
    assert {r["actor_id"] for r in periods} == {"spicy-regs:comment-periods:v14"}

    items_path, relationships_path = build_regulatory_agenda(tmp_path)
    (item,) = pq.read_table(items_path).to_pylist()
    assert (item["rin"], item["first_seen"], item["last_seen"]) == ("0960-AG21", "2010-02-05", "2010-03-10")
    relationships = {r["source"]: r for r in pq.read_table(relationships_path).to_pylist()}
    assert relationships["docket_rin"]["evidence_date"] == "2010-03-10"
    assert relationships["federal_register_rin"]["evidence_date"] == "2010-02-05"
    assert {r["actor_id"] for r in relationships.values()} == {"spicy-regs:agenda-item-proceedings:v6"}
    assert item["actor_id"] == "spicy-regs:regulatory-agenda-items:v6"


def test_the_rulemaking_generation_builds_one_federal_register_index(tmp_path, monkeypatch):
    _labelled_inputs(tmp_path)
    built: list[object] = []
    read: list[object] = []

    def counting_reader(value):
        read.append(value)
        return linked_docket_ids(value)

    monkeypatch.setattr(federal_register, "linked_docket_ids", counting_reader)

    class CountingIndex(FederalRegisterIndex):
        def __init__(self, path):
            built.append(path)
            super().__init__(path)

    def refuse(*_args, **_kwargs):
        raise AssertionError("a stage rebuilt the Federal Register index")

    monkeypatch.setattr(rulemaking_dataset, "FederalRegisterIndex", CountingIndex)
    for module in (build_rule_targets, build_proceedings, build_regulatory_agenda, build_comment_periods):
        monkeypatch.setattr(sys.modules[module.__module__], "FederalRegisterIndex", refuse)
    pipeline = rulemaking_dataset.RulemakingDatasetPipeline(output_dir=tmp_path)
    context = RunContext.resolve(run_id="one-index", asserted_at="2026-09-23T12:00:00Z")
    for stage in pipeline._ordered_stages(pipeline.stages()):
        stage.build(tmp_path, context)

    assert built == [tmp_path / "federal_register.parquet"]
    # Three stages join the link table; each of its rows is read once for all of them.
    assert len(read) == pq.ParquetFile(tmp_path / "fr_docket_links.parquet").metadata.num_rows == 3
    assert all((tmp_path / name).exists() for name in pipeline.published_outputs)


#: Link values from the 2026-09-23 fr_docket_links, with their FR records' own numbers and days.
FDA_LIST = "Docket No. FDA-2000-P-0102, FDA-2000-P-0133, and FDA-2006-P-0033"
FDA_DOCKETS = ["FDA-2000-P-0102", "FDA-2000-P-0133", "FDA-2006-P-0033"]
FMCSA_LIST = "Docket Nos. FMCSA-2001-9709 and FMCSA-00-7382"  # FMCSA-00-7382: no Regulations.gov record
PROSE_FIRST = "Public Notice: EIB-2023-0012"


def _listed_inputs(root):
    records = [
        {
            "document_number": "2011-3678",
            "publication_date": "2011-02-18",
            "type": "Proposed Rule",
            "title": "Health Claim; Phytosterols and Risk of Coronary Heart Disease",
            "docket_ids": [FDA_LIST],
            "cfr_references": [{"title": 21, "part": 101}],
            "regulation_id_numbers": [],
            "comments_close_on": "2011-04-19",
        },
        {
            "document_number": "03-2053",
            "publication_date": "2003-01-29",
            "type": "Rule",
            "title": "Commercial Driver's License Standards, Requirements, and Penalties",
            "docket_ids": [FMCSA_LIST],
            "cfr_references": [{"title": 49, "part": 383}],
            "regulation_id_numbers": [],
        },
        {
            "document_number": "2023-19952",
            "publication_date": "2023-09-15",
            "type": "Notice",
            "title": "Receipt of Requests To Increase the Amount of the Long-Term General Guarantee",
            "docket_ids": [PROSE_FIRST],
            "cfr_references": [],
            "regulation_id_numbers": [],
            "comments_close_on": "2023-10-10",
        },
    ]
    merge_federal_register(root, documents=lambda start: iter(records), download_prior=lambda key, path: False)
    build_fr_docket_links(root)
    stage_text("federal_register", root / "federal_register.parquet")  # as the materializer reads it
    _write(
        root,
        "dockets",
        ("docket_id", "docket_type", "rin"),
        [
            *({"docket_id": docket, "docket_type": "Nonrulemaking"} for docket in FDA_DOCKETS),
            {"docket_id": "FMCSA-2001-9709", "docket_type": "Rulemaking", "rin": "2126-AA60"},
            {"docket_id": "EIB-2023-0012", "docket_type": "Nonrulemaking"},
        ],
    )
    _write(root, "documents", ("document_id", "docket_id", "fr_doc_num", "additional_rins"), [])
    _write(root, "unified_agenda", ("rin", "agenda_edition"), [])


def test_a_listed_docket_value_joins_each_held_docket_once(tmp_path):
    _listed_inputs(tmp_path)

    targets = pq.read_table(build_rule_targets(tmp_path)).to_pylist()
    keys = [(r["docket_id"], r["cfr_ref"], r["rin"], r["source"]) for r in targets]
    assert len(keys) == len(set(keys)), "one edge per docket, never a repeated pair"
    edges = {(r["docket_id"], r["cfr_ref"]): r["evidence_id"] for r in targets if r["source"] == "fr_cfr_ref"}
    assert edges == {
        **{(docket, "21-101"): "2011-3678@2011-02-18" for docket in FDA_DOCKETS},
        ("FMCSA-2001-9709", "49-383"): "03-2053@2003-01-29",
    }, "FMCSA-00-7382 is read but held by no Regulations.gov record; the prose-first notice states no CFR part"

    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    by_dockets = {r["docket_ids_json"]: r for r in proceedings}
    # One FR document naming three dockets unites them, as three link values always did.
    fda = by_dockets[json.dumps(FDA_DOCKETS, separators=(",", ":"))]
    assert json.loads(fda["fr_document_ids_json"]) == ["2011-3678@2011-02-18"]
    assert json.loads(fda["cfr_refs_json"]) == ["21-101"]
    fmcsa = by_dockets['["FMCSA-2001-9709"]']
    assert json.loads(fmcsa["fr_document_ids_json"]) == ["03-2053@2003-01-29"]
    # The prose-first notice names its Nonrulemaking docket, but a RIN-less notice is no action
    # evidence (decision 32), so that docket still forms no proceeding.
    assert not any(
        "EIB-2023-0012" in r["docket_ids_json"] or "FMCSA-00-7382" in r["docket_ids_json"] for r in proceedings
    )

    periods = {r["evidence_ids_json"]: r for r in pq.read_table(build_comment_periods(tmp_path)).to_pylist()}
    assert set(periods) == {'["2011-3678@2011-02-18"]', '["2023-19952@2023-09-15"]'}
    # The notice's own comment period now reaches the docket it names after prose.
    assert json.loads(periods['["2023-19952@2023-09-15"]']["docket_ids_json"]) == ["EIB-2023-0012"]
    period = periods['["2011-3678@2011-02-18"]']
    assert json.loads(period["docket_ids_json"]) == FDA_DOCKETS
    assert json.loads(period["proceeding_ids_json"]) == [fda["proceeding_id"]]
    assert json.loads(period["evidence_ids_json"]) == ["2011-3678@2011-02-18"]


def test_a_list_merging_a_multi_docket_proceeding_keeps_the_outranking_id(tmp_path, monkeypatch):
    """Two real EPA proposals: one names four dockets apart, the other a list reaching into them."""
    records = [
        {
            "document_number": "2012-5760",
            "publication_date": "2012-03-26",
            "type": "Proposed Rule",
            "title": "National Uniform Emission Standards for Storage Vessel and Transfer Operations",
            "docket_ids": [*(f"EPA-HQ-OAR-2010-08{n}" for n in (68, 69, 70, 71)), "FRL-9645-1"],
            "cfr_references": [{"title": 40, "part": 65}],
            "regulation_id_numbers": ["2060-AR00"],
        },
        {
            "document_number": "2011-31530",
            "publication_date": "2012-01-06",
            "type": "Proposed Rule",
            "title": "National Emission Standards for Hazardous Air Pollutants From Petroleum Refineries",
            "docket_ids": ["EPA-HQ-OAR-2003-0146, EPA-HQ-OAR-2010-0870, EPA-HQ-OAR-2011-0002", "FRL-9502-9"],
            "cfr_references": [{"title": 40, "part": 63}],
            "regulation_id_numbers": ["2060-AP84"],
        },
    ]
    merge_federal_register(tmp_path, documents=lambda start: iter(records), download_prior=lambda key, path: False)
    build_fr_docket_links(tmp_path)
    stage_text("federal_register", tmp_path / "federal_register.parquet")  # as the materializer reads it
    _write(
        tmp_path,
        "dockets",
        ("docket_id", "docket_type", "rin"),
        [
            *(
                {"docket_id": f"EPA-HQ-OAR-2010-08{n}", "docket_type": "Rulemaking", "rin": "2060-AR00"}
                for n in (68, 69, 70, 71)
            ),
            {"docket_id": "EPA-HQ-OAR-2003-0146", "docket_type": "Rulemaking", "rin": "2060-AO55"},
            {"docket_id": "EPA-HQ-OAR-2011-0002", "docket_type": "Rulemaking", "rin": "2060-AP84"},
        ],
    )
    _write(tmp_path, "documents", ("document_id", "docket_id", "fr_doc_num", "additional_rins"), [])
    build_rule_targets(tmp_path)
    # The prior generation read one docket per value, so the list joined nothing.
    with monkeypatch.context() as before:
        before.setattr(
            federal_register,
            "linked_docket_ids",
            lambda value: tuple(filter(None, [normalize_docket_reference(value)])),
        )
        prior = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    prior_by_dockets = {r["docket_ids_json"]: r["proceeding_id"] for r in prior}
    four = json.dumps([f"EPA-HQ-OAR-2010-08{n}" for n in (68, 69, 70, 71)], separators=(",", ":"))
    # Unlinked there, 2011-31530 attaches by 2060-AP84, which only EPA-HQ-OAR-2011-0002 states;
    # the fr_cfr_ref rows the plural reader wrote for its other dockets hold nothing (review 2b).
    assert {r["docket_ids_json"]: json.loads(r["fr_document_ids_json"]) for r in prior} == {
        four: ["2012-5760@2012-03-26"],
        '["EPA-HQ-OAR-2003-0146"]': [],
        '["EPA-HQ-OAR-2011-0002"]': ["2011-31530@2012-01-06"],
    }
    shutil.copyfile(tmp_path / "proceedings.parquet", tmp_path / "_proceedings_prior.parquet")

    (merged,) = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    assert json.loads(merged["docket_ids_json"]) == sorted(
        ["EPA-HQ-OAR-2003-0146", "EPA-HQ-OAR-2011-0002", *json.loads(four)]
    )
    assert json.loads(merged["fr_document_ids_json"]) == ["2011-31530@2012-01-06", "2012-5760@2012-03-26"]
    # The four-docket proceeding outranks the singles on docket overlap and keeps its id.
    assert merged["proceeding_id"] == merged["supersedes_id"] == prior_by_dockets[four]


#: The one RIN-less Notice of 2026-09-23's links naming three or more dockets that are all
#: Rulemaking: each of its dockets is a proceeding of its own, whatever the notice says.
EPA_NOTICE = ["EPA-R05-OAR-2016-0135", "EPA-R05-OAR-2016-0269", "EPA-R05-OAR-2016-0372", "EPA-R05-OAR-2016-0396"]


def _links_from_published(root):
    """Rebuild the links as the plain route does: from the Register's published columns, not the stages' text."""
    import pyarrow as pa

    from spicy_regs.transforms.regulations_shape import shape_records, subject_schema

    published = root / "published"
    published.mkdir(exist_ok=True)
    rows = pq.read_table(root / "federal_register.parquet").to_pylist()
    pq.write_table(pa.Table.from_pylist(shape_records("federal_register", rows), schema=subject_schema("federal_register")),
                   published / "federal_register.parquet")
    shutil.copyfile(build_fr_docket_links(published), root / "fr_docket_links.parquet")


def _with_records(root, records, dockets):
    """Add real FR records (and the dockets they name) to the listed inputs, and rebuild the links."""
    fr = pq.read_table(root / "federal_register.parquet").to_pylist()
    fr += [
        {**dict.fromkeys(fr[0]), "regulation_id_numbers_json": "[]", "cfr_references_json": "[]", **r} for r in records
    ]
    _write(root, "federal_register", tuple(fr[0]), fr)
    _links_from_published(root)
    held = pq.read_table(root / "dockets.parquet").to_pylist()
    held += [{**dict.fromkeys(held[0]), "docket_id": docket, "docket_type": "Rulemaking"} for docket in dockets]
    _write(root, "dockets", tuple(held[0]), held)


def test_only_an_action_document_merges_the_dockets_it_names(tmp_path):
    """Decision 33: a Proposed Rule's list unites its dockets; a RIN-less Notice's list does not."""
    _listed_inputs(tmp_path)
    _with_records(
        tmp_path,
        [
            {
                "document_number": "2016-23295",
                "publication_date": "2016-09-27",
                "document_type": "Notice",
                "title": "Adequacy Status of the Cleveland-Akron-Lorain and Columbus, Ohio Areas",
                "docket_ids_json": json.dumps([*EPA_NOTICE, "FRL-9953-10-Region 5"]),
            }
        ],
        EPA_NOTICE,
    )
    build_rule_targets(tmp_path)
    # The generation before merged the Notice's dockets, under the id its second docket
    # minted when it stood alone (as FDA-2014-E-2356 did on the 2026-09-23 parents).
    merged_id = stable_id("proceeding", "docket", EPA_NOTICE[1])
    _write(
        tmp_path,
        "_proceedings_prior",
        ("proceeding_id", "docket_ids_json", "fr_document_ids_json"),
        [
            {
                "proceeding_id": merged_id,
                "docket_ids_json": json.dumps(EPA_NOTICE),
                "fr_document_ids_json": '["2016-23295@2016-09-27"]',
            }
        ],
    )

    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    assert len({r["proceeding_id"] for r in proceedings}) == len(proceedings)
    by_dockets = {r["docket_ids_json"]: r for r in proceedings}
    # The Proposed Rule (a rule stage) unites its three dockets ...
    assert json.dumps(FDA_DOCKETS, separators=(",", ":")) in by_dockets
    # ... the Notice (no RIN, no stage) attaches to each of its four and merges none.
    for docket in EPA_NOTICE:
        row = by_dockets[f'["{docket}"]']
        assert json.loads(row["fr_document_ids_json"]) == ["2016-23295@2016-09-27"]
        assert row["stage_events_json"] == "[]"
        # The merged id stays with the docket that minted it; the others mint their own.
        if docket == EPA_NOTICE[1]:
            assert row["proceeding_id"] == row["supersedes_id"] == merged_id
        else:
            assert row["proceeding_id"] == stable_id("proceeding", "docket", docket)

    # A rerun keeps every id.
    shutil.copyfile(tmp_path / "proceedings.parquet", tmp_path / "_proceedings_prior.parquet")
    rerun = {r["proceeding_id"] for r in pq.read_table(build_proceedings(tmp_path)).to_pylist()}
    assert rerun == {r["proceeding_id"] for r in proceedings}


def test_a_notice_several_proceedings_hold_opens_a_period_listing_each(tmp_path):
    """Decision 33 as amended: the period of a RIN-less notice naming two rulemakings lists both."""
    _listed_inputs(tmp_path)
    dockets = ["FMCSA-2014-0083", "NHTSA-2016-0087"]
    _with_records(
        tmp_path,
        [
            {
                "document_number": "2016-23486",
                "publication_date": "2016-09-29",
                "document_type": "Notice",
                "title": "Notice of Availability of a Draft Environmental Assessment for Rulemaking To Require "
                "the Installation and Maintenance of Speed Limiting Devices in Heavy Vehicles",
                "docket_ids_json": '["Docket No. NHTSA-2016-0087", "Docket No. FMCSA-2014-0083"]',
                "comments_close_on": "2016-11-07",
            }
        ],
        dockets,
    )
    held = pq.read_table(tmp_path / "dockets.parquet").to_pylist()
    rins = {"FMCSA-2014-0083": "2126-AB63", "NHTSA-2016-0087": "2127-AK92"}
    _write(
        tmp_path, "dockets", tuple(held[0]), [{**row, "rin": rins.get(row["docket_id"], row["rin"])} for row in held]
    )
    build_rule_targets(tmp_path)

    proceedings = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    ids = sorted(
        row["proceeding_id"] for row in proceedings if json.loads(row["docket_ids_json"])[:1] in ([d] for d in dockets)
    )
    assert len(ids) == 2, "the notice merges neither"

    periods = pq.read_table(build_comment_periods(tmp_path)).to_pylist()
    (period,) = [r for r in periods if "2016-23486@2016-09-29" in json.loads(r["evidence_ids_json"])]
    assert json.loads(period["docket_ids_json"]) == dockets
    assert json.loads(period["proceeding_ids_json"]) == ids
    assert json.loads(period["rins_json"]) == ["2126-AB63", "2127-AK92"]
    assert (period["open_date"], period["close_date"]) == ("2016-09-29", "2016-11-07")


def test_the_index_reads_each_link_row_once_and_keys_each_docket_it_names(tmp_path):
    _listed_inputs(tmp_path)
    links = tmp_path / "fr_docket_links.parquet"
    # A number the generation does not hold on that day: the reference stays unresolved,
    # and each docket of the list carries it under its own evidence id.
    _write(
        tmp_path,
        "fr_docket_links",
        ("docket_id", "document_number", "publication_date"),
        [
            {"docket_id": FDA_LIST, "document_number": "2011-3678", "publication_date": "2011-02-19"},
            {"docket_id": PROSE_FIRST, "document_number": "2023-19952", "publication_date": "2023-09-15"},
            {"docket_id": FMCSA_LIST, "document_number": None, "publication_date": "2003-01-29"},
        ],
    )
    index = FederalRegisterIndex(tmp_path / "federal_register.parquet")
    first = list(index.docket_links(links))
    assert first == [
        (
            docket,
            {
                "source": "fr_docket_links",
                "evidence_id": docket,
                "document_number": "2011-3678",
                "publication_date": "2011-02-19",
                "status": "missing",
                "candidate_ids": (),
            },
        )
        for docket in FDA_DOCKETS
    ] + [
        (
            "EIB-2023-0012",
            {
                "source": "fr_docket_links",
                "evidence_id": "EIB-2023-0012",
                "document_number": "2023-19952",
                "publication_date": "2023-09-15",
                "status": "dated",
                "candidate_ids": ("2023-19952@2023-09-15",),
            },
        )
    ]
    links.unlink()
    assert list(index.docket_links(links)) == first, "replayed, not re-read"


def test_a_label_join_continues_the_docket_id_and_retires_the_fr_only_one(tmp_path):
    """A label join absorbs a prior FR-only proceeding into the docket's, which keeps its id.

    The prior generation read FR docket values syntax-only and predates decision 56 (a
    Register copy joins its docket), so 2010-2394 stood alone; its rows are written as that
    generation published them, since today's code no longer forms the FR-only one.
    """
    _labelled_inputs(tmp_path)
    build_rule_targets(tmp_path)
    prior_docket = stable_id("proceeding", "docket", "SSA-2010-0037")
    prior_fr_only = stable_id("proceeding", "fr-document", "2010-2394@2010-02-05")
    _write(
        tmp_path,
        "_proceedings_prior",
        ("proceeding_id", "docket_ids_json", "fr_document_ids_json"),
        [
            {"proceeding_id": prior_docket, "docket_ids_json": '["SSA-2010-0037"]', "fr_document_ids_json": "[]"},
            {
                "proceeding_id": prior_fr_only,
                "docket_ids_json": "[]",
                "fr_document_ids_json": '["2010-2394@2010-02-05"]',
            },
        ],
    )

    merged = pq.read_table(build_proceedings(tmp_path)).to_pylist()
    ssa = next(r for r in merged if "SSA-2010-0037" in json.loads(r["docket_ids_json"]))
    assert "2010-2394@2010-02-05" in json.loads(ssa["fr_document_ids_json"])
    # Docket overlap outranks FR overlap, so the docket's id continues.
    assert ssa["proceeding_id"] == ssa["supersedes_id"] == prior_docket
    assert prior_fr_only not in {r["proceeding_id"] for r in merged}
    assert all(r["docket_ids_json"] != "[]" for r in merged)


def test_rule_targets_count_the_cfr_references_they_cannot_read(tmp_path):
    _labelled_inputs(tmp_path)
    fr = pq.read_table(tmp_path / "federal_register.parquet").to_pylist()
    for row in fr:
        if row["document_number"] == "2010-2394":
            row["cfr_references_json"] = '[{"title": 20, "part": 404}, "20 CFR 416"]'
    _write(tmp_path, "federal_register", tuple(fr[0]), fr)
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        targets = pq.read_table(build_rule_targets(tmp_path)).to_pylist()
    finally:
        logger.remove(sink)

    assert {r["cfr_ref"] for r in targets if r["docket_id"] == "SSA-2010-0037"} == {None, "20-404"}
    assert any("dropped 1 Federal Register CFR references that are not objects" in m for m in messages)


@pytest.mark.parametrize(
    ("document_type", "title", "states"),
    [
        ("Notice", "Fishery Management Plan; Notice", True),
        ("Rule", "Air standards", True),
        ("Uncategorized Document", "Federal Family Education Loan Program; Final Rule DEPARTMENT OF EDUCATION", True),
        ("Uncategorized Document", "Defense Acquisition Regulations", False),
        ("", "Anything", False),
    ],
)
def test_a_register_row_states_a_type_unless_it_is_untyped(document_type, title, states):
    """Decision 60 as amended: only a Register row that states a type overrides a copy's own."""
    assert federal_register.register_states_type(document_type, title) is states
