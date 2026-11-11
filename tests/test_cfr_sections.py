"""Hermetic tests for the GovInfo CFR section ingest (no network).

Covers the pieces with real logic: the api.data.gov key resolution fallback
chain, the reader's keyless refusal + default edition window, the
raw-granule → published-schema mapping (``_shape``), which reads the granule
ID grammar + list-level fields (no per-granule ``/summary`` call), and the
placement of section granules under their annual volume's PART headings
(``place_sections``). No live network calls are made.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from spicy_docs.sources.cfr.models import AnnualCfrSelection

from spicy_regs.pipelines.rollups import cfr_sections as rollup
from spicy_regs.sources.cfr_sections import API_KEY_ENV_VARS, CfrSectionsError, CfrSectionsReader, _resolve_api_key
from spicy_regs.transforms.build_cfr_sections import (
    COLUMNS,
    _shape,
    compound_parts,
    place_sections,
    prepare_retained_cfr_rows,
)

ANCESTRY = Path(__file__).parent / "fixtures" / "cfr" / "ancestry"

RETAINED_NODE = """<relatedItem type="constituent"><extension>
<accessId>CFR-2025-title1-vol1</accessId><granuleClass>NODE</granuleClass>
<granuleLabel>title</granuleLabel><granuleNumber>1 </granuleNumber>
<sequenceNumber>1</sequenceNumber><heading>Title 1</heading></extension></relatedItem>"""
RETAINED_SECTION = """<relatedItem type="constituent"><extension>
<accessId>CFR-2025-title1-vol1-sec1-1</accessId><granuleClass>CONTENT</granuleClass>
<heading>Purpose</heading></extension></relatedItem>"""
RETAINED_XML = b"""<CFRDOC><FDSYS><CFRTITLE>1</CFRTITLE><CFRTITLETEXT>General Provisions</CFRTITLETEXT>
<DATE>2023-01-01</DATE></FDSYS><FMTR><TITLEPG><REVISED>Revised as of January 1, 2023</REVISED></TITLEPG></FMTR>
<PART><HD>PART 1-General</HD><SECTION><SECTNO>1.1</SECTNO><P>Literal body</P></SECTION></PART></CFRDOC>"""


def retained_mods(constituents=RETAINED_NODE + RETAINED_SECTION):
    return (
        """<mods xmlns="http://www.loc.gov/mods/v3"><extension>
<collectionCode>CFR</collectionCode><accessId>CFR-2025-title1-vol1</accessId>
<titleNumber>1</titleNumber><volumeNumber>1</volumeNumber>
<originalDateIssued>2023-01-01</originalDateIssued><isCoverOnly>true</isCoverOnly></extension>
<originInfo><dateIssued>2025-01-01</dateIssued></originInfo>"""
        + constituents
        + "</mods>"
    ).encode()


def prepare_retained(mods=None, xml=RETAINED_XML, **kwargs):
    return prepare_retained_cfr_rows(
        retained_mods() if mods is None else mods,
        xml,
        selection=AnnualCfrSelection(2025, 1, 1),
        mods_url="https://www.govinfo.gov/metadata/pkg/CFR-2025-title1-vol1/mods.xml",
        xml_url="https://www.govinfo.gov/bulkdata/CFR/2025/title-1/CFR-2025-title1-vol1.xml",
        **({"expected_constituents": 2, "expected_sections": 1} | kwargs),
    )


def test_retained_annual_preserves_title_inventory_and_distinct_native_revision():
    result = prepare_retained()
    title, section = result["rows"]
    assert title == {
        "granule_id": "CFR-2025-title1-vol1",
        "package_id": "CFR-2025-title1-vol1",
        "cfr_ref": None,
        "title": "1",
        "part": None,
        "section": None,
        "heading": "Title 1",
        "structure_level": "NODE",
        "edition_year": "2025",
        "last_modified": None,
        "url": "https://www.govinfo.gov/app/details/CFR-2025-title1-vol1/CFR-2025-title1-vol1",
        "part_granule": "false",
    }
    assert (section["granule_id"], section["part"], section["section"], section["cfr_ref"]) == (
        "CFR-2025-title1-vol1-sec1-1",
        "1",
        "1",
        "1-1.1",
    )
    assert result["edition"]["date_issued"] == "2025-01-01"
    assert result["edition"]["original_date_issued"] == "2023-01-01"
    assert result["native_identity"]["stated_date"] == "2023-01-01"
    assert result["native_identity"]["volume"] is None
    assert [context["annual_position"] for context in result["contexts"]] == [None, 1]
    assert all("annual_source_path" not in context for context in result["contexts"])


@pytest.mark.parametrize(
    "before,after",
    [
        ("<granuleClass>NODE</granuleClass>", "<granuleClass>CONTENT</granuleClass>"),
        ("<granuleLabel>title</granuleLabel>", ""),
        ("<granuleNumber>1 </granuleNumber>", "<granuleNumber>2</granuleNumber>"),
        ("<sequenceNumber>1</sequenceNumber>", "<sequenceNumber>2</sequenceNumber>"),
        ("<accessId>CFR-2025-title1-vol1</accessId>", "<accessId>CFR-2025-title2-vol1</accessId>"),
        ("<granuleClass>NODE</granuleClass>", "<granuleClass>NODE</granuleClass><granuleClass>NODE</granuleClass>"),
    ],
)
def test_retained_annual_refuses_malformed_title_node(before, after):
    with pytest.raises(ValueError):
        prepare_retained(retained_mods(RETAINED_NODE.replace(before, after) + RETAINED_SECTION))


@pytest.mark.parametrize(
    "constituents,kwargs",
    [
        (RETAINED_SECTION + RETAINED_NODE, {}),
        (RETAINED_NODE + RETAINED_NODE, {}),
        (RETAINED_NODE, {}),
        (RETAINED_NODE + RETAINED_SECTION, {"expected_sections": 2}),
        (RETAINED_NODE + RETAINED_SECTION.replace("sec1-1", "sec2-1"), {}),
    ],
)
def test_retained_annual_refuses_incomplete_or_reordered_inventory(constituents, kwargs):
    with pytest.raises(ValueError):
        prepare_retained(retained_mods(constituents), **kwargs)


def test_retained_annual_checks_native_title_and_scalar_bound():
    with pytest.raises(ValueError, match="native title"):
        prepare_retained(xml=RETAINED_XML.replace(b"<CFRTITLE>1", b"<CFRTITLE>2"))
    oversized = RETAINED_SECTION.replace("sec1-1", "x" * 65537)
    with pytest.raises(ValueError, match="bounded"):
        prepare_retained(retained_mods(RETAINED_NODE + oversized))


# A CONTENT/section granule with real list-level fields + package stamps, using
# the real GovInfo ID grammar. Note: no cfrTitle/cfrPart/cfrSection fields exist
# on list-level granules — everything is parsed from the IDs.
_SECTION_GRANULE = {
    "granuleId": "CFR-2024-title40-vol1-sec1-1",
    "granuleClass": "CONTENT",
    "title": "Definitions.",
    "dateIssued": "2024-07-01",
    "granuleLink": "https://api.govinfo.gov/packages/CFR-2024-title40-vol1/granules/CFR-2024-title40-vol1-sec1-1/summary",
    "_package_id": "CFR-2024-title40-vol1",
    "_package_last_modified": "2026-07-16T20:57:39Z",
    "_package_title": "Protection of Environment",
}

# A part-level CONTENT granule (part token present, no section).
_PART_GRANULE = {
    "granuleId": "CFR-2024-title48-vol5-part700",
    "granuleClass": "CONTENT",
    "title": "Reserved",
    "_package_id": "CFR-2024-title48-vol5",
    "_package_last_modified": "2026-01-02T00:00:00Z",
}

# A chapter appendix granule (part token present via app parent, section absent).
_APPENDIX_GRANULE = {
    "granuleId": "CFR-2024-title48-vol1-part3-app1",
    "granuleClass": "APPENDIX",
    "title": "Cost Accounting Standards.",
    "_package_id": "CFR-2024-title48-vol1",
}

# A NODE granule (chapter) — no part, no section.
_NODE_GRANULE = {
    "granuleId": "CFR-2024-title48-vol5-chap7",
    "granuleClass": "NODE",
    "title": "AGENCY FOR INTERNATIONAL DEVELOPMENT",
    "_package_id": "CFR-2024-title48-vol5",
}


# -- API key resolution -------------------------------------------------------


def _clear_key_env(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_resolve_api_key_none_when_unset(monkeypatch):
    _clear_key_env(monkeypatch)
    assert _resolve_api_key() is None


def test_resolve_api_key_prefers_data_gov(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("DATA_GOV_API_KEY", "primary")
    monkeypatch.setenv("GOVINFO_API_KEY", "secondary")
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "tertiary")
    assert _resolve_api_key() == "primary"


def test_resolve_api_key_fallback_chain(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "tertiary")
    assert _resolve_api_key() == "tertiary"
    monkeypatch.setenv("GOVINFO_API_KEY", "secondary")
    assert _resolve_api_key() == "secondary"


def test_resolve_api_key_ignores_blank(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("DATA_GOV_API_KEY", "   ")
    monkeypatch.setenv("GOVINFO_API_KEY", "real")
    assert _resolve_api_key() == "real"


def test_reader_keyless_refuses(monkeypatch):
    """No configured credential must not become a successful empty selection."""
    _clear_key_env(monkeypatch)
    reader = CfrSectionsReader()
    with pytest.raises(CfrSectionsError, match="requires an api.data.gov key"):
        list(reader.iter_records())


def test_reader_default_edition_window():
    """Default window is last-year..this-year (CFR editions are annual)."""
    reader = CfrSectionsReader(api_key="x")
    this_year = dt.date.today().year
    assert reader.until_year == this_year
    assert reader.since_year == this_year - 1


# -- raw granule -> published schema mapping ----------------------------------


def test_shape_produces_exact_schema():
    row = _shape(_SECTION_GRANULE)
    assert set(row) == set(COLUMNS)


def test_shape_parses_section_granule():
    row = _shape(_SECTION_GRANULE)
    assert row["granule_id"] == "CFR-2024-title40-vol1-sec1-1"
    assert row["package_id"] == "CFR-2024-title40-vol1"
    # CFR title number parsed from the id, not the (heading) ``title`` field.
    assert row["title"] == "40"
    assert row["edition_year"] == "2024"
    # The list has no native ancestry; the token stays until the volume places it.
    assert row["part"] is None
    assert row["section"] == "1-1"
    assert row["cfr_ref"] is None
    assert row["heading"] == "Definitions."
    assert row["structure_level"] == "CONTENT"
    # last_modified comes from the enclosing package stamp.
    assert row["last_modified"] == "2026-07-16T20:57:39Z"
    # Canonical govinfo details URL built from package + granule ids.
    assert row["url"] == "https://www.govinfo.gov/app/details/CFR-2024-title40-vol1/CFR-2024-title40-vol1-sec1-1"


def test_shape_parses_part_granule():
    row = _shape(_PART_GRANULE)
    assert row["title"] == "48"
    assert row["part"] == "700"
    assert row["section"] is None
    assert row["cfr_ref"] == "48-700"
    assert row["structure_level"] == "CONTENT"
    assert row["edition_year"] == "2024"


@pytest.mark.parametrize(
    ("granule_id", "part", "cfr_ref"),
    [
        ("CFR-2026-title14-vol5-part1203a", "1203a", "14-1203a"),
        ("CFR-2026-title14-vol5-part1203b", "1203b", "14-1203b"),
        ("CFR-2026-title7-vol1-part15a-subpartA", "15a", "7-15a"),
        ("CFR-2026-title7-vol1-part15a-toc-id785", "15a", "7-15a"),
    ],
)
def test_shape_preserves_lettered_part_tokens(granule_id, part, cfr_ref):
    # Literal IDs from the 2026-09-21 pinned public cfr_sections table. The raw
    # table and full 336-row counterexample set are retained outside the repo:
    # receipts/data-validation-sprint-2026-09-21/cfr-fix-review.json.
    # Only the listed part token is under test; no source hierarchy is inferred.
    row = _shape({"granuleId": granule_id})
    assert row["part"] == part
    assert row["cfr_ref"] == cfr_ref
    assert row["section"] is None


@pytest.mark.parametrize(
    ("granule_id", "flag"),
    [
        # Real ids of the 2025 edition: 2 CFR part 200 and its own structural rows, and Title 41's part granules.
        ("CFR-2025-title2-vol1-part200", "true"),
        ("CFR-2025-title2-vol1-part200-subpartA", "false"),
        ("CFR-2025-title2-vol1-part200-subpartD-subjectgroup-id362", "false"),
        ("CFR-2025-title2-vol1-part200-appI", "false"),
        ("CFR-2025-title2-vol1-part200-toc-id312", "false"),
        ("CFR-2025-title2-vol1-sec200-1", "false"),
        ("CFR-2025-title41-vol1-part60-id334", "true"),
        ("CFR-2024-title48-vol5-chap7", "false"),
    ],
)
def test_part_granule_marks_only_the_granule_that_is_the_part_itself(granule_id, flag):
    assert _shape({"granuleId": granule_id})["part_granule"] == flag


def test_shape_parses_appendix_granule():
    row = _shape(_APPENDIX_GRANULE)
    assert row["title"] == "48"
    assert row["part"] == "3"
    assert row["section"] is None
    assert row["cfr_ref"] == "48-3"
    assert row["structure_level"] == "APPENDIX"


def test_shape_node_granule_has_no_part_or_section():
    row = _shape(_NODE_GRANULE)
    assert row["title"] == "48"
    assert row["part"] is None
    assert row["section"] is None
    # No part -> no cfr_ref.
    assert row["cfr_ref"] is None
    assert row["structure_level"] == "NODE"


def test_shape_edition_year_falls_back_to_date_issued():
    row = _shape({"granuleId": "weird-id", "dateIssued": "2019-03-04"})
    assert row["edition_year"] == "2019"


def test_shape_cfr_ref_full_citation():
    # A section granule that also carries a part token yields a full title-part.section ref.
    row = _shape(
        {
            "granuleId": "CFR-2024-title21-vol1-part1-sec5",
            "granuleClass": "SECTION",
            "_package_id": "CFR-2024-title21-vol1",
        }
    )
    assert row["title"] == "21"
    assert row["part"] == "1"
    assert row["section"] == "5"
    assert row["cfr_ref"] == "21-1.5"


def test_shape_handles_missing_fields():
    row = _shape({"granuleId": "x"})
    assert row["granule_id"] == "x"
    assert row["package_id"] is None
    assert row["cfr_ref"] is None
    assert row["title"] is None
    assert row["part"] is None
    assert row["section"] is None
    assert row["heading"] is None
    # No package id -> no canonical url.
    assert row["url"] is None


# A section granule whose part carries a letter suffix (5 CFR 5b is a real part).
_LETTERED_PART_GRANULE = {
    "granuleId": "CFR-2025-title5-vol3-sec5b-11",
    "granuleClass": "CONTENT",
    "title": "Safeguarding personal information.",
    "_package_id": "CFR-2025-title5-vol3",
}

# A malformed publisher id: the word "Sec" repeats and no part is recoverable.
_MALFORMED_SECTION_GRANULE = {
    "granuleId": "CFR-2025-title14-vol4-secSec-1-1",
    "granuleClass": "CONTENT",
    "title": "Applicability.",
    "_package_id": "CFR-2025-title14-vol4",
}


def test_shape_requires_ancestry_even_for_a_lettered_section_prefix():
    row = _shape(_LETTERED_PART_GRANULE)
    assert row["part"] is None
    assert row["section"] == "5b-11"
    assert row["cfr_ref"] is None


def test_shape_leaves_a_malformed_section_id_unsplit():
    """The id yields no part, so part stays null until the volume places it (Part 241)."""
    row = _shape(_MALFORMED_SECTION_GRANULE)
    assert row["part"] is None
    assert row["section"] == "Sec-1-1"
    assert row["cfr_ref"] is None


# -- section placement from the annual volume XML -----------------------------


def _place(package_id: str, *suffixes: str) -> dict[str, dict]:
    """Place ``{package_id}-{suffix}`` granules against the package's retained volume excerpt, keyed by suffix."""
    rows = [_shape({"granuleId": f"{package_id}-{suffix}", "_package_id": package_id}) for suffix in suffixes]
    placed = place_sections(rows, (ANCESTRY / f"{package_id}.xml").read_bytes())
    return {row["granule_id"].removeprefix(f"{package_id}-"): row for row in placed}


def _validated_title(package_id: str) -> int | None:
    """The title SpicyDocs' annual validator, which ``acquire_annual`` runs before the scan, reads; a refusal raises."""
    from spicy_docs.sources.cfr.annual import annual_cfr_xml_locator, validate_annual_cfr_xml

    volume = AnnualCfrSelection.from_package_id(package_id)
    assert volume is not None
    xml = (ANCESTRY / f"{package_id}.xml").read_bytes()
    return validate_annual_cfr_xml(xml, identity=volume, final_url=annual_cfr_xml_locator(volume)).title


@pytest.mark.parametrize(
    ("package_id", "suffix", "part", "section", "cfr_ref"),
    [
        # Title 43 numbers sections by subpart: § 1601.0-1 is in Part 1600 and
        # cites as printed (ruling 5), not as the nonexistent Part 1601.
        ("CFR-2025-title43-vol2", "sec1601-0-1", "1600", "1601.0-1", "43-1601.0-1"),
        # Title 41's compound part keeps its hyphen; the id's token splits it at 50.
        ("CFR-2025-title41-vol1", "sec50-201-1", "50-201", "1", "41-50-201.1"),
        # Title 14 Part 241 prints 19-8.1: part 241, never 19, and no citation.
        ("CFR-2025-title14-vol4", "sec19-8-1", "241", "19-8.1", None),
        # An unprefixed number in the same part has a part but no citation.
        ("CFR-2025-title14-vol4", "secSec-1-1", "241", "Sec.1-1", None),
        # Ranges (a §§ em-dash span and a single-§ hyphen span) cite nothing.
        ("CFR-2025-title41-vol1", "sec51-10-104-51-10-109", "51-10", "104—51-10.109", None),
        ("CFR-2025-title26-vol6", "sec1-404a-4-1-404a-7", "1", "404(a)-4-1.404(a)-7", None),
        # A parenthesized citation does not join the Federal Register key yet.
        ("CFR-2025-title26-vol6", "sec1-401k-1", "1", "401(k)-1", None),
        # A publisher typo (§ 206.253 under PART 1206) keeps the heading's part.
        ("CFR-2025-title30-vol3", "sec206-253", "1206", "206.253", None),
        # GovInfo's -id duplicate reaches the canonical copy of § 849.504.
        ("CFR-2025-title48-vol5", "sec849-504-id915", "849", "504", "48-849.504"),
        ("CFR-2025-title48-vol5", "sec849-504", "849", "504", "48-849.504"),
    ],
)
def test_section_granules_take_the_volume_part_heading(package_id, suffix, part, section, cfr_ref):
    placed = _place(package_id, suffix)
    row = placed[suffix]
    assert (row["part"], row["section"], row["cfr_ref"]) == (part, section, cfr_ref)


def test_non_section_and_unscanned_granules_keep_the_identifier_path():
    placed = _place(
        "CFR-2025-title41-vol1",
        "part50",  # a NODE: Title 41's compound part stays cut at the hyphen (documented limit)
        "part50-201-toc-id5",
        "sec50-201-1-app1",  # an appendix of § 50-201.1, which the excerpt holds
        "sec50-201-2-app1",  # an appendix of a section the excerpt does not hold
        "sec50-201-2",  # a plain section token the excerpt does not hold
    )
    assert (placed["part50"]["part"], placed["part50"]["cfr_ref"]) == ("50", "41-50")
    assert placed["part50-201-toc-id5"]["part"] == "50"
    # The held host section lends its part; the appendix's other values stay as its identifier spells them.
    hosted = placed["sec50-201-1-app1"]
    assert (hosted["part"], hosted["section"], hosted["cfr_ref"]) == ("50-201", "201-1-app1", "41-50.201-1-app1")
    appendix = placed["sec50-201-2-app1"]
    assert (appendix["part"], appendix["section"], appendix["cfr_ref"]) == ("50", "201-2-app1", "41-50.201-2-app1")
    unheld = placed["sec50-201-2"]
    assert (unheld["part"], unheld["section"], unheld["cfr_ref"]) == (None, "50-201-2", None)


@pytest.mark.parametrize(
    ("granule_id", "part", "section", "cfr_ref"),
    [
        # Literal rows of the live generation 8fb97150: a section's appendix or TOC
        # keeps its token's leading number as part, exactly as published.
        ("CFR-2026-title15-vol3-sec746-10-app1", "746", "10-app1", "15-746.10-app1"),
        ("CFR-2025-title13-vol1-sec117-20-appA", "117", "20-appA", "13-117.20-appA"),
        ("CFR-2025-title10-vol5-sec1002-31-toc-id1699", "1002", "31-toc-id1699", "10-1002.31-toc-id1699"),
        (
            "CFR-2025-title7-vol12-sec1775-69-1775-99-toc-id178",
            "1775",
            "69-1775-99-toc-id178",
            "7-1775.69-1775-99-toc-id178",
        ),
    ],
)
def test_a_section_appendix_or_toc_keeps_its_published_part(granule_id, part, section, cfr_ref):
    row = _shape({"granuleId": granule_id})
    assert (row["part"], row["section"], row["cfr_ref"]) == (part, section, cfr_ref)


def test_placement_keeps_every_non_placement_column():
    package_id = "CFR-2025-title43-vol2"
    granule = {
        "granuleId": f"{package_id}-sec1601-0-1",
        "_package_id": package_id,
        "title": "Purpose.",
        "granuleClass": "CONTENT",
        "lastModified": "2026-07-30T12:33:07Z",
    }
    shaped = _shape(granule)
    [placed] = place_sections([shaped], (ANCESTRY / f"{package_id}.xml").read_bytes())
    changed = {column for column in COLUMNS if placed[column] != shaped[column]}
    assert changed == {"part", "section", "cfr_ref"}


def test_the_combined_title_34_35_volume_is_admitted_and_placed():
    """CFR-2025-title34-vol4 prints TITLENUM twice: Title 34 and Title 35, reserved, with an empty heading.

    SpicyDocs 0.31.0's validator drops the reserved title (plan A14); 0.30.0 refused the volume.
    """
    assert _validated_title("CFR-2025-title34-vol4") == 34
    placed = _place("CFR-2025-title34-vol4", "sec681-1")
    assert (placed["sec681-1"]["part"], placed["sec681-1"]["cfr_ref"]) == ("681", "34-681.1")


def test_an_appendix_only_volume_is_admitted_and_places_nothing():
    """CFR-2025-title40-vol9 prints only appendices; its scan is empty, so rows keep their identifier values.

    SpicyDocs 0.31.0's validator admits it on its title page's word (plan A14); 0.30.0 refused the volume.
    """
    assert _validated_title("CFR-2025-title40-vol9") == 40
    placed = _place("CFR-2025-title40-vol9", "sec60-1", "part60-appA-1")
    assert (placed["sec60-1"]["part"], placed["sec60-1"]["section"]) == (None, "60-1")
    assert placed["part60-appA-1"]["part"] == "60"


@pytest.mark.parametrize(
    ("package_id", "volume"),
    [
        ("CFR-2025-title14-vol4", (2025, 14, 4)),
        ("CFR-2026-title14-vol0", (2026, 14, 0)),
        ("GPO-CFR-INDEX-2025", None),
        (None, None),
    ],
)
def test_only_a_volume_package_id_names_an_annual_volume(package_id, volume):
    """Which packages the build places: spicy-docs' inverse of the volume package id (X3), not a regex of ours."""
    selection = AnnualCfrSelection.from_package_id(package_id)
    assert (None if selection is None else (selection.year, selection.title, selection.volume)) == volume


# -- the printed-citation join key -------------------------------------------


def _citation_key(text: str) -> str:
    """The ``cfr_section`` key SpicyDocs' grammar gives printed text, as ``document_citations`` stores it."""
    from spicy_docs.interpretation.citations import find_citations

    [finding] = find_citations(text, kinds=("cfr_section",))
    assert finding.target_resolved
    return finding.target_key


def test_title_43_cfr_ref_is_the_printed_citation_key():
    """Ruling 5: the printed citation joins, so "43 CFR 1601.0-1" keys to 43-1601.0-1 while part is 1600.

    The Register's 294,501 CFR reference objects (2026-09-23) carry title, part, chapter and URL only, so
    ``rule_targets`` joins ``cfr_sections`` at (title, part); the section-level agreement held
    here is between ``cfr_ref`` and the grammar's key over citation text (``document_citations``).
    """
    placed = _place("CFR-2025-title43-vol2", "sec1601-0-1")
    assert _citation_key("43 CFR 1601.0-1") == placed["sec1601-0-1"]["cfr_ref"] == "43-1601.0-1"
    assert placed["sec1601-0-1"]["part"] == "1600"


def test_title_41_cfr_ref_is_the_printed_citation_key():
    """Title 41's compound part keeps its hyphen on both sides: "41 CFR 50-201.1" keys to 41-50-201.1.

    The Register's 294,501 CFR reference objects (2026-09-23) carry title, part, chapter and URL only, so
    ``rule_targets`` joins ``cfr_sections`` at (title, part); the section-level agreement held
    here is between ``cfr_ref`` and the grammar's key over citation text (``document_citations``).
    """
    placed = _place("CFR-2025-title41-vol1", "sec50-201-1")
    assert _citation_key("41 CFR 50-201.1") == placed["sec50-201-1"]["cfr_ref"] == "41-50-201.1"


# -- the rollup's replace_all input ------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"), [(None, False), ("", False), ("false", False), ("true", True), ("TRUE", True)]
)
def test_rollup_passes_replace_all_from_the_workflow_input(monkeypatch, tmp_path, raw, expected):
    if raw is None:
        monkeypatch.delenv("CFR_REPLACE_ALL", raising=False)
    else:
        monkeypatch.setenv("CFR_REPLACE_ALL", raw)
    calls = []
    monkeypatch.setattr(
        rollup.CfrSectionsRollup,
        "build_table",
        lambda self, directory, builder, **kwargs: builder(directory, **kwargs),
    )
    monkeypatch.setattr(rollup, "build_cfr_sections", lambda output_dir, **kwargs: calls.append(kwargs) or output_dir)
    rollup.CfrSectionsRollup(output_dir=tmp_path).build(tmp_path)
    assert calls == [{"replace_all": expected}]


def test_rollup_refuses_an_unreadable_replace_all(monkeypatch, tmp_path):
    monkeypatch.setenv("CFR_REPLACE_ALL", "yes")
    with pytest.raises(ValueError, match="CFR_REPLACE_ALL"):
        rollup.CfrSectionsRollup(output_dir=tmp_path).build(tmp_path)


# -- Title 41's compound parts (60-1, 60-2) on structural rows ------------------
# Literal granules of the 2025 edition (cfr_sections eab08a3f…): GovInfo's part ids keep only the chapter, and the
# heading's leading "N-" is the part within it.
_TITLE_41 = [
    ("CFR-2025-title41-vol1-part60", "NODE", "1-OBLIGATIONS OF CONTRACTORS AND SUBCONTRACTORS"),
    ("CFR-2025-title41-vol1-part60-id334", "NODE", "2-AFFIRMATIVE ACTION PROGRAMS"),
    ("CFR-2025-title41-vol1-part60-id334-subpartA", "NODE", "General"),
    ("CFR-2025-title41-vol1-part60-id334-toc-id335", "TOC", "Table Of Contents"),
    ("CFR-2025-title41-vol1-part60-id357-subjectgroup-id379", "APPENDIX", "Appendix to Part 60-3"),
    ("CFR-2025-title41-vol1-part60-id357", "NODE", "3-UNIFORM GUIDELINES ON EMPLOYEE SELECTION PROCEDURES (1978)"),
]


def _granules(specs, package_id="CFR-2025-title41-vol1"):
    return [
        _shape({"granuleId": gid, "granuleClass": level, "title": heading, "_package_id": package_id})
        for gid, level, heading in specs
    ]


def test_title_41_structural_rows_take_the_compound_part_their_part_granule_states():
    rows = {row["granule_id"]: row for row in compound_parts(_granules(_TITLE_41))}
    assert {gid: (row["part"], row["cfr_ref"]) for gid, row in rows.items()} == {
        "CFR-2025-title41-vol1-part60": ("60-1", "41-60-1"),
        "CFR-2025-title41-vol1-part60-id334": ("60-2", "41-60-2"),
        "CFR-2025-title41-vol1-part60-id334-subpartA": ("60-2", "41-60-2"),
        "CFR-2025-title41-vol1-part60-id334-toc-id335": ("60-2", "41-60-2"),
        "CFR-2025-title41-vol1-part60-id357-subjectgroup-id379": ("60-3", "41-60-3"),
        "CFR-2025-title41-vol1-part60-id357": ("60-3", "41-60-3"),
    }


def test_compound_parts_is_idempotent_and_leaves_sections_to_the_volume_scan():
    section = _shape(
        {
            "granuleId": "CFR-2025-title41-vol1-sec60-2-1",
            "granuleClass": "CONTENT",
            "_package_id": "CFR-2025-title41-vol1",
        }
    )
    once = compound_parts([*_granules(_TITLE_41), section])
    assert compound_parts(once) == once
    assert once[-1] == section


@pytest.mark.parametrize(
    ("granule_id", "heading"),
    [
        ("CFR-2025-title7-vol1-part8", "4-H CLUB NAME AND EMBLEM"),
        ("CFR-2025-title31-vol1-part82", "5-CENT AND ONE-CENT COIN REGULATIONS"),
        ("CFR-2025-title49-vol9-part1240", "1259-REPORTS"),
    ],
)
def test_only_title_41_reads_a_part_from_its_heading(granule_id, heading):
    rows = _granules([(granule_id, "NODE", heading)], package_id=granule_id.rsplit("-part", 1)[0])
    assert compound_parts(rows) == rows


def test_a_reserved_part_granule_of_a_compound_chapter_names_no_part():
    """GovInfo's "Reserved" part granules state no number; the chapter (41-102) is no part, so the key is NULL."""
    rows = compound_parts(
        _granules(
            [
                ("CFR-2025-title41-vol3-part102-id1058", "NODE", "74-FACILITY MANAGEMENT"),
                ("CFR-2025-title41-vol3-part102-id122", "CONTENT", "Reserved"),
            ],
            package_id="CFR-2025-title41-vol3",
        )
    )
    assert [(row["part"], row["cfr_ref"]) for row in rows] == [("102-74", "41-102-74"), (None, None)]
