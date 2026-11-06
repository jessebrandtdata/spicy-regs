"""Placeholder bodies read from the offered PDF under its own digest, and whole hearing parents refreshed.

A part whose text is the publisher's placeholder is read again from its offered PDF; a part offering none is
published as read, marked, and complete. The journal keeps digests and counts, never text.
"""
import hashlib
import json
import shutil
from dataclasses import replace
import pyarrow.parquet as pq
import pytest
from spicy_regs.source_evidence import CaptureEvidence, EvidenceMemberSource
from spicy_regs.transforms.build_committee_reports import _read_bodies, build_committee_reports
from spicy_regs.transforms.committee_report_reads import READ_COLUMNS, READS_TABLE, RULE_VERSIONS
from spicy_regs.transforms.table_merge import prior_scratch_path
from spicy_docs.schemas.committee_report_tables import REPORT_SECTIONS
from spicy_docs.schemas.hearing_bill_link_tables import HEARING_BILL_LINKS
from tests.test_committee_reports import CHRG_ID, NoBodies, StubHearings, _package, _parts, _no_prior
from tests.test_report_section_refresh import NoDiscovery, _write_prior
from spicy_docs.sources.govinfo.bodies import GovInfoBodySourceError
from spicy_docs.sources.govinfo.body_acquisition import GovInfoFormatNotOfferedError
from spicy_docs.reading.refusals import RefusedResponse, attach_refused_response
from tests.pdf_fixtures import make_pdf

PLACEHOLDER = b'<html><body><pre>[TEXT NOT AVAILABLE IN REFER TO PDF]</pre></body></html>'
TESTIMONY = ["Actual hearing testimony", "More evidence"]
#: A two-part report (`tests/fixtures/govinfo_bodies/README.md`) whose Part 2 the publisher left as a placeholder.
REPORT_ID, PLACEHOLDER_PART = "CRPT-119hrpt455", "CRPT-119hrpt455-pt2"
REPORT_PAGES = ["Committee report front matter", "DEPARTMENT OF THE TREASURY", "The Committee recommends it."]
OLD = dict(last_modified="2026-09-18T12:00:00Z", outcome="complete", rule_version="27acfc7cf7ae", observed_at="2026-09-20")


class PlaceholderSource:
    def __init__(self, fail=False, offered=("htm", "pdf"), pdf_error=None):
        self.requests = []
        self.fail = fail
        self.offered = offered
        self.pdf_error = pdf_error

    def acquire(self, package_id, *, prefer=(), max_bytes=None):
        self.requests.append(prefer)
        if prefer == ("pdf",):
            if self.fail:
                raise ConnectionError("PDF failed")
            if self.pdf_error is not None:
                raise self.pdf_error
            return _package(package_id, fmt="pdf", media_type="application/pdf", body=make_pdf(TESTIMONY))
        return replace(_package(package_id, body=PLACEHOLDER), offered_formats=self.offered)

    def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
        raise AssertionError("a hearing states no parts")


class PlaceholderReport(NoBodies):
    """Part 2 of a two-part report is a placeholder; its PDF is served, or a PDF set lacking that part."""

    def __init__(self, drop_part=False):
        self.requests = []
        self.drop_part = drop_part

    def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
        self.requests.append(prefer)
        if prefer == ("pdf",):
            pdfs = _parts(package_id, fmt="pdf", media_type="application/pdf", body=make_pdf(REPORT_PAGES))
            return tuple(body for body in pdfs if not (self.drop_part and body.part.part_id == PLACEHOLDER_PART))
        return _parts(package_id, bodies={PLACEHOLDER_PART: PLACEHOLDER})


def _journal(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


def _files(tmp_path, acquirer):
    paths = build_committee_reports(tmp_path, reader=NoDiscovery(), acquirer=acquirer, hearings=StubHearings(),
                                    download_prior=_no_prior)
    return {path.stem: path for path in paths}


def test_placeholder_reads_offered_pdf_and_journals_digests_not_text(tmp_path):
    source = PlaceholderSource()
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    [read] = _read_bodies(source, CHRG_ID, "CHRG", evidence)
    assert source.requests == [(), ("pdf",)]
    assert read.body.format == "pdf" and read.derived.pages is not None and len(read.derived.pages) == 2
    assert "Actual hearing testimony" in read.derived.text and read.completeness == "pdf_extracted"
    events = _journal(evidence)
    captures = [e for e in events if e["event"] == "capture"]
    assert any(e["stage"].endswith(":pdf-fallback") for e in captures)
    # The placeholder's own capture is retained beside the PDF's, each under its own digest.
    source_reader = EvidenceMemberSource(evidence.artifact_dir)
    originals = set()
    for event in captures:
        with source_reader.open(event["blob_member"]) as stream:
            if stream.read() == PLACEHOLDER:
                originals.add(event["sha256"])
    [original] = originals
    [placeholder] = [e for e in events if e["event"] == "body-placeholder"]
    assert placeholder["source_sha256"] == original and placeholder["offered_formats"] == ["htm", "pdf"]
    [text] = [e for e in events if e["event"] == "body-text"]
    assert text["source_sha256"] == read.body.body_capture.sha256 and text["replaces_sha256"] == original
    assert text["text_sha256"] == read.text_sha256 == "sha256:" + hashlib.sha256(read.derived.text.encode()).hexdigest()
    assert text["text_chars"] == len(read.derived.text) and text["page_count"] == 2
    assert text["derivation"] == "pdf-extraction-gpo-normalized" and text["body_completeness"] == "pdf_extracted"
    assert set(text["extraction_versions"]) == {"spicy-docs", "pymupdf"} and text["cleanup"]["pages"]
    # Digests and counts only: the bytes are retained, so the text is re-derivable and never journaled.
    assert "text" not in text and "pages" not in text and "text" not in placeholder
    assert "Actual hearing testimony" not in (evidence.artifact_dir / "journal.jsonl").read_text()


def test_an_ordinary_body_journals_no_text_event(tmp_path):
    """The row binds an ordinary body's digest to its text digest; the journal adds nothing per body."""
    class PlainReport(NoBodies):
        def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
            assert prefer == (), "no placeholder, so no PDF is asked for"
            return _parts(package_id)

    evidence = CaptureEvidence(tmp_path, "committee-reports")
    reads = _read_bodies(PlainReport(), REPORT_ID, "CRPT", evidence)
    assert [read.completeness for read in reads] == ["not_flagged", "not_flagged"]
    assert not [e for e in _journal(evidence) if e["event"] in ("body-text", "body-placeholder")]


@pytest.mark.parametrize("fail", [False, True])
def test_hearing_rule_change_replaces_full_parent_or_keeps_prior_rows(tmp_path, fail):
    """A failed PDF read keeps the prior links and records a refusal, so every later run retries it."""
    old_state = dict(package_id=CHRG_ID, **OLD)
    _write_prior(tmp_path, READS_TABLE, READ_COLUMNS, [old_state])
    _write_prior(tmp_path, "hearing_transcripts", ("package_id", "last_modified"), [old_state])
    old_link = dict(package_id=CHRG_ID, last_modified=old_state["last_modified"], bill_id="119-hr-1",
                    link_source="mods_cover", held_date="2025-01-01", link_rule_version="27acfc7cf7ae")
    _write_prior(tmp_path, "hearing_bill_links", HEARING_BILL_LINKS.columns, [old_link])
    files = _files(tmp_path, PlaceholderSource(fail=fail))
    [state] = pq.read_table(files[READS_TABLE]).to_pylist()
    links = pq.read_table(files["hearing_bill_links"]).to_pylist()
    if fail:
        assert state["outcome"] == "refused" and state["rule_version"] == RULE_VERSIONS["CHRG"]
        assert [{key: link[key] for key in old_link} for link in links] == [old_link]
    else:
        assert links == []  # successful no-COVER result removes the stale relationship
        assert state["rule_version"] == RULE_VERSIONS["CHRG"] != old_state["rule_version"]
        [row] = pq.read_table(files["hearing_transcripts"]).to_pylist()
        assert row["format"] == "pdf" and row["body_completeness"] == "pdf_extracted"


def test_placeholder_with_no_offered_pdf_is_complete_and_not_read_again(tmp_path):
    """Nothing more is published, so the placeholder is the row, marked; re-reading it every run would starve the cap."""
    _write_prior(tmp_path, READS_TABLE, READ_COLUMNS, [dict(package_id=CHRG_ID, **OLD)])
    _write_prior(tmp_path, "hearing_transcripts", ("package_id", "last_modified"), [dict(package_id=CHRG_ID, **OLD)])
    source = PlaceholderSource(offered=("htm",))
    files = _files(tmp_path, source)
    assert source.requests == [()]
    [row] = pq.read_table(files["hearing_transcripts"]).to_pylist()
    assert row["format"] == "htm" and row["body_completeness"] == "publisher_placeholder"
    assert row["text_derivation"] == "markup-reader"
    [state] = pq.read_table(files[READS_TABLE]).to_pylist()
    assert state["outcome"] == "complete" and state["rule_version"] == RULE_VERSIONS["CHRG"]
    for path in files.values():
        shutil.copyfile(path, prior_scratch_path(tmp_path, path.stem))
    again = PlaceholderSource(offered=("htm",))
    _files(tmp_path, again)
    assert again.requests == []


def test_report_placeholder_part_takes_its_pdf_and_its_sections_cite_pages(tmp_path):
    """Only the placeholder part changes text source; its sections are replaced and carry the PDF's pages."""
    _write_prior(tmp_path, READS_TABLE, READ_COLUMNS, [dict(package_id=REPORT_ID, **OLD)])
    _write_prior(tmp_path, "committee_reports", ("package_id", "last_modified", "part_id"),
                 [dict(package_id=REPORT_ID, last_modified=OLD["last_modified"], part_id=part)
                  for part in (f"{REPORT_ID}-pt1", PLACEHOLDER_PART)])
    stale = dict(package_id=REPORT_ID, part_id=PLACEHOLDER_PART, seq="0", pattern="full_report",
                 body="[TEXT NOT AVAILABLE IN REFER TO PDF]", last_modified=OLD["last_modified"])
    _write_prior(tmp_path, "report_sections", REPORT_SECTIONS.columns, [stale])
    source = PlaceholderReport()
    files = _files(tmp_path, source)
    assert source.requests == [(), ("pdf",)]
    reports = {row["part_id"]: row for row in pq.read_table(files["committee_reports"]).to_pylist()}
    first, placeholder = reports[f"{REPORT_ID}-pt1"], reports[PLACEHOLDER_PART]
    assert (first["format"], first["body_completeness"], first["text_derivation"]) == ("htm", "not_flagged", "markup-reader")
    assert (placeholder["format"], placeholder["body_completeness"], placeholder["page_count"]) == ("pdf", "pdf_extracted", "3")
    assert placeholder["text_derivation"] == "pdf-extraction-gpo-normalized"
    assert placeholder["sha256"] == "sha256:" + hashlib.sha256(make_pdf(REPORT_PAGES)).hexdigest()
    sections = pq.read_table(files["report_sections"]).to_pylist()
    assert stale["body"] not in [row["body"] for row in sections]
    assert [(row["pattern"], row["page_start"], row["page_end"]) for row in sections
            if row["part_id"] == PLACEHOLDER_PART] == [("preamble", "1", "1"), ("department", "2", "3")]
    assert {(row["page_start"], row["page_end"]) for row in sections if row["part_id"] != PLACEHOLDER_PART} == {(None, None)}
    [state] = pq.read_table(files[READS_TABLE]).to_pylist()
    assert state["outcome"] == "complete" and state["rule_version"] == RULE_VERSIONS["CRPT"]


def test_a_pdf_set_missing_the_placeholder_part_is_refused_and_prior_rows_stand(tmp_path):
    """The part-identity guard: a PDF read that does not return the placeholder part refuses the package."""
    with pytest.raises(ValueError, match=PLACEHOLDER_PART):
        _read_bodies(PlaceholderReport(drop_part=True), REPORT_ID, "CRPT")
    _write_prior(tmp_path, READS_TABLE, READ_COLUMNS, [dict(package_id=REPORT_ID, **OLD)])
    prior = dict(package_id=REPORT_ID, part_id=PLACEHOLDER_PART, seq="0", body="prior block", last_modified=OLD["last_modified"])
    _write_prior(tmp_path, "report_sections", REPORT_SECTIONS.columns, [prior])
    files = _files(tmp_path, PlaceholderReport(drop_part=True))
    [state] = pq.read_table(files[READS_TABLE]).to_pylist()
    assert state["outcome"] == "refused"
    assert [row["body"] for row in pq.read_table(files["report_sections"]).to_pylist()] == ["prior block"]


def test_unique_date_correction_replaces_nonnull_scalar_on_same_cover_identity(tmp_path):
    old = dict(package_id=CHRG_ID, **OLD)
    _write_prior(tmp_path, READS_TABLE, READ_COLUMNS, [old])
    _write_prior(tmp_path, "hearing_transcripts", ("package_id", "last_modified"), [old])
    _write_prior(tmp_path, "hearing_bill_links", HEARING_BILL_LINKS.columns, [dict(
        package_id=CHRG_ID, last_modified=old["last_modified"], bill_id="119-hr-1", link_source="mods_cover",
        held_date="2025-01-01", link_rule_version="27acfc7cf7ae",
    )])

    class CompiledSource:
        def acquire(self, package_id, **kwargs):
            body = _package(package_id)
            bill = next(b for b in body.mods.bills if b.number == "1")
            return replace(body, mods=replace(body.mods, bills=(replace(bill, context="COVER"),), held_date=None, held_dates=("2025-01-01", "2025-01-02")))

        def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
            raise AssertionError("a hearing states no parts")

    paths = _files(tmp_path, CompiledSource())
    [link] = pq.read_table(paths["hearing_bill_links"]).to_pylist()
    assert link["bill_id"] == "119-hr-1" and link["held_date"] is None
    assert link["last_modified"] == old["last_modified"]
    assert json.loads(link["held_dates_json"]) == ["2025-01-01", "2025-01-02"]


def _over_byte_bound():
    error = GovInfoBodySourceError("Body source response exceeds its byte bound")
    attach_refused_response(error, RefusedResponse("pdf", "transport", None, "application/octet-stream",
                                                   "response-byte-limit", 50_000_000))
    return error


@pytest.mark.parametrize("final", ["over_byte_bound", "format_not_offered"])
def test_an_offered_pdf_refused_for_good_keeps_the_placeholder_and_is_not_read_again(tmp_path, final):
    """A refusal every later run would repeat marks the placeholder complete instead of re-reading it forever."""
    error = _over_byte_bound() if final == "over_byte_bound" else GovInfoFormatNotOfferedError(CHRG_ID, ("pdf",), ("htm",))
    _write_prior(tmp_path, READS_TABLE, READ_COLUMNS, [dict(package_id=CHRG_ID, **OLD)])
    _write_prior(tmp_path, "hearing_transcripts", ("package_id", "last_modified"), [dict(package_id=CHRG_ID, **OLD)])
    source = PlaceholderSource(pdf_error=error)
    files = _files(tmp_path, source)
    assert source.requests == [(), ("pdf",)]
    [row] = pq.read_table(files["hearing_transcripts"]).to_pylist()
    assert row["format"] == "htm" and row["body_completeness"] == "publisher_placeholder"
    [state] = pq.read_table(files[READS_TABLE]).to_pylist()
    assert state["outcome"] == "complete"
    for path in files.values():
        shutil.copyfile(path, prior_scratch_path(tmp_path, path.stem))
    again = PlaceholderSource(pdf_error=error)
    _files(tmp_path, again)
    assert again.requests == []
