"""Corrected report readers and named re-reads refresh unchanged sources and replace their blocks.

A report is one row per published part (decision 29): its parts are read all or none, its part rows are
replaced as a set, and a prior row published before ``part_id`` is kept as the package's one part.
"""

import json
import shutil
from dataclasses import replace
from types import SimpleNamespace

import httpx

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation.cbo_estimates import CBO_ESTIMATE_RULE_VERSION
from spicy_docs.schemas.committee_report_tables import COMMITTEE_REPORTS, REPORT_SECTIONS
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_committee_reports import BODY_BUDGET, build_committee_reports
from spicy_regs.transforms.committee_report_reads import READ_COLUMNS, READS_TABLE, REFUSED_FINAL, RULE_VERSIONS
from spicy_regs.transforms.table_merge import prior_scratch_path
from tests.test_committee_reports import (
    CHRG_ID,
    CRPT_ID,
    FIXTURES,
    OBSERVED_AT,
    REPORT_HTML,
    HearingBodyAcquirer,
    NoBodies,
    StubHearings,
    _no_prior,
    _package,
    _parts,
)


class NoDiscovery:
    """A discovery reader that lists nothing, so only retained evidence can trigger a read."""

    def packages(self, url, *, max_pages=40):
        return iter(())


class RetainedReport(NoBodies):
    """Serves the retained report body, optionally failing or returning an empty body."""

    def __init__(self, fail=False, empty=False):
        self.requested = []
        self.fail = fail
        self.empty = empty

    def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
        self.requested.append(package_id)
        if self.fail:
            raise ConnectionError("retained source unavailable")
        assert package_id == CRPT_ID
        if self.empty:
            return (_package(package_id, body=b"<html><body><pre></pre></body></html>"),)
        return (_package(package_id),)


def _write_prior(directory, name, columns, rows):
    """Write a prior published table where the merge and the reads checkpoint will find it."""
    schema = pa.schema([(column, pa.string()) for column in columns])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), prior_scratch_path(directory, name))


@pytest.mark.parametrize("fail,empty", [(False, False), (False, True), (True, False)])
def test_rule_change_refreshes_outside_window_and_replaces_only_successful_report(tmp_path, fail, empty):
    modified = "2026-09-18T12:00:00Z"
    _write_prior(
        tmp_path,
        "committee_reports",
        ("package_id", "last_modified"),
        [
            {"package_id": CRPT_ID, "last_modified": modified},
        ],
    )
    _write_prior(
        tmp_path,
        READS_TABLE,
        READ_COLUMNS,
        [
            {
                "package_id": CRPT_ID,
                "last_modified": modified,
                "outcome": "complete",
                "rule_version": CBO_ESTIMATE_RULE_VERSION,
                "observed_at": "2026-09-20",
            },
        ],
    )
    old = {
        "package_id": CRPT_ID,
        "seq": "99",
        "agency_label": "CONTENTS",
        "agency_key": "CONTENTS",
        "body": "obsolete block",
        "last_modified": modified,
    }
    unread = {"package_id": "CRPT-119hrpt2", "last_modified": modified, "seq": "0", "body": "unread report"}
    _write_prior(tmp_path, "report_sections", REPORT_SECTIONS.columns, [old, unread])
    acquirer = RetainedReport(fail, empty)
    paths = build_committee_reports(
        tmp_path,
        reader=NoDiscovery(),
        acquirer=acquirer,
        hearings=StubHearings(),
        download_prior=_no_prior,
    )
    assert acquirer.requested == [CRPT_ID]
    files = {path.stem: path for path in paths}
    rows = pq.read_table(files["report_sections"]).to_pylist()
    assert next(row for row in rows if row["package_id"] == unread["package_id"])["body"] == "unread report"
    changed = [row for row in rows if row["package_id"] == CRPT_ID]
    [state] = pq.read_table(files[READS_TABLE]).to_pylist()
    if fail:
        assert changed[0]["agency_label"] == "CONTENTS"
        assert changed[0]["seq"] == "99"
        assert state["outcome"] == "refused"
    else:
        assert all(row["seq"] != "99" for row in changed)
        assert all(row["agency_label"] is None and row["agency_key"] is None for row in changed)
        if empty:
            assert changed == []
        else:
            assert any(row["heading"] == "DEPARTMENT OF THE TREASURY" for row in changed)
        assert state["outcome"] == "complete"
        assert state["rule_version"] == RULE_VERSIONS["CRPT"] != CBO_ESTIMATE_RULE_VERSION
        assert state["last_modified"] == modified
    # A successful current read skips; a failed read remains eligible even
    # with no publisher change and no listing in the discovery window.
    for name in ("committee_reports", "report_sections", READS_TABLE):
        shutil.copyfile(files[name], prior_scratch_path(tmp_path, name))
    again = RetainedReport()
    build_committee_reports(
        tmp_path,
        reader=NoDiscovery(),
        acquirer=again,
        hearings=StubHearings(),
        download_prior=_no_prior,
    )
    assert again.requested == ([CRPT_ID] if fail else [])


class NoListing:
    """A discovery reader a named re-read must never reach."""

    def packages(self, url, *, max_pages=40):
        raise AssertionError("a named re-read lists nothing")


HELD, REFUSED = "CHRG-119hhrg64431", "CRPT-119hrpt2"


def _reread_priors(directory, *, unchecked=()):
    """Two checkpointed hearings and a refused report; ``unchecked`` hearings have a row but no checkpoint."""
    hearings = [
        {"package_id": package_id, "last_modified": "2026-09-18T12:00:00Z", "observed_at": "2026-09-20T00:00:00Z"}
        for package_id in (CHRG_ID, HELD, *unchecked)
    ]
    _write_prior(directory, "hearing_transcripts", ("package_id", "last_modified", "observed_at"), hearings)
    reads = [
        {
            "package_id": package_id,
            "last_modified": "2026-09-18T12:00:00Z",
            "outcome": outcome,
            "rule_version": RULE_VERSIONS[package_id[:4]],
            "observed_at": "2026-09-20T00:00:00+00:00",
        }
        for package_id, outcome in ((CHRG_ID, "complete"), (HELD, "complete"), (REFUSED, "refused"))
    ]
    _write_prior(directory, READS_TABLE, READ_COLUMNS, reads)
    return hearings, reads


def _reread(directory, reread, *, acquirer=None, hearings=None):
    return build_committee_reports(
        directory,
        reader=NoListing(),
        acquirer=acquirer or HearingBodyAcquirer(),
        hearings=hearings or StubHearings(),
        download_prior=_no_prior,
        reread=reread,
    )


def test_a_named_reread_reads_only_those_packages_and_carries_every_other_checkpoint(tmp_path):
    """A complete checkpoint is forced pending by name; a refused one not named stays unread."""
    prior_hearings, prior_reads = _reread_priors(tmp_path)
    acquirer = HearingBodyAcquirer()
    files = {path.stem: path for path in _reread(tmp_path, [CHRG_ID], acquirer=acquirer)}
    assert acquirer.requested == [CHRG_ID]
    reads = {row["package_id"]: row for row in pq.read_table(files[READS_TABLE]).to_pylist()}
    assert [reads[row["package_id"]] for row in prior_reads[1:]] == prior_reads[1:]
    assert reads[CHRG_ID]["outcome"] == "complete"
    assert reads[CHRG_ID]["observed_at"] != prior_reads[0]["observed_at"]
    hearings = {row["package_id"]: row for row in pq.read_table(files["hearing_transcripts"]).to_pylist()}
    assert hearings[HELD]["observed_at"] == prior_hearings[1]["observed_at"]
    assert hearings[CHRG_ID]["observed_at"] == OBSERVED_AT, "the re-read row carries its new capture time"


#: (named ids, whether priors exist, a prior hearing without a checkpoint, the refusal)
UNSAFE_REREADS = {
    "a report would move the discovery watermark": ([REFUSED], True, (), "hearings only"),
    "an id outside both collections": (["PLAW-119publ1"], True, (), "hearings only"),
    "a typo'd hearing": (["CHRG-119hhrg99999"], True, (), "no prior checkpoint"),
    "no prior at all": ([CHRG_ID], False, (), "no prior checkpoint"),
    "a prior row without a checkpoint": ([CHRG_ID], True, ("CHRG-119hhrg63968",), "run discovery first"),
}


@pytest.mark.parametrize(("reread", "priors", "unchecked", "refusal"), UNSAFE_REREADS.values(), ids=UNSAFE_REREADS)
def test_an_unsafe_named_reread_is_refused_before_any_source_request(tmp_path, reread, priors, unchecked, refusal):
    if priors:
        _reread_priors(tmp_path, unchecked=unchecked)
    acquirer, hearings = HearingBodyAcquirer(), StubHearings()
    with pytest.raises(ValueError, match=refusal):
        _reread(tmp_path, reread, acquirer=acquirer, hearings=hearings)
    assert acquirer.requested == [] and hearings.requested == []


class RefusingAcquirer(HearingBodyAcquirer):
    def acquire(self, package_id, *, max_bytes=None, prefer=()):
        self.requested.append(package_id)
        raise ConnectionError("stub: body unavailable")


@pytest.mark.parametrize(
    ("acquirer", "hearings"),
    [(RefusingAcquirer(), StubHearings()), (HearingBodyAcquirer(), StubHearings(details={}))],
    ids=["the body is refused", "the hearing detail is refused"],
)
def test_a_named_read_that_does_not_complete_fails_the_run_before_any_merge(tmp_path, acquirer, hearings):
    """A refused body would publish a retried-forever checkpoint; a refused detail would drop the prior event_id."""
    _reread_priors(tmp_path)
    with pytest.raises(RuntimeError, match="did not complete"):
        _reread(tmp_path, [CHRG_ID], acquirer=acquirer, hearings=hearings)
    assert acquirer.requested == [CHRG_ID]
    assert not (tmp_path / "hearing_transcripts.parquet").exists()
    assert not (tmp_path / f"{READS_TABLE}.parquet").exists()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, []), ("", []), (" CHRG-1, ,CHRG-2,", ["CHRG-1", "CHRG-2"])],
    ids=["unset", "blank", "list"],
)
def test_the_rollup_reads_its_reread_list_from_the_environment(tmp_path, monkeypatch, raw, expected):
    from spicy_regs.pipelines.rollups import committee_reports

    if raw is None:
        monkeypatch.delenv("COMMITTEE_REPORTS_REREAD", raising=False)
    else:
        monkeypatch.setenv("COMMITTEE_REPORTS_REREAD", raw)
    calls = []
    monkeypatch.setattr(committee_reports, "build_committee_reports", lambda *args, **kwargs: calls.append(kwargs))
    # This check concerns environment forwarding; receipt builds are exercised separately.
    monkeypatch.setattr(committee_reports.CommitteeReportsRollup, "build_receipts",
                        lambda self, directory, builder, **kwargs: builder(directory, **kwargs))
    committee_reports.CommitteeReportsRollup().build(tmp_path)
    assert [call["reread"] for call in calls] == [expected]


#: Its MODS (``tests/fixtures/govinfo_bodies/mods-CRPT-119hrpt811.xml``, the
#: bytes refused on 2026-09-23 and published as Part 1 on 2026-09-24) names
#: only ``-pt1``: the one package in this table whose one part is spelled so.
PART_ONLY = "CRPT-119hrpt811"
PART_ONLY_MODS = "sha256:aba0227068f60977bb1ee98d58add159064bb2b38e7b235e70b8673de7b3e125"
#: Both parts are constituents, suffixed, and the root states no bill
#: (``mods-CRPT-119hrpt455.xml``); each part names H.R. 5103 as its own.
TWO_PARTS = "CRPT-119hrpt455"
#: A report with a checkpoint complete under today's rule, so no run reads it.
UNREAD = "CRPT-119hrpt3"


class PartOnlyDiscovery:
    """Lists the part-only report beside the ordinary one, as a retained listing page."""

    def packages(self, url, *, max_pages=40):
        ids = [PART_ONLY, CRPT_ID] if "/CRPT/" in url else []
        records = [{"packageId": key, "lastModified": "2026-09-18T12:00:00Z"} for key in ids]
        page = CapturedBodyResponse(
            requested_url=url,
            resolved_url=url,
            status_code=200,
            observed_at=OBSERVED_AT,
            content_type="application/json",
            body=json.dumps(records).encode(),
        )
        yield SimpleNamespace(records=records, capture=page)


class ReportsAcquirer(NoBodies):
    """Serves each report's parts from its retained MODS; ``REFUSED`` fails the way an unavailable source does."""

    SECOND_PART = b"<html><body><pre>\n  SUPPLEMENTAL VIEWS\n\n  The second part's own text.\n</pre></body></html>\n"

    def __init__(self):
        self.requested = []

    def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
        self.requested.append(package_id)
        if package_id == REFUSED:
            raise ConnectionError("retained source unavailable")
        return _parts(package_id, bodies={f"{TWO_PARTS}-pt2": self.SECOND_PART})


def _tables(paths):
    return {path.stem: pq.read_table(path).to_pylist() for path in paths}


def _run(directory, acquirer, reader=None, **kwargs):
    return _tables(
        build_committee_reports(
            directory,
            reader=reader or NoDiscovery(),
            acquirer=acquirer,
            hearings=StubHearings(),
            download_prior=_no_prior,
            **kwargs,
        )
    )


def _keys(rows):
    return {(row["package_id"], row["part_id"]): row for row in rows}


def _checkpoints(directory, outcomes, rule_version="spicy-docs=0.31.0;cbo=cf790f0f814a;sections=report-headings-001"):
    """Prior read checkpoints; ``UNREAD``'s is complete under today's rule, so no run re-reads it."""
    _write_prior(
        directory,
        READS_TABLE,
        READ_COLUMNS,
        [
            {
                "package_id": key,
                "last_modified": "2026-09-18T12:00:00Z",
                "outcome": outcome,
                "rule_version": RULE_VERSIONS["CRPT"] if key == UNREAD else rule_version,
                "observed_at": "2026-09-24T03:47:22+00:00",
            }
            for key, outcome in outcomes.items()
        ],
    )


def test_the_retained_part_only_record_publishes_its_part_row(tmp_path):
    """``CRPT-119hrpt811``'s record states one part, ``-pt1``: one row keyed on it, numbered 1."""
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    tables = _run(tmp_path, ReportsAcquirer(), reader=PartOnlyDiscovery(), evidence=evidence)
    reports = _keys(tables["committee_reports"])
    assert set(reports) == {(CRPT_ID, CRPT_ID), (PART_ONLY, f"{PART_ONLY}-pt1")}
    part = reports[PART_ONLY, f"{PART_ONLY}-pt1"]
    assert part["part_number"] == "1" and reports[CRPT_ID, CRPT_ID]["part_number"] is None
    assert part["requested_url"].endswith(f"/{PART_ONLY}-pt1.htm")
    assert part["bill_id"] == "119-hr-2317"
    assert {(row["package_id"], row["part_id"]) for row in tables["report_sections"]} == {
        (CRPT_ID, CRPT_ID),
        (PART_ONLY, f"{PART_ONLY}-pt1"),
    }
    assert {row["outcome"] for row in tables[READS_TABLE]} == {"complete"}
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]
    assert not [row for row in events if row["event"] == "refusal"]
    used = [row["sha256"] for row in events if row["event"] == "capture" and row["stage"] == PART_ONLY + ":used"]
    assert used.count(PART_ONLY_MODS) == 1, "the MODS that names the part is retained once"


@pytest.mark.parametrize("prior_has_column", [False, True], ids=["column absent", "column NULL"])
def test_prior_rows_are_backfilled_as_their_package_and_none_is_dropped(tmp_path, prior_has_column):
    """The live state of 2026-09-24, before ``part_id``: four packages, each with a report row and a block.

    Every prior row survives the move to ``(package_id, part_id)``: a NULL or absent ``part_id`` is the
    package id, as it is for a report in one part. The re-read package ``CRPT-119hrpt811`` is replaced
    as a set, so its backfilled ``(pkg, pkg)`` row, which held Part 1, goes and ``-pt1`` takes its place.
    """
    report_columns = ("package_id", "last_modified", "requested_url", "observed_at")
    section_columns = tuple(column for column in REPORT_SECTIONS.columns if column != "part_id")
    if prior_has_column:
        report_columns += ("part_id", "part_number")
        section_columns += ("part_id",)
    part_url = f"https://www.govinfo.gov/content/pkg/{PART_ONLY}/html/{PART_ONLY}-pt1.htm"
    packages = (PART_ONLY, CRPT_ID, REFUSED, UNREAD)
    _write_prior(
        tmp_path,
        "committee_reports",
        report_columns,
        [
            {
                "package_id": key,
                "last_modified": "2026-09-18T12:00:00Z",
                "observed_at": "prior",
                "requested_url": part_url if key == PART_ONLY else None,
            }
            for key in packages
        ],
    )
    _write_prior(
        tmp_path,
        "report_sections",
        section_columns,
        [{"package_id": key, "last_modified": "2026-09-18T12:00:00Z",
          "seq": "0", "body": "prior block"} for key in packages],
    )
    _checkpoints(tmp_path, {PART_ONLY: "complete", CRPT_ID: "complete", REFUSED: "refused", UNREAD: "complete"})
    acquirer = ReportsAcquirer()
    tables = _run(tmp_path, acquirer)
    assert sorted(acquirer.requested) == sorted([PART_ONLY, CRPT_ID, REFUSED]), "the parts rule is a rule change"
    reports = _keys(tables["committee_reports"])
    assert set(reports) == {
        (PART_ONLY, f"{PART_ONLY}-pt1"),
        (CRPT_ID, CRPT_ID),
        (REFUSED, REFUSED),
        (UNREAD, UNREAD),
    }
    assert {row["package_id"] for row in reports.values()} == set(packages), "no prior package lost a row"
    for key in (REFUSED, UNREAD):
        assert reports[key, key]["observed_at"] == "prior" and reports[key, key]["part_number"] is None
    assert reports[PART_ONLY, f"{PART_ONLY}-pt1"]["observed_at"] == OBSERVED_AT
    sections = tables["report_sections"]
    kept = {(row["package_id"], row["part_id"]) for row in sections if row["body"] == "prior block"}
    assert kept == {(REFUSED, REFUSED), (UNREAD, UNREAD)}
    assert {row["part_id"] for row in sections if row["package_id"] == PART_ONLY} == {f"{PART_ONLY}-pt1"}
    reads = {row["package_id"]: row["outcome"] for row in tables[READS_TABLE]}
    assert reads == {PART_ONLY: "complete", CRPT_ID: "complete", REFUSED: "refused", UNREAD: "complete"}
    # Settled: the next run retries only the refusal, and every row stands.
    for name in ("committee_reports", "report_sections", READS_TABLE):
        shutil.copyfile(tmp_path / f"{name}.parquet", prior_scratch_path(tmp_path, name))
    again = ReportsAcquirer()
    assert set(_keys(_run(tmp_path, again)["committee_reports"])) == set(reports)
    assert again.requested == [REFUSED]


def _two_part_priors(directory):
    """Three prior part rows of ``TWO_PARTS``, one a part its record no longer states, each with a block."""
    parts = {f"{TWO_PARTS}-pt{number}": str(number) for number in (1, 2, 3)}
    _write_prior(
        directory,
        "committee_reports",
        COMMITTEE_REPORTS.columns,
        [
            {
                "package_id": TWO_PARTS,
                "part_id": part,
                "part_number": number,
                "observed_at": "prior",
                "last_modified": "2026-09-18T12:00:00Z",
            }
            for part, number in parts.items()
        ],
    )
    _write_prior(
        directory,
        "report_sections",
        REPORT_SECTIONS.columns,
        [{"package_id": TWO_PARTS, "part_id": part, "last_modified": "2026-09-18T12:00:00Z",
          "seq": "0", "body": "prior block"} for part in parts],
    )
    _checkpoints(directory, {TWO_PARTS: "complete"})
    return parts


def test_a_two_part_report_replaces_its_part_rows_as_a_set(tmp_path):
    """Each part is its own row, text and bill; a prior part the record no longer states goes with the rest."""
    _two_part_priors(tmp_path)
    tables = _run(tmp_path, ReportsAcquirer())
    reports = _keys(tables["committee_reports"])
    first, second = (TWO_PARTS, f"{TWO_PARTS}-pt1"), (TWO_PARTS, f"{TWO_PARTS}-pt2")
    assert set(reports) == {first, second}
    assert [reports[key]["part_number"] for key in (first, second)] == ["1", "2"]
    assert {reports[key]["observed_at"] for key in (first, second)} == {OBSERVED_AT}
    assert {reports[key]["bill_id"] for key in (first, second)} == {"119-hr-5103"}, "the root states no bill"
    assert reports[first]["sha256"] != reports[second]["sha256"], "each part is read at its own stem"
    assert reports[second]["requested_url"].endswith(f"/{TWO_PARTS}-pt2.htm")
    sections = tables["report_sections"]
    assert "prior block" not in {row["body"] for row in sections}
    assert {(row["part_id"], row["seq"]) for row in sections} >= {(first[1], "0"), (second[1], "0")}


class SevenParts:
    """The ``CRPT-119hrpt455`` summary and MODS through the real acquirer, its record grown to seven parts.

    Parts 3 to 7 repeat Part 2's constituent under their own id and number, so the record passes every
    check but the budget's. Counts each request by host.
    """

    def __init__(self):
        self.paths = []
        mods = (FIXTURES / f"mods-{TWO_PARTS}.xml").read_text()
        start, end = mods.index('    <relatedItem type="constituent" ID="id-hr455p2"'), mods.rindex("</mods>")
        more = "".join(
            mods[start:end]
            .replace("-pt2", f"-pt{number}")
            .replace("hr455p2", f"hr455p{number}")
            .replace("<partNumber>2</partNumber>", f"<partNumber>{number}</partNumber>")
            for number in range(3, 8)
        )
        self.mods = (mods[:end] + more + mods[end:]).encode()

    def respond(self, request):
        self.paths.append((request.url.host, request.url.path))
        if request.url.path.endswith("/summary"):
            raw, kind = (FIXTURES / f"summary-{TWO_PARTS}.json").read_bytes(), "application/json"
        elif request.url.path.endswith("/mods"):
            raw, kind = self.mods, "application/xml"
        else:
            raw, kind = REPORT_HTML, "text/html"
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": kind})


def test_a_report_of_seven_parts_is_refused_before_any_body_request(tmp_path):
    """``BODY_BUDGET`` allows eight requests, and seven parts need nine: summary, MODS, then no body at all.

    The refusal keeps the package's prior rows, as every refusal does. Every later run would meet it
    again, so it is final: not asked for until the package's stamp, the rule or the budget moves.
    """
    parts = _two_part_priors(tmp_path)
    source = SevenParts()
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    budget = replace(BODY_BUDGET, min_request_interval_seconds=0.0)  # the production count, unpaced
    with GovInfoBodyAcquirer(budget=budget, api_key="test-key", transport=httpx.MockTransport(source.respond)) as owner:
        tables = _run(tmp_path, owner, evidence=evidence)
    assert [path.rsplit("/", 1)[-1] for _, path in source.paths] == ["summary", "mods"]
    assert {host for host, _ in source.paths} == {"api.govinfo.gov"}, "no content-host request"
    [refusal] = [
        row
        for row in (json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines())
        if row["event"] == "refusal"
    ]
    assert refusal["error_type"] == "GovInfoPartsOverBudgetError"
    assert "states 7 parts, which need 9 requests; the request budget allows 8" in refusal["message"]
    assert set(_keys(tables["committee_reports"])) == {(TWO_PARTS, part) for part in parts}
    assert {row["observed_at"] for row in tables["committee_reports"]} == {"prior"}
    assert [row["outcome"] for row in tables[READS_TABLE]] == [REFUSED_FINAL]
    for name in ("committee_reports", "report_sections", READS_TABLE):
        shutil.copyfile(tmp_path / f"{name}.parquet", prior_scratch_path(tmp_path, name))
    again = SevenParts()
    with GovInfoBodyAcquirer(budget=budget, api_key="test-key", transport=httpx.MockTransport(again.respond)) as owner:
        _run(tmp_path, owner)
    assert again.paths == [], "a final refusal is not asked for again"


class ListedReport:
    """Discovery listing ``CRPT_ID`` at one ``lastModified``, the way it answers a publisher refresh."""

    def __init__(self, modified):
        self.modified = modified

    def packages(self, url, *, max_pages=40):
        yield SimpleNamespace(records=[{"packageId": CRPT_ID, "lastModified": self.modified}] if "/CRPT/" in url else [])


def test_a_refreshed_report_whose_read_is_refused_keeps_its_rows_and_is_retried(tmp_path):
    """Complete under today's rule, then refreshed and refused once: the checkpoint says ``refused`` at the new stamp,
    never the old ``complete``, the prior rows stand, and the next run retries it with nothing listed."""
    read = _run(tmp_path, RetainedReport(), reader=ListedReport("2026-09-18T12:00:00Z"))
    assert [row["outcome"] for row in read[READS_TABLE]] == ["complete"]
    names = ("committee_reports", "report_sections", READS_TABLE)
    for name in names:
        shutil.copyfile(tmp_path / f"{name}.parquet", prior_scratch_path(tmp_path, name))
    refused = _run(tmp_path, RetainedReport(fail=True), reader=ListedReport("2026-09-20T12:00:00Z"))
    [state] = refused[READS_TABLE]
    assert (state["outcome"], state["last_modified"]) == ("refused", "2026-09-20T12:00:00Z")
    assert state["rule_version"] == RULE_VERSIONS["CRPT"]
    for name in ("committee_reports", "report_sections"):
        assert refused[name] == read[name], f"{name}: a refused read keeps the prior rows"
    for name in names:
        shutil.copyfile(tmp_path / f"{name}.parquet", prior_scratch_path(tmp_path, name))
    again = RetainedReport()
    _run(tmp_path, again)
    assert again.requested == [CRPT_ID]


HEARING_1946 = "CHRG-79jhrg79716p11"
#: The part's PDF as GovInfo serves it (HEAD, 2026-09-26): 312,487,940 bytes against the 24 MiB bound.
PDF_1946_BYTES = 312_487_940


class OversizedHearing:
    """The 1946 Pearl Harbor part through the real acquirer: its retained summary and MODS, a PDF past the bound.

    The PDF answer states its length and sends nothing more; the transport refuses on the stated length.
    """

    def __init__(self):
        self.paths = []

    def respond(self, request):
        self.paths.append(request.url.path.rsplit("/", 1)[-1])
        if request.url.path.endswith("/summary"):
            raw, kind = (FIXTURES / f"summary-{HEARING_1946}.json").read_bytes(), "application/json"
        elif request.url.path.endswith("/mods"):
            raw, kind = (FIXTURES / f"mods-{HEARING_1946}.xml").read_bytes(), "application/xml"
        else:
            return httpx.Response(200, stream=httpx.ByteStream(b""),
                                  headers={"content-type": "application/pdf", "content-length": str(PDF_1946_BYTES)})
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": kind})


class ListedHearing:
    """Discovery listing the 1946 part at one ``lastModified``."""

    def __init__(self, modified=None):
        self.modified = modified

    def packages(self, url, *, max_pages=40):
        listed = [{"packageId": HEARING_1946, "lastModified": self.modified}] if self.modified and "/CHRG/" in url else []
        page = CapturedBodyResponse(requested_url=url, resolved_url=url, status_code=200, observed_at=OBSERVED_AT,
                                    content_type="application/json", body=json.dumps(listed).encode())
        yield SimpleNamespace(records=listed, capture=page)


def _hearing_run(directory, reader, evidence=None):
    source = OversizedHearing()
    budget = replace(BODY_BUDGET, min_request_interval_seconds=0.0)
    with GovInfoBodyAcquirer(budget=budget, api_key="test-key", transport=httpx.MockTransport(source.respond)) as owner:
        tables = _run(directory, owner, reader=reader, evidence=evidence)
    for name in ("hearing_transcripts", READS_TABLE):
        if (directory / f"{name}.parquet").exists():
            shutil.copyfile(directory / f"{name}.parquet", prior_scratch_path(directory, name))
    return tables, source.paths


def test_a_body_past_the_byte_bound_is_final_on_its_stated_size_until_its_stamp_moves(tmp_path, monkeypatch):
    """The two 1946 parts cost summary, MODS and a refused PDF request on every run; now once per stamp.

    The refusal rests on the size the PDF's response stated, so a larger bound that still falls short
    reads nothing, and only a bound past that size reads it again.
    """
    from spicy_regs.transforms import build_committee_reports as module

    evidence = CaptureEvidence(tmp_path, "committee-reports")
    stamp = "2026-09-24T12:59:27Z"
    first, paths = _hearing_run(tmp_path, ListedHearing(stamp), evidence)
    assert paths == ["summary", "mods", f"{HEARING_1946}.pdf"]
    [state] = first[READS_TABLE]
    assert (state["outcome"], state["last_modified"]) == (REFUSED_FINAL, stamp)
    assert state["rule_version"] == f"{RULE_VERSIONS['CHRG']};refused-under=size={PDF_1946_BYTES}"
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]
    [refusal] = [row for row in journal if row["event"] == "refusal"]
    assert refusal["response"]["unavailable_reason"] == "response-byte-limit"
    assert (refusal["response"]["stated_byte_size"], refusal["response"]["observed_byte_size"]) == (PDF_1946_BYTES, None)
    assert [row["outcome"] for row in journal if row["event"] == "package-outcome"] == [REFUSED_FINAL]

    # Nothing listed, or listed at the same stamp: no request, and the selection names it as settled.
    for reader in (ListedHearing(), ListedHearing(stamp)):
        evidence = CaptureEvidence(tmp_path / "again", "committee-reports")
        _, paths = _hearing_run(tmp_path, reader, evidence)
        assert paths == []
        journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]
        [selection] = [row for row in journal if row["event"] == "package-selection" and row["collection"] == "CHRG"]
        assert selection["selected"] == [] and selection["refused_final"] == [HEARING_1946]
        shutil.rmtree(tmp_path / "again")

    # A bound still short of the stated size, or another request budget, reads nothing. The bound is
    # stood in for: SpicyDocs caps a GovInfo body budget at 24 MiB, so no run can yet read past it.
    for bound, requests in ((100 * 1024 * 1024, 8), (24 * 1024 * 1024, 12)):
        monkeypatch.setattr(module, "BODY_BUDGET", SimpleNamespace(max_body_bytes=bound, max_requests=requests))
        monkeypatch.setattr(module, "REFUSAL_BOUNDS", f"body={bound};requests={requests}")
        _, paths = _hearing_run(tmp_path, ListedHearing())
        assert paths == []
    # A new stamp, or a bound past the stated size, asks again.
    monkeypatch.undo()
    _, paths = _hearing_run(tmp_path, ListedHearing("2026-10-01T00:00:00Z"))
    assert paths == ["summary", "mods", f"{HEARING_1946}.pdf"]
    monkeypatch.setattr(module, "BODY_BUDGET", SimpleNamespace(max_body_bytes=400 * 1024 * 1024, max_requests=8))
    _, paths = _hearing_run(tmp_path, ListedHearing())
    assert paths == ["summary", "mods", f"{HEARING_1946}.pdf"]


def test_a_transient_refusal_stays_pending_every_run(tmp_path):
    """Only a refusal the publisher's record decides is final; a transport failure is asked again."""
    first = _run(tmp_path, RetainedReport(fail=True), reader=ListedReport("2026-09-18T12:00:00Z"))
    assert [row["outcome"] for row in first[READS_TABLE]] == ["refused"]
    shutil.copyfile(tmp_path / f"{READS_TABLE}.parquet", prior_scratch_path(tmp_path, READS_TABLE))
    retry = RetainedReport(fail=True)
    _run(tmp_path, retry)
    assert retry.requested == [CRPT_ID]
