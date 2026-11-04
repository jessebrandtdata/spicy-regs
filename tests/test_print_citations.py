"""Hermetic tests for the print-citations transform's own seam.

No network: the discovery reader, the body acquirer and the roster acquirer are
all stubbed, and the bodies are synthesized PDFs from ``tests/pdf_fixtures.py``
read by the same ``extraction.body_text`` the transform uses.

**What is established here and what deliberately is not.** Whether
``find_citations`` reads ``H.R. 1093`` correctly, whether a print's *ordered
reported* attaches to the right bill, and what a column means are all settled
in spicy-docs beside the code that decides them; re-asserting any of it here
would be a second copy of the same claim that can only drift. What this
repository owns, and what these tests hold, is the acquisition seam: which
packages are enumerated and which are dropped, which are fetched and which are
skipped, that a refusal is a counted row and a credential refusal is not, that
the per-run cap bounds the fetch and not the walk, and that the four tables
come out under their contracts' schemas with ``document_kind`` telling the two
families apart inside the shared link table.
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.sources.govinfo.activity_reports import is_activity_report
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.govinfo.bodies import (
    BODY_PREFERENCE,
    PackageBodyIdentity,
    PackageSummary,
    parse_package_id,
    validate_package_mods,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyBudget, GovInfoPackageBody
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms.build_print_citations import (
    CHECKPOINT_NAMESPACE,
    PRINT_BODY_PREFERENCE,
    ACTIVITY_REPORTS,
    BILL_ACTIONS,
    BUDGET_VOLUMES,
    CITATIONS,
    build_print_citations,
)
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from tests.pdf_fixtures import make_multiline_pdf

MODS_DIR = Path(__file__).parent / "fixtures" / "govinfo_bodies"
BUDGET_DIR = Path(__file__).parent / "fixtures" / "budget_volumes"
OBSERVED_AT = "2026-09-20T00:00:00Z"

#: A real committee-report MODS stands in for an activity report's: the shape
#: the transform reads is the same, and which CRPT packages are activity
#: reports is the title rule's question, tested separately below.
CRPT_ID = "CRPT-119hrpt1"
BUDGET_ID = "BUDGET-2026-MSR"

#: Two pages whose text names keys of several kinds, so citation rows, a bill
#: action row and the page attribution are all reached. What each rule reads
#: out of it is spicy-docs' business; that rows arrive at all is this test's.
REPORT_PAGES = [
    ["ACTIVITY REPORT", "The Committee ordered H.R. 1093 reported on March 4, 2024.", "1"],
    ["Public Law No: 118-11 was enacted.", "See 5 U.S.C. 552 and RIN 1904-AF21.", "2"],
]

BUDGET_PAGES = [
    ["MID-SESSION REVIEW", "Enacted as Public Law 118-5, this account continues.", "1"],
    ["See 31 U.S.C. 1105 for the requirement.", "Funded by H.R. 7806 as reported.", "2"],
]

BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=1 << 20,
    max_metadata_bytes=1 << 18,
    timeout_seconds=10.0,
    min_request_interval_seconds=0.0,
)


def _mods_bytes(package_id: str) -> bytes:
    if package_id.startswith("BUDGET-"):
        return (BUDGET_DIR / f"mods-{package_id}.xml").read_bytes()
    path = MODS_DIR / f"mods-{package_id}.xml"
    if path.exists():
        return path.read_bytes()
    # A queue needs many report ids; each reads as CRPT_ID's MODS under its own id.
    return (MODS_DIR / f"mods-{CRPT_ID}.xml").read_bytes().replace(CRPT_ID.encode(), package_id.encode())


def _package(
    package_id: str,
    *,
    pages: list[list[str]],
    last_modified: str | None = "2026-09-18T12:00:00Z",
    pdf_pages=None,
    title: str | None = None,
):
    """A fetched package body, built the way the acquirer would have built one.

    A report's default title states the Congress its id was filed in, as a
    House report's does; the Senate and no-statement cases pass their own.
    """
    identity = parse_package_id(package_id)
    if title is None:
        congress = getattr(identity, "congress", None)
        title = f"Report {package_id}" if congress is None else f"ACTIVITY REPORT FOR THE {congress}TH CONGRESS"
    body = make_multiline_pdf(pdf_pages if pdf_pages is not None else pages)
    url = f"https://www.govinfo.gov/content/pkg/{package_id}/pdf/{package_id}.pdf"
    capture = CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type="application/pdf",
        observed_at=OBSERVED_AT,
        body=body,
    )
    mods_url = f"https://api.govinfo.gov/packages/{package_id}/mods"
    mods_capture = CapturedBodyResponse(
        requested_url=mods_url,
        resolved_url=mods_url,
        status_code=200,
        content_type="application/xml",
        observed_at=OBSERVED_AT,
        body=_mods_bytes(package_id),
    )
    mods = validate_package_mods(
        mods_capture.body, package=identity, final_url=mods_url, max_bytes=BUDGET.max_metadata_bytes
    )
    return GovInfoPackageBody(
        identity=identity,
        format="pdf",
        preference=("xml", "htm", "txt", "pdf"),
        offered_formats=("pdf",),
        summary=PackageSummary(
            identity=identity,
            collection_code=identity.collection,
            date_issued="2026-01-05",
            last_modified=last_modified,
            title=title,
            download_links=(),
            pages=str(len(pages)),
        ),
        mods=mods,
        body=PackageBodyIdentity(
            identity=identity,
            format="pdf",
            media_type="application/pdf",
            final_url=url,
            byte_size=len(body),
        ),
        summary_capture=capture,
        mods_capture=mods_capture,
        body_capture=capture,
        request_count=3,
        budget=BUDGET,
    )


class _Page:
    def __init__(self, records):
        self.records = tuple(records)
        self.declared_count = len(self.records)
        self.next_url = None


class _Reader:
    """A discovery reader that answers one page per collection and counts the walks."""

    def __init__(self, rows: dict[str, list[dict]]):
        self.rows = rows
        self.walked: list[str] = []

    def packages(self, url, *, max_pages=1):
        collection = url.rsplit("collection=", 1)[-1]
        self.walked.append(collection)
        return iter([_Page(self.rows.get(collection, []))])


class _Acquirer:
    """A body acquirer over prepared packages; anything else refuses, by name."""

    def __init__(self, packages: dict[str, object], refuse: dict[str, Exception] | None = None):
        self.packages = packages
        self.refuse = refuse or {}
        self.asked: list[str] = []
        self.preferences: list[tuple[str, ...]] = []

    def acquire(self, package_id, *, prefer=(), max_bytes=None):
        self.asked.append(package_id)
        self.preferences.append(tuple(prefer))
        if package_id in self.refuse:
            raise self.refuse[package_id]
        return self.packages[package_id]


class _NoRosters:
    """Both chamber files refuse, which is a degraded vocabulary and not a failure."""

    def acquire_house(self, *, congress, session=None, max_bytes=None):
        raise ConnectionError("no network in a hermetic test")

    def acquire_senate(self, *, max_bytes=None):
        raise ConnectionError("no network in a hermetic test")


def _no_download(remote_key: str, local_path: Path) -> bool:
    return False


def _listing(package_id: str, title: str, last_modified: str | None = "2026-09-18T12:00:00Z") -> dict:
    return {"packageId": package_id, "title": title, "lastModified": last_modified}


def _build(tmp_path, reader, acquirer, **kwargs):
    return build_print_citations(
        tmp_path, reader=reader, acquirer=acquirer, rosters=_NoRosters(), download_prior=_no_download, **kwargs
    )


def _rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def _publish_as_prior(paths: tuple[Path, ...]) -> None:
    for path in paths:
        path.rename(path.with_name(f"_{path.stem}_prior.parquet"))


def test_modified_print_preserves_findings_from_the_prior_text_digest(tmp_path):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    actions, citations = _rows(paths[2]), _rows(paths[3])
    assert actions and citations, "the first read must produce findings to retain as history"
    _publish_as_prior(paths)

    modified = "2026-09-19T12:00:00Z"
    acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=[["No bill or citation remains."]], last_modified=modified)
    paths = _build(tmp_path, _Reader({"CRPT": [dict(listing, lastModified=modified)]}), acquirer)
    assert _rows(paths[2]) == actions
    assert _rows(paths[3]) == citations
    current_digest = _rows(paths[0])[0]["text_sha256"]
    assert all(row["text_sha256"] != current_digest for row in actions + citations)


def test_same_text_correction_retires_current_findings_but_keeps_older_text(tmp_path, monkeypatch):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    old_actions, old_citations = _rows(paths[2]), _rows(paths[3])
    _publish_as_prior(paths)

    modified = "2026-09-19T12:00:00Z"
    acquirer.packages[CRPT_ID] = _package(
        CRPT_ID, pages=[["The Committee ordered H.R. 1093 reported on April 4, 2024."]], last_modified=modified
    )
    listing["lastModified"] = modified
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    current_digest = _rows(paths[0])[0]["text_sha256"]
    assert {row["text_sha256"] for row in _rows(paths[2])} == {old_actions[0]["text_sha256"], current_digest}
    assert {row["text_sha256"] for row in _rows(paths[3])} == {old_citations[0]["text_sha256"], current_digest}
    _publish_as_prior(paths)
    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.CITATION_RULE_SET_VERSION", "corrected")
    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.find_citations", lambda *_args, **_kwargs: ())

    paths = _build(tmp_path, _Reader({}), acquirer)
    assert _rows(paths[2]) == old_actions
    assert _rows(paths[3]) == old_citations
    assert _rows(paths[0])[0]["text_sha256"] == current_digest
    _publish_as_prior(paths)
    acquirer.asked.clear()
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    assert acquirer.asked == [], "empty current-digest findings and older history share a complete checkpoint"
    assert _rows(paths[2]) == old_actions
    assert _rows(paths[3]) == old_citations


def _read_differently(monkeypatch, marker: str = "\u25cb") -> None:
    """A new derivation of the same bytes: one more character after every finding, and a moved version."""
    from spicy_docs.extraction.body_text import body_text as real

    def derive(package):
        derived = real(package)
        assert derived.pages is not None
        pages = (*derived.pages[:-1], derived.pages[-1] + f"\n{marker}")
        return replace(derived, text="\n".join(pages), pages=pages)

    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.body_text", derive)
    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.BODY_TEXT_DERIVATION_VERSION", f"read-{marker}")


def _spans(rows: list[dict]) -> list[tuple]:
    """A finding without its text digest: the same span under two digests repeats here."""
    return sorted(tuple(sorted((k, repr(v)) for k, v in row.items() if k not in ("text_sha256", "body_version_id")))
                  for row in rows)


def _forget_source_digest(paths: tuple[Path, ...]) -> None:
    """Leave each output's checkpoints as a build before ``source_sha256`` wrote them."""
    for path in paths:
        table = pq.read_table(path)
        records = [{k: v for k, v in record.items() if k != "source_sha256"}
                   for record in read_checkpoints(path, CHECKPOINT_NAMESPACE)]
        pq.write_table(table.replace_schema_metadata(checkpoint_metadata(path, CHECKPOINT_NAMESPACE, records)), path)


def test_same_bytes_read_differently_keeps_one_copy_in_every_digest_table(tmp_path, monkeypatch):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    actions, citations = _rows(paths[2]), _rows(paths[3])
    assert actions and citations, "both digest tables must begin with findings"
    before = {row["text_sha256"] for row in actions + citations}
    _publish_as_prior(paths)

    _read_differently(monkeypatch)
    acquirer.asked.clear()
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    assert acquirer.asked == [CRPT_ID], "the moved derivation must re-read the print"
    current = _rows(paths[0])[0]["text_sha256"]
    assert current not in before, "the new reading must have a new digest for this to test anything"
    assert {row["text_sha256"] for row in _rows(paths[2]) + _rows(paths[3])} == {current}
    assert _spans(_rows(paths[2])) == _spans(actions)
    assert _spans(_rows(paths[3])) == _spans(citations)


def test_budget_volume_read_differently_keeps_one_copy(tmp_path, monkeypatch):
    listing = _listing(BUDGET_ID, "Mid-Session Review")
    acquirer = _Acquirer({BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)})
    paths = _build(tmp_path, _Reader({"BUDGET": [listing]}), acquirer)
    citations = _rows(paths[3])
    assert citations
    _publish_as_prior(paths)

    _read_differently(monkeypatch)
    paths = _build(tmp_path, _Reader({"BUDGET": [listing]}), acquirer)
    assert {row["text_sha256"] for row in _rows(paths[3])} == {_rows(paths[1])[0]["text_sha256"]}
    assert _spans(_rows(paths[3])) == _spans(citations)


def test_changed_bytes_keep_both_readings_even_under_an_equal_stamp(tmp_path, monkeypatch):
    """A recorded digest decides. An equal ``lastModified`` cannot hide a real revision."""
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    citations = _rows(paths[3])
    _publish_as_prior(paths)

    revised = [[*REPORT_PAGES[0], "The Committee ordered H.R. 1093 reported on April 4, 2024."], *REPORT_PAGES[1:]]
    acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=revised)
    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.CITATION_RULE_SET_VERSION", "corrected")
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    current = _rows(paths[0])[0]["text_sha256"]
    rows = _rows(paths[3])
    assert {row["text_sha256"] for row in rows} == {citations[0]["text_sha256"], current}
    assert [row for row in rows if row["text_sha256"] != current] == citations


def test_a_moved_stamp_over_the_same_bytes_is_not_a_revision(tmp_path, monkeypatch):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    citations = _rows(paths[3])
    _publish_as_prior(paths)

    modified = "2026-09-19T12:00:00Z"
    acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=REPORT_PAGES, last_modified=modified)
    _read_differently(monkeypatch)
    paths = _build(tmp_path, _Reader({"CRPT": [dict(listing, lastModified=modified)]}), acquirer)
    assert {row["text_sha256"] for row in _rows(paths[3])} == {_rows(paths[0])[0]["text_sha256"]}
    assert _spans(_rows(paths[3])) == _spans(citations)


def test_a_checkpoint_without_a_digest_keeps_ambiguous_readings(tmp_path, monkeypatch):
    """A legacy timestamp proves no byte identity, so earlier readings stay."""
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    citations = _rows(paths[3])
    _forget_source_digest(paths)
    _publish_as_prior(paths)

    _read_differently(monkeypatch, "a")
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    current = _rows(paths[0])[0]["text_sha256"]
    assert {row["text_sha256"] for row in _rows(paths[3])} == {citations[0]["text_sha256"], current}
    assert [row for row in _rows(paths[3]) if row["text_sha256"] != current] == citations
    _forget_source_digest(paths)
    _publish_as_prior(paths)

    modified = "2026-09-19T12:00:00Z"
    acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=REPORT_PAGES, last_modified=modified)
    _read_differently(monkeypatch, "b")
    paths = _build(tmp_path, _Reader({"CRPT": [dict(listing, lastModified=modified)]}), acquirer)
    assert len({row["text_sha256"] for row in _rows(paths[3])}) == 3


def test_legacy_checkpoint_same_stamp_changed_pdf_keeps_prior_findings(tmp_path, monkeypatch):
    """Different valid PDF bytes cannot lose history behind an equal timestamp."""
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    first = _package(CRPT_ID, pages=REPORT_PAGES)
    acquirer = _Acquirer({CRPT_ID: first})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    citations, actions = _rows(paths[3]), _rows(paths[2])
    assert citations and actions
    _forget_source_digest(paths)
    _publish_as_prior(paths)

    revised = [[*REPORT_PAGES[0], "The Committee ordered H.R. 1093 reported on April 4, 2024."], *REPORT_PAGES[1:]]
    second = _package(CRPT_ID, pages=revised)
    assert second.summary.last_modified == first.summary.last_modified
    assert second.body_capture.sha256 != first.body_capture.sha256
    acquirer.packages[CRPT_ID] = second
    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.CITATION_RULE_SET_VERSION", "corrected")
    events = []
    journal = SimpleNamespace(credential="", event=lambda name, **fields: events.append((name, fields)),
                              transport=lambda **kwargs: None)
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer, evidence=journal)
    current = _rows(paths[0])[0]["text_sha256"]
    assert current not in {row["text_sha256"] for row in citations}
    assert [row for row in _rows(paths[3]) if row["text_sha256"] != current] == citations
    assert [row for row in _rows(paths[2]) if row["text_sha256"] != current] == actions
    unresolved = [fields for name, fields in events if name == "print-reading-source-unresolved"]
    assert unresolved == [{"package_id": CRPT_ID, "source_sha256": second.body_capture.sha256,
                           "retained_text_sha256": [citations[0]["text_sha256"]],
                           "text_sha256": current, "reason": "missing_checkpoint_source_sha256"}]
    assert not [name for name, fields in events if name == "print-reading-replaced"]


def test_an_earlier_revision_survives_a_later_reading_of_the_current_bytes(tmp_path, monkeypatch):
    """Only the reading the checkpoint names goes: revision one stays when revision two is read again."""
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    first = _rows(paths[3])
    _publish_as_prior(paths)

    modified = "2026-09-19T12:00:00Z"
    second_pages = [["The Committee ordered H.R. 1093 reported on April 4, 2024."]]
    acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=second_pages, last_modified=modified)
    paths = _build(tmp_path, _Reader({"CRPT": [dict(listing, lastModified=modified)]}), acquirer)
    second = [row for row in _rows(paths[3]) if row not in first]
    assert second, "the revision must add findings"
    _publish_as_prior(paths)

    _read_differently(monkeypatch)
    paths = _build(tmp_path, _Reader({"CRPT": [dict(listing, lastModified=modified)]}), acquirer)
    current = _rows(paths[0])[0]["text_sha256"]
    rows = _rows(paths[3])
    assert {row["text_sha256"] for row in rows} == {first[0]["text_sha256"], current}
    assert [row for row in rows if row["text_sha256"] != current] == first
    assert _spans([row for row in rows if row["text_sha256"] == current]) == _spans(second)


def test_a_failed_read_replaces_no_reading(tmp_path, monkeypatch):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    citations = _rows(paths[3])
    _publish_as_prior(paths)

    _read_differently(monkeypatch)
    acquirer.refuse[CRPT_ID] = PagedJsonSourceError("refused")
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    assert _rows(paths[3]) == citations


def test_a_replaced_reading_is_journaled(tmp_path, monkeypatch):
    class _Journal:
        credential = ""

        def __init__(self):
            self.events = []

        def event(self, name, **fields):
            self.events.append((name, fields))

        def transport(self, **_kwargs):
            return None

    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    package = _package(CRPT_ID, pages=REPORT_PAGES)
    acquirer = _Acquirer({CRPT_ID: package})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    earlier = _rows(paths[0])[0]["text_sha256"]
    _publish_as_prior(paths)

    _read_differently(monkeypatch)
    journal = _Journal()
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer, evidence=journal)
    replaced = [fields for name, fields in journal.events if name == "print-reading-replaced"]
    assert replaced == [{"package_id": CRPT_ID, "source_sha256": package.body_capture.sha256,
                         "replaced_text_sha256": [earlier], "text_sha256": _rows(paths[0])[0]["text_sha256"]}]


@pytest.mark.parametrize(
    "version_name", ["CITATION_RULE_SET_VERSION", "PRINT_ACTION_RULE_SET_VERSION", "COVERED_CONGRESS_RULE_VERSION"]
)
def test_rule_correction_revisits_held_print_outside_discovery(tmp_path, monkeypatch, version_name):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    _publish_as_prior(paths)
    acquirer.asked.clear()
    monkeypatch.setattr(
        f"spicy_regs.transforms.build_print_citations.{version_name}", "corrected-rule-version"
    )
    if version_name == "PRINT_ACTION_RULE_SET_VERSION":
        monkeypatch.setattr("spicy_regs.transforms.build_print_citations.find_bill_actions",
                            lambda *_args, **_kwargs: SimpleNamespace(findings=[]))
    else:
        monkeypatch.setattr("spicy_regs.transforms.build_print_citations.find_citations",
                            lambda *_args, **_kwargs: ())

    paths = _build(tmp_path, _Reader({}), acquirer)
    assert acquirer.asked == [CRPT_ID], "a corrected rule must reach held prints outside the discovery window"
    assert _rows(paths[2]) == [], "unchanged source bytes may produce no actions under a corrected rule"
    if version_name == "CITATION_RULE_SET_VERSION":
        assert _rows(paths[3]) == []
    expected = [_rows(path) for path in paths]
    _publish_as_prior(paths)
    acquirer.asked.clear()

    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    assert acquirer.asked == [], "successful empty corrections must have a stable checkpoint"
    assert [_rows(path) for path in paths] == expected


def test_failed_correction_preserves_prior_findings_and_remains_pending(tmp_path, monkeypatch):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    expected = [_rows(path) for path in paths]
    _publish_as_prior(paths)
    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.PRINT_ACTION_RULE_SET_VERSION", "corrected")
    acquirer.refuse[CRPT_ID] = ConnectionError("temporary failure")

    for _attempt in range(2):
        acquirer.asked.clear()
        paths = _build(tmp_path, _Reader({}), acquirer)
        assert acquirer.asked == [CRPT_ID]
        assert [_rows(path) for path in paths] == expected
        _publish_as_prior(paths)


@pytest.mark.parametrize("missing_checkpoint", [0, 2, 3])
def test_every_affected_output_must_confirm_the_completed_read(tmp_path, missing_checkpoint):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    stale = paths[missing_checkpoint]
    pq.write_table(pq.read_table(stale).replace_schema_metadata(None), stale)
    _publish_as_prior(paths)
    acquirer.asked.clear()

    _build(tmp_path, _Reader({}), acquirer)
    assert acquirer.asked == [CRPT_ID], "a partial publication must be repaired even outside discovery"


def test_budget_correction_replaces_only_its_own_citations(tmp_path, monkeypatch):
    from spicy_docs.interpretation.citations import find_citations

    listings = {"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")],
                "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")]}
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES),
                         BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)})
    paths = _build(tmp_path, _Reader(listings), acquirer)
    actions = _rows(paths[2])
    activity_cites = [row for row in _rows(paths[3]) if row["document_key"] == CRPT_ID]
    assert len(_rows(paths[3])) > len(activity_cites), "both families must begin with citations"
    _publish_as_prior(paths)
    monkeypatch.setattr("spicy_regs.transforms.build_print_citations.CITATION_RULE_SET_VERSION", "corrected")
    monkeypatch.setattr(
        "spicy_regs.transforms.build_print_citations.find_citations",
        lambda *args, **kwargs: () if kwargs.get("congress") is None else find_citations(*args, **kwargs),
    )
    acquirer.asked.clear()

    paths = _build(tmp_path, _Reader(listings), acquirer)
    assert acquirer.asked == [CRPT_ID, BUDGET_ID]
    assert _rows(paths[2]) == actions
    assert _rows(paths[3]) == activity_cites


@pytest.mark.parametrize("version_name", ["PRINT_ACTION_RULE_SET_VERSION", "COVERED_CONGRESS_RULE_VERSION"])
def test_a_report_only_rule_change_does_not_reprocess_budget_volumes(tmp_path, monkeypatch, version_name):
    acquirer = _Acquirer({BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)})
    paths = _build(tmp_path, _Reader({"BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")]}), acquirer)
    _publish_as_prior(paths)
    acquirer.asked.clear()
    monkeypatch.setattr(f"spicy_regs.transforms.build_print_citations.{version_name}", "corrected")

    _build(tmp_path, _Reader({}), acquirer)
    assert acquirer.asked == [], "budget volumes use neither the action nor the covered-Congress reading"


def test_unknown_source_timestamp_cannot_establish_an_unchanged_print(tmp_path):
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE", last_modified=None)
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES, last_modified=None)})
    paths = _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    _publish_as_prior(paths)
    acquirer.asked.clear()

    _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    assert acquirer.asked == [CRPT_ID]


# --------------------------------------------------------------------------
# The title rule, which is the one selection rule copied rather than imported.
# --------------------------------------------------------------------------

#: Titles the live ``published`` CRPT window 2025-01-01..2025-03-31 served on
#: 2026-09-20, abbreviated. All five are activity reports.
REAL_ACTIVITY_TITLES = (
    "LEGISLATIVE REVIEW AND OVERSIGHT ACTIVITIES of the COMMITTEE ON FOREIGN AFFAIRS",
    "Summary of the Activities of the Committee on Transportation and Infrastructure",
    "ACTIVITIES OF THE COMMITTEE ON OVERSIGHT AND ACCOUNTABILITY ONE HUNDRED EIGHTEENTH CONGRESS",
    "REPORT ON THE ACTIVITIES OF THE COMMITTEE ON EDUCATION AND THE WORKFORCE",
    "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE of the HOUSE OF REPRESENTATIVES",
)

#: The class a bare ``activit`` match takes in, which is why the rule matches a
#: phrase. spicy-docs measured two of these among its first eight matches.
MEASURED_FALSE_POSITIVES = (
    "DIRECTING THE SECRETARY OF STATE TO TRANSMIT DOCUMENTS RELATING TO ACTIVITIES IN A FOREIGN STATE",
    "PROVIDING FOR CONSIDERATION OF A BILL RELATING TO CERTAIN ACTIVITIES",
)


@pytest.mark.parametrize("title", REAL_ACTIVITY_TITLES)
def test_the_title_rule_matches_every_real_activity_report(title):
    assert is_activity_report(CRPT_ID, title)


@pytest.mark.parametrize("title", MEASURED_FALSE_POSITIVES)
def test_the_title_rule_refuses_the_measured_false_positive_class(title):
    """A bare ``activit`` matches these; the phrase rule must not.

    This is the half that makes the rule worth copying rather than widening:
    without it nothing here would notice the rule being loosened back to a
    word match.
    """
    assert "activit" in title.lower(), "the false positive must contain the word, or this proves nothing"
    assert not is_activity_report(CRPT_ID, title)


# --------------------------------------------------------------------------
# The Congress a report's bills belong to.
# --------------------------------------------------------------------------

#: CRPT-118srpt99, a report on the 117th filed in the 118th: its published
#: title, its cover's lines, and the sentence the 2026-09-26 drift audit found
#: published against 118-hr-5376 (``drift-qualification-2026-09-26/
#: bills-citations/``). Its MODS is the publisher's, reduced
#: (``fixtures/govinfo_bodies/README.md``), and stamps H.R. 5376 ``118``.
SENATE_ID = "CRPT-118srpt99"
SENATE_TITLE = (
    "REPORT ON THE ACTIVITIES OF THE COMMITTEE ON THE BUDGET UNITED STATES SENATE DURING THE ONE HUNDRED "
    "SEVENTEENTH CONGRESS PURSUANT TO Paragraph 8(b) of Rule XXVI of the Standing Rules of the United States Senate"
)
SENATE_PAGES = [
    ["118TH CONGRESS", "1st Session", "REPORT", "118-99", "REPORT ON THE ACTIVITIES", "OF THE COMMITTEE ON THE BUDGET",
     "DURING THE", "ONE HUNDRED SEVENTEENTH CONGRESS", "SEPTEMBER 20, 2023. Ordered to be printed"],
    ["This resolution provided the reconciliation directives for measure H.R. 5376,",
     "colloquially referred to as The Inflation Reduction Act, signed", "into law on August 16, 2022."],
]  # fmt: skip


def _bill_citations(path: Path) -> list[tuple]:
    return [
        (row["target_key"], row["target_resolved"], row["stated_by_index"])
        for row in _rows(path)
        if row["cite_kind"] == "bill_number"
    ]


def test_a_senate_reports_bills_resolve_in_the_congress_it_covers_not_the_one_it_was_filed_in(tmp_path):
    """Both Congresses are kept, the report's own statement keys the bills, and the MODS's disagreement is counted."""
    reader = _Reader({"CRPT": [_listing(SENATE_ID, SENATE_TITLE)]})
    acquirer = _Acquirer({SENATE_ID: _package(SENATE_ID, pages=SENATE_PAGES, title=SENATE_TITLE)})
    activity, _, actions, citations = _build(tmp_path, reader, acquirer)

    [report] = _rows(activity)
    assert (report["congress"], report["covered_congress"], report["covered_congress_source"]) == ("118", "117", "title")
    assert report["bills_congress_mismatch"] == "1", "the MODS keys H.R. 5376 under the filing Congress"
    assert _bill_citations(citations) == [("117-hr-5376", "true", "false")]
    action_rows = _rows(actions)
    assert action_rows and {row["bill_id"] for row in action_rows} == {"117-hr-5376"}
    assert "became_public_law" in {row["print_phrasing"] for row in action_rows}


def test_a_report_stating_no_congress_publishes_its_bills_unresolved_and_attaches_no_action(tmp_path):
    """Unresolved rather than guessed: the package id's Congress is never used as a fallback."""
    title = "ACTIVITY REPORT OF THE COMMITTEE ON THE JUDICIARY"
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES, title=title)})
    activity, _, actions, citations = _build(tmp_path, _Reader({"CRPT": [_listing(CRPT_ID, title)]}), acquirer)

    [report] = _rows(activity)
    assert (report["congress"], report["covered_congress"], report["covered_congress_source"]) == ("119", None, None)
    assert (report["distinct_bills_beyond_index"], report["bills_congress_mismatch"]) == (None, None)
    assert _bill_citations(citations) == [("HR1093", "false", None)]
    assert _rows(actions) == []
    # The same print under a title that states its Congress does attach the
    # action, so the empty table above is the refusal and not a phrase missed.
    stated = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    (tmp_path / "stated").mkdir()
    _, _, actions, citations = _build(tmp_path / "stated", _Reader({"CRPT": [_listing(CRPT_ID, title)]}), stated)
    assert _bill_citations(citations) == [("119-hr-1093", "true", "false")]
    assert {row["bill_id"] for row in _rows(actions)} == {"119-hr-1093"}


# --------------------------------------------------------------------------
# Whose committee wrote the report.
# --------------------------------------------------------------------------

ROSTERS = Path(__file__).parent / "fixtures" / "congress_rosters"

#: CRPT-118srpt99's transmittal letter names its own committee as every
#: report does, unqualified -- a name both chambers hold. The hearing sentence
#: is written for this test: srpt99 states no hearing, and a hearing is the
#: phrasing whose code follows the committee that acted.
CHAMBER_PAGES = [
    *SENATE_PAGES,
    ["herewith a report on the activities of the Committee on the Budget",
     "On March 3, 2021, the Committee on the Budget held a hearing on S. 232."],
]  # fmt: skip


class _FixtureRosters:
    """The wheel's House excerpt (its complete ``<committees>`` block) and a Senate excerpt reaching the Budget committee."""

    def acquire_house(self, *, congress, session=None, max_bytes=None):
        from spicy_docs.sources.congress.committee_rosters import parse_house_member_data

        roster = parse_house_member_data((ROSTERS / "memberdata-119-excerpt.xml").read_bytes(), congress=119)
        return SimpleNamespace(roster=roster)

    def acquire_senate(self, *, max_bytes=None):
        from spicy_docs.sources.congress.committee_rosters import parse_senate_cvc

        return SimpleNamespace(roster=parse_senate_cvc((ROSTERS / "cvc-member-data-budget-excerpt.xml").read_bytes()))


def _committee_and_hearing(tmp_path: Path, package_id: str) -> tuple[set, set]:
    acquirer = _Acquirer({package_id: _package(package_id, pages=CHAMBER_PAGES, title=SENATE_TITLE)})
    reader = _Reader({"CRPT": [_listing(package_id, SENATE_TITLE)]})
    _, _, actions, citations = build_print_citations(
        tmp_path, reader=reader, acquirer=acquirer, rosters=_FixtureRosters(), download_prior=_no_download
    )
    committees = {
        (row["target_key"], row["target_resolved"]) for row in _rows(citations) if row["cite_kind"] == "committee_name"
    }
    hearings = {
        (row["chamber"], row["billstatus_action_code"])
        for row in _rows(actions)
        if row["print_phrasing"] == "held_hearing"
    }
    return committees, hearings


def test_a_senate_report_names_its_own_chambers_committee_and_its_hearings_carry_senate_codes(tmp_path):
    """CRPT-118srpt99 is a Senate report: ``Committee on the Budget`` is ``ssbu00``, not the House's ``hsbu00``."""
    committees, hearings = _committee_and_hearing(tmp_path, SENATE_ID)
    assert committees == {("ssbu00", "true")}
    assert hearings == {("senate", "13100")}


def test_the_same_print_under_a_house_id_names_the_house_committee(tmp_path):
    """The chamber comes from the id alone: the same words in an ``hrpt`` package are the House's, as before."""
    committees, hearings = _committee_and_hearing(tmp_path, CRPT_ID)
    assert committees == {("hsbu00", "true")}
    assert hearings == {("house", "H21000")}


@pytest.mark.parametrize(
    ("package_id", "named", "expected"),
    [
        (CRPT_ID, "Senate Committee on the Budget", ["hsbu00", "ssbu00"]),
        (SENATE_ID, "House Committee on the Budget", ["ssbu00", "hsbu00"]),
        # Only the House roster holds this exact name; the Senate's committee is
        # the longer "... and Governmental Affairs", which this Senate excerpt
        # does not reach. Senate-named, it must not take the House's hshm00.
        (SENATE_ID, "Senate Committee on Homeland Security", ["ssbu00", "COMMITTEEONHOMELANDSECURITY"]),
    ],
)
def test_a_chamber_the_print_names_before_a_committee_wins_over_the_reports(tmp_path, package_id, named, expected):
    """``the Senate Committee on the Budget`` in a House report is ``ssbu00``; the bare name beside it stays the House's.

    The reverse holds in CRPT-118srpt99: its own ``Committee on the Budget`` is
    ``ssbu00`` and a ``House Committee on the Budget`` is ``hsbu00``. A named
    chamber is read among that chamber's committees only.
    """
    pages = [*SENATE_PAGES, ["herewith a report on the activities of the Committee on the Budget,",
                             f"prepared with the staff of the {named}."]]  # fmt: skip
    acquirer = _Acquirer({package_id: _package(package_id, pages=pages, title=SENATE_TITLE)})
    reader = _Reader({"CRPT": [_listing(package_id, SENATE_TITLE)]})
    _, _, _, citations = build_print_citations(
        tmp_path, reader=reader, acquirer=acquirer, rosters=_FixtureRosters(), download_prior=_no_download
    )
    rows = sorted(
        (int(row["span_start"]), row["target_key"]) for row in _rows(citations) if row["cite_kind"] == "committee_name"
    )
    assert [key for _start, key in rows] == expected


def test_a_change_to_how_the_chamber_is_read_revisits_held_reports(tmp_path, monkeypatch):
    """The chamber map is a processing input: a report read under another map is read again."""
    from spicy_docs.schemas.committee_report_tables import CHAMBER_BY_DOCUMENT_TYPE

    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    _publish_as_prior(_build(tmp_path, _Reader({"CRPT": [listing]}), acquirer))
    acquirer.asked.clear()
    monkeypatch.setattr(
        "spicy_regs.transforms.build_print_citations.CHAMBER_BY_DOCUMENT_TYPE",
        {**CHAMBER_BY_DOCUMENT_TYPE, "erpt": "joint"},
    )
    _build(tmp_path, _Reader({"CRPT": [listing]}), acquirer)
    assert acquirer.asked == [CRPT_ID]


def test_a_report_whose_id_states_no_chamber_is_refused_before_any_request(tmp_path, monkeypatch):
    """No default chamber: an id the chamber map does not place is a counted refusal, never fetched or published."""
    from loguru import logger

    monkeypatch.setattr(
        "spicy_regs.transforms.build_print_citations.CHAMBER_BY_DOCUMENT_TYPE", {"hrpt": "house"}
    )
    acquirer = _Acquirer({SENATE_ID: _package(SENATE_ID, pages=CHAMBER_PAGES, title=SENATE_TITLE)})
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(message.record["message"]))
    try:
        paths = _build(tmp_path, _Reader({"CRPT": [_listing(SENATE_ID, SENATE_TITLE)]}), acquirer)
    finally:
        logger.remove(sink)
    assert acquirer.asked == []
    assert all(not _rows(path) for path in paths)
    assert any("refusals by reason" in message and "NoStatedChamber" in message for message in messages)


# --------------------------------------------------------------------------
# The pass itself.
# --------------------------------------------------------------------------


def test_both_families_publish_under_their_contracts_and_share_one_link_table(tmp_path):
    reader = _Reader(
        {
            "CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE")],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {
            CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES),
            BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES),
        }
    )
    activity, budget, actions, citations = _build(tmp_path, reader, acquirer)

    for path, name in (
        (activity, ACTIVITY_REPORTS),
        (budget, BUDGET_VOLUMES),
        (actions, BILL_ACTIONS),
        (citations, CITATIONS),
    ):
        assert path.name == f"{name}.parquet"
        assert pq.read_table(path).schema.names == list(TABLE_CONTRACTS[name].columns)

    assert [row["package_id"] for row in _rows(activity)] == [CRPT_ID]
    assert [row["package_id"] for row in _rows(budget)] == [BUDGET_ID]

    # One link table, two families, told apart by document_kind — and every
    # kind is one of spicy-docs' own two constants, not a spelling invented here.
    citation_rows = _rows(citations)
    assert citation_rows, "the prints state keys; the link table must carry them"
    by_document = {row["document_key"]: row["document_kind"] for row in citation_rows}
    assert by_document == {CRPT_ID: "govinfo_package", BUDGET_ID: "budget_volume"}


def test_a_budget_volumes_bill_citation_states_no_index_comparison(tmp_path):
    """NULL, never ``false``: a BUDGET package has no Congress to compare on.

    The rule is spicy-docs'; what is checked here is that this transform passes
    ``budget_index_stated_keys`` for a budget volume and ``index_stated_keys``
    for a report, which is the only way the distinction reaches a published row.
    """
    reader = _Reader({"CRPT": [], "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")]})
    acquirer = _Acquirer({BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)})
    _, _, _, citations = _build(tmp_path, reader, acquirer)

    bill_rows = [row for row in _rows(citations) if row["cite_kind"] == "bill_number"]
    # Without this the assertion below is satisfied by an empty list, which is
    # what a check that agrees with itself looks like.
    assert bill_rows, "the fixture's print names a bill; the link table must carry it"
    assert all(row["stated_by_index"] is None for row in bill_rows), (
        "a bill cite of a budget volume must say no comparison was possible"
    )


def test_bill_actions_come_only_from_the_activity_reports(tmp_path):
    """A budget volume is not a committee print and states no committee action."""
    reader = _Reader(
        {
            "CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE")],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES), BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)}
    )
    _, _, actions, _ = _build(tmp_path, reader, acquirer)
    action_rows = _rows(actions)
    # Both prints state an action phrase beside a bill; only the report's may
    # become a row, and an empty table would satisfy the subset assertion
    # without establishing that.
    assert action_rows, "the report states an action beside a bill"
    assert {row["document_key"] for row in action_rows} == {CRPT_ID}


def test_a_neighbouring_collections_id_is_dropped_by_name_and_never_fetched(tmp_path):
    """A CRPT walk serves ``ERP-``/``GPO-`` rows; the grammar refuses them before a request."""
    reader = _Reader(
        {
            "CRPT": [
                _listing("GPO-J6-REPORT", "ACTIVITY REPORT of the SELECT COMMITTEE"),
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
            ],
            "BUDGET": [],
        }
    )
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    _build(tmp_path, reader, acquirer)
    assert acquirer.asked == [CRPT_ID], "an id the grammar refuses must not cost a request"


def test_the_id_grammar_not_the_request_places_each_row_in_its_collection(tmp_path):
    """A BUDGET walk drops a CRPT row, and a BUDGET-shaped id the grammar refuses is named, not skipped quietly."""
    from loguru import logger

    from spicy_regs.transforms.build_print_citations import _collection

    reader = _Reader(
        {
            "CRPT": [],
            "BUDGET": [
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
                _listing("BUDGET-2026-NOTAPART", "Appendix"),
                _listing(BUDGET_ID, "Mid-Session Review"),
            ],
        }
    )
    acquirer = _Acquirer({BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)})
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(message.record["message"]), level="WARNING")
    try:
        _build(tmp_path, reader, acquirer)
    finally:
        logger.remove(sink)
    assert acquirer.asked == [BUDGET_ID]
    assert any("BUDGET-2026-NOTAPART is not a BUDGET package id" in message for message in messages)
    assert (_collection(BUDGET_ID), _collection(CRPT_ID), _collection("GPO-J6-REPORT")) == ("BUDGET", "CRPT", None)


def test_a_title_that_is_not_an_activity_report_is_never_fetched(tmp_path):
    reader = _Reader(
        {
            "CRPT": [
                _listing("CRPT-119hrpt2", "PROVIDING FOR CONSIDERATION OF A BILL RELATING TO CERTAIN ACTIVITIES"),
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
            ],
            "BUDGET": [],
        }
    )
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    _build(tmp_path, reader, acquirer)
    assert acquirer.asked == [CRPT_ID]


def test_an_unchanged_package_is_skipped_and_a_modified_one_is_re_read(tmp_path):
    """The publisher's own ``last_modified`` is the comparison, not the date."""
    listing = _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE", "2026-09-18T12:00:00Z")
    reader = _Reader({"CRPT": [listing], "BUDGET": []})
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    paths = _build(tmp_path, reader, acquirer)
    assert acquirer.asked == [CRPT_ID]

    # The published table becomes the prior for the next run.
    _publish_as_prior(paths)
    acquirer.asked.clear()
    paths = _build(tmp_path, _Reader({"CRPT": [listing], "BUDGET": []}), acquirer)
    assert acquirer.asked == [], "a package published at this last_modified costs nothing"

    moved = dict(listing, lastModified="2026-09-19T12:00:00Z")
    _publish_as_prior(paths)
    acquirer.asked.clear()
    acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=REPORT_PAGES, last_modified="2026-09-19T12:00:00Z")
    _build(tmp_path, _Reader({"CRPT": [moved], "BUDGET": []}), acquirer)
    assert acquirer.asked == [CRPT_ID], "a revised package must be re-read"


def test_a_refused_package_is_counted_and_enumerated_again_next_run(tmp_path):
    """AGENTS.md's resume rule, held by the enumeration rather than by a state table.

    The walk is the whole window every run, so a package that refused is
    offered again — which a last-modified watermark could not do once another
    package published past it.
    """
    rows = {
        "CRPT": [
            _listing("CRPT-119hrpt9", "ACTIVITY REPORT of the COMMITTEE ON RULES"),
            _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
        ],
        "BUDGET": [],
    }
    acquirer = _Acquirer(
        {CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)},
        refuse={"CRPT-119hrpt9": PagedJsonSourceError("the publisher served its error page")},
    )
    paths = _build(tmp_path, _Reader(rows), acquirer)
    assert acquirer.asked == ["CRPT-119hrpt9", CRPT_ID]
    assert [row["package_id"] for row in _rows(paths[0])] == [CRPT_ID], "a refusal publishes no row"

    _publish_as_prior(paths)
    acquirer.asked.clear()
    _build(tmp_path, _Reader(rows), acquirer)
    assert acquirer.asked == ["CRPT-119hrpt9"], "the refusal is retried and the published package is not"


def test_a_credential_refusal_aborts_the_run(tmp_path):
    """A 401/403 is not one bad row: every later package would refuse the same way."""
    acquirer = _Acquirer({}, refuse={CRPT_ID: CredentialRefusedError("the key was refused")})
    reader = _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")], "BUDGET": []})
    with pytest.raises(CredentialRefusedError):
        _build(tmp_path, reader, acquirer)


def test_the_per_run_cap_bounds_the_fetch_and_not_the_walk(tmp_path):
    """Both collections are still enumerated; only the fetching stops."""
    reader = _Reader(
        {
            "CRPT": [
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
                _listing("CRPT-119hrpt9", "ACTIVITY REPORT of the COMMITTEE ON RULES"),
            ],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    _build(tmp_path, reader, acquirer, max_packages=1)
    assert acquirer.asked == [CRPT_ID]
    assert reader.walked == ["CRPT", "BUDGET"], "the window is walked whole so the rest is retried next run"


def test_every_body_is_asked_for_pdf_first(tmp_path):
    """The one rendition this transform names, and the reason it names one.

    Under the acquirer's sealed order HTML comes first, and a measured run
    (2026-09-20, retained as the receipt's first attempt) refused 10 of 41
    activity reports on HTML nesting depth and published no page attribution
    at all on the 31 it read. Four published columns state a page, so the
    order is this transform's to choose and the choice must not drift back.
    """
    reader = _Reader(
        {
            "CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE")],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES), BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)}
    )
    _build(tmp_path, reader, acquirer)
    assert acquirer.preferences == [PRINT_BODY_PREFERENCE, PRINT_BODY_PREFERENCE]
    # Equality with the derivation, not a spot check: a subset assertion would
    # pass on a transcription that had silently dropped `uslm`, and an
    # `[0] == "pdf"` assertion says nothing about the order behind it. This
    # fails the moment the constant stops being "PDF, then the sealed order",
    # including when spicy-docs adds a rendition to BODY_PREFERENCE.
    assert PRINT_BODY_PREFERENCE == ("pdf", *(fmt for fmt in BODY_PREFERENCE if fmt != "pdf"))
    assert set(PRINT_BODY_PREFERENCE) == set(BODY_PREFERENCE), "no rendition is dropped or invented"


def test_a_pdf_body_fills_the_page_columns_an_html_body_cannot(tmp_path):
    """The consequence the preference exists for, asserted on a published row."""
    reader = _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")], "BUDGET": []})
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)})
    activity, _, _, citations = _build(tmp_path, reader, acquirer)

    row = _rows(activity)[0]
    assert row["body_rendition"] == "pdf"
    assert row["pages_read"] == str(len(REPORT_PAGES))
    assert row["pages_capped"] == "false"
    assert all(r["evidence_page"] is not None for r in _rows(citations)), "a paginated body attributes every cite"


def test_a_cold_run_over_both_collections_publishes_rows_in_both(tmp_path):
    """Neither family may be starved by a shared cap, whatever the listing order.

    Walking CRPT to exhaustion first spends the whole cap on it whenever CRPT
    has more outstanding packages than the cap, and publishes an **empty**
    ``budget_volumes`` — which is what the first measured run did (40 activity
    reports, 0 budget volumes, 2026-09-20). A consumer cannot tell an empty
    contract table from a family with nothing in it, so a cold deploy must not
    produce one. The cap here is smaller than the CRPT listing on purpose.
    """
    reader = _Reader(
        {
            "CRPT": [
                _listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE ON ENERGY AND COMMERCE"),
                _listing("CRPT-119hrpt9", "ACTIVITY REPORT of the COMMITTEE ON RULES"),
                _listing("CRPT-119hrpt11", "ACTIVITY REPORT of the COMMITTEE ON THE BUDGET"),
            ],
            "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")],
        }
    )
    acquirer = _Acquirer(
        {
            CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES),
            "CRPT-119hrpt9": _package(CRPT_ID, pages=REPORT_PAGES),
            "CRPT-119hrpt11": _package(CRPT_ID, pages=REPORT_PAGES),
            BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES),
        }
    )
    activity, budget, _, _ = _build(tmp_path, reader, acquirer, max_packages=2)

    assert _rows(activity), "the activity-report table must not be empty"
    assert _rows(budget), "nor may the budget table, which the listing order would starve"
    # The cap is still the cap: two packages, one from each family.
    assert len(acquirer.asked) == 2


@pytest.mark.parametrize("part", ["OBJCLASS", "TAB", "DB", "CLIMATE", "LRB", "CROSSCUT", "DOD"])
def test_new_budget_parts_reach_the_publisher_and_root_answers_are_counted(tmp_path, part):
    from loguru import logger
    from spicy_docs.sources.govinfo.body_acquisition import GovInfoFormatNotOfferedError

    package_id = f"BUDGET-2025-{part}"
    acquirer = _Acquirer({}, {package_id: GovInfoFormatNotOfferedError(package_id, PRINT_BODY_PREFERENCE, ())})
    messages = []
    sink = logger.add(lambda message: messages.append(message.record["message"]))
    try:
        paths = _build(tmp_path, _Reader({"BUDGET": [_listing(package_id, "Volume")]}), acquirer)
    finally:
        logger.remove(sink)
    assert acquirer.asked == [package_id]
    assert all(not _rows(path) for path in paths)
    assert any("1 publisher package-root format answers (not failures)" in message for message in messages)


def test_the_source_digest_moves_with_code_and_data_but_not_prose(tmp_path):
    from spicy_regs.generations import source_digest

    (tmp_path / "pkg" / "__pycache__").mkdir(parents=True)
    (tmp_path / "pkg" / "reader.py").write_text("RULE = 1\n")
    (tmp_path / "pkg" / "vocabulary.json").write_text('{"a": 1}')
    suffixes = (".py", ".json")
    before = source_digest(tmp_path / "pkg", suffixes)
    (tmp_path / "pkg" / "README.md").write_text("prose")
    (tmp_path / "pkg" / "__pycache__" / "reader.py").write_text("stale copy")
    assert source_digest(tmp_path / "pkg", suffixes) == before
    (tmp_path / "pkg" / "vocabulary.json").write_text('{"a": 2}')
    assert source_digest(tmp_path / "pkg", suffixes) != before


# --------------------------------------------------------------------------
# The re-read queue: which outstanding packages a capped run reads first.
# --------------------------------------------------------------------------

QUEUE_PAGES = [["ACTIVITY REPORT", "The Committee ordered H.R. 1093 reported on March 4, 2024."]]
MODULE = "spicy_regs.transforms.build_print_citations"


def _queue_reports(count: int) -> tuple[list[str], dict[str, list[dict]], _Acquirer]:
    """``count`` distinct House activity reports, listed in id order, and an acquirer holding each."""
    ids = [f"CRPT-119hrpt{number}" for number in range(100, 100 + count)]
    acquirer = _Acquirer({package_id: _package(package_id, pages=QUEUE_PAGES) for package_id in ids})
    return ids, {"CRPT": [_listing(package_id, "ACTIVITY REPORT of the COMMITTEE") for package_id in ids]}, acquirer


def test_a_capped_run_reads_reports_held_under_another_rule_set_first(tmp_path, monkeypatch):
    """Reports whose rows an older citation rule set wrote go ahead of reports already at the installed one.

    Round 5, W5-2: the merge writes a run's own rows first, so once every report
    was outstanding again the held order put the 20 just read at the head, and a
    cap of 20 re-read those same 20 every day while 21 stayed behind -- among
    them CRPT-118hrpt964, whose "H.R. 8528 (117th Congress)" rule 004 keys to
    the 118th. Here 41 reports are outstanding, the 20 at the installed rule
    set are held first, and the cap is 21: the 21 behind them must be the ones read.
    """
    ids, listing, acquirer = _queue_reports(41)
    monkeypatch.setattr(f"{MODULE}.CITATION_RULE_SET_VERSION", "rule-004")
    _publish_as_prior(_build(tmp_path, _Reader(listing), acquirer, max_packages=41))
    monkeypatch.setattr(f"{MODULE}.CITATION_RULE_SET_VERSION", "rule-006")
    _publish_as_prior(_build(tmp_path, _Reader(listing), acquirer, max_packages=20))
    current = set(acquirer.asked[41:])
    stale = set(ids) - current
    assert (len(current), len(stale)) == (20, 21)
    held = pq.read_table(tmp_path / f"_{ACTIVITY_REPORTS}_prior.parquet").to_pylist()
    assert {row["package_id"] for row in held[:20]} == current, "the run's own rows lead the held table"

    # An action-rule release leaves every report outstanding again.
    monkeypatch.setattr(f"{MODULE}.PRINT_ACTION_RULE_SET_VERSION", "another-action-rule")
    acquirer.asked.clear()
    activity = _build(tmp_path, _Reader(listing), acquirer, max_packages=21)[0]
    assert set(acquirer.asked) == stale
    assert {row["rule_set_version"] for row in _rows(activity)} == {"rule-006"}


def test_a_package_that_refuses_moves_behind_the_others_and_is_still_retried(tmp_path):
    """A print the publisher keeps refusing never holds the head of a capped queue, and never leaves it."""
    ids, listing, acquirer = _queue_reports(3)
    first, second, third = ids
    acquirer.refuse[first] = PagedJsonSourceError("the publisher served its error page")
    asked: list[str] = []
    for _run in range(4):
        acquirer.asked.clear()
        _publish_as_prior(_build(tmp_path, _Reader(listing), acquirer, max_packages=1))
        asked += acquirer.asked
    assert asked == [first, second, third, first]


def test_a_spicy_docs_build_that_moves_no_reading_input_rereads_nothing(tmp_path, monkeypatch):
    """Adopting another SpicyDocs build or release with the same rules re-reads no held print.

    Every vendored build moved the installed package's whole-code digest (the
    FEC, laws and printing builds each did, 2026-09-30..10-02), and each move
    made every held print outstanding; a version-only release (0.39.2 against
    0.39.1) must not either.
    """
    from spicy_regs.transforms import build_print_citations as module

    listings = {"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")],
                "BUDGET": [_listing(BUDGET_ID, "Mid-Session Review")]}
    acquirer = _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES),
                          BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)})
    _publish_as_prior(_build(tmp_path, _Reader(listings), acquirer))
    acquirer.asked.clear()

    def another_build() -> str:
        return "digest-of-another-spicy-docs-build"

    monkeypatch.setattr("spicy_regs.generations.spicy_docs_code", another_build)
    monkeypatch.setattr(module, "spicy_docs_code", another_build, raising=False)
    real = module.version
    monkeypatch.setattr(module, "version", lambda name: "0.53.0+votes.0" if name == "spicy-docs" else real(name))
    _build(tmp_path, _Reader(listings), acquirer)
    assert acquirer.asked == []


def test_a_budget_volume_whose_root_offers_no_body_moves_behind_the_others(tmp_path):
    """A root answer reads nothing, so like a refusal it waits behind every volume that has not just answered."""
    from spicy_docs.sources.govinfo.body_acquisition import GovInfoFormatNotOfferedError

    unoffered = "BUDGET-2025-OBJCLASS"
    acquirer = _Acquirer(
        {BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES)},
        refuse={unoffered: GovInfoFormatNotOfferedError(unoffered, PRINT_BODY_PREFERENCE, ())},
    )
    listing = {"BUDGET": [_listing(unoffered, "Object Class Analysis"), _listing(BUDGET_ID, "Mid-Session Review")]}
    asked: list[str] = []
    for _run in range(3):
        acquirer.asked.clear()
        _publish_as_prior(_build(tmp_path, _Reader(listing), acquirer, max_packages=1))
        asked += acquirer.asked
    assert asked == [unoffered, BUDGET_ID, unoffered]


@pytest.mark.parametrize("moved", ["derivation version", "a rule's target table", "an output contract"])
def test_each_named_reading_input_moves_the_processing_version(monkeypatch, moved):
    """What the whole-code digest stood for still re-reads a held print, one named input at a time."""
    from dataclasses import replace

    from spicy_regs.transforms import build_print_citations as module

    baseline = module._processing_versions({})
    if moved == "derivation version":
        monkeypatch.setattr(module, "BODY_TEXT_DERIVATION_VERSION", "another")
    elif moved == "a rule's target table":
        first, *rest = module.CITATION_RULES
        monkeypatch.setattr(module, "CITATION_RULES", (replace(first, target_table="relabelled"), *rest))
    else:
        contract = TABLE_CONTRACTS[CITATIONS]
        widened = SimpleNamespace(columns=(*contract.columns, "added_column"), types=contract.types)
        monkeypatch.setattr(module, "TABLE_CONTRACTS", {**TABLE_CONTRACTS, CITATIONS: widened})
    after = module._processing_versions({})
    assert {collection: after[collection] != baseline[collection] for collection in baseline} == {
        "CRPT": True, "BUDGET": True}
