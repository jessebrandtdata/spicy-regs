"""Bill-family retry contracts for build_bill_family and its rollup.

A capped or failed body stays pending by its own printing's state, so the next
run reads it without reading its BILLSTATUS again; a status folder is complete
once its bills are shaped, and skips only while its published evidence
verifies; an invalid budget is refused before any acquisition or output.
"""

import importlib
import json
import shutil
from hashlib import sha256
from importlib.metadata import version as package_version
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pyarrow as pa
import pytest
import yaml

from spicy_docs.sources.congress.bill_status import BillSourceError, parse_bill_status
from spicy_docs.sources.govinfo.body_acquisition import GovInfoPackageUnavailableError
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError
from tests.test_bill_family import (
    FIXTURES,
    IDENTITY,
    _Archive,
    _Member,
    _Package,
    StubBodyAcquirer,
    StubBulkAcquirer,
    StubChangedBodyAcquirer,
    StubPdfBodyAcquirer,
    _no_prior,
    _pdf_only_status,
    _priors,
    read_output,
    scoped as fixture_scope,
    write_output,
    zip_entry,
)

scoped = fixture_scope

build = importlib.import_module("spicy_regs.transforms.build_bill_family")
bodies = importlib.import_module("spicy_regs.transforms.bill_family_bodies")
rollup = importlib.import_module("spicy_regs.pipelines.rollups.bill_family")


def run(directory, *, prior=None, budget=600, bulk=None, body=None):
    directory.mkdir()
    bulk = bulk or StubBulkAcquirer()
    body = body or StubBodyAcquirer()
    paths = build.build_bill_family(
        directory,
        bulk_acquirer=bulk,
        body_acquirer=body,
        max_version_fetches=budget,
        **_priors(prior),
    )
    return {path.stem: path for path in paths}, bulk, body


def acquired(paths):
    return {
        row["version_code"]: row
        for row in pq.read_table(paths["bill_versions"]).to_pylist()
        if row["source"] == "govinfo"
    }


def completed_scopes(paths):
    metadata = pq.read_schema(paths[build.ARCHIVES_TABLE]).metadata or {}
    return json.loads(metadata[build.ARCHIVE_COMPLETION_KEY.encode()])


class SenateToo(StubBulkAcquirer):
    """Also serves the fixture bill as 119 S 6028, so a second folder has body work to leave unfinished."""

    def acquire(self, congress, bill_type, **kwargs):
        result = super().acquire(congress, bill_type, **kwargs)
        if (congress, bill_type) == (119, "s") and result.archive is not None:
            body = (FIXTURES / "status-119hr6028.xml").read_bytes()
            body = body.replace(b"<type>HR</type>", b"<type>S</type>").replace(b"119hr6028", b"119s6028")
            result.archive = _Archive([_Member(parse_bill_status(body, identity=replace(IDENTITY, bill_type="s")))])
        return result


def archive_scopes(paths):
    return {
        (row["congress"], row["bill_type"], row["name"])
        for row in pq.read_table(paths[build.ARCHIVES_TABLE]).to_pylist()
    }


def test_a_capped_body_pass_leaves_status_complete_and_resumes_by_printing(tmp_path, scoped, monkeypatch):
    """Run 35946820267 left every folder unfinished over the cap; a body is no longer a folder's to finish."""
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr,s")
    first, _, body = run(tmp_path / "first", budget=1, bulk=SenateToo())
    assert body.requested == ["BILLS-119hr6028ih"], "the cap is hit on the first folder's first printing"
    assert archive_scopes(first) == {("119", "hr", "BILLSTATUS-119-hr.zip"), ("119", "s", "BILLSTATUS-119-s.zip")}
    assert completed_scopes(first) == [["119", "hr"], ["119", "s"]], "both folders' status is shaped"
    _, bulk, body = run(tmp_path / "second", prior=tmp_path / "first", budget=1, bulk=SenateToo())
    assert bulk.zip_downloads == [], "no BILLSTATUS zip is read again to reach a body"
    assert body.requested == ["BILLS-119hr6028eh"], "the pending printing, by its own state"


def test_a_folder_is_complete_whatever_its_bodies(tmp_path, scoped, monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr,s")
    first, _, body = run(tmp_path / "first", budget=2, bulk=SenateToo())
    assert body.requested == ["BILLS-119hr6028ih", "BILLS-119hr6028eh"]
    assert completed_scopes(first) == [["119", "hr"], ["119", "s"]], "119 S 6028's refused bodies do not reopen it"
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", budget=0, bulk=SenateToo())
    assert bulk.zip_downloads == []


def test_one_body_caps_resume_then_retry_pending_pair_without_metadata_change(tmp_path, scoped):
    first, _, first_body = run(tmp_path / "first", budget=1)
    assert first_body.requested == ["BILLS-119hr6028ih"]
    assert len(acquired(first)) == 1 and completed_scopes(first) == [["119", "hr"]]
    held = acquired(first)["introduced-in-house"]

    second, second_bulk, second_body = run(tmp_path / "second", prior=tmp_path / "first", budget=1)
    assert second_bulk.zip_downloads == []
    assert second_body.requested == ["BILLS-119hr6028eh"], "missing body must precede its held neighbour"
    assert len(acquired(second)) == 2
    assert acquired(second)["introduced-in-house"] == held
    assert pq.read_table(second["section_diffs"]).num_rows == 0, "the comparison waits for both documents"

    third, _, third_body = run(tmp_path / "third", prior=tmp_path / "second", budget=2)
    assert set(third_body.requested) == {"BILLS-119hr6028ih", "BILLS-119hr6028eh"}
    assert pq.read_table(third["section_diffs"]).num_rows == 1
    assert acquired(third) == acquired(second), "both sides were read only to be compared"

    fourth, fourth_bulk, fourth_body = run(tmp_path / "fourth", prior=tmp_path / "third", budget=1)
    assert fourth_bulk.zip_downloads == [] and fourth_body.requested == []
    assert acquired(fourth) == acquired(third)


def package_refusals(paths):
    metadata = pq.read_schema(paths[build.ARCHIVES_TABLE]).metadata or {}
    return json.loads(metadata[bodies.PACKAGE_REFUSALS_KEY.encode()])


def _moved_listing() -> bytes:
    """The fixture bill restated: a new text stamp, and each printing listed under another date."""
    body = (FIXTURES / "status-119hr6028.xml").read_bytes()
    for old, new in (
        (b"2026-09-09T17:31:22Z", b"2026-09-20T00:00:00Z"),
        (b"2025-11-12T05:00:00Z", b"2025-11-13T05:00:00Z"),
        (b"2026-06-08T04:00:00Z", b"2026-06-09T04:00:00Z"),
    ):
        assert old in body
        body = body.replace(old, new)
    return body


def _no_tree(monkeypatch, error: Exception | None = None):
    """The reader refuses every tree (the ``BillSourceError`` a document it cannot flatten raises), or fails ``error``."""
    error = error or BillSourceError("bill XML cannot be flattened: no legis-body")
    monkeypatch.setattr(bodies, "parse_bill_tree", lambda *_args, **_kwargs: (_ for _ in ()).throw(error))


def reader(stamp) -> str:
    """The whole reader a tree refusal names, spelled out rather than through ``bodies.reader_id``."""
    return f"{stamp.name} {stamp.version} {stamp.revision} spicy-docs {package_version('spicy-docs')}"


def test_a_refused_tree_is_not_a_read_and_is_not_fetched_again_until_its_listing_moves(tmp_path, scoped, monkeypatch):
    """Bulk remembers a refusal per zip; the per-package route per package, with the listing, bytes and engine."""
    with monkeypatch.context() as failed:
        _no_tree(failed)
        first, _, body = run(tmp_path / "first")
    assert len(body.requested) == 2
    assert acquired(first) == {}, "no body row: the listing stands alone"
    refused = package_refusals(first)
    assert sorted(refused) == ["BILLS-119hr6028eh", "BILLS-119hr6028ih"]
    stamp = build.engine_stamp()
    for package, entry in refused.items():
        body_bytes = (FIXTURES / f"text-{package.removeprefix('BILLS-')}.xml").read_bytes()
        assert entry == {
            "listed": entry["listed"],
            "refusal": "tree",
            "sha256": "sha256:" + sha256(body_bytes).hexdigest(),
            "engine": reader(stamp),
            "status": None,
        }

    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert (bulk.zip_downloads, body.requested) == ([], []), "an unchanged listing is not fetched again"
    assert package_refusals(second) == refused

    moved = StubBulkAcquirer(entry=lambda c, t: zip_entry(c, t, size=31_658_670), status=_moved_listing())
    third, _, body = run(tmp_path / "third", prior=tmp_path / "second", bulk=moved)
    assert sorted(body.requested) == sorted(refused), "a restated listing is fetched once more"
    assert all(row["section_count"] for row in acquired(third).values()) and len(acquired(third)) == 2
    assert package_refusals(third) == {}


@pytest.mark.parametrize("error", [OSError("No space left on device"), MemoryError()], ids=["temp-file", "memory"])
def test_a_tree_this_run_could_not_read_is_neither_a_read_nor_remembered(tmp_path, scoped, monkeypatch, error):
    """Only the reader's own refusal says something about the document; a 108th printing's listing never moves."""
    with monkeypatch.context() as failed:
        _no_tree(failed, error)
        first, _, body = run(tmp_path / "first")
    assert len(body.requested) == 2
    assert acquired(first) == {} and package_refusals(first) == {}
    second, _, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert len(body.requested) == 2, "read again next run, as after a transport failure"
    assert len(acquired(second)) == 2 and all(row["section_count"] for row in acquired(second).values())


def _unavailable(status: int) -> GovInfoPackageUnavailableError:
    url = "https://api.govinfo.gov/packages/BILLS-119hr6028eh/summary"
    return GovInfoPackageUnavailableError(
        CapturedBodyResponse(url, url, status, "application/json", "2026-09-19T00:00:00Z", b"{}"),
        label="package summary",
    )


@pytest.mark.parametrize("absent", [404, 410])
@pytest.mark.parametrize(
    "other",
    [_unavailable(302), _unavailable(400), _unavailable(451), ValueError("HTTP 502 from the transport")],
    ids=["redirect", "400", "451", "502"],
)
def test_only_a_404_or_410_is_remembered_as_absent(tmp_path, scoped, absent, other):
    """``BILLS-116hr7440cph``'s summary answers 404 on every run; a redirect, another 4xx or a 502 establishes nothing."""

    class Absent(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            self.requested.append(package_id)
            raise _unavailable(absent) if package_id.endswith("eh") else other

    first, _, body = run(tmp_path / "first", body=Absent())
    assert sorted(body.requested) == ["BILLS-119hr6028eh", "BILLS-119hr6028ih"]
    [(package, entry)] = package_refusals(first).items()
    assert (package, entry["refusal"], entry["status"], entry["sha256"], entry["engine"]) == (
        "BILLS-119hr6028eh",
        "unavailable",
        absent,
        None,
        None,
    )
    _, _, body = run(tmp_path / "second", prior=tmp_path / "first", body=Absent())
    assert body.requested == ["BILLS-119hr6028ih"], "only what the publisher did not answer absent is asked again"


@pytest.mark.parametrize("same_bytes", [True, False], ids=["same-bytes", "other-bytes"])
def test_a_refused_read_withdraws_only_the_tree_less_row_its_own_bytes_published(
    tmp_path, scoped, monkeypatch, same_bytes
):
    """113-hr-1067's public law kept a listing and a body row without a tree; a refused re-read of the same bytes
    leaves the listing alone, in ``bill_versions`` and ``bill_sections``; a body row of other bytes is not its to go."""
    with monkeypatch.context() as failed:
        _no_tree(failed)
        first, _, _ = run(tmp_path / "first")
    versions = pq.read_table(first["bill_versions"])
    listed = [row for row in versions.to_pylist() if row["version_code"] == "introduced-in-house"]
    body_bytes = (FIXTURES / "text-119hr6028ih.xml").read_bytes()
    digest = "sha256:" + (sha256(body_bytes).hexdigest() if same_bytes else "0" * 64)
    tree_less = {
        **listed[0],
        "source": "govinfo",
        "sha256": digest,
        "byte_size": str(len(body_bytes)),
        "content_type": "application/xml",
        "format_name": "xml",
        "section_count": None,
    }
    pq.write_table(
        pa.Table.from_pylist([*versions.to_pylist(), tree_less], schema=versions.schema), first["bill_versions"]
    )
    columns = build.TABLE_CONTRACTS["bill_sections"].columns
    stale = {c: None for c in columns} | {
        "bill_id": "119-hr-6028",
        "version_code": "introduced-in-house",
        "source": "govinfo",
        "seq": "0",
        "congress": "119",
    }
    write_output(
        first["bill_sections"], pa.Table.from_pylist([stale], schema=pa.schema([(c, pa.string()) for c in columns]))
    )
    pq.write_table(
        pq.read_table(first[build.ARCHIVES_TABLE]).replace_schema_metadata(
            {
                **(pq.read_schema(first[build.ARCHIVES_TABLE]).metadata or {}),
                bodies.PACKAGE_REFUSALS_KEY.encode(): b"{}",
            }
        ),
        first[build.ARCHIVES_TABLE],
    )

    with monkeypatch.context() as failed:
        _no_tree(failed)
        second, _, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert sorted(body.requested) == ["BILLS-119hr6028eh", "BILLS-119hr6028ih"]
    rows = {(row["version_code"], row["source"]) for row in pq.read_table(second["bill_versions"]).to_pylist()}
    sections = [(row["version_code"], row["source"]) for row in read_output(second["bill_sections"]).to_pylist()]
    assert ("introduced-in-house", "congress") in rows
    assert (("introduced-in-house", "govinfo") in rows) is not same_bytes
    assert (sections == [("introduced-in-house", "govinfo")]) is not same_bytes


# --------------------------------------------------------------------------- #
# The per-package route on one printing at a time: ``BodyPass`` over planned work.
# --------------------------------------------------------------------------- #
USLM_PRINTING = ("113-hr-1067", "enrolled-bill", "BILLS-113hr1067enr")
USLM_URL = "https://www.govinfo.gov/content/pkg/BILLS-113hr1067enr/uslm/BILLS-113hr1067enr.xml"
USLM_LINK = {"url": USLM_URL, "type": None, "package_id": None}
USLM = b'<?xml version="1.0"?><pLaw xmlns="http://schemas.gpo.gov/xml/uslm"><main>Public Law 113-237</main></pLaw>'
USLM_DIGEST = "sha256:" + sha256(USLM).hexdigest()


def _row(key, formats, *, source="congress", digest=None, section_count=None, date="2014-12-19T04:59:59Z"):
    bill, code, package = key
    return {
        "bill_id": bill,
        "version_code": code,
        "source": source,
        "label": code.replace("-", " ").title(),
        "version_date": date,
        "package_id": package,
        "offered_formats_json": json.dumps(formats),
        "sha256": digest,
        "section_count": section_count,
    }


class Rendition(StubBodyAcquirer):
    """Serves the packages in ``uslm`` their USLM rendition, as a BILLS MODS record may offer it; others the fixture XML."""

    def __init__(self, uslm=()):
        super().__init__()
        self.uslm = set(uslm)

    def acquire(self, package_id, *, max_bytes=None):
        if package_id not in self.uslm:
            return super().acquire(package_id, max_bytes=max_bytes)
        self.requested.append(package_id)
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/uslm/{package_id}.xml"
        return _Package(
            "uslm",
            CapturedBodyResponse(url, url, 200, "application/xml", "2026-09-19T00:00:00Z", USLM),
            media_type="application/xml",
        )


def _pass(rows, source, *, held=None, package_refusals=None):
    held = held or {}
    work, _, _ = bodies.plan_work(
        rows,
        held=lambda bill: held.get(bill, ()),
        xml=lambda bill: held.get(bill, ()),
        complete_pairs=set(),
        published_pairs={},
    )
    return bodies.BodyPass(
        bills_source=None,
        body_source=source,
        remaining=[10],
        engine=build.engine_stamp(),
        classify=None,
        summarize_diff=None,
        refusals={},
        package_refusals=package_refusals,
    ).run(work)


@pytest.mark.parametrize(
    ("rows", "withdrawn"),
    [
        ([_row(USLM_PRINTING, [USLM_LINK]), _row(USLM_PRINTING, [USLM_LINK], source="govinfo", digest=USLM_DIGEST)], True),
        # No listing beside the body row: withdrawing it would leave the printing no row at all.
        ([_row(USLM_PRINTING, [USLM_LINK], source="govinfo", digest=USLM_DIGEST)], False),
        # A body of other bytes is another read; this refusal says nothing about it.
        (
            [
                _row(USLM_PRINTING, [USLM_LINK]),
                _row(USLM_PRINTING, [USLM_LINK], source="govinfo", digest="sha256:" + "0" * 64),
            ],
            False,
        ),
        # A body row stating a tree is a processed read, whatever a refusal of the same bytes says now.
        (
            [
                _row(USLM_PRINTING, [USLM_LINK]),
                _row(USLM_PRINTING, [USLM_LINK], source="govinfo", digest=USLM_DIGEST, section_count="4"),
            ],
            False,
        ),
    ],
    ids=["listed-same-bytes", "no-listing", "other-bytes", "with-a-tree"],
)
def test_a_uslm_body_is_a_refused_tree_that_withdraws_only_its_own_tree_less_row(rows, withdrawn):
    """A synthetic BILLS USLM rendition exercises parser refusal independently of law target selection."""
    source = Rendition(uslm={USLM_PRINTING[2]})
    outcome = _pass(rows, source)
    assert source.requested == [USLM_PRINTING[2]]
    assert outcome.families == [], "a refused tree is not a read: no body row is written"
    entry = outcome.package_refusals[USLM_PRINTING[2]]
    assert (entry["refusal"], entry["sha256"]) == ("tree", USLM_DIGEST)
    assert outcome.unread == ({(USLM_PRINTING[0], USLM_PRINTING[1], "govinfo")} if withdrawn else set())


def test_a_held_neighbour_read_for_a_comparison_is_never_remembered_or_withdrawn():
    """119-hr-6028's introduced printing is held; the engrossed one is new, so both are read, and the held one refuses."""
    ih = ("119-hr-6028", "introduced-in-house", "BILLS-119hr6028ih")
    eh = ("119-hr-6028", "engrossed-in-house", "BILLS-119hr6028eh")

    def link(package):
        return [
            {
                "url": f"https://www.govinfo.gov/content/pkg/{package}/xml/{package}.xml",
                "type": None,
                "package_id": package,
            }
        ]

    rows = [
        _row(ih, link(ih[2]), source="govinfo", digest=USLM_DIGEST, section_count="3", date="2025-11-12T05:00:00Z"),
        _row(eh, link(eh[2]), date="2026-06-08T04:00:00Z"),
    ]
    source = Rendition(uslm={ih[2]})
    outcome = _pass(rows, source, held={"119-hr-6028": {"introduced-in-house"}})
    assert source.requested == [eh[2], ih[2]], "the new printing, then the neighbour its comparison needs"
    assert outcome.package_refusals == {} and outcome.unread == set()


def test_a_404_withdraws_nothing_even_beside_a_tree_less_row():
    """Absence says nothing about the bytes a row published: the printing keeps both its rows, and is remembered."""

    class Gone(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            self.requested.append(package_id)
            raise _unavailable(404)

    rows = [_row(USLM_PRINTING, [USLM_LINK]), _row(USLM_PRINTING, [USLM_LINK], source="govinfo", digest=USLM_DIGEST)]
    outcome = _pass(rows, Gone())
    assert outcome.package_refusals[USLM_PRINTING[2]]["refusal"] == "unavailable"
    assert outcome.unread == set()


def test_a_refusal_is_asked_again_once_its_listing_or_its_engine_moves():
    rows = [_row(USLM_PRINTING, [USLM_LINK])]
    work, _, _ = bodies.plan_work(rows, held=lambda _: (), xml=lambda _: (), complete_pairs=set(), published_pairs={})
    listed = bodies.listed_digest(work[0].by_code[USLM_PRINTING[1]])
    engine = reader(build.engine_stamp())

    def asked(rows, **entry) -> list[str]:
        source = Rendition(uslm={USLM_PRINTING[2]})
        refusal = {"listed": listed, "refusal": "tree", "sha256": USLM_DIGEST, "engine": engine, "status": None}
        _pass(rows, source, package_refusals={USLM_PRINTING[2]: refusal | entry})
        return source.requested

    assert asked(rows) == [], "unchanged listing and reader"
    assert asked(rows, engine=engine.replace("spicy-docs ", "spicy-docs 0.0.")) == [USLM_PRINTING[2]], (
        "a spicy-docs release reads the tree again"
    )
    assert asked(rows, engine="deltatrack 0.0.9 older") == [USLM_PRINTING[2]], "another engine reads the tree again"
    assert asked(rows, refusal="unavailable", engine="another", status=404) == [], "an absence names no reader"
    assert asked(rows, refusal="unavailable", engine=None, status=451) == [USLM_PRINTING[2]], "not an absence"
    assert asked(rows, refusal="later-kind", engine=None) == [USLM_PRINTING[2]], "an unknown refusal is asked again"
    pdf = {**USLM_LINK, "url": USLM_URL.replace("/uslm/", "/pdf/").replace(".xml", ".pdf")}
    assert asked([_row(USLM_PRINTING, [USLM_LINK, pdf])]) == [USLM_PRINTING[2]], "a listing offering another format moved"


class HtmlOnly(StubBodyAcquirer):
    """A package with no XML rendition: its MODS offers HTML, whatever link the listing states (the plan's 527)."""

    def acquire(self, package_id, *, max_bytes=None):
        self.requested.append(package_id)
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/html/{package_id}.htm"
        capture = CapturedBodyResponse(
            url, url, 200, "text/html", "2026-09-19T00:00:00Z", b"<html><pre>text</pre></html>"
        )
        return _Package("htm", capture, media_type="text/html")


def test_a_printing_is_labelled_by_the_rendition_read_not_the_link_the_listing_prefers(tmp_path, scoped):
    first, _, body = run(tmp_path / "first", body=HtmlOnly())
    assert len(body.requested) == 2
    for row in acquired(first).values():
        assert (row["format_name"], row["content_type"]) == ("html", "text/html")
        assert row["requested_url"].endswith(".htm")
    assert package_refusals(first) == {}, "an HTML body is a read, not a refused XML tree"
    listed = {
        row["version_code"] for row in pq.read_table(first["bill_versions"]).to_pylist() if row["source"] == "congress"
    }
    assert listed == set(), "each body stands for its listing"
    _, _, body = run(tmp_path / "second", prior=tmp_path / "first", body=HtmlOnly())
    assert body.requested == [], "a body read is held, never fetched again"


def test_the_label_is_the_offered_link_of_the_rendition_read_with_the_publishers_own_type():
    """A list-route printing states typed links: read as HTML, the row names the HTML link's type, not the XML one's."""
    key = ("108-hr-1", "introduced-in-house", "BILLS-108hr1ih")
    base = f"https://www.govinfo.gov/content/pkg/{key[2]}"
    offered = [
        {"url": f"{base}/xml/{key[2]}.xml", "type": "Formatted XML", "package_id": key[2]},
        {"url": f"{base}/html/{key[2]}.htm", "type": "HTML", "package_id": key[2]},
    ]
    outcome = _pass([_row(key, offered, date="2003-01-07T05:00:00Z")], HtmlOnly())
    [row] = [row for tables in outcome.families for row in tables.bill_versions if row["source"] == "govinfo"]
    assert (row["format_name"], row["format_type"], row["content_type"]) == ("html", "HTML", "text/html")


def test_missing_published_sections_make_their_printings_pending_again(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    sections = read_output(first["bill_sections"])
    write_output(first["bill_sections"], sections.slice(0, 0))
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [] and len(body.requested) == 2
    assert read_output(second["bill_sections"]).num_rows == sections.num_rows


def test_failed_pdf_cleanup_retries_by_printing_state(tmp_path, scoped, monkeypatch):
    with monkeypatch.context() as failed:
        failed.setattr(bodies, "body_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad PDF")))
        first, _, _ = run(
            tmp_path / "first", bulk=StubBulkAcquirer(status=_pdf_only_status()), body=StubPdfBodyAcquirer()
        )
    assert all(row["sha256"] and row["cleanup_json"] is None for row in acquired(first).values())
    second, _, body = run(
        tmp_path / "second",
        prior=tmp_path / "first",
        bulk=StubBulkAcquirer(status=_pdf_only_status()),
        body=StubPdfBodyAcquirer(),
    )
    assert len(body.requested) == 2
    assert all(row["cleanup_json"] for row in acquired(second).values())


def test_missing_published_diff_is_computed_again(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    diffs = pq.read_table(first["section_diffs"])
    pq.write_table(diffs.slice(0, 0), first["section_diffs"])
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [] and len(body.requested) == 2
    assert pq.read_table(second["section_diffs"]).num_rows == 1


def test_legacy_archive_stamp_without_completion_evidence_is_rechecked(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    path = first[build.ARCHIVES_TABLE]
    pq.write_table(pq.read_table(path).replace_schema_metadata(None), path)
    _, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")]
    assert body.requested == [], "the verified body/diff rows still qualify a completed bill skip"


def status_refusals(paths):
    metadata = pq.read_schema(paths[build.ARCHIVES_TABLE]).metadata or {}
    return json.loads(metadata.get(build.STATUS_REFUSALS_KEY.encode(), b"{}"))


class RefusedMember(StubBulkAcquirer):
    """The folder also holds 119 H.R. 6029, whose document the reader refuses."""

    def acquire(self, *args, **kwargs):
        result = super().acquire(*args, **kwargs)
        if result.archive is not None:
            refused = SimpleNamespace(
                name="BILLSTATUS-119hr6029.xml",
                identity=replace(IDENTITY, number=6029),
                status=None,
                refusal="BILLSTATUS document is not UTF-8",
            )
            result.archive = _Archive([*result.archive.members, refused])
            result.archive.refused_count = 1
        return result


def test_a_refused_member_is_recorded_and_read_again_only_under_another_reader(tmp_path, scoped, monkeypatch):
    """A document the reader refuses does not send its folder's zip back to the network every run."""
    first, _, _ = run(tmp_path / "first", bulk=RefusedMember())
    assert completed_scopes(first) == [["119", "hr"]], "the refusal is recorded, so the folder is complete"
    record = status_refusals(first)["119-hr-6029"]
    assert record["reader"] == build.status_reader() and record["refusal"] == "BILLSTATUS document is not UTF-8"
    assert record["folder"] == "119-hr" and record["zip"] == "2026-09-18T20:26:00+00:00|31656886"
    second, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", bulk=RefusedMember())
    assert bulk.zip_downloads == [], "the same zip under the same reader would refuse the same document"
    assert status_refusals(second) == status_refusals(first), "an unread folder's refusals are carried forward"
    monkeypatch.setattr(build, "status_reader", lambda: "status-v1;code=another")
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "second", bulk=RefusedMember())
    assert bulk.zip_downloads == [(119, "hr")], "another reader retries the refusal"


def test_a_folder_of_refused_documents_is_read_again_under_another_reader(tmp_path, scoped, monkeypatch):
    """No shaped bill goes stale to reopen such a folder, so the recorded refusal's own reader does."""

    class RefusedOnly(RefusedMember):
        def acquire(self, *args, **kwargs):
            result = super().acquire(*args, **kwargs)
            if result.archive is not None:
                result.archive = _Archive([member for member in result.archive.members if member.status is None])
            return result

    run(tmp_path / "first", bulk=RefusedOnly())
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", bulk=RefusedOnly())
    assert bulk.zip_downloads == []
    monkeypatch.setattr(build, "status_reader", lambda: "status-v1;code=another")
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "second", bulk=RefusedOnly())
    assert bulk.zip_downloads == [(119, "hr")], "another reader retries the refusal"


#: The fixture bill with its introduced printing listed twice: 3 text versions, 2 distinct printings, which
#: leaves the bill pending (``version_count`` against its listed rows) however often its status is read.
_TWICE_LISTED = (FIXTURES / "status-119hr6028.xml").read_bytes().replace(
    b"    </textVersions>",
    b"""      <item>
        <type>Introduced in House</type>
        <date>2025-11-12T05:00:00Z</date>
        <formats>
          <item>
            <url>https://www.govinfo.gov/content/pkg/BILLS-119hr6028ih/xml/BILLS-119hr6028ih.xml</url>
          </item>
        </formats>
      </item>
    </textVersions>""",
)


def test_a_bill_its_read_leaves_in_doubt_does_not_reopen_its_folder_every_run(tmp_path, scoped):
    """26 live bills of the 108th-111th and 117th reopened their folders every run (2026-09-29): a doubt the same
    zip and reader cannot settle is recorded, not read again."""
    first, _, _ = run(tmp_path / "first", bulk=StubBulkAcquirer(status=_TWICE_LISTED))
    assert status_refusals(first)["119-hr-6028"]["refusal"] == "version_count 3 against 2 distinct printings"
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", bulk=StubBulkAcquirer(status=_TWICE_LISTED))
    assert bulk.zip_downloads == [], "the bill stays pending, and its folder's zip stays unread"
    moved = StubBulkAcquirer(entry=lambda c, t: zip_entry(c, t, size=31_658_670), status=_TWICE_LISTED)
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "first", bulk=moved)
    assert bulk.zip_downloads == [(119, "hr")], "a moved zip is read"


def test_a_bill_dropped_from_its_zip_does_not_reopen_its_folder_every_run(tmp_path, scoped, monkeypatch):
    """A bill a BILLSTATUS read published and its zip no longer holds keeps an old reader forever: record the drop."""

    class Both(StubBulkAcquirer):
        def acquire(self, congress, bill_type, **kwargs):
            result = super().acquire(congress, bill_type, **kwargs)
            if result.archive is not None and (congress, bill_type) == (119, "hr"):
                body = (FIXTURES / "status-119hr6028.xml").read_bytes().replace(b"6028", b"6029")
                other = _Member(parse_bill_status(body, identity=replace(IDENTITY, number=6029)))
                result.archive = _Archive([*result.archive.members, other])
            return result

    run(tmp_path / "first", bulk=Both())
    monkeypatch.setattr(build, "status_reader", lambda: "status-v1;code=another")
    moved = lambda c, t: zip_entry(c, t, size=31_658_670)  # noqa: E731 — one stub entry
    second, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", bulk=StubBulkAcquirer(entry=moved))
    assert bulk.zip_downloads == [(119, "hr")]
    assert status_refusals(second)["119-hr-6029"]["refusal"] == build.DROPPED_REFUSAL
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "second", bulk=StubBulkAcquirer(entry=moved))
    assert bulk.zip_downloads == [], "the dropped bill's old reader does not reopen the unchanged zip"


def test_failed_held_neighbour_refresh_preserves_its_complete_row(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first", budget=1)
    held = acquired(first)["introduced-in-house"]

    class FailedNeighbour(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            if package_id.endswith("ih"):
                self.requested.append(package_id)
                raise ValueError("temporary body refusal")
            return super().acquire(package_id, **kwargs)

    second, _, body = run(tmp_path / "second", prior=tmp_path / "first", budget=2, body=FailedNeighbour())
    assert body.requested == ["BILLS-119hr6028eh", "BILLS-119hr6028ih"]
    assert acquired(second)["introduced-in-house"] == held
    assert pq.read_table(second["section_diffs"]).num_rows == 0, "the comparison stays pending"


def test_missing_middle_body_does_not_create_a_nonconsecutive_diff(scoped):
    """A held middle printing that cannot be read again stays in the order, so its neighbours are not paired."""
    status = parse_bill_status((FIXTURES / "status-119hr6028.xml").read_bytes(), identity=IDENTITY)
    introduced, engrossed = build._ordered_printings(status)
    enrolled = replace(
        engrossed, type="Enrolled Bill", date="2026-09-20", package_id="BILLS-119hr6028enr",
        formats=tuple(replace(item, url=item.url.replace("6028eh", "6028enr"), package_id="BILLS-119hr6028enr")
                      for item in engrossed.formats),
    )

    def row(version, code, source):
        return {
            "bill_id": "119-hr-6028",
            "version_code": code,
            "source": source,
            "label": version.type,
            "version_date": version.date,
            "package_id": version.package_id,
            "offered_formats_json": json.dumps(
                [{"url": item.url, "type": item.type, "package_id": item.package_id} for item in version.formats]
            ),
            "sha256": "sha256:held" if source == "govinfo" else None,
        }

    class SyntheticThird(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            if package_id.endswith("eh"):
                self.requested.append(package_id)
                raise ValueError("the held middle cannot be read again")
            result = super().acquire(package_id.replace("enr", "eh"), **kwargs)
            capture = result.body_capture
            result.body_capture = replace(
                capture, requested_url=capture.requested_url.replace("6028eh", "6028enr")
                if package_id.endswith("enr") else capture.requested_url,
                resolved_url=capture.resolved_url.replace("6028eh", "6028enr")
                if package_id.endswith("enr") else capture.resolved_url,
            )
            return result

    rows = [
        row(introduced, "introduced-in-house", "congress"),
        row(engrossed, "engrossed-in-house", "govinfo"),
        row(enrolled, "enrolled-bill", "congress"),
    ]
    held = {"engrossed-in-house"}
    work, _, _ = bodies.plan_work(
        rows, held=lambda bill: held, xml=lambda bill: held, complete_pairs=(), published_pairs={}
    )
    acquirer = SyntheticThird()
    outcome = bodies.BodyPass(
        bills_source=None,
        body_source=acquirer,
        remaining=[5],
        engine=build.engine_stamp(),
        classify=None,
        summarize_diff=None,
        refusals={},
    ).run(work)
    assert len(acquirer.requested) == 3, "both new printings, and the held middle for their comparisons"
    [tables] = outcome.families
    assert {row["version_code"] for row in tables.bill_versions} == {"introduced-in-house", "enrolled-bill"}
    assert tables.section_diffs == (), "the held middle placeholder must prevent an introduced-to-enrolled shortcut"


def test_credential_refusal_preserves_every_existing_output(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first", budget=1)
    retry = tmp_path / "retry"
    retry.mkdir()
    for path in first.values():
        if path.is_dir():
            shutil.copytree(path, retry / path.name)
        else:
            (retry / path.name).write_bytes(path.read_bytes())
    before = {path.relative_to(retry): path.read_bytes() for path in retry.rglob("*.parquet")}
    assert any(len(name.parts) > 1 for name in before), "a split table's members are among the outputs"

    class Denied(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            raise CredentialRefusedError("denied")

    with pytest.raises(CredentialRefusedError):
        build.build_bill_family(
            retry,
            bulk_acquirer=StubBulkAcquirer(),
            body_acquirer=Denied(),
            **_priors(tmp_path / "first"),
        )
    assert {name: (retry / name).read_bytes() for name in before} == before


@pytest.mark.parametrize("budget", [-1, 1.5, True, "600"])
def test_invalid_budget_refuses_before_acquisition_or_output(tmp_path, budget):
    bulk = StubBulkAcquirer()
    with pytest.raises(ValueError, match="nonnegative integer"):
        build.build_bill_family(tmp_path, max_version_fetches=budget, bulk_acquirer=bulk, download_prior=_no_prior)
    assert bulk.calls == [] and list(tmp_path.iterdir()) == []


def test_zero_budget_is_metadata_only_and_keeps_body_work_retryable(tmp_path, scoped):
    first, _, body = run(tmp_path / "first", budget=0)
    assert body.requested == []
    assert pq.read_table(first["congress_bills"]).num_rows == 1
    assert not acquired(first) and completed_scopes(first) == [["119", "hr"]]
    _, _, later = run(tmp_path / "second", prior=tmp_path / "first", budget=2)
    assert len(later.requested) == 2


@pytest.mark.parametrize(("raw", "expected"), [(None, 600), ("", 600), ("0", 0), ("17", 17)])
def test_rollup_forwards_explicit_budget(monkeypatch, tmp_path, raw, expected):
    if raw is None:
        monkeypatch.delenv("BILL_FAMILY_MAX_VERSION_FETCHES", raising=False)
    else:
        monkeypatch.setenv("BILL_FAMILY_MAX_VERSION_FETCHES", raw)
    calls = []
    monkeypatch.setattr(rollup.BillFamilyRollup, "build_receipts", lambda self, directory, builder, **kwargs: builder(directory, **kwargs))
    monkeypatch.setattr(rollup, "build_bill_family", lambda output_dir, **kwargs: calls.append(kwargs) or ())
    assert rollup.BillFamilyRollup().build(tmp_path) == ()
    assert calls == [{"max_version_fetches": expected, "evidence": None}]


@pytest.mark.parametrize("raw", ["-1", "1.0", "unlimited"])
def test_rollup_refuses_invalid_budget(monkeypatch, tmp_path, raw):
    monkeypatch.setenv("BILL_FAMILY_MAX_VERSION_FETCHES", raw)
    with pytest.raises(ValueError, match="nonnegative integer"):
        rollup.BillFamilyRollup().build(tmp_path)


def test_dispatch_forwards_zero_as_a_string_without_unlimited_semantics():
    root = Path(__file__).resolve().parents[1]
    dedicated = yaml.safe_load((root / ".github/workflows/rollup-bill-family.yml").read_text())
    shared = yaml.safe_load((root / ".github/workflows/_rollup.yml").read_text())
    inputs = dedicated[True]["workflow_dispatch"]["inputs"]
    assert inputs["max_version_fetches"]["type"] == "string"
    assert inputs["max_version_fetches"]["default"] == "600"
    assert (
        dedicated["jobs"]["run"]["with"]["bill_family_max_version_fetches"]
        == "${{ inputs.max_version_fetches || '600' }}"
    )
    assert shared[True]["workflow_call"]["inputs"]["bill_family_max_version_fetches"]["default"] == "600"
    assert (
        "BILL_FAMILY_MAX_VERSION_FETCHES: ${{ inputs.bill_family_max_version_fetches }}"
        in (root / ".github/workflows/_rollup.yml").read_text()
    )


@pytest.mark.parametrize("missing", ["all", "one", "schema"])
def test_missing_source_versions_reopen_completed_archive(tmp_path, scoped, missing):
    first, _, _ = run(tmp_path / "first")
    versions = pq.read_table(first["bill_versions"])
    damaged = versions.slice(0, 0) if missing == "all" else versions.slice(0, 1)
    if missing == "schema":
        damaged = versions.drop(["sha256"])
    pq.write_table(damaged, first["bill_versions"])

    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")]
    assert bodies.requested
    assert set(acquired(second)) == {"introduced-in-house", "engrossed-in-house"}
    assert completed_scopes(second) == [["119", "hr"]]
    _, next_bulk, next_bodies = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == [] and next_bodies.requested == []


def test_uploaded_version_does_not_conceal_missing_source_version(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    versions = pq.read_table(first["bill_versions"])
    rows = versions.to_pylist()
    rows[0]["source"] = "upload"
    pq.write_table(pa.Table.from_pylist(rows, schema=versions.schema), first["bill_versions"])
    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")] and bodies.requested
    assert set(acquired(second)) == {"introduced-in-house", "engrossed-in-house"}
    assert any(row["source"] == "upload" for row in pq.read_table(second["bill_versions"]).to_pylist())


@pytest.mark.parametrize("missing", ["all", "one", "file", "schema"])
def test_missing_diff_children_reopen_completed_archive(tmp_path, scoped, missing):
    first, _, _ = run(tmp_path / "first", body=StubChangedBodyAcquirer())
    items = pq.read_table(first["section_diff_items"])
    assert items.num_rows > 1
    if missing == "file":
        first["section_diff_items"].unlink()
    else:
        damaged = items.slice(0, 0) if missing == "all" else items.slice(0, 1)
        if missing == "schema":
            damaged = items.drop(["seq"])
        pq.write_table(damaged, first["section_diff_items"])
    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first", body=StubChangedBodyAcquirer())
    assert bulk.zip_downloads == [] and len(bodies.requested) == 2
    assert pq.read_table(second["section_diff_items"]).to_pylist() == items.to_pylist()
    assert completed_scopes(second) == [["119", "hr"]]
    _, next_bulk, next_bodies = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == [] and next_bodies.requested == []


def test_missing_source_version_count_is_requalified_from_status(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    bills = pq.read_table(first["congress_bills"])
    pq.write_table(bills.drop(["version_count"]), first["congress_bills"])
    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")] and bodies.requested == []
    assert pq.read_table(second["congress_bills"])["version_count"].to_pylist() == ["2"]
    _, next_bulk, _ = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == []


def test_corrected_retry_replaces_removed_sections_and_diff_items_only_in_successful_scopes(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first", body=StubChangedBodyAcquirer())
    preserved = {}
    for name in ("bill_sections", "section_diffs", "section_diff_items"):
        table = read_output(first[name])
        rows = table.to_pylist()
        # A section carries its bill's Congress, which its split table is stored by.
        preserved[name] = [
            {**row, "bill_id": "118-hr-99", **({"congress": "118"} if "congress" in row else {})} for row in rows
        ]
        write_output(first[name], pa.Table.from_pylist(rows + preserved[name], schema=table.schema))

    # A missing child makes the existing XML pair retryable. The source now
    # returns the original short bill, which has fewer sections and diff items.
    items = pq.read_table(first["section_diff_items"])
    rows = [row for row in items.to_pylist() if row["bill_id"] == "118-hr-99" or row["seq"] != "0"]
    pq.write_table(pa.Table.from_pylist(rows, schema=items.schema), first["section_diff_items"])
    second, _, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert len(bodies.requested) == 2
    for name in preserved:
        rows = read_output(second[name]).to_pylist()
        assert [row for row in rows if row["bill_id"] == "118-hr-99"] == preserved[name]
    sections = [row for row in read_output(second["bill_sections"]).to_pylist() if row["bill_id"] == "119-hr-6028"]
    # Each original printing has a masthead, enacting clause and short-title node.
    assert len(sections) == 6 and all(row["heading"] != "Funding" for row in sections)
    items = [row for row in pq.read_table(second["section_diff_items"]).to_pylist() if row["bill_id"] == "119-hr-6028"]
    assert len(items) == 3 and all(row["heading"] != "Funding" for row in items)
    parent = next(row for row in pq.read_table(second["section_diffs"]).to_pylist() if row["bill_id"] == "119-hr-6028")
    assert parent["item_count"] == "3"
    assert completed_scopes(second) == [["119", "hr"]]
    _, next_bulk, next_bodies = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == [] and next_bodies.requested == []


@pytest.mark.parametrize("budget", [0, 2])
def test_failed_or_unattempted_retry_preserves_retained_child_scopes(tmp_path, scoped, budget):
    first, _, _ = run(tmp_path / "first", body=StubChangedBodyAcquirer())
    # Force a retry without destroying the retained body/section evidence.
    path = first["section_diffs"]
    parents = pq.read_table(path)
    pq.write_table(parents.slice(0, 0), path)
    before_sections = read_output(first["bill_sections"]).to_pylist()
    before_items = pq.read_table(first["section_diff_items"]).to_pylist()

    class Refused(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            self.requested.append(package_id)
            raise ValueError("temporary source refusal")

    second, _, bodies = run(tmp_path / "second", prior=tmp_path / "first", body=Refused(), budget=budget)
    assert len(bodies.requested) == budget
    assert read_output(second["bill_sections"]).to_pylist() == before_sections
    assert pq.read_table(second["section_diff_items"]).to_pylist() == before_items
    assert pq.read_table(second["section_diffs"]).num_rows == 0, "the comparison stays pending"


# --------------------------------------------------------------------------- #
# The status skip's reader: an unchanged stamp skips a bill only while the
# reader that read it, SpicyDocs' code under this rollup's rule, is the one running (``STATUS_READERS_KEY``).
# --------------------------------------------------------------------------- #
@pytest.fixture
def status_reads(monkeypatch):
    """The bills whose status each run shapes, in order; a test clears it between runs."""
    reads: list[str] = []
    shape = build.build_family

    def spy(capture, **kwargs):
        reads.append(build.bill_key(capture.status.identity))
        return shape(capture, **kwargs)

    monkeypatch.setattr(build, "build_family", spy)
    return reads


def readers(paths):
    return build._held_readers(paths[build.ARCHIVES_TABLE])


def forget_readers(paths):
    """The published state of every bill before the reader was recorded: stamps, outcomes and completion only."""
    path = paths[build.ARCHIVES_TABLE]
    table = pq.read_table(path)
    kept = {k: v for k, v in (table.schema.metadata or {}).items() if k != build.STATUS_READERS_KEY.encode()}
    assert len(kept) < len(table.schema.metadata or {}), "the run recorded a reader to forget"
    pq.write_table(table.replace_schema_metadata(kept), path)


class TwoBills(StubBulkAcquirer):
    """119 HR 6028 and a renumbered copy, 119 HR 6029, in one folder; ``refuse`` has the reader refuse the copy."""

    def __init__(self, *, refuse=False, **kwargs):
        super().__init__(**kwargs)
        self.refuse = refuse

    def acquire(self, congress, bill_type, **kwargs):
        result = super().acquire(congress, bill_type, **kwargs)
        if (congress, bill_type) == (119, "hr") and result.archive is not None:
            body = (FIXTURES / "status-119hr6028.xml").read_bytes().replace(b"6028", b"6029")
            copy = _Member(parse_bill_status(body, identity=replace(IDENTITY, number=6029)))
            if self.refuse:
                copy.status = None
            result.archive = _Archive([*result.archive.members, copy])
            result.archive.refused_count = int(self.refuse)
        return result


def test_a_bill_read_by_the_running_spicy_docs_is_skipped(tmp_path, scoped, status_reads):
    first, _, _ = run(tmp_path / "first")
    assert readers(first) == {"119-hr-6028": build.status_reader()}
    status_reads.clear()
    moved = StubBulkAcquirer(entry=lambda c, t: zip_entry(c, t, size=31_658_670))
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", bulk=moved)
    assert bulk.zip_downloads == [(119, "hr")], "the moved zip is read"
    assert status_reads == [], "and its unchanged bill, read by this SpicyDocs, is not shaped again"


@pytest.mark.parametrize(
    ("moved", "value"), [("spicy_docs_code", lambda: "0" * 64), ("STATUS_READER_RULE", "status-v3")]
)
def test_a_bill_another_reader_read_is_read_again(tmp_path, scoped, status_reads, monkeypatch, moved, value):
    """A new SpicyDocs code, or this rollup's bumped status rule, re-reads the bill."""
    first, _, _ = run(tmp_path / "first")
    status_reads.clear()
    monkeypatch.setattr(build, moved, value)
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")], "its folder is reopened although the zip has not moved"
    assert status_reads == ["119-hr-6028"], "and the bill is shaped by the running reader"
    assert body.requested == [], "its held printings are not fetched again"
    assert readers(second) == {"119-hr-6028": build.status_reader()} != readers(first)


def test_a_prior_with_no_recorded_reader_is_read_exactly_once(tmp_path, scoped, status_reads):
    first, _, _ = run(tmp_path / "first")
    forget_readers(first)
    status_reads.clear()
    second, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")] and status_reads == ["119-hr-6028"], "a completed folder reopens"
    assert readers(second) == {"119-hr-6028": build.status_reader()}
    status_reads.clear()
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "second")
    assert bulk.zip_downloads == [] and status_reads == [], "once read and recorded, it skips"


def test_the_record_survives_a_run_that_rewrites_only_a_sections_partition(tmp_path, scoped, status_reads):
    first, _, _ = run(tmp_path / "first", budget=1)
    status_reads.clear()
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first", budget=1)
    assert bulk.zip_downloads == [] and status_reads == [], "no status is read"
    assert body.requested == ["BILLS-119hr6028eh"], "the body pass reads the pending printing"
    codes = [
        {row["version_code"] for row in read_output(paths["bill_sections"]).to_pylist()} for paths in (first, second)
    ]
    assert codes == [{"introduced-in-house"}, {"introduced-in-house", "engrossed-in-house"}], "congress=119 rewritten"
    assert readers(second) == readers(first) == {"119-hr-6028": build.status_reader()}
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "second", budget=0)
    assert bulk.zip_downloads == [] and status_reads == []


def test_a_capped_run_records_the_reader_of_each_bill_it_read(tmp_path, scoped, status_reads):
    """The status pass has no per-bill cap; a run reaches part of a folder when the reader refuses a member.

    The run is capped as well, at one printing fetch, and records every bill it shaped.
    """
    held, _, _ = run(tmp_path / "held", bulk=TwoBills())
    forget_readers(held)
    status_reads.clear()
    first, _, body = run(tmp_path / "first", prior=tmp_path / "held", budget=1, bulk=TwoBills(refuse=True))
    assert status_reads == ["119-hr-6028"] and len(body.requested) == 1
    assert readers(first) == {"119-hr-6028": build.status_reader()}
    status_reads.clear()
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", budget=1, bulk=TwoBills())
    assert bulk.zip_downloads == [] and status_reads == [], "the recorded refusal does not reopen the same zip"
    moved = TwoBills(entry=lambda c, t: zip_entry(c, t, size=31_658_670))
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "first", budget=1, bulk=moved)
    assert bulk.zip_downloads == [(119, "hr")], "a moved zip is read"
    assert status_reads == ["119-hr-6029"], "the bill the capped run read is not read again"


def test_a_member_shaped_without_a_bill_row_records_no_reader(tmp_path, scoped, status_reads, monkeypatch):
    """A reader is recorded only where the shaper produced the bill's row; the member without one is read again."""
    held, _, _ = run(tmp_path / "held", bulk=TwoBills())
    forget_readers(held)
    shape = build.build_family

    def no_row_for_the_copy(capture, **kwargs):
        tables = shape(capture, **kwargs)
        return replace(tables, bills=()) if capture.status.identity.number == 6029 else tables

    monkeypatch.setattr(build, "build_family", no_row_for_the_copy)
    status_reads.clear()
    first, _, _ = run(tmp_path / "first", prior=tmp_path / "held", bulk=TwoBills())
    assert status_reads == ["119-hr-6028", "119-hr-6029"], "both members are shaped"
    assert readers(first) == {"119-hr-6028": build.status_reader()}, "only the one with a bill row is recorded"
    assert status_refusals(first)["119-hr-6029"]["refusal"] == "no congress_bills row"
    status_reads.clear()
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", bulk=TwoBills())
    assert bulk.zip_downloads == [] and status_reads == [], "the same zip and reader would shape no row again"
    monkeypatch.setattr(build, "build_family", shape)
    moved = TwoBills(entry=lambda c, t: zip_entry(c, t, size=31_658_670))
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "first", bulk=moved)
    assert bulk.zip_downloads == [(119, "hr")], "a moved zip is read"
    assert status_reads == ["119-hr-6029"], "and the member without a row alone is read again"


def test_a_run_over_another_congress_keeps_the_119th_reader_record(tmp_path, scoped, status_reads, monkeypatch):
    first, _, _ = run(tmp_path / "first")
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")
    status_reads.clear()
    second, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(118, "hr")] and status_reads == [], "the 118th's folder holds no bill here"
    assert readers(second) == readers(first) == {"119-hr-6028": build.status_reader()}
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    _, bulk, _ = run(tmp_path / "third", prior=tmp_path / "second")
    assert bulk.zip_downloads == [] and status_reads == [], "so the 119th's next run still skips its bill"


def test_the_reader_record_is_number_runs_that_read_back_every_bill(tmp_path):
    held = {f"119-hr-{n}": "a" for n in (1, 2, 3, 5)} | {"119-s-7": "b", "118-hr-2": "a"}
    encoded = build._readers_metadata(held | {"119-hr-x": "a"})
    assert json.loads(encoded) == {"a": {"118-hr": "2", "119-hr": "1-3,5"}, "b": {"119-s": "7"}}
    path = tmp_path / "archives.parquet"
    for stored, expected in ((encoded, held), ("{", {})):
        pq.write_table(pa.table({"name": pa.array([], pa.string())}, metadata={build.STATUS_READERS_KEY: stored}), path)
        assert build._held_readers(path) == expected
