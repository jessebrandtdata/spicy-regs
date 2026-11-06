"""0.24.0 adoption proofs for committee-report and bill-family rollups: resume, partial corrections, and refusals.

Upstream owns the interpretation rules; this suite pins the wiring.
"""

import html
import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.congress.bill_status import BillIdentity, parse_bill_status
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms.build_bill_family import build_bill_family
from spicy_regs.transforms.build_committee_reports import build_committee_reports
from spicy_regs.transforms.table_merge import prior_members_path, prior_scratch_path
from tests.test_bill_family import (
    StubBodyAcquirer, StubBulkAcquirer, _Acquisition, _Archive, _Member, _capture, _no_prior,
)
from tests.test_committee_reports import (
    CHRG_ID, CRPT_ID, CrptOnlyDiscovery, NoBodies, StubDiscovery, StubHearings, _package,
)
from tests.test_congress_index import _run

FIXTURES = Path(__file__).parent / "fixtures" / "adoption_0_24_0"


def retain(paths):
    """Simulate a retained prior run by copying each output to its prior-scratch path, a split table's members too."""
    for path in paths:
        if path.is_dir():
            shutil.copytree(path, prior_members_path(path.parent, path.name) / path.name, dirs_exist_ok=True)
        else:
            shutil.copyfile(path, prior_scratch_path(path.parent, path.stem))


def test_cover_links_use_mods_and_empty_covers_resume_without_body_requests(tmp_path):
    """Pins ``mods_cover`` linkage with no bill, complete outcomes, and a second run that requests no bodies."""
    package_id = "CHRG-118hhrg56198"

    class Discovery(StubDiscovery):
        IDS = {"CRPT": [], "CHRG": [package_id, CHRG_ID]}

    class Bodies(NoBodies):
        requested = []

        def acquire(self, package_id, *, max_bytes=None, prefer=()):
            key = package_id
            self.requested.append(key)
            mods = (FIXTURES / f"mods-{key}.excerpt.xml").read_bytes() if key == "CHRG-118hhrg56198" else None
            return _package(key, mods_bytes=mods)

    class Hearings:
        def records(self, route, url, *, max_pages=1):
            jacket = url.split("?")[0].rsplit("/", 1)[-1]
            yield SimpleNamespace(records=[{"congress": 118 if jacket == "56198" else 119, "jacketNumber": jacket}])

    bodies = Bodies()
    paths = build_committee_reports(tmp_path, reader=Discovery(), acquirer=bodies,
                                   hearings=Hearings(), download_prior=_no_prior)
    rows = pq.read_table(paths[3]).to_pylist()
    assert len(rows) == 12
    assert {r["link_source"] for r in rows} == {"mods_cover"}
    assert {r["relation"] for r in rows} == {"held_on"}
    assert all(r["held_date"] and r["committee_system_code"] for r in rows)
    assert all(r["bill_id"] is None for r in pq.read_table(paths[2]).to_pylist())
    assert {r["outcome"] for r in pq.read_table(paths[-1]).to_pylist()} == {"complete"}
    retain(paths)
    bodies.requested.clear()
    build_committee_reports(tmp_path, reader=Discovery(), acquirer=bodies,
                            hearings=Hearings(), download_prior=_no_prior)
    assert bodies.requested == []


@pytest.mark.parametrize(("fixture", "states", "has_span"), [
    ("CRPT-118hrpt53.txt", "true", True), ("CRPT-118hrpt18.txt", "false", False),
])
def test_report_pass_runs_the_recital_gate_on_its_existing_body(tmp_path, fixture, states, has_span):
    """A retained report body, not synthetic package metadata, is what the recital gate reads."""
    # Deliberately synthetic package metadata; the retained excerpt exercises body-to-rule wiring.
    body = ("<html><body><pre>" + html.escape((FIXTURES / fixture).read_text()) + "</pre></body></html>").encode()
    class Acquirer(NoBodies):
        def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
            return (_package(package_id, body=body),)

    acquirer = Acquirer()
    paths = build_committee_reports(tmp_path, reader=CrptOnlyDiscovery(), acquirer=acquirer,
                                   hearings=StubHearings(), download_prior=_no_prior)
    row = pq.read_table(paths[0]).to_pylist()[0]
    assert row["report_states_estimate"] == states
    assert row["cbo_letter_count"] == ("1" if has_span else "0")
    letters = pq.read_table(paths[4]).to_pylist()
    assert [letter["letter_text_sha256"] is not None for letter in letters] == ([True] if has_span else [])
    assert all((letter["package_id"], letter["part_id"]) == (row["package_id"], row["part_id"]) for letter in letters)
    assert row["estimate_rule_version"]


def test_pending_packages_survive_an_advanced_window_and_the_cap(tmp_path):
    class Discovery(StubDiscovery):
        IDS = {"CRPT": [CRPT_ID, "CRPT-119hrpt2"], "CHRG": []}

    class Bodies(NoBodies):
        requested = []

        def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
            key = package_id
            self.requested.append(key)
            raise ValueError("publisher refusal")

    bodies = Bodies()
    paths = build_committee_reports(tmp_path, reader=Discovery(), acquirer=bodies,
                                   hearings=StubHearings(), max_packages=1, download_prior=_no_prior)
    assert bodies.requested == [CRPT_ID]
    assert {r["outcome"] for r in pq.read_table(paths[-1]).to_pylist()} == {"refused", "pending"}
    retain(paths)
    discovery = Discovery()
    discovery.IDS = {"CRPT": [], "CHRG": []}
    bodies.requested.clear()
    build_committee_reports(tmp_path, reader=discovery, acquirer=bodies,
                            hearings=StubHearings(), max_packages=2, download_prior=_no_prior)
    assert bodies.requested == [CRPT_ID, "CRPT-119hrpt2"]


@pytest.mark.parametrize("status", [401, 403])
def test_report_body_credential_refusal_aborts_without_checkpoint(tmp_path, status):
    """A 401/403 body fetch raises before any capture table or checkpoint is written."""
    class Refusing(NoBodies):
        def acquire_parts(self, package_id, *, max_bytes=None, prefer=()):
            raise CredentialRefusedError(f"HTTP {status}")

    with pytest.raises(CredentialRefusedError):
        build_committee_reports(tmp_path, reader=CrptOnlyDiscovery(), acquirer=Refusing(),
                                hearings=StubHearings(), download_prior=_no_prior)
    assert not list(tmp_path.glob("*.parquet"))


def test_bill_family_emits_provider_estimates_and_refreshes_old_unchanged_rows(tmp_path, monkeypatch):
    """A prior archive missing the newly adopted field must be re-read; only then may an unchanged one be skipped."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.setattr("spicy_regs.transforms.build_bill_family.resolve_gemini_key", lambda: None)
    xml = (FIXTURES / "BILLSTATUS-118hr801.excerpt.xml").read_bytes()

    class Bulk(StubBulkAcquirer):
        compared = []

        def acquire(self, congress, bill_type, *, unchanged_since=None):
            self.compared.append(unchanged_since)
            if unchanged_since is not None:
                return super().acquire(congress, bill_type, unchanged_since=unchanged_since)
            status = parse_bill_status(xml, identity=BillIdentity(118, "hr", 801))
            return _Acquisition(_Archive([_Member(status)]), _capture("https://www.govinfo.gov/bulkdata/test.zip", xml))

    bulk = Bulk()
    def run():
        return build_bill_family(tmp_path, bulk_acquirer=bulk, body_acquirer=StubBodyAcquirer(),
                                 max_version_fetches=0, download_prior=_no_prior)

    paths = run()
    estimates = pq.read_table(tmp_path / "cbo_cost_estimates.parquet").to_pylist()
    assert [(r["bill_id"], r["publication_id"]) for r in estimates] == [("118-hr-801", "59139")]
    retain(paths)
    bills = prior_scratch_path(tmp_path, "congress_bills")
    pq.write_table(pq.read_table(bills).drop(["cbo_cost_estimates_outcome"]), bills)
    run()
    assert bulk.compared[-1] is None, "an old archive must be re-read for the newly adopted field"
    retain(paths)
    run()
    assert bulk.compared[-1] is not None, "after enrichment the unchanged archive can be skipped"


def test_communication_route_is_filled_on_unchanged_old_rows(tmp_path):
    """A held row gains its route fields without any detail request (``max_details=0``)."""
    rows, _ = _run(tmp_path, "house_communications")
    path = tmp_path / "house_communications.parquet"
    old = pq.read_table(path).drop(["source_route", "record_package_id", "record_granule_id",
                                   "record_entry_text", "reconstruction_rule_version"])
    pq.write_table(old, prior_scratch_path(tmp_path, "house_communications"))
    updated, reader = _run(tmp_path, "house_communications", max_details=0)
    assert len(updated) == len(rows)
    assert reader.details == []
    assert {r["source_route"] for r in updated} == {"congress-gov-detail"}
    assert all(r["record_entry_text"] is None for r in updated)


@pytest.mark.parametrize("correction", ["empty", "partial", "refused"])
def test_hearing_correction_preserves_other_revisions_and_parent_links(tmp_path, correction):
    """A new revision keeps historical links; empty, partial and refused reads stay distinct."""
    package_id = "CHRG-118hhrg56198"
    modified = "2026-09-18T12:00:00Z"
    mods = (FIXTURES / f"mods-{package_id}.excerpt.xml").read_bytes()
    asked = []

    class Discovery:
        def packages(self, url, *, max_pages=1):
            yield SimpleNamespace(records=[{"packageId": package_id, "lastModified": modified}]
                                  if "/CHRG/" in url else [])

    class Bodies(NoBodies):
        def acquire(self, package_id, *, max_bytes=None, prefer=()):
            asked.append(package_id)
            if correction == "refused" and modified.startswith("2026-09-20"):
                raise ValueError("body temporarily unavailable")
            package = _package(package_id, mods_bytes=mods)
            return replace(package, summary=replace(package.summary, last_modified=modified))

    class Hearings:
        def records(self, route, url, *, max_pages=1):
            yield SimpleNamespace(records=[{"congress": 118, "jacketNumber": "56198"}])

    def run():
        return build_committee_reports(tmp_path, reader=Discovery(), acquirer=Bodies(),
                                       hearings=Hearings(), download_prior=_no_prior)

    paths = run()
    original = pq.read_table(paths[3])
    assert original.num_rows == 12
    untouched = original.to_pylist()[0] | {"package_id": "CHRG-118hhrg99999"}
    pq.write_table(pa.Table.from_pylist([*original.to_pylist(), untouched], schema=original.schema), paths[3])
    retain(paths)
    modified = "2026-09-20T12:00:00Z"
    mods = mods.replace(b'context="COVER"', b'context="BODY"', 1 if correction == "partial" else -1)
    paths = run()
    rows = pq.read_table(paths[3]).to_pylist()
    fresh = [row for row in rows if row["package_id"] == package_id and row["last_modified"] == modified]
    assert len(fresh) == {"empty": 0, "partial": 11, "refused": 0}[correction]
    historical = [row for row in rows if row["package_id"] == package_id and row["last_modified"] != modified]
    assert historical == original.to_pylist()
    assert [row for row in rows if row["package_id"] != package_id] == [untouched]
    [checkpoint] = pq.read_table(paths[-1]).to_pylist()
    assert checkpoint["outcome"] == ("refused" if correction == "refused" else "complete")
    retain(paths)
    asked.clear()
    run()
    assert asked == ([package_id] if correction == "refused" else [])
    assert pq.read_table(paths[3]).to_pylist() == rows


@pytest.mark.parametrize("correction", ["absent", "empty", "replacement", "unexpected"])
def test_cbo_correction_replaces_only_successfully_evaluated_bill_estimates(tmp_path, monkeypatch, correction):
    """A correction touches only the evaluated bill; an unreadable answer keeps the old estimate, other bills stand."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.setattr("spicy_regs.transforms.build_bill_family.resolve_gemini_key", lambda: None)
    xml = (FIXTURES / "BILLSTATUS-118hr801.excerpt.xml").read_bytes()

    class Bulk(StubBulkAcquirer):
        def acquire(self, congress, bill_type, *, unchanged_since=None):
            status = parse_bill_status(xml, identity=BillIdentity(118, "hr", 801))
            return _Acquisition(_Archive([_Member(status)]), _capture("https://www.govinfo.gov/bulkdata/test.zip", xml))

    def run():
        return build_bill_family(tmp_path, bulk_acquirer=Bulk(), body_acquirer=StubBodyAcquirer(),
                                 max_version_fetches=0, download_prior=_no_prior)

    paths = run()
    estimates_path = tmp_path / "cbo_cost_estimates.parquet"
    original = pq.read_table(estimates_path)
    [old] = original.to_pylist()
    untouched = old | {"bill_id": "118-hr-9999"}
    pq.write_table(pa.Table.from_pylist([old, untouched], schema=original.schema), estimates_path)
    retain(paths)
    root = ET.fromstring(xml)
    bill = root.find("bill")
    assert bill is not None
    updated = bill.find("updateDateIncludingText")
    estimates = bill.find("cboCostEstimates")
    assert updated is not None and estimates is not None
    updated.text = "2026-09-20T12:00:00Z"
    if correction == "absent":
        bill.remove(estimates)
    elif correction == "empty":
        estimates.clear()
    elif correction == "unexpected":
        estimates.text = "upstream failed"
    else:
        url = estimates.find("item/url")
        assert url is not None
        url.text = "https://www.cbo.gov/publication/99999"
    xml = ET.tostring(root)
    paths = run()
    rows = pq.read_table(estimates_path).to_pylist()
    assert [row for row in rows if row["bill_id"] == "118-hr-9999"] == [untouched]
    fresh = [row for row in rows if row["bill_id"] == "118-hr-801"]
    assert [row["publication_id"] for row in fresh] == (
        ["59139"] if correction == "unexpected" else ["99999"] if correction == "replacement" else []
    )
    if correction != "unexpected":
        retain(paths)
        run()
        assert pq.read_table(estimates_path).to_pylist() == rows
