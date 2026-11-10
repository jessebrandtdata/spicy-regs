"""Acquire Treasury pages and MTS reports into the raw archive, then build a generation from those captures.

A capture selection names each source release as its Fiscal Data scope: the
selection plus the request URL, SHA-256, observation time and size of every
page, exactly as the raw acquisition receipt records them. Releases are
derived from that selection and the raw originals on every build, so no
release directory has to outlive the generation it feeds.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    READ_INDEX_HELP, ReleaseBlobs, admit_release, inherit_read_index, merged_scopes, open_raw, raw_original,
)
from spicy_regs.source_evidence import JOURNAL, CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_treasury_fiscal import SCHEMAS, build_treasury_fiscal

#: The September 2025 and March 2026 MTS PDFs are 3.97 and 4.08 MB.
MAX_REPORT_BYTES = 8 * 1024 * 1024
#: Page sizes of the 2026-10-20 pilot captures; every selection fits one page.
PAGE_SIZES = {"debt_to_penny": 1, "mts_table_1": 50, "mts_table_2": 20}
PUBLISHED_FILES = "https://api.fiscaldata.treasury.gov/services/dtg/publishedfiles?dataset_id=015-BFS-2014Q1-13"
REPORT_HOST = "https://fiscaldata.treasury.gov"
API_STAGE, REPORT_STAGE, LISTING_STAGE = "treasury-api-page", "treasury-report-pdf", "treasury-report-listing"
DATES_STAGE = "treasury-debt-date-listing"
#: Twenty weeks of business days: more than any gap between two hand refreshes so far (three weeks).
MAX_NEW_DEBT_DATES = 100


def _stated_report_url(listing, report_date: date) -> str:
    """The publisher's own MTS PDF path for one issue, never a constructed name."""
    paths = [row["path"] for row in listing
             if row.get("report_date") == report_date.isoformat() and row.get("report_group_id") == "5"]
    if len(paths) != 1 or not paths[0].startswith("/static-data/published-reports/mts/"):
        raise ValueError("Treasury publisher listing does not state exactly one MTS PDF for the issue")
    return REPORT_HOST + paths[0]


def debt_dates_after(evidence, after: date, *, transport=None) -> list[date]:
    """Every Debt to the Penny record date the publisher lists after ``after``, oldest first.

    The listing is captured like any other response. More dates than one page holds are refused, so a long gap
    is selected by hand instead of being silently cut short.
    """
    import httpx
    from spicy_docs.sources.treasury_fiscal import ENDPOINTS, MAX_BYTES

    listed = evidence.transport(transport, stage=DATES_STAGE, max_bytes=MAX_BYTES)
    with httpx.Client(transport=listed, timeout=60) as client:
        listing = client.get(ENDPOINTS["debt_to_penny"], params={
            "fields": "record_date", "filter": f"record_date:gt:{after.isoformat()}", "sort": "record_date",
            "page[size]": str(MAX_NEW_DEBT_DATES)}).raise_for_status().json()
    dates = [date.fromisoformat(row["record_date"]) for row in listing["data"]]
    if listing["meta"]["total-count"] != len(dates) or dates != sorted(set(dates)) or any(d <= after for d in dates):
        raise ValueError("Treasury lists more new debt dates than one run selects, or lists them out of order")
    return dates


def capture(evidence, selections, report_dates, *, transport=None) -> dict:
    """Capture each (dataset, record date) and each stated MTS PDF once through ``evidence``.

    Returns the capture selection read back from the evidence's own receipts.
    """
    import httpx
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.treasury_fiscal import MAX_BYTES, FiscalDataReader, FiscalDataSelection, selected_request

    for dataset, record_date in selections:
        selection = FiscalDataSelection(dataset, record_date.isoformat(), PAGE_SIZES[dataset])
        api = evidence.transport(transport, stage=API_STAGE, max_bytes=MAX_BYTES)
        with FiscalDataReader(budget=PagedJsonBudget(4, MAX_BYTES, 60, 1), transport=api) as reader:
            for _ in reader.read(selection):
                pass
    if report_dates:
        # The listing is retained too, so each PDF path can later be shown to be the publisher's statement.
        listed = evidence.transport(transport, stage=LISTING_STAGE, max_bytes=MAX_BYTES)
        with httpx.Client(transport=listed, timeout=60) as client:
            response = client.get(PUBLISHED_FILES).raise_for_status()
        # The raw archive keeps the wire bytes (gzip when the server encodes),
        # so the digest comes from the completed capture, not the decoded body.
        listing, listing_sha256 = response.json(), response.extensions["spicy_docs.completed_capture"].sha256
        pdf = evidence.transport(transport, stage=REPORT_STAGE, max_bytes=MAX_REPORT_BYTES)
        with httpx.Client(transport=pdf, timeout=120) as client:
            for report_date in report_dates:
                url = _stated_report_url(listing, report_date)
                evidence.event("treasury-report-listing", listing_url=PUBLISHED_FILES, listing_sha256=listing_sha256,
                               report_url=url, report_date=report_date.isoformat())
                response = client.get(url).raise_for_status()
                if response.headers.get("content-type", "").split(";")[0] != "application/pdf":
                    raise ValueError("Treasury report response is not a PDF")
    events = [json.loads(line) for line in (evidence.artifact_dir / JOURNAL).read_text().splitlines()]
    captures = [event for event in events if event["event"] == "capture"]
    releases = {}
    for event in captures:
        if event["stage"] == API_STAGE:
            selection, _ = selected_request(event["requested_url"])
            releases.setdefault((selection.dataset, selection.record_date), {
                "selection": {"dataset": selection.dataset, "record_date": selection.record_date,
                              "page_size": selection.page_size},
                "captures": [],
            })["captures"].append({key: event[source] for key, source in (
                ("request_url", "requested_url"), ("response_sha256", "sha256"),
                ("observed_at", "observed_at"), ("byte_size", "byte_size"))})
    stated = {event["report_url"]: event["report_date"] for event in events
              if event["event"] == "treasury-report-listing"}
    reports = {stated[event["requested_url"]]: {
        "url": event["requested_url"], "sha256": event["sha256"], "byte_size": event["byte_size"],
        "observed_at": event["observed_at"]} for event in captures if event["stage"] == REPORT_STAGE}
    return {"releases": list(releases.values()), "reports": reports}


def acquire(selections, report_dates, output_dir: Path, *, debt_after: date | None = None, transport=None) -> dict:
    """Capture the selections, every debt date listed after ``debt_after`` and the stated MTS PDFs into raw."""
    with acquisition_evidence("treasury-fiscal", output_dir=Path(output_dir)) as evidence:
        listed = debt_dates_after(evidence, debt_after, transport=transport) if debt_after is not None else []
        return capture(evidence, [*selections, *(("debt_to_penny", day) for day in listed)], report_dates,
                       transport=transport)


def published_selection(tables) -> dict:
    """The capture selection behind a published generation, read from its own rows.

    Every row names the page it came from, and every statement row its report control, so the generation
    rebuilds from this selection and its originals without the capture files it was built with.
    """
    import pyarrow.parquet as pq
    from spicy_docs.sources.treasury_fiscal import selected_request

    pages, reports = {}, {}
    for path in tables:
        table = pq.read_table(path)
        for locator in set(table.column("source_locator_json").to_pylist()):
            page = json.loads(locator)
            selection, number = selected_request(page["request_url"])
            key = (selection.dataset, selection.record_date, selection.page_size)
            descriptor = {field: page[field] for field in ("request_url", "response_sha256", "observed_at", "byte_size")}
            if pages.setdefault(key, {}).setdefault(number, descriptor) != descriptor:
                raise ValueError("Published rows name two captures of one Treasury page")
        if "qualification_report_url" in table.column_names:
            controls = table.select(["report_date", "qualification_report_url", "qualification_report_sha256"])
            for issue, url, digest in {tuple(row.values()) for row in controls.to_pylist()}:
                control = {"url": url, "sha256": digest}
                if reports.setdefault(issue.isoformat(), control) != control:
                    raise ValueError("Published rows name two report controls for one issue")
    return {"releases": [
        {"selection": {"dataset": dataset, "record_date": record_date, "page_size": page_size},
         "captures": [numbered[number] for number in sorted(numbered)]}
        for (dataset, record_date, page_size), numbered in sorted(pages.items())], "reports": reports}


def release_readers(selection: dict, destination: Path, *, store):
    """Publish one derived source release per captured scope over raw originals and admit it."""
    from spicy_docs.sources.treasury_fiscal_profile import TREASURY_FISCAL_PROFILE, iter_retained_pages

    blobs = ReleaseBlobs(store, Path(destination) / "blobs")
    return [
        admit_release(TREASURY_FISCAL_PROFILE, iter_retained_pages(scope, blob_source=blobs), scope,
                      Path(destination) / str(index), blobs=blobs,
                      observed=[capture["observed_at"] for capture in scope["captures"]])
        for index, scope in enumerate(selection["releases"])
    ]


def retain_and_build(readers, output_dir: Path, evidence, *, debt_dates, report_dates, reports):
    """Retain the report controls and admitted pages in ``evidence`` and build the tables; returns files and checks."""
    from spicy_docs.sources.treasury_fiscal import MAX_BYTES

    qualifications = {}
    for report_date, control in reports.items():
        with Path(control["path"]).open("rb") as stream:
            body = stream.read(MAX_REPORT_BYTES + 1)
        if len(body) > MAX_REPORT_BYTES or "sha256:" + hashlib.sha256(body).hexdigest() != control["sha256"]:
            raise ValueError("Treasury report control differs from its explicit byte bound or digest")
        evidence.retain_bytes(body, stage="treasury-qualified-report", report_date=report_date.isoformat(),
                              source_url=control["url"], expected_sha256=control["sha256"])
        qualifications[report_date] = {"url": control["url"], "sha256": control["sha256"]}
    files, summary = build_treasury_fiscal(
        readers, output_dir, debt_dates=debt_dates, report_dates=report_dates, qualifications=qualifications,
    )
    for checks in summary:
        evidence.event("treasury-table2-checks", **checks)
    retained = set()
    for reader in readers:
        evidence.event("treasury-source-release", source_logical_id=reader.pin.logical_id,
                       source_artifact_digest=reader.pin.artifact_digest)
        for entry in reader.iter_record_evidence():
            ref = entry["evidenceBlobRef"]
            if ref not in retained:
                evidence.retain_bytes(reader.read_evidence(ref, max_bytes=MAX_BYTES),
                                      stage="treasury-source-page", evidence_blob_ref=ref)
                retained.add(ref)
    return files, summary


def build(readers, output_dir: Path, *, debt_dates, report_dates, reports, read_index: Path | None = None):
    """Retain admitted pages and the explicit report controls through the existing owner."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each Treasury generation")
    evidence = CaptureEvidence(output_dir, "treasury-fiscal")
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        files, summary = retain_and_build(tuple(readers), output_dir, evidence, debt_dates=debt_dates,
                                          report_dates=report_dates, reports=reports)
        artifact = build_generation(
            output_dir / "generation", family="treasury-fiscal", files=files,
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(), read_snapshot=read_snapshot,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir, summary


def merge_selections(selections) -> dict:
    """Combine capture selections; one scope or report per identity, never a silent override."""
    selections, reports = list(selections), {}
    for selection in selections:
        for report_date, report in selection["reports"].items():
            if reports.setdefault(report_date, report) != report:
                raise ValueError("Two Treasury report controls name the same issue")
    return {"releases": merged_scopes(selections), "reports": reports}


def selected_inputs(selection: dict, destination: Path, *, store):
    """Rebuild every selected release from raw originals; returns its readers and the build's date and report inputs."""
    readers = release_readers(selection, destination, store=store)
    dates = {}
    for scope in selection["releases"]:
        dataset = scope["selection"]["dataset"]
        dates.setdefault(dataset == "debt_to_penny", set()).add(date.fromisoformat(scope["selection"]["record_date"]))
    reports = {}
    for report_date, report in selection["reports"].items():
        reports[date.fromisoformat(report_date)] = {
            "url": report["url"], "sha256": report["sha256"],
            "path": str(raw_original(store, report["sha256"], report.get("byte_size")))}
    return readers, {"debt_dates": dates.get(True, set()), "report_dates": dates.get(False, set()), "reports": reports}


def build_from_captures(selection: dict, output_dir: Path, *, archive_root: Path | None = None,
                        read_index: Path | None = None):
    """Rebuild every selected release from raw originals, then the ordinary generation."""
    output_dir = Path(output_dir)
    readers, inputs = selected_inputs(selection, output_dir / "source-releases", store=open_raw(archive_root))
    return build(readers, output_dir, read_index=read_index, **inputs)


def _selection_argument(value: str):
    dataset, _, record_date = value.partition(":")
    if dataset not in PAGE_SIZES:
        raise argparse.ArgumentTypeError("use DATASET:YYYY-MM-DD with a Treasury dataset")
    return dataset, date.fromisoformat(record_date)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire", help="Capture API pages and stated MTS PDFs into the raw archive")
    capture.add_argument("--select", action="append", default=[], type=_selection_argument,
                         help="DATASET:YYYY-MM-DD, repeatable")
    capture.add_argument("--report", action="append", default=[], type=date.fromisoformat,
                         help="MTS issue date whose publisher-stated PDF to capture, repeatable")
    capture.add_argument("--debt-after", type=date.fromisoformat,
                         help="Also capture every Debt to the Penny date the publisher lists after this date")
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--captures-out", required=True, type=Path, help="Where to write the capture selection")
    published = commands.add_parser(
        "published-captures", help="Write the capture selection behind a published generation, from its own rows")
    published.add_argument("--generation", required=True, type=Path, help="A local copy of the published generation")
    published.add_argument("--captures-out", required=True, type=Path)
    generation = commands.add_parser("build", help="Build a generation from capture selections and raw originals")
    generation.add_argument("--captures", required=True, action="append", type=Path)
    generation.add_argument("--output-dir", required=True, type=Path)
    generation.add_argument("--archive-root", type=Path, help="Raw archive holding the originals (read only)")
    generation.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    args = parser.parse_args()
    if args.command in ("acquire", "published-captures"):
        if args.command == "acquire":
            selection = acquire(args.select, args.report, args.output_dir, debt_after=args.debt_after)
        else:
            selection = published_selection(sorted(args.generation.glob("treasury_*.parquet")))
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out), "reports": sorted(selection["reports"]), "releases": [
            f"{scope['selection']['dataset']}:{scope['selection']['record_date']}" for scope in selection["releases"]]}))
        return
    selection = merge_selections(json.loads(path.read_text()) for path in args.captures)
    artifact, evidence, summary = build_from_captures(selection, args.output_dir, archive_root=args.archive_root,
                                                        read_index=args.read_index)
    print(json.dumps({"directory": str(args.output_dir / "generation"), "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest,
                      "table2Checks": summary}))


if __name__ == "__main__":
    main()
