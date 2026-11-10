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
from spicy_regs.pipelines.raw_releases import ReleaseBlobs, admit_release, merged_scopes, open_raw, raw_original
from spicy_regs.source_evidence import JOURNAL, CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_treasury_fiscal import SCHEMAS, build_treasury_fiscal

#: The September 2025 and March 2026 MTS PDFs are 3.97 and 4.08 MB.
MAX_REPORT_BYTES = 8 * 1024 * 1024
#: Page sizes of the 2026-10-20 pilot captures; every selection fits one page.
PAGE_SIZES = {"debt_to_penny": 1, "mts_table_1": 50, "mts_table_2": 20}
PUBLISHED_FILES = "https://api.fiscaldata.treasury.gov/services/dtg/publishedfiles?dataset_id=015-BFS-2014Q1-13"
REPORT_HOST = "https://fiscaldata.treasury.gov"
API_STAGE, REPORT_STAGE, LISTING_STAGE = "treasury-api-page", "treasury-report-pdf", "treasury-report-listing"


def _stated_report_url(listing, report_date: date) -> str:
    """The publisher's own MTS PDF path for one issue, never a constructed name."""
    paths = [row["path"] for row in listing
             if row.get("report_date") == report_date.isoformat() and row.get("report_group_id") == "5"]
    if len(paths) != 1 or not paths[0].startswith("/static-data/published-reports/mts/"):
        raise ValueError("Treasury publisher listing does not state exactly one MTS PDF for the issue")
    return REPORT_HOST + paths[0]


def acquire(selections, report_dates, output_dir: Path, *, transport=None) -> dict:
    """Capture each (dataset, record date) and each stated MTS PDF once through CaptureEvidence.

    Returns the capture selection read back from the run's own receipts.
    """
    import httpx
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.treasury_fiscal import MAX_BYTES, FiscalDataReader, FiscalDataSelection, selected_request

    with acquisition_evidence("treasury-fiscal", output_dir=Path(output_dir)) as evidence:
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


def build(readers, output_dir: Path, *, debt_dates, report_dates, reports):
    """Retain admitted pages and the explicit report controls through the existing owner."""
    from spicy_docs.sources.treasury_fiscal import MAX_BYTES

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each Treasury generation")
    readers = tuple(readers)
    evidence = CaptureEvidence(output_dir, "treasury-fiscal")
    try:
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
        artifact = build_generation(
            output_dir / "generation", family="treasury-fiscal", files=files,
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
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


def build_from_captures(selection: dict, output_dir: Path, *, archive_root: Path | None = None):
    """Rebuild every selected release from raw originals, then the ordinary generation."""
    store = open_raw(archive_root)
    output_dir = Path(output_dir)
    readers = release_readers(selection, output_dir / "source-releases", store=store)
    dates = {}
    for scope in selection["releases"]:
        dataset = scope["selection"]["dataset"]
        dates.setdefault(dataset == "debt_to_penny", set()).add(date.fromisoformat(scope["selection"]["record_date"]))
    reports = {}
    for report_date, report in selection["reports"].items():
        reports[date.fromisoformat(report_date)] = {
            "url": report["url"], "sha256": report["sha256"],
            "path": str(raw_original(store, report["sha256"], report["byte_size"]))}
    return build(readers, output_dir, debt_dates=dates.get(True, set()), report_dates=dates.get(False, set()),
                 reports=reports)


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
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--captures-out", required=True, type=Path, help="Where to write the capture selection")
    generation = commands.add_parser("build", help="Build a generation from capture selections and raw originals")
    generation.add_argument("--captures", required=True, action="append", type=Path)
    generation.add_argument("--output-dir", required=True, type=Path)
    generation.add_argument("--archive-root", type=Path, help="Raw archive holding the originals (read only)")
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.select, args.report, args.output_dir)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out), "releases": len(selection["releases"]),
                          "reports": sorted(selection["reports"])}))
        return
    selection = merge_selections(json.loads(path.read_text()) for path in args.captures)
    artifact, evidence, summary = build_from_captures(selection, args.output_dir, archive_root=args.archive_root)
    print(json.dumps({"directory": str(args.output_dir / "generation"), "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest,
                      "table2Checks": summary}))


if __name__ == "__main__":
    main()
