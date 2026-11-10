"""Acquire selected congressional spending originals and replay source-owned normalization.

Selections contain publisher-listed URLs and report periods, never constructed
file names. Exact originals live in the central raw archive. Per-file outcomes
make acquisition resumable independently of parser support: a captured malformed
CSV remains available for offline repair without another download.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import inspect
import json
import sys
from contextlib import ExitStack
from dataclasses import asdict
from datetime import date
from importlib.resources import files as resource_files
from pathlib import Path
from typing import Any

import httpx

from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError

SCHEMA = "congressional-disbursement-selection/1"


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _write(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".part")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def read_selection(path: Path) -> dict:
    """Refuse conflicting selections before any acquisition or output mutation."""
    from spicy_docs.sources.house_disbursements import house_disbursements_csv_url
    from spicy_docs.sources.disbursements_reports import disbursements_report_locator

    selection = json.loads(path.read_text())
    if selection.get("schema") != SCHEMA or not selection.get("files"):
        raise ValueError("Expected a nonempty congressional disbursement selection")
    identities, urls = set(), set()
    for item in selection["files"]:
        identity = item["id"]
        if (
            not isinstance(identity, str)
            or not identity
            or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for c in identity)
        ):
            raise ValueError("Source selection ID must be a safe lowercase identifier")
        if identity in identities or item["url"] in urls:
            raise ValueError("Select each source ID and URL once; keep repeated listing occurrences in metadata")
        identities.add(identity)
        urls.add(item["url"])
        if item["chamber"] not in {"house", "senate"}:
            raise ValueError("Expected House or Senate source selection")
        if item["format"] == "csv":
            if item["chamber"] != "house" or item["kind"] not in {"detail", "summary"}:
                raise ValueError("CSV selections require House detail or summary")
            house_disbursements_csv_url(item["url"])
            if not item["period_start"] or not item["period_end"]:
                raise ValueError("House CSV normalization requires an evidenced report period")
        elif item["format"] == "pdf":
            locator = disbursements_report_locator(item["url"])
            if item["kind"] not in {"full", "part"} or locator.chamber not in {None, item["chamber"]}:
                raise ValueError("PDF source chamber or full/part selection conflicts with its locator")
        else:
            raise ValueError("Expected a CSV or PDF source")
        start = date.fromisoformat(item["period_start"]) if item["period_start"] else None
        end = date.fromisoformat(item["period_end"]) if item["period_end"] else None
        if start and end and end < start:
            raise ValueError("Report period is reversed")
    return selection


def acquire_selection(
    selection_path: Path,
    output_dir: Path,
    *,
    reuse: dict | None = None,
    archive_root: Path | None = None,
    transport=None,
) -> dict:
    """Resume exact verified captures and retry every unsuccessful source.

    ``reuse`` maps a URL to an explicitly pinned local original. A local import
    records its actual location and prior receipt; it never invents a new HTTP
    observation. A captured-but-unparsed file is an acquisition success and a
    normalization gap, which is retried offline by the normalization stage.
    """
    from spicy_docs.sources.house_disbursements import (
        HouseDisbursementsAcquirer,
        HouseDisbursementsBudget,
        HouseDisbursementsCsv,
    )
    from spicy_docs.sources.disbursements_reports import (
        DisbursementsReportAcquirer,
        DisbursementsReportBudget,
        PDF_MEDIA_TYPES,
    )
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_docs.transport.captured import attached_capture
    from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential
    from spicy_regs.pipelines.disbursement_inventory import record_original

    selection = read_selection(selection_path)
    selection_hash = _digest(selection_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = output_dir / "acquisition.json"
    previous = json.loads(result_path.read_text()) if result_path.exists() else {}
    if previous and previous.get("selection_sha256") != selection_hash:
        raise ValueError("Resume requires the identical source selection; use a new output for changed scope")
    store = CentralRawArchiveStore(archive_root)
    results = {item["id"]: item for item in previous.get("files", [])}
    evidence = CaptureEvidence(output_dir, "congressional-disbursements", archive_root=store.root)
    result = {
        "schema": "congressional-disbursement-acquisition/1",
        "selection": str(selection_path.resolve()),
        "selection_sha256": selection_hash,
        "archive_root": str(store.root),
        "files": [],
    }
    selection_pin = {"path": str(selection_path.resolve()), "sha256": selection_hash}

    def register(source):
        source["inventory_receipt"] = record_original(store, source, selection=selection_pin)

    def checkpoint():
        result["files"] = [results[item["id"]] for item in selection["files"] if item["id"] in results]
        result["pending"] = [
            item["id"]
            for item in selection["files"]
            if item["id"] not in results
            or results[item["id"]]["status"] not in {"retained", "captured", "captured-unparsed"}
        ]
        _write(result_path, result)

    try:
        with ExitStack() as stack:
            readers = {}
            for file_format in {item["format"] for item in selection["files"]}:
                budget = HouseDisbursementsBudget() if file_format == "csv" else DisbursementsReportBudget()
                retained_transport = evidence.transport(
                    transport, stage="disbursement-" + file_format, max_bytes=budget.max_bytes
                )
                reader = (
                    HouseDisbursementsAcquirer(transport=retained_transport, budget=budget)
                    if isinstance(budget, HouseDisbursementsBudget)
                    else DisbursementsReportAcquirer(transport=retained_transport, budget=budget)
                )
                readers[file_format] = stack.enter_context(reader)
            for item in selection["files"]:
                prior = results.get(item["id"])
                if prior and prior["status"] in {"retained", "captured", "captured-unparsed"}:
                    store.describe_blob(prior["source_sha256"], prior["source_byte_size"])
                    register(prior)
                    evidence.event(
                        "reuse",
                        source_id=item["id"],
                        source_url=item["url"],
                        source_sha256=prior["source_sha256"],
                        # The earlier acquisition's run, not its folder on this machine (ss-81bv).
                        prior_run_id=Path(prior["evidence_directory"]).name,
                    )
                    continue
                base = {**item, "evidence_directory": str(evidence.directory)}
                local = (reuse or {}).get(item["url"])
                if local is not None:
                    path = Path(local["path"])
                    if path.stat().st_size != local["byte_size"] or _digest(path) != local["sha256"]:
                        raise ValueError("Selected retained original changed")
                    if local.get("receipt_path") and _digest(Path(local["receipt_path"])) != local["receipt_sha256"]:
                        raise ValueError("Selected original's acquisition receipt changed")
                    captured = evidence.retain_file(
                        path,
                        stage="retained-disbursement-" + item["format"],
                        raw_source=True,
                        source_url=item["url"],
                        original_location=item["url"],
                        prior_receipt=Path(local["receipt_path"]).name if local.get("receipt_path") else None,
                        prior_receipt_sha256=local.get("receipt_sha256"),
                    )
                    results[item["id"]] = {
                        **base,
                        "status": "retained",
                        "source_sha256": captured["sha256"],
                        "source_byte_size": captured["byte_size"],
                    }
                    if item["format"] == "csv":
                        results[item["id"]]["encoding"] = local.get("encoding", item.get("encoding", "utf-8-sig"))
                else:
                    try:
                        reader = readers[item["format"]]
                        if isinstance(reader, HouseDisbursementsAcquirer):
                            parsed, capture = reader.acquire_csv(
                                item["url"], kind=item["kind"], encoding=item.get("encoding", "utf-8-sig")
                            )
                        else:
                            parsed, capture = reader.acquire_report(item["url"])
                    except CredentialRefusedError:
                        raise
                    except (ValueError, httpx.HTTPError, OSError) as error:
                        if isinstance(error, SourceEvidenceError):
                            raise
                        capture = attached_capture(error)
                        media_types = ("text/csv",) if item["format"] == "csv" else PDF_MEDIA_TYPES
                        successful_response = (
                            capture is not None
                            and capture.status_code == 200
                            and capture.resolved_url == item["url"]
                            and (capture.content_type or "").split(";")[0] in media_types
                        )
                        # Every response is already retained by the shared evidence transport.
                        results[item["id"]] = {
                            **base,
                            "status": "captured-unparsed" if successful_response else "failed",
                            "error_type": type(error).__name__,
                            "error": scrub_credential(str(error), "")[:1000],
                        }
                        if capture is not None:
                            results[item["id"]].update(source_sha256=capture.sha256, source_byte_size=capture.byte_size)
                        evidence.event(
                            "source-parse-outcome", source_id=item["id"], status=results[item["id"]]["status"]
                        )
                        evidence.refusal(error, stage="disbursement-source-parse")
                    else:
                        store.describe_blob(capture.sha256, capture.byte_size)
                        results[item["id"]] = {
                            **base,
                            "status": "captured",
                            "source_sha256": capture.sha256,
                            "source_byte_size": capture.byte_size,
                        }
                        if isinstance(parsed, HouseDisbursementsCsv):
                            results[item["id"]].update(
                                encoding=parsed.encoding, parsed_rows=len(parsed.rows), layout=parsed.layout
                            )
                        else:
                            results[item["id"]]["pdf_facts"] = asdict(parsed)
                        del parsed
                if results[item["id"]]["status"] in {"retained", "captured", "captured-unparsed"}:
                    register(results[item["id"]])
                checkpoint()
                print(json.dumps({"id": item["id"], "status": results[item["id"]]["status"]}), flush=True)
        checkpoint()
    except BaseException as error:
        checkpoint()
        evidence.finish(error)
        raise
    else:
        evidence.finish()
    result["last_evidence_directory"] = str(evidence.directory)
    _write(result_path, result)
    return result


def _normalization_implementation() -> dict:
    """Pin the actual imported implementation, including its packaged repair declarations."""
    pins = {}
    for name in ("house_disbursements", "house_disbursements_normalize", "house_disbursements_repairs"):
        module = importlib.import_module("spicy_docs.sources." + name)
        path = Path(inspect.getfile(module))
        pins[name + ".py"] = _digest(path)
    declarations = resource_files("spicy_docs.sources").joinpath("house_disbursements_corrections.json").read_bytes()
    pins["house_disbursements_corrections.json"] = "sha256:" + hashlib.sha256(declarations).hexdigest()
    pins["congressional_disbursements.py"] = _digest(Path(__file__))
    source_rules = {
        entry["source_sha256"]: "sha256:"
        + hashlib.sha256(json.dumps(entry, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        for entry in json.loads(declarations)
    }
    return {"python": sys.version.split()[0], "files": pins, "source_rules": source_rules}


def _source_implementation(source: dict, encoding: str, implementation: dict) -> dict:
    """Changing another original's repair declarations does not invalidate this file."""
    return {
        "python": implementation["python"],
        "code": {
            name: sha for name, sha in implementation["files"].items() if name != "house_disbursements_corrections.json"
        },
        "rules": implementation["source_rules"].get(source["source_sha256"]),
        "source": {
            key: source[key]
            for key in ("id", "url", "source_sha256", "source_byte_size", "kind", "period_start", "period_end")
        },
        "encoding": encoding,
    }


def _read_acquisition(path: Path) -> dict:
    acquisition = json.loads(path.read_text())
    selection_path = Path(acquisition["selection"])
    if _digest(selection_path) != acquisition["selection_sha256"]:
        raise ValueError("Acquisition selection pin differs")
    selected = {item["id"]: item for item in read_selection(selection_path)["files"]}
    seen, pending = set(), set(selected)
    for source in acquisition["files"]:
        identity = source["id"]
        if identity not in selected or identity in seen:
            raise ValueError("Acquisition membership differs from the pinned selection")
        seen.add(identity)
        if any(
            source[key] != selected[identity][key]
            for key in ("url", "chamber", "format", "kind", "period_start", "period_end")
        ):
            raise ValueError("Acquisition source facts differ from the pinned selection")
        if source["status"] in {"captured", "retained", "captured-unparsed"}:
            pending.remove(identity)
        elif source["status"] != "failed":
            raise ValueError("Unknown acquisition outcome")
    if pending != set(acquisition["pending"]):
        raise ValueError("Acquisition pending membership differs from source outcomes")
    return acquisition


def _verify_normalized(entry: dict) -> None:
    receipt_path = Path(entry["receipt"])
    if _digest(receipt_path) != entry["receipt_sha256"]:
        raise ValueError("Normalized receipt changed; preserve the generation for investigation")
    receipt = json.loads(receipt_path.read_text())
    for path, pin in receipt["artifacts"].items():
        artifact = Path(path)
        if artifact.stat().st_size != pin["bytes"] or _digest(artifact) != pin["sha256"]:
            raise ValueError("Normalized output changed; preserve the generation for investigation")


def normalize_selection(
    acquisition_path: Path, output_dir: Path, *, source_options: dict | None = None, reuse_manifest: Path | None = None
) -> dict:
    """Resume exact outputs and retry failures using the source-owned normalizer.

    A source-specific encoding decision names the original digest and its review
    evidence in ``source_options``. There is no automatic lossy decoding. Changed
    inputs, options or implementation require a new generation.
    """
    from spicy_docs.sources.house_disbursements import parse_house_disbursements_csv
    from spicy_docs.sources.house_disbursements_normalize import write_normalized_house_disbursements
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    acquisition = _read_acquisition(acquisition_path)
    if any(item["format"] != "csv" for item in acquisition["files"]):
        raise ValueError("The CSV normalization stage requires CSV inputs; PDF reading is a separate stage")
    implementation = _normalization_implementation()
    options = source_options or {}
    selected_shas = {item.get("source_sha256") for item in acquisition["files"]}
    for sha, option in options.items():
        if (
            sha not in selected_shas
            or option.get("encoding") not in {"utf-8-sig", "cp1252"}
            or not option.get("reason")
            or not option.get("evidence")
        ):
            raise ValueError(
                "Encoding overrides require a selected input digest, supported encoding and review evidence"
            )
    store = CentralRawArchiveStore(Path(acquisition["archive_root"]), create=False)
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    if reuse_manifest is not None and reuse_manifest.resolve() == manifest_path:
        raise ValueError("Cross-generation reuse must name a different immutable manifest")
    reusable = {}
    reuse_pin = None
    if reuse_manifest is not None:
        reuse_pin = {"path": str(reuse_manifest.resolve()), "sha256": _digest(reuse_manifest)}
        reusable = {
            item["id"]: item
            for item in json.loads(reuse_manifest.read_text())["files"]
            if item["status"] == "normalized"
        }
    previous = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    acquisition_hash = _digest(acquisition_path)
    if previous and any(
        previous.get(key) != value
        for key, value in (
            ("acquisition_sha256", acquisition_hash),
            ("implementation", implementation),
            ("source_options", options),
            ("reuse_manifest", reuse_pin),
        )
    ):
        raise ValueError(
            "Normalization resume requires identical inputs, implementation and options; use a new generation"
        )
    results = {item["id"]: item for item in previous.get("files", [])}
    manifest: dict[str, Any] = {
        "schema": "congressional-disbursement-normalization/1",
        "acquisition": str(acquisition_path.resolve()),
        "acquisition_sha256": acquisition_hash,
        "implementation": implementation,
        "source_options": options,
        "reuse_manifest": reuse_pin,
    }

    def checkpoint():
        entries = [results[item["id"]] for item in acquisition["files"] if item["id"] in results]
        manifest.update(
            files=entries,
            rows=sum(item.get("rows", 0) for item in entries),
            pending=sorted(
                set(acquisition["pending"])
                | {
                    item["id"]
                    for item in acquisition["files"]
                    if results.get(item["id"], {}).get("status") != "normalized"
                }
            ),
            complete=not acquisition["pending"]
            and len(entries) == len(acquisition["files"])
            and all(item["status"] == "normalized" for item in entries),
        )
        _write(manifest_path, manifest)

    for source in acquisition["files"]:
        entry = {"id": source["id"], "url": source["url"], "source_sha256": source.get("source_sha256")}
        prior = results.get(source["id"])
        if prior and prior["status"] == "normalized":
            store.describe_blob(source["source_sha256"], source["source_byte_size"])
            _verify_normalized(prior)
            continue
        if source["status"] == "failed":
            results[source["id"]] = {**entry, "status": "acquisition-failed"}
            checkpoint()
            continue
        store.describe_blob(source["source_sha256"], source["source_byte_size"])
        encoding = options.get(source["source_sha256"], {}).get("encoding", source.get("encoding", "utf-8-sig"))
        source_implementation = _source_implementation(source, encoding, implementation)
        reusable_entry = reusable.get(source["id"])
        if reusable_entry:
            _verify_normalized(reusable_entry)
            receipt = json.loads(Path(reusable_entry["receipt"]).read_text())
            if receipt.get("source_implementation") == source_implementation:
                results[source["id"]] = {**reusable_entry, "reused_from": reuse_pin}
                checkpoint()
                print(json.dumps({"id": source["id"], "status": "reused-normalization"}), flush=True)
                continue
        with store.open(source["source_sha256"]) as stream:
            body = stream.read(source["source_byte_size"] + 1)
        start = date.fromisoformat(source["period_start"])
        try:
            table = parse_house_disbursements_csv(body, kind=source["kind"], encoding=encoding)
        except ValueError as error:
            results[source["id"]] = {
                **entry,
                "status": "parse-failed",
                "error_type": type(error).__name__,
                "error": str(error),
            }
            checkpoint()
            continue
        csv_path = output_dir / (source["id"] + ".csv")
        audit_path = output_dir / (source["id"] + ".audit.jsonl")
        with csv_path.open("w", newline="", encoding="utf-8") as out, audit_path.open("w", encoding="utf-8") as audit:
            receipt: dict[str, Any] = write_normalized_house_disbursements(
                table, out, audit=audit, report_year=start.year, report_quarter=(start.month - 1) // 3 + 1
            )
        if (receipt["report_period_start"], receipt["report_period_end"]) != (
            source["period_start"],
            source["period_end"],
        ):
            raise ValueError("Publisher's listed period differs from the normalizer's quarter; review the source label")
        receipt.update(
            source_url=source["url"],
            selection=source,
            implementation=implementation,
            source_implementation=source_implementation,
            source_options=options.get(source["source_sha256"]),
            artifacts={str(p): {"sha256": _digest(p), "bytes": p.stat().st_size} for p in [csv_path, audit_path]},
        )
        receipt_path = output_dir / (source["id"] + ".receipt.json")
        _write(receipt_path, receipt)
        results[source["id"]] = {
            **entry,
            "status": "normalized",
            "rows": receipt["rows"],
            "issues": receipt["issues"],
            "repairs": len(receipt["repairs"]),
            "reviews": len(receipt["date_reviews"]),
            "receipt": str(receipt_path),
            "receipt_sha256": _digest(receipt_path),
        }
        print(json.dumps(results[source["id"]]), flush=True)
        checkpoint()
        del table, body
    checkpoint()
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    acquire = commands.add_parser("acquire")
    acquire.add_argument("--selection", required=True, type=Path)
    acquire.add_argument("--output", required=True, type=Path)
    acquire.add_argument("--reuse", type=Path)
    normalize = commands.add_parser("normalize")
    normalize.add_argument("--acquisition", required=True, type=Path)
    normalize.add_argument("--output", required=True, type=Path)
    normalize.add_argument("--source-options", type=Path)
    normalize.add_argument("--reuse-manifest", type=Path)
    args = parser.parse_args()
    if args.command == "acquire":
        acquire_selection(args.selection, args.output, reuse=json.loads(args.reuse.read_text()) if args.reuse else None)
    else:
        normalize_selection(
            args.acquisition,
            args.output,
            source_options=json.loads(args.source_options.read_text()) if args.source_options else None,
            reuse_manifest=args.reuse_manifest,
        )


if __name__ == "__main__":
    main()
