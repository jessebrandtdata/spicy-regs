"""Read every selected disbursement PDF page into retained source-owned observations.

The caller selects qualified full reports or historical parts. This stage
reuses central originals and the maintained selection validator; it neither
fetches files nor decides whether two renditions duplicate one another.
Completion means every selected page was visited. Unread image/summary content,
unassigned text, ambiguous amounts and source errors remain explicit.
"""

from __future__ import annotations

import argparse
import gzip
import importlib
import inspect
import json
import shutil
import sys
import uuid
from collections import Counter
from importlib.metadata import version
from pathlib import Path
from typing import Any

from spicy_regs.pipelines.congressional_disbursements import _digest, _write, read_selection
from spicy_regs.pipelines.disbursement_page_profiles import (
    PROFILES,
    read_source_pages,
    reader_module,
    record_kind,
    validate_page_content,
)


def pin(path: Path) -> dict:
    return {"path": str(path.resolve()), "sha256": _digest(path), "bytes": path.stat().st_size}


def verify_pin(value: dict) -> Path:
    path = Path(value["path"])
    if pin(path) != value:
        raise ValueError("Retained PDF reading input or output changed")
    return path


def processing_inputs(source: dict) -> list[dict]:
    """Verify the declared retained-observation and independent-review closure."""
    retained = {}
    selected = source.get("structured_observations", [])
    pages = [item["page"] for item in selected]
    if pages != sorted(set(pages)) or any(page not in source.get("reading_pages", []) for page in pages):
        raise ValueError("Structured observations require distinct selected physical pages")
    for item in selected:
        qualification = json.loads(verify_pin(item["qualification"]).read_text())
        if qualification.get("schema") != "source-qualified-structured-summary/1" or qualification.get(
            "status"
        ) not in {"accepted_observation", "accepted_with_reviewed_corrections"}:
            raise ValueError("Structured observation has no accepted source qualification")
        for key in ("source_sha256", "source_byte_size"):
            if qualification[key] != source[key]:
                raise ValueError("Structured qualification names a different original")
        if qualification["page"] != item["page"] or qualification["physical_pages"] != source["physical_page_count"]:
            raise ValueError("Structured qualification names a different original page")
        if not qualification.get("review_evidence"):
            raise ValueError("Structured qualification omits its independent source review")
        for key in ("result", "image"):
            if qualification[key] != {field: item[key][field] for field in ("sha256", "bytes")}:
                raise ValueError("Structured qualification differs from its selected input pins")
        for artifact in [
            item["qualification"],
            item["result"],
            item["image"],
            *item.get("evidence", []),
            *qualification["review_evidence"],
            *(review for rule in qualification.get("corrections", []) for review in rule["review_evidence"]),
        ]:
            verify_pin(artifact)
            prior = retained.setdefault(artifact["path"], artifact)
            if prior != artifact:
                raise ValueError("Structured evidence path has conflicting byte identities")
    return list(retained.values())


def _retain_implementation(producer: dict, directory: Path) -> list[dict]:
    """Keep the small implementation inputs replayable after checkout edits."""
    directory.mkdir(parents=True, exist_ok=True)
    retained = []
    for original in producer["files"]:
        source = verify_pin(original)
        destination = directory / (original["sha256"].removeprefix("sha256:") + "-" + source.name)
        if not destination.exists():
            shutil.copyfile(source, destination)
        captured = pin(destination)
        if any(captured[key] != original[key] for key in ("sha256", "bytes")):
            raise ValueError("Retained PDF implementation copy differs from its original")
        retained.append({"original": original, "retained": captured})
    return retained


def retained_implementation(manifest: dict) -> dict[str, dict]:
    """Resolve every declared implementation input to its exact retained copy."""
    copies = manifest["implementation_copies"]
    originals = manifest["implementation"]["files"]
    if [item["original"] for item in copies] != originals:
        raise ValueError("PDF reading implementation copy membership differs")
    for item in copies:
        verify_pin(item["retained"])
        if any(item["retained"][key] != item["original"][key] for key in ("sha256", "bytes")):
            raise ValueError("PDF reading implementation copy identity differs")
    return {item["original"]["path"]: item["retained"] for item in copies}


def implementation(chamber: str, profile: str = "ledger") -> dict:
    """Pin the imported reader, shared semantics, repair data and actual runtime."""
    source_reader = reader_module(chamber, profile)
    names = (
        "spicy_docs.reading.pdf_grid",
        "spicy_docs.reading." + ("senate_disbursements" if chamber == "senate" else "house_disbursements_pdf"),
        "spicy_docs.reading.senate_payment_candidates",
        "spicy_docs.schemas.senate_expenditure_tables",
        "spicy_regs.pipelines.congressional_disbursements",
        "spicy_docs.reading.disbursement_date_repairs",
        "spicy_regs.pipelines.disbursement_page_profiles",
    )
    if profile != "ledger":
        from spicy_docs.extraction.derivation import derivation_modules

        # Pin the source reader's imported processing closure, including the
        # optional table extractor and literal report-period reader.
        names += derivation_modules(
            source_reader,
            packages=(
                "spicy_docs.reading",
                "spicy_docs.extraction",
                "spicy_docs.schemas",
                "spicy_docs.sources.disbursements_index",
            ),
        )
        names = tuple(dict.fromkeys(names))
    modules = [importlib.import_module(name) for name in names]
    paths = [Path(inspect.getfile(module)) for module in modules]
    correction_file = (
        "senate_disbursements_corrections.json" if chamber == "senate" else "house_disbursements_pdf_corrections.json"
    )
    paths.append(paths[1].with_name(correction_file))
    selected_reader = importlib.import_module(source_reader)
    paths.extend(
        Path(inspect.getfile(selected_reader)).with_name(name)
        for name in getattr(selected_reader, "SOURCE_RESOURCES", ())
    )
    paths.append(Path(__file__))
    return {
        "python": sys.version.split()[0],
        "pymupdf": version("pymupdf"),
        "reading_profile": profile,
        "files": [pin(path) for path in paths],
    }


def selected_sources(selection_path: Path, chamber: str) -> list[dict]:
    sources = [
        source
        for source in read_selection(selection_path)["files"]
        if source["chamber"] == chamber and source["format"] == "pdf"
    ]
    if not sources or any(
        not source.get(key)
        for source in sources
        for key in ("source_sha256", "source_byte_size", "period_start", "period_end")
    ):
        raise ValueError("PDF reading requires retained source pins and evidenced report periods")
    for source in sources:
        if "reading_pages" in source:
            pages = source["reading_pages"]
            if (
                not isinstance(pages, list)
                or not pages
                or any(type(number) is not int or number < 1 for number in pages)
                or pages != sorted(set(pages))
                or not source.get("reading_scope")
            ):
                raise ValueError(
                    "Selected PDF pages require ascending distinct positive integers and a scope explanation"
                )
    return sources


def source_implementation(source: dict, producer: dict, *, retained_files: dict | None = None) -> dict:
    """A correction to another original does not require reading this PDF again."""
    code, rules = [], []
    for artifact in producer["files"]:
        if artifact["path"].endswith("_corrections.json"):
            rules.extend(
                rule
                for rule in json.loads(verify_pin((retained_files or {}).get(artifact["path"], artifact)).read_text())
                if rule["source_sha256"] == source["source_sha256"]
            )
        else:
            code.append(artifact)
    result = {
        "python": producer["python"],
        "pymupdf": producer["pymupdf"],
        "code": code,
        "rules": rules,
        "source": {
            key: source[key]
            for key in (
                "id",
                "chamber",
                "url",
                "kind",
                "source_sha256",
                "source_byte_size",
                "period_start",
                "period_end",
            )
        },
    }
    if producer.get("reading_profile", "ledger") != "ledger":
        result["reading_profile"] = producer["reading_profile"]
    if "reading_pages" in source:
        result["source"].update(reading_pages=source["reading_pages"], reading_scope=source["reading_scope"])
    if source.get("structured_observations"):
        processing_inputs(source)
        result["source"]["structured_observations"] = source["structured_observations"]
    return result


def _read_one(source: dict, body: bytes, output: Path, profile: str = "ledger") -> dict:
    counts, kinds, roles, issues = Counter(), Counter(), Counter(), Counter()
    page_count = row_count = repairs = 0
    temporary = output.with_name(output.name + ".attempt-" + uuid.uuid4().hex)
    # A failed attempt remains available for diagnosis. Resume writes another
    # attempt and adopts it only after whole-source identity/coverage checks.
    selected = source.get("reading_pages")
    physical_pages = None
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=3) as stream:
        for page in read_source_pages(source, body, profile):
            number = selected[page_count] if selected is not None and page_count < len(selected) else page_count + 1
            if (
                page["page"] != number
                or page["source_page"] != number
                or page["source_sha256"] != source["source_sha256"]
                or (selected is not None and page_count >= len(selected))
            ):
                raise ValueError("PDF page stream differs from selected original or page order")
            if physical_pages is not None and physical_pages != page["physical_pages"]:
                raise ValueError("PDF reading changes its original physical page count")
            physical_pages = page["physical_pages"]
            validate_page_content(page, profile)
            page_count += 1
            row_count += len(page["records"])
            repairs += sum(len(row.get("repairs", [])) for row in page["records"])
            counts[page["status"]] += 1
            kinds.update(record_kind(row) for row in page["records"])
            roles.update(line["role"] for line in page.get("lines", []))
            issues.update(page["issues"])
            issues.update(issue for row in page["records"] for issue in row["issues"])
            stream.write(json.dumps(page, ensure_ascii=False, separators=(",", ":")) + "\n")
    expected = source.get("physical_page_count") or source.get("normalization_qualification", {}).get("physical_pages")
    if (
        not page_count
        or page_count != (len(selected) if selected is not None else physical_pages)
        or (expected is not None and physical_pages != expected)
    ):
        raise ValueError("PDF page stream ended before its selected scope or differs from the full retained source")
    temporary.replace(output)
    return {
        "id": source["id"],
        "status": "read",
        "source_sha256": source["source_sha256"],
        "source_byte_size": source["source_byte_size"],
        "pages": page_count,
        "physical_pages": physical_pages,
        "selected_pages": selected,
        "reading_profile": profile,
        "records": row_count,
        "repairs": repairs,
        "page_statuses": dict(counts),
        "row_kinds": dict(kinds),
        "line_roles": dict(roles),
        "issues": dict(issues),
        "output": pin(output),
    }


def read_pages(
    selection_path: Path,
    output_dir: Path,
    *,
    chamber: str,
    archive_root: Path | None = None,
    reuse_manifest: Path | None = None,
    profile: str = "ledger",
) -> dict:
    """Resume unchanged page readings and retry incomplete or failed sources."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    sources = selected_sources(selection_path, chamber)
    producer = implementation(chamber, profile)
    selection = pin(selection_path)
    store = CentralRawArchiveStore(archive_root, create=False)
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    implementation_copies = _retain_implementation(producer, output_dir / "implementation")
    manifest_path = output_dir / "manifest.json"
    if reuse_manifest is not None and reuse_manifest.resolve() == manifest_path:
        raise ValueError("PDF cross-generation reuse must name another retained manifest")
    reusable = {}
    reuse_pin = None
    if reuse_manifest is not None:
        reuse_pin = pin(reuse_manifest)
        prior_generation = json.loads(reuse_manifest.read_text())
        if (
            prior_generation.get("schema") != "congressional-disbursement-pdf-reading/1"
            or prior_generation.get("chamber") != chamber
            or prior_generation.get("reading_profile", "ledger") != profile
        ):
            raise ValueError("PDF reuse manifest belongs to another reading family")
        for entry in prior_generation["files"]:
            if entry["id"] in reusable:
                raise ValueError("PDF reuse manifest repeats a source identity")
            if entry["status"] == "read":
                reusable[entry["id"]] = entry
    manifest: dict[str, Any] = {
        "schema": "congressional-disbursement-pdf-reading/1",
        "chamber": chamber,
        "reading_profile": profile,
        "selection": selection,
        "implementation": producer,
        "implementation_copies": implementation_copies,
        "archive_root": str(store.root),
        "selected": [source["id"] for source in sources],
        "reuse_manifest": reuse_pin,
        "complete_meaning": "Every explicitly selected physical page was visited; omitted pages and unread content are not qualified by completion.",
    }
    previous = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    if previous and any(previous.get(key) != value for key, value in manifest.items()):
        raise ValueError("PDF resume requires identical selection, reader and runtime; use a new output")
    results = {entry["id"]: entry for entry in previous.get("files", [])}
    if set(results) - set(manifest["selected"]) or len(results) != len(previous.get("files", [])):
        raise ValueError("PDF resume has repeated or unselected source outcomes")

    def checkpoint():
        entries = [results[source["id"]] for source in sources if source["id"] in results]
        pending = [source["id"] for source in sources if results.get(source["id"], {}).get("status") != "read"]
        manifest.update(
            files=entries,
            pending=pending,
            complete=not pending,
            pages=sum(entry.get("pages", 0) for entry in entries),
            records=sum(entry.get("records", 0) for entry in entries),
        )
        _write(manifest_path, manifest)

    for source in sources:
        store.describe_blob(source["source_sha256"], source["source_byte_size"])
        source_policy = source_implementation(source, producer)
        prior = results.get(source["id"])
        if prior and prior["status"] == "read":
            if (prior["source_sha256"], prior["source_byte_size"]) != (
                source["source_sha256"],
                source["source_byte_size"],
            ) or prior.get("source_implementation") != source_policy:
                raise ValueError("PDF resume source identity differs")
            verify_pin(prior["output"])
            continue
        reusable_entry = reusable.get(source["id"])
        if reusable_entry and reusable_entry.get("source_implementation") == source_policy:
            verify_pin(reusable_entry["output"])
            results[source["id"]] = {**reusable_entry, "reused_from": reuse_pin}
            checkpoint()
            continue
        with store.open(source["source_sha256"]) as stream:
            body = stream.read(source["source_byte_size"] + 1)
        output = output_dir / (source["id"] + ".pages.jsonl.gz")
        try:
            results[source["id"]] = _read_one(source, body, output, profile)
            results[source["id"]]["source_implementation"] = source_policy
        except (ValueError, RuntimeError, OSError) as error:
            results[source["id"]] = {
                "id": source["id"],
                "status": "read-failed",
                "source_sha256": source["source_sha256"],
                "source_byte_size": source["source_byte_size"],
                "error_type": type(error).__name__,
                "error": str(error),
            }
        if producer != implementation(chamber, profile):
            raise ValueError("PDF reader changed during processing; preserve this candidate and start a new output")
        checkpoint()
        # Full policies and correction witnesses live once in the manifest.
        # Progress logs need only the outcome and its retained output locator.
        print(
            json.dumps(
                {
                    key: value
                    for key, value in results[source["id"]].items()
                    if key in {"id", "status", "pages", "records", "repairs", "output", "error_type", "error"}
                }
            ),
            flush=True,
        )
    if producer != implementation(chamber, profile):
        raise ValueError("PDF reader changed during processing; preserve this candidate and start a new output")
    verify_pin(selection)
    if reuse_pin is not None:
        verify_pin(reuse_pin)
    checkpoint()
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--chamber", choices=("house", "senate"), required=True)
    parser.add_argument("--archive-root", type=Path)
    parser.add_argument("--reuse-manifest", type=Path)
    parser.add_argument("--profile", choices=PROFILES, default="ledger")
    args = parser.parse_args()
    result = read_pages(
        args.selection,
        args.output,
        chamber=args.chamber,
        archive_root=args.archive_root,
        reuse_manifest=args.reuse_manifest,
        profile=args.profile,
    )
    if not result["complete"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
