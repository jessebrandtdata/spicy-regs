"""Build edition-aware U.S. Code structure from explicitly pinned publisher archives.

Annual XHTML and release-point XML retain different edition meanings. These
local destinations describe native structure and provide exact source locators;
they do not replace enacted-law sections or claim normalized legal status.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import gzip
import hashlib
import inspect
from importlib import metadata
import json
from pathlib import Path
import platform
import sys
from tempfile import TemporaryDirectory

import pyarrow as pa

from spicy_docs.sources.uscode import ReleasePoint, TitleSelection
from spicy_docs.sources.uscode import annual, archive, structure, section_bodies
from spicy_regs.etl_receipts import DatasetPolicy
from spicy_regs.parquet_rows import write_rows

FAMILY = "uscode-editions"
#: The journal stage of a release point's corpus zip, captured into the shared raw archive.
CAPTURE_STAGE = "uscode-release-point-corpus"

MAX_ARCHIVE_BYTES = 192 * 1024**2
MAX_MEMBER_BYTES = 128 * 1024**2
MAX_EXPANDED_BYTES = 2 * 1024**3
# Content rows repeat large body/observation text during Arrow conversion.
# This caps held records, not bytes or process memory.
CONTENT_WRITE_BATCH_SIZE = 64
WRITE_BATCH_SIZE = 2_000

EDITION_COLUMNS = (
    "edition_id",
    "source_kind",
    "requested_year",
    "release_point",
    "archive_sha256",
    "member_name",
    "member_sha256",
    "member_bytes",
    "title",
    "native_publication_year",
    "carried_forward",
    "member_role",
    "metadata_json",
)
STRUCTURE_COLUMNS = (
    "observation_id",
    "edition_id",
    "observation_kind",
    "source_position",
    "native_identifier",
    "native_label",
    "native_status",
    "source_observation_json",
)
CONTENT_COLUMNS = (
    "observation_id",
    "edition_id",
    "source_kind",
    "source_position",
    "native_identifier",
    "source_context",
    "body_text",
    "source_observation_json",
)
SCHEMAS = {
    "uscode_editions": pa.schema([(name, pa.string()) for name in EDITION_COLUMNS]),
    "uscode_edition_structure": pa.schema([(name, pa.string()) for name in STRUCTURE_COLUMNS]),
    "uscode_edition_sections": pa.schema([(name, pa.string()) for name in CONTENT_COLUMNS]),
}
#: Each table's installed declaration: schema and identity. Their receipts are retired (``receipts_retired`` in
#: ``etl_policies/``): a row's evidence is its edition row's archive and member digests, the selected original in the
#: shared raw archive, and the generation's source-evidence journal.
POLICIES = {
    name: DatasetPolicy(
        name,
        schema,
        ("edition_id",) if name == "uscode_editions" else ("observation_id",),
        (),
        policy_version="uscode-editions-public-v2",
    )
    for name, schema in SCHEMAS.items()
}


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _identity(*values: object) -> str:
    return _digest(_json(values).encode())


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _code_pins() -> dict[str, str]:
    # Pin maintained packages as complete Python trees: admission and atomic
    # publication call deeper helpers than the public reader entry points.
    import rulespec_artifacts
    import spicy_docs
    import spicy_regs

    paths = {}
    for package in (spicy_docs, rulespec_artifacts, spicy_regs):
        root = Path(inspect.getfile(package)).parent
        for path in root.rglob("*.py"):
            paths[f"{package.__name__}/{path.relative_to(root).as_posix()}"] = path
    checkout = Path(__file__).resolve().parents[3]
    if (checkout / "pyproject.toml").is_file() and (checkout / "uv.lock").is_file():
        for name in ("pyproject.toml", "uv.lock"):
            paths[f"checkout/{name}"] = checkout / name
    pins = {name: _file_digest(path) for name, path in sorted(paths.items())}
    pins["runtime/python"] = platform.python_version()
    for name in ("spicy-docs", "rulespec-artifacts", "pyarrow"):
        pins[f"distribution/{name}"] = metadata.version(name)
    return pins


def build(manifest_path: Path, output_dir: Path) -> dict:
    """Create a new local generation only after all selected archives succeed.

    The only data inputs are the manifest's original ZIP files. Existing tables
    and remote services are never read. Output directories are immutable; a
    refusal removes provisional rows and leaves earlier generations untouched.
    """
    with manifest_path.open("rb") as stream:
        raw_manifest = stream.read(4 * 1024**2 + 1)
    if len(raw_manifest) > 4 * 1024**2:
        raise ValueError("U.S. Code manifest exceeds byte bound")
    manifest = json.loads(raw_manifest)
    if manifest.get("format") != "spicy-uscode-edition-inputs" or manifest.get("version") != 1:
        raise ValueError("unsupported U.S. Code edition input manifest")
    sources = manifest.get("sources")
    if not isinstance(sources, list) or not 1 <= len(sources) <= 1000:
        raise ValueError("a bounded nonempty selection of pinned original archives is required")
    if output_dir.exists():
        raise FileExistsError("U.S. Code output generation already exists")
    code_pins = _code_pins()
    generation = _identity("uscode-editions-public-v2", _digest(raw_manifest), code_pins)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    counts = {name: 0 for name in SCHEMAS}
    inputs, selections = [], set()
    with TemporaryDirectory(dir=output_dir.parent, prefix=".uscode-editions-") as scratch:
        staged = Path(scratch) / "generation"
        staged.mkdir()
        streams = {name: gzip.open(staged / (name + ".jsonl.gz"), "wt", encoding="utf-8") for name in SCHEMAS}

        def emit(table: str, row: dict) -> None:
            streams[table].write(_json(row) + "\n")
            counts[table] += 1

        try:
            for source in sources:
                kind = source["kind"]
                if kind not in ("annual", "release-point-corpus", "release-point-title"):
                    raise ValueError("unsupported U.S. Code archive kind")
                path = Path(source["path"])
                if not path.is_absolute():
                    path = manifest_path.parent / path
                if path.stat().st_size > MAX_ARCHIVE_BYTES:
                    raise ValueError("U.S. Code archive exceeds input bound")
                with path.open("rb") as stream:
                    body = stream.read(MAX_ARCHIVE_BYTES + 1)
                if len(body) > MAX_ARCHIVE_BYTES:
                    raise ValueError("U.S. Code archive exceeds input bound")
                archive_hash = _digest(body)
                if source.get("sha256") != archive_hash:
                    raise ValueError("U.S. Code original archive hash differs from manifest")
                selected = (kind, source.get("year"), source.get("release_point"), source.get("title"))
                if selected in selections:
                    raise ValueError("duplicate U.S. Code edition selection")
                selections.add(selected)
                member_count = 0

                def member(entry, member_body: bytes) -> None:
                    nonlocal member_count
                    member_count += 1
                    metadata = entry.metadata
                    edition_id = _identity(selected, archive_hash, member_count, entry.name, entry.sha256)
                    annual_metadata = metadata if kind == "annual" else None
                    title = None if metadata is None else (metadata.title_enum if annual_metadata else metadata.title)
                    emit(
                        "uscode_editions",
                        {
                            "edition_id": edition_id,
                            "source_kind": kind,
                            "requested_year": str(source["year"]) if kind == "annual" else None,
                            "release_point": None if kind == "annual" else source["release_point"],
                            "archive_sha256": archive_hash,
                            "member_name": entry.name,
                            "member_sha256": entry.sha256,
                            "member_bytes": str(entry.byte_size),
                            "title": title,
                            "native_publication_year": annual_metadata.publication_year if annual_metadata else None,
                            "carried_forward": str(annual_metadata.publication_year != str(source["year"])).lower()
                            if annual_metadata
                            else None,
                            "member_role": "title" if metadata is not None else "supporting-file",
                            "metadata_json": _json(asdict(metadata)) if metadata is not None else None,
                        },
                    )
                    if metadata is None:
                        return

                    def observation(value, observation_kind: str) -> None:
                        if kind == "annual":
                            position = "bytes:" + ":".join(str(v) for v in value.itempath_span)
                            identifier, label, status = value.usckey, value.section_label, None
                        else:
                            position = value.element.source_xpath
                            identifier, label, status = (
                                value.identifier,
                                " ".join(v.text for v in value.numbers),
                                value.status,
                            )
                        emit(
                            "uscode_edition_structure",
                            {
                                "observation_id": _identity(edition_id, observation_kind, position),
                                "edition_id": edition_id,
                                "observation_kind": observation_kind,
                                "source_position": position,
                                "native_identifier": identifier,
                                "native_label": label,
                                "native_status": status,
                                "source_observation_json": _json(asdict(value)),
                            },
                        )

                    def section_content(value) -> None:
                        if kind == "annual":
                            position = "bytes:" + ":".join(str(v) for v in value.structure.itempath_span)
                            identifier = value.structure.usckey
                            source_context = "annual-section-itempath"
                        else:
                            position = value.element.source_xpath
                            identifier = value.element.attributes.get("identifier")
                            source_context = value.context
                        emit(
                            "uscode_edition_sections",
                            {
                                "observation_id": _identity(edition_id, "section-content", position),
                                "edition_id": edition_id,
                                "source_kind": kind,
                                "source_position": position,
                                "native_identifier": identifier,
                                "source_context": source_context,
                                "body_text": value.body_text,
                                "source_observation_json": _json(asdict(value)),
                            },
                        )

                    if kind == "annual":
                        section_bodies.scan_annual_section_bodies(
                            member_body,
                            on_section=section_content,
                            max_bytes=MAX_MEMBER_BYTES,
                        )
                    else:
                        section_bodies.scan_xml_section_bodies(
                            member_body,
                            on_section=section_content,
                            max_bytes=MAX_MEMBER_BYTES,
                        )

                    if kind == "annual":
                        annual.scan_uscode_annual_sections(
                            member_body,
                            on_section=lambda value: observation(value, "annual-itempath"),
                            max_bytes=MAX_MEMBER_BYTES,
                        )
                    else:
                        structure.scan_uscode_structure(
                            member_body,
                            on_section=lambda value: observation(value, "xml-section"),
                            on_chapter=lambda value: observation(value, "xml-chapter"),
                            max_bytes=MAX_MEMBER_BYTES,
                        )

                bounds = dict(max_bytes=MAX_ARCHIVE_BYTES, max_entry_bytes=MAX_MEMBER_BYTES)
                if kind == "annual":
                    archive.read_annual_archive(
                        body, year=source["year"], on_entry=member, max_total_bytes=MAX_EXPANDED_BYTES, **bounds
                    )
                elif kind == "release-point-corpus":
                    archive.read_corpus_archive(
                        body,
                        release_point=ReleasePoint.from_label(source["release_point"]),
                        on_entry=member,
                        max_total_bytes=MAX_EXPANDED_BYTES,
                        **bounds,
                    )
                else:
                    result = archive.read_title_archive(
                        body,
                        selection=TitleSelection(ReleasePoint.from_label(source["release_point"]), source["title"]),
                        **bounds,
                    )
                    member(result.entry, result.xml_bytes)
                inputs.append({**source, "path": str(path.resolve()), "bytes": len(body), "members": member_count})
                with (staged / "progress.jsonl").open("a") as progress:
                    progress.write(
                        _json({"completed": selected, "sources_completed": len(inputs), "rows": counts}) + "\n"
                    )
                del body
        finally:
            for stream in streams.values():
                stream.close()
        outputs = {}
        for table, policy in POLICIES.items():
            records = staged / (table + ".jsonl.gz")
            (staged / table).mkdir()
            subject = staged / table / (table + ".parquet")
            batch = CONTENT_WRITE_BATCH_SIZE if table == "uscode_edition_sections" else WRITE_BATCH_SIZE
            with gzip.open(records, "rt", encoding="utf-8") as stream:
                write_rows(map(json.loads, stream), subject, policy.subject_schema, batch_size=batch)
            outputs[table] = {"rows": counts[table], "sha256": _file_digest(subject), "path": str(subject.relative_to(staged))}
            records.unlink()
        if manifest_path.read_bytes() != raw_manifest or _code_pins() != code_pins:
            raise ValueError("U.S. Code manifest or reader changed during build")
        for source in inputs:
            if _file_digest(Path(source["path"])) != source["sha256"]:
                raise ValueError("U.S. Code original changed during build")
        receipt = {
            "format": "spicy-uscode-edition-rebuild",
            "version": 1,
            "status": "source-only-built",
            "manifest_sha256": _digest(raw_manifest),
            "generation": generation,
            "inputs": inputs,
            "reader_sha256": code_pins,
            "python": platform.python_version(),
            "baseline_inputs": [],
            "outputs": outputs,
            "scope": "Complete selected archive admission and native section/chapter observations. Annual itempaths include nonsections, ranges and stubs. Section content preserves native fields and source context; markup remains in original members. No normalized legal status or publisher-wide completeness claim.",
        }
        (staged / "selection.json").write_bytes(raw_manifest)
        (staged / "rebuild.json").write_text(json.dumps(receipt, indent=2) + "\n")
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(staged, output_dir)
    return receipt


def acquire(release_point: str, output_dir: Path, *, transport=None) -> dict:
    """Capture one release point's complete XML corpus zip into the shared raw archive; return its selection entry.

    The owner's acquirer proves every member's native title and release point before the bytes are kept, and the
    capture's acquisition receipt (URL, time, status, digest) is committed to the raw archive with them. The entry
    names the archive's own stored file, so the build and the audit read the retained original, not a copy.
    """
    from spicy_docs.sources.uscode.acquisition import UsCodeAcquirer, UsCodeAcquisitionBudget
    from spicy_regs.pipelines.raw_releases import open_raw, raw_original
    from spicy_regs.source_evidence import acquisition_evidence

    point = ReleasePoint.from_label(release_point)
    # One request; the timeout bounds each read, not the whole ~110 MB download.
    budget = UsCodeAcquisitionBudget(
        max_requests=1, max_bytes=MAX_ARCHIVE_BYTES, timeout_seconds=120.0, min_request_interval_seconds=1.5
    )
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        acquired = UsCodeAcquirer(budget=budget, transport=transport).acquire_corpus(
            point, max_entry_bytes=MAX_MEMBER_BYTES, max_total_bytes=MAX_EXPANDED_BYTES
        )
        captured = evidence.capture(acquired.capture, stage=CAPTURE_STAGE)
    path = raw_original(open_raw(), captured["sha256"], captured["byte_size"])
    return {"kind": "release-point-corpus", "release_point": point.label, "path": str(path), "sha256": captured["sha256"]}


def main(argv=None) -> None:
    argv = sys.argv[1:] if argv is None else list(argv)
    parser = argparse.ArgumentParser(description=__doc__)
    if argv[:1] == ["acquire"]:
        parser.add_argument("command", choices=["acquire"])
        parser.add_argument("--release-point", required=True, help="e.g. 119-138")
        parser.add_argument("--output-dir", required=True, type=Path, help="Where the acquisition evidence is written")
        parser.add_argument("--selection-out", required=True, type=Path, help="A fresh one-source selection manifest")
        args = parser.parse_args(argv)
        if args.selection_out.exists():
            parser.error(f"--selection-out {args.selection_out} exists")
        source = acquire(args.release_point, args.output_dir)
        manifest = {"format": "spicy-uscode-edition-inputs", "version": 1, "sources": [source]}
        args.selection_out.write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps({"selection": str(args.selection_out), **source}))
        return
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    result = build(args.manifest, args.output)
    print(json.dumps({key: result[key] for key in ("status", "generation", "outputs")}))


if __name__ == "__main__":
    main()
