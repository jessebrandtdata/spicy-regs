"""Build administration-policy statement observations and explicit bill links offline.

SpicyDocs owns native reading. These application tables retain community archive
assertions, source order and evidence without inferring political positions.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import gzip
import hashlib
from importlib import metadata
import inspect
import json
from pathlib import Path
import platform
import re
from tempfile import TemporaryDirectory

import pyarrow as pa
from rulespec_artifacts import publish_directory_no_replace
from spicy_docs.sources import administration_policy
from spicy_docs.schemas.tables import bill_id as canonical_bill_id
from spicy_docs.sources.congress.bill_status import BillIdentity

from spicy_regs import etl_receipts
from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset

VERSION = "administration-policy-v1"
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_SOURCES = 32
SCHEMAS = {
    "administration_policy_statements": pa.schema(
        [
            ("statement_id", pa.string()),
            ("source_record_id", pa.string()),
            ("archive_commit", pa.string()),
            ("administration", pa.string()),
            ("source_record_index", pa.int64()),
            ("congress", pa.int64()),
            ("document_title", pa.string()),
            ("date_issued", pa.string()),
            ("date_issued_status", pa.string()),
            ("rescinded", pa.bool_()),
            ("archived_pdf_url", pa.string()),
            ("source_uri", pa.string()),
            ("source_sha256", pa.string()),
            ("observed_at", pa.string()),
            ("source_record_json", pa.string()),
        ]
    ),
    "administration_policy_bill_links": pa.schema(
        [
            ("link_id", pa.string()),
            ("statement_id", pa.string()),
            ("source_link_index", pa.int64()),
            ("congress", pa.int64()),
            ("bill", pa.string()),
            ("native_bill_id", pa.string()),
            ("bill_id", pa.string()),
        ]
    ),
}
POLICIES = {
    name: DatasetPolicy(
        name, schema, ("statement_id",) if name.endswith("statements") else ("link_id",), (), policy_version=VERSION
    )
    for name, schema in SCHEMAS.items()
}


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _read(path: Path, limit: int) -> bytes:
    # Compressed canonical objects describe decoded original bytes.
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as stream:
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise ValueError("selected input exceeds byte bound")
    return body


def _code_pins() -> dict:
    import spicy_docs
    import rulespec_artifacts

    paths = {"builder": Path(__file__), "etl_receipts": Path(inspect.getfile(etl_receipts))}
    receipt_root = Path(inspect.getfile(etl_receipts)).parent
    for name in (
        "etl_bulk.py",
        "policy_migrations.py",
        "policy_migrations.json",
        "parquet_rows.py",
        "native_types.py",
    ):
        paths[f"spicy_regs/{name}"] = receipt_root / name
    # Include parser/transitive admission and no-replace publication helpers,
    # without pinning unrelated concurrently edited application source files.
    for package in (spicy_docs, rulespec_artifacts):
        root = Path(inspect.getfile(package)).parent
        for path in root.rglob("*.py"):
            paths[f"{package.__name__}/{path.relative_to(root).as_posix()}"] = path
    return {
        "files": {name: _file_digest(path) for name, path in sorted(paths.items())},
        "python": platform.python_version(),
        "distributions": {
            name: metadata.version(name) for name in ("spicy-docs", "rulespec-artifacts", "pyarrow", "duckdb", "pyyaml")
        },
    }


def build(manifest_path: Path, output_dir: Path) -> dict:
    """Admit a bounded pinned selection, then publish one immutable local candidate.

    Existing destinations and the network are never read. A failed selection
    leaves no partial candidate and cannot overwrite a successful generation.
    """
    raw_manifest = _read(manifest_path, MAX_MANIFEST_BYTES)
    manifest = json.loads(raw_manifest)
    if (
        not isinstance(manifest, dict)
        or manifest.get("format") != "spicy-retained-collection-inputs"
        or manifest.get("version") != 1
    ):
        raise ValueError("unsupported retained collection manifest")
    sources = manifest.get("sources")
    if not isinstance(sources, list) or not 1 <= len(sources) <= MAX_SOURCES:
        raise ValueError("require a bounded nonempty source selection")
    if output_dir.exists():
        raise FileExistsError("output candidate already exists")
    pins = _code_pins()
    generation = _digest(_json([VERSION, _digest(raw_manifest), pins]).encode())
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    inputs, selections = [], set()
    counts = {table: 0 for table in SCHEMAS}
    with TemporaryDirectory(dir=output_dir.parent, prefix=".retained-collections-") as scratch:
        staged = Path(scratch) / "candidate"
        staged.mkdir()
        streams = {table: gzip.open(staged / (table + ".jsonl.gz"), "wt", encoding="utf-8") for table in SCHEMAS}

        def emit(table: str, row: dict, context: ReceiptContext) -> None:
            streams[table].write(_json({"row": row, "context": asdict(context)}) + "\n")
            counts[table] += 1

        try:
            for source in sources:
                if not isinstance(source, dict) or source.get("kind") != "administration-policy":
                    raise ValueError("unsupported retained source kind")
                commit, administration = source["commit"], source["administration"]
                url = administration_policy.metadata_url(commit, administration)
                if source.get("url") != url:
                    raise ValueError("metadata locator differs from selected commit/administration")
                if not isinstance(source.get("observed_at"), str) or not source["observed_at"].strip():
                    raise ValueError("source acquisition observation time is required")
                selection = (commit, administration)
                if selection in selections:
                    raise ValueError("duplicate archive selection")
                selections.add(selection)
                path = Path(source["path"])
                if not path.is_absolute():
                    path = manifest_path.parent / path
                body = _read(path, administration_policy.MAX_METADATA_BYTES)
                digest = _digest(body)
                if source.get("sha256") != digest or source.get("bytes") != len(body):
                    raise ValueError("original metadata differs from manifest hash/size")
                parsed = administration_policy.parse_policy_metadata(body, commit=commit, administration=administration)
                for statement in parsed.records:
                    statement_id = _digest(_json([url, digest, statement.source_record_index]).encode())
                    context = ReceiptContext(
                        generation,
                        statement_id,
                        VERSION,
                        [
                            {
                                "source_id": digest,
                                "sha256": digest,
                                "source_uri": url,
                                "locator": f"record:{statement.source_record_index}",
                            },
                        ],
                    )
                    emit(
                        "administration_policy_statements",
                        {
                            "statement_id": statement_id,
                            "source_record_id": statement.source_id,
                            "archive_commit": commit,
                            "administration": administration,
                            "source_record_index": statement.source_record_index,
                            "congress": statement.congress,
                            "document_title": statement.document_title,
                            "date_issued": statement.date_issued,
                            "date_issued_status": statement.date_issued_status,
                            "rescinded": json.loads(statement.raw_json).get("rescinded"),
                            "archived_pdf_url": statement.archived_pdf_url,
                            "source_uri": url,
                            "source_sha256": digest,
                            "observed_at": source["observed_at"],
                            "source_record_json": statement.raw_json,
                        },
                        context,
                    )
                    for index, (bill, bill_id) in enumerate(zip(statement.bills, statement.bill_ids, strict=True)):
                        # The native token is already validated by SpicyDocs;
                        # conversion uses the maintained application's key owner.
                        match = re.fullmatch(r"([a-z]+)([1-9][0-9]*)", bill)
                        assert match is not None
                        identity = BillIdentity(statement.congress, match[1], int(match[2]))
                        link_id = _digest(_json([statement_id, index]).encode())
                        emit(
                            "administration_policy_bill_links",
                            {
                                "link_id": link_id,
                                "statement_id": statement_id,
                                "source_link_index": index,
                                "congress": statement.congress,
                                "bill": bill,
                                "native_bill_id": bill_id,
                                "bill_id": canonical_bill_id(identity),
                            },
                            ReceiptContext(generation, link_id, VERSION, context.witnesses),
                        )
                inputs.append({**source, "path": str(path.resolve()), "records": len(parsed.records)})
        finally:
            for stream in streams.values():
                stream.close()
        outputs, receipts = {}, []
        for table, policy in POLICIES.items():
            spool = staged / (table + ".jsonl.gz")
            with gzip.open(spool, "rt", encoding="utf-8") as stream:

                def rows():
                    for line in stream:
                        item = json.loads(line)
                        yield item["row"], ReceiptContext(**item["context"])

                subject, receipt = write_dataset(rows(), staged / table, policy)
            outputs[table] = {
                "rows": counts[table],
                "path": str(subject.relative_to(staged)) if subject else None,
                "sha256": _file_digest(subject) if subject else None,
            }
            receipts.append(receipt)
            spool.unlink()
        combined = combine_receipts(receipts, staged / "etl_receipts.parquet")
        if _read(manifest_path, MAX_MANIFEST_BYTES) != raw_manifest or _code_pins() != pins:
            raise ValueError("manifest or processing code changed during build")
        for source in inputs:
            if _digest(_read(Path(source["path"]), administration_policy.MAX_METADATA_BYTES)) != source["sha256"]:
                raise ValueError("original metadata changed during build")
        result = {
            "format": "spicy-retained-collection-build",
            "version": 1,
            "status": "local-candidate-built",
            "generation": generation,
            "manifest_sha256": _digest(raw_manifest),
            "code_pins": pins,
            "inputs": inputs,
            "outputs": outputs,
            "baseline_inputs": [],
            "receipts_sha256": _file_digest(combined),
            "limits": [
                "Community archive assertions; no inferred political position.",
                "Complete selected metadata files; publisher-wide coverage unknown.",
                "PDF links are source declarations, not proof of held/readable PDF bodies.",
                "Local qualification is separate from publication and deployed readback.",
            ],
        }
        (staged / "selection.json").write_bytes(raw_manifest)
        (staged / "build.json").write_text(json.dumps(result, indent=2) + "\n")
        publish_directory_no_replace(staged, output_dir)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = build(args.manifest, args.output)
    print(_json({"generation": result["generation"], "outputs": result["outputs"]}))


if __name__ == "__main__":
    main()
