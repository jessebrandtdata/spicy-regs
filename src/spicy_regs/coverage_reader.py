"""Restore private coverage inputs with the maintained selected-prior reader.

This local bridge never publishes files or reconstructs inputs from IDs. Its
caller supplies immutable selected subjects, receipts and generation identity.
"""

from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import inspect
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, receipt_policies
from spicy_regs.legislative_receipts import policy as legislative_policy
from spicy_regs.legislative_documents import field_registry
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors, dataset_policy
from spicy_regs.selected_generations import SelectedDataset, remember_selection


def file_hash(path):
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


PACKAGE_ROOT = Path(inspect.getfile(legislative_policy)).parent


def _admission_files():
    """Shared byte, receipt, policy-history and mutation guards used by every route."""
    root = PACKAGE_ROOT
    return [Path(__file__), *(root / name for name in (
        "etl_receipts.py", "etl_bulk.py", "native_types.py", "parquet_rows.py",
        "transforms/parquet_rows.py", "local_data.py", "runtime_bounds.py",
        "policy_migrations.py", "policy_migrations.json",
    )), *sorted((root / "etl_policies").glob("*.json"))]


RESTORATION_FILES = {
    "congress": ("congress_receipts.py", "congress_subjects.py", "congress_bulk.py", "congress_tables.py",
                 "votes_batch_receipts.py", "receipt_history.py"),
    "legislative": ("legislative_receipts.py", "legislative_documents.py",
                    "legislative_document_fields.json", "navigation_read_outcomes.py"),
    "regulation": ("regulations_bulk.py", "transforms/regulations_receipts.py",
                   "transforms/regulations_shape.py", "schemas/regulations_subjects.py",
                   "sources/regulatory_catalog.py"),
    "fec": ("transforms/fec_identity_receipts.py", "transforms/fec_identity_context_fields.py",
            "transforms/build_fec_committees.py"),
}


def restoration_route(dataset):
    # Match SelectedPriors.get's dispatch; importing a module does not mean every
    # dataset runs its restoration code. Whole-file hashes keep each real owner.
    from spicy_regs.pipelines.rollups import subject_receipts
    if dataset in subject_receipts.CONGRESS:
        return "congress"
    if dataset in field_registry():
        return "legislative"
    if dataset in subject_receipts.REGULATIONS:
        return "regulation"
    if dataset in subject_receipts.FEC:
        return "fec"
    raise ValueError(f"No coordinated processing reader for {dataset}")


def _identity_files(dataset=None):
    root = PACKAGE_ROOT
    routes = RESTORATION_FILES if dataset is None else (restoration_route(dataset),)
    files = _admission_files() + [root / name for name in (
        "pipelines/rollups/subject_receipts.py", "selected_generations.py", "sources/publication.py",
    )]
    files.extend(root / name for route in routes for name in RESTORATION_FILES[route])
    # Citation admission reconstructs native court-opinion document keys.
    if dataset is None or dataset == "document_citations":
        files.append(root / "court_subjects.py")
    return list(dict.fromkeys(files))


def _files_identity(files):
    digest = hashlib.sha256()
    for file in files:
        data = file.read_bytes()
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return "sha256:" + digest.hexdigest()


def admission_identity():
    """Identity for callers that admit directly without SelectedPriors restoration."""
    return _files_identity(_admission_files())


def implementation_identity(dataset=None):
    return _files_identity(_identity_files(dataset))


def checked_member(value):
    if not isinstance(value, dict) or set(value) != {"path", "sha256", "byteSize"}:
        raise ValueError("Coverage input needs an exact local file identity")
    path = Path(value["path"])
    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        raise ValueError("Coverage input must be an existing absolute regular file")
    if type(value["byteSize"]) is not int or path.stat().st_size != value["byteSize"]:
        raise ValueError("Coverage input byte size changed")
    with path.open("rb") as source:
        digest = "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()
    if digest != value["sha256"]:
        raise ValueError("Coverage input SHA-256 changed")
    return path


def _equals(values, expected):
    # Arrow generates its kernels at import time; use its typed entry point. A null never matches.
    matches = pc.call_function("equal", [values, expected])
    return pc.call_function("coalesce", [matches, False])


def current_receipts(path, dataset, policy, *, empty_subjects=False):
    """Admit only receipts of the selected dataset under a current or declared earlier policy version.

    Carried receipts keep the ``policy_version`` and the ``generation_id`` they were written under, so the declared
    earlier versions of ``policy`` (see ``receipt_policies``) are admitted beside the current one, and a receipt
    may name an earlier generation (the shared validator requires only a nonempty generation label). An
    undeclared or null version is refused. What binds the receipts to the selection is the pinned receipt-member
    bytes ``checked_member`` verified; the generation admission re-proves every subject-receipt join. The request's
    ``generationId`` is the caller's selection label, not something these receipts can confirm.
    An explicitly empty legislative selection may have no dataset receipts; its
    restoration still requires the exact retained source-file states.
    """
    versions = pa.array(sorted({item.policy_version for item in receipt_policies(policy)}))
    found = False
    with pq.ParquetFile(path) as source:
        if not source.schema_arrow.equals(RECEIPT_SCHEMA):
            raise ValueError("Unsupported coverage receipt format; refresh current-format data")
        for batch in source.iter_batches(columns=["dataset", "policy_version", "generation_id"], batch_size=65536):
            selected = batch.filter(_equals(batch.column("dataset"), dataset))
            if not selected.num_rows:
                continue
            found = True
            known = pc.call_function("is_in", [selected.column("policy_version")], pc.SetLookupOptions(versions))
            if not pc.call_function("all", [pc.call_function("coalesce", [known, False])]).as_py():
                raise ValueError("Unsupported coverage receipt policy; refresh current-format data")
            ids = selected.column("generation_id")
            if ids.null_count or pc.call_function("any", [pc.call_function("equal", [ids, ""])]).as_py():
                raise ValueError("Coverage receipts lack a generation")
    if not found and not empty_subjects:
        raise ValueError("Selected coverage generation has no receipt context")


REQUEST_FIELDS = frozenset({"dataset", "generationId", "subjects", "receipts", "destination"})


def checked_request(request, policy_for, *, one_subject=False):
    """Admit one selected restoration request: exact shape, byte identities, admitted receipts, new destination."""
    if not isinstance(request, dict) or set(request) != REQUEST_FIELDS:
        raise ValueError("Unsupported coverage restoration request")
    generation = request["generationId"]
    if not isinstance(generation, str) or not generation:
        raise ValueError("Missing coverage receipt generation")
    if not isinstance(request["subjects"], list) or (one_subject and len(request["subjects"]) != 1):
        raise ValueError("Coverage subjects must be explicit")
    destination = Path(request["destination"])
    if not destination.is_absolute() or destination.exists():
        raise ValueError("Coverage destination must be a new absolute directory")
    policy = policy_for(request["dataset"])
    subjects = tuple(checked_member(member) for member in request["subjects"])
    receipts = checked_member(request["receipts"])
    current_receipts(
        receipts, request["dataset"], policy,
        empty_subjects=request["dataset"] in field_registry() and bool(subjects)
        and all(pq.read_metadata(path).num_rows == 0 for path in subjects),
    )
    return policy, subjects, receipts, destination


def restore(request):
    selected_policy, subjects, receipts, destination = checked_request(request, dataset_policy)
    dataset = request["dataset"]
    destination.mkdir(parents=True)
    remember_selection(destination, [SelectedDataset(dataset, subjects, receipts, request["generationId"])])
    selected = SelectedPriors(destination / "selected", root=destination, public_url="")
    restored = selected.get(dataset)
    if restored is None:
        raise ValueError("Selected coverage input is absent")
    members = sorted(restored.rglob("*.parquet")) if restored.is_dir() else [restored]
    schema = None
    rows = 0
    for member in members:
        with pq.ParquetFile(member) as source:
            current = described_schema(source.schema_arrow)
            if schema is not None and schema != current:
                raise ValueError("Restored member schemas differ")
            schema = current
            rows += source.metadata.num_rows
    return {
        "format": "spicygov-private-coverage-input",
        "version": 1,
        "dataset": dataset,
        "generationId": request["generationId"],
        "nativeSchema": [] if selected_policy.receipt_only else described_schema(selected_policy.subject_schema),
        "selection": {"subjects": request["subjects"], "receipts": request["receipts"]},
        "processingSchema": schema,
        "rows": rows,
        "urls": [str(path) for path in members],
        "receiptOnly": selected_policy.receipt_only,
        "policyVersion": selected_policy.policy_version,
        "policy": selected_policy.descriptor(),
        "processingMembers": [
            {"path": str(path), "sha256": file_hash(path), "byteSize": path.stat().st_size} for path in members
        ],
        "implementationSha256": implementation_identity(dataset),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema")
    args = parser.parse_args()
    if args.schema:
        selected_policy = dataset_policy(args.schema)
        print(
            json.dumps(
                {
                    "nativeSchema": []
                    if selected_policy.receipt_only
                    else described_schema(selected_policy.subject_schema),
                    "receiptOnly": selected_policy.receipt_only,
                    "policyVersion": selected_policy.policy_version,
                    "policy": selected_policy.descriptor(),
                    "implementationSha256": implementation_identity(args.schema),
                }
            )
        )
    else:
        print(json.dumps(restore(json.load(sys.stdin))))


if __name__ == "__main__":
    main()
