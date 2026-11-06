"""Government-source adoption of the shared ETL receipt writer and internal reader.

Legacy source shapers and precedence rules run on their literal inputs. The
generation admits the completed typed subjects and receipts once. An incremental
read of a migrated subject requires its matching selected receipts.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import shutil
from contextvars import ContextVar
from collections.abc import Mapping
from dataclasses import replace
from functools import wraps
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spicy_regs import etl_bulk, government_bulk
from spicy_regs.earlier_receipt_policies import earlier_policies
from spicy_regs.etl_receipts import (
    RECEIPT_KEY,
    RECEIPT_SCHEMA,
    DatasetPolicy,
    ReceiptContext,
    combine_receipts,
    failure_receipt,
    observation_receipt,
    read_with_receipts,
    select_receipts,
    split_record,
    subject_identity,
    validate_receipt_bundle,
)
from spicy_regs.native_merge import native_merge
from spicy_regs.receipt_history import RefusalIdentity, carry_receipt_history, crs_revision_identity_migrations
from spicy_regs.selected_generations import SelectedDataset, SelectedInputs
from spicy_regs.sources import publication
from spicy_regs.transforms.government_source_shapes import (
    IDENTITY_FIELDS,
    LEGACY_COLUMNS,
    SUBJECT_SCHEMAS,
    GovernmentShapeError,
    map_subject,
)
from spicy_regs.transforms.parquet_rows import write_rows

#: A dataset whose subject columns changed after it first published states its own version; the rest keep the
#: family's. Columns returned from the receipt to the subject table on 2026-10-05: gao_decisions
#: ``b_numbers_truncated``; gao_recommendations ``first_seen`` and ``last_seen``. Columns added on 2026-10-05 (owner
#: decision 2026-10-04): gao_reports ``major_rule_agency``, ``major_rule_rins`` and ``major_rule_fr_citations``.
#: fcc_proceedings gs/2 (ss-17vd): ``date_nprm``, ``date_reporting_and_order`` and ``rule_section`` added, ECFS fields
#: the July 2026 column list left out by mistake; the four comment-window columns ECFS never filled dropped (owner,
#: 2026-10-31).
POLICY_VERSIONS = {
    "crs_reports": "government-sources/2",
    "fcc_filings": "government-sources/3",
    "fcc_proceedings": "government-sources/2",
    "gao_decisions": "government-sources/3",
    "gao_recommendations": "government-sources/2",
    "gao_reports": "government-sources/4",
    # 2026-10-30 (ss-hqoi): fifteen native filing fields the first mapper dropped. No /1 receipt holds them, so the
    # first /2 generation is a full rebuild from the retained pages, never a window merged onto a /1 prior.
    "lobbying_filings": "government-sources/2",
}
POLICIES = {
    dataset: DatasetPolicy(
        dataset,
        schema,
        tuple(IDENTITY_FIELDS[dataset]),
        ("raw_record",),
        policy_version=POLICY_VERSIONS.get(dataset, "government-sources/1"),
        nullable_identity_fields=("entity_eft_indicator",) if dataset == "sam_entities" else (),
    )
    for dataset, schema in SUBJECT_SCHEMAS.items()
}
REFUSAL_IDENTITIES = {
    dataset: RefusalIdentity(tuple(IDENTITY_FIELDS[dataset]), "raw_record") for dataset in POLICIES
}


def _row_route(route: str, error: etl_bulk.NotBulkEligible, *, dataset: str, rows: int) -> None:
    if dataset in government_bulk.BULK_DATASETS:
        etl_bulk.log_row_fallback(route, error, dataset=dataset, rows=rows)
    else:
        etl_bulk.log_row_eligibility(route, dataset=dataset, rows=rows, reason=str(error))


def _without(dataset: str, *columns: str) -> DatasetPolicy:
    """The dataset's ``government-sources/1`` policy: today's subject schema less the columns returned or added since."""
    schema = POLICIES[dataset].subject_schema
    for column in columns:
        schema = schema.remove(schema.get_field_index(column))
    return replace(POLICIES[dataset], subject_schema=schema, policy_version="government-sources/1")


#: Exact policies a published prior may still carry. A prior is read under one only to restore its receipts' whole
#: original rows; every write uses POLICIES. On 2026-10-04 gao-reports published gao_decisions without
#: ``b_numbers_truncated`` and gao-recommendations published without ``first_seen`` and ``last_seen``; gao-reports
#: published gao_reports to 2026-10-05 without the three major-rule letter columns, which no receipt held either.
#: fcc-proceedings published fcc_proceedings to 2026-10-30 without its three ECFS NPRM/order/rule-section columns and
#: with the four always-NULL comment-window columns.
EARLIER_POLICIES: dict[str, tuple[DatasetPolicy, ...]] = {
    "gao_decisions": (_without("gao_decisions", "b_numbers_truncated"),),
    "gao_recommendations": (_without("gao_recommendations", "first_seen", "last_seen"),),
    "gao_reports": (_without("gao_reports", "major_rule_agency", "major_rule_rins", "major_rule_fr_citations", "citation_product_id"),),
}
# Exact earlier policies restore private source rows without relabeling them.
for _name in ("crs_reports", "gao_decisions", "gao_reports", "fcc_proceedings", "lobbying_filings"):
    EARLIER_POLICIES[_name] = earlier_policies(POLICIES[_name])
_FCC_HISTORY = json.loads(Path(__file__).parents[1].joinpath("navigation_policy_history.json").read_text())
EARLIER_POLICIES["fcc_filings"] = tuple(
    DatasetPolicy.from_descriptor(_FCC_HISTORY[key]) for key in ("fcc_filings", "fcc_filings_v2")
)
_ACTIVE: ContextVar[bool] = ContextVar("government_receipt_build", default=False)
_INHERITED: ContextVar[dict[str, Path] | None] = ContextVar("government_prior_receipts", default=None)
_NATIVE_PRIORS: ContextVar[dict[str, tuple[Path, Path]] | None] = ContextVar("government_native_priors", default=None)
_SELECTED_PRIORS: ContextVar[tuple[SelectedInputs, dict[str, SelectedDataset | None]] | None] = ContextVar(
    "government_selected_priors", default=None)
# This is local build metadata for forwarding the shared API's admission arguments,
# not another receipt format. The receipts themselves use etl_receipts.RECEIPT_SCHEMA.
BUILD_METADATA = ".government-etl-build.json"


def _rows(path: Path):
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _legacy_schema(dataset: str) -> pa.Schema:
    return pa.schema(
        [
            (
                c,
                pa.int64()
                if c in ("recommendation_count", "matters_for_congress_count", "page_count")
                else pa.string(),
            )
            for c in LEGACY_COLUMNS[dataset]
        ]
    )


def _native(path: Path, dataset: str) -> bool:
    schema = pq.read_schema(path)
    return (
        schema.metadata is not None
        and schema.metadata.get(b"government_etl_policy") == b"1"
        or (
            not schema.equals(_legacy_schema(dataset), check_metadata=False)
            and schema.equals(SUBJECT_SCHEMAS[dataset], check_metadata=False)
        )
    )


def _prior_policy(dataset: str, path: Path) -> DatasetPolicy:
    """The earlier policy whose exact subject schema the prior was written under, else the current one."""
    schema = pq.read_schema(path)
    return next(
        (p for p in EARLIER_POLICIES.get(dataset, ()) if schema.equals(p.subject_schema, check_metadata=False)),
        POLICIES[dataset],
    )


def selected_native_prior(dataset: str) -> Path | None:
    """The immutable subject already selected for this native family build."""
    held = _SELECTED_PRIORS.get()
    if held is None:
        return None
    session, selections = held
    session.check()
    selected = selections.get(dataset)
    if selected is None:
        return None
    if len(selected.subjects) != 1:
        raise ValueError(f"{dataset}: government prior requires one subject member")
    return selected.subjects[0]


def internal_prior(
    dataset: str, path: Path, *, receipt_path: Path | None = None, generation_id: str | None = None,
    keep_native: bool = False,
) -> Path:
    """Restore literal processing input from a validated native subject/receipt pair.

    Explicit local receipt selection takes precedence. Otherwise a build metadata
    file must pin the subject bytes, or the captured publication index must pin
    them. Missing, changed, wrong-generation or ambiguous receipts always refuse.
    Legacy source-shaped inputs remain explicitly distinguishable by schema.
    ``keep_native`` admits the selected pair once and leaves it for the builder's
    native merge, avoiding restoration of unchanged rows.
    """
    if not _native(path, dataset):
        if unknown := set(pq.read_schema(path).names) - set(LEGACY_COLUMNS[dataset]):
            raise GovernmentShapeError(f"{dataset}: unclassified prior fields {sorted(unknown)}")
        return path
    scratch = path.parent / f".{path.name}.receipts.parquet"
    selected = path.parent / f".{path.name}.selected-receipts.parquet"
    restored = path.parent / f".{path.name}.processing.parquet"
    held = _SELECTED_PRIORS.get()
    scoped = held[1].get(dataset) if held is not None and receipt_path is None else None
    try:
        if scoped is not None:
            assert held is not None
            held[0].check()
            if scoped.subjects != (path,):
                raise ValueError("Government prior differs from the shared native selection")
            selected, receipt_path, generation_id = scoped.receipts, scoped.receipts, scoped.generation_id
        elif receipt_path is None:
            local = path.parent / BUILD_METADATA
            metadata = json.loads(local.read_text()) if local.exists() else None
            if metadata and metadata.get("subjects", {}).get(dataset, {}).get("sha256") == _digest(path):
                receipt_path = Path(metadata["receipt_path"])
                if _digest(receipt_path) != metadata["receipt_sha256"]:
                    raise ValueError("Local receipt bytes differ from the selected build")
                generation_id = metadata["generation_id"]
            else:
                public_url = os.getenv("R2_PUBLIC_URL")
                if not public_url:
                    raise ValueError(f"{dataset}: native prior requires explicitly selected receipts")
                index = publication.current_index(public_url)
                descriptor = publication.table_descriptor(index, dataset + ".parquet")
                if descriptor is None or descriptor.get("sha256") != _digest(path):
                    raise ValueError("Local native prior differs from the selected publication")
                members = publication.receipt_members(index, dataset=dataset)
                if len(members) != 1:
                    raise ValueError("Expected one selected family receipt member")
                if not publication.fetch_member(public_url, members[0], scratch, dataset + " receipts"):
                    raise ValueError("Selected receipt member is missing")
                receipt_path = scratch
                owner = publication.table_owner(index, dataset + ".parquet")
                assert owner is not None
                generation_id = owner[1]["etlReceipts"]["generationId"]
        if generation_id is None:
            raise ValueError("An explicit receipt generation identity is required")
        if scoped is None:
            select_receipts(receipt_path, selected, dataset=dataset)
        inherited = _INHERITED.get()
        if inherited is not None:
            inherited[dataset] = selected
        policy = _prior_policy(dataset, path)
        if keep_native:
            if policy is not POLICIES[dataset]:
                # A window merged onto an earlier policy's rows would publish them with the current policy's added
                # columns NULL, though no earlier receipt ever held those values. Only a full rebuild moves the policy.
                raise GovernmentShapeError(
                    f"{dataset}: the selected prior was written under {policy.policy_version}, not "
                    f"{POLICIES[dataset].policy_version}; rebuild the family under the current policy before a window")
            native_priors = _NATIVE_PRIORS.get()
            if native_priors is None:
                raise ValueError("Native priors require an active receipt builder")
            validate_receipt_bundle({dataset: [path]}, [selected], [policy], generation_id=generation_id)
            if held is not None:
                held[0].check()
            native_priors[dataset] = (path, selected)
            return path
        try:
            government_bulk.restore_legacy(
                path, selected, restored, dataset=dataset, policy=policy, generation_id=generation_id,
                schema=_legacy_schema(dataset),
            )
        except etl_bulk.NotBulkEligible as error:
            _row_route("prior restore", error, dataset=dataset, rows=pq.ParquetFile(path).metadata.num_rows)
            restored_rows = (
                row["raw_record"] for row in read_with_receipts([path], [selected], policy, generation_id=generation_id)
            )
            write_rows(restored_rows, restored, _legacy_schema(dataset))
        return restored
    finally:
        scratch.unlink(missing_ok=True)
        if scoped is None and _INHERITED.get() is None:
            selected.unlink(missing_ok=True)


def has_native_prior(dataset: str) -> bool:
    """Whether this build retained an admitted pair for a window-only native merge."""
    return dataset in (_NATIVE_PRIORS.get() or {})


def attempt(
    dataset: str, raw: dict, ordinal: int, *, generation_id: str, source_uri: str, digest: str
) -> tuple[dict | None, dict]:
    """One retained producer row's conversion: its subject (None when refused) and its receipt.

    The one step both writers share: ``migrate_outputs`` runs it per row for datasets SQL does not own, and
    ``government_bulk`` for the rows SQL cannot prove. A malformed record is a refused receipt that keeps the whole
    raw row and has no subject.
    """
    policy = POLICIES[dataset]
    witnesses = [
        {
            "source_id": "spicy-regs:producer-output:" + dataset,
            "source_uri": source_uri,
            "sha256": digest,
            "locator": f"row:{ordinal}",
            "body_version": None,
        }
    ]
    mapping_error = None
    subject = None
    try:
        subject = map_subject(dataset, raw, generation_id=generation_id)
        subject_identity(policy, subject)
    except (GovernmentShapeError, ValueError, TypeError) as error:
        mapping_error = error  # Retain the complete failed conversion below.
    # The explicit page digest remains a second source witness. It is
    # not replaced with the current run's time on carried USAspending rows.
    if raw.get("source_capture_sha256") and (
        not isinstance(raw["source_capture_sha256"], str)
        or not re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", raw["source_capture_sha256"])
    ):
        mapping_error = GovernmentShapeError("Invalid retained source capture digest")
    elif raw.get("source_capture_sha256"):
        witnesses.append(
            {
                "source_id": "usaspending:ranking-page",
                "source_uri": None,
                "sha256": raw["source_capture_sha256"],
                "locator": None,
                "body_version": None,
            }
        )
    diagnostics = {"source_qualification": "producer-output; original capture coverage unchanged"}
    if mapping_error is not None or subject is None:
        mapping_error = mapping_error or GovernmentShapeError("No subject")
        context = ReceiptContext(
            generation_id,
            f"{dataset}:{ordinal}",
            policy.policy_version,
            witnesses,
            {**diagnostics, "error_type": type(mapping_error).__name__, "message": str(mapping_error)},
        )
        return None, failure_receipt(policy, context, outcome="refused", raw_fields={"raw_record": raw})
    context = ReceiptContext(generation_id, f"{dataset}:{ordinal}", policy.policy_version, witnesses, diagnostics)
    return split_record(policy, {**subject, "raw_record": raw}, context)


def _write_by_row(retained: Path, subject_path: Path, receipt_path: Path, dataset: str, **context) -> None:
    """The reference writer: ``attempt`` per row; refusals follow the accepted receipts."""
    policy = POLICIES[dataset]
    subjects, receipts, refused = [], [], []
    with (
        pq.ParquetWriter(subject_path, policy.subject_schema, compression="zstd") as subject_writer,
        pq.ParquetWriter(receipt_path, RECEIPT_SCHEMA, compression="zstd") as receipt_writer,
        TemporaryDirectory(dir=receipt_path.parent) as scratch,
        pq.ParquetWriter(Path(scratch) / "refused.parquet", RECEIPT_SCHEMA, compression="zstd") as refused_writer,
    ):
        def flush():
            if subjects:
                subject_writer.write_table(pa.Table.from_pylist(subjects, schema=policy.subject_schema))
            if receipts:
                receipt_writer.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA))
            if refused:
                refused_writer.write_table(pa.Table.from_pylist(refused, schema=RECEIPT_SCHEMA))
            subjects.clear(), receipts.clear(), refused.clear()

        attempted = 0
        for ordinal, raw in enumerate(_rows(retained)):
            attempted += 1
            subject, receipt = attempt(dataset, raw, ordinal, **context)
            if subject is None:
                refused.append(receipt)
            else:
                subjects.append(subject)
                receipts.append(receipt)
            if len(receipts) + len(refused) >= 2000:
                flush()
        if attempted == 0:
            receipts.append(_empty_observation(dataset, context["source_uri"], context["digest"], context["generation_id"]))
        flush()
        refused_writer.close()
        with pq.ParquetFile(Path(scratch) / "refused.parquet") as late:
            for batch in late.iter_batches(batch_size=2000):
                receipt_writer.write_batch(batch)


def _empty_observation(dataset: str, source_uri: str, digest: str, generation_id: str) -> dict:
    policy = POLICIES[dataset]
    return observation_receipt(
        policy,
        ReceiptContext(
            generation_id,
            dataset + ":empty",
            policy.policy_version,
            [
                {
                    "source_id": "spicy-regs:producer-output:" + dataset,
                    "source_uri": source_uri,
                    "sha256": digest,
                    "locator": None,
                    "body_version": None,
                }
            ],
            {"subject_rows": 0},
        ),
        processing_fields={"raw_record": None},
    )


def migrate_outputs(
    paths: tuple[Path, ...], *, generation_id: str | None = None, datasets: tuple[str, ...] | None = None, evidence=None,
    native_orders: Mapping[str, tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    """Convert every family output and retain the exact pre-conversion Parquet.

    A malformed record receives a refused receipt and no subject. Nothing is validated here: the generation's
    admission is the one validation of the new bundle. A row the prior selected receipts already hold unchanged keeps
    its prior receipt verbatim, and a changed one references its direct predecessor (``carry_receipt_history``).
    Pre-conversion inputs stay under .government-inputs.
    """
    if not paths:
        raise ValueError("No government outputs")
    if len({path.parent for path in paths}) != 1:
        raise ValueError("Government family outputs must share a build directory")
    datasets = datasets or tuple(path.stem for path in paths)
    if len(datasets) != len(paths) or len(set(datasets)) != len(datasets):
        raise ValueError("Outputs require distinct declared dataset identities")
    directory = paths[0].parent
    generation_id = generation_id or uuid4().hex
    work = directory / ".government-etl" / generation_id
    if work.exists():
        raise FileExistsError(work)
    work.mkdir(parents=True)
    inputs = directory / ".government-inputs" / generation_id
    inputs.mkdir(parents=True)
    bundles = []
    counts = {}
    for path, dataset in zip(paths, datasets, strict=True):
        policy = POLICIES[dataset]
        retained = inputs / path.name
        # Preserve, do not reclassify, the exact bytes the existing producer wrote.
        shutil.copyfile(path, retained)
        digest = _digest(retained)
        if evidence is not None:
            evidence.retain_file(retained, stage="government-producer-output", dataset=dataset)
        shard = work / dataset
        shard.mkdir()
        subject_path, receipt_path = shard / f"{dataset}.parquet", shard / RECEIPT_KEY
        context = {"generation_id": generation_id, "source_uri": str(retained), "digest": digest}
        rows = pq.ParquetFile(retained).metadata.num_rows
        try:
            government_bulk.write_dataset(
                retained, subject_path, receipt_path, dataset=dataset, policy=policy, generation_id=generation_id,
                source_uri=str(retained), digest=digest,
                last_receipts=[] if rows else [_empty_observation(dataset, str(retained), digest, generation_id)],
                row_attempt=lambda raw, ordinal, dataset=dataset: attempt(dataset, raw, ordinal, **context),
            )
        except etl_bulk.NotBulkEligible as error:
            _row_route("receipt write", error, dataset=dataset, rows=rows)
            _write_by_row(retained, subject_path, receipt_path, dataset, **context)
        with pq.ParquetFile(receipt_path) as receipts:
            refused = sum(pc.call_function("sum", [pc.call_function("equal", [batch.column(0), "refused"])]).as_py() or 0
                          for batch in receipts.iter_batches(columns=["outcome"]))
        if native_prior := (_NATIVE_PRIORS.get() or {}).get(dataset):
            if native_orders is None or dataset not in native_orders:
                raise ValueError(f"{dataset}: retained native prior has no declared output order")
            merged = native_merge(
                policy, fresh=(subject_path, receipt_path), prior=native_prior, order_by=native_orders[dataset],
                subjects=shard / "merged-subjects.parquet", receipts=shard / "merged-receipts.parquet",
                refusal_identity=REFUSAL_IDENTITIES[dataset])
            merged.subjects.replace(subject_path)
            merged.receipts.replace(receipt_path)
        elif prior := (_INHERITED.get() or {}).get(dataset):
            carry_receipt_history(receipt_path, [prior], shard / "carried.parquet",
                                  refusal_identities={dataset: REFUSAL_IDENTITIES[dataset]},
                                  identity_migrations=crs_revision_identity_migrations(
                                      policy, lambda raw: map_subject(dataset, raw)))
            (shard / "carried.parquet").replace(receipt_path)
        subjects = pq.ParquetFile(subject_path).metadata.num_rows
        bundles.append((path, subject_path, receipt_path))
        counts[dataset] = {"inputs": rows, "subjects": subjects, "refused": refused}
    combined = combine_receipts([bundle[2] for bundle in bundles], work / RECEIPT_KEY)
    # A refused conversion is inspectable but never silently replaces a complete
    # producer output with a smaller subject table. Explicit qualification decides
    # whether to admit such a candidate; ordinary ingest refuses the replacement.
    if any(value["refused"] for value in counts.values()):
        (work / "refusal-summary.json").write_text(json.dumps(counts, indent=2) + "\n")
        raise GovernmentShapeError(f"Government conversion refused inputs; preserved candidate at {work}")
    # All prior reads are complete. Refuse mutation before replacing outputs or
    # writing completion metadata that a retry would accept as its local prior.
    if held := _SELECTED_PRIORS.get():
        held[0].check()
    for original, subject, _ in bundles:
        staged = original.with_name("." + original.name + ".native")
        shutil.copyfile(subject, staged)
        staged.replace(original)
    receipt_path = directory / RECEIPT_KEY
    shutil.copyfile(combined, receipt_path)
    metadata = {
        "generation_id": generation_id,
        "receipt_path": str(receipt_path.resolve()),
        "receipt_sha256": _digest(receipt_path),
        "subjects": {
            dataset: {"path": str(p.resolve()), "sha256": _digest(p)}
            for dataset, p in zip(datasets, paths, strict=True)
        },
        "counts": counts,
    }
    (directory / BUILD_METADATA).write_text(json.dumps(metadata, indent=2) + "\n")
    # Final pinned copies own the next read. Keep producer inputs and every
    # incomplete/refused conversion; only this completed work tree is disposable.
    shutil.rmtree(work)
    return metadata


def generation_receipt_args(paths: tuple[Path, ...]) -> dict[str, Any]:
    """Arguments forwarded unchanged to the shared generation writer."""
    directory = paths[0].parent
    metadata = json.loads((directory / BUILD_METADATA).read_text())
    for path in paths:
        if metadata["subjects"][path.stem]["sha256"] != _digest(path):
            raise ValueError("Government subject changed after receipt validation")
    receipts = Path(metadata["receipt_path"])
    if _digest(receipts) != metadata["receipt_sha256"]:
        raise ValueError("Government receipts changed after validation")
    return {
        "receipt_path": receipts,
        "receipt_policies": [POLICIES[path.stem] for path in paths],
        "receipt_generation_id": metadata["generation_id"],
    }


def receipt_builder(function=None, *, output_argument: str | None = None, dataset: str | None = None,
                    native_orders: Mapping[str, tuple[str, ...]] | None = None):
    """Adopt actual producers; nested family builders share the outer conversion."""

    def decorate(builder):
        signature = inspect.signature(builder)

        @wraps(builder)
        def build(*args, **kwargs):
            if _ACTIVE.get():
                return builder(*args, **kwargs)
            generation_id = kwargs.pop("receipt_generation_id", None)
            bound = signature.bind(*args, **kwargs)
            owner = bound.arguments.get("self")
            generation_id = generation_id or getattr(owner, "receipt_generation_id", None)
            evidence = bound.arguments.get("evidence") or getattr(owner, "source_evidence", None)
            token = _ACTIVE.set(True)
            inherited_token = _INHERITED.set({})
            native_token = _NATIVE_PRIORS.set({})
            selection_token = _SELECTED_PRIORS.set(None)
            try:
                # The remote native family shares one immutable receipt member. Legacy
                # inputs and completed local builds keep their existing selection paths.
                prior_datasets = tuple(native_orders or ())
                directory = Path(bound.arguments["output_dir"]) if prior_datasets else None
                public_url = os.getenv("R2_PUBLIC_URL")
                if directory is not None and public_url and not (directory / BUILD_METADATA).exists():
                    index = publication.current_index(public_url)
                    if any((owner := publication.table_owner(index, name + ".parquet")) is not None
                           and "etlReceipts" in owner[1] for name in prior_datasets):
                        session = SelectedInputs(directory, directory / (".selected-priors-" + uuid4().hex),
                                                 index=index, public_url=public_url)
                        _SELECTED_PRIORS.set((session, session.select_scoped(prior_datasets)))
                result = builder(*args, **kwargs)
                if held := _SELECTED_PRIORS.get():
                    held[0].check()
                paths = (
                    (Path(bound.arguments[output_argument]),)
                    if output_argument
                    else result
                    if isinstance(result, tuple)
                    else (result,)
                )
                migrate_outputs(
                    tuple(Path(path) for path in paths),
                    generation_id=generation_id,
                    datasets=(dataset,) if dataset else None,
                    evidence=evidence,
                    native_orders=native_orders,
                )
                return result
            finally:
                _ACTIVE.reset(token)
                _INHERITED.reset(inherited_token)
                _NATIVE_PRIORS.reset(native_token)
                _SELECTED_PRIORS.reset(selection_token)

        return build

    return decorate(function) if function is not None else decorate
