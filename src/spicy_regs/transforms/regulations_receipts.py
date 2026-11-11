"""Regulations-family writers and qualified internal reads using shared receipts.

The local producer seals native subject files and the one
shared receipt member together. It never updates a remote pointer or catalog.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from hashlib import file_digest, sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_SCHEMA, open_receipt_writer,
    combine_receipts,
    failure_receipt,
    read_with_receipts,
    observation_receipt,
    read_attempts,
    exact_json,
    select_receipts,
    write_dataset,
)
from spicy_regs.transforms.regulations_shape import (
    IDENTITIES,
    SOURCE_COLUMNS,
    RECEIPT_COLUMNS,
    TYPES,
    shape_record,
    shape_records,
    subject_schema,
)

# Datasets whose retained processing input restores through the private batch reader.
RESTORE_DATASETS = frozenset({"dockets", "documents"})
#: Datasets whose restore keeps the captured source schema: the batch-restored ones and the typed attribute tables.
ATTRIBUTE_DATASETS = frozenset({"comment_attributes", "docket_attributes", "document_attributes"})
SOURCE_SCHEMA_DATASETS = RESTORE_DATASETS | ATTRIBUTE_DATASETS
#: Datasets the private batch reader restores: the retained-input ones and the typed attribute tables.
BATCH_RESTORE_DATASETS = RESTORE_DATASETS | ATTRIBUTE_DATASETS
#: Datasets whose writer keeps unchanged prior receipts exactly and gives a changed row only a predecessor
#: reference (``carry_receipt_history``). Comments keeps its own catalog rules and inherits nothing.
CARRY_DATASETS = frozenset(IDENTITIES) - {"comments"}
#: Aggregates their scheduled rollups rebuild whole from published parents and publish without receipts (ss-nxew).
#: The generation pins each parent it read (``spec.parents``); a row receipt would hold the row again and witness
#: only that copy. No policy is installed for them, so admission asks for no receipt member.
#: ``policy(name)`` and ``build_native_rollup`` below still build receipts for the four; only tests call that path.
PARENT_PINNED_SUMMARIES = frozenset({"agency_monthly_volume", "agency_stats", "discovery_signals", "feed_summary"})

NULLABLE_IDENTITIES = {
    "agency_lifecycle_stats": ("agency_code",),
    "agency_monthly_volume": ("agency_code", "document_type"),
    "comments_index": ("agency_code", "docket_id", "year", "month"),
    # The Register's regulations.gov statement has no printed-array position.
    "fr_docket_links": ("docket_source_ordinal",),
}


@cache
def policy(dataset: str) -> DatasetPolicy:
    return DatasetPolicy(
        dataset,
        subject_schema(dataset),
        IDENTITIES[dataset],
        (*RECEIPT_COLUMNS[dataset], "raw_conversion_inputs", "input_metadata", "raw_source_record"),
        policy_version=(
            "regulations-native-v3" if dataset == "federal_register" else
            "regulations-native-v2" if dataset in {"comment_periods", "lifecycle_events", "comments", "documents", "agenda_item_proceedings", "rule_targets", "proceedings", "regulatory_agenda_items", "unified_agenda", "fr_docket_links"} else "regulations-native-v1"),
        nullable_identity_fields=NULLABLE_IDENTITIES.get(dataset, ()),
    )


def refusal_identity(dataset):
    from spicy_regs.receipt_history import RefusalIdentity
    return RefusalIdentity(IDENTITIES[dataset], "raw_conversion_inputs")


def carry_regulatory_receipts(dataset, receipts, priors, *, resolution_receipts: Path | None = None):
    """Carry a selected dataset's immutable history; keep other family receipts outside this pair."""
    from spicy_regs import etl_bulk
    from spicy_regs.receipt_history import carry_receipt_history
    if not priors:
        return
    try:
        with TemporaryDirectory(prefix='.regulatory-carry-', dir=receipts.parent) as temporary:
            work = Path(temporary)
            scoped = [select_receipts(path, work / f'prior-{i}.parquet', dataset=dataset)
                      for i, path in enumerate(priors)]
            carried = carry_receipt_history(receipts, scoped, work / 'carried.parquet',
                                           refusal_identities={dataset: refusal_identity(dataset)},
                                           resolution_receipts=resolution_receipts)
            carried.replace(receipts)
    except (etl_bulk.duckdb.Error, pa.ArrowException, ValueError, TypeError, OverflowError) as error:
        raise etl_bulk.bulk_failure('Regulatory receipt carry failed', error) from error


@dataclass(frozen=True)
class ReceiptInput:
    """One selected dataset, its exact generation and its shared receipt member."""

    dataset: str
    subjects: tuple[Path, ...]
    receipts: Path
    generation_id: str

    def __post_init__(self):
        policy(self.dataset)
        if not self.subjects or not self.generation_id:
            raise ValueError("Dataset subjects and selected generation are required")


def _qualified_rows(selected: ReceiptInput, *, scoped: bool = False) -> Iterable[dict]:
    """Validate the whole selected dataset before reconstructing processing input.

    Absence, ambiguity, a different subject version or a different generation
    refuses; no public projection can substitute for missing retry evidence.
    ``scoped`` says ``selected.receipts`` is already this dataset's ``select_receipts`` output.
    """
    from spicy_regs.etl_receipts import selected_subject_policy
    admitted = selected_subject_policy(policy(selected.dataset), selected.subjects)
    with TemporaryDirectory(prefix="regulations-receipt-read-") as temporary:
        receipts = selected.receipts if scoped else select_receipts(
            selected.receipts, Path(temporary) / "receipts.parquet", dataset=selected.dataset)
        # Complete input consistency validation precedes any processor yield.
        # This stays bounded and catches a bad later row before an external fetch.
        for row in read_with_receipts(
            selected.subjects, [receipts], admitted, generation_id=selected.generation_id
        ):
            _processor_input(selected.dataset, row)
        yield from read_with_receipts(
            selected.subjects, [receipts], admitted, generation_id=selected.generation_id
        )


def _comments_processor_input(original):
    """Replay the maintained API projection or flat catalog normalization."""
    if "data" in original:
        return _source_record_projection("comments")(original)
    from spicy_regs.sources.regulatory_catalog import normalize_source_record
    return normalize_source_record("comments", original)


#: Processor of the comments receipts that ``scripts/prepare_comments_selected_export.py`` wrote and the
#: 2026-11-08 catalog rebuild (``scripts/rebuild_native_catalog.py``, ticket ss-zyr1) copied into the live catalog.
COMMENTS_PROMOTION_PROCESSOR = "comments-selected-policy-promotion/3"


def comments_promotion_input(row: Mapping, processor: str | None) -> dict | None:
    """The processor input of a carried-over comments promotion receipt, or None for any other receipt.

    Why: promotion receipts carry ``processing_json={}``; their originals stay in the pinned v1 pair, not in the
    receipt. Without this rule every catalog read that replays such a row (nightly merges, ``replace_native``
    and its readback, the rebuild's processing smoke) refuses, and published comments stay frozen at 2026-10-10.
    The rule: for that one processor, the input is the selected native subject converted back to
    ``SOURCE_COLUMNS`` (``attachments`` re-encoded as ``attachments_json``; ``comment_text`` is derived).
    What proves it: before replacing, the rebuild proved every one of its 26,418,078 rows equal, on all 21 shared
    columns, to the prior catalog, whose receipts held the per-row input; callers still require ``shape_record``
    of this input to reproduce the subject exactly. It is canonical, not the original bytes: JSON spelling and
    absent-versus-null attachment keys stay only in the v1 pair.
    Lifetime: a permanent read rule for every row nothing rewrites. Nightly merges replace only keys whose
    ``modify_date`` is newer, so most rows keep this receipt indefinitely; only a one-time rewrite giving every
    promotion row a normal receipt would retire the rule.
    Maintenance rewrites (comment fields, comment text, prior repairs, PDF/text backfills) build their source from
    ``processing_table``. For a promotion row that source is this derived row, and ``replace_native`` stores it as
    the new ``regulatory-catalog-native-v1`` receipt's ``raw_source_record`` without saying it was derived. Only the
    receipt history (``prior_receipts`` names this processor) shows it; ``_stage`` cannot mark it, because a
    nightly merge over the same prior receipt stages genuinely fresh source rows.
    Readers without the receipt's processor (``read_internal``, ``materialize_internal``, ``_processor_inputs``
    over a published pair) still refuse these rows.
    ``row`` is the subject plus that receipt's processing fields. Any other processor, or a receipt with any
    processing field, returns None, so other missing input still refuses.
    """
    if processor != COMMENTS_PROMOTION_PROCESSOR or any(name in row for name in policy("comments").receipt_fields):
        return None
    attachments = row["attachments"]
    encoded = None if attachments is None else json.dumps(attachments, ensure_ascii=False, separators=(",", ":"))
    return {name: encoded if name == "attachments_json" else row[name] for name, _ in SOURCE_COLUMNS["comments"]}


def _retained_processor_input(dataset, row, *, processor=None):
    """Historical normalized input, Comments' one retained original row, or its promotion-receipt input.

    ``processor`` is the receipt's processor when the caller has it; without it a promotion receipt refuses.
    """
    value = row.get("raw_conversion_inputs")
    if dataset == "comments" and not isinstance(value, Mapping):
        if isinstance(row.get("raw_source_record"), Mapping):
            value = _comments_processor_input(row["raw_source_record"])
        else:
            value = comments_promotion_input(row, processor)
    if not isinstance(value, Mapping):
        raise ValueError(f"{dataset}: exact retained processor input is required")
    return dict(value)


def _processor_input(dataset, row, *, processor=None):
    value = _retained_processor_input(dataset, row, processor=processor)
    shaped = shape_record(dataset, value)
    schema = subject_schema(dataset)
    reproduced = pa.Table.from_pylist([shaped], schema=schema).to_pylist()[0]
    # A pair selected under an earlier policy holds that policy's columns. One promoted since under a new name
    # (a list its receipt held as JSON text) has no selected value to compare: it is reproduced from the retained
    # input alone. A pair under the current policy holds every column, so nothing is left out for it.
    held = [name for name in schema.names if name in row]
    if exact_json({name: reproduced[name] for name in held}) != exact_json({name: row[name] for name in held}):
        raise ValueError(f"{dataset}: retained processor input differs from selected native subject")
    if any(exact_json(shaped.get(name)) != exact_json(row.get(name)) for name in RECEIPT_COLUMNS[dataset]):
        raise ValueError(f"{dataset}: retained processor input differs from selected processing evidence")
    return dict(value)


def _batch_equality_type(dtype):
    """Types whose Arrow equality proves equality of normalized exact values."""
    if pa.types.is_list(dtype):
        return _batch_equality_type(dtype.value_type)
    if pa.types.is_struct(dtype):
        return all(_batch_equality_type(field.type) for field in dtype)
    # Floating equality merges signed zero; unproven types use exact_json.
    return (pa.types.is_string(dtype) or pa.types.is_integer(dtype) or
            pa.types.is_boolean(dtype) or pa.types.is_date32(dtype))


def _processor_inputs(dataset, rows):
    """Prove a bounded admitted batch and return every exact retained source row.

    The row function decides an unproven batch, preserving its errors. Receipt
    admission belongs to the caller's maintained ``read_with_receipts`` read.
    """
    rows = list(rows)
    schema = subject_schema(dataset)
    if not all(_batch_equality_type(field.type) for field in schema):
        return [_processor_input(dataset, row) for row in rows]
    try:
        raw = [_retained_processor_input(dataset, row) for row in rows]
        if any(not isinstance(value, Mapping) for value in raw):
            raise ValueError("Exact retained processor input is required")
        shaped = shape_records(dataset, raw)
        reproduced = pa.Table.from_pylist(shaped, schema=schema)
        selected = pa.Table.from_pylist(rows, schema=schema)
        if not reproduced.equals(selected):
            raise ValueError("Retained processor input differs from selected native subject")
        names = RECEIPT_COLUMNS[dataset]
        if any(exact_json({name: value.get(name) for name in names}) !=
               exact_json({name: row.get(name) for name in names})
               for value, row in zip(shaped, rows)):
            raise ValueError("Retained processor input differs from selected processing evidence")
    except (ValueError, TypeError, OverflowError, RecursionError, pa.ArrowException):
        return [_processor_input(dataset, row) for row in rows]
    return [dict(value) for value in raw]


def read_internal(selected: ReceiptInput) -> Iterable[dict]:
    """Read exact retained processor inputs after validating the selected dataset."""
    for row in _qualified_rows(selected):
        yield _processor_input(selected.dataset, row)


def processing_schema(dataset, *, metadata=None, source_schema=None):
    """The maintained restored Arrow schema, including the original field preservation rules."""
    if source_schema is not None:
        known = dict(SOURCE_COLUMNS[dataset])
        if (dataset not in SOURCE_SCHEMA_DATASETS
                or len(set(source_schema.names)) != len(source_schema.names)
                or any(field.name not in known or field.type != TYPES[known[field.name]] for field in source_schema)):
            raise ValueError("Exact regulatory source schema differs from the declared input fields")
    schema = (source_schema.remove_metadata() if source_schema is not None else
              pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[dataset]]))
    return schema.with_metadata(metadata) if metadata else schema


def materialize_internal(selected: ReceiptInput, destination: Path, *, bulk: bool = False,
                         source_schema: pa.Schema | None = None, scoped: bool = False) -> Path:
    """Bounded exact retained processor inputs, with qualified file metadata.

    ``scoped=True`` says ``selected.receipts`` is already this dataset's ``select_receipts`` output,
    so neither route selects it again. Either route still admits the pair completely.

    File-level placement/aggregation metadata is restored only when every row's
    receipt agrees. This keeps CFR incremental placement from becoming a blind
    full re-read after the split, and never promotes one row's marker to a file.
    Dockets, documents, Federal Register and its docket links use a private batch
    proof before exposing the file;
    ``bulk=False`` retains the complete row reader as the reference authority.
    A DuckDB or Arrow failure in the batch proof is raised, never retried by row.
    """
    from spicy_regs.transforms.parquet_rows import write_rows
    processing_schema(selected.dataset, source_schema=source_schema)
    from spicy_regs import etl_bulk, regulations_bulk
    if bulk and selected.dataset in BATCH_RESTORE_DATASETS:
        if all(pq.read_schema(path).equals(subject_schema(selected.dataset)) for path in selected.subjects):
            try:
                return regulations_bulk.materialize_internal(
                    selected, destination, source_schema=source_schema, scoped=scoped)
            except etl_bulk.NotBulkEligible as error:
                etl_bulk.log_row_fallback("regulatory restore", error, dataset=selected.dataset,
                                          rows=sum(pq.ParquetFile(path).metadata.num_rows for path in selected.subjects))
        else:
            etl_bulk.log_row_fallback("regulatory restore", etl_bulk.NotBulkEligible("subject-schema-differs"),
                                      dataset=selected.dataset,
                                      rows=sum(pq.ParquetFile(path).metadata.num_rows for path in selected.subjects))
    elif bulk:
        etl_bulk.log_row_eligibility("regulatory restore", dataset=selected.dataset,
                                    rows=sum(pq.ParquetFile(path).metadata.num_rows for path in selected.subjects),
                                    reason="no bulk mapping")

    rows = iter(_qualified_rows(selected, scoped=scoped))
    first = next(rows, None)
    metadata = first.get("input_metadata", {}) if first is not None else {}
    if first is None:
        with TemporaryDirectory(prefix="regulations-empty-metadata-") as temporary:
            receipts = selected.receipts if scoped else select_receipts(
                selected.receipts, Path(temporary) / "receipts.parquet", dataset=selected.dataset)
            observations = list(
                read_attempts(
                    [receipts],
                    policy(selected.dataset),
                    generation_id=selected.generation_id,
                    outcomes=frozenset({"observed"}),
                )
            )
            held = [
                a["processing_fields"]["input_metadata"]
                for a in observations
                if a["diagnostics"].get("kind") == "input_file_metadata"
            ]
            if held and any(value != held[0] for value in held):
                raise ValueError("Empty input metadata differs across selected receipts")
            metadata = held[0] if held else {}
    schema = processing_schema(selected.dataset, source_schema=source_schema, metadata=metadata)

    def original(row):
        raw = _processor_input(selected.dataset, row)
        if source_schema is not None and set(raw) != set(source_schema.names):
            raise ValueError("Exact regulatory source field presence differs from retained input")
        return raw

    def restored():
        if first is not None:
            yield original(first)
        for row in rows:
            if row.get("input_metadata", {}) != metadata:
                raise ValueError("Input metadata differs across selected receipts")
            yield original(row)

    return write_rows(restored(), destination, schema)


def _shape_regulations_attempt(dataset: str, row: Mapping, context: ReceiptContext, *,
                            project: Callable[[Mapping], Mapping] | None = None,
                            input_metadata: Mapping | None = None):
    """Shape one exact source attempt using the row writer's refusal and diagnostic rules."""
    declared = policy(dataset)
    try:
        shaped = shape_record(dataset, project(row) if project is not None else row)
        if project is not None:
            if dataset == "comments":
                converted = shaped["raw_source_record"]
                try:
                    reproduced = _comments_processor_input(row)
                except (ValueError, TypeError, OverflowError, pa.ArrowException):
                    reproduced = None
                if exact_json(reproduced) != exact_json(converted):
                    # A custom projection is actual processing input when the
                    # maintained mapper cannot reproduce it from the original.
                    shaped["raw_conversion_inputs"] = converted
            shaped["raw_source_record"] = dict(row)
        if any(shaped.get(key) is None for key in declared.identity_fields
               if key not in declared.nullable_identity_fields):
            raise ValueError(f"{dataset}: missing required subject identity")
    except (ValueError, TypeError, pa.ArrowException) as error:
        failed = ReceiptContext(context.generation_id, context.attempt_id, context.processor, context.witnesses,
                                {**context.diagnostics, "error_type": type(error).__name__, "error": str(error)})
        return None, failure_receipt(declared, failed, outcome="refused", raw_fields={
            "raw_source_record" if dataset == "comments" else "raw_conversion_inputs": dict(row),
            "input_metadata": dict(input_metadata or {})})
    shaped["input_metadata"] = dict(input_metadata or {})
    return shaped, None


def map_regulations_attempt(dataset: str, row: Mapping, context: ReceiptContext, *,
                            project: Callable[[Mapping], Mapping] | None = None,
                            input_metadata: Mapping | None = None):
    """One row attempt for bounded bulk fallbacks, with the exact writer classification."""
    from spicy_regs.etl_receipts import split_record
    shaped, failure = _shape_regulations_attempt(dataset, row, context, project=project, input_metadata=input_metadata)
    return (None, failure) if shaped is None else split_record(policy(dataset), shaped, context)


def write_records(
    dataset: str,
    records: Iterable[tuple[Mapping, ReceiptContext]],
    destination: Path,
    *,
    input_metadata: Mapping | None = None,
    project: Callable[[Mapping], Mapping] | None = None,
    observation_context: ReceiptContext | None = None,
    prior_receipts: Sequence[Path] = (),
    additional_failures: Iterable[dict] = (),
    resolution_receipts: Path | None = None,
    validate: bool = True,
) -> tuple[Path, Path]:
    """Write one dataset and preserve malformed-input attempts without subjects.

    Unknown or invalid source values remain in a refused receipt with their full
    source row. The exception class and reason are diagnostic fields. Successful
    rows require a stable subject identity and exactly one matching receipt.
    Already qualified current failed attempts can follow the mapped attempts
    through ``additional_failures``; they are not prior history to resolve.
    ``resolution_receipts`` restricts historical refusal resolution to records
    with accepted attempts in that qualified input.
    """
    declared = policy(dataset)
    carry = dataset in CARRY_DATASETS and bool(prior_receipts)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-refusals-", dir=destination.parent) as temp:
        refusal_path = Path(temp) / "refused.parquet"
        writer = open_receipt_writer(refusal_path)
        refused = []

        def flush():
            if refused:
                writer.write_table(pa.Table.from_pylist(refused, schema=RECEIPT_SCHEMA))
                refused.clear()

        def mapped():
            try:
                for row, context in records:
                    shaped, receipt = _shape_regulations_attempt(dataset, row, context, project=project,
                                                               input_metadata=input_metadata)
                    if shaped is None:
                        refused.append(receipt)
                        if len(refused) >= 2000:
                            flush()
                        continue
                    yield shaped, context
            finally:
                flush()
                writer.close()

        def failures():
            if observation_context is not None:
                yield observation_receipt(
                    declared, observation_context, processing_fields={"input_metadata": dict(input_metadata or {})}
                )
            for batch in pq.ParquetFile(refusal_path).iter_batches(batch_size=2000):
                yield from batch.to_pylist()
            yield from additional_failures

        try:
            target = Path(temp) / 'bundle' if carry else destination
            subject, receipts = write_dataset(mapped(), target, declared, failures=failures(), validate=validate and not carry)
            assert subject is not None
            if carry:
                from rulespec_artifacts import publish_directory_no_replace
                from spicy_regs.etl_receipts import validate_receipt_bundle
                carry_regulatory_receipts(dataset, receipts, prior_receipts,
                                          resolution_receipts=resolution_receipts)
                if validate:
                    validate_receipt_bundle({dataset: [subject]}, [receipts], [declared])
                publish_directory_no_replace(target, destination)
                subject, receipts = destination / subject.name, destination / receipts.name
        finally:
            writer.close()
        assert subject is not None
        return subject, receipts


def write_held_dataset(
    dataset: str,
    source: Path,
    destination: Path,
    *,
    generation_id: str,
    processor: str = "spicy-regs:regulations-native-v1",
    witnesses: Sequence[Mapping] = (),
    include_source_witness: bool = True,
    prior_receipts: Sequence[Path] = (),
    bulk: bool = False,
    validate: bool = True,
) -> tuple[Path, Path]:
    """Convert retained rows with witnesses to their exact receipt-held input.

    Existing source metadata (placement rules, omitted dates, evaluation clock)
    goes into each receipt. Neither source bytes nor their metadata are deleted.
    Dockets and documents can batch declared text inputs; ``bulk=False`` uses
    the reference writer. Unsupported batch values retain its exact decisions.
    ``validate=False`` leaves the one validation to the caller's generation admission.
    A DuckDB or Arrow failure in the batch writer is raised, never retried by row.
    """
    from spicy_regs import etl_bulk, regulations_bulk
    if bulk and dataset in regulations_bulk.DATASETS:
        try:
            return regulations_bulk.write_held_dataset(
                dataset, source, destination, generation_id=generation_id, processor=processor,
                witnesses=witnesses, include_source_witness=include_source_witness, prior_receipts=prior_receipts,
                validate=validate,
            )
        except etl_bulk.NotBulkEligible as error:
            etl_bulk.log_row_fallback("regulatory write", error, dataset=dataset,
                                      rows=pq.ParquetFile(source).metadata.num_rows)
    elif bulk:
        etl_bulk.log_row_eligibility("regulatory write", dataset=dataset,
                                    rows=pq.ParquetFile(source).metadata.num_rows, reason="no bulk mapping")
    parquet = pq.ParquetFile(source)
    metadata = dict(parquet.schema_arrow.metadata or {})
    # The exact input metadata is receipt data, including byte-valued keys.
    metadata = {key.decode("utf-8"): value for key, value in metadata.items()}

    def records():
        ordinal = 0
        for batch in parquet.iter_batches(batch_size=2000):
            for row in batch.to_pylist():
                witness = {
                    "source_id": f"retained:{dataset}",
                    "source_uri": None,
                    "sha256": sha256(exact_json(row).encode()).hexdigest(),
                    "locator": ("receipt.values.raw_source_record (canonical exact_json)" if dataset == "comments" else
                                "receipt.values.raw_conversion_inputs (canonical exact_json)"),
                    "body_version": None,
                }
                context = ReceiptContext(
                    generation_id,
                    f"{dataset}:row:{ordinal}",
                    processor,
                    ([witness] if include_source_witness else []) + list(witnesses),
                    {"output_row_ordinal": ordinal} if not include_source_witness else {},
                )
                yield row, context
                ordinal += 1

    input_witness = {
        "source_id": f"retained:{dataset}",
        "source_uri": None,
        "sha256": sha256(exact_json(metadata).encode()).hexdigest(),
        "locator": "receipt.values.input_metadata (canonical exact_json)",
        "body_version": None,
    }
    observed = ReceiptContext(
        generation_id,
        f"{dataset}:input-file",
        processor,
        ([input_witness] if include_source_witness else []) + list(witnesses),
        {"kind": "input_file_metadata", "rows": parquet.metadata.num_rows},
    )
    return write_records(dataset, records(), destination, input_metadata=metadata, observation_context=observed,
                         prior_receipts=prior_receipts, validate=validate)


def build_local_generation(
    sources: Mapping[str, Path],
    destination: Path,
    *,
    generation_id: str,
    family: str = "regulations",
    publication_status: str = "local-partial",
    witnesses: Sequence[Mapping] = (),
    include_source_witness: bool = True,
    prior_receipts: Mapping[str, Sequence[Path]] | None = None,
):
    """Replay explicitly selected held datasets and admit their complete row joins.

    Local-partial is deliberate: retained input alone does not prove complete
    source coverage. Publishers must explicitly declare and qualify a full family.
    """
    from spicy_regs.generations import build_generation

    if not sources or not set(sources) <= set(SOURCE_COLUMNS):
        raise ValueError("Explicit assigned regulations datasets are required")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-native-", dir=destination.parent) as temp:
        work = Path(temp)
        subjects = []
        receipts = []
        for name, source in sources.items():
            subject, receipt = write_held_dataset(
                name,
                source,
                work / name,
                generation_id=generation_id,
                witnesses=witnesses,
                include_source_witness=include_source_witness,
                prior_receipts=(prior_receipts or {}).get(name, ()),
                bulk=True,
                validate=False,  # build_generation below admits the pair
            )
            subjects.append(subject)
            receipts.append(receipt)
        shared = combine_receipts(receipts, work / "etl_receipts.parquet")
        artifact = build_generation(
            work / "generation",
            family=family,
            files=subjects,
            expected_keys=[p.name for p in subjects],
            publication_status=publication_status,
            receipt_path=shared,
            receipt_policies=[policy(n) for n in sources],
            receipt_generation_id=generation_id,
        )
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(work / "generation", destination)
        return artifact


def build_from_receipts(
    inputs: Sequence[ReceiptInput],
    destination: Path,
    *,
    generation_id: str,
    outputs: Sequence[str],
    builder: Callable[[Path], object],
    family: str = "regulations",
):
    """Run an existing producer over receipt-qualified internal inputs, then seal outputs.

    This is an explicit producer entry, not a serving-time cleanup. Failed joins
    refuse before the builder runs. The builder must perform local work only.
    """
    if len({i.dataset for i in inputs}) != len(inputs):
        raise ValueError("Repeated input dataset")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-producer-", dir=destination.parent) as temp:
        work = Path(temp)
        for selected in inputs:
            materialize_internal(selected, work / f"{selected.dataset}.parquet")
        builder(work)
        sources = {name: work / f"{name}.parquet" for name in outputs}
        if any(not p.is_file() for p in sources.values()):
            raise ValueError("Producer did not complete every declared output")
        witnesses = []
        for selected in inputs:
            for source in (*selected.subjects, selected.receipts):
                with source.open("rb") as body:
                    digest = file_digest(body, "sha256").hexdigest()
                witnesses.append(
                    {
                        "source_id": f"{selected.dataset}:{source.name}",
                        "source_uri": None,
                        "sha256": digest,
                        "locator": None,
                        "body_version": selected.generation_id,
                    }
                )
        return build_local_generation(
            sources,
            destination,
            generation_id=generation_id,
            family=family,
            witnesses=witnesses,
            include_source_witness=False,
            prior_receipts={name: [i.receipts for i in inputs if i.dataset == name] for name in outputs},
        )


@cache
def _source_record_projection(dataset: str):
    """The same Regulations.gov mapper for source writing and input replay."""
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.transforms.regulations_attributes import _projection_and_digest

    def source_attributes(payload):
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
            raise ValueError("Source data must be an object")
        data = payload["data"]
        attrs = data.get("attributes")
        if attrs is not None and not isinstance(attrs, Mapping):
            raise ValueError("Source attributes must be an object or null")
        return data, attrs or {}

    if dataset in ("docket_attributes", "document_attributes", "comment_attributes"):
        project, _ = _projection_and_digest(dataset)

        def mapper(payload):
            data, attrs = source_attributes(payload)
            return project(data.get("id"), attrs)
    elif dataset in RECORD_TYPES:
        extract = RECORD_TYPES[dataset].extract

        def formats(value):
            if value is None:
                return None
            if not isinstance(value, list):
                raise ValueError("Native fileFormats must be a list or null")
            if any(f is not None and not isinstance(f, Mapping) for f in value):
                raise ValueError("Source file format must be an object or null")
            return [
                None if f is None else {"url": f.get("fileUrl"), "format": f.get("format"), "size": f.get("size")}
                for f in value
            ]

        def mapper(payload):
            data, attrs = source_attributes(payload)
            # The old compact attachment shaper discards null/empty entries.
            # Shape scalar fields with an empty attachment input, then supply
            # the literal attachment structure below, preserving its positions.
            prepared = {**payload, "data": {**data, "attributes": dict(attrs)}}
            if dataset == "documents":
                prepared["data"]["attributes"]["fileFormats"] = []
            elif dataset == "comments":
                prepared["included"] = []
            row = extract(prepared)
            if dataset == "documents":
                if "fileFormats" in attrs:
                    row["attachments_json"] = json.dumps(formats(attrs["fileFormats"]))
                if "additionalRins" in attrs:
                    row["additional_rins"] = json.dumps(attrs["additionalRins"])
            elif dataset == "comments" and "included" in payload:
                included = payload["included"]
                if included is not None and not isinstance(included, list):
                    raise ValueError("Native included attachment records must be a list or null")
                attachments = []
                for item in included or ():
                    if not isinstance(item, Mapping):
                        raise ValueError("Invalid included record")
                    if item.get("type") != "attachments":
                        continue
                    attributes = item.get("attributes") or {}
                    attachments.append(
                        {
                            "attachment_id": item.get("id"),
                            "title": attributes.get("title"),
                            "formats": formats(attributes.get("fileFormats")),
                            "restrict_reason": attributes.get("restrictReason"),
                            "restrict_reason_type": attributes.get("restrictReasonType"),
                        }
                    )
                row["attachments_json"] = json.dumps(None if included is None else attachments)
            return row
    else:
        raise ValueError("Source staging supports only Regulations.gov base and attribute datasets")
    return mapper


def write_source_records(dataset: str, records: Iterable[tuple[Mapping, ReceiptContext]], destination: Path):
    """Stage captured Regulations.gov records with pinned context and refusals.

    Comments retains the original API record once and replays this same mapper.
    Empty/null attachment lists remain distinct. No catalog switch occurs here.
    """
    return write_records(dataset, records, destination, project=_source_record_projection(dataset))


def build_native_rollup(name: str, inputs: Sequence[ReceiptInput], destination: Path, *, generation_id: str):
    """Run an existing local aggregate/link producer over qualified native inputs."""
    from spicy_regs.transforms.build_agency_stats import build_agency_stats
    from spicy_regs.transforms.build_agency_monthly_volume import build_agency_monthly_volume
    from spicy_regs.transforms.build_discovery_signals import build_discovery_signals
    from spicy_regs.transforms.build_feed_summary import build_feed_summary

    def comment_index(work):
        import duckdb
        from spicy_regs.sources.iceberg import _build_comments_index
        from spicy_regs.schemas.regulations import COMMENT

        with duckdb.connect() as connection:
            connection.from_parquet(str(work / "comments.parquet")).create_view("native_comments_input")
            return _build_comments_index(connection, COMMENT, work, source_sql="SELECT * FROM native_comments_input")

    operations = {
        "comments_index": (comment_index, {"comments"}, {"comments"}),
        "agency_stats": (build_agency_stats, {"dockets"}, {"dockets", "documents", "comments", "comments_index"}),
        "agency_monthly_volume": (build_agency_monthly_volume, {"documents"}, {"documents"}),
        "discovery_signals": (build_discovery_signals, {"documents"}, {"documents"}),
        "feed_summary": (build_feed_summary, {"dockets"}, {"dockets", "documents", "comments", "comments_index"}),
    }
    operation, required, allowed = operations[name]
    present = {i.dataset for i in inputs}
    if not required <= present <= allowed:
        raise ValueError("Rollup inputs differ from the explicitly supported datasets")
    return build_from_receipts(
        inputs,
        destination,
        generation_id=generation_id,
        outputs=[name],
        builder=operation,
        family=name.replace("_", "-"),
    )


def merge_native_staging(
    staged: Sequence[ReceiptInput],
    destination: Path,
    *,
    generation_id: str,
    prior: ReceiptInput | None = None,
    source_correction: bool = False,
):
    """Merge qualified base-table staging with typed columns and retained witnesses.

    The existing source-recency/correction checks run in an isolated directory.
    All input generations remain pinned witnesses, including losing copies.
    """
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.transforms.merge_staging_files import merge_staging_files

    if not staged:
        raise ValueError("Native staging requires at least one selected input")
    dataset = staged[0].dataset
    if (
        dataset not in RECORD_TYPES
        or any(s.dataset != dataset for s in staged)
        or prior is not None
        and prior.dataset != dataset
    ):
        raise ValueError("Native staging inputs must name one Regulations.gov base dataset")
    record_type = RECORD_TYPES[dataset]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-staging-", dir=destination.parent) as temp:
        work = Path(temp)
        parts = work / "staging" / dataset
        parts.mkdir(parents=True)
        for i, selected in enumerate(staged):
            materialize_internal(selected, parts / f"{i}.parquet")
        if prior is not None:
            materialize_internal(prior, work / f"{dataset}.parquet")
        merge_staging_files(
            work / "staging",
            work,
            [dataset],
            {dataset: record_type.schema},
            {dataset: record_type.dedup_key},
            source_correction=source_correction,
        )
        pins = []
        for selected in [*staged, *([prior] if prior is not None else [])]:
            for source in (*selected.subjects, selected.receipts):
                with source.open("rb") as body:
                    digest = file_digest(body, "sha256").hexdigest()
                pins.append(
                    {
                        "source_id": f"{dataset}:{source.name}",
                        "source_uri": None,
                        "sha256": digest,
                        "locator": None,
                        "body_version": selected.generation_id,
                    }
                )
        return build_local_generation(
            {dataset: work / f"{dataset}.parquet"},
            destination,
            generation_id=generation_id,
            family=dataset,
            witnesses=pins,
            include_source_witness=False,
            prior_receipts={dataset: [prior.receipts] if prior is not None else []},
        )
