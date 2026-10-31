"""FEC family writer and receipt-bound internal evidence/financial readers.

Pure FEC mappers still return reviewed typed interpretations. This is their
storage boundary: subject rows and processing receipts are written together.
Original schemas are restored only inside explicitly selected internal reads.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import json

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    failure_receipt,
    read_with_receipts,
    split_record,
    write_dataset,
    select_receipts,
    validate_receipt_bundle,
)
from .fec_native_subjects import FIELD_RULES, prepare_subject, subject_schema

VERSION = "fec-subject-receipts/2"
_CONVERSIONS = "fec_conversion_inputs"
_INPUT_COLUMNS = "fec_input_columns"


def dataset_policy(table: str, input_schema: pa.Schema) -> DatasetPolicy:
    schema = subject_schema(table, input_schema)
    rule = FIELD_RULES[table]
    # One receipt field holds only moved/conversion values, never a second full subject.
    return DatasetPolicy(
        table,
        schema,
        tuple(rule.get("identity_fields", ("record_id",))),
        (_CONVERSIONS, _INPUT_COLUMNS),
        policy_version=("fec-subject-receipts/3" if "filing_association_target_generation_scope" in schema.names
                        or table == "fec_agency_mapping_dispositions" else VERSION),
        receipt_only=rule["receipt_only"],
    )


def mapped_record(table: str, row: dict, policy: DatasetPolicy, *, ordinal=None, association=None) -> dict:
    subject, processing = prepare_subject(table, row)
    values = {} if subject is None else {n: subject.get(n) for n in policy.subject_schema.names}
    if "filing_association_status" in values:
        values["filing_association_status"] = "not_evaluated"
    if FIELD_RULES[table].get("observation_control"):
        if type(ordinal) is not int or ordinal < 0:
            raise ValueError("Public FEC control requires its generation-scoped observation ordinal")
        values["observation_ordinal"] = ordinal
    if association is not None:
        # The exact pre-association mapper fields remain restorable. A derived
        # decision never silently replaces already qualified source knowledge.
        resolved = association.get("filing_key")
        if row.get("filing_key") is not None and row["filing_key"] != resolved:
            raise ValueError("Recorded filing key conflicts with the selected association")
        processing = {**processing, "filing_key": row.get("filing_key")}
        if "filing_link_status" in row:
            processing["filing_link_status"] = row["filing_link_status"]
        unknown = set(association) - set(policy.subject_schema.names) - {"filing_link_status"}
        if unknown:
            raise ValueError(f"Unclassified FEC association fields: {sorted(unknown)}")
        values.update({name: value for name, value in association.items()
                       if name in policy.subject_schema.names})
    return {**values, _CONVERSIONS: processing, _INPUT_COLUMNS: list(row)}


def subject_files(path):
    """The Parquet members of a written subject in publication order: a flat file, a member directory, or none."""
    if path is None:
        return []
    return sorted(Path(path).rglob("*.parquet")) if Path(path).is_dir() else [Path(path)]


def write_fec_subjects(
    rows,
    directory: Path,
    *,
    table: str,
    input_schema: pa.Schema,
    generation_id: str,
    context_for,
    batch_size: int = 2000,
    associations_for=None,
    source_digest_for=None,
    observation_rebinding=None,
    current_header_for=None,
    subject_members=None,
):
    """Write actual mapper outputs and preserve failed conversion attempts.

    ``subject_members`` (relative path, rows) writes the subject once into those members.

    ``context_for(row, ordinal)`` supplies a ReceiptContext with all ordered source
    witnesses. The caller owns source-generation selection and witness reachability.
    Unknown fields or malformed structures yield a refused receipt without a subject.
    Failure receipts are spooled to disk, not accumulated in memory.
    """
    policy = dataset_policy(table, input_schema)
    directory = Path(directory)
    directory.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=directory.parent, prefix=".fec-failures-") as temp:
        failures_path = Path(temp) / "failures.jsonl"

        def records():
            with failures_path.open("x", encoding="utf-8") as failed:
                selected = associations_for(rows) if associations_for else ((row, None) for row in rows)
                for ordinal, (row, association) in enumerate(selected):
                    context = context_for(row, ordinal)
                    if not isinstance(context, ReceiptContext) or context.generation_id != generation_id:
                        raise ValueError("FEC context differs from the selected generation")
                    try:
                        mapped = mapped_record(table, row, policy, ordinal=ordinal, association=association)
                        if source_digest_for is not None:
                            if table != "fec_agency_mapping_dispositions":
                                raise ValueError("Source digest lookup is only for agency mapping dispositions")
                            digest = source_digest_for(row)
                            if row.get("source_sha256") is not None and row["source_sha256"] != digest:
                                raise ValueError("Agency disposition digest conflicts with selected source")
                            mapped["source_sha256"] = digest
                        if current_header_for is not None:
                            if table != 'fec_filing_header_associations':
                                raise ValueError('Current header adoption requires its owned header dataset')
                            current = current_header_for(row, ordinal)
                            current_mapped = mapped_record(table, current, policy, ordinal=ordinal)
                            for name in policy.subject_schema.names:
                                value = current_mapped[name]
                                if mapped[name] != value:
                                    mapped[_CONVERSIONS][name] = row.get(name)
                                    mapped[name] = value
                        if observation_rebinding is not None:
                            from spicy_regs.fec_native_conversion import rebind_row
                            rebound = rebind_row(table, row, observation_rebinding)
                            for name in row:
                                if row[name] != rebound[name]:
                                    mapped[_CONVERSIONS][name] = row[name]
                                    mapped[name] = rebound[name]
                        # Check native values before write_dataset can expose any output.
                        split_record(policy, mapped, context)
                    except (ValueError, TypeError, pa.ArrowException) as error:
                        context = replace(context, diagnostics={**context.diagnostics, "conversion_error": str(error)})
                        receipt = failure_receipt(
                            policy,
                            context,
                            outcome="refused",
                            raw_fields={_CONVERSIONS: dict(row), _INPUT_COLUMNS: list(row)},
                        )
                        failed.write(json.dumps(receipt, ensure_ascii=False) + "\n")
                        continue
                    yield mapped, context

        def failures():
            with failures_path.open(encoding="utf-8") as stream:
                for line in stream:
                    yield json.loads(line)

        paths = write_dataset(records(), directory, policy, failures=failures(), batch_size=batch_size,
                              subject_members=subject_members, generation_id=generation_id)
    return paths, policy


def read_fec_with_receipts(subject_paths, receipt_paths, policy, *, generation_id):
    """Restore exact mapper rows for financial eligibility, source evidence and joins.

    Receipt paths must be dataset-scoped. Shared family receipts can be selected
    with select_receipts before this call. The shared reader checks the entire
    content/version/generation join before any row is returned.
    """
    from spicy_regs.etl_receipts import selected_subject_policy
    admitted = selected_subject_policy(policy, subject_paths)
    for row in read_with_receipts(subject_paths, receipt_paths, admitted, generation_id=generation_id):
        conversion = row[_CONVERSIONS]
        original = {}
        for name in row[_INPUT_COLUMNS]:
            original[name] = conversion[name] if name in conversion else row[name]
        yield original


def read_fec_processing(receipt_paths, policy, *, generation_id):
    """Read moved filing definitions/associations/dispositions after receipt validation."""
    if not policy.receipt_only:
        raise ValueError("Processing read requires a receipt-only dataset policy")
    validate_receipt_bundle({policy.dataset: []}, receipt_paths, [policy], generation_id=generation_id)
    # Processing values use the shared exact codec; keep decoding in that module.
    from spicy_regs.etl_receipts import _unpack

    for path in receipt_paths:
        for batch in pq.ParquetFile(path).iter_batches(batch_size=2000):
            for receipt in batch.to_pylist():
                if receipt["outcome"] == "observed":
                    values = _unpack(json.loads(receipt["processing_json"]))
                    yield values[_CONVERSIONS]


def register_internal_fec_table(
    connection,
    *,
    table,
    subject_paths,
    shared_receipt_paths,
    policy,
    input_schema,
    generation_id,
    check_resources=lambda: None,
):
    """Create a private bounded internal SQL relation with exact prior mapper columns.

    Existing financial/evidence/association SQL may run only against this restored
    relation. Public readers use the subject Parquet. Missing receipts never select
    a fallback, skip an association, or weaken a financial qualification check.
    """
    if table != policy.dataset or not table.startswith("fec_"):
        raise ValueError("Internal FEC table must match the selected policy")
    with TemporaryDirectory(prefix="fec-internal-") as temp:
        selected = []
        for i, path in enumerate(shared_receipt_paths):
            selected.append(select_receipts(path, Path(temp) / f"receipts-{i}.parquet", dataset=table))
        rows = (
            read_fec_processing(selected, policy, generation_id=generation_id)
            if policy.receipt_only
            else read_fec_with_receipts(subject_paths, selected, policy, generation_id=generation_id)
        )
        temporary = f"_fec_restored_{table}"
        connection.register(temporary, pa.Table.from_batches([], schema=input_schema))
        connection.execute(f'CREATE TEMP TABLE "{table}" AS SELECT * FROM "{temporary}"')
        try:
            buffer = []
            for row in rows:
                buffer.append(row)
                if len(buffer) >= 2000:
                    check_resources()
                    connection.register(temporary, pa.Table.from_pylist(buffer, schema=input_schema))
                    connection.execute(f'INSERT INTO "{table}" SELECT * FROM "{temporary}"')
                    buffer.clear()
            if buffer:
                check_resources()
                connection.register(temporary, pa.Table.from_pylist(buffer, schema=input_schema))
                connection.execute(f'INSERT INTO "{table}" SELECT * FROM "{temporary}"')
        except BaseException:
            connection.execute(f'DROP TABLE "{table}"')
            raise
        finally:
            connection.unregister(temporary)
