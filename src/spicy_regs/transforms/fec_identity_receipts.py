"""Generation-bound subject/receipt writer and internal reads for FEC identity.

Every field decision lives in the family registry. Receipt encoding, row-version
binding and admission use the shared ETL implementation.
"""

from __future__ import annotations

from contextlib import ExitStack
from functools import cache
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_SCHEMA,
    split_record,
    split_records,
    failure_receipt,
    validate_receipt_bundle,
    select_receipts,
    read_with_receipts,
)
from .fec_identity_context_fields import REGISTRY, VERSION, normalize_record, subject_schema


@cache
def dataset_policy(table):
    rules = REGISTRY[table]
    return DatasetPolicy(
        table,
        subject_schema(table),
        tuple(rules["identity_fields"]),
        tuple(rules["receipt_fields"]),
        policy_version="fec-identity-context-receipts/3" if table == "fec_source_records" else VERSION,
        receipt_only=rules["receipt_only"],
    )


def witnesses_for(row, evidence, input_witness):
    """Preserve all ordered witnesses, including duplicate and split RSS parts."""
    witnesses = [dict(input_witness)]
    if row.get("source_sha256"):
        witnesses.append(
            dict(
                source_id=row.get("source_record_id") or row.get("record_id") or row.get("collection_id"),
                source_uri=row.get("source_url"),
                sha256=row["source_sha256"],
                locator=row.get("source_locator_json") or row.get("source_context_pointer"),
                body_version=None,
            )
        )
    for link in evidence:
        witnesses.append(
            dict(
                source_id=link.get("source_record_id") or link.get("collection_id"),
                source_uri=None,
                sha256=link.get("witness_sha256"),
                locator=json.dumps(link, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                body_version=link.get("witness_generation_pin"),
            )
        )
    return witnesses


def _buffer_size(value):
    """Conservative retained value bytes; includes subjects and witness fields."""
    if isinstance(value, str):
        return 4 * len(value) + 64
    if isinstance(value, bytes):
        return len(value) + 64
    if isinstance(value, dict):
        return 64 + sum(_buffer_size(key) + _buffer_size(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return 64 + sum(_buffer_size(item) for item in value)
    return 64


class IdentityReceiptWriter:
    """Bounded family build; failed admission never replaces an existing output.

    With ``prior_receipts`` (the selected bundle's receipt members), an attempt whose input and subject
    are unchanged keeps its prior receipt exactly, and a changed row references its direct predecessor
    (``carry_receipt_history``), so receipt history does not grow with every refresh.
    """

    def __init__(self, output_dir, *, generation_id, tables, batch_size=512, attempt_offset=0, prior_receipts=()):
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if type(attempt_offset) is not int or attempt_offset < 0:
            raise ValueError("attempt_offset must be a nonnegative integer")
        self.attempt_offset = attempt_offset
        self.prior_receipts = tuple(Path(path) for path in prior_receipts)
        self.output_dir = Path(output_dir)
        self.generation_id = generation_id
        self.policies = {name: dataset_policy(name) for name in tables}
        if not self.policies:
            raise ValueError("Explicit nonempty table selection required")
        self.batch_size = batch_size
        self.counts = {name: 0 for name in self.policies}
        self.attempts = 0
        self.table_attempts = {name: 0 for name in self.policies}
        self._last_observation_ordinal = {name: -1 for name in self.policies}
        self._stack = ExitStack()

    def __enter__(self):
        if self.output_dir.exists():
            raise FileExistsError(self.output_dir)
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._stack.enter_context(TemporaryDirectory(prefix=".fec-identity-", dir=self.output_dir.parent))
        self.stage = Path(temporary) / "tables"
        self.stage.mkdir()
        self.subject_writers = {
            name: self._stack.enter_context(
                pq.ParquetWriter(self.stage / (name + ".parquet"), policy.subject_schema, compression="zstd")
            )
            for name, policy in self.policies.items()
            if not policy.receipt_only
        }
        self.receipt_writer = self._stack.enter_context(
            pq.ParquetWriter(self.stage / "etl_receipts.parquet", RECEIPT_SCHEMA, compression="zstd")
        )
        self.buffers = {name: [] for name in self.subject_writers}
        self.receipts = []
        self.buffer_bytes = 0
        return self

    def _prepare_emit(
        self, table, row, *, input_witness, evidence=(), source_input=None, outcome=None, diagnostic=None,
        observation_ordinal=None, native_filing_subject=None, native_link_subject=None,
    ):
        if table not in self.policies:
            raise ValueError(f"Output outside explicitly selected identity tables: {table}")
        policy = self.policies[table]
        ordinal = self.table_attempts[table] if observation_ordinal is None else observation_ordinal
        if type(ordinal) is not int or ordinal <= self._last_observation_ordinal[table]:
            raise ValueError("FEC observation ordinals must increase across selected members")
        self._last_observation_ordinal[table] = ordinal
        self.table_attempts[table] += 1
        mapped = normalize_record(table, row, source_input=source_input, observation_ordinal=ordinal)
        if native_filing_subject is not None:
            if table != "fec_filings" or set(native_filing_subject) != set(policy.subject_schema.names):
                raise ValueError("Qualified filing adoption requires its complete declared native subject")
            mapped.update(native_filing_subject)
        if native_link_subject is not None:
            if table != "fec_filing_links" or set(native_link_subject) != set(policy.subject_schema.names):
                raise ValueError("Qualified filing reference adoption requires its complete declared native subject")
            if any(mapped[name] != native_link_subject[name] for name in policy.subject_schema.names
                   if name != "target_resolution_status"):
                raise ValueError("Filing reference resolution changed ordered source fields")
            if mapped["target_resolution_status"] != native_link_subject["target_resolution_status"]:
                held = dict(mapped.get("source_input") or {})
                overrides = dict(held.get("fec_retained_overrides", {}))
                original = row.get("target_resolution_status")
                if "target_resolution_status" in overrides and overrides["target_resolution_status"] != original:
                    raise ValueError("Filing reference original status conflicts with retained evidence")
                overrides["target_resolution_status"] = original
                held["fec_retained_overrides"] = overrides
                mapped["source_input"] = held
            mapped.update(native_link_subject)
        if table == "fec_committees":
            mapped["conversion_inputs"] = dict(row)
        self.attempts += 1
        diagnostics = dict(mapped["conversion_diagnostics"])
        if diagnostic is not None:
            diagnostics["mapping"] = diagnostic
        context = ReceiptContext(
            self.generation_id,
            f"{table}/{self.attempt_offset + self.attempts}",
            VERSION,
            witnesses_for(mapped, evidence, input_witness),
            diagnostics,
        )
        return mapped, context, outcome

    def emit(self, table, row, **kwargs):
        mapped, context, outcome = self._prepare_emit(table, row, **kwargs)
        policy = self.policies[table]
        if outcome is not None:
            subject = None
            receipt = failure_receipt(policy, context, outcome=outcome, raw_fields=mapped)
        else:
            subject, receipt = split_record(policy, mapped, context)
        self._append(table, subject, receipt)
        return subject, receipt

    def emit_batch(self, table, records, *, check_resources=lambda: None):
        """Normalize small ordered batches; retain complete single wide values."""
        pending, pending_bytes = [], 0

        def finish():
            nonlocal pending_bytes
            if pending:
                check_resources()
                for subject, receipt in split_records(self.policies[table], pending):
                    check_resources()
                    self._append(table, subject, receipt)
                pending.clear()
                pending_bytes = 0

        for row, kwargs in records:
            check_resources()
            mapped, context, outcome = self._prepare_emit(table, row, **kwargs)
            size = _buffer_size(mapped) + _buffer_size(context.witnesses) + _buffer_size(context.diagnostics)
            if pending and pending_bytes + size > 8 * 1024 * 1024:
                finish()
            if outcome is not None:
                finish()
                self._append(table, None, failure_receipt(self.policies[table], context,
                    outcome=outcome, raw_fields=mapped))
            else:
                pending.append((mapped, context))
                pending_bytes += size
                if len(pending) >= 32 or pending_bytes >= 8 * 1024 * 1024:
                    finish()
        finish()

    def _append(self, table, subject, receipt):
        if subject is not None:
            self.buffers[table].append(subject)
            self.counts[table] += 1
        self.receipts.append(receipt)
        self.buffer_bytes += _buffer_size(subject) + _buffer_size(receipt)
        if len(self.receipts) >= self.batch_size or self.buffer_bytes >= 8 * 1024 * 1024:
            self.flush()

    def flush(self):
        for name, rows in self.buffers.items():
            if rows:
                self.subject_writers[name].write_table(
                    pa.Table.from_pylist(rows, schema=self.policies[name].subject_schema)
                )
                rows.clear()
        if self.receipts:
            self.receipt_writer.write_table(pa.Table.from_pylist(self.receipts, schema=RECEIPT_SCHEMA))
            self.receipts.clear()
        self.buffer_bytes = 0

    def __exit__(self, kind, error, traceback):
        try:
            if kind is None:
                self.flush()
                for writer in self.subject_writers.values():
                    writer.close()
                self.receipt_writer.close()
                self._carry_history()
                subjects = {
                    name: [] if policy.receipt_only else [self.stage / (name + ".parquet")]
                    for name, policy in self.policies.items()
                }
                validate_receipt_bundle(
                    subjects,
                    [self.stage / "etl_receipts.parquet"],
                    list(self.policies.values()),
                    generation_id=self.generation_id,
                )
                (self.stage / "identity-context.json").write_text(
                    json.dumps(
                        {
                            "generation_id": self.generation_id,
                            "policies": [p.descriptor() for p in self.policies.values()],
                            "subject_rows": self.counts,
                            "receipt_rows": pq.ParquetFile(self.stage / "etl_receipts.parquet").metadata.num_rows,
                        },
                        indent=2,
                    )
                    + "\n"
                )
                from rulespec_artifacts import publish_directory_no_replace

                publish_directory_no_replace(self.stage, self.output_dir)
        finally:
            self._stack.close()
        return False


    def _carry_history(self):
        if not self.prior_receipts:
            return
        from spicy_regs.receipt_history import carry_receipt_history

        # Scoped priors live beside the stage, never in the published directory.
        scoped = [
            select_receipts(path, self.stage.parent / f"prior-{index}-{name}.parquet", dataset=name)
            for index, path in enumerate(self.prior_receipts)
            for name in self.policies
        ]
        receipts = self.stage / "etl_receipts.parquet"
        carry_receipt_history(receipts, scoped, receipts)


def read_identity_rows(directory, table, *, generation_id):
    """Internal callers recover qualification fields only after exact receipt admission."""
    directory = Path(directory)
    policy = dataset_policy(table)
    if policy.receipt_only:
        raise ValueError("Processing-only datasets require read_identity_processing")
    with TemporaryDirectory(prefix="fec-identity-read-") as temporary:
        receipts = select_receipts(
            directory / "etl_receipts.parquet", Path(temporary) / "receipts.parquet", dataset=table
        )
        from spicy_regs.etl_receipts import selected_subject_policy
        subjects = [directory / (table + ".parquet")]
        admitted = selected_subject_policy(policy, subjects)
        yield from read_with_receipts(
            subjects, [receipts], admitted, generation_id=generation_id
        )


def read_identity_processing(directory, table, *, generation_id):
    """Read receipt-only source inputs for internal mapping, never a public view."""
    from spicy_regs.etl_receipts import _unpack

    policy = dataset_policy(table)
    if not policy.receipt_only:
        raise ValueError("Subject datasets require read_identity_rows")
    with TemporaryDirectory(prefix="fec-processing-read-") as temporary:
        path = select_receipts(
            Path(directory) / "etl_receipts.parquet", Path(temporary) / "receipts.parquet", dataset=table
        )
        validate_receipt_bundle({table: []}, [path], [policy], generation_id=generation_id)
        with pq.ParquetFile(path) as source:
            for batch in source.iter_batches(batch_size=512):
                for receipt in batch.to_pylist():
                    if receipt["outcome"] == "observed":
                        yield _unpack(json.loads(receipt["processing_json"]))


def seal_identity_context(directory, destination):
    """Seal the exact local subject/receipt set as a verified partial generation."""
    from spicy_regs.generations import build_generation

    directory = Path(directory)
    metadata = json.loads((directory / "identity-context.json").read_text())
    policies = [DatasetPolicy.from_descriptor(p) for p in metadata["policies"]]
    files = [directory / (p.dataset + ".parquet") for p in policies if not p.receipt_only]
    return build_generation(
        Path(destination),
        family="fec-identity-context",
        files=files,
        expected_keys=tuple(p.name for p in files),
        publication_status="local-partial",
        receipt_path=directory / "etl_receipts.parquet",
        receipt_policies=policies,
        receipt_generation_id=metadata["generation_id"],
    )
