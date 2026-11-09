"""Retain attribute-copy selection evidence while writing native subject rows."""

from __future__ import annotations

from dataclasses import replace
from hashlib import file_digest
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.contract_types import arrow_schema
from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA,
    WITNESS_TYPE,
    ReceiptContext,
    combine_receipts,
    failure_receipt,
    observation_receipt,
)
from spicy_regs.generations import build_generation
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.transforms.regulations_attributes import (
    ATTRIBUTE_TABLES,
    _projection_and_digest,
    _with_order_columns,
    contract,
    newest_copy_sql,
)
from spicy_regs.transforms.regulations_receipts import ReceiptInput, policy, read_internal, write_records
from spicy_regs.transforms.regulations_shape import shape_record

CONTEXT_FIELDS = ("_source_attempt", "_source_processor", "_source_witnesses")


def build_attribute_generation(
    dataset: str,
    candidates,
    destination: Path,
    *,
    scan_context: ReceiptContext,
    prior: ReceiptInput | None = None,
):
    """Select raw copies with the existing rule, retaining every read and refusal.

    ``candidates`` yields (raw_payload, receipt_context, written_at_epoch_seconds).
    Supply all copies in one selection, including across source shards: selecting
    each shard first would change the volatile-copy time margin. Fresh identities
    replace prior identities, exactly as the existing attribute merge does.
    """
    if dataset not in ATTRIBUTE_TABLES.values() or prior is not None and prior.dataset != dataset:
        raise ValueError("One explicit Regulations.gov attribute dataset is required")
    declared = policy(dataset)
    generation_id = scan_context.generation_id
    source_contract = contract(dataset)
    project, digest = _projection_and_digest(dataset)
    schema = _with_order_columns(arrow_schema(source_contract))
    schema = schema.append(pa.field(CONTEXT_FIELDS[0], pa.string()))
    schema = schema.append(pa.field(CONTEXT_FIELDS[1], pa.string()))
    schema = schema.append(pa.field(CONTEXT_FIELDS[2], pa.list_(WITNESS_TYPE)))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-attributes-", dir=destination.parent) as temporary:
        work = Path(temporary)
        # Qualify the prior before projecting new input. Parent byte pins retain
        # previous receipts, including the raw copies behind carried rows.
        if prior is not None:
            pins = []
            for path in (*prior.subjects, prior.receipts):
                with path.open("rb") as body:
                    sha256 = file_digest(body, "sha256").hexdigest()
                pins.append(
                    {
                        "source_id": f"{dataset}:{path.name}",
                        "source_uri": None,
                        "sha256": sha256,
                        "locator": None,
                        "body_version": prior.generation_id,
                    }
                )

            def prior_rows():
                for ordinal, row in enumerate(read_internal(prior)):
                    yield {
                        **row,
                        "_modify_date": None,
                        "_written_at": None,
                        "_record_digest": None,
                        "_source_attempt": f"carried:{ordinal}",
                        "_source_processor": "spicy-regs:attribute-prior-v1",
                        "_source_witnesses": pins,
                    }

            write_rows(prior_rows(), work / "prior.parquet", schema)

        observations_path = work / "observations.parquet"
        with pq.ParquetWriter(observations_path, RECEIPT_SCHEMA, compression="zstd") as observation_writer:
            observations = []

            def remember(receipt):
                observations.append(receipt)
                if len(observations) >= 2000:
                    observation_writer.write_table(pa.Table.from_pylist(observations, schema=RECEIPT_SCHEMA))
                    observations.clear()

            remember(
                observation_receipt(
                    declared, replace(scan_context, attempt_id=f"{scan_context.attempt_id}:scan"), processing_fields={}
                )
            )

            def fresh_rows():
                for ordinal, (payload, context, written_at) in enumerate(candidates):
                    if context.generation_id != generation_id:
                        raise ValueError("Candidate receipt belongs to another generation")
                    if written_at is not None and type(written_at) is not int:
                        raise ValueError("Candidate source write time must be epoch seconds or null")
                    selected = replace(context, attempt_id=f"{context.attempt_id}:candidate:{ordinal}")
                    order = {"_written_at": written_at}
                    try:
                        data = payload.get("data") if isinstance(payload, dict) else None
                        if not isinstance(data, dict):
                            raise ValueError("Source data must be an object")
                        attrs = data.get("attributes")
                        if attrs is None:
                            attrs = {}
                        if not isinstance(attrs, dict):
                            raise ValueError("Source attributes must be an object or null")
                        row = project(data.get("id"), attrs)
                        # Validate the final native shape before selection so a
                        # malformed newer copy cannot silently erase a valid one.
                        shape_record(dataset, row)
                        order["_modify_date"] = attrs.get("modifyDate")
                        try:
                            order["_record_digest"] = digest(payload)
                        except ValueError:
                            order["_record_digest"] = None
                        candidate = {
                            **row,
                            **order,
                            "_source_attempt": selected.attempt_id,
                            "_source_processor": selected.processor,
                            "_source_witnesses": list(selected.witnesses),
                        }
                        pa.Table.from_pylist([candidate], schema=schema)
                    except (ValueError, TypeError, pa.ArrowException) as error:
                        remember(
                            failure_receipt(
                                declared,
                                replace(
                                    selected,
                                    diagnostics={
                                        **selected.diagnostics,
                                        "error_type": type(error).__name__,
                                        "error": str(error),
                                    },
                                ),
                                outcome="refused",
                                raw_fields={"raw_source_record": payload, "raw_conversion_inputs": order},
                            )
                        )
                        continue
                    remember(
                        observation_receipt(
                            declared,
                            selected,
                            processing_fields={"raw_source_record": payload, "raw_conversion_inputs": order},
                        )
                    )
                    yield candidate

            write_rows(fresh_rows(), work / "candidates.parquet", schema)
            if observations:
                observation_writer.write_table(pa.Table.from_pylist(observations, schema=RECEIPT_SCHEMA))

        with duckdb.connect() as connection:
            connection.from_parquet(str(work / "candidates.parquet")).create_view("candidate_copies")
            connection.execute(
                "CREATE TEMP VIEW selected_copies AS "
                + newest_copy_sql("candidate_copies", dataset, retained_columns=CONTEXT_FIELDS)
            )
            query = "SELECT * FROM selected_copies"
            if prior is not None:
                connection.from_parquet(str(work / "prior.parquet")).create_view("prior_rows")
                columns = ",".join(f'p."{c}"' for c in (*source_contract.columns, *CONTEXT_FIELDS))
                same = " AND ".join(f'p."{c}" IS NOT DISTINCT FROM s."{c}"' for c in source_contract.identity)
                query += (
                    f" UNION ALL SELECT {columns} FROM prior_rows p WHERE NOT EXISTS "
                    f"(SELECT 1 FROM selected_copies s WHERE {same})"
                )
            reader = connection.execute(query).to_arrow_reader(2000)

            def accepted_rows():
                for batch in reader:
                    for row in batch.to_pylist():
                        source_attempt = row.pop("_source_attempt")
                        processor = row.pop("_source_processor")
                        witnesses = row.pop("_source_witnesses")
                        yield (
                            row,
                            ReceiptContext(
                                generation_id,
                                f"{source_attempt}:selected",
                                processor,
                                witnesses,
                                {
                                    "selected_input_attempt": source_attempt,
                                    "selection_rule": "regulations_attributes.newest_copy_sql",
                                },
                            ),
                        )

            subject, accepted = write_records(dataset, accepted_rows(), work / "selected")
        combined = combine_receipts([accepted, observations_path], work / "etl_receipts.parquet")
        artifact = build_generation(
            work / "generation",
            family=dataset.replace("_", "-"),
            files=[subject],
            expected_keys=[subject.name],
            publication_status="local-partial",
            receipt_path=combined,
            receipt_policies=[declared],
            receipt_generation_id=generation_id,
        )
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(work / "generation", destination)
        return artifact
