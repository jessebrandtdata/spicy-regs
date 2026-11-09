"""Write legislative subjects and shared receipts; restore exact inputs for ETL.

The original qualified builders keep their acquisition, replacement, financial,
and retry checks. ``build_with_receipts`` supplies verified prior observations
to those checks and migrates their actual outputs before generation admission.
"""

from __future__ import annotations

import hashlib
import inspect
import itertools
import json
import shutil
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA,
    DatasetPolicy,
    ReceiptContext,
    combine_receipts,
    failure_receipt,
    select_receipts,
    split_record,
    validate_receipt_bundle,
    write_dataset,
)
from spicy_regs.legislative_documents import (
    LegislativeShapeError,
    field_registry,
    map_subject,
    recorded_subject,
    subject_schema,
)
from spicy_regs.govinfo_revisions import DATED_TABLES
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.phase_log import phase_log
from spicy_regs.navigation_read_outcomes import POLICIES as OUTCOME_POLICIES, READ_TABLES, checkpoint_rows, file_rows


RAW = "raw_source_row"
FILE_STATES = "legislative_document_file_states"
FILE_POLICY = DatasetPolicy(FILE_STATES, pa.schema([]), (), ("file_state",), receipt_only=True)


def policy(dataset: str) -> DatasetPolicy:
    spec = field_registry()[dataset]
    return DatasetPolicy(
        dataset,
        subject_schema(dataset),
        () if spec["processing_only"] else tuple(spec["identity_fields"]),
        (RAW,),
        # committee_reports/2 moves per-letter columns to committee_report_cbo_letters (ss-uep5).
        # Summary /3 retains both source clocks and schema context. Earlier readers dropped
        # the legacy summary clock, so only a replay of retained originals can qualify it.
        # GovInfo /3 defines dated keys for fresh builds; no implicit prior-key migration.
        # Senate /2 preserves funding designations as source strings, including X and 0509.
        policy_version="legislative-documents/3" if dataset in DATED_TABLES or dataset == "bill_publisher_summaries" else "legislative-documents/2" if dataset in {
            "native_legal_references", "native_legal_reference_reads", "document_citations",
            "bill_versions", "senate_expenditures",
        } else "legislative-documents/1",
        receipt_only=spec["processing_only"],
    )


def input_policy(dataset: str, subjects: Sequence[Path]) -> DatasetPolicy:
    """Read only an exact current or declared earlier subject shape.

    An earlier native read scope had no subject files. Its unchanged observed
    receipts remain readable, then the next writer creates a main scope row.
    """
    from spicy_regs.etl_receipts import selected_subject_policy

    current = policy(dataset)
    return selected_subject_policy(current, subjects)


def _processor(dataset: str) -> str:
    if dataset in {"native_legal_references", "native_legal_reference_reads", "bill_publisher_summaries"}:
        return "legislative-documents/3"
    if dataset == "senate_expenditures":
        return "legislative-documents/2"
    # The citation routing namespace is now retained on main rows.
    return "legislative-documents/3" if dataset == "document_citations" else "legislative-documents/1"


def mapped_record(dataset: str, row: Mapping[str, Any]) -> dict:
    return (map_subject(dataset, row) or {}) | {RAW: dict(row)}


def split_source_row(dataset: str, row: Mapping[str, Any], context: ReceiptContext) -> tuple[dict | None, dict]:
    """A refused native conversion retains the entire exact input and its witness."""
    try:
        mapped = mapped_record(dataset, row)
    except LegislativeShapeError as error:
        from dataclasses import replace

        failed_context = replace(context, diagnostics=dict(context.diagnostics) | {"conversion_error": str(error)})
        return None, failure_receipt(policy(dataset), failed_context, outcome="refused", raw_fields={RAW: dict(row)})
    return split_record(policy(dataset), mapped, context)


def _rows(path: Path) -> Iterable[dict]:
    # Do not infer Hive partition values: the stored source spelling is evidence.
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _members(path: Path) -> list[Path]:
    return sorted(path.rglob("*.parquet")) if path.is_dir() else [path]


def write_legislative_outputs(
    outputs: Sequence[Path],
    destination: Path,
    *,
    generation_id: str,
    source_witnesses: Sequence[Mapping] = (),
    prior_receipts: Sequence[Path] = (),
    file_outcomes_table: str | None = None,
    validate: bool = True,
    bulk: bool = True,
) -> dict:
    """Create a new local bundle from explicit producer outputs, preserving partitions.

    Non-owned outputs are returned as dependency paths, never reclassified.
    Source files remain untouched and must remain retained with the build.
    """
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "generation_id": generation_id,
        "subjects": {},
        "datasets": [],
        "source_outputs": {},
        "unowned_outputs": [],
        "refused_rows": {},
        "receipt_file": "etl_receipts.parquet",
        "outcome_subjects": {},
    }
    with TemporaryDirectory(prefix=".legislative-", dir=destination.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        receipt_shards, states = [], []
        for output in outputs:
            output = Path(output)
            dataset = output.name if output.is_dir() else output.stem
            if dataset not in field_registry():
                manifest["unowned_outputs"].append(str(output.resolve()))
                continue
            if dataset in manifest["datasets"]:
                raise ValueError(f"Repeated dataset output: {dataset}")
            manifest["datasets"].append(dataset)
            dataset_started = monotonic()
            manifest["subjects"][dataset] = []
            manifest["source_outputs"][dataset] = str(output.resolve())
            manifest["refused_rows"][dataset] = 0
            members = _members(output)
            if output.is_dir() and not members:
                # An empty partitioned table has no physical member. Its receipt
                # records successful emptiness, without inventing a partition key.
                from spicy_regs.etl_receipts import exact_json

                state = {"dataset": dataset, "relative_path": None, "partitioned": True, "rows": 0}
                witness = {
                    "source_id": dataset,
                    "source_uri": None,
                    "sha256": "sha256:" + hashlib.sha256(exact_json(state).encode()).hexdigest(),
                    "locator": "receipt.values.file_state",
                    "body_version": None,
                }
                states.append(
                    (
                        {"file_state": state},
                        ReceiptContext(generation_id, f"{dataset}/empty", _processor(dataset), [witness]),
                    )
                )
                (stage / dataset).mkdir()
            for member_index, member in enumerate(members):
                digest = _sha(member)
                relative = (
                    str(member.relative_to(output))
                    if output.is_dir() and member.is_relative_to(output)
                    else member.name
                )
                retained_member = destination / member.relative_to(stage) if member.is_relative_to(stage) else member
                witness = {
                    "source_id": dataset,
                    "source_uri": None,
                    "sha256": digest,
                    "locator": None,
                    "body_version": None,
                }
                member_context = ReceiptContext(
                    generation_id,
                    f"{dataset}/member/{member_index}",
                    _processor(dataset),
                    [witness, *source_witnesses],
                )
                with pq.ParquetFile(member) as source:
                    file_state = {
                        "dataset": dataset,
                        "relative_path": relative,
                        "partitioned": output.is_dir(),
                        "schema": source.schema_arrow.serialize().to_pybytes(),
                        "metadata": list((source.metadata.metadata or {}).items()),
                        "sha256": digest,
                        "source_path": str(retained_member.resolve()),
                        "rows": source.metadata.num_rows,
                    }
                states.append(({"file_state": file_state}, member_context))
                failures = []

                def row_context(row_index):
                    return ReceiptContext(generation_id, f"{dataset}/{member_index}/{row_index}",
                                          _processor(dataset), [dict(witness, locator=f"row:{row_index}"), *source_witnesses])

                def records():
                    for row_index, row in enumerate(_rows(member)):
                        context = row_context(row_index)
                        try:
                            mapped = mapped_record(dataset, row)
                        except LegislativeShapeError as error:
                            from dataclasses import replace

                            failed = replace(context, diagnostics={"conversion_error": str(error)})
                            failures.append(
                                failure_receipt(policy(dataset), failed, outcome="refused", raw_fields={RAW: row})
                            )
                            manifest["refused_rows"][dataset] += 1
                            continue
                        yield mapped, context

                shard_dir = stage / ".shards" / dataset / str(member_index)
                from spicy_regs import congress_bulk, etl_bulk

                selected = policy(dataset)
                if bulk and congress_bulk.mapped_eligible(pq.read_schema(member), selected):
                    shard_dir.mkdir(parents=True)
                    subject_path, receipts = shard_dir / f"{dataset}.parquet", shard_dir / "etl_receipts.parquet"
                    def mapper(raw):
                        converted = mapped_record(dataset, raw)
                        return None if selected.receipt_only else {name: converted[name] for name in selected.subject_schema.names}
                    def attempt(raw, ordinal):
                        result = split_source_row(dataset, raw, row_context(ordinal))
                        if result[1]["outcome"] == "refused":
                            manifest["refused_rows"][dataset] += 1
                        return result
                    routed = congress_bulk.write_mapped_bundle(
                        member, subject_path, receipts, policy=selected, generation_id=generation_id,
                        processor=_processor(dataset), mapper=mapper, row_attempt=attempt, processing_field=RAW,
                        attempt_prefix=f"{dataset}/{member_index}/", witness=witness, witnesses=source_witnesses,
                        locator_prefix="row:", refusals_last=True,
                    )
                    if routed:
                        logger.warning("legislative-write route=bulk dataset={} row_decisions={}", dataset, routed)
                    subject = None if selected.receipt_only else subject_path
                else:
                    if bulk:
                        etl_bulk.log_row_fallback("legislative-write", etl_bulk.NotBulkEligible("source value types have no proven SQL spelling"),
                                                  dataset=dataset, rows=pq.ParquetFile(member).metadata.num_rows)
                    subject, receipts = write_dataset(records(), shard_dir, selected, failures=failures, validate=False)
                receipt_shards.append(receipts)
                if subject is not None:
                    final_relative = f"{dataset}/{relative}" if output.is_dir() else f"{dataset}.parquet"
                    target = stage / final_relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(subject, target)
                    manifest["subjects"][dataset].append(final_relative)
            if dataset in READ_TABLES:
                name = READ_TABLES[dataset]
                checkpoint_digest = _sha(output)
                def recorded_checkpoints():
                    for ordinal, (_, row, raw) in enumerate(checkpoint_rows(dataset, output, generation_id=generation_id)):
                        yield row | {"recorded_event": raw}, ReceiptContext(generation_id,
                            f"{dataset}/checkpoint/{ordinal}", "navigation-read-outcomes/1",
                            [{"source_id": dataset, "source_uri": None, "sha256": checkpoint_digest,
                              "locator": f"row:{ordinal}", "body_version": None}])
                subject, receipts = write_dataset(recorded_checkpoints(), stage / ".outcomes" / name,
                                                   OUTCOME_POLICIES[name], validate=False)
                if subject is None:
                    raise ValueError(f"Main read-outcome policy has no subject: {name}")
                shutil.copyfile(subject, stage / f"{name}.parquet")
                receipt_shards.append(receipts)
                manifest["outcome_subjects"][name] = [f"{name}.parquet"]
            logger.info("receipt-phase write-legislative-dataset dataset={} members={} seconds={:.1f} status=ok",
                        dataset, len(members), monotonic() - dataset_started)
        if not manifest["datasets"]:
            raise ValueError("No owned legislative producer outputs selected")
        with phase_log("write-file-states", files=len(states)):
            _, state_receipts = write_dataset(states, stage / ".shards" / FILE_STATES, FILE_POLICY, validate=False)
        receipt_shards.append(state_receipts)
        if file_outcomes_table is not None:
            selected_policy = OUTCOME_POLICIES[file_outcomes_table]
            mapped = manifest["subjects"] | manifest["outcome_subjects"]
            subject, receipts = write_dataset(
                ((row | {"recorded_event": raw}, context) for row, raw, context in
                 file_rows(states, mapped, stage, generation_id=generation_id)),
                stage / ".outcomes" / file_outcomes_table, selected_policy, validate=False)
            if subject is None:
                raise ValueError(f"Main file-outcome policy has no subject: {file_outcomes_table}")
            shutil.copyfile(subject, stage / f"{file_outcomes_table}.parquet")
            receipt_shards.append(receipts)
            manifest["outcome_subjects"][file_outcomes_table] = [f"{file_outcomes_table}.parquet"]
        with phase_log("combine-legislative-receipts", shards=len(receipt_shards)):
            combine_receipts(receipt_shards, stage / manifest["receipt_file"])
        manifest["fresh_refused_rows"] = dict(manifest["refused_rows"])
        if prior_receipts:
            # Carry once over the complete dataset, including all partitions. A
            # shard cannot decide whether another shard resolved a prior refusal.
            import pyarrow.dataset as ds
            from spicy_regs.receipt_history import (
                RefusalIdentity, carry_receipt_history, crs_revision_identity_migrations,
            )

            active = [*manifest["datasets"], *manifest["outcome_subjects"], FILE_STATES]
            with phase_log("scope-legislative-prior", priors=len(prior_receipts)):
                scoped = stage / ".shards" / "selected-prior.parquet"
                with pq.ParquetWriter(scoped, RECEIPT_SCHEMA, compression="zstd") as writer:
                    for path in prior_receipts:
                        with pq.ParquetFile(path) as previous:
                            for batch in previous.iter_batches(batch_size=2000):
                                scoped_rows = pa.Table.from_batches([batch]).filter(ds.field("dataset").isin(active))
                                if scoped_rows.filter(
                                    (ds.field("dataset") == "bill_publisher_summaries") &
                                    (ds.field("policy_version") != policy("bill_publisher_summaries").policy_version)
                                ).num_rows:
                                    raise ValueError("CRS summary prior uses an incompatible clock policy; replay retained originals")
                                writer.write_table(scoped_rows)
            identities, migrations = {}, {}
            for name in manifest["datasets"]:
                current_policy = policy(name)
                keys = current_policy.identity_fields
                migrations.update(crs_revision_identity_migrations(
                    current_policy, lambda raw, name=name: map_subject(name, raw)))
                raw_names = {field["name"] for field in field_registry()[name]["fields"]}
                if keys and set(keys) <= raw_names:
                    identities[name] = RefusalIdentity(keys, RAW)
            receipt_path = stage / manifest["receipt_file"]
            carry_receipt_history(receipt_path, [scoped], receipt_path, refusal_identities=identities,
                                  identity_migrations=migrations)
            with phase_log("count-legislative-refusals", datasets=len(manifest["datasets"])):
                held = ds.dataset(receipt_path, format="parquet")
                for name in manifest["datasets"]:
                    manifest["refused_rows"][name] = held.count_rows(
                        filter=(ds.field("dataset") == name) & (ds.field("outcome") == "refused"))
        if validate:
            with phase_log("legislative-admission"):
                validate_receipt_bundle(
                    {name: [stage / f for f in paths] for name, paths in
                     (manifest["subjects"] | manifest["outcome_subjects"]).items()} | {FILE_STATES: []},
                    [stage / manifest["receipt_file"]],
                    [policy(n) for n in manifest["datasets"]] + [FILE_POLICY] +
                    [OUTCOME_POLICIES[n] for n in manifest["outcome_subjects"]],
                    generation_id=generation_id,
                )
        # Staging shards are private duplicates; the admitted bundle has one shared receipt member.
        shutil.rmtree(stage / ".shards")
        if (stage / ".outcomes").exists():
            shutil.rmtree(stage / ".outcomes")
        (stage / "legislative-bundle.json").write_text(json.dumps(manifest, indent=2) + "\n")
        from rulespec_artifacts import publish_directory_no_replace

        publish_directory_no_replace(stage, destination)
    return manifest


def _processing_rows(receipts: Path, dataset_policy: DatasetPolicy, generation_id: str) -> Iterable[dict]:
    """Replay observations from a pair already admitted by ``restore_prior``."""
    from spicy_regs.etl_receipts import decode_exact_json

    for receipt in _rows(receipts):
        if receipt["outcome"] == "observed":
            yield decode_exact_json(receipt["processing_json"])


def retained_source_rows(dataset, dataset_policy, records):
    """Restore the source spelling using the same mapping and ambiguity checks."""
    if dataset == "senate_expenditures":
        from spicy_regs.etl_receipts import receipt_policies

        if dataset_policy.descriptor() not in [p.descriptor() for p in receipt_policies(policy(dataset))]:
            raise ValueError("Selected Senate funding policy differs from its exact declaration")
    seen = set()
    for record in records:
        raw = record[RAW]
        if dataset_policy.receipt_only:
            map_subject(dataset, raw)
            identity = tuple(raw[k] for k in field_registry()[dataset]["identity_fields"])
            if identity in seen:
                raise ValueError("Ambiguous selected processing scope")
            seen.add(identity)
        elif recorded_subject(dataset, raw, record, policy_version=dataset_policy.policy_version) != {
            k: v for k, v in record.items() if k != RAW
        }:
            raise ValueError("Retained conversion input disagrees with its native subject")
        yield raw


def _bulk_source_rows(con, dataset, selected_policy, subjects, states, start, receipts):
    """Decode ordered source batches, retaining the mapper's complete reproduction check."""
    from spicy_regs import etl_bulk
    from spicy_regs.etl_receipts import decode_exact_json
    import pyarrow.dataset as ds

    schemas = [pa.ipc.read_schema(pa.BufferReader(state["schema"])) for state in states if state["relative_path"] is not None]
    if not schemas:
        return iter(())
    schema = schemas[0]
    if not all(all(pa.types.is_string(field.type) for field in item) and item.equals(schema) for item in schemas):
        raise etl_bulk.NotBulkEligible("Legislative retained source schemas require the exact row decoder")
    names = sorted(schema.names)
    # Share the JSON parse across names, type tags and values, as Congress restore does.
    paths = [f"$[1][0][1][1][{i}]{suffix}" for i in range(len(names)) for suffix in ("[0]", "[1][0]", "[1][1]")]
    values = "json_extract_string(processing_json, [" + ", ".join(etl_bulk._literal(path) for path in paths) + "])"
    plain = " AND ".join([
        "json_array_length(processing_json, '$[1]') = 1",
        "json_extract_string(processing_json, '$[1][0][0]') = 'raw_source_row'",
        "json_extract_string(processing_json, '$[1][0][1][0]') = 'dict'",
        f"json_array_length(processing_json, '$[1][0][1][1]') = {len(names)}",
        *[f"_source_values[{3 * i + 1}] = {etl_bulk._literal(name)}" for i, name in enumerate(names)],
        *[f"_source_values[{3 * i + 2}] IN ('str','null')" for i in range(len(names))],
    ])
    columns = ", ".join(f'_source_values[{3 * names.index(field.name) + 3}] AS "{field.name}"' for field in schema)
    query = f"""WITH extracted AS MATERIALIZED (
        SELECT *, {values} AS _source_values FROM legislative_restore_batch)
        SELECT {columns}, _selected_subject,
            CASE WHEN NOT coalesce({plain}, false) THEN processing_json END AS _raw_reference
        FROM extracted ORDER BY _held_ordinal"""

    def source_batches():
        if selected_policy.receipt_only:
            # This file is already selected and admitted. Preserve its physical observed-row order.
            with pq.ParquetFile(receipts) as source:
                position = 0
                for batch in source.iter_batches(batch_size=2000, columns=["outcome", "processing_json"]):
                    etl_bulk.checkpoint()
                    table = pa.Table.from_batches([batch]).filter(ds.field("outcome") == "observed").select(["processing_json"])
                    yield table.append_column("_selected_subject", pa.nulls(table.num_rows)).append_column(
                        "_held_ordinal", pa.arange(position, position + table.num_rows))
                    position += table.num_rows
            return
        offset = start
        fields = ", ".join(f'"{field.name}" := held."{field.name}"' for field in selected_policy.subject_schema)
        joined = f"""SELECT struct_pack({fields}) AS _selected_subject, r.processing_json, held._held_ordinal
            FROM legislative_restore_subjects held
            JOIN (SELECT * FROM subject_receipts WHERE n >= ? AND n < ?) m ON m.n=held._held_ordinal
            JOIN receipts r ON r.n=m.receipt ORDER BY held._held_ordinal"""
        for path in subjects:
            with pq.ParquetFile(path) as source:
                for batch in source.iter_batches(batch_size=2000):
                    etl_bulk.checkpoint()
                    held = pa.Table.from_batches([batch]).append_column("_held_ordinal", pa.arange(offset, offset + batch.num_rows))
                    con.register("legislative_restore_subjects", held)
                    try:
                        # Finish this bounded join before starting another statement on the same connection.
                        restored = con.execute(joined, [offset, offset + batch.num_rows]).to_arrow_table()
                    finally:
                        con.unregister("legislative_restore_subjects")
                    if restored.num_rows != batch.num_rows:
                        raise ValueError("Receipt replay did not reconstruct every legislative source batch")
                    yield restored
                    offset += batch.num_rows

    def records():
        for source in source_batches():
            for batch in source.to_batches(max_chunksize=2000):
                # Slice the retained JSON too: narrow subjects can carry large original source text.
                # One row and Arrow backing buffers can exceed this logical-byte bound.
                for piece in etl_bulk._insert_batches(batch):
                    con.register("legislative_restore_batch", piece)
                    try:
                        table = con.execute(query).to_arrow_table()
                    finally:
                        con.unregister("legislative_restore_batch")
                    raws = table.select(schema.names).to_pylist()
                    for raw, subject, reference in zip(raws, table['_selected_subject'].to_pylist(), table['_raw_reference'].to_pylist(), strict=True):
                        if reference is not None:
                            raw = decode_exact_json(reference)[RAW]
                        yield (subject or {}) | {RAW: raw}

    return retained_source_rows(dataset, selected_policy, records())


def restore_prior(bundle: Path, destination: Path, *, bulk: bool = True) -> dict[str, Path]:
    """Rebuild exact source-owner inputs after one complete pair admission.

    Partition states and source-to-subject reproduction remain mandatory. A
    receipt generation may be older than its selected publication generation.
    """
    from spicy_regs import etl_bulk

    bundle = Path(bundle)
    manifest = json.loads((bundle / "legislative-bundle.json").read_text())
    if any(manifest.get("fresh_refused_rows", manifest["refused_rows"]).values()):
        raise ValueError("A bundle with conversion refusals cannot qualify an incremental prior")
    destination.mkdir(parents=True, exist_ok=False)
    generation = manifest["generation_id"]
    with TemporaryDirectory(prefix=".receipt-select-", dir=destination) as temp, ExitStack() as stack:
        selected = Path(temp)
        receipts = bundle / manifest["receipt_file"]
        selected_policies = {n: input_policy(n, [bundle / p for p in manifest["subjects"][n]]) for n in manifest["datasets"]}
        subject_paths = {n: [bundle / p for p in paths] for n, paths in (manifest["subjects"] | manifest.get("outcome_subjects", {})).items()} | {FILE_STATES: []}
        policies = list(selected_policies.values()) + [FILE_POLICY] + [OUTCOME_POLICIES[n] for n in manifest.get("outcome_subjects", {})]
        con = None
        if bulk:
            try:
                con = stack.enter_context(etl_bulk._validated_bundle(subject_paths, [receipts], policies,
                    generation_id=generation, retain_processing=True))
            except etl_bulk.NotBulkEligible as error:
                etl_bulk.log_row_fallback("legislative-restore", error, dataset=",".join(manifest["datasets"]))
                etl_bulk.reference_fallback(error)
        row_con = None
        if con is None:
            import sqlite3
            from spicy_regs import etl_receipts

            # Keep the reference reader's admitted index for replay rather than
            # loading and validating the same receipts again for each dataset.
            row_con = sqlite3.connect(str(selected / "reference.db"))
            stack.callback(row_con.close)
            registered = etl_receipts._bundle_policies(subject_paths, [receipts], policies)
            etl_receipts._load_receipts(row_con, [receipts], registered, generation)
            for _ in etl_receipts._match_subjects(row_con, subject_paths, registered):
                pass
        starts, offset = {}, 0
        for name, paths in subject_paths.items():
            starts[name] = offset
            offset += sum(pq.read_metadata(path).num_rows for path in paths)
        file_receipts = select_receipts(receipts, selected / "files.parquet", dataset=FILE_STATES)
        file_states = [r["file_state"] for r in _processing_rows(file_receipts, FILE_POLICY, generation)]
        outputs = {}
        for dataset in manifest["datasets"]:
            dataset_policy = selected_policies[dataset]
            dataset_receipts = select_receipts(receipts, selected / f"{dataset}.parquet", dataset=dataset)
            states = [state for state in file_states if state["dataset"] == dataset]
            if not states or len({state["relative_path"] for state in states}) != len(states):
                raise ValueError("Missing or duplicate retained source file state")
            expected = None
            if con is not None:
                try:
                    expected = _bulk_source_rows(con, dataset, dataset_policy, subject_paths[dataset], states, starts[dataset], dataset_receipts)
                except etl_bulk.NotBulkEligible as error:
                    etl_bulk.log_row_fallback("legislative-decode", error, dataset=dataset)
                    etl_bulk.reference_fallback(error)
            if expected is None:
                if dataset_policy.receipt_only:
                    records = _processing_rows(dataset_receipts, dataset_policy, generation)
                elif con is not None:
                    records = etl_bulk._replay_admitted_subjects(con, subject_paths[dataset], dataset_policy, start=starts[dataset])
                else:
                    assert row_con is not None
                    row_con.execute("UPDATE receipts SET used=0 WHERE dataset=?", [dataset])
                    records = (subject | processing for subject, processing in etl_receipts._match_subjects(
                        row_con, {dataset: subject_paths[dataset]}, {dataset: dataset_policy}))
                expected = retained_source_rows(dataset, dataset_policy, records)
            expected = iter(expected)
            for state in states:
                if state["relative_path"] is None:
                    if len(states) != 1 or not state["partitioned"] or state["rows"] != 0:
                        raise ValueError("Invalid empty partitioned source state")
                    target = destination / dataset
                    target.mkdir(parents=True)
                    outputs[dataset] = target
                    continue
                relative = Path(dataset) / state["relative_path"] if state["partitioned"] else Path(f"{dataset}.parquet")
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError("Invalid retained member path")
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                schema = pa.ipc.read_schema(pa.BufferReader(state["schema"]))
                write_rows(itertools.islice(expected, state["rows"]), target, schema)
                if pq.read_metadata(target).num_rows != state["rows"]:
                    raise ValueError("Missing raw inputs for retained source file")
                outputs[dataset] = destination / dataset if state["partitioned"] else target
            if next(expected, None) is not None:
                raise ValueError("Extra prior receipt inputs without retained source rows")
        return outputs


def build_with_receipts(
    builder: Callable[..., Path | tuple[Path, ...]],
    source_directory: Path,
    destination: Path,
    *,
    generation_id: str,
    prior_bundle: Path | None = None,
    builder_kwargs: Mapping[str, Any] | None = None,
    source_witnesses: Sequence[Mapping] = (),
) -> dict:
    """Run an actual family producer with verified priors, then split its outputs.

    Callers supply retained/injected source readers for local-only builds.
    Download fallback is deliberately disabled: a missing pinned prior refuses
    source qualification through the producer's existing initial-build behavior.
    """
    source_directory.mkdir(parents=True, exist_ok=False)
    kwargs = dict(builder_kwargs or {})
    if {"download_prior", "download_members"} & set(kwargs):
        raise ValueError("Receipt builds select priors through prior_bundle only")
    if prior_bundle is not None:
        prior_manifest = json.loads((prior_bundle / "legislative-bundle.json").read_text())
        if prior_manifest["unowned_outputs"]:
            raise ValueError(
                "Mixed-owner producer requires the integrated family prior; owned-only rows cannot qualify it"
            )
    priors = {} if prior_bundle is None else restore_prior(prior_bundle, source_directory / ".verified-prior")

    def download_prior(key: str, target: Path) -> bool:
        name = key.removesuffix(".parquet")
        if name not in priors:
            return False
        source = priors[name]
        if source.is_dir():
            raise ValueError("Partitioned prior requested as a single file")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return True

    def download_members(key: str, target: Path) -> list[Path]:
        source = priors.get(key.removesuffix(".parquet"))
        if source is None:
            return []
        target.mkdir(parents=True, exist_ok=True)
        result = []
        for member in _members(source):
            relative = member.relative_to(source) if source.is_dir() else Path(member.name)
            output = target / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(member, output)
            result.append(output)
        return result

    if "download_members" in inspect.signature(builder).parameters:
        kwargs["download_members"] = download_members
    built = builder(source_directory, download_prior=download_prior, **kwargs)
    return write_legislative_outputs(
        built if isinstance(built, tuple) else (built,),
        destination,
        generation_id=generation_id,
        source_witnesses=source_witnesses,
        prior_receipts=() if prior_bundle is None else (prior_bundle / "etl_receipts.parquet",),
    )


def admit_bundle(bundle: Path, destination: Path, *, family: str = "legislative-documents"):
    """Qualify a local scoped generation through the shared admission path.

    Global publication combines other owners' policies and regenerates serving
    declarations. This artifact records local-partial status explicitly.
    """
    from spicy_regs.generations import build_generation, verify_generation
    from spicy_regs.native_types import described_schema

    manifest = json.loads((bundle / "legislative-bundle.json").read_text())
    files, partitions = [], {}
    for dataset, members in manifest["subjects"].items():
        if policy(dataset).receipt_only:
            continue
        if members and members[0].startswith(dataset + "/"):
            files.append(bundle / dataset)
            partitions[dataset + ".parquet"] = tuple(p.split("=", 1)[0] for p in Path(members[0]).parts[1:-1])
        else:
            files.append(bundle / f"{dataset}.parquet")
    for members in manifest.get("outcome_subjects", {}).values():
        files.extend(bundle / path for path in members)
    policies = ([policy(n) for n in manifest["datasets"]] + [FILE_POLICY] +
                [OUTCOME_POLICIES[n] for n in manifest.get("outcome_subjects", {})])
    artifact = build_generation(
        destination,
        family=family,
        files=files,
        expected_keys=tuple(p.dataset + ".parquet" for p in policies if not p.receipt_only),
        schemas={p.dataset: described_schema(p.subject_schema) for p in policies if not p.receipt_only},
        partitioned=partitions,
        publication_status="local-partial",
        receipt_path=bundle / manifest["receipt_file"],
        receipt_policies=policies,
        receipt_generation_id=manifest["generation_id"],
    )
    verify_generation(destination, expected_pin=artifact.pin)
    return artifact
