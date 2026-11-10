"""Native regulatory file generations and generation-bound retry checkpoints."""

from __future__ import annotations

import json
import hashlib
import os
import shutil

from loguru import logger
import pyarrow.parquet as pq
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from spicy_regs.etl_receipts import ReceiptContext, combine_receipts
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication, r2
from spicy_regs.selected_generations import SelectedInputs, SelectedDataset, remember_selection
from spicy_regs.transforms.regulations_checkpoints import checkpoint_policy, read_checkpoint, write_checkpoint
from spicy_regs.transforms.regulations_receipts import ReceiptInput, build_local_generation, materialize_internal


def restore_dataset(root: Path, dataset: str, destination: Path, *, index=None, inputs=None) -> bool:
    """Restore only an explicitly selected native generation, never a loose file."""
    root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=root, prefix='.selected-') as temporary:
        inputs = inputs or SelectedInputs(root, temporary, index=index, public_url=None if index is not None else '')
        selected = inputs.select(dataset)
        if selected is None:
            return False
        receipts = selected.required_receipts("the regulatory restore")
        materialize_internal(ReceiptInput(dataset, selected.subjects, receipts, selected.generation_id),
                             destination, bulk=True)
        return True


def _publish(directory, *, inputs):
    if inputs is None or inputs.index is None:
        raise ValueError('Publication requires the captured native input selection used by this build')
    r2.require_credentials('Regulatory generation publication')
    return publication.publish_generation(
        directory, client=r2.get_r2_client(), bucket=os.getenv('R2_BUCKET_NAME', 'spicy-regs'),
        prior_index=inputs.index)


def _remember(root, datasets, directory, generation, *, inputs=None, published=None):
    """Select what this write built for the rest of the run; ``generation`` is ``None`` for a table without receipts."""
    selections = []
    for dataset in datasets:
        subject = directory / (dataset + '.parquet')
        selections.append(SelectedDataset(dataset, (subject,) if subject.exists() else (),
                                           directory / 'etl_receipts.parquet' if generation else None, generation))
    remember_selection(root, selections)
    if inputs is not None:
        inputs.cache.update({selected.dataset: selected for selected in selections})
        if published is not None:
            from copy import deepcopy
            inputs.index = deepcopy(inputs.index)
            for dataset in datasets:
                # Advance only families this write committed, preserving the
                # prior selections actually read for all other datasets. A table
                # whose receipts are retired is found by its table, not its receipts.
                owner = (next((name for name, entry in published['families'].items()
                               if dataset in entry.get('etlReceipts', {}).get('datasets', ())), None) if generation
                         else (publication.table_owner(published, dataset + '.parquet') or (None,))[0])
                if owner is None:
                    raise ValueError('Published dataset is absent from its receipt family')
                inputs.index['families'][owner] = published['families'][owner]


def finish_dataset(root: Path, dataset: str, source: Path, *, publish: bool, inputs=None):
    if publish and (inputs is None or inputs.index is None):
        raise ValueError('Publication requires the captured native input selection used by this build')
    index = inputs.index if inputs is not None and inputs.index is not None else publication.empty_index()
    owner = publication.table_owner(index, dataset + ".parquet")
    family = owner[0] if owner else dataset.replace("_", "-")
    directory = root / "generations" / uuid4().hex
    selected_inputs = inputs or SelectedInputs(root, root / '.selected-write' / uuid4().hex, public_url='')
    prior = selected_inputs.select(dataset)
    if prior is not None:
        from spicy_regs.etl_receipts import select_receipts, selected_subject_policy, validate_receipt_bundle
        from spicy_regs.transforms.regulations_receipts import policy
        with TemporaryDirectory(prefix='.regulatory-prior-', dir=root) as temporary:
            scoped = select_receipts(prior.required_receipts('the regulatory writer'),
                                     Path(temporary) / 'receipts.parquet', dataset=dataset)
            validate_receipt_bundle({dataset: prior.subjects}, [scoped],
                [selected_subject_policy(policy(dataset), prior.subjects)], generation_id=prior.generation_id)
    artifact = build_local_generation(
        {dataset: source}, directory, generation_id=uuid4().hex, family=family, publication_status="complete-family",
        prior_receipts={dataset: [prior.receipts]} if prior is not None else {}
    )
    from spicy_regs.transforms.regulations_receipts import CARRY_DATASETS
    if dataset in CARRY_DATASETS:
        # Carry preserves historical refusals. A refused current source row is instead proved by the missing
        # corresponding subject: the regulatory writer makes exactly one accepted subject or refusal per input.
        if pq.ParquetFile(source).metadata.num_rows != pq.ParquetFile(directory / (dataset + '.parquet')).metadata.num_rows:
            raise ValueError(f'{dataset}: native conversion refused; checkpoint remains unadvanced')
    return _seal(root, dataset, directory, artifact, publish=publish, inputs=inputs,
                 check_refusals=dataset not in CARRY_DATASETS)


def _seal(root, dataset, directory, artifact, *, publish, inputs, check_refusals):
    """Publish (when asked) and remember a built generation of one dataset."""
    if check_refusals:
        # A current source conversion must be complete.
        for batch in pq.ParquetFile(directory / 'etl_receipts.parquet').iter_batches(columns=['outcome']):
            if any(value in {'refused', 'error'} for value in batch.column(0).to_pylist()):
                raise ValueError(f'{dataset}: native conversion refused; checkpoint remains unadvanced')
    published = _publish(directory, inputs=inputs) if publish else None
    _remember(root, [dataset], directory, artifact.root['spec'].get('etlReceipts', {}).get('generationId'),
              inputs=inputs, published=published)
    shutil.copyfile(directory / (dataset + ".parquet"), root / (dataset + ".parquet"))
    return directory / (dataset + ".parquet")


def finish_attribute_dataset(root: Path, dataset: str, fresh: Path, *, publish: bool, inputs):
    """Upsert the day's attribute rows (``fresh``, in the contract's columns) over the selected native table.

    Design B: the selected prior subject and receipt files are kept, receipts are written only for ``fresh``'s rows, and
    :func:`native_merge` upserts them (an unchanged refetch keeps its prior receipt, a changed row references its
    predecessor, a kept row keeps its receipt verbatim). Nothing is restored to rows. The one validation is the
    generation's admission in ``build_generation``.
    """
    from spicy_regs.etl_receipts import select_receipts, validate_receipt_bundle
    from spicy_regs.native_merge import native_merge
    from spicy_regs.native_types import described_schema
    from spicy_regs.subject_catalog import retired_receipt_datasets
    from spicy_regs.transforms.regulations_receipts import ATTRIBUTE_DATASETS, policy, refusal_identity, write_held_dataset

    if dataset not in ATTRIBUTE_DATASETS:
        raise ValueError('Attribute upsert requires an attribute dataset')
    if publish and (inputs is None or inputs.index is None):
        raise ValueError('Publication requires the captured native input selection used by this build')
    declared = policy(dataset)
    prior = inputs.select(dataset)
    if prior is None or len(prior.subjects) != 1:
        raise ValueError(f'{dataset}: the selected native table is required to upsert the day\'s rows')
    if not pq.read_schema(prior.subjects[0]).equals(declared.subject_schema):
        raise ValueError(f'{dataset}: the selected native table has another schema than its policy; convert it first')
    index = inputs.index if inputs.index is not None else publication.empty_index()
    owner = publication.table_owner(index, dataset + ".parquet")
    family = owner[0] if owner else dataset.replace("_", "-")
    directory = root / "generations" / uuid4().hex
    generation = uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    if dataset in retired_receipt_datasets():
        return _finish_retired_attributes(root, dataset, fresh, prior.subjects[0], directory, family=family,
                                          generation=generation, publish=publish, inputs=inputs)
    with TemporaryDirectory(dir=root, prefix='.attribute-merge-') as temporary:
        work = Path(temporary)
        scoped = select_receipts(prior.required_receipts('the attribute upsert'), work / 'prior-receipts.parquet',
                                 dataset=dataset)
        validate_receipt_bundle({dataset: prior.subjects}, [scoped], [declared], generation_id=prior.generation_id)
        metadata = _attribute_metadata(scoped)
        source = fresh
        if (pq.read_schema(fresh).metadata or {}) != metadata:
            source = work / 'fresh.parquet'
            with pq.ParquetFile(fresh) as held, pq.ParquetWriter(
                    source, pq.read_schema(fresh).with_metadata(metadata), compression='zstd') as writer:
                for batch in held.iter_batches(batch_size=10_000):
                    writer.write_batch(batch.replace_schema_metadata(metadata))
        fresh_pair = write_held_dataset(dataset, source, work / 'fresh', generation_id=generation, bulk=True, validate=False)
        for batch in pq.ParquetFile(fresh_pair[1]).iter_batches(columns=['outcome']):
            if any(value in {'refused', 'error'} for value in batch.column(0).to_pylist()):
                raise ValueError(f'{dataset}: native conversion refused; checkpoint remains unadvanced')
        merged = native_merge(declared, fresh=fresh_pair, prior=(prior.subjects[0], scoped),
                              order_by=declared.identity_fields, subjects=work / f'{dataset}.parquet',
                              receipts=work / 'etl_receipts.parquet', refusal_identity=refusal_identity(dataset))
        logger.info('{}: {:,} rows kept, {:,} replaced, {:,} added', dataset, merged.kept, merged.replaced, merged.added)
        artifact = build_generation(
            directory, family=family, files=[merged.subjects], expected_keys=[dataset + ".parquet"],
            schemas={dataset: described_schema(declared.subject_schema)}, publication_status="complete-family",
            receipt_path=merged.receipts, receipt_policies=[declared], receipt_generation_id=generation)
    return _seal(root, dataset, directory, artifact, publish=publish, inputs=inputs, check_refusals=False)


def _finish_retired_attributes(root, dataset, fresh, prior, directory, *, family, generation, publish, inputs):
    """:func:`finish_attribute_dataset` for a table whose receipts are retired (ss-vse3): a plain table upsert.

    The day's rows are converted by the receipt route's own writer, so their native values are exactly its; its
    receipts for them are only the refusal check and are never published. The published prior table is never read
    through receipts: its rows the day did not touch are carried as they are (``native_merge.upsert_subjects``), and
    the generation states no receipts. What this no longer does, per batch, on comment_attributes' 26.5 million rows:
    select and validate the prior receipts (about 16 minutes), merge them (most of 5), admit them (most of 12) and
    upload the 4.2 GiB receipt member (docs/comment-attributes-receipt-archive.md).
    """
    from spicy_regs.native_merge import upsert_subjects
    from spicy_regs.native_types import described_schema
    from spicy_regs.transforms.regulations_receipts import policy, write_held_dataset

    declared = policy(dataset)
    with TemporaryDirectory(dir=root, prefix='.attribute-merge-') as temporary:
        work = Path(temporary)
        fresh_subjects, fresh_receipts = write_held_dataset(dataset, fresh, work / 'fresh', generation_id=generation,
                                                            bulk=True, validate=False)
        for batch in pq.ParquetFile(fresh_receipts).iter_batches(columns=['outcome']):
            if any(value in {'refused', 'error'} for value in batch.column(0).to_pylist()):
                raise ValueError(f'{dataset}: native conversion refused; checkpoint remains unadvanced')
        merged = upsert_subjects(declared.subject_schema, identity=declared.identity_fields, fresh=fresh_subjects,
                                 prior=prior, order_by=declared.identity_fields, subjects=work / f'{dataset}.parquet')
        logger.info('{}: {:,} rows kept, {:,} replaced, {:,} added', dataset, merged.kept, merged.replaced, merged.added)
        artifact = build_generation(
            directory, family=family, files=[merged.subjects], expected_keys=[dataset + ".parquet"],
            schemas={dataset: described_schema(declared.subject_schema)}, publication_status="complete-family")
    return _seal(root, dataset, directory, artifact, publish=publish, inputs=inputs, check_refusals=False)


def _attribute_metadata(receipts):
    """Prove uniform input metadata without decoding every receipt; empty tables use their observation."""
    import pyarrow as pa
    from spicy_regs import etl_bulk
    from spicy_regs.etl_receipts import decode_exact_json

    item = ("json_extract(list_extract(list_filter(json_extract(processing_json, '$[1]')::JSON[], "
            "entry -> json_extract_string(entry, '$[0]') = 'input_metadata'), 1), '$[1]')")
    kind = item.replace('processing_json', 'diagnostic_json').replace("'input_metadata'", "'kind'")
    with etl_bulk.bulk_connection() as (con, _):
        # Accepted receipts are the same per-row metadata the maintained restore checks. With no subjects,
        # only the input-file observation can supply it. LIMIT bounds distinct metadata returned to Python.
        query = (f"WITH r AS (SELECT * FROM read_parquet({etl_bulk._literal(str(receipts))})) "
                 f"SELECT DISTINCT CAST({item} AS VARCHAR) FROM r WHERE outcome='accepted' OR "
                 f"(outcome='observed' AND json_extract_string({kind}, '$[1]')='input_file_metadata' "
                 "AND NOT EXISTS (SELECT 1 FROM r WHERE outcome='accepted')) LIMIT 2")
        values = [decode_exact_json(text) if text is not None else {} for text, in con.execute(query).fetchall()]
    if len(values) > 1:
        raise ValueError('Input metadata differs across selected receipts')
    return pa.schema([], metadata=values[0] if values else {}).metadata or {}


def restore_checkpoint(root: Path, dataset: str, *, index=None, inputs=None) -> list[dict]:
    root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=root, prefix='.checkpoint-') as temporary:
        inputs = inputs or SelectedInputs(root, temporary, index=index, public_url=None if index is not None else '')
        selected = inputs.select(dataset)
        if selected is None:
            return []
        if selected.subjects:
            raise ValueError('Checkpoint selection contains subject files')
        return read_checkpoint(dataset, selected.required_receipts('the checkpoint reader'),
                               generation_id=selected.generation_id)


def finish_checkpoints(root: Path, checkpoints: dict[str, list[dict]], *, publish: bool, witnesses=(), inputs=None):
    """Commit complete retry sets together, including explicit empty retirement."""
    if publish and (inputs is None or inputs.index is None):
        raise ValueError('Publication requires the captured native input selection used by this build')
    root.mkdir(parents=True, exist_ok=True)
    generation = uuid4().hex
    directory = root / "generations" / generation
    with TemporaryDirectory(dir=root, prefix=".checkpoints-") as temp:
        work = Path(temp)
        evidence = root / ".checkpoint-evidence" / (generation + ".json")
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(checkpoints, sort_keys=True, separators=(",", ":")))
        witness = {
            "source_id": "regulatory-retry-state",
            "source_uri": None,
            "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
            "locator": None,
            "body_version": generation,
        }
        context = ReceiptContext(generation, "checkpoint", "spicy-regs:regulatory-retries-v1", [witness, *witnesses])
        shards = [write_checkpoint(name, rows, work / name, context) for name, rows in checkpoints.items()]
        receipt = combine_receipts(shards, work / "etl_receipts.parquet")
        build_generation(
            directory,
            family="regulatory-checkpoints",
            files=[],
            expected_keys=[],
            schemas={},
            publication_status="complete-family",
            receipt_path=receipt,
            receipt_policies=[checkpoint_policy(name) for name in checkpoints],
            receipt_generation_id=generation,
        )
    published = _publish(directory, inputs=inputs) if publish else None
    _remember(root, list(checkpoints), directory, generation, inputs=inputs, published=published)
    return directory
