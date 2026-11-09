"""Court builders use the shared receipt writer and exact internal receipt reads."""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import court_bulk, etl_bulk
from spicy_regs.receipt_history import RefusalIdentity, carry_receipt_history
from spicy_regs.court_subjects import (
    IDENTITIES, RECEIPT_FIELDS, SUBJECT_SCHEMAS, normalize_court_row, opinion_body_id,
)
from spicy_regs.etl_receipts import (
    DatasetPolicy, RECEIPT_SCHEMA, ReceiptContext, combine_receipts, failure_receipt,
    read_with_receipts, select_receipts, split_record, validate_receipt_bundle, write_dataset,
)

POLICIES = {
    name: DatasetPolicy(name, schema, IDENTITIES[name], RECEIPT_FIELDS[name], policy_version='courts/2' if name in {'court_opinions', 'court_opinion_pdf_extractions'} else 'courts/1')
    for name, schema in SUBJECT_SCHEMAS.items()
}


def file_witness(path: Path, *, source_id: str | None = None) -> dict:
    """Pin retained bytes, not a mutable filename or inferred edition."""
    with path.open('rb') as stream:
        digest = 'sha256:' + hashlib.file_digest(stream, 'sha256').hexdigest()
    return {'source_id': source_id or path.name, 'source_uri': None,
            'sha256': digest, 'locator': None, 'body_version': None}


def parquet_rows(path: Path) -> Iterable[dict]:
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def record_source_failure(dataset: str, output_dir: Path, *, witnesses: Sequence[Mapping], error: Exception) -> Path:
    """Retain a failed build attempt without selecting its empty subject file."""
    context = ReceiptContext(uuid4().hex, 'source-read', 'spicy_regs/courts/1', witnesses,
                             {'error_type': type(error).__name__, 'error': str(error)})
    directory = output_dir / '.court-etl' / ('failed-' + uuid4().hex)
    failure = failure_receipt(POLICIES[dataset], context, outcome='error', raw_fields={})
    _, receipt = write_dataset((), directory, POLICIES[dataset], failures=[failure])
    return receipt


def _pdf_failure(row: Mapping) -> str | None:
    if row.get('sha1_matches') not in ('true', True):
        return 'native_digest_mismatch'
    diagnostics = json.loads(row.get('pdf_extraction_results_json') or '[]')
    if not isinstance(diagnostics, list) or len(diagnostics) != 1:
        return 'missing_extraction_diagnostic'
    status = diagnostics[0].get('status')
    return None if status == 'ok' and isinstance(row.get('text_content'), str) else status or 'missing_body'


def _refusal(policy, context, error, raw):
    failed = replace(context, diagnostics={**context.diagnostics, 'error_type': type(error).__name__, 'error': str(error)})
    return failure_receipt(policy, failed, outcome='refused', raw_fields={'raw_source_record': dict(raw)})


_REFUSED = (ValueError, TypeError, KeyError, pa.ArrowException)


def _mapped(dataset: str, raw: Mapping) -> dict:
    """The mapped row; a refused one raises ``_REFUSED``."""
    mapped = normalize_court_row(dataset, raw)
    reason = _pdf_failure(mapped) if dataset == 'court_opinion_pdf_extractions' else None
    if reason:
        raise ValueError(f'Opinion body unavailable: {reason}')
    return mapped


def _expose(dataset: str, output_dir: Path, generation_id: str, witnesses: Sequence[Mapping], build) -> Path:
    """Build a bundle with ``build(bundle) -> subject path``, then point the public name at it.

    The public local filename points into the immutable shared-writer bundle. Replacing that one link keeps a
    previous subject and its receipts together on a partial source read, failed conversion, or interrupted retry.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle = output_dir / '.court-etl' / uuid4().hex
    bundle.parent.mkdir(exist_ok=True)
    try:
        subject = build(bundle)
    except Exception as error:
        try:
            record_source_failure(dataset, output_dir, witnesses=witnesses, error=error)
        except Exception as receipt_error:
            error.add_note(f'Failed to retain source failure receipt: {receipt_error}')
        raise
    assert subject is not None
    (bundle / 'court-build.json').write_text(json.dumps({'generation_id': generation_id, 'dataset': dataset}) + '\n')
    public = output_dir / (dataset + '.parquet')
    pointer = output_dir / ('.' + dataset + '.' + uuid4().hex + '.partial')
    try:
        pointer.symlink_to(os.path.relpath(subject, output_dir))
        pointer.replace(public)
    finally:
        pointer.unlink(missing_ok=True)
    # Generation admission accepts regular immutable files, never mutable links.
    return public.resolve()


def _check_refused(receipt: Mapping, dataset: str, generation_id: str) -> bool:
    if receipt['dataset'] != dataset or receipt['generation_id'] != generation_id:
        raise ValueError('Rejected updates differ from the selected court build')
    return receipt['outcome'] == 'refused'


def write_court_rows(
    dataset: str, rows: Iterable[Mapping], output_dir: Path, *, witnesses: Sequence[Mapping],
    generation_id: str | None = None, processor: str = 'spicy_regs/courts/1',
    diagnostics: Mapping | None = None, refused_receipts: Path | None = None,
    attempt_prefix: str = "row", prior_receipts: Path | None = None,
    resolution_receipts: Path | None = None,
) -> Path:
    """Expose a validated pair; optional fresh admission proves refusal resolution.

    ``resolution_receipts`` does not append failures. It can therefore be supplied
    separately when failed fresh inputs are already present in ``rows``.
    Otherwise the admitted ``refused_receipts`` file supplies fresh success keys.
    """
    policy = POLICIES[dataset]
    generation_id = generation_id or uuid4().hex

    def build(bundle: Path) -> Path:
        with TemporaryDirectory(prefix='.court-failures-', dir=output_dir) as temporary:
            failures_path = Path(temporary) / 'etl_receipts.parquet'
            with pq.ParquetWriter(failures_path, RECEIPT_SCHEMA, compression='zstd') as writer:
                failures = []

                def records():
                    for ordinal, raw in enumerate(rows):
                        context = ReceiptContext(generation_id, f'{dataset}:{attempt_prefix}:{ordinal}', processor,
                            [dict(w) for w in witnesses],
                            diagnostics or {})
                        try:
                            # Validate before inheriting evidence or replacing a subject.
                            mapped = _mapped(dataset, raw)
                            split_record(policy, mapped, context)
                        except _REFUSED as error:
                            failures.append(_refusal(policy, context, error, raw))
                            if len(failures) >= 2000:
                                writer.write_table(pa.Table.from_pylist(failures, schema=RECEIPT_SCHEMA))
                                failures.clear()
                        else:
                            yield mapped, context
                    if failures:
                        writer.write_table(pa.Table.from_pylist(failures, schema=RECEIPT_SCHEMA))
                        failures.clear()
                    if refused_receipts is not None:
                        for receipt in parquet_rows(refused_receipts):
                            if _check_refused(receipt, dataset, generation_id):
                                writer.write_table(pa.Table.from_pylist([receipt], schema=RECEIPT_SCHEMA))
                    writer.close()

                subject, receipt = write_dataset(records(), bundle, policy, failures=parquet_rows(failures_path),
                                                  validate=False)
        assert subject is not None
        if prior_receipts is not None:
            _carry(dataset, receipt, prior_receipts,
                   resolution_receipts=resolution_receipts if resolution_receipts is not None else refused_receipts)
        validate_receipt_bundle({dataset: [subject]}, [receipt], [policy], generation_id=generation_id)
        return subject

    return _expose(dataset, output_dir, generation_id, witnesses, build)


def write_court_bulk(
    dataset: str, source: Path, output_dir: Path, *, witnesses: Sequence[Mapping],
    generation_id: str | None = None, processor: str = 'spicy_regs/courts/1',
    diagnostics: Mapping | None = None, refused_receipts: Path | None = None,
    attempt_prefix: str = "row", prior_receipts: Path | None = None,
) -> Path:
    """``write_court_rows`` over a staged Parquet source, a column at a time (``court_bulk``).

    Unchanged rows keep their prior receipt (``carry_receipt_history``, as ``write_dataset(prior_receipts=)``).
    No receipt is validated here: the generation's admission is the one validation.
    """
    from rulespec_artifacts import publish_directory_no_replace

    generation_id = generation_id or uuid4().hex

    def build(bundle: Path) -> Path:
        assert generation_id is not None
        with TemporaryDirectory(prefix='.court-bulk-', dir=bundle.parent) as temporary:
            stage = Path(temporary) / 'bundle'
            stage.mkdir()
            subject_path, receipt_path = stage / (dataset + '.parquet'), stage / 'etl_receipts.parquet'
            _write_bulk_pair(dataset, source, subject_path, receipt_path,
                generation_id=generation_id, processor=processor, witnesses=witnesses,
                diagnostics=diagnostics, attempt_prefix=attempt_prefix, refused_receipts=refused_receipts)
            if prior_receipts is not None:
                _carry(dataset, receipt_path, prior_receipts, resolution_receipts=refused_receipts)
            publish_directory_no_replace(stage, bundle)
        return bundle / subject_path.name

    return _expose(dataset, output_dir, generation_id, witnesses, build)


def _write_bulk_pair(dataset, source, subject, receipt, *, generation_id, witnesses,
                     processor='spicy_regs/courts/1', diagnostics=None, attempt_prefix='row',
                     refused_receipts=None, receipt_writer=None, subject_writer=None, segments=None):
    policy = POLICIES[dataset]

    def attempt(raw: Mapping, ordinal: int):
        context = ReceiptContext(generation_id, f'{dataset}:{attempt_prefix}:{ordinal}', processor,
                                 [dict(w) for w in witnesses], diagnostics or {})
        try:
            return split_record(policy, _mapped(dataset, raw), context)
        except _REFUSED as error:
            return None, _refusal(policy, context, error, raw)

    return court_bulk.write_bundle(dataset, source, subject, receipt, policy=policy,
        generation_id=generation_id, processor=processor, witnesses=witnesses,
        diagnostics=diagnostics or {}, attempt_prefix=attempt_prefix, attempt=attempt,
        receipt_writer=receipt_writer, subject_writer=subject_writer, segments=segments,
        last_receipts=() if refused_receipts is None else (
            row for row in parquet_rows(refused_receipts) if _check_refused(row, dataset, generation_id)))


@contextmanager
def court_bulk_family(output_dir: Path, datasets: Sequence[str], *, generation_id: str):
    """Build fresh whole-family outputs privately, with one receipt member and no prior-history carry.

    The yielded writer returns final private paths. They exist only after every table and the receipt footer
    finish, and remain unselected until ordinary generation admission. Retained sources must live outside this
    temporary bundle. Standalone builders keep their own selected subject/receipt pairs.
    """
    from rulespec_artifacts import publish_directory_no_replace

    datasets = tuple(datasets)
    if not datasets or len(set(datasets)) != len(datasets) or not set(datasets) <= court_bulk.DATASETS:
        raise ValueError('Court bulk family requires distinct mapped datasets')
    bundle = output_dir / '.court-etl' / uuid4().hex
    bundle.parent.mkdir(parents=True, exist_ok=True)
    current, current_witnesses = datasets[0], ()
    completed = False
    try:
        with TemporaryDirectory(prefix='.court-family-', dir=bundle.parent) as temporary:
            stage = Path(temporary) / 'bundle'
            stage.mkdir()
            receipt = stage / 'etl_receipts.parquet'
            written = []
            with etl_bulk.new_receipt_writer(receipt) as writer:
                def write(dataset, source, *, witnesses):
                    nonlocal current, current_witnesses
                    current = dataset
                    current_witnesses = witnesses
                    if len(written) >= len(datasets) or dataset != datasets[len(written)]:
                        raise ValueError('Court bulk family tables differ from the declared order')
                    try:
                        _write_bulk_pair(dataset, source, stage / (dataset + '.parquet'), receipt,
                                         generation_id=generation_id, witnesses=witnesses, receipt_writer=writer)
                    except Exception as error:
                        try:
                            record_source_failure(dataset, output_dir, witnesses=witnesses, error=error)
                        except Exception as receipt_error:
                            error.add_note(f'Failed to retain source failure receipt: {receipt_error}')
                        raise
                    written.append(dataset)
                    return bundle / (dataset + '.parquet')

                yield write
                if tuple(written) != datasets:
                    raise ValueError('Court bulk family is incomplete')
                completed = True
            (stage / 'court-family.json').write_text(json.dumps({
                'generation_id': generation_id, 'datasets': datasets}) + '\n')
            publish_directory_no_replace(stage, bundle)
    except Exception as error:
        if completed:
            try:
                record_source_failure(current, output_dir, witnesses=current_witnesses, error=error)
            except Exception as receipt_error:
                error.add_note(f'Failed to retain source failure receipt: {receipt_error}')
        raise


def _pdf_refusal_identity(raw: Mapping) -> dict | None:
    """Prove a refused body key by the same function used by its successful mapper."""
    identity = opinion_body_id(raw['opinion_id'], raw['source_sha256'])
    if raw.get('opinion_body_id') not in (None, identity):
        return None
    return {'opinion_body_id': identity}


def _carry(dataset: str, current: Path, prior: Path, *, resolution_receipts: Path | None = None) -> None:
    # Match literal court keys to accepted declared identities. PDF body keys
    # alone use the maintained derivation from the exact opinion id and digest.
    carry_receipt_history(current, [prior], current, resolution_receipts=resolution_receipts, refusal_identities={
        dataset: RefusalIdentity(IDENTITIES[dataset], None, "raw_source_record",
                                _pdf_refusal_identity if dataset == 'court_opinion_pdf_extractions' else None)})


def finish_court_output(dataset: str, staged: Path, output_dir: Path, *, witnesses=None,
                        generation_id: str | None = None, diagnostics: Mapping | None = None,
                        refused_receipts: Path | None = None, prior_receipts: Path | None = None,
                        attempt_prefix: str = 'row') -> Path:
    """Split a completed private mapper output; keep its bytes as retained evidence.

    The tables ``court_bulk`` maps go through it, a column at a time. Any other table is the row writer's, and
    says so: a scheduled run (``bulk_limits(require_bulk=True)``) refuses a mappable table that cannot be bulk.
    """
    witnesses = witnesses or [file_witness(staged)]
    schema = pq.read_schema(staged)
    if court_bulk.eligible(dataset, schema):
        return write_court_bulk(dataset, staged, output_dir, witnesses=witnesses, generation_id=generation_id,
                                diagnostics=diagnostics, refused_receipts=refused_receipts, prior_receipts=prior_receipts,
                                attempt_prefix=attempt_prefix)
    reason = etl_bulk.NotBulkEligible(f'{dataset} has no court bulk mapping for this source schema')
    if dataset in court_bulk.DATASETS:
        etl_bulk.log_row_fallback('court write', reason, dataset=dataset, rows=pq.ParquetFile(staged).metadata.num_rows)
    else:
        etl_bulk.log_row_eligibility('court write', dataset=dataset,
            rows=pq.ParquetFile(staged).metadata.num_rows, reason=str(reason))
    return write_court_rows(dataset, parquet_rows(staged), output_dir,
                            witnesses=witnesses, generation_id=generation_id,
                            diagnostics=diagnostics, refused_receipts=refused_receipts, prior_receipts=prior_receipts,
                            attempt_prefix=attempt_prefix)


def admit_court_updates(dataset: str, source: Path, directory: Path, *, schema: pa.Schema,
                        generation_id: str) -> tuple[Path, Path]:
    """Validate updates before merging identities; return accepted inputs and all refusal receipts."""
    subject = finish_court_output(dataset, source, directory,
        witnesses=[file_witness(source)], generation_id=generation_id, attempt_prefix='incoming')
    receipts, _ = local_receipt_selection(subject)
    accepted = restore_processing_input(subject, directory / 'accepted-inputs.parquet',
        dataset=dataset, schema=schema)
    return accepted, receipts


def local_receipt_selection(path: Path) -> tuple[Path, str]:
    bundle = path.resolve().parent
    descriptor = bundle / 'court-build.json'
    family = bundle / 'court-family.json'
    if family.exists() and (bundle / 'etl_receipts.parquet').is_file():
        values = json.loads(family.read_text())
        if path.resolve().stem not in values['datasets']:
            raise ValueError('Court receipt family names another dataset')
        return bundle / 'etl_receipts.parquet', values['generation_id']
    if not descriptor.exists() or not (bundle / 'etl_receipts.parquet').is_file():
        raise ValueError(f'Court subject has no selected receipt bundle: {path}')
    values = json.loads(descriptor.read_text())
    if values['dataset'] != path.resolve().stem:
        raise ValueError('Court receipt bundle names another dataset')
    return bundle / 'etl_receipts.parquet', values['generation_id']


def read_court_rows(path: Path, *, dataset: str, receipt_path: Path | None = None,
                    generation_id: str | None = None) -> Iterable[dict]:
    """Internal processing read; a migrated row never falls back to missing evidence."""
    from spicy_regs.etl_receipts import selected_subject_policy
    try:
        selected_policy = selected_subject_policy(POLICIES[dataset], [path])
    except ValueError as error:
        raise ValueError('Court prior must use the native subject schema and selected receipts') from error
    if receipt_path is None:
        receipt_path, generation_id = local_receipt_selection(path)
    if generation_id is None:
        raise ValueError('Court internal read requires the selected receipt generation')
    with TemporaryDirectory(prefix='court-read-') as temporary:
        selected = select_receipts(receipt_path, Path(temporary) / 'etl_receipts.parquet', dataset=dataset)
        yield from read_with_receipts([path], [selected], selected_policy, generation_id=generation_id)


def processing_rows(rows, *, dataset: str, schema: pa.Schema):
    """Decode the maintained court mapper input from admitted exact matching rows."""
    names = tuple(schema.names)
    string_fields = tuple(field.name for field in schema if pa.types.is_string(field.type))

    def restored():
        for row in rows:
            row = dict(row)
            row.update(row.get('conversion_inputs') or {})
            if dataset == 'court_dockets':
                for name in ('parties', 'attorneys', 'firms'):
                    if name + '_json' not in row:
                        row[name + '_json'] = json.dumps(row.get(name)) if row.get(name) is not None else None
            if dataset == 'court_opinion_clusters':
                value = row.get('attorneys')
                row['attorneys'] = [value] if isinstance(value, str) else value
            for name in string_fields:
                value = row.get(name)
                if value is not None and not isinstance(value, str):
                    row[name] = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
            yield {name: row.get(name) for name in names}
    yield from restored()



def restore_processing_input(path: Path, destination: Path, *, dataset: str, schema: pa.Schema,
                             receipt_path: Path | None = None, generation_id: str | None = None) -> Path:
    """Reconstruct the mapper's private merge columns, preserving exact raw literals."""
    from spicy_regs.transforms.parquet_rows import write_rows
    from spicy_regs.conversion_reads import _open_family_reader
    from spicy_regs.etl_receipts import selected_subject_policy
    from spicy_regs.local_data import file_signature
    from spicy_regs import court_restore

    if court_bulk.eligible(dataset, schema):
        if receipt_path is None:
            receipt_path, generation_id = local_receipt_selection(path)
        if not generation_id:
            raise ValueError('Court internal read requires the selected receipt generation')
        # Capture the immutable local bundle selected by the public link once.
        path = path.resolve(strict=True)
        policy = selected_subject_policy(POLICIES[dataset], [path])
        paths = (path, receipt_path)
        before = [file_signature(member) for member in paths]

        def guard():
            if [file_signature(member) for member in paths] != before:
                raise ValueError('Selected court input changed during restoration')

        with _open_family_reader({dataset: [path]}, [receipt_path], [policy], generation_id=generation_id,
                                 guard=guard, scoped=True) as reader:
            try:
                return court_restore.materialize(reader, dataset, schema, destination)
            except etl_bulk.NotBulkEligible as error:
                etl_bulk.log_row_fallback('court restore', error, dataset=dataset,
                                         rows=pq.read_metadata(path).num_rows)
                return write_rows(processing_rows(reader.read_subjects(dataset), dataset=dataset, schema=schema),
                                  destination, schema)

    reason = etl_bulk.NotBulkEligible(f'{dataset} has no court bulk restore for this input schema')
    rows = pq.read_metadata(path).num_rows
    if dataset in court_bulk.DATASETS:
        etl_bulk.log_row_fallback('court restore', reason, dataset=dataset, rows=rows)
    else:
        etl_bulk.log_row_eligibility('court restore', dataset=dataset, rows=rows, reason=str(reason))
    return write_rows(processing_rows(
        read_court_rows(path, dataset=dataset, receipt_path=receipt_path, generation_id=generation_id),
        dataset=dataset, schema=schema), destination, schema)

def prior_receipt_selection(path: Path, *, dataset: str) -> tuple[Path | None, str | None]:
    """Select receipts from a local bundle or the same pinned publication as the prior."""
    if path.is_symlink() or (path.parent / 'court-build.json').exists() or (path.parent / 'court-family.json').exists():
        return local_receipt_selection(path)
    from spicy_regs.sources import publication
    base = os.getenv('R2_PUBLIC_URL')
    if not base:
        return None, None
    index = publication.current_index(base)
    owner = publication.table_owner(index, dataset + '.parquet')
    if owner is None or 'etlReceipts' not in owner[1]:
        return None, None
    member, = publication.receipt_members(index, dataset=dataset)
    destination = path.parent / ('.' + dataset + '.selected-receipts-' + uuid4().hex + '.parquet')
    if not publication.fetch_member(base, member, destination, dataset + ' receipts'):
        raise ValueError('Selected court receipts are missing')
    return destination, owner[1]['etlReceipts']['generationId']


def generation_options(paths: Sequence[Path]) -> dict:
    """Arguments for shared build_generation; receipts are never ordinary subject outputs."""
    resolved = [path.resolve() for path in paths]
    families = {path.parent for path in resolved if (path.parent / 'court-family.json').exists()}
    if families:
        if len(families) != 1:
            raise ValueError('Court outputs mix receipt families')
        bundle, = families
        values = json.loads((bundle / 'court-family.json').read_text())
        expected = [bundle / (dataset + '.parquet') for dataset in values['datasets']]
        if resolved != expected or len(set(resolved)) != len(resolved):
            raise ValueError('Court outputs differ from the complete receipt family')
        receipt, generation = local_receipt_selection(resolved[0])
        return {'receipt_path': receipt, 'receipt_policies': [POLICIES[path.stem] for path in resolved],
                'receipt_generation_id': generation, 'adopt_owned_receipt': True}
    selections = [local_receipt_selection(path) for path in paths]
    ids = {generation for _, generation in selections}
    if len(ids) != 1:
        raise ValueError('Court build outputs do not share one receipt generation')
    directory = paths[0].parent / '.court-etl' / uuid4().hex
    directory.mkdir(parents=True)
    receipt = combine_receipts([path for path, _ in selections], directory / 'etl_receipts.parquet')
    return {'receipt_path': receipt, 'receipt_policies': [POLICIES[path.stem] for path in paths],
            'receipt_generation_id': ids.pop(), 'adopt_owned_receipt': True}


def build_court_generation(directory: Path, *, family: str, files: Sequence[Path], **kwargs):
    """Admit court subjects and receipts together, including standalone local builds."""
    from spicy_regs.generations import build_generation
    from spicy_regs.native_types import described_schema
    schemas = {path.stem: described_schema(SUBJECT_SCHEMAS[path.stem]) for path in files}
    return build_generation(directory, family=family, files=files,
                            expected_keys=[path.name for path in files], schemas=schemas,
                            **generation_options(files), **kwargs)
