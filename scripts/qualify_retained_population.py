"""Measure selected legacy output conversion and private serving.

This replays the hash-pinned published population. It does not qualify the
CourtListener CSV acquisition or Federal Register REST acquisition/merge.
An enclosing owned supervisor supplies the complete wall/resource bound.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
from itertools import zip_longest
import json
from pathlib import Path
import time
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.parquet as pq
from rulespec_artifacts import LocalMemberSource, admit_artifact

from spicy_regs.sources.disk_store import DiskStore
from scripts.retained_qualification_support import (
    assert_pin, phase, serve,
    compare_files, capture_code_pins, mcp_controls, verify_code_pins,
)
from spicy_regs import court_receipts, etl_bulk, regulations_bulk
from spicy_regs.etl_receipts import carry_receipt_history, validate_receipt_bundle
from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.sources import publication
from spicy_regs.transforms import regulations_receipts as regulations


DATASETS = {'court_opinions': 'court-opinions', 'federal_register': 'federal-register',
            'dockets': 'dockets', 'documents': 'documents'}
BASE_DATASETS = frozenset({'dockets', 'documents'})


@contextmanager
def observe_base_routes(record, name):
    """Count completed private adapter calls; an attempted fallback is not a pass."""
    original = getattr(regulations_bulk, name)
    record['baseAdapterCalls'] = record['baseAdapterCompleted'] = 0

    def observed(*args, **kwargs):
        record['baseAdapterCalls'] += 1
        result = original(*args, **kwargs)
        record['baseAdapterCompleted'] += 1
        return result

    setattr(regulations_bulk, name, observed)
    try:
        yield
        if record['baseAdapterCalls'] != 1 or record['baseAdapterCompleted'] != 1:
            raise ValueError('Required regulatory base adapter did not complete exactly once')
    finally:
        setattr(regulations_bulk, name, original)


def capture_prerequisites(plan, *, required):
    """Bind a full base replay to actual reviewed immutable capture evidence."""
    refs = plan.get('captureEvidence', {})
    if required and set(refs) != {'plan', 'result', 'qualification', 'review'}:
        raise ValueError('Full base replay requires exact capture plan, result, qualification and review pins')
    for item in refs.values():
        path = Path(item['path'])
        if path.stat().st_size != item['bytes'] or 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise ValueError('Capture evidence differs from its frozen pin')
    if required:
        result = json.loads(Path(refs['result']['path']).read_text())
        qualified = json.loads(Path(refs['qualification']['path']).read_text())
        capture_plan = json.loads(Path(refs['plan']['path']).read_text())
        for item in capture_plan['metadataPins']:
            path = Path(item['path'])
            if path.stat().st_size != item['bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
                raise ValueError('Original capture selection metadata differs from its frozen pin')
        entry = plan['source']
        rows = [row for row in qualified['phases'] if row['key'] == 'source:' + entry['dataset'] + '.parquet']
        if (result['state'] != 'passed' or result.get('qualificationEvidenceAdmitted') is not True
                or result.get('remainingOwnedPids') != [] or result.get('portClosed') is not True
                or qualified['state'] != 'passed' or len(rows) != 1):
            raise ValueError('Actual immutable capture did not qualify this source')
        row = rows[0]
        if (any(type(value) is not int for value in (row['bytes'], row['rows'], entry['bytes'], entry['rows']))
                or (row['path'], row['bytes'], row['rows'], 'sha256:' + row['sha256']) != (
                entry.get('localInput'), entry['bytes'], entry['rows'], entry['sha256'])):
            raise ValueError('Selected original source differs from the admitted capture')
    return refs


def handoff(output, candidate, dataset, family, generation, declared, entry, plan, frozen, *, fixture):
    """Describe actual qualified pair bytes for a later selected-input consumer."""
    directory = output / 'generation'
    index = json.loads((output / 'private-publication.json').read_text())
    files = {str(path.relative_to(directory)): publication.file_identity(path)
             for path in sorted(directory.rglob('*')) if path.is_file()}
    record = {'state': 'source-pair-and-private-serving-passed-resource-release-not-yet-admitted',
              'dataset': dataset, 'family': family, 'source': entry, 'fixture': fixture,
              'captureEvidence': plan.get('captureEvidence', {}), 'codePins': frozen,
              'receiptGenerationId': generation, 'receiptPolicies': [declared.descriptor()],
              'generationDirectory': str(directory), 'generationMembers': files,
              'privatePublicationPath': str(output / 'private-publication.json'),
              'privatePublicationPin': publication.file_identity(output / 'private-publication.json'),
              'selectedFamily': index['families'][family],
              'buildPhases': str(candidate / 'phases.jsonl'),
              'buildPhasesPin': publication.file_identity(candidate / 'phases.jsonl'),
              'fullComparisonPhases': str(output / 'phases.jsonl'),
              'fullComparisonPhasesPin': publication.file_identity(output / 'phases.jsonl'),
              'resourceReleaseEvidenceRequired': True,
              'unqualified': ['ordinary next materialization', 'documents annotation and reconciliation input',
                              'production publication', 'complete regulatory production unit'],
              'productionActions': []}
    (output / 'HANDOFF.json').write_text(json.dumps(record, indent=2) + '\n')
    return record


def declared_policy(dataset):
    return court_receipts.POLICIES[dataset] if dataset == 'court_opinions' else regulations.policy(dataset)


@contextmanager
def observe_validation_routes(record):
    """Record actual full-admission reference routes without changing their inputs."""
    original_ordinals, original_validate = etl_bulk._ordinal_file, etl_bulk.validate_bundle
    routes = []
    record['validationRoutes'] = routes

    def ordinals(con, query, parameters, path):
        observed = {'selection': path.name, 'selectedRows': None, 'yieldedRows': 0, 'fullyConsumed': False}
        routes.append(observed)
        record['actualBulkSettings'] = dict(con.execute("SELECT name,value FROM duckdb_settings() WHERE name IN "
            "('threads','memory_limit','max_temp_directory_size','temp_directory')").fetchall())
        try:
            for ordinal in original_ordinals(con, query, parameters, path):
                if observed['selectedRows'] is None:
                    observed['selectedRows'] = pq.read_metadata(path).num_rows
                observed['yieldedRows'] += 1
                yield ordinal
            observed['selectedRows'] = pq.read_metadata(path).num_rows
            observed['fullyConsumed'] = True
        finally:
            if observed['selectedRows'] is None and path.exists():
                observed['selectedRows'] = pq.read_metadata(path).num_rows

    def validate(*args, **kwargs):
        try:
            return original_validate(*args, **kwargs)
        except etl_bulk.NotBulkEligible:
            record['fullDatasetRowFallback'] = True
            raise

    setattr(etl_bulk, '_ordinal_file', ordinals)
    setattr(etl_bulk, 'validate_bundle', validate)
    try:
        yield
    finally:
        setattr(etl_bulk, '_ordinal_file', original_ordinals)
        setattr(etl_bulk, 'validate_bundle', original_validate)


def capture_source(entry, destination):
    """Keep partial bytes on failure; pin a complete source before any producer."""
    if type(entry['rows']) is not int or entry['rows'] <= 0:
        raise ValueError('Selected full population must declare a positive row count')
    selected = entry.get('localInput')
    source = Path(selected).open('rb') if selected else urlopen(entry['url'], timeout=30)
    with source, destination.open('xb') as sink:
        total = 0
        while block := source.read(1024 * 1024):
            total += len(block)
            sink.write(block)
            if total > entry['bytes']:
                raise ValueError('Selected source exceeds its declared byte size')
    pin = {name: entry['bytes' if name == 'byteSize' else name] for name in ('sha256', 'byteSize', 'rows')}
    assert_pin(destination, pin)
    schema = pq.read_schema(destination)
    if schema.names != entry['columns'] or any(not pa.types.is_string(f.type) for f in schema):
        raise ValueError('Original legacy source columns/order/string types differ')
    destination.chmod(0o444)
    return pin, schema


def produce(dataset, source, directory, generation, *, bulk=False):
    if dataset == 'court_opinions':
        subject = court_receipts.write_court_rows(dataset, court_receipts.parquet_rows(source), directory,
            witnesses=[court_receipts.file_witness(source)], generation_id=generation,
            attempt_prefix='qualification')
        receipt, selected_generation = court_receipts.local_receipt_selection(subject)
        if selected_generation != generation:
            raise ValueError('Court receipt selection differs from the declared generation')
        return subject, receipt
    return regulations.write_held_dataset(dataset, source, directory, generation_id=generation, bulk=bulk)


def selected_pair(dataset, directory):
    """The produced subject and its selected receipts; court subjects are link-selected attempts."""
    pointer = directory / (dataset + '.parquet')
    if dataset == 'court_opinions':
        receipt, _ = court_receipts.local_receipt_selection(pointer)
        subject = pointer.resolve()
        if not subject.is_relative_to(directory.resolve()):
            raise ValueError('Court subject escapes its candidate bundle')
        return subject, receipt
    return pointer, directory / 'etl_receipts.parquet'


def produce_selected(dataset, source, directory, generation, **options):
    """Produce one candidate bundle and require it to be the one the audit will select."""
    subject, receipt = produce(dataset, source, directory, generation, **options)
    if tuple(map(Path.resolve, (subject, receipt))) != tuple(map(Path.resolve, selected_pair(dataset, directory))):
        raise ValueError('Producer bundle differs from the maintained selection rule')
    return subject, receipt


def populations(dataset, source, subject, receipt):
    with etl_bulk.bulk_connection() as (con, _):
        counts = dict(con.execute('SELECT outcome,count(*) FROM read_parquet(?) GROUP BY outcome',
                                 [str(receipt)]).fetchall())
    rows = pq.read_metadata(source).num_rows
    if counts.get('accepted', 0) != rows or pq.read_metadata(subject).num_rows != rows:
        raise ValueError('Selected population contains refused/lost source rows; full qualification stops')
    expected = {'accepted': rows}
    if dataset != 'court_opinions':
        expected['observed'] = 1
    if counts != expected:
        raise ValueError('Receipt outcomes differ from the complete selected source population')
    return {'sourceRows': rows, 'subjectRows': rows, 'receiptOutcomes': counts}


def complete_sample(dataset, source, destination, *, bound=2000):
    """Scan all source shapes; retain first eight per shape plus periodic rows."""
    fields = {'court_opinions': ('per_curiam', 'page_count', 'author_str'),
              'federal_register': ('document_type', 'agencies_json', 'title'),
              'dockets': ('docket_type', 'rin', 'title'),
              'documents': ('document_type', 'attachment_records_json', 'title')}[dataset]
    selected, seen = [], {}
    with pq.ParquetFile(source) as parquet:
        ordinal = 0
        for batch in parquet.iter_batches(batch_size=2000):
            for row in batch.to_pylist():
                text = row.get(fields[2])
                shape = (row.get(fields[0]), row.get(fields[1]) is None,
                         text is None, bool(text and not text.isascii()),
                         bool(text and any(value in text for value in ('\x00', '\x1b', '\n'))))
                seen[shape] = seen.get(shape, 0) + 1
                if seen[shape] <= 8 or ordinal % 100003 == 0:
                    if len(selected) == bound:
                        raise ValueError('Complete stratified reference sample exceeds its declared bound')
                    selected.append(row)
                ordinal += 1
        pq.write_table(pa.Table.from_pylist(selected, schema=parquet.schema_arrow), destination)
    return {'sampleRows': len(selected), 'sampleBound': bound, 'sourceRowsScanned': ordinal,
            'strata': [fields[0], fields[1] + ' null', fields[2] + ' null/Unicode/control'],
            'referenceScope': 'Complete sample bundle through both public validator modes; actual producer is row-based'}


def restore(dataset, source, subject, receipt, destination, generation, *, bulk=False):
    if dataset == 'court_opinions':
        court_receipts.restore_processing_input(subject, destination, dataset=dataset, schema=pq.read_schema(source),
                                               receipt_path=receipt, generation_id=generation)
        return compare_files(source, destination)
    regulations.materialize_internal(regulations.ReceiptInput(dataset, (subject,), receipt, generation), destination, bulk=bulk)
    # The real processor emits canonical column order, including rin before the
    # final Regulations.gov fields. Check that separately from original order.
    source_schema = pq.read_schema(source)
    canonical = pa.schema([(name, regulations.TYPES[kind]) for name, kind in regulations.SOURCE_COLUMNS[dataset]],
                          metadata=source_schema.metadata)
    if any(name not in canonical.names for name in source_schema.names):
        raise ValueError('Unclassified original columns cannot be reconstructed')
    with pq.ParquetFile(source) as original, pq.ParquetFile(destination) as actual:
        if not actual.schema_arrow.equals(canonical, check_metadata=True):
            raise ValueError('Canonical processor schema/metadata differs')
        rows = 0
        for left, right in zip_longest(original.iter_batches(batch_size=2000), actual.iter_batches(batch_size=2000)):
            if left is None or right is None:
                raise ValueError('Canonical restoration changed the complete source count')
            expected = pa.Table.from_batches([left])
            for field in canonical:
                if field.name not in expected.column_names:
                    expected = expected.append_column(field.name, pa.nulls(left.num_rows, type=field.type))
            expected = expected.select(canonical.names).cast(canonical)
            held = pa.Table.from_batches([right])
            if not expected.equals(held, check_metadata=True):
                raise ValueError(f'Canonical source values/order differ at row {rows}')
            if not held.select(source_schema.names).cast(source_schema).equals(pa.Table.from_batches([left]),
                                                                              check_metadata=True):
                raise ValueError('Original source field order/literals cannot be reconstructed')
            rows += left.num_rows
    return rows


def build_candidates(plan_path, output, *, fixture=False):
    """Produce the selected prior/current pair; owner closure seals reuse outside this child."""
    output.mkdir(parents=True, exist_ok=False)
    raw_plan = plan_path.read_bytes()
    (output / 'PLAN.json').write_bytes(raw_plan)
    plan, log = json.loads(raw_plan), output / 'phases.jsonl'
    entry = plan['source']
    dataset, family = entry['dataset'], entry['family']
    if DATASETS.get(dataset) != family:
        raise ValueError('Explicit supported dataset and complete single-table family required')
    base = dataset in BASE_DATASETS
    with phase(log, 'frozen-code-and-dependency-pins') as record:
        frozen = capture_code_pins()
        if not fixture and (frozen['dirty'] or frozen != plan['codePins']):
            raise ValueError('Full qualification requires the exact clean reviewed code/dependencies')
        record.update(frozen, fixture=fixture, planSha256=hashlib.sha256(raw_plan).hexdigest())
    with phase(log, 'immutable-capture-prerequisites') as record:
        record['pins'] = capture_prerequisites(plan, required=base and not fixture)
    source = output / 'original.parquet'
    with phase(log, 'complete-original-acquisition-and-admission') as record:
        pin, schema = capture_source(entry, source)
        record.update(pin, originalSchema=str(schema), originalMetadata=str(schema.metadata),
                      acquisitionScope='Published legacy mapper output; upstream CSV/REST acquisition unqualified')
    declared, generation = declared_policy(dataset), 'qualification-' + family
    with phase(log, 'separate-selected-prior-producer-and-full-admission') as record, observe_validation_routes(record):
        if base:
            with observe_base_routes(record, 'write_held_dataset'):
                prior_subject, prior = produce_selected(dataset, source, output / 'selected-prior', generation + '-prior', bulk=True)
        else:
            prior_subject, prior = produce_selected(dataset, source, output / 'selected-prior', generation + '-prior')
        validate_receipt_bundle({dataset: [prior_subject]}, [prior], [declared], generation_id=generation + '-prior')
        record.update(populations(dataset, source, prior_subject, prior))
    with phase(log, 'fresh-current-producer-and-full-admission') as record, observe_validation_routes(record):
        if base:
            with observe_base_routes(record, 'write_held_dataset'):
                subject, fresh = produce_selected(dataset, source, output / 'current', generation, bulk=True)
        else:
            subject, fresh = produce_selected(dataset, source, output / 'current', generation)
        validate_receipt_bundle({dataset: [subject]}, [fresh], [declared], generation_id=generation)
        record.update(populations(dataset, source, subject, fresh))
    descriptor = {'version': 1, 'planSha256': hashlib.sha256(raw_plan).hexdigest(),
                  'codePins': frozen, 'policy': declared.descriptor(), 'fixture': fixture,
                  'candidateBuildCalls': 2,
                  'scope': 'Candidate description only; trusted run owner must close and seal bytes for reuse'}
    (output / 'BUILD.json').write_text(json.dumps(descriptor, indent=2) + '\n')
    return descriptor


def audit_candidates(plan_path, candidate, output, *, fixture=False, httpfs_extension=None):
    """Run the complete independent checks against a closed candidate in a fresh attempt.

    This command does not authorize continuation: the enclosing operational
    runner admits its separately granted owner completion before invoking it.
    Reference/oracle production remains part of independent audit.
    """
    started = time.monotonic()
    output.mkdir(parents=True, exist_ok=False)
    raw_plan = plan_path.read_bytes()
    (output / 'PLAN.json').write_bytes(raw_plan)
    plan, log = json.loads(raw_plan), output / 'phases.jsonl'
    entry = plan['source']
    dataset, family = entry['dataset'], entry['family']
    if DATASETS.get(dataset) != family:
        raise ValueError('Explicit supported dataset and complete single-table family required')
    base = dataset in BASE_DATASETS
    declared, generation = declared_policy(dataset), 'qualification-' + family
    with phase(log, 'frozen-code-and-dependency-pins') as record:
        descriptor = json.loads((candidate / 'BUILD.json').read_text())
        frozen = capture_code_pins()
        if (descriptor['version'] != 1 or descriptor['codePins'] != frozen
                or descriptor['planSha256'] != hashlib.sha256(raw_plan).hexdigest()
                or descriptor['policy'] != declared.descriptor() or descriptor['fixture'] != fixture
                or (candidate / 'PLAN.json').read_bytes() != raw_plan):
            raise ValueError('Candidate code/dependency/policy/plan identity differs')
        if not fixture and (frozen['dirty'] or frozen != plan['codePins']):
            raise ValueError('Full qualification requires the exact clean reviewed code/dependencies')
        capture_prerequisites(plan, required=base and not fixture)
        record.update(frozen, fixture=fixture, planSha256=hashlib.sha256(raw_plan).hexdigest(), candidate=str(candidate))
    source = candidate / 'original.parquet'
    pin = {name: entry['bytes' if name == 'byteSize' else name] for name in ('sha256', 'byteSize', 'rows')}
    assert_pin(source, pin)
    prior_subject, prior = selected_pair(dataset, candidate / 'selected-prior')
    subject, fresh = selected_pair(dataset, candidate / 'current')
    # Re-admit the complete pair independently; a candidate description is not proof.
    with phase(log, 'independent-closed-candidate-full-admission') as record:
        for member, receipts, selected_generation in ((prior_subject, prior, generation + '-prior'),
                                                       (subject, fresh, generation)):
            validate_receipt_bundle({dataset: [member]}, [receipts], [declared], generation_id=selected_generation)
            populations(dataset, source, member, receipts)
        record.update(candidate=str(candidate), candidateBuildCalls=0, independentOracleProductionRequired=True)
    if base:
        with phase(log, 'full-original-row-producer-and-all-field-equivalence') as record:
            row_subject, row_receipt = produce(dataset, source, output / 'full-row-reference', generation + '-prior', bulk=False)
            validate_receipt_bundle({dataset: [row_subject]}, [row_receipt], [declared],
                                    generation_id=generation + '-prior', bulk=False)
            record.update(subjectRows=compare_files(row_subject, prior_subject), receiptRows=compare_files(row_receipt, prior))
    with phase(log, 'complete-stratified-public-row-validator-oracle') as record:
        sample = output / 'reference-source.parquet'
        record.update(complete_sample(dataset, source, sample))
        sample_subject, sample_receipt = produce(dataset, sample, output / 'reference-bundle', generation + '-reference')
        populations(dataset, sample, sample_subject, sample_receipt)
        for bulk in (True, False):
            validate_receipt_bundle({dataset: [sample_subject]}, [sample_receipt], [declared],
                                    generation_id=generation + '-reference', bulk=bulk)
    with phase(log, 'full-unchanged-history-and-exact-prior-fields') as record:
        carried = carry_receipt_history(fresh, [prior], output / 'carried-receipts.parquet')
        record.update(rows=compare_files(prior, carried), current=publication.file_identity(fresh),
                      selectedPrior=publication.file_identity(prior), carried=publication.file_identity(carried),
                      configuredHistoryThreads=4, configuredHistoryMemory='1GB', configuredHistorySpill='32GB')
        if record['current']['sha256'] == record['selectedPrior']['sha256']:
            raise ValueError('History qualification requires distinct separately produced current/prior receipts')
        validate_receipt_bundle({dataset: [subject]}, [carried], [declared], generation_id=generation)
    with phase(log, 'actual-full-processor-restore-original-and-canonical-conservation') as record:
        if base:
            with observe_base_routes(record, 'materialize_internal'):
                record['rows'] = restore(dataset, source, subject, carried, output / 'restored.parquet', generation, bulk=True)
            restore(dataset, source, subject, carried, output / 'row-restored.parquet', generation, bulk=False)
            record['rowReaderEquivalentRows'] = compare_files(output / 'row-restored.parquet', output / 'restored.parquet')
        else:
            record['rows'] = restore(dataset, source, subject, carried, output / 'restored.parquet', generation)
    directory = output / 'generation'
    with phase(log, 'complete-native-generation-admission'):
        artifact = build_generation(directory, family=family, files=[subject], expected_keys=[dataset + '.parquet'],
                         schemas={dataset: described_schema(declared.subject_schema)}, receipt_path=carried,
                         receipt_policies=[declared], receipt_generation_id=generation)
    store = DiskStore(output / 'private-store', bucket='retained-qualification')
    with phase(log, 'actual-private-publication-upload-readback'):
        index = publication.publish_generation(directory, client=store, bucket=store.bucket,
                                               prior_index=publication.empty_index())
        (output / 'private-publication.json').write_text(json.dumps(index, indent=2) + '\n')
        (output / 'private-store-calls.json').write_text(json.dumps(store.calls, indent=2) + '\n')
    with phase(log, 'actual-production-mcp-http-and-dataset-correct-controls') as record:
        marker = {'probe': 'actual-mcp-startup-and-controls'}
        with serve({'/' + key: path for key, path in store.objects.items()},
                   output / 'mcp-requests.jsonl', marker) as served_base:
            controls = mcp_controls(served_base, output, {dataset: subject}, policies={dataset: declared},
                                    httpfs_extension=httpfs_extension)
        (output / 'mcp-controls.json').write_text(json.dumps(controls, indent=2) + '\n')
        record.update(settings=controls['settings'], transport=controls['transport'], schemeOverride=controls['schemeOverride'])
    with phase(log, 'final-complete-artifact-stored-bytes-and-original-pins'):
        admit_artifact(LocalMemberSource(directory), expected_pin=artifact.pin)
        admit_artifact(publication._S3Members(store, store.bucket, index['families'][family]['prefix']),
                       expected_pin=artifact.pin)
        assert_pin(source, pin)
        if entry.get('localInput'):
            assert_pin(Path(entry['localInput']), pin)
        capture_prerequisites(plan, required=base and not fixture)
    verify_code_pins(log, frozen)
    if base:
        handoff(output, candidate, dataset, family, generation, declared, entry, plan, frozen, fixture=fixture)
    result = {'status': 'passed', 'seconds': time.monotonic() - started, 'fixture': fixture,
              'scope': 'Selected published legacy population replay/private DiskStore/actual loopback MCP',
              'unqualified': ['upstream CSV/REST acquisition/merge', 'hosted publication/MCP'], 'productionActions': []}
    (output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--fixture', action='store_true')
    parser.add_argument('--stage', choices=('build', 'audit'), required=True)
    parser.add_argument('--candidate', type=Path, help='Closed build root for the audit stage only')
    parser.add_argument('--httpfs-extension', type=Path, help='Exact installed signed httpfs file for offline audit')
    args = parser.parse_args()
    if args.stage == 'build':
        if args.candidate:
            parser.error('--candidate applies only to audit')
        if args.httpfs_extension:
            parser.error('--httpfs-extension applies only to audit')
        result = build_candidates(args.plan, args.output, fixture=args.fixture)
    elif args.stage == 'audit':
        if not args.candidate:
            parser.error('--stage audit requires --candidate')
        result = audit_candidates(args.plan, args.candidate, args.output, fixture=args.fixture,
                                  httpfs_extension=args.httpfs_extension)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
