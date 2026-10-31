"""Complete retained FEC families through their maintained receipt writers.

This is representation conversion, not a publisher-source rebuild: original member
order and schemas remain the comparison authority, except for REBUILT_TABLES, which
are built from their retained originals in the raw archive (fec_original_tables).
"""
from contextlib import ExitStack, contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass, replace
import base64
import json
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication
from spicy_regs.transforms.assemble_fec_query import (
    TypedInput, assemble_subject_table, hashed_input, owned_input, unchanged_input, _sha, _equal_streams)
from spicy_regs.transforms.build_fec_observations import OUTPUTS
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
from spicy_regs.transforms.fec_native_subjects import FIELD_RULES
from spicy_regs.transforms.fec_subject_receipts import subject_files as _members

FAMILIES = frozenset({'fec-observations', 'fec-query'})
LEGACY_FIELDS = json.loads(Path(__file__).with_name('fec_retained_legacy_fields.json').read_text())


@dataclass(frozen=True)
class ObservationRebinding:
    """Fresh complete original comparison authorized this representation-only pin change."""
    original_pin: str
    staged_pin: str


REBIND_FIELDS = {
    'fec_collection_selection': 'source_generation_pin',
    'fec_filing_definition_evidence': 'witness_generation_pin',
    'fec_filing_header_associations': 'source_generation_pin',
    'fec_record_evidence': 'witness_generation_pin',
}


def rebind_row(table, row, proof):
    field = REBIND_FIELDS.get(table)
    if proof is None or field is None:
        return row
    if not isinstance(proof, ObservationRebinding):
        raise ValueError('Generation rebinding requires the fresh staged observations comparison')
    if row.get(field) != proof.original_pin:
        return row
    return {**row, field: proof.staged_pin}


def rebind_filing_inputs(selection, proof, directory, check_resources):
    """Rebind only byte-admitted header coordinates after complete source equivalence."""
    if proof is None or selection.source_generation_pin == proof.staged_pin:
        return selection
    if selection.source_generation_pin != proof.original_pin:
        raise ValueError('Filing source selection is foreign to the captured and staged observations')
    # This prepares all bounded dependencies and verifies their original bytes.
    from spicy_regs.transforms.fec_query import RECEIPT_SCHEMA
    selection.prepare('fec_receipts', RECEIPT_SCHEMA, check_resources)
    directory.mkdir(parents=True, exist_ok=False)
    headers = []
    for ordinal, item in enumerate(selection.header_associations):
        check_resources()
        source = pq.ParquetFile(item.path)
        from spicy_regs.transforms.fec_filing_associations import _base, POLICY_VERSION
        rows = []
        for row in source.read(use_threads=False).to_pylist():
            if row.get('policy_version') != POLICY_VERSION:
                raise ValueError('Header source rebinding requires current maintained decisions')
            raw = {**row, 'record_id': row['target_record_id']}
            before = _base(raw, row['target_table'], row['source_generation_pin'],
                           row['selection_evidence_sha256'], row['association_basis'])
            if row['record_id'] != before['record_id']:
                raise ValueError('Current header identity differs from its maintained source-generation rule')
            rebound = rebind_row('fec_filing_header_associations', row, proof)
            current = _base(raw, row['target_table'], rebound['source_generation_pin'],
                            row['selection_evidence_sha256'], row['association_basis'])
            rows.append({**rebound, 'record_id': current['record_id']})
        path = directory / f'headers-{ordinal}.parquet'
        pq.write_table(pa.Table.from_pylist(rows, schema=source.schema_arrow), path, compression='zstd')
        if _sha(item.path) != item.sha256:
            raise ValueError('Header dependency changed during qualified rebinding')
        headers.append(TypedInput(path, _sha(path), item.rows))
    return replace(selection, source_generation_pin=proof.staged_pin, header_associations=tuple(headers))


def family_tables(family):
    if family == 'fec-observations':
        return frozenset(OUTPUTS)
    if family == 'fec-query':
        root = Path(__file__).resolve().parents[2]
        return frozenset(name + '.parquet' for name in json.loads(
            (root / 'data_dictionary/fec_typed_schemas.json').read_text())['tables'])
    raise ValueError('Unsupported FEC conversion family')


# Published tables the owner retired (ss-j4vz B10, 2026-10-30; Q2, 2026-10-31): a captured
# prior still holds them, the converted generation omits them, and their originals stay in the raw store.
RETIRED_TABLES = {'fec-query': frozenset({'fec_research_document_observations.parquet',
                                          'fec_retained_csv_observations.parquet',
                                          'fec_research_response_outcomes.parquet',
                                          'fec_research_context_dispositions.parquet'})}


# Tables the owner moved off receipt context (ss-j4vz B7-B9, Q6-Q8, 2026-10-30): the captured
# prior still holds them under the key on the left, but each generation builds the table on the
# right from the retained originals the committed selection names, never from the captured rows.
# The filing feed and source pages move to the reference namespace (Q1, 2026-10-31).
REBUILT_TABLES = {'fec-query': {
    'fec_historical_ie_statistics.parquet': 'fec_historical_ie_statistics',
    'fec_research_meeting_observations.parquet': 'fec_research_meeting_observations',
    'fec_research_filing_feed_items.parquet': 'fec_reference_filing_feed_items',
    'fec_research_source_pages.parquet': 'fec_reference_source_pages',
}}
# Stored evidence rows that target a retired or rebuilt table (by its captured name) describe
# receipt context that no published row carries any longer; a rebuilt row is its own witness.
_UNREFERENCED_EVIDENCE = {family: RETIRED_TABLES.get(family, frozenset()) | REBUILT_TABLES.get(family, {}).keys()
                          for family in FAMILIES}


def published_outputs(family, captured_tables):
    """A captured prior's table entries keyed by the member a converted generation publishes them as."""
    rebuilt = REBUILT_TABLES.get(family, {})
    return {(rebuilt[key] + '.parquet' if key in rebuilt else key): table
            for key, table in captured_tables.items() if key not in RETIRED_TABLES.get(family, frozenset())}


def converted_tables(family, captured_tables):
    """The member keys a converted generation publishes for a captured prior's tables."""
    return set(published_outputs(family, captured_tables))


def referenced_evidence(original, destination, family, *, check_resources=lambda: None):
    """Copy a retained fec_record_evidence member without rows that target a retired or rebuilt table.

    This is what the conversion publishes, so qualification compares against it.
    Returns the number of rows left out. One streaming pass; schema and metadata kept.
    """
    unreferenced = {name.removesuffix('.parquet') for name in _UNREFERENCED_EVIDENCE[family]}
    if original.is_dir():
        raise ValueError('Evidence filtering supports only a flat retained member')
    dropped = 0
    with pq.ParquetFile(original) as source, pq.ParquetWriter(destination, source.schema_arrow, compression='zstd') as out:
        for batch in source.iter_batches(batch_size=8192, use_threads=False):
            check_resources()
            keep = pa.array([name not in unreferenced for name in batch.column('target_table').to_pylist()])
            kept = batch.filter(keep)
            dropped += batch.num_rows - kept.num_rows
            out.write_batch(kept)
    return dropped


def rebuilt_inputs(table, directory, *, store=None, check_resources=lambda: None):
    """One declared-schema member and one raw-archive witness per admitted original.

    Returns (members, witnesses, schema, refusals). Refused originals (failed or
    receipt-less captures, unsupported bodies) write nothing and are listed by digest.
    """
    from spicy_regs.transforms import fec_original_tables as originals

    schema = originals.SCHEMAS[table]
    members, witnesses, refusals = [], [], []
    directory.mkdir(parents=True, exist_ok=True)
    for ordinal, entry in enumerate(e for e in originals.selection() if e['table'] == table):
        check_resources()
        result = originals.rebuild(table, store=store, originals=[entry])
        refusals.extend(result.refusals)
        if not result.rows:
            continue
        path = directory / f'{table}-{ordinal:04d}.parquet'
        pq.write_table(pa.Table.from_pylist(result.rows, schema=schema), path, compression='zstd')
        members.append(owned_input(path, len(result.rows)))
        digest = entry['source_sha256'].removeprefix('sha256:')
        witnesses.append(dict(source_id=f'raw-archive:objects/sha256/{digest[:2]}/{digest}', source_uri=entry['source_url'],
                              sha256=entry['source_sha256'], locator=None, body_version=entry['acquisition_receipt']))
    if not members:
        raise ValueError(f'{table}: no retained original was admitted')
    (directory / f'{table}.refusals.json').write_text(json.dumps(sorted(refusals), indent=1) + '\n')
    return members, witnesses, schema, refusals


def check_membership(family, old):
    if converted_tables(family, old['tables']) != family_tables(family):
        raise ValueError('FEC conversion requires the complete registered family membership')
    for key, table in old['tables'].items():
        if 'members' in table and [member['key'] for member in table['members']] != sorted(member['key'] for member in table['members']):
            raise ValueError('Noncanonical captured FEC member order is unsupported; no family build started')
        if 'members' in table and table.get('partitionColumns') != ['source_namespace']:
            raise ValueError(f'{key}: unsupported FEC source partition declaration')


def staged_snapshot(captured, generation, operation):
    """Bind a prepared parent without claiming that its pointer has moved."""
    artifact, _, source = operation.admit_generation(generation)
    spec = artifact.root['spec']
    if spec['family'] != 'fec-observations' or spec['publicationStatus'] != 'complete-family':
        raise ValueError('Staged FEC parent must be complete observations')
    old = captured['families']['fec-observations']
    check_membership('fec-observations', old)
    if spec['readSnapshot'] != captured:
        raise ValueError('Staged observations differ from the captured prior snapshot')
    descriptors = {p['dataset'] for p in spec['etlReceipts']['policies']}
    if descriptors != {key.removesuffix('.parquet') for key in family_tables('fec-observations')}:
        raise ValueError('Staged observations omit a registered receipt sibling')
    result = deepcopy(captured)
    from rulespec_artifacts import iter_member_descriptors
    result['families']['fec-observations'] = publication.generation_entry(artifact, iter_member_descriptors(artifact, source))
    return result


def _partition_subject(flat, original, destination, check_resources):
    """Keep exact member names/lengths; generation admission checks partition cells.

    Only an identity writer's flat output takes this second write; subject assembly
    writes its members directly (assemble_subject_table subject_members).
    """
    from spicy_regs.etl_receipts import MemberWriter
    members = _member_layout(original)
    destination.mkdir(parents=True)
    with pq.ParquetFile(flat) as source:
        with MemberWriter(destination, members, source.schema_arrow) as writer:
            for batch in source.iter_batches(batch_size=8192, use_threads=False):
                check_resources()
                writer.write_table(pa.Table.from_batches([batch]))
        if source.metadata.num_rows != sum(rows for _, rows in members):
            raise ValueError('Native FEC member lost original rows')
        def batches(paths):
            for path in paths:
                with pq.ParquetFile(path) as reader:
                    for batch in reader.iter_batches(batch_size=8192, use_threads=False):
                        check_resources()
                        yield batch
        _equal_streams(batches([flat]), batches(_members(destination)))
    if flat.is_symlink() or flat.stat().st_nlink != 1:
        raise ValueError('Partitioning may consume only its single-link owned flat output')
    flat.unlink()
    return destination


def _member_layout(original):
    """The captured partitioned original's (relative path, rows) members in publication order."""
    return [(member.relative_to(original).as_posix(), pq.ParquetFile(member).metadata.num_rows)
            for member in _members(original)]


def _consume_receipt(receipt, writer, check_resources, *, coalesce=False):
    """Consume a completed owned bundle, checking every batch and its byte binding."""
    if receipt.is_symlink() or receipt.stat().st_nlink != 1:
        raise ValueError('Receipt consolidation requires a single-link owned output')
    digest = _sha(receipt)
    count = 0
    with pq.ParquetFile(receipt) as source:
        if not source.schema_arrow.equals(RECEIPT_SCHEMA, check_metadata=False):
            raise ValueError('Receipt consolidation schema differs')
        expected = source.metadata.num_rows
        batches = source.iter_batches(batch_size=512, use_threads=False)
        if coalesce:
            from spicy_regs.fec_observation_segments import coalesced_tables
            for held in coalesced_tables(batches, RECEIPT_SCHEMA, check_resources=check_resources):
                check_resources()
                writer.write_table(held, row_group_size=8192)
                count += held.num_rows
        else:
            for batch in batches:
                check_resources()
                writer.write_batch(batch)
                count += batch.num_rows
    if count != expected or _sha(receipt) != digest:
        raise ValueError('Receipt changed during consolidation')
    receipt.unlink()


def _bounded_record_batches(batch, *, max_bytes=8 * 1024 * 1024):
    """Limit each Python materialization; a single wide value stays intact."""
    start = 0
    while start < batch.num_rows:
        count = batch.num_rows - start
        part = batch.slice(start, count)
        while count > 1 and part.nbytes > max_bytes:
            count = max(1, count // 2)
            part = batch.slice(start, count)
        yield part
        start += count


def validate_identity_schema(table, schema):
    """Only the supported mapper schema or exact retained /1 schema may convert."""
    source = Path(__file__).resolve().parents[2] / 'data_dictionary/fec_typed_schemas.json'
    current = json.loads(source.read_text())['tables'].get(table)
    legacy = json.loads(Path(__file__).with_name('fec_retained_legacy_schemas.json').read_text()).get(table)
    candidates = [value for value in (None if current is None else current['arrow_schema_ipc_base64'], legacy) if value]
    if not any(schema.equals(pa.ipc.read_schema(pa.BufferReader(base64.b64decode(value))), check_metadata=False)
               for value in candidates):
        # The observations producer has its own complete schemas outside the query declaration.
        from spicy_regs.transforms.build_fec_observations import RECORD_SCHEMA, COLLECTION_SCHEMA, RELATIONSHIP_SCHEMA
        expected = {'fec_source_records': RECORD_SCHEMA, 'fec_collections': COLLECTION_SCHEMA,
                    'fec_relationships': RELATIONSHIP_SCHEMA}.get(table)
        if expected is None or not schema.equals(expected, check_metadata=False):
            raise ValueError(f'{table}: retained identity schema is neither supported current nor captured legacy shape')


def retained_identity_row(table, original, *, diagnostics=None, source_schema=None):
    """Interpret only named legacy shapes; retain exact old values separately."""
    from spicy_regs.transforms.fec_identity_context_fields import REGISTRY
    registry = REGISTRY[table]
    legacy = LEGACY_FIELDS
    extra = set(original) - set(registry['input_fields'])
    if extra - set(legacy.get(table, ())):
        raise ValueError(f'{table}: unexplained retained identity fields {sorted(extra)}')
    row = {name: value for name, value in original.items() if name not in extra}
    def node(name):
        raw = original.get(name)
        value = json.loads(raw) if isinstance(raw, str) else raw
        if value is not None and not isinstance(value, dict):
            raise ValueError(f'{table}: retained {name} must be a named source object')
        return value
    count_fields = {'fec_collections': ('record_count', 'relationship_count')}.get(table)
    if count_fields and source_schema is not None:
        # These historic producers emitted textual counts. Only the exact
        # admitted captured schema proves that path; current rows remain strict.
        captured = json.loads(Path(__file__).with_name('fec_retained_legacy_schemas.json').read_text())[table]
        expected = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(captured)))
        if source_schema.equals(expected, check_metadata=False):
            from spicy_regs.transforms.fec_identity_shape import typed_values
            values, problems = typed_values(original, dict.fromkeys(count_fields, 'integer'))
            row.update({name: values[name] for name in count_fields})
            if diagnostics is not None:
                diagnostics.update(legacy_bridge='retained-counts/1',
                    field_statuses={name: values[name+'_status'] for name in count_fields},
                    raw_values={name: values[name+'_raw'] for name in count_fields},
                    refusals=problems)
    if table in {'fec_committee_master_observations', 'fec_postgres_committee_history_observations'} and 'native_fields' in original:
        from spicy_regs.transforms.fec_committee_history_observations import _history_values
        native = node('native_fields')
        if native is None:
            raise ValueError('Legacy committee history has no retained native fields')
        if table == 'fec_committee_master_observations':
            from spicy_docs.schemas.fec_committee_history import project_committee_master_row
            native = project_committee_master_row({'fields': native, 'cycle': original.get('source_cycle')})
        values, _ = _history_values(native)
        row.update(values)
    elif table == 'fec_registration_statements':
        from spicy_regs.transforms.fec_identity_observations import STATEMENT_TYPED
        from spicy_regs.transforms.fec_identity_shape import typed_values
        # Captured /1 statements have year scalars but no raw/status columns.
        # Current rows already carry their producer's interpretation, including
        # refused values; do not reinterpret those nulls or replace their evidence.
        if (original.get('mapping_version') == 'fec-identity-observations/1'
                and not any(name + suffix in original for name in STATEMENT_TYPED for suffix in ('_raw', '_status'))):
            values, problems = typed_values(row, STATEMENT_TYPED)
            row.update(values)
            if diagnostics is not None:
                diagnostics.update(legacy_bridge='registration-years/1',
                                   field_statuses={name:values[name+'_status'] for name in STATEMENT_TYPED},
                                   refusals=problems)
    elif table == 'fec_api_response_controls' and 'native_metadata_json' in original:
        from spicy_regs.transforms.fec_context_shape import api_control_values
        row.update(api_control_values(node('native_metadata_json')))
    elif table in {'fec_candidate_api_observations', 'fec_committee_observations'} and 'native_metadata_json' in original:
        from spicy_regs.transforms.fec_identity_shape import typed_values, array_values
        if table == 'fec_candidate_api_observations':
            from spicy_regs.transforms.fec_candidate_observations import TYPED, ARRAYS
        else:
            from spicy_regs.transforms.fec_committee_observations import TYPED, ARRAYS, AGENT_FIELDS
        native = node('native_metadata_json')
        if native is None:
            raise ValueError('Legacy API observation has no retained native business object')
        values, _ = typed_values(native, TYPED)
        row.update(values)
        for name in ARRAYS:
            row.update(array_values(native, name))
        if table == 'fec_committee_observations':
            from spicy_regs.transforms.fec_identity_observations import _scalar
            row.update(array_values(native, 'sponsor_candidate_list'))
            row.update({name: _scalar(native.get(name)) for name in ('affiliated_committee_name', 'organization_type', *AGENT_FIELDS)})
    if set(row) - set(registry['input_fields']):
        raise ValueError(f'{table}: maintained legacy interpretation produced undeclared fields')
    overrides = {name: value for name, value in original.items()
                 if name not in row or type(row[name]) is not type(value) or row[name] != value}
    return row, overrides


def load_filing_associations(path):
    """Read the explicit admitted dependency selection supplied by the coordinator."""
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs
    value = json.loads(Path(path).read_text())
    def members(items):
        if isinstance(items, dict):
            items = [items]
        return tuple(TypedInput(Path(item['path']), item['sha256'], item['rows']) for item in items)
    return FilingAssociationInputs(value['source_generation_pin'], value.get('target_generation_pin'),
        members(value['filings']), value['namespace_evidence'], members(value.get('header_associations', [])),
        value.get('target_generation_scope', 'external'))


def admit_filing_dependency_selection(selection, *, check_resources=lambda: None, on_read=None):
    """Admit exact small inputs, never a native parent or final self correspondence."""
    from collections import Counter
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    from spicy_regs.transforms.fec_filing_associations import (
        ASSOCIATION_SCHEMA, POLICY_VERSION, TARGET_IDENTITY_VERSION, TARGET_MAPPING_VERSION,
        _base, validate_header_filing_witnesses)
    if not selection.filings or not selection.header_associations:
        raise ValueError('Dependency admission requires both selected populations')
    filings, headers, proof = selection.read_dependencies(check_resources,
        expected_filing_schema=dataset_policy('fec_filings').subject_schema,
        expected_header_schema=ASSOCIATION_SCHEMA, on_read=on_read)
    for row in filings:
        check_resources()
        if row.get('identity_version') != TARGET_IDENTITY_VERSION or row.get('mapping_version') != TARGET_MAPPING_VERSION:
            raise ValueError('Filing dependency must state maintained target identity and mapping versions')
    for row in headers:
        check_resources()
        if row.get('policy_version') != POLICY_VERSION or row.get('source_generation_pin') != selection.source_generation_pin:
            raise ValueError('Header dependency policy or source capture differs')
        raw = {**row, 'record_id': row['target_record_id']}
        expected = _base(raw, row['target_table'], row['source_generation_pin'],
                         row['selection_evidence_sha256'], row['association_basis'])
        if row['record_id'] != expected['record_id']:
            raise ValueError('Header dependency identity differs from maintained source coordinates')
        if not isinstance(row.get('association_status'), str) or not row['association_status']:
            raise ValueError('Header dependency must retain a declared outcome')
        if row['association_status'] != 'resolved_native_filing_key' and row.get('filing_key') is not None:
            raise ValueError('Unresolved header cannot claim a resolved filing key')
    validate_header_filing_witnesses(filings, headers)
    proof.update(header_statuses=dict(Counter(row['association_status'] for row in headers)),
        fullParentAdmitted=False, fullTargetCorrespondence=False, generationAdmitted=False,
        targetBinding='provisional-self' if selection.filing_generation_scope == 'self' else 'declared-external-unadmitted',
        namespaceDefinitionSemanticsAdmitted=False)
    return dict(filings=filings, headers=headers, proof=proof)


def filing_subjects(selection, retained, check_resources):
    """Admit the complete /2 remap, retaining old interpretation values in receipts."""
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    # Use the same bounded, fully byte-checked dependency preparation as assembly.
    from spicy_regs.transforms.fec_query import RECEIPT_SCHEMA
    selection.prepare('fec_receipts', RECEIPT_SCHEMA, check_resources)
    expected = dataset_policy('fec_filings').subject_schema
    candidates = []
    for item in selection.filings:
        check_resources()
        schema = pq.read_schema(item.path)
        if not schema.equals(expected, check_metadata=False):
            raise ValueError('Filing remap schema differs from the complete native target policy')
        candidates.extend(pq.ParquetFile(item.path).read(use_threads=False).to_pylist())
    originals = []
    for path in _members(retained['fec_filings.parquet']):
        originals.extend(pq.ParquetFile(path).read(use_threads=False).to_pylist())
    if len(originals) != len(candidates):
        raise ValueError('Filing remap does not preserve complete original population')
    coordinates = ('record_id', 'collection_id', 'source_record_id', 'source_sha256', 'source_locator_json', 'filing_key')
    remapped = frozenset(('mapping_version', 'most_recent', 'report_year', 'reported_cycle', 'pages', 'is_amended', 'election_year'))
    from spicy_regs.transforms.fec_identity_context_fields import normalize_record
    for original, candidate in zip(originals, candidates, strict=True):
        if any(original.get(name) != candidate.get(name) for name in coordinates):
            raise ValueError('Filing remap changes source identity, coordinates, order or business key')
        if candidate.get('mapping_version') != 'fec-identity-observations/2':
            raise ValueError('Filing remap must state its actual maintained mapping version')
        normalized = normalize_record('fec_filings', retained_identity_row('fec_filings', original)[0])
        from spicy_regs.transforms.fec_identity_observations import FILING_TYPED
        from spicy_regs.transforms.fec_identity_shape import typed_values
        serialized = dict(original)
        for name, kind in FILING_TYPED.items():
            if kind == 'boolean' and original.get(name) in ('true', 'false'):
                # /1 wrote native JSON boolean tokens into VARCHAR. Retain that
                # exact token in the override; /2's maintained mapper is typed.
                serialized[name] = json.loads(original[name])
        typed, _ = typed_values(serialized, FILING_TYPED)
        if any(candidate.get(name) != typed.get(name) for name in remapped - {'mapping_version'}):
            raise ValueError('Filing remap reinterprets a prior scalar instead of applying maintained native typing')
        if candidate.get('amendment_chain') != normalized.get('amendment_chain'):
            raise ValueError('Filing remap changes ordered amendment observations')
        if any(original[name] != candidate[name] for name in (original.keys() & candidate.keys()) - remapped):
            raise ValueError('Filing remap changes an unqualified original field')
    return candidates


def header_subjects(selection, retained, proof, check_resources):
    """Adopt current header decisions only for the complete same ordered population."""
    originals, candidates = [], []
    for path in _members(retained['fec_filing_header_associations.parquet']):
        originals.extend(pq.ParquetFile(path).read(use_threads=False).to_pylist())
    for item in selection.header_associations:
        check_resources()
        if _sha(item.path) != item.sha256:
            raise ValueError('Current header dependency changed')
        candidates.extend(pq.ParquetFile(item.path).read(use_threads=False).to_pylist())
    if len(originals) != len(candidates):
        raise ValueError('Current header decisions omit or add original observations')
    allowed = {'record_id', 'policy_version', 'selection_evidence_sha256', 'source_generation_pin'}
    for old, candidate in zip(originals, candidates, strict=True):
        if candidate.get('policy_version') != 'fec-retained-filing-association/2':
            raise ValueError('Current headers must state the maintained association policy')
        if candidate.get('source_generation_pin') != selection.source_generation_pin:
            raise ValueError('Current header decision has a foreign source generation')
        from spicy_regs.transforms.fec_filing_associations import _base
        raw = {**candidate, 'record_id': candidate['target_record_id']}
        expected = _base(raw, candidate['target_table'], candidate['source_generation_pin'],
                         candidate['selection_evidence_sha256'], candidate['association_basis'])
        if candidate['record_id'] != expected['record_id']:
            raise ValueError('Current header identity differs from its maintained source-generation rule')
        if set(old) != set(candidate) or any(old[k] != candidate[k] for k in old.keys() - allowed):
            raise ValueError('Current header decisions change original coordinates, order or decision outcomes')
        if old.get('source_generation_pin') != candidate.get('source_generation_pin') and (
                proof is None or old.get('source_generation_pin') != proof.original_pin
                or candidate.get('source_generation_pin') != proof.staged_pin):
            raise ValueError('Header source change lacks the complete staged original comparison')
    return candidates


@contextmanager
def filing_reference_subjects(selected, native_filings, *, filing_generation_scope, check_resources):
    """Resolve ordered links against the complete already-admitted self filing set.

    Keep the maintained resolver and the same bounded SQL settings as the filing
    association reader. The source-member order is explicit, not an engine order
    or a deduplicated business key. Only the resolution status changes.
    """
    import duckdb
    from spicy_regs.relationship_views.fec_typed import filing_reference_resolution
    from spicy_regs.transforms.fec_identity_context_fields import normalize_record
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy

    if filing_generation_scope != 'self' or native_filings is None:
        raise ValueError('Filing reference resolution requires complete admitted self-scoped filings')
    policy = dataset_policy('fec_filing_links')
    filing_columns = ('record_id', 'filing_key', 'source_authority', 'source_namespace',
                      'report_number', 'native_filer_id', 'form_type')
    filing_schema = pa.schema([(name, pa.string()) for name in filing_columns])
    check_resources()
    with duckdb.connect(config={'threads': 1, 'memory_limit': '128MiB',
                                'max_temp_directory_size': '0B'}) as con:
        con.register('fec_filings', pa.Table.from_pylist(native_filings, schema=filing_schema))
        branches = []
        for ordinal, item in enumerate(selected):
            check_resources()
            # These inputs have already passed the captured hash/footer/member
            # checks in build_retained_family. Paths are bound as parameters.
            name = f'link_member_{ordinal}'
            con.from_parquet(str(item.path), file_row_number=True, hive_partitioning=False).create_view(name)
            branches.append(f'SELECT *, {ordinal} AS selected_member_ordinal FROM {name}')
        if not branches:
            raise ValueError('Filing reference resolution requires the selected link members')
        con.execute('CREATE VIEW fec_filing_links AS ' + ' UNION ALL '.join(branches))
        check_resources()
        result = con.execute('SELECT * FROM (' + filing_reference_resolution() +
                             ') ORDER BY selected_member_ordinal, file_row_number').to_arrow_reader(512)

        def subjects():
            count = 0
            for batch in result:
                for part in _bounded_record_batches(batch):
                    check_resources()
                    for resolved in part.to_pylist():
                        status = resolved.pop('target_resolution_status')
                        for extra in ('resolution_policy_version', 'target_observation_count',
                                      'selected_member_ordinal', 'file_row_number'):
                            resolved.pop(extra)
                        source, _ = retained_identity_row('fec_filing_links', resolved)
                        native = normalize_record('fec_filing_links', source)
                        native['target_resolution_status'] = status
                        count += 1
                        yield {name: native[name] for name in policy.subject_schema.names}
            check_resources()
            if count != sum(item.rows for item in selected):
                raise ValueError('Filing reference resolution changed selected row membership')
            for item in selected:
                check_resources()
                if _sha(item.path) != item.sha256:
                    raise ValueError('Selected filing references changed during resolution')
        yield subjects()


def agency_source_lookup(generation, operation, coordinates, expected_pin, check_resources):
    """Qualify each disposition's exact coordinate against the admitted source.

    One DuckDB semi-join over the admitted fec_source_records subject members: the
    coordinate and digest are subject columns, so replaying every parent row with its
    receipts through Python (80.4M rows, 17.7 GB on 4cb0dffa) adds nothing here.
    """
    if not coordinates:
        return lambda row: None
    if generation is None:
        raise ValueError('Agency source navigation requires the exact admitted observations generation')
    artifact, reader, source = operation.admit_generation(generation)
    if artifact.pin.artifact_digest != expected_pin:
        raise ValueError('Agency source lookup generation differs from selected parent')
    from spicy_regs.etl_bulk import bulk_connection
    from spicy_regs.transforms.fec_query import _digest
    root = Path(generation).absolute()
    paths = [str(root / key) for key in sorted(source.keys())
             if publication.member_table(key) == 'fec_source_records.parquet']
    wanted = pa.table({'collection_id': [key[0] for key in coordinates],
                       'source_record_id': [key[1] for key in coordinates]})
    reader.check()  # the admitted member bytes are still the ones on disk
    check_resources()
    with bulk_connection() as (connection, _):
        connection.register('wanted', wanted)
        # A NULL digest counts as its own witness, as the row comparison did.
        rows = connection.execute(
            "SELECT collection_id, source_record_id, max(source_sha256),"
            " count(DISTINCT source_sha256) + max((source_sha256 IS NULL)::INTEGER)"
            " FROM read_parquet(?, hive_partitioning=false) SEMI JOIN wanted USING (collection_id, source_record_id)"
            " GROUP BY collection_id, source_record_id",
            [paths]).fetchall()
    reader.check()
    check_resources()
    found = {}
    for collection_id, source_record_id, digest, witnesses in rows:
        if witnesses != 1:
            raise ValueError('Agency source coordinate has conflicting digest witnesses')
        found[collection_id, source_record_id] = _digest(digest)
    if set(found) != coordinates:
        raise ValueError('Agency source coordinate is absent from selected observations')
    return lambda row: found[(row.get('collection_id'), row.get('source_record_id'))]


def _identity_conversion_row(table, original_row, schema, observation_rebinding, adopted=None):
    """One maintained mapping and exact retained-value restoration for either writer."""
    diagnostics = {}
    row, overrides = retained_identity_row(table, original_row, diagnostics=diagnostics, source_schema=schema)
    rebound = rebind_row(table, row, observation_rebinding)
    overrides.update({name: row[name] for name in row if row[name] != rebound[name]})
    if table == 'fec_relationships':
        overrides['record_id'] = original_row.get('record_id')
    if adopted is not None:
        overrides.update({name: original_row[name] for name in original_row.keys() & adopted.keys()
                          if original_row[name] != adopted[name]})
    source_input = {'fec_retained_overrides': overrides} if overrides else {}
    if diagnostics:
        source_input['fec_retained_interpretation'] = diagnostics
    return rebound, source_input or None


def _captured_inputs(captured, old, key, table, original, *, verify=True):
    """The captured prior's exact members for one table, checked against its publication pins.

    Each member is hashed here once per run (``verify=False`` leaves that to the frozen
    checkpoint kernel, which hashes its own inputs); later checks compare file signatures.
    """
    published_members = publication.table_members(captured, key)
    members = [original / pin.key.removeprefix(table + '/') if original.is_dir() else original
               for pin in published_members]
    if set(members) != set(_members(original)):
        raise ValueError('FEC retained member set differs from publication')
    if original.is_dir() and members != sorted(members):
        raise ValueError('Noncanonical captured FEC member order is unsupported; no reordered build is admitted')
    schema = pq.read_schema(members[0])
    if any(not pq.read_schema(p).equals(schema, check_metadata=True) for p in members):
        raise ValueError('FEC retained split member schemas differ')
    selected = []
    witnesses = []
    if len(members) != len(published_members):
        raise ValueError('FEC retained member set differs from publication')
    for path, pin in zip(members, published_members, strict=True):
        namespace = None
        if original.is_dir():
            namespace = path.relative_to(original).parts[0].removeprefix('source_namespace=')
        if pin.sha256 is None or pin.byte_size is None or pin.rows is None:
            raise ValueError('FEC conversion requires exact digest, size and row membership')
        message = 'FEC retained input differs from captured member bytes or rows'
        if path.stat().st_size != pin.byte_size or pq.ParquetFile(path).metadata.num_rows != pin.rows:
            raise ValueError(message)
        item = TypedInput(path, pin.sha256, pin.rows, namespace)
        selected.append(hashed_input(item, message) if verify else item)
        witnesses.append(dict(source_id=pin.path, source_uri=None, sha256=pin.sha256,
                              locator=None, body_version=old['artifactDigest']))
    return selected, witnesses, schema


def build_retained_family(family, old, captured, retained, work, *, operation,
                          inputs=(), filing_associations=None, check_resources=lambda: None,
                          read_snapshot=None, observation_generation=None, observation_rebinding=None,
                          observation_checkpoint=None, observation_source_pin=None, observation_hosted_main=None,
                          originals_store=None):
    """Actual production call: identity writers and qualified financial assembly.

    originals_store is the raw archive REBUILT_TABLES read (default: the shared
    CentralRawArchiveStore, opened only when the family rebuilds a table).
    """
    if observation_checkpoint is not None and (family != 'fec-observations' or observation_source_pin is None):
        raise ValueError('Observation checkpoint requires observations and exact producer source pins')
    check_membership(family, old)
    if set(retained) != set(old['tables']):
        raise ValueError('FEC retained table membership differs from captured family')
    owned_headers = ()
    snapshot = captured if read_snapshot is None else read_snapshot
    if snapshot['families'][family] != old:
        raise ValueError('FEC candidate staging changed its own captured prior')
    if family == 'fec-query':
        parent = snapshot['families'].get('fec-observations')
        if parent is None or 'etlReceipts' not in parent:
            raise ValueError('FEC query conversion requires selected native observations')
        if filing_associations is None:
            raise ValueError('FEC query conversion requires admitted filing association inputs')
        prior_selection = filing_associations
        filing_associations = rebind_filing_inputs(filing_associations, observation_rebinding,
            work / 'build' / 'rebound-filing-inputs', check_resources)
        if filing_associations is not prior_selection:
            owned_headers = filing_associations.header_associations
        if filing_associations.source_generation_pin != parent['artifactDigest']:
            raise ValueError('FEC association source differs from selected observations')
    native_filings, native_headers = None, None
    if family == 'fec-query':
        if filing_associations is None:
            raise ValueError('FEC query requires its admitted filing selection')
        if filing_associations.filing_generation_scope != 'self':
            raise ValueError('Complete FEC query conversion requires a self-scoped complete filing remap')
        native_filings = filing_subjects(filing_associations, retained, check_resources)
        native_headers = header_subjects(filing_associations, retained, observation_rebinding, check_resources)
    agency_digest_for = None
    agency = retained.get('fec_agency_mapping_dispositions.parquet')
    if agency is not None:
        coordinates = set()
        for path in _members(agency):
            for batch in pq.ParquetFile(path).iter_batches(columns=['collection_id', 'source_record_id'], batch_size=512):
                check_resources()
                coordinates.update((r['collection_id'], r['source_record_id']) for r in batch.to_pylist())
        if any(not all(isinstance(v, str) and v for v in key) for key in coordinates):
            raise ValueError('Agency source lookup needs exact collection and source record coordinates')
        agency_digest_for = agency_source_lookup(observation_generation, operation, coordinates,
            snapshot['families']['fec-observations']['artifactDigest'], check_resources)
    generation_id = uuid4().hex
    build = work / 'build'
    build.mkdir(parents=True, exist_ok=True)
    policies, subjects, partitioned, pending_partitions, rebuilt_members = [], [], {}, [], []
    shared = build / 'etl_receipts.parquet'
    observation_output = build / 'tables' / 'observations'
    with ExitStack() as stack:
        from spicy_regs.fec_observation_segments import observation_segments
        segments = (stack.enter_context(observation_segments(observation_checkpoint,
            captured_family=old, source_pin=observation_source_pin, hosted_main=observation_hosted_main)) if observation_checkpoint is not None else None)
        if segments is not None:
            generation_id = segments.generation_id
        observation_writer = (stack.enter_context(IdentityReceiptWriter(observation_output,
            generation_id=generation_id, tables=[key.removesuffix('.parquet') for key in sorted(retained)]))
            if family == 'fec-observations' and segments is None else None)
        receipt_writer = None
        if observation_writer is None:
            stream = stack.enter_context(shared.open('xb'))
            receipt_writer = stack.enter_context(pq.ParquetWriter(stream, RECEIPT_SCHEMA, compression='zstd'))
        for key, original in sorted(retained.items()):
            check_resources()
            if key in RETIRED_TABLES.get(family, ()):
                continue
            table = key.removesuffix('.parquet')
            if key in REBUILT_TABLES.get(family, {}):
                table = REBUILT_TABLES[family][key]
                if originals_store is None:
                    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
                    originals_store = CentralRawArchiveStore(create=False)
                selected, witnesses, schema, _ = rebuilt_inputs(table, build / 'rebuilt' / table,
                    store=originals_store, check_resources=check_resources)
                rebuilt_members.extend(selected)
                original = selected[0].path  # flat members: no captured source partitions apply
            else:
                selected, witnesses, schema = _captured_inputs(captured, old, key, table, original,
                                                               verify=segments is None or table in FIELD_RULES)
            output = observation_output if observation_writer is not None else build / 'tables' / table
            if table in FIELD_RULES:
                from bisect import bisect_right
                from itertools import accumulate
                member_ends = list(accumulate(i.rows for i in selected))
                def context_for(row, ordinal):
                    witness = witnesses[bisect_right(member_ends, ordinal)]
                    return ReceiptContext(generation_id, f'{table}/{ordinal}', 'fec-retained-conversion/1', [witness])
                applicable = (table != 'fec_filing_header_associations'
                              and 'filing_key' in FIELD_RULES[table]['keep'] and 'filing_key' in schema.names)
                def current_header_for(row, ordinal):
                    if native_headers is None:
                        raise ValueError('Current header interpretation has no complete admitted dependency')
                    return native_headers[ordinal]
                result = assemble_subject_table(selected, table=table, schema=schema, output=output,
                    generation_id=generation_id, context_for=context_for, check_resources=check_resources,
                    source_partitioned=original.is_dir(), filing_associations=filing_associations if applicable else None,
                    source_digest_for=agency_digest_for if table == 'fec_agency_mapping_dispositions' else None,
                    observation_rebinding=observation_rebinding,
                    current_header_for=current_header_for if table == 'fec_filing_header_associations' else None,
                    subject_members=_member_layout(original) if original.is_dir() else None)
                if not result['full_rebuild_qualified'] or result['exact_mapper_rows_compared'] != sum(i.rows for i in selected):
                    raise ValueError(f'{table}: retained FEC rows did not all convert exactly')
                from spicy_regs.transforms.fec_subject_receipts import dataset_policy
                policy = dataset_policy(table, schema)
                subject = None if result['subject_path'] is None else Path(result['subject_path'])
                receipts = Path(result['receipt_path'])
            elif segments is not None:
                from spicy_regs.transforms.fec_identity_receipts import dataset_policy
                validate_identity_schema(table, schema)
                subject, receipts = segments.prepare_table(table, selected, witnesses, schema, output,
                    prepare_row=lambda original: _identity_conversion_row(table, original, schema, observation_rebinding),
                    check_resources=check_resources)
                policy = dataset_policy(table)
            else:
                validate_identity_schema(table, schema)
                unreferenced = {name.removesuffix('.parquet') for name in _UNREFERENCED_EVIDENCE[family]}
                def rows():
                    peak_arrow, peak_python, input_rows = 0, 0, 0
                    def report():
                        (build / 'input-materialization.json').write_text(json.dumps({
                            'table': table, 'input_rows_observed': input_rows,
                            'peak_arrow_batch_bytes': peak_arrow, 'peak_python_batch_arrow_bytes': peak_python,
                            'scope': 'Progress and logical Arrow bytes; not Python heap or qualification',
                        }) + '\n')
                    report()
                    for item, witness in zip(selected, witnesses, strict=True):
                        for batch in pq.ParquetFile(item.path).iter_batches(batch_size=32, use_threads=False):
                            check_resources()
                            input_rows += batch.num_rows
                            if batch.nbytes > peak_arrow:
                                peak_arrow = batch.nbytes
                                report()
                            for part in _bounded_record_batches(batch):
                                check_resources()
                                if part.nbytes > peak_python:
                                    peak_python = part.nbytes
                                    report()
                                for row in part.to_pylist():
                                    if table == 'fec_record_evidence' and row.get('target_table') in unreferenced:
                                        continue
                                    yield row, witness
                    report()
                with (filing_reference_subjects(selected, native_filings,
                        filing_generation_scope=filing_associations.filing_generation_scope if filing_associations is not None else None,
                        check_resources=check_resources) if table == 'fec_filing_links' else nullcontext(None)) as native_links, \
                     (nullcontext(observation_writer) if observation_writer is not None else
                      IdentityReceiptWriter(output, generation_id=generation_id, tables=[table])) as writer:
                    def prepared_rows():
                        for ordinal, (row, witness) in enumerate(rows()):
                            adopted = native_filings[ordinal] if table == 'fec_filings' and native_filings is not None else None
                            adopted_link = next(native_links, None) if native_links is not None else None
                            if native_links is not None and adopted_link is None:
                                raise ValueError('Filing reference resolution lost a selected observation')
                            row, source_input = _identity_conversion_row(table, row, schema, observation_rebinding, adopted)
                            yield row, dict(input_witness=witness, source_input=source_input,
                                native_filing_subject=adopted, native_link_subject=adopted_link)
                    if table == 'fec_source_records':
                        writer.emit_batch(table, prepared_rows(), check_resources=check_resources)
                    else:
                        for row, kwargs in prepared_rows():
                            writer.emit(table, row, **kwargs)
                    if native_links is not None and next(native_links, None) is not None:
                        raise ValueError('Filing reference resolution added selected observations')
                policy = writer.policies[table]
                subject = None if policy.receipt_only else output / f'{table}.parquet'
                receipts = output / 'etl_receipts.parquet'
            policies.append(policy)
            if receipt_writer is not None:
                _consume_receipt(receipts, receipt_writer, check_resources, coalesce=segments is not None)
            if subject is not None:
                if original.is_dir():
                    # Assembly already wrote the captured members; an identity writer's flat output is split here.
                    if not subject.is_dir():
                        destination = build / 'partitioned' / table
                        if observation_writer is not None:
                            pending_partitions.append((subject, original, destination))
                            subject = destination
                        else:
                            subject = _partition_subject(subject, original, destination, check_resources)
                    partitioned[key] = old['tables'][key]['partitionColumns']
                subjects.append(subject)
        if segments is not None:
            segments.finish()
    for flat, original, destination in pending_partitions:
        _partition_subject(flat, original, destination, check_resources)
    if family == 'fec-observations' and observation_checkpoint is None:
        shared = observation_output / 'etl_receipts.parquet'
    if native_filings is not None:
        target = next(p for p in subjects if p.name == 'fec_filings.parquet')
        stored = pq.ParquetFile(target).read(use_threads=False).to_pylist()
        if len(stored) != len(native_filings) or any(
                {name: row[name] for name in candidate} != candidate
                for row, candidate in zip(stored, native_filings, strict=True)):
            raise ValueError('Self-scoped filing target differs from the exact admitted complete remap')
    parents = {} if family == 'fec-observations' else {
        key: publication.table_pin(snapshot, key) for key in family_tables('fec-observations')
        if key in snapshot['families']['fec-observations']['tables']}
    generation = build / 'generation'
    build_generation(generation, family=family, files=subjects,
        expected_keys=[p.dataset + '.parquet' for p in policies if not p.receipt_only],
        read_snapshot=snapshot, parents=parents, inputs=inputs, partitioned=partitioned,
        receipt_path=shared, receipt_policies=policies, receipt_generation_id=generation_id,
        read_operation=operation, adopt_owned_receipt=True, owned_output_root=build)
    # These bounded remaps have been consumed and admitted in the final main/
    # receipts. Resume uses that artifact and original selections, never these files.
    for item in owned_headers:
        path = item.path
        with pq.ParquetFile(path) as source:
            count = source.metadata.num_rows
        if (path.parent != build / 'rebound-filing-inputs' or path.is_symlink()
                or path.stat().st_nlink != 1 or _sha(path) != item.sha256 or count != item.rows):
            raise ValueError('Owned header remap changed before disposal')
    for item in owned_headers:
        item.path.unlink()
    # Rebuilt members are consumed into the generation; their refusal lists stay beside it.
    for item in rebuilt_members:
        if item.path.is_symlink() or item.path.stat().st_nlink != 1:
            raise ValueError('Rebuilt original member changed before disposal')
        unchanged_input(item, 'Rebuilt original member changed before disposal')
        item.path.unlink()
    if owned_headers:
        (build / 'rebound-filing-inputs').rmdir()
    return generation
