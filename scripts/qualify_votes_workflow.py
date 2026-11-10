"""Measure complete pinned vote conversion and private publication/MCP serving.

The caller enforces the whole wall-clock limit and samples process-tree RSS and
private disk allocation. This command exercises the real publisher and MCP over
a disk-backed test client and streaming loopback HTTP. Hosted R2 and deployed
MCP remain separate qualification; no production credentials or writes are used.
Run from the checkout with ``uv run --frozen python -m scripts.qualify_votes_workflow``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import time

import pyarrow.parquet as pq
from starlette.testclient import TestClient
from rulespec_artifacts import LocalMemberSource, admit_artifact

from spicy_regs.sources.disk_store import DiskStore
from scripts.retained_qualification_support import (
    assert_pin, call, capture_code_pins, child, copy_exact, phase, records, serve, verify_code_pins,
)
from scripts.qualify_congress_bulk import compare_files
from spicy_regs.congress_receipts import policy
from spicy_regs.votes_batch_receipts import restore_processing_input, write_congress_dataset
from spicy_regs.etl_receipts import combine_receipts
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication


def mcp_controls(base: str, output: Path, subjects: dict[str, Path], *, policies=None):
    import spicy_regs.public_url as public_url
    setattr(public_url, 'resolve_r2_base_url', lambda value=None: base)
    os.environ['SPICY_REGS_MEMORY_LIMIT'] = '4GB'
    os.environ['SPICY_REGS_TEMP_DIR'] = str(output / 'mcp-spill')
    os.environ['SPICY_REGS_HOME_DIR'] = str(output / 'mcp-home')
    for name in ('R2_CATALOG_URI', 'R2_CATALOG_WAREHOUSE', 'R2_CATALOG_TOKEN'):
        os.environ.pop(name, None)
    (output / 'mcp-home').mkdir()
    from spicy_regs import mcp_server
    mcp_server.R2_BASE_URL, mcp_server.DATA_DIR = base, None
    mcp_server.MEMORY_LIMIT = '4GB'
    mcp_server.TEMP_DIR = str(output / 'mcp-spill')
    mcp_server.HOME_DIRECTORY = str(output / 'mcp-home')
    production_security = mcp_server._apply_security_settings

    def bounded_security(con, allowed_paths=None):
        con.execute('SET threads=4')
        con.execute("SET max_temp_directory_size='32GB'")
        production_security(con, allowed_paths)

    setattr(mcp_server, '_apply_security_settings', bounded_security)
    con = mcp_server._build_connection()
    setattr(mcp_server, '_get_connection', lambda: con)
    replies = {}
    try:
        settings = dict(con.execute("SELECT name,value FROM duckdb_settings() WHERE name IN "
            "('threads','memory_limit','max_temp_directory_size','temp_directory','enable_external_access',"
            "'lock_configuration')").fetchall())
        with TestClient(mcp_server.build_app()) as http:
            for dataset, source in subjects.items():
                described = call(http, 'describe_table', {'table': dataset})
                if described.get('isError') or not described['structuredContent']['schema_matches_declared']:
                    raise ValueError(f'Native MCP schema differs for {dataset}')
                count = call(http, 'query_sql', {'sql': f'SELECT count(*) AS rows FROM {dataset}'})
                if records(count) != [{'rows': pq.read_metadata(source).num_rows}]:
                    raise ValueError(f'Native MCP population differs for {dataset}')
                with pq.ParquetFile(source) as parquet:
                    first = next(parquet.iter_batches(batch_size=1)).to_pylist()[0]
                # Exact declared identity selects the source row without relying on SQL scan order.
                filters = []
                declared = policy(dataset) if policies is None else policies[dataset]
                for name in declared.identity_fields:
                    value = first[name]
                    literal = 'NULL' if value is None else "'" + str(value).replace("'", "''") + "'"
                    filters.append(f'"{name}" IS NOT DISTINCT FROM {literal}')
                row = call(http, 'query_sql', {'sql': f'SELECT * FROM {dataset} WHERE ' + ' AND '.join(filters)})
                if records(row) != [json.loads(json.dumps(first, default=str))]:
                    raise ValueError(f'Native MCP source values differ for {dataset}')
                replies[dataset] = {'describe': described, 'count': count, 'sourceRow': row}
            present_table = next(iter(subjects))
            for sql in (f'DELETE FROM "{present_table}" WHERE FALSE',
                        "SELECT * FROM read_parquet('https://unmapped.invalid/forbidden.parquet')"):
                refusal = call(http, 'query_sql', {'sql': sql})
                if refusal.get('isError') is not True:
                    raise ValueError('MCP failed to refuse an unauthorized control')
                replies[sql] = refusal
        return {'settings': settings, 'replies': replies,
                'transport': 'real /mcp HTTP through TestClient; production connection and security checks',
                'schemeOverride': 'loopback HTTP only; production HTTPS resolver replaced explicitly'}
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--fixture', action='store_true', help='Declared bounded producer fixture, not full qualification')
    args = parser.parse_args()
    started = time.monotonic()
    args.output.mkdir(parents=True, exist_ok=False)
    log = args.output / 'phases.jsonl'
    with phase(log, 'frozen-plan-code-and-dependency-pins') as record:
        raw_plan = args.plan.read_bytes()
        plan = json.loads(raw_plan)
        (args.output / 'plan-pinned.json').write_bytes(raw_plan)
        frozen_code = capture_code_pins()
        if frozen_code['dirty'] and not args.fixture:
            raise ValueError('Full qualification requires a clean frozen checkout')
        record.update(frozen_code, fixture=args.fixture, planSha256=hashlib.sha256(raw_plan).hexdigest())
    inputs = {entry['dataset']: entry for entry in plan['sources']}
    if len(plan['sources']) != 2 or set(inputs) != {'member_votes', 'member_vote_terms'}:
        raise ValueError('Whole workflow requires both exact vote inputs')
    inputs['roll_call_votes'] = plan['completeFamilySibling']
    with phase(log, 'all-original-source-pins'):
        for entry in inputs.values():
            assert_pin(Path(entry['localInput']), {name: entry['bytes' if name == 'byteSize' else name]
                for name in ('sha256', 'byteSize', 'rows')})
    subjects, receipts = {}, {}
    labels = {'member_votes': 'qualification-roll-call-votes', 'roll_call_votes': 'qualification-roll-call-votes',
              'member_vote_terms': 'qualification-member-vote-terms'}
    for dataset in ('member_votes', 'member_vote_terms'):
        entry = inputs[dataset]
        directory = args.output / dataset
        with phase(log, dataset + '-full-source-writer-validator-restore-history'):
            child('qualify_congress_bulk.py', [entry['localInput'], str(directory), '--dataset', dataset,
                '--sha256', entry['sha256'], '--generation', labels[dataset]])
        subjects[dataset] = directory / 'bundle' / (dataset + '.parquet')
        receipts[dataset] = directory / 'carried-receipts.parquet'
    with phase(log, 'roll-call-family-sibling-write-and-admit'):
        source = args.output / 'roll-call-original.parquet'
        copy_exact(Path(inputs['roll_call_votes']['localInput']), source)
        subject, receipt = write_congress_dataset(source, args.output / 'roll-calls',
            dataset='roll_call_votes', generation_id=labels['roll_call_votes'])
        if subject is None:
            raise ValueError('Complete roll-call family lacks native roll calls')
        subjects['roll_call_votes'], receipts['roll_call_votes'] = subject, receipt
        shared = combine_receipts([receipt, receipts['member_votes']], args.output / 'roll-call-receipts.parquet')
    with phase(log, 'roll-call-family-sibling-full-restore-and-compare') as record:
        restored = restore_processing_input(subject, receipt, args.output / 'roll-call-restored.parquet',
            dataset='roll_call_votes', generation_id=labels['roll_call_votes'])
        record['rows'] = compare_files(source, restored)
    store = DiskStore(args.output / 'private-store', bucket='votes-qualification')
    index = publication.empty_index()
    artifacts = {}
    for family, datasets in [('roll-call-votes', ['roll_call_votes', 'member_votes']),
                             ('member-vote-terms', ['member_vote_terms'])]:
        receipt = shared if family == 'roll-call-votes' else receipts['member_vote_terms']
        generation = args.output / (family + '-generation')
        with phase(log, family + '-complete-generation-admission'):
            artifacts[family] = build_generation(generation, family=family, files=[subjects[name] for name in datasets],
                expected_keys=[name + '.parquet' for name in datasets], receipt_path=receipt,
                receipt_policies=[policy(name) for name in datasets], receipt_generation_id=labels[datasets[0]])
        with phase(log, family + '-actual-private-publication-upload-readback'):
            index = publication.publish_generation(generation, client=store, bucket=store.bucket, prior_index=index)
    (args.output / 'private-publication.json').write_text(json.dumps(index, indent=2) + '\n')
    (args.output / 'private-store-calls.json').write_text(json.dumps(store.calls, indent=2) + '\n')
    marker = {'probe': 'actual-mcp-startup-and-controls'}
    with phase(log, 'actual-private-publication-mcp-connection-and-http-controls') as record:
        with serve({'/' + key: path for key, path in store.objects.items()},
                   args.output / 'mcp-file-http-requests.jsonl', marker) as base:
            controls = mcp_controls(base, args.output, subjects)
            replies = args.output / 'mcp-controls.json'
            replies.write_text(json.dumps(controls, indent=2) + '\n')
            record.update(settings=controls['settings'], controls=str(replies), subjects=list(subjects),
                          transport=controls['transport'], schemeOverride=controls['schemeOverride'])
    with phase(log, 'final-private-artifact-and-served-member-pins'):
        for family, artifact in artifacts.items():
            admit_artifact(LocalMemberSource(args.output / (family + '-generation')), expected_pin=artifact.pin)
            admit_artifact(publication._S3Members(store, store.bucket, index['families'][family]['prefix']),
                           expected_pin=artifact.pin)
    with phase(log, 'final-all-original-source-pins'):
        for entry in inputs.values():
            assert_pin(Path(entry['localInput']), {name: entry['bytes' if name == 'byteSize' else name]
                for name in ('sha256', 'byteSize', 'rows')})
        entry = inputs['roll_call_votes']
        assert_pin(args.output / 'roll-call-original.parquet', {name: entry['bytes' if name == 'byteSize' else name]
            for name in ('sha256', 'byteSize', 'rows')})
    verify_code_pins(log, frozen_code)
    result = {'status': 'passed', 'seconds': time.monotonic() - started, 'fixture': args.fixture,
              'scope': 'private disk-store publication and actual loopback MCP; hosted R2/public MCP unqualified',
              'productionActions': [], 'driverSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (args.output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
