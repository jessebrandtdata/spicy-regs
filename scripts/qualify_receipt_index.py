"""Measure an explicit receipt index, local disk upload and locked HTTP reads.

This is local qualification. It does not publish, select or upload remote data.
Pass a receipt pin admitted by the owning bundle validator. The caller measures
the complete votes workflow wall clock, including this command and both inputs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import resource
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.receipt_key_index import KEY, check_reader, lookup_receipts, verify_key_index
from spicy_regs.receipt_key_index_writer import build_key_index
from spicy_regs.sources.publication import file_identity


def append(path: Path, record: dict):
    with path.open('a') as sink:
        sink.write(json.dumps(record, sort_keys=True)+'\n')


@contextmanager
def phase(log: Path, name: str):
    started = time.monotonic()
    record: dict[str, Any] = {'phase': name, 'status': 'running', 'scope': 'local-simulation',
                            'startedMonotonicNs': time.monotonic_ns()}
    append(log, record)
    try:
        yield record
        record['status'] = 'passed'
    except BaseException as error:
        record.update(status='failed', errorType=type(error).__name__, error=str(error))
        raise
    finally:
        record.update(seconds=time.monotonic()-started, finishedMonotonicNs=time.monotonic_ns(),
            peakRssBytes=resource.getrusage(
            resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform == 'darwin' else 1024))
        append(log, record)
        print(json.dumps(record, sort_keys=True), flush=True)


def assert_pin(path: Path, pin: dict):
    if set(pin) != {'sha256', 'byteSize', 'rows'} or type(pin.get('sha256')) is not str or (
            re.fullmatch(r'sha256:[0-9a-f]{64}', pin['sha256']) is None) or (
            type(pin.get('byteSize')) is not int or pin['byteSize'] <= 0) or (
            type(pin.get('rows')) is not int or pin['rows'] < 0):
        raise ValueError('Receipt pin must have exact canonical hash, byte-size and row fields')
    actual = file_identity(path)
    if actual['sha256'] != pin['sha256'] or actual['bytes'] != pin['byteSize'] or (
            pq.read_metadata(path).num_rows != pin['rows']):
        raise ValueError('Receipt differs from its admitted hash, byte-size or footer row pin')


def copy_exact(source: Path, target: Path) -> dict:
    """Stream real bytes to private local object storage, then independently hash both."""
    before = file_identity(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open('rb') as origin, target.open('xb') as sink:
        for chunk in iter(lambda: origin.read(1024*1024), b''):
            sink.write(chunk)
    if file_identity(target) != before or file_identity(source) != before:
        raise ValueError('Local uploaded member or original source bytes changed')
    return before


def bind_key_index_for_qualification(directory: Path, index: Path, descriptor: dict):
    """Explicitly bind a measured sidecar to a fresh PRIVATE complete-family artifact.

    The caller owns this scratch generation. Never call on retained or published
    artifacts. No scheduled writer imports this script. The returned artifact is
    re-admitted through the production reader; publication still belongs to the
    existing private-store publication harness.
    """
    from rulespec_artifacts import (ArtifactInput, KnownLimit, LocalMemberSource, Producer, Supersedes,
        build_artifact_root, canonical_json_bytes, describe_member, iter_member_descriptors, write_member_manifest)
    from spicy_regs.generations import MANIFEST, verify_generation
    artifact = verify_generation(directory)
    specification = dict(artifact.root['spec']['etlReceipts'])
    if 'keyIndex' in specification:
        raise ValueError('Qualification generation already declares an index')
    receipts = directory/specification['key']
    held = file_identity(receipts)
    receipt = {'sha256': held['sha256'], 'byteSize': held['bytes'], 'rows': specification['rows']}
    assert_pin(receipts, receipt)
    actual = file_identity(index)
    if actual['sha256'] != descriptor['sha256'] or actual['bytes'] != descriptor['byteSize']:
        raise ValueError('Measured key-index bytes differ from its descriptor')
    verify_key_index(receipts, index, descriptor, receipt)
    target = directory/KEY
    if target.resolve() != index.resolve():
        copy_exact(index, target)
    source = LocalMemberSource(directory)
    members = list(iter_member_descriptors(artifact, source))
    if any(member.object_key == KEY for member in members):
        raise ValueError('Qualification generation already has an index member')
    members.append(describe_member(source, object_key=KEY, role='table',
        media_type='application/vnd.apache.parquet', record_count=receipt['rows']))
    specification['keyIndex'] = descriptor
    with (directory/MANIFEST).open('wb') as sink:
        manifest = write_member_manifest(sink, scope_kind='global', scope_id=artifact.root['spec']['family'],
            object_key=MANIFEST, members=sorted(members, key=lambda member: member.object_key))
    root = artifact.root
    sealed = build_artifact_root(kind=root['kind'], spec={**root['spec'], 'etlReceipts': specification},
        producer=Producer.from_dict(root['producer'], path='producer'),
        inputs=[ArtifactInput.from_dict(value, path='inputs') for value in root['inputs']],
        known_limits=[KnownLimit.from_dict(value, path='knownLimits') for value in root.get('knownLimits', [])],
        supersedes=Supersedes.from_dict(root['supersedes'], path='supersedes') if 'supersedes' in root else None,
        manifests=[manifest])
    (directory/'artifact.json').write_bytes(canonical_json_bytes(sealed))
    return verify_generation(directory)


@contextmanager
def serve(objects: dict[str, Path], request_log: Path, marker: dict[str, str]):
    """Serve only immutable, explicitly mapped objects; retain real body/range bytes."""
    log_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_HEAD(self):
            self.transfer(False)

        def do_GET(self):
            self.transfer(True)

        def transfer(self, body: bool):
            began = time.monotonic_ns()
            probe = marker['probe']
            path = objects.get(self.path)
            if path is None:
                self.send_error(404)
                return
            size = path.stat().st_size
            start, end, status = 0, size-1, 200
            requested = self.headers.get('Range')
            if requested:
                try:
                    if not requested.startswith('bytes=') or ',' in requested:
                        raise ValueError('Unsupported range')
                    left, right = requested[6:].split('-', 1)
                    if left:
                        start = int(left)
                        end = min(int(right), size-1) if right else size-1
                    else:
                        suffix = int(right)
                        if suffix <= 0:
                            raise ValueError('Invalid suffix')
                        start = max(0, size-suffix)
                    if start < 0 or start > end or start >= size:
                        raise ValueError('Invalid range')
                    status = 206
                except ValueError:
                    self.send_response(416)
                    self.send_header('Content-Range', f'bytes */{size}')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
            self.send_response(status)
            self.send_header('Content-Length', str(end-start+1))
            self.send_header('Accept-Ranges', 'bytes')
            if status == 206:
                self.send_header('Content-Range', f'bytes {start}-{end}/{size}')
            self.end_headers()
            sent, failure = 0, None
            try:
                if body:
                    with path.open('rb') as stream:
                        stream.seek(start)
                        remaining = end-start+1
                        while remaining:
                            chunk = stream.read(min(remaining, 1024*1024))
                            if not chunk:
                                raise ValueError('Served member truncated')
                            self.wfile.write(chunk)
                            sent += len(chunk)
                            remaining -= len(chunk)
            except (OSError, ValueError) as error:
                failure = str(error)
                raise
            finally:
                with log_lock:
                    append(request_log, {'method': self.command, 'object': self.path,
                        'range': requested, 'start': start, 'end': end, 'status': status,
                        'bodyBytes': sent, 'error': failure, 'probe': probe,
                        'startedMonotonicNs': began, 'finishedMonotonicNs': time.monotonic_ns()})

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


def install_local_httpfs(con, path: Path):
    """Seed this connection's private cache; ordinary LOAD validates signature/version."""
    local = path.resolve(strict=True)
    if not local.is_file() or local.name != 'httpfs.duckdb_extension':
        raise ValueError('Expected an exact local httpfs.duckdb_extension file')
    con.execute("INSTALL '" + str(local).replace("'", "''") + "'")


def connection(paths: list[str], *, remote: bool, scratch: Path, httpfs_extension: Path | None = None):
    from spicy_regs.mcp_server import _apply_security_settings
    con = duckdb.connect()
    try:
        con.execute('SET threads=4')
        con.execute("SET memory_limit='1GB'")
        con.execute("SET max_temp_directory_size='32GB'")
        con.execute('SET allow_persistent_secrets=false')
        if remote:
            home = scratch / 'duckdb-home'
            extensions = scratch / 'duckdb-extensions'
            home.mkdir(exist_ok=True)
            extensions.mkdir(exist_ok=True)
            con.execute('SET home_directory=?', [str(home)])
            con.execute('SET extension_directory=?', [str(extensions)])
            if httpfs_extension is not None:
                install_local_httpfs(con, httpfs_extension)
            load_public_http(con, retries={})
            con.execute('SET http_timeout=60')
        _apply_security_settings(con, paths)
        return con
    except BaseException:
        con.close()
        raise


def requests(index: Path, dataset: str, scratch: Path) -> list[list[str]]:
    with duckdb.connect() as con:
        con.execute('SET threads=4')
        con.execute("SET memory_limit='1GB'")
        con.execute("SET max_temp_directory_size='32GB'")
        con.execute('SET temp_directory=?', [str(scratch/'sample-spill')])
        keys = [r[0] for r in con.execute("SELECT record_id FROM read_parquet(?) WHERE dataset=? "
            "AND outcome='accepted' ORDER BY record_id LIMIT 100", [str(index), dataset]).fetchall()]
        if not keys:
            raise ValueError('No actual accepted producer keys to qualify serving')
        absent = 'sha256:'+hashlib.sha256(b'qualification-absent').hexdigest()
        if con.execute("SELECT count(*) FROM read_parquet(?) WHERE dataset=? AND outcome='accepted' "
                       'AND record_id=?', [str(index), dataset, absent]).fetchone() != (0,):
            raise ValueError('Absent-key probe unexpectedly exists; choose another before the run')
    return [[keys[len(keys)//2]], keys[:10], keys, [keys[0], absent, keys[0]]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('receipts', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--pin', type=Path, required=True, help='JSON {sha256,byteSize,rows} admitted receipt pin')
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--member', action='append', type=Path, default=[], help='Admitted subject member to copy too')
    parser.add_argument('--force-small-fixture', action='store_true')
    parser.add_argument('--httpfs-extension', type=Path, help='Exact installed signed httpfs file to seed the private cache offline')
    args = parser.parse_args()
    started = time.monotonic()
    args.receipts = args.receipts.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    scratch = args.output/'scratch'
    scratch.mkdir()
    # This is a standalone private measurement process. Attribute all spill here.
    os.environ['SPICY_REGS_TEMP_DIR'] = str(scratch/'reader-spill')
    os.environ['SPICY_REGS_MEMORY_LIMIT'] = '1GB'
    tempfile.tempdir = str(scratch)
    log = args.output/'phases.jsonl'
    receipt = json.loads(args.pin.read_text())
    with phase(log, 'receipt-pin'):
        assert_pin(args.receipts, receipt)
    with phase(log, 'key-index-build-and-complete-map') as record:
        descriptor = build_key_index(args.receipts, args.output/KEY, force=args.force_small_fixture)
        if descriptor is None:
            raise ValueError('Receipt is below index threshold; force only a declared small producer fixture')
        record['descriptor'] = descriptor
        (args.output/'descriptor.json').write_text(json.dumps(descriptor, indent=2)+'\n')
    with phase(log, 'sample-keys-and-local-locked-oracle') as record:
        probes = requests(args.output/KEY, args.dataset, scratch)
        with connection([str(args.receipts), str(args.output/KEY)], remote=False, scratch=scratch) as con:
            expected = [lookup_receipts(con.cursor(), str(args.receipts), str(args.output/KEY), descriptor,
                receipt, dataset=args.dataset, record_ids=probe) for probe in probes]
        if [len(rows) for rows in expected[-1]] != [1, 0, 1] or expected[-1][0] != expected[-1][2]:
            raise ValueError('Repeated/absent local probe differs')
        record['actualProducerKeys'] = len(probes[2])
    objects = {}
    with phase(log, 'local-upload-and-exact-copy-admission') as record:
        members = []
        for source in [args.receipts, args.output/KEY, *args.member]:
            pin = file_identity(source)
            route = '/'+pin['sha256'].removeprefix('sha256:')+'/'+source.name
            target = args.output/'objects'/route.lstrip('/')
            identity = copy_exact(source, target)
            objects[route] = target
            members.append({'source': str(source), 'route': route, **identity})
        record['members'] = members
        (args.output/'objects.json').write_text(json.dumps(members, indent=2)+'\n')
    with phase(log, 'local-http-startup-cold-admission-and-warm-locked-serving') as record:
        marker = {'probe': 'cold-admission'}
        with serve(objects, args.output/'http-requests.jsonl', marker) as base:
            locations = {member['source']: base+member['route'] for member in members}
            receipt_url, index_url = locations[str(args.receipts)], locations[str(args.output/KEY)]
            before = time.monotonic()
            with connection([receipt_url, index_url], remote=True, scratch=scratch,
                            httpfs_extension=args.httpfs_extension) as con:
                check_reader(con.cursor(), index_url, descriptor, receipt)
                record['coldAdmissionSeconds'] = time.monotonic()-before
                record['lookups'] = []
                names = ['one-key', 'up-to-ten-keys', 'up-to-hundred-keys', 'repeat-and-absent']
                for name, probe, oracle in zip(names, probes, expected, strict=True):
                    marker['probe'] = name
                    began = time.monotonic_ns()
                    before = time.monotonic()
                    found = lookup_receipts(con.cursor(), receipt_url, index_url, descriptor, receipt,
                        dataset=args.dataset, record_ids=probe)
                    if found != oracle:
                        raise ValueError('HTTP returned rows differ from exact local producer receipts')
                    record['lookups'].append({'requested': len(probe), 'seconds': time.monotonic()-before,
                        'probe': name, 'startedMonotonicNs': began, 'finishedMonotonicNs': time.monotonic_ns(),
                        'returnedPerKey': [len(rows) for rows in found],
                        'replyJsonBytes': len(json.dumps(found).encode())})
    with phase(log, 'final-original-and-served-byte-pins'):
        assert_pin(args.receipts, receipt)
        for member in members:
            wanted = {name: member[name] for name in ('sha256', 'bytes', 'etag')}
            if file_identity(Path(member['source'])) != wanted or file_identity(objects[member['route']]) != wanted:
                raise ValueError('An original or served member changed during measurement')
    (args.output/'RESULT.json').write_text(json.dumps({'status': 'passed', 'scope': 'local-simulation',
        'seconds': time.monotonic()-started, 'dataset': args.dataset,
        'productionActions': [], 'wholeVotesBudget': 'caller must measure both datasets and all other phases',
        'hostedUploadAndPublicMcp': 'not qualified by this command'}, indent=2)+'\n')


if __name__ == '__main__':
    main()
