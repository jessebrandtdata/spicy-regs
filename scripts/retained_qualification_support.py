"""Shared controls for current retained regulatory qualification only."""
from __future__ import annotations

import hashlib
import json
import os
import re
import resource
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from importlib.metadata import version
from itertools import zip_longest
from pathlib import Path
import subprocess
import sys

import pyarrow.parquet as pq
from starlette.testclient import TestClient

from spicy_regs.generations import implementation_id, source_digest
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


def compare_files(source: Path, restored: Path):
    expected, actual = pq.ParquetFile(source), pq.ParquetFile(restored)
    if not expected.schema_arrow.equals(actual.schema_arrow, check_metadata=True):
        raise ValueError('Restore schema or metadata differs from the original source')
    rows = 0
    for a, b in zip_longest(expected.iter_batches(batch_size=10000), actual.iter_batches(batch_size=10000)):
        if a is None or b is None or not a.equals(b):
            raise ValueError(f'Restored source differs at batch beginning at row {rows}')
        rows += a.num_rows
    if rows != expected.metadata.num_rows:
        raise ValueError('Full restore row count differs from source footer')
    return rows


def child(script: str, arguments: list[str]):
    subprocess.run([sys.executable, '-m', 'scripts.' + script.removesuffix('.py'), *arguments],
                   cwd=Path(__file__).resolve().parent.parent, check=True)


def capture_code_pins():
    """Read code and dependency identity afresh, including subprocess helpers."""
    import rulespec_artifacts
    import spicy_docs

    checkout = Path(__file__).resolve().parent.parent
    return {
        'codeHead': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip(),
        'dirty': subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout, text=True),
        'implementationId': implementation_id(),
        'qualificationScriptsSha256': source_digest(checkout / 'scripts'),
        'spicyDocsCodeSha256': source_digest(Path(spicy_docs.__file__).parent, ('.py', '.json', '.xsd')),
        'rulespecCodeSha256': source_digest(Path(rulespec_artifacts.__file__).parent, ('.py', '.json', '.xsd')),
        'lockSha256': hashlib.sha256((checkout / 'uv.lock').read_bytes()).hexdigest(),
        'packages': {name: version(name) for name in
                     ('spicy-regs', 'spicy-docs', 'rulespec-artifacts', 'duckdb', 'pyarrow')},
    }


def verify_code_pins(log: Path, frozen: dict):
    with phase(log, 'final-code-and-dependency-pins') as record:
        current = capture_code_pins()
        record.update(current)
        if current != frozen:
            changed = sorted(key for key in current.keys() | frozen.keys() if current.get(key) != frozen.get(key))
            raise ValueError('Code or dependency pins changed during qualification: ' + ', '.join(changed))


def call(http, name, arguments):
    response = http.post('/mcp', headers={'Accept': 'application/json, text/event-stream',
        'MCP-Protocol-Version': '2025-06-18'}, json={'jsonrpc': '2.0', 'id': 'qualification',
        'method': 'tools/call', 'params': {'name': name, 'arguments': arguments}})
    if response.status_code != 200:
        raise ValueError(f'MCP HTTP status {response.status_code}')
    messages = [json.loads(line[5:]) for line in response.text.splitlines() if line.startswith('data:')]
    return messages[-1]['result']


def records(reply):
    if reply.get('isError'):
        raise ValueError(f'MCP refused a qualification control: {reply}')
    data = reply['structuredContent']
    return [dict(zip(data['columns'], row, strict=True)) for row in data['rows']]


def mcp_controls(base: str, output: Path, subjects: dict[str, Path], *, policies=None, httpfs_extension=None):
    import spicy_regs.public_url as public_url
    setattr(public_url, 'resolve_r2_base_url', lambda value=None: base)
    os.environ['SPICY_REGS_MEMORY_LIMIT'] = '4GB'
    os.environ['SPICY_REGS_TEMP_DIR'] = str(output / 'mcp-spill')
    os.environ['SPICY_REGS_HOME_DIR'] = str(output / 'mcp-home')
    for name in ('R2_CATALOG_URI', 'R2_CATALOG_WAREHOUSE', 'R2_CATALOG_TOKEN'):
        os.environ.pop(name, None)
    (output / 'mcp-home').mkdir()
    if httpfs_extension is not None:
        import duckdb

        with duckdb.connect() as seed:
            seed.execute('SET home_directory=?', [str(output / 'mcp-home')])
            install_local_httpfs(seed, httpfs_extension)
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
                if policies is None:
                    raise ValueError('MCP qualification requires declared receipt policies')
                declared = policies[dataset]
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
