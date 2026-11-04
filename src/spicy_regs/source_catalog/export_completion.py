"""Native export completion in the existing trusted local selection cache.

Only the exporter issues these records, after validating every emitted member.
They are local custody evidence, not portable attestations supplied with exports.
Readers recheck member bytes and every external reference decision; transferred
or unadmitted exports still use selection's canonical replay.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import tempfile

from .storage import digest, stable
from .progress import phase


def implementation():
    from spicy_docs.storage import raw_archive

    paths = [*Path(__file__).parent.glob('*.py'), Path(raw_archive.__file__)]
    return {
        'sources': {str(path.resolve()): digest(path) for path in sorted(paths)},
        'python': sys.version,
        'packages': {name: importlib.metadata.version(name) for name in ('spicy-docs', 'pyarrow')},
    }


def observation(path):
    from .selection import identity, plain

    path = plain(path)
    return list(identity(path)) if path.exists() else None


def check_references(expected):
    for name, before in expected.items():
        if observation(Path(name)) != before:
            raise ValueError(f'Export reference changed: {name}')


def completion_path(cache, index, output):
    from .selection import plain

    return plain(cache / '.export-completions' / (stable(str(index), str(output)) + '.json'))


def completion_digest(record):
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def load(cache, index, output, generation, *, page_size=None):
    """Missing is a replay; present but invalid is a refusal, never a cache miss."""
    from .selection import check_identities, identity, metadata, public_members, read, require

    path = completion_path(cache, index, output)
    if not path.exists():
        return None
    before = identity(path)
    try:
        saved = read(path)
        record = saved['payload']
        require(saved['sha256'] == completion_digest(record), 'Export completion checksum differs')
        require(record['version'] == 1 and record['index'] == str(index)
                and record['output'] == str(output) and record['generation'] == generation
                and record['implementation'] == implementation(), 'Export completion identity differs')
        require(page_size is None or record['pageSize'] == page_size, 'Export completion page size differs')
        require(record['candidateCustody'] == list(identity(index)), 'Export candidate changed')
        require(metadata(index)['generation'] == generation, 'Export candidate generation differs')
        require(len(record['indexSha256']) == 71 and record['indexSha256'].startswith('sha256:'),
                'Invalid export candidate hash')
        check_references(record['references'])
        with phase('export-member-check'):
            members, identities = public_members(output, generation, decode=False)
        require(members == record['members'], 'Export completion members differ')
        check_identities({path: before, index: tuple(record['candidateCustody']), **identities},
                         'Export completion changed while checking')
        check_references(record['references'])
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError('Invalid native export completion') from error
    return record, identities


def export_completed(index, output, cache, *, page_size=5000):
    """Issue or recover one validated export before returning its CLI response."""
    from .exporter import export
    from .selection import check_identities, identity, metadata, plain, public_members, read, require, selection_lock

    index, output, cache = (plain(path) for path in (index, output, cache))
    require(cache.is_dir() and output != cache and not cache.is_relative_to(output),
            'Export completion requires a separate existing selection cache')
    with selection_lock(cache):
        generation = metadata(index)['generation']
        previous = load(cache, index, output, generation, page_size=page_size)
        if previous is not None:
            record, _ = previous
            return read(output / 'manifest.json'), record['members']
        candidate = identity(index)
        code = implementation()
        references, emitted = {}, {}

        def emit(path, body):
            path = plain(path)
            name = str(path.relative_to(output))
            require(name not in emitted, 'Repeated export member')
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                require(path.read_bytes() == body, f'Immutable export collision: {path}')
            else:
                with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
                    temporary = Path(stream.name)
                    try:
                        stream.write(body)
                        stream.flush()
                        os.fsync(stream.fileno())
                        os.replace(temporary, path)
                    finally:
                        temporary.unlink(missing_ok=True)
            emitted[name] = {'path': name, 'bytes': len(body),
                             'sha256': 'sha256:' + hashlib.sha256(body).hexdigest()}

        with phase('export-encode-validate'):
            export(index, output, page_size=page_size, emit=emit, reference_dependencies=references)
        # Encoding already validated rows. Check the persisted bytes and layout
        # without decoding them again, and bind the candidate hash once here.
        with phase('export-member-check'):
            members, identities = public_members(output, generation, decode=False)
        require(members == sorted(emitted.values(), key=lambda item: item['path']),
                'Written export differs from validated output')
        with phase('candidate-hash'):
            index_sha = digest(index)
        check_references(references)
        metadata(index)
        check_identities({index: candidate, **identities}, 'Export inputs or output changed')
        require(implementation() == code, 'Export implementation changed')
        record = {'version': 1, 'index': str(index), 'output': str(output), 'generation': generation,
                  'candidateCustody': list(candidate), 'indexSha256': index_sha, 'implementation': code,
                  'pageSize': page_size, 'members': members, 'references': references}
        target = completion_path(cache, index, output)
        target.parent.mkdir(exist_ok=True)
        # The response may be lost after this atomic commit. A retry uses this
        # owned record before touching catalogue rows or original payloads.
        with tempfile.NamedTemporaryFile(mode='w', dir=target.parent, delete=False) as stream:
            temporary = Path(stream.name)
            try:
                json.dump({'payload': record, 'sha256': completion_digest(record)}, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return read(output / 'manifest.json'), members
