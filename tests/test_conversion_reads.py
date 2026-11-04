"""Actual admitted operation lifetime, exact reconstruction, and changed-source refusals."""
from contextlib import contextmanager
from dataclasses import replace
from io import BytesIO

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import etl_bulk
from spicy_regs.congress_receipts import policy, write_congress_dataset
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.generations import build_generation


def build(tmp_path, *, operation=None, canonical_root=None):
    original = tmp_path / 'members-original.parquet'
    schema = pa.schema([('bioguide_id', pa.string()), ('name_first', pa.string())], metadata={b'original': b'exact'})
    pq.write_table(pa.Table.from_pylist([{'bioguide_id': 'X', 'name_first': '  Thomas  '}], schema=schema), original)
    subject, receipts = write_congress_dataset(original, tmp_path / 'bundle', dataset='members', generation_id='g')
    assert subject is not None
    directory = tmp_path / 'generation'
    artifact = build_generation(directory, family='members', files=[subject], expected_keys=['members.parquet'],
                                receipt_path=receipts, receipt_policies=[policy('members')], receipt_generation_id='g',
                                read_operation=operation, canonical_root=canonical_root)
    if canonical_root is not None:
        directory = canonical_root / artifact.pin.artifact_digest.removeprefix('sha256:')
    return directory, artifact, original


@pytest.mark.parametrize('reference', [False, True])
def test_same_admission_exact_replay_and_streamed_original(tmp_path, monkeypatch, reference):
    directory, artifact, original = build(tmp_path)
    if reference:
        @contextmanager
        def not_bulk(*args, **kwargs):
            raise etl_bulk.NotBulkEligible('reference control')
            yield
        monkeypatch.setattr(etl_bulk, '_validated_bundle', not_bulk)
    with ConversionReadOperation() as operation:
        actual, reader, source = operation.admit_generation(directory, expected_pin=artifact.pin)
        assert actual.pin == artifact.pin
        assert operation.admit_generation(directory) == (actual, reader, source)
        assert reader.compare_original('members', original)['rows_only_in_retained'] == 0
        assert reader.outcome_counts()['accepted'] == 1
        assert list(reader.read_subjects('members'))[0]['name_first'] == '  Thomas  '
        assert operation.verified_source(directory) == (actual, source)
        with pytest.raises(ValueError, match='expected pin'):
            operation.admit_generation(directory, expected_pin=ArtifactPin(actual.pin.logical_id, 'sha256:' + 'f' * 64))
        operation.release_generation(directory)
        assert operation.verified_source(directory) == (actual, source)
        with pytest.raises(ValueError, match='closed'):
            reader.compare_original('members', original)
    with pytest.raises(ValueError, match='live conversion'):
        operation.verified_source(directory)


def test_canonical_adoption_uses_final_paths(tmp_path):
    canonical = tmp_path / 'generations'
    canonical.mkdir()
    with ConversionReadOperation() as operation:
        directory, artifact, original = build(tmp_path, operation=operation, canonical_root=canonical)
        assert not (tmp_path / 'generation').exists()
        actual, reader, _ = operation.admit_generation(directory)
        assert actual.pin == artifact.pin
        assert reader.compare_original('members', original)['rows_only_in_restored'] == 0


@pytest.mark.parametrize('mutation', ['replace', 'write', 'symlink'])
def test_verified_source_and_replay_refuse_later_member_change(tmp_path, mutation):
    directory, _, _ = build(tmp_path)
    with pytest.raises((ValueError, RuntimeError)), ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        path = directory / 'etl_receipts.parquet'
        if mutation == 'write':
            with path.open('ab') as stream:
                stream.write(b'changed')
        else:
            changed = directory / 'changed'
            changed.write_bytes(path.read_bytes())
            if mutation == 'replace':
                changed.replace(path)
            else:
                path.unlink()
                path.symlink_to(changed)
        with pytest.raises((ValueError, RuntimeError)):
            operation.verified_source(directory)
        with pytest.raises((ValueError, RuntimeError)):
            list(reader.read_subjects('members'))


def test_concurrent_stream_and_closed_stream_refuse(tmp_path):
    directory, _, _ = build(tmp_path)
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        stream = reader.read_subjects('members')
        next(stream)
        with pytest.raises(ValueError, match='active family stream'):
            list(reader.processing('members'))
        stream.close()
        assert list(reader.processing('members'))
    with pytest.raises(ValueError, match='closed'):
        list(reader.processing('members'))


def test_exact_descriptor_disagreement_refuses(tmp_path):
    original = tmp_path / 'original.parquet'
    pq.write_table(pa.table({'bioguide_id': ['X'], 'name_first': ['Thomas']}), original)
    subject, receipts = write_congress_dataset(original, tmp_path / 'bundle', dataset='members', generation_id='g')
    assert subject is not None
    changed = replace(policy('members'), receipt_fields=(*policy('members').receipt_fields, 'unknown'))
    with ConversionReadOperation() as operation, pytest.raises(ValueError):
        build_generation(tmp_path / 'generation', family='members', files=[subject], expected_keys=['members.parquet'],
                         receipt_path=receipts, receipt_policies=[changed], receipt_generation_id='g', read_operation=operation)


@pytest.mark.parametrize('release', [False, True])
def test_started_stream_refuses_after_owner_exit_or_release(tmp_path, release):
    directory, _, _ = build(tmp_path)
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        stream = reader.processing('members')
        next(stream)
        if release:
            operation.release_generation(directory)
            with pytest.raises(ValueError, match='closed'):
                next(stream)
    if not release:
        with pytest.raises(ValueError, match='closed'):
            next(stream)


def test_owner_closes_platform_state_index(tmp_path):
    directory, _, _ = build(tmp_path)
    with ConversionReadOperation() as operation:
        artifact, _, _ = operation.admit_generation(directory)
        assert len(artifact.local_member_states) == 2
        operation.release_generation(directory)
        assert len(artifact.local_member_states) == 2
    with pytest.raises(RuntimeError, match='closed'):
        len(artifact.local_member_states)


def test_reference_cancelled_dataset_does_not_poison_next_dataset(tmp_path, monkeypatch):
    from spicy_regs.etl_receipts import combine_receipts
    _, _, _ = build(tmp_path)
    original = tmp_path / 'actions.parquet'
    pq.write_table(pa.table({'bill_id': ['119-hr-1'], 'action_index': ['0'], 'action_text': ['Repeated text']}), original)
    action, receipt = write_congress_dataset(original, tmp_path / 'actions', dataset='bill_actions', generation_id='g')
    assert action is not None
    combined = tmp_path / 'receipts.parquet'
    combine_receipts([tmp_path / 'bundle' / 'etl_receipts.parquet', receipt], combined)
    directory = tmp_path / 'multi'
    build_generation(directory, family='mixed', files=[tmp_path / 'bundle' / 'members.parquet', action],
                     expected_keys=['members.parquet', 'bill_actions.parquet'], receipt_path=combined,
                     receipt_policies=[policy('members'), policy('bill_actions')], receipt_generation_id='g')
    @contextmanager
    def not_bulk(*args, **kwargs):
        raise etl_bulk.NotBulkEligible('reference control')
        yield
    monkeypatch.setattr(etl_bulk, '_validated_bundle', not_bulk)
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        stream = reader.read_subjects('members')
        next(stream)
        stream.close()
        assert len(list(reader.read_subjects('bill_actions'))) == 1
        assert len(list(reader.read_subjects('members'))) == 1


def test_failed_admission_cleanup_closes_platform_index_even_if_reader_cleanup_refuses(tmp_path, monkeypatch):
    from spicy_regs import generations
    directory, _, _ = build(tmp_path)
    original = generations._verify_generation_source
    captured = []
    def verify_then_change(*args, **kwargs):
        artifact = original(*args, **kwargs)
        captured.append(artifact)
        (directory / 'unexpected').write_bytes(b'changed')
        return artifact
    monkeypatch.setattr(generations, '_verify_generation_source', verify_then_change)
    with ConversionReadOperation() as operation, pytest.raises(ValueError, match='changed after admission'):
        operation.admit_generation(directory)
    assert len(captured) == 1
    with pytest.raises(RuntimeError, match='closed'):
        len(captured[0].local_member_states)


@pytest.mark.parametrize('upload, damage, verified_by', [
    ('checksummed', None, {'sha256': 2, 'md5': 0, 'streamed': 0}),
    ('multipart', None, {'sha256': 2, 'md5': 0, 'streamed': 0}),
    ('etag-only', None, {'sha256': 0, 'md5': 2, 'streamed': 0}),
    ('multipart-etag-only', None, {'sha256': 0, 'md5': 2, 'streamed': 0}),
    ('multipart-etag-other-width', None, {'sha256': 0, 'md5': 0, 'streamed': 2}),
    ('multipart-etag-same-count-width', None, {'sha256': 0, 'md5': 0, 'streamed': 2}),
    ('multipart-etag-unknown', None, {'sha256': 0, 'md5': 0, 'streamed': 2}),
    *(('checksummed', damage, None) for damage in
      ('root', 'manifest', 'subject', 'receipt', 'missing', 'oversize', 'binding')),
    ('multipart', 'subject', None), ('etag-only', 'subject', None), ('multipart-etag-only', 'subject', None),
    ('multipart-etag-other-width', 'subject', None), ('multipart-etag-same-count-width', 'subject', None),
    ('multipart-etag-unknown', 'subject', None),
])
def test_public_bytes_match_live_candidate_without_copy_or_second_admission(tmp_path, monkeypatch, upload, damage,
                                                                            verified_by):
    """Each member is held to its admitted pin by storage's own hash, one HEAD and no body; a member storage holds no
    comparable hash for streams from the public URL instead. Any differing byte refuses, and nothing is written."""
    from rulespec_artifacts import iter_member_descriptors
    from spicy_regs.sources import publication
    from tests.generation_fakes import Store

    directory, _, _ = build(tmp_path)
    store = Store()
    if upload.startswith('multipart'):
        monkeypatch.setattr(publication, 'PART_BYTES', 2048)  # several parts per member
    stored_part_bytes = 1024 if upload == 'multipart-etag-other-width' else publication.PART_BYTES
    with ConversionReadOperation() as operation:
        artifact, reader, source = operation.admit_generation(directory)
        entry = publication.generation_entry(artifact, list(iter_member_descriptors(artifact, source)))
        index = {'families': {'members': entry}}
        stored = {entry['prefix'] + '/' + key: (directory / key).read_bytes() for key in source.keys()}
        if upload == 'multipart-etag-same-count-width':
            stored_part_bytes = next(width for width in (2049, 2047) if all(
                (len(raw) + width - 1) // width == (len(raw) + 2047) // 2048
                for key, raw in stored.items() if key.endswith('.parquet')))
        monkeypatch.setattr(publication, 'PART_BYTES', stored_part_bytes)
        for key, raw in stored.items():
            publication._put_immutable_stream(store, 'bucket', key, lambda raw=raw: BytesIO(raw), len(raw))
            # The uploader sends Content-MD5 alone, so a hash storage reports is the only way to reach these branches.
            if upload == 'checksummed':
                store.checksums[key] = None
            elif upload == 'multipart':
                store.checksums[key] = [len(raw[start:start + publication.PART_BYTES])
                                        for start in range(0, len(raw), publication.PART_BYTES)]
        if upload in {'multipart-etag-other-width', 'multipart-etag-same-count-width'}:
            monkeypatch.setattr(publication, 'PART_BYTES', 2048)
        if upload == 'multipart-etag-unknown':
            for key in stored:
                if key.endswith('.parquet'):
                    store.etags[key] = '"opaque-multipart-layout"'
        assert all(len(store.objects[key]) > publication.PART_BYTES * 2 for key in stored
                   if key.endswith('.parquet')) == upload.startswith('multipart')
        if damage == 'binding':
            entry['tables']['members.parquet']['rows'] += 1
        elif damage:
            key = entry['prefix'] + '/' + {
                'root': 'artifact.json', 'manifest': artifact.root['memberManifests'][0]['objectKey'],
                'subject': 'members.parquet', 'receipt': 'etl_receipts.parquet', 'missing': 'etl_receipts.parquet',
                'oversize': 'members.parquet'}[damage]
            if damage == 'missing':
                del store.objects[key]
            elif damage == 'oversize':
                store.objects[key] += b'additional bytes'
            else:
                raw = store.objects[key]
                store.objects[key] = raw[:100] + bytes([raw[100] ^ 1]) + raw[101:]
                if upload in {'multipart-etag-only', 'multipart-etag-other-width', 'multipart-etag-same-count-width'}:
                    # Model a wrong upload, whose storage-computed ETag covers its actual bytes.
                    # Keeping the old ETag after editing fake storage models later at-rest decay,
                    # outside every HEAD-only checksum's promise.
                    import hashlib
                    damaged = store.objects[key]
                    parts = [hashlib.md5(damaged[start:start + stored_part_bytes], usedforsecurity=False).digest()
                             for start in range(0, len(damaged), stored_part_bytes)]
                    store.etags[key] = f'"{hashlib.md5(b"".join(parts), usedforsecurity=False).hexdigest()}-{len(parts)}"'
        base = 'https://test.invalid'
        monkeypatch.setattr(publication, '_bounded_get', lambda url, **kwargs: store.objects.get(url.removeprefix(base + '/')))
        streamed = []

        @contextmanager
        def stream(method, url, **kwargs):
            assert kwargs['headers']['Accept-Encoding'] == 'identity'
            assert not reader.active  # Validation scratch is released before the network pass.
            streamed.append(url)
            raw = store.objects.get(url.removeprefix(base + '/'))
            yield httpx.Response(404 if raw is None else 200, content=raw or b'', request=httpx.Request(method, url))

        monkeypatch.setattr(httpx, 'stream', stream)
        operation.release_generation(directory)
        before = sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob('*'))
        reads = sum(store.reads.values())
        heads = []
        original_head = store.head_object

        def head(**kwargs):
            heads.append(kwargs['Key'])
            return original_head(**kwargs)

        monkeypatch.setattr(store, 'head_object', head)
        if damage:
            with pytest.raises((ValueError, RuntimeError), match='Public generation|Published generation'):
                operation.verify_public_generation(base, index, 'members', directory, store, 'bucket')
        else:
            checked = operation.verify_public_generation(base, index, 'members', directory, store, 'bucket')
            members = [entry['prefix'] + '/' + key for key in ('members.parquet', 'etl_receipts.parquet')]
            assert checked == {'artifactDigest': artifact.pin.artifact_digest, 'members': 2,
                               'bytes': sum(len(store.objects[key]) for key in members), 'verified_by': verified_by,
                               'streamed_bytes': sum(len(store.objects[key]) for key in members) if streamed else 0,
                               'root_and_manifests': 'exact', 'retained_payload_copies': 0}
            assert sorted(streamed) == (sorted(base + '/' + key for key in members) if verified_by['streamed'] else [])
            assert sorted(heads) == sorted(members)
        assert sum(store.reads.values()) == reads  # storage bodies are never read: HEAD only
        # Nothing is written: no public copy is kept on disk.
        assert sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob('*')) == before


def test_multipart_md5_proof_rechecks_local_sha256_and_uses_bounded_reads(monkeypatch):
    import hashlib
    from spicy_regs.sources import publication
    from tests.generation_fakes import Store

    raw = b'one exact local candidate' * 11
    store = Store()
    monkeypatch.setattr(publication, 'PART_BYTES', 31)
    publication._put_immutable_stream(store, 'bucket', 'member', lambda: BytesIO(raw), len(raw))
    member = publication.Member('member', 'sha256:' + hashlib.sha256(raw).hexdigest(), len(raw), None)
    reads = []

    class Bounded(BytesIO):
        def read(self, size=-1):
            assert size == 31
            value = super().read(size)
            reads.append(len(value))
            return value

    assert publication.verify_stored_member(store, 'bucket', member, lambda: Bounded(raw)) == 'md5'
    assert sum(reads) == len(raw) and store.reads == {}
    with pytest.raises(RuntimeError, match='Local candidate differs'):
        publication.verify_stored_member(store, 'bucket', member, lambda: Bounded(b'!' + raw[1:]))
