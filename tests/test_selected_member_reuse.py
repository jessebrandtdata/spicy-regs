"""Reuse actual verified member bytes; replacements and failed fetches cannot acquire pins."""
import hashlib
import types

import pytest

from spicy_regs.selected_generations import SelectedInputs
from spicy_regs.sources import publication

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def fixture(tmp_path, monkeypatch, *, mutate=None, fail=False):
    values = {'members.parquet': b'verified', 'laws.parquet': b'law', 'etl_receipts.parquet': b'receipts'}
    prefix = 'generations/test/' + 'a' * 64
    def descriptor(data):
        return {'sha256': 'sha256:' + hashlib.sha256(data).hexdigest(), 'byteSize': len(data), 'rows': 1}
    index = publication.empty_index()
    index['families']['test'] = {
        'prefix': prefix,
        'tables': {key: descriptor(data) for key, data in values.items() if key != 'etl_receipts.parquet'},
        'etlReceipts': {'key': 'etl_receipts.parquet', **descriptor(values['etl_receipts.parquet']),
                        'generationId': 'g1', 'datasets': ['members', 'laws']},
    }
    calls = []
    class Response:
        status_code = 200
        def __init__(self, data): self.data = data
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def iter_bytes(self):
            if fail:
                raise RuntimeError('broken stream')
            yield self.data
    def stream(method, url, **kwargs):
        calls.append(url)
        return Response(values[url.rsplit('/', 1)[1]])
    monkeypatch.setattr(publication.httpx, 'stream', stream)
    def info(message, label):
        if mutate and message == 'Downloaded {} from R2' and label.endswith('/members.parquet'):
            mutate(tmp_path / 'selected' / '.members' / prefix / 'members.parquet')
    monkeypatch.setattr(publication, 'logger', types.SimpleNamespace(info=info))
    return SelectedInputs(tmp_path, tmp_path / 'selected', index=index, public_url='https://fixture.invalid'), calls


def replace(path):
    swap = path.with_name('replacement')
    swap.write_bytes(b'replaced')
    swap.replace(path)


def test_verified_download_replaced_before_handoff_refuses(tmp_path, monkeypatch):
    reader, _ = fixture(tmp_path, monkeypatch, mutate=replace)
    with pytest.raises(ValueError, match='after download verification'):
        reader.select('members')
    assert not reader.checked and not reader.fetched and not reader.cache


def test_shared_member_downloads_once_and_provenance_rechecks(tmp_path, monkeypatch):
    reader, calls = fixture(tmp_path, monkeypatch)
    # A real stream download already hashes these bytes. Reuse must add no disk hash.
    monkeypatch.setattr('spicy_regs.selected_generations._pin', lambda _: pytest.fail('duplicate hash'))
    assert reader.select('absent') is None and not calls
    first, second = reader.select('members'), reader.select('laws')
    assert first.receipts == second.receipts and len(calls) == 3
    assert reader.select('members') == first
    assert reader.member_pin(first.receipts)['byteSize'] == 8
    assert len(calls) == 3


@pytest.mark.parametrize('mutation', ['write', 'replace', 'symlink'])
def test_same_instance_reselection_and_provenance_refuse_change(tmp_path, monkeypatch, mutation):
    reader, _ = fixture(tmp_path, monkeypatch)
    chosen = reader.select('members')
    target = chosen.receipts
    if mutation == 'write':
        target.write_bytes(b'mutated!')
    elif mutation == 'replace':
        replace(target)
    else:
        original = target.with_name('original')
        target.replace(original)
        target.symlink_to(original)
    for call in (lambda: reader.select('members'), lambda: reader.member_pin(target)):
        with pytest.raises((ValueError, RuntimeError)):
            call()


def test_failed_fetch_does_not_cache_verified_pin(tmp_path, monkeypatch):
    reader, _ = fixture(tmp_path, monkeypatch, fail=True)
    with pytest.raises(RuntimeError, match='broken stream'):
        reader.select('members')
    assert not reader.checked and not reader.fetched and not reader.cache
    assert not list((tmp_path / 'selected').rglob('*.tmp'))


def test_processing_pin_refuses_concurrent_hash_replacement(tmp_path, monkeypatch):
    from spicy_regs import selected_generations
    path = tmp_path / 'processing.parquet'
    path.write_bytes(b'original')
    reader = SelectedInputs(tmp_path, tmp_path / 'selected', public_url='')
    original_pin = selected_generations._pin
    def pin_and_replace(target):
        digest = original_pin(target)
        replace(target)
        return digest
    monkeypatch.setattr(selected_generations, '_pin', pin_and_replace)
    with pytest.raises(ValueError, match='while hashing'):
        reader.member_pin(path)
    assert not reader.checked


def test_processing_pin_refuses_same_instance_change(tmp_path):
    path = tmp_path / 'processing.parquet'
    path.write_bytes(b'original')
    reader = SelectedInputs(tmp_path, tmp_path / 'selected', public_url='')
    assert reader.member_pin(path)['byteSize'] == 8
    replace(path)
    with pytest.raises(ValueError, match='changed during'):
        reader.member_pin(path)


def test_boolean_adapter_hash_handoff_replacement_refuses(tmp_path, monkeypatch):
    from spicy_regs import selected_generations
    reader, _ = fixture(tmp_path, monkeypatch)
    real_fetch = publication.fetch_member
    monkeypatch.setattr(publication, 'fetch_member', lambda *args: bool(real_fetch(*args)))
    real_pin = selected_generations._member_pin
    def pin_and_replace(path, checked):
        value = real_pin(path, checked)
        replace(path)
        return value
    monkeypatch.setattr(selected_generations, '_member_pin', pin_and_replace)
    with pytest.raises(ValueError, match='adapter verification handoff'):
        reader.select('members')
    assert not reader.fetched and not reader.cache


def test_boolean_adapter_size_pin_disagreement_refuses(tmp_path, monkeypatch):
    reader, _ = fixture(tmp_path, monkeypatch)
    reader.index['families']['test']['tables']['members.parquet']['byteSize'] += 1
    def adapter(base, member, target, label):
        target.write_bytes(b'verified')
        return True
    monkeypatch.setattr(publication, 'fetch_member', adapter)
    with pytest.raises(ValueError, match='byte size pin'):
        reader.select('members')
    assert not reader.fetched and not reader.cache


def test_public_download_works_without_writer_only_rulespec_package(tmp_path, monkeypatch):
    import sys
    from spicy_regs.local_data import file_state_from_stat
    reader, _ = fixture(tmp_path, monkeypatch)
    member, = publication.table_members(reader.index, 'members.parquet')
    monkeypatch.setitem(sys.modules, 'rulespec_artifacts', None)
    target = tmp_path / 'download.parquet'
    downloaded = publication.fetch_member('https://fixture.invalid', member, target)
    assert isinstance(downloaded, publication.DownloadedMember)
    assert downloaded.sha256 == member.sha256
    assert downloaded.state == file_state_from_stat(target.lstat())


def test_runtime_download_in_fresh_process_without_rulespec():
    import subprocess
    import sys
    from pathlib import Path

    script = Path(__file__).resolve().parents[1] / 'deploy/cloudflare/runtime_readers.py'
    code = '''
import importlib.abc
import runpy
import sys
class NoWriterPackage(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'rulespec_artifacts' or fullname.startswith('rulespec_artifacts.'):
            raise ImportError('writer-only dependency unavailable in runtime')
sys.meta_path.insert(0, NoWriterPackage())
runpy.run_path(sys.argv[1], run_name='__main__')
'''
    subprocess.run([sys.executable, '-c', code, str(script)], check=True, capture_output=True, text=True)
