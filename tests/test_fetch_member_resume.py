"""fetch_member resumes a reset transfer by exact byte range, hashes each byte once, and refuses a changed object."""
import hashlib
import types

import httpx
import pytest

from spicy_regs.runtime_bounds import RequestBound
from spicy_regs.sources import publication

DATA = bytes(range(256)) * 40  # 10,240 bytes
ETAG = '"v1"'


def pinned(data=DATA):
    return publication.Member('generations/t/' + 'a' * 64 + '/t.parquet',
                              'sha256:' + hashlib.sha256(data).hexdigest(), len(data), 1)


class Reply:
    """One scripted response: status, headers, the body, and an optional ``error`` after ``fail_after`` bytes."""

    def __init__(self, status, body=b'', headers=None, fail_after=None, error=None):
        self.status_code, self.body, self.fail_after = status, body, fail_after
        self.headers = httpx.Headers(headers or {})
        self.error = error or httpx.RemoteProtocolError('peer closed connection without sending complete message body')

    def __enter__(self): return self
    def __exit__(self, *args): return False

    def iter_bytes(self):
        sent = self.body if self.fail_after is None else self.body[:self.fail_after]
        for start in range(0, len(sent), 1000):
            yield sent[start:start + 1000]
        if self.fail_after is not None:
            raise self.error


class Unanswered:
    """A request whose response headers never arrive: httpx raises its read timeout on entering the stream."""

    def __enter__(self): raise httpx.ReadTimeout('The read operation timed out')
    def __exit__(self, *args): return False


def partial(start, data=DATA, etag=ETAG, **kwargs):
    return Reply(206, data[start:], {'etag': etag, 'content-range': f'bytes {start}-{len(data) - 1}/{len(data)}'},
                 **kwargs)


@pytest.fixture
def host(monkeypatch):
    """Serve scripted replies in order; record each request's kwargs and every byte fed to a digest."""
    script, calls, hashed = [], [], []

    def stream(method, url, **kwargs):
        calls.append(kwargs)
        return script.pop(0)

    class Counting:
        def __init__(self): self.inner = hashlib.sha256()
        def update(self, data):
            hashed.append(len(data))
            self.inner.update(data)
        def hexdigest(self): return self.inner.hexdigest()

    monkeypatch.setattr(publication.httpx, 'stream', stream)
    monkeypatch.setattr(publication, 'hashlib', types.SimpleNamespace(sha256=Counting))
    monkeypatch.setattr(publication.time, 'sleep', lambda seconds: None)
    return types.SimpleNamespace(script=script, calls=calls, hashed=hashed)


def no_temp(tmp_path):
    return [p.name for p in tmp_path.iterdir()] == []


def test_reset_resumes_with_exact_range_and_hashes_each_byte_once(tmp_path, host):
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=3000), partial(3000, fail_after=4000), partial(7000)]
    target = tmp_path / 't.parquet'
    result = publication.fetch_member('https://fixture.invalid', pinned(), target)
    assert isinstance(result, publication.DownloadedMember)
    assert target.read_bytes() == DATA and result.sha256 == pinned().sha256
    assert sum(host.hashed) == len(DATA)
    assert 'headers' not in host.calls[0]  # the first request keeps its original call shape
    assert host.calls[1]['headers'] == {'Range': 'bytes=3000-', 'If-Match': ETAG, 'Accept-Encoding': 'identity'}
    assert host.calls[2]['headers']['Range'] == 'bytes=7000-'
    assert [p.name for p in tmp_path.iterdir()] == ['t.parquet']


def test_caller_headers_are_kept_on_resume(tmp_path, host):
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=10), partial(10)]
    publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't', headers={'User-Agent': 'x'})
    assert host.calls[1]['headers'] == {'User-Agent': 'x', 'Range': 'bytes=10-', 'If-Match': ETAG,
                                        'Accept-Encoding': 'identity'}


@pytest.mark.parametrize('answer', [lambda: Reply(200, DATA, {'etag': '"v2"'}), lambda: Reply(412)])
def test_changed_object_or_ignored_range_restarts_from_zero(tmp_path, host, answer):
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=5000), answer()]
    if host.script[-1].status_code == 412:
        host.script.append(Reply(200, DATA, {'etag': '"v2"'}))
    target = tmp_path / 't.parquet'
    publication.fetch_member('https://fixture.invalid', pinned(), target)
    assert target.read_bytes() == DATA  # the stale prefix was truncated, not kept
    assert sum(host.hashed) == 5000 + len(DATA)
    assert host.script == []
    if len(host.calls) == 3:  # after a 412 the next request starts over without a range
        assert 'headers' not in host.calls[2]


@pytest.mark.parametrize('reply', [
    lambda: Reply(206, DATA[3001:], {'etag': ETAG, 'content-range': f'bytes 3001-10239/{len(DATA)}'}),
    lambda: Reply(206, DATA[3000:9000], {'etag': ETAG, 'content-range': f'bytes 3000-8999/{len(DATA)}'}),
    lambda: partial(3000, etag='"v2"'),
    lambda: Reply(500),
])
def test_wrong_resume_answer_refuses_and_leaves_nothing(tmp_path, host, reply):
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=3000), reply()]
    with pytest.raises(RuntimeError, match='exact remaining range'):
        publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't.parquet')
    assert no_temp(tmp_path)


def test_reconnects_without_progress_stop_at_the_cap(tmp_path, host):
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=100)]
    host.script += [partial(100, fail_after=0) for _ in range(publication.RESUME_STALLS)]
    with pytest.raises(RuntimeError, match='without progress'):
        publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't.parquet')
    assert len(host.calls) == 1 + publication.RESUME_STALLS and no_temp(tmp_path)


def test_a_restart_that_progresses_is_not_a_stall(tmp_path, host):
    """Progress after a restart is measured from byte 0, not from the abandoned resume offset."""
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=8000)]
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=1000) for _ in range(publication.RESUME_STALLS)]
    host.script.append(partial(1000))
    target = tmp_path / 't.parquet'
    publication.fetch_member('https://fixture.invalid', pinned(), target)
    assert target.read_bytes() == DATA and host.script == []


@pytest.mark.parametrize('headers', [{}, {'etag': 'W/"v1"'}, {'etag': ETAG, 'content-encoding': 'gzip'}])
def test_unresumable_response_restarts_from_zero(tmp_path, host, headers):
    """No strong ETag pins a range: the held bytes are discarded and the whole body is asked for again."""
    host.script += [Reply(200, DATA, headers, fail_after=100), Reply(200, DATA, headers)]
    target = tmp_path / 't.parquet'
    publication.fetch_member('https://fixture.invalid', pinned(), target)
    assert target.read_bytes() == DATA and sum(host.hashed) == 100 + len(DATA)
    assert 'Range' not in host.calls[1].get('headers', {}) and host.script == []


def test_unresumable_restarts_stop_at_the_cap(tmp_path, host):
    host.script += [Reply(200, DATA, {}, fail_after=100) for _ in range(publication.RESUME_STALLS)]
    with pytest.raises(RuntimeError, match='no ETag to resume on'):
        publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't.parquet')
    assert len(host.calls) == publication.RESUME_STALLS and no_temp(tmp_path)


def test_a_timeout_before_the_headers_asks_again(tmp_path, host):
    """Run 42875578221 died on one ReadTimeout waiting for a member's headers, after httpx's 5-second default."""
    host.script += [Unanswered(), Reply(200, DATA, {'etag': ETAG})]
    target = tmp_path / 't.parquet'
    result = publication.fetch_member('https://fixture.invalid', pinned(), target)
    assert isinstance(result, publication.DownloadedMember) and result.sha256 == pinned().sha256
    assert target.read_bytes() == DATA
    assert [call['timeout'] for call in host.calls] == [publication.MEMBER_REQUEST_TIMEOUT] * 2
    assert 'headers' not in host.calls[1]  # nothing was held: the same request again, not a range


def test_a_timeout_mid_body_resumes_once(tmp_path, host):
    timeout = httpx.ReadTimeout('The read operation timed out')
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=3000, error=timeout), partial(3000)]
    target = tmp_path / 't.parquet'
    publication.fetch_member('https://fixture.invalid', pinned(), target)
    assert target.read_bytes() == DATA and sum(host.hashed) == len(DATA)
    assert host.calls[1]['headers']['Range'] == 'bytes=3000-'


def test_a_host_that_always_times_out_stops_at_the_cap(tmp_path, host):
    host.script += [Unanswered() for _ in range(publication.RESUME_STALLS)]
    with pytest.raises(RuntimeError, match='without progress') as raised:
        publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't.parquet')
    assert isinstance(raised.value.__cause__, httpx.ReadTimeout)
    assert len(host.calls) == publication.RESUME_STALLS and no_temp(tmp_path)


def test_a_caller_timeout_is_kept(tmp_path, host):
    host.script += [Reply(200, DATA, {'etag': ETAG})]
    publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't', timeout=7)
    assert host.calls[0]['timeout'] == 7


def test_bytes_that_differ_from_the_pin_after_a_retry_still_refuse(tmp_path, host):
    """A retried request is held to the pin like the first: integrity failures are never retried."""
    host.script += [Unanswered(), Reply(200, DATA[:-1] + b'x', {'etag': ETAG})]
    with pytest.raises(RuntimeError, match='differs from its pin'):
        publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't.parquet')
    assert len(host.calls) == 2 and no_temp(tmp_path)


def test_unpinned_member_resumes_on_its_etag(tmp_path, host):
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=100), partial(100)]
    member = publication.Member('comments.parquet', None, None, None)
    result = publication.fetch_member('https://fixture.invalid', member, tmp_path / 'c.parquet')
    assert isinstance(result, publication.DownloadedMember)
    assert (tmp_path / 'c.parquet').read_bytes() == DATA and result.byte_size == len(DATA)


def test_resumed_bytes_that_differ_from_the_pin_leave_no_temp_file(tmp_path, host):
    wrong = DATA[:100] + b'x' * (len(DATA) - 100)
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=100), partial(100, data=wrong)]
    with pytest.raises(RuntimeError, match='differs from its pin'):
        publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't.parquet')
    assert no_temp(tmp_path)


def test_caller_deadline_bounds_all_reconnects(tmp_path, host, monkeypatch):
    bound = RequestBound(60)
    def expire(seconds):
        bound.deadline = 0  # the deadline passes while backing off
    monkeypatch.setattr(publication.time, 'sleep', expire)
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=100), partial(100)]
    with pytest.raises(TimeoutError), bound.scope():
        publication.fetch_member('https://fixture.invalid', pinned(), tmp_path / 't.parquet')
    assert len(host.calls) == 1 and no_temp(tmp_path)


def test_verify_member_resumes_through_the_same_body(host):
    """verify_member streams through the same resuming body and keeps its identity request headers."""
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=3000), partial(3000)]
    assert publication.verify_member('https://fixture.invalid', pinned()) == len(DATA)
    assert sum(host.hashed) == len(DATA)
    assert host.calls[1]['headers'] == {'User-Agent': 'spicy-regs', 'Accept-Encoding': 'identity',
                                        'Range': 'bytes=3000-', 'If-Match': ETAG}


def test_pin_holds_across_a_resumed_range(host):
    """Bytes a resumed range carries are held to the pin like the first response's."""
    changed = DATA[:3000] + bytes(len(DATA) - 3000)
    host.script += [Reply(200, DATA, {'etag': ETAG}, fail_after=3000), partial(3000, data=changed)]
    with pytest.raises(RuntimeError, match='differs from its pin'):
        publication.verify_member('https://fixture.invalid', pinned())
