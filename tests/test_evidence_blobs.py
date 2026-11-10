"""Shared content-addressed evidence blobs: stored once, shown equal to the local bytes once, refused when damaged."""

from collections import Counter
from hashlib import sha256
from io import BytesIO
import tempfile

from botocore.exceptions import ClientError
import pytest
from rulespec_artifacts import ArtifactVerificationError

from spicy_regs.source_evidence import CaptureEvidence, EvidenceMemberSource
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_source_evidence import candidate, capture, read_evidence_member

BODY = b"retained source bytes" * 64


def evidence_run(tmp_path, name, *bodies):
    evidence = CaptureEvidence(tmp_path / name, "test")
    for body in bodies:
        evidence.capture(capture(body), stage="used")
    directory, _ = candidate(tmp_path / name, evidence)
    return directory, evidence


def publish(store, directory, evidence, prior=None):
    return pub.publish_generation(directory, client=store, bucket="test", prior_index=prior or pub.empty_index(),
                                  evidence_directories=(evidence.artifact_dir,))


def blob_keys(evidence):
    return sorted("source-evidence/" + key for key in EvidenceMemberSource(evidence.artifact_dir).keys()
                  if key.startswith("blobs/sha256/"))


class Counting(Store):
    """Counts HEADs per key, and local opens per blob member through ``watch``."""

    def __init__(self):
        super().__init__()
        self.heads = Counter()

    def head_object(self, *, Bucket, Key, ChecksumMode=None):
        self.heads[Key] += 1
        return super().head_object(Bucket=Bucket, Key=Key, ChecksumMode=ChecksumMode)


def test_new_blob_costs_one_head_one_put_and_no_get(tmp_path, monkeypatch):
    """ss-ag8i: the serial read-back of each new blob was 104 of 130 minutes per 20,000 (run 42674437011)."""
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY, b"a second response", b"a third response")
    local_opens, opened = Counter(), EvidenceMemberSource.open

    def counted(self, object_key):
        local_opens["source-evidence/" + object_key] += 1
        return opened(self, object_key)

    monkeypatch.setattr(EvidenceMemberSource, "open", counted)
    store = Counting()
    publish(store, directory, evidence)
    blobs = blob_keys(evidence)
    assert len(blobs) == 3
    for key in blobs:
        assert (store.heads[key], store.writes.count(key), store.reads[key]) == (1, 1, 0)
        assert store.sent[key] == len(store.objects[key])
        assert sha256(store.objects[key]).hexdigest() == key.rsplit("/", 1)[1]
    # Each local copy is read for the build's own verification, once to be hashed and sent in one buffer,
    # and once by the admission that binds the artifact to those bytes.
    before_publication = 1
    assert all(local_opens[key] == before_publication + 2 for key in blobs)
    prefix = "source-evidence/" + evidence.artifact.pin.artifact_digest.removeprefix("sha256:")
    assert not any(key.startswith(prefix + "/blobs/") for key in store.objects)


def test_identical_evidence_uploads_and_reads_back_no_blob_bytes(tmp_path):
    for name in ("one", "two"):
        (tmp_path / name).mkdir()
    first_dir, first = evidence_run(tmp_path, "one", BODY)
    store = Store()
    prior = publish(store, first_dir, first)
    second_dir, second = evidence_run(tmp_path, "two", BODY)
    assert second.artifact.pin != first.artifact.pin
    blobs = blob_keys(second)
    assert blobs == blob_keys(first)
    sent = dict(store.sent)
    publish(store, second_dir, second, prior)
    assert all(store.sent[key] == sent[key] and store.reads[key] == 0 for key in blobs)
    assert store.writes[-1] == pub.INDEX_V2_KEY


def test_only_the_new_blob_of_a_mixed_run_is_sent_and_verified(tmp_path):
    for name in ("one", "two"):
        (tmp_path / name).mkdir()
    first_dir, first = evidence_run(tmp_path, "one", BODY)
    store = Store()
    prior = publish(store, first_dir, first)
    second_dir, second = evidence_run(tmp_path, "two", BODY, b"a new response")
    [new] = sorted(set(blob_keys(second)) - set(blob_keys(first)))
    publish(store, second_dir, second, prior)
    assert store.writes.count(new) == 1
    assert not any(store.reads[key] for key in blob_keys(second))


def refused(store, directory, evidence, error=ArtifactVerificationError):
    with pytest.raises(error):
        publish(store, directory, evidence)
    assert pub.INDEX_V2_KEY not in store.objects
    assert not any(key.startswith("generations/") for key in store.writes)


def test_existing_object_with_other_bytes_is_never_overwritten_and_refuses(tmp_path):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    store = Store()
    [blob] = blob_keys(evidence)
    other = bytes([BODY[0] ^ 1]) + BODY[1:]  # same size, different bytes: only the stored identity can tell
    store.objects[blob] = other
    refused(store, directory, evidence)
    assert store.objects[blob] == other and blob not in store.writes


@pytest.mark.parametrize("same_bytes", [True, False])
def test_key_created_between_the_head_and_the_put_is_read_back_not_overwritten(tmp_path, same_bytes):
    """Another writer, or this publisher's own request retried after a lost answer, created the key first."""
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    store = Store()
    [blob] = blob_keys(evidence)
    first = BODY if same_bytes else bytes([BODY[0] ^ 1]) + BODY[1:]

    def race(key):
        if key == blob:
            store.objects.setdefault(blob, first)

    store.before_put = race
    if same_bytes:
        publish(store, directory, evidence)
        assert store.reads[blob] == 1
    else:
        refused(store, directory, evidence)
    assert store.objects[blob] == first and blob not in store.writes


def test_body_damaged_in_transit_is_refused_by_its_checksum_at_the_write(tmp_path):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    [blob] = blob_keys(evidence)

    class Damaging(Store):
        def put_object(self, **kwargs):
            if kwargs["Key"] == blob:
                assert kwargs["ContentMD5"] and kwargs["IfNoneMatch"] == "*"
                kwargs["Body"] = kwargs["Body"][:-1] + bytes([kwargs["Body"][-1] ^ 1])
            return super().put_object(**kwargs)

    store = Damaging()
    refused(store, directory, evidence, ClientError)
    assert blob not in store.objects


@pytest.mark.parametrize("answer", ["other-bytes-kept", "no-etag", "foreign-etag"])
def test_created_blob_is_trusted_only_on_the_md5_of_the_buffer_sent(tmp_path, answer):
    """Storage's answer is the evidence: anything but the sent buffer's MD5 costs a read of the stored bytes."""
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    [blob] = blob_keys(evidence)

    class Answering(Store):
        def put_object(self, **kwargs):
            result = super().put_object(**kwargs)
            if kwargs["Key"] != blob or answer == "other-bytes-kept":
                return result  # ``corrupt_key``: storage ignored the checksum and reports what it kept
            return {} if answer == "no-etag" else {"ETag": '"not-an-md5-of-the-body"'}

    store = Answering()
    if answer == "other-bytes-kept":
        store.corrupt_key = blob
        refused(store, directory, evidence)
        assert store.reads[blob] == 2  # the worker's read-back, then the admission that refuses
    else:
        publish(store, directory, evidence)
        assert store.reads[blob] == 1 and store.objects[blob] == BODY
    assert store.writes.count(blob) == 1


def test_blob_removed_after_its_write_is_the_stated_limit_of_not_reading_back(tmp_path):
    """Until ss-ag8i a new blob was read during admission, which saw one removed since its write.

    An already-stored blob never had that check (one HEAD, then local bytes), and now a created one has none
    either: docs/source-evidence.md states the limit. Readers still hash every blob they fetch against its key.
    """
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    store = Store()
    [blob] = blob_keys(evidence)

    def delete(key):
        if key.endswith("/members.json"):
            store.objects.pop(blob, None)

    store.before_put = delete
    publish(store, directory, evidence)
    assert blob not in store.objects and pub.INDEX_V2_KEY in store.objects


def test_upload_refuses_bytes_that_differ_from_their_content_address(tmp_path):
    path = tmp_path / "blob"
    path.write_bytes(BODY)
    store = Store()
    with pytest.raises(pub.PublicationError, match="differ"):
        pub._put_immutable(store, "test", "source-evidence/blobs/sha256/x", path, sha256="sha256:" + "0" * 64)
    assert not store.objects


def test_blob_requests_run_concurrently(tmp_path):
    """1,810 serial round trips took 23.5 minutes (bill family, 2026-09-26); two requests must overlap."""
    import threading

    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY, b"a second response", b"a third response")
    both, arrivals, lock = threading.Barrier(2, timeout=5), [], threading.Lock()

    class Overlapping(Store):
        def head_object(self, *, Bucket, Key, ChecksumMode=None):
            with lock:
                arrivals.append(Key)
                first_two = "/blobs/" in Key and len([k for k in arrivals if "/blobs/" in k]) <= 2
            if first_two:
                try:
                    both.wait()
                except threading.BrokenBarrierError:
                    pass
            return super().head_object(Bucket=Bucket, Key=Key, ChecksumMode=ChecksumMode)

    store = Overlapping()
    publish(store, directory, evidence)
    assert not both.broken, "no two blob requests were in flight together"
    assert all(store.writes.count(key) == 1 and store.reads[key] == 0 for key in blob_keys(evidence))


def test_read_backs_run_concurrently_and_each_blob_is_read_once(tmp_path):
    """The read-back that remains must not be serial: 0.31 s a blob, one at a time, was 104 minutes per 20,000."""
    import threading

    (tmp_path / "one").mkdir()
    bodies = (BODY, b"a second response", b"a third response", b"a fourth response")
    directory, evidence = evidence_run(tmp_path, "one", *bodies)
    both, lock, arrivals = threading.Barrier(2, timeout=5), threading.Lock(), []

    class Overlapping(Store):
        def get_object(self, *, Bucket, Key):
            if Key in blob_keys(evidence):
                with lock:
                    arrivals.append(Key)
                    first_two = len(arrivals) <= 2
                if first_two:
                    try:
                        both.wait()
                    except threading.BrokenBarrierError:
                        pass
            return super().get_object(Bucket=Bucket, Key=Key)

    store = Overlapping()
    for key in blob_keys(evidence):  # stored by an uploader whose ETag cannot be compared: each needs its bytes read
        store.objects[key] = read_evidence_member(evidence, key.removeprefix("source-evidence/"))
        store.etags[key] = '"different-etag-requires-readback"'
    publish(store, directory, evidence)
    assert not both.broken, "no two read-backs were in flight together"
    assert len(blob_keys(evidence)) == 4 and all(store.reads[key] == 1 for key in blob_keys(evidence))
    assert not any(key in store.writes for key in blob_keys(evidence))


@pytest.mark.parametrize("already_present", [False, True])
def test_blobs_are_read_back_in_bounded_chunks_without_local_scratch(tmp_path, monkeypatch, already_present):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY * 1024, b"a second response" * 80_000)
    local_files = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file())
    bodies, active, peak = [], 0, 0

    class Bounded(BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 1024 * 1024, "A read-back must request bounded chunks"
            return super().read(size)

        def close(self):
            nonlocal active
            if not self.closed:
                active -= 1
            super().close()

    class Streaming(Store):
        def get_object(self, *, Bucket, Key):
            nonlocal active, peak
            result = super().get_object(Bucket=Bucket, Key=Key)
            if Key in blob_keys(evidence):
                result["Body"].close()
                result["Body"] = Bounded(self.objects[Key])
                bodies.append(result["Body"])
                active += 1
                peak = max(peak, active)
            return result

        def put_object(self, **kwargs):
            if kwargs["Key"] in blob_keys(evidence):
                assert kwargs["IfNoneMatch"] == "*"
            return super().put_object(**kwargs)

    store = Streaming()
    if already_present:
        for key in blob_keys(evidence):
            store.objects[key] = read_evidence_member(evidence, key.removeprefix("source-evidence/"))
            store.etags[key] = '"different-etag-requires-readback"'

    def no_scratch(*args, **kwargs):
        raise AssertionError("Publication must not create a verification scratch directory")

    monkeypatch.setattr(tempfile, "TemporaryDirectory", no_scratch)
    publish(store, directory, evidence)
    assert peak <= pub.EVIDENCE_WORKERS and active == 0 and all(body.closed for body in bodies)
    assert all(store.reads[key] == (1 if already_present else 0) for key in blob_keys(evidence))
    assert all(store.writes.count(key) == (0 if already_present else 1) for key in blob_keys(evidence))
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file()) == local_files


@pytest.mark.parametrize("failure", ["interrupted-body", "extra-metadata"])
def test_remote_stream_or_membership_failure_closes_body_and_never_publishes(tmp_path, failure):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    opened = []

    class Interrupted(BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise OSError("interrupted read")
            return super().read(min(size, 3))

    class Failing(Store):
        def get_object(self, *, Bucket, Key):
            result = super().get_object(Bucket=Bucket, Key=Key)
            if failure == "interrupted-body" and Key in blob_keys(evidence):
                result["Body"].close()
                result["Body"] = Interrupted(self.objects[Key])
                opened.append(result["Body"])
            return result

    store = Failing()
    if failure == "interrupted-body":  # an existing object whose ETag cannot be compared is read back
        [blob] = blob_keys(evidence)
        store.objects[blob], store.etags[blob] = BODY, '"different-etag-requires-readback"'
    if failure == "extra-metadata":
        prefix = "source-evidence/" + evidence.artifact.pin.artifact_digest.removeprefix("sha256:")
        store.objects[prefix + "/undeclared.json"] = b"undeclared"
    with pytest.raises((ArtifactVerificationError, OSError)):
        publish(store, directory, evidence)
    assert all(body.closed for body in opened) and len(opened) == (failure == "interrupted-body")
    assert pub.INDEX_V2_KEY not in store.objects
    assert not any(key.startswith("generations/") for key in store.writes)
