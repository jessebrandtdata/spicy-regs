"""In-memory S3 fake for tests: conditional puts, multipart uploads, pagination; no credentials or network access."""

import base64
from collections import Counter
from datetime import datetime, timezone
from hashlib import md5, sha256
from io import BytesIO

from botocore.exceptions import ClientError


#: The write time listed for an object a test seeded into ``objects`` directly.
_SEEDED = datetime(2026, 1, 1, tzinfo=timezone.utc)


def error(code):
    return ClientError({"Error": {"Code": code}}, "test")


def _md5(raw: bytes) -> bytes:
    return md5(raw, usedforsecurity=False).digest()


def _sha256_b64(raw: bytes) -> str:
    return base64.b64encode(sha256(raw).digest()).decode()


class Store:
    """Records successful put keys in order; ``before_put`` and ``corrupt_key`` are fault-injection hooks.

    ETags follow S3/R2: a single put's is its MD5, a multipart one hashes the
    part MD5s and appends the part count. ``reads`` counts full GETs per key and
    ``sent`` counts request-body bytes per key, so tests can assert transfer volume.
    ``modified`` holds each key's last write time, which listings report.
    ``checksums`` holds, for each key uploaded with a SHA-256 checksum, its part sizes (``None`` for a single put);
    HEAD with ``ChecksumMode="ENABLED"`` reports the checksum of the stored bytes over that layout, composite with a
    ``-N`` suffix for multipart, as S3 does. A copy keeps no checksum, as R2's CopyObject.
    """

    def __init__(self):
        self.objects = {}
        self.modified = {}
        self.etags = {}  # multipart ETags only; others derive from the stored bytes
        self.checksums = {}
        self.writes = []
        self.deletes = []
        self.copies = []
        self.uploads = {}
        self.reads = Counter()
        self.sent = Counter()
        self.before_put = None
        self.corrupt_key = None

    def head_object(self, *, Bucket, Key, ChecksumMode=None):
        if Key not in self.objects:
            raise error("NoSuchKey")
        head = {"ContentLength": len(self.objects[Key]), "ETag": self._etag(Key)}
        if ChecksumMode == "ENABLED" and Key in self.checksums:
            raw, parts = self.objects[Key], self.checksums[Key]
            if parts is None:
                head["ChecksumSHA256"] = _sha256_b64(raw)
            else:
                digests, start = [], 0
                for size in parts:
                    digests.append(sha256(raw[start:start + size]).digest())
                    start += size
                head["ChecksumSHA256"] = f"{_sha256_b64(b''.join(digests))}-{len(parts)}"
        return head

    def _etag(self, key):
        return self.etags.get(key) or '"' + _md5(self.objects[key]).hex() + '"'

    def copy_object(self, *, Bucket, Key, CopySource, **kwargs):
        source_key = CopySource["Key"]
        if source_key not in self.objects:
            raise error("NoSuchKey")
        if Key in self.objects:
            raise error("PreconditionFailed")
        self.objects[Key] = self.objects[source_key]
        self.etags.pop(Key, None)
        self.checksums.pop(Key, None)
        if source_key in self.etags:
            self.etags[Key] = self.etags[source_key]
        self.modified[Key] = datetime.now(timezone.utc)
        self.writes.append(Key)
        self.copies.append(Key)
        return {}

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise error("NoSuchKey")
        self.reads[Key] += 1
        return {"Body": BytesIO(self.objects[Key]), "ETag": self._etag(Key)}

    def put_object(self, *, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None, ContentMD5=None, etag=None,
                   ChecksumSHA256=None, **kwargs):
        if self.before_put:
            self.before_put(Key)
        raw = Body.read() if hasattr(Body, "read") else Body
        self.sent[Key] += len(raw)
        if ContentMD5 is not None and ChecksumSHA256 is not None:
            raise error("InvalidRequest")  # R2: "You can only specify one non-default checksum at a time."
        if ContentMD5 is not None and ContentMD5 != base64.b64encode(_md5(raw)).decode():
            raise error("BadDigest")
        if ChecksumSHA256 is not None and ChecksumSHA256 != _sha256_b64(raw):
            raise error("BadDigest")
        if IfNoneMatch == "*" and Key in self.objects:
            raise error("PreconditionFailed")
        if IfMatch is not None and (Key not in self.objects or IfMatch != self._etag(Key)):
            raise error("PreconditionFailed")
        if self.corrupt_key and Key.endswith(self.corrupt_key):
            raw = raw[:-1] + bytes([raw[-1] ^ 1])
        self.objects[Key] = raw
        self.etags.pop(Key, None)
        if etag:
            self.etags[Key] = etag
        self.checksums.pop(Key, None)
        if ChecksumSHA256 is not None:
            self.checksums[Key] = None  # or a test's part sizes: a composite on HEAD
        self.modified[Key] = datetime.now(timezone.utc)
        self.writes.append(Key)
        return {}

    def delete_objects(self, *, Bucket, Delete):
        for item in Delete["Objects"]:
            self.delete_object(Bucket=Bucket, Key=item["Key"])
        return {}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop(Key, None)
        self.deletes.append(Key)
        return {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        # Two pages prove the adapter consumes the whole prefix listing.
        listed = [{"Key": k, "Size": len(self.objects[k]), "LastModified": self.modified.get(k, _SEEDED)}
                  for k in sorted(self.objects) if k.startswith(Prefix)]
        yield {"Contents": listed[:1]}
        yield {"Contents": listed[1:]}

    def create_multipart_upload(self, *, Bucket, Key, **kwargs):
        self.uploads[Key] = []
        return {"UploadId": Key}

    def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body, ContentMD5, ChecksumSHA256=None):
        assert PartNumber == len(self.uploads[Key]) + 1
        self.sent[Key] += len(Body)
        if ChecksumSHA256 is not None:
            raise error("NotImplemented")  # R2 on UploadPart, probe 2026-11-01
        if ContentMD5 != base64.b64encode(_md5(Body)).decode():
            raise error("BadDigest")
        self.uploads[Key].append(Body)
        return {"ETag": str(PartNumber)}

    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload, IfNoneMatch):
        parts = self.uploads[Key]
        etag = f'"{md5(b"".join(_md5(part) for part in parts), usedforsecurity=False).hexdigest()}-{len(parts)}"'
        self.sent[Key] -= sum(map(len, parts))  # put_object below recounts the assembled bytes
        self.put_object(Bucket=Bucket, Key=Key, Body=b"".join(parts), IfNoneMatch=IfNoneMatch, etag=etag)
        del self.uploads[Key]

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        self.uploads.pop(Key, None)


def download_generation(monkeypatch, generation, evidence, output, tables):
    """Exercise the real publication and download paths using an in-memory bucket."""
    import argparse
    from contextlib import contextmanager

    import httpx

    from spicy_regs import cli
    from spicy_regs.sources import publication

    store = Store()
    publication.publish_generation(
        generation, client=store, bucket="test", prior_index=publication.empty_index(),
        evidence_directories=(evidence,) if evidence is not None else (),
    )

    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://test/")
        yield httpx.Response(
            200 if key in store.objects else 404, content=store.objects.get(key, b""),
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx, "stream", stream)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test")
    monkeypatch.delenv("SPICY_REGS_R2_URL", raising=False)
    monkeypatch.setattr(publication, "load_index", lambda _: publication.parse_index(store.objects[publication.INDEX_V2_KEY]))
    cli.cmd_download(argparse.Namespace(output_dir=output, types=list(tables), force=False))
    return output
