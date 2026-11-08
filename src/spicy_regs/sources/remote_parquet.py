"""Bounded Parquet streams for caller-owned, unpublished S3 staging keys.

The digest describes the bytes produced, including the Parquet footer. A head
response confirms size and supplies an ETag; it does not independently verify
that digest. Generation admission must reread the pinned object before use.
"""

from __future__ import annotations

from collections.abc import Buffer, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import io
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

READ_BUFFER_BYTES = 1024 * 1024
READ_RANGE_BYTES = 8 * 1024 * 1024
UPLOAD_PART_BYTES = 64 * 1024 * 1024
ROW_GROUP_ROWS = 8192
ROW_GROUP_BYTES = 8 * 1024 * 1024


def parquet_schema(schema: pa.Schema) -> pa.Schema:
    """The declared schema after the same maintained Parquet serialization.

    Compliant LIST encoding names its structural child ``element``. Preserve
    types, nullability and metadata when comparing that readback with a policy.
    """
    sink = pa.BufferOutputStream()
    pq.write_metadata(schema, sink)
    return pq.read_schema(pa.BufferReader(sink.getvalue()))


@dataclass(frozen=True)
class StoredParquet:
    """Produced bytes and the stored object's pin, pending independent admission."""

    key: str
    byte_size: int
    sha256: str
    etag: str
    rows: int


class _ClosingReadClient:
    """Close the last HTTP body even though smart_open 8.0.1 Reader.close is a no-op."""

    def __init__(self, client: Any):
        self.client = client
        self.body: Any = None

    def get_object(self, **kwargs: Any) -> Any:
        self.close()
        response = self.client.get_object(**kwargs)
        self.body = response["Body"]
        return response

    def close(self) -> None:
        if self.body is not None:
            self.body.close()
            self.body = None


@contextmanager
def open_pinned_s3(*, client: Any, bucket: str, key: str, etag: str) -> Iterator[io.BufferedIOBase]:
    """Read seekable bounded ranges; every remote GET must match the same ETag."""
    from smart_open import s3

    if not isinstance(etag, str) or not etag.strip():
        raise ValueError("A nonempty ETag is required for pinned S3 reads")
    tracked = _ClosingReadClient(client)
    try:
        with s3.open(
            bucket,
            key,
            "rb",
            client=tracked,
            client_kwargs={"S3.Client.get_object": {"IfMatch": etag}},
            defer_seek=True,
            buffer_size=READ_BUFFER_BYTES,
            range_chunk_size=READ_RANGE_BYTES,
        ) as stream:
            yield stream
    finally:
        tracked.close()


@contextmanager
def open_pinned_http(url: str, *, etag: str) -> Iterator[io.BufferedIOBase]:
    """Credentialless seekable reads with a version guard on every HTTP request."""
    import requests
    from smart_open import http

    if not isinstance(etag, str) or not etag.strip():
        raise ValueError("A nonempty ETag is required for pinned HTTP reads")

    class ClosingSession:
        def __init__(self, session):
            self.session, self.response = session, None

        def close(self):
            if self.response is not None:
                self.response.close()
                self.response = None

        def get(self, *args, **kwargs):
            self.close()
            self.response = self.session.get(*args, **kwargs)
            self.response.raise_for_status()
            if self.response.headers.get("ETag") != etag:
                self.close()
                raise ValueError("Public member changed during its pinned read")
            return self.response

    with requests.Session() as session:
        session.trust_env = False  # No environment credentials or proxy auth in anonymous admission.
        tracked = ClosingSession(session)
        # smart_open uses only Session.get; the proxy keeps response closure and
        # ETag admission around that exact transport without changing its owner.
        try:
            with http.open(url, "rb", headers={"If-Match": etag, "Accept-Encoding": "identity"},
                           session=cast(requests.Session, tracked), buffer_size=READ_BUFFER_BYTES, timeout=120) as stream:
                if not stream.seekable():
                    raise ValueError("Public member does not support bounded seekable reads")
                yield stream
        finally:
            tracked.close()


#: Attempts per exact range before a ranged published read fails.
RANGE_ATTEMPTS = 4


class _PublishedRanges(io.RawIOBase):
    """Exact ``Range: bytes=a-b`` reads of one published object, each held to its ETag and stored size.

    smart_open's HTTP reader asks for open-ended ranges and drops each body on seek; measured on the bill-family
    receipt member, its row-group read consumed 34 MB while the interface received about 414 MB (86 MB with exact ranges). A Parquet reader states every length.
    """

    def __init__(self, client: Any, url: str, *, byte_size: int):
        self.client, self.url, self.size, self.position = client, url, byte_size, 0
        head = client.head(url, headers={"Accept-Encoding": "identity"})
        head.raise_for_status()
        self.etag = head.headers.get("ETag")
        if not self.etag or int(head.headers.get("Content-Length", -1)) != byte_size:
            raise ValueError(f"Published member differs from its pinned byte size or has no ETag: {url}")

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self.position = {io.SEEK_SET: 0, io.SEEK_CUR: self.position, io.SEEK_END: self.size}[whence] + offset
        return self.position

    def _get(self, byte_range: str) -> Any:
        """One exact range, asked again after a dropped connection or a 429/5xx, up to :data:`RANGE_ATTEMPTS` times.

        A range is bounded by the reader's stated length, so a retry re-reads at most that range.
        """
        import time

        import httpx

        for attempt in range(1, RANGE_ATTEMPTS + 1):
            try:
                response = self.client.get(self.url, headers={
                    "Range": byte_range, "If-Match": self.etag, "Accept-Encoding": "identity"})
                response.read()
            except httpx.TransportError:
                if attempt == RANGE_ATTEMPTS:
                    raise
            else:
                if response.status_code != 429 and response.status_code < 500 or attempt == RANGE_ATTEMPTS:
                    response.raise_for_status()
                    return response
            time.sleep(min(30, 2 ** attempt))
        raise AssertionError("unreachable")

    def readinto(self, buffer: Any) -> int:
        view = memoryview(buffer).cast("B")
        end = min(self.position + len(view), self.size)
        if end <= self.position:
            return 0
        response = self._get(f"bytes={self.position}-{end - 1}")
        data = response.content
        if (response.status_code != 206 or response.headers.get("ETag") != self.etag
                or response.headers.get("Content-Range") != f"bytes {self.position}-{end - 1}/{self.size}"
                or len(data) != end - self.position):
            raise ValueError(f"Published member changed or answered a different range: {self.url}")
        view[: len(data)] = data
        self.position = end
        return len(data)


@contextmanager
def open_published_http(url: str, *, byte_size: int) -> Iterator[io.BufferedIOBase]:
    """A seekable reader over a published member that issues only the ranges it is asked for.

    The index pins size and SHA-256, not an ETag: one HEAD supplies the ETag that then holds every range to that
    object. A partial read cannot check the whole-file SHA-256; the caller admits what it reads.
    """
    import httpx

    with httpx.Client(timeout=120, follow_redirects=True, trust_env=False) as client:
        # Unbuffered: a read-ahead buffer would fetch bytes past every stated length.
        yield cast(io.BufferedIOBase, _PublishedRanges(client, url, byte_size=byte_size))


class _HashingSink(io.RawIOBase):
    """Count and hash each accepted write without buffering a whole output file."""

    def __init__(self, stream: io.BufferedIOBase, max_bytes: int):
        self.stream = stream
        self.max_bytes = max_bytes
        self.byte_size = 0
        self.digest = hashlib.sha256()

    def writable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.byte_size

    def write(self, data: Buffer) -> int:
        if self.closed:
            raise ValueError("Parquet byte sink is closed")
        view = memoryview(data).cast("B")
        if self.byte_size + len(view) > self.max_bytes:
            raise ValueError("Parquet output exceeds max_bytes")
        written = self.stream.write(view)
        if written != len(view):
            raise OSError("Incomplete Parquet stream write")
        self.digest.update(view)
        self.byte_size += written
        return written


@dataclass
class ParquetUpload:
    """One open upload; its stored receipt exists only after successful closure."""

    writer: pq.ParquetWriter
    schema: pa.Schema
    rows: int = 0
    stored: StoredParquet | None = None
    _pending: list[pa.RecordBatch] = field(default_factory=list, repr=False)
    _pending_rows: int = 0
    _pending_bytes: int = 0

    def flush(self) -> None:
        """Write accumulated Arrow batches in order, without a Python row copy."""
        if self._pending:
            self.writer.write_table(pa.Table.from_batches(self._pending, schema=self.schema),
                                    row_group_size=ROW_GROUP_ROWS)
            self._pending.clear()
            self._pending_rows = self._pending_bytes = 0

    def write(self, batch: pa.Table | pa.RecordBatch) -> None:
        if not isinstance(batch, (pa.Table, pa.RecordBatch)):
            raise TypeError("Each Parquet batch must be an Arrow table or record batch")
        if not batch.schema.equals(self.schema, check_metadata=True):
            raise ValueError("Parquet batch schema differs from the declared schema")
        table = pa.Table.from_batches([batch]) if isinstance(batch, pa.RecordBatch) else batch
        for part in table.to_batches(max_chunksize=ROW_GROUP_ROWS):
            if self._pending and (self._pending_rows + part.num_rows > ROW_GROUP_ROWS
                                  or self._pending_bytes + part.nbytes > ROW_GROUP_BYTES):
                self.flush()
            self._pending.append(part)
            self._pending_rows += part.num_rows
            self._pending_bytes += part.nbytes
            if self._pending_rows >= ROW_GROUP_ROWS or self._pending_bytes >= ROW_GROUP_BYTES:
                self.flush()
        self.rows += batch.num_rows


@contextmanager
def remote_parquet_writer(
    *,
    client: Any,
    bucket: str,
    key: str,
    schema: pa.Schema,
    max_bytes: int,
) -> Iterator[ParquetUpload]:
    """Write a new staging object; abort multipart state on body or close failure.

    The caller chooses a unique staging key. Conditional completion preserves
    an already-existing object. This helper never changes a publication index.
    Arrow batches coalesce up to 8192 rows or 8 MiB of logical values; an
    oversized supplied chunk flushes immediately. Slices can retain their
    input buffers. Memory also holds the supplied batch, the 64 MiB upload
    buffer and accumulated footer metadata;
    max_bytes limits serialized bytes, including headers and the final footer.
    """
    from smart_open import s3

    if not isinstance(schema, pa.Schema):
        raise TypeError("schema must be an Arrow schema")
    if type(max_bytes) is not int or not 0 < max_bytes <= UPLOAD_PART_BYTES * 10_000:
        raise ValueError("max_bytes must be positive and fit within 10,000 upload parts")
    stream = cast(
        s3.MultipartWriter,
        s3.open(
            bucket,
            key,
            "wb",
            client=client,
            multipart_upload=True,
            min_part_size=UPLOAD_PART_BYTES,
            client_kwargs={
                "S3.Client.complete_multipart_upload": {"IfNoneMatch": "*"},
                "S3.Client.put_object": {"IfNoneMatch": "*"},
            },
        ),
    )
    sink = _HashingSink(stream, max_bytes)
    try:
        with pq.ParquetWriter(sink, schema, compression="zstd") as writer:
            upload = ParquetUpload(writer, schema)
            yield upload
            upload.flush()
        # smart_open's __exit__ does not terminate when its own close raises.
        # Keep explicit completion inside the same abort guard as Arrow writes.
        stream.close()
    except BaseException as error:
        try:
            stream.terminate()
        except BaseException as abort_error:
            error.add_note(f"Multipart abort also failed: {type(abort_error).__name__}: {abort_error}")
        raise
    finally:
        sink.close()
    head = client.head_object(Bucket=bucket, Key=key)
    etag = head.get("ETag")
    if head.get("ContentLength") != sink.byte_size or not isinstance(etag, str) or not etag.strip():
        raise ValueError("Stored Parquet size or ETag differs from the completed upload")
    upload.stored = StoredParquet(key, sink.byte_size, "sha256:" + sink.digest.hexdigest(), etag, upload.rows)


def write_remote_parquet(*, client: Any, bucket: str, key: str, schema: pa.Schema,
                         batches: Iterable[pa.Table | pa.RecordBatch], max_bytes: int) -> StoredParquet:
    """Consume a batch stream through the same bounded, create-only upload."""
    with remote_parquet_writer(client=client, bucket=bucket, key=key, schema=schema,
                               max_bytes=max_bytes) as upload:
        for batch in batches:
            upload.write(batch)
    assert upload.stored is not None
    return upload.stored
