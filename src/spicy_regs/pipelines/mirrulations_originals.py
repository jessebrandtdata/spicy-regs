"""Keep every Mirrulations object the regulations ETL reads, exactly as received, before it is parsed.

The SpicyDocs reader hands its caller parsed payloads (and derived text) only;
the exact bytes exist in the GETs it makes through the S3 resource the caller
supplies (``reader_factory(resource_factory=...)``, ``MirrulationsReader(resource,
...)``, ``DerivedTextPool(resource_factory)``). :meth:`MirrulationsOriginals.resource`
wraps that resource so each complete GET body goes to the shared source-evidence
owner (:meth:`CaptureEvidence.retain_bytes`) before anything decodes it, as the
Federal Register build does around its byte-only fetcher. HTTP sources use
``CaptureEvidence.transport`` instead, which cannot wrap a boto3 resource. No
object is downloaded twice. A follow-up in SpicyDocs that exposes each GET's
bytes on ``KeyedPayload`` and the derived-text records would retire this wrapper.

Originals are content-addressed: the central raw archive locally, and R2's
shared ``source-evidence/blobs/sha256/<hex>`` prefix through the existing
evidence publisher, which the generation-retention planner never deletes.
Identical bytes are stored once; a changed upstream object hashes to a new
blob beside the old one. Each artifact also gets an append-only index
(key, sha256, size, ETag, VersionId, LastModified, fetch time, artifact
digest) at ``source-evidence/indexes/regulations-mirrulations/``, so a key's
original can be found without walking every journal.

:meth:`MirrulationsOriginals.commit` runs before every durable write that uses
what was read: the catalog merge, the manifest that retires keys, and each
derived-text upsert. It refuses when any retention failed or any key about to
be used has no retained original (for example, if SpicyDocs stopped reading
through the supplied resource), so nothing is written without its original
kept. The first retention failure halts the run at once (:class:`RetentionHalted`),
so no further object is downloaded. The keys stay unretired and the next run
reads them again. It refuses rather than retrying: botocore already retries each R2 request, and the
scheduled retry sweep reruns an unfinished batch from its manifest.

Retention is serialized because the evidence journal is not thread-safe. It
measured about 4 ms per 2.8 KB object on APFS (2026-11-08), mostly the raw
archive's descriptor-relative path walks, so a 12,000-object night adds under a
minute. An artifact is flushed (sealed and, when publishing, sent to R2) every
``OBJECTS_PER_ARTIFACT`` objects. With ``scratch`` (hosted runners) each
flushed artifact's local archive and run directory are then deleted, so disk
holds at most one artifact's originals however large the pass.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from threading import Lock
from typing import Any
from uuid import uuid4

from loguru import logger

from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError

FAMILY = "regulations-mirrulations"
#: Objects per evidence artifact. Its journal and member manifest grow by about
#: 1.2 KB per object, and readers bound both at 64 MiB (``EVIDENCE_CONTROL_LIMIT``).
OBJECTS_PER_ARTIFACT = 20_000
#: The largest body retained, as the largest any Mirrulations caller here reads
#: (``derived_text.MAX_ATTACHMENT_BYTES``). A larger object is not read here, so
#: a reader that consumes it anyway fails the commit's conservation check.
MAX_OBJECT_BYTES = 64 * 1024 * 1024
INDEX_NAME = "originals-index.parquet"


class RetentionHalted(BaseException):
    """Stops the run at the first retention failure, before another object is downloaded.

    A ``BaseException`` because SpicyDocs' reader turns any ``Exception`` from a GET into
    an unresolved key and moves on (a credential refusal is the one it re-raises), so
    an ordinary error would let a whole night download, then refuse only at commit.
    """


class _RetainingObject:
    def __init__(self, inner: Any, key: str, owner: MirrulationsOriginals) -> None:
        self._inner, self._key, self._owner = inner, key, owner

    def get(self, **options: Any) -> dict:
        """GET once, retain a complete whole-object body, and hand the reader the same bytes."""
        from io import BytesIO

        from botocore.response import StreamingBody

        if self._owner.failure is not None:
            raise RetentionHalted(f"Mirrulations original retention failed: {self._owner.failure}")
        response = self._inner.get(**options)
        declared = response.get("ContentLength")
        if "Range" in options or response.get("ContentRange") or (declared or 0) > MAX_OBJECT_BYTES:
            return response  # Not a whole original within the cap: the reader decides, nothing is kept.
        body = response["Body"]
        try:
            # Read as the reader would (S3 always declares a length, checked against the cap above).
            content = body.read()
        finally:
            body.close()
        if len(content) > MAX_OBJECT_BYTES:
            raise ValueError(f"{self._key} exceeds the {MAX_OBJECT_BYTES} byte retention cap")
        if declared is None or declared == len(content):
            self._owner.retain(self._key, content, response)
        # An incomplete body is not an original; the reader's own length check
        # leaves the key unresolved for the next run.
        return {**response, "Body": StreamingBody(BytesIO(content), len(content))}


class _RetainingResource:
    """The reader's S3 resource, with ``Object(...).get()`` retaining each body."""

    def __init__(self, inner: Any, owner: MirrulationsOriginals) -> None:
        self._inner, self._owner = inner, owner

    def Object(self, bucket: str, key: str) -> _RetainingObject:  # noqa: N802 - boto3 resource API
        return _RetainingObject(self._inner.Object(bucket, key), key, self._owner)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class MirrulationsOriginals:
    """A run's retained originals, made durable at each :meth:`commit`."""

    def __init__(self, output_dir: Path, bucket: str, *, publish: bool, scratch: bool = False) -> None:
        if scratch and not publish:
            raise ValueError("Local originals are scratch only when R2 keeps them")
        self.output_dir, self.bucket, self.publish, self.scratch = output_dir, bucket, publish, scratch
        self._lock = Lock()
        self._current: CaptureEvidence | None = None
        self._rows: list[dict] = []
        self._flushed: list[CaptureEvidence] = []
        #: Keys retained, and keys a durable write will use, since the last commit.
        self.retained: set[str] = set()
        self._expected: set[str] = set()
        #: The first retention failure. It halts the run (:class:`RetentionHalted`), and the
        #: commit refuses on it too, in case a caller caught the halt.
        self.failure: str | None = None

    def resource(self, inner: Any) -> _RetainingResource:
        return _RetainingResource(inner, self)

    def retain(self, key: str, content: bytes, response: dict) -> None:
        """Retain one GET's exact bytes and source metadata, remembering the first failure."""
        last_modified = response.get("LastModified")
        if isinstance(last_modified, datetime):
            last_modified = last_modified.isoformat()
        source = {
            "source_version": response.get("ETag"),
            "version_id": response.get("VersionId"),
            "last_modified": last_modified,
            "observed_at": datetime.now(UTC).isoformat(),
        }
        with self._lock:
            try:
                if len(self._rows) >= OBJECTS_PER_ARTIFACT:
                    self._flush()
                if self._current is None:
                    root = self.output_dir / "mirrulations-originals" / uuid4().hex if self.scratch else None
                    self._current = CaptureEvidence(self.output_dir, FAMILY, archive_root=root)
                receipt = self._current.retain_bytes(
                    content, raw_source=True, stage="mirrulations-object",
                    source_url=f"s3://{self.bucket}/{key}", source_record_key=key, **source,
                )
            except BaseException as error:
                self.failure = self.failure or f"{type(error).__name__} retaining {key}"
                if isinstance(error, RetentionHalted):
                    raise
                raise RetentionHalted(f"Mirrulations original retention failed: {self.failure}") from error
            self._rows.append({"key": key, "sha256": receipt["sha256"], "bytes": len(content), **source})
            self.retained.add(key)

    def expect_fill(self, fill: Any) -> None:
        """Register the derived-text objects a fill's provenance cites; the next commit requires them."""
        keys = {item["key"] for item in json.loads(fill.provenance)["attachments"]}
        with self._lock:
            self._expected |= keys

    def _flush(self) -> None:
        """Seal the current artifact, publish it and its index, then finish it; caller holds the lock."""
        evidence, rows = self._current, self._rows
        if evidence is None:
            return
        self._current, self._rows = None, []
        self._flushed.append(evidence)
        try:
            self._publish(evidence, rows)
        except BaseException as error:
            with suppress(Exception):
                evidence.finish(error)
            raise
        evidence.finish()
        if self.scratch:
            # R2 has admitted the bytes, journal and index; the runner copy was scratch.
            rmtree(evidence.store.root, ignore_errors=True)
            rmtree(evidence.directory, ignore_errors=True)

    def _publish(self, evidence: CaptureEvidence, rows: list[dict]) -> None:
        artifact = evidence.seal(outcome="build-complete")
        digest = artifact.pin.artifact_digest
        index = evidence.directory / INDEX_NAME
        _write_index(index, [{**row, "artifact_digest": digest} for row in rows])
        if self.publish:
            from spicy_regs.sources import publication, r2

            r2.require_credentials("Mirrulations original retention")
            client, bucket = r2.get_r2_client(), os.getenv("R2_BUCKET_NAME", "spicy-regs")
            publication.publish_evidence(client, bucket, evidence.artifact_dir, artifact, archive_root=evidence.store.root)
            location = publication.publish_evidence_index(client, bucket, FAMILY, index)
            logger.info("Retained {:,} Mirrulations originals in {} (index {})", len(rows), digest, location)
        else:
            logger.info("Retained {:,} Mirrulations originals locally in {}", len(rows), digest)

    def commit(self, retiring: Iterable[str] = ()) -> None:
        """Make every retained original durable before a write can use or retire what was read."""
        with self._lock:
            if self.failure is not None:
                raise SourceEvidenceError(f"Mirrulations original retention failed: {self.failure}")
            missing = (set(retiring) | self._expected) - self.retained
            if missing:
                raise SourceEvidenceError(
                    f"{len(missing)} Mirrulations objects would be used without a retained original, "
                    f"e.g. {sorted(missing)[0]}"
                )
            try:
                self._flush()
            except Exception as error:
                # As in retain(): a failed seal or R2 write is an evidence failure, and every later commit refuses.
                self.failure = self.failure or f"{type(error).__name__} publishing originals"
                raise SourceEvidenceError(f"Mirrulations original retention failed: {self.failure}") from error
            self.retained, self._expected = set(), set()

    def __enter__(self) -> MirrulationsOriginals:
        return self

    def __exit__(self, kind, error, traceback) -> None:
        """An unflushed artifact finishes as failed when the run raised; otherwise it is committed."""
        if error is None:
            self.commit()
            return
        if self._current is not None:
            # The run reports its own error, not a failure to write this receipt.
            with suppress(Exception):
                self._current.finish(error)


def _write_index(path: Path, rows: list[dict]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    columns = ("key", "sha256", "bytes", "source_version", "version_id", "last_modified", "observed_at",
               "artifact_digest")
    schema = pa.schema([(name, pa.int64() if name == "bytes" else pa.string()) for name in columns])
    table = pa.Table.from_pylist([{name: row.get(name) for name in columns} for row in rows], schema=schema)
    pq.write_table(table, path, compression="zstd")
