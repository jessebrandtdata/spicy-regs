"""Keep every Mirrulations object the regulations ETL reads, exactly as received, before it is parsed.

The SpicyDocs reader hands its caller parsed payloads only; the exact bytes
exist in the GET it makes through the S3 resource the caller supplies
(``reader_factory(resource_factory=...)``, ``MirrulationsReader(resource, ...)``).
:meth:`MirrulationsOriginals.resource` wraps that resource so each completed
GET body goes to the shared source-evidence owner (:class:`CaptureEvidence`)
before the reader decodes it, the same seam the Federal Register build uses
around its fetcher. No object is downloaded twice.

Originals are content-addressed: the central raw archive locally, and R2's
shared ``source-evidence/blobs/sha256/<hex>`` prefix through the existing
evidence publisher, which the generation-retention planner never deletes.
Identical bytes are stored once; a changed upstream object hashes to a new
blob beside the old one. Each GET also journals its key, ETag, VersionId,
LastModified and fetch time.

:meth:`MirrulationsOriginals.commit` runs before the catalog merge and the
manifest that retires the keys. It refuses when any retention failed or any key
about to be retired has no retained original (for example, if SpicyDocs stopped
reading through the supplied resource), so nothing is processed without its
original kept. The keys stay unretired and the next run reads them again.
It refuses rather than retrying: botocore already retries each R2 request, and
the scheduled retry sweep reruns an unfinished batch from its manifest.

Retention is serialized because the evidence journal is not thread-safe. It
measured about 4 ms per 2.8 KB object on APFS (2026-11-08), mostly the raw
archive's descriptor-relative path walks, so a 12,000-object night adds under a
minute.
"""

from __future__ import annotations

import os
from contextlib import suppress
from datetime import UTC, datetime
from collections.abc import Iterable
from pathlib import Path
from threading import Lock
from typing import Any

from loguru import logger

from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError

FAMILY = "regulations-mirrulations"
#: Objects per evidence artifact. Its journal and member manifest grow by about
#: 1 KB per object, and readers bound both at 64 MiB (``EVIDENCE_CONTROL_LIMIT``).
OBJECTS_PER_ARTIFACT = 20_000


class _RetainingObject:
    def __init__(self, inner: Any, key: str, owner: MirrulationsOriginals) -> None:
        self._inner, self._key, self._owner = inner, key, owner

    def get(self, **options: Any) -> dict:
        """GET once, retain the complete body, and hand the reader the same bytes."""
        from io import BytesIO

        response = self._inner.get(**options)
        body = response["Body"]
        try:
            content = body.read()
        finally:
            body.close()
        declared = response.get("ContentLength")
        if declared is None or declared == len(content):
            self._owner.retain(self._key, content, response)
        # An incomplete body is not an original; the reader's own length check
        # leaves the key unresolved for the next run.
        return {**response, "Body": BytesIO(content)}


class _RetainingResource:
    """The reader's S3 resource, with ``Object(...).get()`` retaining each body."""

    def __init__(self, inner: Any, owner: MirrulationsOriginals) -> None:
        self._inner, self._owner = inner, owner

    def Object(self, bucket: str, key: str) -> _RetainingObject:  # noqa: N802 - boto3 resource API
        return _RetainingObject(self._inner.Object(bucket, key), key, self._owner)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class MirrulationsOriginals:
    """One commit unit's retained originals: a staging pass, or one chunk."""

    def __init__(self, output_dir: Path, bucket: str, *, publish: bool) -> None:
        self.output_dir, self.bucket, self.publish = output_dir, bucket, publish
        self.retained: set[str] = set()
        self._lock = Lock()
        self._current: CaptureEvidence | None = None
        self._current_objects = 0
        self._sealed: list[CaptureEvidence] = []
        #: The first retention failure. The reader turns an exception raised in its GET into an
        #: unresolved key, so this, not the exception, is what makes the commit refuse.
        self.failure: str | None = None

    def resource(self, inner: Any) -> _RetainingResource:
        return _RetainingResource(inner, self)

    def retain(self, key: str, content: bytes, response: dict) -> None:
        """Retain one GET's exact bytes and source metadata, remembering the first failure."""
        last_modified = response.get("LastModified")
        if isinstance(last_modified, datetime):
            last_modified = last_modified.isoformat()
        with self._lock:
            try:
                if self._current is not None and self._current_objects >= OBJECTS_PER_ARTIFACT:
                    full, self._current = self._current, None
                    self._sealed.append(full)
                    full.seal(outcome="build-complete")
                if self._current is None:
                    self._current, self._current_objects = CaptureEvidence(self.output_dir, FAMILY), 0
                self._current.retain_bytes(
                    content,
                    raw_source=True,
                    stage="mirrulations-object",
                    source_url=f"s3://{self.bucket}/{key}",
                    source_record_key=key,
                    source_version=response.get("ETag"),
                    version_id=response.get("VersionId"),
                    last_modified=last_modified,
                    observed_at=datetime.now(UTC).isoformat(),
                )
            except BaseException as error:
                self.failure = self.failure or f"{type(error).__name__} retaining {key}"
                raise
            self._current_objects += 1
            self.retained.add(key)

    def _evidences(self) -> list[CaptureEvidence]:
        return [*self._sealed, *([self._current] if self._current is not None else [])]

    def commit(self, retiring: Iterable[str]) -> None:
        """Make every retained original durable before ``retiring`` keys can be merged or manifested."""
        if self.failure is not None:
            raise SourceEvidenceError(f"Mirrulations original retention failed: {self.failure}")
        missing = set(retiring) - self.retained
        if missing:
            raise SourceEvidenceError(
                f"{len(missing)} Mirrulations keys would be retired without a retained original, "
                f"e.g. {sorted(missing)[0]}"
            )
        evidences = self._evidences()
        artifacts = [evidence.seal(outcome="build-complete") for evidence in evidences]
        if self.publish and artifacts:
            from spicy_regs.sources import publication, r2

            r2.require_credentials("Mirrulations original retention")
            client = r2.get_r2_client()
            bucket = os.getenv("R2_BUCKET_NAME", "spicy-regs")
            for evidence, artifact in zip(evidences, artifacts, strict=True):
                publication.publish_evidence(client, bucket, evidence.artifact_dir, artifact)
        if self.retained:
            logger.info("Retained {:,} Mirrulations originals in {} evidence artifacts", len(self.retained), len(evidences))

    def __enter__(self) -> MirrulationsOriginals:
        return self

    def __exit__(self, kind, error, traceback) -> None:
        """Record each artifact's outcome in the raw archive; a pass that raised finishes as failed."""
        for evidence in self._evidences():
            if error is None:
                evidence.finish()
            else:
                # The run reports its own error, not a failure to write this receipt.
                with suppress(Exception):
                    evidence.finish(error)
