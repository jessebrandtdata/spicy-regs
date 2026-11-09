"""Cloudflare R2 storage connector — both bookends of an incremental run.

:func:`download_from_r2` pulls existing datasets down so a run can append to
them. :func:`upload_file`, :func:`upload_dataset`, and
:func:`upload_comment_partitions` publish the finished Parquet back.

Downloads use the public ``R2_PUBLIC_URL`` over HTTPS; uploads use the S3 API
with ``R2_*`` credentials. Every upload clears a shrink guard
(:func:`_assert_upload_safe`), added after the March 2026 incident: a transient
download error produced an empty local file, and the upload overwrote the
historical 3.3 GB ``comments.parquet``.
"""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
import json
from os import getenv
from pathlib import Path

import boto3
from boto3.exceptions import S3UploadFailedError
import httpx
from loguru import logger

from spicy_regs.sources.cloudflare import purge_urls
from spicy_regs.sources.publication import (  # noqa: F401  (the public version check lives with public reads)
    PUBLIC_HEADERS as _PUBLIC_HEADERS, _get_bounded, _head, public_object_version, public_version_drift,
)



# --- download (public URL) -------------------------------------------------

#: The tables fetched through :func:`download_from_r2` while :func:`recorded_reads` is active, by key.
_reads: ContextVar[dict[str, dict] | None] = ContextVar("r2_reads", default=None)


@contextmanager
def recorded_reads() -> Iterator[dict[str, dict]]:
    """Note every table :func:`download_from_r2` fetches while active, at the bytes fetched.

    A managed table is noted by its pin: digest, size, family and generation. Any other object is noted by the digest
    and size of the file written, and an input read outside the bucket by what :func:`note_read` is given.
    Native readers use :func:`note_pinned_read` after admission to preserve a captured publication pin. A read that
    found nothing is not noted. What a caller then does with the bytes, including failing to use them, does not change
    the note: the build read them.
    """
    reads: dict[str, dict] = {}
    token = _reads.set(reads)
    try:
        yield reads
    finally:
        _reads.reset(token)


def note_read(key: str, *, sha256: str, byte_size: int) -> None:
    """Note a read of ``key`` at the bytes ``sha256`` and ``byte_size`` name, while :func:`recorded_reads` is active.

    For an input a build reads outside the bucket, such as a local capture read by reference; ``key`` names it.
    """
    reads = _reads.get()
    if reads is not None:
        reads[key] = {"sha256": sha256, "byteSize": byte_size}


def note_pinned_read(key: str, pin: dict) -> None:
    """Record an admitted input's pin from the build's captured publication index."""
    reads = _reads.get()
    if reads is not None:
        reads[key] = dict(pin)


def _note_read(remote_key: str, member, owner, local_path: Path) -> None:
    reads = _reads.get()
    if reads is None:
        return
    if member.sha256 is not None and owner is not None:
        reads[remote_key] = {"sha256": member.sha256, "byteSize": member.byte_size,
                             "family": owner[0], "artifactDigest": owner[1]["artifactDigest"]}
    else:
        with local_path.open("rb") as stream:
            note_read(remote_key, sha256="sha256:" + hashlib.file_digest(stream, "sha256").hexdigest(),
                      byte_size=local_path.stat().st_size)


def download_from_r2(remote_key: str, local_path: Path, *, bare: bool = False) -> bool:
    """Download one object from R2 over the public URL.

    ``remote_key`` resolves through the publication index unless ``bare``, which
    reads the object at that key even when a managed family publishes the key.

    Returns ``True`` on success, ``False`` when R2 is unconfigured or the object
    is missing (HTTP 404). Everything else — 5xx, network failures, disk write
    errors — raises.

    Returning ``False`` for the rest caused the March 2026 data loss: a
    transient error on the 3.3 GB ``comments.parquet`` read as "absent", the
    merge wrote a fresh empty file, and the upload overwrote the history.
    Raising aborts the run before it can publish an empty table.

    The download is atomic: bytes stream to ``{local_path}.tmp``, which is
    renamed into place only after a complete transfer. Any failure deletes the
    temp file and leaves an existing ``local_path`` untouched.
    """
    public_url = getenv("R2_PUBLIC_URL")
    if not public_url:
        logger.warning("R2_PUBLIC_URL not set; cannot download {}", remote_key)
        return False

    from spicy_regs.sources.publication import Member, current_index, fetch_member, single_member, table_owner

    index = None if bare else current_index(public_url)
    member = Member(remote_key, None, None, None) if index is None else single_member(index, remote_key)
    if not fetch_member(public_url, member, local_path, remote_key):
        return False
    _note_read(remote_key, member, None if index is None else table_owner(index, remote_key), local_path)
    return True


def download_members(remote_key: str, directory: Path) -> list[Path]:
    """Download every member of published table ``remote_key`` under ``directory``, each checked against its pin.

    A split table's members land at their ``<table>/<col>=<value>/part-NNNNNN.parquet`` keys, the layout
    ``build_generation`` takes back, so an unchanged partition republishes as a server-side copy.
    A single-file table lands at ``<table>.parquet``. Raises when R2 is unconfigured or the table is unpublished.

    Rewrite a partition by replacing its whole ``<col>=<value>/`` directory: a prior part file left beside the new
    ones would publish its rows a second time.
    """
    from spicy_regs.sources.publication import current_index, fetch_member, table_members

    public_url = getenv("R2_PUBLIC_URL")
    if not public_url:
        raise RuntimeError(f"R2_PUBLIC_URL not set; cannot download {remote_key}")
    members = table_members(current_index(public_url), remote_key)
    if members[0].sha256 is None:
        raise RuntimeError(f"{remote_key} is not published by any family")
    paths = []
    for member in members:
        path = directory / member.key
        path.parent.mkdir(parents=True, exist_ok=True)
        fetch_member(public_url, member, path, member.path)
        paths.append(path)
    return paths


def download(remote_key: str, local_path: Path) -> bool:
    """Alias for :func:`download_from_r2` (the connector's download verb)."""
    return download_from_r2(remote_key, local_path)


def download_working_copy(remote_key: str, local_path: Path) -> bool:
    """Download the ETL's own bare copy of a base table, never its managed family.

    ``dockets.parquet`` and ``documents.parquet`` are updated after every sweep
    batch; the managed family is published once the sweep completes. Priming
    from the family after a partly completed sweep would drop the batches whose
    keys the manifest already retired.
    """
    return download_from_r2(remote_key, local_path, bare=True)


# --- upload (S3 API) -------------------------------------------------------

#: Whole transfers per file before an upload fails.
UPLOAD_ATTEMPTS = 3


def get_r2_client():
    """Create a boto3 client configured for R2."""
    return boto3.client(
        "s3",
        endpoint_url=getenv("R2_ENDPOINT"),
        aws_access_key_id=getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=getenv("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def _get_remote_size(client, bucket: str, remote_key: str) -> int | None:
    """Return the remote object's size in bytes, or ``None`` when it is absent.

    Permissions and transient 5xx errors propagate: the shrink guard skips its
    check when nothing is there, so a guessed absence would wave every upload
    through.
    """
    stored = _head(client, bucket, remote_key)
    return None if stored is None else int(stored["ContentLength"])


def _assert_upload_safe(
    local_size: int,
    remote_size: int | None,
    remote_key: str,
) -> None:
    """Abort if the new upload would catastrophically shrink the remote.

    Controlled by ``R2_MIN_SIZE_RATIO`` (default ``0.5``) — the new file
    must be at least that fraction of the existing remote file. Set
    ``R2_ALLOW_SHRINK=1`` to bypass (used by recovery scripts).
    """
    # Retry state must shrink to zero when the last unresolved key recovers.
    # Dataset size guards still apply to every data table and the manifest.
    if remote_key in {"failed_keys.parquet", "pending_comment_text.parquet"}:
        return
    if remote_size is None or remote_size == 0:
        return
    if getenv("R2_ALLOW_SHRINK") == "1":
        logger.warning("R2_ALLOW_SHRINK=1: bypassing shrink guard for {}", remote_key)
        return

    ratio_env = getenv("R2_MIN_SIZE_RATIO", "0.5")
    try:
        min_ratio = float(ratio_env)
    except ValueError:
        raise RuntimeError(f"Invalid R2_MIN_SIZE_RATIO={ratio_env!r}; expected a float")

    ratio = local_size / remote_size
    if ratio < min_ratio:
        raise RuntimeError(
            f"Refusing to upload {remote_key}: new file would shrink remote "
            f"from {remote_size / 1024 / 1024:.1f} MB to "
            f"{local_size / 1024 / 1024:.1f} MB (ratio {ratio:.3f} < "
            f"threshold {min_ratio}). Set R2_ALLOW_SHRINK=1 to override."
        )


def _raise_for_failures(action: str, failures: list[tuple[str, BaseException]], total: int) -> None:
    """Log every failure, then raise one error naming them all.

    Collecting rather than raising on the first keeps one broken file from
    hiding the next behind it (see :func:`upload_dataset`).
    """
    if not failures:
        return
    for name, error in failures:
        logger.error("{} failed for {}: {}", action, name, error)
    names = ", ".join(sorted(name for name, _ in failures))
    raise RuntimeError(f"{action} failed for {len(failures)} of {total} file(s): {names}") from failures[0][1]


def _remote_key(output_dir: Path, local_path: Path) -> str:
    """The object key a file published from ``output_dir`` lands under.

    One definition so the preflight guards exactly the key the upload writes;
    two derivations that drifted would silently check the wrong object.
    """
    return str(local_path.relative_to(output_dir))


def dataset_files(output_dir: Path, data_types: list[str]) -> list[Path]:
    """The base-table ``{data_type}.parquet`` files that exist under ``output_dir``.

    Shared so a caller plans over exactly the set :func:`upload_dataset`
    publishes instead of rebuilding it.
    """
    return [pf for data_type in data_types if (pf := output_dir / f"{data_type}.parquet").exists()]


def require_credentials(action: str) -> None:
    """Refuse ``action`` without R2 write credentials.

    A caller that reaches an uploader means to publish: its dry run is its own
    ``skip_upload``, never a missing secret. Skipping here instead let the
    fork's GAO rollup run green every day from 2026-09-05 to 09-21 with empty
    secrets, each run logging "Skipping upload" and publishing nothing.
    """
    if not getenv("R2_ACCESS_KEY_ID"):
        raise RuntimeError(f"{action} requires R2 credentials (R2_ACCESS_KEY_ID); skip the upload for a dry run")


def preflight_uploads(output_dir: Path, files: list[Path]) -> None:
    """HEAD every planned object and run its size guard before the first byte lands.

    One worker used to replace its object while a sibling was still discovering
    that its own local file would catastrophically shrink production. Guarding
    the whole set first makes that refusal stop the publication before it starts.
    Each file is keyed by its path relative to ``output_dir``, the derivation
    :func:`upload_comment_partitions` publishes under. Without R2 credentials
    this refuses, like :func:`upload_file`.

    A transfer can still fail after a clean preflight, so callers must leave the
    manifest unpublished until every data upload succeeds.

    Costs one HEAD per file over a fixed-width pool (8 workers), and
    :func:`upload_file` HEADs each again during the upload itself: the
    upload-time check stays because it is every caller's own shrink guard,
    not just this preflight's.
    """
    require_credentials("Publication preflight")

    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
    client = get_r2_client()
    failures: list[tuple[str, BaseException]] = []

    def preflight_one(local_path: Path) -> None:
        remote_key = _remote_key(output_dir, local_path)
        try:
            remote_size = _get_remote_size(client, bucket, remote_key)
            _assert_upload_safe(local_path.stat().st_size, remote_size, remote_key)
        except Exception as error:
            failures.append((remote_key, error))

    # Fixed-width pool: the preflight is one HEAD per file and N is unbounded
    # for comment partitions; a pool sized to the file count would still burst
    # thousands of round trips at once.
    with ThreadPoolExecutor(max_workers=min(8, len(files) or 1)) as executor:
        list(executor.map(preflight_one, files))

    _raise_for_failures("Publication preflight", failures, len(files))


def upload_file(local_path: Path, remote_key: str | None = None, *, cache_control: str | None = None) -> None:
    """Publish one file to R2 (the remote key defaults to the filename).

    HEADs the existing object first and refuses to overwrite it with a much
    smaller one (:func:`_assert_upload_safe`), so an upstream error that
    produced a near-empty local file cannot wipe production.
    """
    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
    require_credentials(f"Uploading {local_path.name}")

    if remote_key is None:
        remote_key = local_path.name

    client = get_r2_client()

    local_size_bytes = local_path.stat().st_size
    file_size = local_size_bytes / (1024 * 1024)
    logger.info("Uploading {} ({:.1f} MB) to R2...", local_path.name, file_size)

    remote_size = _get_remote_size(client, bucket, remote_key)
    _assert_upload_safe(local_size_bytes, remote_size, remote_key)

    # Cache-Control policy. Parquet must never be edge-cached: DuckDB (the MCP
    # server and the browser DuckDB-WASM UI) reads each file through many
    # byte-range requests, and an edge cache serving inconsistent bytes across
    # those ranges under concurrent load corrupts the read — `utf-8 codec can't
    # decode` and ETag mismatches at ~c=10+. Parquet was `no-cache` originally; a
    # caching experiment brought the corruption back and was reverted. Everything
    # else (the UI MiniSearch json.gz, which DuckDB never reads) caches safely.
    # `R2_CACHE_CONTROL` overrides.
    configured_cache_control = getenv("R2_CACHE_CONTROL")
    cache_control = cache_control or configured_cache_control or (
        "public, no-cache, must-revalidate"
        if remote_key.endswith(".parquet")
        else "public, max-age=3600, stale-while-revalidate=86400"
    )
    # botocore retries each part, but not a CompleteMultipartUpload that R2 answers
    # InvalidPart (ETL 36680463442, 36908618438). The old object stays until a
    # complete succeeds, so the whole transfer is safe to repeat.
    for attempt in range(1, UPLOAD_ATTEMPTS + 1):
        try:
            client.upload_file(
                str(local_path),
                bucket,
                remote_key,
                ExtraArgs={"ContentType": "application/octet-stream", "CacheControl": cache_control},
            )
            break
        except S3UploadFailedError as error:
            if attempt == UPLOAD_ATTEMPTS:
                raise
            logger.warning("Upload of {} failed ({}); repeating the transfer ({}/{})",
                           remote_key, error, attempt + 1, UPLOAD_ATTEMPTS)

    public_url = getenv("R2_PUBLIC_URL", "")
    logger.info("Uploaded: {}/{}", public_url, remote_key)

    # Invalidate the edge cache for exactly this object so the fresh version is
    # visible immediately. Best-effort and a no-op without Cloudflare creds; it
    # must never fail a publish that already wrote the data (see cloudflare.py).
    if public_url:
        purge_urls([f"{public_url.rstrip('/')}/{remote_key}"])


def read_json_object(remote_key: str) -> dict | None:
    """Read a small control object directly from storage; only absence is optional."""
    stored = _get_bounded(get_r2_client(), getenv("R2_BUCKET_NAME", "spicy-regs"), remote_key)
    return None if stored is None else json.loads(stored[0])


def object_version(remote_key: str) -> dict | None:
    """Storage version used to detect out-of-band mirror replacements."""
    stored = _head(get_r2_client(), getenv("R2_BUCKET_NAME", "spicy-regs"), remote_key)
    return None if stored is None else {"etag": stored["ETag"], "bytes": stored["ContentLength"]}


def verify_public_file(local_path: Path, remote_key: str, base_url: str) -> dict:
    """Read back bounded public bytes and bind their digest to the stored version."""
    with local_path.open("rb") as file:
        digest = hashlib.file_digest(file, "sha256").hexdigest()
    size = local_path.stat().st_size
    before = object_version(remote_key)
    actual = hashlib.sha256()
    received = 0
    url = f"{base_url.rstrip('/')}/{remote_key}"
    with httpx.stream("GET", url, follow_redirects=True, timeout=120,
                      headers=_PUBLIC_HEADERS) as response:
        response.raise_for_status()
        etag = response.headers.get("etag")
        for chunk in response.iter_bytes(chunk_size=1_048_576):
            actual.update(chunk)
            received += len(chunk)
    version = {"etag": etag, "bytes": received}
    if (not etag or received != size or actual.hexdigest() != digest
            or before != version or object_version(remote_key) != version):
        raise RuntimeError(f"Public readback differs from the candidate: {remote_key}")
    return {**version, "sha256": digest}


def upload_directory_to_r2(local_dir: Path, remote_prefix: str | None = None) -> None:
    """Recursively upload a directory of Parquet to R2, preserving relative paths."""
    if not local_dir.is_dir():
        logger.warning("Skipping (not a directory): {}", local_dir)
        return

    if remote_prefix is None:
        remote_prefix = local_dir.name

    files = sorted(local_dir.rglob("*.parquet"))
    logger.info("Uploading {} files from {}/ to R2...", len(files), local_dir.name)

    for file_path in files:
        relative = file_path.relative_to(local_dir)
        remote_key = f"{remote_prefix}/{relative}"
        upload_file(file_path, remote_key=remote_key)

    logger.info("Uploaded {} files under {}/", len(files), remote_prefix)


def upload_dataset(output_dir: Path, data_types: list[str]) -> None:
    """Publish the merged base tables in parallel.

    Every size guard runs before the first upload starts, so a table this run
    would refuse cannot land after a sibling has already been replaced. The
    pipeline caller preflights a superset (partitions, index, manifest) first,
    and re-checking here is deliberate rather than an oversight: this function
    fans out to a thread pool, so its own preflight is what stops one table
    landing while a sibling is refused. The base tables number two, so it costs
    two HEAD requests.

    Fixed object keys give R2 no atomic multi-object commit. This ordering keeps
    a known guard failure from publishing anything; the transfers themselves
    stay exposed to a network or service failure mid-flight. So the caller
    publishes ``manifest.parquet`` only after every base and partitioned data
    file has succeeded.

    Every upload is awaited and its outcome inspected. A bare
    ``executor.map(upload_file, ...)`` whose lazy iterator nobody consumed used
    to swallow every publish failure while the ETL exited 0, hiding an 8-week
    ``dockets`` outage: the shrink guard rightly refused a truncated
    ``dockets.parquet`` on every run (the Iceberg dockets table was never
    seeded, so the exported snapshot held ~5k rows instead of ~276k) while the
    workflow stayed green and the published table sat frozen at 2026-07-02.

    Rollups (feed_summary, agency_stats, ...) belong to their own decoupled
    ``run-rollup-*`` pipelines now.
    """
    base_files = dataset_files(output_dir, data_types)

    # ThreadPoolExecutor rejects max_workers=0, so an empty publish set would
    # otherwise raise ValueError rather than being the no-op it should be.
    if not base_files:
        logger.warning("upload_dataset: no files to publish in {}", output_dir)
        return

    preflight_uploads(output_dir, base_files)

    failures: list[tuple[str, BaseException]] = []
    with ThreadPoolExecutor(max_workers=len(base_files)) as executor:
        futures = {executor.submit(upload_file, pf): pf for pf in base_files}
        for future in as_completed(futures):
            if (error := future.exception()) is not None:
                failures.append((futures[future].name, error))

    _raise_for_failures("R2 base table publish", failures, len(base_files))


def upload_comment_partitions(output_dir: Path, changed_files: list[Path]) -> None:
    """Publish the changed per-agency comment files, then the refreshed comments index.

    Each file is keyed by its path relative to ``output_dir``. Refuses before
    the first upload if the index is missing: files the index cannot see are
    rows nobody can find.
    """
    index_file = output_dir / "comments_index.parquet"
    if not index_file.exists():
        raise RuntimeError("Expected comments_index.parquet beside the changed partitions")

    # Fixed-width pool for the partitions; the index uploads last so a
    # published index can never name partitions that are not yet there.
    with ThreadPoolExecutor(max_workers=min(8, len(changed_files) or 1)) as executor:
        futures = {
            executor.submit(upload_file, local_path, _remote_key(output_dir, local_path)): local_path
            for local_path in changed_files
        }
        for future in as_completed(futures):
            if (error := future.exception()) is not None:
                raise RuntimeError(f"partition upload failed for {futures[future].name}") from error
    upload_file(index_file, remote_key="comments_index.parquet")

    logger.info("Uploaded {} comment partitions + index", len(changed_files))


def list_r2_files() -> list:
    """List files in the R2 bucket."""
    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
    client = get_r2_client()

    response = client.list_objects_v2(Bucket=bucket)
    if "Contents" not in response:
        logger.info("Bucket is empty")
        return []

    for obj in response["Contents"]:
        logger.info("{} ({:.1f} MB)", obj["Key"], obj["Size"] / 1024 / 1024)
    return response["Contents"]


if __name__ == "__main__":
    from sys import argv

    if len(argv) > 1:
        upload_file(Path(argv[1]))
    else:
        logger.info("Files in R2 bucket:")
        list_r2_files()
