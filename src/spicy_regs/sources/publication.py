"""Immutable table generations and a single conditional publication pointer.

Writers verify all remote bytes before changing the pointer. Readers resolve
the pointer once per operation and use immutable table URLs. The materialized
rulemaking dataset publishes under its own pointer (``SNAPSHOT_POINTER``),
which readers resolve the same way. Legacy bare URLs remain readable only for
tables neither pointer names.

The pointer is ``INDEX_V2_KEY`` alone; it lists split tables (a table stored as
several files) with their members (``docs/research/multi-file-tables-2026-09-26.md``).
Readers resolve any table's files through :func:`table_members`. The older
``publication.json`` is neither written nor read: one left in a bucket is a stale
copy that names whatever generations it last did.
Source-evidence blobs are content-addressed and stored once for all artifacts.

A version-2 family entry may also state ``publishedAt``: the UTC instant its
publisher moved the pointer to that generation, never when the data was read.
:func:`publish_generation` stamps it at the pointer write, and
:func:`backfill_published_at` fills it in for entries written before. Neither
may write to a live bucket until every reader admits the key: a
``parse_index`` that predates it requires the exact family key set and
refuses the whole index. The server image that admits it deploys first.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
from collections.abc import Generator, Iterable, Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkstemp
from typing import TYPE_CHECKING, BinaryIO, Callable, ContextManager, NamedTuple

from spicy_regs.runtime_bounds import checkpoint, remaining_timeout

import httpx
from loguru import logger

if TYPE_CHECKING:
    from botocore.exceptions import ClientError

#: The publication pointer: every family's current generation, split tables included.
INDEX_V2_KEY = "publication.v2.json"
#: The materialized rulemaking dataset's pointer to its current snapshot manifest.
SNAPSHOT_POINTER = "materialized/rulemaking/latest.json"
#: A materialized snapshot id: safe in an object key and inlined SQL. ``pipelines.materialized`` writes and reads by it.
SNAPSHOT_ID = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
#: Snapshot format versions a reader accepts. A published snapshot outlives the
#: code that wrote it, and the prior generation a run restores from was written
#: before version 2 made ``visibility`` required, so refusing version 1 outright
#: would strand every existing dataset's state at its next build.
SNAPSHOT_FORMAT_VERSIONS = (1, 2)
INDEX_LIMIT = 1024 * 1024
PART_BYTES = 64 * 1024 * 1024
EVIDENCE_PREFIX = "source-evidence"
#: Concurrent blob requests while publishing evidence, as r2.py bounds its uploads.
EVIDENCE_WORKERS = 8
#: Bytes per read when a stored evidence blob is read back, so eight concurrent read-backs hold eight chunks.
_READ_BACK_CHUNK = 1024 * 1024
#: An evidence manifest lists every retained body and its journal every
#: capture, so both grow with a run's reads (house communications' 1,043
#: captures: a 261 KB manifest and a 588 KB journal, 2026-09-26). Rulespec
#: bounds a manifest at the same 64 MiB.
EVIDENCE_CONTROL_LIMIT = 64 * 1024 * 1024
FEC_DESCRIPTOR_LIMIT = 8 * 1024 * 1024
_IMMUTABLE_HEADERS = {"ContentType": "application/octet-stream", "CacheControl": "public, max-age=31536000, immutable"}
_BLOB_KEY = re.compile(r"blobs/sha256/([0-9a-f]{64})\Z")
_POINTER_ATTEMPTS = 8
_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_FAMILY_KEYS = frozenset({"prefix", "logicalId", "artifactDigest", "tables"})
#: ``publishedAt``, the one optional family field: when the publisher moved the family's pointer to this
#: generation, as a UTC instant. It says when the pointer moved, not when the data was read.
_INSTANT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z\Z")
_PARTITION_VALUE = re.compile(r"[A-Za-z0-9_.-]+\Z")
_PART = re.compile(r"part-\d{6}\.parquet\Z")
_MEMBER_PATH = re.compile(r"generations/[a-z][a-z0-9_-]*/[0-9a-f]{64}/(.+)\Z")
_snapshot: ContextVar[tuple[str, dict] | None] = ContextVar("publication_snapshot", default=None)


class PublicationError(RuntimeError):
    """Publication or resolution cannot establish one complete generation."""


class NativeConversionProof(NamedTuple):
    """In-memory result of this invocation's complete processing restoration checks.

    Only native_conversion creates this after checking every captured table's
    schema, metadata, ordered values and repetitions, plus native receipts and
    family membership. A row count cannot supply this proof, nor can a saved
    receipt on its own word: a prepared receipt's restoration stands in only
    under the digest its operator carried from the preparation and under the
    same admission key (native_conversion.publish_prepared).
    """

    logical_id: str
    artifact_digest: str
    prior_entry: bytes
    restored_tables: frozenset[str]


def _conversion_tables(proof: NativeConversionProof | None, artifact, prior: Mapping | None) -> frozenset[str]:
    """Bind the restoration proof to the admitted candidate and the complete live prior."""
    if proof is None:
        return frozenset()
    from rulespec_artifacts import canonical_json_bytes

    if (not isinstance(proof, NativeConversionProof) or prior is None or "etlReceipts" in prior
            or not artifact.root["spec"].get("etlReceipts")
            or proof.logical_id != artifact.pin.logical_id or proof.artifact_digest != artifact.pin.artifact_digest
            or proof.prior_entry != canonical_json_bytes(prior)
            or proof.restored_tables != frozenset(prior["tables"])
            or artifact.root["spec"]["readSnapshot"]["families"].get(artifact.root["spec"]["family"]) != prior):
        raise PublicationError("Native conversion proof differs from the candidate or complete captured family")
    return proof.restored_tables


def empty_index() -> dict:
    return {"format": "spicy-regs-publication", "version": 1, "families": {}}


def _pairs(pairs):
    """JSON object-pairs hook that refuses a repeated key.

    Readers such as the MCP server parse the index without ETL dependencies, so
    this cannot use Rulespec's parser.
    """
    result = {}
    for key, value in pairs:
        if key in result:
            raise PublicationError("Publication index repeats a key")
        result[key] = value
    return result


def _counts(value: Mapping) -> bool:
    return all(type(value[field]) is int and value[field] >= 0 for field in ("byteSize", "rows"))


def _split_table(key: str, table: Mapping) -> None:
    """A split table: its members partition it by declared columns, and their counts sum to the table's.

    A partition column is a plain name, since it spells a directory of every member key.
    """
    if set(table) != {"byteSize", "rows", "columns", "partitionColumns", "members"} or not _counts(table):
        raise ValueError("invalid split table descriptor")
    by = table["partitionColumns"]
    if (not isinstance(by, list) or not by or len(set(by)) != len(by)
            or not all(isinstance(column, str) and _NAME.fullmatch(column) for column in by)
            or not set(by) <= {column[0] for column in table["columns"]}):
        raise ValueError("invalid partition columns")
    members, seen = table["members"], set()
    if not isinstance(members, list) or not members:
        raise ValueError("split table has no members")
    for member in members:
        if set(member) != {"key", "sha256", "byteSize", "rows", "partition"} or not _counts(member):
            raise ValueError("invalid member descriptor")
        partition = member["partition"]
        if (not isinstance(partition, dict) or set(partition) != set(by)
                or not all(isinstance(v, str) and _PARTITION_VALUE.fullmatch(v) for v in partition.values())):
            raise ValueError("invalid member partition")
        # JSON object order is not directory order: canonical serialization sorts
        # keys. The explicit partitionColumns list alone declares their order.
        directory = "/".join([key[:-8], *(f"{column}={partition[column]}" for column in by)])
        name = member["key"].removeprefix(directory + "/")
        if name == member["key"] or not _PART.fullmatch(name) or member["key"] in seen:
            raise ValueError("member key differs from its partition")
        if not _DIGEST.fullmatch(member["sha256"]):
            raise ValueError("invalid member digest")
        seen.add(member["key"])
    if (sum(m["rows"] for m in members) != table["rows"]
            or sum(m["byteSize"] for m in members) != table["byteSize"]):
        raise ValueError("member counts differ from their table's")


def member_table(key: str) -> str:
    """The table a member key belongs to: the key itself for a single file, else ``<first directory>.parquet``."""
    return key if "/" not in key else key.split("/", 1)[0] + ".parquet"


def member_partition(key: str) -> dict[str, str]:
    """A split member's ``col=value`` directories, in order; a malformed one raises ``ValueError``."""
    partition = {}
    for part in key.split("/")[1:-1]:
        column, separator, value = part.partition("=")
        if not separator:
            raise ValueError(f"member directory is not col=value: {part!r}")
        partition[column] = value
    return partition


def table_entries(tables: Mapping, members) -> dict[str, dict]:
    """Index descriptors for a generation's declared ``tables`` from its member descriptors, in the index's shape.

    A table declaring ``partitionColumns`` is split: its members are ``<table>/<col>=<value>/part-NNNNNN.parquet``
    and must add up to it. Any other table is exactly one member at its own key. ``ValueError`` names a mismatch.
    """
    grouped: dict[str, list] = {}
    for member in members:
        grouped.setdefault(member_table(member.object_key), []).append(member)
    if not tables or set(grouped) != set(tables):
        raise ValueError("Generation membership differs from its table declarations")
    entries = {}
    for key, declared in tables.items():
        found = sorted(grouped[key], key=lambda member: member.object_key)
        if "partitionColumns" not in declared:
            if [member.object_key for member in found] != [key]:
                raise ValueError("Generation membership differs from its table declarations")
            entries[key] = {"sha256": found[0].sha256, "byteSize": found[0].byte_size, **declared}
            continue
        entries[key] = {**declared, "byteSize": sum(member.byte_size for member in found), "members": [
            {"key": member.object_key, "sha256": member.sha256, "byteSize": member.byte_size,
             "rows": member.record_count, "partition": member_partition(member.object_key)} for member in found]}
        _split_table(key, entries[key])
    return entries


def generation_entry(artifact, members) -> dict:
    """Describe a verified generation using its admitted member descriptors.

    Publication and read-only recovery use the same immutable family fields;
    only a successful pointer write adds ``publishedAt``.
    """
    from spicy_regs.subject_catalog import shared_receipt_logs

    spec = artifact.root["spec"]
    members = list(members)
    try:
        tables = (table_entries(spec["tables"],
                                [m for m in members if m.object_key != "etl_receipts.parquet"])
                  if spec["tables"] else {})
    except ValueError as exc:
        raise PublicationError(str(exc)) from exc
    entry = {
        "prefix": f"generations/{spec['family']}/{artifact.pin.artifact_digest.removeprefix('sha256:')}",
        "logicalId": artifact.pin.logical_id,
        "artifactDigest": artifact.pin.artifact_digest,
        "tables": tables,
    }
    if "etlReceipts" in spec:
        declared = spec["etlReceipts"]
        receipt = next(m for m in members if m.object_key == "etl_receipts.parquet")
        entry["etlReceipts"] = {
            "key": receipt.object_key, "sha256": receipt.sha256, "byteSize": receipt.byte_size,
            "rows": receipt.record_count, "columns": declared["columns"], "generationId": declared["generationId"],
            "datasets": [p["dataset"] for p in declared["policies"] if p["dataset"] not in shared_receipt_logs()],
        }
    return entry


def parse_index(raw: bytes) -> dict:
    """Validate the small mutable pointer without claiming payload verification.

    Version 1 lists only single-file tables. Version 2 may also list split tables, each with its members.
    A family's optional ``publishedAt`` is preserved as a valid UTC instant ending in ``Z``.
    """
    if len(raw) > INDEX_LIMIT:
        raise PublicationError("Publication index exceeds its byte limit")
    try:
        value = json.loads(raw, object_pairs_hook=_pairs)
        if (
            set(value) != {"format", "version", "families"}
            or value["format"] != "spicy-regs-publication"
            or type(value["version"]) is not int
            or value["version"] not in (1, 2)
            or not isinstance(value["families"], dict)
        ):
            raise ValueError("invalid publication index")
        seen = set()
        dataset_owners = set()
        for family, entry in value["families"].items():
            if not _NAME.fullmatch(family) or not _FAMILY_KEYS <= set(entry) <= _FAMILY_KEYS | {"publishedAt", "etlReceipts"}:
                raise ValueError("invalid family")
            if "publishedAt" in entry:
                instant = entry["publishedAt"]
                if not isinstance(instant, str) or not _INSTANT.fullmatch(instant):
                    raise ValueError("invalid publication instant")
                datetime.fromisoformat(instant)  # a calendar instant, not only its shape
            digest = entry["artifactDigest"]
            if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
                raise ValueError("invalid artifact pin")
            if entry["prefix"] != f"generations/{family}/{digest.removeprefix('sha256:')}":
                raise ValueError("prefix differs from artifact identity")
            if not isinstance(entry["logicalId"], str) or not entry["logicalId"].startswith("urn:"):
                raise ValueError("invalid logical identity")
            if not isinstance(entry["tables"], dict) or (not entry["tables"] and "etlReceipts" not in entry):
                raise ValueError("empty family")
            if "etlReceipts" in entry:
                receipt = entry["etlReceipts"]
                if ({"key", "sha256", "byteSize", "rows", "columns", "generationId", "datasets"} != set(receipt)
                        or receipt["key"] != "etl_receipts.parquet"
                        or not _DIGEST.fullmatch(receipt["sha256"]) or not _counts(receipt)
                        or not isinstance(receipt["generationId"], str) or not receipt["generationId"]
                        or not isinstance(receipt["columns"], list) or not receipt["columns"]
                        or any(not isinstance(c, list) or len(c) != 2
                               or not all(isinstance(v, str) and v for v in c) for c in receipt["columns"])
                        or len({c[0] for c in receipt["columns"]}) != len(receipt["columns"])
                        or not isinstance(receipt["datasets"], list) or not receipt["datasets"]
                        or any(not isinstance(name, str) or not _NAME.fullmatch(name) for name in receipt["datasets"])
                        or len(set(receipt["datasets"])) != len(receipt["datasets"])):
                    raise ValueError("invalid ETL receipt member")
            datasets = {key.removesuffix(".parquet") for key in entry["tables"]}
            datasets.update(entry.get("etlReceipts", {}).get("datasets", []))
            if datasets & dataset_owners:
                raise ValueError("multiply owned dataset")
            dataset_owners.update(datasets)
            for key, table in entry["tables"].items():
                if not key.endswith(".parquet") or not _NAME.fullmatch(key[:-8]) or key in seen:
                    raise ValueError("invalid or multiply owned table")
                seen.add(key)
                columns = table.get("columns")
                if not isinstance(columns, list) or not columns:
                    raise ValueError("missing columns")
                if any(
                    not isinstance(c, list) or len(c) != 2 or not all(isinstance(x, str) and x for x in c)
                    for c in columns
                ):
                    raise ValueError("invalid columns")
                if len({c[0] for c in columns}) != len(columns):
                    raise ValueError("duplicate columns")
                if "members" in table and value["version"] == 2:
                    _split_table(key, table)
                    continue
                if set(table) != {"sha256", "byteSize", "rows", "columns"}:
                    raise ValueError("invalid table descriptor")
                if not _DIGEST.fullmatch(table["sha256"]):
                    raise ValueError("invalid table digest")
                if not _counts(table):
                    raise ValueError("invalid table counts")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid publication index") from exc
    return value


def _bounded_get(
    url: str, *, allow_missing: bool, headers: Mapping[str, str] | None = None, limit: int = INDEX_LIMIT
) -> bytes | None:
    """GET at most ``limit`` bytes; a 404 is ``None`` only when ``allow_missing``."""
    with httpx.stream("GET", url, headers={"User-Agent": "spicy-regs", **(headers or {})},
                      follow_redirects=True, timeout=remaining_timeout(60)) as response:
        if allow_missing and response.status_code == 404:
            return None
        response.raise_for_status()
        raw = bytearray()
        for chunk in response.iter_bytes():
            checkpoint()
            raw.extend(chunk)
            if len(raw) > limit:
                raise PublicationError(f"{url} exceeds its byte limit")
    return bytes(raw)


def load_index_bytes(base_url: str) -> bytes | None:
    """The publication pointer's bytes as served, unparsed; ``None`` when it is absent (HTTP 404)."""
    return _bounded_get(
        f"{base_url.rstrip('/')}/{INDEX_V2_KEY}", allow_missing=True, headers={"Cache-Control": "no-cache"}
    )


def load_index(base_url: str) -> dict:
    """Read the publication pointer. Only a 404 permits legacy table resolution."""
    raw = load_index_bytes(base_url)
    return empty_index() if raw is None else parse_index(raw)


def rulemaking_receipt(snapshot: Mapping) -> dict | None:
    """The captured snapshot's declared receipt member; legacy snapshots declare none.

    The internal member must belong to the same snapshot and run as its subjects.
    Validate its path and digest before a consumer constructs a URL from it.
    """
    manifest = snapshot.get("manifest", {})
    declaration = manifest.get("etlReceipts")
    if declaration is None:
        return None
    try:
        snapshot_id = snapshot["snapshot_id"]
        prefix = f"materialized/rulemaking/snapshots/{snapshot_id}"
        generation = declaration["generationId"]
        record = manifest["artifacts"]["etl_receipts.parquet"]
        datasets = [policy["dataset"] for policy in declaration["policies"]]
        if (not SNAPSHOT_ID.fullmatch(snapshot_id)
                or snapshot["manifest_key"] != f"{prefix}/manifest.json"
                or manifest["snapshot_id"] != snapshot_id or manifest["dataset"] != "rulemaking"
                or declaration["key"] != "etl_receipts.parquet"
                or not isinstance(generation, str) or not generation or generation != manifest["run_id"]
                or record["remote_key"] != f"{prefix}/etl_receipts.parquet"
                or record["visibility"] != "internal"
                or not isinstance(record["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", record["sha256"])
                or any(type(record[name]) is not int or record[name] < 0 for name in ("bytes", "rows"))
                or not datasets or any(not isinstance(name, str) or not _NAME.fullmatch(name) for name in datasets)
                or len(set(datasets)) != len(datasets)):
            raise ValueError("receipt is not the declared snapshot/run member")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid rulemaking snapshot receipt") from exc
    return {**record, "generationId": generation, "datasets": datasets}


def load_rulemaking_snapshot(base_url: str, *, read=None) -> dict | None:
    """Read the rulemaking pointer and the manifest it names, once; ``None`` while no snapshot is published.

    Returns the snapshot id, its manifest key and, by table key, the record of
    each artifact the manifest marks public. As ``pipelines.materialized``
    reads its prior generation, both documents must be a readable format
    version, the same one, of this dataset and one snapshot; each public
    artifact must also sit at its own name under that snapshot's prefix,
    because readers inline these keys into URLs and SQL. An artifact that does
    not say it is public is left out.
    """
    read = _bounded_get if read is None else read
    base = base_url.rstrip("/")
    raw = read(f"{base}/{SNAPSHOT_POINTER}", allow_missing=True, headers={"Cache-Control": "no-cache"})
    if raw is None:
        return None
    root = SNAPSHOT_POINTER.removesuffix("latest.json")
    dataset = root.removeprefix("materialized/").rstrip("/")
    try:
        pointer = json.loads(raw, object_pairs_hook=_pairs)
        snapshot_id = pointer["snapshot_id"]
        prefix = f"{root}snapshots/{snapshot_id}"
        if pointer["format_version"] not in SNAPSHOT_FORMAT_VERSIONS or pointer["dataset"] != dataset:
            raise ValueError("pointer is not a readable rulemaking pointer")
        if not SNAPSHOT_ID.fullmatch(snapshot_id) or pointer["manifest_key"] != f"{prefix}/manifest.json":
            raise ValueError("pointer names no manifest of its own snapshot")
        raw = read(f"{base}/{prefix}/manifest.json", allow_missing=False)
        assert raw is not None
        manifest = json.loads(raw, object_pairs_hook=_pairs)
        if (manifest["format_version"], manifest["dataset"], manifest["snapshot_id"]) != (
            pointer["format_version"], dataset, snapshot_id
        ):
            raise ValueError("pointer and manifest name different snapshots")
        tables = {key: record for key, record in manifest["artifacts"].items() if record.get("visibility") == "public"}
        for key, record in tables.items():
            if not key.endswith(".parquet") or not _NAME.fullmatch(key[:-8]) or record["remote_key"] != f"{prefix}/{key}":
                raise ValueError(f"public artifact {key!r} is not its snapshot's member")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid rulemaking snapshot") from exc
    snapshot = {"snapshot_id": snapshot_id, "manifest_key": f"{prefix}/manifest.json", "tables": tables,
                "manifest": manifest}
    rulemaking_receipt(snapshot)
    return snapshot


#: The comment files the export receipt identifies at fixed public URLs, as table names; the receipt's agency
#: partitions are downloads, not served tables.
#: Public reads ask for the stored bytes, so ETag and length are the stored version's.
PUBLIC_HEADERS = {"User-Agent": "spicy-regs", "Accept-Encoding": "identity"}

COMMENTS_EXPORT_TABLES = ("comments", "comments_index")


def comments_file_record(record) -> bool:
    """One export receipt file: a stored version, a non-negative size and row count, and its read-back digest."""
    return (isinstance(record, Mapping) and isinstance(record.get("etag"), str) and bool(record["etag"])
            and isinstance(record.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", record["sha256"]) is not None
            and all(type(record.get(name)) is int and record[name] >= 0 for name in ("rows", "bytes")))


def load_comments_publication(base_url: str, *, read=None) -> dict | None:
    """Read the catalog export receipt; fixed member URLs still require version checks.

    This receipt is written only after both public comment files and all agency
    partitions have passed readback. It identifies one catalog snapshot, not an
    immutable generation URL. Readers must check member ETags around their read.
    """
    read = _bounded_get if read is None else read
    raw = read(f"{base_url.rstrip('/')}/comments-publication.json", allow_missing=True,
                       headers={"Cache-Control": "no-cache"})
    if raw is None:
        return None
    try:
        receipt = json.loads(raw, object_pairs_hook=_pairs)
        source = receipt["source"]
        if receipt["format_version"] != 1 or not source["table_uuid"]:
            raise ValueError("unknown receipt format or table identity")
        if any(type(source[key]) is not int or source[key] < 0 for key in ("snapshot_id", "schema_id")):
            raise ValueError("invalid catalog version")
        if not all(comments_file_record(receipt["files"][table + ".parquet"]) for table in COMMENTS_EXPORT_TABLES):
            raise ValueError("invalid public file identity")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid comments publication receipt") from exc
    return {"receipt_sha256": "sha256:" + hashlib.sha256(raw).hexdigest(), "receipt": receipt}


def comments_export_pins(base_url: str, comments: Mapping, tables: Iterable[str]) -> dict[str, dict]:
    """Each named export table's pin from a loaded receipt: its file record, fixed URL and the receipt's identity."""
    base = base_url.rstrip("/")
    receipt = comments["receipt"]
    pins = {}
    for table in tables:
        record = receipt["files"][table + ".parquet"]
        pins[table] = {**record, "sha256": "sha256:" + record["sha256"], "kind": "comments-mirror",
                       "family": "comments-mirror", "receipt_sha256": comments["receipt_sha256"],
                       "source": receipt["source"], "urls": [f"{base}/{table}.parquet"]}
    return pins


def public_object_version(url: str, *, timeout: float = 60, attempts: int = 1) -> dict | None:
    """The stored version of a public object, as ``r2.object_version`` states it; ``None`` when it is absent.

    The request asks for the stored bytes: the public host otherwise answers a JSON object gzip-encoded, with a
    weak ETag and no length (``publication.v2.json`` on the custom domain, 2026-10-05). It also bypasses the edge
    cache, which can answer a replaced object's old version. ``attempts`` above one retries 429, 5xx and transport
    failures with a capped backoff; other refusals fail at once. Plain httpx: the MCP image installs neither boto3
    nor SpicyDocs, and ``mutable_versions_match`` reaches this there.
    """
    for attempt in range(1, attempts + 1):
        try:
            response = httpx.head(url, follow_redirects=True, timeout=timeout,
                                  headers={**PUBLIC_HEADERS, "Cache-Control": "no-cache"})
        except httpx.TransportError:
            if attempt == attempts:
                raise
        else:
            if (response.status_code != 429 and response.status_code < 500) or attempt == attempts:
                break
        time.sleep(remaining_timeout(min(2 ** attempt, 16)))
    if response.status_code == 404:
        return None
    response.raise_for_status()
    etag = response.headers.get("etag")
    if not etag:
        raise RuntimeError(f"Public object has no ETag: {url}")
    return {"etag": etag, "bytes": int(response.headers["content-length"])}


def public_version_drift(expected: Mapping[str, Mapping], *, timeout: float = 60, attempts: int = 1) -> list[str]:
    """``url: expected, public`` for each URL whose public stored version is not ``{"etag", "bytes"}`` as expected.

    Any rewrite or removal of an object changes its ETag, so a match binds the bytes a publisher read back.
    """
    def drift(entry: tuple[str, Mapping]) -> str | None:
        url, item = entry
        wanted = {"etag": item["etag"], "bytes": item["bytes"]}
        actual = public_object_version(url, timeout=timeout, attempts=attempts)
        return None if actual == wanted else f"{url}: expected {wanted}, public {actual}"

    if len(expected) == 1:
        return [error for error in map(drift, expected.items()) if error]
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        return [error for error in pool.map(drift, sorted(expected.items())) if error]


def mutable_versions_match(pins: Mapping[str, Mapping | None]) -> bool:
    """Fixed comment URLs are usable only while their published object versions match the receipt.

    One HEAD per comments-mirror pin; other pins are immutable and pass.
    """
    mutable = {pin["urls"][0]: pin for pin in pins.values() if pin is not None and pin.get("kind") == "comments-mirror"}
    return not mutable or not public_version_drift(mutable, timeout=remaining_timeout(30))


def published_urls(base_url: str, *, index: Mapping | None = None) -> dict[str, list[str]]:
    """Each table the publisher's two pointers name, by name, to its immutable file URLs.

    The publication index's generations come first, then the rulemaking
    snapshot's public artifacts. A table neither names is legacy: its URL is
    the bare ``<name>.parquet`` key, which the caller adds.
    """
    base = base_url.rstrip("/")
    index = load_index(base) if index is None else index
    snapshot = load_rulemaking_snapshot(base)
    urls = {table: [f"{base}/{member.path}" for member in table_members(index, f"{table}.parquet")]
            for table in parquet_tables(index)}
    for key, record in (snapshot or {"tables": {}})["tables"].items():
        urls.setdefault(key.removesuffix(".parquet"), [f"{base}/{record['remote_key']}"])
    return urls


@dataclass(frozen=True)
class DownloadedMember:
    """Byte verification tied to the actual final file, rather than a later path lookup."""
    sha256: str
    byte_size: int
    state: tuple[int, int, int, int, int, int]


RESUME_STALLS = 5  # consecutive reconnects that add no byte before a member fetch gives up
_STRONG_ETAG = re.compile(r'"[\x21\x23-\x7e\x80-\xff]*"')


def _resume_etag(response) -> str | None:
    """The strong ETag that makes a byte-range resume of this response safe, or ``None`` when it cannot be resumed.

    Same rule as ``spicy_docs.transport.resume.PartialDownload``: an encoded body has no stable byte offsets, and a
    weak or missing validator cannot pin the object across ranges.
    """
    etag = response.headers.get("etag")
    if (response.headers.get("content-encoding") or "identity").lower() != "identity" or not etag:
        return None
    return etag if _STRONG_ETAG.fullmatch(etag) else None


class _MemberAbsent(Exception):
    """The first response for a member was HTTP 404."""


def _resumable_body(url: str, label: str, headers: Mapping[str, str] | None,
                    timeout: float | None) -> Generator[bytes | None, None, None]:
    """Yield one object's body across connection resets: ``b""`` once the first response is accepted, then its chunks.

    ``None`` means the object changed: every byte yielded before it must be discarded and the body restarts at 0.
    The public host resets long responses (seven members from 163 MB to 3.5 GB in one mirror run), so a transport
    error mid-body reconnects for the remainder: ``Range`` from the held byte with ``If-Match`` on the body's strong
    ETag, admitting only HTTP 206 with the exact remaining ``Content-Range`` and the same ETag (the rules of
    ``explorer_publications._range``). A 200 or 412 answer means the object changed or the range was ignored.
    Resume is gated on a strong ETag rather than on a pin, so a mutable legacy key resumes too; the pin is
    :class:`_PinnedBody`'s rule. ``RESUME_STALLS`` reconnects in a row without progress, or the caller's runtime
    deadline, end the body. An in-process reconnect is used rather than ``PartialDownload``'s fsync'd checkpoint
    because this contract keeps no state across processes. A 404 before any response was accepted raises
    :class:`_MemberAbsent`; any other refusal raises ``RuntimeError``.
    """
    etag, source, held, stalls, resuming, started = None, None, 0, 0, False, False
    while True:
        sent = dict(headers or {})
        if resuming:
            sent |= {"Range": f"bytes={held}-", "If-Match": etag, "Accept-Encoding": "identity"}
        request: dict = {name: value for name, value in (("headers", sent or None),
                                                         ("timeout", remaining_timeout(timeout)))
                         if value is not None}
        start = held
        try:
            with httpx.stream("GET", url, follow_redirects=True, **request) as response:
                status = response.status_code
                if resuming and status in (200, 412):
                    logger.warning("{} changed or ignored the range at byte {}; restarting", label, held)
                    yield None
                    etag, source, held, start, resuming = None, None, 0, 0, False
                    if status == 412:
                        stalls += 1
                        if stalls >= RESUME_STALLS:
                            raise RuntimeError(f"{label} kept changing during download")
                        continue
                elif resuming:
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("content-range", ""))
                    if (status != 206 or match is None or int(match[1]) != held
                            or int(match[2]) != int(match[3]) - 1 or response.headers.get("etag") != etag):
                        raise RuntimeError(f"Resume of {label} did not return the exact remaining range "
                                           f"(HTTP {status})")
                elif status == 404 and not started:
                    raise _MemberAbsent(label)
                elif status != 200:
                    raise RuntimeError(f"Failed to download {label} from R2: HTTP {status}")
                if status == 200:
                    source = response
                if not started:
                    started = True
                    yield b""
                for chunk in response.iter_bytes():
                    yield chunk
                    held += len(chunk)
            return
        except httpx.TransportError as error:
            etag = etag or (_resume_etag(source) if source is not None else None)
            if etag is None:
                raise
            stalls = 0 if held > start else stalls + 1
            if stalls >= RESUME_STALLS:
                raise RuntimeError(f"{label}: {stalls} reconnects without progress at byte {held}") from error
            logger.warning("{} reset at byte {} ({}); resuming", label, held, type(error).__name__)
            time.sleep(remaining_timeout(min(30, 2 ** stalls)))
            resuming = True


class _PinnedBody:
    """One member's body, chunk by chunk, held to its pin while it streams.

    The single streaming loop behind :func:`fetch_member` and :func:`verify_member`, fed by
    :func:`_resumable_body`, so the pin holds across resumed ranges. ``None`` passes through as the restart marker
    and resets the digest. A pinned body longer than its pin stops at the first excess chunk; once exhausted,
    ``sha256`` and ``byte_size`` describe what arrived, and a pinned body that differs raises.
    """

    def __init__(self, chunks: Iterator[bytes | None], member: Member, label: str):
        self._chunks, self._member, self._label = chunks, member, label
        self.sha256: str | None = None
        self.byte_size = 0

    def __iter__(self) -> Iterator[bytes | None]:
        member, digest = self._member, hashlib.sha256()
        for chunk in self._chunks:
            checkpoint()
            if chunk is None:
                digest, self.byte_size = hashlib.sha256(), 0
                yield None
                continue
            self.byte_size += len(chunk)
            if member.sha256 is not None and self.byte_size > (member.byte_size or 0):
                raise RuntimeError(f"Published generation member differs from its pin: {self._label}")
            digest.update(chunk)
            yield chunk
        self.sha256 = "sha256:" + digest.hexdigest()
        if member.sha256 is not None and (member.byte_size != self.byte_size or member.sha256 != self.sha256):
            raise RuntimeError(f"Published generation member differs from its pin: {self._label}")


@contextmanager
def _member_body(base_url: str, member: Member, label: str, *, headers: Mapping[str, str] | None,
                 timeout: float | None) -> Iterator[_PinnedBody | None]:
    """``None`` only when an unpinned (legacy) file is absent (HTTP 404); any other refusal raises."""
    with closing(_resumable_body(f"{base_url.rstrip('/')}/{member.path}", label, headers, timeout)) as chunks:
        try:
            next(chunks)
        except _MemberAbsent:
            if member.sha256 is not None:
                raise RuntimeError(f"Published generation member is missing: {label}") from None
            logger.info("{} not found on R2 (404)", label)
            body = None
        else:
            body = _PinnedBody(chunks, member, label)
        yield body


def fetch_member(base_url: str, member: Member, local_path: Path, label: str | None = None, *,
                 headers: Mapping[str, str] | None = None, timeout: float | None = None) -> DownloadedMember | bool:
    """Stream one table file to ``local_path`` through a sibling temp file, checked against its pin when it has one.

    ``False`` only when an unpinned (legacy) file is absent (HTTP 404). A missing pinned member, any other status, a
    transport error that cannot be resumed (``_resumable_body``) or bytes that differ from the pin raise, leaving
    ``local_path`` untouched and no temp file. ``headers`` and ``timeout`` pass through to each request when given.
    Each byte is hashed once, as it arrives; a restarted body truncates the temp file and the digest.
    """
    label = label or member.path
    from spicy_regs.local_data import file_state_from_stat

    temp_path = None
    try:
        with _member_body(base_url, member, label, headers=headers, timeout=timeout) as body:
            if body is None:
                return False
            fd, temporary = mkstemp(prefix=local_path.name + ".", suffix=".tmp", dir=local_path.parent)
            temp_path = Path(temporary)
            with os.fdopen(fd, "wb") as out:
                for chunk in body:
                    if chunk is None:
                        out.seek(0)
                        out.truncate()
                        continue
                    out.write(chunk)
                out.flush()
                written = file_state_from_stat(os.fstat(out.fileno()))
                if written[2] != body.byte_size or file_state_from_stat(temp_path.lstat()) != written:
                    raise RuntimeError(f"Downloaded member changed before adoption: {label}")
                temp_path.replace(local_path)
                state = file_state_from_stat(os.fstat(out.fileno()))
                if state[:4] + state[5:] != written[:4] + written[5:]:
                    raise RuntimeError(f"Downloaded member changed during adoption: {label}")
                if file_state_from_stat(local_path.lstat()) != state:
                    raise RuntimeError(f"Downloaded member replaced during adoption: {label}")
                result = DownloadedMember(str(body.sha256), body.byte_size, state)
    except BaseException:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise
    logger.info("Downloaded {} from R2", label)
    return result


@contextmanager
def open_member(base_url: str, member: Member):
    """A seekable ranged reader over one pinned member: its stored size must equal the pin, and one ETag keeps the
    ranges consistent within the read. The whole-file SHA-256 is not verified."""
    from spicy_regs.sources.remote_parquet import open_published_http

    if member.byte_size is None:
        raise ValueError(f"A ranged read needs a pinned member: {member.path}")
    with open_published_http(f"{base_url.rstrip('/')}/{member.path}", byte_size=member.byte_size) as stream:
        yield stream


def verify_member(base_url: str, member: Member, label: str | None = None) -> int:
    """Stream one pinned member as an anonymous consumer receives it and hold it to its pin; nothing is written.

    Returns the byte count. An absent member, any other status, a transport error that cannot be resumed or bytes
    that differ raise.
    ``Accept-Encoding: identity`` keeps the hash over the stored bytes rather than a decoded transfer.
    """
    if member.sha256 is None:
        raise ValueError(f"Read-back needs a pinned member: {member.path}")
    label = label or member.path
    with _member_body(base_url, member, label, headers={"User-Agent": "spicy-regs", "Accept-Encoding": "identity"},
                      timeout=60) as body:
        assert body is not None  # a pinned 404 raised above
        for _ in body:
            pass
    return body.byte_size


def parquet_scan(paths: Sequence[str]) -> str:
    """``read_parquet`` over one table's files as a SQL expression.

    Hive partitioning is off: a split table stores its partition column inside each file, so a path's ``col=value``
    must not add a second one.
    """
    listed = ", ".join("'" + path.replace("'", "''") + "'" for path in paths)
    return f"read_parquet([{listed}], hive_partitioning = false)"


def current_index(base_url: str) -> dict:
    """Return the index bound by the active ``snapshot`` for ``base_url``, otherwise load it."""
    held = _snapshot.get()
    return held[1] if held is not None and held[0] == base_url.rstrip("/") else load_index(base_url)


def family_root(raw: bytes, entry: Mapping) -> dict:
    """Parse an artifact root and require it to be the one ``entry`` pins; the caller chooses where ``raw`` came from."""
    from rulespec_artifacts import ArtifactVerificationError, expected_artifact_digest, parse_canonical_json

    try:
        root = parse_canonical_json(raw)
    except ArtifactVerificationError as exc:
        raise PublicationError("Pinned root is not canonical artifact JSON") from exc
    if (not isinstance(root, dict) or root.get("artifactDigest") != entry["artifactDigest"]
            or expected_artifact_digest(root) != entry["artifactDigest"]
            or root.get("logicalId") != entry["logicalId"] or not isinstance(root.get("inputs"), list)):
        raise PublicationError("Pinned root differs from its captured pin")
    return root


def _root_bytes(base_url: str, entry: Mapping) -> bytes:
    raw = _bounded_get(f"{base_url.rstrip('/')}/{entry['prefix']}/artifact.json", allow_missing=False)
    assert raw is not None
    return raw


def load_family_root(base_url: str, entry: Mapping) -> tuple[bytes, dict]:
    """Read only the pinned prior root for lineage over HTTPS; this does not re-admit its tables."""
    raw = _root_bytes(base_url, entry)
    return raw, family_root(raw, entry)


def read_pinned_root(base_url: str, entry: Mapping) -> dict:
    """The root at ``entry``'s immutable prefix, read the way the MCP server reads members.

    The URL is pinned and the root must name the pin, but its digest is not
    recomputed: the server image does not install rulespec-artifacts, which
    :func:`load_family_root` verifies with for lineage.
    """
    try:
        root = json.loads(_root_bytes(base_url, entry), object_pairs_hook=_pairs)
    except ValueError as exc:
        raise PublicationError("Pinned root is not JSON") from exc
    if (not isinstance(root, dict) or root.get("artifactDigest") != entry["artifactDigest"]
            or root.get("logicalId") != entry.get("logicalId")):
        raise PublicationError("Pinned root differs from its captured pin")
    return root


def load_evidence_journal(base_url: str, pin: Mapping, *, predating_ok: bool = False) -> bytes:
    """Read one published source-evidence journal, each hop checked against the digest above it; no blob is read.

    The root must be the one ``pin`` names and current completed-build evidence
    (refused before any member is requested), its member manifest the bytes the
    root declares, and the journal the bytes that manifest declares. With
    ``predating_ok``, evidence written before ``evidence_version`` 2 reads as an
    empty journal instead, for a caller that inherits nothing from it.
    """
    from rulespec_artifacts import ROOT_OBJECT_KEY, parse_admitted_json, sha256_digest

    from spicy_regs.source_evidence import (
        JOURNAL,
        SourceEvidenceError,
        predates_current_evidence,
        require_current_evidence_root,
    )

    prefix = f"{base_url.rstrip('/')}/{EVIDENCE_PREFIX}/{pin['artifactDigest'].removeprefix('sha256:')}"

    def pinned(key: str, digest: str) -> bytes:
        raw = _bounded_get(f"{prefix}/{key}", allow_missing=False, limit=EVIDENCE_CONTROL_LIMIT)
        if raw is None or sha256_digest(raw) != digest:
            raise PublicationError(f"Source evidence {key} differs from its declared digest")
        return raw

    raw_root = _bounded_get(f"{prefix}/{ROOT_OBJECT_KEY}", allow_missing=False)
    assert raw_root is not None
    root = family_root(raw_root, pin)
    if predating_ok and predates_current_evidence(root):
        return b""
    try:
        require_current_evidence_root(root)
    except SourceEvidenceError as error:
        raise PublicationError(f"Inherited source evidence refused: {error}") from error
    for reference in root["memberManifests"]:
        manifest = parse_admitted_json(pinned(reference["objectKey"], reference["sha256"]))
        for member in manifest["members"]:
            if member["objectKey"] == JOURNAL:
                return pinned(JOURNAL, member["sha256"])
    raise PublicationError("Source evidence declares no journal")


@contextmanager
def snapshot(base_url: str) -> Iterator[dict]:
    """Bind all member reads in this operation to one pointer observation."""
    selected = current_index(base_url)
    token = _snapshot.set((base_url.rstrip("/"), selected))
    try:
        yield selected
    finally:
        _snapshot.reset(token)


def parquet_tables(index: Mapping) -> tuple[str, ...]:
    """Active Parquet table names, independent of any consumer's supported-table list."""
    return tuple(sorted({key.removesuffix(".parquet")
                         for family in index["families"].values()
                         for key in family["tables"] if key.endswith(".parquet")}))


class Member(NamedTuple):
    """One file of a table: its path under the public base and, when published, its pin."""

    path: str
    sha256: str | None
    byte_size: int | None
    rows: int | None

    @property
    def key(self) -> str:
        """The file's key within its generation, ``<table>.parquet`` or a split member's; a legacy file's own path.

        A downloaded table keeps this layout locally. A pinned member's path must be
        ``generations/<family>/<digest>/<key>``.
        """
        if self.sha256 is None:
            return self.path
        match = _MEMBER_PATH.fullmatch(self.path)
        if match is None:
            raise PublicationError(f"Member path is not under a generation prefix: {self.path}")
        return match[1]


def table_members(index: Mapping, key: str) -> tuple[Member, ...]:
    """Every file of table ``key``, in index order; one unpinned member at the bare key when no family publishes it.

    The bare-key fallback is what keeps legacy table URLs readable.
    """
    owner = table_owner(index, key)
    if owner is None:
        return (Member(key, None, None, None),)
    prefix, table = owner[1]["prefix"], owner[1]["tables"][key]
    return tuple(Member(f"{prefix}/{m['key']}", m["sha256"], m["byteSize"], m["rows"])
                 for m in table.get("members", [{"key": key, **table}]))


def table_descriptor(index: Mapping, key: str) -> dict | None:
    """The index's descriptor for table ``key`` (rows, bytes, columns and, when split, members), or ``None``."""
    owner = table_owner(index, key)
    return None if owner is None else owner[1]["tables"][key]


def table_pin(index: Mapping, key: str) -> dict:
    """Pin a managed table, including every member of a partitioned table.

    Single-file tables retain their byte digest. A split table has no single
    byte stream: ``tableDescriptorDigest`` hashes its complete UTF-8 JSON
    descriptor with sorted keys, compact separators and unescaped Unicode.
    The descriptor contains each member's path, byte digest, size and partition.
    """
    owner = table_owner(index, key)
    if owner is None:
        raise PublicationError(f"Cannot pin an unmanaged table: {key}")
    descriptor = owner[1]["tables"][key]
    if "members" in descriptor:
        raw = json.dumps(descriptor, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
        digest = {"tableDescriptorDigest": "sha256:" + hashlib.sha256(raw).hexdigest()}
    else:
        digest = {"sha256": descriptor["sha256"]}
    return {**digest, "byteSize": descriptor["byteSize"], "family": owner[0],
            "artifactDigest": owner[1]["artifactDigest"]}


def single_member(index: Mapping, key: str) -> Member:
    """The one file of table ``key``; a split table refuses, naming the resolver its reader needs."""
    members = table_members(index, key)
    if len(members) != 1:
        raise PublicationError(f"{key} is published as {len(members)} files; read it through table_members")
    return members[0]


def table_owner(index: Mapping, key: str) -> tuple[str, Mapping] | None:
    """The family publishing ``key`` in ``index`` and its entry, or ``None`` for a legacy table."""
    return next(((name, entry) for name, entry in index["families"].items() if key in entry["tables"]), None)


def _missing(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}


def _precondition(error: ClientError) -> bool:
    """A refused conditional write; S3 reports a concurrent conditional write as ``ConditionalRequestConflict``."""
    return error.response.get("Error", {}).get("Code") in {"412", "PreconditionFailed", "ConditionalRequestConflict"}


def _head(client, bucket: str, key: str, **options) -> dict | None:
    """HEAD one object; only absence is ``None``, so permission and 5xx errors propagate."""
    from botocore.exceptions import ClientError

    try:
        return client.head_object(Bucket=bucket, Key=key, **options)
    except ClientError as exc:
        if _missing(exc):
            return None
        raise


def file_identity(path: Path, *, part_bytes: int = PART_BYTES) -> dict:
    """Size, SHA-256 and the S3/R2 ETag of ``path`` uploaded in ``part_bytes`` parts, in one read.

    A single-part upload's ETag is the file's MD5; a multipart one is the MD5 of
    the part MD5s plus the part count. ``_put_immutable`` uses ``PART_BYTES``;
    boto3's managed transfer uses 8 MiB parts.
    """
    with path.open("rb") as stream:
        return _stream_identity(stream, part_bytes=part_bytes)


def _stream_identity(stream, *, part_bytes: int = PART_BYTES) -> dict:
    """Measure the same upload identity for a file or a decoded central original."""
    digest = hashlib.sha256()
    parts, size = [], 0
    while chunk := stream.read(part_bytes):
        checkpoint()
        digest.update(chunk)
        size += len(chunk)
        parts.append(hashlib.md5(chunk, usedforsecurity=False).digest())
    if len(parts) <= 1:
        etag = (parts[0] if parts else hashlib.md5(b"", usedforsecurity=False).digest()).hex()
    else:
        etag = f"{hashlib.md5(b''.join(parts), usedforsecurity=False).hexdigest()}-{len(parts)}"
    return {"bytes": size, "sha256": "sha256:" + digest.hexdigest(), "etag": f'"{etag}"'}


def _get_bounded(client, bucket: str, key: str) -> tuple[bytes, str] | None:
    """Read a small control object and its ETag; only absence is ``None`` and oversize refuses."""
    from botocore.exceptions import ClientError

    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if _missing(exc):
            return None
        raise
    body = response["Body"]
    try:
        raw = body.read(INDEX_LIMIT + 1)
    finally:
        body.close()
    if len(raw) > INDEX_LIMIT:
        raise PublicationError(f"{key} exceeds its byte limit")
    return raw, response["ETag"]


def _stored_index(client, bucket: str) -> tuple[dict, str | None]:
    """Read the stored publication index and its conditional-write token.

    While the index is absent it is empty with no token, so the first write creates it. A response without an
    ETag refuses.
    """
    stored = _get_bounded(client, bucket, INDEX_V2_KEY)
    if stored is None:
        return {**empty_index(), "version": 2}, None
    raw, etag = stored
    if not isinstance(etag, str) or not etag:
        raise PublicationError("Publication index has no conditional-write token")
    return {**parse_index(raw), "version": 2}, etag


def _put_pointer(client, bucket: str, key: str, raw: bytes, etag: str | None) -> bool:
    """Replace pointer ``key`` only if it still has ``etag`` (``None``: only if absent); ``False`` when it moved."""
    from botocore.exceptions import ClientError

    condition = {"IfMatch": etag} if etag is not None else {"IfNoneMatch": "*"}
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType="application/json",
                          CacheControl="no-store, no-cache, must-revalidate", **condition)
    except ClientError as exc:
        if not _precondition(exc):
            raise
        return False
    return True


def _copy_unchanged_member(client, bucket: str, source_key: str, destination_key: str) -> None:
    """Copy an unchanged member server-side instead of re-uploading its bytes.

    Destination creation stays conditional: it HEADs first and skips when the
    object already exists, so a resumed publication never overwrites. The
    caller re-verifies the whole prefix afterwards (``admit_artifact``), so a
    copy can never publish wrong bytes — a wrong source digest fails
    verification before the pointer moves.
    """
    if _head(client, bucket, destination_key) is None:
        client.copy_object(Bucket=bucket, Key=destination_key, CopySource={"Bucket": bucket, "Key": source_key})


def _create_multipart(client, bucket: str, key: str, upload_parts: Callable[[str], list[dict]]) -> None:
    """Create-only multipart lifecycle: any failure aborts; a refused completion means the key already exists.

    The caller admits the destination bytes afterwards, so an existing object is
    only reused once it proves to hold the expected bytes.
    """
    from botocore.exceptions import ClientError

    args = {"Bucket": bucket, "Key": key}
    upload_id = client.create_multipart_upload(**args, **_IMMUTABLE_HEADERS)["UploadId"]
    completing = False
    try:
        parts = upload_parts(upload_id)
        completing = True
        client.complete_multipart_upload(**args, UploadId=upload_id, MultipartUpload={"Parts": parts},
                                         IfNoneMatch="*")
    except BaseException as exc:
        client.abort_multipart_upload(**args, UploadId=upload_id)
        if not completing or not isinstance(exc, ClientError) or not _precondition(exc):
            raise


def _put_immutable(client, bucket: str, key: str, path: Path, *, sha256: str | None = None) -> None:
    """Conditional creation, including multipart completion for large tables.

    Every request carries Content-MD5 and no SHA-256 checksum, so storage refuses bytes
    damaged in transit. R2 refuses Content-MD5 beside a SHA-256 checksum ("You can only
    specify one non-default checksum at a time") and answers NotImplemented to a SHA-256
    checksum on UploadPart (probe, 2026-11-01). The ETag is then the whole-object MD5 or
    the composite MD5 over these parts, which a later HEAD can compare (:func:`verify_stored_member`). With ``sha256``, the bytes actually sent must hash to it
    before the object is created, so a content-addressed key never holds other bytes.
    """
    _put_immutable_stream(client, bucket, key, lambda: path.open("rb"), path.stat().st_size, sha256=sha256)


def _put_immutable_stream(client, bucket, key, open_body, size, *, sha256=None) -> bool:
    """Conditional bounded upload from any supported original-byte reader.

    ``True`` only when this call created the object with one PUT and storage answered with the MD5 of the very
    buffer sent as its ETag. ``False`` otherwise: the key already held an object (including this call's own
    retried request), the upload was multipart, or the answer named another ETag or none. The answer is storage's
    account of what it received, not a read of what it holds; the evidence publisher logs it and relies on a read.
    """
    from botocore.exceptions import ClientError

    if size > PART_BYTES * 10_000:
        raise PublicationError("Generation member exceeds the bounded multipart size")
    digest = hashlib.sha256()

    def checked(chunk: bytes) -> dict:
        digest.update(chunk)
        return {"Body": chunk, "ContentMD5": base64.b64encode(hashlib.md5(chunk, usedforsecurity=False).digest()).decode()}

    def assert_content_address() -> None:
        if sha256 is not None and "sha256:" + digest.hexdigest() != sha256:
            raise PublicationError(f"Bytes sent for {key} differ from their content address")

    if size <= PART_BYTES:
        with open_body() as stream:
            raw = stream.read(size + 1)
            if len(raw) != size:
                raise PublicationError(f"Bytes sent for {key} differ from their declared size")
            body = checked(raw)
        assert_content_address()
        try:
            created = client.put_object(Bucket=bucket, Key=key, **_IMMUTABLE_HEADERS, **body, ContentLength=size,
                                        IfNoneMatch="*")
        except ClientError as exc:
            if not _precondition(exc):
                raise
            return False
        # ``body`` holds the one buffer that was read, hashed and sent; its Content-MD5 is that buffer's MD5.
        return isinstance(created, Mapping) and created.get("ETag") == f'"{base64.b64decode(body["ContentMD5"]).hex()}"'

    def upload_parts(upload_id: str) -> list[dict]:
        parts = []
        with open_body() as stream:
            observed = 0
            while chunk := stream.read(PART_BYTES):
                observed += len(chunk)
                if observed > size:
                    raise PublicationError(f"Bytes sent for {key} exceed their declared size")
                part = len(parts) + 1
                body = checked(chunk)
                result = client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=part, **body)
                parts.append({"PartNumber": part, "ETag": result["ETag"]})
        if observed != size:
            raise PublicationError(f"Bytes sent for {key} differ from their declared size")
        assert_content_address()
        return parts

    _create_multipart(client, bucket, key, upload_parts)
    return False


def verify_stored_member(client, bucket: str, member: Member, open_local: Callable[[], ContextManager[BinaryIO]]
                         ) -> str | None:
    """Hold one stored member to its pin by a hash storage computed itself: one HEAD, no body read.

    Returns the hash that matched: ``"sha256"``, the SHA-256 checksum the uploader sent and storage checked, or
    ``"md5"``, a single-part or supported-layout multipart ETag storage computed. ``None`` when neither is comparable
    (no checksum or ETag, or a composite hash over an unknown or different part layout):
    the caller then streams the bytes (:func:`verify_member`). A missing object, another size, a full-object checksum
    or single-part ETag that differs, or a local candidate that no longer matches its pin raises.

    Composite checksums and ETags are compared with one local pass over ``open_local``, the admitted candidate's
    bytes. Storage computes these hashes when it accepts the bytes, so they cover transfer and the object the key
    names, not later decay at rest.
    """
    if member.sha256 is None or member.byte_size is None:
        raise ValueError(f"Read-back needs a pinned member: {member.path}")
    head = _head(client, bucket, member.path, ChecksumMode="ENABLED")
    if head is None:
        raise RuntimeError(f"Published generation member is missing: {member.path}")
    differs = RuntimeError(f"Published generation member differs from its pin: {member.path}")
    if head["ContentLength"] != member.byte_size:
        raise differs
    checksum, etag = head.get("ChecksumSHA256"), str(head.get("ETag") or "").strip('"')
    # The uploader puts an object of at most PART_BYTES whole, so its checksum is the SHA-256 the pin names.
    if checksum and "-" not in checksum and member.byte_size <= PART_BYTES:
        if checksum != base64.b64encode(bytes.fromhex(member.sha256.removeprefix("sha256:"))).decode():
            raise differs
        return "sha256"
    if not checksum and "-" in etag:
        count = (member.byte_size + PART_BYTES - 1) // PART_BYTES
        # The maintained uploader uses this width and at most 10,000 parts.
        # Another count/shape cannot establish that layout and must stream.
        if not 1 < count <= 10_000 or re.fullmatch(rf"[0-9a-f]{{32}}-{count}", etag) is None:
            return None
        with open_local() as stream:
            identity = _stream_identity(stream, part_bytes=PART_BYTES)
        if identity["sha256"] != member.sha256 or identity["bytes"] != member.byte_size:
            raise RuntimeError(f"Local candidate differs from its pin: {member.path}")
        return "md5" if identity["etag"].strip('"') == etag else None
    if not checksum and not etag:
        return None
    digest, whole_md5, parts = hashlib.sha256(), hashlib.md5(usedforsecurity=False), []
    with open_local() as stream:
        while chunk := stream.read(PART_BYTES):
            checkpoint()
            digest.update(chunk)
            if checksum:
                parts.append(hashlib.sha256(chunk).digest())
            else:
                whole_md5.update(chunk)
    if "sha256:" + digest.hexdigest() != member.sha256:
        raise RuntimeError(f"Local candidate differs from its pin: {member.path}")
    if checksum:  # composite over PART_BYTES parts; storage may omit the part-count suffix
        composite = base64.b64encode(hashlib.sha256(b"".join(parts)).digest()).decode()
        return "sha256" if checksum in (composite, f"{composite}-{len(parts)}") else None
    if etag != whole_md5.hexdigest():
        raise differs
    return "md5"


class _S3Members:
    """Rulespec storage adapter: list exact membership and stream actual bytes."""

    def __init__(self, client, bucket: str, prefix: str):
        self.client, self.bucket, self.prefix = client, bucket, prefix + "/"

    def keys(self):
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in pages:
            for entry in page.get("Contents", []):
                yield entry["Key"][len(self.prefix) :]

    @contextmanager
    def open(self, object_key: str) -> Iterator[BinaryIO]:
        from botocore.exceptions import ClientError
        from rulespec_artifacts import MemberNotFoundError

        try:
            result = self.client.get_object(Bucket=self.bucket, Key=self.prefix + object_key)
        except ClientError as exc:
            if _missing(exc):
                raise MemberNotFoundError(object_key) from exc
            raise
        body = result["Body"]
        try:
            yield body
        finally:
            body.close()


class _EvidenceMembers:
    """Admission source for one evidence artifact whose blobs live in the shared prefix.

    Metadata members come from ``source-evidence/<digest>/``. Each
    ``blobs/sha256/<hex>`` member comes from the locally admitted copy when
    stored bytes were already shown equal to it (``trusted``, see
    :func:`publish_evidence`), and otherwise directly from the shared remote prefix.
    """

    def __init__(self, client, bucket: str, prefix: str, local, blobs: set[str], trusted: set[str]):
        self.artifact = _S3Members(client, bucket, prefix)
        self.shared = _S3Members(client, bucket, EVIDENCE_PREFIX)
        self.local, self.blobs, self.trusted = local, blobs, trusted

    def keys(self):
        yield from sorted({*self.artifact.keys(), *self.blobs})

    @contextmanager
    def open(self, object_key: str) -> Iterator[BinaryIO]:
        source = (self.local if object_key in self.trusted
                  else self.shared if object_key in self.blobs else self.artifact)
        with source.open(object_key) as stream:
            yield stream


def publish_evidence(client, bucket: str, path: Path, artifact, *, archive_root: Path | None = None) -> None:
    """Upload one locally admitted evidence artifact, reading every blob it creates back once before admission.

    Blob members are content-addressed, so each is stored once at
    ``source-evidence/blobs/sha256/<hex>`` for every artifact that cites it.
    Each blob is handled on one of ``EVIDENCE_WORKERS`` workers: created if
    absent, then read back at once and held to its size and SHA-256. Only an
    existing blob whose stored size and ETag equal the local bytes is not
    read. Admission then hashes the local copy of each blob shown equal and
    streams any other from storage, which refuses the artifact. Small
    metadata stays under the artifact's prefix. ``archive_root`` names the
    evidence's own raw archive when it is not ``SPICYGOV_RAW_ROOT``.

    The rule, what each check can and cannot see, and its cost are stated
    once, in docs/source-evidence.md ("Stored blobs").
    """
    from collections import Counter
    from concurrent.futures import ThreadPoolExecutor
    from rulespec_artifacts import MemberNotFoundError, admit_artifact, iter_member_descriptors
    from spicy_regs.source_evidence import EvidenceMemberSource

    local = EvidenceMemberSource(path, archive_root=archive_root)
    shared = _S3Members(client, bucket, EVIDENCE_PREFIX)
    prefix = f"{EVIDENCE_PREFIX}/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    declared = {member.object_key: member for member in iter_member_descriptors(artifact, local)}
    blobs, metadata = [], []
    for key in sorted(local.keys()):
        match, member = _BLOB_KEY.fullmatch(key), declared.get(key)
        if match is None or member is None:
            metadata.append(key)
            continue
        if member.sha256 != "sha256:" + match[1]:
            raise PublicationError(f"Evidence blob {key} is not addressed by its content")
        blobs.append(key)

    def stored_bytes_match(key: str) -> bool:
        """Read the stored blob once, in bounded chunks; ``True`` when it has the member's size and SHA-256."""
        member, digest, size = declared[key], hashlib.sha256(), 0
        try:
            with shared.open(key) as stream:
                while size <= member.byte_size and (chunk := stream.read(_READ_BACK_CHUNK)):
                    checkpoint()
                    digest.update(chunk)
                    size += len(chunk)
        except MemberNotFoundError:
            return False
        return (size, "sha256:" + digest.hexdigest()) == (member.byte_size, member.sha256)

    def store(key: str) -> str | None:
        """Make storage hold the blob; how its stored bytes were shown equal to the local ones, else ``None``."""
        remote = f"{EVIDENCE_PREFIX}/{key}"
        stored = _head(client, bucket, remote)
        how = "read back"
        if stored is None:
            # Recorded for the log, never trusted: a created blob is read back whatever storage answered.
            if _put_immutable_stream(client, bucket, remote, lambda: local.open(key), declared[key].byte_size,
                                     sha256=declared[key].sha256):
                how = "created with the sent MD5 as ETag and read back"
        else:
            with local.open(key) as stream:
                identity = _stream_identity(stream)
            if (stored["ContentLength"], stored.get("ETag")) == (identity["bytes"], identity["etag"]):
                return "already stored"
        if stored_bytes_match(key):
            return how
        logger.warning("Stored evidence blob {} is absent or differs from its content address; admission will "
                       "refuse it", key)
        return None

    with ThreadPoolExecutor(max_workers=EVIDENCE_WORKERS) as pool:
        shown = dict(zip(blobs, pool.map(store, blobs), strict=True))
    if blobs:
        logger.info("Evidence blobs: {}", ", ".join(
            f"{count:,} {how or 'refused'}" for how, count in Counter(shown.values()).most_common()))
    for key in metadata:
        _put_immutable(client, bucket, f"{prefix}/{key}", path / key)
    admit_artifact(
        _EvidenceMembers(client, bucket, prefix, local, set(blobs), {key for key, how in shown.items() if how}),
        expected_pin=artifact.pin,
    )


def publish_evidence_index(client, bucket: str, family: str, path: Path) -> str:
    """Create an append-only, content-addressed index of retained originals beside the shared blobs.

    ``source-evidence/indexes/<family>/<sha256>.parquet``: an existing object
    under that key already holds these bytes, so a rerun writes nothing new.
    """
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", family):
        raise PublicationError(f"Invalid evidence index family {family!r}")
    identity = file_identity(path)
    key = f"{EVIDENCE_PREFIX}/indexes/{family}/{identity['sha256'].removeprefix('sha256:')}.parquet"
    _put_immutable(client, bucket, key, path, sha256=identity["sha256"])
    stored = _head(client, bucket, key)
    if stored is None or (stored["ContentLength"], stored.get("ETag")) != (identity["bytes"], identity["etag"]):
        raise PublicationError(f"Evidence index {key} differs from its local bytes")
    return key


def publish_generation(directory: Path, *, client, bucket: str, prior_index: Mapping,
                       evidence_directories: tuple[Path, ...] = (), added_tables: frozenset[str] = frozenset(),
                       receipt_only_tables: frozenset[str] = frozenset(), exact_prior: bool = False,
                       conversion_proof: NativeConversionProof | None = None, before_pointer=None) -> dict:
    """Verify/upload/verify, then compare-and-swap the publication pointer.

    Validation and conditional-write refusals preserve the current pointer.
    A transport failure after the pointer request can have an uncertain result;
    reread the index to establish whether the complete generation is current.
    ``added_tables`` is an explicit migration: the family's table set may grow by
    exactly those tables. ``receipt_only_tables`` explicitly moves processing
    tables into the shared receipt member; each must have a receipt-only policy.
    A published table stops having receipts only once its installed policy is marked
    ``subject_catalog.RECEIPTS_RETIRED``: it keeps its table and is never claimed by a receipt member again, on this
    and every other publishing path (:func:`_stage_verified_generation`).
    Prior immutable bytes remain retained. Unreferenced immutable uploads may
    remain and are safe to reuse. A pointer
    moved by another family's writer is reread and this family's entry merged
    onto it, which equals a first attempt made a moment later; a change to this
    family or its tables refuses as stale and never overwrites the other writer.
    Managed parents must also remain current on every pointer attempt.
    Input provenance and semantic quality are separate checks.
    ``exact_prior`` also holds the captured publication timestamp unchanged on every pointer retry.
    ``conversion_proof`` replaces the byte-size heuristic only for original tables
    completely restored by this native-conversion invocation; it also requires the exact prior on every retry.
    ``before_pointer`` runs after immutable staging and before the timestamp on each pointer attempt. A callback
    exception preserves the current pointer. Callers that pause for an observer should make their gate one-shot.
    """
    from rulespec_artifacts import LocalMemberSource
    from spicy_regs.generations import admit_generation_for_publication

    artifact = admit_generation_for_publication(directory)
    return _publish_verified_generation(
        artifact, LocalMemberSource(directory), client=client, bucket=bucket, prior_index=prior_index,
        evidence_directories=evidence_directories, added_tables=added_tables, receipt_only_tables=receipt_only_tables,
        exact_prior=exact_prior, conversion_proof=conversion_proof, before_pointer=before_pointer, local=directory,
        upload_member=lambda prefix, key: _put_immutable(client, bucket, prefix + "/" + key, directory / key),
    )


#: Opt-in local mirror root (the operator's ``corpora/spicyregs/output/publication``); unset in CI.
PUBLICATION_MIRROR = "SPICYGOV_PUBLICATION_MIRROR"
_ROOT_FILES = ("artifact.json", "members.json")
SNAPSHOT_DIRECTORY = "publication.v2"  # content-addressed copies of the pointer under a mirror root


def generation_files(entry: Mapping) -> dict[str, Member]:
    """Every file under a published family entry's generation prefix, by key.

    The tables' members and the receipt member carry their pins; the artifact root and member list
    are unpinned here (``artifactDigest`` pins the root). This is the key set ``_stage_verified_generation`` uploads.
    """
    prefix = entry["prefix"]
    files = {name: Member(f"{prefix}/{name}", None, None, None) for name in _ROOT_FILES}
    described = [m for key, table in entry["tables"].items() for m in table.get("members", [{"key": key, **table}])]
    if receipt := entry.get("etlReceipts"):
        described.append(receipt)
    for m in described:
        files[m["key"]] = Member(f"{prefix}/{m['key']}", m["sha256"], m["byteSize"], m.get("rows"))
    return files


def mirror_generation(root: Path, entry: Mapping, *, local: Path | None = None,
                      base_url: str | None = None) -> dict[str, int]:
    """Place one published generation under ``root`` in the R2 key layout, ``<root>/<prefix>/<key>``; return counts.

    A file already present at its pinned size is kept without rehashing: the prefix is content-addressed and this
    function, like the sync it replaces, only ever renames a checked file into place. Otherwise a key held in
    ``local`` is cloned (copy-on-write ``cp -c`` on APFS, a byte copy elsewhere; never a hardlink, so a later edit of
    the build directory cannot reach the mirror), and any other key is fetched from ``base_url`` by ``fetch_member``,
    which checks it against its pin. With neither, the key is counted ``missing``. The artifact root is then checked
    against the entry's ``artifactDigest``.
    """
    counts = {"kept": 0, "cloned": 0, "fetched": 0, "missing": 0}
    for key, member in sorted(generation_files(entry).items()):
        target = root / member.path
        if _mirrored(target, member):
            counts["kept"] += 1
        elif local is not None and (local / key).is_file():
            _clone_into(local / key, target, member.byte_size)
            counts["cloned"] += 1
        elif base_url is not None:
            target.parent.mkdir(parents=True, exist_ok=True)
            if fetch_member(base_url, member, target) is False:
                raise PublicationError(f"{member.path} is absent from {base_url}")
            counts["fetched"] += 1
        else:
            counts["missing"] += 1
    if (held := root / entry["prefix"] / "artifact.json").is_file():
        family_root(held.read_bytes(), entry)
    return counts


def _mirrored(target: Path, member: Member) -> bool:
    """``target`` holds ``member`` as far as the mirror checks: present, at its pinned size when it has one."""
    return target.is_file() and (member.byte_size is None or target.stat().st_size == member.byte_size)


def mirror_absent_bytes(root: Path, entry: Mapping) -> int:
    """The pinned bytes ``mirror_generation`` would still have to write under ``root`` for ``entry``."""
    return sum(member.byte_size or 0 for member in generation_files(entry).values()
               if not _mirrored(root / member.path, member))


def prune_superseded(root: Path, entry: Mapping) -> list[Path]:
    """Delete every other generation of ``entry``'s family under ``root``; return what was removed.

    The mirror's retention rule: it holds the selected generation per family and nothing older. R2 keeps every
    published generation (``restore_family`` rolls back there), so a superseded local copy is only disk.
    """
    import shutil

    selected = root / entry["prefix"]
    removed = [path for path in sorted(selected.parent.iterdir())
               if path != selected and path.is_dir() and re.fullmatch(r"[0-9a-f]{64}", path.name)]
    for path in removed:
        shutil.rmtree(path)
    return removed


def mirror_index(root: Path, raw: bytes) -> Path:
    """Keep the publication pointer ``raw`` that selects the mirrored generations as a content-addressed snapshot.

    The snapshot is ``<root>/publication.v2/<sha256 of raw>.json``, the exact bytes stored at ``INDEX_V2_KEY``, so a
    mirrored generation can be traced to the selection that named it. Written once (an existing name already holds
    these bytes) and renamed into place; ``<root>/publication.v2.json`` itself is never rewritten here, because a
    mirror can lag the live pointer.
    """
    parse_index(raw)
    target = root / SNAPSHOT_DIRECTORY / f"{hashlib.sha256(raw).hexdigest()}.json"
    if not target.is_file():
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_bytes(raw)
        temporary.replace(target)
    return target


class PublishedIndex(dict):
    """The index a publish wrote; ``mirror`` reports the opt-in local mirror (``None`` when it is not configured).

    A ``dict`` so every caller that reads, copies or serializes the index is unchanged; the report is an attribute,
    so it never enters a serialized index.
    """

    mirror: dict | None = None
    catalog_refresh_request: dict | None = None


def _published(index: dict, raw: bytes, family: str, local: Path | None) -> PublishedIndex:
    from spicy_regs.catalog_refresh_request import notify

    result = PublishedIndex(index)
    result.mirror = _mirror_published(index, raw, family, local)
    result.catalog_refresh_request = notify("publication-complete", {
        "family":family,"publicationSha256":"sha256:"+hashlib.sha256(raw).hexdigest(),
        "artifactDigest":index["families"][family]["artifactDigest"],
    })
    return result


def _mirror_published(index: Mapping, raw: bytes, family: str, local: Path | None) -> dict | None:
    """After the pointer moved, mirror ``family``'s generation and the pointer when ``PUBLICATION_MIRROR`` is set.

    Never raises: the publish has already succeeded, so a failure is logged and returned as ``error`` (the caller's
    exit status is unchanged) and left to ``spicy-regs mirror``, which also brings in generations published
    elsewhere (CI) and keys a remote promotion never held locally.
    """
    if not (root := os.environ.get(PUBLICATION_MIRROR)):
        return None
    target = Path(root).expanduser().resolve()
    entry = index["families"][family]
    outcome: dict = {"root": str(target), "prefix": entry["prefix"]}
    try:
        outcome |= mirror_generation(target, entry, local=local)
        outcome["snapshot"] = str(mirror_index(target, raw).relative_to(target))
    except Exception as error:
        logger.warning("publication: {} is published but its local mirror failed: {!r}", entry["prefix"], error)
        return outcome | {"error": f"{type(error).__name__}: {error}"}
    logger.info("publication: mirrored {} under {}: {}", entry["prefix"], target, outcome)
    if outcome["missing"]:
        logger.warning("publication: {} file(s) of {} are not held locally; run `spicy-regs mirror {} --family {}`",
                       outcome["missing"], entry["prefix"], target, family)
    return outcome


def _clone_into(source: Path, target: Path, size: int | None) -> None:
    import shutil
    import subprocess
    import sys

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        if sys.platform != "darwin" or subprocess.run(["cp", "-c", str(source), str(temporary)],
                                                      capture_output=True).returncode != 0:
            shutil.copyfile(source, temporary)
        if temporary.stat().st_size != (source.stat().st_size if size is None else size):
            raise PublicationError(f"Mirror copy of {source.name} is incomplete")
        temporary.replace(target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


#: Byte floor for a table that kept its rows and columns, as a fraction of its prior. Measured on fec_committees:
#: a writer's layout change alone left 0.392 (0.295 had it been one row group); within one layout, every value
#: but the key nulled leaves 0.163, and every string but the key emptied leaves 0.371, above this floor.
_KEPT_ROWS_MIN_RATIO = 0.2


def _generation_scoped_tables() -> frozenset[str]:
    """Tables that describe only the generation holding them: its producer members, their footers and conversions.

    Their size follows the writer that produced the members, not the amount of data: DuckDB's merge writes no
    ``ARROW:schema`` footer, a pyarrow writer writes one the size of the schema's field metadata (49,720 bytes for
    ``document_citations``), and the recorded footers halved this table on 2026-10-14 (ratio 0.461) with every
    row and checkpoint intact. A byte-size guard against the previous generation cannot see data loss in them.
    """
    from spicy_regs.navigation_read_outcomes import FILE_TABLES

    return frozenset(name + ".parquet" for name in FILE_TABLES.values())


def _require_receipt_archive_pins(family: str, live: str, datasets) -> None:
    """Refuse the first generation without a retired table's receipts until the docs pin the one they live in.

    That is the family's current generation, the last with receipts: weekly retention deletes it within days unless
    docs/pins/receipt-archive.json cites it (generation_retention). The file is read through retention's own reader
    from the checkout the publishing job runs in; an install without docs/ cannot publish a retirement.
    """
    from spicy_regs import generation_retention

    if not generation_retention.RECEIPT_ARCHIVE.is_file():
        raise PublicationError(f"Cannot check the receipt archive pins: {generation_retention.RECEIPT_ARCHIVE} is not "
                               "readable here; publish a retirement from a spicy-regs checkout")
    pins = generation_retention.receipt_archive_pins()
    for name in sorted(datasets):
        pin = pins.get(name, {})
        if pin.get("artifactDigest") != live or pin.get("family") != family:
            raise PublicationError(
                f"{name} leaves its receipts in {family} generation {live}, but docs/pins/receipt-archive.json pins "
                f"{pin.get('family')} {pin.get('artifactDigest')}: pin the live generation before this publish")


def _stage_verified_generation(
    artifact, source, *, upload_member, client, bucket: str, index: dict,
    evidence_directories: tuple[Path, ...] = (), added_tables: frozenset[str] = frozenset(),
    receipt_only_tables: frozenset[str] = frozenset(),
    conversion_proof: NativeConversionProof | None = None, members=None,
    _verified_replacement_tables: frozenset[str] = frozenset(),
) -> dict:
    """Check and upload one verified generation without making it visible."""
    from rulespec_artifacts import admit_artifact, iter_member_descriptors
    from spicy_regs.sources.r2 import _assert_upload_safe, _get_remote_size
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE, verify_evidence

    if artifact.root["spec"]["publicationStatus"] != "complete-family":
        raise PublicationError("A local partial candidate cannot be published")
    from spicy_regs.etl_policy_registry import require_registered_receipts
    require_registered_receipts(artifact.root["spec"]["tables"], artifact.root["spec"].get("etlReceipts"))
    family = artifact.root["spec"]["family"]
    if not _NAME.fullmatch(family):
        raise PublicationError("Invalid family name")
    converted = _conversion_tables(conversion_proof, artifact, index["families"].get(family))
    members = list(iter_member_descriptors(artifact, source)) if members is None else list(members)
    _assert_parents_current(index, artifact.root["spec"].get("parents", {}))
    entry = generation_entry(artifact, members)
    prefix, tables = entry["prefix"], entry["tables"]
    exempt = converted | _verified_replacement_tables | _generation_scoped_tables()
    # The guards run per table: a partition of a split table may legitimately shrink, but none may vanish,
    # since a build that forgot a partition would otherwise pass whenever the rest outweighs the shrink ratio.
    # Bytes follow the writer's layout as well as the data, so a table whose index shows every partition kept its
    # rows and the table its columns faces only the lower floor (:func:`_keeps_rows_and_columns`, and its limits).
    for key, table in tables.items():
        prior = table_descriptor(index, key)
        if prior and (missing := _partitions(prior) - _partitions(table)):
            raise PublicationError(f"{key} lacks partitions its prior generation holds: " + ", ".join(sorted(
                "/".join(f"{c}={dict(p)[c]}" for c in prior["partitionColumns"]) for p in missing)))
        if key not in exempt:
            old_size = prior["byteSize"] if prior else _get_remote_size(client, bucket, key)
            kept = prior is not None and _keeps_rows_and_columns(table, prior)
            _assert_upload_safe(table["byteSize"], old_size, key, floor=_KEPT_ROWS_MIN_RATIO if kept else None)
    old_family = index["families"].get(family)
    # The table set may change only by an explicit migration: exactly the declared added tables join, none leave.
    # A union, so the declaration is inert once those tables are published.
    allowed_receipt_only = {p["dataset"] + ".parquet" for p in
                            artifact.root["spec"].get("etlReceipts", {}).get("policies", []) if p["receipt_only"]}
    if not set(receipt_only_tables) <= allowed_receipt_only or set(receipt_only_tables) & set(tables):
        raise PublicationError("Only explicitly classified processing tables may move into receipts")
    expected_tables = (set((old_family or {}).get("tables", {})) | set(added_tables)) - set(receipt_only_tables)
    if old_family is not None and set(tables) != expected_tables:
        raise PublicationError("Family membership changed; explicit migration is required")
    from spicy_regs.subject_catalog import retired_receipt_datasets, shared_receipt_logs
    # ``datasets`` is the family's claim: every index reader gives each listed name one owning family. A shared log
    # is in the receipt member and in the generation's stated policies, and is claimed by no family.
    shared = shared_receipt_logs()
    claimed = set(entry.get("etlReceipts", {}).get("datasets", []))
    # A retirement is declared once, on the table's installed policy (subject_catalog.RECEIPTS_RETIRED), never
    # inferred, and read here whatever path publishes (ss-ag1j): the table stays published, its evidence is the run's
    # journal and retained originals, and no generation may claim receipts for it again. The prior generation's
    # receipt member is not touched; retention keeps it as long as docs/pins/receipt-archive.json cites it.
    retired = retired_receipt_datasets()
    if retired & claimed:
        raise PublicationError("A retired receipt dataset publishes no receipts")
    # An entry written before logs were shared may still list one; ceasing to list it drops no ownership.
    dropped = set((old_family or {}).get("etlReceipts", {}).get("datasets", [])) - claimed - shared
    if not {name + ".parquet" for name in dropped & retired} <= set(tables):
        raise PublicationError("A retired receipt dataset keeps its table and publishes no receipts")
    if dropped & retired and old_family is not None:
        _require_receipt_archive_pins(family, old_family["artifactDigest"], dropped & retired)
    # The member itself may go only when every dataset it owned is retired: one holding shared logs alone stays.
    if (old_family and "etlReceipts" in old_family and "etlReceipts" not in entry
            and not (dropped and dropped <= retired)):
        raise PublicationError("Cannot publish a generation that drops required ETL receipts")
    if dropped - retired:
        raise PublicationError("Cannot publish a generation that drops receipt dataset ownership")
    _merge_family(index, family, entry)
    evidence = [verify_evidence(path) for path in evidence_directories]
    declared = [item for item in artifact.root["inputs"] if item["role"] == INPUT_ROLE]
    actual = [{"role": INPUT_ROLE, **item.pin.as_dict()} for item in evidence]
    if actual != declared:
        raise PublicationError("Generation source evidence is missing or differs from its input pins")
    for path, item in zip(evidence_directories, evidence, strict=True):
        if item.root["spec"]["family"] != family:
            raise PublicationError("Source evidence belongs to a different family")
        if item.root["inputs"] != [value for value in artifact.root["inputs"] if value["role"] == PRIOR_ROLE]:
            raise PublicationError("Source evidence and generation name different inherited inputs")
        publish_evidence(client, bucket, path, item)
    # Members whose bytes the prior generation already holds under the same
    # digest are copied server-side instead of re-uploaded. The copy can never
    # publish wrong bytes: ``admit_artifact`` below re-verifies every member of
    # the new prefix against the artifact pin before the pointer moves.
    # A split table's members are compared one by one, so an untouched partition is copied, never re-uploaded.
    prior_members = {
        member["key"]: (member["sha256"], member["byteSize"])
        for key, table in (old_family or {}).get("tables", {}).items()
        for member in table.get("members", [{"key": key, **table}])
    }
    prior_prefix = old_family.get("prefix") if old_family is not None else None
    reused = 0
    current = {member.object_key: (member.sha256, member.byte_size) for member in members}
    for key in sorted(source.keys()):
        if prior_prefix is not None and key in current and prior_members.get(key) == current[key]:
            _copy_unchanged_member(client, bucket, f"{prior_prefix}/{key}", f"{prefix}/{key}")
            reused += 1
        else:
            upload_member(prefix, key)
    if reused:
        logger.info("publication: reused {} unchanged member(s) across generation prefixes", reused)
    # S3 success or caller-supplied metadata is not byte-verification evidence.
    admit_artifact(_S3Members(client, bucket, prefix), expected_pin=artifact.pin)
    # Coverage metadata has its own prefix: table artifacts remain Parquet-only.
    # Save and verify it before advancing the table pointer, including callers
    # that publish native retained generations without RollupPipeline.
    from io import BytesIO
    from rulespec_artifacts import canonical_json_bytes
    from spicy_regs.source_catalog.input_proof import input_proof
    from spicy_regs.source_evidence import EvidenceMemberSource

    proof_prefix = f"generation-inputs/{family}/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    receipt, controls = input_proof(artifact, source, tuple(
        (EvidenceMemberSource(path), item) for path, item in zip(evidence_directories, evidence, strict=True)
    ), members=members)
    for name, raw in sorted({**controls, "inputs.json": canonical_json_bytes(receipt)}.items()):
        if isinstance(raw, tuple):
            owner, member, expected_sha, size = raw
            def open_body(owner=owner, member=member):
                return owner.open(member)
        else:
            expected_sha, size = "sha256:" + hashlib.sha256(raw).hexdigest(), len(raw)
            def open_body(raw=raw):
                return BytesIO(raw)
        _put_immutable_stream(client, bucket, proof_prefix + "/" + name, open_body, size, sha256=expected_sha)
        with _S3Members(client, bucket, proof_prefix).open(name) as body:
            if _stream_identity(body)["sha256"] != expected_sha:
                raise PublicationError("Stored generation input proof differs from its exact inputs")
    return entry


def _publish_verified_generation(
    artifact, source, *, upload_member, client, bucket: str, prior_index: Mapping,
    evidence_directories: tuple[Path, ...] = (), added_tables: frozenset[str] = frozenset(),
    receipt_only_tables: frozenset[str] = frozenset(), exact_prior: bool = False,
    conversion_proof: NativeConversionProof | None = None,
    before_pointer=None, local: Path | None = None,
) -> dict:
    """Shared publication gates; both callers fully verify their source first.

    ``local`` is the directory ``upload_member`` reads, for the opt-in local mirror once the pointer moves; the
    returned ``PublishedIndex.mirror`` reports that mirror's outcome.
    """
    family = artifact.root["spec"]["family"]
    index, etag = _stored_index(client, bucket)
    exact_prior = exact_prior or conversion_proof is not None
    _assert_family_unchanged(index, prior_index, family, exact=exact_prior)
    entry = _stage_verified_generation(
        artifact, source, upload_member=upload_member, client=client, bucket=bucket, index=index,
        evidence_directories=evidence_directories, added_tables=added_tables,
        receipt_only_tables=receipt_only_tables, conversion_proof=conversion_proof,
    )
    # Each lost race means another writer's pointer write succeeded, so
    # contention resolves in at most one round per concurrent writer.
    for _ in range(_POINTER_ATTEMPTS):
        _assert_parents_current(index, artifact.root["spec"].get("parents", {}))
        if before_pointer is not None:
            before_pointer()
        # Stamped per attempt, so it is the instant this write moves the pointer. Replaying the generation already
        # current moves nothing, so its entry is kept as stored, with or without an instant.
        current = index["families"].get(family)
        moved = current if _generation_entry(current) == entry else {**entry, "publishedAt": _instant()}
        updated, raw = _merge_family(index, family, moved)
        if _put_pointer(client, bucket, INDEX_V2_KEY, raw, etag):
            return _published(updated, raw, family, local)
        logger.info("publication: pointer moved concurrently; merging {} onto the reread index", family)
        index, etag = _stored_index(client, bucket)
        _assert_family_unchanged(index, prior_index, family, exact=exact_prior)
    raise PublicationError("Publication changed concurrently; retry from a fresh snapshot")


def fec_compatibility_key(observations_digest: str, query_digest: str) -> str:
    """Immutable public descriptor location for one exact FEC selection."""
    if not all(isinstance(value, str) and _DIGEST.fullmatch(value)
               for value in (observations_digest, query_digest)):
        raise PublicationError("FEC descriptor requires exact artifact digests")
    return (f"fec-compatibility/v1/{observations_digest.removeprefix('sha256:')}/"
            f"{query_digest.removeprefix('sha256:')}.json")


def _publish_fec_descriptor(compatibility, *, observations_digest, query_digest,
                            client, bucket: str, public_base_url: str) -> None:
    """Deliver the checked descriptor and verify both storage and public bytes."""
    path, digest = Path(compatibility.descriptor_path), compatibility.descriptor_sha256
    if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
        raise PublicationError("FEC descriptor requires its checked SHA-256")
    if path.stat().st_size > FEC_DESCRIPTOR_LIMIT:
        raise PublicationError("FEC descriptor exceeds its byte limit")
    key = fec_compatibility_key(observations_digest, query_digest)
    _put_immutable(client, bucket, key, path, sha256=digest)
    response = client.get_object(Bucket=bucket, Key=key)
    try:
        raw = response["Body"].read(FEC_DESCRIPTOR_LIMIT + 1)
    finally:
        response["Body"].close()
    if len(raw) > FEC_DESCRIPTOR_LIMIT or "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise PublicationError("Stored FEC descriptor differs from its checked bytes")
    public = _bounded_get(public_base_url.rstrip("/") + "/" + key,
                          limit=FEC_DESCRIPTOR_LIMIT, allow_missing=False)
    if public != raw:
        raise PublicationError("Public FEC descriptor differs from its checked bytes")


def publish_fec_release(
    directory: Path, *, observations_basis: dict, query_basis: dict,
    client, bucket: str, prior_index: Mapping, public_base_url: str,
    evidence_directories: tuple[Path, ...] = (), receipt_only_tables: frozenset[str] = frozenset(),
) -> dict:
    """Select proved observations and the exact compatible historical query together.

    The retained query keeps its production lineage and publication timestamp.
    Maintained verifiers bind both selections and the serving compatibility
    evidence. Only observations are staged; no retained query bodies are copied.
    The composed observations proof replaces the size heuristic for its prior
    tables, while membership, receipts, evidence and transport checks still run.
    """
    from rulespec_artifacts import LocalMemberSource
    from spicy_regs.fec_release_verification import verify_publication_basis
    from spicy_regs.fec_query_compatibility import verify_retained_query_compatibility

    observations = verify_publication_basis(directory, observations_basis)
    if observations.root["spec"]["family"] != "fec-observations":
        raise PublicationError("FEC release proof must name fec-observations")
    old = prior_index["families"].get("fec-observations")
    query = prior_index["families"].get("fec-query")
    if old is None or query is None:
        raise PublicationError("Retained FEC release requires both captured families")
    if observations.proof["priorPin"] != {key: old[key] for key in ("logicalId", "artifactDigest")}:
        raise PublicationError("FEC observations proof names another prior generation")
    compatibility = verify_retained_query_compatibility(observations, query, query_basis)
    if compatibility.entry != query or compatibility.observations_pin != observations.pin:
        raise PublicationError("FEC compatibility proof names another selection")

    def check():
        observations.check()
        compatibility.check()

    index, etag = _stored_index(client, bucket)
    for family in ("fec-observations", "fec-query"):
        _assert_family_unchanged(index, prior_index, family, exact=True)
    check()
    _publish_fec_descriptor(
        compatibility, observations_digest=observations.pin.artifact_digest,
        query_digest=query["artifactDigest"], client=client, bucket=bucket, public_base_url=public_base_url,
    )
    entry = _stage_verified_generation(
        observations, LocalMemberSource(directory), client=client, bucket=bucket, index=index,
        members=observations.members, evidence_directories=evidence_directories,
        receipt_only_tables=receipt_only_tables,
        _verified_replacement_tables=frozenset(old["tables"]),
        upload_member=lambda prefix, key: _put_immutable(client, bucket, prefix + "/" + key, directory / key),
    )
    for _ in range(_POINTER_ATTEMPTS):
        for family in ("fec-observations", "fec-query"):
            _assert_family_unchanged(index, prior_index, family, exact=True)
        _assert_parents_current(index, observations.root["spec"].get("parents", {}))
        current = index["families"].get("fec-observations")
        moved = current if _generation_entry(current) == entry else {**entry, "publishedAt": _instant()}
        updated, _ = _merge_family(index, "fec-observations", moved)
        updated, raw = _merge_family(updated, "fec-query", compatibility.entry)
        check()
        if _put_pointer(client, bucket, INDEX_V2_KEY, raw, etag):
            return _published(updated, raw, "fec-observations", directory)
        index, etag = _stored_index(client, bucket)
    raise PublicationError("Publication changed concurrently; retry from a fresh snapshot")


def stored_family(client, bucket: str, family: str) -> dict | None:
    """``family``'s entry in the stored index, read with credentials: what the pointer names now, never a cached copy.

    A transport failure after the pointer request leaves the result uncertain (:func:`publish_generation`); this is
    the reread that settles it.
    """
    return _stored_index(client, bucket)[0]["families"].get(family)


def restore_family(client, bucket: str, family: str, entry: Mapping | None, *, expected: Mapping | None) -> dict:
    """Point ``family`` back at ``entry``, an earlier generation whose objects are still stored; return the index.

    A rollback is a pointer write, never a publish: :func:`publish_generation` refuses a generation that drops
    receipts. ``expected`` is the generation the family must still name (``None``: whatever it names now), so a
    generation another writer published since is not discarded unseen. ``entry`` is restored as it was captured,
    ``publishedAt`` included.

    ``entry=None`` restores prior absence. It requires the exact recorded ``expected`` entry, including its
    publication time, and removes only that family through the same conditional pointer write. Immutable objects
    remain stored. An already-absent family is an idempotent success when a recorded expected entry was supplied.
    """
    if entry is None:
        if expected is None:
            raise PublicationError("Restoring family absence requires the recorded expected release")
    else:
        prefix = entry["prefix"]
        stored = {f"{prefix}/artifact.json": None} | {
            f"{prefix}/{member['key']}": member["byteSize"]
            for key, table in entry["tables"].items() for member in table.get("members", [{"key": key, **table}])}
        for key, size in stored.items():
            head = _head(client, bucket, key)
            if head is None or (size is not None and head["ContentLength"] != size):
                raise PublicationError(f"Cannot restore {family}: {key} is no longer stored as published")
    for _ in range(_POINTER_ATTEMPTS):
        index, etag = _stored_index(client, bucket)
        if etag is None:
            raise PublicationError("No publication index holds this family")
        current = index["families"].get(family)
        if current == entry:
            return index
        matches = current == expected if entry is None else _generation_entry(current) == _generation_entry(expected)
        if expected is not None and not matches:
            raise PublicationError(f"{family} names another generation than the one being rolled back")
        if entry is None:
            from rulespec_artifacts import canonical_json_bytes

            updated = deepcopy(index)
            del updated["families"][family]
            raw = canonical_json_bytes(updated)
            parse_index(raw)
        else:
            updated, raw = _merge_family(index, family, dict(entry))
        if _put_pointer(client, bucket, INDEX_V2_KEY, raw, etag):
            return updated
    raise PublicationError("Publication changed concurrently; retry the rollback")


def _partition(member: Mapping) -> tuple[tuple[str, str], ...]:
    """A file's partition as sorted ``(column, value)`` pairs; ``()`` for a single file.

    Sorted because the two sides list the columns differently: the stored index is canonical JSON, which sorts
    them, and a new generation's entry lists them in directory order.
    """
    return tuple(sorted(member.get("partition", {}).items()))


def _partitions(table: Mapping) -> set[tuple[tuple[str, str], ...]]:
    """The partitions a split table's members hold, each as :func:`_partition` keys it; none for a single file."""
    return {_partition(member) for member in table.get("members", ())}


def _partition_rows(table: Mapping) -> dict[tuple[tuple[str, str], ...], int]:
    """Rows per partition, each as :func:`_partition` keys it, a single file being the one partition ``()``."""
    rows: dict[tuple[tuple[str, str], ...], int] = {}
    for member in table.get("members", (table,)):
        partition = _partition(member)
        rows[partition] = rows.get(partition, 0) + member["rows"]
    return rows


def _keeps_rows_and_columns(table: Mapping, prior: Mapping) -> bool:
    """Whether ``table`` records exactly ``prior``'s columns and at least its rows in every partition ``prior`` holds.

    Such a table may shrink to :data:`_KEPT_ROWS_MIN_RATIO` of its prior's bytes, not only to the ordinary ratio:
    its bytes can halve with nothing lost, as when ``fec_committees`` went from 1,767 row groups to 176 on
    2026-11-09 (10.2 MB to 4.0 MB, ratio 0.392, all 89,974 rows and 16 columns kept). A lost row or a changed
    column leaves the ordinary ratio in force, as does a single file rebuilt as a split table, which holds no
    partition its prior holds. These are counts, names and types: rows kept with their values emptied, or exchanged
    for as many others, look the same, and nothing at publication compares values with the prior generation. The
    floor refuses only a wipe that takes most of the bytes.
    """
    kept = _partition_rows(table)
    return (table["columns"] == prior["columns"]
            and all(kept.get(partition, -1) >= rows for partition, rows in _partition_rows(prior).items()))


def _instant(seconds: float | None = None) -> str:
    """A ``publishedAt`` value for ``seconds`` since the epoch, or for now: UTC, whole seconds, a trailing Z."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(seconds))


def _generation_entry(entry: Mapping | None) -> dict | None:
    """``entry`` without ``publishedAt``: the generation it names, not when the pointer moved to it."""
    return None if entry is None else {key: value for key, value in entry.items() if key != "publishedAt"}


def _assert_family_unchanged(index: Mapping, prior_index: Mapping, family: str, *, exact: bool = False) -> None:
    """Hold the captured generation, or the complete captured entry when ``exact`` includes ``publishedAt``."""
    before, current = prior_index["families"].get(family), index["families"].get(family)
    unchanged = current == before if exact else _generation_entry(current) == _generation_entry(before)
    if not unchanged:
        raise PublicationError("Family changed since the build read its inputs; rebuild before publishing")


def _assert_parents_current(index: Mapping, parents: Mapping) -> None:
    """Hold managed parents to the exact index used by the conditional write."""
    from spicy_regs.generations import _check_parents

    try:
        _check_parents(parents, index)
    except ValueError as exc:
        raise PublicationError("Managed parent changed since the build read its inputs; rebuild before publishing") from exc


def _merge_family(index: dict, family: str, entry: dict) -> tuple[dict, bytes]:
    """Update one family, preserving unique subject and receipt-only dataset ownership.

    Ownership is what an entry lists. A shared log (``subject_catalog.SHARED_LOG``) is never listed, so every other
    dataset, a retry checkpoint included, keeps exactly one family, by the same rule every index reader applies.
    """
    from rulespec_artifacts import canonical_json_bytes

    datasets = {key.removesuffix(".parquet") for key in entry["tables"]}
    datasets.update(entry.get("etlReceipts", {}).get("datasets", []))
    for owner, other in index["families"].items():
        owned = {key.removesuffix(".parquet") for key in other["tables"]}
        owned.update(other.get("etlReceipts", {}).get("datasets", []))
        if owner != family and (overlap := datasets & owned):
            raise PublicationError(f"{sorted(overlap)} already belongs to family {owner}")
    updated = deepcopy(index)
    updated["families"][family] = entry
    raw = canonical_json_bytes(updated)
    parse_index(raw)
    return updated, raw


#: When :func:`backfill_published_at` was applied to the live index (2026-10-03, round 5, its only run). Every
#: ``publishedAt`` at or before it was written by that backfill: the last object write under the generation's
#: prefix, with the pointer moved at or after it. Every later one was stamped at the pointer write. A writer-stamped
#: value at or before this instant would be the move itself, which "at or after" still describes. Another
#: backfill moves this instant.
PUBLISHED_AT_BACKFILLED_THROUGH = "2026-10-03T16:13:56Z"


def published_at_basis(instant: str | None) -> str | None:
    """What a ``publishedAt`` value observed: ``pointer_move``, or ``last_object_write`` for one the backfill wrote."""
    if instant is None:
        return None
    moved = datetime.fromisoformat(instant) > datetime.fromisoformat(PUBLISHED_AT_BACKFILLED_THROUGH)
    return "pointer_move" if moved else "last_object_write"


def backfill_published_at(client, bucket: str, *, apply: bool = False) -> dict:
    """Plan, or with ``apply`` write, ``publishedAt`` for each version-2 family entry that lacks it.

    The instant is the latest write under the generation's prefix. That prefix is create-only, and the pointer
    moves only after every member is uploaded and read back, so the instant is a lower bound on the move, short by
    the read-back. The root alone is a looser one: it is uploaded first, up to 915 s before the last member on
    2026-10-03. The write is conditional on the version-2 ETag read here, so a pointer that moved meanwhile refuses
    with nothing written, and a rerun plans only what is still missing.
    """
    from rulespec_artifacts import ROOT_OBJECT_KEY, canonical_json_bytes

    index, etag = _stored_index(client, bucket)
    if etag is None:
        raise PublicationError(f"{INDEX_V2_KEY} is absent, so there is no entry to stamp")
    stamps = {}
    for family, entry in sorted(index["families"].items()):
        if "publishedAt" in entry:
            continue
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=entry["prefix"] + "/")
        written = {item["Key"]: item["LastModified"] for page in pages for item in page.get("Contents", ())}
        if f"{entry['prefix']}/{ROOT_OBJECT_KEY}" not in written:
            raise PublicationError(f"{family}'s current generation has no root under {entry['prefix']}")
        stamps[family] = _instant(max(written.values()).timestamp())
    if not (stamps and apply):
        return {"index": INDEX_V2_KEY, "etag": etag, "stamps": stamps, "applied": False}
    updated = deepcopy(index)
    for family, instant in stamps.items():
        updated["families"][family]["publishedAt"] = instant
    raw = canonical_json_bytes(updated)
    parse_index(raw)
    if not _put_pointer(client, bucket, INDEX_V2_KEY, raw, etag):
        raise PublicationError(f"{INDEX_V2_KEY} moved since the backfill read it; nothing was written, run it again")
    return {"index": INDEX_V2_KEY, "etag": etag, "stamps": stamps, "applied": True}


def receipt_members(index: Mapping, *, dataset: str | None = None) -> tuple[Member, ...]:
    """Resolve the shared receipt table from the same captured index as subjects.

    Dataset-scoped reads require receipts in that subject's selected family;
    no bare-key or previous-generation fallback is permitted. Unscoped reads
    combine the receipt member of every selected migrated family.
    """
    entries = list(index["families"].values())
    if dataset is not None:
        from spicy_regs.subject_catalog import shared_receipt_logs
        if dataset in shared_receipt_logs():
            raise PublicationError(f"{dataset} is a shared log with no owning family: read its rows from the "
                                   "receipt member of one of the family's own datasets")
        owner = table_owner(index, dataset + ".parquet")
        if owner is not None:
            if dataset not in owner[1].get("etlReceipts", {}).get("datasets", []):
                raise PublicationError(f"Dataset has no generation-bound receipts: {dataset}")
            entries = [owner[1]]
        else:
            entries = [entry for entry in entries if dataset in entry.get("etlReceipts", {}).get("datasets", [])]
            if len(entries) != 1:
                raise PublicationError(f"Dataset has no unambiguous generation-bound receipts: {dataset}")
    return tuple(Member(f"{entry['prefix']}/{receipt['key']}", receipt["sha256"],
                        receipt["byteSize"], receipt["rows"])
                 for entry in entries if (receipt := entry.get("etlReceipts")))
