"""Bounded native file inventories; originals and receipt bodies stay in the archive."""

from __future__ import annotations

import hashlib
import io
import json
import re
from datetime import UTC, datetime
from pathlib import Path

SCHEMA_ID = "spicy-source-files/v1"
FORMAT = "spicy-source-files-parquet"
MAX_ROWS = 5000
MAX_BYTES = 4 * 1024 * 1024
MAX_DECODED_BYTES = 64 * 1024 * 1024
METADATA_KEYS = {
    "spicy_catalog.format",
    "spicy_catalog.schema_version",
    "spicy_catalog.generation",
    "spicy_catalog.dataset_id",
    "spicy_catalog.inventory_kind",
}


def schema(generation: str, dataset_id: str, inventory_kind: str):
    import pyarrow as pa

    text, binary = pa.string(), pa.binary(32)
    fields = [
        pa.field("alias_id", text, nullable=False),
        pa.field("dataset_id", text, nullable=False),
        pa.field("publisher", text),
        pa.field("inventory_kind", text, nullable=False),
        pa.field("name", text, nullable=False),
        pa.field("status", text, nullable=False),
        pa.field("provenance_basis", text, nullable=False),
        pa.field("availability", text, nullable=False),
        pa.field("source_id", text),
        pa.field("source_version", text),
        pa.field("route_id", text),
        pa.field("content_sha256", binary),
        pa.field("original_bytes", pa.uint64()),
        pa.field("content_key", text),
        pa.field("stored_bytes", pa.uint64()),
        pa.field("captured_at", pa.timestamp("us", tz="UTC")),
        pa.field("source_url", text),
        pa.field("source_urls", pa.list_(pa.field("element", text))),
        *[pa.field("period_" + key, text) for key in ("kind", "label", "basis", "start", "end")],
        pa.field(
            "periods",
            pa.list_(
                pa.field(
                    "element", pa.struct([pa.field(key, text) for key in ("kind", "label", "basis", "start", "end")])
                )
            ),
        ),
        pa.field("conflict_fields", pa.list_(pa.field("element", text))),
        pa.field("receipt_key", text),
        pa.field("receipt_sha256", binary),
        pa.field("receipt_entry", pa.uint64()),
        pa.field("receipt_kind", text),
        pa.field("original_receipt_key", text),
        pa.field("original_receipt_sha256", binary),
        pa.field("original_receipt_pointer", text),
    ]
    return pa.schema(
        fields,
        metadata={
            key.encode(): value.encode()
            for key, value in {
                "spicy_catalog.format": FORMAT,
                "spicy_catalog.schema_version": "1",
                "spicy_catalog.generation": generation,
                "spicy_catalog.dataset_id": dataset_id,
                "spicy_catalog.inventory_kind": inventory_kind,
            }.items()
        },
    )


def sha_bytes(value):
    return (
        bytes.fromhex(value.removeprefix("sha256:"))
        if isinstance(value, str) and re.fullmatch(r"(?:sha256:)?[a-f0-9]{64}", value)
        else None
    )


def contained_key(value):
    return (
        isinstance(value, str)
        and bool(value)
        and not value.startswith("/")
        and not any(part in {"", ".", ".."} for part in value.split("/"))
        and not any(char in value for char in "\\?#:%")
    )


def safe_pointer(value):
    if not isinstance(value, str) or len(value) > 2048 or not re.fullmatch(r"(?:/(?:[A-Za-z0-9_.-]|~[01])+)+", value):
        return None
    if re.search(
        r"/(Users|home|private|tmp|Volumes)(/|$)|credential|password|secret|token|api[_-]?key|://|\.\.",
        value,
        re.IGNORECASE,
    ):
        return None
    return value


class ReceiptReferences:
    """Resolve only pinned inputs and indexed archive objects; never scan a tree."""

    def __init__(self, db, dependencies=None):
        self.db, self.pins, self.cache = db, {}, {}
        self.dependencies = dependencies
        row = db.execute("SELECT value FROM metadata WHERE key='receipt_admission'").fetchone()
        admission = json.loads(row[0]) if row else {}
        self.admitted_receipts = admission.get('receipts', {}) if admission.get('version') == 1 else {}
        self.roots = []
        for row in db.execute("SELECT path,sha256,bytes FROM inputs"):
            path, digest, size = row
            self.pins.setdefault(digest, []).append((Path(path), size))
            if Path(path).name == "snapshot.json":
                self.roots.append(Path(path).parent)
            elif Path(path).parent.parent.name == "sha256" and Path(path).parent.parent.parent.name == "acquisitions":
                self.roots.append(Path(path).parents[3])
        self.roots = list(dict.fromkeys(self.roots))
        self.raw = bool(db.execute("SELECT 1 FROM main.sqlite_master WHERE name='raw_objects'").fetchone())

    def observe(self, path):
        if self.dependencies is not None:
            from .export_completion import observation

            value = observation(path)
            key = str(path.absolute())
            if key in self.dependencies and self.dependencies[key] != value:
                raise ValueError("Export reference changed during resolution")
            self.dependencies[key] = value

    def indexed(self, ref):
        key, digest = ref.get("manifest"), ref.get("sha256")
        if not contained_key(key) or sha_bytes(digest) is None:
            return None
        identity = (key, digest)
        if identity not in self.cache:
            candidates = [
                (path, size) for path, size in self.pins.get(digest, []) if path.as_posix().endswith("/" + key)
            ]
            found = False
            for path, size in candidates:
                self.observe(path)
                if path.is_file() and not path.is_symlink() and path.stat().st_size == size:
                    from .storage import digest as hash_file
                    from .selection import identity as file_identity

                    admitted = self.admitted_receipts.get(key)
                    qualified = (admitted is not None and admitted.get('sha256') == digest
                                 and admitted.get('bytes') == size
                                 and admitted.get('custody') == list(file_identity(path)))
                    if not qualified and hash_file(path) != digest:
                        raise ValueError("Pinned receipt bytes changed")
                    found = True
                    break
            self.cache[identity] = key if found else None
        return self.cache[identity]

    def original(self, digest):
        if not self.raw or sha_bytes(digest) is None:
            return None
        cache_key = ("original", digest)
        if cache_key not in self.cache:
            row = self.db.execute(
                "SELECT r.object,r.present,o.bytes FROM main.raw_objects r JOIN main.objects o USING(sha256) WHERE r.sha256=?",
                (digest,),
            ).fetchone()
            found = None
            if row and row[1] and contained_key(row[0]):
                from spicy_docs.storage.raw_archive import CentralRawArchiveStore

                for root in self.roots:
                    path = root / row[0]
                    self.observe(path)
                    # The raw owner prefers an uncompressed object when both
                    # storage forms exist. Preserve that negative decision too.
                    key = f"objects/sha256/{digest.removeprefix('sha256:')[:2]}/{digest.removeprefix('sha256:')}"
                    self.observe(root / key)
                    self.observe(root / (key + '.gz'))
                    if path.is_file():
                        descriptor = CentralRawArchiveStore(root, create=False).describe_blob(digest, row[2])
                        if descriptor["object"] != row[0]:
                            raise ValueError("Original receipt storage key differs")
                        found = row[0]
                        break
            self.cache[cache_key] = found
        return self.cache[cache_key]


def native_row(row, facts, *, dataset_id, publisher, inventory_kind, references):
    from .exporter import file_provenance, safe_text

    mapping = facts.get("mapping") or {}
    provenance = file_provenance(facts, status=row["status"], local=facts.get("encoded_bytes") is not None)
    conflicts = set((mapping.get("field_conflicts") or {}).keys())

    def text(value, field):
        if value is None:
            return None
        if not isinstance(value, str) or safe_text(value) != value:
            conflicts.add(field + "-withheld")
            return None
        return value

    captured = None
    value = provenance.get("capturedAt")
    if value is not None:
        try:
            fraction = re.search(r"\.(\d+)", value)
            if not re.search(r"T.*(?:Z|[+-]\d{2}:\d{2})$", value) or (fraction and len(fraction[1]) > 6):
                raise ValueError("Unsupported capture timestamp")
            captured = datetime.fromisoformat(value)
            if captured.tzinfo is None:
                raise ValueError("Naive capture timestamp")
            captured = captured.astimezone(UTC)
        except (TypeError, ValueError, OverflowError):
            conflicts.add("capture-time-invalid-or-unsupported")
    urls = provenance.get("sourceUrls", [])
    periods = []
    for period in mapping.get("periods") or []:
        # Content locators can resemble private paths. Publish the evidence
        # type without the locator so consumers can distinguish observations
        # from requested dates, while the receipt retains the exact pointer.
        basis = period.get("basis")
        if period.get("origin") == "content" and isinstance(basis, str) and basis.startswith("Original /"):
            basis = "Observed in source content"
        elif (period.get("origin") == "publication-context" and period.get("kind") == "publication-date"
              and period.get("rule") == "exact-original-unique-published-date-context/1"):
            basis = "Observed in pinned publication context"
        item = {key: text(basis if key == "basis" else period.get(key), "period-" + key)
                for key in ("kind", "label", "basis", "start", "end")}
        if item["kind"] and item["label"]:
            periods.append(item)
    ref = mapping.get("receipt_evidence_ref") or {}
    receipt_key = references.indexed(ref)
    entry = ref.get("entry")
    if type(entry) is not int or entry <= 0:
        entry = None
    if ref and (receipt_key is None or entry is None):
        conflicts.add("receipt-reference-unavailable")
    kind = (
        (
            "acquisition"
            if receipt_key.startswith("acquisitions/sha256/")
            else "frozen-index"
            if receipt_key.endswith(".jsonl.gz")
            else "retained-manifest"
        )
        if receipt_key
        else None
    )
    original_sha = provenance.get("originalReceiptSha256")
    original_key = references.original(original_sha) if original_sha else None
    pointer = safe_pointer((facts.get("receipt_context") or {}).get("original_receipt_pointer"))
    available_fields = set(row.keys())  # sqlite3.Row membership checks values, not column names.
    result = {
        "alias_id": row["id"],
        "dataset_id": dataset_id,
        "publisher": text(publisher, "publisher"),
        "inventory_kind": inventory_kind,
        "name": safe_text(row["name"]),
        "status": row["status"],
        "provenance_basis": provenance["basis"],
        "availability": provenance["availability"],
        "source_id": text(mapping.get("native_id"), "source-id"),
        "source_version": text(mapping.get("source_version"), "source-version"),
        "route_id": text(row["route_id"] if "route_id" in available_fields else None, "route-id"),
        "content_sha256": sha_bytes(row["sha256"]),
        "original_bytes": row["bytes"],
        "content_key": row["content_key"] if "content_key" in available_fields else None,
        "stored_bytes": row["stored_bytes"] if "stored_bytes" in available_fields else None,
        "captured_at": captured,
        "source_url": urls[0] if len(urls) == 1 else None,
        "source_urls": urls if len(urls) > 1 else None,
        **{
            "period_" + key: periods[0][key] if len(periods) == 1 else None
            for key in ("kind", "label", "basis", "start", "end")
        },
        "periods": periods if len(periods) > 1 else None,
        "conflict_fields": sorted(conflicts) or None,
        "receipt_key": receipt_key,
        "receipt_sha256": sha_bytes(ref.get("sha256")),
        "receipt_entry": entry,
        "receipt_kind": kind,
        "original_receipt_key": original_key,
        "original_receipt_sha256": sha_bytes(original_sha),
        "original_receipt_pointer": pointer if original_key else None,
    }
    return result


def encode(rows, *, generation, dataset_id, inventory_kind):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(rows, schema=schema(generation, dataset_id, inventory_kind))
    output = io.BytesIO()
    with pq.ParquetWriter(
        output,
        table.schema,
        compression="zstd",
        compression_level=3,
        use_dictionary=[field.name for field in table.schema if pa.types.is_string(field.type)],
        write_statistics=True,
        store_schema=False,
    ) as writer:
        writer.add_key_value_metadata(table.schema.metadata)
        writer.write_table(table, row_group_size=MAX_ROWS)
    return output.getvalue()


def validate(body, *, generation, dataset_id, inventory_kind):
    """The same physical schema, identity, privacy and bounds check used by selection."""
    import pyarrow.parquet as pq

    from .exporter import public_url, validate_public

    if len(body) > MAX_BYTES:
        raise ValueError("Parquet file inventory exceeds compressed bound")
    file = pq.ParquetFile(io.BytesIO(body))
    expected = schema(generation, dataset_id, inventory_kind)
    metadata = file.metadata.metadata or {}
    if metadata != expected.metadata or not file.schema_arrow.equals(expected, check_metadata=False):
        raise ValueError("Parquet file inventory schema or identity differs")
    if not 0 < file.metadata.num_rows <= MAX_ROWS:
        raise ValueError("Parquet file inventory exceeds row bound")
    if sum(file.metadata.row_group(i).total_byte_size for i in range(file.metadata.num_row_groups)) > MAX_DECODED_BYTES:
        raise ValueError("Parquet file inventory exceeds decoded bound")
    rows = file.read().to_pylist()
    ids = set()
    for row in rows:
        alias = row["alias_id"]
        if not isinstance(alias, str) or not alias.strip() or alias in ids:
            raise ValueError("Missing or duplicate Parquet alias")
        ids.add(alias)
        if row["dataset_id"] != dataset_id or row["inventory_kind"] != inventory_kind:
            raise ValueError("Parquet row identity differs")
        if row["provenance_basis"] not in {"receipt-backed", "provisional", "inferred", "unknown"} or row[
            "availability"
        ] not in {"local", "missing", "external", "not-acquired", "unknown"}:
            raise ValueError("Invalid Parquet file qualification")
        if not all(isinstance(row[key], str) and row[key].strip() for key in ("name", "status")):
            raise ValueError("Missing Parquet file text")
        if row["captured_at"] is not None and row["provenance_basis"] != "receipt-backed":
            raise ValueError("Unqualified Parquet capture time")
        if row["source_url"] is not None and row["source_urls"] is not None:
            raise ValueError("Duplicated Parquet source locations")
        for url in [row["source_url"]] if row["source_url"] else row["source_urls"] or []:
            if not isinstance(url, str) or public_url(url) != url:
                raise ValueError("Unsafe Parquet source URL")
        if row["source_urls"] is not None and (
            len(row["source_urls"]) < 2 or len(set(row["source_urls"])) != len(row["source_urls"])
        ):
            raise ValueError("Invalid Parquet source locations")
        if row["periods"] is not None and (
            len(row["periods"]) < 2
            or any(row["period_" + key] is not None for key in ("kind", "label", "basis", "start", "end"))
        ):
            raise ValueError("Duplicated Parquet period facts")
        if (row["period_kind"] is None) != (row["period_label"] is None):
            raise ValueError("Incomplete Parquet period facts")
        for key in ("receipt_key", "original_receipt_key", "content_key"):
            if row[key] is not None and not contained_key(row[key]):
                raise ValueError("Unsafe Parquet receipt key")
        if row["receipt_key"] is not None and (
            row["receipt_sha256"] is None
            or row["receipt_entry"] is None
            or row["receipt_entry"] <= 0
            or row["receipt_kind"] not in {"frozen-index", "acquisition", "retained-manifest"}
        ):
            raise ValueError("Incomplete Parquet receipt reference")
        key = row["receipt_key"]
        if key is not None:
            kind = (
                "acquisition"
                if key.startswith("acquisitions/sha256/")
                else "frozen-index"
                if key.endswith(".jsonl.gz")
                else "retained-manifest"
            )
            if row["receipt_kind"] != kind:
                raise ValueError("Parquet receipt kind differs from its key")
            if (
                kind == "acquisition"
                and key != f"acquisitions/sha256/{row['receipt_sha256'].hex()[:2]}/{row['receipt_sha256'].hex()}"
            ):
                raise ValueError("Parquet acquisition receipt identity differs")
        for key_field, hash_field in (
            ("content_key", "content_sha256"),
            ("original_receipt_key", "original_receipt_sha256"),
        ):
            key, digest = row[key_field], row[hash_field]
            if key is not None:
                base = f"objects/sha256/{digest.hex()[:2]}/{digest.hex()}" if digest is not None else None
                if base is None or key not in {base, base + ".gz"}:
                    raise ValueError("Parquet archive object identity differs")
        pointer = row["original_receipt_pointer"]
        if pointer is not None and (safe_pointer(pointer) != pointer or row["original_receipt_key"] is None):
            raise ValueError("Unsafe Parquet original receipt pointer")
        public = {}
        for key, value in row.items():
            if key in {
                "content_sha256",
                "receipt_sha256",
                "original_receipt_sha256",
                "captured_at",
                "original_receipt_pointer",
            }:
                continue
            public[key] = value
        validate_public(public)
    return rows


def export_pages(db, output: Path, *, dataset_id, publisher, folder, generation, page_size, inventory_kind, references, emit=None, page_rows=None):
    """Stream selected SQL observations into immutable, size-bounded shards."""
    source = "files" if inventory_kind == "active" else "retained_response_files"

    def values(owner, fields, json_fields=()):
        entries = []
        for field in fields:
            expression = f"json_extract(data,'$.{owner + '.' if owner else ''}{field}')"
            entries.extend(["'" + field + "'", "json(" + expression + ")" if field in json_fields else expression])
        return "json_object(" + ",".join(entries) + ")"

    mapping = values(
        "mapping",
        ("acquired_at", "native_id", "source_version", "periods", "field_conflicts", "receipt_evidence_ref"),
        ("periods", "field_conflicts", "receipt_evidence_ref"),
    )
    context = values(
        "receipt_context",
        ("source_url", "requested_url", "resolved_url", "url", "original_receipt_sha256", "original_receipt_pointer"),
    )
    extra = values(
        "",
        (
            "encoded_bytes",
            "source_url",
            "requested_url",
            "resolved_url",
            "original_location",
            "receipt_source_url",
            "receipt_status",
            "receipt_matches",
            "receipt_object",
        ),
    )
    projection = f"json_set({extra},'$.mapping',json({mapping}),'$.receipt_context',json({context})) AS facts"
    storage = (
        "r.object AS content_key,r.stored_bytes AS stored_bytes"
        if references.raw
        else "NULL AS content_key,NULL AS stored_bytes"
    )
    join = " LEFT JOIN main.raw_objects r ON r.sha256=f.sha256" if references.raw else ""
    cursor = db.execute(
        f"SELECT f.id,f.route_id,f.name,f.status,f.sha256,f.bytes,{storage},{projection} FROM {source} f{join} WHERE f.collection_id=? ORDER BY f.id",
        (dataset_id,),
    )
    pages = []

    def write(rows):
        body = encode(rows, generation=generation, dataset_id=dataset_id, inventory_kind=inventory_kind)
        if len(body) > MAX_BYTES:
            if len(rows) == 1:
                raise ValueError("One file observation exceeds the Parquet shard bound")
            middle = len(rows) // 2
            write(rows[:middle])
            write(rows[middle:])
            return
        validate(body, generation=generation, dataset_id=dataset_id, inventory_kind=inventory_kind)
        prefix = "files" if inventory_kind == "active" else "retained-files"
        relative = f"{folder}/{prefix}-{len(pages):05d}.parquet"
        path = output / relative
        if emit is not None:
            emit(path, body)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                if path.read_bytes() != body:
                    raise ValueError(f"Immutable export collision: {path}")
            else:
                temporary = path.with_suffix(".tmp")
                temporary.write_bytes(body)
                temporary.replace(path)
        pages.append(
            {
                "path": relative,
                "format": "parquet",
                "schema": SCHEMA_ID,
                "rows": len(rows),
                "bytes": len(body),
                "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            }
        )

    sizes = iter(page_rows) if page_rows is not None else None
    while True:
        size = next(sizes, None) if sizes is not None else min(page_size, MAX_ROWS)
        if size is None:
            if cursor.fetchone() is not None:
                raise ValueError("Staged public pages omit candidate rows")
            break
        if not 0 < size <= MAX_ROWS:
            raise ValueError("Invalid staged public page size")
        batch = cursor.fetchmany(size)
        if not batch:
            if sizes is not None:
                raise ValueError("Staged public pages exceed candidate rows")
            break
        if sizes is not None and len(batch) != size:
            raise ValueError("Staged public page size differs from candidate rows")
        rows = [
            native_row(
                row,
                json.loads(row["facts"]),
                dataset_id=dataset_id,
                publisher=publisher,
                inventory_kind=inventory_kind,
                references=references,
            )
            for row in batch
        ]
        write(rows)
    return pages
