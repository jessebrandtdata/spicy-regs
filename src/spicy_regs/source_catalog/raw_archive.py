"""Rebuild source holdings from raw bytes' manifests and acquisition receipts.

The SQLite index is disposable. Dataset assignments are derived from retained
provenance and the checked-in mapper; the archive never depends on this index.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import stat
from collections import Counter
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from .importer import Importer, encoded
from .raw_content import SUPPORTED_DATASETS, extract_metadata
from .raw_mapping import SourceMapper
from .raw_classification import (
    classification_inputs, fec_processing_metadata as fec_processing_metadata, retained_content_metadata,
    retained_content_reviews,
)
from .storage import digest, stable


def legacy_fec_evidence(row: dict, mapping: dict) -> bool:
    """Identify the legacy producer whose generic evidence role also held inputs."""
    context = row.get("receipt_context") or {}
    artifact = context.get("artifact", "")
    sha = str(row.get("sha256", "")).removeprefix("sha256:")
    member = f"blobs/sha256/{sha}"
    dataset = mapping.get("dataset_id")
    narrowed_fec = (
        mapping.get("mapping_status") == "mapped"
        and any(
            item.get("origin") == "receipt"
            and item.get("rule") == "registry-family-object"
            and item.get("value") == "fec-query"
            and item.get("dataset_id") == dataset
            for item in mapping.get("evidence", [])
        )
    )
    return (
        (dataset == "family:fec-query" or narrowed_fec)
        and mapping.get("role") == context.get("declared_role") == "evidence"
        and context.get("family") == "fec-query"
        and isinstance(artifact, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", artifact) is not None
        and re.fullmatch(r"[0-9a-f]{64}", sha) is not None
        and context.get("member") == member
        and str(row.get("original_location", "")).endswith(f"/source-evidence/{artifact[7:]}/{member}")
    )


def contained_file(root: Path, reference: str) -> Path:
    relative = Path(reference)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"Invalid archive reference: {reference}")
    path = root
    for part in relative.parts:
        path = path / part
        if path.is_symlink():
            raise ValueError(f"Archive reference traverses a symlink: {reference}")
    return path


def apply_content_review(db, review: dict, *, receipt_ref: dict) -> dict:
    """Apply pinned byte-content findings without rewriting acquisition facts.

    Callers pin and verify the enclosing immutable acquisition receipt first.
    Classification applies to every alias of the exact bytes, across captures.
    A review can refuse or qualify a fragment; it cannot assert acceptance,
    transport status, acquisition time or a dataset association.
    """
    if (not isinstance(review, dict) or set(review) != {"format", "version", "decisions"}
            or review["format"] != "spicygov-content-review" or type(review["version"]) is not int
            or review["version"] != 1 or not isinstance(review["decisions"], list)):
        raise ValueError("Invalid content review document")
    if not isinstance(receipt_ref, dict) or set(receipt_ref) != {"manifest", "sha256"}:
        raise ValueError("Content review requires its pinned receipt")
    sha = receipt_ref["sha256"]
    if (not isinstance(sha, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", sha)
            or receipt_ref["manifest"] != f"acquisitions/sha256/{sha[7:9]}/{sha[7:]}"):
        raise ValueError("Invalid content review receipt identity")
    fields = {"sha256", "bytes", "classification", "role", "reason", "evidence", "limits", "provenance",
              "scope_complete", "non_publisher_source", "verified_original", "source_validation",
              "source_file_complete"}
    seen, updates = set(), []
    for i, raw_decision in enumerate(review["decisions"]):
        if not isinstance(raw_decision, dict):
            raise ValueError("Invalid content review decision")
        decision = cast(dict[str, Any], raw_decision)
        if (set(decision) != fields
                or not isinstance(decision["sha256"], str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", decision["sha256"])
                or type(decision["bytes"]) is not int or decision["bytes"] < 0
                or any(not isinstance(decision[k], str) or not decision[k].strip()
                       for k in ("classification", "role", "reason"))
                or not isinstance(decision["evidence"], list) or not decision["evidence"]
                or not all(isinstance(e, dict) and e for e in decision["evidence"])
                or not isinstance(decision["limits"], list) or not decision["limits"]
                or not all(isinstance(e, str) and e.strip() for e in decision["limits"])
                or not isinstance(decision["provenance"], dict) or not decision["provenance"]
                or decision["scope_complete"] is not False or decision["verified_original"] is not True
                or type(decision["non_publisher_source"]) is not bool
                or decision["source_validation"] not in (None, "refused")
                or decision["source_file_complete"] is not None and decision["source_file_complete"] is not False
                or decision["non_publisher_source"] and decision["source_validation"] != "refused"):
            raise ValueError("Invalid content review decision")
        original = decision["sha256"]
        if original in seen:
            raise ValueError("Duplicate content review original")
        seen.add(original)
        rows = list(db.execute("SELECT id,status,bytes,data FROM files WHERE sha256=?", (original,)))
        if not rows or any(row[2] != decision["bytes"] for row in rows):
            raise ValueError("Content review does not match retained originals")
        for ident, status, _size, data in rows:
            facts = json.loads(data)
            prior = facts.get("content_review")
            if prior:
                if prior.get("decision") != decision:
                    raise ValueError("Conflicting content reviews require resolution")
                continue
            facts["content_review"] = {
                "decision": decision,
                "receipt": {**receipt_ref, "pointer": f"/receipt/content_review/decisions/{i}"},
            }
            target = status
            if status in {"held", "unclassified-source"}:
                if decision["source_validation"] == "refused":
                    target = "refused-source"
                elif decision["source_file_complete"] is False:
                    target = "partial-source"
            updates.append((target, encoded(facts), ident))
    # Validate every decision before changing any alias. The caller owns the
    # build transaction, including preservation of the previous selected index.
    db.executemany("UPDATE files SET status=?,data=? WHERE id=?", updates)
    return {"decisions": len(seen), "annotated_aliases": len(updates)}


def read_original(root: Path, sha: str, obj) -> bytes:
    """Read only a caller-bounded object and verify its decoded byte identity."""
    path = contained_file(root, obj["object"])
    with gzip.open(path, "rb") if obj["encoding"] == "gzip" else path.open("rb") as stream:
        body = stream.read(obj["bytes"] + 1)
    if len(body) != obj["bytes"] or hashlib.sha256(body).hexdigest() != sha.removeprefix("sha256:"):
        raise ValueError("Original changed while extracting native metadata")
    return body


def verify_reviewed_original(root: Path, sha: str, obj) -> None:
    """Verify an already reviewed large object without loading it into memory."""
    path = contained_file(root, obj["object"])
    digest, size = hashlib.sha256(), 0
    with gzip.open(path, "rb") if obj["encoding"] == "gzip" else path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(chunk)
            if size > obj["bytes"]:
                raise ValueError("Reviewed original exceeds its recorded size")
            digest.update(chunk)
    if size != obj["bytes"] or digest.hexdigest() != sha.removeprefix("sha256:"):
        raise ValueError("Reviewed original changed")


def retained_classification_candidates(db, configuration: dict | None = None):
    """Yield indexed legacy objects and exact-byte reviews, never scan raw bytes."""
    reviews = retained_content_reviews()
    for sha in sorted(reviews):
        if db.execute("SELECT 1 FROM main.files WHERE sha256=? LIMIT 1", (sha,)).fetchone():
            yield sha
    seen = set(reviews)
    for binding in (configuration or {}).get("object_datasets", []):
        if binding.get("family") != "fec-query":
            continue
        for raw_digest in binding.get("sha256", []):
            sha = "sha256:" + str(raw_digest).removeprefix("sha256:")
            if sha in seen:
                continue
            for row in db.execute("SELECT sha256,data FROM main.files WHERE sha256=? ORDER BY id", (sha,)):
                facts = json.loads(row["data"])
                if legacy_fec_evidence(facts, facts.get("mapping", {})):
                    seen.add(sha)
                    yield sha
                    break
    for row in db.execute(
        "SELECT sha256,data FROM main.files WHERE collection_id=? ORDER BY id", ("family:fec-query",)
    ):
        if row["sha256"] in seen:
            continue
        facts = json.loads(row["data"])
        if legacy_fec_evidence(facts, facts.get("mapping", {})):
            seen.add(row["sha256"])
            yield row["sha256"]


def remap_object_datasets(db, configuration: dict) -> int:
    """Re-apply configured object_datasets bindings to inherited file rows by hash."""
    mapper = SourceMapper(configuration)
    if not mapper.objects:
        return 0
    remapped_rows = 0
    for sha in sorted({"sha256:" + raw_digest for _, raw_digest in mapper.objects}):
        rows = db.execute(
            "SELECT id,collection_id,sha256,data FROM main.files "
            "WHERE sha256=? AND collection_id!='archive:receipts' ORDER BY id",
            (sha,),
        ).fetchall()
        for row in rows:
            facts = json.loads(row["data"])
            prior_mapping = facts.get("mapping")
            if not isinstance(prior_mapping, dict):
                continue
            raw_row = {
                key: value
                for key, value in facts.items()
                if key not in {
                    "mapping", "verification", "encoding", "stored_sha256",
                    "encoded_bytes", "presence_mtime_ns", "content_review",
                }
            }
            remapped = mapper.map_source(raw_row)
            if "receipt_evidence_ref" in prior_mapping:
                remapped["receipt_evidence_ref"] = prior_mapping["receipt_evidence_ref"]
            if "content_metadata" in prior_mapping:
                content = dict(
                    prior_mapping["content_metadata"],
                    evidence=[
                        item for item in prior_mapping.get("evidence", [])
                        if item.get("origin") != "receipt"
                    ],
                    periods=[
                        item for item in prior_mapping.get("periods", [])
                        if item.get("origin") in {"content", "publication-context"}
                    ],
                )
                remapped = merge_content(remapped, content, row["sha256"])
            cid = remapped.get("dataset_id") or "archive:unmapped"
            db.execute(
                "INSERT OR IGNORE INTO main.collections VALUES(?,?,?)",
                (
                    cid,
                    cid.removeprefix("dataset:").replace("-", " "),
                    encoded({
                        "mapping_status": remapped.get("mapping_status", "receipt-only"),
                        "description": "Retained source files; completeness and usable record counts are not inferred.",
                    }),
                ),
            )
            data = encoded(dict(facts, mapping=remapped))
            if cid != row["collection_id"] or data != row["data"]:
                db.execute("UPDATE main.files SET collection_id=?,data=? WHERE id=?", (cid, data, row["id"]))
                remapped_rows += 1
    return remapped_rows


def classify_retained_objects(db, root: Path, sha256s, *, max_bytes: int = 1024 * 1024) -> dict:
    """Classify an explicit candidate set and all exact-byte aliases in place.

    Operate only on an unselected candidate inside its caller's transaction.
    Exact-byte reviews apply independently of producer provenance. Schema-based
    generated-content recognition still requires its legacy producer witness.
    Reads use the object/alias hash indexes; no archive scan, raw write, inferred
    transport fact or dataset reassignment is involved. Unresolved legacy bytes
    stay searchable as unclassified-source, with the reason and limits retained.
    """
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or not 0 < max_bytes <= 1024 * 1024:
        raise ValueError("Classification read limit must be between 1 byte and 1 MiB")
    counts = Counter()
    reviews = retained_content_reviews()
    db.execute("SAVEPOINT retained_content_classification")
    try:
        for sha in sha256s:
            if not isinstance(sha, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", sha):
                raise ValueError("Classification requires qualified original SHA-256 identities")
            aliases = list(db.execute("SELECT id,status,bytes,data FROM main.files WHERE sha256=? ORDER BY id", (sha,)))
            decoded = [(row, json.loads(row["data"])) for row in aliases]
            producers = [(row, facts) for row, facts in decoded
                         if legacy_fec_evidence(facts, facts.get("mapping", {}))]
            if not producers and sha not in reviews:
                counts["outside_producer_scope"] += 1
                continue
            obj = db.execute("""SELECT raw_objects.*,objects.bytes FROM main.raw_objects
                              JOIN main.objects USING(sha256) WHERE sha256=?""", (sha,)).fetchone()
            if not obj or not obj["present"]:
                counts["missing_bytes"] += 1
                continue
            if any(row["bytes"] != obj["bytes"] for row in aliases):
                raise ValueError("Classification alias byte size differs from object")
            if sha in reviews:
                review = reviews[sha]
                if review["bytes"] != obj["bytes"]:
                    raise ValueError("Reviewed original byte size differs from object")
                verify_reviewed_original(root, sha, obj)
                result: dict[str, Any] | None = dict(review["result"])
                result["scope"] = dict(result["scope"], review_sha256="sha256:" + stable(review))
                counts["reviewed_objects"] += 1
                counts["verified_original_reads"] += 1
                counts["verified_original_bytes"] += obj["bytes"]
            elif obj["bytes"] > max_bytes:
                counts["above_byte_limit"] += 1
                result = {"outcome": "not-read", "classification": "uncertain",
                          "scope": {"reason": "byte-limit", "max_bytes": max_bytes}}
            else:
                body = read_original(root, sha, obj)
                counts["verified_original_reads"] += 1
                counts["verified_original_bytes"] += len(body)
                result = retained_content_metadata(body)
            if result is None:
                counts["unrecognized_objects"] += 1
                continue
            classification = result.get("classification", "uncertain")
            if not isinstance(classification, str):
                raise ValueError("Invalid retained content classification")
            counts[classification + "_objects"] += 1
            if producers:
                producer_row, producer = producers[0]
                result["producer_evidence"] = {
                    "file_id": producer_row["id"],
                    "receipt_context": producer["receipt_context"],
                    "original_location": producer["original_location"],
                    "receipt_evidence_ref": producer.get("mapping", {}).get("receipt_evidence_ref"),
                }
            for row, facts in decoded:
                prior_mapping = facts.get("mapping", {})
                prior = prior_mapping.get("content_metadata", {})
                status = row["status"]
                review = facts.get("content_review", {}).get("decision", {})
                reviewed_original = (review.get("non_publisher_source") is False
                                     and (review.get("classification") in {"source-original", "publisher-original"}
                                          or review.get("role") == "original-response"))
                if reviewed_original and result.get("classification") != "source-original":
                    raise ValueError("Content classification conflicts with a reviewed source original")
                if status in {"held", "unclassified-source"}:
                    if result.get("classification") == "uncertain":
                        status = "unclassified-source"
                    elif result.get("classification") == "source-error":
                        status = "refused-source"
                    elif result.get("role") == "acquisition-receipt":
                        status = "receipt-only"
                    elif result.get("role") == "processing-evidence":
                        status = "supporting-evidence"
                    elif result.get("classification") == "source-original":
                        status = "held"
                if (status == row["status"] and prior.get("raw_sha256") == sha
                        and all(prior.get(k) == value for k, value in result.items() if k != "evidence")):
                    continue
                mapping = merge_content(prior_mapping, result, sha)
                prior_evidence = prior_mapping.get("evidence", [])
                mapping["evidence"] = list(prior_evidence) + [
                    item for item in mapping["evidence"][len(prior_evidence):] if item not in prior_evidence
                ]
                data = encoded(dict(facts, mapping=mapping))
                if status != row["status"] or data != row["data"]:
                    db.execute("UPDATE main.files SET status=?,data=? WHERE id=?", (status, data, row["id"]))
                    counts["annotated_aliases"] += 1
        db.execute("RELEASE retained_content_classification")
    except BaseException:
        db.execute("ROLLBACK TO retained_content_classification")
        db.execute("RELEASE retained_content_classification")
        raise
    return dict(sorted(counts.items()))


class RawImporter(Importer):
    def run(self) -> None:
        import spicy_docs.storage.raw_archive as archive_reader
        from spicy_docs.storage.raw_archive import iter_base_records

        self.inputs.pin(Path(archive_reader.__file__))
        for source in classification_inputs():
            self.inputs.pin(source)
        self.prepare_metadata()
        # Build secondary file indexes once after loading the immutable manifests.
        self.db.executescript("DROP INDEX files_collection; DROP INDEX files_sha;")
        snapshot = self.inputs.json(self.root / "snapshot.json") if (self.root / "snapshot.json").is_file() else {}
        self.generated_at = snapshot.get("recorded_at", self.generated_at)
        for name, expected in snapshot.get("indexes", {}).items():
            path = contained_file(self.root, name)
            self.inputs.pin(path)
            if self.inputs.pins[str(path.resolve())]["sha256"].removeprefix("sha256:") != expected:
                raise ValueError(f"Archive index checksum mismatch: {name}")
        self.db.executescript("""
            CREATE TABLE raw_objects(sha256 TEXT PRIMARY KEY REFERENCES objects,
              object TEXT NOT NULL, encoding TEXT NOT NULL, stored_bytes INTEGER NOT NULL,
              stored_sha256 TEXT NOT NULL, mtime_ns INTEGER, present INTEGER NOT NULL);
            CREATE TABLE raw_periods(file_id TEXT REFERENCES files, kind TEXT, label TEXT,
              basis TEXT, PRIMARY KEY(file_id,kind,label));
        """)
        if snapshot:
            for row in iter_base_records(self.root, "objects.jsonl.gz"):
                self.object(row)
            for position, row in enumerate(iter_base_records(self.root, "files.jsonl.gz")):
                self.raw_file(row, f"files.jsonl.gz:{position + 1}")
        self.collection("archive:receipts", "Original acquisition receipts")
        if snapshot:
            for position, row in enumerate(iter_base_records(self.root, "receipts.jsonl.gz")):
                self.raw_file(row, f"receipts.jsonl.gz:{position + 1}", receipt_only=True)
            for position, row in enumerate(iter_base_records(self.root, "unavailable-receipts.jsonl.gz")):
                self.evidence(None, "unavailable-source", dict(row, manifest_position=position + 1))
        content_reviews = self.import_receipts()
        self.db.executescript("""
            CREATE INDEX files_collection ON files(collection_id,id);
            CREATE INDEX files_sha ON files(sha256);
            CREATE INDEX raw_periods_label ON raw_periods(kind,label);
        """)
        self.finish_reviews(content_reviews)

    def prepare_metadata(self) -> None:
        self.mapper = SourceMapper(self.config)
        self.refused_sources: set[tuple[str, str, str]] = set()
        self.metadata_counts: Counter = Counter()
        # Reuse extraction across aliases in a disposable SQLite cache. The
        # build chooses temporary storage; disk remains the memory-bounded default.
        self.db.executescript("""
            CREATE TEMP TABLE native_metadata_cache(
              sha256 TEXT, dataset TEXT, data TEXT, PRIMARY KEY(sha256,dataset));
            CREATE TEMP TABLE retained_classification_candidates(sha256 TEXT PRIMARY KEY);
        """)

    def reuse_receipts(self, pins):
        """Reuse accepted metadata only when the complete receipt set is unchanged.

        Receipt bytes are authenticated again and checked at commit. Accepted
        references receive one size check per unique original, using bounded
        temporary state. New receipts still use the maintained full reader.
        """
        paths = self.receipt_paths()
        if {str(self.root / path) for path in paths} != pins.keys():
            return None
        self.db.execute("CREATE TEMP TABLE receipt_checked_objects(object TEXT PRIMARY KEY)")
        for relative in paths:
            path = contained_file(self.root, relative)
            expected = pins[str(path)]
            with path.open("rb") as source:
                payload = source.read(expected["bytes"] + 1)
            actual = {"sha256": "sha256:" + hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}
            if actual != expected or actual["sha256"] != "sha256:" + path.name:
                raise ValueError("Previously indexed acquisition receipt changed")
            if self.inputs.pins.setdefault(str(path), actual) != actual:
                raise ValueError("Acquisition receipt changed during metadata refresh")
            # The exact bytes were admitted by the maintained reader before
            # entering this index. Reuse that admission; only custody is mutable.
            receipt = json.loads(payload)
            for obj in receipt["objects"]:
                inserted = self.db.execute("INSERT OR IGNORE INTO receipt_checked_objects VALUES(?)", (obj["object"],))
                if inserted.rowcount:
                    current = contained_file(self.root, obj["object"]).lstat()
                    if not stat.S_ISREG(current.st_mode) or current.st_size != obj["stored_bytes"]:
                        raise ValueError("Retained original stored size differs from its indexed receipt")
        content_reviews = []
        indexed = set()
        for (data,) in self.db.execute("SELECT data FROM evidence WHERE kind='acquisition-receipt'"):
            item = json.loads(data)
            relative = item["object"]
            path = str(self.root / relative)
            if path not in pins or item["sha256"] != pins[path]["sha256"] or relative in indexed:
                raise ValueError("Indexed acquisition receipt metadata differs from its accepted pin")
            indexed.add(relative)
            if "content_review" in item["receipt"]:
                content_reviews.append((item["receipt"]["content_review"], {
                    "manifest": relative, "sha256": item["sha256"],
                }))
        if indexed != set(paths):
            raise ValueError("Indexed acquisition receipt metadata is incomplete")
        self.acquisition_paths = paths
        if self.receipt_paths() != paths:
            raise RuntimeError("Acquisition receipts changed during catalog rebuild; retry")
        return content_reviews

    def qualified_reviews(self, pins):
        """Recover accepted review decisions from the authenticated base index."""
        reviews, indexed = [], set()
        for (data,) in self.db.execute("SELECT data FROM evidence WHERE kind='acquisition-receipt'"):
            item = json.loads(data)
            relative = item['object']
            path = str(self.root / relative)
            if path not in pins or item['sha256'] != pins[path]['sha256'] or relative in indexed:
                raise ValueError('Indexed acquisition receipt metadata differs from its accepted pin')
            indexed.add(relative)
            if 'content_review' in item['receipt']:
                reviews.append((item['receipt']['content_review'], {'manifest': relative, 'sha256': item['sha256']}))
        if indexed != {str(Path(path).relative_to(self.root)) for path in pins}:
            raise ValueError('Indexed acquisition receipt metadata is incomplete')
        return reviews

    def import_receipts(self, skip=None, *, receipt_snapshot=None):
        from spicy_docs.storage.raw_archive import iter_acquisition_receipts

        skip = skip or {}
        if skip and receipt_snapshot is None:
            reused = self.reuse_receipts(skip)
            if reused is not None:
                return reused
        visited = set()
        content_reviews = []
        receipt_keys = None
        if receipt_snapshot is not None:
            frozen = receipt_snapshot['receipts']
            inherited = {str(Path(path).relative_to(self.root)) for path in skip}
            if not inherited <= frozen.keys() or any(
                    {k: frozen[relative][k] for k in ('sha256', 'bytes')} != skip[str(self.root / relative)]
                    for relative in inherited):
                raise ValueError('Frozen receipts differ from accepted base pins')
            visited.update(inherited)
            content_reviews = self.qualified_reviews(skip)
            receipt_keys = sorted(frozen.keys() - inherited)
            self.metadata_counts['inherited_receipt_payloads_reused'] = len(inherited)
        for path, receipt in iter_acquisition_receipts(self.root, receipt_keys=receipt_keys):
            visited.add(str(path.relative_to(self.root)))
            if str(path.resolve()) in skip:
                if "content_review" in receipt["receipt"]:
                    content_reviews.append((receipt["receipt"]["content_review"], {
                        "manifest": str(path.relative_to(self.root)), "sha256": digest(path),
                    }))
                continue
            if receipt_snapshot is None:
                self.inputs.pin(path)
            else:
                # The provider just authenticated these bytes against the key.
                # Reuse that read for the input/evidence pin; Inputs.check still
                # verifies the small new delta at the commit boundary.
                pin = receipt_snapshot['receipts'][str(path.relative_to(self.root))]
                self.inputs.pins[str(path)] = {k: pin[k] for k in ('sha256', 'bytes')}
                self.metadata_counts['new_receipt_payload_reads'] += 1
                self.metadata_counts['new_receipt_bytes'] += pin['bytes']
            receipt_pin = self.inputs.pins[str(path.resolve())]['sha256']
            recorded_at = receipt["receipt"].get("recorded_at") or receipt["receipt"].get("acquired_at")
            if isinstance(recorded_at, str):
                self.generated_at = max(self.generated_at, recorded_at)
            for row in receipt["objects"]:
                self.object(row)
            for position, row in enumerate(receipt["files"]):
                self.raw_file(
                    dict(
                        row,
                        receipt_object=str(path.relative_to(self.root)),
                        acquisition_run_id=receipt["receipt"].get("run_id"),
                    ),
                    f"{path.relative_to(self.root)}:{position + 1}",
                )
            self.evidence(
                None,
                "acquisition-receipt",
                {
                    "sha256": receipt_pin,
                    "object": str(path.relative_to(self.root)),
                    "receipt": receipt["receipt"],
                },
            )
            if "content_review" in receipt["receipt"]:
                content_reviews.append((receipt["receipt"]["content_review"], {
                    "manifest": str(path.relative_to(self.root)), "sha256": receipt_pin,
                }))
        self.acquisition_paths = tuple(sorted(visited))
        if receipt_snapshot is None and self.receipt_paths() != self.acquisition_paths:
            raise RuntimeError("Acquisition receipts changed during catalog rebuild; retry")
        return sorted(content_reviews, key=lambda item: item[1]['manifest'])

    def finish_reviews(self, content_reviews) -> None:
        # The transport can retain bytes before parsing. A later explicit source
        # refusal applies to the same bytes in that run, without erasing custody
        # or invalidating an independent accepted capture.
        for sha, cid, run_id in sorted(self.refused_sources):
            self.db.execute(
                """UPDATE files SET status='refused-source' WHERE sha256=? AND collection_id=?
                   AND status='held' AND json_extract(data,'$.acquisition_run_id')=?""",
                (sha, cid, run_id),
            )
        self.db.executemany(
            "INSERT OR IGNORE INTO retained_classification_candidates SELECT ? WHERE EXISTS "
            "(SELECT 1 FROM files WHERE sha256=?)",
            ((sha, sha) for sha in retained_content_reviews()),
        )
        self.classification_counts = classify_retained_objects(
            self.db, self.root,
            (row[0] for row in self.db.execute("SELECT sha256 FROM retained_classification_candidates ORDER BY sha256")),
        )
        for review, receipt_ref in content_reviews:
            apply_content_review(self.db, review, receipt_ref=receipt_ref)
        self.evidence(
            None,
            "raw-archive",
            {
                "snapshot_sha256": digest(self.root / "snapshot.json") if (self.root / "snapshot.json").is_file() else None,
                "new_acquisition_receipts": len(self.acquisition_paths),
                "scope": "Raw originals and receipts; dataset assignments do not establish completeness",
            },
        )

    def refresh_content(self, collections, *, objects=None) -> int:
        """Reinterpret named collections without replaying the frozen inventory."""
        refreshed = 0
        self.db.execute("CREATE TEMP TABLE IF NOT EXISTS refresh_content_objects(sha256 TEXT PRIMARY KEY)")
        self.db.execute("DELETE FROM refresh_content_objects")
        if objects is not None:
            self.db.executemany("INSERT INTO refresh_content_objects VALUES(?)", ((sha,) for sha in objects))
        for cid in collections:
            last = ""
            while rows := self.db.execute(
                "SELECT id,data FROM files WHERE collection_id=? AND id>? "
                + ("AND sha256 IN (SELECT sha256 FROM refresh_content_objects) " if objects is not None else "")
                + "ORDER BY id LIMIT 32",
                (cid, last),
            ).fetchall():
                for ident, data in rows:
                    facts = json.loads(data)
                    ref = facts.get("mapping", {}).get("receipt_evidence_ref")
                    if not isinstance(ref, dict) or not isinstance(ref.get("entry"), int):
                        raise ValueError("Content refresh requires a retained per-file receipt reference")
                    path = self.inputs.pin(contained_file(self.root, ref["manifest"]))
                    prior = self.db.execute("SELECT sha256,bytes FROM inputs WHERE path=?", (str(path),)).fetchone()
                    current = self.inputs.pins[str(path)]
                    if prior is None or tuple(prior) != (current["sha256"], current["bytes"]):
                        raise ValueError("Content refresh receipt changed from the selected inventory")
                    original = {key: value for key, value in facts.items() if key not in {
                        "mapping", "verification", "encoding", "stored_sha256", "encoded_bytes",
                        "presence_mtime_ns", "content_review",
                    }}
                    self.db.execute("DELETE FROM raw_periods WHERE file_id=?", (ident,))
                    self.db.execute("DELETE FROM files WHERE id=?", (ident,))
                    self.raw_file(original, f"{ref['manifest']}:{ref['entry']}")
                    if ident not in self.expected_files:
                        raise ValueError("Content refresh changed the source occurrence identity")
                    refreshed += 1
                last = rows[-1][0]
        return refreshed

    def receipt_paths(self) -> tuple[str, ...]:
        return tuple(sorted(str(p.relative_to(self.root)) for p in self.root.glob("acquisitions/sha256/*/*")))

    def object(self, row: dict) -> None:
        sha = str(row["sha256"]).removeprefix("sha256:")
        if not re.fullmatch(r"[0-9a-f]{64}", sha):
            raise ValueError("Invalid original byte identity")
        for key in ("bytes", "stored_bytes"):
            if not isinstance(row[key], int) or isinstance(row[key], bool) or row[key] < 0:
                raise ValueError("Invalid original or stored byte size")
        if not re.fullmatch(r"[0-9a-f]{64}", str(row["stored_sha256"])):
            raise ValueError("Invalid stored byte identity")
        encoding = row["encoding"]
        if encoding not in {"raw", "gzip"}:
            raise ValueError("Unsupported raw archive encoding")
        if encoding == "raw" and (row["stored_sha256"] != sha or row["stored_bytes"] != row["bytes"]):
            raise ValueError("Raw storage descriptor differs from original identity")
        expected = f"objects/sha256/{sha[:2]}/{sha}" + (".gz" if encoding == "gzip" else "")
        if row["object"] != expected:
            raise ValueError("Object reference does not match its identity and encoding")
        path = contained_file(self.root, expected)
        try:
            info = path.stat()
        except FileNotFoundError:
            info = None
        if info and (not stat.S_ISREG(info.st_mode) or info.st_size != row["stored_bytes"]):
            raise ValueError(f"Stored object metadata mismatch: {expected}")
        sha = "sha256:" + sha
        existing = self.db.execute("SELECT bytes FROM objects WHERE sha256=?", (sha,)).fetchone()
        if existing and existing[0] != row["bytes"]:
            raise ValueError("Conflicting original byte sizes")
        self.db.execute("INSERT OR IGNORE INTO objects VALUES(?,?)", (sha, row["bytes"]))
        values = (
            sha,
            expected,
            encoding,
            row["stored_bytes"],
            row["stored_sha256"],
            info.st_mtime_ns if info else None,
            int(info is not None),
        )
        existing = self.db.execute("SELECT * FROM raw_objects WHERE sha256=?", (sha,)).fetchone()
        if existing and tuple(existing) != values:
            raise ValueError("Conflicting stored object descriptors")
        self.db.execute("INSERT OR IGNORE INTO raw_objects VALUES(?,?,?,?,?,?,?)", values)

    def raw_file(self, row: dict, context: str, *, receipt_only: bool = False) -> None:
        sha = "sha256:" + str(row["sha256"]).removeprefix("sha256:")
        obj = self.db.execute(
            "SELECT raw_objects.*,objects.bytes FROM raw_objects JOIN objects USING(sha256) WHERE sha256=?", (sha,)
        ).fetchone()
        if not obj or row["object"] != obj["object"] or row["bytes"] != obj["bytes"]:
            raise ValueError(f"Raw file has no matching object descriptor: {context}")
        mapping = self.mapper.map_source(row) if not receipt_only else {}
        if mapping.get("role") in {"acquisition-receipt", "request-body"}:
            receipt_only = True
        if mapping:
            manifest, position = context.rsplit(":", 1)
            mapping["receipt_evidence_ref"] = {
                "manifest": manifest,
                "entry": int(position),
                "sha256": self.inputs.pins[str((self.root / manifest).resolve())]["sha256"],
            }
        status = "missing-bytes" if not obj["present"] else "receipt-only" if receipt_only else "held"
        if status == "held":
            transport_fields = {"http_status", "response_complete", "source_file_complete", "source_validation"}
            if transport_fields.intersection(mapping.get("field_conflicts", {})):
                status = "conflicting-receipt"
            elif mapping.get("source_validation") == "refused":
                status = "refused-source"
            elif mapping.get("response_complete") is False:
                status = "incomplete-response"
            elif isinstance(mapping.get("http_status"), int) and mapping["http_status"] >= 400:
                status = "failed-response"
            elif mapping.get("source_file_complete") is False or mapping.get("role") == "source-fragment":
                status = "partial-source"
        if status == "held" and mapping.get("mapping_status") != "conflict":
            content = self.content_metadata(sha, obj, mapping, row)
            if content is not None:
                mapping = merge_content(mapping, content, sha)
                if content.get("role") == "acquisition-receipt":
                    receipt_only, status = True, "receipt-only"
                elif mapping.get("content_metadata", {}).get("conflict"):
                    status = "conflicting-metadata"
        if status == "held":
            from .scope import SUPPORTING_ROLES

            if (mapping.get("role") in SUPPORTING_ROLES
                    or mapping.get("content_metadata", {}).get("role") in SUPPORTING_ROLES):
                status = "supporting-evidence"
        cid = "archive:receipts" if receipt_only else mapping.get("dataset_id") or "archive:unmapped"
        self.collection(
            cid,
            cid.removeprefix("dataset:").replace("-", " "),
            {
                "mapping_status": mapping.get("mapping_status", "receipt-only"),
                "description": "Retained source files; completeness and usable record counts are not inferred.",
            },
        )
        ident = "file:" + stable("raw-archive", context, sha)
        self.expected_files.add(ident)
        if status == "refused-source" and isinstance(row.get("acquisition_run_id"), str):
            self.refused_sources.add((sha, cid, row["acquisition_run_id"]))
        facts = dict(
            row,
            mapping=mapping,
            verification="pinned-manifest;presence-checked;not-rehashed",
            encoding=obj["encoding"],
            stored_sha256=obj["stored_sha256"],
        )
        if obj["present"]:
            facts.update(encoded_bytes=obj["stored_bytes"], presence_mtime_ns=obj["mtime_ns"])
        if legacy_fec_evidence(row, mapping):
            self.db.execute("INSERT OR IGNORE INTO retained_classification_candidates VALUES(?)", (sha,))
        native = row.get("receipt_context", {})
        name = (
            str(row.get("name") or native.get("member") or row.get("original_location") or sha).split("/")[-1].strip()
            or sha
        )
        self.db.execute(
            "INSERT INTO files VALUES(?,?,?,?,?,?,?,?,?)",
            (
                ident,
                cid,
                None,
                name,
                status,
                sha,
                row["bytes"],
                str(self.root / obj["object"]) if obj["present"] else None,
                encoded(facts),
            ),
        )
        for period in mapping.get("periods", []):
            self.db.execute(
                "INSERT OR IGNORE INTO raw_periods VALUES(?,?,?,?)",
                (
                    ident,
                    period["kind"],
                    period["label"],
                    period["basis"],
                ),
            )
            self.evidence(
                cid,
                "periods",
                {
                    "kind": period["kind"],
                    "label": period["label"],
                    "basis": "Retained per-file mapping; native/requested scope; completeness unknown",
                },
            )

    def content_metadata(self, sha: str, obj, mapping: dict, row: dict) -> dict | None:
        """Verify bounded originals once per parser context in this rebuild."""
        dataset = mapping.get("dataset_id")
        if legacy_fec_evidence(row, mapping):
            # The post-load pass reads these once, then applies the proved kind
            # to every alias, including witnesses imported before this producer.
            return None
        context = row.get("receipt_context") or {}
        sources = [row.get(key) for key in ("original_location", "source_url", "receipt_source_url")]
        sources += [context.get(key) for key in ("member", "source_url", "requested_url", "resolved_url")]
        suffixes = set()
        for source in sources:
            if isinstance(source, str):
                try:
                    suffixes.add(Path(urlsplit(source).path).suffix.lower())
                except ValueError:
                    pass  # A malformed location supplies no format evidence.
        # Historical CourtListener selection/download receipts have hash-only
        # locations. Inspect small originals; the content shape decides the role.
        receipt_candidate = obj["bytes"] <= 64 * 1024 and (
            bool(suffixes & {".json", ".jsonl"}) or dataset == "family:court-citations"
        )
        # These formats have no supported native metadata extractor. In particular
        # media filenames and capture dates never become meeting dates.
        if (suffixes & {".vtt", ".m3u8", ".pdf", ".zip", ".mp3", ".mp4"}
                and not receipt_candidate):
            self.metadata_counts["unsupported_format_locations"] += 1
            return None
        if dataset and dataset not in SUPPORTED_DATASETS and not receipt_candidate:
            return None
        key = dataset or ""
        cached = self.db.execute(
            "SELECT data FROM native_metadata_cache WHERE sha256=? AND dataset=?", (sha, key)
        ).fetchone()
        if cached:
            self.metadata_counts["cache_hits"] += 1
            return json.loads(cached[0])
        limit = (4 if dataset == "family:federal-register" else 1) * 1024 * 1024
        if obj["bytes"] > limit:
            result: dict[str, Any] = {"outcome": "not-read", "scope": {"reason": "byte-limit", "max_bytes": limit}}
        else:
            result = self.read_content(sha, obj, dataset)
            self.metadata_counts["verified_original_bytes"] += obj["bytes"]
            self.metadata_counts["verified_original_reads"] += 1
        self.metadata_counts["outcome_" + result["outcome"]] += 1
        self.db.execute("INSERT INTO native_metadata_cache VALUES(?,?,?)", (sha, key, encoded(result)))
        return result

    def read_content(self, sha: str, obj, dataset: str | None) -> dict[str, Any]:
        body = read_original(self.root, sha, obj)
        return extract_metadata(dataset, body)

    def summary(self) -> dict:
        if self.receipt_paths() != self.acquisition_paths:
            raise RuntimeError("Acquisition receipts changed during catalog rebuild; retry")
        result = holdings(self.db)
        result["metadata_extraction"] = dict(sorted(self.metadata_counts.items()))
        result["retained_classification"] = self.classification_counts
        return result


def merge_content(mapping: dict, content: dict, sha: str) -> dict:
    """Keep independent receipt and content claims in the existing mapping."""
    mapping = dict(mapping)
    mapping["content_metadata"] = {
        "raw_sha256": sha,
        **{
            key: content[key]
            for key in (
                "outcome",
                "classification",
                "content_kind",
                "producer_evidence",
                "scope",
                "primary_records",
                "native_id",
                "source_version",
                "dataset_id",
                "role",
                "error",
                "reason",
            )
            if key in content
        },
    }
    mapping["evidence"] = list(mapping.get("evidence", [])) + [
        dict(item, origin="publication-context" if item.get("origin") == "publication-context" else "content")
        for item in content.get("evidence", [])
    ]
    if content.get("outcome") != "ok" or content.get("role") == "acquisition-receipt":
        return mapping
    if not mapping.get("dataset_id") and content.get("dataset_id"):
        mapping.update(dataset_id=content["dataset_id"], mapping_status="mapped")
    receipt_id, content_id = mapping.get("native_id"), content.get("native_id")
    package = r"^(BILLS|BILLSTATUS|CHRG|CRPT|CPRT|PLAW|CDOC|TDOC|CREC|FR|CFR|GAOREPORTS)-"
    left = re.match(package, receipt_id) if isinstance(receipt_id, str) else None
    right = re.match(package, content_id) if isinstance(content_id, str) else None
    conflicts = {}
    if left and right and left[1] == right[1] and receipt_id != content_id:
        conflicts["receipt_content_native_id"] = [receipt_id, content_id]
    receipt_congress = {p["label"] for p in mapping.get("periods", []) if p["kind"] == "congress"}
    content_congress = {p["label"] for p in content.get("periods", []) if p["kind"] == "congress"}
    if (
        content.get("primary_records") == 1
        and len(receipt_congress) == len(content_congress) == 1
        and receipt_congress != content_congress
    ):
        conflicts["receipt_content_congress"] = sorted(receipt_congress | content_congress)
    if conflicts:
        mapping["field_conflicts"] = dict(mapping.get("field_conflicts", {}))
        mapping["field_conflicts"].update(conflicts)
        mapping["content_metadata"]["conflict"] = True
        mapping["native_id"] = None
        mapping["candidate_periods"] = list(mapping.get("periods", [])) + list(content.get("periods", []))
        mapping["periods"] = []
        return mapping
    # A publisher package ID and its contained record ID can legitimately differ.
    # Preserve both representations; never replace receipt identity with content.
    for field in ("native_id", "source_version"):
        if content.get(field) and not mapping.get(field) and field not in mapping.get("field_conflicts", {}):
            mapping[field] = content[field]
    periods = list(mapping.get("periods", []))
    for item in content.get("periods", []):
        if item not in periods:
            periods.append(dict(item, origin="publication-context" if item.get("origin") == "publication-context" else "content"))
    mapping["periods"] = periods
    return mapping


def holdings(db, dataset: str | None = None) -> dict:
    """Report custody and native/requested periods without a completeness claim."""
    clauses = ["f.collection_id!='archive:receipts'", "f.status!='receipt-only'"]
    params = []
    if dataset:
        clauses.append("f.collection_id=?")
        params.append(dataset)
    where = " AND ".join(clauses)
    datasets = [
        dict(row)
        for row in db.execute(
            f"""
        SELECT f.collection_id AS dataset, count(*) AS file_locations,
        count(DISTINCT f.sha256) AS raw_objects,
        sum(f.status='missing-bytes') AS missing_locations,
        sum(NOT EXISTS(SELECT 1 FROM main.raw_periods p WHERE p.file_id=f.id)) AS unknown_period_locations
        FROM files f WHERE {where} GROUP BY f.collection_id ORDER BY raw_objects DESC,f.collection_id
    """,
            params,
        )
    ]
    periods = [
        dict(row)
        for row in db.execute(
            f"""
        SELECT f.collection_id AS dataset,p.kind,p.label,count(DISTINCT f.sha256) AS raw_objects
        FROM files f JOIN main.raw_periods p ON p.file_id=f.id WHERE {where}
        GROUP BY f.collection_id,p.kind,p.label ORDER BY f.collection_id,p.kind,p.label
    """,
            params,
        )
    ]
    totals = dict(
        db.execute(
            f"""
        SELECT count(*) AS file_locations,count(DISTINCT f.sha256) AS raw_objects,
        count(DISTINCT CASE WHEN f.collection_id!='archive:unmapped' THEN f.sha256 END) AS mapped_raw_objects,
        count(DISTINCT CASE WHEN EXISTS(SELECT 1 FROM main.raw_periods p WHERE p.file_id=f.id)
          THEN f.sha256 END) AS raw_objects_with_period
        FROM files f WHERE {where}
    """,
            params,
        ).fetchone()
    )
    return {
        "totals": totals,
        "datasets": datasets,
        "periods": periods,
        "limit": "Native/requested periods are not proof of complete coverage. Unknowns remain explicit.",
    }
