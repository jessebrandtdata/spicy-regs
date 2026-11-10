"""Plan which superseded rollup generations to delete, reading only (owner decision 36, 2026-09-26).

Storage grows as runs times family bytes, so the owner chose to keep every
generation something cites and the last ``KEEP_LAST`` per family. Kept:

* the family's current generation and its predecessors, ``KEEP_LAST`` in
  all. Each root's ``readSnapshot`` is the index its publish swapped from, so
  its own family's entry there is the generation it replaced. A family the
  index no longer names keeps its newest ``KEEP_LAST`` by upload time;
* every generation that a hex prefix of eight or more digits anywhere under
  ``docs/`` names. The output ledger states its audits that way, and the MCP's
  qualification record is parsed from the ledger, so this covers both;
* every generation DocSpec pins in its ``docs/pins/fork-generations.json``;
* every generation the Congressional Tech archive pins in its
  ``pins/spicy-generations.json``;
* every generation holding a table the current rulemaking snapshot states as
  an input, by the table's sha256, so the snapshot's inputs stay readable;
* the managed parents a kept generation records, transitively, so its inputs
  stay readable;
* a generation that stopped being current less than ``GRACE`` ago, when its
  successor on the chain was written, since a run may still be reading it;
* a generation off the chain written after the current one: a publish uploads
  its prefix before it swaps the pointer, so it may be in flight. One off the
  chain and older than current lost its swap and was never read.

Everything else under ``generations/`` is planned for deletion. Source evidence
under ``source-evidence/`` is content-addressed and shared, and is not planned
here. Planning deletes nothing. ``execute`` takes an approved plan (one a
person reviewed, or the weekly run's own, decision 63) and deletes only the
prefixes a fresh plan also marks, so anything published, pinned or cited since
is spared. It deletes a generation's members and keeps its root
(``artifact.json``, tens of KB): an audit walks the roots' ``readSnapshot``
chain from a pinned generation back to a pinned prior, and a missing
intermediate root breaks that walk (spicy-stack-a8, 2026-09-27). A generation
holding only its root is reported as pruned and never planned again. Each
execution leaves its record under ``RECORDS``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from os import getenv
from pathlib import Path

from spicy_regs.output_ledger import LEDGER
from spicy_regs.sources import publication

PLAN_FORMAT = "spicy-regs-generation-retention-plan"
KEEP_LAST = 3
#: A rollup reads its parents through the index it captured at start, and a
#: GitHub-hosted job runs at most 6 hours, so a generation stays readable that
#: long after it stops being current.
GRACE = timedelta(hours=6)
PREFIX = "generations/"
ROOT = "artifact.json"
RECORDS = "retention/"
#: DeleteObjects accepts at most this many keys per request.
DELETE_BATCH = 1000
DOCS = LEDGER.parents[1]
DOCSPEC_PINS = "https://raw.githubusercontent.com/mikewolfd/DocSpec/main/docs/pins/fork-generations.json"
ARCHIVE_PINS = "https://congressional-data.spicygov.ai/viewer-publications/pins/spicy-generations.json"
ARCHIVE_S3_PINS = "s3://congressional-tech-raw/viewer-publications/pins/spicy-generations.json"
DOCSPEC_PINS_FORMAT = "docspec-fork-generation-pins"
_KEY = re.compile(r"generations/([a-z][a-z0-9_-]*)/([0-9a-f]{64})/")
_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")
_HEX = re.compile(r"(?<![0-9a-f])[0-9a-f]{8,64}(?![0-9a-f])")
#: The last generation each family published with per-row receipts, for every table whose receipts are retired
#: (``subject_catalog.RECEIPTS_RETIRED``). The docs scan below keeps each one, as it keeps any digest a docs file
#: cites; :func:`receipt_archive_pins` reads and checks the entries.
RECEIPT_ARCHIVE = DOCS / "pins/receipt-archive.json"
RECEIPT_ARCHIVE_FORMAT = "spicy-regs-receipt-archive"
_DIGEST = re.compile(r"sha256:([0-9a-f]{64})\Z")
_ARCHIVE_DIGEST = re.compile(r"(?:sha256:)?([0-9a-f]{64})\Z")
_NEVER = datetime.min.replace(tzinfo=timezone.utc)


@dataclass
class Generation:
    family: str
    digest: str
    bytes: int = 0
    objects: int = 0
    #: Whether any object but the root is stored; a root alone is a pruned generation's chain link.
    has_members: bool = False
    newest: datetime = _NEVER
    keep: list[str] = field(default_factory=list)

    @property
    def prefix(self) -> str:
        return f"{PREFIX}{self.family}/{self.digest}"


def inventory(client, bucket: str) -> tuple[dict[str, Generation], list[str]]:
    """Every generation prefix in the bucket by digest, with its bytes; keys outside the layout come back apart."""
    found: dict[str, Generation] = {}
    stray: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for item in page.get("Contents", ()):
            match = _KEY.match(item["Key"])
            if match is None:
                stray.append(item["Key"])
                continue
            generation = found.setdefault(match[2], Generation(match[1], match[2]))
            if generation.family != match[1]:
                raise publication.PublicationError(f"Generation {match[2][:8]} is stored under two families")
            generation.bytes += item["Size"]
            generation.objects += 1
            generation.has_members |= not item["Key"].endswith(f"/{ROOT}")
            generation.newest = max(generation.newest, item["LastModified"])
    return found, stray


def note_pins(notes: Iterable[tuple[str, str]], generations: Mapping[str, Generation]) -> dict[str, set[str]]:
    """Digests that a hex run of eight or more digits in a note prefixes, with the notes that name each."""
    by_prefix: dict[str, list[str]] = {}
    for digest in generations:
        by_prefix.setdefault(digest[:8], []).append(digest)
    named: dict[str, set[str]] = {}
    for name, text in notes:
        for token in _HEX.findall(text):
            for digest in by_prefix.get(token[:8], ()):
                if digest.startswith(token):
                    named.setdefault(digest, set()).add(name)
    return named


def receipt_archive_pins(raw: bytes | None = None) -> dict[str, dict]:
    """Each retired table's archive pin, by dataset, checked: family, digest, publication instant, receipts key.

    The receipts key is the member under that very generation, so the pin names the bytes it keeps.
    """
    document = json.loads(RECEIPT_ARCHIVE.read_bytes() if raw is None else raw)
    if document.get("format") != RECEIPT_ARCHIVE_FORMAT or document.get("version") != 1:
        raise ValueError(f"{RECEIPT_ARCHIVE.name} is not a {RECEIPT_ARCHIVE_FORMAT} v1 document")
    pins = document["archives"]
    for dataset, pin in pins.items():
        match = _DIGEST.fullmatch(str(pin.get("artifactDigest")))
        family = pin.get("family")
        if (set(pin) != {"family", "artifactDigest", "publishedAt", "receipts"} or match is None
                or not isinstance(family, str) or not _NAME.fullmatch(family)
                or pin["receipts"] != f"{PREFIX}{family}/{match[1]}/etl_receipts.parquet"):
            raise ValueError(f"Invalid receipt archive pin for {dataset}: {pin}")
        datetime.fromisoformat(pin["publishedAt"])
    return pins


def docspec_pins(raw: bytes, *, label: str = "DocSpec") -> dict[str, str]:
    """Pinned digests and why each is held; a document outside the stated shape refuses."""
    try:
        document = json.loads(raw)
        if document["format"] != DOCSPEC_PINS_FORMAT or document["version"] != 1:
            raise ValueError("unknown format")
        pins = {}
        for pin in document["pins"]:
            match = _DIGEST.fullmatch(pin["artifactDigest"])
            if match is None or not isinstance(pin["family"], str):
                raise ValueError("invalid pin")
            pins[match[1]] = f"{label} {pin['role']} pin ({pin['cites']})"
    except (ValueError, TypeError, KeyError) as exc:
        raise publication.PublicationError(f"{label} pins are not a docspec-fork-generation-pins v1 document") from exc
    return pins


def archive_pins(raw: bytes) -> dict[str, str]:
    """The Congressional Tech archive's pinned digests (``pins/spicy-generations.json``); another shape refuses.

    The archive writes ``{"version": 1, "generations": [{"family", "digest"}]}``, the generations also as a mapping
    of family to digest, a digest bare or ``sha256:``-prefixed; DocSpec's ``docspec-fork-generation-pins`` v1 is
    accepted too.
    """
    try:
        document = json.loads(raw)
        if not isinstance(document, Mapping) or document.get("version") != 1:
            raise ValueError("unknown version")
        if document.get("format", DOCSPEC_PINS_FORMAT) != DOCSPEC_PINS_FORMAT:
            raise ValueError("unknown format")
        if "pins" in document:
            return docspec_pins(raw, label="Archive")
        entries = document["generations"]
        if isinstance(entries, Mapping):
            entries = [{"family": family, **(target if isinstance(target, Mapping) else {"digest": target})}
                       for family, target in entries.items()]
        if isinstance(entries, (str, bytes)) or not isinstance(entries, Sequence):
            raise ValueError("invalid generations")
        pins: dict[str, str] = {}
        for entry in entries:
            family, digest = entry["family"], entry.get("digest", entry.get("artifactDigest"))
            match = _ARCHIVE_DIGEST.fullmatch(digest) if isinstance(digest, str) else None
            if match is None or not isinstance(family, str) or not family:
                raise ValueError("invalid pin")
            pins[match[1]] = f"Archive {entry.get('role') or 'archive'} pin ({entry.get('cites') or family})"
    except (ValueError, TypeError, KeyError, AttributeError, publication.PublicationError) as exc:
        raise publication.PublicationError("Archive pins are not a spicy-generations v1 document") from exc
    return pins


#: The archive's pin document has two homes; a read that fails at one tries the other.
_ARCHIVE_HOMES = {ARCHIVE_PINS: ARCHIVE_S3_PINS, ARCHIVE_S3_PINS: ARCHIVE_PINS}


def _read_one(source: str, client) -> bytes:
    if source.startswith("s3://"):
        pin_bucket, _, pin_key = source.removeprefix("s3://").partition("/")
        if not pin_key:
            raise publication.PublicationError(f"Invalid S3 pin URI: {source}")
        if client is None:
            from spicy_regs.sources import r2

            client = r2.get_r2_client()
        stored = publication._get_bounded(client, pin_bucket, pin_key)
        if stored is None:
            raise publication.PublicationError(f"Pin document is missing in S3: {source}")
        return stored[0]
    if source.startswith("https://"):
        raw = publication._bounded_get(source, allow_missing=False)
        assert raw is not None
        return raw
    return Path(source).read_bytes()


def _read_pin_source(source: str, *, client=None) -> bytes:
    """Read a pin document from an ``s3://`` URI, an ``https://`` URL, or a local path."""
    from botocore.exceptions import BotoCoreError, ClientError
    import httpx

    try:
        return _read_one(source, client)
    except (publication.PublicationError, httpx.HTTPError, BotoCoreError, ClientError) as exc:
        other = _ARCHIVE_HOMES.get(source)
        if other is None:
            raise
        print(f"{source} unreadable ({exc}); reading {other}", file=sys.stderr)
        return _read_one(other, client)


def _snapshot_inputs(client, bucket: str) -> tuple[str, dict[str, str], dict[str, str]]:
    """The current rulemaking snapshot's id and its source tables' sha256 by table key; empty when none is published."""
    pointer = publication._get_bounded(client, bucket, publication.SNAPSHOT_POINTER)
    if pointer is None:
        return "", {}, {}
    head = json.loads(pointer[0])
    stored = publication._get_bounded(client, bucket, head["manifest_key"])
    if stored is None:
        raise publication.PublicationError(f"Rulemaking pointer names a missing manifest: {head['manifest_key']}")
    inputs = json.loads(stored[0]).get("inputs", {})
    sources = inputs.get("sources", {})
    native = {name: value["publication"]["artifactDigest"].removeprefix("sha256:")
              for name, value in inputs.get("native_inputs", {}).items() if "publication" in value}
    return head["snapshot_id"], {key: f"sha256:{value['sha256']}" for key, value in sources.items()}, native


def plan(client, bucket: str, *, notes: Iterable[tuple[str, str]], docspec: Mapping[str, str],
         archive: Mapping[str, str] | None = None, now: datetime) -> dict:
    """Mark every generation kept or deletable under decision 36 and return the plan as a JSON-able record."""
    from rulespec_artifacts import expected_artifact_digest, parse_canonical_json

    archive_map = dict(archive or {})
    index, etag = publication._stored_index(client, bucket)
    generations, stray = inventory(client, bucket)
    roots: dict[str, dict | None] = {}

    def root(digest: str) -> dict | None:
        """The generation's own root, checked against its digest; ``None`` while it is still uploading."""
        if digest not in roots:
            stored = publication._get_bounded(client, bucket, f"{generations[digest].prefix}/{ROOT}")
            value = None if stored is None else parse_canonical_json(stored[0])
            if value is not None and expected_artifact_digest(value) != f"sha256:{digest}":
                raise publication.PublicationError(f"Root of {generations[digest].prefix} differs from its digest")
            roots[digest] = value
        return roots[digest]

    def keep(digest: str, reason: str) -> bool:
        generation = generations.get(digest)
        if generation is None:
            return False
        first = not generation.keep
        generation.keep.append(reason)
        return first

    #: Table descriptors (with sha256) per chain generation: the index states
    #: the current one's, and each root's readSnapshot the one it replaced.
    described: dict[str, Mapping] = {}
    by_family: dict[str, list[Generation]] = {}
    for generation in generations.values():
        by_family.setdefault(generation.family, []).append(generation)
    for family, members in by_family.items():
        entry = index["families"].get(family)
        if entry is None:
            for generation in sorted(members, key=lambda g: g.newest, reverse=True)[:KEEP_LAST]:
                keep(generation.digest, f"last {KEEP_LAST} by upload time: {family} is not in the index")
            continue
        current = entry["artifactDigest"].removeprefix("sha256:")
        if current not in generations:
            raise publication.PublicationError(f"Current {family} generation {current[:8]} is not in the listing")
        chain: list[str] = []
        step: str | None = current
        described[current] = entry["tables"]
        while step is not None and step in generations and step not in chain:
            chain.append(step)
            replaced = (root(step) or {}).get("spec", {}).get("readSnapshot", {}).get("families", {}).get(family)
            step = replaced["artifactDigest"].removeprefix("sha256:") if replaced else None
            if replaced:
                described.setdefault(replaced["artifactDigest"].removeprefix("sha256:"), replaced["tables"])
        for place, digest in enumerate(chain):
            superseded = generations[chain[place - 1]].newest if place else now
            if place < KEEP_LAST:
                keep(digest, "current" if place == 0 else f"last {KEEP_LAST}: {place} before current")
            elif now - superseded < GRACE:
                keep(digest, f"current until {superseded:%Y-%m-%dT%H:%MZ}, within the {GRACE.seconds // 3600}h grace")
        for generation in members:
            if generation.digest not in chain and generation.newest > generations[current].newest:
                keep(generation.digest, "written after the current generation: a publish may be in flight")
    for digest, names in note_pins(notes, generations).items():
        keep(digest, "named in " + ", ".join(sorted(names)))
    for digest, reason in docspec.items():
        keep(digest, reason)
    for digest, reason in archive_map.items():
        keep(digest, reason)
    snapshot_id, inputs, native_inputs = _snapshot_inputs(client, bucket)
    for name, digest in native_inputs.items():
        if digest not in generations or not generations[digest].has_members:
            raise publication.PublicationError(f"Native rulemaking input generation is missing: {name}/{digest}")
        keep(digest, f"native input of rulemaking {snapshot_id[:17]} ({name})")
    for key, sha256 in inputs.items():
        for digest, tables in described.items():
            if (tables.get(key) or {}).get("sha256") == sha256:
                keep(digest, f"input of rulemaking {snapshot_id[:17]} ({key})")

    pending = [digest for digest, generation in generations.items() if generation.keep]
    while pending:
        child = generations[pending.pop()]
        for key, parent in ((root(child.digest) or {}).get("spec", {}).get("parents") or {}).items():
            if "artifactDigest" in parent:
                read = parent["artifactDigest"].removeprefix("sha256:")
                if keep(read, f"parent of {child.family}/{child.digest[:8]} ({key})"):
                    pending.append(read)

    families: dict[str, dict] = {}
    for generation in sorted(generations.values(), key=lambda g: (g.family, g.newest), reverse=True):
        family = families.setdefault(generation.family, {
            "current": (index["families"].get(generation.family) or {}).get("artifactDigest"), "generations": []})
        family["generations"].append({
            "digest": generation.digest, "bytes": generation.bytes, "objects": generation.objects,
            "newest": generation.newest.isoformat(),
            "keep": generation.keep, "pruned": not generation.has_members,
        })
    pruned = [g for g in generations.values() if not g.has_members]
    kept = [g for g in generations.values() if g.keep and g.has_members]
    deleted = sorted((g for g in generations.values() if not g.keep and g.has_members), key=lambda g: g.prefix)
    return {
        "format": PLAN_FORMAT, "version": 1, "planned_at": now.isoformat(), "bucket": bucket,
        "index_etag": etag, "keep_last": KEEP_LAST,
        "totals": {
            "generations": len(generations), "bytes": sum(g.bytes for g in generations.values()),
            "keep_generations": len(kept), "keep_bytes": sum(g.bytes for g in kept),
            "delete_generations": len(deleted), "delete_bytes": sum(g.bytes for g in deleted),
            "pruned_generations": len(pruned), "pruned_bytes": sum(g.bytes for g in pruned),
        },
        "unlisted_docspec_pins": sorted(set(docspec) - set(generations)),
        "unlisted_archive_pins": sorted(set(archive_map) - set(generations)),
        "stray_keys": stray,
        "delete": [g.prefix for g in deleted],
        "families": dict(sorted(families.items())),
    }


def execute(client, bucket: str, approved: Mapping, fresh: Mapping) -> dict:
    """Delete what both the reviewed plan and a fresh one mark deletable, and store the record of it."""
    if approved.get("format") != PLAN_FORMAT or approved.get("version") != 1 or approved.get("bucket") != bucket:
        raise publication.PublicationError(f"The approved plan is not a {PLAN_FORMAT} v1 plan for {bucket}")
    doomed = sorted(set(approved["delete"]) & set(fresh["delete"]))
    for prefix in doomed:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
        keys = sorted(item["Key"] for page in pages for item in page.get("Contents", ()))
        root = f"{prefix}/{ROOT}"
        members = [key for key in keys if key != root]
        for start in range(0, len(members), DELETE_BATCH):
            batch = members[start:start + DELETE_BATCH]
            failed = client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True}).get("Errors")
            if failed:
                raise publication.PublicationError(f"Deleting {prefix} failed for {len(failed)} objects")
    record = {
        "format": "spicy-regs-generation-retention-record", "version": 1,
        "approved_plan": approved["planned_at"], "fresh_plan": fresh["planned_at"],
        "deleted": doomed, "spared": sorted(set(approved["delete"]) - set(doomed)),
    }
    client.put_object(Bucket=bucket, Key=f"{RECORDS}{fresh['planned_at']}.json", IfNoneMatch="*",
                      Body=json.dumps(record, indent=2).encode(), ContentType="application/json")
    return record


def summary(record: Mapping) -> list[str]:
    """A per-family table of the plan for a person to review."""
    gib = 1024 ** 3
    lines = ["| family | generations | keep | keep GiB | delete | delete GiB |", "|---|---:|---:|---:|---:|---:|"]
    for family, entry in record["families"].items():
        held = [g for g in entry["generations"] if g["keep"] and not g.get("pruned")]
        gone = [g for g in entry["generations"] if not g["keep"] and not g.get("pruned")]
        lines.append(f"| {family} | {len(entry['generations'])} | {len(held)} | "
                     f"{sum(g['bytes'] for g in held) / gib:.2f} | {len(gone)} | "
                     f"{sum(g['bytes'] for g in gone) / gib:.2f} |")
    totals = record["totals"]
    lines.append(f"| **all** | {totals['generations']} | {totals['keep_generations']} | "
                 f"{totals['keep_bytes'] / gib:.2f} | {totals['delete_generations']} | "
                 f"{totals['delete_bytes'] / gib:.2f} |")
    if totals.get("pruned_generations"):
        lines.append(f"\nPruned generations kept as roots for audit chains: {totals['pruned_generations']} "
                     f"({totals['pruned_bytes'] / 1024 ** 2:.1f} MiB)")
    if record["unlisted_docspec_pins"]:
        lines.append(f"\nDocSpec pins no longer stored: {', '.join(d[:8] for d in record['unlisted_docspec_pins'])}")
    if record.get("unlisted_archive_pins"):
        lines.append(f"\nArchive pins no longer stored: {', '.join(d[:8] for d in record['unlisted_archive_pins'])}")
    if record["stray_keys"]:
        lines.append(f"\nKeys under {PREFIX} outside the generation layout, left alone: {len(record['stray_keys'])}")
    return lines


def _notes() -> list[tuple[str, str]]:
    return [(str(path.relative_to(DOCS.parent)), path.read_text(encoding="utf-8"))
            for path in sorted(DOCS.rglob("*")) if path.suffix in {".md", ".json"} and path.is_file()]


def main(argv: Sequence[str] | None = None) -> int:
    from spicy_regs.sources import r2

    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--output", type=Path, required=True, help="Write the JSON plan here")
    parser.add_argument("--docspec-pins", default=DOCSPEC_PINS, help="DocSpec's pin document, an s3:// URI, a URL, or a path")
    parser.add_argument("--archive-pins", default=ARCHIVE_PINS, help="Archive's pin document, an s3:// URI, a URL, or a path")
    parser.add_argument("--execute", type=Path, help="A reviewed plan: delete what it and this fresh plan both mark")
    args = parser.parse_args(argv)
    bucket = getenv("R2_BUCKET_NAME")
    if not bucket:
        parser.error("R2_BUCKET_NAME is not set")
    client = r2.get_r2_client()
    raw = _read_pin_source(args.docspec_pins, client=client)
    archive_raw = _read_pin_source(args.archive_pins, client=client)
    record = plan(
        client,
        bucket,
        notes=_notes(),
        docspec=docspec_pins(raw),
        archive=archive_pins(archive_raw),
        now=datetime.now(timezone.utc),
    )
    args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print("\n".join(summary(record)))
    if args.execute:
        done = execute(client, bucket, json.loads(args.execute.read_text(encoding="utf-8")), record)
        print(f"\nDeleted {len(done['deleted'])} generations; spared {len(done['spared'])} the fresh plan keeps. "
              f"Record: {RECORDS}{record['planned_at']}.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
