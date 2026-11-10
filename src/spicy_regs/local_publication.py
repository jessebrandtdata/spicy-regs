"""Publish one locally built generation of a family that no hosted workflow builds.

``--dry-run`` runs the real publication guards against a private disk store seeded with the public
index: no credentials are read and nothing leaves this machine. Without it the command compares with
the stored index, writes to the verified R2 target, then reads the family back as an anonymous reader
and records what was served. The per-family steps are in docs/local-family-refresh.md.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from spicy_regs.catalog_refresh_request import REQUEST_ENV
from spicy_regs.generations import implementation_id
from spicy_regs.source_evidence import ARCHIVE_REFERENCE, INPUT_ROLE, JOURNAL
from spicy_regs.sources import publication, r2
from spicy_regs.sources.publication import PublicationError

#: Families refreshed by hand. A family with a hosted workflow is published by that workflow, never from here.
LOCAL_FAMILIES = frozenset({"administration-policy", "executive-orders", "grants-opportunities", "ofac-sanctions",
                            "treasury-fiscal"})
RAW_ROOT_ENV = "SPICYGOV_RAW_ROOT"
#: Either one loosens the publisher's shrink guard (``r2._assert_upload_safe``).
SHRINK_OVERRIDES = ("R2_ALLOW_SHRINK", "R2_MIN_SIZE_RATIO")


def _candidate(directory: Path, family: str, evidence: tuple[Path, ...]) -> dict:
    """The generation's root, refused unless it is the expected family and pins source evidence.

    All four families build with evidence, so a root without it is not one of their builds. Publication
    admits the members and holds ``evidence`` to the root's pins later.
    """
    root = json.loads((directory / "artifact.json").read_bytes())
    if root["spec"]["family"] != family:
        raise PublicationError(f"Generation is family {root['spec']['family']}, not the expected {family}")
    if not evidence or not any(item["role"] == INPUT_ROLE for item in root["inputs"]):
        raise PublicationError(f"A {family} generation is published with the source evidence it pins; pass --evidence")
    return root


def _require_prior(live: dict | None, expected: str | None, family: str) -> None:
    """Refuse when the live family is not the generation the build was compared against."""
    if expected is None:
        return
    wanted = expected.removeprefix(f"generations/{family}/").removeprefix("sha256:")
    if not re.fullmatch(r"[0-9a-f]{8,64}", wanted):
        raise ValueError("--expect-prior takes a generation digest, its prefix path or its first 8 or more hex digits")
    held = live["artifactDigest"].removeprefix("sha256:") if live else None
    if held is None or not held.startswith(wanted):
        raise PublicationError(
            f"Live {family} is {held or 'absent'}, not the expected prior {wanted}; compare the build with it again"
        )


def _cited_originals(evidence: Path) -> set[str]:
    """The digest of every original an evidence journal says the build read or downloaded."""
    cited = set()
    for line in (evidence / JOURNAL).read_text().splitlines():
        event = json.loads(line)
        if event["event"] in ("capture", "retained-file") and event.get("sha256"):
            cited.add(event["sha256"])
    return cited


def _require_shared_raw(evidence: tuple[Path, ...]) -> None:
    """Refuse a build unless every original it cites is in the shared raw archive.

    A scratch build's tables and evidence pins equal a real build's, so two things are checked. The pointer
    each build leaves beside its evidence must name the shared archive; an evidence directory moved away from
    its pointer is refused. And each original the journal cites must be present in the shared archive by
    digest, which also catches a build that read its originals from another archive than its pointer names.
    Presence is checked by name; the archive's own verification hashes the bytes.
    """
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root

    if os.environ.get(RAW_ROOT_ENV):
        raise PublicationError(f"Unset {RAW_ROOT_ENV}: a publish reads its originals from the shared raw archive")
    shared = default_raw_archive_root().resolve()
    store = CentralRawArchiveStore(shared, create=False)
    for directory in evidence:
        pointer = directory.parent / ARCHIVE_REFERENCE
        if not pointer.exists():
            raise PublicationError(f"{directory} is not beside the {ARCHIVE_REFERENCE} its build wrote; "
                                   "publish the evidence from its build directory")
        if Path(json.loads(pointer.read_bytes())["root"]).resolve() != shared:
            raise PublicationError("Generation was built on another raw archive than the shared one")
        absent = []
        for digest in sorted(_cited_originals(directory)):
            try:
                with store.open(digest):
                    pass
            except FileNotFoundError:
                absent.append(digest)
        if absent:
            raise PublicationError(f"{len(absent)} cited original(s) are not in the shared raw archive, "
                                   f"first {absent[0]}; acquire into it before publishing")


def _require_default_thresholds() -> None:
    """Refuse when the shrink guard is loosened, so a dry run and the publish it rehearses apply one threshold."""
    if loosened := sorted(name for name in SHRINK_OVERRIDES if os.environ.get(name)):
        raise PublicationError("Unset " + " and ".join(loosened) + ": a hand publish keeps the default shrink guard")


def _prior_index(root: dict, stored: dict) -> dict:
    """The index the build read, when it recorded one: the publisher then refuses a family changed since."""
    return publication.parse_index(json.dumps(root["spec"]["readSnapshot"]).encode()) if (
        root["spec"]["readSnapshot"]) else stored


@contextmanager
def _no_local_side_effects():
    """A dry run neither mirrors its unpublished generation nor asks the catalogue to refresh."""
    held = {name: os.environ.pop(name) for name in (publication.PUBLICATION_MIRROR, REQUEST_ENV) if name in os.environ}
    try:
        yield
    finally:
        os.environ.update(held)


def _public_readback(family: str, entry: dict) -> int:
    """Hold what an anonymous reader is served to the published pins; returns the member bytes read."""
    served = publication.load_index(r2.VERIFIED_PUBLIC_URL)["families"].get(family)
    if served != entry:
        raise PublicationError(f"The public index serves another {family} generation than the one published")
    publication.load_family_root(r2.VERIFIED_PUBLIC_URL, entry)
    return sum(
        publication.verify_member(r2.VERIFIED_PUBLIC_URL, member)
        for member in publication.generation_files(entry).values() if member.sha256 is not None
    )


def _receipt(status: str, root: dict, entry: dict, prior: dict | None, **facts) -> dict:
    prior_tables = (prior or {}).get("tables", {})
    return {
        "status": status,
        "observed_at": datetime.now(UTC).isoformat(),
        "family": root["spec"]["family"],
        **{key: entry.get(key) for key in ("prefix", "artifactDigest", "logicalId", "publishedAt")},
        "prior": prior and {key: prior.get(key) for key in ("prefix", "artifactDigest", "publishedAt")},
        "tables": {
            key: {**{field: table[field] for field in ("rows", "byteSize", "sha256") if field in table},
                  "priorRows": prior_tables.get(key, {}).get("rows")}
            for key, table in sorted(entry["tables"].items())
        },
        "etlReceipts": entry.get("etlReceipts") and {
            field: entry["etlReceipts"][field] for field in ("key", "rows", "byteSize", "sha256")},
        "evidence": [{key: item[key] for key in ("logicalId", "artifactDigest")}
                     for item in root["inputs"] if item["role"] == INPUT_ROLE],
        "verifier_implementation_id": implementation_id(),
        **facts,
    }


def _unrelated_changes(before: dict, after: dict, family: str) -> list[str]:
    names = (before["families"].keys() | after["families"].keys()) - {family}
    return sorted(name for name in names if before["families"].get(name) != after["families"].get(name))


def _seed_unchanged_members(store, live: dict | None, generation: Path) -> None:
    """Give the dry-run store each live member the candidate holds unchanged, from the candidate's own file.

    The publisher copies such a member inside the bucket instead of uploading it, so the copy needs its
    source. The bytes are the live member's by digest; that R2 still holds it is what a dry run cannot see.
    """
    for key, member in publication.generation_files(live or {"prefix": "", "tables": {}}).items():
        local = generation / key
        if (member.sha256 is not None and local.is_file() and local.stat().st_size == member.byte_size
                and publication.file_identity(local)["sha256"] == member.sha256):
            with local.open("rb") as body:
                store.put_object(Bucket=store.bucket, Key=member.path, Body=body)


def dry_run(args, root: dict) -> dict:
    """Every guard of a real publish, against a disk store holding a copy of the public index."""
    from spicy_regs.sources.disk_store import DiskStore

    if not args.scratch_raw:
        _require_shared_raw(tuple(args.evidence))
    _require_default_thresholds()
    raw = args.live_index.read_bytes() if args.live_index else publication.load_index_bytes(r2.VERIFIED_PUBLIC_URL)
    if raw is None:
        raise PublicationError("The public index is absent; pass --live-index for a rehearsal without one")
    stored = publication.parse_index(raw)
    live = stored["families"].get(args.family)
    _require_prior(live, args.expect_prior, args.family)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=args.output.parent, prefix=".dry-run-store-") as scratch, _no_local_side_effects():
        store = DiskStore(Path(scratch) / "store", bucket="dry-run")
        store.put_object(Bucket=store.bucket, Key=publication.INDEX_V2_KEY, Body=raw)
        _seed_unchanged_members(store, live, args.generation)
        seeded = len(store.calls)
        after = publication.publish_generation(
            args.generation, client=store, bucket=store.bucket, prior_index=_prior_index(root, stored),
            evidence_directories=tuple(args.evidence), added_tables=frozenset(args.added_table),
        )
        # Bytes a publish would upload: the pointer and every member and evidence blob it does not copy.
        puts = [call for call in store.calls[seeded:] if call["method"] in ("put_object", "upload_part")]
        copies = [call for call in store.calls if call["method"] == "copy_object"]
    return _receipt(
        "dry_run_accepted", root, after["families"][args.family], live,
        added_tables=sorted(args.added_table),
        other_family_changes=_unrelated_changes(stored, after, args.family),
        raw_archive="scratch: not publishable" if args.scratch_raw else "shared",
        store_bytes_written=sum(call["bytes"] for call in puts),
        store_members_copied=len(copies),
    )


def publish(args, root: dict) -> dict:
    """Publish to the verified target unless the family already is this generation, then read it back publicly."""
    from dotenv import load_dotenv

    _require_shared_raw(tuple(args.evidence))
    load_dotenv(args.env_file)
    _require_default_thresholds()
    r2.require_credentials("Local generation publication")
    client, bucket = r2.verified_target_client()
    before, _ = publication._stored_index(client, bucket)
    live = before["families"].get(args.family)
    # A rerun after an uncertain result finds its own generation selected: it writes nothing and only reads back.
    write = live is None or live["artifactDigest"] != root["artifactDigest"]
    if write:
        if live is not None and args.expect_prior is None:
            raise PublicationError(
                f"{args.family} is live: pass --expect-prior with the digest the dry run printed, so a generation "
                "published since then is not replaced unseen")
        _require_prior(live, args.expect_prior, args.family)
        publication.publish_generation(
            args.generation, client=client, bucket=bucket, prior_index=_prior_index(root, before),
            evidence_directories=tuple(args.evidence), added_tables=frozenset(args.added_table),
        )
    after, _ = publication._stored_index(client, bucket)
    entry = after["families"].get(args.family)
    if entry is None or entry["artifactDigest"] != root["artifactDigest"]:
        raise PublicationError(f"Stored {args.family} is not this generation after publication")
    return _receipt(
        "published_public_readback" if write else "already_published_public_readback", root, entry,
        live if write else None,
        added_tables=sorted(args.added_table),
        other_family_changes=_unrelated_changes(before, after, args.family),
        public_index_url=f"{r2.VERIFIED_PUBLIC_URL}/{publication.INDEX_V2_KEY}",
        public_bytes_read=_public_readback(args.family, entry),
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generation", type=Path, required=True, help="The built generation directory")
    parser.add_argument("--family", required=True, choices=sorted(LOCAL_FAMILIES), help="The family it must be")
    parser.add_argument("--evidence", type=Path, action="append", default=[],
                        help="A source-evidence artifact directory the generation pins, repeatable")
    parser.add_argument("--added-table", action="append", default=[],
                        help="A table key (NAME.parquet) this generation adds to the family, repeatable")
    parser.add_argument("--expect-prior", help="Digest (or its first 8+ hex digits) of the live generation the "
                                               "build was compared against")
    parser.add_argument("--output", type=Path, required=True, help="A fresh directory for the receipt")
    parser.add_argument("--dry-run", action="store_true", help="Run every guard on a private disk store; no credentials")
    parser.add_argument("--live-index", type=Path, help="Dry run only: a saved publication.v2.json instead of the "
                                                        "public one")
    parser.add_argument("--env-file", type=Path, default=Path(".env"), help="Publish only: where R2_* values load from")
    parser.add_argument("--scratch-raw", action="store_true",
                        help="Dry run only: the build is a rehearsal on a scratch raw archive, which a publish refuses")
    args = parser.parse_args(argv)
    if (args.live_index or args.scratch_raw) and not args.dry_run:
        parser.error("--live-index and --scratch-raw are dry-run inputs; a publish uses the stored index "
                     "and the shared raw archive")
    if args.output.exists():
        parser.error(f"--output {args.output} exists; a receipt directory is written once")
    root = _candidate(args.generation, args.family, tuple(args.evidence))
    receipt = dry_run(args, root) if args.dry_run else publish(args, root)
    # Created only now, so a refused run leaves nothing behind and the same command can be run again.
    args.output.mkdir(parents=True)
    name = "dry-run.json" if args.dry_run else "receipt.json"
    (args.output / name).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
