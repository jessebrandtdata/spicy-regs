"""Publish one locally built generation of a family that no hosted workflow builds.

``--dry-run`` runs the real publication guards against a private disk store seeded with the live
index: no credentials are read and nothing leaves this machine. Without it the command writes to
the verified R2 target, then reads the family back as an anonymous reader and records what was
served. The per-family steps are in docs/local-family-refresh.md.
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
from spicy_regs.source_evidence import ARCHIVE_REFERENCE, INPUT_ROLE
from spicy_regs.sources import publication, r2
from spicy_regs.sources.publication import PublicationError

#: Families refreshed by hand. A family with a hosted workflow is published by that workflow, never from here.
LOCAL_FAMILIES = frozenset({"administration-policy", "grants-opportunities", "ofac-sanctions", "treasury-fiscal"})
RAW_ROOT_ENV = "SPICYGOV_RAW_ROOT"


def _candidate(directory: Path, family: str) -> dict:
    """The generation's root, refused unless it is the expected family; publication admits its members later."""
    root = json.loads((directory / "artifact.json").read_bytes())
    if root["spec"]["family"] != family:
        raise PublicationError(f"Generation is family {root['spec']['family']}, not the expected {family}")
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


def _raw_roots(evidence: tuple[Path, ...]) -> list[str]:
    """The raw archive each evidence directory was built on, from the pointer its build left beside it."""
    pointers = [directory.parent / ARCHIVE_REFERENCE for directory in evidence]
    return [json.loads(pointer.read_bytes())["root"] for pointer in pointers if pointer.exists()]


def _require_shared_raw(evidence: tuple[Path, ...]) -> None:
    """Refuse a build whose originals are in a scratch raw archive.

    A build's evidence pins and tables are the same on a scratch and on the shared archive; only this
    pointer differs. Publishing a scratch build would cite originals the shared archive never received.
    """
    from spicy_docs.storage.raw_archive import default_raw_archive_root

    if os.environ.get(RAW_ROOT_ENV):
        raise PublicationError(f"Unset {RAW_ROOT_ENV}: a publish reads its originals from the shared raw archive")
    shared = default_raw_archive_root().resolve()
    if scratch := sorted(root for root in _raw_roots(evidence) if Path(root).resolve() != shared):
        raise PublicationError("Generation was built on another raw archive than the shared one: " + ", ".join(scratch))


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


def dry_run(args, root: dict) -> dict:
    """Every guard of a real publish, against a disk store holding a copy of the live index."""
    from spicy_regs.sources.disk_store import DiskStore

    raw = args.live_index.read_bytes() if args.live_index else publication.load_index_bytes(r2.VERIFIED_PUBLIC_URL)
    if raw is None:
        raise PublicationError("The public index is absent; pass --live-index for a rehearsal without one")
    prior = publication.parse_index(raw)
    live = prior["families"].get(args.family)
    _require_prior(live, args.expect_prior, args.family)
    with TemporaryDirectory(dir=args.output, prefix="dry-run-store-") as scratch, _no_local_side_effects():
        store = DiskStore(Path(scratch) / "store", bucket="dry-run")
        store.put_object(Bucket=store.bucket, Key=publication.INDEX_V2_KEY, Body=raw)
        after = publication.publish_generation(
            args.generation, client=store, bucket=store.bucket, prior_index=prior,
            evidence_directories=tuple(args.evidence), added_tables=frozenset(args.added_table),
        )
        puts = [call for call in store.calls if call["method"] in ("put_object", "upload_part")]
    return _receipt(
        "dry_run_accepted", root, after["families"][args.family], live,
        added_tables=sorted(args.added_table),
        other_family_changes=_unrelated_changes(prior, after, args.family),
        raw_roots=_raw_roots(tuple(args.evidence)),
        store_bytes_written=sum(call["bytes"] for call in puts),
    )


def publish(args, root: dict) -> dict:
    """Publish to the verified target unless the family already is this generation, then read it back publicly."""
    from dotenv import load_dotenv

    _require_shared_raw(tuple(args.evidence))
    load_dotenv(args.env_file)
    r2.require_credentials("Local generation publication")
    client, bucket = r2.verified_target_client()
    before, _ = publication._stored_index(client, bucket)
    live = before["families"].get(args.family)
    # A rerun after an uncertain result finds its own generation selected: it writes nothing and only reads back.
    write = live is None or live["artifactDigest"] != root["artifactDigest"]
    if write:
        _require_prior(live, args.expect_prior, args.family)
        publication.publish_generation(
            args.generation, client=client, bucket=bucket, prior_index=before,
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
    args = parser.parse_args(argv)
    if args.live_index and not args.dry_run:
        parser.error("--live-index is a dry-run input; a publish compares with the stored index")
    root = _candidate(args.generation, args.family)
    args.output.mkdir(parents=True)
    receipt = dry_run(args, root) if args.dry_run else publish(args, root)
    name = "dry-run.json" if args.dry_run else "receipt.json"
    (args.output / name).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
