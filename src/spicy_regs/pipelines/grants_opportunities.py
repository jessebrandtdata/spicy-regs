"""Acquire Grants.gov daily exports into the raw archive, then build observations from selected snapshots.

A capture selection names each snapshot as its Grants.gov scope: the export date and the request URL,
SHA-256, observation time and size of its ZIP, as the raw acquisition receipt records them. The build admits
one source release per scope from the raw original; releases admitted earlier can be selected instead.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    add_snapshot_build, download_original, journal_snapshot_releases, snapshot_inputs,
)
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_grants_opportunities import OUTPUT, SCHEMA, TABLE, build_grants_opportunities

STAGE = "grants-source-archive"
RELEASE_EVENT = "grants-source-release"


def stated_version(snapshot_date: str, *, transport=None) -> tuple[str, int, str] | None:
    """The URL, size and ETag the publisher states for one dated export now, or ``None`` while it has none.

    The bucket answers 403 for an object that does not exist; any other failure raises.
    """
    import httpx
    from spicy_docs.sources import grants_gov

    url = f"{grants_gov.EXPORT_BASE}GrantsDBExtract{snapshot_date.replace('-', '')}v2.zip"
    with httpx.Client(transport=transport, timeout=60) as client:
        stated = client.head(url, headers={"User-Agent": "spicy-regs", "Accept-Encoding": "identity"})
    if stated.status_code in (403, 404):
        return None
    stated.raise_for_status()
    etag, size = stated.headers.get("etag"), stated.headers.get("content-length", "")
    if not etag or not size.isascii() or not size.isdecimal():
        raise ValueError("Grants.gov export states no ETag or size to bind the download to")
    return url, int(size), etag


def capture(evidence, snapshot_dates, *, transport=None) -> list[dict]:
    """Capture each dated export once through ``evidence``, bound to its stated version; returns their scopes."""
    from spicy_docs.sources import grants_gov

    def official(url):
        if not url.startswith(grants_gov.EXPORT_BASE):
            raise ValueError("Grants.gov export URL is outside the official extract location")
        return url

    scopes = []
    for snapshot_date in snapshot_dates:
        stated = stated_version(snapshot_date, transport=transport)
        if stated is None:
            raise ValueError(f"Grants.gov has no export for {snapshot_date}")
        url, size, etag = stated
        evidence.event("grants-export-version", export_url=url, etag=etag, content_length=size)
        descriptor = download_original(
            evidence, lambda acquirer, store, snapshot_date=snapshot_date, size=size, etag=etag:
            grants_gov.acquire_snapshot(acquirer, snapshot_date, store=store, expected_size=size, etag=etag),
            stage=STAGE, max_bytes=grants_gov.MAX_ZIP_BYTES, validate_url=official, transport=transport,
        )
        # The scope check holds the probed URL to the owner's own name for this export date.
        scopes.append(grants_gov.grants_scope(descriptor, snapshot_date))
    return scopes


def acquire(snapshot_dates, output_dir: Path, *, transport=None) -> dict:
    """Capture the dated exports into the raw archive; returns the capture selection from the run's receipts."""
    with acquisition_evidence("grants-opportunities", output_dir=Path(output_dir)) as evidence:
        return {"releases": capture(evidence, snapshot_dates, transport=transport)}


def build(readers, output_dir: Path, *, archives):
    """Use normal evidence retention and generation admission; acquire nothing."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each Grants generation")
    readers = tuple(readers)
    evidence = CaptureEvidence(output_dir, "grants-opportunities")
    try:
        retained = set()
        for reader in readers:
            capture = reader.collection_outcome["requestedScope"]["capture"]
            ref = capture["response_sha256"]
            if ref not in retained:
                path = Path(archives[ref])
                if path.stat().st_size != capture["byte_size"]:
                    raise ValueError("Grants archive differs from the selected byte size")
                # The request URL, not the local path: this journal is published.
                saved = evidence.retain_file(path, stage=STAGE, original_location=capture["request_url"])
                if saved.get("sha256") != ref:
                    raise ValueError("Grants archive differs from the admitted source digest")
                retained.add(ref)
        journal_snapshot_releases(evidence, readers, RELEASE_EVENT)
        table = build_grants_opportunities(readers, output_dir)
        artifact = build_generation(
            output_dir / "generation",
            family="grants-opportunities",
            files=(table,),
            expected_keys=(OUTPUT,),
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    from spicy_docs.sources.grants_gov_profile import GRANTS_GOV_PROFILE, iter_retained_pages

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire", help="Capture daily exports into the raw archive")
    capture.add_argument("--snapshot-date", action="append", required=True, dest="snapshot_dates",
                         help="Export date YYYY-MM-DD, repeatable")
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--captures-out", required=True, type=Path, help="Where to write the capture selection")
    add_snapshot_build(commands)
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.snapshot_dates, args.output_dir)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out), "releases": selection["releases"]}))
        return
    readers, archives = snapshot_inputs(args, GRANTS_GOV_PROFILE, iter_retained_pages)
    artifact, evidence = build(readers, args.output_dir, archives=archives)
    print(
        json.dumps(
            {
                "directory": str(args.output_dir / "generation"),
                "sourceEvidence": str(evidence),
                "logicalId": artifact.pin.logical_id,
                "artifactDigest": artifact.pin.artifact_digest,
            }
        )
    )


if __name__ == "__main__":
    main()
