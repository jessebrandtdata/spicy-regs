"""Acquire every executive order the Federal Register API serves into the raw archive, then build the family.

``acquire`` lists every order the API serves (publication dates from ``FR_EPOCH``, the API's first, through
today, in the two traversals the source owner requires), saving each listing page in the raw archive before it is
parsed, and admits the listing as a SpicyDocs source release. When no order was added, changed or removed since
the generation it read, it stops there. Otherwise it captures each order's body through the same evidence, so
every response is in the raw archive before SpicyDocs validates it. An order the published generation holds with
an acquired body keeps that body result; its bytes are restored from published source evidence when the raw
archive lacks them.

A capture selection is the listing (each page's request URL, SHA-256, size and request time, in request order)
and one SpicyDocs body result per order. ``build`` admits the same release again from the raw pages, lays the
body results out for SpicyDocs' offline loader and builds the table.
"""

from __future__ import annotations

import argparse
import json
from collections import deque
from datetime import UTC, date, datetime
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    READ_INDEX_HELP, ReleaseBlobs, admit_release, inherit_read_index, restore_originals,
)
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_executive_orders import (
    OUTPUT, SCHEMA, TABLE, build_executive_orders, record_pin, require_all_orders,
)
from spicy_regs.transforms.build_federal_register import FR_EPOCH

FAMILY = "executive-orders"
LISTING_STAGE = "executive-order-listing"
BODY_STAGE = "executive-order-body"


def query_scope(through: date) -> dict:
    """Every publication date the API serves through ``through``, under the executive-order predicate."""
    return {"publishedFrom": FR_EPOCH.isoformat(), "publishedThrough": through.isoformat(),
            "presidentialDocumentType": "executive_order"}


def list_orders(evidence, scope: dict, *, fetch=None) -> dict:
    """Fetch the listing through the Federal Register owner, keeping each page in raw before it is parsed."""
    from spicy_docs.sources.federal_register.native import iter_federal_register_pages
    from spicy_docs.transport.acquisition import federal_register_fetcher

    pages = []
    with federal_register_fetcher(fetch) as get:
        def retained(url):
            requested_at = datetime.now(UTC).isoformat()
            body = get(url)
            saved = evidence.retain_bytes(body, raw_source=True, stage=LISTING_STAGE, source_url=url,
                                          requested_url=url, observed_at=requested_at)
            pages.append({"request_url": url, "response_sha256": saved["sha256"], "byte_size": saved["byte_size"],
                          "observed_at": requested_at})
            return body

        for _ in iter_federal_register_pages(retained, query_scope=scope, traversals=2):
            pass
    return {"scope": scope, "pages": pages}


def listing_release(listing: dict, destination: Path, *, store):
    """Admit the listing from its raw pages, replaying every request in the order it was made; returns its reader."""
    from spicy_docs.sources.federal_register.native import iter_federal_register_pages
    from spicy_docs.sources.federal_register.profile import FEDERAL_REGISTER_PROFILE
    from spicy_docs.storage.blobs import iter_verified_blob

    queued: dict[str, deque] = {}
    for page in listing["pages"]:
        queued.setdefault(page["request_url"], deque()).append(page)

    def replay(url):
        if not queued.get(url):
            raise ValueError("Executive-order listing replay requested a page the selection lacks")
        page = queued[url].popleft()
        return b"".join(iter_verified_blob(store, page["response_sha256"], page["byte_size"]))

    destination = Path(destination)
    reader = admit_release(
        FEDERAL_REGISTER_PROFILE, iter_federal_register_pages(replay, query_scope=listing["scope"], traversals=2),
        listing["scope"], destination / "release", blobs=ReleaseBlobs(store, destination / "blobs"),
        observed=[page["observed_at"] for page in listing["pages"]],
    )
    if any(queued.values()):
        raise ValueError("Executive-order listing selection holds pages its traversal never requested")
    return reader


def published_table(read_index: Path | None) -> Path | None:
    """The family's table in the ``spicy-regs mirror`` beside ``read_index``, or ``None`` when it is unpublished."""
    if read_index is None:
        return None
    from spicy_regs.sources import publication

    prior = publication.parse_index(read_index.read_bytes())["families"].get(FAMILY)
    return None if prior is None else read_index.parent.parent / prior["prefix"] / OUTPUT


def prior_rows(table: Path | None) -> dict[str, dict]:
    """The published rows by source identity; empty without a published table."""
    if table is None:
        return {}
    import pyarrow.parquet as pq

    rows = pq.read_table(table).to_pylist()
    held = {row["source_record_id"]: row for row in rows}
    if len(held) != len(rows):
        raise ValueError("Published executive orders repeat a source identity")
    return held


def changes(reader, prior: dict) -> dict:
    """Orders added, changed (another record pin) and removed since the published rows."""
    listed = {item["sourceRecordId"]: record_pin(item["record"]) for item in reader.iter_records()}
    held = {key: record_pin(json.loads(row["raw_metadata_json"])) for key, row in prior.items()}
    return {"added": sorted(listed.keys() - held.keys()), "removed": sorted(held.keys() - listed.keys()),
            "changed": sorted(key for key in listed.keys() & held.keys() if listed[key] != held[key])}


def capture_bodies(evidence, reader, prior: dict, results: Path, *, transport=None) -> tuple[dict, list]:
    """One body result per order: the published one when it was acquired, otherwise SpicyDocs' capture.

    ``capture_body`` reuses a result already at its path, so a rerun into the same ``results`` resumes. Returns
    the results and the digests of the reused ones' bytes.
    """
    from spicy_docs.sources.federal_register.body_acquisition import (
        FederalRegisterBodyAcquirer, FederalRegisterBodyBudget,
    )
    from spicy_docs.sources.federal_register.body_captures import capture_body

    # The limits the published 2025 bodies were captured under, so a reused result and a new one state one budget.
    budget = FederalRegisterBodyBudget(max_requests=4, max_body_bytes=8 * 1024 * 1024, max_mods_bytes=16 * 1024 * 1024,
                                       timeout_seconds=30, min_request_interval_seconds=0.4)
    pin = {"logicalId": reader.pin.logical_id, "artifactDigest": reader.pin.artifact_digest}
    retained = evidence.transport(transport, stage=BODY_STAGE,
                                  max_bytes=max(budget.max_body_bytes, budget.max_mods_bytes))
    bodies, reused = {}, []
    with FederalRegisterBodyAcquirer(budget=budget, transport=retained) as acquirer:
        for item in reader.iter_records():
            identity, held = item["sourceRecordId"], prior.get(item["sourceRecordId"])
            if held is not None and held["body_status"] == "acquired":
                bodies[identity] = json.loads(held["body_result_json"])
                reused += [capture["sha256"] for capture in bodies[identity]["captures"]]
            else:
                bodies[identity] = capture_body(results / identity / "result.json", record=item["record"],
                                                source_pin=pin, acquirer=acquirer).manifest
    return bodies, reused


def capture(evidence, *, prior: dict, results: Path, through: date, fetch=None, transport=None) -> dict:
    """List every order, then capture the bodies a build needs unless nothing changed; returns the selection.

    A selection without ``bodies`` means the listing equals the published rows: there is nothing to build.
    """
    from spicy_docs.sources.federal_register.native import MAX_PAGE_BYTES

    from spicy_regs.sources import r2

    listing = list_orders(evidence, query_scope(through), fetch=fetch)
    results.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=results, prefix=".release-") as scratch:
        reader = listing_release(listing, Path(scratch), store=evidence.store)
        # A record the release refused would otherwise read as removed, or as nothing new.
        require_all_orders(reader.collection_outcome)
        selection = {"listing": listing, "changes": changes(reader, prior)}
        if not any(selection["changes"].values()):
            return selection
        bodies, reused = capture_bodies(evidence, reader, prior, results, transport=transport)
    # A row kept unchanged points to its published page; both it and the reused bodies must be in raw for the build.
    pages = [json.loads(row["metadata_locator_json"])["evidenceBlobRef"] for key, row in prior.items() if key in bodies]
    restore_originals(evidence.store, reused + pages, getenv("R2_PUBLIC_URL", r2.VERIFIED_PUBLIC_URL),
                      max_bytes=MAX_PAGE_BYTES, transport=transport)
    return {**selection, "bodies": bodies}


def acquire(output_dir: Path, *, prior: dict, through: date | None = None, fetch=None, transport=None) -> dict:
    """Capture into the raw archive with the run's own receipt; returns the capture selection."""
    output_dir = Path(output_dir)
    with acquisition_evidence(FAMILY, output_dir=output_dir) as evidence:
        return capture(evidence, prior=prior, results=output_dir / "body-captures",
                       through=through or datetime.now(UTC).date(), fetch=fetch, transport=transport)


def materialize_bodies(bodies: dict, directory: Path, *, store) -> dict[str, Path]:
    """Lay each body result out as SpicyDocs' offline loader reads it: the manifest beside its captures' bytes."""
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
    from spicy_docs.transport.resume import write_json_atomic

    paths = {}
    for identity, manifest in bodies.items():
        path = Path(directory) / identity / "result.json"
        blobs = LocalSourceNativeBlobStore(path.parent / "blobs")
        refusal = manifest.get("refusal") or {}
        held = [(capture["sha256"], capture["byte_size"]) for capture in manifest["captures"]]
        held += [(refusal["sha256"], refusal["bytes"])] if "sha256" in refusal else []
        for digest, size in held:
            with store.open(digest) as stream:
                blobs.put_blob(digest, size, iter(lambda stream=stream: stream.read(1 << 20), b""))
        write_json_atomic(path, manifest)
        paths[identity] = path
    return paths


def retain_and_build(selection: dict, output_dir: Path, evidence, *, prior: dict) -> Path:
    """Admit the selected release and body results from raw and build the table through ``evidence``."""
    from spicy_docs.sources.federal_register.body_captures import load_body_capture

    if "bodies" not in selection:
        raise ValueError("The capture selection found nothing new; there is nothing to build")
    output_dir = Path(output_dir)
    reader = listing_release(selection["listing"], output_dir / "source-release", store=evidence.store)
    paths = materialize_bodies(selection["bodies"], output_dir / "body-results", store=evidence.store)
    return build_executive_orders(reader, output_dir, body_paths=paths, load_body=load_body_capture,
                                  evidence=evidence, prior=prior)


def build(selection: dict, output_dir: Path, *, read_index: Path | None = None):
    """Build one generation from a capture selection and raw originals; no network calls."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each executive-order generation")
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        table = retain_and_build(selection, output_dir, evidence, prior=prior_rows(published_table(read_index)))
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=(table,),
            expected_keys=(OUTPUT,),
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture_command = commands.add_parser("acquire", help="List every order and capture new bodies into raw")
    capture_command.add_argument("--read-index", type=Path,
                                 help=READ_INDEX_HELP + "; the orders its generation holds are compared and reused")
    capture_command.add_argument("--output-dir", required=True, type=Path)
    capture_command.add_argument("--captures-out", required=True, type=Path,
                                 help="Where to write the capture selection")
    generation = commands.add_parser("build", help="Build a generation from a capture selection and raw originals")
    generation.add_argument("--captures", required=True, type=Path)
    generation.add_argument("--output-dir", required=True, type=Path)
    generation.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.output_dir, prior=prior_rows(published_table(args.read_index)))
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out), "nothingNew": "bodies" not in selection,
                          "listingRequests": len(selection["listing"]["pages"]),
                          **{key: len(value) for key, value in selection["changes"].items()}}))
        return
    artifact, evidence = build(json.loads(args.captures.read_text()), args.output_dir, read_index=args.read_index)
    print(json.dumps({"directory": str(args.output_dir / "generation"), "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest}))


if __name__ == "__main__":
    main()
