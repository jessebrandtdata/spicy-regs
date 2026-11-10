"""Download originals into the raw archive and derive admitted source releases from them.

Shared by the hand-refreshed source pipelines (Treasury, OFAC, Grants.gov). A capture descriptor is the
request URL, SHA-256, observation time and size of one original, as its raw acquisition receipt records them;
a release is rebuilt from descriptors and raw originals on every build, so no release directory has to
outlive the generation it feeds.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit, urlunsplit

from rulespec_artifacts import ArtifactPin, LocalMemberSource, Producer

from spicy_regs.generations import implementation_id
from spicy_regs.source_evidence import JOURNAL


def download_original(evidence, download, *, stage: str, max_bytes: int, validate_url, transport=None) -> dict:
    """Run the source owner's ``download(acquirer, store)`` through the evidence transport; return its descriptor.

    The transport retains the bytes in the raw archive with a receipt, and the descriptor is read back from that
    receipt. The owner's local copy only satisfies its writer and is removed. When a redirect delivered the bytes,
    the receipt names the redirect target, so a second receipt binds the same bytes to the official URL.
    """
    from spicy_docs.transport.download import BoundedAcquirer

    retained = evidence.transport(transport, stage=stage, max_bytes=max_bytes)
    with TemporaryDirectory(dir=evidence.directory, prefix="download-") as scratch:
        with BoundedAcquirer(validate_url=validate_url, transport=retained, max_requests=6, timeout=120) as acquirer:
            result = download(acquirer, Path(scratch))
        events = [json.loads(line) for line in (evidence.artifact_dir / JOURNAL).read_text().splitlines()]
        received = [event for event in events if event["event"] == "capture" and event["stage"] == stage
                    and event["sha256"] == result["sha256"] and event["byte_size"] == result["bytes"]]
        if not received:
            raise ValueError("Downloaded original has no raw acquisition receipt")
        receipt = received[-1]
        if receipt["requested_url"] != result["url"]:
            target = urlsplit(receipt["requested_url"])
            evidence.retain_file(
                Path(scratch) / result["blob_path"], stage=stage, raw_source=True, original_location=result["url"],
                source_url=result["url"], observed_at=receipt["observed_at"], redirect_capture_id=receipt["capture_id"],
                # A signed redirect's query is a temporary credential, never a locator.
                redirect_url=urlunsplit((target.scheme, target.netloc, target.path, "", "")),
            )
    return {"request_url": result["url"], "response_sha256": receipt["sha256"],
            "observed_at": receipt["observed_at"], "byte_size": receipt["byte_size"]}


class ReleaseBlobs:
    """Raw originals are only verified and read; derived release members go to a local store.

    Building a release from raw therefore never writes to the archive.
    """

    def __init__(self, raw, local: Path):
        from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

        self.raw, self.local, self.derived = raw, LocalSourceNativeBlobStore(local), set()

    def put_blob(self, blob_ref, byte_size, chunks):
        from spicy_docs.storage.blobs import SourceNativeBlobWrite

        try:
            self.raw.describe_blob(blob_ref, byte_size)
        except FileNotFoundError:
            self.derived.add(blob_ref)
            return self.local.put_blob(blob_ref, byte_size, chunks)
        return SourceNativeBlobWrite(blob_ref, byte_size, True, 0)

    def open(self, blob_ref):
        return (self.local if blob_ref in self.derived else self.raw).open(blob_ref)


def admit_release(profile, pages, scope: dict, destination: Path, *, blobs, observed):
    """Publish one derived source release of ``scope`` and admit it.

    Producer and clock come from the code and the captures' ``observed`` times, so the same scope and code
    rebuild the same release digest.
    """
    from spicy_docs.source_native import SourceNativeReleaseBuild, SourceNativeReleasePublisher, SourceNativeReleaseReader

    implementation = implementation_id()
    producer = Producer("spicy-docs", implementation, "urn:spicy-regs:source-native-release-verifier", "2.0",
                        implementation)
    times = sorted(datetime.fromisoformat(instant).astimezone(UTC) for instant in observed)
    published = SourceNativeReleasePublisher(profile, blob_store=blobs, clock=lambda: times[-1]).publish(
        pages, destination=destination,
        build=SourceNativeReleaseBuild(
            query_scope=scope, producer=producer,
            started_at=times[0].replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        ),
    )
    return SourceNativeReleaseReader(
        LocalMemberSource(published.root), blob_source=blobs, profile=profile,
        accepted_verifier_implementation_ids=frozenset({implementation}), expected_pin=published.artifact.pin,
    )


def raw_original(store, sha256: str, byte_size: int) -> Path:
    """The stored file of one original, verified against its digest and size; a gzip-wrapped one is refused."""
    described = store.describe_blob(sha256, byte_size)
    if described["encoding"] != "raw":
        raise ValueError("Raw original must be stored unwrapped to be read as a file")
    return store.root / described["object"]


def open_raw(archive_root: Path | None = None):
    """The raw archive holding a build's originals, opened read-only."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root

    return CentralRawArchiveStore(archive_root or default_raw_archive_root(), create=False)


def snapshot_readers(profile, iter_retained_pages, scopes, destination: Path, *, store):
    """Admit one release per single-archive snapshot scope; returns the readers and each original's raw file."""
    blobs = ReleaseBlobs(store, Path(destination) / "blobs")
    readers, archives = [], {}
    for index, scope in enumerate(scopes):
        capture = scope["capture"]
        archives[capture["response_sha256"]] = raw_original(store, capture["response_sha256"], capture["byte_size"])
        readers.append(admit_release(profile, iter_retained_pages(scope, blob_source=blobs), scope,
                                     Path(destination) / str(index), blobs=blobs, observed=[capture["observed_at"]]))
    return readers, archives


def selected_readers(profile, selection: dict):
    """Readers over releases admitted earlier and kept on disk, each held to its selected pin."""
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    return [
        SourceNativeReleaseReader(
            LocalMemberSource(item["release"]),
            blob_source=LocalSourceNativeBlobStore(item["blobs"], create=False),
            profile=profile,
            accepted_verifier_implementation_ids=frozenset({item["verifierImplementationId"]}),
            expected_pin=ArtifactPin(item["logicalId"], item["artifactDigest"]),
        )
        for item in selection["sources"]
    ]


def merged_scopes(selections) -> list[dict]:
    """Every release scope of the capture selections, once; a repeated scope is refused, never overridden."""
    scopes = {}
    for selection in selections:
        for scope in selection["releases"]:
            key = json.dumps(scope, sort_keys=True)
            if key in scopes:
                raise ValueError("A capture scope was selected twice")
            scopes[key] = scope
    return list(scopes.values())


def add_snapshot_build(commands) -> None:
    """The ``build`` subcommand of a single-archive snapshot pipeline: captures to admit, or kept releases."""
    build = commands.add_parser("build", help="Build a generation from capture selections or kept releases")
    inputs = build.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--captures", action="append", type=Path,
                        help="Capture selection whose originals are in the raw archive, repeatable")
    inputs.add_argument("--selection", type=Path, help="Releases admitted earlier, with their original archives")
    build.add_argument("--output-dir", required=True, type=Path)
    build.add_argument("--archive-root", type=Path, help="Raw archive holding the originals (read only)")


def snapshot_inputs(args, profile, iter_retained_pages):
    """The readers and original archives a parsed ``build`` command selected."""
    if args.selection is not None:
        selection = json.loads(args.selection.read_text())
        return selected_readers(profile, selection), selection["archives"]
    scopes = merged_scopes(json.loads(path.read_text()) for path in args.captures)
    return snapshot_readers(profile, iter_retained_pages, scopes, args.output_dir / "source-releases",
                            store=open_raw(args.archive_root))
