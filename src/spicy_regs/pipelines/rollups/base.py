"""Base class for decoupled, per-rollup pipelines, each on its own cron.

Each rollup (``feed_summary``, ``agency_stats``, ``discovery_signals``, ...)
is materialized by its own :class:`RollupPipeline` subclass with its own console
entry (``run-rollup-*``) and GitHub Actions workflow, so it can be re-run or
backfilled alone and a failure stays isolated to one artifact. ``run()`` reads
top-to-bottom as the data flow: prime the base tables this rollup reads from R2
(skipping any already present locally), materialize the artifact(s) via a
transform, retain one complete immutable generation, then — unless
``skip_upload`` is set — verify uploaded bytes before atomically replacing its
public pointer.

**Contract:** a rollup reads only tables no other rollup *derives* — the ETL's
published base tables (``dockets``, ``documents``, ``comments_index``) and the
published output of an **ingest** rollup, which fetches from a publisher and
has no upstream dependency inside this repository. Reading a derived rollup's
output stays forbidden, because that is the read that makes two pipelines race,
whereas an ingest table's content comes from outside, so the worst a stale copy
costs is one cron's lag, which the offsets already handle. A best-effort
merge-time join whose absence must not fail the run is declared in
``soft_inputs``, not ``inputs``: it primes nothing, fails nothing, and leaves
the linkage columns NULL so the rollup still publishes everything its own
source establishes — it exists so the read is *stated* rather than buried in a
transform, and so ``tests/test_hosted_rollups.py`` can hold each entry to being
an ingest rollup's output whose cron runs before this one's. A read that does
fetch the table is recorded like any other: every table of another family the
build fetches through :func:`spicy_regs.sources.r2.download_from_r2`, soft or
undeclared, is a parent of the generation at the bytes fetched; a read that
found nothing records nothing, and a read that bypasses the helper is not
recorded. The family's own prior is never a parent: the generation's
prior-generation input names it. Multi-output
rollups declare ``outputs`` instead of ``output`` and return a tuple of paths
from ``build()``; every declared output must be present, including successful
empty tables, and each goes through the same per-file shrink guard on upload.
Uploads use immutable generation paths, and ``publication.v2.json`` changes only
after the full family passes local schema checks, shrink checks and remote byte
verification; readers capture that index once per operation. The old bare
Parquet URLs are not rewritten by this path.
"""

import hashlib
from abc import abstractmethod
from collections.abc import Mapping
from contextlib import nullcontext
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, ClassVar
from uuid import uuid4

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.pipelines.base import Pipeline
from spicy_regs.sources import r2


class Unchanged(Exception):
    """``build`` found the source holds nothing the published generation lacks.

    ``run`` then builds and publishes nothing and returns normally, so a scheduled run on a day without
    news is green. Raise it only once every byte acquired is known to be published already.
    """


class RollupPipeline(Pipeline):
    """A single materialized rollup, run standalone from the base tables on R2."""

    #: Base-table Parquet files this rollup reads (R2 remote keys, e.g.
    #: ``"documents.parquet"``). Primed from R2 before the build, and a
    #: missing one fails the run.
    inputs: ClassVar[tuple[str, ...]] = ()

    #: Published tables this rollup reads **best-effort** at merge time, to
    #: fill columns its own source does not carry (R2 remote keys). Not
    #: primed and never fatal: absence leaves those columns NULL and the
    #: rollup still publishes everything its own source establishes; a read
    #: that fetches one is recorded as a parent (see the module note). Each
    #: entry must name an *ingest* rollup's output whose cron runs before
    #: this one's — ``tests/test_hosted_rollups.py`` holds both.
    soft_inputs: ClassVar[tuple[str, ...]] = ()
    #: Public objects the transform reads over HTTP instead of downloading (for
    #: Parquet projection pushdown). Their storage versions are recorded as
    #: parents and must not change while the rollup builds.
    remote_inputs: ClassVar[tuple[str, ...]] = ()

    #: Zero or more artifacts a multi-output rollup writes and publishes (R2
    #: remote keys, same convention as ``output``). Leave empty and declare
    #: ``output`` directly for the (default) single-output case; declaring
    #: ``outputs`` instead picks up the ``output`` property below, which
    #: reads as ``outputs[0]`` for anything that only needs one key (e.g. the
    #: freshness checker).
    outputs: ClassVar[tuple[str, ...]] = ()

    #: A partial writer can update this existing complete family. Unchanged
    #: siblings are carried forward from its captured generation, never rebuilt.
    publication_family: ClassVar[str | None] = None
    generation_tables: ClassVar[bool] = True
    retain_source_evidence: ClassVar[bool] = False
    #: Explicit policies for every output. Builders write one shared receipt member in build_dir.
    receipt_policies: ClassVar[tuple] = ()
    #: Tables this rollup adds to its published family: the explicit migration publication requires before a
    #: family's table set may grow. Inert once they are published; never a way to drop one.
    added_tables: ClassVar[tuple[str, ...]] = ()
    receipt_only_tables: ClassVar[tuple[str, ...]] = ()
    #: Published tables of this rollup that no longer have per-row receipts: the explicit migration publication
    #: requires before a family's receipt datasets may leave, once (ss-vse3). Each keeps its table. It stays
    #: declared afterwards, where it refuses a generation that would claim receipts for one again.
    retired_receipt_datasets: ClassVar[tuple[str, ...]] = ()
    #: Outputs stored as several files, by key, to their partition columns. The build returns each as a directory
    #: named for the table, holding ``<col>=<value>/part-NNNNNN.parquet`` (multi-file design §4.2).
    partitioned: ClassVar[Mapping[str, tuple[str, ...]]] = {}

    #: The single artifact this rollup writes and publishes (e.g.
    #: ``"feed_summary.parquet"``). Its R2 remote key is the same filename.
    #: Single-output rollups set this directly, which — because Python
    #: resolves a class attribute from the most-derived class first —
    #: shadows the ``output`` property below entirely; it never runs for them.
    output: ClassVar[str]

    @property
    def output(self) -> str:  # noqa: F811 — intentional redefinition, see docstring above
        """``outputs[0]``, for callers that only need this rollup's first/only key."""
        if self.outputs:
            return self.outputs[0]
        raise AttributeError(f"{type(self).__name__} must set 'output' or 'outputs'")

    def __init__(self, *, output_dir: Path | None = None, skip_upload: bool = True) -> None:
        self.output_dir = output_dir
        self.skip_upload = skip_upload
        self.source_evidence = None
        #: A failure ``build`` met and built around (one source refused, the rest was read): ``run`` publishes the
        #: generation and then raises it, so the run still fails.
        self.deferred_failure: BaseException | None = None
        self.receipt_generation_id = uuid4().hex

    def run(self, *, read_operation=None) -> None:
        self._conversion_read_operation = read_operation
        if not self.generation_tables:
            # Docket search is a browser gzip object, not a table family. Keep
            # its existing single-object path until that consumer is migrated.
            output_dir = self.output_dir or (Path.cwd() / "output")
            output_dir.mkdir(parents=True, exist_ok=True)
            self._prime(output_dir)
            built = self.build(output_dir)
            if isinstance(built, tuple) or built.name != self.output or self.outputs:
                raise ValueError("Legacy object rollups must produce exactly their declared single output")
            if not self.skip_upload:
                r2.upload_file(built, remote_key=self.output)
            logger.info("Legacy unversioned object retained: {}", built)
            return

        from spicy_regs.source_evidence import CaptureEvidence

        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        if self.retain_source_evidence:
            self.source_evidence = CaptureEvidence(output_dir, self.publication_family or self.name)
        try:
            self._run_tables(output_dir)
            if self.deferred_failure is not None:
                raise self.deferred_failure
        except Unchanged as unchanged:
            if self.source_evidence:
                self.source_evidence.finish()
            logger.info("Rollup {}: {}; nothing built or published", self.name, unchanged)
        except BaseException as error:
            if self.source_evidence:
                self.source_evidence.finish(error)
            raise
        else:
            if self.source_evidence:
                self.source_evidence.finish()

    def generation_schemas(self):
        """The schemas this producer declares for generation admission."""
        from spicy_regs.data_dictionary import expected_schemas

        return expected_schemas()

    def generation_kwargs(self, out_paths):
        """Additional shared generation admission arguments supplied by an adopter."""
        return {}

    def _run_tables(self, output_dir: Path) -> None:
        from rulespec_artifacts import publish_directory_no_replace
        from spicy_regs.generations import build_generation, output_keys, verify_generation
        from spicy_regs.sources import publication

        read_operation = getattr(self, "_conversion_read_operation", None)

        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        public_url = getenv("R2_PUBLIC_URL")
        context = publication.snapshot(public_url) if public_url else nullcontext(publication.empty_index())
        with context as prior_index:
            if self.source_evidence:
                self.source_evidence.inherit(prior_index, public_url=public_url)
            # Bespoke ingest builders also cache priors. An isolated build
            # directory covers all of them without deleting user evidence.
            build_dir = output_dir
            if public_url:
                build_dir = output_dir / ".builds" / uuid4().hex
                build_dir.mkdir(parents=True)
            with r2.recorded_reads() as reads:
                parents = self._prime(build_dir, prior_index)
                remote = self._observe_remote_inputs(build_dir, public_url)
                logger.info("Building rollup {}...", self.output)
                self._defer_native_selection = True
                try:
                    built = self.build(build_dir)
                finally:
                    self._defer_native_selection = False
            if self._observe_remote_inputs(build_dir, public_url) != remote:
                raise publication.PublicationError("A remote input changed while the rollup built; rebuild")
            # Every other family's table the build fetched is a parent, read soft or undeclared included.
            parents |= remote | {key: read for key, read in reads.items()
                                 if key not in parents and not self._own(key, read.get("family"))}
            out_paths = built if isinstance(built, tuple) else (built,)
            family = self.name
            expected_keys = tuple(
                key for key in (self.outputs or (self.output,)) if key not in self.receipt_only_tables
            )
            if len(out_paths) != len(expected_keys) or set(output_keys(out_paths, self.partitioned)) != set(
                expected_keys
            ):
                raise publication.PublicationError("Build outputs differ from its declared complete set")
            carried_forward = {}
            partitioned = dict(self.partitioned)
            publication_status = "complete-family"
            if self.publication_family:
                prior = prior_index["families"].get(self.publication_family)
                if prior is None:
                    if not self.skip_upload:
                        raise publication.PublicationError(
                            f"Publish a complete {self.publication_family} generation before its partial writer"
                        )
                    logger.warning("Retaining a local partial candidate; no complete publication family exists")
                    publication_status = "local-partial"
                else:
                    family = self.publication_family
                    expected_keys = tuple(prior["tables"])
                    written = set(output_keys(out_paths, self.partitioned))
                    if not written <= set(expected_keys):
                        raise publication.PublicationError("Partial writer output is outside its existing family")
                    for key in expected_keys:
                        if key in written:
                            continue
                        descriptor = prior["tables"][key]
                        if "members" in descriptor:
                            # A split sibling is carried member for member, each checked against its pin, and is
                            # not declared carried whole: publication copies every unchanged member server-side.
                            r2.download_members(key, build_dir)
                            out_paths += (build_dir / key.removesuffix(".parquet"),)
                            partitioned[key] = tuple(descriptor["partitionColumns"])
                            continue
                        path = build_dir / key
                        if not r2.download(key, path):
                            raise publication.PublicationError(f"Missing carried-forward member: {key}")
                        out_paths += (path,)
                        carried_forward[key] = prior["artifactDigest"]
        generations = output_dir / "generations"
        generations.mkdir(exist_ok=True)
        with TemporaryDirectory(prefix=".generation-", dir=output_dir) as staging:
            directory = Path(staging) / "artifact"
            artifact = build_generation(
                directory,
                family=family,
                files=out_paths,
                expected_keys=expected_keys,
                schemas=self.generation_schemas(),
                read_snapshot=prior_index,
                carried_forward=carried_forward,
                publication_status=publication_status,
                inputs=self.source_evidence.inputs() if self.source_evidence else (),
                parents=parents,
                partitioned=partitioned,
                read_operation=read_operation,
                canonical_root=generations if read_operation is not None else None,
                **self.generation_kwargs(out_paths),
                **(
                    {
                        "receipt_path": build_dir / "etl_receipts.parquet",
                        "receipt_policies": self.receipt_policies,
                        "receipt_generation_id": self.receipt_generation_id,
                    }
                    if self.receipt_policies
                    else {}
                ),
            )
            destination = generations / artifact.pin.artifact_digest.removeprefix("sha256:")
            if destination.exists():
                if read_operation is None:
                    verify_generation(destination, expected_pin=artifact.pin)
                else:
                    read_operation.admit_generation(destination, expected_pin=artifact.pin)
            else:
                publish_directory_no_replace(directory, destination)
        from rulespec_artifacts import LocalMemberSource
        from spicy_regs.source_catalog.input_proof import save_input_proof
        from spicy_regs.source_evidence import EvidenceMemberSource

        evidence = ((EvidenceMemberSource(self.source_evidence.artifact_dir),
                     self.source_evidence.artifact),) if self.source_evidence else ()
        save_input_proof(
            output_dir / "generation-inputs" / family / artifact.pin.artifact_digest.removeprefix("sha256:"),
            artifact, LocalMemberSource(destination), evidence,
        )
        if self.skip_upload:
            logger.info("Verified local generation {}; upload skipped", destination)
        else:
            r2.require_credentials("Generation publication")
            if not public_url:
                raise RuntimeError("Generation publication requires R2_PUBLIC_URL")
            publish = publication.publish_generation
            arguments: dict[str, Any] = {}
            if read_operation is not None:
                artifact, source = read_operation.verified_source(destination)
                publish = publication._publish_verified_generation
                arguments = {"artifact": artifact, "source": source, "local": destination,
                             "upload_member": lambda prefix, key: publication._put_immutable(
                                 r2.get_r2_client(), getenv("R2_BUCKET_NAME", "spicy-regs"),
                                 prefix + "/" + key, destination / key)}
            else:
                arguments = {"directory": destination}
            publish(
                **arguments,
                client=r2.get_r2_client(),
                bucket=getenv("R2_BUCKET_NAME", "spicy-regs"),
                prior_index=prior_index,
                evidence_directories=(self.source_evidence.artifact_dir,) if self.source_evidence else (),
                added_tables=frozenset(self.added_tables),
                receipt_only_tables=frozenset(self.receipt_only_tables),
                retired_receipt_datasets=frozenset(self.retired_receipt_datasets),
            )
            from spicy_regs.sources.cloudflare import purge_urls

            purge_urls([f"{public_url.rstrip('/')}/{publication.INDEX_V2_KEY}"])
        from spicy_regs.selected_generations import remember_generation

        remember_generation(output_dir, destination, artifact)
        logger.info("Done!")

    def _own(self, key: str, family: str | None) -> bool:
        """Whether ``key`` is this rollup's own table: its prior is a prior-generation input, never a parent."""
        return family == (self.publication_family or self.name) or key in (self.outputs or (self.output,))

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        """Download each required base table from R2 unless already local; return the parents read.

        A missing base table is fatal: a rollup built from absent inputs would
        publish a truncated artifact that the upload shrink-guard would then
        (correctly) reject — better to fail loudly here with a clear message.
        Each parent records the digest and size of the bytes read, plus its
        family and generation when a managed family published it. A verified
        managed download reuses its descriptor; any other file is hashed once.
        The rollup's own table is primed but is no parent (:meth:`_own`).
        """
        return self._prime_tables(self.inputs, output_dir, snapshot)

    def _prime_tables(self, keys: tuple[str, ...], output_dir: Path, snapshot: Mapping | None) -> dict[str, dict]:
        """:meth:`_prime` for ``keys``: each published table downloaded, pinned and returned as a parent."""
        from spicy_regs.sources import publication

        parents = {}
        for remote_key in keys:
            local = output_dir / remote_key
            downloaded = False
            if local.exists():
                logger.info("Using local {} (skipping download)", remote_key)
            elif not r2.download(remote_key, local):
                raise RuntimeError(
                    f"Rollup {self.output!r}: required base table {remote_key!r} "
                    f"not found on R2 and not present locally in {output_dir}"
                )
            else:
                downloaded = True
            owner = publication.table_owner(snapshot or publication.empty_index(), remote_key)
            if self._own(remote_key, owner[0] if owner else None):
                continue
            descriptor = owner[1]["tables"][remote_key] if owner else None
            if downloaded and descriptor:
                parent = {"sha256": descriptor["sha256"], "byteSize": descriptor["byteSize"]}
            else:
                with local.open("rb") as stream:
                    parent = {
                        "sha256": "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest(),
                        "byteSize": local.stat().st_size,
                    }
            if owner:
                parent |= {"family": owner[0], "artifactDigest": owner[1]["artifactDigest"]}
            parents[remote_key] = parent
        return parents

    def _observe_remote_inputs(self, output_dir: Path, public_url: str | None) -> dict[str, dict]:
        """The storage version of each remote input, or the digest of a local copy that replaces it."""
        observed = {}
        for remote_key in self.remote_inputs:
            local = output_dir / remote_key
            if local.exists():
                with local.open("rb") as stream:
                    observed[remote_key] = {
                        "sha256": "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest(),
                        "byteSize": local.stat().st_size,
                    }
                continue
            if not public_url:
                raise RuntimeError(f"Rollup {self.output!r}: {remote_key} needs R2_PUBLIC_URL or a local copy")
            version = r2.public_object_version(f"{public_url.rstrip('/')}/{remote_key}")
            if version is None:
                raise RuntimeError(f"Rollup {self.output!r}: remote input {remote_key!r} is missing")
            observed[remote_key] = {"etag": version["etag"], "byteSize": version["bytes"]}
        return observed

    @abstractmethod
    def build(self, output_dir: Path) -> Path | tuple[Path, ...]:
        """Materialize the rollup into ``output_dir``.

        Return the single artifact's path, or — for a rollup that declares
        ``outputs`` — a tuple of paths, one per declared key, in any order.
        """
        ...


def make_rollup_app(pipeline_cls: type[RollupPipeline]) -> App:
    """Build the ``run-rollup-*`` cyclopts CLI for one rollup pipeline.

    All rollups share the same two flags, so each module just does::

        app = make_rollup_app(FeedSummaryRollup)
    """
    app = App(
        name=f"run-rollup-{pipeline_cls.name}",
        help=(pipeline_cls.__doc__ or "").strip().splitlines()[0] if pipeline_cls.__doc__ else "",
    )

    @app.default
    def main(
        *,
        output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
        skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    ) -> None:
        load_dotenv()
        pipeline_cls(output_dir=output_dir, skip_upload=skip_upload).run()

    return app
