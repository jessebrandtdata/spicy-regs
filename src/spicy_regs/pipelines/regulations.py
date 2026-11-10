"""The regulations.gov ETL, expressed through the Reader/Writer/Pipeline base classes.

This is also the run file: invoke it via the ``run-pipeline`` console script,
e.g. ``uv run run-pipeline --skip-upload --since-year 2025``. ``run()`` reads
top-to-bottom as the data flow: prime the incremental Manifest and existing
output, extract every agency's record streams in parallel into staging Parquet while
keeping each fetched original (``mirrulations_originals``), merge and
deduplicate the staging, then save the Manifest and publish to R2
(``_publish``, which advances the manifest strictly last). Publishing stays off
until this path is vetted; pass ``--no-skip-upload`` to turn it on. The
reusable pieces live elsewhere — connection details and the reader factory in
``spicy_docs.sources.mirrulations``, the json→record transform in
``transforms.extract``, the parallel fan-out in ``pipelines.staging``, R2 in
``sources.r2``, processed-key tracking in ``manifest`` — so this module is just
the wiring.
"""

from contextlib import ExitStack
from os import getenv
from pathlib import Path
from shutil import rmtree
from time import monotonic
from typing import Annotated, ClassVar

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_docs.sources import mirrulations

from spicy_regs.manifest import Manifest
from spicy_regs.pipelines.regulations_state import UnresolvedKeys, source_record_type
from spicy_regs.pipelines.base import Pipeline
from spicy_regs.pipelines.mirrulations_originals import MirrulationsOriginals
from spicy_regs.pipelines.staging import stage_agencies
from spicy_regs.schemas import RECORD_TYPES, RecordType
from spicy_regs.sources import iceberg, r2
from spicy_regs.pipelines.comment_text import PendingCommentText
from spicy_regs.transforms.derived_text_pool import DerivedTextPool
from spicy_regs.transforms.regulations_attributes import ATTRIBUTE_TABLES, TeeAttributes, merge_attribute_parts
from spicy_regs.transforms.comment_partitions import validate_staged_comments
from spicy_regs.transforms.reviewed_comments import ExcludeReviewedComments
from spicy_regs.transforms.chain import Chain
from spicy_regs.transforms.enrich_derived_text import EnrichCommentText
from spicy_regs.transforms.extract import ExtractRecords
from spicy_regs.transforms.base import Transform
from spicy_regs.transforms.merge_staging_files import merge_staging_files
from spicy_regs.transforms.write_staging import write_staging

# With ``batch_count``, a batch's size is derived from the discovered agencies so
# none falls off the end: a fixed ``--batch-size 22`` over 15 batches (330 slots)
# silently left VCNP, VETS, WAPA, WCPO and WHD in no batch once discovery
# returned 335 agencies (2026-09-24). The ceiling makes a jump in the mirror's
# agency list a refusal to act on (raise BATCH_COUNT) instead of quietly longer jobs.
MAX_DERIVED_BATCH_SIZE = 25
#: The least a sweep assumes one more batch needs: the one whole batch measured that merged comment rows took
#: 79.4 minutes ("ETL batch completed in 4762.7s", run 42674437011, 2026-11-09), 60.4 of them after its reads,
#: republishing the base families. A batch with nothing new takes about 5.
SWEEP_BATCH_FLOOR_SECONDS = 80 * 60
#: Written to the output directory by a sweep that stopped at its budget: the batch (0-indexed) it stopped before.
SWEEP_STOPPED_NAME = "sweep-stopped.txt"


class RegulationsPipeline(Pipeline):
    """Mirrulations S3 → Parquet ETL, composed from Readers, Writers, and transforms."""

    name: ClassVar[str] = "regulations"

    def __init__(
        self,
        *,
        agency: str | None = None,
        output_dir: Path | None = None,
        since_year: int | None = None,
        skip_upload: bool = True,
        skip_comments: bool = False,
        only_comments: bool = False,
        batch_number: int | None = None,
        batch_size: int = 45,
        batch_count: int | None = None,
        full_refresh: bool = False,
        allow_fresh_start: bool = False,
        max_workers: int = 4,
        text_workers: int = 8,
        use_iceberg: bool = False,
        enrich_text: bool = True,
        chunk_size: int = 0,
        scratch_originals: bool = False,
        allow_unbounded_originals: bool = False,
        verbose: bool = False,
    ) -> None:
        self.agency = agency
        self.output_dir = output_dir
        self.since_year = since_year
        self.skip_upload = skip_upload
        self.skip_comments = skip_comments
        self.only_comments = only_comments
        self.batch_number = batch_number
        self.batch_size = batch_size
        self.batch_count = batch_count
        self.full_refresh = full_refresh
        self.allow_fresh_start = allow_fresh_start
        self.max_workers = max_workers
        self.text_workers = text_workers
        self.use_iceberg = use_iceberg
        self.enrich_text = enrich_text
        self.chunk_size = chunk_size
        self.scratch_originals = scratch_originals
        self.allow_unbounded_originals = allow_unbounded_originals
        self.verbose = verbose
        self._sweep_agencies: list[str] | None = None
        self._pending_text: PendingCommentText | None = None
        self._text_pool: DerivedTextPool | None = None
        self._staging_dir: Path | None = None
        self._merged_attributes: list[str] = []
        self._unresolved: UnresolvedKeys | None = None
        self._publication_index = None
        self._selected_inputs = None
        self._run_originals: MirrulationsOriginals | None = None

    def _refuse_comments_without_catalog(self) -> None:
        """Comments merge only into the catalog (decision 40), so refuse before any manifest is read."""
        if not self.skip_comments and not self.use_iceberg:
            raise ValueError("Comments ingest only through the catalog: pass --use-iceberg, or --skip-comments")

    def run(self, *, manifest: Manifest | None = None) -> None:
        """Run one batch; local batches sharing output_dir may reuse their manifest."""
        self._refuse_comments_without_catalog()
        if manifest is not None and self.full_refresh:
            raise ValueError("A reused manifest cannot be combined with full_refresh")
        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        from spicy_regs.selected_generations import SelectedInputs, unique_build_directory
        self._selected_inputs = SelectedInputs(output_dir, unique_build_directory(output_dir))
        self._publication_index = self._selected_inputs.index
        self._pending_text: PendingCommentText | None = None
        self._text_pool: DerivedTextPool | None = None
        started = monotonic()
        if manifest is None:
            manifest = (
                Manifest.empty()
                if self.full_refresh
                else Manifest.load(
                    output_dir,
                    allow_fresh_start=self.allow_fresh_start,
                )
            )
        logger.info("ETL manifest ready in {:.1f}s", monotonic() - started)
        self._refuse_unbounded_retention(manifest)
        with ExitStack() as resources:
            # Exits last: an uncommitted artifact finishes after the pool has stopped reading.
            originals = self._run_originals = resources.enter_context(self._retention(output_dir))
            if self.enrich_text and not self.skip_comments:
                self._pending_text = PendingCommentText(output_dir, index=self._publication_index)
                self._text_pool = resources.enter_context(
                    DerivedTextPool(
                        lambda: originals.resource(mirrulations.s3_resource()),
                        max_workers=self.text_workers,
                        on_fill=originals.expect_fill,
                    )
                )
            self._run(manifest)
        logger.info("ETL batch completed in {:.1f}s", monotonic() - started)

    def _refuse_unbounded_retention(self, manifest: Manifest) -> None:
        """Refuse a whole-mirror re-read through per-object retention unless explicitly allowed.

        A full refresh or fresh start without an agency or year scope re-reads every
        Mirrulations key (26.6M comments on 2026-09-28). Retention is serialized at about
        4 ms an object, about 30 hours, past any hosted runner's limit. Retaining that
        history belongs to the bundled back-fill planned in ticket ss-te3x.
        """
        scoped = self.agency is not None or getenv("AGENCIES") is not None or self.since_year is not None
        if manifest.inherits_nothing and not scoped and not self.allow_unbounded_originals:
            raise ValueError(
                "A full refresh or fresh start over every agency would retain each Mirrulations object one at a "
                "time (about 30 hours for the whole mirror). Scope it with --agency or --since-year, use the "
                "bundled back-fill planned in ticket ss-te3x, or pass --allow-unbounded-originals."
            )

    def run_sweep(self, *, budget_minutes: float | None = None, start_batch: int = 0) -> int | None:
        """Reuse membership and agency discovery; commit every pass before the next.

        Every agency's dockets and documents come first, in one pass that publishes
        them once; the batches then read comments only. The documents rode in the
        comment batches, which take about 74 minutes even with 10 new keys (run
        42769233858), so a sweep reached two or three of the 15 and on 2026-11-10
        twelve batches' documents still ended on 10-03. The pass lists the whole
        mirror, as the 15 batches did in 42 minutes on 2026-10-03 (run 37147050861).
        With ``only_comments`` the first pass is skipped; with ``skip_comments`` it
        is the whole sweep.

        Stop on failure: continuing with uncommitted in-memory keys could retire
        a failed batch when a later batch publishes its manifest. A fresh runner
        resumes from the last durable checkpoint; final publication is separate.

        With ``budget_minutes``, a batch starts only while the time since the sweep
        began plus an estimate for one more batch fits the budget. The estimate is
        the longest batch this process has completed, and at least
        ``SWEEP_BATCH_FLOOR_SECONDS``. A sweep that stops there returns the batch
        (0-indexed) it stopped before and writes it to ``SWEEP_STOPPED_NAME``; every
        batch before it is already committed, so the caller can exit cleanly and
        publish. ``None`` means every batch ran. The hosted limit killed run
        42674437011 (2026-11-09) inside its second batch and nothing was published.

        ``start_batch`` begins the round at that batch and wraps to the first, so a
        sweep that resumes where the last one stopped still visits every batch once.
        Sweeps that always began at the first batch would never reach the later ones
        while a batch that merges new comment rows takes 79.4 minutes (run 42674437011).
        """
        if (self.batch_number is not None or not self.batch_count or self.batch_count < 1 or self.full_refresh
                or self.chunk_size):
            raise ValueError("A sweep requires positive batch_count, no batch_number, chunk_size or full_refresh")
        if not 0 <= start_batch < self.batch_count:
            raise ValueError(f"start_batch {start_batch} is outside 0..{self.batch_count - 1}")
        self._refuse_comments_without_catalog()
        started = monotonic()
        output_dir = self.output_dir or Path.cwd() / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / SWEEP_STOPPED_NAME).unlink(missing_ok=True)
        self._sweep_agencies = self._agencies()
        scope = self.skip_comments, self.only_comments
        try:
            self.batch_number = 0
            self._agencies()  # Refuse an oversized batch before hashing the manifest.
            manifest = Manifest.load(output_dir, allow_fresh_start=self.allow_fresh_start)
            if not self.only_comments:
                self.batch_number, self.skip_comments = None, True
                logger.info("ETL sweep: dockets and documents of all {} agencies", len(self._sweep_agencies))
                self.run(manifest=manifest)
                self.skip_comments = scope[0]
            if self.skip_comments:
                return None
            self.only_comments = True
            longest = 0.0
            for batch in (*range(start_batch, self.batch_count), *range(start_batch)):
                self.batch_number = batch
                if not self._agencies():
                    continue
                batch_started = monotonic()
                estimate = max(longest, SWEEP_BATCH_FLOOR_SECONDS)
                if budget_minutes is not None and batch_started - started + estimate > budget_minutes * 60:
                    logger.warning(
                        "ETL sweep stopped before batch {}/{}: {:.0f} of {:.0f} budget minutes are used and one more "
                        "batch is estimated at {:.0f}. Every earlier batch is committed; the rest wait for the next sweep",
                        batch + 1, self.batch_count, (batch_started - started) / 60, budget_minutes, estimate / 60,
                    )
                    (output_dir / SWEEP_STOPPED_NAME).write_text(f"{batch}\n")
                    return batch
                logger.info("ETL sweep batch {}/{}", batch + 1, self.batch_count)
                self.run(manifest=manifest)
                longest = max(longest, monotonic() - batch_started)
            return None
        finally:
            self.batch_number = None
            self._sweep_agencies = None
            self.skip_comments, self.only_comments = scope

    def _run(self, manifest: Manifest) -> None:
        state_dir = self.output_dir or (Path.cwd() / "output")
        from spicy_regs.selected_generations import unique_build_directory
        build_dir = self._selected_inputs.directory if self._selected_inputs is not None else unique_build_directory(state_dir)
        output_dir = build_dir / "processing"
        output_dir.mkdir(parents=True, exist_ok=True)
        staging_dir = output_dir / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        self._staging_dir = staging_dir

        record_types = self._record_types()
        agencies = self._agencies()

        # Chunked comments path (see _ingest_comments_chunked). Only the Iceberg
        # comments-only case can use it: the catalog is a row-level upsert
        # surface, so each chunk commits on its own.
        if self.chunk_size and self.only_comments and self.use_iceberg:
            for agency in agencies:
                self._ingest_comments_chunked(agency, output_dir, staging_dir, manifest)
            self._retry_text(output_dir, agencies)
            if self._pending_text is not None:
                self._save_checkpoints(publish=not self.skip_upload)
                if not self.skip_upload:
                    self._publish(output_dir, record_types, {})
            rmtree(staging_dir, ignore_errors=True)
            logger.info("Done!")
            return

        # 1. Prime: load processed-key manifest + existing output for incremental work.
        started = monotonic()
        if self.full_refresh:
            logger.info("Full refresh — ignoring manifest and existing output")
        else:
            self._download_existing(output_dir, record_types)

        logger.info("ETL priming completed in {:.1f}s", monotonic() - started)
        started = monotonic()
        # 2. Extract → stage: fan agencies out, pumping each source into staging.
        logger.info(
            "Processing {} agencies × {} record types ({} workers)",
            len(agencies),
            len(record_types),
            self.max_workers,
        )
        unresolved = UnresolvedKeys(self.output_dir or Path.cwd() / "output", index=self._publication_index)
        self._unresolved = unresolved
        source_types = {rt.name: source_record_type(rt) for rt in record_types}
        originals = self._originals()
        read = mirrulations.reader_factory(
            list(source_types.values()),
            processed_keys=manifest,
            unresolved_keys=unresolved.for_reader,
            since_year=self.since_year,
            verbose=self.verbose,
            resource_factory=lambda: originals.resource(mirrulations.s3_resource()),
        )
        result = stage_agencies(
            agencies,
            record_types,
            staging_dir,
            lambda agency, rt: read(agency, source_types[rt.name]),
            transform_for=self._transform_for,
            max_workers=self.max_workers,
        )
        # Before any merge or manifest can use or retire what was read.
        originals.commit(result.consumed_keys)
        logger.info("ETL source staging completed in {:.1f}s", monotonic() - started)
        started = monotonic()
        # 3. Transform: merge per-agency staging into the deduplicated dataset.
        staged = result.rows_by_type
        if any(staged.values()):
            self._merge(staging_dir, output_dir, record_types, staged)
            rmtree(staging_dir, ignore_errors=True)
            # Rollups (feed_summary, agency_stats, agency_monthly_volume,
            # docket_search, discovery_signals, fr_docket_links) are no
            # longer built here. Each is materialized by
            # its own decoupled pipeline (`run-rollup-*`) on an independent cron,
            # reading the base tables this ETL publishes below.
        else:
            logger.info("No new records staged; skipping merge.")

        self._retry_text(output_dir, agencies)
        logger.info("ETL merge and text retries completed in {:.1f}s", monotonic() - started)
        started = monotonic()
        # 4. Load: persist the manifest, then publish to R2 (off by default while vetting).
        unresolved.update(result.consumed_keys, result.unresolved)
        self._save_checkpoints(publish=not self.skip_upload)
        manifest.record(result.consumed_keys)
        manifest.save(self.output_dir or Path.cwd() / "output")
        if self.skip_upload:
            logger.info("skip_upload=True — output left in {}", output_dir)
        else:
            logger.info("Uploading to R2...")
            self._publish(output_dir, record_types, staged)

        logger.info("ETL checkpoint and publication completed in {:.1f}s", monotonic() - started)
        logger.info("Done!")

    def _retry_text(self, output_dir: Path, agencies: list[str]) -> None:
        if self._pending_text is not None and self._text_pool is not None:
            self._pending_text.retry(self._text_pool, output_dir, agencies, before_write=self._originals().commit)
            # Texts fetched but not written are kept too, before checkpoints retire anything.
            self._originals().commit()

    def _publish(
        self,
        output_dir: Path,
        record_types: list[RecordType],
        staged: dict[str, int],
    ) -> None:
        """Publish this run's output to R2, advancing the manifest strictly last.

        ``manifest.parquet`` is the restart checkpoint: a fresh workspace pulls
        it from R2 and skips every key it lists, so publishing it retires the
        work this run consumed. Publish it while a data file is still missing
        and the next run skips those keys as already processed — the rows stay
        missing from the bucket until a ``--full-refresh``. So every planned
        object clears its size guard first, and the manifest uploads last, once
        every data upload has returned.
        """
        state_dir = self.output_dir or Path.cwd() / "output"
        manifest_file = state_dir / "manifest.parquet"
        if manifest_file.exists():
            r2.preflight_uploads(state_dir, [manifest_file])
            r2.upload_file(manifest_file, remote_key="manifest.parquet")

    def _save_checkpoints(self, *, publish: bool) -> None:
        from spicy_regs.pipelines.regulatory_publication import finish_checkpoints, restore_checkpoint

        root = self.output_dir or Path.cwd() / "output"
        checkpoints = {
            "failed_keys": list(self._unresolved.rows.values())
            if self._unresolved is not None
            else restore_checkpoint(root, "failed_keys", inputs=self._selected_inputs),
            "pending_comment_text": list(self._pending_text.rows.values())
            if self._pending_text is not None
            else restore_checkpoint(root, "pending_comment_text", inputs=self._selected_inputs),
        }
        finish_checkpoints(root, checkpoints, publish=publish, inputs=self._selected_inputs)
        self._publication_index = self._selected_inputs.index if self._selected_inputs is not None else None

    def _ingest_comments_chunked(self, agency: str, output_dir: Path, staging_dir: Path, manifest: Manifest) -> None:
        """Ingest one agency's comments in bounded key-chunks, committing each.

        Downloads at most ``chunk_size`` comment files at a time, stages them,
        MERGEs them into the catalog, then deletes the staging chunk — so peak
        memory stays at about one chunk and a multi-million-comment agency, which
        would otherwise buffer every record and OOM, ingests in durable pieces.
        """
        comment_rt = RECORD_TYPES["comments"]
        resource = mirrulations.s3_resource()
        keys = mirrulations.list_agency_files_by_type(
            resource,
            mirrulations.BUCKET,
            mirrulations.PREFIX,
            agency,
            [source_record_type(comment_rt)],
            processed_keys=manifest,
            since_year=self.since_year,
            verbose=self.verbose,
        )["comments"]
        # Each chunk checkpoints the manifest; an attribute row merged only at the end could be lost with a crash, so
        # this manual path stages none (the full comment re-read seeds comment_attributes, and the scheduled ETL feeds it).
        transform = self._transform_for(comment_rt, attributes=False)
        total = len(keys)
        logger.info("[{}] comments: {} files, ingesting in chunks of {}", agency, total, self.chunk_size)

        unresolved = UnresolvedKeys(self.output_dir or Path.cwd() / "output", index=self._publication_index)
        self._unresolved = unresolved
        previous = unresolved.for_reader(agency, comment_rt)
        keys = list(dict.fromkeys([*(item.key for item in previous), *keys]))
        total = len(keys)
        for start in range(0, total, self.chunk_size):
            chunk = keys[start : start + self.chunk_size]
            reader = mirrulations.MirrulationsReader(
                self._originals().resource(resource),
                mirrulations.BUCKET,
                mirrulations.PREFIX,
                agency,
                source_record_type(comment_rt),
                key_lister=lambda: chunk,
                unresolved_keys=[item for item in previous if item.key in chunk],
            )
            records = list(transform.apply(reader.iter_records()))
            self._originals().commit(reader.last_keys)
            if records:
                write_staging(agency, comment_rt.name, records, staging_dir, comment_rt.schema)
                iceberg.merge_comments(staging_dir, comment_rt)
                rmtree(staging_dir / comment_rt.name, ignore_errors=True)
            # Only a completed read and successful merge can retire a key.
            unresolved.update(reader.last_keys, {(agency, comment_rt.name): reader.unresolved})
            self._save_checkpoints(publish=not self.skip_upload)
            manifest.record(reader.last_keys)
            manifest.save(self.output_dir or Path.cwd() / "output")
            if not self.skip_upload:
                self._publish(output_dir, [comment_rt], {comment_rt.name: len(records)})
            logger.info("[{}] comments: committed {}/{}", agency, start + len(chunk), total)

    # -- regulations-specific wiring ---------------------------------------

    def _originals(self) -> MirrulationsOriginals:
        if self._run_originals is None:
            raise RuntimeError("Mirrulations originals are retained only inside run()")
        return self._run_originals

    def _retention(self, output_dir: Path) -> MirrulationsOriginals:
        """This run's original retention: the local raw archive always, and R2 whenever the run writes durably.

        A ``skip_upload`` run with the catalog still merges rows into it, so its
        originals publish too; only a fully local run keeps them local alone.
        """
        return MirrulationsOriginals(
            output_dir,
            mirrulations.BUCKET,
            publish=not self.skip_upload or self.use_iceberg,
            scratch=self.scratch_originals,
        )

    def _transform_for(self, record_type: RecordType, *, attributes: bool = True) -> Transform:
        """Build the staging transform for one record type.

        All agencies share the run's bounded comment-text pool and retry state. Decisions 65-67: the same read also
        yields the record's attributes row, for comments after the reviewed exclusions so an excluded comment gets
        none. ``attributes=False`` (the chunked comments path) stages no attribute rows.
        """
        extract: Transform = ExtractRecords(record_type)
        table = ATTRIBUTE_TABLES.get(record_type.name) if attributes else None
        tee = TeeAttributes(table, self._staging_dir) if table is not None and self._staging_dir is not None else None
        if record_type.name != "comments":
            return Chain(tee, extract) if tee is not None else extract
        steps: list[Transform] = [ExcludeReviewedComments(keyed=tee is not None), *([tee] if tee else []), extract]
        if self._text_pool is not None and self._pending_text is not None:
            steps.append(EnrichCommentText(self._text_pool, observe=self._pending_text.observe))
        return Chain(*steps)

    def _record_types(self) -> list[RecordType]:
        """The record types to process, honoring skip/only-comments."""
        names = list(RECORD_TYPES)
        if self.skip_comments:
            names = [n for n in names if n != "comments"]
        elif self.only_comments:
            names = ["comments"]
        return [RECORD_TYPES[n] for n in names]

    def _agencies(self) -> list[str]:
        """Agencies to process: explicit, AGENCIES env, or S3 discovery; then batched.

        ``batch_count`` splits the list into that many contiguous batches of
        ``ceil(len / batch_count)`` agencies, overriding ``batch_size``.
        """
        if self._sweep_agencies is not None:
            agencies = self._sweep_agencies
        elif self.agency is not None:
            agencies = [self.agency]
        elif (agencies_env := getenv("AGENCIES")) is not None:
            agencies = agencies_env.split(",")
        else:
            agencies = mirrulations.discover_agencies()

        if self.batch_number is not None:
            size = self.batch_size
            if self.batch_count is not None:
                if not 0 <= self.batch_number < self.batch_count:
                    raise ValueError(f"batch_number {self.batch_number} is outside 0..{self.batch_count - 1}")
                size = -(-len(agencies) // self.batch_count)
                if size > MAX_DERIVED_BATCH_SIZE:
                    raise RuntimeError(
                        f"{len(agencies)} agencies over {self.batch_count} batches needs {size} per batch, "
                        f"above the ceiling of {MAX_DERIVED_BATCH_SIZE}; raise BATCH_COUNT in etl-new-pipeline.yml"
                    )
            start = self.batch_number * size
            agencies = agencies[start : start + size]
        return agencies

    def _download_existing(self, output_dir: Path, record_types: list[RecordType]) -> None:
        """Fetch existing output from R2 so an incremental run appends to it.

        Monolithic ``{type}.parquet`` files are pulled whole. The attribute tables are not: the day's rows upsert
        over the selected native table without restoring it (:func:`finish_attribute_dataset`).
        """
        from spicy_regs.pipelines.regulatory_publication import restore_dataset

        root = self.output_dir or Path.cwd() / "output"
        for rt in record_types:
            if rt.name != "comments":
                restore_dataset(root, rt.name, output_dir / f"{rt.name}.parquet", inputs=self._selected_inputs)

    def _merge_attributes(self, staging_dir: Path, output_dir: Path, table: str) -> bool:
        """Dedupe this run's attribute rows into ``output_dir/<table>.parquet``, once the full sweep has seeded the table.

        Before the table is seeded (``run-attributes-sweep`` for documents and dockets, the comment re-read's
        ``fill-comment-fields attributes`` for comments), a daily run's rows would publish a table of only
        that day's changes, so they are dropped with a warning and the sweep's pass covers them. The file holds only
        the day's rows; :func:`finish_attribute_dataset` upserts them over the selected native table.
        """
        parts = staging_dir / table
        if not any(parts.glob("*.parquet")):
            return False
        if self._selected_inputs is None or self._selected_inputs.select(table) is None:
            seeder = (
                "fill-comment-fields attributes" if table == ATTRIBUTE_TABLES["comments"] else "run-attributes-sweep"
            )
            logger.warning(
                "{}: no seeded native table yet; this run's rows are dropped until {} seeds it", table, seeder
            )
            return False
        rows = merge_attribute_parts(table, parts, None, output_dir / f"{table}.parquet")
        logger.info("{}: {:,} rows fetched this run", table, rows)
        return True

    def _merge(
        self,
        staging_dir: Path,
        output_dir: Path,
        record_types: list[RecordType],
        staged: dict[str, int],
    ) -> None:
        """Merge staging files: dockets and documents monolithically, comments into the catalog.

        ``comments`` always go through :func:`iceberg.merge_comments` (a
        row-level upsert into the catalog; the final mirror owns index
        publication). ``dockets`` and ``documents`` merge into the selected
        generation that :meth:`_download_existing` restored.

        Owner decision 2026-11-09: dockets (15.5 MB) and documents (91 MB) stay
        whole-file, since a nightly rewrite takes seconds; Iceberg is for comments
        (9.7 GB). The dockets catalog route was deleted after its never-seeded
        native table exported only the day's rows, which the upload shrink guard
        refused on every nightly run from 2026-10-31 (run 42574758742).
        """
        names = [rt.name for rt in record_types]
        if "comments" in names:
            validate_staged_comments(staging_dir)

        non_comment = [n for n in names if n != "comments"]
        if non_comment:
            schemas = {n: RECORD_TYPES[n].schema for n in non_comment}
            dedup_keys = {n: RECORD_TYPES[n].dedup_key for n in non_comment}
            merge_staging_files(staging_dir, output_dir, non_comment, schemas, dedup_keys)

        self._merged_attributes = [
            table
            for name in names
            if (table := ATTRIBUTE_TABLES.get(name)) and self._merge_attributes(staging_dir, output_dir, table)
        ]

        from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset, finish_dataset

        root = self.output_dir or Path.cwd() / "output"
        for name in non_comment:
            source = output_dir / f"{name}.parquet"
            if source.exists():
                finish_dataset(root, name, source, publish=not self.skip_upload, inputs=self._selected_inputs)
        for table in self._merged_attributes:
            finish_attribute_dataset(root, table, output_dir / f"{table}.parquet",
                                     publish=not self.skip_upload, inputs=self._selected_inputs)

        if "comments" in names and staged.get("comments", 0) > 0:
            # Finalization owns the one mirror/index build after the sweep.
            iceberg.merge_comments(staging_dir, RECORD_TYPES["comments"])


# --- CLI / run file --------------------------------------------------------

app = App(name="run-pipeline", help="Run the regulations.gov ETL through the Pipeline contract.")


@app.default
def main(
    *,
    agency: Annotated[str | None, Parameter(help="Process only this agency")] = None,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    since_year: Annotated[int | None, Parameter(help="Only process dockets from this year onward")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    skip_comments: Annotated[bool, Parameter(help="Skip comments")] = False,
    only_comments: Annotated[bool, Parameter(help="Only process comments")] = False,
    batch_number: Annotated[int | None, Parameter(help="Batch number (0-indexed)")] = None,
    batch_size: Annotated[int, Parameter(help="Agencies per batch")] = 45,
    batch_count: Annotated[
        int | None,
        Parameter(help="Split the agencies into this many batches, deriving each batch's size (overrides batch-size)"),
    ] = None,
    sweep: Annotated[
        bool,
        Parameter(help="With one manifest, read every agency's dockets and documents, then each batch's comments; "
                       "requires --batch-count"),
    ] = False,
    sweep_budget_minutes: Annotated[
        float | None,
        Parameter(help="With --sweep: start no batch that is not expected to finish within this many minutes of "
                       "the sweep's start. Completed batches stay committed and the exit is clean"),
    ] = None,
    sweep_start_batch: Annotated[
        int,
        Parameter(help="With --sweep: begin at this batch (0-indexed) and wrap to the first, to resume a sweep "
                       "that stopped at its budget"),
    ] = 0,
    defer_comments_publication: Annotated[
        bool,
        Parameter(help="Caller will finalize the comments mirror after ingestion (workflow use)"),
    ] = False,
    full_refresh: Annotated[bool, Parameter(help="Ignore manifest + existing output")] = False,
    allow_fresh_start: Annotated[
        bool,
        Parameter(help="Start from an empty manifest when none exists locally or on R2 (first run or bootstrap)"),
    ] = False,
    max_workers: Annotated[int, Parameter(help="Agencies processed in parallel")] = 4,
    text_workers: Annotated[int, Parameter(help="Comment text reads in parallel across all agencies (at most 16)")] = 8,
    use_iceberg: Annotated[
        bool,
        Parameter(help="Route comments through R2 Data Catalog (Iceberg); comments require it"),
    ] = False,
    enrich_text: Annotated[
        bool,
        Parameter(help="Fill comment text_content inline from Mirrulations derived-data extracted text"),
    ] = True,
    chunk_size: Annotated[
        int,
        Parameter(
            help="Ingest comments in bounded key-chunks of this size (0 = whole agency at once). "
            "Use for agencies too large to buffer in memory; requires --only-comments --use-iceberg."
        ),
    ] = 0,
    scratch_originals: Annotated[
        bool,
        Parameter(help="Delete each batch's local Mirrulations originals once R2 has admitted them (hosted runners)"),
    ] = False,
    allow_unbounded_originals: Annotated[
        bool,
        Parameter(help="Allow a full refresh or fresh start over every agency to retain originals one object at a "
                       "time (about 30 hours for the whole mirror); prefer the bundled back-fill (ss-te3x)"),
    ] = False,
    verbose: Annotated[bool, Parameter(name=["--verbose", "-v"], help="Verbose logging")] = False,
) -> None:
    """Run the regulations.gov ETL pipeline."""
    load_dotenv()
    pipeline = RegulationsPipeline(
        agency=agency,
        output_dir=output_dir,
        since_year=since_year,
        skip_upload=skip_upload,
        skip_comments=skip_comments,
        only_comments=only_comments,
        batch_number=batch_number,
        batch_size=batch_size,
        batch_count=batch_count,
        full_refresh=full_refresh,
        allow_fresh_start=allow_fresh_start,
        max_workers=max_workers,
        text_workers=text_workers,
        use_iceberg=use_iceberg,
        enrich_text=enrich_text,
        chunk_size=chunk_size,
        scratch_originals=scratch_originals,
        allow_unbounded_originals=allow_unbounded_originals,
        verbose=verbose,
    )
    if (sweep_budget_minutes is not None or sweep_start_batch) and not sweep:
        raise ValueError("--sweep-budget-minutes and --sweep-start-batch apply only to --sweep")
    if sweep:
        pipeline.run_sweep(budget_minutes=sweep_budget_minutes, start_batch=sweep_start_batch)
    else:
        pipeline.run()
    if use_iceberg and not skip_upload and not skip_comments and not defer_comments_publication:
        from spicy_regs.pipelines.comments_mirror import publish_comments_mirror

        publish_comments_mirror(output_dir or Path.cwd() / "output")


if __name__ == "__main__":
    app()
