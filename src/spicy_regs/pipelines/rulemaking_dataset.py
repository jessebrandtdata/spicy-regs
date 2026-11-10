"""Materialized dataset pipeline for the rulemaking join surface."""

from __future__ import annotations

from functools import cache
import json
import os
from pathlib import Path
import shutil
from typing import Annotated, ClassVar

from cyclopts import App, Parameter

from spicy_regs.ontology.common import RunContext
from spicy_regs.ontology.federal_register import FederalRegisterIndex
from spicy_regs.pipelines.materialized import DatasetStage, MaterializedDatasetPipeline
from spicy_regs.transforms.regulations_receipts import policy
from spicy_regs.transforms.build_agency_lifecycle_stats import build_agency_lifecycle_stats
from spicy_regs.transforms.build_comment_periods import build_comment_periods
from spicy_regs.transforms.build_lifecycles import build_lifecycles
from spicy_regs.transforms.build_proceedings import build_proceedings
from spicy_regs.transforms.build_regulatory_agenda import build_regulatory_agenda
from spicy_regs.transforms.build_rule_targets import build_rule_targets


#: Sources read as their pinned published tables, never through receipts: every field a stage reads is a public
#: column there, and their families stop publishing receipts (ss-vse3).
PUBLISHED_SOURCES = ("federal_register.parquet", "fr_docket_links.parquet")


def stage_text(dataset: str, path: Path) -> None:
    """Rewrite a published native table in place in the text shape the stages read.

    Each native list becomes the JSON text of its source column (``docket_ids`` as ``docket_ids_json``) and every
    other column its text; a file already in that shape is left alone. It is the published value, not the
    publisher's spelling: an agency entry keeps ``id``, ``name``, ``raw_name``, ``parent_id`` and ``slug``, and a CFR
    part reads as text whether the Register wrote ``60`` or ``"60"``, which the stages' parsers take alike.
    """
    import duckdb
    import pyarrow.parquet as pq

    from spicy_regs.transforms.regulations_shape import NATIVE_FIELDS, SOURCE_COLUMNS

    held = set(pq.read_schema(path).names)
    native = {source: target for source, (target, _) in NATIVE_FIELDS[dataset].items() if source != target}
    if not held & set(native.values()):
        return
    columns = []
    for name, _ in SOURCE_COLUMNS[dataset]:
        target = native.get(name, name)
        if target not in held:
            continue
        columns.append(f"array_to_string({target}, ',') AS {name}" if name == "agency_slugs" else
                       f"CAST(to_json({target}) AS VARCHAR) AS {name}" if name in native else
                       f"CAST({target} AS VARCHAR) AS {name}")
    text = path.with_name(path.name + ".text")
    duckdb.sql(f"COPY (SELECT {', '.join(columns)} FROM read_parquet('{path}')) "
               f"TO '{text}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    text.replace(path)


class RulemakingDatasetPipeline(MaterializedDatasetPipeline):
    """Build one coherent generation of the rule-identity tables.

    A rule is a RIN on reginfo, a docket on regulations.gov, a set of CFR parts
    in the Code, and a document number in the Federal Register. Nothing joins
    the four. These stages do: ``rule_targets`` is the docket ↔ CFR ↔ RIN
    spine; ``proceedings`` promotes each rulemaking to a first-class record with
    its actions; ``regulatory_agenda`` links agenda items to those actions;
    ``comment_periods`` gives every notice stating a comment close its period, a named
    extension merged and a reopening apart;
    ``lifecycles`` pairs each docketed proceeding's proposal with its final from
    its cleaned events; and ``agency-lifecycle-stats`` estimates time to final.

    Stages run in dependency order and publish atomically as one generation
    under ``materialized/rulemaking/``, so a consumer never sees a spine from
    one run beside proceedings from another.
    """

    name: ClassVar[str] = "rulemaking-dataset"
    dataset_name: ClassVar[str] = "rulemaking"
    source_inputs: ClassVar[tuple[str, ...]] = (
        "dockets.parquet",
        "documents.parquet",
        "federal_register.parquet",
        "unified_agenda.parquet",
        "fr_docket_links.parquet",
    )
    prior_outputs: ClassVar[tuple[tuple[str, str], ...]] = (
        ("proceedings.parquet", "_proceedings_native_prior.parquet"),
        ("etl_receipts.parquet", "_proceedings_receipts.parquet"),
    )
    published_outputs: ClassVar[tuple[str, ...]] = (
        "rule_targets.parquet",
        "proceedings.parquet",
        "regulatory_agenda_items.parquet",
        "agenda_item_proceedings.parquet",
        "comment_periods.parquet",
        "rulemaking_lifecycles.parquet",
        "lifecycle_events.parquet",
        "agency_lifecycle_stats.parquet",
    )
    receipt_policies: ClassVar[tuple] = tuple(policy(Path(name).stem) for name in published_outputs)

    def _publish(self, *, manifest_path, pointer_path, artifact_paths) -> None:
        """Keep ordinary admission and uploads; guard the captured prior before they start."""
        from spicy_regs.sources import publication, r2

        next_pointer = json.loads(pointer_path.read_bytes(), object_pairs_hook=publication._pairs)
        next_manifest = json.loads(manifest_path.read_bytes(), object_pairs_hook=publication._pairs)
        next_snapshot = next_manifest["snapshot_id"]
        if (not publication.SNAPSHOT_ID.fullmatch(next_snapshot)
                or (next_pointer["dataset"], next_pointer["snapshot_id"], next_pointer["format_version"]) != (
                    "rulemaking", next_snapshot, next_manifest["format_version"])
                or next_manifest["dataset"] != "rulemaking"
                or next_manifest["format_version"] not in publication.SNAPSHOT_FORMAT_VERSIONS
                or next_pointer["manifest_key"] != f"materialized/rulemaking/snapshots/{next_snapshot}/manifest.json"):
            raise ValueError("Rulemaking candidate pointer and manifest differ")
        client = r2.get_r2_client()
        bucket = os.getenv("R2_BUCKET_NAME", "spicy-regs")
        directory = manifest_path.parent
        captured_pointer = directory / "_rulemaking_latest.json"
        captured_manifest = directory / "_rulemaking_previous_manifest.json"
        prior = captured_pointer.read_bytes() if captured_pointer.exists() else None
        stored = publication._get_bounded(client, bucket, publication.SNAPSHOT_POINTER)
        manifest_key = None
        manifest = None
        etag = None
        if prior is None:
            if not self.allow_bootstrap or stored is not None:
                raise ValueError("Rulemaking publication requires its captured full prior pointer")
        else:
            if stored is None or stored[0] != prior or not isinstance(stored[1], str) or not stored[1]:
                raise ValueError("Rulemaking prior pointer changed before publication")
            pointer = json.loads(prior, object_pairs_hook=publication._pairs)
            snapshot = pointer["snapshot_id"]
            manifest_key = f"materialized/rulemaking/snapshots/{snapshot}/manifest.json"
            if (not publication.SNAPSHOT_ID.fullmatch(snapshot) or pointer["dataset"] != "rulemaking"
                    or pointer["format_version"] not in publication.SNAPSHOT_FORMAT_VERSIONS
                    or pointer["manifest_key"] != manifest_key):
                raise ValueError("Rulemaking captured prior pointer is invalid")
            manifest = captured_manifest.read_bytes()
            old = json.loads(manifest, object_pairs_hook=publication._pairs)
            if (old["dataset"], old["snapshot_id"], old["format_version"]) != (
                    "rulemaking", snapshot, pointer["format_version"]):
                raise ValueError("Rulemaking captured prior manifest differs from its pointer")
            current_manifest = publication._get_bounded(client, bucket, manifest_key)
            if current_manifest is None or current_manifest[0] != manifest:
                raise ValueError("Rulemaking prior manifest changed before publication")
            etag = stored[1]
        self._rulemaking_pointer_guard = (client, bucket, etag, prior, manifest_key, manifest)
        try:
            super()._publish(manifest_path=manifest_path, pointer_path=pointer_path, artifact_paths=artifact_paths)
        finally:
            del self._rulemaking_pointer_guard

    def _publish_pointer(self, pointer_path: Path) -> None:
        from spicy_regs.sources import publication
        from spicy_regs.sources.cloudflare import purge_urls

        client, bucket, etag, prior, manifest_key, manifest = self._rulemaking_pointer_guard
        stored = publication._get_bounded(client, bucket, publication.SNAPSHOT_POINTER)
        if (stored is None) != (prior is None) or stored is not None and (stored[0] != prior or stored[1] != etag):
            raise ValueError("Rulemaking prior pointer changed during publication")
        if manifest_key is not None:
            current_manifest = publication._get_bounded(client, bucket, manifest_key)
            if current_manifest is None or current_manifest[0] != manifest:
                raise ValueError("Rulemaking prior manifest changed during publication")
        if not publication._put_pointer(client, bucket, publication.SNAPSHOT_POINTER, pointer_path.read_bytes(), etag):
            raise ValueError("Rulemaking latest pointer changed at conditional publication")
        public_url = os.getenv("R2_PUBLIC_URL", "")
        if public_url:
            purge_urls([f"{public_url.rstrip('/')}/{publication.SNAPSHOT_POINTER}"])

    def _prime_sources(self, output_dir: Path) -> None:
        """Restore stage inputs from one captured index: native pairs through their receipts, and the two
        Register tables (``PUBLISHED_SOURCES``) as their pinned published tables."""
        from contextlib import nullcontext

        from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
        from spicy_regs.selected_generations import unique_build_directory
        from spicy_regs.sources import publication, r2

        public_url = os.getenv("R2_PUBLIC_URL")
        with publication.snapshot(public_url) if public_url else nullcontext():
            self._selected_priors = SelectedPriors(unique_build_directory(output_dir), root=output_dir)
            for key in self.source_inputs:
                if key not in PUBLISHED_SOURCES:
                    if not self._selected_priors.download(key, output_dir / key):
                        raise ValueError(f"Rulemaking requires selected native input {key}")
                # A local run reads the table already beside it, as every materialized source does.
                elif not (not public_url and (output_dir / key).exists()) and not r2.download(key, output_dir / key):
                    raise ValueError(f"Rulemaking requires published input {key}")
        stage_text("federal_register", output_dir / "federal_register.parquet")

    def _prime_previous_generation(self, output_dir: Path) -> dict | None:
        """Restore proceedings identity only from its admitted materialized pair."""
        from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal

        manifest = super()._prime_previous_generation(output_dir)
        if manifest is not None:
            expected = {"key": "etl_receipts.parquet", "generationId": manifest["run_id"],
                        "policies": [p.descriptor() for p in self.receipt_policies]}
            if manifest.get("etlReceipts") != expected:
                raise ValueError("Prior rulemaking requires native subjects and their declared receipts; migrate explicitly")
            materialize_internal(
                ReceiptInput("proceedings", (output_dir / "_proceedings_native_prior.parquet",),
                             output_dir / "_proceedings_receipts.parquet", manifest["run_id"]),
                output_dir / "_proceedings_prior.parquet",
            )
        else:
            for name in ("_proceedings_prior.parquet", "_proceedings_native_prior.parquet", "_proceedings_receipts.parquet"):
                (output_dir / name).unlink(missing_ok=True)
        return manifest

    def _input_snapshot(self, output_dir: Path, previous_manifest: dict | None) -> dict:
        from spicy_regs.court_receipts import file_witness

        snapshot = super()._input_snapshot(output_dir, previous_manifest)
        snapshot["native_inputs"] = {
            name: {"generationId": generation, "subjects": [file_witness(p) for p in subjects],
                   "receipts": file_witness(receipts)}
            for name, (subjects, receipts, generation) in self._selected_priors.selections.items()
        }
        # A published source has no receipt pair to witness: its pin below names the generation read.
        snapshot["native_inputs"] |= {Path(key).stem: {} for key in PUBLISHED_SOURCES}
        index = self._selected_priors.selected.index
        if index is not None:
            from spicy_regs.sources.publication import table_pin
            for name, value in snapshot["native_inputs"].items():
                value["publication"] = table_pin(index, name + ".parquet")
        return snapshot

    def _classify_outputs(self, output_dir, context):
        """Classify every stage output before materialized admission or upload."""
        from spicy_regs.etl_receipts import combine_receipts
        from spicy_regs.selected_generations import unique_build_directory
        from spicy_regs.transforms.regulations_receipts import write_held_dataset

        candidate = unique_build_directory(output_dir)
        receipts, subjects = [], []
        prior = output_dir / "_proceedings_receipts.parquet"
        for key in self.published_outputs:
            subject, receipt = write_held_dataset(
                Path(key).stem, output_dir / key, candidate / Path(key).stem,
                generation_id=context.run_id, prior_receipts=[prior] if prior.exists() else (),
            )
            subjects.append((subject, output_dir / key))
            receipts.append(receipt)
        combined = combine_receipts(receipts, candidate / "etl_receipts.parquet")
        for source, target in subjects:
            shutil.copyfile(source, target)
        shutil.copyfile(combined, output_dir / "etl_receipts.parquet")

    def build_native(self, inputs, destination: Path, *, generation_id: str, prior=None):
        """Build the whole local rulemaking generation from qualified native inputs.

        This explicit migration entry point preserves all existing stage rules.
        Prior identity and evidence fields are reconstructed only after receipt
        validation. The returned artifact binds native subjects and receipts;
        no materialized or public pointer changes here.
        """
        from spicy_regs.transforms.regulations_receipts import build_from_receipts

        required = {Path(name).stem for name in self.source_inputs}
        if {selected.dataset for selected in inputs} != required or len(inputs) != len(required):
            raise ValueError("Native rulemaking requires exactly its declared source datasets")
        selected = list(inputs)
        if prior is not None:
            if prior.dataset != "proceedings":
                raise ValueError("Prior rulemaking identity must come from proceedings")
            selected.append(prior)
        context = RunContext.resolve(run_id=generation_id, prefix="rulemaking")

        def build(work):
            if prior is not None:
                (work / "proceedings.parquet").rename(work / "_proceedings_prior.parquet")
            for stage in self._processing_stages():
                stage.build(work, context)

        return build_from_receipts(
            selected, destination, generation_id=generation_id,
            outputs=[Path(name).stem for name in self.published_outputs], builder=build, family="rulemaking",
        )

    def source_column_requirements(self) -> dict[str, tuple[str, ...]]:
        return {
            "dockets.parquet": (
                "docket_id",
                "rin",
                "docket_type",
                "title",
                "abstract",
                "agency_code",
                "modify_date",
            ),
            "documents.parquet": (
                "document_id",
                "docket_id",
                "additional_rins",
                "fr_doc_num",
                "document_type",
                "title",
                "agency_code",
                "posted_date",
                "modify_date",
                "comment_start_date",
                "comment_end_date",
                "withdrawn",
            ),
            "federal_register.parquet": (
                "document_number",
                "title",
                "abstract",
                "document_type",
                "publication_date",
                "comments_close_on",
                "docket_ids_json",
                "regulation_id_numbers_json",
                "cfr_references_json",
                "agencies_json",
                "volume",
                "start_page",
            ),
            "unified_agenda.parquet": (
                "rin",
                "agenda_edition",
                "legal_authority_json",
                "cfr_references_json",
                "title",
                "agency_code",
                "rule_stage",
                "priority_category",
                "major",
                "timetable_json",
                "first_action_date",
                "next_action_date",
                "url",
            ),
            "fr_docket_links.parquet": (
                "document_number",
                "publication_date",
                "docket_id",
            ),
        }

    def _processing_stages(self) -> tuple[DatasetStage, ...]:
        # Every stage reads the one federal_register snapshot, so its index is built once
        # per generation rather than once per stage.
        @cache
        def fr_index(output_dir: Path) -> FederalRegisterIndex:
            return FederalRegisterIndex(output_dir / "federal_register.parquet")

        def rule_targets(output_dir: Path, context: RunContext) -> None:
            build_rule_targets(
                output_dir, run_id=context.run_id, asserted_at=context.asserted_at, fr_index=fr_index(output_dir)
            )

        def proceedings(output_dir: Path, context: RunContext) -> None:
            build_proceedings(
                output_dir, run_id=context.run_id, asserted_at=context.asserted_at, fr_index=fr_index(output_dir),
                allow_output_prior=False,
            )

        def regulatory_agenda(output_dir: Path, context: RunContext) -> None:
            build_regulatory_agenda(
                output_dir, run_id=context.run_id, asserted_at=context.asserted_at, fr_index=fr_index(output_dir)
            )

        def comment_periods(output_dir: Path, context: RunContext) -> None:
            build_comment_periods(
                output_dir, run_id=context.run_id, asserted_at=context.asserted_at, fr_index=fr_index(output_dir)
            )

        def lifecycles(output_dir: Path, context: RunContext) -> None:
            build_lifecycles(
                output_dir, run_id=context.run_id, asserted_at=context.asserted_at, fr_index=fr_index(output_dir)
            )

        def agency_lifecycle_stats(output_dir: Path, context: RunContext) -> None:
            build_agency_lifecycle_stats(output_dir, run_id=context.run_id, asserted_at=context.asserted_at)

        return (
            DatasetStage(
                name="rule-targets",
                depends_on=(),
                outputs=("rule_targets.parquet",),
                build=rule_targets,
            ),
            DatasetStage(
                name="proceedings",
                depends_on=("rule-targets",),
                outputs=("proceedings.parquet",),
                build=proceedings,
            ),
            DatasetStage(
                name="regulatory-agenda",
                depends_on=("proceedings",),
                outputs=("regulatory_agenda_items.parquet", "agenda_item_proceedings.parquet"),
                build=regulatory_agenda,
            ),
            DatasetStage(
                name="comment-periods",
                depends_on=("proceedings",),
                outputs=("comment_periods.parquet",),
                build=comment_periods,
            ),
            DatasetStage(
                name="lifecycles",
                depends_on=("proceedings", "regulatory-agenda"),
                outputs=("rulemaking_lifecycles.parquet", "lifecycle_events.parquet"),
                build=lifecycles,
            ),
            DatasetStage(
                name="agency-lifecycle-stats",
                depends_on=("lifecycles",),
                outputs=("agency_lifecycle_stats.parquet",),
                build=agency_lifecycle_stats,
            ),
        )


    def stages(self) -> tuple[DatasetStage, ...]:
        return (*self._processing_stages(), DatasetStage(
            name="native-outputs", depends_on=("agency-lifecycle-stats", "comment-periods"),
            outputs=("etl_receipts.parquet",), build=self._classify_outputs,
        ))


app = App(
    name="materialize-rulemaking",
    help="Build and atomically publish the rulemaking join-surface dataset.",
)


@app.default
def main(
    *,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    allow_bootstrap: Annotated[
        bool,
        Parameter(help="Allow a first publication with no prior rulemaking generation"),
    ] = False,
) -> None:
    RulemakingDatasetPipeline(
        output_dir=output_dir,
        skip_upload=skip_upload,
        allow_bootstrap=allow_bootstrap,
    ).run()


if __name__ == "__main__":
    app()
