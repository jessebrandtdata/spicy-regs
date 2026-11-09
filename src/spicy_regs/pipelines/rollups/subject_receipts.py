"""Scheduled subject writers with exact, selected-generation processing priors.

Only private working files restore processing fields. The returned outputs are
native subjects and one generation-bound receipt member; no public mirrors.
"""

from __future__ import annotations

import inspect
import os
import json
import shutil
from pathlib import Path
from typing import ClassVar

import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.congress_subjects import INPUT_COLUMNS as CONGRESS
from spicy_regs.congress_receipts import (
    ACQUISITION_POLICY,
    CongressInput,
    _build_events,
    policy as congress_policy,
    restore_processing_input,
    write_congress_dataset,
    has_unmatched_failures,
)
from spicy_regs.etl_receipts import combine_receipts, read_with_receipts, select_receipts, validate_receipt_bundle, selected_subject_policy
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import (
    FILE_POLICY,
    FILE_STATES,
    write_legislative_outputs,
    policy as legislative_policy,
    input_policy as legislative_input_policy,
    restore_prior,
)
from spicy_regs.native_types import described_schema
from spicy_regs.navigation_read_outcomes import (
    POLICIES as NAVIGATION_OUTCOME_POLICIES, READ_TABLES, FILE_TABLES, family_tables, write_recorded_outcomes,
)
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS as REGULATIONS
from spicy_regs.transforms.fec_identity_receipts import dataset_policy as fec_policy
from spicy_regs.transforms.fec_identity_context_fields import REGISTRY as FEC
from spicy_regs.transforms.regulations_receipts import (
    RESTORE_DATASETS,
    ReceiptInput,
    materialize_internal,
    policy as regulations_policy,
    write_held_dataset,
)


def dataset_policy(name):
    if name in CONGRESS:
        return congress_policy(name)
    if name in field_registry():
        return legislative_policy(name)
    if name in REGULATIONS:
        return regulations_policy(name)
    if name in FEC:
        return fec_policy(name)
    raise ValueError(f"No coordinated processing reader for {name}")


class SelectedPriors:
    """Capture one publication index and fetch only its pinned subject/receipt pairs."""

    def __init__(self, directory, *, index=None, public_url=None, root=None, ranged=frozenset()):
        """``ranged`` names the Congress datasets whose receipts may be read by row group (``SelectedInputs.select``):
        another family's soft inputs, each read for its own rows. A family's own priors read many datasets of one
        member, so they fetch it whole once."""
        from spicy_regs.selected_generations import SelectedInputs

        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.selected = SelectedInputs(
            root or self.directory, self.directory / "native", index=index, public_url=public_url
        )
        self.ranged = frozenset(ranged) & set(CONGRESS)
        self.restored = {}
        self.selections = {}
        self.processing_members = {}

    def get(self, dataset):
        if dataset in self.restored:
            if self.restored[dataset] is not None:
                self.selected.select(dataset, ranged=dataset in self.ranged)
                self._check_processing(dataset)
            return self.restored[dataset]
        selected_policy = dataset_policy(dataset)
        selection = self.selected.select(dataset, ranged=dataset in self.ranged)
        if selection is None:
            self.restored[dataset] = None
            return None
        subjects, receipt_path, generation = list(selection.subjects), selection.receipts, selection.generation_id
        if dataset in field_registry():
            selected_policy = legislative_input_policy(dataset, subjects)
        directory = self.directory / dataset
        directory.mkdir()
        # A ranged read already wrote exactly this dataset's receipts, in source order, through the same selector.
        scoped = (receipt_path if dataset in self.selected.ranged
                  else select_receipts(receipt_path, directory / "scoped.parquet", dataset=dataset))
        selected_policy = selected_subject_policy(selected_policy, subjects)
        # The regulatory restore below admits the pair itself: complete admission in
        # regulations_bulk._materialize_selected, or the full read_with_receipts pass in _qualified_rows.
        if dataset in CONGRESS or dataset in FEC:
            validate_receipt_bundle({dataset: subjects}, [scoped], [selected_policy], generation_id=generation)
        self.selections[dataset] = (subjects, receipt_path, generation)
        output = directory / "processing" / (dataset + ".parquet")
        if dataset in CONGRESS:
            output.parent.mkdir(parents=True, exist_ok=True)
            restore_processing_input(
                tuple(subjects), scoped, output, dataset=dataset, generation_id=generation,
                bulk=True, validate=False, scoped=True,  # this exact pair was admitted just above
            )
        elif dataset in field_registry():
            files = select_receipts(receipt_path, directory / "file-receipts.parquet", dataset=FILE_STATES)
            bundle = directory / "legislative"
            bundle.mkdir()
            combine_receipts([scoped, files], bundle / "etl_receipts.parquet")
            relative = []
            for i, subject in enumerate(subjects):
                target = bundle / f"subject-{i}.parquet"
                shutil.copyfile(subject, target)
                relative.append(target.name)
            manifest = dict(
                generation_id=generation,
                datasets=[dataset],
                subjects={dataset: relative},
                receipt_file="etl_receipts.parquet",
                unowned_outputs=[],
                refused_rows={},
            )
            (bundle / "legislative-bundle.json").write_text(json.dumps(manifest))
            output = restore_prior(bundle, directory / "processing")[dataset]
        elif dataset == "fec_committees":
            from spicy_regs.transforms.build_fec_committees import COLUMNS, _SCHEMA
            from spicy_regs.transforms.parquet_rows import write_rows

            def original_rows():
                for row in read_with_receipts(subjects, [scoped], selected_policy, generation_id=generation):
                    original = row["conversion_inputs"]
                    yield {name: original[name] for name in COLUMNS}

            output.parent.mkdir(parents=True, exist_ok=True)
            write_rows(original_rows(), output, _SCHEMA)
        elif dataset == "fec_committee_history":
            import pyarrow as pa
            from spicy_regs.transforms.parquet_rows import write_rows

            # Its builder writes these columns as text and each is a subject column. The mapper converts one of them
            # (cycle) and keeps that original in the receipt, so the rows org-committee-links reads restore exactly.
            columns = FEC[dataset]["input_fields"]

            def stated_rows():
                for row in read_with_receipts(subjects, [scoped], selected_policy, generation_id=generation):
                    originals = row["conversion_inputs"]
                    yield {name: originals.get(name, row[name]) for name in columns}

            output.parent.mkdir(parents=True, exist_ok=True)
            write_rows(stated_rows(), output, pa.schema([(name, pa.string()) for name in columns]))
        elif dataset in FEC:
            raise ValueError(f"No processing reconstruction declared for {dataset}")
        else:
            output.parent.mkdir(parents=True, exist_ok=True)
            materialize_internal(
                ReceiptInput(dataset, tuple(subjects), scoped, generation), output,
                bulk=dataset in RESTORE_DATASETS, scoped=True,
            )
        self.restored[dataset] = output
        members = tuple(sorted(output.rglob("*.parquet"))) if output.is_dir() else (output,)
        self.processing_members[dataset] = members
        self._check_processing(dataset)
        return output

    def _check_processing(self, dataset):
        output = self.restored[dataset]
        members = tuple(sorted(output.rglob("*.parquet"))) if output.is_dir() else (output,)
        if output.is_symlink() or members != self.processing_members[dataset]:
            raise ValueError("Selected restored processing member set changed")
        for path in members:
            self.selected.member_pin(path)

    def download(self, key, target):
        source = self.get(key.removesuffix(".parquet"))
        if source is None:
            return False
        if source.is_dir():
            raise ValueError("Partitioned prior requested as one file")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return True

    def input_provenance(self, dataset):
        """Bind a restored processing input to the selected native subjects and receipts."""
        from spicy_regs.sources import publication, r2

        source = self.get(dataset)
        if source is None:
            return None
        subjects, receipts, generation = self.selections[dataset]

        def pin(path):
            return self.selected.member_pin(path)

        processing = ({"members": [{"key": path.relative_to(source).as_posix(), **pin(path)}
                                    for path in self.processing_members[dataset]]}
                      if source.is_dir() else pin(source))
        binding = {"dataset": dataset, "generationId": generation,
                   "subjects": [pin(path) for path in subjects], "receipts": self.selected.receipts_pin(dataset, receipts),
                   "processing": processing}
        if verification := self.selected.receipts_verification(dataset):
            # The parent pin below stays the index's {sha256, byteSize}, the only shape a generation parent admits;
            # this record says that sha256 was not checked against the bytes read.
            binding["receiptsVerified"] = verification
        index = self.selected.index
        key = dataset + ".parquet"
        published = publication.table_pin(index, key) if index is not None and publication.table_owner(index, key) else None
        binding["publication"] = published
        binding["publicationDisposition"] = (
            "local-selection" if index is None else "captured-publication" if published else "no-published-subject"
        )
        if published is not None:
            r2.note_pinned_read(key, published)
        else:
            for ordinal, subject in enumerate(binding["subjects"]):
                r2.note_pinned_read(f"{dataset}/subjects/{ordinal}.parquet", subject)
        r2.note_pinned_read(f"{dataset}/etl_receipts.parquet", binding["receipts"])
        return binding

    def download_members(self, key, target):
        source = self.get(key.removesuffix(".parquet"))
        if source is None:
            return ()
        members = sorted(source.rglob("*.parquet")) if source.is_dir() else [source]
        result = []
        for member in members:
            relative = member.relative_to(source) if source.is_dir() else Path(member.name)
            destination = target / source.name / relative if source.is_dir() else target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(member, destination)
            result.append(destination)
        return tuple(result)


class NativeReceiptLifecycle(RollupPipeline):
    """Prime and build from the same captured native inputs, never convenience copies."""

    output_dir: Path | None
    inputs: ClassVar[tuple[str, ...]]

    def _new_receipt_work(self, output_dir, snapshot=None):
        from spicy_regs.selected_generations import unique_build_directory

        private = unique_build_directory(output_dir)
        prior = SelectedPriors(
            private / "selected-priors",
            root=self.output_dir or output_dir,
            index=snapshot if os.getenv("R2_PUBLIC_URL") else None,
            # Another family's soft inputs (votes' bill_vote_references); never this family's own priors.
            ranged=frozenset(key.removesuffix(".parquet") for key in self.soft_inputs
                             if key not in (self.outputs or (self.output,))),
        )
        return private, prior

    def _receipt_work(self, output_dir):
        selected = getattr(self, "_primed_receipt_work", None)
        self._primed_receipt_work = None
        if selected is not None:
            directory, private, prior = selected
            if directory != output_dir:
                raise ValueError("Primed native inputs belong to a different build directory")
            return private, prior
        return self._new_receipt_work(output_dir)

    def _prime(self, output_dir, snapshot=None):
        from spicy_regs.sources import publication

        private, prior = self._new_receipt_work(output_dir, snapshot)
        parents = {}
        for key in self.inputs:
            dataset = key.removesuffix(".parquet")
            if prior.get(dataset) is None:
                raise ValueError(f"Required selected processing input unavailable: {key}")
            selection = prior.selected.select(dataset)
            index = prior.selected.index
            owner = publication.table_owner(index, key) if index is not None else None
            if self._own(key, owner[0] if owner else None):
                continue
            if owner is not None:
                parents[key] = publication.table_pin(index, key)
            else:
                for ordinal, path in enumerate(selection.subjects):
                    parents[f"{dataset}/subjects/{ordinal}.parquet"] = prior.selected.member_pin(path)
            parents[f"{dataset}/etl_receipts.parquet"] = prior.selected.member_pin(selection.receipts)
        self._primed_receipt_work = output_dir, private, prior
        return parents


class SubjectReceiptRollup(NativeReceiptLifecycle):
    """Congress, legislative and regulatory producers sharing a complete family."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        keys = cls.outputs or (cls.output,)
        cls.source_outputs = keys
        if any(not isinstance(key, str) for key in keys):
            raise ValueError("Receipt rollup requires explicit output names")
        names = tuple(str(key).removesuffix(".parquet") for key in keys)
        policies = [dataset_policy(name) for name in names]
        outcomes = family_tables(names)
        outcomes += tuple(READ_TABLES[name] for name in names if name in READ_TABLES)
        family = getattr(cls, "publication_family", None) or cls.name
        if any(name in field_registry() for name in names) and family in FILE_TABLES:
            outcomes += (FILE_TABLES[family],)
        policies.extend(NAVIGATION_OUTCOME_POLICIES[name] for name in outcomes)
        cls.added_tables = tuple(dict.fromkeys((*cls.added_tables, *(name + ".parquet" for name in outcomes))))
        if any(name in field_registry() for name in names):
            policies.append(FILE_POLICY)
        if any(name in CONGRESS for name in names):
            policies.append(ACQUISITION_POLICY)
        cls.receipt_policies = tuple(policies)
        cls.receipt_only_tables = tuple(
            p.dataset + ".parquet" for p in policies if p.receipt_only and p.dataset in names
        )
        visible = tuple(key for key in keys if key not in cls.receipt_only_tables) + tuple(name + ".parquet" for name in outcomes)
        if cls.outputs or outcomes:
            cls.outputs = visible
        elif visible:
            cls.output = visible[0]

    def generation_schemas(self):
        return {p.dataset: described_schema(p.subject_schema) for p in self.receipt_policies if not p.receipt_only}

    def build_receipts(self, output_dir, builder, **kwargs):
        from spicy_regs.selected_generations import remember_selection, SelectedDataset

        private, prior = self._receipt_work(output_dir)
        work = private / "source-output"
        work.mkdir()
        candidate = private / "candidate"
        candidate.mkdir()
        for key in self.inputs:
            if not prior.download(key, work / key):
                raise ValueError(f"Required selected processing input unavailable: {key}")
        parameters = inspect.signature(builder).parameters
        if "download_prior" in parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
            kwargs["download_prior"] = prior.download
        if "download_members" in parameters:
            kwargs["download_members"] = prior.download_members
        if "selected_input" in parameters:
            kwargs["selected_input"] = prior.input_provenance
        try:
            built = builder(work, **kwargs)
        except Exception as error:
            if any(p.dataset == ACQUISITION_POLICY.dataset for p in self.receipt_policies):
                _build_events(candidate, self.receipt_generation_id, builder.__name__, self.source_evidence, error)
            raise
        paths = built if isinstance(built, tuple) else (built,)
        observed = {path.name if path.is_dir() else path.stem for path in paths}
        if observed != {key.removesuffix(".parquet") for key in self.source_outputs}:
            raise ValueError("Producer outputs differ from the complete classified family")
        receipts, subjects, legislative = [], [], []
        fresh_failure_datasets = []
        for path in paths:
            dataset = path.name if path.is_dir() else path.stem
            if dataset in field_registry():
                legislative.append(path)
                continue
            destination = private / "datasets" / dataset
            if dataset in CONGRESS:
                selection = prior.selections.get(dataset)
                held = None if selection is None else CongressInput(tuple(selection[0]), selection[1], selection[2])
                subject, receipt = write_congress_dataset(
                    path, destination, dataset=dataset, generation_id=self.receipt_generation_id, prior=held,
                    bulk=True, on_fresh_failure=lambda: fresh_failure_datasets.append(dataset),
                    validate=False,  # the prior was admitted when selected; the new pair is admitted with its generation
                )
            else:
                etl_bulk.log_row_eligibility(
                    "regulatory write", dataset=dataset, rows=pq.ParquetFile(path).metadata.num_rows,
                    reason="bulk not requested by subject receipt rollup",
                )
                subject, receipt = write_held_dataset(
                    dataset, path, destination, generation_id=self.receipt_generation_id
                )
            receipts.append(receipt)
            if subject is not None:
                target = candidate / subject.name
                shutil.copyfile(subject, target)
                subjects.append(target)
        if legislative:
            bundle = private / "legislative-bundle"
            manifest = write_legislative_outputs(
                legislative,
                bundle,
                generation_id=self.receipt_generation_id,
                prior_receipts=list(dict.fromkeys(v[1] for v in prior.selections.values())),
                file_outcomes_table=FILE_TABLES.get(getattr(self, "publication_family", None) or self.name),
                validate=False,  # the completed generation admits these exact bytes once
            )
            if manifest["unowned_outputs"] or any(manifest["fresh_refused_rows"].values()):
                raise ValueError("Legislative output conversion refused")
            receipts.append(bundle / "etl_receipts.parquet")
            for name, members in manifest["subjects"].items():
                if legislative_policy(name).receipt_only:
                    continue
                if (bundle / name).is_dir():
                    target = candidate / name
                    shutil.copytree(bundle / name, target)
                else:
                    target = candidate / (name + ".parquet")
                    shutil.copyfile(bundle / members[0], target)
                subjects.append(target)
            for name, members in manifest.get("outcome_subjects", {}).items():
                target = candidate / (name + ".parquet")
                shutil.copyfile(bundle / members[0], target)
                subjects.append(target)
        if any(p.dataset == ACQUISITION_POLICY.dataset for p in self.receipt_policies):
            receipts.append(
                _build_events(candidate, self.receipt_generation_id, builder.__name__, self.source_evidence)
            )
        outcome_names = family_tables(tuple(key.removesuffix(".parquet") for key in self.source_outputs))
        if outcome_names:
            journal = None if self.source_evidence is None else self.source_evidence.artifact_dir / "journal.jsonl"
            outcome_subjects, outcome_receipts = write_recorded_outcomes(
                journal, private / "read-outcomes", generation_id=self.receipt_generation_id, tables=outcome_names,
                priors={name: prior.selected.select(name) for name in outcome_names})
            receipts.extend(outcome_receipts)
            for source in outcome_subjects:
                target = candidate / source.name
                shutil.copyfile(source, target)
                subjects.append(target)
        combined = combine_receipts(receipts, candidate / "etl_receipts.parquet")
        if fresh_failure_datasets or has_unmatched_failures(combined, {name: [selection[1]] for name, selection in prior.selections.items()}):
            raise ValueError("Subject conversion refused; retained receipts require review")
        by_dataset = {p.dataset: [] for p in self.receipt_policies}
        for path in subjects:
            by_dataset[path.name if path.is_dir() else path.stem] = (
                sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
            )
        # ``_defer_native_selection`` is set only by ``_run_tables``, whose ``build_generation`` admits this same pair
        # (every policy, every subject member) before anything is visible or selected. That admission is the one
        # validation; outside a run, ``remember_selection`` below pins "admitted members", so validate here.
        if not getattr(self, "_defer_native_selection", False):
            validate_receipt_bundle(by_dataset, [combined], self.receipt_policies, generation_id=self.receipt_generation_id)
        # Private files are final here; the copies below make them visible.
        visible = []
        for source in subjects:
            target = output_dir / source.name
            if source.is_dir():
                if target.exists():
                    previous = private / "previous" / source.name
                    previous.parent.mkdir(parents=True, exist_ok=True)
                    target.rename(previous)
                shutil.copytree(source, target)
            else:
                shutil.copyfile(source, target)
            visible.append(target)
        shutil.copyfile(combined, output_dir / "etl_receipts.parquet")
        if not getattr(self, "_defer_native_selection", False):
            remember_selection(
                self.output_dir or output_dir,
                [
                    SelectedDataset(
                        policy.dataset, tuple(by_dataset[policy.dataset]), combined, self.receipt_generation_id
                    )
                    for policy in self.receipt_policies
                ],
            )
        return tuple(visible) if self.outputs else visible[0]
