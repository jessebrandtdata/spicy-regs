"""Build native Congress subjects with generation-bound receipts and exact resume reads.

The source builders retain their acquisition, safety, merge and interpretation
rules. This entry point supplies explicit prior selections, reconstructs private
working inputs from matching receipts, and splits every owned output before
admission. Every incremental input must contain selected native subjects and
matching receipts. There is no legacy import path.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.congress_subjects import IDENTITIES, INPUT_COLUMNS, RECEIPT_ONLY, map_record, subject_schema
from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA, open_receipt_writer,
    DatasetPolicy,
    ReceiptContext,
    combine_receipts,
    failure_receipt,
    observation_receipt,
    select_receipts,
    split_record,
    validate_receipt_bundle,
)
from spicy_regs.receipt_history import RefusalIdentity, carry_receipt_history
from spicy_regs.transforms.parquet_rows import write_rows

PROCESSOR = "congress-subjects/1"
RECEIPT_FIELDS = ("source_fields", "source_metadata", "source_schema", "entry_kind")
ACQUISITION_POLICY = DatasetPolicy(
    "congress_acquisition",
    pa.schema([]),
    (),
    ("capture_event", "build_event"),
    policy_version=PROCESSOR,
    receipt_only=True,
)


def policy(dataset: str) -> DatasetPolicy:
    return DatasetPolicy(
        dataset,
        subject_schema(dataset),
        IDENTITIES.get(dataset, ()),
        RECEIPT_FIELDS,
        policy_version=(
            "congress-subjects/3"
            if dataset == "nominations"
            else "congress-subjects/2"
            if dataset
            in {"committee_assignments", "committee_meetings", "house_communications", "member_vote_terms", "roll_call_votes"}
            else "congress-subjects/1"
        ),
        receipt_only=dataset in RECEIPT_ONLY,
    )


def refusal_identities(dataset: str) -> dict[str, RefusalIdentity]:
    """Resolve a refused raw row only when every declared key is retained literally.

    Derived native keys have no raw-field declaration here and remain unresolved.
    Missing raw keys never substitute another column or an ordinal.
    """
    keys = policy(dataset).identity_fields
    return {dataset: RefusalIdentity(keys, "source_fields")} if keys and set(keys) <= set(INPUT_COLUMNS[dataset]) else {}


def has_unmatched_failures(receipts: Path, priors: Mapping[str, Sequence[Path]] | None = None) -> bool:
    """A failed attempt is historical only when the selected prior retains its exact receipt id."""
    from spicy_regs import etl_bulk

    by_path: dict[Path, list[str]] = {}
    for dataset, paths in (priors or {}).items():
        for path in paths:
            by_path.setdefault(path, []).append(dataset)
    with etl_bulk.bulk_connection() as (con, _):
        con.execute("CREATE TABLE historical_failures(receipt_id VARCHAR)")
        for path, datasets in by_path.items():
            con.execute("INSERT INTO historical_failures SELECT receipt_id FROM read_parquet(?) "
                        "WHERE dataset=ANY(?) AND outcome IN ('refused','error')", [str(path), datasets])
        return bool(con.execute("SELECT EXISTS(SELECT 1 FROM read_parquet(?) r WHERE outcome IN ('refused','error') "
                                "AND NOT EXISTS(SELECT 1 FROM historical_failures p WHERE p.receipt_id=r.receipt_id))",
                                [str(receipts)]).fetchone()[0])


def input_policy(dataset: str, subjects: Path | tuple[Path, ...] | None) -> DatasetPolicy:
    """Admit only a current or explicitly retained historical subject schema."""
    from spicy_regs.etl_receipts import receipt_policies

    selected = policy(dataset)
    paths = () if subjects is None else (subjects,) if isinstance(subjects, Path) else subjects
    for candidate in receipt_policies(selected):
        if not paths or all(pq.read_schema(path).equals(candidate.subject_schema, check_metadata=False) for path in paths):
            return candidate
    history = json.loads(Path(__file__).with_name("navigation_policy_history.json").read_text())
    for key in (f"{dataset}_v2", dataset):
        if key in history:
            earlier = DatasetPolicy.from_descriptor(history[key])
            if all(pq.read_schema(path).equals(earlier.subject_schema, check_metadata=False) for path in paths):
                return earlier
    raise ValueError(f"{dataset}: subject schema has no supported receipt policy")


def _rows(path: Path):
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def _digest(path: Path) -> str:
    with path.open("rb") as source:
        return "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()


def _build_events(directory: Path, generation_id: str, builder: str, evidence, error=None):
    """Copy the already-scrubbed source journal into shared receipt observations.

    This is a receipt dataset label, never another subject table. Refusals,
    captured inputs, parser versions and incomplete responses keep their exact
    journal objects. A top-level failure still raises after this local record.
    """
    event = {
        "builder": builder,
        "outcome": "error" if error else "built",
        "error_type": type(error).__name__ if error else None,
    }
    invocation = directory / "build-event.json"
    invocation.write_text(json.dumps(event, sort_keys=True) + "\n")
    journal = directory / "source-journal.jsonl"
    if evidence is not None:
        shutil.copyfile(evidence.artifact_dir / "journal.jsonl", journal)

    def records():
        files = [(invocation, "build_event")]
        if journal.exists():
            files.append((journal, "capture_event"))
        for path, field_name in files:
            digest = _digest(path)
            with path.open() as stream:
                for line, raw in enumerate(stream):
                    context = ReceiptContext(
                        generation_id,
                        f"{field_name}:{line}",
                        PROCESSOR,
                        [
                            {
                                "source_id": field_name,
                                "source_uri": None,
                                "sha256": digest,
                                "locator": f"line:{line + 1}",
                                "body_version": None,
                            }
                        ],
                    )
                    fields = {field_name: json.loads(raw)}
                    if field_name == "build_event" and error is not None:
                        yield failure_receipt(ACQUISITION_POLICY, context, outcome="error", raw_fields=fields)
                    else:
                        yield observation_receipt(ACQUISITION_POLICY, context, processing_fields=fields)

    return write_rows(records(), directory / "acquisition-receipts.parquet", RECEIPT_SCHEMA)


def write_congress_dataset(
    source: Path,
    directory: Path,
    *,
    dataset: str,
    generation_id: str,
    witnesses: Sequence[Mapping[str, Any]] = (),
    prior: CongressInput | None = None,
    bulk: bool = False,
    validate: bool = True,
    on_fresh_failure: Callable[[], None] | None = None,
) -> tuple[Path | None, Path]:
    """Retain one complete shaped output, including rejected rows and its exact footer.

    The source file remains in place. Its digest and row ordinal identify the
    conversion input; callers can additionally retain ordered native capture
    witnesses. The first witness never asserts that a shaped file is an original
    publisher response.

    ``on_fresh_failure`` reports a fresh refused/error attempt before carry can
    reuse its older receipt. The caller can retain the completed private bundle
    while refusing its publication.
    """
    if bulk:
        if dataset not in INPUT_COLUMNS:
            raise ValueError("Batch Congress writing requires a declared Congress dataset")
        from spicy_regs.votes_batch_receipts import write_congress_dataset as write_votes
        return write_votes(source, directory, dataset=dataset, generation_id=generation_id,
                           witnesses=witnesses, prior=prior, bulk=True, validate=validate, on_fresh_failure=on_fresh_failure)

    from rulespec_artifacts import publish_directory_no_replace

    if directory.exists():
        raise FileExistsError(directory)
    selected = policy(dataset)
    digest = _digest(source)
    directory.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".congress-", dir=directory.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        subjects = None if selected.receipt_only else stage / (dataset + ".parquet")
        receipts = stage / "etl_receipts.parquet"
        base_witness = {
            "source_id": "shaped-observation:" + dataset,
            "source_uri": None,
            "sha256": digest,
            "body_version": None,
        }
        footer = dict(pq.read_schema(source).metadata or {})
        prior_paths = []
        if prior is not None:
            scoped = select_receipts(prior.receipts, Path(temporary) / "prior.parquet", dataset=dataset)
            if validate:
                validate_receipt_bundle({dataset: prior.subjects}, [scoped], [input_policy(dataset, prior.subjects)], generation_id=prior.generation_id)
            prior_paths.append(scoped)

        def context(ordinal, diagnostics=None, subject=None, processing=None):
            return ReceiptContext(
                generation_id,
                f"{dataset}:{digest}:{ordinal}",
                PROCESSOR,
                [{**base_witness, "locator": str(ordinal)}, *witnesses],
                diagnostics or {},
            )

        with open_receipt_writer(receipts) as rw:
            sw = None if subjects is None else pq.ParquetWriter(subjects, selected.subject_schema, compression="zstd")
            try:
                receipt_batch, subject_batch = [], []
                metadata = {
                    "source_metadata": list(footer.items()),
                    "source_schema": pq.read_schema(source).serialize().to_pybytes(),
                    "entry_kind": "table_metadata",
                }
                receipt = observation_receipt(
                    selected, context("metadata", processing=metadata), processing_fields=metadata
                )
                receipt_batch.append(receipt)
                for ordinal, raw in enumerate(_rows(source)):
                    try:
                        mapped = map_record(dataset, raw)
                        processing = {"source_fields": mapped.source_fields, "entry_kind": "row"}
                        if mapped.subject is None and not selected.receipt_only:
                            receipt = failure_receipt(
                                selected,
                                context(ordinal, {"reason": "no_domain_subject"}),
                                outcome="rejected",
                                raw_fields=processing,
                            )
                        else:
                            subject, receipt = split_record(
                                selected,
                                (mapped.subject or {}) | processing,
                                context(ordinal, subject=mapped.subject, processing=processing),
                            )
                            if subject is not None:
                                subject_batch.append(subject)
                    except (ValueError, TypeError, OverflowError, pa.ArrowException) as error:
                        # Missing identity and bad native values both remain attempts.
                        # Arbitrary exception text can contain source content or credentials.
                        receipt = failure_receipt(
                            selected,
                            context(ordinal, {"reason": "conversion_refused", "error_type": type(error).__name__}),
                            outcome="refused",
                            raw_fields={"source_fields": raw, "entry_kind": "row"},
                        )
                    receipt_batch.append(receipt)
                    if len(receipt_batch) >= 2000:
                        if subject_batch:
                            assert sw is not None
                            sw.write_table(pa.Table.from_pylist(subject_batch, schema=selected.subject_schema))
                            subject_batch.clear()
                        rw.write_table(pa.Table.from_pylist(receipt_batch, schema=RECEIPT_SCHEMA))
                        receipt_batch.clear()
                if subject_batch:
                    assert sw is not None
                    sw.write_table(pa.Table.from_pylist(subject_batch, schema=selected.subject_schema))
                if receipt_batch:
                    rw.write_table(pa.Table.from_pylist(receipt_batch, schema=RECEIPT_SCHEMA))
            finally:
                if sw is not None:
                    sw.close()
        if on_fresh_failure is not None and has_unmatched_failures(receipts):
            on_fresh_failure()
        if prior_paths:
            # The same carry rule as the batch writer: unchanged rows keep their prior receipt, changed ones name it.
            carry_receipt_history(receipts, prior_paths, stage / "carried-receipts.parquet",
                                  refusal_identities=refusal_identities(dataset)).replace(receipts)
        if validate:
            validate_receipt_bundle(
                {dataset: [] if subjects is None else [subjects]}, [receipts], [selected], generation_id=generation_id
            )
        publish_directory_no_replace(stage, directory)
    return (None if subjects is None else directory / subjects.name, directory / receipts.name)


def retained_source_rows(attempts, *, schema=None):
    """Decode source rows and require the same retained footer on complete consumption."""
    retained = None
    for receipt in attempts:
        data, diagnostic = receipt["processing_fields"], receipt["diagnostics"]
        if data.get("entry_kind") == "table_metadata":
            if retained is not None and retained != data:
                raise ValueError("Congress input requires consistent retained source footers")
            retained = data
            if schema is not None and not schema.equals(
                    pa.ipc.read_schema(pa.BufferReader(data["source_schema"])), check_metadata=True):
                raise ValueError("Congress retained source schema or metadata changed")
        elif data.get("entry_kind") == "row" and diagnostic.get("reason") != "conversion_refused":
            yield data["source_fields"]
    if retained is None:
        raise ValueError("Congress input requires consistent retained source footers")


def restore_processing_input(
    subject: Path | tuple[Path, ...] | None, receipts: Path, destination: Path, *, dataset: str, generation_id: str,
    bulk: bool = False, validate: bool = True, scoped: bool = False,
) -> Path:
    """Restore exact retained input after one complete subject/receipt admission.

    The decoding route depends on the retained source schema, independently of
    which Congress datasets the SQL mapper can write. Callers that just admitted
    this exact pair may pass ``validate=False``; ``scoped`` avoids filtering an
    already selected receipt file again.
    """
    from spicy_regs.votes_batch_receipts import restore_processing_input as restore_selected

    return restore_selected(subject, receipts, destination, dataset=dataset,
                            generation_id=generation_id, bulk=bulk, validate=validate, scoped=scoped)


@dataclass(frozen=True)
class CongressInput:
    """One admitted native table, including every selected partition member."""

    source: Path | tuple[Path, ...] | None
    receipts: Path
    generation_id: str

    @property
    def subjects(self) -> tuple[Path, ...]:
        return () if self.source is None else ((self.source,) if isinstance(self.source, Path) else tuple(self.source))

    def materialize(self, dataset: str, destination: Path) -> Path:
        if not self.receipts or not self.generation_id:
            raise ValueError("Native Congress input requires selected generation receipts")
        return restore_processing_input(
            self.subjects, self.receipts, destination, dataset=dataset, generation_id=self.generation_id
        )


@dataclass
class CongressBuild:
    """A local build selection passed to an existing Congress producer.

    Empty inputs explicitly mean cold start. No unselected remote prior is read.
    Cross-family inputs must be supplied by their owner in an explicitly admitted
    private working shape until the integration reader registry is available.
    """

    generation_id: str
    inputs: Mapping[str, CongressInput] = field(default_factory=dict)
    witnesses: Sequence[Mapping[str, Any]] = ()
    receipt_path: Path | None = field(default=None, init=False)
    policies: tuple[DatasetPolicy, ...] = field(default=(), init=False)
    unclassified_outputs: tuple[Path, ...] = field(default=(), init=False)
    source_evidence: Any = field(default=None, init=False)
    completed: bool = field(default=False, init=False)
    fresh_refusals: bool = field(default=False, init=False)

    def run(self, builder: Callable, output_dir: Path, **kwargs):
        import inspect

        self.completed = False
        self.fresh_refusals = False
        if (
            not self.generation_id
            or self.generation_id in {".", ".."}
            or Path(self.generation_id).name != self.generation_id
        ):
            raise ValueError("A local build needs a plain generation identity")
        directory = output_dir / self.generation_id
        directory.mkdir(parents=True, exist_ok=False)
        work = directory / "processing"
        work.mkdir()
        for name, selected in self.inputs.items():
            selected.materialize(name, work / (name + ".parquet"))

        def download(key: str, destination: Path) -> bool:
            name = Path(key).stem
            selected = self.inputs.get(name)
            if selected is None:
                return False
            selected.materialize(name, destination)
            return True

        parameters = inspect.signature(builder).parameters
        options = dict(kwargs)
        if "download_prior" in parameters:
            options["download_prior"] = download
        if "download_members" in parameters:

            def download_members(key, destination):
                name = Path(key).stem
                selected = self.inputs.get(name)
                if selected is None:
                    return ()
                destination.mkdir(parents=True, exist_ok=True)
                return (selected.materialize(name, destination / key),)

            options["download_members"] = download_members
        self.source_evidence = options.get("evidence")
        try:
            built = builder(work, **options)
        except Exception as error:
            events = _build_events(
                directory,
                self.generation_id,
                getattr(builder, "__name__", type(builder).__name__),
                self.source_evidence,
                error,
            )
            self.receipt_path = combine_receipts([events], directory / "etl_receipts.parquet")
            self.policies = (ACQUISITION_POLICY,)
            raise
        paths = built if isinstance(built, tuple) else (built,)
        subjects, receipts, policies, untouched = [], [], [], []
        for source in paths:
            dataset = source.stem
            if dataset not in INPUT_COLUMNS:
                untouched.append(source)
                continue
            subject, receipt = write_congress_dataset(
                source,
                directory / "datasets" / dataset,
                dataset=dataset,
                generation_id=self.generation_id,
                witnesses=self.witnesses,
                prior=self.inputs.get(dataset),
                on_fresh_failure=lambda: setattr(self, "fresh_refusals", True),
            )
            if subject is not None:
                subjects.append(subject)
            receipts.append(receipt)
            policies.append(policy(dataset))
        if not receipts:
            raise ValueError("Builder emitted no owned Congress datasets")
        receipts.append(
            _build_events(
                directory,
                self.generation_id,
                getattr(builder, "__name__", type(builder).__name__),
                self.source_evidence,
            )
        )
        policies.append(ACQUISITION_POLICY)
        self.receipt_path = combine_receipts(receipts, directory / "etl_receipts.parquet")
        self.policies = tuple(policies)
        self.unclassified_outputs = tuple(untouched)
        validate_receipt_bundle(
            {p.dataset: [s for s in subjects if s.stem == p.dataset] for p in policies},
            [self.receipt_path],
            policies,
            generation_id=self.generation_id,
        )
        # Other family outputs remain exact; complete-family admission below
        # refuses them until their owner supplies policies and receipts.
        result = tuple(subjects) + tuple(untouched)
        self.completed = True
        return result if isinstance(built, tuple) else result[0]

    def admit(self, directory: Path, *, family: str, subjects: Sequence[Path]):
        from spicy_regs.generations import build_generation

        if self.unclassified_outputs:
            raise ValueError("Complete family awaits policies and receipts for sibling outputs")
        if not self.completed or self.receipt_path is None:
            raise ValueError("No completed Congress build to admit")
        if self.fresh_refusals or has_unmatched_failures(self.receipt_path, {name: [held.receipts] for name, held in self.inputs.items()}):
            raise ValueError("Conversion refusals require review before complete generation admission")
        return build_generation(
            directory,
            family=family,
            files=subjects,
            expected_keys=[path.name for path in subjects],
            receipt_path=self.receipt_path,
            receipt_policies=self.policies,
            receipt_generation_id=self.generation_id,
            inputs=self.source_evidence.inputs() if self.source_evidence is not None else (),
        )
