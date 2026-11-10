"""Scorecard family adoption of the shared, generation-bound ETL receipt API."""

from __future__ import annotations

from collections.abc import Mapping

import hashlib
from dataclasses import replace
import json
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow.parquet as pq

from spicy_regs.policy_migrations import earlier_policies
from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_KEY,
    combine_receipts,
    failure_receipt,
    read_receipt_bundle,
    visit_receipt_bundle,
    carry_receipt_history,
    read_with_receipts,
    select_receipts,
    validate_receipt_bundle,
    write_dataset,
)
from spicy_regs.scorecards.subject_shapes import (
    BOOLEAN_FIELDS,
    DECIMAL_FIELDS,
    DOMAIN_COLUMNS,
    IDENTITIES,
    INTEGER_FIELDS,
    POLICY_VERSION,
    RATING_POLICY_VERSION,
    MEMBER_LINK_POLICY_VERSION,
    SOURCE_COLUMNS,
    map_source_row,
    restore_source_row,
    source_row_index,
    subject_schema,
)

LINK_NAMES = ("scorecard_member_links", "scorecard_item_links")
ANALYSIS_NAMES = (*LINK_NAMES, "scorecard_action_comparisons")
SOURCE_NAMES = tuple(name for name in SOURCE_COLUMNS if name not in ANALYSIS_NAMES)


def policy(name):
    schema = subject_schema(name)
    fields = tuple(c for c in SOURCE_COLUMNS[name] if c not in schema.names)
    if set(DOMAIN_COLUMNS[name]) & (BOOLEAN_FIELDS | DECIMAL_FIELDS | INTEGER_FIELDS):
        fields += ("conversion_inputs",)
    return DatasetPolicy(
        name,
        schema,
        IDENTITIES[name] if schema.names else (),
        fields + ("raw_source",),
        policy_version={
            "scorecard_member_ratings": RATING_POLICY_VERSION,
            "scorecard_member_links": MEMBER_LINK_POLICY_VERSION,
        }.get(name, POLICY_VERSION),
        receipt_only=not schema.names,
    )


POLICIES = {name: policy(name) for name in SOURCE_COLUMNS}
# Source replay and shared admission use one exact historical declaration source.
EARLIER_POLICIES = {name: earlier_policies(policy) for name, policy in POLICIES.items()}
LEGACY_RATING_POLICY_V2, LEGACY_RATING_POLICY = (p for p in EARLIER_POLICIES["scorecard_member_ratings"]
                                                if p.policy_version in {"scorecards-etl-ratings-v2", "scorecards-etl-v1"})


def admitted_read_policies(names, *, descriptors=None, columns=None):
    """Select only known exact policies from a verified artifact or pinned table declarations."""
    from spicy_regs.contract_types import described_schema

    if (descriptors is None) == (columns is None):
        raise ValueError("Scorecard reads require exactly one admitted policy declaration")
    from itertools import product
    choices = {name: (POLICIES[name], *EARLIER_POLICIES[name]) for name in names}
    if descriptors is not None:
        if not isinstance(descriptors, list) or len(descriptors) != len(names):
            raise ValueError("Scorecard policy declaration count differs")
        declared = {value.get("dataset"): value for value in descriptors if isinstance(value, dict)}
        if len(declared) != len(names):
            raise ValueError("Scorecard declarations differ from supported exact policies: repeated dataset")
        selected = {}
        for name, candidates in choices.items():
            matches = [p for p in candidates if p.descriptor() == declared.get(name)]
            if len(matches) != 1:
                raise ValueError("Scorecard policy is not a supported exact declaration")
            selected[name] = matches[0]
        return selected
    assert columns is not None
    # Schema-only historical inputs must identify one exact known family layout.
    # Avoid a Cartesian search when each table's columns already decide its policy.
    matches = {}
    for name, candidates in choices.items():
        matches[name] = [p for p in candidates if (p.receipt_only and name not in columns) or
                         (not p.receipt_only and name in columns and
                          [tuple(c) for c in columns[name]] == described_schema(p.subject_schema))]
        if not matches[name]:
            raise ValueError("Scorecard columns are not a supported exact layout")
    for variant in product(*(matches[name] for name in names)):
        selected = dict(zip(names, variant))
        if set(columns) == {name for name, p in selected.items() if not p.receipt_only}:
            return selected
    raise ValueError("Scorecard input differs from the supported exact historical or current policies")


def _witnesses(row):
    captures = json.loads(row["capture_ids_json"]) if row.get("capture_ids_json") else [row.get("capture_id")]
    witnesses = []
    for capture in captures:
        if not capture:
            raise ValueError("Scorecard row lacks its retained capture identity")
        # Capture IDs are immutable observation identities, not byte hashes.
        # This preserves the metadata-only policy without inventing content evidence.
        witnesses.append(
            dict(
                source_id=capture,
                source_uri=row.get("source_url") if capture == row.get("capture_id") else None,
                sha256=None,
                locator=row.get("source_path") if capture == row.get("capture_id") else None,
                body_version="capture:" + capture,
            )
        )
    if row.get("input_pins_json"):
        for name, pin in json.loads(row["input_pins_json"]).items():
            witnesses.append(
                dict(
                    source_id=name,
                    source_uri=None,
                    sha256=pin.get("sha256"),
                    locator=None,
                    body_version=pin["artifactDigest"],
                )
            )
    return witnesses


def generation_options(directory: Path, names) -> dict:
    """Explicit options for generation admission; receipts are not subject outputs."""
    state = json.loads((directory / "scorecard-etl-build.json").read_text())
    if set(state["datasets"]) != set(names) or len(state["datasets"]) != len(names):
        raise ValueError("Scorecard receipt policy set differs from the selected build")
    from spicy_regs.contract_types import described_schema

    return dict(
        schemas={
            name: described_schema(POLICIES[name].subject_schema) for name in names if not POLICIES[name].receipt_only
        },
        receipt_path=directory / RECEIPT_KEY,
        receipt_policies=[POLICIES[n] for n in names],
        receipt_generation_id=state["generation_id"],
    )


def write_family(
    directory: Path,
    tables: dict,
    *,
    generation_id: str | None = None,
    attempt_failures=(),
    prior_receipts: Path | None = None,
) -> tuple[Path, ...]:
    """Write subjects and every source/resolution attempt, then verify persisted joins."""
    names = tuple(tables)
    generation_id = generation_id or "scorecard-build:" + uuid4().hex
    stage = directory / (".scorecard-etl-" + uuid4().hex)
    stage.mkdir(parents=True)
    receipts, outputs, errors = [], [], []

    def mapped_records(name, rows, failures):
        for index, raw in enumerate(rows):
            context = ReceiptContext(
                generation_id,
                f"{name}:{index}",
                raw.get("rule_version") or raw.get("parser_version") or POLICIES[name].policy_version,
                _witnesses(raw),
            )
            try:
                mapped = map_source_row(name, raw)
            except (ValueError, TypeError, OverflowError) as error:
                failures.append(
                    failure_receipt(
                        POLICIES[name],
                        replace(
                            context,
                            diagnostics={
                                "error_type": type(error).__name__,
                                "reason_code": "native_conversion_refused",
                            },
                        ),
                        outcome="error",
                        raw_fields={"raw_source": raw},
                    )
                )
                errors.append(f"{name}: {type(error).__name__}")
                continue
            # Unresolved is a valid recorded domain decision, not a failed ETL
            # conversion. Publish the complete association with no invented edge.
            yield mapped, context

    for name, rows in tables.items():
        failures: list[dict] = list(attempt_failures) if name == "scorecard_snapshots" else []
        subject, receipt = write_dataset(
            mapped_records(name, rows, failures), stage / name, POLICIES[name], failures=failures,
            validate=False,  # The complete private family is validated before final outputs are replaced.
        )
        receipts.append(receipt)
        if subject:
            outputs.append(subject)
    shared = combine_receipts(receipts, stage / RECEIPT_KEY)
    if prior_receipts is not None and not errors:
        # The caller admitted the complete prior family. Keep unchanged receipts
        # exactly; changed accepted rows name only their direct predecessor.
        shared = carry_receipt_history(shared, [prior_receipts], shared)
    validate_receipt_bundle(
        {n: [] if POLICIES[n].receipt_only else [stage / n / (n + ".parquet")] for n in names},
        [shared],
        [POLICIES[n] for n in names],
        generation_id=generation_id,
    )
    if errors:
        raise ValueError(f"Scorecard conversion refused; retained attempt receipts in {stage}: {errors}")
    paths = []
    for subject in outputs:
        target = directory / subject.name
        subject.replace(target)
        paths.append(target)
    shared.replace(directory / RECEIPT_KEY)
    (directory / "scorecard-etl-build.json").write_text(
        json.dumps({"generation_id": generation_id, "datasets": list(names)}, indent=2) + "\n"
    )
    rmtree(stage)
    return tuple(paths)


def _family_read_arguments(
    directory: Path,
    names,
    *,
    receipt_path: Path | None = None,
    generation_id: str | None = None,
    policies: Mapping[str, DatasetPolicy] | None = None,
) -> dict:
    receipt_path = receipt_path or directory / RECEIPT_KEY
    if generation_id is None:
        generation_id = generation_options(directory, names)["receipt_generation_id"]
    if policies is not None and (set(policies) != set(names) or any(p.dataset != name for name, p in policies.items())):
        raise ValueError("Scorecard read policy keys differ from their selected datasets")
    selected = (
        {n: POLICIES[n] for n in names}
        if policies is None
        else admitted_read_policies(names, descriptors=[p.descriptor() for p in policies.values()])
    )
    subjects = {n: [] if selected[n].receipt_only else [directory / (n + ".parquet")] for n in names}
    return dict(
        subjects=subjects,
        receipt_paths=[receipt_path],
        policies=[selected[n] for n in names],
        generation_id=generation_id,
        processing_outcomes={
            name: frozenset({"observed", "refused"} if name in LINK_NAMES else {"observed"}) for name in names
        },
    )


def read_family(
    directory: Path,
    names,
    *,
    receipt_path: Path | None = None,
    generation_id: str | None = None,
    policies: Mapping[str, DatasetPolicy] | None = None,
) -> dict:
    """Reconstruct provider rows only after validating the entire selected native family."""
    joined = read_receipt_bundle(**_family_read_arguments(
        directory, names, receipt_path=receipt_path, generation_id=generation_id, policies=policies,
    ))
    for name, rows in joined.items():
        for index, row in enumerate(rows):
            rows[index] = restore_source_row(name, row)
    return joined


def verify_family_readback(
    directory: Path,
    expected: Mapping,
    *,
    receipt_path: Path | None = None,
    generation_id: str | None = None,
    policies: Mapping[str, DatasetPolicy] | None = None,
) -> dict[str, int]:
    """Prove exact persisted source rows without constructing a second raw family.

    Only identity indexes referencing existing rows are retained. Each dataset's
    index is built when first visited and emptied as its persisted rows arrive;
    reordered native rows remain valid. Counts escape only after every receipt,
    join and literal source value has passed.
    """
    remaining = {}

    def visit(name, native):
        if name not in remaining:
            remaining[name] = source_row_index(name, expected[name])
        row = restore_source_row(name, native)
        key = tuple(row[field] for field in IDENTITIES[name])
        before = remaining[name].pop(key, None)
        if before is None or json.dumps(before, sort_keys=True) != json.dumps(row, sort_keys=True):
            raise ValueError("Persisted scorecard source row differs: " + name)
        if not remaining[name]:
            remaining[name].clear()  # Free the exhausted index's bucket storage; keep its visited marker.

    counts = visit_receipt_bundle(
        **_family_read_arguments(
            directory, tuple(expected), receipt_path=receipt_path, generation_id=generation_id, policies=policies,
        ),
        visit=visit,
    )
    if counts != {name: len(rows) for name, rows in expected.items()} or any(remaining.values()):
        raise ValueError("Persisted scorecard source family is incomplete")
    return counts


def read_indexed_family(directory: Path, names, family: Mapping, *, receipt_path: Path | None = None) -> dict:
    """Read caller-pinned downloads under the index's exact supported schema and receipt generation."""
    return read_family(
        directory,
        names,
        receipt_path=receipt_path,
        generation_id=family["etlReceipts"]["generationId"],
        policies=admitted_read_policies(
            names,
            columns={key.removesuffix(".parquet"): pin["columns"] for key, pin in family["tables"].items()},
        ),
    )


def read_source_inputs(paths, receipt_path: Path, *, generation_id: str) -> dict:
    """Receipt-checked resolver inputs; selected rows share one source generation."""
    result = {}
    with TemporaryDirectory(prefix="scorecard-analysis-inputs-") as temporary:
        for name, subject_paths in paths.items():
            selected = select_receipts(receipt_path, Path(temporary) / (name + ".parquet"), dataset=name)
            result[name] = [
                restore_source_row(name, row)
                for row in read_with_receipts(subject_paths, [selected], POLICIES[name], generation_id=generation_id)
            ]
    return result


def verified_receipt_download(index, target: Path, *, public_url: str | None, dataset="scorecards") -> Path:
    """Fetch the same selected family's receipt member and verify local bytes as well."""
    from spicy_regs.sources import publication

    members = publication.receipt_members(index, dataset=dataset)
    if len(members) != 1:
        raise ValueError("Scorecard input requires one selected receipt member")
    member = members[0]
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.is_file():
        if not public_url or not publication.fetch_member(public_url, member, target, RECEIPT_KEY):
            raise ValueError("Pinned scorecard receipt is unavailable")
    with target.open("rb") as stream:
        digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    if target.stat().st_size != member.byte_size or digest != member.sha256:
        raise ValueError("Pinned scorecard receipt changed")
    return target


def read_source_generation(directory: Path) -> dict:
    """Qualification read of an admitted source generation, with explicit legacy migration."""
    from spicy_regs.generations import verify_generation
    from spicy_regs.transforms.build_scorecards import installed_provider

    artifact = verify_generation(directory)
    spec = artifact.root["spec"]
    if spec["family"] != "scorecards":
        raise ValueError("Expected a scorecard source generation")
    if "etlReceipts" in spec:
        selected = admitted_read_policies(SOURCE_NAMES, descriptors=spec["etlReceipts"]["policies"])
        rows = read_family(
            directory, SOURCE_NAMES, generation_id=spec["etlReceipts"]["generationId"], policies=selected
        )
    else:
        if set(spec["tables"]) != {name + ".parquet" for name in SOURCE_NAMES}:
            raise ValueError("Incomplete legacy scorecard source generation")
        rows = {name: pq.ParquetFile(directory / (name + ".parquet")).read().to_pylist() for name in SOURCE_NAMES}
    installed_provider().validate(rows)
    return rows


def source_failure_receipts(failures, *, generation_id: str, registry: Path):
    """Safe scope-level refusals pinned to the actual input selection, without source bodies."""
    digest = "sha256:" + hashlib.sha256(registry.read_bytes()).hexdigest()
    return [
        failure_receipt(
            POLICIES["scorecard_snapshots"],
            ReceiptContext(
                generation_id,
                f"source-attempt:{index}",
                POLICY_VERSION,
                [dict(source_id="scorecard-registry", source_uri=None, sha256=digest, locator=None, body_version=None)],
                diagnostics={"reason_code": "source_acquisition_refused"},
            ),
            outcome="refused",
            raw_fields={"raw_source": raw},
        )
        for index, raw in enumerate(failures)
    ]


def selected_table_entry(snapshot: Mapping, key: str) -> Mapping:
    """Require a selected published table before reading its generation receipts."""
    from spicy_regs.sources import publication

    owner = publication.table_owner(snapshot, key)
    if owner is None:
        raise publication.PublicationError(f"Missing selected table owner: {key}")
    return owner[1]
