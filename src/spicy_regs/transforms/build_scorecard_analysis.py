"""Resolve publisher identities against one verified set of congressional tables."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from spicy_regs.scorecards.etl import read_family, read_source_inputs, write_family
from spicy_regs.scorecards.action_comparisons import TABLE as COMPARISON_TABLE, COLUMNS as COMPARISON_COLUMNS, KEYS as COMPARISON_KEYS, build_comparisons

SOURCE_TABLES = ("scorecards", "scorecard_members", "scorecard_items")
OFFICIAL_TABLES = ("members", "member_terms", "congress_bills", "amendments", "roll_call_votes")
COMPARISON_SOURCE_INPUTS = ("scorecard_member_item_results", "scorecard_metric_items", "scorecard_methodologies")
COMPARISON_INPUTS = (*COMPARISON_SOURCE_INPUTS, "member_votes", "bill_cosponsors")
INPUTS = tuple(f"{name}.parquet" for name in (*SOURCE_TABLES, *OFFICIAL_TABLES, *COMPARISON_INPUTS))
OUTPUTS = ("scorecard_member_links.parquet", "scorecard_item_links.parquet", COMPARISON_TABLE + ".parquet")


def analysis_input_entries(snapshot: Mapping, *, convert_published_official_inputs: bool = False) -> dict[str, Mapping]:
    """Require a complete native input selection before downloading its members.

    This only checks the captured index. The caller still verifies every pinned
    byte, and the build still admits subject/receipt joins under their selected
    generations. The preparation host may explicitly convert already-published
    official shaped inputs through the normal Congress writer; it must supply
    verified new native bundles before building. Source scorecard receipts are
    always required. No legacy or prior receipts stand in for missing receipts.
    """
    from spicy_regs.scorecards.analysis_inputs import published_input
    from spicy_regs.scorecards.etl import selected_table_entry
    from spicy_regs.sources import publication

    entries = {name: selected_table_entry(snapshot, name + ".parquet")
               for name in (*SOURCE_TABLES, *OFFICIAL_TABLES, *COMPARISON_INPUTS)}
    published = {name for name, owner in entries.items() if published_input(owner, name)}
    source = entries["scorecards"]
    if any(entries[name]["artifactDigest"] != source["artifactDigest"] for name in (*SOURCE_TABLES, *COMPARISON_SOURCE_INPUTS)):
        raise publication.PublicationError("Scorecard analysis source inputs must share one generation")
    missing = [name for name, owner in entries.items()
               if name not in owner.get("etlReceipts", {}).get("datasets", ()) and name not in published
               and not (convert_published_official_inputs and name in OFFICIAL_TABLES and "etlReceipts" not in owner)]
    if missing:
        raise publication.PublicationError("Scorecard analysis requires native receipts for: " + ", ".join(missing))
    for name, owner in entries.items():
        if name in published or (convert_published_official_inputs and name in OFFICIAL_TABLES
                                 and "etlReceipts" not in owner):
            continue
        generation = owner["etlReceipts"].get("generationId")
        if not isinstance(generation, str) or not generation or len(publication.receipt_members(snapshot, dataset=name)) != 1:
            raise publication.PublicationError("Scorecard analysis requires one selected native receipt generation: " + name)
    return entries


def _read(paths: Sequence[Path], columns: Sequence[str] | None = None) -> list[dict]:
    if not paths or any(not path.is_file() for path in paths):
        raise FileNotFoundError("Every analysis input needs its verified Parquet members")
    # ParquetFile avoids inventing hive partition fields from a source path.
    rows = []
    for path in paths:
        reader = pq.ParquetFile(path)
        missing = sorted(set(columns or ()) - set(reader.schema_arrow.names))
        if missing:
            raise ValueError(f"Analysis input {path.name} lacks required columns: {', '.join(missing)}")
        for batch in reader.iter_batches(columns=list(columns) if columns else None):
            rows.extend(batch.to_pylist())
    return rows


def _checked_rows(rows: list[dict], columns: Sequence[str], key: Sequence[str]) -> None:
    seen = set()
    for row in rows:
        if set(row) != set(columns) or any(value is not None and not isinstance(value, str) for value in row.values()):
            raise ValueError("Analysis rows must match the declared string/null schema")
        identity = tuple(row[column] for column in key)
        if any(not value for value in identity) or identity in seen:
            raise ValueError("Analysis rows have an empty or duplicate identity")
        seen.add(identity)


def build_scorecard_analysis(
    output_dir: Path,
    *,
    input_pins: Mapping[str, dict],
    input_paths: Mapping[str, Sequence[Path]],
    comparison_index: Mapping,
    member_overrides: Sequence[dict] = (),
    source_receipt_path: Path | None = None,
    source_generation_id: str | None = None,
    official_receipts: Mapping[str, tuple[Path, str]] | None = None,
    receipt_generation_id: str | None = None,
    projected_official_index: Mapping | None = None,
) -> tuple[Path, ...]:
    """Resolve already verified inputs; the rollup owns download and generation pins.

    Logical table names key both mappings. A published table may have multiple
    Parquet members, all drawn from the same captured publication index. Missing
    files refuse the build; they never become an empty congressional table.
    """
    from spicy_regs.scorecards.resolution import (
        ITEM_LINK_COLUMNS,
        MEMBER_LINK_COLUMNS,
        OFFICIAL_COLUMNS,
        RULE_VERSION,
        resolve_scorecard_links,
    )

    expected = set((*SOURCE_TABLES, *OFFICIAL_TABLES, *COMPARISON_INPUTS))
    if set(input_pins) != expected or set(input_paths) != expected:
        raise ValueError("Scorecard analysis requires every source and official input pin")
    if source_receipt_path is None or not source_generation_id:
        raise ValueError("Scorecard inputs require their selected native receipts and generation")
    source = read_source_inputs(
        {name: input_paths[name] for name in SOURCE_TABLES}, source_receipt_path, generation_id=source_generation_id
    )
    if not source["scorecards"]:
        raise ValueError("An empty scorecard corpus cannot qualify an analysis generation")
    from spicy_regs.scorecards.analysis_inputs import (
        PROJECTED_NATIVE_OFFICIAL_TABLES, read_published_native_resolver_rows,
    )
    projected = PROJECTED_NATIVE_OFFICIAL_TABLES if projected_official_index is not None else frozenset()
    if projected_official_index is not None:
        from spicy_regs.sources import publication
        if any(input_pins[name] != publication.table_pin(projected_official_index, name + ".parquet")
               for name in projected):
            raise ValueError("Projected resolver inputs differ from their selected publication pins")
    from spicy_regs.subject_catalog import retired_receipt_datasets

    # A table whose receipts are retired comes without them once its family lists none (``published_input``) and is
    # read as its published table, in the builder's spelling (``congress_tables.as_builder_input``).
    needed = set(OFFICIAL_TABLES) - projected
    if official_receipts is None or not set(official_receipts) <= needed or (
            needed - set(official_receipts) - retired_receipt_datasets()):
        raise ValueError("Every official analysis input requires its selected native receipts")
    from spicy_regs.congress_receipts import CongressInput
    from spicy_regs.congress_tables import as_builder_input

    official = {}
    with TemporaryDirectory(prefix="scorecard-official-inputs-") as temporary:
        for name in OFFICIAL_TABLES:
            if name in projected:
                official[name] = read_published_native_resolver_rows(projected_official_index, name, input_paths[name])
                continue
            path = Path(temporary) / (name + ".parquet")
            if name in official_receipts:
                receipts, generation_id = official_receipts[name]
                CongressInput(tuple(input_paths[name]), receipts, generation_id).materialize(name, path)
            else:
                as_builder_input(name, tuple(input_paths[name]), path)
            official[name] = _read([path], OFFICIAL_COLUMNS[name])
    results = resolve_scorecard_links(source, official, input_pins, member_overrides=member_overrides)
    from spicy_regs.sources import publication
    if any(input_pins[name] != publication.table_pin(comparison_index, name + ".parquet")
           for name in COMPARISON_INPUTS):
        raise ValueError("Action-comparison inputs differ from their selected publication pins")
    results[COMPARISON_TABLE], sponsorship = build_comparisons(
        comparison_index, input_paths, source, results, official["members"], input_pins)
    definitions = (
        ("scorecard_member_links", MEMBER_LINK_COLUMNS, ("scorecard_id", "publisher_member_key")),
        ("scorecard_item_links", ITEM_LINK_COLUMNS, ("scorecard_id", "item_id", "reference_id")),
        (COMPARISON_TABLE, COMPARISON_COLUMNS, COMPARISON_KEYS),
    )
    if set(results) != {name for name, _, _ in definitions}:
        raise ValueError("Resolver output differs from the complete analysis family")
    for name, columns, key in definitions:
        _checked_rows(results[name], columns, key)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = write_family(output_dir, results, generation_id=receipt_generation_id)
    read_family(output_dir, tuple(results))  # Persisted one-to-one linkage and refusal readback.
    receipt = {
        "rule_version": RULE_VERSION,
        "input_pins": dict(input_pins),
        "counts": {name: len(rows) for name, rows in results.items()},
        "resolution_statuses": {
            name: dict(sorted(Counter(row["resolution_status"] for row in rows).items()))
            for name, rows in results.items() if name != COMPARISON_TABLE
        },
        "scope": "Exact identity links, LCV 2025 floor votes and the selected Common Cause 2022 House DISCLOSE sponsorship; publisher ratings remain unchanged.",
        "comparison": {
            "rule_versions": sorted({r["rule_version"] for r in results[COMPARISON_TABLE]}),
            "scopes": sorted({r["scorecard_id"] for r in results[COMPARISON_TABLE]}),
            "historical_sponsorship": sponsorship,
            "statuses": dict(sorted(Counter(r["comparison_status"] for r in results[COMPARISON_TABLE]).items())),
            "deferred": ["other_historical_sponsorship_items", "score_reproduction", "other_publishers_and_editions"],
            "input_representations": {name: "complete_pinned_native_main_fields" for name in COMPARISON_INPUTS},
        },
        "official_input_representations": {
            name: "complete_pinned_native_main_fields" if name in projected else "receipt_checked_original_source_fields"
            for name in OFFICIAL_TABLES
        },
    }
    (output_dir / "scorecard-analysis-qualification.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return paths
