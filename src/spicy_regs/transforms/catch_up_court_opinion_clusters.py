"""Append a completed bounded search to an explicitly selected native court prior.

The caller supplies the retained evidence for the prior's source edition. This
route preserves that parent and adds a disjoint ID range; it does not infer that
the parent is a complete bulk export or refresh existing cluster values.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow.compute as pc
import pyarrow.parquet as pq
from rulespec_artifacts import publish_directory_no_replace

from spicy_regs.court_receipts import finish_court_output, local_receipt_selection
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.etl_receipts import combine_receipts
from spicy_regs.selected_generations import SelectedInputs
from spicy_regs.sources import publication
from spicy_regs.sources.courtlistener import CourtListenerOpinionSearchReader, require_api_token
from spicy_regs.transforms._courtlistener_writer import CourtListenerTableWriter, check_headroom
from spicy_regs.transforms.build_court_bulk_tables import _highest_cluster_id
from spicy_regs.transforms.build_court_opinion_clusters import BATCH_ROWS, _SCHEMA, _shape_search
from spicy_regs.transforms.court_scope import CourtScope, court_jurisdictions

if TYPE_CHECKING:
    from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget
    from spicy_regs.source_evidence import CaptureEvidence

DATASET = "court_opinion_clusters"
OUTPUT = DATASET + ".parquet"
FAMILY = "court-opinion-clusters"


def _append_native(prior: Path, delta: Path, destination: Path, *, guard) -> None:
    """Copy exact Arrow values in bounded batches; never choose an overlap winner."""
    schema = pq.read_schema(prior)
    if not schema.equals(SUBJECT_SCHEMAS[DATASET]) or not schema.equals(pq.read_schema(delta)):
        raise ValueError("Court catch-up requires matching native subject schemas")
    ids = pq.read_table(delta, columns=["cluster_id"])["cluster_id"]
    if len(pc.call_function("unique", [ids])) != len(ids) or ids.null_count:
        raise ValueError("Court catch-up delta repeats or omits a cluster identity")
    with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
        for source in (prior, delta):
            with pq.ParquetFile(source) as parquet:
                for batch in parquet.iter_batches(batch_size=BATCH_ROWS):
                    guard()
                    check_headroom(0, path=destination.parent)
                    if source == prior:
                        overlap = pc.call_function(
                            "is_in", [batch.column("cluster_id")], options=pc.SetLookupOptions(value_set=ids)
                        )
                        if pc.call_function("any", [overlap]).as_py():
                            raise ValueError("Court catch-up delta overlaps the selected prior")
                    writer.write_batch(batch, row_group_size=BATCH_ROWS)


def catch_up_court_opinion_clusters(
    output_dir: Path,
    *,
    prior_directory: Path,
    expected_prior: str,
    source_edition: date,
    source_edition_evidence: Path,
    source_edition_sha256: str,
    courts_local_file: Path,
    courts_sha256: str,
    search_through: int,
    search_budget: CourtListenerTraversalBudget,
    evidence: CaptureEvidence,
    retained_search_directory: Path | None = None,
) -> Path:
    """Fetch before prior admission/restoration; return a private complete candidate.

    ``evidence`` must carry the ordinary rollup's captured publication snapshot.
    The caller selects the exact parent and retained source-edition evidence;
    their relationship is an explicit reviewed input, not an edition detector.
    The local courts original must belong to that edition. Both small inputs
    are byte-pinned and retained with the selection. No bulk source is read.

    Prior member hashes and a native ID-only projection precede source access.
    After complete bounded traversal, only fresh rows are converted. Prior rows
    and all receipt values/identities survive unchanged, in prior-then-delta
    order. The returned owned family MUST pass ordinary generation admission
    and publication; this function neither admits nor selects a generation.
    """
    from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget

    if not isinstance(source_edition, date) or not isinstance(search_budget, CourtListenerTraversalBudget):
        raise ValueError("Court catch-up requires an explicit source edition and traversal allocation")
    require_api_token("Court opinion-cluster search catch-up")
    snapshot = evidence.read_snapshot
    if snapshot is None or evidence.family != FAMILY:
        raise ValueError("Court catch-up requires the captured court publication snapshot")
    owner = publication.table_owner(snapshot, OUTPUT)
    if owner is None or owner[0] != FAMILY or owner[1]["artifactDigest"] != expected_prior:
        raise ValueError("Court catch-up prior differs from the captured publication")
    entry = owner[1]
    if entry.get("etlReceipts", {}).get("datasets") != [DATASET]:
        raise ValueError("Court catch-up requires the selected single-dataset receipt family")
    (subject_member,) = publication.table_members(snapshot, OUTPUT)
    (receipt_member,) = publication.receipt_members(snapshot, dataset=DATASET)
    prior = prior_directory / OUTPUT
    receipts = prior_directory / entry["etlReceipts"]["key"]
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = SelectedInputs(output_dir, output_dir / ".catchup-pins", public_url="")
    for path, member in ((prior, subject_member), (receipts, receipt_member)):
        if selected.member_pin(path) != {"sha256": member.sha256, "byteSize": member.byte_size}:
            raise ValueError("Court catch-up prior bytes differ from the captured publication")
    for path, digest in ((source_edition_evidence, source_edition_sha256), (courts_local_file, courts_sha256)):
        if selected.member_pin(path)["sha256"] != digest:
            raise ValueError("Court catch-up source edition evidence differs from its byte pin")
    if not pq.read_schema(prior).equals(SUBJECT_SCHEMAS[DATASET]):
        raise ValueError("Court catch-up prior is not the native cluster schema")
    above = _highest_cluster_id([str(prior)])
    if above is None:
        raise ValueError("Court catch-up prior has no numeric cluster ceiling")
    retained_search = None
    if retained_search_directory is not None:
        from spicy_regs.sources.courtlistener_resume import RetainedCourtSearch

        retained_search = RetainedCourtSearch(retained_search_directory, {
            "family": FAMILY,
            "parent": entry,
            "source_edition": source_edition.isoformat(),
            "source_edition_sha256": source_edition_sha256,
            "courts_sha256": courts_sha256,
            "above": above,
            "through": search_through,
        })
    reader = CourtListenerOpinionSearchReader(
        above=above,
        through=search_through,
        traversal_budget=search_budget,
        evidence=evidence,
        retained_search=retained_search,
    )
    evidence.retain_file(
        source_edition_evidence,
        stage="catchup-source-edition",
        source_edition=source_edition.isoformat(),
        prior_generation=expected_prior,
    )
    evidence.retain_file(courts_local_file, stage="catchup-courts-original", source_edition=source_edition.isoformat())
    scope = CourtScope.for_courts(court_jurisdictions(dump_date=source_edition, local_file=courts_local_file))
    selected.check()
    evidence.event(
        "catchup-selection",
        parent=expected_prior,
        source_edition=source_edition.isoformat(),
        above=above,
        through=search_through,
        max_requests=search_budget.max_requests,
        max_wait_seconds=str(search_budget.max_wait_seconds),
        coverage=("Preserved parent plus bounded all-status indexed search; unindexed clusters may be absent. "
                  "Bulk coverage inherited; maximum ID is not citation membership proof."),
    )
    staged = output_dir / (".clusters-delta-" + uuid4().hex + ".parquet")
    writer = CourtListenerTableWriter(staged, schema=_SCHEMA, batch_size=BATCH_ROWS)
    try:
        for result in reader.iter_records():
            writer.add(_shape_search(result, scope=scope))
        writer.close()
        selected.check()
    except BaseException:
        writer.abort()
        raise

    generation_id = uuid4().hex
    delta = finish_court_output(
        DATASET,
        staged,
        output_dir / ".court-delta" / generation_id,
        generation_id=generation_id,
        diagnostics={
            "source_edition": source_edition.isoformat(),
            "prior_generation": expected_prior,
            "above": above,
            "through": search_through,
            "search_rows": writer.written,
        },
    )
    delta_receipts, _ = local_receipt_selection(delta)
    selected.member_pin(delta)
    selected.member_pin(delta_receipts)
    bundle = output_dir / ".court-etl" / uuid4().hex
    bundle.parent.mkdir(exist_ok=True)
    with TemporaryDirectory(prefix=".court-append-", dir=bundle.parent) as temporary:
        complete = Path(temporary) / "bundle"
        complete.mkdir()
        _append_native(prior, delta, complete / OUTPUT, guard=selected.check)
        combine_receipts([receipts, delta_receipts], complete / "etl_receipts.parquet")
        selected.check()
        (complete / "court-family.json").write_text(
            json.dumps({"generation_id": generation_id, "datasets": [DATASET]}) + "\n"
        )
        publish_directory_no_replace(complete, bundle)
    return bundle / OUTPUT
