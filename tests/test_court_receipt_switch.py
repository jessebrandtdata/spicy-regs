"""Court opinions and citations publish without per-row receipts, and lose nothing a reader saw (ss-vse3).

The fixture exports are the heads of the real retained exports (citations, citation map and parentheticals of
2026-09-30, opinions of 2026-06-30; 300 to 400 records each, reduced to the columns the builder reads), plus a few
appended rows the publisher's canonical spellings do not cover. ``expected/`` holds what main e20a8ea1's receipt route
built from those same files: each table, its receipts and their generation id. The plain route must give the same
table (schema, rows both ways, order) on the receipt route's columns, and every promoted column must equal the copy
its receipts held, with no receipts file written anywhere.

What these cannot see: a full export (the fixtures are heads; the full-size replay is in
docs/court-receipt-archive.md), and row-group layout.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.policy_migrations import earlier_policy
from spicy_regs.receipt_switch import promoted_differences, receipt_files, table_differences
from spicy_regs.transforms.build_court_bulk_tables import (
    CITATION_MAP,
    CITATIONS,
    OPINIONS,
    PARENTHETICALS,
    build_court_bulk_table,
)

FIXTURE = Path(__file__).parent / "fixtures/court_receipt_switch"
GENERATIONS = json.loads((FIXTURE / "expected/generations.json").read_text())
FLAGS = {"t": True, "f": False}

#: Table, fixture export, edition, the policy the receipt route wrote it under, promoted columns and how each reads
#: its copy from an admitted receipt-route row.
CASES = [
    (CITATIONS, "citations", "2026-09-30", "courts/1",
     {name: (lambda row, name=name: row[name]) for name in ("date_created", "date_modified", "dump_date")}),
    (CITATION_MAP, "citation-map", "2026-09-30", "courts/1", {"dump_date": lambda row: row["dump_date"]}),
    (PARENTHETICALS, "parentheticals", "2026-09-30", "courts/1", {"dump_date": lambda row: row["dump_date"]}),
    (OPINIONS, "opinions", "2026-06-30", "courts/2", {
        **{name: (lambda row, name=name: row[name])
           for name in ("local_path", "date_created", "date_modified", "dump_date")},
        "extracted_by_ocr": lambda row: None if row["extracted_by_ocr"] is None else FLAGS[row["extracted_by_ocr"]],
    }),
]


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("table,export,edition,version,copies", CASES, ids=lambda case: getattr(case, "dataset", None))
def test_the_plain_route_publishes_the_receipt_routes_table_with_its_fields_as_columns(
        tmp_path, table, export, edition, version, copies):
    name = table.output.removesuffix(".parquet")
    source = FIXTURE / "exports" / f"{export}.csv.bz2"
    built = build_court_bulk_table(table, tmp_path / "out", local_file=source, dump_date=date.fromisoformat(edition))
    expected = FIXTURE / "expected" / table.output
    old = earlier_policy(name, version)
    assert pq.read_schema(expected).equals(old.subject_schema)
    key = list(old.identity_fields)

    # The receipt route's columns, unchanged: schema, every row both ways, and the key at every position.
    projected = tmp_path / "projected.parquet"
    pq.write_table(pq.read_table(built, columns=old.subject_schema.names), projected)
    assert table_differences(projected, expected, key=key) == []
    # The promoted columns follow them, equal to the copies the receipt route's receipts held.
    names = pq.read_schema(built).names
    assert names[:len(old.subject_schema)] == old.subject_schema.names
    assert set(names[len(old.subject_schema):]) == {*copies, "source_sha256"}
    assert promoted_differences(built, expected, FIXTURE / "expected" / f"{name}.receipts.parquet", old,
                                generation_id=GENERATIONS[name], key=key, copies=copies) == []
    # Every row points at the export file it was read from.
    assert set(pq.read_table(built, columns=["source_sha256"])["source_sha256"].to_pylist()) == {_digest(source)}
    assert receipt_files(tmp_path) == []


def test_a_spelling_the_rule_does_not_reproduce_or_a_refusal_is_recorded_with_its_source_row(tmp_path):
    """The receipt route kept these in a receipt's conversion inputs; the plain route keeps them in its exceptions."""
    out = tmp_path / "out"
    for table, export, edition in ((CITATION_MAP, "citation-map", "2026-09-30"),
                                   (PARENTHETICALS, "parentheticals", "2026-09-30"),
                                   (OPINIONS, "opinions", "2026-06-30")):
        build_court_bulk_table(table, out, local_file=FIXTURE / "exports" / f"{export}.csv.bz2",
                               dump_date=date.fromisoformat(edition))
    found = {}
    KEYS = {"court_citation_map": "citing_opinion_id", "court_parentheticals": "parenthetical_id",
            "court_opinions": "opinion_id"}
    for path in sorted(out.glob("*.exceptions.parquet")):
        for row in pq.read_table(path).to_pylist():
            record = json.loads(row["source_record"])
            found[row["dataset"], record[KEYS[row["dataset"]]]] = (row["outcome"], record)
    assert {key: outcome for key, (outcome, _) in found.items()} == {
        ("court_citation_map", "12"): "respelled",           # depth "007": published as 7
        ("court_citation_map", "14"): "refused",             # depth "deep": not published, as before
        ("court_parentheticals", "990000003"): "respelled",  # score "0.50": published as 0.5
        ("court_opinions", "990000004"): "respelled",        # per_curiam "true": published as true
    }
    assert found["court_citation_map", "12"][1]["depth"] == "007"
    published = pq.read_table(out / CITATION_MAP.output).to_pylist()
    assert {"citing_opinion_id": "12", "cited_opinion_id": "11", "depth": 7} in [
        {k: row[k] for k in ("citing_opinion_id", "cited_opinion_id", "depth")} for row in published]
    assert not any(row["citing_opinion_id"] == "14" for row in published)
    # A table with nothing to record leaves no exceptions file.
    build_court_bulk_table(CITATIONS, out, local_file=FIXTURE / "exports/citations.csv.bz2", dump_date=date(2026, 9, 30))
    assert not (out / "court_citations.exceptions.parquet").exists()


def test_a_download_with_evidence_goes_once_into_the_raw_archive_and_is_read_there(tmp_path, monkeypatch):
    import httpx
    from spicy_docs.sources.courtlistener.listing import BulkObject

    from spicy_regs.source_evidence import JOURNAL, CaptureEvidence
    from spicy_regs.transforms import build_court_bulk_tables as module

    body = (FIXTURE / "exports/citation-map.csv.bz2").read_bytes()
    listed = BulkObject("bulk-data/citation-map-2026-09-30.csv.bz2", len(body), '"etag-1"', "2026-09-30T12:00:00Z")
    served = []

    def serve(request: httpx.Request) -> httpx.Response:
        served.append(str(request.url))
        return httpx.Response(200, content=body, headers={"etag": '"etag-1"', "content-length": str(len(body))})

    monkeypatch.delenv(module.RETAINED_DIR_ENV, raising=False)
    monkeypatch.setenv("SPICY_REGS_DISK_FLOOR_GIB", "0")
    evidence = CaptureEvidence(tmp_path / "work", "court-citations", archive_root=tmp_path / "raw")
    path, digest = module.export_file(listed, tmp_path / "work", evidence, transport=httpx.MockTransport(serve))
    assert served == [listed.url]
    assert path.is_relative_to(tmp_path / "raw") and path.read_bytes() == body
    assert digest == "sha256:" + hashlib.sha256(body).hexdigest()
    assert not (tmp_path / "work" / "courtlistener-exports").exists(), "the archive object is the only copy"
    events = [json.loads(line) for line in (evidence.artifact_dir / JOURNAL).read_text().splitlines()]
    [capture] = [event for event in events if event["event"] == "capture"]
    assert capture["stage"] == module.EXPORT_STAGE and capture["sha256"] == digest
    assert capture["blob_store"] == "central-raw"

    stale = BulkObject(listed.key, listed.size, '"etag-2"', listed.last_modified)
    with pytest.raises(RuntimeError, match="not the listed"):
        module.export_file(stale, tmp_path / "work", evidence, transport=httpx.MockTransport(serve))


@pytest.mark.parametrize("table,export,edition,version,copies", CASES, ids=lambda case: getattr(case, "dataset", None))
def test_the_full_size_check_reads_the_same_copies_in_sql_and_sees_a_change(
        tmp_path, table, export, edition, version, copies):
    """The check docs/court-receipt-archive.md runs on the whole published receipts, by range, before a publish."""
    from scripts.check_court_receipt_switch import COPIES as SQL_COPIES
    from spicy_regs.receipt_switch import receipt_copy_differences

    name = table.output.removesuffix(".parquet")
    built = build_court_bulk_table(table, tmp_path / "out", local_file=FIXTURE / "exports" / f"{export}.csv.bz2",
                                   dump_date=date.fromisoformat(edition))
    receipts = FIXTURE / "expected" / f"{name}.receipts.parquet"
    key = list(earlier_policy(name, version).identity_fields)
    found = receipt_copy_differences(built, receipts, dataset=name, key=key, columns=SQL_COPIES[name])
    assert found == {"receipts": pq.read_metadata(FIXTURE / "expected" / table.output).num_rows, "without_row": 0,
                     **dict.fromkeys(SQL_COPIES[name], 0)}
    rows = pq.read_table(built).to_pylist()
    rows[0]["dump_date"] = "2000-01-01"
    damaged = tmp_path / "damaged.parquet"
    pq.write_table(pa.Table.from_pylist(rows[1:] if name == "court_citation_map" else rows,
                                        schema=pq.read_schema(built)), damaged)
    changed = receipt_copy_differences(damaged, receipts, dataset=name, key=key, columns=SQL_COPIES[name])
    assert (changed["without_row"], changed["dump_date"]) == ((1, 1) if name == "court_citation_map" else (0, 1))
