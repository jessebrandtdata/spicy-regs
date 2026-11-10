"""Hermetic tests for the CourtListener tables copied from one bulk export (citations, map, parentheticals, opinions)."""

from __future__ import annotations

import bz2
import hashlib
import importlib
import tomllib
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from spicy_docs.sources.courtlistener.listing import BulkObject

from spicy_regs import data_dictionary as dd
from spicy_regs import mcp_server
from spicy_regs.pipelines.rollups.court_citations import CourtCitationsRollup
from spicy_regs.pipelines.rollups.court_opinions import CourtOpinionsRollup
from spicy_regs.transforms.build_court_bulk_tables import (
    CITATION_MAP,
    CITATIONS,
    OPINIONS,
    PARENTHETICALS,
    build_court_bulk_table,
    build_court_bulk_tables,
    export_file,
    latest_common_dump_date,
)

# The package re-exports the builder function under the module's own name.
module = importlib.import_module("spicy_regs.transforms.build_court_bulk_tables")
REPO_ROOT = Path(__file__).resolve().parents[1]
EDITION = date(2026, 6, 30)


def _export(path: Path, body: bytes) -> Path:
    path.write_bytes(bz2.compress(body))
    return path


def _listed(path: Path, dataset: str, edition: str = "2026-06-30") -> BulkObject:
    return BulkObject(f"bulk-data/{dataset}-{edition}.csv.bz2", path.stat().st_size, '"etag"', f"{edition}T12:00:00Z")


def test_a_table_renames_selected_fields_stamps_the_edition_and_keeps_values_exact(tmp_path):
    source = _export(
        tmp_path / "citations.csv.bz2",
        b"id,volume,reporter,page,type,cluster_id,date_created,date_modified\n"
        b'"7","410","U.S.","113","1","108713","2014-10-30 06:37:34+00",""\n'
        b'"8","1","F. Supp. \\"X\\"","","2","5",,"2025-01-01"\n',
    )
    out = build_court_bulk_table(CITATIONS, tmp_path / "out", local_file=source, dump_date=EDITION)
    table = pq.read_table(out)
    assert table.schema == CITATIONS.schema
    assert table.column_names == list(CITATIONS.columns) == [c for c, _ in dd.expected_schemas()["court_citations"]]
    digest = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    assert table.to_pylist() == [
        {
            "citation_id": "7",
            "cluster_id": "108713",
            "volume": "410",
            "reporter": "U.S.",
            "page": "113",
            "citation_type": "1",
            "date_created": "2014-10-30 06:37:34+00",
            "date_modified": "",
            "dump_date": "2026-06-30",
            "source_sha256": digest,
        },
        {
            "citation_id": "8",
            "cluster_id": "5",
            "volume": "1",
            "reporter": 'F. Supp. "X"',
            "page": "",
            "citation_type": "2",
            "date_created": None,
            "date_modified": "2025-01-01",
            "dump_date": "2026-06-30",
            "source_sha256": digest,
        },
    ]
    assert not list((tmp_path / "out").glob(".*partial"))


def test_the_citation_map_drops_only_its_surrogate_id(tmp_path):
    source = _export(tmp_path / "map.csv.bz2", b'id,depth,cited_opinion_id,citing_opinion_id\n"1","3","20","10"\n')
    out = build_court_bulk_table(CITATION_MAP, tmp_path, local_file=source, dump_date=EDITION)
    assert pq.read_table(out).to_pylist() == [
        {"citing_opinion_id": "10", "cited_opinion_id": "20", "depth": 3, "dump_date": "2026-06-30",
         "source_sha256": "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()}
    ]


def test_a_failed_export_leaves_the_previous_table_in_place(tmp_path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    previous = out_dir / PARENTHETICALS.output
    previous.write_bytes(b"previous generation")
    source = _export(
        tmp_path / "parentheticals.csv.bz2",
        b'id,text,score,described_opinion_id,describing_opinion_id,group_id\n"1","unterminated\n',
    )
    with pytest.raises(Exception, match="CSV"):
        build_court_bulk_table(PARENTHETICALS, out_dir, local_file=source, dump_date=EDITION)
    assert previous.read_bytes() == b"previous generation"
    assert not list(out_dir.glob(".*partial"))


def test_one_run_reads_the_newest_edition_that_publishes_every_table(tmp_path):
    path = _export(tmp_path / "x.csv.bz2", b"a\n")
    objects = [
        _listed(path, "citations", "2026-09-30"),
        _listed(path, "citations", "2026-06-30"),
        _listed(path, "citation-map", "2026-06-30"),
    ]
    assert latest_common_dump_date(objects, ["citations", "citation-map"]) == EDITION
    with pytest.raises(RuntimeError, match="no export date publishes all"):
        latest_common_dump_date(objects, ["citations", "parentheticals"])


def test_a_retained_export_is_used_only_at_its_listed_size(tmp_path, monkeypatch):
    retained = tmp_path / "retained"
    retained.mkdir()
    copy = _export(retained / "citations-2026-06-30.csv.bz2", b'a\n"1"\n')
    monkeypatch.setenv(module.RETAINED_DIR_ENV, str(retained))
    monkeypatch.setattr(module, "check_headroom", lambda *a, **k: pytest.fail("a retained copy needs no download"))
    assert export_file(_listed(copy, "citations"), tmp_path) == (copy, None)

    wrong = BulkObject(
        "bulk-data/citations-2026-06-30.csv.bz2", copy.stat().st_size + 1, '"etag"', "2026-06-30T12:00:00Z"
    )
    with pytest.raises(RuntimeError, match="not the listed"):
        export_file(wrong, tmp_path)


def test_a_missing_export_is_downloaded_bound_to_its_listing(tmp_path, monkeypatch):
    monkeypatch.delenv(module.RETAINED_DIR_ENV, raising=False)
    listed = BulkObject("bulk-data/citations-2026-06-30.csv.bz2", 123, '"abc"', "2026-06-30T12:00:00Z")
    checked, requests = [], []

    class Acquirer:
        def __init__(self, *, validate_url, timeout):
            validate_url(listed.url)
            with pytest.raises(ValueError, match="bulk-data bucket"):
                validate_url("https://example.com/citations.csv.bz2")

        def __enter__(self):
            return self

        def __exit__(self, *error):
            return None

        def download(self, url, **kwargs):
            requests.append((url, kwargs))
            return {"blob_path": "sha256/abc"}

    import spicy_docs.transport.download as download

    monkeypatch.setattr(download, "BoundedAcquirer", Acquirer)
    monkeypatch.setattr(module, "check_headroom", lambda needed, path: checked.append((needed, path)))
    path, downloaded = export_file(listed, tmp_path)
    assert (path, downloaded) == (tmp_path / "courtlistener-exports" / "sha256/abc", None)
    assert checked == [(123, tmp_path)]
    [(url, kwargs)] = requests
    assert url == listed.url
    assert kwargs["max_bytes"] == kwargs["expected_size"] == 123 and kwargs["etag"] == '"abc"'


def test_downloaded_and_preexisting_exports_remain_retained(tmp_path, monkeypatch):
    bodies = {
        "citations": b'id,volume,reporter,page,type,cluster_id,date_created,date_modified\n"1","1","U.S.","1","1","2",,\n',
        "citation-map": b'id,depth,cited_opinion_id,citing_opinion_id\n"1","1","2","3"\n',
    }
    files = {name: _export(tmp_path / f"{name}.csv.bz2", body) for name, body in bodies.items()}
    import spicy_docs.sources.courtlistener.bulk as bulk

    monkeypatch.setattr(bulk, "list_bulk_dumps", lambda: [_listed(path, name) for name, path in files.items()])
    monkeypatch.setattr(module, "export_file", lambda obj, work, evidence=None: (files[obj.dataset], None))
    monkeypatch.setattr(module, "published_cluster_ceiling", lambda: 2)
    built = build_court_bulk_tables((CITATIONS, CITATION_MAP), tmp_path / "out")
    assert built == (tmp_path / "out" / CITATIONS.output, tmp_path / "out" / CITATION_MAP.output)
    assert files["citations"].exists() and files["citation-map"].exists(), "an export is never removed"
    # Each row names the export it was read from by content, not by this machine's path; no receipt is written.
    for path, name in zip(built, ("citations", "citation-map")):
        digest = "sha256:" + hashlib.sha256(files[name].read_bytes()).hexdigest()
        assert set(pq.read_table(path, columns=["source_sha256"])["source_sha256"].to_pylist()) == {digest}
    assert not list(tmp_path.rglob("etl_receipts.parquet")) and not list(tmp_path.rglob(".court-etl"))
    assert not list((tmp_path / "out").glob(".*"))


_CITATIONS_OF_CLUSTER_9 = (
    b'id,volume,reporter,page,type,cluster_id,date_created,date_modified\n"1","1","U.S.","1","1","9",,\n'
)


def test_a_table_naming_a_cluster_newer_than_the_published_clusters_waits_for_them(tmp_path):
    """The publisher cuts citations hours after clusters; publishing them first would orphan the newest."""
    source = _export(tmp_path / "citations.csv.bz2", _CITATIONS_OF_CLUSTER_9)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    previous = out_dir / CITATIONS.output
    previous.write_bytes(b"previous generation")
    with pytest.raises(RuntimeError, match="names cluster 9, above the published court_opinion_clusters.parquet"):
        build_court_bulk_table(CITATIONS, out_dir, local_file=source, dump_date=EDITION, cluster_ceiling=8)
    assert previous.read_bytes() == b"previous generation"
    assert not list(out_dir.glob(".*partial"))
    out = build_court_bulk_table(CITATIONS, out_dir, local_file=source, dump_date=EDITION, cluster_ceiling=9)
    assert pq.read_table(out, columns=["cluster_id"]).to_pylist() == [{"cluster_id": "9"}]
    assert not CITATION_MAP.names_clusters and not PARENTHETICALS.names_clusters and OPINIONS.names_clusters


def test_tables_naming_clusters_are_refused_before_any_download_when_none_are_published(tmp_path, monkeypatch):
    import spicy_docs.sources.courtlistener.bulk as bulk

    source = _export(tmp_path / "citations.csv.bz2", _CITATIONS_OF_CLUSTER_9)
    monkeypatch.setattr(bulk, "list_bulk_dumps", lambda: [_listed(source, "citations")])
    monkeypatch.setattr(module, "export_file", lambda *a: pytest.fail("nothing is downloaded before the check"))
    monkeypatch.setattr(module, "published_cluster_ceiling", lambda: None)
    with pytest.raises(RuntimeError, match="court_opinion_clusters.parquet is not published"):
        build_court_bulk_tables((CITATIONS,), tmp_path / "out")


@pytest.mark.parametrize("table", [CITATIONS, CITATION_MAP, PARENTHETICALS, OPINIONS], ids=lambda t: t.dataset)
def test_every_table_is_registered_everywhere_the_dictionary_needs_it(table):
    name = table.output.removesuffix(".parquet")
    assert name in dd.TABLES and name in dd.MCP_QUERYABLE and name in mcp_server.TABLES
    from spicy_regs.native_types import described_schema
    from spicy_regs.court_receipts import POLICIES
    assert dd.expected_schemas()[name] == described_schema(POLICIES[name].subject_schema)
    assert (REPO_ROOT / "docs" / "tables" / f"{name}.md").exists()


def test_the_rollups_have_console_scripts_and_only_the_citations_rollup_is_scheduled():
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert CourtCitationsRollup.outputs == (CITATIONS.output, CITATION_MAP.output, PARENTHETICALS.output)
    assert CourtOpinionsRollup.output == OPINIONS.output
    for rollup in (CourtCitationsRollup, CourtOpinionsRollup):
        assert f"run-rollup-{rollup.name}" in scripts
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/rollup-court-citations.yml").read_text())
    assert workflow["jobs"]["run"]["with"]["command"] == "run-rollup-court-citations"
    # The opinions export takes ~8.6 hours to download, past a hosted runner's six: it is built and published by
    # hand, and the citations only by their workflow.
    assert not (REPO_ROOT / ".github/workflows/rollup-court-opinions.yml").exists()
    from spicy_regs.local_publication import LOCAL_FAMILIES

    assert "court-opinions" in LOCAL_FAMILIES and "court-citations" not in LOCAL_FAMILIES


def test_a_machine_can_state_its_own_disk_floor(tmp_path, monkeypatch):
    """A CI runner's whole disk is smaller than the workstation floor; it states its own instead."""
    import shutil

    from spicy_regs.transforms import _courtlistener_writer as writer

    free = 20 * 2**30
    monkeypatch.setattr(shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(total=free * 2, used=free, free=free))
    with pytest.raises(RuntimeError, match="below the 100 GiB floor"):
        writer.check_headroom(2**30, path=tmp_path)
    monkeypatch.setenv(writer.DISK_FLOOR_ENV, "4")
    writer.check_headroom(2**30, path=tmp_path)
    with pytest.raises(RuntimeError, match="below the 4 GiB floor"):
        writer.check_headroom(17 * 2**30, path=tmp_path)
