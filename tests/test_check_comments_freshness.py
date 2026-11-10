"""The nightly check binds what readers see to the publisher's receipt and verifies only what moved."""

import importlib.util
from pathlib import Path

import duckdb
import pytest

from spicy_regs.comments_health import check_comments
from spicy_regs.pipelines.comments_mirror import receipt_files, receipt_source
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, publication, regulatory_catalog

_SPEC = importlib.util.spec_from_file_location(
    "check_comments_freshness", Path(__file__).parents[1] / "scripts/check_comments_freshness.py")
assert _SPEC and _SPEC.loader
freshness = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(freshness)

BASE = "https://data.example"
SHA = "a" * 64
SOURCE = {"table_uuid": "t", "snapshot_id": 7, "schema_id": 0,
          "receipts": {"table_uuid": "r", "snapshot_id": 9, "schema_id": 0}}
FAMILY = {"artifactDigest": "sha256:" + "b" * 64, "prefix": "generations/comments/" + "b" * 64,
          "tables": {"comments_index.parquet": {"rows": 3, "byteSize": 5, "sha256": "sha256:" + SHA}}}


def _receipt(**changes):
    files = {
        "comments.parquet": {"etag": '"m"', "bytes": 10, "rows": 2_000_000, "sha256": SHA},
        "comments_index.parquet": {"etag": '"i"', "bytes": 5, "rows": 3, "sha256": SHA},
        "comments/agency/agency_code=EPA/part-0.parquet": {"etag": '"e"', "bytes": 7, "rows": 2_000_000, "sha256": SHA},
    }
    return {"format_version": 1, "source": SOURCE, "files": files, "generation": FAMILY, **changes}


@pytest.fixture
def published(monkeypatch):
    """Public objects and the generation as the receipt states them, a bound catalog, and a content-check recorder."""
    receipt = _receipt()
    versions = {f"{BASE}/{key}": {"etag": item["etag"], "bytes": item["bytes"]}
                for key, item in receipt["files"].items()}
    state: dict = {"receipt": receipt, "versions": versions, "family": FAMILY, "bound": True, "receipts_bound": True,
                   "catalog_errors": [], "verified": []}
    monkeypatch.setenv("SPICY_REGS_R2_URL", BASE)
    monkeypatch.setattr("sys.argv", ["check"])

    def load(base):
        if isinstance(state["receipt"], Exception):
            raise state["receipt"]
        return None if state["receipt"] is None else {"receipt": state["receipt"], "receipt_sha256": "sha256:" + SHA}

    def check(rt, indexes, *, agency, limit):
        state["verified"].append((agency, sorted(indexes)))
        return state["catalog_errors"]

    monkeypatch.setattr(publication, "load_comments_publication", load)
    monkeypatch.setattr(publication, "load_index", lambda base: {
        "families": {} if state["family"] is None else {"comments": state["family"]}})
    monkeypatch.setattr(publication, "public_object_version", lambda url, **_: state["versions"].get(url))
    monkeypatch.setattr(iceberg, "compacted_since",
                        lambda rt, snapshot, **_: (state["bound"], state["receipts_bound"]))
    monkeypatch.setattr(iceberg, "check_comments_against_indexes", check)
    return state


def test_unchanged_publication_generation_and_compacted_catalog_read_no_rows(published, capsys):
    assert freshness.main() == 0
    assert published["verified"] == []
    assert "catalog snapshot 7 (compaction only)" in capsys.readouterr().out


def test_replaced_public_file_fails_before_the_catalog(published, capsys):
    published["versions"][f"{BASE}/comments/agency/agency_code=EPA/part-0.parquet"]["etag"] = '"stale"'
    assert freshness.main() == 1
    assert "agency_code=EPA" in capsys.readouterr().out
    assert published["verified"] == []


def test_missing_public_file_fails(published):
    del published["versions"][f"{BASE}/comments_index.parquet"]
    assert freshness.main() == 1


@pytest.mark.parametrize("receipt", [
    None,
    publication.PublicationError("Invalid comments publication receipt"),
    {"format_version": 1},
    _receipt(files={**_receipt()["files"], "comments.parquet": {**_receipt()["files"]["comments.parquet"], "rows": 3}}),
])
def test_absent_unreadable_or_incomplete_receipt_is_not_success(published, receipt, capsys):
    published["receipt"] = receipt
    assert freshness.main() == 1
    assert "FAIL" in capsys.readouterr().out


def test_generation_readers_see_must_be_the_receipts(published, capsys):
    """The website and MCP server read the index's generation, not the fixed files."""
    published["family"] = {**FAMILY, "artifactDigest": "sha256:" + "c" * 64}
    assert freshness.main() == 0
    assert published["verified"] == [(None, ["fixed", "generation"])]
    assert "generation differs from receipt" in capsys.readouterr().out
    published["catalog_errors"] = ["comments coverage EPA/EPA-1/2026-9: generation index=1, actual=2"]
    assert freshness.main() == 1


def test_generation_is_the_artifact_digest_not_the_whole_entry(published):
    """The index may restate a family's other fields (a new key or prefix spelling); the digest names the bytes."""
    published["family"] = {**FAMILY, "publishedAt": "2026-11-09T00:00:00Z"}
    assert freshness.main() == 0
    assert published["verified"] == []


def test_receipt_without_generation_or_pair_cannot_bind(published, capsys):
    published["receipt"] = _receipt(source={k: v for k, v in SOURCE.items() if k != "receipts"})
    del published["receipt"]["generation"]
    assert freshness.main() == 0
    out = capsys.readouterr().out
    assert "receipt names no generation" in out and "receipt predates pair snapshots" in out
    assert published["verified"] == [(None, ["fixed", "generation"])]


def test_comments_table_commit_fails_even_when_counts_agree(published, capsys):
    """A text-only writer (fill-comment-fields, enrich_pdf, ...) keeps IDs and counts; the commit alone fails."""
    published["bound"] = False
    assert freshness.main() == 1
    out = capsys.readouterr().out
    assert "unpublished catalog commits" in out
    assert published["verified"] == [(None, ["fixed", "generation"])]  # the counts still run as the diagnostic
    published["catalog_errors"] = ["comments coverage EPA/EPA-1/2026-9: fixed index=1, actual=2"]
    assert freshness.main() == 1
    assert "fixed index=1, actual=2" in capsys.readouterr().out


def test_a_shared_receipt_table_write_keeps_the_bound_path(published, capsys):
    """Comments publish no receipts (ss-juz8): a receipt-only commit leaves the publication current, as the mirror
    judges it, so no row is read (it cost ~95 s per night on 2026-11-09)."""
    published["receipts_bound"] = False
    published["catalog_errors"] = ["would only show if the rows were read"]
    assert freshness.main() == 0
    out = capsys.readouterr().out
    assert "OK: generation" in out and "etl_receipts" not in out
    assert published["verified"] == []


def test_declared_schema_drift_fails_the_bound_path(published, monkeypatch, capsys):
    def refuse(rt, snapshot, **options):
        assert options == {"declared_schema": True}
        raise ValueError("Native catalog schema differs from declared policy: missing ['subtype']")

    monkeypatch.setattr(iceberg, "compacted_since", refuse)
    assert freshness.main() == 1
    assert "missing ['subtype']" in capsys.readouterr().out


def test_agency_scope_forces_its_content_check(published, monkeypatch):
    monkeypatch.setattr("sys.argv", ["check", "--agency", "PHMSA"])
    assert freshness.main() == 0
    assert published["verified"] == [("PHMSA", ["fixed", "generation"])]


def test_public_surface_skips_generation_and_catalog(published, monkeypatch):
    published["bound"] = False
    monkeypatch.setattr("sys.argv", ["check", "--surface", "public"])
    assert freshness.main() == 0
    assert published["verified"] == []


def test_one_throttled_head_is_retried(monkeypatch):
    import httpx
    answers = [429, 200]

    def head(url, **options):
        assert options["headers"]["Cache-Control"] == "no-cache"
        return httpx.Response(answers.pop(0), headers={"etag": '"e"', "content-length": "7"},
                              request=httpx.Request("HEAD", url))

    monkeypatch.setattr(publication.httpx, "head", head)
    monkeypatch.setattr(publication.time, "sleep", lambda _: None)
    assert publication.public_version_drift({f"{BASE}/x": {"etag": '"e"', "bytes": 7}}, attempts=2) == []


def test_mcp_image_checks_comment_pins_without_boto3_or_spicy_docs(tmp_path):
    """The Cloudflare MCP image installs spicy_regs ``--no-deps``: neither boto3 nor SpicyDocs is importable there."""
    import subprocess
    import sys
    probe = """
import sys
for name in ("boto3", "botocore", "spicy_docs"):
    sys.modules[name] = None
import httpx
from spicy_regs.sources import publication
httpx.head = lambda url, **kw: httpx.Response(200, headers={"etag": '"e"', "content-length": "7"},
                                              request=httpx.Request("HEAD", url))
pin = {"kind": "comments-mirror", "urls": ["https://data.example/comments.parquet"], "etag": '"e"', "bytes": 7}
assert publication.mutable_versions_match({"comments": pin}) is True
assert publication.mutable_versions_match({"comments": {**pin, "etag": '"x"'}}) is False
"""
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, cwd=tmp_path)
    assert result.returncode == 0, result.stderr


def test_receipt_helpers_bind_only_complete_receipts():
    assert receipt_files(_receipt()) is not None
    assert receipt_source(_receipt()) == iceberg.CatalogPairSnapshot("t", 7, 0, iceberg.CatalogSnapshot("r", 9, 0))
    assert receipt_source({"source": {"table_uuid": "t", "snapshot_id": 7, "schema_id": 0}}) is None
    assert receipt_files(_receipt(files={"comments.parquet": _receipt()["files"]["comments.parquet"]})) is None


def test_one_schema_rule_names_the_missing_column(monkeypatch):
    """The 2026-10-30 refusal, by the DESCRIBE rule both the initialization check and the bound path apply."""
    rt = RECORD_TYPES["comments"]
    con = duckdb.connect()
    con.execute(f"CREATE TABLE comments ({regulatory_catalog._ddl(regulatory_catalog.policy('comments').subject_schema)})")
    monkeypatch.setattr(regulatory_catalog, "qualified", lambda _: "comments")
    assert regulatory_catalog.schema_difference(con, rt) is None
    con.execute("ALTER TABLE comments DROP COLUMN text_extraction_status")
    difference = regulatory_catalog.schema_difference(con, rt)
    assert difference is not None and "missing ['text_extraction_status']" in difference


def test_catalog_counts_must_equal_every_index(tmp_path):
    """A stale generation index fails even when the fixed index agrees; a duplicate ID also fails."""
    for name, rows in (("fixed", 2), ("generation", 1)):
        duckdb.sql(f"COPY (SELECT 'EPA' agency_code, 'EPA-1' docket_id, 2026::BIGINT AS year, 9::BIGINT AS month, "
                   f"{rows}::BIGINT AS row_count) TO '{tmp_path / name}.parquet'")
    con = duckdb.connect()
    con.execute("CREATE TABLE comments AS SELECT * FROM (VALUES ('a', 'EPA', 'EPA-1', '2026-09-01'), "
                "('b', 'EPA', 'EPA-1', '2026-09-02')) t(comment_id, agency_code, docket_id, posted_date)")
    indexes = {name: f"SELECT * FROM read_parquet('{tmp_path / name}.parquet')" for name in ("fixed", "generation")}
    assert check_comments(con, "SELECT * FROM comments", indexes) == [
        "comments coverage EPA/EPA-1/2026-9: generation index=1, actual=2"]
    con.execute("UPDATE comments SET comment_id = 'a'")
    assert any("unique IDs" in error for error in check_comments(con, "SELECT * FROM comments", indexes))
