"""The nightly check binds the public mirror and catalog to the publisher's receipt and verifies only what moved."""

import importlib.util
from pathlib import Path

import duckdb
import pytest

from spicy_regs.pipelines.comments_mirror import receipt_files, receipt_source
from spicy_regs.sources import iceberg

_SPEC = importlib.util.spec_from_file_location(
    "check_comments_freshness", Path(__file__).parents[1] / "scripts/check_comments_freshness.py")
assert _SPEC and _SPEC.loader
freshness = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(freshness)

SHA = "a" * 64
SOURCE = {"table_uuid": "t", "snapshot_id": 7, "schema_id": 0,
          "receipts": {"table_uuid": "r", "snapshot_id": 9, "schema_id": 0}}


def _receipt(**changes):
    files = {
        "comments.parquet": {"etag": '"m"', "bytes": 10, "rows": 2_000_000, "sha256": SHA},
        "comments_index.parquet": {"etag": '"i"', "bytes": 5, "rows": 3, "sha256": SHA},
        "comments/agency/agency_code=EPA/part-0.parquet": {"etag": '"e"', "bytes": 7, "rows": 2_000_000, "sha256": SHA},
    }
    return {"format_version": 1, "source": SOURCE, "files": files, **changes}


@pytest.fixture
def published(monkeypatch):
    """Public objects as the receipt states them, a bound catalog, and a recorder for content checks."""
    receipt = _receipt()
    versions = {f"https://data.example/{key}": {"etag": item["etag"], "bytes": item["bytes"]}
                for key, item in receipt["files"].items()}
    state: dict = {"receipt": receipt, "versions": versions, "bound": True, "catalog_errors": [], "verified": []}
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://data.example")
    monkeypatch.setattr("sys.argv", ["check"])

    def read_receipt(base):
        if isinstance(state["receipt"], Exception):
            raise state["receipt"]
        return state["receipt"]

    def verify_catalog(base, agency, *, limit):
        state["verified"].append(agency)
        return state["catalog_errors"]

    monkeypatch.setattr(freshness, "read_receipt", read_receipt)
    monkeypatch.setattr(freshness.r2, "public_object_version", lambda url: state["versions"].get(url))
    monkeypatch.setattr(iceberg, "rows_unchanged_since", lambda rt, snapshot: state["bound"])
    monkeypatch.setattr(freshness, "verify_catalog", verify_catalog)
    return state


def test_unchanged_publication_and_compacted_catalog_read_no_rows(published, capsys):
    assert freshness.main() == 0
    assert published["verified"] == []
    assert "unchanged since published snapshot 7" in capsys.readouterr().out


def test_replaced_public_file_fails_before_the_catalog(published, capsys):
    published["versions"]["https://data.example/comments/agency/agency_code=EPA/part-0.parquet"]["etag"] = '"stale"'
    assert freshness.main() == 1
    assert "agency_code=EPA" in capsys.readouterr().out
    assert published["verified"] == []


def test_missing_public_file_fails(published):
    del published["versions"]["https://data.example/comments_index.parquet"]
    assert freshness.main() == 1


@pytest.mark.parametrize("receipt", [
    ValueError("not JSON"),
    {"format_version": 1},
    _receipt(files={**_receipt()["files"], "comments.parquet": {**_receipt()["files"]["comments.parquet"], "rows": 3}}),
])
def test_unreadable_or_invalid_receipt_is_not_success(published, receipt, capsys):
    published["receipt"] = receipt
    assert freshness.main() == 1
    assert "FAIL" in capsys.readouterr().out


def test_changed_catalog_is_verified_and_its_mismatch_fails(published, capsys):
    published["bound"] = False
    assert freshness.main() == 0
    assert published["verified"] == [None]
    published["catalog_errors"] = ["comments coverage EPA/EPA-1/2026-9: index=1, actual=2"]
    assert freshness.main() == 1
    assert "catalog changed since publication" in capsys.readouterr().out


def test_receipt_without_pair_snapshot_cannot_bind(published, capsys):
    published["receipt"] = _receipt(source={k: v for k, v in SOURCE.items() if k != "receipts"})
    assert freshness.main() == 0
    assert published["verified"] == [None]
    assert "receipt predates pair snapshots" in capsys.readouterr().out


def test_agency_scope_forces_its_content_check(published, monkeypatch):
    monkeypatch.setattr("sys.argv", ["check", "--agency", "PHMSA"])
    assert freshness.main() == 0
    assert published["verified"] == ["PHMSA"]


def test_receipt_helpers_bind_only_complete_receipts():
    assert receipt_files(_receipt()) is not None
    assert receipt_source(_receipt()) == iceberg.CatalogPairSnapshot("t", 7, 0, iceberg.CatalogSnapshot("r", 9, 0))
    assert receipt_source({"source": {"table_uuid": "t", "snapshot_id": 7, "schema_id": 0}}) is None
    assert receipt_files(_receipt(files={"comments.parquet": _receipt()["files"]["comments.parquet"]})) is None


def test_catalog_content_check_catches_a_stale_publication(tmp_path, monkeypatch):
    """The catalog gained a row the published index lacks; a duplicate ID also fails."""
    duckdb.sql("COPY (SELECT 'EPA' agency_code, 'EPA-1' docket_id, 2026::BIGINT AS year, 9::BIGINT AS month, "
               "1::BIGINT AS row_count) TO '" + str(tmp_path / "comments_index.parquet") + "'")
    catalog = duckdb.connect()
    catalog.execute("CREATE TABLE comments AS SELECT * FROM (VALUES ('a', 'EPA', 'EPA-1', '2026-09-01')) "
                    "t(comment_id, agency_code, docket_id, posted_date)")
    monkeypatch.setattr(iceberg, "_connect", lambda: catalog.cursor())
    monkeypatch.setattr(iceberg, "_qualified", lambda rt: "comments")
    monkeypatch.setattr(freshness, "load_public_http", lambda con: None)
    monkeypatch.setattr(freshness, "require_initialized", lambda con, rt: None)
    assert freshness.verify_catalog(str(tmp_path), None, limit=5) == []
    catalog.execute("INSERT INTO comments VALUES ('b', 'EPA', 'EPA-1', '2026-09-02')")
    assert freshness.verify_catalog(str(tmp_path), None, limit=5) == [
        "comments coverage EPA/EPA-1/2026-9: index=1, actual=2"]
    catalog.execute("UPDATE comments SET comment_id = 'a'")
    assert any("unique IDs" in error for error in freshness.verify_catalog(str(tmp_path), None, limit=5))


def test_public_surface_skips_the_catalog(published, monkeypatch):
    published["bound"] = False
    monkeypatch.setattr("sys.argv", ["check", "--surface", "public"])
    assert freshness.main() == 0
    assert published["verified"] == []
