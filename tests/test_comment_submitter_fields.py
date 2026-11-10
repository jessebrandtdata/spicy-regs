"""The agency's submitter class and campaign count survive ingestion, the catalog and the public mirror."""

import hashlib
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.regulations import COMMENT as SOURCE_COMMENT

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.transforms.regulations_receipts import policy
from spicy_regs.transforms.write_staging import write_staging


FIXTURES = Path(__file__).parent / "fixtures/comments_submitter_fields"
#: comment id -> (subtype, duplicate_comments) as the retained record states them.
STATED = {
    "EPA-HQ-OW-2022-0114-1811": ("Mass Mail Campaign", 15851),
    "EPA-HQ-OW-2022-0114-0017": ("Company/Organization Comment", 1),
    "EPA-HQ-OW-2022-0114-0002": ("Public Comment", 1),
    "CMS-2016-0123-0993": ("Public Comment", 0),
}
SHA256 = {
    "EPA-HQ-OW-2022-0114-1811": "dccedb9f286dfca158b5a2ae6d0a77cda946b7d1fd884098e36a2444391b0261",
    "EPA-HQ-OW-2022-0114-0017": "895d3528bd3bdaa12cd1e43ff9401c8f388724e0a7fa338d8454015fedd43127",
    "EPA-HQ-OW-2022-0114-0002": "69ec303bb31cac609d99c9619109059c95a8d5e4133e125f1826e7d0b23829af",
    "CMS-2016-0123-0993": "8d6e7d9e6dcbcd02d7c597690836175eb0ace9a5402ac8323df7c65f53961630",
}


def _raw(comment_id: str) -> dict:
    body = (FIXTURES / f"{comment_id}.source.json").read_bytes()
    assert hashlib.sha256(body).hexdigest() == SHA256[comment_id]
    return json.loads(body)


@pytest.mark.parametrize("comment_id", STATED)
def test_extract_carries_subtype_and_count_as_stated(comment_id):
    row = COMMENT.extract(_raw(comment_id))
    assert (row["subtype"], row["duplicate_comments"]) == STATED[comment_id]
    assert type(row["duplicate_comments"]) is int
    assert tuple(row) == tuple(COMMENT.schema)


@pytest.mark.parametrize("comment_id", STATED)
def test_etl_and_source_repair_extract_the_same_row(comment_id):
    """The repair reads through SpicyDocs' extract; one extract means a repair never clears what the ETL filled."""
    raw = _raw(comment_id)
    assert COMMENT.extract(raw) == {**SOURCE_COMMENT.extract(raw), "pdf_extraction_results_json": None}


def test_source_extract_declares_submitter_types_in_table_order():
    """The source reader and source row schema agree before native conversion."""
    assert tuple(COMMENT.schema) == TABLE_CONTRACTS["comments"].columns
    assert tuple(COMMENT.schema)[-2:] == ("subtype", "duplicate_comments")


def test_organization_is_classified_in_subtype_not_organization():
    row = COMMENT.extract(_raw("EPA-HQ-OW-2022-0114-0017"))
    assert row["organization"] is None and row["subtype"] == "Company/Organization Comment"


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    """A local DuckDB file attached as the catalog, reopened like the connector reopens R2."""
    path = tmp_path / "catalog.duckdb"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{path}' AS {iceberg._CATALOG_ALIAS}")
        return con

    monkeypatch.setattr(iceberg, "_connect", connect)
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "default")
    return connect


def _stage(tmp_path: Path, rows: list[dict]) -> Path:
    staging = tmp_path / "staging"
    write_staging("EPA", COMMENT.name, rows, staging, COMMENT.schema)
    return staging


def test_ingested_values_reach_the_mirror_typed_with_null_and_zero_distinct(tmp_path, catalog, monkeypatch):
    rows = [COMMENT.extract(_raw(comment_id)) for comment_id in STATED]
    unread = {**rows[-1], "comment_id": "CMS-2016-0123-9999", "subtype": None, "duplicate_comments": None}
    iceberg.merge_comments(_stage(tmp_path, [*rows, unread]), COMMENT)

    with catalog() as con:
        # The business catalog keeps the campaign count as a native integer.
        types = iceberg._column_types(con, COMMENT)
        assert types["subtype"] == "VARCHAR" and types["duplicate_comments"] == "INTEGER"
        stored = dict(
            con.execute(f"SELECT comment_id, duplicate_comments FROM {iceberg._qualified(COMMENT)}").fetchall()
        )
        assert stored["EPA-HQ-OW-2022-0114-1811"] == 15851 and stored["CMS-2016-0123-9999"] is None

    monkeypatch.setattr(
        iceberg, "_read_pair_snapshot",
        lambda *_, **kw: iceberg.CatalogPairSnapshot("local", 1, 1, iceberg.CatalogSnapshot("receipts", 2, 1)),
    )
    result = iceberg.export_public_comments(tmp_path / "out", COMMENT)
    assert pl.read_parquet(result["comments"]).columns == policy("comments").subject_schema.names
    with duckdb.connect() as con:
        described = dict(
            con.execute(f"SELECT column_name, column_type FROM (DESCRIBE '{result['comments']}')").fetchall()
        )
        assert described["duplicate_comments"] == "INTEGER" and described["subtype"] == "VARCHAR"
        published = {
            row[0]: row[1:]
            for row in con.execute(
                f"SELECT comment_id, subtype, duplicate_comments FROM '{result['comments']}'"
            ).fetchall()
        }
    assert published == {**STATED, "CMS-2016-0123-9999": (None, None)}


def test_a_malformed_count_refuses_the_etl_extract_rather_than_being_coerced():
    """SpicyDocs never coerces duplicateComments; the ETL's extract refuses, so no coerced row is staged."""
    raw = _raw("CMS-2016-0123-0993")
    for stated in ("5", True, 1.5, -1, 2**31):
        raw["data"]["attributes"]["duplicateComments"] = stated
        with pytest.raises(ValueError, match="duplicateComments"):
            COMMENT.extract(raw)
