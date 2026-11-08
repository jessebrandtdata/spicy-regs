"""Source-null docket links survive ingestion, retries, counts and publication."""

from hashlib import sha256
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from tests.regulatory_publication_fakes import install as install_publication

from spicy_regs.comments_health import check_comments
from spicy_regs.pipelines import regulations
from spicy_regs.schemas import COMMENT, DOCKET
from spicy_regs.sources import iceberg
from spicy_regs.transforms.regulations_shape import shape_record
from spicy_regs.transforms.build_agency_stats import build_agency_stats
from spicy_regs.transforms.build_feed_summary import build_feed_summary
from tests.test_regulations_pipeline import _FakeS3Resource


FIXTURES = Path(__file__).parent / "fixtures/comments_null_dockets"


def reviewed_sources():
    store = {}
    rows = []
    for receipt in json.loads((FIXTURES / "source-receipts.json").read_text()):
        raw = (FIXTURES / receipt["retained_file"]).read_bytes()
        assert sha256(raw).hexdigest() == receipt["sha256"]
        store[receipt["source"].removeprefix("s3://mirrulations/")] = raw
        rows.append(COMMENT.extract(json.loads(raw)))
    return store, rows


@pytest.mark.parametrize("mode", ["catalog", "chunked"])
def test_reviewed_comments_survive_ingestion_and_public_mirror(tmp_path, monkeypatch, mode):
    store, expected = reviewed_sources()
    assert all(row["docket_id"] is None for row in expected)
    monkeypatch.setattr(regulations.mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    monkeypatch.setattr(regulations.r2, "download", lambda *args, **kwargs: False)
    # A catalog run keeps its originals in R2 even with --skip-upload.
    install_publication(monkeypatch)
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH '{tmp_path / 'catalog.duckdb'}' AS {iceberg._CATALOG_ALIAS}")
        iceberg._ensure_table(con, rt)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)
    output = tmp_path / "output"
    for _ in range(2):
        regulations.RegulationsPipeline(
            output_dir=output,
            agency="ODNI",
            only_comments=True,
            allow_fresh_start=True,
            use_iceberg=True,
            chunk_size=1 if mode == "chunked" else 0,
            enrich_text=False,
            skip_upload=True,
        ).run()
        assert set(pl.read_parquet(output / "manifest.parquet")["key"]) == set(store)
        assert not (output / "comments_index.parquet").exists()

    by_id = sorted(expected, key=lambda row: row["comment_id"])
    with connect(COMMENT) as con:
        got = con.execute(f"SELECT * FROM {iceberg._qualified(COMMENT)}").pl()
        # The catalog stores every column as VARCHAR, a stated count as its decimal text.
        stored = [shape_record("comments", row) for row in by_id]
        from spicy_regs.transforms.regulations_receipts import policy

        stored = [{k: row[k] for k in policy("comments").subject_schema.names} for row in stored]
        assert got.sort("comment_id").to_dicts() == stored
    monkeypatch.setattr(iceberg, "_connect", lambda: connect(COMMENT))
    monkeypatch.setattr(iceberg, "_read_pair_snapshot", lambda *a: iceberg.CatalogPairSnapshot(
        "local", 1, 0, receipts=iceberg.CatalogSnapshot("local-receipts", 1, 0)))
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, _: f"SELECT * FROM {iceberg._qualified(rt)}")
    result = iceberg.export_public_comments(output, COMMENT)
    index = pl.read_parquet(result["index"])
    assert index["row_count"].sum() == len(expected)
    assert index["docket_id"].null_count() == len(index)

    # Exercise the same per-agency files and count check used by publication.
    directory = result["partitions"]
    with duckdb.connect() as con:
        mirrored = f"SELECT * FROM read_parquet('{directory}/agency_code=*/part-0.parquet', hive_partitioning=true)"
        index_sql = f"SELECT * FROM read_parquet('{output / 'comments_index.parquet'}')"
        assert check_comments(con, mirrored, index_sql) == []
        # The mirror publishes the extract's values, the count typed again.
        assert con.execute(mirrored).pl().select(got.columns).sort("comment_id").to_dicts() == stored

    # An agency with no docket records still gets its comments counted. A
    # docket-only feed cannot manufacture docket rows for these comments.
    pl.DataFrame(schema=DOCKET.schema).write_parquet(output / "dockets.parquet")
    assert pl.read_parquet(build_agency_stats(output)).to_dicts() == [
        {"agency_code": "ODNI", "docket_count": 0, "document_count": 0, "comment_count": len(expected)}
    ]
    assert pl.read_parquet(build_feed_summary(output)).is_empty()
