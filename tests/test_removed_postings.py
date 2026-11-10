"""A posting Regulations.gov removed stays held, and the derived counts leave it and its comments out.

FNA's rows on 2026-10-03 (documents generation 066964e8, the per-agency comments file of the same day):
FNA-2026-0301-0004, the Utah notice filed to the Idaho docket, answers 404, and its four comments (-0006 to -0009)
were re-posted on FNA-2026-0313-0006 as -0002 to -0005, body for body. FNA-2026-0301 holds seven comments in all.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.transforms.build_agency_monthly_volume import build_agency_monthly_volume
from spicy_regs.transforms.build_agency_stats import build_agency_stats
from spicy_regs.transforms.build_discovery_signals import build_discovery_signals
from spicy_regs.transforms.build_feed_summary import build_feed_summary

IDAHO, UTAH = "FNA-2026-0301", "FNA-2026-0313"
REMOVED = "FNA-2026-0301-0004"


def _documents(path: Path, *, status: bool) -> None:
    documents = RECORD_TYPES["documents"]
    held = [
        (f"{IDAHO}-0001", IDAHO, "Supporting & Related Material", "2026-09-08T04:00:00Z"),
        (REMOVED, IDAHO, "Notice", "2026-09-15T04:00:00Z"),
        (f"{IDAHO}-0005", IDAHO, "Notice", "2026-09-15T04:00:00Z"),
        (f"{UTAH}-0006", UTAH, "Notice", "2026-09-18T04:00:00Z"),
    ]
    rows = [
        {**dict.fromkeys(documents.schema), "document_id": d, "docket_id": k, "agency_code": "FNA",
         "document_type": kind, "posted_date": posted}
        for d, k, kind, posted in held
    ]
    frame = pl.DataFrame(rows, schema=documents.schema)
    if status:
        frame = frame.with_columns(
            publisher_status=pl.when(pl.col("document_id") == REMOVED).then(pl.lit("removed")).otherwise(
                pl.lit("listed")),
            removed_observed_at=pl.when(pl.col("document_id") == REMOVED).then(pl.lit("2026-10-03T12:40:00+00:00")),
        )
    frame.write_parquet(path / "documents.parquet")


def _inputs(path: Path, *, status: bool = True) -> None:
    dockets = RECORD_TYPES["dockets"]
    pl.DataFrame(
        [{**dict.fromkeys(dockets.schema), "docket_id": k, "agency_code": "FNA"} for k in (IDAHO, UTAH)],
        schema=dockets.schema,
    ).write_parquet(path / "dockets.parquet")
    _documents(path, status=status)
    comments = [(f"{IDAHO}-{n:04d}", IDAHO, REMOVED) for n in range(6, 10)]
    comments += [(f"{IDAHO}-{n:04d}", IDAHO, f"{IDAHO}-0005") for n in range(10, 13)]
    comments += [(f"{UTAH}-{n:04d}", UTAH, f"{UTAH}-0006") for n in range(2, 6)]
    pl.DataFrame(
        [{"comment_id": c, "docket_id": k, "agency_code": "FNA", "comment_on_document_id": d,
          "posted_date": "2026-09-18T04:00:00Z"} for c, k, d in comments]
    ).write_parquet(path / "comments.parquet")
    pl.DataFrame(
        {"agency_code": ["FNA", "FNA"], "docket_id": [IDAHO, UTAH], "year": [2026, 2026], "month": [9, 9],
         "row_count": [7, 4]}
    ).write_parquet(path / "comments_index.parquet")


def test_the_derived_counts_leave_out_a_removed_posting_and_its_comments(tmp_path):
    _inputs(tmp_path)
    stats = pl.read_parquet(build_agency_stats(tmp_path)).to_dicts()
    assert stats == [{"agency_code": "FNA", "docket_count": 2, "document_count": 3, "comment_count": 7}]
    feed = dict(pl.read_parquet(build_feed_summary(tmp_path)).select("docket_id", "comment_count").iter_rows())
    assert feed == {IDAHO: 3, UTAH: 4}
    volume = pl.read_parquet(build_agency_monthly_volume(tmp_path))
    assert dict(volume.select("document_type", "document_count").iter_rows()) == {
        "Supporting & Related Material": 1, "Notice": 2}


def test_documents_never_reconciled_count_as_before_and_read_no_comments(tmp_path, monkeypatch):
    """Before the first reconcile publishes, nothing is removed, so no comment file is read."""
    _inputs(tmp_path, status=False)
    (tmp_path / "comments.parquet").unlink()
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    assert pl.read_parquet(build_agency_stats(tmp_path))["comment_count"].to_list() == [11]
    feed = dict(pl.read_parquet(build_feed_summary(tmp_path)).select("docket_id", "comment_count").iter_rows())
    assert feed == {IDAHO: 7, UTAH: 4}
    assert pl.read_parquet(build_agency_monthly_volume(tmp_path))["document_count"].sum() == 4


@pytest.mark.parametrize("builder", [build_agency_stats, build_feed_summary])
def test_a_removed_posting_with_no_held_comments_subtracts_nothing(tmp_path, builder):
    _inputs(tmp_path)
    pl.read_parquet(tmp_path / "comments.parquet").filter(
        pl.col("comment_on_document_id") != REMOVED).write_parquet(tmp_path / "comments.parquet")
    assert pl.read_parquet(builder(tmp_path))["comment_count"].sum() == 11


def _spike_documents(path: Path) -> pl.DataFrame:
    """One agency's documents: 24 over the prior year (a monthly mean of 2), then 4 in the last 30 days."""
    now = datetime.now(UTC)
    documents = RECORD_TYPES["documents"]
    posted = [now - timedelta(days=60 + 14 * n) for n in range(24)] + [now - timedelta(days=5)] * 4
    rows = [
        {**dict.fromkeys(documents.schema), "document_id": f"FNA-2026-0001-{n:04d}", "agency_code": "FNA",
         "posted_date": day.strftime("%Y-%m-%dT%H:%M:%SZ")}
        for n, day in enumerate(posted)
    ]
    frame = pl.DataFrame(rows, schema=documents.schema)
    frame.write_parquet(path / "documents.parquet")
    return frame


def test_a_removed_posting_is_no_part_of_its_agencys_output_spike(tmp_path):
    """Spike: 30 days' output at least twice the prior year's monthly mean, over at least 24 documents a year."""
    frame = _spike_documents(tmp_path)
    assert pl.read_parquet(build_discovery_signals(tmp_path))["recent_30d"].to_list() == [4]
    frame.with_columns(
        publisher_status=pl.when(pl.col("document_id") == "FNA-2026-0001-0027").then(pl.lit("removed")),
        removed_observed_at=pl.lit(None, pl.Utf8),
    ).write_parquet(tmp_path / "documents.parquet")
    assert pl.read_parquet(build_discovery_signals(tmp_path)).is_empty(), "3 is under twice the monthly mean of 2"


def test_only_the_removed_postings_agencies_are_read(tmp_path):
    """Two removed postings of one agency still read only that agency's rows, so a comment filed under another
    agency is not matched (the stated limit: 11 such comments on 2 documents on 2026-10-03)."""
    import duckdb

    from spicy_regs.transforms.removed_postings import removed_comments

    _inputs(tmp_path)
    documents = pl.read_parquet(tmp_path / "documents.parquet")
    documents.with_columns(
        publisher_status=pl.when(pl.col("document_id").is_in([REMOVED, f"{IDAHO}-0005"])).then(pl.lit("removed"))
    ).write_parquet(tmp_path / "documents.parquet")
    comments = pl.read_parquet(tmp_path / "comments.parquet")
    elsewhere = comments.head(1).with_columns(comment_id=pl.lit("HUD-2026-0001-0001"), agency_code=pl.lit("HUD"))
    pl.concat([comments, elsewhere]).write_parquet(tmp_path / "comments.parquet")
    con = duckdb.connect()
    removed_comments(con, tmp_path / "documents.parquet", tmp_path)
    assert con.sql("SELECT agency_code, docket_id, n FROM removed_comments").fetchall() == [("FNA", IDAHO, 7)]
