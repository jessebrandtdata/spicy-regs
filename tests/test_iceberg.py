"""Native regulatory catalog merge, rollback, paired export, and text updates.

A local DuckDB catalog exercises actual subject and receipt writes. REST-engine
transactions remain covered separately by catalog integration tests.
"""

from pathlib import Path

import duckdb
import polars as pl
import pytest

from spicy_regs.schemas import COMMENT, DOCKET
from spicy_regs.sources import iceberg
from spicy_regs.sources import regulatory_catalog as native

def _source(con, record_type, rows, name='incoming'):
    full = [{c: row.get(c) for c in record_type.schema} for row in rows]
    con.register('_test_source', pl.DataFrame(full, schema=record_type.schema).to_arrow())
    try:
        con.execute(f'CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM _test_source')
    finally:
        con.unregister('_test_source')
    return name


def _put(con, record_type, rows):
    iceberg.replace_rows(con, record_type, _source(con, record_type, rows))


def _fresh(con, record_type, *, where='TRUE', name='fresh'):
    prior = native.processing_table(con, record_type)
    con.execute(f'CREATE TEMP TABLE {name} AS SELECT * FROM {prior} WHERE {where}')
    con.execute(f'DROP TABLE {prior}')
    return name


def _write_staging(staging_dir: Path, agency: str, rows: list[dict]) -> None:
    """Mimic transforms.write_staging: staging_dir/dockets/{agency}.parquet."""
    type_dir = staging_dir / DOCKET.name
    type_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=DOCKET.schema).write_parquet(type_dir / f"{agency}.parquet")


def _write_comment_staging(staging_dir: Path, agency: str, rows: list[dict]) -> None:
    """Mimic transforms.write_staging: staging_dir/comments/{agency}.parquet."""
    type_dir = staging_dir / COMMENT.name
    type_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=COMMENT.schema).write_parquet(type_dir / f"{agency}.parquet")


def _comment(comment_id: str, docket_id: str, agency: str, posted_date: str, modify_date: str | None = None) -> dict:
    row = {col: None for col in COMMENT.schema}
    row.update(
        comment_id=comment_id,
        docket_id=docket_id,
        agency_code=agency,
        posted_date=posted_date,
        modify_date=modify_date or posted_date,
    )
    return row


@pytest.fixture
def local_catalog():
    """A local DuckDB standing in for the attached R2 catalog (alias reg_catalog)."""
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        yield con
    finally:
        con.close()


def _docket(docket_id: str, agency: str, title: str, modify_date: str) -> dict:
    row = {col: None for col in DOCKET.schema}
    row.update(docket_id=docket_id, agency_code=agency, title=title, modify_date=modify_date)
    return row


def test_is_configured(monkeypatch) -> None:
    for var in iceberg._REQUIRED_ENV:
        monkeypatch.delenv(var, raising=False)
    assert iceberg.is_configured() is False

    for var in iceberg._REQUIRED_ENV:
        monkeypatch.setenv(var, "x")
    assert iceberg.is_configured() is True


def test_namespace_empty_defaults(monkeypatch) -> None:
    # Unset -> default; explicitly empty (e.g. an unset GH secret) -> default too.
    monkeypatch.delenv("R2_CATALOG_NAMESPACE", raising=False)
    assert iceberg._namespace() == "default"
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "")
    assert iceberg._namespace() == "default"
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "custom")
    assert iceberg._namespace() == "custom"


def test_connect_raises_when_unconfigured(monkeypatch) -> None:
    for var in iceberg._REQUIRED_ENV:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(RuntimeError, match="R2_CATALOG_URI"):
        iceberg._connect()


def test_merge_inserts_then_dedups(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)

    # First batch: two distinct dockets.
    staging = tmp_path / "staging"
    _write_staging(
        staging,
        "EPA",
        [
            _docket("EPA-1", "EPA", "First", "2025-01-01"),
            _docket("EPA-2", "EPA", "Second", "2025-01-02"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)
    assert con.execute(f"SELECT count(*) FROM {iceberg._qualified(DOCKET)}").fetchone()[0] == 2

    # Second batch: one updated row (newer modify_date) + one brand-new row.
    # An older copy of EPA-1 in the same batch must lose to the newer one.
    staging2 = tmp_path / "staging2"
    _write_staging(
        staging2,
        "EPA",
        [
            _docket("EPA-1", "EPA", "First UPDATED", "2025-02-01"),
            _docket("EPA-1", "EPA", "stale dup", "2024-12-31"),
            _docket("EPA-3", "EPA", "Third", "2025-02-02"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging2, DOCKET), DOCKET)

    rows = dict(con.execute(f"SELECT docket_id, title FROM {iceberg._qualified(DOCKET)} ORDER BY docket_id").fetchall())
    assert rows == {"EPA-1": "First UPDATED", "EPA-2": "Second", "EPA-3": "Third"}


def test_merge_keeps_existing_when_incoming_is_older(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)

    staging = tmp_path / "staging"
    _write_staging(staging, "EPA", [_docket("EPA-1", "EPA", "Newer", "2025-05-01")])
    iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)

    # An older row for the same key must NOT overwrite the newer one.
    staging2 = tmp_path / "staging2"
    _write_staging(staging2, "EPA", [_docket("EPA-1", "EPA", "Older", "2025-01-01")])
    iceberg._merge(con, iceberg._staging_files(staging2, DOCKET), DOCKET)

    title = con.execute(f"SELECT title FROM {iceberg._qualified(DOCKET)} WHERE docket_id = 'EPA-1'").fetchone()[0]
    assert title == "Newer"


def test_replacement_failure_after_write_rolls_back(local_catalog) -> None:
    """A write or readback failure cannot leave a partially replaced catalog."""
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    table = iceberg._qualified(DOCKET)
    _put(con, DOCKET, [dict(docket_id='D', title='old')])
    _fresh(con, DOCKET)
    con.execute("UPDATE fresh SET title='new'")
    prior_receipts = con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall()

    class FailingCon:
        def execute(self, sql, *args, **kwargs):
            result = con.execute(sql, *args, **kwargs)
            if sql.startswith("MERGE INTO"):
                raise RuntimeError("readback failure after merge")
            return result

    with pytest.raises(RuntimeError, match="readback failure"):
        iceberg.replace_rows(FailingCon(), DOCKET, "fresh")
    assert con.execute(f"SELECT docket_id,title FROM {table}").fetchall() == [('D', 'old')]
    assert con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall() == prior_receipts
    iceberg.replace_rows(con, DOCKET, "fresh")
    assert con.execute(f"SELECT docket_id,title FROM {table}").fetchall() == [('D', 'new')]


def test_scoped_replacement_preserves_other_agency_and_refuses_collision(local_catalog):
    con = local_catalog
    _put(con, COMMENT, [dict(comment_id='c1', agency_code='EPA'),
                        dict(comment_id='c2', agency_code='FAA', text_content='kept')])
    _fresh(con, COMMENT, where="agency_code='EPA'", name='prior')
    con.execute("CREATE TEMP TABLE fresh AS SELECT * REPLACE ('filled' AS text_content) FROM prior")
    iceberg.replace_rows(con, COMMENT, 'fresh', expected_prior='prior', scope={'agency_code': 'EPA'})
    assert con.execute(f'SELECT agency_code,text_content FROM {iceberg._qualified(COMMENT)} ORDER BY 1').fetchall() == [
        ('EPA', 'filled'), ('FAA', 'kept')]
    with pytest.raises(ValueError, match='within scope'):
        iceberg.replace_rows(con, COMMENT, 'fresh', scope={'agency_code': 'FAA'})
    with pytest.raises(ValueError, match='unknown columns'):
        iceberg.replace_rows(con, COMMENT, 'fresh', scope={'agency': 'EPA'})
    con.execute("UPDATE fresh SET agency_code='FAA'")
    with pytest.raises(ValueError, match='another scope'):
        iceberg.replace_rows(con, COMMENT, 'fresh', scope={'agency_code': 'FAA'})


def test_unlocked_catalog_write_warns_once(local_catalog, monkeypatch) -> None:
    from loguru import logger

    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    _source(con, DOCKET, [dict(docket_id='D')], 'fresh_full')
    monkeypatch.setattr(iceberg, "_warned_unlocked", set())
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        monkeypatch.setenv(iceberg.CATALOG_LOCK_ENV, "comments-catalog-write")
        iceberg.replace_rows(con, DOCKET, "fresh_full")
        assert messages == []
        monkeypatch.delenv(iceberg.CATALOG_LOCK_ENV)
        iceberg.replace_rows(con, DOCKET, "fresh_full")
        iceberg.replace_rows(con, DOCKET, "fresh_full")
    finally:
        logger.remove(sink)
    assert len(messages) == 1
    assert "Catalog changed during export" in messages[0] and "comments-catalog-write" in messages[0]


def _metadata(*commits: tuple[int, int | None, str], uuid: str = "t", schema: int = 0) -> dict:
    """Iceberg table metadata whose current snapshot is the last of ``(id, parent, operation)``."""
    return {"table-uuid": uuid, "current-schema-id": schema, "current-snapshot-id": commits[-1][0],
            "snapshots": [{"snapshot-id": s, "parent-snapshot-id": p, "summary": {"operation": op}}
                          for s, p, op in commits]}


@pytest.mark.parametrize(("commits", "unchanged"), [
    ([(1, None, "append")], True),
    ([(1, None, "append"), (2, 1, "replace"), (3, 2, "replace")], True),
    ([(1, None, "append"), (2, 1, "replace"), (3, 2, "append")], False),
    ([(1, None, "append"), (2, 1, "overwrite")], False),
    ([(1, None, "append"), (2, None, "replace")], False),
], ids=["unmoved", "compaction", "write-after-compaction", "overwrite", "not-a-descendant"])
def test_only_compaction_may_move_an_exported_snapshot(commits, unchanged) -> None:
    assert iceberg._compacted_only(_metadata(*commits), iceberg.CatalogSnapshot("t", 1, 0)) is unchanged


def test_a_parent_cycle_is_not_bound() -> None:
    """Corrupt metadata whose parents loop ends the walk after one visit per snapshot instead of spinning."""
    metadata = _metadata((2, 3, "replace"), (3, 2, "replace"))
    assert not iceberg._compacted_only(metadata, iceberg.CatalogSnapshot("t", 1, 0))


@pytest.mark.parametrize("change", [{"uuid": "recreated"}, {"schema": 1}])
def test_a_recreated_table_or_new_schema_is_a_change(change) -> None:
    metadata = _metadata((1, None, "append"), (2, 1, "replace"), **change)
    assert not iceberg._compacted_only(metadata, iceberg.CatalogSnapshot("t", 1, 0))


@pytest.mark.parametrize("side", ["source", "prior"])
def test_replacement_refuses_duplicate_identities_before_writing(local_catalog, side) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    table = iceberg._qualified(DOCKET)
    _put(con, DOCKET, [dict(docket_id='D', title='old')])
    _fresh(con, DOCKET)
    con.execute("UPDATE fresh SET title='new'")
    duplicate_table = "fresh" if side == "source" else table
    con.execute(f"INSERT INTO {duplicate_table} SELECT * FROM {duplicate_table}")
    before = con.execute(f"SELECT * FROM {table}").fetchall()
    with pytest.raises(ValueError, match="identities|reused subject receipt"):
        iceberg.replace_rows(con, DOCKET, "fresh")
    assert con.execute(f"SELECT * FROM {table}").fetchall() == before


def test_export_parquet_matches_published_shape(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    staging = tmp_path / "staging"
    _write_staging(
        staging,
        "EPA",
        [
            _docket("EPA-2", "EPA", "b", "2025-01-02"),
            _docket("EPA-1", "EPA", "a", "2025-01-01"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)

    out = tmp_path / "output"
    out_file = iceberg._export_parquet(con, DOCKET, out)
    assert out_file == out / f"{DOCKET.name}.parquet"

    df = pl.read_parquet(out_file)
    # Same columns as the published schema, sorted by (agency_code, modify_date).
    assert df.columns == native.policy('dockets').subject_schema.names
    assert sorted(df["docket_id"].to_list()) == ["EPA-1", "EPA-2"]
    from spicy_regs.etl_receipts import validate_receipt_bundle
    import json
    pair = out / '.catalog-pairs/dockets'
    generation = json.loads((pair / 'generation.json').read_text())['generation_id']
    validate_receipt_bundle({'dockets': [out_file]}, [pair / 'etl_receipts.parquet'],
                           [native.policy('dockets')], generation_id=generation)




# --- comments path (catalog table + derived index) -------------------------


def test_build_comments_index_counts_per_partition(tmp_path, local_catalog) -> None:
    """The index derived from the catalog must hold one row per
    (agency, docket, year, month) with the right counts and schema."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)

    staging = tmp_path / "staging"
    _write_comment_staging(
        staging,
        "EPA",
        [
            # Two comments in the same partition (EPA, EPA-1, 2025-01).
            _comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z"),
            _comment("c2", "EPA-1", "EPA", "2025-01-20T00:00:00Z"),
            # A different month -> its own partition.
            _comment("c3", "EPA-1", "EPA", "2025-02-03T00:00:00Z"),
            # A different docket.
            _comment("c4", "EPA-2", "EPA", "2025-01-09T00:00:00Z"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging, COMMENT), COMMENT)

    out = tmp_path / "output"
    index_file = iceberg._build_comments_index(con, COMMENT, out)
    assert index_file == out / "comments_index.parquet"

    df = pl.read_parquet(index_file)
    assert set(df.columns) == {"agency_code", "docket_id", "year", "month", "row_count"}
    got = {(r["agency_code"], r["docket_id"], r["year"], r["month"]): r["row_count"] for r in df.iter_rows(named=True)}
    assert got == {
        ("EPA", "EPA-1", 2025, 1): 2,
        ("EPA", "EPA-1", 2025, 2): 1,
        ("EPA", "EPA-2", 2025, 1): 1,
    }


def test_build_comments_index_rebuilds_after_merge(tmp_path, local_catalog) -> None:
    """A second merge (new rows + a dedup'd update) must be reflected in a
    full index rebuild — the index always mirrors the current table."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    out = tmp_path / "output"

    s1 = tmp_path / "s1"
    _write_comment_staging(s1, "EPA", [_comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z")])
    iceberg._merge(con, iceberg._staging_files(s1, COMMENT), COMMENT)
    iceberg._build_comments_index(con, COMMENT, out)

    s2 = tmp_path / "s2"
    _write_comment_staging(
        s2,
        "EPA",
        [
            # Re-posted c1 (same id) must not inflate the count.
            _comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z", modify_date="2025-03-01T00:00:00Z"),
            _comment("c5", "EPA-1", "EPA", "2025-01-25T00:00:00Z"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(s2, COMMENT), COMMENT)
    index_file = iceberg._build_comments_index(con, COMMENT, out)

    df = pl.read_parquet(index_file)
    assert df.height == 1
    row = next(df.iter_rows(named=True))
    assert (row["agency_code"], row["docket_id"], row["year"], row["month"]) == ("EPA", "EPA-1", 2025, 1)
    assert row["row_count"] == 2  # c1 (deduped) + c5


def test_merge_comments_noop_without_staging(tmp_path) -> None:
    # No staged comments -> returns None and never touches the catalog.
    assert iceberg.merge_comments(tmp_path / "empty", COMMENT) == 0


@pytest.mark.parametrize("options", [{}, {"memory_limit": "64MB", "threads": 1}])
def test_export_public_comments_rebuilds_mirror(tmp_path, local_catalog, monkeypatch, options) -> None:
    """export_public_comments writes the public monolith + index straight from the
    catalog, and that monolith feeds partition_comments to produce the per-agency
    tree the UI reads."""
    from spicy_regs.transforms.partition_comments import partition_comments

    con = local_catalog
    iceberg._ensure_table(con, COMMENT)

    staging = tmp_path / "staging"
    _write_comment_staging(
        staging,
        "EPA",
        [
            _comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z"),
            _comment("c2", "EPA-2", "EPA", "2025-02-03T00:00:00Z"),
        ],
    )
    _write_comment_staging(
        staging,
        "OMB",
        [
            _comment("c3", "OMB-1", "OMB", "2026-06-29T00:00:00Z"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging, COMMENT), COMMENT)

    # export_public_comments opens its own catalog connection; point it at the
    # local in-memory one (it closes the connection when done — safe to double-close).
    monkeypatch.setattr(iceberg, "_connect", lambda: con)

    out = tmp_path / "output"
    monkeypatch.setattr(iceberg, "_read_pair_snapshot", lambda *_: iceberg.CatalogPairSnapshot("local", 1, 0, iceberg.CatalogSnapshot("receipts", 2, 0)))
    result = iceberg.export_public_comments(out, COMMENT, **options)

    assert result["comments"] == out / "comments.parquet"
    assert result["index"] == out / "comments_index.parquet"
    monolith = pl.read_parquet(result["comments"])
    assert monolith.height == 3
    assert set(monolith["agency_code"].to_list()) == {"EPA", "OMB"}
    from spicy_regs.etl_receipts import validate_receipt_bundle
    import json
    generation = json.loads(result['generation'].read_text())['generation_id']
    validate_receipt_bundle({'comments': [result['comments']]}, [result['receipts']],
                           [native.policy('comments')], generation_id=generation)

    # The monolith drives the coarse per-agency tree the UI reads for scoped queries.
    partition_dir = partition_comments(out)
    omb_part = partition_dir / "agency_code=OMB" / "part-0.parquet"
    assert omb_part.exists()
    assert pl.read_parquet(omb_part).height == 1


def test_upsert_comment_text_fills_in_place(tmp_path, local_catalog) -> None:
    """upsert_comment_text sets text_content + text_extraction_status for the
    matched rows, preserves other columns, never duplicates rows, and COALESCE
    keeps the existing text when _new_text is NULL."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)

    rows = [
        _comment("c1", "EPA-1", "EPA", "2025-01-01T00:00:00Z"),
        _comment("c2", "EPA-1", "EPA", "2025-01-02T00:00:00Z"),
        # c3 already has text; a NULL _new_text must not clobber it (COALESCE).
        _comment("c3", "EPA-1", "EPA", "2025-01-03T00:00:00Z"),
        # A different agency's row must be untouched.
        _comment("d1", "DOL-1", "DOL", "2025-02-01T00:00:00Z"),
    ]
    rows[2]["text_content"] = "existing text"
    rows[2]["text_extraction_status"] = "ok"
    _put(con, COMMENT, rows)

    updates = pl.DataFrame(
        {
            "comment_id": ["c1", "c2", "c3"],
            "_new_text": ["filled one", "filled two", None],
            "_new_status": ["ok", "ok", None],
        },
        schema={"comment_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8},
    )
    iceberg.upsert_comment_text(con, COMMENT, "EPA", updates)

    tbl = iceberg._qualified(COMMENT)
    # No row duplication (4 rows in, 4 rows out).
    assert con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0] == 4

    got = dict(con.execute(f"SELECT comment_id, text_content FROM {tbl} ORDER BY comment_id").fetchall())
    assert got == {
        "c1": "filled one",
        "c2": "filled two",
        "c3": "existing text",  # COALESCE kept the existing text (NULL _new_text)
        "d1": None,  # other agency untouched
    }

    processing = native.processing_table(con, COMMENT)
    statuses = dict(con.execute(f"SELECT comment_id, text_extraction_status FROM {processing} ORDER BY comment_id").fetchall())
    assert statuses == {"c1": "ok", "c2": "ok", "c3": "ok", "d1": None}

    # Other columns preserved (posted_date on a filled row).
    posted = con.execute(f"SELECT posted_date FROM {tbl} WHERE comment_id = 'c1'").fetchone()[0]
    assert posted == "2025-01-01T00:00:00Z"


def test_upsert_comment_text_noop_on_empty(tmp_path, local_catalog) -> None:
    """An empty updates frame leaves the table untouched (and creates no temp tables)."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    _put(con, COMMENT, [_comment("c1", "EPA-1", "EPA", "2025-01-01T00:00:00Z")])

    empty = pl.DataFrame(schema={"comment_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8})
    iceberg.upsert_comment_text(con, COMMENT, "EPA", empty)

    tbl = iceberg._qualified(COMMENT)
    assert con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0] == 1


def test_comments_index_retains_unknown_dates_and_refuses_malformed_rows(tmp_path, local_catalog):
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    _put(con, COMMENT, [dict(comment_id='unknown', agency_code='EPA', docket_id='EPA-1'),
                        dict(comment_id='known', agency_code='EPA', docket_id='EPA-1', posted_date='2025-01-01')])
    index = iceberg._build_comments_index(con, COMMENT, tmp_path)
    got = {(r["year"], r["month"]): r["row_count"] for r in pl.read_parquet(index).to_dicts()}
    assert got == {(None, None): 1, (2025, 1): 1}
    before = index.read_bytes()
    _put(con, COMMENT, [dict(comment_id='broken', agency_code='EPA', docket_id='EPA-1', posted_date='not-a-date')])
    with pytest.raises(ValueError, match="invalid coordinates"):
        iceberg._build_comments_index(con, COMMENT, tmp_path)
    assert index.read_bytes() == before


def test_comment_merge_does_not_recount_or_build_index(tmp_path, local_catalog, monkeypatch):
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    staging = tmp_path / "staging"
    _write_comment_staging(staging, "EPA", [_comment("a", "EPA-1", "EPA", "2026-09-01")])
    monkeypatch.setattr(iceberg, "_connect_for_table", lambda _: con)
    monkeypatch.setattr(iceberg, "_build_comments_index", lambda *a: pytest.fail("per-batch recount"))
    assert iceberg.merge_comments(staging, COMMENT) == 1
    assert not (tmp_path / "comments_index.parquet").exists()


@pytest.mark.parametrize('prior_exists', [False, True])
def test_merge_refuses_intervening_insert_or_update(tmp_path, local_catalog, monkeypatch, prior_exists):
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    table = iceberg._qualified(DOCKET)
    if prior_exists:
        _put(con, DOCKET, [_docket('D','EPA','old','2025-01-01')])
    staging = tmp_path / 'staging'
    _write_staging(staging, 'EPA', [_docket('D','EPA','prepared','2025-02-01')])
    real_replace = native.replace_native

    def intervening_write(connection, record_type, source, **kwargs):
        concurrent = _source(connection, record_type, [_docket('D','EPA','concurrent','2026-01-01')], 'concurrent')
        real_replace(connection, record_type, concurrent)
        real_replace(connection, record_type, source, **kwargs)

    monkeypatch.setattr(native, 'replace_native', intervening_write)
    with pytest.raises(RuntimeError, match='changed after preparation'):
        iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)
    assert con.execute(f'SELECT title,modify_date FROM {table}').fetchall() == [('concurrent','2026-01-01')]
    # Refusal leaves no open failed transaction.
    con.execute('BEGIN')
    con.execute('ROLLBACK')


def test_text_fill_refuses_intervening_unrelated_cell_change(tmp_path, local_catalog, monkeypatch):
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    table = iceberg._qualified(COMMENT)
    _put(con, COMMENT, [dict(comment_id='c1', agency_code='EPA', docket_id='old')])
    real_replace = iceberg.replace_rows

    def intervening_write(connection, record_type, source, **kwargs):
        concurrent = _source(connection, COMMENT, [dict(comment_id='c1', agency_code='EPA', docket_id='newer-source')], 'concurrent')
        real_replace(connection, record_type, concurrent)
        real_replace(connection, record_type, source, **kwargs)

    monkeypatch.setattr(iceberg, 'replace_rows', intervening_write)
    updates = pl.DataFrame({'comment_id':['c1'], '_new_text':['filled'], '_new_status':['ok']})
    with pytest.raises(RuntimeError, match='changed after preparation'):
        iceberg.upsert_comment_text(con, COMMENT, 'EPA', updates)
    assert con.execute(f'SELECT docket_id,text_content FROM {table}').fetchall() == [('newer-source',None)]


def test_replace_preserves_commit_error_when_transaction_already_aborted(local_catalog):
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    table = iceberg._qualified(DOCKET)
    _put(con, DOCKET, [dict(docket_id='D', title='prior')])
    _fresh(con, DOCKET, name='replacement')
    con.execute("UPDATE replacement SET title='replacement'")
    conflict = duckdb.TransactionException('simulated concurrent catalog commit conflict')

    class AbortedCommit:
        rollback_attempts = 0

        def execute(self, sql, *args):
            if sql == 'COMMIT':
                con.execute('ROLLBACK')
                raise conflict
            if sql == 'ROLLBACK':
                self.rollback_attempts += 1
            return con.execute(sql, *args)

    connection = AbortedCommit()
    with pytest.raises(duckdb.TransactionException, match='concurrent catalog commit conflict') as raised:
        iceberg.replace_rows(connection, DOCKET, 'replacement')
    assert raised.value is conflict
    assert connection.rollback_attempts == 1
    assert con.execute(f'SELECT title FROM {table}').fetchall() == [('prior',)]
    con.execute('BEGIN')
    con.execute('ROLLBACK')


class _CatalogWriteInterrupted:
    """Connection proxy that interrupts after a catalog mutation executes,
    before its transaction commits."""

    def __init__(self, con) -> None:
        self._con = con

    def execute(self, sql: str, *args):
        result = self._con.execute(sql, *args)
        if sql.lstrip().startswith((f"INSERT INTO {iceberg._CATALOG_ALIAS}", f"MERGE INTO {iceberg._CATALOG_ALIAS}")):
            raise KeyboardInterrupt("killed mid-upsert")
        return result

    def __getattr__(self, name):
        return getattr(self._con, name)


def test_merge_interrupted_before_commit_keeps_existing_rows(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    staging = tmp_path / "staging"
    _write_staging(staging, "EPA", [_docket("EPA-1", "EPA", "First", "2025-01-01")])
    iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)

    staging2 = tmp_path / "staging2"
    _write_staging(staging2, "EPA", [_docket("EPA-1", "EPA", "First UPDATED", "2025-02-01")])
    with pytest.raises(KeyboardInterrupt):
        iceberg._merge(_CatalogWriteInterrupted(con), iceberg._staging_files(staging2, DOCKET), DOCKET)

    rows = con.execute(f"SELECT docket_id, title FROM {iceberg._qualified(DOCKET)}").fetchall()
    assert rows == [("EPA-1", "First")]
    con.execute("BEGIN")
    con.execute("ROLLBACK")


def test_upsert_comment_text_interrupted_keeps_rows(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    _put(con, COMMENT, [_comment("c1", "EPA-1", "EPA", "2025-01-01T00:00:00Z")])

    updates = pl.DataFrame(
        {"comment_id": ["c1"], "_new_text": ["filled"], "_new_status": ["ok"]},
        schema={"comment_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8},
    )
    with pytest.raises(KeyboardInterrupt):
        iceberg.upsert_comment_text(_CatalogWriteInterrupted(con), COMMENT, "EPA", updates)

    tbl = iceberg._qualified(COMMENT)
    assert con.execute(f"SELECT comment_id, text_content FROM {tbl}").fetchall() == [("c1", None)]
    con.execute("BEGIN")
    con.execute("ROLLBACK")


def test_unchanged_merge_retains_rejected_attempt_without_changing_subject(tmp_path, local_catalog):
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    staging = tmp_path / 'staging'
    _write_staging(staging, 'EPA', [_docket('D', 'EPA', 'Held', '2026-01-01')])
    files = iceberg._staging_files(staging, DOCKET)
    assert iceberg._merge(con, files, DOCKET) == 1
    before = con.execute(f'SELECT * FROM {iceberg._qualified(DOCKET)}').fetchall()
    assert iceberg._merge(con, files, DOCKET) == 0
    assert con.execute(f'SELECT * FROM {iceberg._qualified(DOCKET)}').fetchall() == before
    rejected = con.execute(f"SELECT processing_json FROM {native.receipts_table()} WHERE outcome='rejected'").fetchall()
    assert len(rejected) == 1 and 'Held' in rejected[0][0]


def test_duplicate_audit_reports_corruption_without_writing(local_catalog):
    con = local_catalog
    _put(con, COMMENT, [_comment('c1', 'EPA-1', 'EPA', '2026-01-01')])
    table = iceberg._qualified(COMMENT)
    con.execute(f'INSERT INTO {table} SELECT * FROM {table}')  # Deliberate corruption, not a seed path.
    before = con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall()
    assert iceberg.audit_duplicates(con, COMMENT) == [('EPA', 2, 1)]
    assert con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall() == before


@pytest.mark.parametrize('invalid', ['unknown_column', 'fractional_integer'])
def test_merge_refuses_unclassified_or_lossy_source_before_casting(tmp_path, local_catalog, invalid):
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    row = _comment('c1', 'EPA-1', 'EPA', '2026-01-01')
    schema = dict(COMMENT.schema)
    if invalid == 'unknown_column':
        row['unclassified_fact'] = 'must not disappear'
        schema['unclassified_fact'] = pl.String
    else:
        row['duplicate_comments'] = '2.8'
        schema['duplicate_comments'] = pl.String
    file = tmp_path / 'bad.parquet'
    pl.DataFrame([row], schema=schema).write_parquet(file)
    with pytest.raises(ValueError):
        iceberg._merge(con, [file], COMMENT)
    assert con.execute(f'SELECT count(*) FROM {iceberg._qualified(COMMENT)}').fetchone() == (0,)


@pytest.mark.parametrize('operation,unchanged', [('replace', True), ('append', False), ('overwrite', False)])
def test_paired_export_checks_receipt_history_even_when_subject_snapshot_is_unchanged(monkeypatch, operation, unchanged):
    snapshot = iceberg.CatalogPairSnapshot('subjects', 1, 0, iceberg.CatalogSnapshot('receipts', 2, 0))
    monkeypatch.setattr(iceberg, '_connect', duckdb.connect)
    monkeypatch.setattr(iceberg, '_table_metadata', lambda *_, **__: _metadata((1, None, 'append'), uuid='subjects'))
    monkeypatch.setattr(iceberg, '_receipt_metadata', lambda *_, **__: _metadata(
        (2, None, 'append'), (3, 2, operation), uuid='receipts'))
    assert iceberg.rows_unchanged_since(COMMENT, snapshot) is unchanged
