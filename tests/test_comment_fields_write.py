"""The comment fill's write phase against a local DuckDB catalog: every guard, each able to catch a wrong write.

Plain DuckDB has no data files, so ``FILE_COLUMN`` stands in with ``agency_code`` (one "file" per agency) and the
snapshot is a digest of the table's rows. ``test_comment_fields_iceberg.py`` runs the same write on a real Iceberg
catalog, where rows move files and commits carry parents and summaries.
"""

import json
import re
from pathlib import Path

import duckdb
import polars as pl
import pytest

from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines import comment_fields_write as cfw
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg



T0 = "2020-01-01T00:00:00Z"

# One comment's attachments in both shapes. HELD is the rule before SpicyDocs b19b092 (2026-03-15 to 0.53.0): only
# downloadable renditions, an entry with none dropped. LISTED is b19b092's: every attachment the record lists, a
# withheld one with ``formats`` null and its restriction, a rendition with no file with ``url`` null.
_PDF = {"url": "https://downloads.regulations.gov/EPA-X-0001-0002/attachment_1.pdf", "format": "pdf", "size": 1000}
HELD = json.dumps([{"title": "Letter", "formats": [_PDF]}])
LISTED = json.dumps([
    {"title": "Letter", "formats": [_PDF, {"url": None, "format": "docx", "size": 900}]},
    {"title": "Study", "formats": None, "restrictReasonType": "Copyrighted", "restrictReason": "Copyrighted material"},
])


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    path = tmp_path / "catalog.duckdb"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{path}' AS {iceberg._CATALOG_ALIAS}")
        return con

    def snapshot(con, record_type, **_options):
        digest = con.execute(f"SELECT coalesce(sum(hash(t)), 0) FROM {iceberg._qualified(record_type)} t").fetchone()[0]
        return iceberg.CatalogSnapshot("local", int(digest % 2**62), 1)

    monkeypatch.setattr(iceberg, "_connect", connect)
    monkeypatch.setattr(iceberg, "_read_snapshot", snapshot)
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, _, **kw: f"SELECT * FROM {iceberg._qualified(rt)}")
    monkeypatch.setattr(cfw, "FILE_COLUMN", "agency_code")
    files = lambda con: {r[0] for r in con.execute(  # noqa: E731
        f"SELECT DISTINCT agency_code FROM {iceberg._qualified(COMMENT)}").fetchall()}
    monkeypatch.setattr(cfw, "live_files", files)
    monkeypatch.setattr(cfw, "data_files", lambda con: dict.fromkeys(files(con), 1000))
    monkeypatch.setattr(cfw, "commit_record", lambda con, parent_id: None)  # plain DuckDB records no commits
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "default")
    monkeypatch.setenv(iceberg.CATALOG_LOCK_ENV, "test")
    with connect() as con:
        iceberg._ensure_table(con, COMMENT)
    return connect


def row(comment_id, agency="EPA", modify=T0, **values):
    return {**dict.fromkeys(COMMENT.schema), "comment_id": comment_id, "agency_code": agency, "modify_date": modify,
            "comment": f"body of {comment_id}", "text_content": f"text of {comment_id}", "title": "t", **values}


def seed(connect, rows):
    with connect() as con:
        con.register("seed", pl.DataFrame(rows, schema=dict.fromkeys(COMMENT.schema, pl.Utf8)).to_arrow())
        iceberg.replace_rows(con, COMMENT, "seed")


def reads(tmp_path, rows, agency="EPA"):
    """One read part, named as ``read`` names this record shape's parts."""
    part = tmp_path / "parts" / f"agency={agency}" / f"p-{cf.RECORD_SHAPE}-{len(list(tmp_path.glob('parts/*/*'))):05d}.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    full = [{**dict.fromkeys(cf.PART_SCHEMA), "agency_code": agency, "modify_date": T0, **r} for r in rows]
    pl.DataFrame(full, schema=cf.PART_SCHEMA).write_parquet(part)


def table(connect) -> dict[str, dict]:
    with connect() as con:
        from spicy_regs.sources.regulatory_catalog import processing_table
        return {r["comment_id"]: r for r in con.execute(f"SELECT * FROM {processing_table(con, COMMENT)}").pl().to_dicts()}


def fill(tmp_path, connect, **options):
    prepared = cfw.prepare(tmp_path)
    with connect() as con:
        return prepared, cfw.write(tmp_path, con=con, **options)


class Proxy:
    """A connection whose statements can be rewritten, to model a faulty engine or a crash."""

    def __init__(self, con, rewrite=None, after=None):
        self.con, self.rewrite, self.after = con, rewrite, after

    def execute(self, sql, *args):
        result = self.con.execute(self.rewrite(sql) if self.rewrite else sql, *args)
        if self.after:
            self.after(sql)
        return result

    def __getattr__(self, name):
        return getattr(self.con, name)


def journal(tmp_path) -> list[dict]:
    (path,) = (tmp_path / "fill").glob("write-journal-*.jsonl")
    return [json.loads(line) for line in path.read_text().splitlines()]


# --------------------------------------------------------------------------- #
# The rules
# --------------------------------------------------------------------------- #
def test_only_null_cells_are_filled_and_a_stated_zero_is_a_value(tmp_path, catalog):
    seed(catalog, [row("A", subtype="Kept"), row("B"), row("C")])
    reads(tmp_path, [
        {"key": "a", "comment_id": "A", "subtype": "Read", "duplicate_comments": 3, "attachments_json": '[{"title":"attachment"}]'},
        {"key": "b", "comment_id": "B", "subtype": "Public Comment", "duplicate_comments": 0},
        {"key": "c", "comment_id": "C"},
    ])
    prepared, written = fill(tmp_path, catalog)
    rows = table(catalog)
    assert rows["A"]["subtype"] == "Kept"
    assert (rows["A"]["duplicate_comments"], rows["A"]["attachments_json"]) == (3, '[{"title":"attachment"}]')
    assert (rows["B"]["subtype"], rows["B"]["duplicate_comments"]) == ("Public Comment", 0)
    assert rows["C"]["duplicate_comments"] is None
    assert (rows["A"]["comment"], rows["A"]["text_content"]) == ("body of A", "text of A")
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (2, 2)
    assert prepared["cells_by_column"] == {**dict.fromkeys(cfw.FILL_COLUMNS, 0), "subtype": 1,
                                           "duplicate_comments": 2, "attachments_json": 1}


def test_submitter_fields_the_row_predates_are_filled_and_a_stated_one_is_kept(tmp_path, catalog):
    """Rows ingested before the extract mapped the submitter (ff812e5, 2026-06-15) hold NULL where the object states
    a name: FWS-HQ-ES-2025-0034-212973 states Earthjustice / Kristen / Boyles at the publisher and NULL in the catalog.
    The read copy fills them; a value the catalog already states is never overwritten."""
    seed(catalog, [row("A"), row("B", organization="Kept, Inc.")])
    reads(tmp_path, [
        {"key": "a", "comment_id": "A", "organization": "Earthjustice", "first_name": "Kristen", "last_name": "Boyles"},
        {"key": "b", "comment_id": "B", "organization": "Other"},
    ])
    prepared, written = fill(tmp_path, catalog)
    rows = table(catalog)
    assert (rows["A"]["organization"], rows["A"]["first_name"], rows["A"]["last_name"]) == ("Earthjustice", "Kristen", "Boyles")
    assert rows["B"]["organization"] == "Kept, Inc."
    assert {c: prepared["cells_by_column"][c] for c in ("organization", "first_name", "last_name")} == {
        "organization": 1, "first_name": 1, "last_name": 1}
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (1, 1)


def test_prepare_leaves_only_its_outputs_where_the_artifact_collects_them(tmp_path, catalog):
    """Its whole-table intermediates stream through ``fill/work`` and go: the artifact uploads ``fill/*.parquet``."""
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}, {"key": "a(1)", "comment_id": "A", "subtype": "T"},
                     {"key": "b", "comment_id": "B", "subtype": "S"}])
    prepared = cfw.prepare(tmp_path)
    assert (prepared["rows_to_fill"], prepared["conflicted_versions_by_column"]["subtype"]) == (1, 1)
    assert sorted(path.name for path in (tmp_path / "fill").glob("*.parquet")) == [
        "conflicts.parquet", "files.parquet", "fill.parquet", "refusals.parquet"]
    assert not (tmp_path / "fill" / "work").exists()


def test_a_read_of_another_version_fills_nothing(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "modify_date": "2021-06-01T00:00:00Z", "subtype": "Read"}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["subtype"] is None
    assert (prepared["other_version_only"], prepared["rows_to_fill"], written["rows_changed"]) == (1, 0, 0)


def test_copies_that_disagree_on_one_column_block_only_that_column(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [
        {"key": "a", "comment_id": "A", "subtype": "S", "attachments_json": '[{"title":"attachment"}]'},
        {"key": "a(1)", "comment_id": "A", "subtype": "S", "attachments_json": "[2]"},
    ])
    prepared, _ = fill(tmp_path, catalog)
    assert (table(catalog)["A"]["subtype"], table(catalog)["A"]["attachments_json"]) == ("S", None)
    conflicts = pl.read_parquet(tmp_path / "fill" / "conflicts.parquet").to_dicts()
    assert [(c["comment_id"], c["column"], list(c["keys"])) for c in conflicts] == [("A", "attachments_json",
                                                                                     ["a", "a(1)"])]
    assert prepared["conflicted_versions_by_column"]["attachments_json"] == 1


def test_a_read_that_only_adds_withheld_attachments_replaces_the_held_list(tmp_path, catalog):
    """The 2026-10 re-read: the held list is the read's with its withheld entries and file-less renditions removed."""
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == LISTED
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (1, 1)
    assert (prepared["replaced_by_column"], prepared["cells_by_column"]["attachments_json"]) == ({"attachments_json": 1}, 0)
    assert prepared["refused_not_additive_by_column"] == {"attachments_json": 0}


def test_the_downloadable_part_of_a_listed_value_is_the_earlier_rules_value():
    """Both shapes: b19b092's value less what offers no file is the earlier rule's, and the earlier rule's is its own."""
    assert cfw.downloadable_attachments(LISTED) == HELD
    assert cfw.downloadable_attachments(HELD) == HELD
    only_withheld = json.dumps([{"title": "Study", "formats": None, "restrictReasonType": "Copyrighted"}])
    assert cfw.downloadable_attachments(only_withheld) is None  # the earlier rule wrote NULL for it
    for other in ("{}", "[1]", '[{"title": "x"}]', "not json", None):
        assert cfw.downloadable_attachments(other) is None


def test_the_installed_extract_projects_to_the_earlier_rules_value():
    """Whichever SpicyDocs is installed, before or after b19b092 or 0.85.0 (the entry's ``attachment_id``), its value for
    a record listing a withheld attachment and a rendition with no file projects to the earlier rule's value."""
    record = {"data": {"id": "EPA-X-0001-0002", "attributes": {"modifyDate": T0}}, "included": [
        {"id": "09000064b8f4997d", "type": "attachments", "attributes": {"title": "Letter", "fileFormats": [
            {"fileUrl": _PDF["url"], "format": "pdf", "size": 1000}, {"fileUrl": None, "format": "docx", "size": 900}]}},
        {"id": "09000064b8f4997e", "type": "attachments", "attributes": {"title": "Study", "fileFormats": None,
                                               "restrictReasonType": "Copyrighted",
                                               "restrictReason": "Copyrighted material"}},
    ]}
    assert cfw.downloadable_attachments(COMMENT.extract(record)["attachments_json"]) == HELD


def _identified(listed: str) -> str:
    """``listed`` as SpicyDocs 0.85.0 spells it: each entry ends with its own ``attachment_id``."""
    return json.dumps([{**entry, "attachment_id": f"0900006400000{n:03d}"} for n, entry in enumerate(json.loads(listed))])


# b19b092-to-0.84.0 lists (no id): one holding a withheld entry, one a rendition with no file.
_WITHHELD_LISTED = json.dumps([
    {"title": "Letter", "formats": [_PDF]},
    {"title": "Study", "formats": None, "restrictReasonType": "Copyrighted", "restrictReason": "Copyrighted material"},
])
_URLLESS_LISTED = json.dumps([{"title": "Letter", "formats": [_PDF, {"url": None, "format": "docx", "size": 900}]}])


@pytest.mark.parametrize(("held", "read"), [
    (_WITHHELD_LISTED, _identified(_WITHHELD_LISTED)),  # held by 0.54.0-0.84.0
    (_URLLESS_LISTED, _identified(_URLLESS_LISTED)),  # held by 0.54.0-0.84.0
    (LISTED, _identified(LISTED)),  # held by 0.54.0-0.84.0, both at once
    (HELD, _identified(LISTED)),  # held before b19b092
    (HELD, LISTED),  # held before b19b092, read by 0.54.0-0.84.0
], ids=["084-withheld", "084-urlless", "084-both", "pre-b19b092", "pre-b19b092-read-084"])
def test_a_read_that_only_adds_ids_or_withheld_entries_replaces_either_eras_list(tmp_path, catalog, held, read):
    """0.85.0 adds each entry's ``attachment_id``: a list held in either earlier spelling is replaced by the full read."""
    assert cfw.only_adds_attachments(read, held)
    seed(catalog, [row("A", attachments_json=held)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": read}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == read
    assert (prepared["replaced_by_column"], prepared["refused_not_additive_by_column"], written["rows_changed"]) == (
        {"attachments_json": 1}, {"attachments_json": 0}, 1)


_CHANGED = json.dumps([{**entry, "formats": [{**_PDF, "url": _PDF["url"] + "?v=2"}] if entry["formats"] else None}
                       for entry in json.loads(_WITHHELD_LISTED)])
_DROPPED = json.dumps(json.loads(_WITHHELD_LISTED)[:1])


@pytest.mark.parametrize(("held", "read"), [
    (_WITHHELD_LISTED, _identified(_CHANGED)),  # held by 0.54.0-0.84.0; the read changes a file as well as adding ids
    (HELD, _identified(_CHANGED)),  # held before b19b092; the same read
    (_WITHHELD_LISTED, _identified(_DROPPED)),  # held by 0.54.0-0.84.0; the read no longer lists the withheld entry
], ids=["084-changed-file", "pre-b19b092-changed-file", "084-entry-gone"])
def test_a_read_that_changes_more_than_an_earlier_spelling_is_refused_in_either_era(tmp_path, catalog, held, read):
    assert not cfw.only_adds_attachments(read, held)
    seed(catalog, [row("A", attachments_json=held)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": read}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == held
    assert (prepared["refused_not_additive_by_column"], written["rows_changed"]) == ({"attachments_json": 1}, 0)


@pytest.mark.parametrize("read", [
    json.dumps([{"title": "Letter", "formats": [{**_PDF, "url": _PDF["url"] + "?v=2"}]}]),  # a changed file
    json.dumps([{"title": "Study", "formats": None, "restrictReasonType": "Copyrighted"}]),  # the held entry gone
    json.dumps([{"title": "Letter", "formats": [_PDF]}], separators=(",", ":")),  # another spelling of the same list
    json.dumps([{"formats": [_PDF], "title": "Letter"}]),  # the same entry, its keys reordered
    json.dumps([{"title": "Letter", "formats": [{**_PDF, "checksum": "x"}]}]),  # a rendition with more than a file
])
def test_a_read_that_does_more_than_add_is_refused_and_counted(tmp_path, catalog, read):
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": read}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == HELD
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (0, 0)
    assert prepared["refused_not_additive_by_column"] == {"attachments_json": 1}
    assert pl.read_parquet(tmp_path / "fill" / "refusals.parquet").to_dicts() == [
        {"comment_id": "A", "modify_date": T0, "column": "attachments_json"}]


def test_a_read_of_another_version_replaces_nothing(tmp_path, catalog):
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "modify_date": "2021-06-01T00:00:00Z", "attachments_json": LISTED}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == HELD
    assert (prepared["other_version_only"], prepared["replaced_by_column"], written["rows_changed"]) == (
        1, {"attachments_json": 0}, 0)


def test_copies_that_disagree_replace_nothing(tmp_path, catalog):
    longer = json.dumps([*json.loads(LISTED), {"title": "Annex", "formats": None, "restrictReasonType": "Other"}])
    seed(catalog, [row("A", attachments_json=HELD)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED},
                     {"key": "a(1)", "comment_id": "A", "attachments_json": longer}])
    prepared, written = fill(tmp_path, catalog)
    assert table(catalog)["A"]["attachments_json"] == HELD
    assert prepared["conflicted_versions_by_column"]["attachments_json"] == 1
    assert (prepared["replaced_by_column"], prepared["refused_not_additive_by_column"]) == (
        {"attachments_json": 0}, {"attachments_json": 0})


def test_a_null_held_list_still_fills_and_an_equal_one_is_left_alone(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", attachments_json=LISTED)])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED},
                     {"key": "b", "comment_id": "B", "attachments_json": LISTED}])
    prepared, written = fill(tmp_path, catalog)
    assert (table(catalog)["A"]["attachments_json"], table(catalog)["B"]["attachments_json"]) == (LISTED, LISTED)
    assert (prepared["cells_by_column"]["attachments_json"], prepared["replaced_by_column"]) == (
        1, {"attachments_json": 0})
    assert written["rows_changed"] == 1


def test_a_replacement_is_written_only_over_the_value_it_replaces(tmp_path, catalog):
    """A stale fill row expecting another held value matches no MERGE update, so the cell keeps what it holds."""
    seed(catalog, [row("A", attachments_json=HELD), row("B", attachments_json="[]")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)  # B holds "[]"
    _restamp(tmp_path)
    with catalog() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 1
    assert (table(catalog)["A"]["attachments_json"], table(catalog)["B"]["attachments_json"]) == (LISTED, "[]")


def test_a_merge_that_replaces_without_the_held_guard_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A", attachments_json=HELD), row("B", attachments_json="[]")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)
    _restamp(tmp_path)
    unguarded = lambda sql: re.sub(  # noqa: E731
        r'WHEN p\."attachments_json" = b\."_held_attachments_json" THEN', "WHEN TRUE THEN",
        sql.split(" WHERE ")[0]) if "CREATE OR REPLACE TEMP TABLE _filled" in sql else sql
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=unguarded))
    assert (table(catalog)["A"]["attachments_json"], table(catalog)["B"]["attachments_json"]) == (HELD, "[]")


def test_a_stated_null_disagreeing_with_a_value_is_a_conflict(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": None}, {"key": "a(1)", "comment_id": "A", "subtype": "S"}])
    fill(tmp_path, catalog)
    assert table(catalog)["A"]["subtype"] is None


# --------------------------------------------------------------------------- #
# The guards: each test is a wrong write that must be refused before COMMIT.
# --------------------------------------------------------------------------- #
def _merge(sql: str) -> bool:
    return sql.lstrip().startswith("MERGE INTO")


def test_a_merge_that_damages_a_non_fill_column_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}, {"key": "b", "comment_id": "B", "subtype": "S"}])
    cfw.prepare(tmp_path)
    blank = lambda sql: sql.replace('"text_content"=s."text_content"', '"text_content"=NULL') if _merge(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="differ"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=blank))
    assert {r["text_content"] for r in table(catalog).values()} == {"text of A", "text of B"}  # nothing committed
    assert journal(tmp_path)[-1]["state"] == "failed" and journal(tmp_path)[-1]["committed"] is False


def test_an_overwriting_merge_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A", subtype="Kept")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "Read", "duplicate_comments": 1}])
    cfw.prepare(tmp_path)
    overwrite = lambda sql: sql.replace('COALESCE(p."subtype", CAST(b."subtype" AS VARCHAR))', 'b."subtype"') if 'CREATE OR REPLACE TEMP TABLE _filled' in sql else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=overwrite))
    assert table(catalog)["A"]["subtype"] == "Kept"


def test_a_merge_that_rewrites_rows_needing_nothing_is_refused_by_the_count(tmp_path, catalog):
    """Rewriting a row identically leaves every value right; only the planned count sees the extra row."""
    seed(catalog, [row("A"), row("B", subtype="S", duplicate_comments="0")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    extra = pl.read_parquet(fill_file)
    stale = extra.with_columns(pl.lit("B").alias("comment_id"), pl.lit(None, pl.Utf8).alias("subtype"))
    pl.concat([extra, stale]).write_parquet(fill_file)  # B needs nothing: a stale fill row
    _restamp(tmp_path)
    unguarded = lambda sql: sql.split(' WHERE ')[0] if 'CREATE OR REPLACE TEMP TABLE _filled' in sql else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="planned"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=unguarded))


def test_a_stale_row_that_needs_nothing_is_left_alone(tmp_path, catalog):
    """A fill row whose cells the catalog already holds matches no MERGE update: the guard, not the count, skips it."""
    seed(catalog, [row("A"), row("B", subtype="S", duplicate_comments="0")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)
    _restamp(tmp_path)
    with catalog() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 1
    assert journal(tmp_path)[-1]["state"] == "verified"


def test_a_merge_that_ignores_modify_date_is_refused(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", modify="2021-06-01T00:00:00Z")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    fill_file = tmp_path / "fill" / "fill.parquet"
    rows = pl.read_parquet(fill_file)
    pl.concat([rows, rows.with_columns(pl.lit("B").alias("comment_id"))]).write_parquet(fill_file)  # B at T0: stale
    _restamp(tmp_path)
    loose = lambda sql: sql.replace(' AND p.modify_date IS NOT DISTINCT FROM b.modify_date', '') if 'CREATE OR REPLACE TEMP TABLE _filled' in sql else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=loose))
    with catalog() as con:
        cfw.write(tmp_path, con=con, clear_failure=True)  # the true MERGE leaves the other version alone
    assert (table(catalog)["A"]["subtype"], table(catalog)["B"]["subtype"]) == ("S", None)


def _restamp(tmp_path: Path) -> None:
    """Point prepare.json at a hand-edited fill.parquet, as a stale fill would be."""
    path = tmp_path / "fill" / "prepare.json"
    prepared = json.loads(path.read_text())
    prepared["fill_sha256"] = cfw._sha256(tmp_path / "fill" / "fill.parquet")
    path.write_text(json.dumps(prepared))


# --------------------------------------------------------------------------- #
# Fail-closed, resume and refusals
# --------------------------------------------------------------------------- #
def test_a_failed_batch_stops_every_later_run_until_cleared(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    blank = lambda sql: sql.replace('"comment"=s."comment"', '"comment"=NULL') if _merge(sql) else sql  # noqa: E731
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=Proxy(con, rewrite=blank))
    with catalog() as con, pytest.raises(RuntimeError, match="clear-failure"):
        cfw.write(tmp_path, con=con)
    with catalog() as con:
        assert cfw.write(tmp_path, con=con, clear_failure=True)["rows_changed"] == 1
    assert table(catalog)["A"]["subtype"] == "S"


def test_the_preimage_is_on_disk_before_the_merge(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    seen = []
    watch = lambda sql: seen.append(list((tmp_path / "fill" / "preimage").glob("*.parquet"))) if _merge(sql) else None  # noqa: E731
    with catalog() as con:
        cfw.write(tmp_path, con=Proxy(con, after=watch))
    (preimage,) = seen[0]
    assert pl.read_parquet(preimage).to_dicts()[0]["subtype"] is None  # the row as it was before


def test_passed_work_is_kept_by_file_across_a_changed_batch_budget(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", agency="CMS"), row("C", agency="FDA")])
    for agency, cid in (("EPA", "A"), ("CMS", "B"), ("FDA", "C")):
        reads(tmp_path, [{"key": cid, "comment_id": cid, "subtype": "S"}], agency=agency)
    cfw.prepare(tmp_path)
    fail_second = {"n": 0}

    def crash(sql):
        if _merge(sql):
            fail_second["n"] += 1
            if fail_second["n"] == 2:
                raise ConnectionError("network")

    with catalog() as con, pytest.raises(ConnectionError):
        cfw.write(tmp_path, con=Proxy(con, after=crash), batch_bytes=1000)
    with catalog() as con:
        again = cfw.write(tmp_path, con=con, batch_bytes=10**9)
    assert again["files_skipped_verified"] == 1 and again["rows_changed"] == 2
    assert {r["subtype"] for r in table(catalog).values()} == {"S"}


def test_another_writer_between_batches_stops_the_run_and_a_new_prepare_resumes(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(tmp_path)
    seed(catalog, [row("Z")])  # an ETL commit lands after prepare
    with catalog() as con, pytest.raises(RuntimeError, match="another writer"):
        cfw.write(tmp_path, con=con)
    _, written = fill(tmp_path, catalog)
    assert written["rows_changed"] == 2


def test_the_write_needs_the_lock_the_columns_and_for_a_pilot_a_scope(tmp_path, catalog, monkeypatch):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    with catalog() as con, pytest.raises(RuntimeError, match="scoped pilot"):
        cfw.write(tmp_path, con=con, by_file=False)
    monkeypatch.delenv(iceberg.CATALOG_LOCK_ENV)
    with catalog() as con, pytest.raises(RuntimeError, match="lock"):
        cfw.write(tmp_path, con=con)


def test_a_catalog_without_the_new_columns_is_refused_not_migrated(tmp_path, catalog):
    with catalog() as con:
        con.execute(f"ALTER TABLE {iceberg._qualified(COMMENT)} DROP COLUMN subtype")
    with pytest.raises(ValueError, match="schema differs"):
        cfw.prepare(tmp_path)
    with catalog() as con:
        assert "subtype" not in iceberg._column_types(con, COMMENT)


def test_a_scoped_pilot_writes_one_batch(tmp_path, catalog):
    seed(catalog, [row("A", docket_id="D-1"), row("B", agency="CMS", docket_id="D-2")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(tmp_path, scope={"docket_id": "D-1"})
    with catalog() as con:
        assert cfw.write(tmp_path, con=con, by_file=False)["batches"] == 1
    assert (table(catalog)["A"]["subtype"], table(catalog)["B"]["subtype"]) == ("S", None)


def test_batches_pack_whole_files_by_bytes():
    files = {"f1": {"rows": 3, "bytes": 300}, "f2": {"rows": 1, "bytes": 100}, "f3": {"rows": 5, "bytes": 900}}
    assert cfw.batches(files, 400, by_file=True) == [["f1", "f2"], ["f3"]]
    assert cfw.batches(files, 400, by_file=False) == [["f1", "f2", "f3"]]


# --------------------------------------------------------------------------- #
# Round 2: damage outside the batch's rows, the commit after COMMIT, one journal per prepare, the undo
# --------------------------------------------------------------------------- #
def _before_merge(statement: str):
    """Run ``statement`` in the batch's transaction just before its MERGE; the MERGE's count is still the one read."""
    return lambda sql: f"{statement}; {sql}" if _merge(sql) else sql


def _table() -> str:
    return iceberg._qualified(COMMENT)


def test_a_transaction_that_also_deletes_an_unread_row_in_the_batch_file_is_rolled_back(tmp_path, catalog):
    """The review's scenario Z: B shares A's file but needs nothing; only the row count sees it go."""
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="held"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=_before_merge(f"DELETE FROM {_table()} WHERE comment_id = 'B'")))
    assert set(table(catalog)) == {"A", "B"} and table(catalog)["A"]["subtype"] is None


def test_a_transaction_that_deletes_a_row_in_another_file_is_rolled_back(tmp_path, catalog):
    seed(catalog, [row("A"), row("C", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="held"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=_before_merge(f"DELETE FROM {_table()} WHERE comment_id = 'C'")))
    assert set(table(catalog)) == {"A", "C"}


def test_a_row_the_transaction_moves_to_a_new_file_outside_the_batch_is_rolled_back(tmp_path, catalog):
    """C keeps its values and the row count holds (a moved row); only the new files' contents show it."""
    seed(catalog, [row("A"), row("C", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    move = _before_merge(f"UPDATE {_table()} SET agency_code = 'NEW' WHERE comment_id = 'C'")
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match="outside the batch"):
        cfw.write(tmp_path, con=Proxy(con, rewrite=move))
    assert table(catalog)["C"]["agency_code"] == "CMS"


def _recorded(summary: dict):
    """A ``commit_record`` stand-in: the one child of the checked snapshot is the catalog's current snapshot."""

    def record(con, parent_id):
        current = iceberg._read_snapshot(con, COMMENT)
        return {"table_uuid": current.table_uuid, "snapshot_id": current.snapshot_id, "schema_id": current.schema_id,
                "summary": summary}

    return record


@pytest.mark.parametrize(("record", "problem"), [
    (_recorded({"added-records": "1", "added-position-deletes": "1"}), None),
    (_recorded({"added-records": "2", "added-position-deletes": "2"}), "added 2 records"),
    (_recorded({"added-records": "1", "added-position-deletes": "0"}), "0 position deletes"),
    (None, "no single commit"),
])
def test_the_commit_must_be_the_one_child_of_the_checked_snapshot_adding_exactly_the_batch(
    tmp_path, catalog, monkeypatch, record, problem,
):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(tmp_path)
    monkeypatch.setattr(cfw, "commit_record", record or (lambda con, parent_id: None))
    monkeypatch.setattr(cfw, "_records_commits", lambda con: True)
    if problem is None:
        with catalog() as con:
            assert cfw.write(tmp_path, con=con)["rows_changed"] == 1
        assert journal(tmp_path)[-1]["state"] == "verified"
        return
    with catalog() as con, pytest.raises(cfw.FillVerificationError, match=problem):
        cfw.write(tmp_path, con=con)
    assert journal(tmp_path)[-1] | {"reason": None} == journal(tmp_path)[-1] | {
        "state": "failed", "committed": True, "reason": None}


def test_a_committed_failure_blocks_every_prepare_and_write_until_it_is_undone(tmp_path, catalog, monkeypatch):
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(tmp_path)
    monkeypatch.setattr(cfw, "commit_record", _recorded({"added-records": "9", "added-position-deletes": "9"}))
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(tmp_path, con=con, batch_bytes=1000)  # CMS first: it commits, then fails the post-commit check
    monkeypatch.setattr(cfw, "commit_record", lambda con, parent_id: None)
    with pytest.raises(RuntimeError, match="undo"):
        cfw.prepare(tmp_path)  # a fresh prepare no longer escapes it
    with catalog() as con, pytest.raises(RuntimeError, match="undo"):
        cfw.write(tmp_path, con=con)
    (failed,) = [line for line in journal(tmp_path) if line["state"] == "failed"]
    with catalog() as con:
        cfw.undo(tmp_path, failed["batch"], expected_snapshot=iceberg._read_snapshot(con, COMMENT).snapshot_id,
                 con=con)
    assert (table(catalog)["B"]["subtype"], table(catalog)["A"]["subtype"]) == (None, None)
    _, written = fill(tmp_path, catalog)
    assert written["rows_changed"] == 2


def test_write_refuses_a_committed_failure_whose_journal_arrived_after_its_prepare(tmp_path, catalog, monkeypatch):
    """Another prepare's failure, pulled in late (a runner's journals, a copied workdir), still stops the write."""
    first, second = tmp_path / "first", tmp_path / "second"
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(first, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    reads(second, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    cfw.prepare(second)
    cfw.prepare(first)
    monkeypatch.setattr(cfw, "commit_record", _recorded({"added-records": "9", "added-position-deletes": "9"}))
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(first, con=con)
    monkeypatch.setattr(cfw, "commit_record", lambda con, parent_id: None)
    for path in (first / "fill").glob("write-journal-*.jsonl"):
        (second / "fill" / path.name).write_bytes(path.read_bytes())
    with catalog() as con, pytest.raises(RuntimeError, match="undo"):
        cfw.write(second, con=con)
    assert table(catalog)["B"]["subtype"] is None


def test_a_fresh_prepare_never_reuses_an_earlier_journal(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    first, second = cfw.prepare(tmp_path), cfw.prepare(tmp_path)
    assert first["fill_sha256"] == second["fill_sha256"] and first["prepare_id"] != second["prepare_id"]


def test_prepare_refuses_a_file_whose_size_it_cannot_read(tmp_path, catalog, monkeypatch):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    monkeypatch.setattr(cfw, "data_files", lambda con: {})
    with pytest.raises(RuntimeError, match="size"):
        cfw.prepare(tmp_path)


def test_prepare_reads_the_staged_fill_input_as_it_reads_the_parts(tmp_path, catalog):
    seed(catalog, [row("A"), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S", "duplicate_comments": 0},
                     {"key": "b", "comment_id": "B", "attachments_json": '[{"title":"attachment"}]'}])
    from_parts = cfw.prepare(tmp_path)
    staged = tmp_path / "reads.parquet"
    pl.read_parquet(tmp_path / "parts" / "*" / "*.parquet").select(cfw.READ_COLUMNS).write_parquet(staged)
    assert cfw.prepare(tmp_path, reads=staged)["fill_sha256"] == from_parts["fill_sha256"]


# --------------------------------------------------------------------------- #
# Profiles and the staged read
# --------------------------------------------------------------------------- #
def test_the_attachments_profile_stages_four_columns_and_fills_only_attachments(tmp_path, catalog):
    seed(catalog, [row("A", attachments_json=HELD), row("B")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED, "subtype": "S"},
                     {"key": "b", "comment_id": "B", "attachments_json": LISTED, "subtype": "S"},
                     {"key": "b(1)", "comment_id": "B", "attachments_json": LISTED, "first_name": "Kristen"}])
    staged = cf.stage(tmp_path, tmp_path / "staged", profile="attachments")
    assert staged["columns"] == ["key", "comment_id", "modify_date", "attachments_json"] and all(staged["checks"].values())
    assert pl.read_parquet(tmp_path / "staged" / "reads.parquet").columns == staged["columns"]
    assert (staged["rows"], staged["distinct_comment_ids"], staged["non_null_by_column"]["attachments_json"]) == (3, 2, 3)
    from_parts = cfw.prepare(tmp_path, profile="attachments")
    prepared = cfw.prepare(tmp_path, reads=tmp_path / "staged" / "reads.parquet", profile="attachments")
    assert prepared["fill_sha256"] == from_parts["fill_sha256"] and prepared["columns"] == ["attachments_json"]
    assert (prepared["replaced_by_column"], prepared["cells_by_column"]) == ({"attachments_json": 1},
                                                                            {"attachments_json": 1})
    with catalog() as con:
        assert cfw.write(tmp_path, con=con)["rows_changed"] == 2
    rows = table(catalog)
    assert (rows["A"]["attachments_json"], rows["B"]["attachments_json"]) == (LISTED, LISTED)
    assert (rows["A"]["subtype"], rows["B"]["subtype"], rows["B"]["first_name"]) == (None, None, None)


def test_a_staged_read_lacking_a_profile_column_is_refused(tmp_path, catalog):
    seed(catalog, [row("A")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    cf.stage(tmp_path, tmp_path / "staged", profile="attachments")
    with pytest.raises(RuntimeError, match="lack"):
        cfw.prepare(tmp_path, reads=tmp_path / "staged" / "reads.parquet")  # the "all" profile
    with pytest.raises(ValueError, match="profile"):
        cfw.prepare(tmp_path, profile="attachment")


def test_no_profile_stages_a_private_column_and_the_check_would_catch_one(tmp_path, monkeypatch):
    """The private-attributes rule: a staged read holds the key and columns ``comments`` publishes, never the parts'
    attribute JSON (email, phone, fax), ETag, size or body digest."""
    staged = {c for profile in cfw.PROFILES for c in cfw.read_columns(profile)}
    assert staged - {"key"} <= set(COMMENT.schema) - {"comment"}
    assert not {c for c in staged if any(word in c for word in cf.PRIVATE_WORDS)}
    private = set(cf.PART_SCHEMA) - set(COMMENT.schema) - {"key"}
    assert private == {"etag", "size", "comment_sha256", "comment_length", "attributes_json"}
    assert all(any(word in c for word in cf.PRIVATE_WORDS) for c in private)
    reads(tmp_path, [{"key": "a", "comment_id": "A", "attributes_json": '{"email": "x@example.org"}'}])
    monkeypatch.setitem(cfw.PROFILES, "leaky", ("attachments_json", "attributes_json"))
    with pytest.raises(RuntimeError, match="private"):
        cf.stage(tmp_path, tmp_path / "staged", profile="leaky")
    assert not (tmp_path / "staged" / "reads.parquet").exists()


def test_a_staged_read_is_built_once_and_uploaded_once_never_over_an_object(tmp_path):
    from tests.generation_fakes import Store

    reads(tmp_path, [{"key": "a", "comment_id": "A", "attachments_json": LISTED}])
    out = tmp_path / "staged"
    staged = cf.stage(tmp_path, out, profile="attachments")
    with pytest.raises(RuntimeError, match="already holds"):
        cf.stage(tmp_path, out, profile="attachments")
    store = Store()
    uploaded = cf.upload_staged(out, client=store)
    sha = staged["file"]["sha256"]
    assert uploaded["key"] == f"staging/comment-fields-fill-{sha[:12]}/reads.parquet"
    assert store.objects[uploaded["key"]] == (out / "reads.parquet").read_bytes()
    assert store.objects[uploaded["key"] + ".sha256"] == f"{sha}  reads.parquet\n".encode()
    assert json.loads((out / "staging.json").read_text())["upload"] == uploaded
    with pytest.raises(RuntimeError, match="already holds"):
        cf.upload_staged(out, client=store)
    (out / "reads.parquet").write_bytes(b"other bytes")
    with pytest.raises(RuntimeError, match="not the staged read"):
        cf.upload_staged(out, client=Store())


def _filled(tmp_path, catalog) -> dict:
    seed(catalog, [row("A"), row("B", agency="CMS")])
    reads(tmp_path, [{"key": "a", "comment_id": "A", "subtype": "S", "duplicate_comments": 3}])
    reads(tmp_path, [{"key": "b", "comment_id": "B", "subtype": "S"}], agency="CMS")
    before = table(catalog)
    fill(tmp_path, catalog, batch_bytes=1000)
    (epa,) = [line for line in journal(tmp_path) if line["state"] == "verified" and line["files"] == ["EPA"]]
    return {"before": before, "batch": epa["batch"]}


def _current(catalog) -> int:
    with catalog() as con:
        return iceberg._read_snapshot(con, COMMENT).snapshot_id


def test_undo_restores_a_committed_batch_from_its_preimage_as_a_new_commit(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    expected = _current(catalog)
    with catalog() as con:
        undone = cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=con)
    assert table(catalog)["A"] == filled["before"]["A"] and table(catalog)["B"]["subtype"] == "S"
    assert undone["rows_restored"] == 1 and journal(tmp_path)[-1]["state"] == "undone"
    expected = _current(catalog)
    with catalog() as con, pytest.raises(RuntimeError, match="already undone"):
        cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=con)


def test_undo_needs_the_snapshot_the_operator_reviewed(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    with catalog() as con, pytest.raises(RuntimeError, match="expected"):
        cfw.undo(tmp_path, filled["batch"], expected_snapshot=7, con=con)
    assert table(catalog)["A"]["subtype"] == "S"


def test_undo_refuses_a_commit_between_its_check_and_its_transaction(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    expected = _current(catalog)

    def foreign(sql):
        if "_undo_now" in sql and sql.lstrip().startswith("CREATE"):
            proxy.con.execute(f"UPDATE {_table()} SET title = 'etl' WHERE comment_id = 'B'")

    with catalog() as con:
        proxy = Proxy(con, after=foreign)
        with pytest.raises(RuntimeError, match="snapshot changed"):
            cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=proxy)
    with catalog() as con:
        assert con.execute(f"SELECT subtype FROM {_table()} WHERE comment_id='A'").fetchone() == ('S',)


def test_undo_refuses_a_row_changed_since_the_fill(tmp_path, catalog):
    filled = _filled(tmp_path, catalog)
    with catalog() as con:
        con.execute(f"UPDATE {_table()} SET title = 'edited' WHERE comment_id = 'A'")
    expected = _current(catalog)
    with catalog() as con, pytest.raises(ValueError, match="subject receipt"):
        cfw.undo(tmp_path, filled["batch"], expected_snapshot=expected, con=con)
    with catalog() as con:
        assert con.execute(f"SELECT subtype,title FROM {_table()} WHERE comment_id='A'").fetchone() == ('S', 'edited')


class _Bucket:
    """The three S3 calls the journal sync and the staged fetch make, over a dict."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def upload_file(self, path, bucket, key):
        self.objects[key] = Path(path).read_bytes()

    def download_file(self, bucket, key, path):
        Path(path).write_bytes(self.objects[key])

    def get_paginator(self, name):
        objects = self.objects

        class Pages:
            def paginate(self, Bucket, Prefix):  # noqa: N803 (boto3's spelling)
                return [{"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}]

        return Pages()


def test_a_committed_failure_in_an_earlier_runs_journal_blocks_a_fresh_workdir(tmp_path, catalog, monkeypatch):
    """On a runner every run starts empty; the journals kept beside the staged read carry the failure over."""
    first, second, bucket = tmp_path / "run1", tmp_path / "run2", _Bucket()
    seed(catalog, [row("A")])
    reads(first, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.prepare(first)
    monkeypatch.setattr(cfw, "commit_record", _recorded({"added-records": "9", "added-position-deletes": "9"}))
    with catalog() as con, pytest.raises(cfw.FillVerificationError):
        cfw.write(first, con=con)
    assert cfw.sync_journals(first, "staging/x", push=True, client=bucket)
    reads(second, [{"key": "a", "comment_id": "A", "subtype": "S"}])
    cfw.sync_journals(second, "staging/x", push=False, client=bucket)
    with pytest.raises(RuntimeError, match="undo"):
        cfw.prepare(second)


def test_the_staged_read_is_refused_unless_its_digest_matches(tmp_path):
    bucket = _Bucket({"staging/x/reads.parquet": b"staged bytes"})
    good = cfw.hashlib.sha256(b"staged bytes").hexdigest()
    assert cfw.fetch_staged(tmp_path, "staging/x/reads.parquet", good, client=bucket).read_bytes() == b"staged bytes"
    with pytest.raises(RuntimeError, match="sha256"):
        cfw.fetch_staged(tmp_path, "staging/x/reads.parquet", "0" * 64, client=bucket)
    assert not (tmp_path / "reads.parquet").exists()
