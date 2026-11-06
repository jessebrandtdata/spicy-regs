"""Hermetic tests for the shared incremental-merge helper (transforms/table_merge.py).

No network: where a test needs a "prior" table, it's seeded on disk at
``table_merge``'s own scratch path (:func:`prior_scratch_path`), which the
merge then treats as an already-downloaded prior and never asks
``download_prior`` to fetch — enforced here by a ``download_prior`` stub that
raises if called.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
import pyarrow.parquet as pq

from spicy_regs.generations import build_generation
from spicy_regs.transforms import table_merge
from spicy_regs.transforms.table_merge import (
    Partitioning,
    merge_partitioned_table,
    merge_table,
    prior_members_path,
    prior_scratch_path,
    published_members,
)

COLUMNS = ("id", "name", "version")
NAME = "widgets"
REMOTE_KEY = "widgets.parquet"


def _write_prior(output_dir: Path, rows: list[dict]) -> None:
    schema = pa.schema([(c, pa.string()) for c in COLUMNS])
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, prior_scratch_path(output_dir, NAME))


def _read(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def _never_called(remote_key: str, local_path: Path) -> bool:
    raise AssertionError("download_prior must not be called when the prior is already on disk")


def _merge(tmp_path: Path, rows: list[dict], *, download_prior=_never_called, prior_present=None) -> Path:
    return merge_table(
        tmp_path,
        name=NAME,
        columns=COLUMNS,
        identity=("id",),
        version_column="version",
        rows=rows,
        remote_key=REMOTE_KEY,
        download_prior=download_prior,
        prior_present=prior_present,
    )


_PRIOR_ROWS = [
    {"id": "1", "name": "old", "version": "2024-01-01"},
    {"id": "2", "name": "prior-only", "version": "2024-01-02"},
]


def test_merge_prefers_fresh_row_on_repeated_identity(tmp_path):
    _write_prior(tmp_path, _PRIOR_ROWS)
    fresh = [{"id": "1", "name": "new", "version": "2024-02-01"}]

    out = _merge(tmp_path, fresh)

    by_id = {row["id"]: row for row in _read(out)}
    assert by_id["1"]["name"] == "new"
    assert by_id["1"]["version"] == "2024-02-01"


def test_merge_keeps_prior_only_rows(tmp_path):
    _write_prior(tmp_path, _PRIOR_ROWS)
    fresh = [{"id": "1", "name": "new", "version": "2024-02-01"}]

    out = _merge(tmp_path, fresh)

    by_id = {row["id"]: row for row in _read(out)}
    assert by_id["2"] == {"id": "2", "name": "prior-only", "version": "2024-01-02"}


def test_crs_summary_merge_preserves_revisions_and_deduplicates_rereads(tmp_path):
    import shutil

    from spicy_docs.schemas import TABLE_CONTRACTS

    name = "bill_publisher_summaries"
    contract = TABLE_CONTRACTS[name]
    original = dict(
        bill_id="119-hr-1", summary_version_code="49", action_date="2025-07-04",
        action_desc="Public Law", update_date="2025-07-07T12:00:00Z",
        summary_html="<p>Original summary.</p>", summary_chars="24",
        last_summary_update_date=None, source_schema_version="3.0.0",
    )
    revision = {**original, "update_date": "2025-07-09T12:00:00Z", "summary_html": "<p>Revised summary.</p>",
                "summary_chars": "23"}
    # Both native clock fields were read from originals under the current policy.
    pq.write_table(pa.Table.from_pylist([original], schema=pa.schema([(c, pa.string()) for c in contract.columns])),
                   prior_scratch_path(tmp_path, name))
    output = table_merge.merge_contract_table(tmp_path, name, [revision], download_prior=_never_called)
    assert _read(output) == [revision, original]

    # An unchanged fetch, including an older retained revision, creates no duplicate.
    for incoming in ([revision], [original, revision], []):
        shutil.copyfile(output, prior_scratch_path(tmp_path, name))
        output = table_merge.merge_contract_table(tmp_path, name, incoming, download_prior=_never_called)
        assert _read(output) == [revision, original]
    # Metadata update dates select retained revisions; no first-publication inference.
    assert _read(output)[0]["update_date"] == revision["update_date"]


@pytest.mark.parametrize("change", [
    {"summary_html": "<p>Unclocked source revision.</p>"},
    {"last_summary_update_date": "2014-09-09T00:00:00Z"},
])
@pytest.mark.parametrize("prior", [False, True])
def test_crs_summary_same_key_conflict_refuses_without_replacing_prior(tmp_path, change, prior):
    from spicy_docs.schemas import TABLE_CONTRACTS

    name = "bill_publisher_summaries"
    original = dict(bill_id="113-hr-4200", summary_version_code="00", action_date="2014-03-11",
                    action_desc="Introduced in House", update_date="2014-03-11T04:00:00Z",
                    last_summary_update_date="2014-09-08T21:32:48Z", source_schema_version="1.0.0",
                    summary_html="<p>Original summary.</p>", summary_chars="24")
    scratch = prior_scratch_path(tmp_path, name)
    if prior:
        pq.write_table(pa.Table.from_pylist([original], schema=pa.schema([
            (c, pa.string()) for c in TABLE_CONTRACTS[name].columns])), scratch)
        before = scratch.read_bytes()
    with pytest.raises(ValueError, match="contradictory source evidence"):
        table_merge.merge_contract_table(tmp_path, name,
            ([original] if not prior else []) + [{**original, **change}],
            download_prior=_never_called if prior else lambda *_: False)
    if prior:
        assert scratch.read_bytes() == before
    assert not (tmp_path / f"{name}.parquet").exists()


def test_crs_summary_schema_context_enrichment_is_not_a_source_conflict(tmp_path):
    from spicy_docs.schemas import TABLE_CONTRACTS

    name = "bill_publisher_summaries"
    original = dict.fromkeys(TABLE_CONTRACTS[name].columns)
    original.update(bill_id="119-hr-1", summary_version_code="00", action_date="2025-01-03",
                    update_date="2025-01-07T00:00:00Z", summary_html="same source text",
                    source_schema_version="3.0.0")
    pq.write_table(pa.Table.from_pylist([original], schema=pa.schema([
        (c, pa.string()) for c in original])), prior_scratch_path(tmp_path, name))
    output = table_merge.merge_contract_table(tmp_path, name, [{**original, "source_schema_version": "3.0.1"}],
                                             download_prior=_never_called)
    assert _read(output) == [{**original, "source_schema_version": "3.0.1"}]


def test_crs_summary_prior_requires_native_clock_replay(tmp_path):
    from spicy_docs.schemas import TABLE_CONTRACTS

    name = "bill_publisher_summaries"
    columns = [c for c in TABLE_CONTRACTS[name].columns if c not in {
        "last_summary_update_date", "source_schema_version"}]
    original = dict.fromkeys(columns)
    original.update(bill_id="113-hr-4200", summary_version_code="00", action_date="2014-03-11",
                    update_date="2014-03-11T04:00:00Z")
    prior = prior_scratch_path(tmp_path, name)
    pq.write_table(pa.Table.from_pylist([original], schema=pa.schema([(c, pa.string()) for c in columns])), prior)
    before = prior.read_bytes()
    with pytest.raises(ValueError, match="incompatible clock policy"):
        table_merge.merge_contract_table(tmp_path, name, [], download_prior=_never_called)
    assert prior.read_bytes() == before


def test_crs_revision_key_refuses_to_drop_an_undated_prior(tmp_path):
    from spicy_docs.schemas import TABLE_CONTRACTS

    name = "bill_publisher_summaries"
    row = dict.fromkeys(TABLE_CONTRACTS[name].columns)
    row.update(bill_id="119-hr-1", summary_version_code="49", action_date="2025-07-04")
    prior = prior_scratch_path(tmp_path, name)
    pq.write_table(pa.Table.from_pylist([row], schema=pa.schema([(c, pa.string()) for c in row])), prior)
    before = prior.read_bytes()
    with pytest.raises(ValueError, match="missing update_date"):
        table_merge.merge_contract_table(tmp_path, name, [], download_prior=_never_called)
    assert prior.read_bytes() == before
    assert not (tmp_path / f"{name}.parquet").exists()


def test_merge_orders_by_version_then_identity(tmp_path):
    fresh = [
        {"id": "3", "name": "c", "version": "2024-01-01"},
        {"id": "1", "name": "a", "version": "2024-03-01"},
        {"id": "2", "name": "b", "version": "2024-03-01"},
    ]

    out = _merge(tmp_path, fresh, download_prior=lambda remote_key, local_path: False)

    # version DESC first, id ascending breaks the tie between "1" and "2".
    assert [row["id"] for row in _read(out)] == ["1", "2", "3"]


def test_merge_tolerates_a_missing_prior(tmp_path):
    fresh = [{"id": "1", "name": "a", "version": "2024-01-01"}]

    out = _merge(tmp_path, fresh, download_prior=lambda remote_key, local_path: False)

    assert _read(out) == fresh


@pytest.mark.parametrize("rows", [[], [{"id": "1", "name": "a", "version": "2024-01-01"}]])
def test_merge_writes_processing_checkpoint_with_rows_in_same_artifact(tmp_path, rows):
    metadata = {"spicy_regs.checkpoint": '{"source":"publisher\'s file", "rule":"α"}'}
    out = merge_table(
        tmp_path,
        name=NAME,
        columns=COLUMNS,
        identity=("id",),
        version_column="version",
        rows=rows,
        remote_key=REMOTE_KEY,
        prior_present=False,
        parquet_metadata=metadata,
    )
    assert _read(out) == rows
    assert pq.read_metadata(out).metadata[b"spicy_regs.checkpoint"] == metadata["spicy_regs.checkpoint"].encode()


def test_contract_merge_keeps_empty_success_checkpoint(tmp_path):
    from spicy_regs.transforms.table_merge import merge_contract_table

    out = merge_contract_table(
        tmp_path,
        "hearing_bill_links",
        [],
        prior_present=False,
        parquet_metadata={"spicy_regs.checkpoint": '{"outcome":"complete","rows":0}'},
    )
    assert pq.read_metadata(out).num_rows == 0
    assert pq.read_metadata(out).metadata[b"spicy_regs.checkpoint"] == b'{"outcome":"complete","rows":0}'


def test_composite_replacement_retires_only_successfully_read_file(tmp_path):
    columns = ("package_id", "file_name", "row_id", "value")
    rows = [
        {"package_id": "a", "file_name": "one.pdf", "row_id": "1", "value": "obsolete"},
        {"package_id": "a", "file_name": "two.pdf", "row_id": "1", "value": "failed read"},
        {"package_id": "b", "file_name": "one.pdf", "row_id": "1", "value": "other package"},
    ]
    pq.write_table(pa.Table.from_pylist(rows), prior_scratch_path(tmp_path, NAME))
    out = merge_table(
        tmp_path, name=NAME, columns=columns, identity=("package_id", "file_name", "row_id"),
        version_column=None, rows=[], remote_key=REMOTE_KEY, download_prior=_never_called,
        replace_parents=(("package_id", "file_name"), {("a", "one.pdf")}),
        parquet_metadata={"spicy_regs.checkpoint": '{"file":"one.pdf","rows":0}'},
    )
    assert _read(out) == rows[1:]
    assert b"spicy_regs.checkpoint" in pq.read_metadata(out).metadata


@pytest.mark.parametrize("scope", [(("id", "name"), {("1",)}), (("id", "id"), {("1", "1")})])
def test_invalid_replacement_scope_keeps_prior_bytes(tmp_path, scope):
    _write_prior(tmp_path, _PRIOR_ROWS)
    prior = prior_scratch_path(tmp_path, NAME)
    retained = prior.read_bytes()
    with pytest.raises(ValueError, match="replace_parents"):
        merge_table(
            tmp_path, name=NAME, columns=COLUMNS, identity=("id",), version_column="version",
            rows=[], remote_key=REMOTE_KEY, download_prior=_never_called, replace_parents=scope,
        )
    assert prior.read_bytes() == retained


#: A report-shaped table whose identity gained ``part_id`` after rows were published.
PARTS = ("package_id", "part_id", "body")


def _write_parts_prior(tmp_path: Path, prior: list[dict], prior_columns: tuple[str, ...]) -> Path:
    schema = pa.schema([(c, pa.string()) for c in prior_columns])
    pq.write_table(pa.Table.from_pylist(prior, schema=schema), prior_scratch_path(tmp_path, NAME))
    return prior_scratch_path(tmp_path, NAME)


def _merge_parts(tmp_path: Path, prior: list[dict], prior_columns: tuple[str, ...], **kwargs) -> list[dict]:
    _write_parts_prior(tmp_path, prior, prior_columns)
    out = merge_table(
        tmp_path, name=NAME, columns=PARTS, identity=("package_id", "part_id"), version_column=None,
        rows=[], remote_key=REMOTE_KEY, download_prior=_never_called, **kwargs,
    )
    return _read(out)


@pytest.mark.parametrize("coalesce_prior", [False, True], ids=["row-wise", "column-wise"])
def test_backfill_prior_fills_an_absent_identity_column_from_its_source(tmp_path, coalesce_prior):
    """A prior published before ``part_id`` keeps every row, each spelled as its package's one part."""
    prior = [{"package_id": "A", "body": "a"}, {"package_id": "B", "body": "b"}]
    rows = _merge_parts(
        tmp_path, prior, ("package_id", "body"),
        backfill_prior={"part_id": "package_id"}, coalesce_prior=coalesce_prior,
    )
    assert rows == [
        {"package_id": "A", "part_id": "A", "body": "a"},
        {"package_id": "B", "part_id": "B", "body": "b"},
    ]


@pytest.mark.parametrize("coalesce_prior", [False, True], ids=["row-wise", "column-wise"])
def test_backfill_prior_fills_a_null_and_leaves_a_set_value(tmp_path, coalesce_prior):
    prior = [{"package_id": "A", "part_id": None, "body": "a"}, {"package_id": "B", "part_id": "B-pt2", "body": "b"}]
    rows = _merge_parts(
        tmp_path, prior, PARTS, backfill_prior={"part_id": "package_id"}, coalesce_prior=coalesce_prior
    )
    assert rows == [
        {"package_id": "A", "part_id": "A", "body": "a"},
        {"package_id": "B", "part_id": "B-pt2", "body": "b"},
    ]


def test_without_backfill_prior_a_null_identity_part_drops_the_prior_row(tmp_path):
    """The hazard the backfill exists for: the identity test drops the row without a word."""
    prior = [{"package_id": "A", "body": "a"}]
    assert _merge_parts(tmp_path, prior, ("package_id", "body")) == []


@pytest.mark.parametrize("mapping", [{"part_id": "missing"}, {"missing": "package_id"}], ids=["source", "target"])
def test_backfill_prior_refuses_a_name_outside_the_columns_and_keeps_prior_bytes(tmp_path, mapping):
    prior = _write_parts_prior(tmp_path, [{"package_id": "A", "body": "a"}], ("package_id", "body"))
    retained = prior.read_bytes()
    with pytest.raises(ValueError, match="backfill_prior"):
        merge_table(
            tmp_path, name=NAME, columns=PARTS, identity=("package_id", "part_id"), version_column=None,
            rows=[], remote_key=REMOTE_KEY, download_prior=_never_called, backfill_prior=mapping,
        )
    assert prior.read_bytes() == retained


def test_merge_leaves_no_scratch_files_behind(tmp_path):
    _write_prior(tmp_path, _PRIOR_ROWS)
    _merge(tmp_path, [{"id": "1", "name": "new", "version": "2024-02-01"}])

    leftovers = sorted(p.name for p in tmp_path.glob(f"_{NAME}_*.parquet"))
    assert leftovers == []


def test_merge_refuses_null_identity_rows(tmp_path):
    fresh = [
        {"id": None, "name": "orphan", "version": "2024-01-01"},
        {"id": "1", "name": "a", "version": "2024-01-01"},
    ]

    out = _merge(tmp_path, fresh, download_prior=lambda remote_key, local_path: False)

    assert [row["id"] for row in _read(out)] == ["1"]


class _CountingDownload:
    """Counts calls; returns False (no prior found) without writing a file."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, remote_key: str, local_path: Path) -> bool:
        self.calls += 1
        return False


def test_merge_skips_download_prior_on_a_known_cold_start(tmp_path):
    """``prior_present=False`` is the caller saying "I already tried, and there's
    nothing there" — a cold-start regression: without it, a caller that already
    downloaded (and got nothing) pays for the same failed request twice, once for
    itself and once inside merge_table.
    """
    download_prior = _CountingDownload()
    fresh = [{"id": "1", "name": "a", "version": "2024-01-01"}]

    out = _merge(tmp_path, fresh, download_prior=download_prior, prior_present=False)

    assert download_prior.calls == 0
    assert _read(out) == fresh


def test_merge_still_asks_download_prior_when_presence_is_unknown(tmp_path):
    """The default (``prior_present`` omitted) is unchanged: no file on disk and
    no claim from the caller means merge_table asks ``download_prior`` itself.
    """
    download_prior = _CountingDownload()
    fresh = [{"id": "1", "name": "a", "version": "2024-01-01"}]

    out = _merge(tmp_path, fresh, download_prior=download_prior)

    assert download_prior.calls == 1
    assert _read(out) == fresh


def test_merge_null_fills_columns_the_prior_table_lacks(tmp_path):
    """A contract that gained columns merges onto a prior published without them.

    This is ``congress_bills``' live shape: ten frozen columns already
    published, thirty-eight appended by the bill family. The prior rows must
    survive with NULLs in the new columns rather than failing the binder, and a
    fresh row must still win on a repeated identity.
    """
    narrow = ("id", "name", "version")
    schema = pa.schema([(c, pa.string()) for c in narrow])
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"id": "1", "name": "old", "version": "2024-01-01"},
                {"id": "2", "name": "prior-only", "version": "2024-01-02"},
            ],
            schema=schema,
        ),
        prior_scratch_path(tmp_path, NAME),
    )

    wide = ("id", "name", "version", "stage", "stage_rule")
    out = merge_table(
        tmp_path,
        name=NAME,
        columns=wide,
        identity=("id",),
        version_column="version",
        rows=[{"id": "1", "name": "new", "version": "2024-02-01", "stage": "enacted", "stage_rule": "law_line"}],
        remote_key=REMOTE_KEY,
        download_prior=_never_called,
    )

    rows = {r["id"]: r for r in _read(out)}
    assert list(pq.read_table(out).schema.names) == list(wide)
    # The fresh row wins and carries the appended columns.
    assert rows["1"] == {
        "id": "1",
        "name": "new",
        "version": "2024-02-01",
        "stage": "enacted",
        "stage_rule": "law_line",
    }
    # The prior-only row survives, NULL-filled in the columns it predates.
    assert rows["2"] == {
        "id": "2",
        "name": "prior-only",
        "version": "2024-01-02",
        "stage": None,
        "stage_rule": None,
    }


# --------------------------------------------------------------------------- #
# A8/A9 (laws and rosters): scope replacement, and the statutes join on congress_bills.
# --------------------------------------------------------------------------- #
def _contract_rows(name: str, rows: list[dict]) -> list[dict]:
    from spicy_docs.schemas import TABLE_CONTRACTS

    columns = TABLE_CONTRACTS[name].columns
    return [{c: None for c in columns} | row for row in rows]


def _write(path: Path, name: str, rows: list[dict]) -> None:
    from spicy_docs.schemas import TABLE_CONTRACTS

    columns = TABLE_CONTRACTS[name].columns
    pq.write_table(
        pa.Table.from_pylist(_contract_rows(name, rows), schema=pa.schema([(c, pa.string()) for c in columns])), path
    )


def test_retire_prior_rows_drops_exactly_the_scope(tmp_path):
    from spicy_regs.transforms.table_merge import prior_scratch_path, retire_prior_rows

    prior = prior_scratch_path(tmp_path, "committee_assignments")
    _write(
        prior,
        "committee_assignments",
        [
            {"congress": "119", "chamber": "house", "system_code": "a", "bioguide_id": "1"},
            {"congress": "119", "chamber": "house", "system_code": "b", "bioguide_id": "2"},
            {"congress": "119", "chamber": "senate", "system_code": "c", "bioguide_id": "3"},
            {"congress": "118", "chamber": "house", "system_code": "a", "bioguide_id": "1"},
            {"congress": None, "chamber": "house", "system_code": "z", "bioguide_id": "9"},
        ],
    )
    assert retire_prior_rows(prior, congress="119", chamber="house") == 2
    kept = {(r["congress"], r["chamber"]) for r in pq.read_table(prior).to_pylist()}
    assert kept == {("119", "senate"), ("118", "house"), (None, "house")}
    with pytest.raises(ValueError, match="at least one scope column"):
        retire_prior_rows(prior)
    with pytest.raises(ValueError, match="not a snake_case identifier"):
        retire_prior_rows(prior, **{"congress; DROP": "1"})


def _seed_laws(tmp_path: Path, rows: list[dict]) -> None:
    from spicy_regs.transforms.table_merge import prior_scratch_path

    _write(prior_scratch_path(tmp_path, "laws"), "laws", rows)


def _no_download(remote_key: str, local_path: Path) -> bool:
    return False


def test_congress_bills_takes_its_citation_from_the_published_laws_table(tmp_path):
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import merge_contract_table

    _seed_laws(
        tmp_path,
        [
            {
                "law_id": "119-public-1",
                "congress": "119",
                "law_type": "public",
                "number": "1",
                "bill_id": "119-s-5",
                "statutes_at_large_cite": "139 Stat. 3",
                "update_date": "2026-07-30",
            },
            # A law whose PLAW lagged states no citation and fills nothing.
            {
                "law_id": "119-public-110",
                "congress": "119",
                "law_type": "public",
                "number": "110",
                "bill_id": "119-s-307",
                "update_date": "2026-09-18",
            },
        ],
    )
    fresh = _contract_rows(
        "congress_bills",
        [
            {
                "bill_id": "119-s-5",
                "congress": "119",
                "bill_type": "s",
                "bill_number": "5",
                "update_date": "2026-09-01",
            },
            {
                "bill_id": "119-s-307",
                "congress": "119",
                "bill_type": "s",
                "bill_number": "307",
                "update_date": "2026-09-01",
            },
            {
                "bill_id": "119-hr-1",
                "congress": "119",
                "bill_type": "hr",
                "bill_number": "1",
                "update_date": "2026-09-02",
            },
        ],
    )
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=_no_download)
    table = pq.read_table(out)
    assert table.schema.names == list(TABLE_CONTRACTS["congress_bills"].columns)
    rows = {r["bill_id"]: r for r in table.to_pylist()}
    assert rows["119-s-5"]["statutes_at_large_cite"] == "139 Stat. 3"
    assert rows["119-s-307"]["statutes_at_large_cite"] is None
    assert rows["119-hr-1"]["statutes_at_large_cite"] is None
    assert [r["bill_id"] for r in table.to_pylist()] == ["119-hr-1", "119-s-307", "119-s-5"], (
        "the merge's order is kept"
    )
    assert not (tmp_path / "_laws_prior.parquet").exists(), "the soft input's scratch copy is not left behind"


def test_a_writers_null_never_erases_a_citation_and_no_laws_table_leaves_it_as_it_was(tmp_path):
    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    _write(
        prior_scratch_path(tmp_path, "congress_bills"),
        "congress_bills",
        [
            {
                "bill_id": "119-s-5",
                "congress": "119",
                "update_date": "2026-09-01",
                "statutes_at_large_cite": "139 Stat. 3",
                "stage": "law",
            }
        ],
    )
    # The family's fresh row states no citation (the one-pass rule) and no laws table is published.
    fresh = _contract_rows(
        "congress_bills", [{"bill_id": "119-s-5", "congress": "119", "update_date": "2026-09-02", "title": "fresh"}]
    )
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=_no_download, prior_present=True)
    row = pq.read_table(out).to_pylist()[0]
    assert (row["statutes_at_large_cite"], row["stage"], row["title"]) == ("139 Stat. 3", "law", "fresh")


def test_a_recaptured_citation_in_laws_wins_over_the_one_already_here(tmp_path):
    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    _write(
        prior_scratch_path(tmp_path, "congress_bills"),
        "congress_bills",
        [
            {
                "bill_id": "119-s-5",
                "congress": "119",
                "update_date": "2026-09-01",
                "statutes_at_large_cite": "139 Stat. 2",
            }
        ],
    )
    _seed_laws(
        tmp_path,
        [
            {
                "law_id": "119-public-1",
                "congress": "119",
                "law_type": "public",
                "number": "1",
                "bill_id": "119-s-5",
                "statutes_at_large_cite": "139 Stat. 3",
                "update_date": "2026-07-30",
            }
        ],
    )
    out = merge_contract_table(tmp_path, "congress_bills", [], download_prior=_no_download, prior_present=True)
    assert pq.read_table(out).to_pylist()[0]["statutes_at_large_cite"] == "139 Stat. 3"


@pytest.mark.parametrize(
    "laws_file",
    ["truncated", "column_short"],
)
def test_a_laws_table_that_cannot_be_read_leaves_congress_bills_as_merged(tmp_path, laws_file):
    """The soft-input promise: a bad ``laws`` file is logged, not a failed bill-family run."""
    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    laws_path = prior_scratch_path(tmp_path, "laws")
    if laws_file == "truncated":
        laws_path.write_bytes(b"PAR1 not a parquet file")
    else:
        # A laws table that predates the citation column: the binder cannot find it.
        pq.write_table(
            pa.Table.from_pylist([{"bill_id": "119-s-5"}], schema=pa.schema([("bill_id", pa.string())])), laws_path
        )
    fresh = _contract_rows(
        "congress_bills", [{"bill_id": "119-s-5", "congress": "119", "update_date": "2026-09-01", "title": "as merged"}]
    )
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=_no_download)
    row = pq.read_table(out).to_pylist()[0]
    assert (row["title"], row["statutes_at_large_cite"]) == ("as merged", None)
    assert not laws_path.exists(), "the unreadable scratch copy is removed either way"
    assert not (tmp_path / "_congress_bills_cited.parquet").exists()


# --------------------------------------------------------------------------- #
# A table stored one file per partition (multi-file design §4.3).
# --------------------------------------------------------------------------- #
SECTIONS = ("bill_id", "seq", "body", "version", "congress")
BY_CONGRESS = Partitioning("congress", "bill_id", lambda bill: bill.split("-", 1)[0])


def _member(root: Path, congress: str, rows: list[dict], part: int = 0) -> Path:
    path = root / "sections" / f"congress={congress}" / f"part-{part:06d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = SECTIONS if all("congress" in row for row in rows) else SECTIONS[:-1]
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in columns])), path)
    return path


def _row(bill: str, seq: str, body: str, *, congress: bool = True) -> dict:
    return {"bill_id": bill, "seq": seq, "body": body, "version": "2026-01-01",
            **({"congress": bill.split("-", 1)[0]} if congress else {})}


def _merge_split(tmp_path: Path, rows: list[dict], prior: list[Path] | None, **kwargs) -> Path:
    return merge_partitioned_table(
        tmp_path, name="sections", columns=SECTIONS, identity=("bill_id", "seq"), version_column="version",
        rows=rows, partitioning=BY_CONGRESS, prior=prior, **kwargs,
    )


def _members(table: Path) -> dict[str, bytes]:
    return {path.relative_to(table).as_posix(): path.read_bytes() for path in sorted(table.rglob("*.parquet"))}


def _split_rows(table: Path) -> list[dict]:
    return sorted((row for path in sorted(table.rglob("*.parquet")) for row in pq.ParquetFile(path).read().to_pylist()),
                  key=lambda row: (row["bill_id"], row["seq"]))


def test_an_untouched_partition_keeps_its_bytes_and_a_rewritten_one_replaces_its_directory(tmp_path):
    """A prior part file left beside the rewritten one would publish its rows twice; an untouched one is not re-merged."""
    prior_dir = tmp_path / "prior"
    old = _member(prior_dir, "118", [_row("118-hr-1", "0", "kept")])
    held = old.read_bytes()
    prior = [old, _member(prior_dir, "119", [_row("119-hr-1", "0", "stale")]),
             _member(prior_dir, "119", [_row("119-hr-2", "0", "second part")], part=1)]
    stale = tmp_path / "sections" / "congress=117" / "part-000000.parquet"  # an earlier run's output, not a prior
    stale.parent.mkdir(parents=True)
    stale.write_bytes(held)

    out = _merge_split(tmp_path, [_row("119-hr-1", "0", "fresh")], prior)

    assert list(_members(out)) == ["congress=118/part-000000.parquet", "congress=119/part-000000.parquet"]
    assert _members(out)["congress=118/part-000000.parquet"] == held, "untouched: byte for byte, never re-merged"
    assert [(row["bill_id"], row["body"]) for row in _split_rows(out)] == [
        ("118-hr-1", "kept"), ("119-hr-1", "fresh"), ("119-hr-2", "second part")]


def test_a_replacement_scope_rewrites_only_the_partitions_it_names_or_all_when_it_cannot_name_them(tmp_path):
    prior = [_member(tmp_path / "p1", "118", [_row("118-hr-1", "0", "a")]),
             _member(tmp_path / "p1", "119", [_row("119-hr-1", "0", "b"), _row("119-hr-2", "0", "c")])]
    held = prior[1].read_bytes()
    out = _merge_split(tmp_path / "one", [], prior, replace_parents=("bill_id", {"118-hr-1"}))
    assert list(_members(out)) == ["congress=118/part-000000.parquet", "congress=119/part-000000.parquet"]
    assert _members(out)["congress=119/part-000000.parquet"] == held
    assert [row["bill_id"] for row in _split_rows(out)] == ["119-hr-1", "119-hr-2"], "118 kept its now-empty file"

    prior = [_member(tmp_path / "p2", "118", [_row("118-hr-1", "0", "a")]),
             _member(tmp_path / "p2", "119", [_row("119-hr-1", "0", "b"), _row("119-hr-2", "0", "c")])]
    out = _merge_split(tmp_path / "two", [], prior, replace_parents=("seq", {"0"}))
    assert _split_rows(out) == [] and len(_members(out)) == 2, "a scope without bill_id reaches every partition"


def test_the_first_split_derives_the_partition_column_and_keeps_every_row(tmp_path):
    """The prior is one file with no partition column: every row lands in its Congress's file, deriving the column."""
    rows = [_row(bill, "0", f"body of {bill}", congress=False) for bill in ("118-hr-1", "119-hr-1", "119-s-2")]
    rows.append({**rows[0], "seq": "1"})
    single = tmp_path / "_sections_prior" / "sections.parquet"
    single.parent.mkdir()
    pq.write_table(pa.Table.from_pylist(rows), single)

    out = _merge_split(tmp_path, [], [single])

    assert list(_members(out)) == ["congress=118/part-000000.parquet", "congress=119/part-000000.parquet"]
    for member in out.rglob("*.parquet"):
        congress = member.parent.name.removeprefix("congress=")
        read = pq.ParquetFile(member).read()
        assert read.schema.names == list(SECTIONS)
        assert set(read.column("congress").to_pylist()) == {congress}
        assert all(bill.startswith(congress + "-") for bill in read.column("bill_id").to_pylist())
    assert [{k: v for k, v in row.items() if k != "congress"} for row in _split_rows(out)] == sorted(
        rows, key=lambda row: (row["bill_id"], row["seq"]))
    assert not (tmp_path / "_sections_prior").exists() and not (tmp_path / "_sections_partitions").exists()


def test_a_prior_member_outside_its_partition_layout_refuses(tmp_path):
    loose = tmp_path / "sections" / "part-000000.parquet"
    loose.parent.mkdir()
    pq.write_table(pa.table({"bill_id": ["119-hr-1"]}), loose)
    with pytest.raises(ValueError, match="is not <name>/congress=<value>"):
        _merge_split(tmp_path / "out", [], [loose])


def test_published_members_downloads_once_and_reads_a_cold_start_as_none(tmp_path):
    calls = []

    def download(remote_key, directory):
        calls.append(remote_key)
        return [_member(directory, "119", [_row("119-hr-1", "0", "x")])] if remote_key == "sections.parquet" else []

    assert published_members(tmp_path, "absent", download) is None
    first = published_members(tmp_path, "sections", download)
    assert first == published_members(tmp_path, "sections", download) == [
        prior_members_path(tmp_path, "sections") / "sections" / "congress=119" / "part-000000.parquet"]
    assert calls == ["absent.parquet", "sections.parquet"], "a present prior is reused, never fetched twice"


def test_a_column_the_contract_gains_reaches_every_partition_not_only_the_rewritten_one(tmp_path):
    """A nightly rewriting the 119th must not leave the other partitions without the column: members share columns."""
    prior = [_member(tmp_path / "p", "118", [_row("118-hr-1", "0", "a")]),
             _member(tmp_path / "p", "119", [_row("119-hr-1", "0", "b")])]
    grown = (*SECTIONS, "extra")
    out = merge_partitioned_table(
        tmp_path / "out", name="sections", columns=grown, identity=("bill_id", "seq"), version_column="version",
        rows=[{**_row("119-hr-2", "0", "c"), "extra": "x"}], partitioning=BY_CONGRESS, prior=prior)

    assert {path.parent.name: pq.read_schema(path).names for path in out.rglob("*.parquet")} == {
        "congress=118": list(grown), "congress=119": list(grown)}
    assert [(row["bill_id"], row["extra"]) for row in _split_rows(out)] == [
        ("118-hr-1", None), ("119-hr-1", None), ("119-hr-2", "x")], "a prior row takes the new column as NULL"
    build_generation(tmp_path / "artifact", family="sections", files=[out], expected_keys=["sections.parquet"],
                     partitioned={"sections.parquet": ["congress"]})


def test_a_merge_that_fails_part_way_leaves_its_prior_whole_for_the_next_attempt(tmp_path, monkeypatch):
    """A re-run in the same directory reads the whole prior again, never a half-moved one (review S5: 2,659,863 to 1)."""
    rows = {"118": [_row("118-hr-1", "0", "a")], "119": [_row("119-hr-1", "0", "b"), _row("119-hr-2", "0", "c")]}

    def download(remote_key, directory):
        return [_member(directory, congress, held) for congress, held in rows.items()]

    real = table_merge.merge_table

    def fail_once(*args, **kwargs):
        monkeypatch.setattr(table_merge, "merge_table", real)
        raise RuntimeError("simulated failure on the partition merged")

    # The 118th is untouched and carried first; the failure comes after it, on the 119th's merge.
    monkeypatch.setattr(table_merge, "merge_table", fail_once)
    with pytest.raises(RuntimeError, match="simulated"):
        _merge_split(tmp_path, [_row("119-hr-9", "0", "new")], published_members(tmp_path, "sections", download))

    again = published_members(tmp_path, "sections", lambda *_: pytest.fail("the intact prior is reused"))
    out = _merge_split(tmp_path, [_row("119-hr-9", "0", "new")], again)
    assert [row["bill_id"] for row in _split_rows(out)] == ["118-hr-1", "119-hr-1", "119-hr-2", "119-hr-9"]


def test_a_download_that_fails_leaves_no_prior_to_be_read(tmp_path):
    def partial(remote_key, directory):
        _member(directory, "118", [_row("118-hr-1", "0", "a")])
        raise RuntimeError("connection reset after the first member")

    with pytest.raises(RuntimeError, match="connection reset"):
        published_members(tmp_path, "sections", partial)
    assert not prior_members_path(tmp_path, "sections").exists()

    def whole(remote_key, directory):
        return [_member(directory, "118", [_row("118-hr-1", "0", "a")]), _member(directory, "119", [_row("119-hr-1", "0", "b")])]

    assert [path.parent.name for path in published_members(tmp_path, "sections", whole) or []] == [
        "congress=118", "congress=119"]


def test_a_download_a_killed_run_left_part_way_is_discarded(tmp_path):
    """A kill skips every cleanup; the half-written download sits in ``.partial``, never where the prior is read."""
    killed = prior_members_path(tmp_path, "sections").with_name("_sections_prior.partial")
    _member(killed, "117", [_row("117-hr-1", "0", "from the killed run")])

    def whole(remote_key, directory):
        return [_member(directory, "118", [_row("118-hr-1", "0", "a")])]

    assert [path.parent.name for path in published_members(tmp_path, "sections", whole) or []] == ["congress=118"]
    assert not killed.exists()


def test_a_scope_over_a_partition_that_holds_nothing_writes_no_member(tmp_path):
    prior = [_member(tmp_path / "p", "118", [_row("118-hr-1", "0", "a")])]
    out = _merge_split(tmp_path, [], prior, replace_parents=("bill_id", {"120-hr-1"}))
    assert list(_members(out)) == ["congress=118/part-000000.parquet"]


def test_an_untouched_partition_of_several_parts_keeps_every_part(tmp_path):
    prior = [_member(tmp_path / "p", "118", [_row("118-hr-1", "0", "a")]),
             _member(tmp_path / "p", "118", [_row("118-hr-2", "0", "b")], part=1),
             _member(tmp_path / "p", "119", [_row("119-hr-1", "0", "c")])]
    held = {f"congress=118/{path.name}": path.read_bytes() for path in prior[:2]}
    out = _merge_split(tmp_path, [_row("119-hr-2", "0", "d")], prior)
    members = _members(out)
    assert list(members) == [*held, "congress=119/part-000000.parquet"]
    assert {key: members[key] for key in held} == held


def test_a_fresh_row_lands_in_the_partition_its_identity_names_whatever_it_states(tmp_path):
    """The partition column is the builder's to write, from the identity; a stale or missing statement cannot move a row."""
    stale = {**_row("119-hr-5", "0", "x"), "congress": "118"}
    missing = _row("119-hr-6", "0", "y", congress=False)
    out = _merge_split(tmp_path, [stale, missing], None)
    assert list(_members(out)) == ["congress=119/part-000000.parquet"]
    assert [row["congress"] for row in _split_rows(out)] == ["119", "119"]


def test_a_partition_whose_columns_are_the_contracts_in_another_order_is_rewritten(tmp_path):
    """Members share one ordered column list; the same names in another order would publish a second shape."""
    reordered = tmp_path / "p" / "sections" / "congress=118" / "part-000000.parquet"
    reordered.parent.mkdir(parents=True)
    pq.write_table(pa.table({c: ["118" if c == "congress" else "118-hr-1" if c == "bill_id" else "x"]
                             for c in reversed(SECTIONS)}), reordered)
    prior = [reordered, _member(tmp_path / "p", "119", [_row("119-hr-1", "0", "b")])]
    out = _merge_split(tmp_path, [_row("119-hr-2", "0", "c")], prior)
    assert {path.parent.name: pq.read_schema(path).names for path in out.rglob("*.parquet")} == {
        "congress=118": list(SECTIONS), "congress=119": list(SECTIONS)}
    build_generation(tmp_path / "artifact", family="sections", files=[out], expected_keys=["sections.parquet"],
                     partitioned={"sections.parquet": ["congress"]})


def test_every_part_of_a_partition_is_checked_for_the_contracts_columns(tmp_path):
    """A partition whose first part has the new column and whose second lacks it is merged, by name, into one."""
    grown = (*SECTIONS, "extra")
    first = tmp_path / "p" / "sections" / "congress=118" / "part-000000.parquet"
    first.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{**_row("118-hr-1", "0", "a"), "extra": "x"}],
                                        schema=pa.schema([(c, pa.string()) for c in grown])), first)
    prior = [first, _member(tmp_path / "p", "118", [_row("118-hr-2", "0", "b")], part=1)]
    out = merge_partitioned_table(
        tmp_path / "out", name="sections", columns=grown, identity=("bill_id", "seq"), version_column="version",
        rows=[], partitioning=BY_CONGRESS, prior=prior)
    assert list(_members(out)) == ["congress=118/part-000000.parquet"]
    assert [(row["bill_id"], row["extra"]) for row in _split_rows(out)] == [("118-hr-1", "x"), ("118-hr-2", None)]


def test_merge_local_prior_null_fills_a_column_the_prior_lacks(tmp_path):
    """Appending a column is a NULL backfill on published rows, as merge_table makes it, not a binder error."""
    import duckdb

    from spicy_regs.transforms.table_merge import merge_local_prior

    prior, new, out = tmp_path / "prior.parquet", tmp_path / "new.parquet", tmp_path / "out.parquet"
    pq.write_table(pa.table({"id": ["a", "b"], "n": [1, 2]}), prior)
    pq.write_table(pa.table({"id": ["b"], "n": [3], "added": ["x"]}), new)
    merge_local_prior(duckdb.connect(), columns=("id", "n", "added"), identity="id", order_by="id",
                      prior_file=prior, new_file=new, out_file=out)
    table = pq.read_table(out)
    assert table.schema.field("n").type == pa.int64()
    assert table.to_pylist() == [{"id": "a", "n": 1, "added": None}, {"id": "b", "n": 3, "added": "x"}]
