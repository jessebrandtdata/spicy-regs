"""The votes trio publishes without per-row receipts (ss-vse3): members, roll-call-votes and member-vote-terms.

The fixtures are real rows of the generations of 2026-11-11 (members 26d652e1, roll-call-votes a77051b2,
member-vote-terms 27ebf57b), the last published with receipts: ``*.public.parquet`` is what the receipt route
published, ``*.legacy.parquet`` the builder's rows those receipts restore (the receipt route's prior), and
``member_votes.parquet`` both (it has no field outside the table). They hold the three roll calls whose
``member_vote_terms`` rows the rule leaves without a term, every member those roll calls name plus the five with
``other_names``, their terms, all 59 affiliations, and a sample of roll calls whose Senate documents omit
``nomination_id``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.congress_subjects import IDENTITIES, PROMOTED, subject_schema
from spicy_regs.congress_tables import as_builder_input, to_public
from spicy_regs.receipt_switch import receipt_files, table_differences

FIXTURES = Path(__file__).parent / "fixtures" / "votes_receipt_switch"
RETIRED = ("members", "member_terms", "member_party_affiliations", "roll_call_votes", "member_votes",
           "member_vote_terms")


def _shared(path: Path, columns: list[str], into: Path) -> Path:
    duckdb.sql(f"COPY (SELECT {', '.join(columns)} FROM read_parquet('{path}')) TO '{into}' (FORMAT PARQUET)")
    return into


@pytest.mark.parametrize("dataset", ["members", "member_terms", "member_party_affiliations", "roll_call_votes"])
def test_the_public_table_is_the_receipt_routes_with_its_promoted_fields_last(tmp_path, dataset):
    """The receipt route's columns are the same table; each promoted column holds its receipt copy, appended last."""
    public = tmp_path / "public.parquet"
    assert to_public(dataset, FIXTURES / f"{dataset}.legacy.parquet", public) == (0, [])
    published = pq.read_schema(FIXTURES / f"{dataset}.public.parquet").names
    names = pq.read_schema(public).names
    assert names == [*published, *PROMOTED[dataset]] and pq.read_schema(public).equals(subject_schema(dataset))
    shared = _shared(public, published, tmp_path / "shared.parquet")
    assert table_differences(shared, FIXTURES / f"{dataset}.public.parquet", key=IDENTITIES[dataset]) == []
    keys = " AND ".join(f"p.{key} = l.{key}" for key in IDENTITIES[dataset])
    for name in PROMOTED[dataset]:
        [(differing,)] = duckdb.sql(
            f"SELECT count(*) FROM read_parquet('{public}') p JOIN read_parquet('{FIXTURES}/{dataset}.legacy.parquet') l "
            f"ON {keys} WHERE CAST(p.{name} AS VARCHAR) IS DISTINCT FROM l.{name}").fetchall()
        assert differing == 0, name


@pytest.mark.parametrize("dataset", ["members", "member_terms", "member_party_affiliations", "roll_call_votes",
                                     "member_votes"])
def test_the_published_table_reads_back_as_the_builders_rows(tmp_path, dataset):
    """Public -> builder rows -> public is the identity; all but roll calls come back byte for byte as text."""
    legacy = FIXTURES / (f"{dataset}.legacy.parquet" if dataset != "member_votes" else "member_votes.parquet")
    public = tmp_path / "public.parquet"
    to_public(dataset, legacy, public)
    back = as_builder_input(dataset, [public], tmp_path / "back.parquet")
    again = tmp_path / "again.parquet"
    to_public(dataset, back, again)
    assert table_differences(again, public, key=IDENTITIES[dataset]) == []
    restored, read_back = pq.read_table(legacy), pq.read_table(back)
    assert read_back.column_names == restored.column_names
    if dataset != "roll_call_votes":
        assert read_back.equals(restored)
    else:
        # The one difference: a Senate document whose source omitted nomination_id reads back stating it null.
        differing = [name for name in restored.column_names if not read_back[name].equals(restored[name])]
        assert differing == ["documents_json"]


def test_a_rows_omitted_keys_survive_only_in_other_names_json(tmp_path):
    """Two of the five members' alias patches state a key as null; the struct cannot, the promoted text does."""
    public = tmp_path / "members.parquet"
    to_public("members", FIXTURES / "members.legacy.parquet", public)
    rows = [row for row in pq.read_table(public, columns=["other_names", "other_names_json"]).to_pylist()
            if row["other_names_json"]]
    assert len(rows) == 5
    import json
    stated_null = [patch for row in rows for patch in json.loads(row["other_names_json"])
                   if any(value is None for value in patch.values())]
    assert len(stated_null) == 2 and all(set(patch) != {"first", "middle", "last", "suffix", "nickname",
                                                        "official_full", "start", "end"} for patch in stated_null)


def test_member_vote_terms_leaves_out_the_votes_it_gives_no_term(tmp_path):
    """The builder's rows of the three roll calls: the receipt route's table, and the rows it rejected left out."""
    from spicy_regs.transforms.build_member_vote_terms import build_member_vote_terms

    work = tmp_path / "work"
    work.mkdir()
    (work / "member_votes.parquet").write_bytes((FIXTURES / "member_votes.parquet").read_bytes())
    (work / "member_terms.parquet").write_bytes((FIXTURES / "member_terms.legacy.parquet").read_bytes())
    built = build_member_vote_terms(work)
    public = tmp_path / "member_vote_terms.parquet"
    left_out, shown = to_public("member_vote_terms", built, public)
    assert (left_out, sorted(shown)) == (4, [("106-house-1-306", "name:Brown (CA)"), ("119-house-2-130", "S001157"),
                                             ("119-senate-2-258", "lis:S438"), ("119-senate-2-258", "lis:S439")])
    assert table_differences(public, FIXTURES / "member_vote_terms.public.parquet", key=("vote_id", "member_key")) == []


def test_the_pass_through_mapping_in_sql_is_the_row_mappers(tmp_path):
    """member_votes and member_vote_terms map in DuckDB; the per-row mapper gives the same table."""
    from spicy_regs import congress_tables

    for dataset, source in (("member_votes", FIXTURES / "member_votes.parquet"),
                            ("member_vote_terms", FIXTURES / "member_vote_terms.public.parquet")):
        legacy = as_builder_input(dataset, [source], tmp_path / f"{dataset}.legacy.parquet")
        schema = subject_schema(dataset)
        in_sql = pa.concat_tables(table for table, _ in congress_tables._mapped_in_sql(dataset, legacy, schema))
        in_python = pa.concat_tables(table for table, _ in congress_tables._mapped_in_python(dataset, legacy, schema))
        assert in_sql.equals(in_python) and in_sql.num_rows == pq.ParquetFile(source).metadata.num_rows


@pytest.mark.parametrize("dataset, column, value", [
    ("member_vote_terms", "term_index", "1st"),  # SQL path: a non-digit integer
    ("member_vote_terms", "vote_id", None),  # SQL path: no identity
    ("roll_call_votes", "conflict_count", "x"),  # row path: a promoted integer
])
def test_a_row_the_mapping_refuses_refuses_the_table(tmp_path, dataset, column, value):
    source = FIXTURES / ("member_vote_terms.public.parquet" if dataset == "member_vote_terms"
                         else "roll_call_votes.legacy.parquet")
    legacy = (as_builder_input(dataset, [source], tmp_path / "legacy.parquet") if dataset == "member_vote_terms"
              else source)
    table = pq.read_table(legacy)
    values = table[column].to_pylist()
    values[3] = value
    broken = tmp_path / "broken.parquet"
    pq.write_table(table.set_column(table.column_names.index(column), column, pa.array(values, pa.string())), broken)
    with pytest.raises(ValueError, match="row 3; nothing is published|row 3 \\(|source row 3"):
        to_public(dataset, broken, tmp_path / "public.parquet")
    assert not (tmp_path / "public.parquet").exists()


def test_a_published_table_without_its_promoted_columns_is_read_through_its_receipts(tmp_path):
    with pytest.raises(ValueError, match="predates its promoted columns"):
        as_builder_input("roll_call_votes", [FIXTURES / "roll_call_votes.public.parquet"], tmp_path / "x.parquet")
    with pytest.raises(ValueError, match="no reader of its published table"):
        as_builder_input("committees", [FIXTURES / "members.public.parquet"], tmp_path / "y.parquet")


def test_every_votes_table_is_retired_and_its_rollup_is_plain():
    from spicy_regs.pipelines.rollups.member_vote_terms import MemberVoteTermsRollup
    from spicy_regs.pipelines.rollups.members import MembersRollup
    from spicy_regs.pipelines.rollups.roll_call_votes import RollCallVotesRollup
    from spicy_regs.pipelines.rollups.subject_receipts import SubjectTableRollup
    from spicy_regs.subject_catalog import retired_receipt_datasets

    assert set(RETIRED) <= retired_receipt_datasets()
    for rollup in (MembersRollup, RollCallVotesRollup, MemberVoteTermsRollup):
        assert issubclass(rollup, SubjectTableRollup) and rollup.receipt_policies == ()
    with pytest.raises(ValueError, match="still publish receipts"):
        class Committees(SubjectTableRollup):  # noqa: F841
            name = "committee-rosters"
            outputs = ("committees.parquet",)


def test_the_terms_rollup_builds_a_generation_with_no_receipts(tmp_path, monkeypatch):
    """A local run beside the two published inputs: the generation holds the table and nothing else."""
    from spicy_regs.pipelines.rollups.member_vote_terms import MemberVoteTermsRollup

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    (tmp_path / "member_votes.parquet").write_bytes((FIXTURES / "member_votes.parquet").read_bytes())
    public_terms = tmp_path / "member_terms.parquet"
    to_public("member_terms", FIXTURES / "member_terms.legacy.parquet", public_terms)
    MemberVoteTermsRollup(output_dir=tmp_path, skip_upload=True).run()
    (generation,) = (tmp_path / "generations").iterdir()
    assert receipt_files(generation) == [] and not list(tmp_path.rglob("etl_receipts.parquet"))
    assert table_differences(generation / "member_vote_terms.parquet", FIXTURES / "member_vote_terms.public.parquet",
                             key=("vote_id", "member_key")) == []


def test_scorecard_analysis_reads_the_retired_votes_tables_as_published(tmp_path, monkeypatch):
    """Once the trio's families list no receipts, the analysis reads their published tables and links the same.

    Members, terms, roll calls and member votes come as a family without receipts under the real retirement marker;
    the same rows with receipts, read as before the switch, give the same links. Input pins differ, so they are left out.
    """
    from spicy_regs import subject_catalog
    from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
    from tests.conftest import VOTES_TRIO
    from tests.test_scorecard_analysis import _inputs, _serve

    def links(directory, **options):
        directory.mkdir()
        store, index = _inputs(directory, **options)
        _serve(monkeypatch, store)
        output = directory / "analysis"
        output.mkdir()
        rollup = ScorecardAnalysisRollup(output_dir=output)
        rollup._prime(output, index)
        tables = [pq.read_table(path) for path in rollup.build(output)]
        return set(rollup._official_receipts), [table.drop_columns([c for c in table.column_names if "pins" in c])
                                                 for table in tables]

    retired = subject_catalog.retired_receipt_datasets()
    with monkeypatch.context() as before:
        before.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired - VOTES_TRIO)
        with_receipts = links(tmp_path / "receipts")
    published = links(tmp_path / "published", published=frozenset({"members", "member_terms", "roll_call_votes",
                                                                    "member_votes"}))
    assert with_receipts[0] == {"members", "member_terms"} and published[0] == set()
    assert all(a.equals(b) for a, b in zip(with_receipts[1], published[1], strict=True))
    assert published[1][0].to_pylist()[0]["bioguide_id"] == "X000001"
