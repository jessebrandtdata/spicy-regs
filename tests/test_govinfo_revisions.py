"""Bounded revision refresh/replay acceptance through the maintained builders."""

from dataclasses import replace
from pathlib import Path
import shutil

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS

from spicy_regs.govinfo_revisions import DATED_TABLES, latest_package_rows
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

OLD = "2026-08-01T00:00:00Z"
NEW = "2026-09-01T00:00:00Z"


def prior_download(directory):
    def download(key, target):
        source = directory / key
        if not source.exists():
            return False
        shutil.copyfile(source, target)
        return True
    return download


def dated_row(name, stamp, **values):
    contract = TABLE_CONTRACTS[name]
    row = dict.fromkeys(contract.columns)
    row.update({key: "one" for key in contract.identity})
    return row | {"package_id": "pkg", "last_modified": stamp} | values


@pytest.mark.parametrize("name", sorted(DATED_TABLES))
def test_dated_keys_retain_revisions_through_rerun_old_replay_and_empty_refresh(tmp_path, name):
    previous = tmp_path / "cold"
    previous.mkdir()
    for index, stamp in enumerate((OLD, NEW, NEW, OLD, None)):
        output = tmp_path / str(index)
        output.mkdir()
        rows = [] if stamp is None else [dated_row(name, stamp)]
        scope = (("package_id", "last_modified"), {("pkg", stamp)}) if stamp else None
        merged = merge_contract_table(output, name, rows, download_prior=prior_download(previous),
                                      replace_parents=scope)
        held = pq.read_table(merged).to_pylist()
        assert {row["last_modified"] for row in held} == ({OLD} if index == 0 else {OLD, NEW})
        assert len(held) == (1 if index == 0 else 2)
        previous = output


@pytest.mark.parametrize("stamp", [None, "", "unknown", "2026-09-01", "2026-09-01T00:00:00", "2026-02-30T00:00:00Z"])
def test_unknown_source_dates_refuse_before_null_identity_filter(tmp_path, stamp):
    name = "budget_volumes"
    prior = dated_row(name, stamp)
    path = prior_scratch_path(tmp_path, name)
    pq.write_table(pa.Table.from_pylist([prior], schema=pa.schema([(c, pa.string()) for c in TABLE_CONTRACTS[name].columns])), path)
    original = path.read_bytes()
    with pytest.raises(ValueError, match="last_modified"):
        merge_contract_table(tmp_path, name, [], download_prior=lambda *_: False)
    assert path.read_bytes() == original
    assert not (tmp_path / f"{name}.parquet").exists()


def test_conflicting_same_revision_inputs_refuse_but_an_explicit_reread_corrects(tmp_path):
    name = "budget_volumes"
    with pytest.raises(ValueError, match="conflicting incoming"):
        merge_contract_table(tmp_path, name, [dated_row(name, OLD, title="A"), dated_row(name, OLD, title="B")],
                             download_prior=lambda *_: False)
    first = merge_contract_table(tmp_path, name, [dated_row(name, OLD, title="A", text_sha256="old")],
                                 download_prior=lambda *_: False)
    first.rename(prior_scratch_path(tmp_path, name))
    corrected = merge_contract_table(tmp_path, name, [dated_row(name, OLD, title="B", text_sha256="corrected")],
                                     download_prior=lambda *_: False)
    assert [(r["title"], r["text_sha256"]) for r in pq.read_table(corrected).to_pylist()] == [("B", "corrected")]


def test_report_and_hearing_children_stay_with_their_revision_and_latest_checkpoint(tmp_path):
    from tests.test_committee_reports import (_package, _parts, _Page, StubHearings, CRPT_ID, CHRG_ID)
    from spicy_regs.transforms.build_committee_reports import build_committee_reports
    from spicy_regs.transforms.committee_report_reads import prior_reads, READS_TABLE

    class Discovery:
        stamp = OLD
        empty = False
        def packages(self, url, **_):
            package = CRPT_ID if "/CRPT/" in url else CHRG_ID
            yield _Page([] if self.empty else [{"packageId": package, "lastModified": self.stamp}])

    class Bodies:
        stamp = OLD
        def dated(self, body):
            return replace(body, summary=replace(body.summary, last_modified=self.stamp))
        def acquire_parts(self, package_id, **_):
            original = Path(__file__).parent / "fixtures/adoption_0_24_0/CRPT-118hrpt53.txt"
            text = (b"<html><body><pre>" + original.read_bytes() + b"</pre></body></html>" if self.stamp == OLD
                    else b"<html><body><pre>No heading remains.</pre></body></html>")
            return tuple(self.dated(body) for body in _parts(package_id, body=text))
        def acquire(self, package_id, **_):
            body = _package(package_id)
            mods = replace(body.mods, bills=(replace(body.mods.bills[0], context="COVER"),)) if self.stamp == OLD else replace(body.mods, bills=())
            return self.dated(replace(body, mods=mods))

    discovery, bodies = Discovery(), Bodies()
    previous = tmp_path / "absent"
    for index, stamp in enumerate((OLD, NEW, NEW, OLD, None)):
        output = tmp_path / str(index)
        output.mkdir()
        discovery.empty = stamp is None
        if stamp:
            discovery.stamp = bodies.stamp = stamp
        paths = {p.stem: p for p in build_committee_reports(output, reader=discovery, acquirer=bodies,
                 hearings=StubHearings(), download_prior=prior_download(previous))}
        if index:
            reports = pq.read_table(paths["committee_reports"]).to_pylist()
            assert {r["last_modified"] for r in reports} == {OLD, NEW}
            links = pq.read_table(paths["hearing_bill_links"]).to_pylist()
            assert links and {r["last_modified"] for r in links} == {OLD}
            hearings = pq.read_table(paths["hearing_transcripts"]).to_pylist()
            assert {r["last_modified"] for r in hearings} == {OLD, NEW}
            with duckdb.connect() as con:
                for table in ("committee_reports", "report_sections", "committee_report_cbo_letters", "hearing_transcripts", "hearing_bill_links"):
                    con.from_parquet(str(paths[table])).create_view(table)
                assert con.sql("SELECT count(*) FROM hearing_bill_links l JOIN hearing_transcripts h USING(package_id,last_modified)").fetchall()[0][0] == len(links)
                sections = con.sql("SELECT count(*) FROM report_sections").fetchall()[0][0]
                assert con.sql("SELECT count(*) FROM report_sections s JOIN committee_reports r USING(package_id,part_id,last_modified)").fetchall()[0][0] == sections
                letters = con.sql("SELECT last_modified FROM committee_report_cbo_letters").fetchall()
                assert letters and {row[0] for row in letters} == {OLD}
                assert con.sql("SELECT count(*) FROM committee_report_cbo_letters l JOIN committee_reports r USING(package_id,part_id,last_modified)").fetchall()[0][0] == len(letters)
                current = latest_package_rows("committee_reports")
                assert con.sql(f"SELECT count(*) FROM ({current})").fetchall()[0][0] == 1
            checkpoints = prior_reads(paths[READS_TABLE], {})
            assert {r["last_modified"] for r in checkpoints.values()} == {NEW}
        previous = output
