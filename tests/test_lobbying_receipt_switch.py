"""The lobbying family across the move away from per-row receipts (ss-vse3), on a cut of real data.

``fixtures/lobbying_receipt_switch`` (``index.json`` is the live index entry pointed at the cut files):

- ``prior/``: rows of a few filings as generation 233aef1f (2026-11-10T19:36:38Z, the last with receipts) published
  them, in all eight tables, with their receipts at the same positions, so each cut pair still admits. One filing is
  among the ten whose ``url`` is an API page address, not a document page.
- ``window/``: the builder's own files for a few filings of the real window read on 2026-11-11 (posted 2026-11-02 to
  2026-11-10): some also in ``prior/`` (refetched), some new, and one of each rare list.
- ``expected/``: the eight tables main's receipt route (000770ea) published from them.

What this cannot see: 2 million rows (the full-size equality is in docs/lobbying-receipt-archive.md), the walk and
pages themselves (tests/test_lobbying_filings.py), and Parquet layout.
"""

import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

FIXTURE = Path(__file__).parent / "fixtures/lobbying_receipt_switch"
TABLES = ("lobbying_filings", "lobbying_activities", "lobbying_activity_lobbyists", "lobbying_foreign_entities",
          "lobbying_affiliated_organizations", "lobbying_conviction_disclosures", "lobbying_registrants",
          "lobbying_clients")
PROMOTED = {"lobbying_filings": "url", "lobbying_registrants": "url", "lobbying_clients": "url"}


def night(monkeypatch, tmp_path, index, members, *, receipts_allowed):
    """One nightly build on ``index``, its members read from ``members``, the window frozen."""
    import spicy_regs.transforms.build_lobbying_filings  # noqa: F401  (the package re-exports the function)
    import sys

    from spicy_regs.sources import publication

    bld = sys.modules["spicy_regs.transforms.build_lobbying_filings"]

    def fetch_member(base_url, member, local_path, label=None, **_):
        name = Path(member.path).name
        if name == "etl_receipts.parquet" and not receipts_allowed:
            pytest.fail("a night after the first reads no receipts")
        shutil.copyfile(members / name, local_path)
        return True

    def frozen(filings, output_dir):
        paths = {}
        for key, _, _ in (*bld._TABLES, *bld._PROFILES):
            name = f"_{key.removesuffix('.parquet')}_new.parquet"
            shutil.copyfile(FIXTURE / "window" / name, Path(output_dir) / name)
            paths[key] = Path(output_dir) / name
        return paths

    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.invalid")
    monkeypatch.setattr(publication, "current_index", lambda *a, **k: index)
    monkeypatch.setattr(publication, "fetch_member", fetch_member)
    monkeypatch.setattr(bld, "_write_fresh", frozen)
    out = tmp_path / "night"
    out.mkdir()
    return {path.stem: path for path in bld.build_lobbying_filings(out)}


def receipt_values(path, dataset, field):
    """``field`` of each accepted receipt of ``dataset``'s raw_record, in subject order."""
    from spicy_regs.etl_receipts import _unpack

    rows = pq.read_table(path, filters=[("dataset", "=", dataset), ("outcome", "=", "accepted")]).to_pylist()
    return [dict(_unpack(json.loads(row["processing_json"]))["raw_record"])[field] for row in rows]


def test_the_first_night_equals_the_receipt_route_and_promotes_url_as_received(tmp_path, monkeypatch):
    index = json.loads((FIXTURE / "index.json").read_text())
    built = night(monkeypatch, tmp_path, index, FIXTURE / "prior", receipts_allowed=True)
    for table in TABLES:
        expected, actual = pq.read_table(FIXTURE / f"expected/{table}.parquet"), pq.read_table(built[table])
        promoted = [PROMOTED[table]] if table in PROMOTED else []
        assert actual.schema.names == [*expected.schema.names, *promoted]
        # Every existing column, every row, in order, as the receipt route published them.
        assert actual.select(expected.schema.names).equals(expected), table
    # url: a carried row's as its last receipt held it; a fetched row's as this night's window states it.
    for table, column in PROMOTED.items():
        carried = dict(zip(pq.read_table(FIXTURE / f"prior/{table}.parquet").column(0).to_pylist(),
                           receipt_values(FIXTURE / "prior/etl_receipts.parquet", table, column), strict=True))
        fresh = pq.read_table(FIXTURE / f"window/_{table}_new.parquet").to_pylist()
        fetched = {row[pq.read_schema(built[table]).names[0]]: row[column] for row in fresh}
        for row in pq.read_table(built[table]).to_pylist():
            key = row[pq.read_schema(built[table]).names[0]]
            assert row[column] == (fetched[key] if key in fetched else carried[key]), (table, key)
            assert row[column] is not None
    api_page = [row for row in pq.read_table(built["lobbying_filings"]).to_pylist() if "/api/v1/filings/?" in row["url"]]
    assert len(api_page) == 1  # kept as received, not rebuilt from filing_uuid


def test_the_second_night_reads_no_receipts_and_carries_the_first(tmp_path, monkeypatch):
    import hashlib

    first_dir = tmp_path / "first"
    first_dir.mkdir()
    index = json.loads((FIXTURE / "index.json").read_text())
    with monkeypatch.context() as first_night:
        first = night(first_night, first_dir, index, FIXTURE / "prior", receipts_allowed=True)
    entry = index["families"]["lobbying-filings"]
    del entry["etlReceipts"]
    for table, path in first.items():
        body = path.read_bytes()
        entry["tables"][f"{table}.parquet"].update(sha256="sha256:" + hashlib.sha256(body).hexdigest(),
                                                   byteSize=len(body), rows=pq.read_metadata(path).num_rows)
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    second = night(monkeypatch, second_dir, index, first_dir / "night", receipts_allowed=False)
    # The same window applied again changes nothing.
    for table in TABLES:
        assert pq.read_table(second[table]).equals(pq.read_table(first[table])), table
