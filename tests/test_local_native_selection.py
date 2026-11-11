"""Local consumers read the captured native selection, never candidate copies."""
from argparse import Namespace

import pyarrow.parquet as pq
import pytest

from spicy_regs import cli, mcp_server
from spicy_regs.congress_receipts import write_congress_dataset
from spicy_regs.local_data import local_selection
from spicy_regs.selected_generations import SelectedDataset, remember_selection
from tests.test_congress_receipts import shaped
from tests.test_subject_receipt_rollups import MixedRollup, mixed_builder

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def selected_family(root, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    pipeline = MixedRollup(output_dir=root)
    pipeline.build_receipts(root, mixed_builder)
    return local_selection(root)


def connection(root, monkeypatch):
    monkeypatch.setattr(mcp_server, "DATA_DIR", root)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    return mcp_server._build_connection()


def test_refused_candidate_and_loose_files_cannot_replace_selected_cli_rows(tmp_path, monkeypatch, capsys):
    selected = selected_family(tmp_path, monkeypatch)
    original = pq.read_table(selected.paths("members")[0]).to_pylist()

    def bad(work, **kwargs):
        members, law, archive = mixed_builder(work, **kwargs)
        shaped(members, [{"bioguide_id": "X", "fec_ids_json": "broken"}])
        return members, law, archive

    with pytest.raises(ValueError, match="conversion refused"):
        MixedRollup(output_dir=tmp_path).build_receipts(tmp_path, bad)
    # Even a separate writer's broken convenience copy cannot redirect a consumer.
    shaped(tmp_path / "members.parquet", [{"bioguide_id": "unselected-candidate"}])
    shaped(tmp_path / "extra.parquet", [{"value": "unselected"}])
    current = local_selection(tmp_path)
    assert pq.read_table(current.paths("members")[0]).to_pylist() == original
    assert "extra" not in current.files
    cli.cmd_sample(Namespace(output_dir=str(tmp_path), data_type="members", agency=None, n=1))
    output = capsys.readouterr().out
    assert "native-selected" in output and "unselected-candidate" not in output
    with pytest.raises(SystemExit):
        cli.cmd_sample(Namespace(output_dir=str(tmp_path), data_type="extra", agency=None, n=1))


def test_mcp_captures_multiple_generations_and_scopes_shared_receipts(tmp_path, monkeypatch):
    old = selected_family(tmp_path, monkeypatch)
    con = connection(tmp_path, monkeypatch)
    raw = shaped(tmp_path / "next-source.parquet", [{"bioguide_id": "Y", "fec_ids_json": "[]"}])
    subject, receipt = write_congress_dataset(raw, tmp_path / "next", dataset="members", generation_id="next")
    assert subject is not None
    remember_selection(tmp_path, [SelectedDataset("members", (subject,), receipt, "next")])
    newer = connection(tmp_path, monkeypatch)
    try:
        assert con.execute("SELECT bioguide_id FROM members").fetchall() == [("X",)]
        assert newer.execute("SELECT bioguide_id FROM members").fetchall() == [("Y",)]
        assert newer.execute("SELECT law_id FROM laws").fetchall() == [("119-public-1",)]
        assert newer.execute("SELECT DISTINCT generation_id FROM etl_receipts WHERE dataset='members'").fetchall() == [("next",)]
        assert newer.execute("SELECT DISTINCT generation_id FROM etl_receipts WHERE dataset='laws'").fetchall() == [(old.native["laws"].generation_id,)]
        assert mcp_server._publication_status(newer)["publication"]["members"]["status"] == "native_selected"
    finally:
        con.close()
        newer.close()


@pytest.mark.parametrize("damage", ["subject", "receipt", "generation", "policy"])
def test_native_selection_refuses_damage_without_convenience_fallback(tmp_path, monkeypatch, damage):
    selected = selected_family(tmp_path, monkeypatch)
    member = selected.native["members"]
    if damage == "policy":
        import pyarrow as pa
        from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest
        rows = pq.read_table(member.receipts).to_pylist()
        for row in rows:
            if row["dataset"] == "members":
                row["policy_version"] = "undeclared"
                row["receipt_id"] = _digest({k: v for k, v in row.items() if k != "receipt_id"})
        changed = tmp_path / "different-policy.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), changed)
        remember_selection(tmp_path, [SelectedDataset("members", member.subjects, changed, member.generation_id)])
    elif damage == "generation":
        remember_selection(tmp_path, [SelectedDataset("members", member.subjects, member.receipts, "")])
    else:
        path = member.subjects[0] if damage == "subject" else member.receipts
        path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="changed|generation|policy"):
        local_selection(tmp_path)


def test_native_multipart_selection_reads_all_members(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    parts, receipts = [], []
    for key in ("A", "B"):
        raw = shaped(tmp_path / f"{key}.parquet", [{"bioguide_id": key, "fec_ids_json": "[]"}])
        subject, receipt = write_congress_dataset(raw, tmp_path / key, dataset="members", generation_id="parts")
        parts.append(subject)
        receipts.append(receipt)
    # Each member's source metadata receipt has identical identity: retain it once.
    import pyarrow as pa
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA
    rows = {row["receipt_id"]: row for path in receipts for row in pq.read_table(path).to_pylist()}
    shared = tmp_path / "shared.parquet"
    pq.write_table(pa.Table.from_pylist(list(rows.values()), schema=RECEIPT_SCHEMA), shared)
    remember_selection(tmp_path, [SelectedDataset("members", tuple(parts), shared, "parts")])
    con = connection(tmp_path, monkeypatch)
    try:
        assert con.execute("SELECT bioguide_id FROM members ORDER BY bioguide_id").fetchall() == [("A",), ("B",)]
    finally:
        con.close()


@pytest.mark.parametrize("surface", ["current", "download.json"])
def test_native_and_download_selection_require_explicit_directory(tmp_path, monkeypatch, surface):
    selected_family(tmp_path, monkeypatch)
    (tmp_path / surface).write_text("unselected")
    with pytest.raises(RuntimeError, match="Ambiguous local selection"):
        local_selection(tmp_path)


def test_empty_native_table_has_its_declared_columns(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    raw = shaped(tmp_path / "raw.parquet", [])
    _, receipts = write_congress_dataset(raw, tmp_path / "empty", dataset="members", generation_id="empty")
    remember_selection(tmp_path, [SelectedDataset("members", (), receipts, "empty")])
    con = connection(tmp_path, monkeypatch)
    try:
        assert con.execute("SELECT bioguide_id FROM members").fetchall() == []
    finally:
        con.close()


@pytest.mark.parametrize("damage", [None, "unknown_schema", "mixed_schema"])
def test_earlier_gao_selection_reads_original_fields_and_refuses_unknown_or_mixed_schemas(tmp_path, monkeypatch, damage):
    import duckdb
    import pyarrow as pa
    from spicy_regs.etl_receipts import ReceiptContext, receipt_policies, write_dataset
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    current, *historical = receipt_policies(installed_policies()["gao_reports"])
    earlier = next(policy for policy in historical if policy.policy_version == "government-sources/1")
    original = {**dict.fromkeys(earlier.subject_schema.names), "report_id": "gao-26-1", "title": "As published"}
    witness = {"source_id": "original", "source_uri": None, "sha256": "sha256:" + "0" * 64,
               "locator": None, "body_version": None}
    context = ReceiptContext("original", "report", "original-reader", [witness])
    subject, receipts = write_dataset([({**original, "raw_record": {"literal": "unchanged"}}, context)],
                                     tmp_path / "old", earlier)
    assert subject is not None
    from spicy_regs.generations import build_generation
    # A new artifact declaring /2 must never admit the older subject shape.
    with pytest.raises(ValueError, match="Subject schema differs"):
        build_generation(tmp_path / "false-current", family="gao-reports", files=[subject],
                         expected_keys=[subject.name], receipt_path=receipts, receipt_policies=[current],
                         receipt_generation_id="original")
    paths = [subject]
    if damage == "unknown_schema":
        pq.write_table(pq.read_table(subject).append_column("unknown", pa.array(["value"])), subject)
    elif damage == "mixed_schema":
        newer, _ = write_dataset([({**dict.fromkeys(current.subject_schema.names), **original,
                                  "report_id": "gao-26-2"}, context)], tmp_path / "new", current)
        assert newer is not None
        paths.append(newer)
    remember_selection(tmp_path, [SelectedDataset("gao_reports", tuple(paths), receipts, "original")])
    if damage:
        with pytest.raises(ValueError, match="schema|Mixed subject policies"):
            local_selection(tmp_path)
        return
    selection = local_selection(tmp_path)
    selected = selection.native["gao_reports"]
    assert pq.read_table(selection.paths("gao_reports")[0]).to_pylist() == [original]
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {}, "", local_native={"gao_reports": {
            "subjects": selected.subjects, "receipts": selected.receipts, "generation_id": selected.generation_id}})
        assert list(adapter.selected_rows("gao_reports")) == [{**original, "raw_record": {"literal": "unchanged"}}]
