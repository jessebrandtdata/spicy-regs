"""Offline typed candidates preserve identity, order, field coverage and failures."""

import hashlib
import json
from datetime import date

import duckdb
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.federal_register import native
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.transforms.build_federal_register import build_federal_register_layer1
from tests.test_federal_register import _RAW_DOC


def document(**changes):
    return {**dict.fromkeys(native.DOCUMENT_FIELDS), **_RAW_DOC, "publication_date": "2024-03-01",
            "effective_on": "2024-04-01", "executive_order_number": "14101", **changes}


def inputs(tmp_path, records, *, repeated=False, body_changes=None):
    scope = {"publishedFrom": "2024-03-01", "publishedThrough": "2024-03-02"}
    url = native.federal_register_documents_url(scope)
    body = {"count": len(records), "total_pages": 1, "results": records, **(body_changes or {})}
    raw = json.dumps(body).encode()
    sha = "sha256:" + hashlib.sha256(raw).hexdigest()
    archive = tmp_path / "raw"
    CentralRawArchiveStore(archive).put_blob(sha, len(raw), [raw])
    page = {"url": url, "sha256": sha}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"query_scope": scope, "pages": [page, page] if repeated else [page]}))
    return manifest, archive, sha


def test_typed_source_values_and_children(tmp_path):
    raw = document(cfr_references=[{"title": 40, "part": 60, "chapter": "I", "citation_url": None},
                                    {"title": 40, "part": "60a", "chapter": 2, "citation_url": "https://example.org"}],
                   regulations_dot_gov_info={"docket_id": "EPA-HQ-OAR-2024-0001"})
    manifest, archive, sha = inputs(tmp_path, [raw])
    out = tmp_path / "candidate"
    build_federal_register_layer1(manifest, archive, out)
    with duckdb.connect() as con:
        assert con.execute("SELECT typeof(effective_on), effective_on, typeof(executive_order_number), "
                           "executive_order_number, source_record_id FROM read_parquet(?)", [str(out / "federal_register_documents.parquet")]).fetchone() == (
                               "DATE", date(2024, 4, 1), "BIGINT", 14101, "2024-00001@2024-03-01")
        assert con.execute("SELECT position, chapter, citation_url, typeof(chapter), typeof(citation_url) "
                           "FROM read_parquet(?) ORDER BY position", [str(out / "federal_register_cfr_references.parquet")]).fetchall() == [
                               (0, "I", None, "VARCHAR", "VARCHAR"), (1, "2", "https://example.org", "VARCHAR", "VARCHAR")]
    child = pq.read_table(out / "federal_register_agencies.parquet").to_pylist()
    assert [r["position"] for r in child] == [0, 1]
    assert all(r["source_sha256"] == sha and r["source_record_id"] == "2024-00001@2024-03-01" for r in child)
    assert child[1]["source_locator"] == "/results/0/agencies/1"
    receipt = json.loads((out / "inputs.json").read_text())
    assert all(c["present"] == 1 and c["absent"] == 0 and not c["unrequested"] for c in receipt["coverage"].values())
    row = pq.read_table(out / "federal_register_documents.parquet").to_pylist()[0]
    assert json.loads(row["regulations_dot_gov_info"])["docket_id"] == "EPA-HQ-OAR-2024-0001"
    assert pq.read_table(out / "federal_register_docket_candidates.parquet").to_pylist()[0]["docket_id"] == "EPA-HQ-OAR-2024-0001"


def test_dated_identity_duplicate_and_conflict(tmp_path):
    raw = document()
    records = [raw, dict(reversed(list(raw.items()))), document(publication_date="2024-03-02")]
    manifest, archive, _ = inputs(tmp_path, records)
    out = tmp_path / "candidate"
    build_federal_register_layer1(manifest, archive, out)
    assert pq.read_table(out / "federal_register_documents.parquet").num_rows == 2
    assert json.loads((out / "inputs.json").read_text())["duplicate_records"] == 1
    manifest, archive, _ = inputs(tmp_path, [raw, document(title="Conflicting title")])
    with pytest.raises(ValueError, match="Conflicting Federal Register"):
        build_federal_register_layer1(manifest, archive, tmp_path / "refused")
    assert not (tmp_path / "refused").exists()


@pytest.mark.parametrize("failure", ["cursor", "cyclic", "date", "inventory", "bounds", "extra"])
def test_refuses_incomplete_or_ambiguous_inputs(tmp_path, failure):
    record = document()
    if failure == "date":
        record["effective_on"] = "unknown"
    body_changes = {"count": 2} if failure == "inventory" else None
    if failure == "cyclic":
        scope = {"publishedFrom": "2024-03-01", "publishedThrough": "2024-03-02"}
        body_changes = {"next_page_url": native.federal_register_documents_url(scope)}
    manifest, archive, _ = inputs(tmp_path, [record], repeated=failure == "cursor", body_changes=body_changes)
    if failure == "extra":
        value = json.loads(manifest.read_text())
        value["pages"].append({**value["pages"][0], "url": "https://example.org/unused"})
        manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        build_federal_register_layer1(manifest, archive, tmp_path / "refused", max_records=0 if failure == "bounds" else 10000)
    assert not (tmp_path / "refused").exists()


def test_sparse_source_distinguishes_absent_from_null(tmp_path):
    record = document(action=None)
    del record["title"]
    manifest, archive, _ = inputs(tmp_path, [record])
    out = tmp_path / "candidate"
    build_federal_register_layer1(manifest, archive, out)
    row = pq.read_table(out / "federal_register_documents.parquet").to_pylist()[0]
    assert row["title"] is None and row["action"] is None
    assert "title" not in row["member_names"] and "action" in row["member_names"]
    coverage = json.loads((out / "inputs.json").read_text())["coverage"]
    assert coverage["title"] == {"present": 0, "null": 0, "populated": 0, "absent": 1, "unrequested": False}
    assert coverage["action"]["present"] == coverage["action"]["null"] == 1


@pytest.mark.parametrize("symlink", [False, True])
def test_refuses_output_in_raw_archive(tmp_path, symlink):
    manifest, archive, _ = inputs(tmp_path, [document()])
    selected = tmp_path / "raw-link" if symlink else archive
    if symlink:
        selected.symlink_to(archive, target_is_directory=True)
    before = sorted(p.relative_to(archive) for p in archive.rglob("*"))
    with pytest.raises(ValueError, match="outside the raw archive"):
        build_federal_register_layer1(manifest, archive, selected / "derived")
    assert sorted(p.relative_to(archive) for p in archive.rglob("*")) == before


def test_failed_write_leaves_no_output_and_retry_succeeds(tmp_path, monkeypatch):
    from spicy_regs.transforms import federal_register_layer1 as layer1

    manifest, archive, _ = inputs(tmp_path, [document()])
    out = tmp_path / "candidate"
    original = layer1.pq.write_table
    writes = []

    def interrupted(table, path, **options):
        writes.append(path)
        if len(writes) == 2:
            raise OSError("injected second-table write failure")
        original(table, path, **options)

    monkeypatch.setattr(layer1.pq, "write_table", interrupted)
    with pytest.raises(OSError, match="injected"):
        build_federal_register_layer1(manifest, archive, out)
    assert not out.exists() and not list(tmp_path.glob(".candidate-*"))
    monkeypatch.setattr(layer1.pq, "write_table", original)
    build_federal_register_layer1(manifest, archive, out)
    assert (out / "inputs.json").is_file()
    receipt = (out / "inputs.json").read_bytes()
    with pytest.raises(FileExistsError):
        build_federal_register_layer1(manifest, archive, out)
    assert (out / "inputs.json").read_bytes() == receipt


def test_competing_destination_is_preserved(tmp_path, monkeypatch):
    from spicy_regs.transforms import federal_register_layer1 as layer1

    manifest, archive, _ = inputs(tmp_path, [document()])
    out = tmp_path / "candidate"
    original = layer1.pq.write_table

    def competing_writer(table, path, **options):
        out.mkdir(exist_ok=True)
        (out / "other-owner").write_text("preserve")
        original(table, path, **options)

    monkeypatch.setattr(layer1.pq, "write_table", competing_writer)
    with pytest.raises(FileExistsError):
        build_federal_register_layer1(manifest, archive, out)
    assert list(out.iterdir()) == [out / "other-owner"]
    assert (out / "other-owner").read_text() == "preserve"
