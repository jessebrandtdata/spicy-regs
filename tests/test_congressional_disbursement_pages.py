"""Full-page completion, unread content, retries and retained output integrity."""

import gzip
import json
from pathlib import Path

import httpx
import pymupdf
import pytest

from spicy_regs.pipelines.congressional_disbursement_pages import read_pages, retained_implementation
from spicy_regs.pipelines.congressional_disbursements import acquire_selection


def selected(tmp_path):
    with pymupdf.open() as document:
        document.new_page().insert_text((50, 50), "Unqualified table: 12.34")
        document.new_page()
        body = document.tobytes()
    path = tmp_path / "selection.json"
    value = {
        "schema": "congressional-disbursement-selection/1",
        "files": [
            {
                "id": "senate-fixture",
                "chamber": "senate",
                "format": "pdf",
                "kind": "full",
                "url": "https://www.govinfo.gov/content/pkg/GPO-CDOC-116sdoc10/pdf/GPO-CDOC-116sdoc10.pdf",
                "period_start": "2019-04-01",
                "period_end": "2019-09-30",
            }
        ],
    }
    path.write_text(json.dumps(value))
    captured = acquire_selection(
        path,
        tmp_path / "acquired",
        archive_root=tmp_path / "raw",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=body, headers={"Content-Type": "application/pdf"})
        ),
    )
    qualified = tmp_path / "qualified.json"
    qualified.write_text(json.dumps({**value, "files": captured["files"]}))
    return qualified


def test_every_page_is_retained_without_calling_unread_money_a_normalized_payment(tmp_path):
    path = selected(tmp_path)
    result = read_pages(path, tmp_path / "pages", chamber="senate", archive_root=tmp_path / "raw")
    assert result["complete"] and result["pages"] == 2 and result["records"] == 0
    retained = retained_implementation(result)
    assert set(retained) == {item["path"] for item in result["implementation"]["files"]}
    assert all(Path(item["path"]).is_relative_to(tmp_path / "pages/implementation") for item in retained.values())
    entry = result["files"][0]
    assert entry["page_statuses"] == {"non_ledger_content_pending": 2}
    output = Path(entry["output"]["path"])
    with gzip.open(output, "rt") as stream:
        pages = [json.loads(line) for line in stream]
    assert [page["page"] for page in pages] == [1, 2]
    assert "12.34" in pages[0]["text"]
    assert pages[0]["outside_ledger_word_indices"] == list(range(len(pages[0]["words"])))
    timestamp = output.stat().st_mtime_ns
    assert read_pages(path, tmp_path / "pages", chamber="senate", archive_root=tmp_path / "raw") == result
    assert output.stat().st_mtime_ns == timestamp
    reused = read_pages(
        path,
        tmp_path / "reused",
        chamber="senate",
        archive_root=tmp_path / "raw",
        reuse_manifest=tmp_path / "pages/manifest.json",
    )
    assert reused["complete"] and reused["files"][0]["output"] == entry["output"]
    assert reused["files"][0]["reused_from"]["sha256"] == reused["reuse_manifest"]["sha256"]
    assert not list((tmp_path / "reused").glob("*.jsonl.gz"))
    output.write_bytes(output.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="changed"):
        read_pages(path, tmp_path / "pages", chamber="senate", archive_root=tmp_path / "raw")


def test_partial_page_stream_is_an_explicit_retryable_failure(tmp_path, monkeypatch):
    from spicy_docs.reading import senate_disbursements

    path = selected(tmp_path)
    reader = senate_disbursements.iter_senate_disbursements_pages

    def incomplete(body, **kwargs):
        yield next(reader(body, **kwargs))

    monkeypatch.setattr(senate_disbursements, "iter_senate_disbursements_pages", incomplete)
    result = read_pages(path, tmp_path / "pages", chamber="senate", archive_root=tmp_path / "raw")
    assert not result["complete"] and result["pending"] == ["senate-fixture"]
    assert result["files"][0]["status"] == "read-failed"
    attempts = list((tmp_path / "pages").glob("*.attempt-*"))
    assert len(attempts) == 1
    monkeypatch.setattr(senate_disbursements, "iter_senate_disbursements_pages", reader)
    repaired = read_pages(path, tmp_path / "pages", chamber="senate", archive_root=tmp_path / "raw")
    assert repaired["complete"] and repaired["pages"] == 2
    assert attempts[0].exists()
