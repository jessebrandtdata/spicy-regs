"""Offline retained-input custody, refusal, reuse and ordinary typed consumption.

The small invented financial page checks integration mechanics. Actual source
accuracy is tested independently in SpicyDocs' retained source fixtures.
"""

import gzip
import hashlib
import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

import httpx
import pymupdf
import pytest

from spicy_docs.extraction import Raster
from spicy_docs.extraction.gemini import Gemini
from spicy_regs import mcp_server
from spicy_regs.pipelines.congressional_disbursement_pages import pin, processing_inputs, read_pages
from spicy_regs.pipelines.congressional_disbursement_tables import build
from spicy_regs.pipelines.congressional_disbursements import acquire_selection
from spicy_regs.schemas.disbursement_summaries import family_tables
from spicy_regs.source_evidence import verify_evidence
from spicy_regs.transforms.disbursement_pdf_tables import pages
from tests.generation_fakes import download_generation


def selected(tmp_path):
    outcome = {
        "page_label": "A-1",
        "headings": ["SUMMARY OF TRANSACTIONS BY APPROPRIATIONS ($)"],
        "tables": [
            {
                "title": "",
                "columns": [
                    "APPROPRIATION TITLE",
                    "NO.",
                    "FUNDS AVAILABLE AS OF April 1, 2021",
                    "FUNDING ADJUSTMENTS SUPPLEMENTALS",
                    "FUNDING ADJUSTMENTS TRANSFERS",
                    "FUNDING ADJUSTMENTS RESCISSIONS/ WITHDRAWALS",
                    "NET EXPENDITURES",
                    "REVOLVING FUND RECEIPTS",
                    "UNEXPENDED BALANCE AS OF September 30, 2021",
                ],
                "rows": [
                    {"context": "", "cells": ["TEST ACCOUNT", "0123", "", "", "", "", "", "", ""]},
                    {"context": "", "cells": ["2021", "", "123.45", "0.00", "-5.00", "0.00", "0.00", "", "118.45"]},
                ],
            }
        ],
        "other_text": [],
        "uncertainties": [],
    }
    with pymupdf.open() as document:
        page = document.new_page(width=1200, height=400)
        page.insert_text((30, 40), outcome["headings"][0])
        page.insert_text((30, 65), "TEST ACCOUNT 0123")
        page.insert_text((30, 90), "2021 | 123.45 | 0.00 | -5.00 | 0.00 | 0.00 | [blank] | 118.45")
        body = document.tobytes()
        pixmap = page.get_pixmap()
        image = Raster(pixmap.tobytes("png"), pixmap.width, pixmap.height)
    selection = {
        "schema": "congressional-disbursement-selection/1",
        "files": [
            {
                "id": "synthetic-senate-summary",
                "chamber": "senate",
                "format": "pdf",
                "kind": "full",
                "url": "https://www.govinfo.gov/content/pkg/GPO-CDOC-117sdoc6/pdf/GPO-CDOC-117sdoc6.pdf",
                "period_start": "2021-04-01",
                "period_end": "2021-09-30",
            }
        ],
    }
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps(selection))
    captured = acquire_selection(
        selection_path,
        tmp_path / "acquired",
        archive_root=tmp_path / "raw",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=body, headers={"Content-Type": "application/pdf"})
        ),
    )
    source = captured["files"][0]
    source.update(physical_page_count=1, reading_pages=[1], reading_scope="Synthetic offline host test")
    response = {
        "candidates": [
            {"finishReason": "STOP", "content": {"role": "model", "parts": [{"text": json.dumps(outcome)}]}}
        ],
        "modelVersion": "synthetic-test",
    }

    class Client:
        def generate(self, model, body):
            return response

    reading = Gemini(
        Client(),
        model="synthetic-test",
        mode="structured",
        prompt="Synthetic offline test",
        outcome_schema={"type": "object"},
    ).recognize(image)
    image_path = tmp_path / "input.png"
    image_path.write_bytes(image.data)
    observation = asdict(reading)
    observation["id"] = "full"
    observation["images"] = [
        {
            **pin(image_path),
            "sha256": hashlib.sha256(image.data).hexdigest(),
            "width": image.width,
            "height": image.height,
            "box": asdict(image.box),
        }
    ]
    result = {
        "metadata": {
            "source_sha256": hashlib.sha256(image.data).hexdigest(),
            "geometry": {"source_sha256": source["source_sha256"], "original_pdf_page": 1},
        },
        "observations": [observation],
        "tables": [],
    }
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(result))
    review_path = tmp_path / "synthetic-review.json"
    review_path.write_text(json.dumps({"scope": "Synthetic test expectation; no real model/source accuracy claim"}))
    q = {
        "schema": "source-qualified-structured-summary/1",
        "status": "accepted_observation",
        "source_sha256": source["source_sha256"],
        "source_byte_size": source["source_byte_size"],
        "page": 1,
        "physical_pages": 1,
        "result": {k: pin(result_path)[k] for k in ("sha256", "bytes")},
        "image": {k: pin(image_path)[k] for k in ("sha256", "bytes")},
        "counts": {"records": 1, "amounts": 6, "blanks": 1},
        "review_evidence": [pin(review_path)],
    }
    q_path = tmp_path / "qualification.json"
    q_path.write_text(json.dumps(q))
    source["structured_observations"] = [
        {"page": 1, "result": pin(result_path), "image": pin(image_path), "qualification": pin(q_path), "evidence": []}
    ]
    selection_path.write_text(json.dumps({**selection, "files": [source]}))
    return selection_path, source


def test_retained_input_to_existing_typed_download_and_mcp(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    selection, source = selected(tmp_path)
    result = read_pages(
        selection, tmp_path / "reading", chamber="senate", profile="financial-summary", archive_root=tmp_path / "raw"
    )
    assert result["complete"] and result["records"] == result["pages"] == 1
    output = Path(result["files"][0]["output"]["path"])
    stamp = output.stat().st_mtime_ns
    assert (
        read_pages(
            selection,
            tmp_path / "reading",
            chamber="senate",
            profile="financial-summary",
            archive_root=tmp_path / "raw",
        )
        == result
    )
    assert output.stat().st_mtime_ns == stamp
    with gzip.open(output, "rt") as stream:
        page = json.loads(next(stream))
    assert "words" not in page and page["structured_provenance"]["geometry"] is None
    assert page["records"][0]["source_origin"]["cells"]["revolving_fund_receipts"] == {
        "table": 0,
        "row": 1,
        "column": 7,
    }
    _, evidence = build(tmp_path / "reading/manifest.json", tmp_path / "typed")
    admitted = verify_evidence(evidence, archive_root=tmp_path / "raw")
    # Every declared input, including the source review, is retained: by value, or as a JSON copy cut of this
    # machine's folders whose ``local_sha256`` is the exact input's (ss-81bv).
    events = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    retained = {event.get(key) for event in events if event["event"] == "retained-file"
                for key in ("sha256", "local_sha256")}
    for item in processing_inputs(source):
        assert item["sha256"] in retained
    assert admitted.pin.artifact_digest
    _, tables, _ = family_tables("senate", "financial-summary")
    local = download_generation(monkeypatch, tmp_path / "typed/generation", evidence, tmp_path / "selected", tables)
    monkeypatch.setattr(mcp_server, "DATA_DIR", local)
    with mcp_server._build_connection() as connection:
        rows = connection.execute(
            "SELECT value_name,value_decimal,value_status,raw_value FROM senate_financial_summary_values ORDER BY value_index"
        ).fetchall()
        assert rows[0] == ("funds_available", Decimal("123.4500"), "parsed", "123.45")
        assert rows[2] == ("transfers", Decimal("-5.0000"), "parsed", "-5.00")
        assert rows[5] == ("revolving_fund_receipts", None, "blank", "")
        assert connection.execute(
            "SELECT native_word_count,native_text FROM senate_financial_summary_pages"
        ).fetchone() == (None, None)


@pytest.mark.parametrize("change", ["stale_image", "wrong_original", "unqualified", "wrong_page"])
def test_invalid_or_unqualified_input_cannot_reuse_a_prior_success(tmp_path, change):
    selection, source = selected(tmp_path)
    read_pages(
        selection, tmp_path / "reading", chamber="senate", profile="financial-summary", archive_root=tmp_path / "raw"
    )
    item = source["structured_observations"][0]
    if change == "stale_image":
        Path(item["image"]["path"]).write_bytes(b"changed")
    else:
        path = Path(item["qualification"]["path"])
        q = json.loads(path.read_text())
        if change == "wrong_original":
            q["source_sha256"] = "sha256:" + "0" * 64
        elif change == "wrong_page":
            q["page"] = 2
        else:
            q["status"] = "observed_unqualified"
        path.write_text(json.dumps(q))
        item["qualification"] = pin(path)
        value = json.loads(selection.read_text())
        value["files"] = [source]
        selection.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        read_pages(
            selection,
            tmp_path / "reused",
            chamber="senate",
            profile="financial-summary",
            archive_root=tmp_path / "raw",
            reuse_manifest=tmp_path / "reading/manifest.json",
        )


def test_qualification_is_part_of_source_resume_identity(tmp_path):
    selection, source = selected(tmp_path)
    first = read_pages(
        selection, tmp_path / "reading", chamber="senate", profile="financial-summary", archive_root=tmp_path / "raw"
    )
    item = source["structured_observations"][0]
    q_path = Path(item["qualification"]["path"])
    q = json.loads(q_path.read_text())
    q["note"] = "Additional reviewed scope explanation"
    q_path.write_text(json.dumps(q))
    item["qualification"] = pin(q_path)
    value = json.loads(selection.read_text())
    value["files"] = [source]
    selection.write_text(json.dumps(value))
    second = read_pages(
        selection,
        tmp_path / "second",
        chamber="senate",
        profile="financial-summary",
        archive_root=tmp_path / "raw",
        reuse_manifest=tmp_path / "reading/manifest.json",
    )
    assert second["complete"]
    assert "reused_from" not in second["files"][0]
    assert first["files"][0]["source_implementation"] != second["files"][0]["source_implementation"]


@pytest.mark.parametrize("field", ["configuration", "input_metadata", "response_model_version", "qualification"])
def test_typed_replay_refuses_changed_nonfinancial_provenance(tmp_path, field):
    selection, source = selected(tmp_path)
    result = read_pages(
        selection, tmp_path / "reading", chamber="senate", profile="financial-summary", archive_root=tmp_path / "raw"
    )
    entry = result["files"][0]
    assert len(list(pages(source, entry))) == 1
    with gzip.open(entry["output"]["path"], "rt") as stream:
        page = json.loads(next(stream))
    provenance = page["structured_provenance"]
    if field == "configuration":
        provenance[field]["model"] = "different-model"
    elif field == "input_metadata":
        provenance[field]["geometry"]["original_pdf_page"] = 99
    elif field == "response_model_version":
        provenance[field] = "different-response-model"
    else:
        provenance[field]["review_evidence"][0]["sha256"] = "sha256:" + "0" * 64
    changed = tmp_path / "changed.pages.jsonl.gz"
    with gzip.open(changed, "wt") as stream:
        stream.write(json.dumps(page) + "\n")
    entry["output"] = pin(changed)
    with pytest.raises(ValueError, match="exact retained input and approved corrections"):
        list(pages(source, entry))
