"""Offline processing-boundary tests; the source adapter owns raw financial fixtures."""

import json
from pathlib import Path

import httpx
import pymupdf
import pytest

from spicy_docs.extraction import Box, ExtractionError, Recognition, TextBlock
from spicy_regs.pipelines.congressional_disbursement_ocr import OcrBudget, read_ocr_pages, read_ocr_selection
from spicy_regs.pipelines.congressional_disbursement_pages import pin
from spicy_regs.pipelines.congressional_disbursements import acquire_selection

LAYOUT = {"backend": "synthetic-positioned-test"}
TEXT = {"backend": "synthetic-text-test"}
DETAILS = [f"01-23 C1 VOUCHER{i} PAYEE{i} 12/31/07 12/31/07 SUPPLIES {i}.00" for i in range(1, 4)]


class Backend:
    def __init__(self, *, layout=False, failures=0, configuration=None):
        self.layout, self.failures, self.calls = layout, failures, 0
        self.config = configuration or (LAYOUT if layout else TEXT)

    def configuration(self):
        return {key: value for key, value in self.config.items() if key != "media_type"}

    def recognize(self, image):
        self.calls += 1
        if self.failures:
            self.failures -= 1
            raise ExtractionError("synthetic model interruption", details={"partial_response": "kept raw"})
        if self.layout:
            blocks = [TextBlock("DESCRIPTION", Box(0.6, 0.03, 0.8, 0.06))]
            for number, text in enumerate(DETAILS, 1):
                label, amount = text.rsplit(" ", 1)
                blocks.extend(
                    (
                        TextBlock(label, Box(0.05, number * 0.2, 0.8, number * 0.2 + 0.03)),
                        TextBlock(amount, Box(0.9, number * 0.2, 0.95, number * 0.2 + 0.03)),
                    )
                )
            return Recognition(
                "\n".join(block.text for block in blocks),
                self.config,
                {"synthetic": True},
                blocks=tuple(blocks),
                images=(image,),
            )
        return Recognition("\n".join(DETAILS), self.config, {"synthetic": True}, images=(image,))


def selection(tmp_path, *, chamber="house", profile="house-2008"):
    with pymupdf.open() as document:
        for number in range(5):
            page = document.new_page(width=200, height=120)
            page.insert_text((12, 20), f"Synthetic processing test page {number + 1}", fontsize=6)
        body = document.tobytes()
    source_path = tmp_path / "sources.json"
    source_path.write_text(
        json.dumps(
            {
                "schema": "congressional-disbursement-selection/1",
                "files": [
                    {
                        "id": "house-test",
                        "chamber": chamber,
                        "format": "pdf",
                        "kind": "part",
                        "url": "https://archive.org/download/ocr-fixture/ocr-fixture.pdf",
                        "period_start": "2009-07-01",
                        "period_end": "2009-09-30",
                    }
                ],
            }
        )
    )
    capture = acquire_selection(
        source_path,
        tmp_path / "acquisition",
        archive_root=tmp_path / "raw",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=body, headers={"Content-Type": "application/pdf"})
        ),
    )
    item = capture["files"][0]
    qualified = tmp_path / "qualified.json"
    qualified.write_text(
        json.dumps(
            {
                "schema": "congressional-disbursement-selection/1",
                "files": [
                    {**item, "normalization_qualification": {"physical_pages": 5}},
                ],
            }
        )
    )
    evidence = tmp_path / "synthetic-review.json"
    evidence.write_text('{"scope":"synthetic processing fixture, no real source qualification"}')
    selected = tmp_path / "ocr-selection.json"
    selected.write_text(
        json.dumps(
            {
                "schema": "congressional-disbursement-ocr-selection/1",
                "source_selection": pin(qualified),
                "sources": [{"id": "house-test", "physical_pages": 5}],
                "pages": [
                    {
                        "source_id": "house-test",
                        "page": 1,
                        "action": "recognize",
                        "profile": profile,
                        "reason": "exercise processing boundaries",
                        "layout_backend": "geometry",
                        "text_backend": "text",
                        "orientation": {"clockwise_degrees": 0, "evidence": pin(evidence)},
                    },
                    {
                        "source_id": "house-test",
                        "page": 2,
                        "action": "exclude",
                        "reason": "synthetic exclusion",
                        "evidence": [pin(evidence)],
                    },
                    {"source_id": "house-test", "page": 3, "action": "defer", "reason": "not selected for this batch"},
                ],
            }
        )
    )
    return selected


def run(path, tmp_path, backends=None, **kwargs):
    return read_ocr_pages(
        path,
        tmp_path / "ocr",
        backends=backends or {"geometry": Backend(layout=True), "text": Backend()},
        configurations=kwargs.pop("configurations", {"geometry": LAYOUT, "text": TEXT}),
        archive_root=tmp_path / "raw",
        budget=kwargs.pop("budget", OcrBudget(min_free_disk_bytes=1)),
        **kwargs,
    )


def test_observations_exclusions_and_all_unread_pages_remain_distinct(tmp_path):
    path = selection(tmp_path)
    result = run(path, tmp_path)
    assert result["states"] == {"observed_unqualified": 1, "excluded_by_review": 1, "deferred": 1}
    assert result["scoped_physical_pages"] == 5 and result["pending_unselected_pages"] == 2
    assert result["qualified_pages"] == 0 and result["full_source_coverage"] is False
    assert result["observation_complete"] is False
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore, iter_verified_blob

    blobs = LocalSourceNativeBlobStore(Path(result["derived_blob_root"]), create=False)
    reference = result["pages"][0]["result"]
    page = json.loads(b"".join(iter_verified_blob(blobs, reference["sha256"], reference["byte_size"])))
    assert len(page["associations"]) == 3
    assert [row["money_cells"][0]["model_decimal_literal"] for row in page["associations"]] == ["1.00", "2.00", "3.00"]
    assert Path(result["derived_blob_root"]).is_relative_to(tmp_path / "ocr")
    assert not Path(result["derived_blob_root"]).is_relative_to(tmp_path / "raw")


class SenateBackend(Backend):
    def recognize(self, image):
        self.calls += 1
        if not self.layout:
            return Recognition(
                "\n".join(f"V{i} 04/20/2023 PAYEE{i} 02/21/2023 03/20/2023 SERVICE ${i}.00" for i in (1, 2)),
                self.config,
                {"synthetic": True},
                images=(image,),
            )
        columns = (
            (0.04, "DOCUMENT NO."),
            (0.18, "DATE"),
            (0.28, "PAYEE NAME"),
            (0.45, "START"),
            (0.54, "END"),
            (0.68, "DESCRIPTION"),
            (0.9, "AMOUNT ($)"),
        )
        blocks = [TextBlock(text, Box(x, 0.04, x + 0.08, 0.06)) for x, text in columns]
        for i in (1, 2):
            for (x, _), text in zip(
                columns,
                (f"V{i}", "04/20/2023", f"PAYEE{i}", "02/21/2023", "03/20/2023", "SERVICE", f"${i}.00"),
                strict=True,
            ):
                blocks.append(TextBlock(text, Box(x, 0.2 * i, x + 0.08, 0.2 * i + 0.02)))
        return Recognition(
            "\n".join(b.text for b in blocks), self.config, {"synthetic": True}, blocks=tuple(blocks), images=(image,)
        )


def test_senate_profile_reuses_the_retention_and_resume_pipeline(tmp_path):
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore, iter_verified_blob

    path = selection(tmp_path, chamber="senate", profile="senate-ledger")
    backends = {"geometry": SenateBackend(layout=True), "text": SenateBackend()}
    first = run(path, tmp_path, backends)
    assert first["states"] == {"observed_unqualified": 1, "excluded_by_review": 1, "deferred": 1}
    blobs = LocalSourceNativeBlobStore(Path(first["derived_blob_root"]), create=False)
    ref = first["pages"][0]["result"]
    reading = json.loads(b"".join(iter_verified_blob(blobs, ref["sha256"], ref["byte_size"])))
    assert reading["rule"] == "senate-disbursement-ocr-page/1"
    assert len(reading["associations"]) == 2
    assert reading["unmatched_source_financial_rows"] == []
    assert "source_account_context_unread" in reading["issues"]
    counts = {key: backend.calls for key, backend in backends.items()}
    resumed = run(path, tmp_path, backends)
    assert resumed["pages"][0]["result"] == ref
    assert {key: backend.calls for key, backend in backends.items()} == counts


def test_source_profile_must_match_the_original_chamber(tmp_path):
    path = selection(tmp_path, chamber="house", profile="senate-ledger")
    with pytest.raises(ValueError, match="chamber"):
        read_ocr_selection(path)


def test_resume_keeps_completed_stages_and_does_not_reopen_original(tmp_path, monkeypatch):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    path = selection(tmp_path)
    backends = {"geometry": Backend(layout=True), "text": Backend(failures=1)}
    failed = run(path, tmp_path, backends)
    assert failed["pages"][0]["status"] == "read_failed"
    failure = failed["pages"][0]["failure"]
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore, iter_verified_blob

    blobs = LocalSourceNativeBlobStore(Path(failed["derived_blob_root"]), create=False)
    details = json.loads(b"".join(iter_verified_blob(blobs, failure["sha256"], failure["byte_size"])))
    assert details["details"]["partial_response"] == "kept raw"
    geometry = tmp_path / "ocr/steps/house-test-page-1/geometry.json"
    modified = geometry.stat().st_mtime_ns
    monkeypatch.setattr(CentralRawArchiveStore, "open", lambda *_: pytest.fail("completed render reopened original"))
    resumed = run(path, tmp_path, backends)
    assert resumed["pages"][0]["status"] == "observed_unqualified"
    assert backends["geometry"].calls == 1 and backends["text"].calls == 2
    assert geometry.stat().st_mtime_ns == modified
    assert Path(failed["pages"][0]["attempt"]["path"]).exists()
    run(path, tmp_path, backends)
    assert backends["geometry"].calls == 1 and backends["text"].calls == 2


def test_retained_blob_corruption_never_reexecutes_or_becomes_empty_success(tmp_path):
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    path = selection(tmp_path)
    backends = {"geometry": Backend(layout=True), "text": Backend()}
    result = run(path, tmp_path, backends)
    checkpoint = json.loads((tmp_path / "ocr/steps/house-test-page-1/geometry.json").read_text())
    blobs = LocalSourceNativeBlobStore(Path(result["derived_blob_root"]), create=False)
    retained = blobs.root / "sha256" / checkpoint["result"]["sha256"].removeprefix("sha256:")
    retained.write_bytes(b"corrupt")
    refused = run(path, tmp_path, backends)
    assert refused["pages"][0]["status"] == "retained_invalid"
    assert not refused["observation_complete"]
    assert backends["geometry"].calls == backends["text"].calls == 1


@pytest.mark.parametrize("change", ["page", "duplicate", "count", "exclude", "profile", "manual", "orientation"])
def test_selection_refuses_ambiguous_scope_before_recognition(tmp_path, change):
    path = selection(tmp_path)
    value = json.loads(path.read_text())
    if change == "page":
        value["pages"][0]["page"] = 6
    if change == "duplicate":
        value["pages"].append(value["pages"][0])
    if change == "count":
        value["sources"][0]["physical_pages"] = 4
    if change == "exclude":
        value["pages"][1]["evidence"] = []
    if change == "profile":
        value["pages"][0]["profile"] = "senate-unqualified"
    if change == "manual":
        value["pages"][0]["manual_transcriptions"] = [{"independent_review_sha256": "sha256:" + "a" * 64}]
    if change == "orientation":
        value["pages"][0]["orientation"]["clockwise_degrees"] = False
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        read_ocr_selection(path)


def test_changed_configuration_requires_another_generation(tmp_path):
    path = selection(tmp_path)
    backends = {"geometry": Backend(layout=True), "text": Backend()}
    run(path, tmp_path, backends)
    with pytest.raises(ValueError, match="identical"):
        run(path, tmp_path, backends, configurations={"geometry": LAYOUT, "text": {**TEXT, "changed": True}})
    assert backends["geometry"].calls == backends["text"].calls == 1


def test_unexpected_actual_configuration_is_retained_and_refused(tmp_path):
    path = selection(tmp_path)
    backends = {"geometry": Backend(layout=True, configuration={**LAYOUT, "wrong": True}), "text": Backend()}
    result = run(path, tmp_path, backends)
    assert result["pages"][0]["status"] == "refused" and not result["observation_complete"]
    assert (tmp_path / "ocr/steps/house-test-page-1/geometry.json").exists()
    assert backends["geometry"].calls == 1 and backends["text"].calls == 0


def test_resource_selection_and_scaled_model_input_are_bounded(tmp_path):
    path = selection(tmp_path)
    value = json.loads(path.read_text())
    value["pages"].append({**value["pages"][0], "page": 4})
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="invocation size"):
        run(path, tmp_path, budget=OcrBudget(max_pages=1, min_free_disk_bytes=1))
    config = {"backend": "docling", "pipeline_options": {"images_scale": 4.0}, "media_type": "image/png"}
    backends = {"geometry": Backend(layout=True), "text": Backend(configuration=config)}
    result = run(path, tmp_path, backends, configurations={"geometry": LAYOUT, "text": config})
    assert result["pages"][0]["status"] == "refused"
    assert backends["text"].calls == 0


def test_derived_artifacts_cannot_use_original_archive(tmp_path):
    path = selection(tmp_path)
    with pytest.raises(ValueError, match="outside the central raw"):
        read_ocr_pages(path, tmp_path / "raw/ocr", backends={}, configurations={}, archive_root=tmp_path / "raw")
    assert not (tmp_path / "raw/ocr").exists()


def test_no_financial_landmarks_is_an_explicit_retained_refusal(tmp_path, monkeypatch):
    path = selection(tmp_path)
    backend = Backend(layout=True)
    monkeypatch.setattr(backend, "recognize", lambda image: Recognition("unread", LAYOUT, {"raw": "retained"}))
    result = run(path, tmp_path, {"geometry": backend, "text": Backend()})
    assert result["pages"][0]["status"] == "refused"
    assert result["qualified_pages"] == 0
    assert (tmp_path / "ocr/steps/house-test-page-1/geometry.json").exists()


def test_exact_retained_render_reuses_source_page_evidence_without_opening_original(tmp_path, monkeypatch):
    import hashlib
    from PIL import Image
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore, iter_verified_blob
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    path = selection(tmp_path)
    first = run(path, tmp_path)
    store = LocalSourceNativeBlobStore(Path(first["derived_blob_root"]), create=False)
    record = json.loads((tmp_path / "ocr/steps/house-test-page-1/render.json").read_text())["result"]
    rendered = json.loads(b"".join(iter_verified_blob(store, record["sha256"], record["byte_size"])))
    ref = rendered["raster"]["data"]["$derived_blob"]
    image = tmp_path / "retained.png"
    image.write_bytes(b"".join(iter_verified_blob(store, ref["sha256"], ref["byte_size"])))
    value = json.loads(path.read_text())
    source = json.loads(Path(value["source_selection"]["path"]).read_text())["files"][0]
    page = value["pages"][0]
    page["expected_image_sha256"] = pin(image)["sha256"]
    with Image.open(image) as opened:
        pixels = list(opened.size)
        pixel_hash = "sha256:" + hashlib.sha256(opened.convert("RGB").tobytes()).hexdigest()
    proof = tmp_path / "synthetic-render-proof.json"
    proof.write_text(
        json.dumps(
            {
                "schema": "source-qualified-retained-ocr-render/1",
                "source_sha256": source["source_sha256"],
                "source_page": 1,
                "model_input": pin(image),
                "reproduced_image": pin(image),
                "image_pixels": pixels,
                "decoded_rgb_sha256": pixel_hash,
                "reproduced_rgb_sha256": pixel_hash,
                "clockwise_degrees": 0,
                "render_recipe": {"kind": "synthetic processing test"},
                "original_geometry": rendered["original_geometry"],
            }
        )
    )
    page["retained_render"] = {
        "source_sha256": source["source_sha256"],
        "source_page": 1,
        "image": pin(image),
        "evidence": pin(proof),
    }
    path.write_text(json.dumps(value))
    monkeypatch.setattr(CentralRawArchiveStore, "open", lambda *_: pytest.fail("retained raster reopened original"))
    backends = {"geometry": Backend(layout=True), "text": Backend()}
    second = read_ocr_pages(
        path,
        tmp_path / "replayed",
        backends=backends,
        configurations={"geometry": LAYOUT, "text": TEXT},
        archive_root=tmp_path / "raw",
        budget=OcrBudget(min_free_disk_bytes=1),
    )
    assert second["pages"][0]["status"] == "observed_unqualified"
    page["retained_render"]["source_page"] = 2
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="another original or page"):
        read_ocr_selection(path)


def test_changed_render_is_retained_before_refusal(tmp_path):
    path = selection(tmp_path)
    value = json.loads(path.read_text())
    value["pages"][0]["expected_image_sha256"] = "sha256:" + "a" * 64
    path.write_text(json.dumps(value))
    backends = {"geometry": Backend(layout=True), "text": Backend()}
    result = run(path, tmp_path, backends)
    assert result["pages"][0]["status"] == "refused"
    assert (tmp_path / "ocr/steps/house-test-page-1/render.json").exists()
    assert backends["geometry"].calls == backends["text"].calls == 0
