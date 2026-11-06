"""Resume explicitly selected OCR pages without treating recognition as acceptance.

Originals remain in the shared raw archive. Rendered pages, complete provider
responses and source-owned associations use a separate content-addressed store.
Unselected pages stay pending; even an associated page awaits independent review.
There is deliberately no all-pages or live-model command-line default.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import io
import json
import math
import platform
import shutil
import sys
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, is_dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from time import perf_counter
from typing import Any, TypedDict

from spicy_regs.fec_release_verification import CompletedSteps, _lock
from spicy_regs.pipelines.congressional_disbursement_pages import _retain_implementation, pin, verify_pin
from spicy_regs.pipelines.congressional_disbursements import _write, read_selection

SELECTION_SCHEMA = "congressional-disbursement-ocr-selection/1"
READING_SCHEMA = "congressional-disbursement-ocr-reading/1"


class _OcrProfile(TypedDict):
    chamber: str
    module: str
    plan: str
    read: str
    witness: str | None


_PROFILES: dict[str, _OcrProfile] = {
    "house-2008": {
        "chamber": "house",
        "module": "spicy_docs.reading.house_disbursements_ocr",
        "plan": "plan_house_disbursements_ocr",
        "read": "read_house_disbursements_ocr",
        "witness": "house_ocr_witness_region",
    },
    "senate-ledger": {
        "chamber": "senate",
        "module": "spicy_docs.reading.senate_disbursements_ocr",
        "plan": "plan_senate_disbursements_ocr",
        "read": "read_senate_disbursements_ocr",
        "witness": None,
    },
}


@dataclass(frozen=True)
class OcrBudget:
    """Explicit per-invocation bounds; the caller also runs the memory watchdog."""

    max_pages: int = 4
    max_source_bytes: int = 128 * 1024**2
    max_raster_pixels: int = 8_000_000
    max_region_pixels: int = 2_620_620
    max_page_output_bytes: int = 128 * 1024**2
    max_failure_output_bytes: int = 32 * 1024**2
    min_free_disk_bytes: int = 8 * 1024**3

    def __post_init__(self):
        if any(type(value) is not int or value < 1 for value in asdict(self).values()):
            raise ValueError("OCR resource bounds must be positive integers")


def _json(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n").encode()


def _sha(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def read_ocr_selection(path: Path) -> tuple[dict, dict[str, dict]]:
    """Bind page choices to the maintained original selection and its page counts."""
    value = json.loads(Path(path).read_text())
    if value.get("schema") != SELECTION_SCHEMA:
        raise ValueError("Expected a selective congressional OCR plan")
    original = verify_pin(value["source_selection"])
    available = {item["id"]: item for item in read_selection(original)["files"]}
    sources = {}
    for selected in value["sources"]:
        identity = selected["id"]
        source = available[identity]
        if identity in sources or source["format"] != "pdf":
            raise ValueError("OCR scope repeats a source or selects a non-PDF original")
        count = source.get("physical_page_count") or source.get("normalization_qualification", {}).get("physical_pages")
        if type(count) is not int or count < 1 or selected["physical_pages"] != count:
            raise ValueError("OCR scope needs the retained physical-page count")
        if not source.get("source_sha256") or not source.get("source_byte_size"):
            raise ValueError("OCR scope needs a retained original identity")
        sources[identity] = source
    if not sources:
        raise ValueError("OCR scope cannot be empty")
    seen = set()
    for page in value["pages"]:
        key = (page["source_id"], page["page"])
        source = sources[page["source_id"]]
        count = source.get("physical_page_count") or source["normalization_qualification"]["physical_pages"]
        if key in seen or type(page["page"]) is not int or not 1 <= page["page"] <= count:
            raise ValueError("OCR page is repeated or outside its retained original")
        seen.add(key)
        if page["action"] not in {"recognize", "defer", "exclude"} or not page.get("reason"):
            raise ValueError("Every selected page needs an explicit action and reason")
        for evidence in page.get("evidence", []):
            verify_pin(evidence)
        if page["action"] == "exclude" and not page.get("evidence"):
            raise ValueError("Excluding a page requires retained source-review evidence")
        if page["action"] == "recognize":
            profile = _PROFILES.get(page.get("profile"))
            if profile is None or source["chamber"] != profile["chamber"]:
                raise ValueError("Select a supported OCR source profile for the original's chamber")
            if profile["witness"] is None and (page.get("witnesses") or page.get("manual_transcriptions")):
                raise ValueError("This OCR source profile has no qualified targeted-transcription path")
            orientation = page["orientation"]
            if type(orientation["clockwise_degrees"]) is not int or orientation["clockwise_degrees"] not in {
                0,
                90,
                180,
                270,
            }:
                raise ValueError("OCR orientation must be an explicitly reviewed quarter turn")
            verify_pin(orientation["evidence"])
            if retained := page.get("retained_render"):
                if (retained["source_sha256"], retained["source_page"]) != (source["source_sha256"], page["page"]):
                    raise ValueError("Retained OCR render belongs to another original or page")
                verify_pin(retained["image"])
                proof = json.loads(verify_pin(retained["evidence"]).read_text())
                if (
                    proof.get("schema") != "source-qualified-retained-ocr-render/1"
                    or proof.get("source_sha256") != source["source_sha256"]
                    or proof.get("source_page") != page["page"]
                    or proof.get("model_input") != retained["image"]
                    or proof.get("clockwise_degrees") != orientation["clockwise_degrees"]
                    or proof.get("decoded_rgb_sha256") != proof.get("reproduced_rgb_sha256")
                    or not proof.get("render_recipe")
                    or not proof.get("original_geometry")
                ):
                    raise ValueError("Retained render needs an exact source/page/rotation and pixel-equivalence proof")
                verify_pin(proof["reproduced_image"])
                if retained["image"]["sha256"] != page.get("expected_image_sha256"):
                    raise ValueError("Retained OCR render differs from the selected input image")
            if not page.get("layout_backend") or not page.get("text_backend"):
                raise ValueError("OCR page needs declared geometry and text backends")
            evidence_hashes = {item["sha256"] for item in page.get("evidence", [])}
            for manual in page.get("manual_transcriptions", []):
                if not {manual.get("independent_review_sha256"), manual.get("model_result_sha256")} <= evidence_hashes:
                    raise ValueError(
                        "Manual OCR transcription requires retained independent review and original model-result pins"
                    )
    for evidence in value.get("body_gaps", []):
        verify_pin(evidence)
    return value, sources


def implementation(backends: dict, profiles: set[str]) -> dict:
    """Retain imported code and runtime identity without loading a model."""
    from spicy_docs.extraction.derivation import derivation_modules

    roots = (
        *(_PROFILES[name]["module"] for name in sorted(profiles)),
        "spicy_docs.extraction.ocr",
        "spicy_docs.extraction.pages",
        "spicy_docs.storage.blobs",
    )
    names = set()
    for root in roots:
        names.update(derivation_modules(root, ("spicy_docs.extraction", "spicy_docs.reading", "spicy_docs.storage")))
    paths = {Path(inspect.getfile(importlib.import_module(name))) for name in names}
    paths.update(Path(inspect.getfile(type(backend))) for backend in backends.values())
    paths.update(
        Path(inspect.getfile(function)) for function in (CompletedSteps, _retain_implementation, read_selection)
    )
    paths.add(Path(__file__))
    packages = {}
    for package in ("pymupdf", "Pillow", "ocrmac", "docling", "mlx", "mlx-vlm"):
        try:
            packages[package] = version(package)
        except PackageNotFoundError:
            packages[package] = None
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "packages": packages,
        "files": [pin(path) for path in sorted(paths)],
    }


class _RetainedOcrInvalid(RuntimeError):
    """An existing step lost integrity; do not hide it by running the model again."""


class _PageSteps:
    """Compose existing atomic checkpoints with verified derived-blob storage."""

    def __init__(self, directory, binding, blobs, budget):
        directory.mkdir(parents=True, exist_ok=True)
        self.steps = CompletedSteps(directory, binding)
        self.blobs, self.budget = blobs, budget
        self.referenced = {}

    def _blob(self, payload):
        if len(payload) > self.budget.max_page_output_bytes:
            raise ValueError("OCR artifact exceeds the admitted page output size")
        ref = {"sha256": _sha(payload), "byte_size": len(payload)}
        self._account(ref)
        self.blobs.put_blob(ref["sha256"], ref["byte_size"], (payload,))
        return ref

    def _account(self, ref):
        proposed = {**self.referenced, ref["sha256"]: ref["byte_size"]}
        if sum(proposed.values()) > self.budget.max_page_output_bytes:
            raise ValueError("Retained OCR page exceeds its admitted output size")
        self.referenced = proposed

    def _encode(self, value):
        if is_dataclass(value):
            value = asdict(value)
        if isinstance(value, bytes):
            return {"$derived_blob": self._blob(value)}
        if isinstance(value, dict):
            return {key: self._encode(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._encode(item) for item in value]
        return value

    def _read_blob(self, ref):
        from spicy_docs.storage.blobs import iter_verified_blob

        self._account(ref)
        return b"".join(iter_verified_blob(self.blobs, ref["sha256"], ref["byte_size"]))

    def _decode(self, value):
        if isinstance(value, dict) and set(value) == {"$derived_blob"}:
            return self._read_blob(value["$derived_blob"])
        if isinstance(value, dict):
            return {key: self._decode(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._decode(item) for item in value]
        return value

    def run(self, name, operation):
        retained = (self.steps.directory / (name + ".json")).exists()
        try:
            ref = self.steps.run(name, lambda: self._blob(_json(self._encode(operation()))))
            return self._decode(json.loads(self._read_blob(ref)))
        except (ValueError, RuntimeError, OSError) as error:
            if retained:
                raise _RetainedOcrInvalid(f"Retained OCR step {name} failed verification: {error}") from error
            raise

    def retain_failure(self, error):
        """A provider refusal preserves its detailed response without closing a step."""
        payload = _json(
            {
                "error_type": type(error).__name__,
                "error": str(error),
                "details": getattr(error, "details", None),
            }
        )
        if len(payload) > self.budget.max_failure_output_bytes:
            raise RuntimeError(
                "OCR failure details exceed the reserved retention bound; no completion was recorded"
            ) from error
        ref = {"sha256": _sha(payload), "byte_size": len(payload)}
        self.blobs.put_blob(ref["sha256"], ref["byte_size"], (payload,))
        return ref


def _recognition(value):
    from spicy_docs.extraction import Box, Recognition, TextBlock

    # The complete serialized provider result remains retained. Association uses
    # only text/config/raw and measured text blocks, not invented model geometry.
    blocks = []
    for block in value["blocks"]:
        fields: dict[str, Any] = {
            **block,
            "box": Box(**block["box"]) if block["box"] else None,
            "inspected_region": Box(**block["inspected_region"]) if block.get("inspected_region") else None,
        }
        blocks.append(TextBlock(**fields))
    return Recognition(
        value["text"],
        value["configuration"],
        value["raw"],
        blocks=tuple(blocks),
        text_format=value["text_format"],
    )


def _render(document, page):
    from PIL import Image
    from spicy_docs.extraction import Raster

    original = document.page(page["page"])
    raster = original.render()
    degrees = page["orientation"]["clockwise_degrees"]
    if degrees:
        with Image.open(io.BytesIO(raster.data)) as image:
            rotated = image.transpose(
                {90: Image.Transpose.ROTATE_270, 180: Image.Transpose.ROTATE_180, 270: Image.Transpose.ROTATE_90}[
                    degrees
                ]
            )
            output = io.BytesIO()
            rotated.save(output, format="PNG")
            raster = Raster(output.getvalue(), rotated.width, rotated.height)
    return {
        "raster": raster,
        "original_geometry": original.geometry,
        "clockwise_degrees": degrees,
        "orientation_evidence": page["orientation"]["evidence"],
    }


def _read_page(source, page, steps, document_factory, backends, configurations, budget):
    from spicy_docs.extraction import Box, Raster
    from spicy_docs.reading.ocr_observations import OcrView

    profile = _PROFILES[page["profile"]]
    module = importlib.import_module(profile["module"])

    def render():
        if retained := page.get("retained_render"):
            from PIL import Image

            path = verify_pin(retained["image"])
            if path.stat().st_size > budget.max_page_output_bytes:
                raise ValueError("Retained OCR render exceeds the page byte bound")
            with Image.open(path) as original:
                if original.format != "PNG" or original.width * original.height > budget.max_raster_pixels:
                    raise ValueError("Retained OCR render exceeds the admitted raster format or dimensions")
                proof = json.loads(verify_pin(retained["evidence"]).read_text())
                if (
                    list(original.size) != proof["image_pixels"]
                    or _sha(original.convert("RGB").tobytes()) != proof["decoded_rgb_sha256"]
                ):
                    raise ValueError("Retained OCR input pixels differ from their source-render proof")
                image = Raster(path.read_bytes(), original.width, original.height)
            return {
                "raster": image,
                "retained_render": retained,
                "render_proof": proof,
                "clockwise_degrees": page["orientation"]["clockwise_degrees"],
                "orientation_evidence": page["orientation"]["evidence"],
            }
        with document_factory() as document:
            expected = source.get("physical_page_count") or source["normalization_qualification"]["physical_pages"]
            if document.page_count != expected:
                raise ValueError("Retained original page count differs from the selected source")
            return _render(document, page)

    rendered = steps.run("render", render)
    raster: dict[str, Any] = {**rendered["raster"], "box": Box(**rendered["raster"]["box"])}
    image = Raster(**raster)
    if page.get("expected_image_sha256") and _sha(image.data) != page["expected_image_sha256"]:
        raise ValueError("Rendered OCR page differs from its pinned source image")
    if image.width * image.height > budget.max_raster_pixels:
        raise ValueError("Retained render exceeds the admitted raster size")

    def recognize(name, raster):
        configuration = configurations[name]
        if configuration.get("backend") == "docling":
            scale = configuration["pipeline_options"]["images_scale"]
            if (
                not isinstance(scale, (int, float))
                or not math.isfinite(scale)
                or scale <= 0
                or math.ceil(raster.width * scale) * math.ceil(raster.height * scale) > budget.max_region_pixels
            ):
                raise ValueError("Scaled model input exceeds the admitted OCR region size")
            actual = backends[name].configuration()
            if {**actual, "media_type": raster.media_type} != configuration:
                raise ValueError("Configured Docling backend differs from its pinned recipe")
        return backends[name].recognize(raster)

    def reading_for(stage, name, raster):
        reading = _recognition(steps.run(stage, lambda: recognize(name, raster)))
        if reading.configuration != configurations[name]:
            raise ValueError("Actual OCR backend configuration differs from its pinned recipe")
        return reading

    geometry = reading_for("geometry", page["layout_backend"], image)
    plan = getattr(module, profile["plan"])(
        image,
        geometry,
        source_sha256=source["source_sha256"],
        source_page=page["page"],
        max_region_pixels=budget.max_region_pixels,
    )
    # Retain actual selected input bytes even when a provider emits no input image.
    regions = [(region, page["text_backend"]) for region in plan.regions]
    for witness in page.get("witnesses", []):
        witness_reader = profile["witness"]
        assert witness_reader is not None  # read_ocr_selection already checks this profile's witness support.
        regions.append(
            (
                getattr(module, witness_reader)(
                    plan, witness["row_id"], block_index=witness.get("field_block_index")
                ),
                witness["backend"],
            )
        )
    if len({region.id for region, _ in regions}) != len(regions):
        raise ValueError("OCR selection repeats a source region")
    views = []
    for region, name in regions:
        if region.raster.width * region.raster.height > budget.max_region_pixels:
            raise ValueError("OCR region exceeds the admitted model input size")
        selected = steps.run("input-" + region.id, lambda region=region: asdict(region))
        if selected["raster"]["data"] != region.raster.data or selected["row_ids"] != list(region.row_ids):
            raise ValueError("Retained OCR region differs from current observed geometry")
        reading = reading_for("read-" + region.id, name, region.raster)
        views.append(OcrView(region, reading))

    def associate():
        result = getattr(module, profile["read"])(
            plan, tuple(views), manual_transcriptions=page.get("manual_transcriptions", ())
        )
        # Full observations and crop bytes already have verified stage records.
        # Keep associations small without copying the model's embedded images.
        result["geometry"] = steps.steps.results["geometry"]["result"]
        result["views"] = [
            {
                "reading": view.evidence,
                "selected_row_ids": list(view.region.row_ids),
                "observation": steps.steps.results["read-" + view.region.id]["result"],
                "input": steps.steps.results["input-" + view.region.id]["result"],
            }
            for view in views
        ]
        return result

    result = steps.run("associate", associate)
    return {
        "status": "observed_unqualified",
        "result": steps.steps.results["associate"]["result"],
        "associated_rows": len(result["associations"]),
        "unmatched_source_rows": len(result["unmatched_source_financial_rows"]),
        "unmatched_model_lines": len(result["unmatched_model_financial_lines"]),
        "manual_transcriptions": result["manual_transcription_count"],
        "interpretation": "A retained OCR association is not independent financial or source-coverage acceptance.",
    }


def read_ocr_pages(
    selection_path: Path,
    output_dir: Path,
    *,
    backends: dict,
    configurations: dict,
    archive_root: Path | None = None,
    budget: OcrBudget = OcrBudget(),
) -> dict:
    """Resume exact completed render/recognition steps; retry failed steps only.

    Backends are maintained SpicyDocs image recognizers, built by the caller from
    retained configurations and local model assets. This function never downloads
    models, acquires originals, publishes subject rows, or auto-excludes a page.
    """
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    output_dir = Path(output_dir).resolve()
    store = CentralRawArchiveStore(archive_root, create=False)
    if output_dir.is_relative_to(store.root.resolve()):
        raise ValueError("Derived OCR output must remain outside the central raw archive")
    output_dir.mkdir(parents=True, exist_ok=True)
    with _lock(output_dir / ".processing.lock"):
        return _read_ocr_pages(
            selection_path,
            output_dir,
            backends=backends,
            configurations=configurations,
            archive_root=store.root,
            budget=budget,
        )


def _read_ocr_pages(selection_path, output_dir, *, backends, configurations, archive_root, budget):
    from contextlib import contextmanager
    from spicy_docs.extraction import DefaultReader
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore, iter_verified_blob
    from spicy_docs.storage.publication import write_bytes_once
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    selection, sources = read_ocr_selection(selection_path)
    selected = [page for page in selection["pages"] if page["action"] == "recognize"]
    if len(selected) > budget.max_pages:
        raise ValueError("OCR page selection exceeds the explicitly admitted invocation size")
    required = {page[key] for page in selected for key in ("layout_backend", "text_backend")}
    required.update(witness["backend"] for page in selected for witness in page.get("witnesses", []))
    if set(backends) != required or set(configurations) != required:
        raise ValueError("Supply exactly the selected OCR backends and configurations")
    output_dir = Path(output_dir).resolve()
    store = CentralRawArchiveStore(archive_root, create=False)
    if output_dir.is_relative_to(store.root.resolve()):
        raise ValueError("Derived OCR output must remain outside the central raw archive")
    output_dir.mkdir(parents=True, exist_ok=True)
    profiles = {page["profile"] for page in selected}
    producer = implementation(backends, profiles)
    binding = {
        "schema": READING_SCHEMA,
        "selection": pin(selection_path),
        "implementation": producer,
        "configurations": configurations,
        "budget": asdict(budget),
        "archive_root": str(store.root),
    }
    header = output_dir / "binding.json"
    if header.exists():
        if json.loads(header.read_text()) != binding:
            raise ValueError("OCR resume requires identical input, code, runtime and bounds; use a new output")
    else:
        write_bytes_once(header, _json(binding))
    copies = _retain_implementation(producer, output_dir / "implementation")
    blobs = LocalSourceNativeBlobStore(output_dir / "derived-blobs")
    results = []
    reader = DefaultReader(dpi=300, max_pixels=budget.max_raster_pixels)
    manifest = {
        **binding,
        "implementation_copies": copies,
        "derived_blob_root": str(blobs.root),
        "full_source_coverage": False,
        "body_gaps": selection.get("body_gaps", []),
    }

    def checkpoint():
        by_source = Counter(page["source_id"] for page in results)
        counts = Counter(page["status"] for page in results)
        total = sum(source["physical_pages"] for source in selection["sources"])
        manifest.update(
            pages=results,
            states=dict(counts),
            scoped_physical_pages=total,
            pending_unselected_pages=total - sum(by_source.values()),
            source_scope=[
                {**item, "unselected_pending_pages": item["physical_pages"] - by_source[item["id"]]}
                for item in selection["sources"]
            ],
            observation_complete=all(
                item["status"] in {"observed_unqualified", "excluded_by_review"} for item in results
            )
            and len(results) == len(selection["pages"]),
            qualified_pages=0,
        )
        _write(output_dir / "manifest.json", manifest)

    checkpoint()
    for page in selection["pages"]:
        base = {"source_id": page["source_id"], "page": page["page"], "reason": page["reason"]}
        if page["action"] != "recognize":
            results.append(
                {
                    **base,
                    "status": "excluded_by_review" if page["action"] == "exclude" else "deferred",
                    "evidence": page.get("evidence", []),
                }
            )
            checkpoint()
            continue
        source = sources[page["source_id"]]
        key = f"{source['id']}-page-{page['page']}"
        steps = _PageSteps(output_dir / "steps" / key, {**binding, "source": source, "page": page}, blobs, budget)

        @contextmanager
        def document_factory():
            if source["source_byte_size"] > budget.max_source_bytes:
                raise ValueError("Original exceeds the admitted source byte size")
            # Only a missing render opens the original. A resumed page verifies
            # retained processing blobs instead of rereading a large source PDF.
            body = b"".join(iter_verified_blob(store, source["source_sha256"], source["source_byte_size"]))
            with reader.open(body, "application/pdf") as document:
                yield document

        if shutil.disk_usage(output_dir).free < (
            budget.min_free_disk_bytes + budget.max_page_output_bytes + budget.max_failure_output_bytes
        ):
            raise ValueError("Insufficient disk for one bounded OCR page beside the retained reserve")
        started = perf_counter()
        try:
            result = _read_page(source, page, steps, document_factory, backends, configurations, budget)
        except (ValueError, RuntimeError, OSError) as error:
            failure = steps.retain_failure(error)
            attempt = output_dir / "failures" / (key + "-" + uuid.uuid4().hex + ".json")
            write_bytes_once(attempt, _json({**base, "failure": failure, "binding": pin(header)}))
            result = {
                "status": "retained_invalid"
                if isinstance(error, _RetainedOcrInvalid)
                else ("refused" if isinstance(error, ValueError) else "read_failed"),
                "failure": failure,
                "attempt": pin(attempt),
            }
        if implementation(backends, profiles) != producer:
            raise ValueError("OCR implementation changed during processing; preserve this candidate")
        results.append(
            {
                **base,
                **result,
                "seconds_this_invocation": perf_counter() - started,
                "retained_bytes": sum(steps.referenced.values()),
                "checkpoints": str(steps.steps.directory),
            }
        )
        checkpoint()
    verify_pin(binding["selection"])
    read_ocr_selection(selection_path)
    checkpoint()
    return manifest
