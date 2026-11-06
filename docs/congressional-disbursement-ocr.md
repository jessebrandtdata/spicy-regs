# Selective congressional disbursement OCR

`spicy_regs.pipelines.congressional_disbursement_ocr.read_ocr_pages` processes an
explicit page selection from retained originals. It produces processing evidence
and source-owned associations, with financial acceptance left to an independent
raw review. It does not publish financial rows or start an entire report by
default.

The stage reuses the existing congressional source-selection validator and
implementation-copy helpers, `CentralRawArchiveStore`, the maintained SpicyDocs
document reader and image recognizers, and the House OCR association adapter.
`CompletedSteps` supplies atomic per-step completion records; the shared blob
store retains derived bytes outside the raw archive. No new acquisition path or
model downloader is involved.

## Inputs and page states

A `congressional-disbursement-ocr-selection/1` file pins the canonical PDF
selection. Its `sources` list states the complete physical-page count for each
source in the OCR scope. Its `pages` list gives individual physical page numbers,
actions and reasons. Every omitted physical page remains pending. A source's
embedded OCR or native-text presence never establishes financial completeness.

Each page action is `recognize`, `defer`, or `exclude`. An exclusion requires a
pinned source-review artifact. Recognition requires the supported source profile,
explicit reviewed orientation, and named geometry/text backend configurations.
The bounded House profile does not authorize Senate row association. A Senate
image-only page should remain deferred until a separate source-owned profile and
raw fixture qualify its layout.

| Processing outcome | Meaning |
| --- | --- |
| `observed_unqualified` | Exact inputs, provider outputs and associations are retained; financial acceptance remains open. |
| `deferred` | The page was explicitly held for later work. |
| `excluded_by_review` | A pinned review excludes this page from this OCR scope. |
| `refused` | A resource, configuration, layout or source-evidence check refused the attempted reading. |
| `read_failed` | A backend or I/O operation failed; completed earlier steps remain reusable. |
| `retained_invalid` | A saved step or blob failed verification; the stage preserves the failure and does not silently regenerate it. |

`observation_complete` describes only the explicit selection. `qualified_pages`
remains zero and `full_source_coverage` remains false in this processing stage.
Missing historical report bodies remain separate pinned gap evidence, with unknown
page counts; they are not empty reports.

## Exact render and model-input reuse

The normal path renders through `DefaultReader` and applies the selected quarter
turn to the resulting raster. An expected image hash is checked **after** the
actual render is retained, so an input mismatch remains inspectable.

A previously reviewed raster can instead be reused through `retained_render`.
Its receipt must use `source-qualified-retained-ocr-render/1` and bind the exact
original SHA, physical page, source geometry, rotation recipe, retained PNG byte
pin, decoded RGB pixel hash, dimensions and a reproduced-image pin. The stage
checks the input pixels and reuses the original encoded bytes. Equal pixels do
not make a new PNG interchangeable with a model response keyed to older bytes.

This distinction was observed in the retained November 2026 experiment. Rotation
before rasterization differed from rotation after rasterization. Pillow 12.1.1
and 12.3.0 also produced different crop PNG bytes with equal decoded pixels. The
qualified replay uses the exact retained images and Pillow 12.3.0. See the
campaign's `ocr-selective-stage-v3/replay-proof.json`, render proofs and encoding
refusal receipt for input and runtime pins; retain the earlier failures.

## Resume and resource limits

Render, positioned recognition, selected region bytes, region recognition and
association each have a completion record. Resume verifies referenced blobs and
reuses completed work before opening a large original. A provider failure retains
its detailed response without closing that step. Changing source selection,
implementation, runtime, backend configuration or bounds requires a new output
directory. A process lock prevents simultaneous writers.

Callers supply maintained SpicyDocs recognizers and their exact expected
configurations. Build Docling from retained settings and
`prepare_artifacts(local_files_only=True)`; reuse installed pinned assets. The
host checks actual returned configuration and bounds Docling's **scaled** image
size before calling it. Configuration and raw response remain separate from
source geometry: the host never invents token positions for an unpositioned
language-model result.

`OcrBudget` declares invocation page count, source byte size, raster pixels,
model-region pixels, retained page bytes, failure-diagnostic bytes and free-disk
reserve. Run live recognition under the existing memory watchdog with one serial
model worker. The budget is not a measured whole-process memory guarantee. Keep
the last measured peak and a retained resource report with each admitted pilot.

The dated campaign plan is
`historical-expansion/ocr-selective-integration-plan.json`. Its storage estimate
uses retained bytes from the actual host replay, not just the input images. It
keeps the historical run pending until source coverage, financial accuracy and
available retention capacity are established.

## Manual corrections and verification

Manual transcription requires both the original model-result pin and an
independent review pin in the page evidence. The source adapter also verifies the
original/page/image, measured row and crop, raw OCR text, previous value and
corrected field. The correction stays marked `manual_review`; model accuracy is
reported separately. No global punctuation repair or `D0` to `DO` substitution
is implied.

The focused host tests exercise interrupted-model recovery without reopening the
original, exact retained-raster reuse, artifact corruption, configuration drift,
page membership, exclusions, scaled input bounds and explicit unread outcomes.
The SpicyDocs source adapter owns the independent financial fixtures. An offline
host replay compares its complete bounded association fields with that frozen
candidate; this establishes retained-response equivalence, not a fresh OCR
accuracy claim.
