# Versioned CRS files

`spicy_regs.transforms.build_crs_report_files.build_crs_report_files` prepares an
explicit, bounded local `crs_report_files` candidate. Its maintained caller uses
SpicyDocs `CrsFileAcquirer.acquire_report` for the exact numbered PDF stated in a
qualified CRS detail original. `include_html=True` separately selects offered
HTML. HTML has no numbered version, so `publisher_version` and
`qualified_publisher_version` remain NULL even when its URL appeared beside a PDF.
The existing `crs_reports` metadata key remains `(report_id, update_date)`.

Pass qualified literal metadata rows with `detail_state='ok'`,
`detail_witness_json`, and `formats_json` (or native `formats`). The builder reads
the pinned original from `CentralRawArchiveStore`, checks the native report,
update instant, version and stated URLs, then resolves retained bodies before
acquisition. It never derives a family from the report id or guesses version
numbers. `retained_file_pin` translates a selected file-catalog occurrence into
the `retained_files` map keyed by exact source URL. Historical reconciliation
keeps unknown acquisition time, capture id and final URL unknown.
The catalog's receipt entry is a positive one-based ordinal. The adapter converts
it to the zero-based `capture_receipt_entry` index without changing the pinned
receipt key or digest; the original catalog evidence retains the source ordinal.

The central archive retains original bytes and immutable receipts. File subjects
keep the original digest, exact source link, capture/reference receipt and the
metadata association receipt. Metadata-only updates can associate the same PDF
with a later revision without creating another download observation. A different
numbered URL remains a different edition even if both files share a cover date.
An explicit printed `REPORT_ID · VERSION N · UPDATED` footer supplies an additional
byte identity check. A mismatch sets `body_state='content_mismatch'`, clears the
qualified version, and withholds usable text. An absent footer remains unknown.
`file_validation_basis` distinguishes observed final-URL checks from historical
source association checks. A historical PDF with unknown final URL and no
matching explicit footer retains the publisher-stated version while its
`qualified_publisher_version` remains NULL.

Text comes from the existing pinned pypdf reader for PDFs and the maintained HTML
markup reader for HTML. Each derivative names its exact input digest, output
digest and processor pin. Image-only PDFs can have successful empty embedded text;
OCR is outside this path. Raw originals remain retrievable with
`CentralRawArchiveStore.open(source_sha256)` and through the file catalog's normal
receipt/original lookup. Public `source_url` names the original publisher route;
mutable HTML at that route may subsequently change.

The existing shared receipt writer stores atomic body-complete and text-complete
phase bundles under the caller-owned `.crs-file-phases` directory. A retry reads
and admits those bundles before use. If the process stopped after original
capture but before its body phase committed, the builder reconciles its existing
bounded `CaptureEvidence` journals and reuses the completed archived capture.
The shared receipt reader refuses missing or corrupt original/receipt data.
Unchanged reruns reuse completed text. An explicit prior bundle retains earlier
file associations; it is a file subject history, not another metadata history.

The caller owns run directories through local acceptance. Keep them for recovery
while work is incomplete; existing run cleanup can remove disposable phases
after the accepted candidate and required evidence are retained. Raw originals
and their immutable receipts remain under the raw archive's existing owner.

The module CLI accepts a JSON object containing `reports` and optional
`retained_files`. It performs no network requests unless `--allow-network` is
explicit. Declare report, request, per-file, total decoded-body and inherited
subject/receipt limits. The prior-state limit also checks Parquet uncompressed
sizes and local recovery-journal bytes before decoding.

```sh
python -m spicy_regs.transforms.build_crs_report_files \
  --selection selection.json --output candidate \
  --max-reports 2 --max-requests 4 --max-file-bytes 8388608 \
  --max-total-bytes 33554432 --max-prior-bytes 134217728 --max-prior-rows 10000
```

The result names an admitted native generation and shared ETL receipts. It does
not publish or select a release. The maintained MCP `query_sql` path exposes
`crs_report_files` when that artifact is configured. For example:

```sql
SELECT f.report_id, f.metadata_update_date, f.publisher_version,
       f.qualified_publisher_version, f.rendition, f.body_state,
       f.source_url, f.source_sha256, f.capture_receipt_key,
       f.text_input_sha256, f.text_processor_sha256, f.text_content
FROM crs_report_files f
WHERE f.report_id = 'IN12418'
```

Join `f.report_id = r.report_id` and
`CAST(f.metadata_update_date AS TIMESTAMPTZ) = r.update_date` to query the exact
metadata revision in `crs_reports r`. File capture time and printed cover dates
have separate meanings and do not replace that revision identity.
