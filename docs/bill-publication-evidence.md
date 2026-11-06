Bill printings expose native issue, metadata-update and ingest dates through
`bill_versions.publication_dates` and the maintained
`bill_text_publication_dates` query. Each date keeps its exact value, lexical
precision, missing/null/empty state, MODS attributes, original metadata digest,
source locator and capture observation. `action_version_date` remains the
Congress action-associated text-version date. An enrolled empty date stays `''`.

```sql
SELECT bill_id, version_code, package_id, body_version_id,
       action_version_date, native_field, native_value, native_precision,
       evidence_sha256, evidence_locator, metadata_observed_at,
       publication_evidence_status, publication_limit
FROM bill_text_publication_dates
WHERE package_id = 'BILLS-119hjres25enr'
ORDER BY date_ordinal;
```

The matching ETL receipt retains `publication_evidence_json`: the exact selected
body digest/locator and original body observation, every metadata capture, and
the explicit publication/availability limit. Main rows carry a typed native
date list and the binding status. No new history table or observation clock is
introduced. The fresh bill-version policy is `legislative-documents/2`; existing
selected generations keep their own policies and receipts.

The ordinary per-package body pass attaches already acquired SUMMARY/MODS
evidence. Bulk bodies remain unenriched until an explicit selection is made.
For a metadata-only refresh, call `build_bill_family` with
`publication_package_ids=(...)` and `publication_metadata_acquirer=source`.
`source` uses `GovInfoBodyAcquirer.acquire_bill_metadata` through the existing
`CaptureEvidence` transport. The standalone maintained
`refresh_bill_publication(path, package_ids=..., metadata_source=..., evidence=...)`
updates a candidate table through the existing merge path. Admit the full
candidate rewrite and receipt conversion, as well as the metadata requests,
before running it on a large inherited candidate.

Metadata refresh reuses the existing body digest and extraction. An unchanged
capture pair leaves the selected version row byte-for-byte intact; an older
observation preserves current evidence. An observed change between bracketing
SUMMARY requests records `summary_changed_during_acquisition` and preserves the
selected row. Wrong-printing metadata or an interrupted/refused acquisition
aborts before table replacement; retained captures remain available for diagnosis.
No captured date re-stamps body history.

All bindings remain limited. `separate_requests_unverified` describes ordinary
body acquisition; `stable_summary_bracket_body_unverified` means equal SUMMARY
bytes around the MODS request. Neither proves a frozen publisher metadata/body
revision, first online availability, or the publication time of the retained
body bytes. Unsupported publication events stay unknown. PREMIS is required
only if a concrete file-fixity claim needs it; no such claim is made here.

Qualification reused exact enrolled, introduced and numbered-reprint bodies and
captured only missing metadata. The measured bounded acquisition made 9 requests
and read 55,764 decoded bytes; no body was fetched. Exact originals and capture
pins are in SpicyDocs' `tests/fixtures/bill_publication/provenance.json`; the
retained body SHA-256 values are documented beside it. Local admission, raw
visual review, native candidate/receipts and the actual consumer result are in
`~/Work/worktrees/crs-depth-p1-20261106/evidence/ss-m4r1/`. This is local source-to-query
qualification, separate from hosted adoption or publication.
