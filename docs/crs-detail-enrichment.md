# Enrich a selected CRS metadata revision

`build_crs_reports` keeps `(report_id, update_date)` as the report revision key.
Its ordinary list refresh preserves qualified detail and source witnesses when
that coordinate and the shared metadata agree. An explicit `detail_limit` adds a
bounded detail pass; the scheduled rollup continues to request list metadata.

```python
from spicy_regs.transforms.build_crs_reports import build_crs_reports

build_crs_reports(output_dir, detail_limit=10)
```

For a prepared local prior, `refresh_list=False` reads only that prior and the
selected detail responses. It requires a positive detail limit. Supply
`retained_details={report_id: (original_path, "sha256:...")}` to reuse explicitly
pinned original JSON with no origin fallback. Otherwise the configured
api.data.gov key authorizes the live detail route. The detail pass admits at most
100 selected current report revisions per call and bounds each response to
2 MiB. This does not bound the inherited table or list traversal: size their
retained subjects, receipts, restored input, replacement and recovery reserve
before a campaign. The incremental merge reads the inherited table once into
memory; it does not require replaying the original corpus.

SpicyDocs reads the complete original through the same native checks for live
and retained input. List `version` and item `currentVersion` name the publication
version; when both item spellings are present they must agree. The report ID,
update instant and stated version must match the selected row. A different ID,
advanced detail, contradictory version, or incompatible shared metadata refuses
replacement. Full exact responses enter `CaptureEvidence` and the central raw
archive before parsing. Unknown members stay recoverable through their exact raw
witness rather than a second stored copy of the response.

The table exposes the literal summary and ordered arrays `authors`, `topics`,
`formats` and `related_materials`. Repeated members remain repeated. The actual
JSON uses `formats[].url` and `relatedMaterials[].URL`; the latter becomes native
`related_materials[].url`. Conflicting simultaneous `URL`/`url` values refuse.
Law numbers such as `118-159` and numeric bill numbers use text in the native
identifier column, while the original JSON retains their original types. No
bill/law relationship is inferred from summary prose or a topic label.

`detail_fields` records `missing`, `null`, `empty` or `value` for each promoted
member. `detail_state` identifies the qualified read; `detail_attempt_state`
identifies the latest attempt. A failed retry preserves all qualified values,
field states and qualified witnesses, records the failed attempt separately,
and retries next time. An unchanged successful selected revision needs no new
read. `retry_completed=True` explicitly requests another read of completed rows.
A matched reread may explicitly clear a value with JSON null, an empty string or
an empty array. Dropping a previously stated member, or changing a populated
fact at the same coordinate, refuses replacement. It never joins retained values
from one response to field states or witnesses from another. A new native
revision can supply new content without changing an older row.

The processing receipt keeps separate list, qualified-detail and latest-attempt
witnesses. Same-coordinate list reobservations add their own list witness without
erasing the qualified detail witness. The native field policy retains earlier
report-only and dated-list policies for recovery; new writes use
`government-sources/3`. Local qualification does not select a hosted release.
