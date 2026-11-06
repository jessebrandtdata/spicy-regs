# Querying CRS summary clocks

`bill_publisher_summaries` preserves two literal source fields. In BILLSTATUS
1.0.0, `update_date` is the `billSummaries` item's `updateDate`, while
`last_summary_update_date` is the summary's own `lastSummaryUpdateDate`.
The [publisher guide](https://github.com/usgpo/bill-status/blob/main/BILLSTATUS-XML_User_User-Guide.md#summaries)
states this distinction. For retained 113-H.R.4200's introduced stage, the
item clock is March 11, 2014 and the summary clock is September 8, 2014.

Current 3.0.0 records state `updateDate` inside `summary`. The query below
compares that summary-record update clock with the legacy summary clock.
It keeps both raw witnesses and labels the selected field. This is a
schema-qualified update interpretation, not evidence of first publication.
Unknown schemas, absent fields, empty leaves and malformed timestamps remain
separate outcomes. A minimum update timestamp cannot establish first posting
or recover uncaptured earlier revisions.

`update_date` is also part of the existing row key. A source item without that
clock cannot qualify a subject row; an empty key is likewise refused by the
native writer, which retains the original row in its refusal receipt. That
outcome differs from a valid row whose optional `last_summary_update_date`
is absent or empty. Malformed nonempty date spellings remain literal values;
the query marks them rather than manufacturing a timestamp.

```sql
WITH clocks AS (
    SELECT bill_id, summary_version_code, action_date, update_date,
           last_summary_update_date, source_schema_version,
           CASE source_schema_version
               WHEN '1.0.0' THEN last_summary_update_date
               WHEN '3.0.0' THEN update_date
           END AS summary_update_literal,
           CASE source_schema_version
               WHEN '1.0.0' THEN 'lastSummaryUpdateDate'
               WHEN '3.0.0' THEN 'updateDate'
           END AS summary_update_field
    FROM bill_publisher_summaries
)
SELECT *,
       CASE
           WHEN summary_update_field IS NULL THEN 'unknown_schema'
           WHEN summary_update_literal IS NULL THEN 'absent'
           WHEN summary_update_literal = '' THEN 'empty'
           WHEN NOT regexp_full_match(summary_update_literal,
               '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})')
               OR try_cast(summary_update_literal AS TIMESTAMPTZ) IS NULL THEN 'malformed'
           ELSE 'valid'
       END AS summary_update_status,
       CASE WHEN regexp_full_match(summary_update_literal,
               '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})')
            THEN try_cast(summary_update_literal AS TIMESTAMPTZ)
       END AS summary_update_instant
FROM clocks
ORDER BY bill_id, summary_version_code, action_date, update_date;
```

The existing bill/stage/action/`update_date` key remains unchanged. The bounded
retained-original census for `ss-ze62` found no distinct text sharing that key;
it does not establish that every publisher revision fits it. Incoming rows or
prior merges with different summary text or `last_summary_update_date` at one
key refuse, preserving the original inputs for review. Changing only schema
context does not imply a source revision. Replays of unchanged or older keys
retain every admitted dated row and do not replace the newest dated row.

Summary policy `legislative-documents/3` requires a replay from original XML.
Earlier policies dropped the legacy summary clock, so their subject/receipt
bundles cannot qualify an incremental prior under this policy. Their refusal
does not mean the source changed. A rebuild that adds the previously unread
field is a processing correction, keeps the original item clock and key, and
does not invent a receipt predecessor. Build an exact affected subset into a
separate candidate; keep selected generations and raw originals unchanged.

The [final-text-to-first-summary feasibility investigation](final-text-summary-delay-feasibility.md)
qualifies the remaining publication and stage-matching gaps in a bounded cohort.
