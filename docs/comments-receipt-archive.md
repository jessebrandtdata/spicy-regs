# Comments: leaving per-row receipts

`comments` and `comments_index` publish without ETL receipts (ss-juz8, epic
ss-vse3). The mirror copies the pinned catalog table and publishes it; it no
longer exports, rebinds, admits or replays the 26.5 million receipts beside it.
This page is the operator's record of that move: what each removed pass
guarded and what guards it now, what must merge first, how to check the first
publication, and where the last generation published with receipts is pinned.

## Why

At full size (M4 Pro, 3GB / 1 thread) the paired export spent 138.5 of its
169.6 measured minutes on receipts: copy 1.3, rebind every receipt in Python
42.6, admit receipts 23.7, admit subjects 26.9, replay every subject 44.0. The
generation then admitted them again (estimated about 50 minutes) and uploaded
and read back the 3.97 GB member. The hosted estimate was 280 to 370 minutes
against a 360-minute limit.

The published receipts held nothing the table lacks. Footer statistics of all
4,708 row groups of the live member (26,561,684 rows): `processing_json` and
`diagnostic_json` are `["dict",[]]` in every row; every comments receipt is a
`comments-selected-policy-promotion/3` receipt whose witnesses point at the
v1 pair by a local path and file digest; every index receipt is derived from
the published file; the one non-accepted row is the catalog's initialization
marker. No column is added to the table.

Those per-row pointers no longer resolve. The recorded path,
`corpora/session-reports-2026-10-05/server/reader-navigation-release-20261006/run/comments-original-stage1-retained-pair-admission-20261007/original-v1/`,
does not exist, and no `original-v1` directory is left under `corpora/` (its
siblings were emptied on 2026-10-28). The pair's two file digests are recorded
only in `corpora/session-reports-2026-10-07/comments-efficiency/comments-bulk-fail-fast-findings-20261007.md`.
The pinned generation keeps the pointers, not what they point at.

## What stays

The catalog keeps its receipt table and the ETL keeps writing it:
`replace_native` writes each row and its receipt in one transaction, and every
catalog read (`processing_table`) restores rows through receipts. Untangling
that is the regulations-path step of ss-vse3, not this one. Rejected attempts
and retained refusals stay there as processing evidence; the mirror never
published a rejected or refused comments receipt (the live member has none).

## Each removed pass, and what checks it now

| Pass | Failure it detected | Now |
| --- | --- | --- |
| Rebind every receipt | A receipt row whose digest no longer matches its contents | Receipts are not published, so a damaged one cannot reach a reader. The catalog's own reads still check each digest. |
| Admit receipts | Duplicate, missing or malformed receipts for a row | `validate_export`: every `comment_id` unique and non-null, `agency_code` non-null, index counts equal per agency, docket and month, every previously published ID kept; plus the 1,000,000-row floor. |
| Admit subjects | Rows and accepted receipts not one to one, which also caught a partial read; a row whose values differ from what its receipt attests; a table off its declared schema | Completeness: the copy must hold the rows the pinned snapshot states in its Iceberg summary or, without a usable summary, as many as its accepted receipts; otherwise the export refuses with both numbers, and its log names the count used. The retained-ID check alone could not see a lost row never published, which on the first run is everything since 2026-10-10. Values: `replace_native` reads every written row back inside the writing transaction. Schema: the export refuses a catalog whose columns differ from the policy, and the generation is built against each table's declared columns and types. |
| Replay every row | A row the current mapper would not reproduce from its retained input | Dropped. For the 26.4M promotion rows the input was the row itself, so the replay could only refuse after a mapper change; such a change needs a deliberate rewrite of the catalog, not a refused publication. |
| Pair snapshot | A receipt-only commit during export | The comments table's own identity binds the export and the no-op check. A receipt-only commit (a rejected attempt, a retained refusal) publishes nothing. |

Dropped outright: detecting, at publication, a value changed outside
`replace_native` (by hand, or by an engine fault after commit). A row inserted
or deleted outside it is seen only when the receipt count decides, and only if
no receipt changed with it; the summary counts the table as written. The
predecessor check still refuses a lost ID.

The export reads no receipt row's contents. It reads the receipt table's
metadata (the pinned pair's identity) and, only when the snapshot's summary
gives no usable count, the number of accepted receipts: a scan of about 78 s.
A summary is usable when it states `total-records` and no delete file; after
R2's compaction the dockets table stated 45 records beside 51 position
deletes, so the subtraction is not trusted.

## What marks the switch

`etl_policies/comments.json` and `comments_index.json` stay as the tables'
schema declarations and carry `"receipts_retired": true`
(`subject_catalog.RECEIPTS_RETIRED`), the one owner of retirement (ss-ag1j).
Admission and every publishing path then accept a generation of the two
tables without receipts and refuse receipts for them; the dictionary and the
MCP server's `describe_table` read the marked tables as current, not legacy. The mirror asks the same rule before any catalog read,
so a family marked in part refuses in seconds: `The installed comments
policies still require ETL receipts`.

## The first publication: checks from the public files

Run in DuckDB after the first receipts-free publication; `old_prefix` is the
prefix the comments family held before it (the pinned generation below).

```sql
INSTALL httpfs; LOAD httpfs;
SET VARIABLE base = '{{ R2_PUBLIC_URL }}';

-- 1. The family has no receipt member; note the prefix.
SELECT families.comments.prefix, families.comments.etlReceipts, families.comments.publishedAt
FROM read_json(getvariable('base') || '/publication.v2.json');
-- expect etlReceipts NULL

SET VARIABLE new = getvariable('base') || '/<prefix>/comments.parquet';
SET VARIABLE old = getvariable('base') || '/<old_prefix>/comments.parquet';

-- 2. Same 24 columns in the same order and types.
SELECT (SELECT list(column_name || ' ' || column_type) FROM (DESCRIBE SELECT * FROM read_parquet(getvariable('new'))))
     = (SELECT list(column_name || ' ' || column_type) FROM (DESCRIBE SELECT * FROM read_parquet(getvariable('old'))))
       AS same_schema;
-- expect true

-- 3. No published ID lost; the IDs are unique.
SELECT (SELECT count(*) FROM (SELECT comment_id FROM read_parquet(getvariable('old'))
                              EXCEPT SELECT comment_id FROM read_parquet(getvariable('new')))) AS lost,
       (SELECT count(*) - count(DISTINCT comment_id) FROM read_parquet(getvariable('new')))     AS repeated;
-- expect 0 and 0
```

Check 3 reads one column of each generation, about 0.5 GB. Then, outside
DuckDB: the job log has no `Paired export` line; the generation's
`artifact.json` names two tables and no `etlReceipts`; the
`comments-publication.json` receipt names the new generation;
`uv run --frozen python scripts/check_comments_freshness.py` passes.

## Catalog recovery

`scripts/rebuild_native_catalog.py` seeds the catalog from the live
generation's pair and copies its receipts verbatim. Once comments publish
without receipts it refuses, naming the pinned archive generation; it
cannot be pointed at that generation, which is older than the live table. A
rebuild from a receipts-free generation needs a seeding mode that writes one
promotion-style receipt per row, which is not built. The catalog's own receipt
table is untouched by this change; this matters only if the catalog is lost.

## Archive pin

The last comments generation published **with receipts** holds the only
complete exported copy of them. It is pinned, for both datasets, in
`docs/pins/receipt-archive.json`, the one pin file for every retired table;
weekly retention keeps a generation a file under `docs/` cites by eight or more
hex digits, and `tests/test_receipt_archive_pins.py` holds every marked table
to an entry there that the scan finds. Existing receipt files are never
deleted by this change.

That generation is the 2026-10-10 one (published 2026-10-10T15:45:23Z). Its
receipt member holds 26,561,684 rows (datasets `comments` and
`comments_index`), 3,968,509,123 bytes,
`sha256:3a9a9cddb235d5055ae49d8fbe430c61dcce8bcfdec9e14023c7db5adcd9ea0d`,
receipt generation `comments-v2-c2dd1f166abd441b80eb02373d625c88`; the live
index stated all of this on 2026-11-10, when the generation was still current.
It is the last one because the receipt-route publication started from the
operator's machine on 2026-11-10 was stopped before it uploaded anything, so
the next comments publication is this route's. The catalog took these receipts
in its 2026-11-08 rebuild and keeps writing its own receipt table; the pin
keeps the exported copy.

## The bare `comments.parquet`

The fixed `comments.parquet` at the bucket root (6,441,122,905 bytes, last
written 2026-10-03), its 180 agency files and `comments_index.parquet` are
rewritten by every mirror publication: they are the predecessor whose IDs it
must keep, and `agency_stats`, `feed_summary` and `org_committee_links` read
the root file. The 2026-10-10 generation came from the v1 promotion, which
wrote only the generation, so this route's first publication is the first to
refresh them since 2026-10-03.

**Their shape changes for anonymous readers.** Today they hold
`attachments_json`, the attachment list as JSON text (read from the root
file's and the FAA file's footers on 2026-11-10). The first receipts-free
publication writes the generation's shape: `attachments`, a list of structs
(`title`, `formats`, `attachment_id`, `restrict_reason`, `restrict_reason_type`),
in its place (18th in the monolith; the agency files have no `agency_code`); every other
column is unchanged. No scheduled
reader here uses `attachments_json` from these files. Two manual commands do,
on local copies of them: `enrich-pdf-text` and `backfill-comment-text` without
`--use-iceberg`. The scheduled backfill runs both with `--use-iceberg`, which
reads the catalog. Only the agency files may shrink below half their prior
size on upload; the monolith and the generation keep the guard.
