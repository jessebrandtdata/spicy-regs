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
`comments-selected-policy-promotion/3` receipt whose witnesses name the
retained v1 pair; every index receipt is derived from the published file; the
one non-accepted row is the catalog's initialization marker. No column is
added to the table.

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
| Admit subjects | A row whose values differ from what its receipt attests; a table off its declared schema | Values: `replace_native` reads every written row back inside the writing transaction. Schema: the export refuses a catalog whose columns differ from the policy, and the generation is built against each table's declared columns and types. |
| Replay every row | A row the current mapper would not reproduce from its retained input | Dropped. For the 26.4M promotion rows the input was the row itself, so the replay could only refuse after a mapper change; such a change needs a deliberate rewrite of the catalog, not a refused publication. |
| Pair snapshot | A receipt-only commit during export | The comments table's own identity binds the export and the no-op check. A receipt-only commit (a rejected attempt, a retained refusal) publishes nothing. |

Dropped outright: detecting, at publication, a catalog row changed outside
`replace_native` (by hand, or by an engine fault after commit). The predecessor
check still refuses a lost ID; it cannot see a changed value.

## Before it can run

- **The receipts kit marks both policies retired.** `etl_policies/comments.json`
  and `comments_index.json` stay as the schema declaration, with the kit's
  "receipts retired" marker, which `require_registered_receipts` must skip.
  Until then every build is refused at admission, so the mirror refuses first,
  in seconds: `The installed comments policies still require ETL receipts`.
- **The dictionary and the MCP server treat a marked family as native.**
  Today a family without `etlReceipts` is read as legacy
  (`data_dictionary.selected_dictionary_definitions`, `mcp_server._legacy_column_meanings`):
  the live check would expect `attachments_json` where the table has
  `attachments`, and "Deploy data dictionary" would fail from the first
  receipts-free publication.

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

`scripts/rebuild_native_catalog.py` seeds the catalog from a published pair
and copies its receipts verbatim. A receipts-free generation has none, so a
rebuild from it would first need a seeding mode that writes one
promotion-style receipt per row. The catalog's own receipt table is untouched
by this change; this matters only if the catalog is lost.

## Archive pin

Weekly retention keeps a family's last three generations and any generation a
file under `docs/` cites by eight or more hex digits of its digest. The last
comments generation published **with receipts** holds the only complete
exported copy of those receipts, so it is pinned here. Existing receipt files
are never deleted by this change.

| Family | Last generation published with receipts | Published |
| --- | --- | --- |
| `comments` | `08e42cccdb0a57ff5e9fb7c0e0a825ff578c2e349e308289acaab86f766266a1` | 2026-10-10T15:45:23Z |

Its receipt member is `generations/comments/08e42cccdb0a57ff5e9fb7c0e0a825ff578c2e349e308289acaab86f766266a1/etl_receipts.parquet`:
26,561,684 rows (datasets `comments` and `comments_index`), 3,968,509,123
bytes, `sha256:3a9a9cddb235d5055ae49d8fbe430c61dcce8bcfdec9e14023c7db5adcd9ea0d`,
receipt generation `comments-v2-c2dd1f166abd441b80eb02373d625c88`. The live
index stated all of this on 2026-11-10, when the generation was still current.

It is the last one because the receipt-route publication started from the
operator's machine on 2026-11-10 was stopped before it uploaded anything, so
the next comments publication is this route's. The catalog took these receipts
in its 2026-11-08 rebuild and keeps writing its own receipt table; this pin
keeps the exported copy. `tests/test_generation_retention.py` fails if the
docs scan stops finding the digest. Do not write any other generation's digest
on this page: every digest cited here is kept indefinitely.

## The bare `comments.parquet`

The fixed `comments.parquet` at the bucket root (6.44 GB, last written
2026-10-03), its agency files and `comments_index.parquet` are rewritten by
every mirror publication: they are the predecessor whose IDs it must keep, and
`agency_stats`, `feed_summary` and `org_committee_links` read the root file.
The 2026-10-10 generation came from the v1 promotion, which wrote only the
generation, so this route's first publication is the first to refresh them
since 2026-10-03.
