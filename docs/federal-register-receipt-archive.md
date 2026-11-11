# Federal Register tables: leaving per-row receipts

`federal_register` and `fr_docket_links` are the first tables to move from
per-row ETL receipts to a merge at the public table. The evidence for a row
becomes the run's journal, the retained API page `source_sha256` names, and the
generation's pinned prior. This page is the operator's record of that move:
what each merged step does on its first night, how to check it from the public
files, and where the last generations published with receipts are pinned.

## Where the change stands

| Step | State | What a nightly does |
| --- | --- | --- |
| 1. Promote the fields the receipts held | merged | Receipt route. `federal_register` gains `html_url`, `pdf_url`, `body_html_url`, `source_sha256`, `regulations_dot_gov_info_json` as its last columns; `fr_docket_links` gains `normalized_docket_candidates`. |
| 2. Readers use the published tables | merged | The links job and the rulemaking materializer read the pinned `federal_register` table, not its receipts. Receipts are still written. |
| Retirement rule | merged | A table's installed policy marked `"receipts_retired": true` stops needing receipts and may never state them again, on every path that builds or publishes (ss-ag1j); `comments` and `comments_index` were the first marked (ss-juz8). |
| 3. Merge at the public table, no receipts | merged | Both rollups are plain: the Register merges the night's mapped documents into the pinned public table, the links rebuild from it, neither publishes receipts, both policies are marked `receipts_retired`, the jobs time out at 30 minutes. It followed one clean night of steps 1 and 2 (2026-11-10, run 42902380757: the eight checks below as expected), whose generations, the last with receipts, are pinned below. |

## After step 3: what the merge does not do

- **A carried row keeps the mapping of the night that last read it.** The
  receipt route mapped every row again each night; the merge maps only the
  night's documents and carries every other row as published. `rin` is the one
  derived column it recomputes on every row. Any other mapping change, in
  `regulations_shape` or in SpicyDocs' projection, reaches rows older than the
  120-day window only through a full re-read: run the Register workflow with
  `since` 1994-01-01 (`FR_SINCE=1994-01-01`), which keeps 240 minutes. A column
  the contract adds reads NULL on carried rows until then; a prior without one
  of the columns that left receipts is refused.
- **A document the mapping refuses fails the night.** The receipt route
  refused such a row into its receipt and published the rest; the merge has no
  receipt to refuse it into, so the job stops with the mapper's message,
  publishes nothing, and the prior generation stays live until the mapping is
  fixed.
- **Recovery is forward only.** Once a generation without receipts is live, a
  revert of step 3 cannot run: the receipt route selects its prior by the
  generation stating receipts for the table and refuses when none does
  (`selected native input requires one ETL receipt generation`). A fault in
  the merge route is fixed in the merge route. The pinned generation stays
  stored, so its receipts stay readable.

## The first night of steps 1 and 2

- **Job 1 (`federal_register`) takes about 55 to 80 minutes, once.** The prior
  was published under `regulations-native-v2` and the code is `-v3`, so the
  restore reads it row by row: its log has exactly one line
  `regulatory restore route=row dataset=federal_register ... reason=subject-schema-differs`.
  The two earlier nights that took this route on a hosted runner spent 28.7
  minutes (2026-10-31) and 56.2 minutes (2026-11-01) in the restore alone,
  against 4.6 to 6.9 on an ordinary night. The job's limit is 180 minutes.
  From the second night the line is gone and the job is back to about 29.
- **Job 2 (`fr_docket_links`) takes about 10 to 13 minutes**, down from 15.6: it
  no longer restores the Register table through receipts.
- **The rulemaking materializer runs unattended.** It is not a manual job:
  `_regulations-refresh.yml` runs it inside the scheduled ETL refresh, so the
  first refresh after the merge reads the two Register tables as published. It
  works against either Register generation, before or after job 1.
- **"Deploy data dictionary" stays green.** Until both jobs publish, the live
  check prints one notice per table (`declared, not yet published: ...`) and no
  failure. After both have published it prints none.
- **Run alone, the links workflow refuses** until job 1 has published:
  `The pinned federal_register generation predates its promoted columns`.

## After the first night: checks from the public files

Run in DuckDB. Take the two prefixes from the index first; `old_*` is the
generation each family held before the first night, from the new root's
`prior-generation` input or from the previous copy of the index.

```sql
INSTALL httpfs; LOAD httpfs;
SET VARIABLE base = '{{ R2_PUBLIC_URL }}';

-- 1. Both families still claim their receipt dataset; note the two prefixes.
SELECT families."federal-register".prefix               AS register_prefix,
       families."federal-register".etlReceipts.datasets AS register_receipts,
       families."fr-docket-links".prefix                AS links_prefix,
       families."fr-docket-links".etlReceipts.datasets  AS links_receipts
FROM read_json(getvariable('base') || '/publication.v2.json');
-- expect register_receipts = [federal_register], links_receipts = [fr_docket_links]

SET VARIABLE register     = getvariable('base') || '/<register_prefix>/federal_register.parquet';
SET VARIABLE links        = getvariable('base') || '/<links_prefix>/fr_docket_links.parquet';
SET VARIABLE old_register = getvariable('base') || '/<old register prefix>/federal_register.parquet';
SET VARIABLE old_links    = getvariable('base') || '/<old links prefix>/fr_docket_links.parquet';
```

```sql
-- 2. The roots declare regulations-native-v3 and -v2.
SELECT policy.dataset, policy.policy_version
FROM (SELECT unnest(spec.etlReceipts.policies) AS policy
      FROM read_json(getvariable('base') || '/<register_prefix>/artifact.json'));
-- repeat with <links_prefix>; expect federal_register regulations-native-v3, fr_docket_links regulations-native-v2

-- 3. Widths: 43 and 17 columns, the promoted ones last.
SELECT count(*) AS columns, list(column_name)[-5:] AS last_five
FROM (DESCRIBE SELECT * FROM read_parquet(getvariable('register')));
SELECT count(*) AS columns, list(column_name)[-1:] AS last_one
FROM (DESCRIBE SELECT * FROM read_parquet(getvariable('links')));
-- expect 43, [html_url, pdf_url, body_html_url, source_sha256, regulations_dot_gov_info_json]
-- expect 17, [normalized_docket_candidates]

-- 4. Every row was filled from its receipt.
SELECT count(*)                                                  AS rows,
       count(*) FILTER (html_url IS NULL)                        AS html_url_null,
       count(*) FILTER (body_html_url IS NULL)                   AS body_html_url_null,
       count(*) FILTER (pdf_url IS NULL)                         AS pdf_url_null,
       count(*) FILTER (regulations_dot_gov_info_json IS NULL)   AS statement_null,
       count(*) FILTER (regulations_dot_gov_info_json = '{}')    AS statement_empty,
       count(source_sha256)                                      AS rows_with_pointer
FROM read_parquet(getvariable('register'));
-- expect html_url_null 0, body_html_url_null 0, statement_null 0,
-- pdf_url_null about 33,500 (33,485 on 2026-11-09), statement_empty about 216,700,
-- rows_with_pointer about 8,700 (the documents of the 120-day window)

-- 5. html_url is the document's own page. Every row should match; explain any that does not.
SELECT document_number, publication_date, html_url
FROM read_parquet(getvariable('register'))
WHERE NOT starts_with(html_url, 'https://www.federalregister.gov/documents/'
                                || strftime(CAST(publication_date AS DATE), '%Y/%m/%d') || '/' || document_number || '/')
LIMIT 20;
-- expect no rows

-- 6. The pointer is a page digest and sits inside the re-read window.
SELECT count(*) FILTER (NOT regexp_full_match(source_sha256, 'sha256:[0-9a-f]{64}')) AS malformed,
       count(DISTINCT source_sha256)                                                AS pages,
       min(publication_date) FILTER (source_sha256 IS NOT NULL)                     AS oldest_pointed,
       CAST(CAST(max(publication_date) AS DATE) - INTERVAL 120 DAY AS DATE)         AS newest_minus_120_days
FROM read_parquet(getvariable('register'));
-- expect malformed 0, about 10 pages, oldest_pointed within a day or two of newest_minus_120_days
-- (the window starts 120 days before the PRIOR table's newest date) and never earlier than it by more than that

-- 7. Nothing older than the window changed: the 38 prior columns, both ways.
SET VARIABLE window_start = (SELECT CAST(CAST(max(publication_date) AS DATE) - INTERVAL 120 DAY AS VARCHAR)
                             FROM read_parquet(getvariable('old_register')));
SELECT (SELECT count(*) FROM (
          SELECT * EXCLUDE (html_url, pdf_url, body_html_url, source_sha256, regulations_dot_gov_info_json)
          FROM read_parquet(getvariable('register')) WHERE publication_date < getvariable('window_start')
          EXCEPT ALL
          SELECT * FROM read_parquet(getvariable('old_register')) WHERE publication_date < getvariable('window_start')))
                                                                                    AS new_not_in_old,
       (SELECT count(*) FROM (
          SELECT * FROM read_parquet(getvariable('old_register')) WHERE publication_date < getvariable('window_start')
          EXCEPT ALL
          SELECT * EXCLUDE (html_url, pdf_url, body_html_url, source_sha256, regulations_dot_gov_info_json)
          FROM read_parquet(getvariable('register')) WHERE publication_date < getvariable('window_start')))
                                                                                    AS old_not_in_new;
-- expect 0 and 0

-- 8. The same for the links' 16 prior columns.
SELECT (SELECT count(*) FROM (
          SELECT * EXCLUDE (normalized_docket_candidates)
          FROM read_parquet(getvariable('links')) WHERE publication_date < getvariable('window_start')
          EXCEPT ALL
          SELECT * FROM read_parquet(getvariable('old_links')) WHERE publication_date < getvariable('window_start')))
                                                                                    AS new_not_in_old,
       (SELECT count(*) FROM (
          SELECT * FROM read_parquet(getvariable('old_links')) WHERE publication_date < getvariable('window_start')
          EXCEPT ALL
          SELECT * EXCLUDE (normalized_docket_candidates)
          FROM read_parquet(getvariable('links')) WHERE publication_date < getvariable('window_start')))
                                                                                    AS old_not_in_new,
       (SELECT count(*) FROM read_parquet(getvariable('links')) WHERE normalized_docket_candidates IS NULL)
                                                                                    AS candidates_null;
-- expect 0, 0 and 0 (a label the normalizer reads no docket from holds an empty list)
```

Checks 7 and 8 read both generations whole, about 0.7 GB over HTTP; the others
read a few columns.

Then, outside DuckDB:

- Job 1's log has exactly one
  `regulatory restore route=row dataset=federal_register` line, with
  `reason=subject-schema-differs`. Job 2's log has none for `federal_register`.
  On the second night job 1's log has none.
- `uv run --frozen spicy-regs-dict check --source r2 --base "$R2_PUBLIC_URL"`
  passes and prints no `declared, not yet published` notice for either table.
- `uv run --frozen python -m spicy_regs.policy_migrations --base-url "$R2_PUBLIC_URL"`
  reports every published policy recorded.

A comparison of two generations must leave `source_sha256` out, as check 7
does: it changes on every row in the window each night whether or not the
record changed.

## Archive pins

Weekly retention keeps a family's last three generations and any generation a
file under `docs/` cites by eight or more hex digits of its digest. The last
generation each family publishes **with receipts** holds the only complete copy
of those receipts, so step 3 pins it in `docs/pins/receipt-archive.json`, one
entry per table, in the same change that marks the table's receipts retired.
`tests/test_receipt_archive_pins.py` fails while a retired table has no entry
or retention's docs scan cannot find one. Existing receipt files are never
deleted by this change; the pin is what keeps retention from removing them.

Step 3 pins the generations of 2026-11-10 (run 42902380757), the last
published with receipts; a night run before it merged would have moved both
pins to that night's. A pin is filled from the public index: read
`families."federal-register"` and `families."fr-docket-links"` from
`publication.v2.json` (query 1 above, with `artifactDigest` and `publishedAt`
in place of `prefix`), confirm each still lists `etlReceipts`, and write each
table's entry: `family`, `artifactDigest` (`sha256:` and the 64 hex digits),
`publishedAt`, and `receipts`, the key
`generations/<family>/<64 hex>/etl_receipts.parquet`. Commit each pinned
generation's `artifact.json`, gzipped, as
`tests/fixtures/receipt_archive/<64 hex>.json.gz`: the pin test recomputes its
digest and reads its receipt policies. Do not write any other generation's
digest under `docs/`: every digest cited there is kept indefinitely.
