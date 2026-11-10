# Court tables: leaving per-row receipts

The four tables rebuilt whole from one CourtListener bulk export publish
without ETL receipts: `court_opinions` (family `court-opinions`) and
`court_citations`, `court_citation_map`, `court_parentheticals` (family
`court-citations`). Epic ss-vse3. This page is the operator's record: what the
receipts held and where it went, the archive pins, how the first receipts-free
publication of each family is checked and run, and the refresh of
`court_opinions` from the 2026-09-30 export that waited for this change.

The other court families (`court-opinion-clusters`, `courtlistener`,
`court-docket-groups`, `court-opinion-pdf-extractions`) still publish receipts;
their switch is planned at the end of this page.

## What the receipts held, and where it went

Measured 2026-11-10 from the public index and the receipts' Parquet footers
(statistics of every row group) and twelve evenly spaced row groups of each
receipts file, read by range:

| Table | Rows | Table | Receipts | Receipt-only fields |
| --- | ---: | ---: | ---: | --- |
| `court_citations` | 18,130,235 | 157 MB | 12.0 GB for the family | `date_created`, `date_modified`, `dump_date` |
| `court_citation_map` | 78,404,647 | 446 MB | (shared) | `dump_date`; the original text of `depth` |
| `court_parentheticals` | 6,561,878 | 415 MB | (shared) | `dump_date`; the original text of `score` |
| `court_opinions` | 10,798,347 | 410 MB | 4.22 GB | `local_path`, `extracted_by_ocr`, `date_created`, `date_modified`, `dump_date`; the original text of `per_curiam` and `page_count` |

Every receipt in both families is `accepted` (footer statistics, all 51,550 and
5,400 row groups): none records a refusal. 91% of the citations family's
receipt bytes are three SHA-256 columns (`record_id`, `receipt_id`,
`subject_version`); `processing_json`, which holds every field above, is 63.5
MB of its 12.0 GB.

Each field the publisher's record holds is now a column, appended after the
contract's: `date_created`, `date_modified` (the publisher's text),
`local_path`, `extracted_by_ocr` (BOOLEAN, as `per_curiam`), `dump_date` (the
export's edition) and `source_sha256`, the SHA-256 of the export file the row
was read from. That file is the original: a run with evidence downloads it
once into the shared raw archive with its capture receipt and reads it there,
and publication keeps it as source evidence. The policies moved to `courts/2`
(citations family) and `courts/3` (`court_opinions`) for the new columns, and
each carries `"receipts_retired": true`.

The original text of a converted value (`t`/`f`, an integer, a score) is not a
column: in every sampled row it was the canonical spelling of the typed value
(`t`/`f`; a decimal integer without leading zeros; the shortest round-trip
double without a trailing `.0`, as PostgreSQL writes them). The writer checks
every row against that rule. A row spelled otherwise is published typed and
recorded as `respelled` in `<table>.exceptions.parquet` with its whole source
row; a row the mapping refuses is left out, as before, and recorded as
`refused`. The run's evidence keeps that file and journals each table's
counts (`courtlistener-bulk-table`). Those are the only row-level records left.

`dump_date` being a column also makes the declared joins on
`(opinion_id, dump_date)` from the citation map and parentheticals readable
without receipts.

## Equality with the receipt route

`tests/test_court_receipt_switch.py` builds each table from the head of the
real retained export (citations, map and parentheticals of 2026-09-30, opinions
of 2026-06-30; 300 to 400 records each, plus a few rows with non-canonical
spellings and one refusal) and compares it with what main e20a8ea1's receipt
route built from the same files: the receipt route's columns are the same
table (schema, every row both ways, the key at every position), every promoted
column equals the copy its receipts held, and no receipts file is written.

What that cannot see: the whole export. `scripts/check_court_receipt_switch.py`
runs the same two comparisons at full size before each family's first
receipts-free publication: the table with `receipt_switch.table_differences`,
the promoted columns against the whole live receipts with
`receipt_switch.receipt_copy_differences`, which reads them in DuckDB by range
instead of row by row in Python (the same test file runs it on the fixtures and
shows it sees a changed value or a lost row).

## Archive pins

`docs/pins/receipt-archive.json` pins the last generation each family
published with receipts, the only complete copy of them, and
`tests/test_receipt_archive_pins.py` checks each pin against that generation's
own root (`tests/fixtures/receipt_archive/`). Neither family republishes on a
schedule before 2027-01-09, so these are the generations a first receipts-free
publication replaces; publication refuses it otherwise.

| Family | Generation | Published | Receipts |
| --- | --- | --- | --- |
| `court-citations` | `7c2fc74c1b8096801f0f779baceb77e538e34047f234afbd9e56aa6515261d9d` | 2026-10-20 | 103,096,760 rows, 12,024,021,363 bytes |
| `court-opinions` | `85589a33b63f22bfac522e5f2c41210eb69437b763b81380562eda1fd33ef298` | 2026-10-07 | 10,798,347 rows, 4,220,509,993 bytes |

## The first receipts-free publication

Run from a checkout of main whose `.env` holds `R2_PUBLIC_URL` (the build reads
the live index through it), through the project runner, with
`SPICYGOV_RAW_ROOT` unset so originals go to the shared raw archive
(`~/Work/corpora/spicyregs/raw`). Check first that no nightly job is running
(`docs/local-family-refresh.md`). The workstation's disk floor is 100 GiB by
default; `SPICY_REGS_DISK_FLOOR_GIB` states a lower one.

```sh
export UV="/opt/homebrew/bin/uv run --frozen --extra comments-reader"
export WORK=/path/to/a/fresh/scratch/dir RAW=~/Work/corpora/spicyregs/raw/objects/sha256
mkdir -p "$WORK/bulk"
```

### court-citations: replay the published edition

The live generation is the 2026-09-30 edition, and its three exports are in the
raw archive (0.95 GB). An APFS clone (`cp -c`) names each under its publisher
filename without a second copy:

```sh
cp -c "$RAW/f8/f8ab2a9a8722733c5573ce7ef8e3863a32b66a5e8a1dabdf38d8c9180cc8b75d" "$WORK/bulk/citations-2026-09-30.csv.bz2"
cp -c "$RAW/a9/a992c9272fc9b5b8629189e78a6106e1d20d76c2cbfde4c30dbd610b2fc8deab" "$WORK/bulk/citation-map-2026-09-30.csv.bz2"
cp -c "$RAW/db/dbbd1d6030033f06950901d36aea93032d1e59e4f3d46a19369759787bee8fcc" "$WORK/bulk/parentheticals-2026-09-30.csv.bz2"
COURTLISTENER_BULK_DIR="$WORK/bulk" SPICY_REGS_DISK_FLOOR_GIB=10 \
  $UV run-rollup-court-citations --output-dir "$WORK/citations"
$UV python scripts/check_court_receipt_switch.py --family court-citations \
  --generation "$WORK/citations/generations/<digest>" --scratch "$WORK/live-citations"
```

The rollup builds the newest edition the bucket lists for all three tables;
while that is 2026-09-30 it reads these files and downloads nothing (it refuses
if the bucket lists 2026-12-31 by then: rebuild that edition instead and skip
the comparison, which needs the same edition). Expected, from the workflow's
2026-09-22 measurement and not re-run: about 17 minutes of decoding on one core
plus writing 102 million rows; DuckDB is held to 4 GB; about 3 GB of scratch.
The check downloads the live tables (1.0 GB) and reads the live receipts by
range (identity and processing columns of the three datasets' row groups,
estimated 0.7 GB of the 12.0 GB file); its `EXCEPT ALL` over 78 million map
rows spills to disk and was not measured at that size. Every count it prints
must be 0. Then publish as the hand-run families do
(`docs/local-family-refresh.md`): `publish-local-generation --generation …
--evidence … --family court-citations --dry-run`, then again with
`--expect-prior sha256:7c2fc74c1b8096801f0f779baceb77e538e34047f234afbd9e56aa6515261d9d`.

Otherwise the hosted quarterly run of 2027-01-09 is the first, on the
2026-12-31 edition, with no same-edition comparison. Its peak is the 0.95 GB of
exports in the runner's raw directory, about 1.5 GB of staging and two copies
of the 1.1 GB tables: about 5 GB of the runner's 14 GB, where admitting 103
million receipts ran out of disk (ss-4n0p).

### court-opinions: equality on 2026-06-30, then the 2026-09-30 refresh

The 2026-09-30 export is 55,252,643,429 bytes (51.5 GiB; HEAD on 2026-11-10,
ETag `"7c4caf053f805946e812728f507fd002-6587"`). The raw archive holds the
2026-06-30 one (`65db4902…`, 54.6 GB), the edition the live table holds, and
not this one.

1. **Equality on the published edition.** The rollup always builds the newest
   edition, so this step calls the builder for 2026-06-30 and admits the result
   the way the rollup does, upload skipped:

   ```sh
   cp -c "$RAW/65/65db4902a4ae42c48ef9958f52600dedca5401c194d3611d0b2672cfc6dd4d9c" "$WORK/bulk/opinions-2026-06-30.csv.bz2"
   COURTLISTENER_BULK_DIR="$WORK/bulk" SPICY_REGS_DISK_FLOOR_GIB=10 $UV python -c '
   import sys
   from datetime import date
   from pathlib import Path
   from spicy_regs.transforms.build_court_bulk_tables import OPINIONS, build_court_bulk_tables
   build_court_bulk_tables((OPINIONS,), Path(sys.argv[1]), dump_date=date(2026, 6, 30))' "$WORK/opinions-0630"
   mkdir -p "$WORK/opinions-0630/generation" && mv "$WORK/opinions-0630/court_opinions.parquet" "$WORK/opinions-0630/generation/"
   $UV python scripts/check_court_receipt_switch.py --family court-opinions \
     --generation "$WORK/opinions-0630/generation" --scratch "$WORK/live-opinions"
   ```

   About an hour of decoding (SpicyDocs measured its parallel local decoder at
   about an hour for this export), one more read of the 54.6 GB to hash it for
   `source_sha256`, no download. The check downloads the live table (0.41 GB)
   and reads the live receipts' identity and processing columns by range
   (about 0.7 GB of 4.2 GB, estimated from their footers).
2. **The refresh.**

   ```sh
   SPICY_REGS_DISK_FLOOR_GIB=10 $UV run-rollup-court-opinions --output-dir "$WORK/opinions"
   ```

   It downloads the export once, straight into the shared raw archive with its
   capture receipt (no second copy), and reads it there. Expected: about 8.4
   hours of download at the bucket's 1.75 MiB/s (a rate the citations workflow
   recorded on a hosted runner, not measured from this machine), then about an
   hour of decoding and writing about 11 million rows. Disk: 51.5 GiB kept in
   the archive for good, plus a few GB of decode pieces, staging and two copies
   of a table estimated at 0.6 to 0.7 GB (0.41 GB today plus the promoted
   fields, which take at most the 0.31 GB of the receipts' processing column).
   Free space on the workstation was 77 GB on 2026-11-10: about 20 GB of
   margin. Memory: DuckDB is held to 4 GB; the decoder's pieces and parse
   workers add to that (not measured; run under the memory watchdog).
3. **Publish** with `publish-local-generation`, `--expect-prior
   sha256:85589a33b63f22bfac522e5f2c41210eb69437b763b81380562eda1fd33ef298`.
   Publication uploads the run's evidence, the 51.5 GiB export included, to
   `source-evidence/blobs/sha256/` and reads each blob back once: about 103 GiB
   of transfer, whose time depends on the uplink (not measured).

How each step scales: the download and the decode are linear in the export's
bytes, and the writer is linear in rows, a batch of 100,000 at a time with one
DuckDB query each; only the rows the SQL flags (none in the samples) go
through Python one at a time. Nothing reads the prior table, so a refresh
costs the same whatever was published before. The receipt route also hashed
the export a second time for a witness and wrote, admitted, uploaded and read
back 4.2 GB of receipts; none of that remains. The comparisons are joins on
the key, linear with spill; they run once per family, before its first
receipts-free publication.

## Decisions for the owner

- **Keep the 51.5 GiB opinions export in R2 as well as in the raw archive?**
  Publication does so for every original a run captures: the export can then
  be restored by a hosted job, at about 103 GiB of transfer at publish and
  about $0.77 a month (at $0.015 per GB-month). Not keeping it there needs a
  publication path that cites an original without uploading it.
- **First citations publication now (replay of 2026-09-30) or at the
  2027-01-09 quarterly?** Now gives readers the new columns at once and proves
  the route at full size on a known edition; waiting costs nothing to run.

## The other court families (planned)

| Family | Cadence | Receipt-only fields | Prior read today | Switch |
| --- | --- | --- | --- | --- |
| `court-opinion-clusters` | daily search catch-up; rebuild per export | `source`, `slug`, `absolute_url`, `date_created`, `date_modified`, `ingest_source`, `raw_source_record` (search rows) | rebuild restores the 3.4 GB table through 2.1 GB of receipts; catch-up downloads both and combines receipts | promote these columns; the catch-up appends to the table alone; the rebuild merges in DuckDB at the published table |
| `courtlistener` (`court_dockets`) | daily | `date_created`, `absolute_url`, `raw_source_record`, the list originals | restores through receipts every night | promote; merge at the published table |
| `court-docket-groups` | by hand | `edition`, `rule_version`, `match_basis` | none | promote |
| `court-opinion-pdf-extractions` | by hand | none (`sha1_matches` text is the writer's own) | prior and receipts | mark retired |

The clusters' daily catch-up keeps its place inside the 125-a-day quota in the
generation's evidence journal (`catchup-selection` / `export-selection`:
edition and the id reached), not in receipts, so its resume state survives the
switch unchanged. Each daily family's first receipts-free night must read its
prior's receipts one last time to fill the new columns, and its pin must be
written the day it merges.
