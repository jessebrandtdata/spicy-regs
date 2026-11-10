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
their switch is planned in ticket ss-vse3.

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
5,400 row groups): none records a refusal. The review of this change read every
live receipt (2026-11-10, 26 minutes for the citations family's 12.0 GB in one
pass): every original spelling of `depth`, `score`, `per_curiam` and
`page_count` is the canonical one, and the live map repeats no
`(citing_opinion_id, cited_opinion_id)` pair. 91% of the citations family's
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
More than 1,000 of them in one table stop the build (`court_bulk.MAX_EXCEPTIONS`):
that would be a change of the export's spelling, to review, not to route row by
row through Python. A table that holds an identity twice also stops the build,
as the receipt route's admission did; the previous table stays.

The declared joins from the citation map and parentheticals to
`court_opinions` are on `(opinion_id, dump_date)`: they match only rows of one
edition. Until opinions are refreshed to 2026-09-30, the citation tables
(2026-09-30) and opinions (2026-06-30) do not meet on them.

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

## Decisions (2026-11-10)

- **D1.** The opinions export is uploaded to R2 source evidence once, at its
  publish: the evidence is then whole by itself (about $0.8 a month). The raw
  mirror under `spicyregs/raw/` is not read by publication.
- **D2.** Citations' first receipts-free publish is hosted, through
  `rollup-court-citations.yml`: a run with upload off, the check below on its
  artifact, then the real run.
- **D3.** Opinions: the same-edition check on 2026-06-30 runs before the
  2026-09-30 refresh.
- **D4.** One download connection. One 32 MiB range from the workstation came
  at about 19.9 MiB/s (review, 2026-11-10): about 45 minutes for the whole
  export if that holds, against 8.4 hours at the 1.75 MiB/s a hosted runner saw.
- **D5.** For the later clusters step: `absolute_url` is derived
  (`https://www.courtlistener.com/opinion/<cluster_id>/<slug>/`), `slug` is
  promoted.

## The first receipts-free publication

Publish soon after this change merges. From the merge on, `read_receipt_fields`
answers that these four tables' receipts are retired, while the live tables do
not yet carry the promoted columns: until each family publishes, those fields
are readable only from the pinned generations' receipts.

The check, `scripts/check_court_receipt_switch.py`, compares a build of the
edition the live generation holds with it: the contract columns with
`receipt_switch.table_differences` against the live table (downloaded and
checked against its pin) and every promoted column with
`receipt_switch.receipt_copy_differences` against the live receipts, read by
range. DuckDB is held to 4 GB and spills. Every count it prints must be 0;
anything else stops the publish.

### court-citations, hosted

The live generation holds the 2026-09-30 edition, and the workflow builds the
newest edition the bucket lists for all three tables, which is 2026-09-30 until
the 2026-12-31 export is listed. The parent dispatches:

```sh
gh workflow run rollup-court-citations.yml -R mikewolfd/spicy-regs --ref main -f skip_upload=true
gh run list -R mikewolfd/spicy-regs --workflow rollup-court-citations.yml --limit 1   # its run id: $RUN
gh run download "$RUN" -R mikewolfd/spicy-regs -n "rollup-output-run-rollup-court-citations-$RUN-1" -D "$WORK/citations-run"
/opt/homebrew/bin/uv run --frozen --extra comments-reader python scripts/check_court_receipt_switch.py \
  --family court-citations --generation "$WORK/citations-run/generations/<digest>" --scratch "$WORK/live-citations"
gh workflow run rollup-court-citations.yml -R mikewolfd/spicy-regs --ref main -f skip_upload=false
```

The build downloads the three exports (0.95 GB; nine minutes at 1.75 MiB/s)
into the runner's raw directory, decodes 102 million rows (17 minutes on a
workstation core on 2026-09-22, slower hosted) and writes them; the receipt
route ran out of disk admitting the receipts (ss-4n0p), and without them the
peak is about 5 GB of the runner's 14. The check reads the live receipts' key
and processing columns of all three datasets in one pass: the review's similar
read took 26 minutes. The artifact holds the exports again, so the check needs
about 3 GB of scratch. The real run publishes the tables and uploads the
exports to source evidence; the pin is checked there.

### court-opinions, on the workstation

The 2026-09-30 export is 55,252,643,429 bytes (51.5 GiB; HEAD on 2026-11-10,
ETag `"7c4caf053f805946e812728f507fd002-6587"`). The raw archive holds the
2026-06-30 one (`65db4902…`, 54.6 GB), the edition the live table holds, and
not this one. Run from a checkout of main whose `.env` holds `R2_PUBLIC_URL`,
through the project runner, with `SPICYGOV_RAW_ROOT` unset (the shared raw
archive, `~/Work/corpora/spicyregs/raw`), after checking that no nightly job is
running (`docs/local-family-refresh.md`).

```sh
export UV="/opt/homebrew/bin/uv run --frozen --extra comments-reader"
export WORK=/path/to/a/fresh/scratch/dir RAW=~/Work/corpora/spicyregs/raw/objects/sha256
mkdir -p "$WORK/bulk"
```

**Disk floor.** The default floor is 100 GiB of free space after a download,
which the workstation (71 to 77 GiB free on 2026-11-10) cannot meet for this
export. `SPICY_REGS_DISK_FLOOR_GIB=10` is safe here because, once the export is
in the archive, the build needs at most about 5 GiB more: the decoder's pieces
(at most nine of 256 MiB), the staged rows (about 1 GB) and the table with its
generation copy (about 1.4 GB). The floor is checked on the raw archive's
volume, which is also the work directory's here. A download a killed run left
staged in the archive's `.pending` (bzip2 bytes untouched for six hours) is
removed at the next start.

1. **Same-edition check (D3).** Name the held 2026-06-30 export under its
   publisher filename (an APFS clone adds no copy), build that edition, check it:

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
   about an hour for this export) and one read of the 54.6 GB to hash it for
   `source_sha256`; nothing is downloaded but the live table (0.41 GB). The
   receipts read is about a third of the citations family's: estimate 10 to 15
   minutes.
2. **The refresh.**

   ```sh
   SPICY_REGS_DISK_FLOOR_GIB=10 $UV run-rollup-court-opinions --output-dir "$WORK/opinions"
   ```

   It downloads the export once, straight into the shared raw archive with its
   capture receipt, and reads it there: 45 minutes to 8.4 hours by D4's two
   rates, then about an hour of decoding and the write of about 11 million rows.
   Disk: 51.5 GiB kept in the archive for good, plus about 5 GiB of working
   files. Memory: DuckDB is held to 4 GB; the decoder's pieces and parse workers
   add to that (not measured; run under the memory watchdog). If the run must
   be repeated after the download, do not download again: clone the archive's
   object (its digest is the `capture` event of the first run's evidence
   journal) into `$WORK/bulk/opinions-2026-09-30.csv.bz2` and run again with
   `COURTLISTENER_BULK_DIR="$WORK/bulk"`; the run then retains that file as its
   original, which adds nothing to the archive.
3. **Publish** the build as it stands, with the evidence it wrote
   (`publish-local-generation`, which accepts `court-opinions`):

   ```sh
   $UV publish-local-generation --generation "$WORK/opinions/generations/<digest>" \
     --evidence "$WORK/opinions/source-evidence/<run>/artifact" --family court-opinions --dry-run --output "$WORK/opinions-dry-run"
   $UV publish-local-generation --generation "$WORK/opinions/generations/<digest>" \
     --evidence "$WORK/opinions/source-evidence/<run>/artifact" --family court-opinions \
     --expect-prior sha256:85589a33b63f22bfac522e5f2c41210eb69437b763b81380562eda1fd33ef298 --output "$WORK/opinions-receipt"
   ```

   Publication uploads the evidence, the 51.5 GiB export included (D1), to
   `source-evidence/blobs/sha256/` and reads each blob back once: about 103 GiB
   of transfer, whose time depends on the uplink (not measured).

How each step scales: the download and the decode are linear in the export's
bytes, and the writer is linear in rows, a batch of 100,000 at a time with one
DuckDB query each; only rows the SQL flags (none in the 2026 exports) go
through Python, and more than 1,000 stop the build. The repeated-identity check
is one grouped scan of the written table. Nothing reads the prior table, so a
refresh costs the same whatever was published before. The receipt route also
hashed the export a second time for a witness and wrote, admitted, uploaded and
read back 4.2 GB of receipts; none of that remains. The checks are joins on the
key, linear with spill; they run once per family, before its first
receipts-free publication.
