# unified_agenda and cfr_sections: leaving per-row receipts

`unified_agenda` (family `unified-agenda`) and `cfr_sections` (`cfr-sections`)
publish without ETL receipts (epic ss-vse3). This page records what the
receipts held, how the switch is shown to change nothing, the pins, and what
to check after the first receipts-free nights.

## Why

From the hosted logs, the receipts were most of each job:

| Job | Run | Total | Prior restored through receipts | Receipt writer |
| --- | --- | ---: | ---: | ---: |
| unified-agenda | 42892148572 (2026-11-10) | 13.5 min | 490 s | 202 s |
| cfr-sections | 42693875268 (2026-11-09) | 20 min | 172 s | 84 s |

The CFR job's remaining time is GovInfo's granule listing (13.6 minutes that
night), which receipts never touched.

## What the receipts held

Read in full from the live generations: unified-agenda `ab8eb298…`
(2026-11-10T20:20:59Z, 241,726 rows, 241,727 receipts) and cfr-sections
`897008e4…` (2026-11-09T20:52:01Z, 339,877 rows, 339,878 receipts). Every row
receipt is `accepted`; the one other is `observed`, the input file's metadata.
No receipt names a source file: each witness is a hash of the receipt's own
copy.

- `raw_conversion_inputs`: the builder's text row. Mapped again, all of them
  give exactly the published table (schema, rows both ways, order), and so does
  the published table read back into the text shape and mapped again.
- `url`, the only field not in the table: on every row of both, the builder's
  template of the row's identity
  (`https://www.reginfo.gov/public/do/eAgendaViewRule?pubId={agenda_edition}&RIN={rin}`,
  `https://www.govinfo.gov/app/details/{package_id}/{granule_id}`). It is not
  promoted; the dictionary states the template.
- The observed record: for CFR the placement marker (every row placed from its
  volume's PART headings), which lets a night skip unchanged packages; for the
  agenda the digest of the run's edition manifest. Both now live in the
  published file's key-value metadata (`regulations_shape.mapped_table`), and
  the next night reads them back with the prior (`published_as_source`).

## What changes

- Both rollups are `RegulationsTableRollup`s: the builder is unchanged and
  still merges with its prior, but the prior is the table the captured index
  pins, read back into its text shape, never restored through receipts; the
  builder's table is mapped once to the published columns in its own order,
  2,000 rows a row group as before. No receipt is written.
- The agenda run keeps each edition file it reads as source evidence and
  journals the editions' manifest (`unified-agenda-editions`). CFR's granule
  listing is still not retained (its rollup stays on the evidence exemption
  list).
- The rulemaking materializer (inside the scheduled ETL refresh) reads
  `unified_agenda` as it reads the Register: the pinned published table, its
  lists as JSON text (`rulemaking_dataset.PUBLISHED_SOURCES`). Its agenda stage
  counted observations by `(edition, url)` and now by edition, the same on all
  46,562 RINs of 2026-11-10.
- `regulations_ingest`, whose two entry points wrote these tables with
  receipts, is removed; the agenda's source-only replay runs through
  `build_table` (docs/unified-agenda-retained-rebuild.md).
- Workflows: the agenda's limit is 30 minutes (was 60); CFR keeps 60, set by
  the listing (still listing at 30 minutes in 42104322158).

## Equality with the receipt route

- `tests/test_agenda_cfr_receipt_switch.py`: rows of both live generations with
  their receipts (70 agenda rows in two editions, 93 CFR rows in three
  packages) and a night read from those receipts with labelled edits. The
  receipt route (prior restored by `materialize_internal`, table by
  `write_held_dataset`) and the plain route give the same table.
- Hosted, no upload: `rollup-unified-agenda.yml` run 43038758250 (2026-11-11)
  read edition 202510, byte for byte the edition of 2026-11-10 (its manifest
  digest `0b3413a0…` equals the live receipts' input record), and built a
  table equal to the live receipt-route table (241,726 rows, both ways, same
  order). The job's rollup step took 68 s; mapping 22 s.
- Full size, on the workstation, each live generation built twice for the same
  night (scratch `agendacfr/scripts/ac_fullsize.py`, under the watchdog, peak
  5.6 GiB). The receipt route restored the prior through the live receipts
  with full admission, then ran the builder and the held writer. The plain
  route ran `build_table` on the table alone. Both were compared with
  `receipt_switch.table_differences`:

  | Table | Night | Rows | Differences | Receipt route | Plain route |
  | --- | --- | ---: | --- | ---: | ---: |
  | unified_agenda | edition 202510 read live | 241,726 | none | 450 s restore + 85 s writer | 17.7 s |
  | cfr_sections | every listed granule, two packages restamped, one volume placed live | 339,877 | none | 144 s restore + 80 s writer | 8.1 s |

  The plain CFR table carries the placement marker.

What these cannot see:

- The agenda nights read an edition unchanged since 2026-11-10, so they change
  no row; the fixture's labelled edits cover changed and added rows.
- The CFR full-size listing is rebuilt from the receipts' rows, not GovInfo's
  live listing; the hosted rehearsal read the live one.
- Neither sees Parquet encodings beyond rows, order and row-group size.

## First nights

CFR's first night reads a table the receipt route published, which does not
carry the placement marker, so it places every package again once, as a
`replace_all` dispatch does, and logs "re-placing every listed package: the
prior table lacks the placement marker". Measured in the no-upload rehearsal
43038763537 (2026-11-11): 4.7 minutes for 339,877 granules after a 16-minute
listing; against the live table, part, section and cfr_ref were equal on every
row (108,340 rows took GovInfo's newer lastModified, six a new heading, as any
night would). That run also showed a failed volume with no prior rows
(CFR-2026-title2-vol01, failing every night) kept the output unmarked, so every
night would have re-placed everything; the marker now needs only every prior
package re-placed (`tests/test_cfr_failure_preservation.py`). From the second
night only changed packages are placed.

After each family's first receipts-free run:

- `families."unified-agenda"` and `families."cfr-sections"` in
  `publication.v2.json` list no `etlReceipts`.
- Rows: the agenda's are the previous count plus the night's new
  `(rin, agenda_edition)` keys; CFR's change only in re-listed packages.
- `SELECT count(*) - count(DISTINCT (rin, agenda_edition))` and
  `count(*) - count(DISTINCT granule_id)` read 0.
- The CFR table's file metadata holds
  `spicy_regs.cfr_sections.placement` (`pyarrow.parquet.read_schema(url).metadata`),
  and the second night logs no full re-placement.
- The agenda run's evidence journal holds one `capture` per edition read and a
  `unified-agenda-editions` event.
- Each pinned generation's `etl_receipts.parquet` still answers a HEAD.

## Pins

`docs/pins/receipt-archive.json` pins each family's last generation with
receipts; publication refuses the first receipts-free generation until the pin
names the live one. Both families publish every evening (agenda 20:00 UTC, CFR
20:30 UTC). If either publishes before this merges, rewrite its pin:
`uv run --frozen --extra comments-reader python scripts/pin_receipt_archive.py unified-agenda`
(and `cfr-sections`), run `tests/test_receipt_archive_pins.py`, commit.
