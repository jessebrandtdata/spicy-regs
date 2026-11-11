# comment_attributes: leaving per-row receipts

`comment_attributes` (family `comment-attributes`, 26.5 million rows) publishes
without ETL receipts (epic ss-vse3), as `comments` and `comments_index` do
([comments](comments-receipt-archive.md)). This page records why, what the
receipts held, how each sweep batch's upsert is shown to be unchanged, and the
pin and merge window that the first receipts-free publication needs.

## Why

Every comment batch of the Regulations.gov sweep upserts its rows over the whole
table. From the sweep's logs of 2026-11-10, a batch spent about 46 minutes on
whole-table work whether it merged 1 row or 60,028:

| Phase | Minutes |
| --- | ---: |
| download table and receipts | 1 to 1.6 |
| select and validate the prior's 26.5M receipts | 15.6 to 16.7 |
| merge subjects and receipts (`native_merge`) | 4.3 to 5.6 |
| build and admit the generation | 12.2 to 13.1 |
| publish (0.44 GB table, 4.2 GiB receipts) | about 7.6 |

About 33 to 38 of those minutes were receipts. The refresh's
`run-rollup-comment-attributes` then rewrote every receipt again in Python
(estimated 121 minutes; cancelled at 180 on 2026-11-10).

## What the receipts held

The live generation of 2026-11-10 20:34Z: 26,499,409 rows (440 MB) and
26,499,410 receipts (4.17 GB), every one `accepted` except one `observed`
(the input file's metadata). A receipt's processing holds two things:
`input_metadata`, empty, and `raw_conversion_inputs`, the row's own
contract-shape copy (each field, with the two lists as their JSON text). None
holds a source record or a pointer to one.

The review of this change read a later generation's whole receipts member
(26,500,505 receipts): one processor and policy, `input_metadata`
empty in every receipt, no `raw_source_record`, and an order-independent typed
hash of every row's contract copy equal to the same hash of the published table
(26,500,504 rows). Every copy is implied by the published row, so no column is
added.

## What changes

- **A sweep batch** (`regulatory_publication.finish_attribute_dataset`, retired
  branch): the day's rows are converted by the same writer as before
  (`write_held_dataset`; its receipts for them are only the refusal check and
  are never kept), upserted over the published table by
  `native_merge.upsert_subjects` (the prior rows the day did not touch, as they
  are, plus the day's rows, ordered by `comment_id`, as `native_merge` orders
  them), and published as a generation with no receipts. Nothing reads the
  prior through receipts.
- **The refresh** (`run-rollup-comment-attributes`): the ETL's generation is
  the family's own, so it publishes nothing (`Unchanged`), before selecting
  anything.
- `docket_attributes` and `document_attributes` keep their receipts and their
  route.

Measured on the workstation over the live table, with 61,028 fresh rows (60,028
existing rows, half of them changed, and 1,000 new): conversion 2.5 s, upsert
33.1 s, generation 3.9 s; 2.6 GB peak memory. Add the table's download and
upload (0.44 GB each way): an estimated 3 to 6 minutes a batch hosted, against
about 46, so about 40 minutes saved a batch and, at three comment batches a
run, about two hours a run, plus the refresh job's. The first batch after the
merge reads the table alone, though the live generation still lists the
receipts (`SelectedInputs.select`).

## Equality with the receipt route

- `tests/test_comment_attributes_receipt_switch.py`: real rows (each a published
  row's contract copy, from receipt row groups across the file: 2,506 prior
  rows, six with file formats; 600 fresh rows: 300 changed, 100 unchanged, 200
  new) built by main 45e278f0's receipt route are committed; the plain route
  gives the same table (schema, rows both ways, order). A batch writes no
  receipt anywhere, the next batch of a run reads the previous one's table, and
  a refused row refuses the batch and leaves the table.
- Full size, on the live table and the batch above: the plain route's 26,500,409
  rows equal, both ways, the receipt route's rule stated independently in
  DuckDB (prior rows not replaced, verbatim, plus the fresh rows), in strictly
  increasing `comment_id` order, in row groups of 50,000 as before.

What these cannot see: a full-size run of the receipt route itself (it needs
the 4.2 GiB receipts and about 35 minutes); the fixture covers the
implementation's equality, the full-size check the rule's. Neither sees
Parquet encodings beyond rows and order.

## Pin and merge window

Publication refuses the first receipts-free generation until
`docs/pins/receipt-archive.json` pins the family's live generation, the last
with receipts. This family republishes with every comment batch of the sweep
(`etl-new-pipeline.yml`, 06:25 and 18:25 UTC, four to six hours each) and on
manual sweeps, so the pin is written right before the merge:

1. Wait until no sweep is running and its refresh has finished (the refresh is
   part of `etl-new-pipeline.yml`'s run):
   `gh run list -R mikewolfd/spicy-regs --workflow etl-new-pipeline.yml --status in_progress`
   must list nothing, nor any other run that holds the `comments-catalog-write`
   group (`publish-comments-mirror.yml`, `fill-comment-fields.yml` and the
   other comment workflows), nor `convert-native-family.yml`, which can also
   publish this family.
2. On the branch: `uv run --frozen --extra comments-reader python
   scripts/pin_receipt_archive.py comment-attributes`. It writes the entry from
   the live index and commits the generation's root as
   `tests/fixtures/receipt_archive/<digest>.json.gz` (and removes the root it
   replaces); it refuses once the family publishes without receipts. Run
   `tests/test_receipt_archive_pins.py`, commit, push.
3. Merge before the next scheduled sweep, after checking that
   `families."comment-attributes".artifactDigest` in
   `https://data.spicygov.ai/publication.v2.json` is still the pinned one. If
   anything published it in between, repeat step 2.

If the pin is stale anyway, every batch refuses at its start, in seconds,
naming `scripts/pin_receipt_archive.py` (`publication.require_live_receipt_pins`),
and nothing is published: re-pin from the live generation (still the last with
receipts) and run again.

## After the first receipts-free batch

- `families."comment-attributes"` lists no `etlReceipts`, and its table rows are
  the previous count plus the batch's added rows (the run logs `comment_attributes:
  N rows kept, N replaced, N added`).
- `comment_id` is unique and ascending:
  `SELECT count(*) - count(DISTINCT comment_id) FROM read_parquet('<table url>')`
  reads only that column (about 61 MB).
- The pinned generation's `etl_receipts.parquet` still answers a HEAD.

## Not done here

- A per-row source pointer: neither the receipts nor the sweep's staged rows
  name a source digest today (the comments switch is the same).
- The refresh's 180-minute limit and `free_disk` for this job: it now takes the
  setup's few minutes; `tests/test_sweep_budget.py` pins the expression, which
  the sweep's own pull requests also change.
- A full rebuild of this table from source: native conversion refuses a
  retired table, so only sweep upserts write it.
- The comments catalog's receipt table, through which the sweep's catalog merge
  still restores rows (6 to 11 minutes a batch): the regulations path's step.
