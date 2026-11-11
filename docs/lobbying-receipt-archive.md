# Lobbying filings: leaving per-row receipts

The eight tables of the nightly `rollup-lobbying-filings.yml` publish without
ETL receipts (epic ss-vse3): `lobbying_filings`, `lobbying_activities`,
`lobbying_activity_lobbyists`, `lobbying_foreign_entities`,
`lobbying_affiliated_organizations`, `lobbying_conviction_disclosures`,
`lobbying_registrants` and `lobbying_clients`. This page records what the
receipts held and where it went, how the switch was shown to change nothing a
reader sees, and the pin and first night the switch needs.

## Why

On 2026-11-10 (run 42882128420, keyed) the job ran 28.8 minutes, of which the
LDA read took 23 seconds. About 20 minutes went to receipts: the 2.94 GB
receipt member downloaded, each prior table admitted against its receipts
(about 7 minutes), the window's receipts written and merged (5 minutes), the
generation's 16.3M receipts admitted again (7.1 minutes), and the member
uploaded again for 0.73 GB of tables.

## What the receipts held, and where it went

Each receipt held its row's builder output (`raw_record`, all text). Every
receipt of generation `233aef1f` is `accepted` (16,283,820; none refused or
observed). Read back in subject order and compared with the published row,
column by column (2026-11-11, all rows):

| Table | Rows | Fields only the receipt held | Every other field |
| --- | ---: | --- | --- |
| `lobbying_filings` | 1,990,737 | `url` (non-null on every row) | equal in value |
| `lobbying_registrants` | 14,924 | `url` (non-null on every row) | equal in value |
| `lobbying_clients` | 130,088 | `url` (non-null on every row) | equal in value |
| the other five | 14,148,071 | none | equal in value |

`url` is now the last column of those three tables, as LDA served it
(`government_source_shapes.PROMOTED_LAST`; policies `government-sources/3`
for `lobbying_filings`, `/2` for the other two; `policy_migrations.json` reads
the earlier ones). It is promoted as received rather than rebuilt: a
registrant's and a client's `url` follow their id on every row, but a filing's
does not. 1,990,727 filings point at
`https://lda.gov/filings/public/filing/<filing_uuid>/print/`, and ten filings of
2006-2007 hold an API page address instead (two pairs of them share one).

Only spelling differs in the other fields: the receipt kept the builder's
text (`dt_posted` with LDA's `-05:00` offset, money as `30000.00`, booleans as
`True`/`False`), the table the typed value. Those spellings stay in the pinned
receipts below.

## How a night runs

`build_lobbying_filings` selects each table the captured index pins
(`SelectedInputs`: a retired table is its public table), reads the posted-date
window from the filings table's newest `dt_posted`, maps the window to the
public columns (`map_subject`, Python per row over the window only) and
upserts it over each table on its key in the table's order
(`native_merge.upsert_subjects`): a fetched row replaces its key's row, every
other row is carried as published. Nothing writes or reads a receipt. The
counts per table go to the run's journal as a `lobbying-merge` event.

On the first night the live generation still lists receipts, and its
`lobbying_filings`, `lobbying_registrants` and `lobbying_clients` lack `url`.
Those three priors are restored through those receipts once, by the reader
every receipt prior used (`government_receipts.internal_prior`, which admits
the pair), and each restored row's `url` is joined to the published row at
its position, refused unless every row's key agrees
(`build_lobbying_filings._with_promoted`). From the second night the priors
have `url` and no receipt is read. A prior without `url` and no receipts
listed is refused rather than carried with NULLs.

The run refuses at its start, in seconds, while
`docs/pins/receipt-archive.json` does not pin the live generation that still
lists the family's receipts (`GovernmentTableRollup`,
`publication.require_live_receipt_pins`).

## Equality with the receipt route

EQUALITY

## Pin and first night

`docs/pins/receipt-archive.json` pins generation
`233aef1f22cc036a22760e43fe48488ac32db9628dbe89e51d36db7f913cbfaf`
(2026-11-10T19:36:38Z, 16,283,820 receipts, 2,942,145,578 bytes). The nightly
publishes at about 19:35Z: if it publishes before this change merges, re-pin
it, `uv run --frozen --extra comments-reader python scripts/pin_receipt_archive.py lobbying-filings`.

After the first night (the 19:00Z run):

- `families."lobbying-filings"` lists no `etlReceipts`; each table's rows are
  the previous ones plus the journal's `new_rows` (`lobbying-merge` event).
- `lobbying_filings`, `lobbying_registrants` and `lobbying_clients` end with
  `url`, NULL on no row; the filings table's ten API page addresses are still
  there.
- The log has `['url'] filled from the last receipts` once for each of those
  three tables; the next night has none, and its run downloads no
  `etl_receipts.parquet`.
- The run finishes well inside its 45 minutes (about 5 to 9 measured below).
- The pinned generation's `etl_receipts.parquet` still answers a HEAD.

Once those hold, delete the one-night fill: `build_lobbying_filings._with_promoted`
and its test. The pin stays.
