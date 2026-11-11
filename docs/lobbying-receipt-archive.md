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
does not: 1,990,727 filings point at their own
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

Both routes ran on the same frozen inputs (2026-11-11, workstation, nothing
fetched but the window): generation `233aef1f` as the prior, and one real
nightly window read from lda.gov once and frozen (916 filings posted
2026-11-02 to 2026-11-10; 816 of them already held, 100 new). The receipt route is
main at `000770ea`, the new one this change:

| Table | Rows after the night | Every existing column, both ways (EXCEPT ALL) | Row order | `url` against the receipt route's receipts |
| --- | ---: | --- | --- | --- |
| `lobbying_filings` | 1,990,837 | 0 and 0 | identical | equal on every row, none NULL |
| `lobbying_activities` | 3,676,643 | 0 and 0 | identical | – |
| `lobbying_activity_lobbyists` | 10,423,866 | 0 and 0 | identical | – |
| `lobbying_foreign_entities` | 16,188 | 0 and 0 | identical | – |
| `lobbying_affiliated_organizations` | 30,082 | 0 and 0 | identical | – |
| `lobbying_conviction_disclosures` | 1,606 | 0 and 0 | identical | – |
| `lobbying_registrants` | 14,924 | 0 and 0 | identical | equal on every row, none NULL |
| `lobbying_clients` | 130,096 | 0 and 0 | identical | equal on every row, none NULL |

Column names, order and types are equal; `url` is the only addition, last.
The `url` comparison reads the receipt route's own new receipts (its
`raw_record`, decoded in DuckDB), so the carried rows' values come through a
different reader from the one that filled them. The builders took 453.8 s
(receipt route, 7.0 GiB peak) and 207.4 s (new route, first night, 5.2 GiB
peak, of which about 154 s fill `url`); the comparison 326 s at 3.7 GiB.

The second night's path ran next: an index listing no receipts and the new
route's first-night tables as the prior, the same window again. It read no
receipt (the run had none to fetch and would have failed asking), filled
nothing, built in 69.1 s at a 1.57 GiB peak, and all eight tables equal the
first night's on every column, `url` included: 0 and 0 both ways, same order.

On GitHub's runner the first night, rehearsed with upload skipped
(run 43037935543, 2026-11-11 14:23Z, keyed), built in 7.5 minutes: 23 s
reading 858 filings, 5.3 minutes filling `url` from the receipts (2.94 GB
downloaded and read), the upserts in seconds, and the generation admitted in
5.4 s (428.7 s with receipts on 2026-11-10). `tests/test_lobbying_receipt_switch.py`
holds both nights on real rows cut from these inputs.

What these checks cannot see: Parquet layout (row groups, encodings), a
night with a repeated or reordered page (the frozen window was read once),
and text spelling, which the receipts held and the tables never did.

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
- The run finishes well inside its 45 minutes: the rehearsal above built in
  7.5, and from the second night the 5.3 minutes of filling are gone.
- The pinned generation's `etl_receipts.parquet` still answers a HEAD.

Once those hold, cut `build_lobbying_filings._with_promoted` down to its
refusal (a prior without `url` is refused) and drop the first-night test. The
pin stays.
