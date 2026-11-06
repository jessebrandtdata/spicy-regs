# Congressional disbursement data

SpicyGov retains the selected originals in its central raw archive and exposes
their shared file inventory and queryable data. SpicyDocs owns discovery,
acquisition, reading and source-qualified correction rules. The SpicyRegs
pipelines run those readers and build the table generations used by SpicyGov.
The CSV and PDF paths share the acquisition host and generation lifecycle.

The House CSV reader serves `house_disbursements` and
`house_disbursements_summary`. Historical native PDF readings serve
`house_disbursement_pdf_rows` and `house_disbursement_pdf_pages`. Senate native
readings serve `senate_disbursements` and `senate_disbursement_pages`. See the
[House PDF table dictionary](tables/house_disbursement_pdf_rows.md) and the related table pages for fields, grain and coverage limits.

Financial, mailing and annual allowance summaries use the same page host and generation builder.
Their separate `house_financial_summary_*`, `house_mailing_summary_*`,
`house_allowance_summary_*`, `house_allowance_formula_*` and `senate_financial_summary_*` tables preserve source rows, individual numeric
values and complete page readings. Summary amounts remain separate from ledger
payments.

## Select and acquire originals

SpicyGov owns the retained originals and their shared source-file inventory;
SpicyDocs owns the downloaders, readers and normalization rules. SpicyRegs builds
the queryable table generations. Congressional Tech can reference the same files
without requiring a committee or meeting relationship. Keep the authoritative
bytes in the central raw archive described in the
[shared catalog guide](shared-source-catalog.md), including PDFs whose OCR is
pending. A selected PDF list or upload manifest is run evidence, not another
catalog. Remote mirroring requires an explicit destination and its own verification.

Use SpicyDocs' House and Senate index readers to retain actual advertised links
and their reporting context. Select a complete original or a qualified set of
parts; overlapping full reports and parts can repeat the same financial rows.
Source labels and report covers establish reporting periods. A filename alone
does not establish the period or document role.

`pipelines.congressional_disbursements acquire` consumes a
`congressional-disbursement-selection/1` manifest. Each selected file declares
`id`, `chamber`, `format`, `kind`, `url`, `period_start` and `period_end`.
Acquisition adds the original byte hash, size and receipt reference. Reuse a
matching retained original through `--reuse`; the host verifies its bytes and
preserves its original acquisition evidence. Refused and unavailable sources
remain distinct from successful empty results.

Acquisition also adds an immutable source-inventory receipt through the existing
central archive writer. It names the House or Senate dataset, literal source URL,
retained byte version and declared report period. Full reports and individual
parts remain distinct. This metadata lets the shared source catalog list an
original before any PDF reading or OCR succeeds. Original acquisition receipts
and bytes stay unchanged; an inventory receipt does not invent a capture time.
After acquisitions, use the [shared catalog refresh](shared-source-catalog.md#refresh-additions-without-replaying-the-base-inventory)
to import the new receipts into a candidate. Remote mirroring and public catalog
selection remain separate operations.

Run checkout commands through the locked Python environment. The default
`source-readers` dependency group installs the qualified SpicyDocs wheel from
`vendor/`; no sibling checkout is required. For reader development, an explicit
`--with-editable ../spicy-docs` overlay selects local code and requires fresh
qualification for any changed extraction behavior.

## Normalize CSVs and read PDFs

```sh
uv run --frozen python -m spicy_regs.pipelines.congressional_disbursements normalize \
  --acquisition acquired/manifest.json --output normalized
uv run --frozen python -m spicy_regs.pipelines.house_disbursements \
  --normalization normalized/manifest.json --output house-tables

uv run --frozen python -m spicy_regs.pipelines.congressional_disbursement_pages \
  --selection selected-pdfs.json --chamber house --output house-pages
uv run --frozen python -m spicy_regs.pipelines.congressional_disbursement_tables \
  --reading house-pages/manifest.json --output house-pdf-tables
```

Use `--chamber senate` for the Senate page reader. The shared PDF table builder
reads the chamber and profile from its pinned manifest. It checks original membership,
every selected page, original-word coverage where supplied, monetary row positions and
population totals before building tables. Source implementation snapshots are
retained with the reading so later checkout edits do not erase replay evidence.
`--reuse-manifest` on the reading and normalization hosts accepts only verified,
compatible prior outcomes. Use a fresh directory for a new table generation.

## Read financial, mailing and allowance summaries

Use `--profile financial-summary` for either chamber or
`--profile mailing-summary` for House mailing and communication tables.
Use `--profile allowance-summary` for House annual allowance tables and their
formula introductions. The default profile is `ledger`.

Use `--profile allowance-formula` for explicit formula parameters, mileage bands,
relationships, adjustments and external-input references. The same retained-page
and typed-generation stages emit separate `house_allowance_formula_*` tables.
Each numeric field keeps its own unit and exact scale. Unstated bounds and
unsupplied external inputs produce no synthetic numeric rows. Native span
witnesses, printed operations, source-bound repairs and amended scope remain in
the source JSON. Formula target years stay separate from funding and adjustment
basis years; an adjacent-heading inference requires that page to be selected and
retained as a witness. No annual allowance is recomputed, and unit-incomplete
travel expressions remain explicit.

```sh
uv run --frozen python -m spicy_regs.pipelines.congressional_disbursement_pages \
  --selection reviewed-summary-pages.json --chamber house \
  --profile financial-summary --output house-financial-pages
uv run --frozen python -m spicy_regs.pipelines.congressional_disbursement_tables \
  --reading house-financial-pages/manifest.json --output house-financial-tables
```

A selected original can declare `reading_pages`, an ascending list of physical
page numbers, and a `reading_scope` explanation with its review evidence.
Without `reading_pages`, the reader visits every physical page. Completion of a
bounded selection covers only those declared pages; the original physical page
count remains in the manifest. Include preceding pages when a table's title or
account continues across pages.

Source byte hash, physical page and `summary_row_index` identify each summary
row. Repeated title/year pairs remain separate occurrences. Numeric cells add
`value_name` to that identity and use exact `DECIMAL(38,4)` values with their
original scale and unit. A blank cell, a literal zero and an unparsed value have
different `value_status` values. `source_row_json` and `page_reading_json` retain
the full source reader observations.

The report period and a table's own period are separate. Mailing tables may
describe an amended earlier quarter or a legislative period that starts on
January 3. Annual allowance rows additionally retain `allowance_year`,
`printed_title_year` and `printed_column_year`. An exact-source governed-year
repair preserves both printed years and its approval evidence. State and district
fields keep source punctuation, leading zeros and caucus codes.

Include each allowance section's formula introduction and contiguous table
pages to witness amendment scope and headerless continuations. Formula pages
retain their source text with `allowance_formula_unparsed`; the reader does not
convert their mixed-unit calculations into annual allowance values. Current and
amended sections remain separate even when amounts match. Recognized empty
mailing tables also retain their own period and context.

Historical postage-only tables, allowance calculations and image-only content
require their own qualified reading; a modern mailing-table reading does
not establish their completeness. Repeated front statements in historical report
parts also need an explicit selection before aggregation.

Selected image pages use the shared
`pipelines.congressional_disbursement_ocr.read_ocr_pages` host. Its explicit
`house-2008` and `senate-ledger` profiles choose source-owned SpicyDocs readers;
both reuse the same bounded retention, checkpoint, failure and resume path.
Callers supply the reviewed orientation, exact original selection and backend
configurations. Retained render and response pins permit replay without a new
model call. The host has no implicit whole-corpus or live-model default.

OCR associations remain separate from the native tables until independently
qualified. An observed ledger does not qualify a nearby summary or establish an
account for a following page. Missing source bodies, unselected pages, reviewed
blank pages and failed observations retain distinct outcomes.

Separately reviewed Senate image headings can establish context for exact native
continuation pages. Their `context_basis` is `reviewed_image_section` and
`context_origin_json` retains the original image, review and bounded correction.
Native word positions remain empty for that image origin. The native ledger on
the image page stays unread, and later account headings keep their own context.

## Check the result

Original source review and ordinary download/query tests establish different
facts. A raw reviewer checks the original monetary columns, dates, signs,
blank values, repeated occurrences and reporting hierarchy. The integration
tests exercise acquisition, source reading, exact decimal conversion, managed
download and actual MCP database queries. Mutation cases check that changed
bytes, populations, policy and implementation copies refuse admission.

Each PDF page keeps an explicit outcome. Native ledger success does not imply
that an image, appropriation grid, mailing table or other financial summary has
been normalized. The companion page tables expose those gaps. CSV occurrences
and PDF occurrences are separate; do not combine overlapping periods without
an explicit source selection. Printed subtotals and summaries must not be added
to transaction amounts. A date after the reporting period is a review candidate,
not sufficient evidence of an error.

The builder creates a local candidate and retained evidence. Public publication
and reader activation use the maintained release workflow separately.
