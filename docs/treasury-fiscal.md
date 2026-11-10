# Treasury fiscal comparisons

`treasury_debt_observations` holds daily debt balances.
`treasury_statement_periods` holds Monthly Treasury Statement (MTS) Table 1
monthly and fiscal-year-to-date receipts, outlays and deficit.
`treasury_statement_classification_cells` holds MTS Table 2, budget and
off-budget results and financing: one row per issue, native source line and
measure. Debt is a stock; the budget amounts are period flows. A change in debt
is not equated to borrowing from the public or the budget deficit.

The first published scope is United States federal debt on October 16, 2026 and
the September 30, 2026 MTS issue. Table 2 is typed since 2026-10-31 (the owner
reversed the Table 1-only scope on 2026-10-30). The issues 2015-03-31 (the
oldest the API serves), 2025-09-30, 2025-10-31 and 2026-03-31 were captured on
2026-10-31 with their printed reports, and the owner approved recapturing the
2026-09-30 Table 1 and Table 2 responses the same day so that issue carries
observed capture times (both byte-identical to the pilot's retained responses).
A local generation over all five issues passes every check; 2025-09-30 ships a
publisher inconsistency under a digest-pinned exception (see below). Nothing
is published by a local build. Other MTS tables, inflation adjustment and
historical backfills are outside this delivery.

## Amounts, dates and revisions

Table 1 and debt amounts use exact `DECIMAL(38,2)` US dollars. Literal `"null"`,
JSON null and empty strings remain distinct statuses; none becomes zero. Original rows, metadata,
request selection and row locators remain alongside the typed amounts.

The [September 2026 report](https://fiscaldata.treasury.gov/static-data/published-reports/mts/MonthlyTreasuryStatement_202609.pdf)
was retained with SHA-256
`fcabeccdc7052960ffe8ec2c8a539aed9c1f49d47aedc2cbee39d50f0585e7db`.
Its page 5 budget actuals reconcile to the selected API Tables 1 and 2; Table 1
uses outlays minus receipts, while Table 2's surplus/deficit field reverses that
sign. Table 1's raw `gross` field names remain intact. The statement's
modified-cash methodology subtracts specified refunds and reimbursements; this
mapper does not invent a separate gross series.

## Table 2 cells, units and markers

Fiscal Data declares `dataFormats` `"$10.20"` and `dataTypes` `CURRENCY` for all
five Table 2 amount fields. The two budget-estimate fields nevertheless hold
$ millions: the printed page says `[$ millions]`, and in the 2026-09-30 issue the
fiscal-year-to-date dollars divided by the matching estimate is about 986,781.93.
Units therefore come from the reviewed file
`src/spicy_regs/transforms/treasury_units.json`, never from the API metadata.
Any `CURRENCY` or `*_amt` field of any Treasury dataset without an entry there
refuses the build. `amount` stays in `amount_unit` (`usd` or `usd_millions`);
nothing is rescaled. `amount_usd` beside it multiplies by the unit's scale
(`unit_scales` in the same file: 1 or 1,000,000). The publisher states
estimates to $0.01 million, so an estimate's dollar figure is exact arithmetic
but precise only to $10,000; its last four digits are scale, not information.

`marker_kind` is `value`, `zero`, `no_transactions`, `publisher_null`,
`json_null` or `blank`. The API returns `0.00` for estimate cells the printed
statement shows as `......` ("No Transactions"). Whether a given `0.00` is that
marker or a projected zero is decided per issue from the printed page, recorded
in the same file under the Table 2 response digest; such a cell keeps
`amount_literal` `0.00` and has a null `amount`. An unreviewed `0.00` estimate
refuses, and so does a review entry naming a cell that is not `0.00`. On every
reviewed page each `0.00` estimate prints as dots, and no page carries a
suppression legend; the 2015 issue leaves one estimate blank, which the API
returns as the text `null`. A suppressed or otherwise non-decimal literal
refuses rather than typing silently.

Each build checks, per measure, that on- plus off-budget equals the total for
receipts, outlays and surplus; receipts minus outlays equals the surplus;
financing equals minus the surplus; and the three means of financing sum to
financing. An identity is evaluated only when every cell in it has a number, so
estimates printed as dots are skipped: of 40 identity checks per issue the
local build skipped 1 for 2015-03-31, 2 for 2025-09-30, 16 for 2025-10-31 and
12 each for 2026-03-31 and 2026-09-30. Table 2 actuals must also equal Table 1: the month,
the current fiscal year to date, and the prior fiscal year's months through the
same month. Same-issue revisions stay separate releases, so each Table 2 release
must reconcile with at least one Table 1 release of its issue.

The 2025-09-30 issue prints prior-year means of financing of 1,959,809, -228,836
and 85,831 ($ millions), which sum to 1,816,804, against financing of 1,816,790.
The API carries the same figures (a gap of exactly $14,258,000.00). The owner
kept the identity as a refusal and allowed one exception, pinned in
`identity_exceptions` of `treasury_units.json` to that Table 2 response digest,
that identity, measure and exact difference, citing the page. The build
publishes the publisher's values unchanged, so the gap stays visible in the
cells, and journals each excepted identity in `treasury-table2-checks`. A
pinned exception whose identity holds or differs by another amount refuses.

`classification_id` is reassigned for every issue (no id is shared between any
two captured issues), while the native line structure is identical; join issues
on `src_line_nbr`, never on `classification_id`.

The source `record_date` and `record_fiscal_year` describe the report issue even
on prior-year rows. Economic years come from explicit `FY YYYY` parents; month
names determine calendar periods. Sequence codes such as `1.10` remain text.
The supported layout requires complete prior fiscal years and current-year
October-through-report-month rows, each with its own fiscal-year-to-date total.
Unsupported or missing periods refuse. Real-source qualification currently
covers the five retained issues (2015-03-31 through 2026-09-30), not every
historical MTS layout.

The report's revision note describes an August outlay correction. The earlier
issue was not acquired, so no exact historical revision delta is claimed.
Different issues and same-issue refreshes remain separate observations keyed by
source release digest as well as report/economic period. A build includes all
explicitly supplied source releases; it neither deduplicates by economic month
nor automatically discovers prior releases.

## Acquisition, rebuild and query

SpicyDocs owns the Fiscal Data reader, page validation and source-release
format. This application drives that reader through the maintained
`CaptureEvidence` transport, so every API page and report PDF lands once in the
central raw archive with a receipt carrying its request URL, SHA-256,
observation time and size:

```sh
python -m spicy_regs.pipelines.treasury_fiscal acquire \
  --select mts_table_1:2026-03-31 --select mts_table_2:2026-03-31 \
  --select debt_to_penny:2026-03-31 --report 2026-03-31 \
  --output-dir "$RUN_DIR" --captures-out captures.json
```

The report URL is read from the publisher's own listing
(`services/dtg/publishedfiles`, report group 5), never constructed. The listing
is captured into the raw archive like the pages and PDFs, and the digest of
those retained wire bytes (gzip-encoded as served) is journaled beside each
report URL it stated. The command
writes a capture selection: each source release as its Fiscal Data scope (the
selection and the descriptors of every page, copied from the receipts) plus
each report's URL and digest.

`--debt-after 2026-10-16` also captures every Debt to the Penny date the
publisher lists after that date (the listing is captured too); more than 100
listed dates are refused rather than cut short.

A build needs the captures of everything it publishes, not only the new ones.
`published-captures` writes that selection from a local copy of the published
generation: every row names the page it came from and every statement row its
report control. It needs all three tables, so it serves generations published
since `treasury_statement_classification_cells` joined the family (ss-lq21).

```sh
python -m spicy_regs.pipelines.treasury_fiscal published-captures \
  --generation "$PUBLISHED_GENERATION" --captures-out published.json
python -m spicy_regs.pipelines.treasury_fiscal build \
  --captures captures.json [--captures published.json] --output-dir "$TREASURY_OUTPUT"
```

The build publishes one source release per scope into
`$TREASURY_OUTPUT/source-releases` from the raw originals (verified, never
written), admits it through the SpicyDocs reader and builds the generation.
Release members are therefore disposable: the capture selection plus the raw
archive rebuild the same release digests with the same code. Keep the capture
selection with the generation it fed. Debt and report dates come from the
selection; a report issue needs Table 1, Table 2 and a report control. The 2026-10-20
debt descriptor predates receipt observation times; it was copied from the
published generation's `source_locator_json`. The 2026-09-30 Table 1 and Table 2
descriptors now come from the 2026-10-31 recaptures.

The report is an explicitly reviewed qualification input: the build checks its
bytes and retains it, but does not rerun PDF interpretation.

The existing `CaptureEvidence` owner retains each source page once, records
every source release pin and journals the Table 2 check counts as
`treasury-table2-checks`. `build_generation` produces all three ordinary
tables; `sources.publication.publish_generation` accepts that generation with
the emitted source-evidence directory. Building locally does not publish it.

Point the existing local SQL consumer's `SPICY_REGS_DATA_DIR` at `generation/`.
This trend query retains the report issue and revision identity:

```sql
SELECT report_date, economic_fiscal_year, period_start,
       budget_receipts_usd, budget_outlays_usd, budget_deficit_usd,
       json_extract_string(source_locator_json, '$.request_url') AS source_url,
       qualification_report_url, source_release_digest
FROM treasury_statement_periods
WHERE period_kind = 'month' AND economic_fiscal_year = 2026
ORDER BY period_start, report_date, source_release_digest;
```

Table 2 cells join Table 1 periods on the report issue:

```sql
SELECT c.report_date, c.classification_desc, c.measure, c.measure_fiscal_year,
       c.amount, c.amount_unit, c.amount_usd, c.marker_kind, p.budget_receipts_usd
FROM treasury_statement_classification_cells c
JOIN treasury_statement_periods p
  ON p.report_date = c.report_date AND p.period_kind = 'month'
 AND p.period_start = date_trunc('month', c.report_date)
WHERE c.src_line_nbr = 2
ORDER BY c.report_date, c.measure, c.source_release_digest;
```

Query `treasury_debt_observations` separately for the selected daily balance.
Never sum monthly and fiscal-year-to-date rows together.

Source routes, field mappings and methodology reuse the
[official Fiscal Data repository](https://github.com/fedspendingtransparency/fiscal-data/tree/43be46fa6479de64f8a1a577cd761679a2f5cf7e)
at the reviewed revision. Its MIT notice is retained with the source adapter.
No website runtime, second HTTP client, new query engine or history cache is
introduced.
