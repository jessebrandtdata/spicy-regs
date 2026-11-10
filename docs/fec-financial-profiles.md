# Reported financial profiles

`fec_financial_profiles` presents one candidate or committee in one explicitly
selected election-cycle snapshot. Receipts and disbursements retain the source's
cumulative coverage; cash and debt retain its period-end balances. These values
are not additive across periods, between candidates and their committees, or with
itemized transactions.

The first supported inputs are the candidate-summary and committee-summary CSVs,
one exact original digest for each. The
[candidate summary definitions](https://www.fec.gov/campaign-finance-data/candidate-summary-file-description/)
describe candidate-wide reporting, while the
[committee summary definitions](https://www.fec.gov/campaign-finance-data/committee-summary-file-description/)
describe committee cycle-to-date summaries. The separate
[committee report summaries](https://www.fec.gov/campaign-finance-data/committee-report-summary-file-metadata/)
are report-by-report data and are not substituted here. Weball, Webl and Webk
layouts remain separate observations.

The existing `reported_summary_measure` rule decides whether each original
field is an exact eligible dollar amount. The profile preserves that result as
decimal **text**, including zero and negative amounts, so a browser cannot round
large values through JavaScript numbers. Each friendly label retains its native
field, raw spelling and status. The observation-level
`current_financial_total_qualified` limitation stays unchanged.

Repeated committee rows for linked candidates are equivalent only when all
reported monetary measures, coverage dates, entity name and relevant source
interpretation facts agree. They produce one profile with every complete witness
retained. Conflicting amounts or overlapping but different periods produce an
ambiguous profile, with no chosen amount or date. Capture timestamps never break
ties. A changed or amended source snapshot requires an explicit new selection;
this output does not infer amendment chains or combine captures.

Missing, invalid, reversed, outside-cycle or after-capture coverage dates remain
visible independently of amount eligibility. An exact amount can therefore have
unqualified coverage. No row is dropped for an unresolved date or conflict.
Missing entity identity, unsupported source interpretation, changed bytes or
incomplete declared membership refuses the entire build.

## Offline build

Use the normal installed command:

```sh
build-fec-financial-profiles --selection selection.json --output-dir ./profiles
```

The equivalent module command is `python -m spicy_regs.pipelines.fec_financial_profiles build`.
The selection supplies:

- `scope_id` and an even `cycle`.
- `publication`: retained index `path` and exact `sha256`.
- `table`: local summary `path` and the exact managed table `pin` returned by
  `publication.table_pin`.
- `collections`: exactly one `candidate` and one `committee`, each with
  `entity_kind`, `collection_id`, `source_sha256`, `expected_rows`, `captured_at`
  and `source_url`. Capture facts must come from the retained source acquisition
  controls; the build does not refresh or independently acquire them.

The builder verifies the input file's size, digest, complete typed schema and
declared footer count, then reads only the two selected collections in ordered
Arrow batches. It checks every selected row's source identity and current value
mapping. DuckDB uses one thread, a 512 MB execution-memory limit and no disk
spill; this is not a whole-process RSS limit. Python retains one entity's
witnesses plus the selected witness IDs, bounded by the declared population.
Membership and unchanged-file checks finish before the atomic output replaces
its destination.

The normal managed generation records the exact parent table, selection controls
and population reconciliation. The publisher refuses a changed selected parent.
The fresh table is adopted into the generation without a second retained copy.
No full original CSV or other derived FEC table is copied into this output.

Local fixture acceptance does not establish actual source reconciliation, public
publication or SpicyGov delivery. Each must be recorded separately for the exact
selected generation. The capture timestamp states the snapshot's age, not a
claim that a current FEC web page still has the same values.

## Capture the summary CSVs

FEC rewrites `candidate_summary_<cycle>.csv` and `committee_summary_<cycle>.csv`
every day under the same URL, so a day that is not captured cannot be fetched
again. `acquire` saves both files of each named cycle into the shared raw archive,
each with its acquisition receipt, before anything reads them, and writes the
capture record (URL, SHA-256, size and observation time of each):

```sh
python -m spicy_regs.pipelines.fec_financial_profiles acquire --cycle 2026 \
  --output-dir "$WORK/fec-summaries" --captures-out "$WORK/fec-summaries.json"
```

`.github/workflows/capture-fec-summaries.yml` runs the same capture on a hosted
runner. Its raw archive is scratch, so `--publish-evidence` seals the run's
evidence and sends it, both originals and an index row per file
(`source-evidence/indexes/fec-summaries/`) to R2, which retention never deletes.

A capture does not change the profiles. They are a selection over the published
fec-query summary table (`source_table_generation`, the generation's recorded
parent), so newer summaries reach them only through a rebuilt fec-query.
