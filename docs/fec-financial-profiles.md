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
Missing entity identity, a row or header that does not fit the layout, or
original bytes that differ from their captured digest refuse the entire build.

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

## Build from a captured pair

```sh
python -m spicy_regs.pipelines.fec_financial_profiles build --captures "$WORK/fec-summaries.json" \
  --cycle 2026 --read-index "$LIVE_INDEX" --output-dir "$WORK/profiles-build"
```

`build-fec-financial-profiles` is the same command. The build reads the cycle's
two captures from the raw archive the capture used and cites both originals in
its evidence by digest and URL. Each CSV is split into rows by the SpicyDocs
positional reader and named by its own header exactly as fec-observations does,
then mapped by the fec-query summary mapper (`fec_summaries.map_summary`). A
witness therefore keeps the byte locator and `record_id` a rebuilt fec-query
would give the same bytes; its `selection_evidence_sha256` is the digest of this
build's retained selection. Rebuilding the 2026-09-12 pair this way reproduced
all 18,521 rows of the generation selected from fec-query, every column equal
but `source_table_generation` and that digest.

The mapped rows are staged in one temporary Parquet file beside the output and
read back in one ordered pass. DuckDB uses one thread, a 512 MB execution-memory
limit and no disk spill; this is not a whole-process RSS limit (about 650 MB for
the 2026 pair). Python retains one entity's witnesses. A row whose field count
differs from its header, a header missing a layout field, or a profile count
that differs from the mapped rows refuses the build before the output replaces
its destination. The generation records no parent: `source_table_generation` is
null, and `scope_id` names the cycle and capture day.

The published profiles are as current as their capture, not as the fec-query
tables. Those keep their September captures until fec-query is rebuilt, so the
same committee can show newer totals here than in
`fec_reported_financial_summaries` or its itemized rows; compare `period_end`
and `captured_at` before setting them side by side. The last generation selected
from fec-query is
`847f51c252c26208d2d80b672314ce26c5930ef9cd1c05887e5ec4b0ecca55fb` (2026-09-12
captures), kept by this page.

Local fixture acceptance does not establish actual source reconciliation, public
publication or SpicyGov delivery. The capture timestamp states the snapshot's
age, not a claim that a current FEC web page still has the same values. How to
publish a build and the hosted daily run are in
[local-family-refresh.md](local-family-refresh.md).
