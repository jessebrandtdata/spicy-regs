# Court opinion clusters: bulk exports and the Search catch-up

`court_opinion_clusters` has two sources. CourtListener's quarterly bulk export
is the complete table. Between exports, authenticated Search adds the clusters
created since, above the highest id the table holds. The scheduled job does both
(below); the `catch-up` subcommand at the end is the operator's explicit,
retained route to the same append.

## The daily job

`run-rollup-court-opinion-clusters` runs daily at 04:20 UTC
(`.github/workflows/rollup-court-opinion-clusters.yml`). It reads the published
generation's own journal for the export edition the table holds and the id it
is complete through (`held_selection`), then does one of two things:

- **Rebuild.** The publisher lists a newer export, or the journal names none:
  `build_court_opinion_clusters` reads the export and merges it over the prior,
  whose search rows above the export are carried. It makes no Search request, so
  no quota refusal can discard the multi-hour export read. It journals
  `export-selection` (edition, highest id).
- **Catch up.** Otherwise `daily_catch_up` appends one day's share of the
  backlog and journals `catchup-selection` (edition, `above`, `through`,
  `declared_backlog`, `clusters`, `requests`).

### Quota

The account's limits are 5 requests a minute, 50 an hour and 125 a day, in
rolling windows ([rate limits](https://www.courtlistener.com/help/api/rest/),
[usage API](https://wiki.free.law/c/courtlistener/help/api/rest/v4/api-usage),
read 2026-11-10). The daily docket job (18:30 UTC, about 7 requests) shares
them and has priority.

- A catch-up spends at most **105** requests: 125, less the docket job's 7,
  less the docket job's 7 of the day before (still inside the rolling day when
  it runs), less 6 spare.
- Requests are **75 seconds** apart (48 an hour), so a full day takes 2.2
  hours, nearly all of it waiting. No lock is held: publication is a
  compare-and-swap that merges other families' writes.
- Before any Search request the run reads the usage API, which has its own
  throttle and spends nothing. A job that spends most of a rolling day never
  finds it idle: yesterday's requests leave the window one at a time, 24 hours
  after each was made. So when the day's next slot frees within 90 minutes the
  run waits for it and keeps step with yesterday's; when the quota was spent at
  another hour it takes only what is free now, less 7; a blocked account or an
  unreadable reply fails the run. Fewer than 2 requests is "nothing to do":
  the run succeeds and publishes nothing.
- A 429 is waited out by its `Retry-After` (up to 30 minutes a run in all).
  One that asks for longer ends the day; whole windows already read are
  published.
- Other Search readers pace at 13 seconds, under 5 a minute, with a default
  allocation of 50 attempts and 900 seconds of waits (49 pacing gaps are 637).

Requests made on the same token by hand count against the same day. More than
6 of them in the 24 hours before the docket job can leave it waiting on its own
`Retry-After`.

### What one catch-up does

1. Reads usage (free) and decides the day's requests.
2. One Search request for everything above the published highest id states the
   backlog. None: nothing to do.
3. Downloads the published table and receipts (5.6 GB) against their pins, and
   refuses if the table holds an id above the one its journal states.
4. Walks **id windows** upward. Search can only be ordered by filing date, so a
   half-read range cannot be resumed by id; a window is therefore read whole or
   not kept. A window of `W` ids holds at most `W` clusters, so one sized at 20
   ids for each page left always fits; no window is sized for more than 30
   pages. An empty window doubles the next (up to 8 in a row); a doubled window
   holding more than its pages is left after one request and halved.
5. Appends the rows to the prior without restoring it (`_append_native`,
   `combine_receipts`), and the ordinary generation admission and publication
   follow.

Progress lives in the published generation: the rows are the table's, and
`through` is its highest id, in the journal that generation pins. The next run
starts at `through + 1`, so no page is read twice and no row leaves the table.
A day that stops early has still published every whole window; a day that
publishes nothing leaves the previous generation current.

### What the table then claims

- Complete through `through` for the clusters Search indexed when each window
  was read, in every publisher status. A cluster indexed later, changed, merged
  or deleted below `through` is corrected only by the next export; the catch-up
  never updates or removes a row. Search omits unindexed RECAP clusters.
- `declared_backlog` less `clusters` is how many indexed clusters remained
  above `through` when the run began.
- Ids 10,987,421 to 10,991,197 were added on 2026-10-19 by a search that
  named no status, which the publisher reads as Published only (2,278 rows).
  Other statuses in that range arrive with the next export.

### Cost and pace (2026-11-10)

- Clusters arrive at about 1,685 a day (84 pages). A full day reads about 104
  pages, about 2,040 clusters: the backlog closes by about 350 clusters a day.
- On 2026-11-10 the backlog was 69,088 clusters above the 2026-09-30 export.
  At that pace it is still about 51,000 when the 2026-12-31 export arrives.
  The rebuild then resets it to the days since that export was cut, and the
  daily runs close that in three to four weeks and hold the table within about
  a day of the publisher until the next export.
- A catch-up restores nothing (the weekly walk it replaces first restored 10.2
  million rows for 47 minutes and 8 GB of memory). It rewrites the 3.4 GB
  table and 2.2 GB receipts once each, in 25,000-row batches. On a workstation
  on 2026-10-19 the same append and its receipts were done 6.5 minutes after
  the run began (journal timestamps); hosted time is unmeasured.

## The explicit `catch-up` command

`run-rollup-court-opinion-clusters catch-up` adds one bounded Search selection
to an explicitly qualified **same-edition** native parent, with a retained
checkpoint. Use the default command for a new bulk edition or historical
corrections.
The edition evidence must establish the selected parent's edition and the courts
original's edition; a filename or maximum cluster ID does not establish this.

This local route uses the checkout's pinned SpicyDocs provider for authenticated
replay, checkpoints and status selection. With the locked source-reader
dependencies installed, `uv run --frozen --offline python -m
spicy_regs.pipelines.rollups.court_opinion_clusters` invokes the same app as the
console script. Use an owned environment for local work.

```sh
run-rollup-court-opinion-clusters catch-up \
  --output-dir /owned/run \
  --prior-directory /retained/exact-native-generation \
  --expected-prior sha256:EXACT_CURRENT_COURT_GENERATION \
  --source-edition YYYY-MM-DD \
  --source-edition-evidence /retained/qualified-edition-evidence.json \
  --source-edition-sha256 sha256:EXACT_EVIDENCE_HASH \
  --courts-local-file /retained/same-edition-courts.csv \
  --courts-sha256 sha256:EXACT_COURTS_HASH \
  --search-through UPPER_CLUSTER_ID \
  --retained-search-directory /retained/this-selected-search
```

Choose a separate retained-search directory. Keep the run output and its evidence, build,
and generation directories outside the selected prior, including any symlink targets.

Set `R2_PUBLIC_URL` to the intended publication. The command preserves the existing
`COURTLISTENER_API_TOKEN` prerequisite, including for replay. Upload remains off by default; selecting
`--no-skip-upload` uses ordinary complete admission and conditional publication.
`--max-requests` and `--max-wait-seconds` bound each traversal, including replay
attempts and live pacing/retry waits.
Replayed prefix pages count toward `--max-requests` (default 50, with 900
seconds of waits). A longer
retained walk needs an explicit sufficient allocation; resuming does not make
the traversal unlimited.

Before its first live Search request, an incomplete walk reads the account's
[API usage](https://wiki.free.law/c/courtlistener/help/api/rest/v4/api-usage)
once. That read has its own throttle and spends no Search quota. A blocked
account, any exhausted window, or an unreadable response stops the run before
Search, with the checkpoint unchanged; the error names the window and its
`reset_at`. Otherwise the live part of `--max-requests` shrinks to what every
window too long to refill under the 13-second pacing can still absorb, so a
short quota ends in an ordinary checkpointed stop instead of a 429. Another job
sharing the token can still spend the quota first; a later `Retry-After` stays
authoritative. A complete replay makes no usage read.

After an interruption, invoke the same command with the same selection and
retained directory. An exclusive lock protects the checkpoint. Each successful
page is retained before its rows leave the reader. Resume verifies those bytes,
replays their original observation times through the original count/identity
checks, then requests only the pending cursor. A saved `Retry-After` deadline
refuses live dispatch until it elapses; the command does not schedule itself.
Changed parent, edition/input pins, query/status selection, or missing/corrupt
captures refuse. Retained bytes without a checkpoint entry also refuse because
they lack a verified cursor and observation time. Keep the retained directory
and recover its original controls before retrying, or choose a new directory
when an intentional fresh walk is appropriate.

Only complete source traversal reaches delta conversion and prior append.
The native append preserves prior values/receipts and refuses overlapping IDs;
full generation admission and publication checks remain unchanged. Prior-file
hashing, the ceiling projection, full append/receipt output and admission still
cost time and disk. This change saves repeated successful HTTP acquisitions and
removes task-specific continuation wrappers; it does not eliminate those costs.
Search completion proves the selected indexed, all-status walk, not exhaustive
bulk coverage, historical corrections, or citation endpoint membership.
Historical default-status pages cannot be relabeled as an all-status selection;
retain their original query, request times and membership evidence.

## Selected-export history decision

The `ss-9684` design serves a specific future query: for one opinion, compare
the body and decision metadata stated by two explicitly retained export sets,
or return the last qualified export set on or before a requested export date.
This is history of held publisher snapshots. It cannot establish the first
public text availability or reconstruct overwritten exports. Search catch-up
and retrieval delivery under `ss-bb8c` remain separate from this design.

### Source selection and dated meaning

Select complete, explicitly pinned export sets before building. The existing
generation inputs and receipts must bind each set's opinions, opinion-clusters,
citations, citation-map and required court/relationship inputs to their original
SHA-256, declared export date, capture receipt and local object path. One date or
a matching filename does not prove that these inputs belong together. Preserve
the exact selected input mapping as generation evidence; do not introduce a
separate history catalog. Union only the export sets that the operator selects.

The bounded selected-catalog inventory on 2026-11-06 found the 2026-06-30
opinions original, SHA-256
`65db4902a4ae42c48ef9958f52600dedca5401c194d3611d0b2672cfc6dd4d9c`,
recorded at 54,561,543,156 bytes, with same-date cluster and citation inputs.
It did not qualify an earlier complete export set. Earlier paired originals and
their acquisition/edition evidence remain a coverage gap, rather than proof
that every other local holding is absent. No source was acquired or reparsed for
this decision. Exact inventory records and limits are retained with the
`ss-9684` acceptance evidence.

Expose the literal `date_created`, `date_modified` and `dump_date` values through
the existing opinion, cluster and citation tables when implementing history;
keep their receipt lineage. CourtListener's
[base model](https://github.com/freelawproject/courtlistener/blob/main/cl/lib/models.py)
sets creation and modification automatically for database rows and defines a
modification year of 1750 as unknown. Preserve missing, unparsable and sentinel
values in source evidence, and give them a null usable modification date with an
explicit reason. Do not drop such rows or substitute an export/capture date.
`date_filed` is the court decision date; database `date_created` and modification
are separate from a text's first public availability.

### Identity, body comparison and coherent joins

Extend the existing tables with an exact source-export digest coordinate, using
the native ID plus that digest as the retained occurrence identity. For
`court_opinion_texts` and `court_opinions`, this is the selected opinions-export
digest. The cluster and citation tables use their own selected original digests.
Replaying the same export under the same interpretation must not add another
occurrence. A converter change makes a new derived generation, not a publisher
text revision. No new history table or `first_observed_at` is needed.

Compare the receipt's selected source column and `source_sha256` before calling
an observation a body change. An updated database timestamp with the same body
coordinate is metadata-only. Different selected renderings with equal displayed
text are rendition changes; they do not by themselves prove a judicial text
revision. A body change with a missing or unchanged timestamp still remains a
distinct evidenced occurrence. Preserve the existing `none` body observation
and refused-conversion receipts as separate outcomes.

Choose one qualified export set before current or as-of joins. Text joins its
opinion by `(opinion_id, opinions-export digest)`; that set's pinned cluster export
then qualifies the `cluster_id` join, and its citation export qualifies citation
and citation-map joins. Never join historical text to whichever parent happens
to be current, or independently choose a latest row for each table. A missing
parent remains an explicit unresolved relationship in that selected set; it does
not borrow a parent from another date. An opinion omitted by a later export
remains queryable in the earlier export, without inventing a deletion event or
carrying its old row into the later snapshot.

For current queries choose the latest admitted complete export set. For as-of
queries choose the latest admitted complete set with export date no later than
the requested date and report that set's exact pins and coverage boundary. If
several distinct sets have the same date, require an explicit selected set rather
than ordering digests or database timestamps. This query says what the held
export stated, not what was publicly available at an arbitrary historical instant.

### Resource admission and implementation slices

Reuse the retained measurements in `work/court-text-measure-20261030/` before a
full build. `lengths-full.json` records 349,716,707,859 decompressed bytes for the
selected opinions export. `markdown/sizes.json` estimates 31.87 GB of compressed
Markdown under that measured layout; this is a model, not the final partitioned
table size. These size controls do not qualify conversion time, peak live memory
or spill for the history build; those remain resource-admission gaps.

Use completed matching conversion results before charging that work again.

Budget new output members, receipts, the live spill/receipt parts, largest decoded
bucket, workers, Arrow buffers and the protected selected/prior generations
together. Originals stay in place. Full replay work scales with the sum of
selected decoded export bytes; retained history scales with selected occurrences.
A timestamp key does not reduce either cost. No full conversion or historical
union is selected by this planning result. Admission must use current capacity,
actual retained completion metadata and a bounded reserve; enough space for a
modeled final table alone does not establish enough space for the build.

Keep the existing completed spill marker, per-block receipt commits and run
receipt. Resume only exact export/converter/bound matches; preserve committed
parts and qualified completed generations. New history identity must not be
retroactively asserted on an older completion that lacks it. Greenfield delivery
starts a separately admitted generation and leaves retained historical evidence
unchanged, without a speculative compatibility migration.

Implement through bounded follow-ups, in this order:

1. **Pinned export selection and queryable dates:** reuse existing input and
   receipt controls, validate the exact same-set mapping, expose literal dates
   with missing/sentinel reasons, and qualify a small two-export fixture. Missing
   historical originals remain explicit; this slice acquires nothing.
2. **Retained union and consumers:** qualify native-ID/export occurrence keys,
   revision-scoped replacement, exact current/as-of input selection, parent joins
   and block routing in the existing tables. Cover changed body, unchanged body
   with changed metadata, identical rerun, same-date ambiguity, missing dates,
   refused/no body and later-export omission. Check values and joins as well as
   counts; the default query must select one export set before counting opinions.
3. **Capacity-bound build:** only after retained-input and consumer qualification,
   inventory reusable completed parts and measure the missing work's full live
   storage/memory budget. Prove failure/resume without repeating completed
   conversion, then build a separately admitted candidate if capacity permits.
   Publication and hosted retrieval remain their existing acceptance boundaries.

These are implementation prerequisites, not a claim that multi-export history
is already available. Reuse the bounded output-protection/resume evidence and
the existing conversion measurements; no full export replay is needed to accept
this planning decision.
