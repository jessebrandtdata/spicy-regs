# Retained court Search catch-up

The existing `run-rollup-court-opinion-clusters catch-up` command adds a bounded
Search selection to an explicitly qualified **same-edition** native parent.
Use the default command for a new bulk edition or historical corrections.
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
`COURTLISTENER_API_TOKEN` prerequisite, including for replay. It does not read
account quota state. Upload remains off by default; selecting
`--no-skip-upload` uses ordinary complete admission and conditional publication.
`--max-requests` and `--max-wait-seconds` bound each traversal, including replay
attempts and live pacing/retry waits. They do not estimate a token's free quota.
Replayed prefix pages count toward `--max-requests` (default 50). A longer
retained walk needs an explicit sufficient allocation; resuming does not make
the traversal unlimited.

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
