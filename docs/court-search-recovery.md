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
