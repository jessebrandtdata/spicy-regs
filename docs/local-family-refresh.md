# Refreshing the hand-run families

Four published families are refreshed from a workstation: `treasury-fiscal`,
`administration-policy`, `ofac-sanctions` and `grants-opportunities`. Each
hand refresh is the same five steps: acquire, build, dry-run publish, publish, read
back. What each command does and refuses is in the family's own page
([Treasury](treasury-fiscal.md), [administration policy](retained-collections.md),
[OFAC](ofac-sanctions.md), [Grants](grants-opportunities.md)); this page is only
the order.

## Before any family

Run every command from the checkout through
`uv run --frozen --extra comments-reader`, written `$UV` below.

```sh
export WORK=/path/to/a/fresh/scratch/dir
unset SPICYGOV_RAW_ROOT            # a real refresh writes originals to the shared raw archive
SPICY_REGS_R2_URL=https://data.spicygov.ai $UV spicy-regs mirror "$WORK/live" \
  --family treasury-fiscal --family administration-policy
```

`mirror` fetches the live generations it names, each file checked against its
pin, and prints their prefixes; `$LIVE_TREASURY` and `$LIVE_POLICY` below are
those directories under `$WORK/live`.

A rehearsal instead sets `SPICYGOV_RAW_ROOT` to a scratch directory for every
step and stops after the dry run. `publish-local-generation` refuses to publish
a build whose evidence points at any raw archive but the shared one, so a
rehearsal build cannot be published by mistake.

The publish step alone needs credentials. It reads `R2_ENDPOINT`,
`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and `R2_BUCKET_NAME` from `.env`
(`--env-file` names another file) and refuses any account or bucket but the
verified one. The dry run reads none of them.

Before OFAC or Grants admission, check that no nightly job is running; the
admission holds a core for minutes:

```sh
ps -axo pid,etime,command | grep -E 'nightly_publish\.py|archive_publish_job\.py' | grep -v -E 'grep|pytest|test_'
```

## The publish steps, for every family

`$GENERATION` and `$EVIDENCE` are the `directory` and `sourceEvidence` the build
printed. `$PRIOR` is the live generation's digest, which the dry run prints as
`prior.artifactDigest`; passing it to the publish refuses a live table that
changed after you compared against it.

```sh
$UV publish-local-generation --generation "$GENERATION" --evidence "$EVIDENCE" \
  --family "$FAMILY" --dry-run --output "$WORK/$FAMILY-dry-run"
# compare the built tables with the live ones, then:
$UV publish-local-generation --generation "$GENERATION" --evidence "$EVIDENCE" \
  --family "$FAMILY" --expect-prior "$PRIOR" --output "$WORK/$FAMILY-receipt"
```

The dry run runs the real publication guards against a private disk store
holding a copy of the live index, and its receipt lists each table's rows
beside the live rows. The publish then reads the family back anonymously from
`https://data.spicygov.ai` (the index, the artifact root and every member's
bytes) and writes `receipt.json`: prefix, digest, each table's rows and
SHA-256, and the evidence it pins. If the publish ends with an error after
uploading, run the same command again with a new `--output`: when the family
already is this generation it writes nothing and only reads back.

## treasury-fiscal

About 15 seconds end to end; 17 MB uploaded, most of it the five report PDFs.

```sh
$UV python -m spicy_regs.pipelines.treasury_fiscal acquire --debt-after "$LAST_DEBT_DATE" \
  --output-dir "$WORK/treasury-acquire" --captures-out "$WORK/treasury-new.json"
$UV python -m spicy_regs.pipelines.treasury_fiscal published-captures \
  --generation "$LIVE_TREASURY" --captures-out "$WORK/treasury-published.json"
$UV python -m spicy_regs.pipelines.treasury_fiscal build --captures "$WORK/treasury-published.json" \
  --captures "$WORK/treasury-new.json" --output-dir "$WORK/treasury-build"
```

`$LAST_DEBT_DATE` is the latest `record_date` in the live
`treasury_debt_observations`. A new monthly statement is added by naming it
(`--select mts_table_1:DATE --select mts_table_2:DATE --report DATE`) once its
zero estimates are reviewed, as the Treasury page describes.

Two things hold for the first refresh after 2026-10-20 only:

- The live generation predates `treasury_statement_classification_cells`, so
  `published-captures` cannot name its Table 2 pages. Pass the kept capture
  files instead of `treasury-published.json`: `captures-20261031.json`,
  `captures-20261031-t1.json` and `captures-20261020-debt-report.json` in
  `corpora/session-reports-2026-10-30/treasury-t2/`, with `--debt-after 2026-10-16`.
- Both publish commands need `--added-table treasury_statement_classification_cells.parquet`.
  Without it publication refuses with "Family membership changed; explicit
  migration is required". Once the table is published the flag is inert.

## administration-policy

About 10 seconds; 6 MB uploaded.

```sh
ARCHIVE_COMMIT=$(git ls-remote https://github.com/unitedstates/statements-of-administration-policy.git HEAD | cut -f1)
$UV python -m spicy_regs.pipelines.administration_policy acquire --commit "$ARCHIVE_COMMIT" \
  --prior-statements "$LIVE_POLICY/administration_policy_statements.parquet" \
  --output-dir "$WORK/policy-acquire" --captures-out "$WORK/policy-captures.json"
$UV python -m spicy_regs.pipelines.administration_policy build \
  --captures "$WORK/policy-captures.json" --output-dir "$WORK/policy-build"
```

`acquire` prints each administration with the commit its capture comes from: a
file unchanged since publication keeps the published commit. If every line
shows a published commit, nothing changed and there is nothing to publish. A
new administration's file is added with `--administration 48-Name`.

## ofac-sanctions

Acquire takes seconds (7 MB). The build takes about 12 minutes, nearly all of
it admitting the SDN list, and about 0.5 GB of scratch; 41 MB uploaded.

```sh
$UV python -m spicy_regs.pipelines.ofac acquire --list sdn --list consolidated \
  --output-dir "$WORK/ofac-acquire" --captures-out "$WORK/ofac-captures.json"
$UV python -m spicy_regs.pipelines.ofac build \
  --captures "$WORK/ofac-captures.json" --output-dir "$WORK/ofac-build"
```

The published table then holds these two snapshots only. Their ZIPs stay in
the raw archive and in R2 source evidence, which retention never deletes. What
happens to the replaced generation's tables is under "After a publish".

## grants-opportunities

Acquire takes about 15 seconds (78 MB). The build takes about 6 minutes, nearly
all of it admission, and about 1 GB of scratch; 192 MB uploaded.

```sh
$UV python -m spicy_regs.pipelines.grants_opportunities acquire --snapshot-date "$EXPORT_DATE" \
  --output-dir "$WORK/grants-acquire" --captures-out "$WORK/grants-captures.json"
$UV python -m spicy_regs.pipelines.grants_opportunities build \
  --captures "$WORK/grants-captures.json" --output-dir "$WORK/grants-build"
```

`$EXPORT_DATE` is the newest daily export, `YYYY-MM-DD`. The 2026-11-09 export
was written at 09:41 UTC; before a day's export exists, `acquire` for that date
fails with HTTP 403 and the day before is the newest. The published table then
holds this snapshot only.

## After a publish

Nothing in the repository fails when these go stale; each is prose or a
reviewed note that stops describing the live table. Update them in the commit
that records the refresh.

- **Source-coverage notes.** `source_catalog/config/registry.v1.json` holds one
  reviewed `source-coverage` observation per table, bound to a `table_version`
  (the generation digest). The catalog shows a note only beside that exact
  version ([shared-source-catalog.md](shared-source-catalog.md)), so after a
  publish the family's tables show no reviewed note until an observation for
  the new digest is added. The old observations stay: they describe the old
  generation.
- **`docs/grants-opportunities.md`** names the accepted 2026-10-20 ZIP, source
  release and generation digests. They remain true of that export. Add the
  published snapshot's date and digests beside them, from `receipt.json` and
  the capture selection.
- **Coverage sentences** in `src/spicy_regs/table_metadata.json` (rendered to
  `docs/tables/`): `ofac_*` says prior selected snapshots remain on refresh,
  which a refresh that selects only the new snapshots does not do, and
  `grants_opportunity_observations` still says public delivery is pending.
  Correct them, then run `$UV spicy-regs-dict generate` and `spicy-regs-dict check`.
- Keep `$WORK`'s capture selections and both receipts with the session's
  records. The OFAC and Grants selections are the only record of which
  captures a published snapshot was admitted from besides the published rows.
- **The replaced generation.** Weekly retention (Sundays 05:45 UTC,
  `spicy_regs/generation_retention.py`) finds a family's earlier generations
  through the index each build read. A hand-built generation records none, so
  after a hand publish the replaced generation's tables are deleted at the
  next weekly run unless a page under `docs/` names its digest. The OFAC and
  Grants pages name their 2026-10-20 generations for that reason; the replaced
  Treasury and administration-policy generations are not named, and their rows
  are all in the new generation. Source evidence is never deleted.

## Hosted refresh

Each family also has a hosted workflow, `rollup-<family>.yml`, that runs
`run-rollup-<family>` through the shared `_rollup.yml`. They are dispatch-only
until one hosted run with `skip_upload` (the default when dispatched) has
passed for each; a schedule starts running the day it reaches main.

A hosted runner has no shared raw archive. Each run downloads through its
generation's source evidence, and publication uploads every evidence blob to
`source-evidence/blobs/sha256/` in R2 before the family pointer moves. That is
where a hosted run's downloads are kept; the workstation's raw archive does not
receive them.

| Family | What a run does | When it publishes nothing |
| --- | --- | --- |
| `treasury-fiscal` | Reads the published generation's captures from its rows, restores their bytes from R2 evidence, adds each Debt to the Penny day listed after the newest published one | No later day is listed |
| `administration-policy` | Reads every published administration's file at the archive's current commit | No file's bytes differ |
| `ofac-sanctions` | Downloads the SDN and consolidated exports and publishes them as the two snapshots | Both ZIPs are the published ones |
| `grants-opportunities` | Finds the newest daily export of the last seven days and publishes it as the one snapshot | That export date is published |

The Treasury run needs the three-table generation, so it can start only after
the first hand refresh above. It adds debt days only: a new monthly statement
still needs its reviewed estimate entries and a hand refresh.

Schedules to add once the rehearsals pass, each beside `workflow_dispatch`:

```yaml
# rollup-grants-opportunities.yml: two hours after the 2026-11-09 export was written (09:41 UTC)
  schedule:
    - cron: '47 11 * * *'
# rollup-treasury-fiscal.yml: after the US business day
  schedule:
    - cron: '43 21 * * *'
# rollup-ofac-sanctions.yml
  schedule:
    - cron: '17 22 * * *'
# rollup-administration-policy.yml
  schedule:
    - cron: '13 23 * * *'
```
