# Refreshing the hand-run families

These published families are refreshed from a workstation: `treasury-fiscal`,
`administration-policy`, `ofac-sanctions`, `house-disbursements`,
`grants-opportunities` and `executive-orders`. Each hand refresh is the same
five steps: acquire, build, dry-run publish, publish, read back. What each
command does and refuses is in the family's own page
([Treasury](treasury-fiscal.md), [administration policy](retained-collections.md),
[OFAC](ofac-sanctions.md), [House](congressional-disbursements.md),
[Grants](grants-opportunities.md), [executive orders](executive-orders.md));
this page is only the order.

## Before any family

Run every command from the checkout through
`uv run --frozen --extra comments-reader`, written `$UV` below.

```sh
export WORK=/path/to/a/fresh/scratch/dir
unset SPICYGOV_RAW_ROOT            # a real refresh writes originals to the shared raw archive
SPICY_REGS_R2_URL=https://data.spicygov.ai $UV spicy-regs mirror "$WORK/live" \
  --family treasury-fiscal --family administration-policy \
  --family ofac-sanctions --family grants-opportunities      # 150 MB
```

`mirror` fetches the live generations it names, each file checked against its
pin, and prints their prefixes and the pointer snapshot that selected them.
Below, `$LIVE_TREASURY`, `$LIVE_POLICY` and the like are those generation
directories under `$WORK/live`, and `$LIVE_INDEX` is the snapshot
(`$WORK/live/publication.v2/<sha>.json`). Every build takes
`--read-index "$LIVE_INDEX"`: the generation then records the index it read,
so the publisher refuses it once the family has moved on, and retention finds
the generation it replaces.

A rehearsal instead sets `SPICYGOV_RAW_ROOT` to a scratch directory for every
step, adds `--scratch-raw` to the dry run and stops there. What keeps a
rehearsal build from being published:

- a publish, and a dry run without `--scratch-raw`, refuse while
  `SPICYGOV_RAW_ROOT` is set;
- they refuse evidence whose `source-archive.json` pointer is missing or names
  another archive than the shared one;
- they refuse when any original the evidence journal cites is absent from the
  shared archive, whatever the pointer says. An original is a capture or a file
  retained with `raw_source=True`; a selection, manifest, receipt or normalized
  input the build wrote for itself is published with its evidence instead.

The last check is by name: it does not re-hash the originals, and it cannot
tell a scratch build from a real one once the same originals have also been
acquired into the shared archive. That build is publishable, and correctly so:
its tables come from bytes the shared archive holds.

The publish step alone needs credentials. It reads `R2_ENDPOINT`,
`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and `R2_BUCKET_NAME` from `.env`
(`--env-file` names another file) and refuses any account or bucket but the
verified one. The dry run reads none of them. Both refuse while
`R2_ALLOW_SHRINK` or `R2_MIN_SIZE_RATIO` is set, in the shell or in `.env`, so
the dry run and the publish apply the same shrink guard.

Two settings in `.env` do not do in a publish run what they do elsewhere:

- `SPICYGOV_CATALOG_REQUEST`: the catalogue refresh request needs
  `SPICYGOV_RAW_ROOT` set and the publish needs it unset, so the request is not
  written (the publish logs that and succeeds). Refresh the catalogue by hand.
- `SPICYGOV_PUBLICATION_MIRROR`: `.env.example` gives it as a relative path,
  which resolves from the directory the command runs in. From a worktree that
  is a stray directory beside the worktree. Give it as an absolute path, or
  run the publish from the primary checkout.

Before OFAC or Grants admission, check that no nightly job is running; the
admission holds a core for minutes:

```sh
ps -axo pid,etime,command | grep -E 'nightly_publish\.py|archive_publish_job\.py' | grep -v -E 'grep|pytest|test_'
```

## The publish steps, for every family

`$GENERATION` and `$EVIDENCE` are the `directory` and `sourceEvidence` the build
printed; pass the evidence from where the build wrote it. `$PRIOR` is the live
generation's digest, which the dry run prints as `prior.artifactDigest`. A
publish of a family that is already live requires it and refuses when the live
generation is another one, so a generation published after your dry run is
never replaced unseen.

```sh
$UV publish-local-generation --generation "$GENERATION" --evidence "$EVIDENCE" \
  --family "$FAMILY" --dry-run --output "$WORK/$FAMILY-dry-run"
# compare the built tables with the live ones, then:
$UV publish-local-generation --generation "$GENERATION" --evidence "$EVIDENCE" \
  --family "$FAMILY" --expect-prior "$PRIOR" --output "$WORK/$FAMILY-receipt"
```

The dry run runs the real publication guards against a private disk store
holding a copy of the public index (`--live-index` names a saved copy), and
its receipt lists each table's rows beside the live rows. The publish compares
with the index stored in the bucket, read with the credentials; the two are
the same object unless a publish lands in between, which `--expect-prior`
catches. Neither creates `--output` until every guard has passed, so a refused
command can be corrected and run again as typed. The publish then reads the
family back anonymously from
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
  --captures "$WORK/treasury-new.json" --read-index "$LIVE_INDEX" --output-dir "$WORK/treasury-build"
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
  --captures "$WORK/policy-captures.json" --read-index "$LIVE_INDEX" --output-dir "$WORK/policy-build"
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
  --captures "$WORK/ofac-captures.json" --read-index "$LIVE_INDEX" --output-dir "$WORK/ofac-build"
```

The published table then holds these two snapshots only. Their ZIPs stay in
the raw archive and in R2 source evidence, which retention never deletes. What
happens to the replaced generation's tables is under "After a publish".

## house-disbursements

The House posts each quarter's statement about two months after it ends
(July-September in November). Measured on a loaded workstation on 2026-11-10,
rebuilding the published 84 files: `select` 25 seconds, `acquire` 32 seconds
(0.92 GB read from the raw archive, nothing downloaded), `normalize` 4 minutes
and under 1 GB of memory, `build` 4.5 minutes, the dry run 31 seconds; about
4.3 GB of scratch.

```sh
SPICY_REGS_R2_URL=https://data.spicygov.ai $UV spicy-regs mirror "$WORK/live" --family house-disbursements
$UV python -m spicy_regs.pipelines.house_disbursements select --generation "$LIVE_HOUSE" \
  --output-dir "$WORK/house-select"
$UV python -m spicy_regs.pipelines.congressional_disbursements acquire \
  --selection "$WORK/house-select/selection.json" --reuse "$WORK/house-select/reuse.json" \
  --output "$WORK/house-acquire"
$UV python -m spicy_regs.pipelines.congressional_disbursements normalize \
  --acquisition "$WORK/house-acquire/acquisition.json" --output "$WORK/house-normalized"
$UV python -m spicy_regs.pipelines.house_disbursements build \
  --normalization "$WORK/house-normalized/manifest.json" --read-index "$LIVE_INDEX" --output "$WORK/house-build"
```

`select` writes the selection behind the published rows plus each CSV the
House's current and archive pages list beside them (both pages are captured
into the raw archive first), and the `--reuse` map that reads the published
originals from the raw archive. It prints what it added: nothing added means
nothing to publish. A listed revision of a published quarter refuses; select
it by hand. A new file the normalizer cannot read stays `parse-failed` and the
build refuses; review its bytes and pass `--source-options`
([congressional-disbursements.md](congressional-disbursements.md)). The build
prints `directory` and `source_evidence`, the `$GENERATION` and `$EVIDENCE` of
the publish steps. Every quarter is normalized again and every partition is
rewritten, because each row names its run's normalization receipt.

## grants-opportunities

Acquire takes about 15 seconds (78 MB). The build takes about 6 minutes, nearly
all of it admission, and about 1 GB of scratch; 192 MB uploaded.

```sh
$UV python -m spicy_regs.pipelines.grants_opportunities acquire --snapshot-date "$EXPORT_DATE" \
  --output-dir "$WORK/grants-acquire" --captures-out "$WORK/grants-captures.json"
$UV python -m spicy_regs.pipelines.grants_opportunities build \
  --captures "$WORK/grants-captures.json" --read-index "$LIVE_INDEX" --output-dir "$WORK/grants-build"
```

`$EXPORT_DATE` is the newest daily export, `YYYY-MM-DD`. The 2026-11-09 export
was written at 09:41 UTC; before a day's export exists, `acquire` for that date
fails with HTTP 403 and the day before is the newest. The published table then
holds this snapshot only.

## executive-orders

Mirror it first (`--family executive-orders`, under 1 MB). `acquire` lists
every order the Federal Register API serves (268 requests, 5 MB, under a
minute) and fetches a body only for an order the live generation lacks or holds
as failed, about 0.9 seconds a request. The orders the live generation holds
keep their published bodies, which the shared raw archive already has. The
build takes under a minute; a publish uploads about 40 MB.

```sh
$UV python -m spicy_regs.pipelines.executive_orders acquire --read-index "$LIVE_INDEX" \
  --output-dir "$WORK/eo-acquire" --captures-out "$WORK/eo-captures.json"
$UV python -m spicy_regs.pipelines.executive_orders build --captures "$WORK/eo-captures.json" \
  --read-index "$LIVE_INDEX" --output-dir "$WORK/eo-build"
```

`acquire` prints how many orders were added, changed and removed. When all three
are 0 it prints `"nothingNew": true`, fetches no body, and `build` refuses the
selection: there is nothing to publish. Rerunning `acquire` into the same
`--output-dir` keeps the bodies it already fetched. An order the publisher has
not touched keeps its published row byte for byte, so compare the build with
the live table by `source_record_id`: only added, changed and removed orders
should differ. [How the family acquires and builds](executive-orders.md).

The first refresh after 2026-10-20 adds every year at once. The 2026-11-10
rehearsal fetched 1,335 bodies in 1,593 requests (the 257 orders from before
2000 and one from 2010 have no publisher XML and come from GovInfo HTML), about
24 minutes and 15 MB. The 239 published rows kept their pointers and text; they
differ from the live rows only in `text_derivation_version` (002 to 004; SpicyDocs
derives byte-identical text) and the new `source_sha256` column. Ten orders
from 2015 to 2021 build as `source_failed`: SpicyDocs' XML validator refuses
their FRDOC marker.

The hosted `rollup-executive-orders.yml` runs on weekdays; it restores the
published bodies from R2 source evidence and publishes nothing when nothing is
new.

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
- **Coverage sentences.** A table's coverage is written in
  `data_dictionary/descriptions.yaml`; `$UV spicy-regs-dict generate` renders
  it to `src/spicy_regs/table_metadata.json` and `docs/tables/`, and
  `spicy-regs-dict check` holds them equal. Change it there when a refresh
  changes what the table covers, such as selecting earlier snapshots again.
- Keep `$WORK`'s capture selections and both receipts with the session's
  records. The OFAC and Grants selections are the only record of which
  captures a published snapshot was admitted from besides the published rows.
- **The replaced generation.** Weekly retention (Sundays 05:45 UTC,
  `spicy_regs/generation_retention.py`) keeps a family's current generation
  and the two before it, which it finds through the index each build recorded
  with `--read-index`, and any generation whose digest a page under `docs/`
  names. A build without `--read-index` records no predecessor, and the tables
  of the generation it replaces are deleted at the next weekly run unless
  named. The four generations the first refreshes replace are named below, so
  they stay whichever way their successors are built. Source evidence is never
  deleted.

## Generations kept by name

Each row keeps one generation's tables on R2 for as long as it stays here.
These are the 2026-10 generations the first refreshes replace.

| Family | Generation |
| --- | --- |
| `treasury-fiscal` | `372240db17b82c781e027d351c38618610dcf342b3111445736ac776dceca8fe` |
| `administration-policy` | `57069ca7317f126e30829cdf955a8f672d36c28dd2faae11a048a6894db8abc1` |
| `ofac-sanctions` | `30353c6e00d9a6d374b6862cc6c4774e9171eb95b5b3d921aba8f362315804d1` |
| `grants-opportunities` | `b98a33ef40900f6dde4f49bfd9a41609178750d2af0207b00b52dbf1f0b0e5ab` |

## Hosted refresh

Each family also has a hosted workflow, `rollup-<family>.yml`, that runs
`run-rollup-<family>` through the shared `_rollup.yml`. They are dispatch-only
until one hosted run with `skip_upload` (the default when dispatched) has
passed for each; a schedule starts running the day it reaches main.
`treasury-fiscal` passed its rehearsal on 2026-11-10 (it built and admitted the
generation with the newly listed debt day) and has run daily at 22:43 UTC since.

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
| `house-disbursements` | Captures the House's two listing pages and reads the published tables (162 MB); when the pages list a CSV the published rows lack, restores the published originals from R2 evidence, downloads the new ones and rebuilds every quarter | The pages list no new CSV |
| `grants-opportunities` | Finds the newest daily export of the last seven days and publishes it as the one snapshot | That export date is published |
| `executive-orders` | Lists every order the API serves, restores the published bodies from R2 evidence and fetches only the bodies the published generation lacks; scheduled on weekdays from the start | No order was added, changed or removed |

The Treasury run needs the three-table generation, so it can start only after
the first hand refresh above. It adds debt days only: a new monthly statement
still needs its reviewed estimate entries and a hand refresh.

Schedules to add once the rehearsals pass, each beside `workflow_dispatch`:

```yaml
# rollup-grants-opportunities.yml: two hours after the 2026-11-09 export was written (09:41 UTC)
  schedule:
    - cron: '47 11 * * *'
# rollup-ofac-sanctions.yml
  schedule:
    - cron: '17 22 * * *'
# rollup-house-disbursements.yml: weekly; a quarter posts about two months after it ends
  schedule:
    - cron: '29 13 * * 1'
# rollup-administration-policy.yml
  schedule:
    - cron: '13 23 * * *'
```
