# Publish complete table generations

Rollups now retain a complete local generation and, when upload is enabled,
publish it through `publication.v2.json`, the one pointer. Readers that understand this index resolve
its immutable table paths once per operation. Existing bare Parquet URLs remain
legacy, unversioned data; this change does not rewrite or certify them.

## The retired `publication.json`

Nothing writes or reads `publication.json` any more; `publication.v2.json` is
the only pointer. The object already in the bucket is left in place on
purpose, so it stays valid but frozen at the last state it was derived to, and
a reader that still fetches it (a user script, a pre-2026-09-27 checkout, the
historical probes in sibling repos) gets an outdated index with no error. A
tombstone that v1 parsers refuse would make such readers fail loudly; it is an
owner decision, not done here. Likewise `_fold_v1` is gone: a workflow run from
a branch that predates v2 publishes to a pointer nobody reads instead of being
folded into v2. Before merging, confirm no workflow log since v2 went live
carries the warning "a writer predating publication.v2.json published them".

## What is checked

A rollup must return every declared output, including a real zero-row Parquet
file for successful empty results. Admission checks the exact member set,
expected schemas where declared, every data page through a bounded full-column
read, row counts, and every byte digest. It uses the installed Rulespec artifact
library for membership, canonical identity, manifests and verification.

The resulting artifact records the host implementation digest, installed package
versions, the captured publication index, any unchanged siblings carried
forward by a partial writer, and its `parents`. A parent is each declared input
the build read: the SHA-256 and size of its bytes (with its family and
generation when a managed family published it, checked against the captured
index), or for an input read in place over HTTP (`remote_inputs`) its storage
ETag and size, which must not change while the rollup builds. The non-table
`docket_search.json.gz` has no generation, so only the refresh receipt binds
its parent. It does not establish source completeness, common
publisher timestamps, correct interpretation, or model qualification. The
captured index identifies managed table inputs, not all original source requests.

When `R2_PUBLIC_URL` is configured, each run uses a new retained directory under
`output/.builds/`. Old local priors therefore cannot bypass the captured index.
Offline runs without a public URL can still use explicitly retained local inputs;
they produce local candidates and cannot publish. Completed artifacts live under
`output/generations/<artifact-digest>/`.

The producing owner keeps a bounded process-local record of final successful
generation admission. Before publication, it freshly admits the exact artifact
bytes and checks current installed policy. Matching owner completion avoids
repeating semantic receipt joins and page decoding. A first completion checks
member bytes before and after those semantic checks. Independent local and
remote reader admission remains deep. Restart owner processes after code or
policy changes; no producer-written success field supplies admission.

## Publication and failure

All shrink guards run before any upload. A table that records its prior's
columns and at least its prior's rows in every partition may shrink to 0.2 of
its prior's bytes rather than 0.5, since a writer's row-group layout can halve
the bytes with no row lost. That comparison reads counts, names and types, never
values: the floor refuses a table whose rows were kept and nearly all their
values lost, and one emptied column still publishes. Objects are created
conditionally under `generations/<family>/<artifact-digest>/`. The publisher
rereads and verifies all remote bytes before conditionally replacing the small
publication index. A concurrent change refuses the update; rebuild from a fresh
index before retrying.
A crash before the pointer update can leave unreferenced immutable objects but
keeps the previous published family intact.
If the connection fails after submitting the pointer update, reread the index:
the complete new family may already be current even though the caller saw an error.

With `SPICYGOV_PUBLICATION_MIRROR` set to a local root (the operator's
`corpora/spicyregs/output/publication`; unset in CI), every publisher, once its
pointer write succeeds, also places the generation at
`<root>/generations/<family>/<digest>/<key>`: the keys the family entry names
(`publication.generation_files`), cloned copy-on-write from the build directory,
never hardlinked. Beside it the mirror keeps the selection that named the
generation: the exact `publication.v2.json` bytes the publish wrote, as the
content-addressed snapshot `<root>/publication.v2/<sha256>.json`, which
supersedes any legacy `<root>/publication.v2.json` file left at the mirror
root. The hook sits
in `_publish_verified_generation` and `publish_fec_release`, so rollups, native
conversion and remote promotion are covered too. A mirror failure does not fail
the publish: it is logged and reported on the returned index
(`PublishedIndex.mirror`, with `error`), and native conversion copies that
report into its receipt's `publish_attempt.mirror`. Keys the publishing host
never held (a remote promotion), and generations published from CI, reach the
mirror through `spicy-regs mirror <root> [--family F]`, which fetches only
absent files through the resuming, pin-checked `fetch_member`, keeps a file
already present at its pinned size without rehashing it, and snapshots the
pointer it read. When there is anything to fetch, it refuses before writing if
those bytes would leave less than `--min-free-gib` (default 30) free on the
mirror's volume. R2 retains every published generation for rollback; the
mirror accumulates superseded generations until `spicy-regs mirror <root>
--prune` deletes, for each mirrored family whose selection did not move during
the run, every generation directory but the selected one. The publish hook
never deletes. `.env.example` sets the variable for a local checkout.

A family cannot silently change its table membership or take another family's
table. Those changes need an explicit migration. Successful empty tables still
face the existing shrink guard; an intentional destructive replacement requires
the existing explicit override after reviewing the result.

Nor can a family silently stop publishing receipts for a table. A rollup
declares `retired_receipt_datasets`; publication then accepts the first
generation without them, requires each named table to stay published, and
refuses any later generation that claims receipts for one again. Undeclared,
the generation is refused. The prior generation's receipt file is not touched.
A receipt file must still classify every table beside it, so the tables of one
family leave receipts together.

The Congress.gov archive walker is a partial writer of `bill-family`. Its bill
update preserves the existing wider columns using the existing merge. It copies
all sibling tables byte-for-byte from the captured complete family and records
which generation supplied them. These siblings are carried forward, not
reprocessed. A public cold start must first produce a complete `bill-family`
generation; a lone bill file cannot bootstrap a complete family.
An offline cold-start partial candidate is marked `local-partial`; the generic
publisher refuses it too. Merely retaining a valid Parquet artifact does not
promote that partial result to a complete family.

## Regulatory base tables

The ETL rewrites the bare `dockets.parquet` and `documents.parquet` after every
sweep batch and primes each run from those bare objects
(`r2.download_working_copy`), never from a managed family. Once a sweep
completes, the regulatory refresh publishes each as its own family,
`dockets` and `documents` (`run-rollup-dockets`, `run-rollup-documents`), from
the same working copy, refusing a null or repeated identity. It does this
before the comments mirror captures base versions, so every dependent reads
that sweep's snapshot, and `check_refresh_inputs.py` holds both family pins
along with the bare objects' ETags. Priming from a family instead would drop
the batches of a partly completed sweep whose keys the manifest had already
retired. Bare-URL readers, such as notebooks and the browser, keep reading
the working copies, which change batch by batch as before.

Before the families publish, `fill-docket-gaps` (`pipelines/docket_gaps.py`)
asks the Regulations.gov API once for each docket that documents or the comments
index name and the mirror never captured, and merges what it serves into the
dockets working copy through the ETL's own extraction. It records every other
answer in the bare `docket_gap_outcomes.parquet` and does not ask again for 30
days: 404 means the publisher does not publish that docket, and 400 "Invalid
ID" means the id is outside its grammar. A legacy `-RULEMAKING` id whose base
id is served is recorded as an alias of it. If a request gets no answer, the
job fails, but the families still publish whatever it served.

## Completion and freshness for active releases

Candidate completion means the selected source scope finished and its complete
family passed existing admission. Release also requires the existing publication
checks. A publication timestamp
says when those bytes became current; it does not advance their source edition,
prove full historical coverage, or erase unresolved source outcomes. Retain the
selected scope and earlier coverage when refreshing it.

| Family | Selected coverage and completion | Freshness claim and current signal | Smallest useful consumer check |
| --- | --- | --- | --- |
| Court opinion clusters | Complete selected bulk edition with its docket-to-court map and receipts; keep the existing prior-ID preservation check for the recovery. An explicitly offline edition can finish without live search. | Name the bulk edition and any separately completed search scope. The [cluster workflow](https://github.com/mikewolfd/spicy-regs/blob/12353ea83aa035cc3a2758276be67e5efc1fabdd/.github/workflows/rollup-court-opinion-clusters.yml) runs daily: a rebuild when a newer export is listed, otherwise a bounded search catch-up whose journal states the id reached and the backlog left ([court search](court-search-recovery.md)). That catch-up is not evidence that an offline candidate includes later decisions. | Read a selected decision with its court fields and receipt. |
| Court citations | Every declared citation-family output from one common bulk edition. The [builder](https://github.com/mikewolfd/spicy-regs/blob/12353ea83aa035cc3a2758276be67e5efc1fabdd/src/spicy_regs/transforms/build_court_bulk_tables.py) requires published clusters and refuses a referenced cluster ID above their maximum. | Name the quarterly edition; the [citation workflow](https://github.com/mikewolfd/spicy-regs/blob/12353ea83aa035cc3a2758276be67e5efc1fabdd/.github/workflows/rollup-court-citations.yml) follows that source cadence. Additional cluster coverage is required when the actual dependency check fails or selected product scope requires later decisions; bounded catch-up is one route. | Read a citation through the intended court relationship; retain explicit missing/ambiguous endpoints. The maximum-ID guard does not prove every endpoint exists. |
| Legislative families | Finish the selected Congresses, bill types and declared family outputs; retain carried siblings' provenance and unresolved outcomes. The [vote/term chain](https://github.com/mikewolfd/spicy-regs/blob/12353ea83aa035cc3a2758276be67e5efc1fabdd/.github/workflows/refresh-congress-vote-terms.yml) needs both votes and members before terms. | State each input's acquisition scope and carried generation. Daily workflows do not imply historical completeness; bill `update_date` and `latest_action_date` are the existing lag signals. | Read a selected bill or vote with its receipt and the relationship the release changes. |
| Lobbying filings | Finish the requested posted-date window or filing year and all declared [family outputs](https://github.com/mikewolfd/spicy-regs/blob/12353ea83aa035cc3a2758276be67e5efc1fabdd/src/spicy_regs/pipelines/rollups/lobbying_filings.py), merged with the selected prior. Incomplete acquisition or fresh conversion refusals block completion. | Record the actual bounded window/year. The [workflow](https://github.com/mikewolfd/spicy-regs/blob/12353ea83aa035cc3a2758276be67e5efc1fabdd/.github/workflows/rollup-lobbying-filings.yml) runs daily; the freshness monitor uses `dt_posted` as its lag signal. A recent filing does not prove all history was read. | Read a filing with its activities/lobbyists and a carried receipt field through the intended reader. |
| Regulatory release | Complete the selected sweep and publish its base families before dependent refreshes capture their pins, as above. Preserve docket-gap outcomes and existing dependency/input checks. | Report sweep scope and source watermarks separately from publication time. Dockets/documents use `modify_date`; comment coverage is separate. | Read the affected base row or dependent view at the selected pins; keep unavailable source outcomes explicit. |

These consumer checks describe the evidence needed from an already required
release check, not additional full scans or workflow dispatches. They do not
replace existing qualification, refresh-before-coverage or deployment checks.
Operational holds recorded in the active work ticket still apply; this guidance
does not authorize resuming held work.

The age budgets in [the freshness monitor](https://github.com/mikewolfd/spicy-regs/blob/12353ea83aa035cc3a2758276be67e5efc1fabdd/scripts/check_rollup_freshness.py)
are alerts, not agreed product tolerances. Acceptable product lag for these
families remains unresolved: a decision must name the consumer need, required
source scope and allowed delay before changing cadence or coverage. Existing
schedules stay in force. Do not make a completed selected bulk edition wait for
optional enrichment or open-ended catch-up; retain a pending dependency when
its actual required check fails.

## Readers

- A family can publish processing evidence in an optional `etlReceipts` member.
  MCP exposes the selected families' immutable receipt files through the generic
  `etl_receipts` SQL view and reports each family's generation and byte pin.
  CLI downloads retain the selected families' receipt files alongside their
  source tables; local MCP verifies their hashes and never fetches replacement
  evidence remotely. A missing file or schema mismatch refuses the connection.
  Deploy a reader that admits this field before advancing a publication pointer
  that includes it.
- Rollup reads share one captured index. Managed download failures and digest
  mismatches abort; only an index 404 permits legacy resolution.
- MCP captures one index per cached connection, creates managed views at immutable
  URLs, checks their schemas, and refuses a connection if a managed member is
  missing. `list_sources` distinguishes actual available tables from declarations;
  `describe_table` labels managed generations versus legacy data. Query responses
  include the publication pin of each table the query names. The remote query reader relies on immutability;
  it does not rehash whole tables for each query.
- The rulemaking dataset publishes under its own pointer,
  `materialized/rulemaking/latest.json`. Each MCP connection build reads it once
  beside the index (`publication.load_rulemaking_snapshot`): as the pipeline reads
  its prior generation, the pointer and its manifest must be one readable format
  of this dataset naming one snapshot, and each public artifact must sit under
  that snapshot's prefix. An invalid pointer refuses the connection, as an
  invalid index does. Views read those immutable keys, so a moved pointer is seen
  when the cached connection rebuilds (`SPICY_REGS_CONNECTION_TTL`, default 300
  seconds), and a member that cannot be read refuses the connection. The
  manifest states no columns, so `describe_table` compares each view with the
  dictionary. Status reports `rulemaking_snapshot` with the snapshot id, and the
  ledger's `snapshot_…` pins compare with it. Local MCP does not read the pointer.
- CLI download accepts rollup names. When the requested set includes managed data,
  it downloads the requested set into one batch, verifies every managed member,
  records the index and selected keys, then switches `current` only on success.
  Local reads resolve this link once per command. Requested legacy members remain
  explicitly unversioned even when downloaded in that batch.
- Local MCP accepts that download root, its `current` link, or a specific batch
  directory through `SPICY_REGS_DATA_DIR`. A connection selects the batch once,
  rehashes managed members and checks their schemas. It exposes only the selected
  tables and reports their pins as `managed_download`; loose Parquet directories
  remain `local_unversioned`. File-change checks around tool statements refuse
  ordinary replacement or mutation after verification. A new `current` target
  takes effect when the cached connection rebuilds. These checks do not make
  writable local storage immutable or establish the source's completeness.
- Dictionary remote schema discovery captures the same index and rulemaking
  pointer (`publication.published_urls`), as does `scripts/check_table_joins.py`,
  and holds each live column's type to its declaration as well as its name.
  Declared schema pages alone continue to make no claim of production availability.

## Auditing a published generation

`scripts/audit_generation.py` re-reads one family's (or one table's) current
generation anonymously and writes a JSON report. It never writes to storage.

```sh
uv run --frozen python scripts/audit_generation.py --family laws --prior 43130abc \
  --env-file ../spicy-docs/.env --output report.json
```

The report keeps its sections separate: publication (every member byte admitted;
index, manifest and root agree), schema (names and types against the dictionary),
identity (duplicate and NULL keys counted before any row is paired), conservation
against a prior pin from the generation's captured chain (both directions, and
multisets where identity cannot pair rows; a table whose build journals a
`rows-retired` event, as `table_merge.retired_rows` computes it, must remove
exactly the identities it names, and a removal it does not name, one it names that
the output still holds, or one the prior never held is a failure), evidence (admission, binding,
credential and body-shape scans) and state (an empty table is state, not a
failure). Every report names source qualification and deployment as not assessed,
and `limits` lists what the run could not see. Exit status 1 means at least one
`fail` finding; 2 means the audit could not run. `check_ledger_pins.py` compares
pins only; this tool audits what a pin holds.

## Rollout limits

The offline object-store tests exercise conditional creation, interrupted
uploads, concurrent index changes, byte corruption and stale inputs. They are
not a live R2 deployment rehearsal. Validate these operations in a disposable
bucket before enabling the new writer in production. Partitioned comments,
Iceberg publication and the browser's non-table `docket_search.json.gz` object
are outside this table-generation path; base regulations.gov dockets and
documents joined it as families (above). Historical rebuilds, semantic
qualification and public adoption remain separate work. No publication index is
created remotely by the test suite.
