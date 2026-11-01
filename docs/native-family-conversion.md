# Convert an old-shape family to native, once

A scheduled rollup refuses a published family that carries no ETL receipts
(`selected_generations.py`: "selected native input requires one ETL receipt
generation"). A family whose builder reads its own prior therefore cannot
publish its first native generation by itself. The old-shape readers were
removed on purpose on 2026-10-03, and the owner's direction of 2026-10-04 is
to fix forward: no scheduled path reads an old shape again.

`scripts/convert_family_to_native.py` converts one named family, once. Owner
decision, 2026-10-05: convert the small families now; the large tables wait
for a cheaper writer. Evidence for everything below:
`~/Work/corpora/native-migration-dryrun-20261004/` (`dryrun-receipt.json`,
`state/family-inventory.json`).

## What the command does

1. Refuses unless this checkout is main, clean, and has the SpicyDocs wheel
   main pins, all equal to the values the run was started for
   (`--expect-main`, `--expect-spicy-docs`). Clean includes untracked files
   under `src/` and `scripts/`, which the command could load. Main is read
   from the remote's URL; a local path is refused. The installed SpicyDocs
   files are hashed against the vendored wheel, and the wheel against its
   digest in `uv.lock`.
2. With `--publish`: checks the credentials and that `R2_BUCKET_NAME` is the
   `--expect-bucket`, and prints the bucket and endpoint host, before it
   converts anything.
3. Captures the publication index, its ETag and the family's entry. Reads
   the replaced generation's source evidence (roots, manifests and journal,
   never a table) before anything is downloaded, and refuses unless it is
   current completed-build evidence. Evidence written before
   `evidence_version` 2 is not refused: it stays with the old generation,
   nothing is handed on from it, and the converted generation gets version 2
   evidence of its own. `--publish-prepared` and `--resume-generation` read it
   before they hash a retained table. Then downloads every published member by
   its hash and byte-size pins. Each member must match its declared physical
   columns and row count.
4. Runs the family's own writer over those tables with upload skipped: the
   rollup with its builder replaced by "return the retained tables", or the
   court writer for a court family. Field policies are read at run time.
5. Refuses unless every row converts (every receipt `accepted` or
   `observed`) and a native read restores each table to exactly the retained
   ordered rows and repetitions, with the same full Arrow schema and file metadata.
   Split tables also retain exact relative member names, empty members,
   per-member row counts, and each member's full Arrow schema and metadata.
   A table-wide union cannot substitute for this member-by-member check.
6. Refuses unless a family that carries source evidence still does: the
   converted generation names the one it replaces and one evidence artifact,
   and journals again the events the next run reads back from its prior.
7. With `--publish`: checks main and the wheel again, saves
   `conversion.json`, then publishes through the standard conditional
   pointer write and reads the result back without credentials: the entry
   carries `etlReceipts`, row counts match, and the read that refused before
   now passes.

A successful conversion passes an in-memory restoration proof to publication.
It names the admitted candidate, the complete captured family entry, and every
original table checked during this invocation. For those tables, full
restoration replaces the ordinary byte-size heuristic: moving fields into
receipts can make a complete native subject much smaller. The proof requires
the exact prior entry, including its timestamp, on every pointer retry.
Partition, table membership, receipt ownership, installed policy, and immutable
byte-verification checks still run. Added tables and ordinary publications keep
the size guard. A saved receipt cannot supply the proof by itself, because a
receipt can be edited and resealed with its own digest. Prepared publication
accepts the seal's admission and restoration only when two things hold: the
operator supplies the receipt digest from the preparing run's own output, and
the seal's admission key (generation pin plus validator code, SpicyDocs code,
runtime and platform) equals the publisher's. Otherwise it repeats the full
checks. Either way the original seal and candidate bytes stay unchanged.

Conversion uses one `ConversionReadOperation` for the complete native family.
It admits the subject and receipt members once, holds their checked processing
rows, and compares them directly with each retained original. Qualification
does not write disposable restored Parquet files. The operation checks that
members remain unchanged before reuse and rejects readers after it closes.
Publication in the same process reuses the admitted bytes. Readback then
holds the public root and every manifest to the admitted candidate's bytes,
and each member to its admitted digest by one HEAD against the hash storage
computed when it took the bytes: a single-part object's MD5 ETag, or a SHA-256
checksum where storage reports one. Uploads send Content-MD5 alone, because R2
refuses it beside a SHA-256 checksum and does not implement that checksum on
UploadPart. A member with neither, which is every multipart object,
streams from the public URL through a SHA-256 instead. Readback keeps no copy
on disk and does not admit the same bytes a second time, because identical
bytes admit identically. Storage hashes cover transfer and the object a key
names, not later decay at rest. A missing or differing member fails the
readback with the family published and the rollback record intact. The
receipt's `read_back.public_bytes` records the members and bytes checked,
`verified_by` (members per hash: `sha256`, `md5`, `streamed`) and
`streamed_bytes`.

If a writer completed its immutable generation but stopped before saving
`conversion.json`, `--resume-generation` can qualify that generation without
running the writer again. Its work directory must retain
`captured-publication.v2.json`, the original inputs under `retained/`, and any
required artifacts under `build/source-evidence/`. Pass the current clean main
and installed wheel pins, the explicit family allow-list, and the historical
`--expect-producer` commit. The command verifies the historical producer,
shipped policy descriptors, native members, evidence, complete captured prior,
and every original before creating the first preparation seal. It preserves
the producer identity and records the current verifier separately. It does not
publish; use `--publish-prepared` for the subsequent checked publication.

```sh
$UV run --frozen python scripts/convert_family_to_native.py \
  --resume-generation "$RUN/bill-family/build/generations/<digest>" \
  --work "$RUN/bill-family" --allow bill-family \
  --expect-main "$MAIN" --expect-spicy-docs "$WHEEL" \
  --expect-producer <original-producer-commit> --remote <main-remote>
```

Each run writes `conversion.json` in its `--work` directory. It holds the
captured entry and the generation the run intends to publish, and it is
saved before the pointer write, so `--rollback` works even when the process
died or lost the write's response. Updates write and sync a sibling temporary
file before replacing the receipt, preserving the previous complete record
if a write is interrupted. A confirmed publication is recorded
before the public read-back. If recovery cannot read the
stored index, the command reports an unknown outcome and keeps the rollback
record; it does not claim that nothing was published.

The command refuses a family outside `--allow`, one that is already native,
and one with no subject/receipt rollup or court writer (the regulations
base tables, the derived rollups, FEC and government families). A split
table requires a subject/receipt rollup with the same declared partition
columns. The court converter still refuses split tables.

### What the checks cannot see

- Arrow value equality does not distinguish floating-point `-0.0` from `0.0`.
- On the rollup path the restored table is rebuilt from the receipt's copy
  of each input row. A mapper defect in a public column is therefore
  invisible to the comparison: it proves the next run's prior, not the
  public table. The public table is checked only by row count and by the
  writer's own validation.
- On the court path the restored file takes the retained table's types, so
  the command holds those against the declared native schema instead. A
  column that moves to receipts has no declared type to hold it against.

## One owner for each receipt dataset

The index gives each subject or checkpoint dataset one owning family
(`publication.py`, `_merge_family`). Two explicitly marked receipt-only
logs are shared: `congress_acquisition` and
`legislative_document_file_states`. Each family retains its own log rows
inside its pinned receipt member, but does not claim the shared dataset in
the index. A reader must select the family's receipt member for these logs.

All other receipt datasets still require one owner. A source checkpoint
with a policy but no declared writer remains unavailable: shared-log
support does not assign it an owner or authorize a conversion. Family
reader checks and a full retained-data rehearsal remain release gates.

## Families

A family converts only when every reader of its published table, other than
its own rollup, still works on the native shape. Two servers read each
table: the hosted one (branch `3a1d3140`, old layout) until the release, and
main's afterwards. Audit of 2026-10-05: main's code at `90dfa370`, DocSpec
and spicyengine by an independent read; the hosted build by the release
session. Server controls and saved user queries were not checked.

Safe to convert.

| Order | Family | Columns that leave the table | Outside readers and what they read | Next scheduled run |
|---|---|---|---|---|
| 1 | `courtlistener` | `parties_json`, `attorneys_json`, `firms_json`, `date_created`, `absolute_url` (`parties`, `attorneys`, `firms`, `blocked`, `date_blocked` arrive) | `build_court_docket_groups.py:141` (`cl_docket_id`, `court_id`, `docket_number`); `relationship_views/courts.py:44` and `table_joins.py:797` (`cl_docket_id`); `check_rollup_freshness.py:76` (`date_filed`). Hosted build: one view joins on `cl_docket_id`. | daily 18:30Z; passed on the converted copy, and its journal reads "Inherited pins" |
| 2 | `court-docket-groups` | `edition`, `rule_version`, `match_basis` | `table_joins.py:797` (`cl_docket_id`, `parent_cl_docket_id`). Hosted build: a plain view. | none; built by hand; its input `court_dockets` is native after step 1 |
| 3 | `cfr-sections` | `url`; `part_granule` becomes BOOLEAN | `citation_resolution.py:104` (`cfr_ref`, `package_id`, `granule_id`, `section`, and `part_granule = 'true'`, which gives the same rows on both shapes, on main and on the hosted build); `table_joins.py:345` (`title`, `part`); `check_rollup_freshness.py:74` (`last_modified`). Nothing reads `url`. | daily 20:30Z; limit raised to 60 minutes (`rollup-cfr-sections.yml` says why) |

User SQL on `cfr_sections.part_granule` keeps working with `= 'true'`;
`lower(part_granule)` and `LIKE` stop working on a BOOLEAN.

`court-docket-groups` names no parents after conversion (the conversion read
none), so `describe_table` shows no recorded inputs for it until its next
build. The replaced generation, named in the converted one's read snapshot,
still holds them.

Held.

| Family | Why | What releases it |
|---|---|---|
| `unified-agenda` | `materialize-rulemaking` downloads the table raw and requires `timetable_json`, `cfr_references_json`, `legal_authority_json` and `url` (`pipelines/materialized.py:253`, `rulemaking_dataset.py:133`), which all leave the native table. | That reader reads the native table. The same change raises the rollup's 30-minute limit: measured on loopback storage 10 to 14 minutes here and 862.9 s and 476.0 s by the reviewer, so a hosted runner at half the speed lands near it. |
| `court-opinion-pdf-extractions` | The hosted citation tool keys `court_opinion_derived_pdf` by `(opinion_id, source_sha256)`; natively the table is `opinion_body_id`, `opinion_id`, `cluster_id`, `text_content`. | Verified new hosted server, below. |
| `amendments`, `committee-meetings`, `house-communications`, `nominations`, `record-issues`, `treaties`, `members` | Shared-log ownership is implemented; the historical dry runs do not qualify the current release. | A current retained-data rehearsal and verified readers. |
| `committee-reports`, `senate-expenditures`, `native-legal-references` | Shared-log ownership is implemented; current writer declarations and outside readers still need qualification. | The same checks, with every published checkpoint declared by its source owner. |

Never put these in `--allow` yet. The command would convert each, and none
has been checked for outside readers.

- Too large for the per-row writer; they wait for the bulk writer:
  `court-citations` (102.0M rows), `court-opinions` (10.8M),
  `court-opinion-clusters` (10.1M), `roll-call-votes` (10.6M),
  `member-vote-terms` (10.6M), `federal-register` (1.0M).
- Are not on an approved list: `committee-rosters`, `press-releases`, `laws`,
  `print-citations`.

`bill-subjects` converts through the hosted workflow. `congress_bills` is
native (the `bill-family` generation carries receipts), so its scheduled
rollup can read it, and its scheduled rollup refuses until this runs once.
Its published evidence carries no `evidence_version`; converting it needs
f4dc7227 (pre-v2 evidence is left behind, nothing handed on, version 2 written)
on main.

1. Digest of the family entry, with `R2_PUBLIC_URL` set:
   `uv run --frozen python -c "import os; from scripts.native_conversion_invocation import entry_digest;
   from spicy_regs.sources import publication as p;
   print(entry_digest(p.current_index(os.environ['R2_PUBLIC_URL'])['families']['bill-subjects']))"`.
2. Dispatch `convert-native-family.yml` on main with `family=bill-subjects`,
   `expected_entry_sha256` from step 1 and `publish=false`. The 180,695 rows
   convert, restore and compare in minutes (98,482 real rows: 40 s locally,
   publication and anonymous readback included). Check `conversion.json` in the
   `native-family-bill-subjects` artifact: `tables.bill_subjects` has no rows or
   columns only on one side, and `lineage.inputs` names one evidence artifact.
3. Dispatch again with `publish=true`, `expected_bucket`, `source_run_id` of
   step 2, `expected_producer` its full commit and `expected_receipt_digest`
   from its job log's `prepared receipt digest` line. The job reads the family back
   anonymously.
4. Check the pointer: the index entry for `bill-subjects` carries `etlReceipts`
   listing `bill_subjects`, and its evidence input reads (`publication.load_evidence_journal`
   refuses anything but current completed evidence).
5. Dispatch `rollup-bill-subjects.yml` with `skip_upload=true`; it must pass
   the selection that refused every night since 2026-10-04 (on `congress_bills` until 10-06, then `bill_subjects`), then let the 21:15Z
   schedule publish.

### Bulk execution and remaining operational checks

The scheduled lobbying, bill-family, votes/terms, mapped court and regulatory
refresh paths set `SPICY_REGS_REQUIRE_BULK=true`. The reusable rollup's
`require_bulk` input defaults to false for its other callers. The setting
reaches the existing CLI process, worker threads and child processes, and keeps
each writer's existing thread and memory limits. An explicit
`bulk_limits(require_bulk=False)` does not override the runner's requirement.
Only the literal values `true` and `false` are accepted; an invalid value
refuses a requested row fallback.

This setting refuses a **whole mapped dataset or bundle** falling back to the
reference reader or writer. A dataset without a bulk mapping still logs its
row count and reason and uses its supported row path. Sparse exceptions inside
a bulk scan, such as an unusual JSON escape, still use the reference mapper for
those rows. A failed bulk operation never retries the whole dataset by row.
Local callers can use the same environment setting without changing their CLI
arguments. Initial conversions also support the existing `--require-bulk`,
`--bulk-threads` and `--bulk-memory-limit` options.

`comment-attributes` is now offered in the hosted conversion menu and accepted
by the converter's regulatory-family selection. It uses the typed regulatory
bulk writer. The preparation and publication limits are unchanged; the
full-size preparation still has to establish its runtime and disk use before
publication. The historical estimates below describe the earlier row writer.

For votes, dispatch `refresh-congress-vote-terms.yml` with `skip_upload=true`
after the integrated producer is available. Its votes and terms jobs already
free hosted toolchain space and sample available disk every 20 seconds. Each
`rollup-audit-*` artifact now retains `disk-watch.log`, including start and end
samples; the log reports the lowest **sampled** free space, not an instantaneous
peak-use guarantee. The manual votes and terms repair workflows enable the same
probe. The scheduled votes limit remains 60 minutes, and manual repair limits
remain unchanged. Record the producer revision, exit status, output sizes and
lowest sampled free space from the actual run before claiming hosted capacity.
No hosted measurement is established by this wiring change.

The vote rollups' `skip_upload` option leaves verified generations local and
does not require publication credentials; source requests still happen. The
chain's terms job reads the current published inputs, so this rehearsal does
not test a newly published votes-to-terms handoff. In contrast,
`etl-new --skip-upload --use-iceberg` still merges into the catalog: its flag
suppresses the later publication, not the catalog writes. It is not a read-only
catalog rehearsal.

The owner selected this Mac for the large court-citations job. Use a clean
integrated main and a new work directory, record free disk before starting,
and retain elapsed time and resource measurements. First check the local
corpus catalog and retained generations for the exact selected input digests.

The productive first conversion is `court-opinion-clusters`: its normal refresh
requires a native prior. Prepare the verified retained file using a member map,
then publish those same bytes with `--publish-prepared`. After refreshing the
clusters, the citations job requires their published cluster-ID ceiling to cover
its selected export. Converting the old citations publication does not refresh
its source edition and is unnecessary merely to test the bulk writer.

```sh
UV=/opt/homebrew/bin/uv
MAIN=$(git rev-parse HEAD)
WHEEL=$($UV run --frozen python -c "from importlib.metadata import version; print(version('spicy-docs'))")
COURT_WORK="$HOME/Work/spicy-stack-review-work/native-conversion-$(date +%Y%m%d-%H%M%S)/court-opinion-clusters"
COURT_INPUT_MAP=/absolute/path/to/verified-court-cluster-members.json
SPICY_REGS_REQUIRE_BULK=true R2_PUBLIC_URL=https://data.spicygov.ai \
  $UV run --frozen python scripts/convert_family_to_native.py court-opinion-clusters \
  --allow court-opinion-clusters --work "$COURT_WORK" --expect-main "$MAIN" \
  --expect-spicy-docs "$WHEEL" --remote fork --require-bulk \
  --retained-member-map "$COURT_INPUT_MAP"
PRODUCER="$MAIN"  # write it down with the preparation; main may move before publication
# Copy the "prepared receipt digest sha256:..." line the command printed into RECEIPT_DIGEST.
```

Publish the sealed receipt with the producer pin, from the clean main current
at publication time:

```sh
MAIN=$(git rev-parse HEAD)
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py \
  --publish-prepared "$COURT_WORK/conversion.json" --allow court-opinion-clusters \
  --expect-main "$MAIN" --expect-spicy-docs "$WHEEL" --expect-producer "$PRODUCER" --remote fork \
  --expect-receipt-digest "$RECEIPT_DIGEST" --env-file <file with the R2 settings> --expect-bucket "$BUCKET"
```

### Families the nightly refreshes wait on (historical audit of 2026-10-17)

A scheduled rollup refuses an input family published without ETL receipts. `etl-new`
(dockets, documents, comments and every consumer behind `refresh-consumers`) failed
on `dockets` until it was converted on 2026-10-06, and has failed on
`docket_attributes` since 2026-10-10; `document_attributes` and `comment_attributes`
are next. Each converts with the procedure for
`bill-subjects` above (entry digest, `publish=false`, `publish=true`, readback),
substituting the family; sizes are the 2026-10-17 pins and the time is the
measured rate (this repository's per-row writer, one loaded laptop: 2.1k rows/s
to convert, 0.86k/s with publication and readback; hosted court-opinions:
10.8M rows in 182 minutes to prepare, 42 to publish).

| Family | Rows / bytes | Estimate | Fits the workflow (240 prepare + 90 publish, 360 job) |
|---|---|---|---|
| `docket-attributes` | 0.28M / 5 MB | about 5 minutes | yes |
| `document-attributes` | 2.0M / 79 MB | about 40 minutes | yes |
| `comment-attributes` | 26.4M / 306 MB | 3.5 to 8.5 hours | no: not offered. It needs a bulk writer for the typed regulatory tables (`regulations_bulk` takes declared string columns only). `etl-new` stays blocked until it converts. |
| `court-opinion-clusters` | 10.1M / 3.95 GB | about 3 hours to prepare, 45 minutes to publish, by `court-opinions`' rate | borderline: the `publish=false` dispatch is the measurement, since its bytes are 9.6 times `court-opinions`' for a similar row count and the row-based estimate may be low; the runner's free disk (about 14 GB) holds the retained table, the native copy and the readback |
| `court-citations` | 102M / 1.0 GB | more than a day | no: not offered; wait for the bulk writer. Its nightly also needs `court_opinion_clusters` published first. |

The attribute families convert through the regulatory base converter
(`_regulatory_base_class`, and the retained-source-schema rule in
`regulations_receipts.processing_schema`); before 2026-10-17 only dockets and
documents did.

`lobbying-filings` is not converted by this command: its scheduled rollup converts
it in its first native run, and refuses nothing on an old shape (it reads the old
table as literal input). That conversion is per-row over every held row and does
not finish inside the job's 30-minute limit (about 82 minutes measured), so the
shape change will not arrive nightly until the bulk receipt writer lands. Audit of its readers on 2026-10-17, main at `c0e01f9e` in
spicy-regs and each sibling repository's `main`. The first native publication
removes `url`, `lobbying_activities_json` and `government_entities_json` from
`lobbying_filings` and `government_entities_json` from `lobbying_activities`
(`url` moves to the receipt; the JSON arrays become `lobbying_activities` and
`government_entities`). It retypes `filing_year`, `activity_index` and
`lobbyist_index` to BIGINT, `dt_posted` to TIMESTAMP WITH TIME ZONE, `income` and
`expenses` to DECIMAL(38,6), and `new` to BOOLEAN. The native filings table is
1.03 times the old file's size on 27,989 real rows, so the 0.5 shrink guard passes.

| Reader | Reads | Disposition |
|---|---|---|
| `relationship_views/lobbying_native.py`, `artifacts_topics.py` | `government_entities` (native) or `government_entities_json` (old) | Fine: the installer binds whichever shape the table has (`test_relationship_view_variants.py`). The `3a1d3140` hosted build has only the old view, so its `lobbying_contacted_entities*` views go unavailable until the server switch; the tables stay queryable. |
| `citation_sources.py`, `held_citations.py`, `mcp_server.py` citation tool | `lobbying_activities.description`, keyed `(filing_uuid, activity_index)` | Fine: each key is cast to the column's type and compared again as text (`src/spicy_regs/transforms/held_citations.py:103-104`). |
| `scripts/check_rollup_freshness.py` | `max(TRY_CAST(dt_posted AS TIMESTAMP))` | Fine on a TIMESTAMPTZ on UTC runners (the cast renders in the session zone). |
| `table_joins.py`, `join_audit.json`, `join_measurements.json` | `filing_uuid`; `(filing_uuid, activity_index)` | Fine: both sides retype together. |
| MCP `describe_table`/`list_sources`, `data_dictionary`, `docs/tables`, `table_metadata.json` | the dictionary already declares the native columns | Fine, and better: `schema_matches_declared` becomes true; `test_legacy_lobbying_descriptions.py` keeps the old meanings readable. |
| `publish-explorer-metadata.yml`, the SpicyGov explorer | `explorer-metadata.v1.json`, fetched at run time and republished after this rollup | Fine. `spicygov/lib/data/table_joins.json` is read only by one test, as a fixture. |
| Saved user SQL | the removed columns, `filing_year = '2025'` style text compares | Not checked, as for every family; `url` is `read_receipt_fields`. |
| `spicygov/content/coverage-definitions/regulation.json` | an exact `schema` per table; `build-coverage-maps.py` refuses drift ("Published schema needs coverage review") | Needs a change before the next coverage build: replace the three `schema` arrays with the native ones. The dimensions already cast to VARCHAR, so `dt_posted` and `filing_year` keep working. `content/coverage-reviews` pin generation `8374129a`; the new generation needs its review. |
| `spicysearch/vendor`, RefSpec evidence, `spicy-docs`, `spicyengine`, `spicyregs-web`, DocSpec, BillTrax | a vendored dictionary copy and research files; no code reads the tables | Fine. |

A Congress.gov or GovInfo receipt also records the local path of the file it
was converted from. Run those conversions from a neutral `--work` path.

## Hosted-server precondition

Deploy and verify the new hosted server before converting citation families.
The server switch keeps the selected data unchanged and runs independently.
There is no conversion release window. Until each parent table converts,
the new citation tool reports that its native input is unavailable.

`court-opinion-pdf-extractions` waits for this verification because the
hosted reader at `3a1d3140` keys its body by `(opinion_id, source_sha256)`;
the new reader keys by `opinion_body_id`. `print-citations`,
`committee-meetings`, `house-communications` and `members` also wait for the
new hosted reader. The shared-log branch qualifies those families separately.

The historical audit permits `courtlistener`, dependent
`court-docket-groups`, and independent `cfr-sections` before the server from
merged converter code and an approved run package. The coordinated execution
of 2026-10-05 orders these canaries after the first verified switch as well.

Rehearse each conversion on a pinned loopback copy, build the server's own
connection over it, and call the citation tool for each supported kind.
Keep rollback serving and data compatible together: restoring old-shape
citation tables makes the new tool refuse their input; restoring the old
server while its dependent tables remain native breaks its readers. Verify
the chosen server/data pair before either rollback.

## Before a production run

- The owner has approved the concrete merged-head run package and the list.
- Tell the server release session, which replays the server's controls
  before and after: a converted table changes shape for readers (its field
  policy is `src/spicy_regs/etl_policies/<table>.json`).
- Check out main in a clean worktree and sync it with the pinned lock. Take
  the commit and the wheel version from that checkout; the command re-checks
  both against the remote immediately before it publishes.
- No run of the family's own rollup is in flight (`gh run list --workflow
  rollup-<name>.yml`). A concurrent publication is refused, not overwritten.
- Record the prior objects, restore proof and rollback recovery date for the actual run. `plan-generation-retention.yml` deletes on
  its own every Sunday at 05:45Z and keeps each family's last three
  generations. The conversion is one generation and each scheduled run adds
  one. After two successful publications following conversion, the captured
  generation falls outside those three. The next weekly retention may delete
  its members unless docs, DocSpec, rulemaking inputs, managed parents, or the
  grace period still protect it. Recalculate the earliest eligible cleanup
  from the actual conversion date, successful subsequent publications, fresh
  retention plan, and next retention run. To keep a rollback open
  longer, disable that workflow's schedule first (`gh workflow disable
  plan-generation-retention.yml`) and enable it again once the rollback is
  no longer wanted.

No concurrency group is involved: these conversions write only immutable
generation objects and the index pointer.

## Commands

```sh
cd <clean worktree at main>
UV=/opt/homebrew/bin/uv   # the lock needs uv 0.11 or later; a bare `uv` may be an older shim that cannot read it
MAIN=$(git rev-parse HEAD)
WHEEL=$($UV run --frozen python -c "from importlib.metadata import version; print(version('spicy-docs'))")
ALLOW=courtlistener,court-docket-groups,cfr-sections
RUN=~/Work/corpora/native-conversion-<date>
BUCKET=<the production bucket's name, typed by hand>

# Dry: reads production anonymously, uploads nothing.
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py courtlistener \
  --allow $ALLOW --work $RUN/courtlistener-dry --expect-main $MAIN --expect-spicy-docs $WHEEL --remote fork

# Publish.
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py courtlistener \
  --allow $ALLOW --work $RUN/courtlistener --expect-main $MAIN --expect-spicy-docs $WHEEL --remote fork \
  --env-file <file with the R2 settings> --expect-bucket $BUCKET --publish

# Roll back.
$UV run --frozen python scripts/convert_family_to_native.py --rollback $RUN/courtlistener/conversion.json \
  --env-file <file with the R2 settings> --expect-bucket $BUCKET
```

One family per invocation, in the order above. `--remote` names the git
remote whose `main` is checked (default `origin`).

### Prepare, qualify, and publish the same build

A successful official dry run now seals its `conversion.json`. Run it from
clean merged main and its pinned environment:

```sh
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py bill-family \
  --allow bill-family --work "$RUN/bill-family" --expect-main "$MAIN" --expect-spicy-docs "$WHEEL" --remote fork
```

Before publishing, the release package must record that receipt's
`prepared.generationPin`, the full physical-key qualification, the exact
deployed reader image tested against this generation, and the coverage
preview's exact subject, receipt and supporting-table pins. Keep each proof
file and its SHA-256 beside the run package. Reports for another generation
do not qualify this one. These source and consumer checks remain separate
from the converter's receipt admission and restoration checks.

Publish those same bytes once the package is qualified:

```sh
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py \
  --publish-prepared "$RUN/bill-family/conversion.json" --allow bill-family \
  --expect-main "$MAIN" --expect-spicy-docs "$WHEEL" --expect-producer "$PRODUCER" --remote fork \
  --expect-receipt-digest "$RECEIPT_DIGEST" --env-file <file with the R2 settings> --expect-bucket "$BUCKET"
```

`MAIN` names the current clean publisher; `PRODUCER` is the full commit in
that sealed receipt's `source.checkout` and `source.main`. `RECEIPT_DIGEST`
is the `prepared receipt digest` line the preparation printed (its terminal,
log or job output). Never copy it from `conversion.json`: a digest read from
the receipt it covers checks nothing. A version-1 seal (prepared before
2026-10-30) printed no digest; take it from that preparation's archived log if
one exists, otherwise from the receipt's `prepared.receiptDigest`. That is
acceptable only because a version-1 seal always takes the full re-check
below, so an edited receipt still meets every row check. For an earlier
producer, the command verifies its commit and ancestry in the same repository,
hashes historical Python Git blobs using the generation's path ordering,
and compares both producer and verifier implementation identities. Its
historical vendored wheel must match its lock. Then one of two rules binds
the runtime. A seal that recorded its loaded code (`prepared.closure`: the
bytes of every SpicyRegs, SpicyDocs and Rulespec module the preparing process
loaded, every data file in those packages, the versions of the third-party
distributions it loaded, Python and the platform) publishes from a later main
when all of that is byte-identical in the publisher; `uv.lock`, the SpicyDocs
wheel and its version may have moved. Even a comment-only change to a loaded
module refuses. A seal without it (older seals, resumed or checkpointed
producers, whose writer ran in another process) requires identical historical
and current `uv.lock` bytes and the same installed wheel. A publish rebound by
loaded code also relies on the seal's admission instead of repeating it,
because the validators it ran are among the identical bytes;
`publish_attempt.verification.rebound_by_loaded_code` records this. Missing
history, a different locked runtime or loaded code, or an unproved producer
refuses. Every operator `--publish-prepared` passes
`--expect-producer` with that receipt's `source.checkout`, even when main has
not moved: peers land on main while a publication is prepared and qualified,
and the pin is what keeps a sealed receipt publishable from a later main. On
an unmoved main the pin costs nothing (the producer is its own ancestor and
the lock is identical). Only same-revision API callers, such as tests, omit it.

Prepared publication never runs the source writer. It always re-hashes every
generation member and retained original (decoding the originals) and reruns
the structural, membership, policy, lineage and handed-on-journal checks. Row
admission (bundle validation, key-index check) and original restoration run
again only when the seal's admission key differs from the publisher's, for a
version-1 seal, for `fec-query` with staged observations (its parent's
admission is not in the seal), or with `--full-verification`; the receipt's
`publish_attempt.verification` records which mode ran and how long it took.
After any commit to `src/spicy_regs`, or any change to the SpicyDocs or
Rulespec code bytes, the key differs; a publish from a moved main then skips
row admission only when it is rebound by loaded code (above). Such commits are
frequent: about a third of 3-hour windows on fork main from 2026-09-30 to
028736f3 cross a `src/spicy_regs` commit (653 commits; 34.2% of windows
started every 10 minutes, by commit time). To get the skip for a Mac
preparation without a rebind, publish from a clean worktree at `PRODUCER`
itself. On 2026-10-30 the comment-attributes publish spent 25.0 minutes
re-checking locally and 24.1 minutes downloading and re-admitting the public
copy. The digest read-back above replaces the second on every publish (an
estimate of about 13.6 minutes of re-admission and a 4.5 GB copy); the sealed
path skips the first only when no validator code moved or a rebind applies.
Replace these estimates with `publish_attempt.verification.seconds` from the
next approved publish. Prepared publication refuses an operator digest that
differs from the seal (an edited and resealed receipt), an altered or
incomplete receipt, unproved producer or changed publisher/runtime, missing
or altered members,
different restoration results, or a changed captured family entry.
The generation bytes, producer source, and original prepared seal stay
unchanged. After qualification, the publication attempt separately records
the actual publisher source, runtime and implementation identity. Main and
its installed locked wheel are rechecked immediately before that attempt.
Original-row restoration does not prove every mapped native field, so the
release package's native-field and installed reader qualifications still
apply; a known relevant mapper defect requires a fresh preparation.
Timestamp-only changes to that entry also refuse, including during a
conditional pointer retry. Unrelated family updates remain intact.

A preparation seals the validator identity it started with. If the checkout,
its uncommitted or untracked files, the installed SpicyDocs, or the SpicyRegs,
SpicyDocs or Rulespec code bytes change while it runs, it refuses to seal and
saves the receipt unsealed; finish it with `--resume-generation` from a
clean checkout of current main plus `--expect-producer <the preparation's
checkout>` (the refusal names that commit). Hosted main may move during a
preparation.

Only a completed official dry run is eligible. Partial writer outputs,
old unsealed receipts, rehearsal reports, and already attempted or published
receipts are refused. A failed or interrupted publication retains its
rollback record. Reconcile the stored pointer read-only and inspect the
rollback record before any further write; the recovery procedure below
explains the available rollback. Do not remove the attempt journal or reseal
it to force another prepared publication. No report's success flag bypasses
verification.

Every exit that is not a success prints two lines. `REFUSED:` (exit 1) or
`FAILED:` (exit 2, or 130 when interrupted) says why. `STATE:` says what the
bucket holds: nothing was published; or the family is published, with the
rollback command; or a publish was attempted and its result is not known,
with the command that finds out. A publish whose response was lost is not a
refusal: the command rereads the stored index, and when it names this
conversion's generation it finishes the read-back and exits 0.

When prepared publication stops before saving a new attempt, `STATE:` says
that this invocation made no new attempt and calls for read-only
reconciliation of any previous outcome using the receipt. This also applies
when changed code, a changed wheel, or conflicting settings prevent the
receipt from being opened; it makes no claim about the current pointer.

Settings, by name: a dry run needs only `R2_PUBLIC_URL`. Publishing and
rolling back need `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`
and `R2_BUCKET_NAME`; no bucket is assumed. `CLOUDFLARE_API_TOKEN` with
`CLOUDFLARE_ZONE_ID` purges the two index URLs and is optional. Nothing is
loaded from a `.env` implicitly; `--env-file` names one. A variable already
set in the shell that the file contradicts is refused, by name.

### Hosted execution

`convert-native-family.yml` runs the same maintained converter on a hosted
runner, one named family per manual dispatch. It supports the bulk regulatory,
court and votes families, `bill-family`, `bill-subjects`, `docket-attributes`,
`document-attributes`, `comment-attributes` and `court-opinion-clusters`. Their owners still coordinate
reader compatibility and concurrent ordinary writers.

Bind `expected_entry_sha256` to the complete observed family entry with
`scripts.native_conversion_invocation.entry_digest(entry)`. This digest
includes timestamps, schemas and every table member. The workflow checks it
before conversion, then checks the prepared or downloaded artifact's captured
entry against the same digest before publication. The first check does not
lock the publication index; a change before the converter captures it makes
the later check fail, retaining the candidate without publishing it.
The workflow explicitly checks out current main with complete Git history,
even when the manual dispatch selects a workflow repair branch. The invocation's
`revision` identifies the workflow revision; `producer-revision.txt` and the
conversion receipt identify the retained producer revision. `publisher-revision.txt`
identifies the current publisher. The converter receives the publisher revision
as `--expect-main` and the full retained revision as `--expect-producer`.
It separately checks clean current main, the installed locked wheel, complete processing restoration,
source evidence and the exact stored predecessor before publication.

Leave `publish` false to prepare without publication credentials. The run
retains the sealed family under the `native-family-<family>` artifact, and its
invocation, logs and observed process resources in a separate evidence
artifact. Preparation has a 240-minute command timeout; publication has a
90-minute timeout inside the 360-minute job, leaving time to retain failed
outputs. The former 45-minute preparation bound interrupted normal work in
[court](https://github.com/mikewolfd/spicy-regs/actions/runs/37448262153),
[votes](https://github.com/mikewolfd/spicy-regs/actions/runs/37448264683),
[vote terms](https://github.com/mikewolfd/spicy-regs/actions/runs/37448267222)
and [bills](https://github.com/mikewolfd/spicy-regs/actions/runs/37448753043).
Publication also needs time for complete restoration and anonymous readback.
The [later bill run](https://github.com/mikewolfd/spicy-regs/actions/runs/37455256044)
finished mapping and combining its legislative receipts after about 165 minutes,
then reached its 180-minute limit during validation. Decimal fields forced the
entire bundle onto the reference row validator. The corrected fresh run requires
the proven decimal encoder and maintained batch restoration; additional time
alone does not qualify the interrupted bytes.
The runner, disk floor, thread limits and acceptance checks remain unchanged.
Hosted limits do not establish that a larger family fits the runner.

After checks against the actual reader, dispatch with `publish=true`, the
explicit `expected_bucket`, `source_run_id` naming that preparation run,
`expected_producer` naming its full producer commit, and
`expected_receipt_digest` copied from that run's job log or step summary
(the `prepared receipt digest` line). Never take it from an uploaded artifact,
`prepare.log` in the evidence bundle included: the downloaded artifact cannot
supply its own digest. A single dispatch that prepares and
publishes hands the digest from the prepare step to the publish step; that
guards against a stale or edited receipt, not against a compromised job.
The workflow downloads those same bytes and calls `--publish-prepared`;
it does not rerun the writer. The runner records whether retained output
exists before downloading the missing official archive. It retains the original
layout, including the sealed absolute generation path; a native-only subset
cannot replace the complete publish directory. Never rewrite receipt paths or
reseal a copied receipt. Current clean main, the same locked runtime, retained
producer identities, and complete prior entry must pass the checks above.
Alternatively, a cleared single dispatch can prepare and publish immediately by leaving `source_run_id` empty. Always retain a failed
attempt and reconcile its pointer read-only; never clear its journal, reseal
it, or dispatch a blind retry. Ordinary producer acceptance remains a later
check after successful publication and anonymous readback.

The ordinary Federal Register, docket-link, narrow vote/term repair and
rulemaking workflows also declare finite limits that allow complete
restoration of native priors.
These workflows explicitly check out main, so a workflow timing repair can
run before its merge without changing the producer. The shared rollup's
optional `producer_ref` leaves other callers at their original revision.
Its invocation records the actual `code_revision` and the separate
`workflow_revision`. See `rollup-federal-register.yml`,
`rollup-fr-docket-links.yml`, `rollup-roll-call-votes.yml`,
`rollup-member-vote-terms.yml` and `materialize-rulemaking.yml` for their
limits. The combined scheduled vote/member/term refresh stays unchanged;
the narrow repairs do not rebuild members. Source schedules, publication
defaults and command arguments stay the same.

## Verify

- The command's last line is "published and read back", and
  `conversion.json` shows `rows_only_in_restored` and `rows_only_in_retained`
  both 0 for every table, no `type_changes` or `metadata_changes`, and
  `read_back.public_bytes.members` equal to the generation's member count.
- `conversion.json` records the bucket and endpoint host under `target`, and
  under `lineage` the inputs the converted generation names.
- `publication.v2.json` carries `etlReceipts` for the family.
- The family's next scheduled run succeeds and moves the generation. A
  failure there leaves the converted generation in place.

## Roll back

`--rollback` reads the stored index and decides. If the family names this
conversion's generation, it rewrites the entry to the captured one. If it
still names the captured generation, there is nothing to do and it says so.
If it names anything else, a scheduled run or another writer has published
since: the rollback refuses, because it would discard that run's rows, and
`--discard-newer` says to do it anyway. `--rollback` takes no conversion
arguments.

The old-shape objects are still stored, and the command checks that first;
see "Before a production run" for when retention removes them. The converted
objects stay, unreferenced. After a rollback the family refuses its
scheduled runs again, as before the conversion.

A family whose receipts are narrower than its rows (the FEC identity tables)
restores from the subject plus the receipt, never the receipt alone. None is
in this list.

## The regulations unit: pending the bulk writer

`dockets`, `documents`, the three attributes tables, the dockets and
comments catalogs and the comments mirror convert together, later, through
the bulk writer. The ETL restores all five tables every batch, so the per-row
writer cannot serve them (measured 2026-10-04: one batch's priming alone
would run 9 to 13 hours against a 6-hour limit). The public files stay as
published on 2026-10-03 until then.

The dry run fixed the order, which the bulk conversion keeps:

1. Tell spicy-stack-d0, take `comments-catalog-write`, create the native
   tables, and disable compaction on them and confirm it, as
   `fill-comment-fields.yml` does. `ensure_native` starts a table empty, so
   it must be populated before any scheduled writer reaches it.
2. Convert `dockets`, then `comments` one agency per commit, reading the old
   namespace at one pinned snapshot. A stopped run resumes at the next
   agency. The old namespace is never written.
3. Verify counts for each agency and exact rows in both directions, then
   re-enable compaction.
4. Convert the two old retry files (`failed_keys.parquet`,
   `pending_comment_text.parquet`) through `finish_checkpoints`.
5. Publish the five families last. That is what unblocks the ETL.
6. Let one sweep run, then the refresh.

Rollbacks rehearsed: an agency's rows through `replace_native(...,
delete_scope=True)`; the whole catalog requires an explicit reviewed recovery plan preserving native tables;
each family by its index entry.

### Materialized rulemaking snapshots

`materialized/rulemaking/latest.json` selects a separate snapshot. Converting a
flat `proceedings` family does not migrate that snapshot. The scheduled pipeline
requires native proceedings and their admitted receipts; an old snapshot
without receipts refuses before processing. Preserve proceedings IDs through
explicit migration rather than bootstrap an existing snapshot.

Prepare the migration with `scripts/prepare_rulemaking_snapshot_native.py`.
Pass retained `--pointer`, `--manifest`, and `--sources`, their exact
`--expected-pointer-sha256` and `--expected-manifest-sha256`, a fresh
`--destination`, `--generation-id`, and `--asserted-at`. The command checks
snapshot membership, each artifact's path, bytes, hash, and rows; it copies the
complete prior snapshot into `retained/`, classifies its existing outputs, and
checks restored rows in order, schema, and file metadata. It writes a candidate
native manifest, pointer, and `migration-checks.json` locally. It performs no
upload or pointer replacement.

Before production migration, qualify the complete retained snapshot and the
following scheduled materialization on merged readers and writers. Preserve the
old pointer and every old snapshot object, verify restore, and include the
actual retention deadline and compatible server rollback in the same concrete
run approval package. Scheduled materialization remains held until the native
snapshot is admitted and the verified consumers read it.

Current rulemaking publication retains the full prior pointer and manifest
captured during priming. Before uploading the candidate, it checks both against
canonical storage. After uploading validated artifacts and the manifest, it
checks the same prior again and replaces `latest.json` conditionally on the
captured pointer's ETag. A changed pointer or manifest refuses publication;
an explicit bootstrap requires an absent canonical pointer. If the pointer
write loses its response, reconcile the canonical pointer before retrying.
The publisher does not retry or roll back that uncertain write automatically.
