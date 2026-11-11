# Explorer metadata

`explorer-metadata.v1.json` is a public, versioned companion to
`publication.v2.json` and the separate rulemaking and comments publications at
the configured data host. It lets an explorer pick up
new descriptions, source attribution and declared joins without rebuilding its
website. The publication index remains the authority for which tables exist,
their current files, columns, row counts and publication times.

## Contents and ownership

The bundle contains `format: spicy-regs-explorer-metadata`, `version: 1`,
`generatedAt`, `sourceRevision`, `publication`, `tables` and `joins`.
`generatedAt` is the metadata build time, not a source observation date.
`publication.sha256` hashes the canonical JSON of the index read for the build;
`publication.families` retains each family's artifact digest.

Each table retains the current data dictionary's label, summary, coverage,
quality notes and column descriptions. It also records:

- `family` and `publicationSchema`: the publication family and exact ordered
  `[column name, type]` pairs this metadata was checked against.
- `sources`: named original publishers, source kinds and notes. Saved historical
  scorecard publisher links may use HTTP; data access still requires HTTPS.
- `inputs`: documented input table IDs. An input outside the public index stays
  listed; consumers must label it unavailable instead of making a broken link.
- `transformation`, `modelGenerated` and optional `sourceEvidence`: how derived
  records were produced and where that statement was checked. Inputs describe
  the transformation, not a claim that its latest run consumed today's tables.
- `metadataStatus` and `sourceStatus`: separate known/unknown states. A new table
  remains discoverable even when its description or origin is undocumented.
- Optional `emptyReason` and `joinAudit`: documented empty-output reasons and
  relationship dispositions. Counts never establish completeness.
- Optional `category`: the table's role. `processing_evidence` and `diagnostics`
  appear under Collection status, `source_evidence` under Source evidence, and
  `reference_data` under Reference data. These supporting tables remain
  inspectable through explicit links and declared relationships, outside the
  ordinary dataset list and shared-field browsing. `source_data` contains
  original provider records and remains browsable alongside `query_data`.
  `fec_receipts` contains financial records, not processing receipts.
  `kind` continues to describe coverage separately.
- Optional `row_unit`: the plural count unit, such as `checkpoints`,
  `response fields` or `selection decisions`. Supporting tables default to
  `rows` when no unit is supplied; their row counts do not establish source
  completeness.

`data_dictionary/descriptions.yaml` owns curated table descriptions, generated
into `src/spicy_regs/table_metadata.json`. Still-published processing tables use
that same curated dictionary when absent from the subject-only generated file.
`fec_query_catalog.py` owns FEC table roles; both generated metadata and the
publication fallback use it, including tables outside the installed schema set.
`explorer_sources.json` owns source attribution and derived inputs.
Source mappings are explicit: a shared column name never creates a relationship.
Scorecard publisher names and URLs are read from the current published
`scorecard_publishers` table, with size and digest checks. This is the only table
whose record bodies the metadata publisher reads.

The publisher also resolves `materialized/rulemaking/latest.json` and
`comments-publication.json` through the existing publication readers. Only
public files they name enter the bundle; main-index tables take precedence.
`extra_tables` records each separate file's family, exact schema, normalized
file descriptor and `publicationIdentity`. The identity is SHA-256 of the
sorted-key compact UTF-8 descriptor JSON, including the snapshot ID or mutable
file ETag. It does not change the main index's hash or family digests.

For a new separate file identity, bounded range requests read its Parquet
footer, checking the advertised byte size, ETag and row count. They inspect
metadata rather than scan records or independently recompute the file checksum.
The previous validated bundle supplies reusable schemas for unchanged file
identities. Mutable comments still require a matching ETag and size each run.

All table descriptions, including scorecards, come from the primary dictionary.
All relationships are authored in `table_joins.py` and generated into
`table_joins.json`. Both the explorer and MCP read this one generated registry.
There is no supplemental record-join registry. Shared-field browsing in the
website applies reviewed category filters; it does not claim that two records
identify the same entity. Detailed MCP
descriptions include full measurements on outgoing joins. Incoming joins retain
their keys, reasons and baseline counts, and point to the child description for
full measurement evidence so heavily connected tables stay within reply limits.
MCP groups incoming links under `incoming_parent` and, when identical for every
link, `incoming_parent_columns`. Otherwise each link keeps its own complete
`parent_columns`. An absent incoming reason means empty; an absent `floor_pct`
means no measured floor. Canonical JSON and public explorer metadata retain the
full per-link records. CI runs a focused catalog-size check alongside the full test partitions.

Every declared join uses the same checks. Pull requests measure added or changed
joins against published data; the scheduled check measures the whole registry.
CI checks complete composite keys, declared parent cardinality, and resolution
against each join's measured minimum. A `scope` join may have missing parent
keys for its documented reason; a measured rate does not establish that an
unexplained missing record is correct. Empty joins have no measured minimum;
newly populated keys require a baseline before the check passes. Native-key
connections marked `unmeasured` retain that uncertainty and do not borrow their
previous processing-key baseline. Existing lag
allowances remain in effect for independently published tables.

`join_measurements.json` packages full-input measurements and their source
pointers. The [join audit receipt](evidence/explorer-joins-2026-10-03.json) and
[scorecard navigation receipt](evidence/scorecard-navigation-2026-10-03.json)
retain the checked immutable inputs. The
[scorecard source receipt](evidence/scorecard-source-joins-2026-10-04.json)
establishes baselines for 25 populated source joins and records five with no
non-null keys. `join_audit.json` explains tables that
remain standalone or need special handling; it does not define executable joins.
The [column navigation receipt](evidence/column-navigation-2026-10-06.json)
adds full-key checks for 13 populated scalar links. The comments-to-dockets link
has 28 missing docket keys out of 60,296 and remains an explicit `scope` join.

Before publishing, the builder validates every declared join's table names,
columns, complete composite-key length, resolution kind and declared
cardinality. Measurements establish data cardinality; this structural check
does not invent it. A previously documented key missing from the current files
pauses that connection with the exact missing fields; unrelated connections and
descriptions still refresh. Malformed or never-documented keys fail validation.
Valid joins with an unpublished endpoint are retained in
`omittedJoins` with an explanation, and are excluded from actionable navigation.

## Main-field navigation availability

Each table may expose `recordIdentity`, the complete declared main row key from
its dictionary or subject policy. `uniqueness: unknown` means this declaration
has not proved that the selected target population is unique. Receipt-only
policies do not supply main record identities.

Navigation targets expose `requiredMainFields`, `requiredElementFields` and
`requiredTargetFields`, with each path marked `published`, `runtime_checked` or
`missing`. Native nested fields are checked against the selected STRUCT schema.
JSON paths are checked by the shared key recipes for each occurrence. Missing
row context, nested keys or target columns disable only the affected target.
`receiptFields` is always `[]`: it named what the retired receipt lookup read,
and stays only because the deployed site refuses a recipe without it (ss-v5my
makes it optional there; the key can go after that). Unpublished sources and targets retain explicit dispositions.

`directions.forward` and `.reverse` describe each route's availability,
measurement scope and lookup capability. Unknown measurements stay unknown;
a declared cardinality or a historical reference-only proof does not establish
current complete-key uniqueness. Available ordinary routes use exact scans.
`completeKey` only says the destination columns form its declared main identity;
opening one record still requires an exact current lookup with one result.
`retiredJoins` accounts for processing relationships excluded from operational
navigation, alongside `omittedJoins` for currently unavailable scalar routes.

## Refresh and deployment

The `Publish explorer metadata` workflow runs at minute 7 of each hour and on
manual dispatch. The hourly run catches hosted and workstation publications and
new `main` revisions alike, so a new data workflow needs no listing and no
website change; GitHub may delay scheduled runs. Every run first requires
successful push CI for the exact current `main` revision and checks out that
verified revision. Pending or failed CI leaves existing metadata in place until
a later run finds it passed. Metadata failures do not fail a data
publication or replace the previous valid metadata object.

The workflow uses the existing `R2_*` secrets and the existing public URL/domain
configuration. It reads the current index and separate publication controls
directly from R2 and writes only
`explorer-metadata.v1.json`; it never changes the index, Parquet objects or MCP
container. The write is one complete JSON object, followed by an R2 readback.
It uses a 60-second cache lifetime and the existing best-effort Cloudflare URL
purge. Unchanged inputs leave `generatedAt` unchanged.

The bundle's `inputs` records the SHA-256 of every mutable storage object the
publisher read (the index, the separate publication controls and the
measurement companion; absent ones as null). Before installing anything, every
trigger except a manual dispatch runs `scripts/explorer_metadata_unchanged.py`:
it reads the live metadata and each recorded input from R2, and when
`sourceRevision` equals the verified `main` revision and every input digest
matches, it logs `Explorer metadata unchanged: ...` and the job ends. Any
difference, a missing or older record, an unreadable object or a failure of the
check itself runs the full validate-and-publish path. The decision is
`explorer_inputs.unchanged` (standard library only).

Read-only local validation:

```sh
uv run --frozen python scripts/publish_explorer_metadata.py \
  --base-url https://data.example.org --output output/explorer-metadata.v1.json
```

The default base URL follows `SPICY_REGS_R2_URL`, then `R2_PUBLIC_URL`, then
`SPICYREGS_DOMAIN` and its upstream fallback, as the rest of the project does.
`--index local-publication.v2.json` supplies a local index for validation; the
publisher lookup still reads the immutable publisher file that index names.
To write using configured credentials:

```sh
uv run --frozen python scripts/publish_explorer_metadata.py --publish
```

## Consumer safety

Fetch the publication index and metadata independently. Always show current
tables from the index, including unknown ones. Compare `publicationSchema`
before using descriptions or navigation for each table. For separate tables,
also match the current file descriptor and `publicationIdentity` before using
the verified footer schema. Reject joins unless
both endpoint schemas and all key columns match. A newer family digest with an
unchanged schema can keep its metadata, but must display its older-publication
status separately. A failed or invalid bundle must show an explicit unavailable
state while leaving the data browser usable. Do not silently substitute a
bundled old join dictionary.

Coverage text is documented scope, not an inferred date range. A missing or
invalid table publication date stays unavailable; it must not be replaced by
`generatedAt`, the source modification date or a backfill time.

## Reusing navigation measurements

`scripts/measure_source_navigation.py` measures named routes from the canonical
registry using an explicit saved publication and a JSON map of its member paths
to retained local files. It reads main data only and writes derived cache files
and a measurement receipt. It does not refresh data or alter join baselines.

Every member must pass SHA-256, byte-size, footer-row and exact-schema checks;
member rows must sum to the selected table population. Unchanged local file
signatures reuse prior byte admission while every footer is still checked.
A changed member rebuilds that member's projected keys and guarded occurrences.
Unchanged member projections remain reusable. Aggregate proofs bind every
member, schema, generation, recipe and relevant implementation; changed rules
reuse unaffected target populations. Partial or damaged cache outputs are misses.

Counts distinguish occurrences, repeated references, physical source rows,
qualified distinct source records and target-key multiplicity. Missing, null,
duplicate or unsupported source identities prevent distinct-record claims.
Complete target-key populations qualify uniqueness; reference-only observations
do not. Native scalar joins refuse text/numeric coercion.

Each refresh reads the optional `navigation-measurements.v1.json` companion
from the configured data host; publishing reads it directly from R2. An owner
release job uploads that proof object separately. The metadata publisher still
writes only `explorer-metadata.v1.json`. `--measurements <receipt.json>` overrides
the companion with a local receipt. A missing companion supplies no measurements;
malformed or oversized input, authentication errors and transport failures stop
the refresh before it writes metadata.

The publisher attaches counts only when each proof reconciles and matches the
current complete source and target bindings and unchanged recipe. Separate publications retain their
exact file identities, including mutable ETags. Missing, stale, conflicting or
partial measurements leave each direction unknown. Public lookup capabilities
remain exact scans; measurement caches are neither browser row locators nor
receipt indexes. The scheduled publisher revalidates saved proofs against current
code and publication details; it does not start corpus scans or rewrite proofs.

### Affected routes and a complete census

The runner reads the maintained `joins_record()` declarations directly. Select
named `--route` values, or repeat `--affected-table` to select both outgoing and
incoming routes for changed tables. Use `--all` for a census of every canonical
route target. These selections never replace a main table with a cheaper index
or a receipt table.

```sh
PYTHONPATH=src python scripts/measure_source_navigation.py \
  --publication selected-publication.json --members held-members.json \
  --extra-tables selected-extra-tables.json \
  --cache .navigation-measurements --output navigation-measurements.json \
  --affected-table nominations --affected-table house_communications
```

Omit `--extra-tables` when only generation publications are involved. The member
map associates each exact selected public member path with a held local file.
For separate publications, supply the complete descriptor, its exact
`publicationIdentity`, and `publicationSchema`; the runner does not infer these
from filenames or a prior key-column projection.

The output retains the publisher's `results` list and adds a `census` entry for
every declared route target. Status is `complete`, `unavailable`,
`not_requested`, or `not_applicable` for an external URL. Unavailable entries
retain current source/target bindings, the recipe fingerprint, the failure
stage and reason. Their counts stay null; a missing file is not zero references.
Exit status 2 means at least one requested table route is unavailable. Keep
that receipt for the release review rather than changing a baseline to pass.

The default total input budget is 256 MiB per route across all selected source
and target members. It is checked before opening data files, including when a
table is split into small members. `--max-route-input-bytes` explicitly changes
that total budget. Existing per-member input and projected-column budgets still
apply. An unchanged larger historical proof remains historical evidence; this
runner does not bypass its current admission budget to promote it.

Keep the derived cache between owner runs. At unchanged member and rule pins,
the cache performs no projection, occurrence expansion or result aggregation.
A changed member rebuilds its own keys; a changed guard rebuilds the affected
recipe. The runner adds no prior-state cache and never downloads source files.
The receipt's per-route `work` and aggregate `work` show executed and reused
stages.

CI can run `tests/test_navigation_measurement_runner.py` with fixture members
and read-only repository permissions. An owner release job can run the command
against already-staged pinned members, retain the census as an artifact, and
upload the receipt as `navigation-measurements.v1.json`, or pass it to
`publish_explorer_metadata.py --measurements` for that run. Do not add a
scheduled full-corpus download. The existing exact-main schema gates remain
required before metadata publication; production website and MCP checks remain
separate release steps.
