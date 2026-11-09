<div align="center">

<img src="assets/icon.png" alt="Spicy Regs" width="96" height="96">

# Spicy Regs

**An open, queryable mirror of U.S. federal regulatory data — and the pipeline that builds it.**

<a href="https://www.civictechdc.org/">
  <img src="assets/civictechdc-logo.png" alt="Civic Tech DC" width="22" height="22" align="top">
</a>
&nbsp;A <a href="https://www.civictechdc.org/"><b>Civic Tech DC</b></a> project

[![CI](https://github.com/civictechdc/spicy-regs/actions/workflows/ci.yml/badge.svg)](https://github.com/civictechdc/spicy-regs/actions/workflows/ci.yml)
[![Integration](https://github.com/civictechdc/spicy-regs/actions/workflows/integration.yml/badge.svg)](https://github.com/civictechdc/spicy-regs/actions/workflows/integration.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Slack](https://img.shields.io/badge/Slack-join%20us-4A154B?logo=slack&logoColor=white)](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)

[Explore the data](https://app.spicy-regs.dev) ·
[Data dictionary](https://docs.spicy-regs.dev/) ·
[MCP server](#use-it-from-an-ai-assistant) ·
[Contributing](CONTRIBUTING.md) ·
[Changelog](CHANGELOG.md)

</div>

---

Every federal rule that gets proposed generates a public record: a docket, the
agency's documents, and the comments people file on it. That record is public
but awkward to work with — paginated APIs, rate limits, no bulk access, and no
way to join it to the rest of the federal picture.

Spicy Regs turns it into files you can query. A nightly pipeline reads
[regulations.gov](https://www.regulations.gov) data (via the public
[Mirrulations](https://github.com/MoravianUniversity/mirrulations) mirror) plus a
dozen complementary federal sources, and publishes the result as Parquet and
Apache Iceberg on Cloudflare R2 — public, anonymous read, no API key.

**You can query ~25M public comments from a laptop, a browser tab, or an AI
assistant, without downloading a database or asking anyone for access.**

This repo is the pipeline, the rollups, and the read-only MCP server. It's a
[Civic Tech DC](https://www.civictechdc.org/) project and new contributors are
welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## Public domain configuration

Set the GitHub Actions **repository variable** `SPICYREGS_DOMAIN` to your bare
base domain (for example, `spicygov.ai`). Unset or empty uses the upstream
`spicy-regs.dev` domain. Builds and deployments derive `data.`, `docs.` and
`mcp.` service addresses from it; the public links below show upstream defaults.
See [domain setup and override precedence](deploy/public-domain.md), including
local runs and the separate DNS and GitHub Pages settings.

## Contents

- [Try it without installing anything](#try-it-without-installing-anything)
- [What's in the corpus](#whats-in-the-corpus)
- [Quickstart](#quickstart)
- [Working with the data locally](#working-with-the-data-locally)
- [Running the pipeline yourself](#running-the-pipeline-yourself)
- [Use it from an AI assistant](#use-it-from-an-ai-assistant)
- [Project layout](#project-layout)
- [License](#license)
- [Contact](#contact)
- [Acknowledgments](#acknowledgments)

## Try it without installing anything

| I want to… | Go here |
|---|---|
| Browse dockets, agencies, and comment activity | **[app.spicy-regs.dev](https://app.spicy-regs.dev)** |
| Read every column of every published table | **[docs.spicy-regs.dev](https://docs.spicy-regs.dev/)** |
| Ask an AI assistant questions about the data | **[MCP server](#use-it-from-an-ai-assistant)** (`https://mcp.spicy-regs.dev/mcp`) |
| Run SQL in a notebook | **[Binder](https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD)** or [`docs/querying-python.md`](docs/querying-python.md) |

One line of SQL against the public bucket — no credentials, no download:

```sql
-- DuckDB, anywhere: CLI, notebook, or the browser
SELECT agency_code, comment_count
FROM read_parquet('https://data.spicy-regs.dev/agency_stats.parquet')
ORDER BY comment_count DESC
LIMIT 10;
```

## What's in the corpus

Published files are available under `https://data.spicy-regs.dev` with public,
anonymous read. The [data dictionary](https://docs.spicy-regs.dev/) describes
supported schemas and dated coverage observations. A declaration does not
establish that its current output is published: use MCP `list_sources` for
loaded availability and `describe_table` to compare the actual schema.

**Core regulations.gov tables**

| Table | What it is | Scale |
|---|---|---|
| `dockets` | Regulatory proceedings | ~276K rows |
| `documents` | Documents within dockets | ~2.0M rows |
| `comments` | Public comments (Hive-partitioned Parquet + an Iceberg table) | ~25.4M rows |

**Rollups** — small, denormalized, meant to be read whole: `feed_summary`,
`agency_stats`, `agency_monthly_volume`, `comments_index`, `docket_search`,
`discovery_signals`, `fr_docket_links`.

**Rulemaking dataset** — derived tables that publish together as one snapshot
(`materialize-rulemaking`; see `docs/ontology.md`), queryable through MCP:
`rule_targets`, `proceedings`, `regulatory_agenda_items`,
`agenda_item_proceedings`, `comment_periods`, `rulemaking_lifecycles`,
`lifecycle_events`, `agency_lifecycle_stats`.

**Complementary federal sources** — each ingested from its own API so the
rulemaking lifecycle, the organizations engaged in it, and its downstream
context are all joinable in one place:

- *Lifecycle:* `federal_register`, `unified_agenda`, `congress_bills`, `cfr_sections`
- *Organizations & influence:* `sam_entities`, `lobbying_filings`, `fec_committees`, `org_committee_links`
- *Outcomes & context:* `usaspending_recipients`, `court_dockets`, `gao_reports`, `crs_reports`
- *Telecom:* `fcc_proceedings`, `fcc_filings`

Cross-source join keys: **RIN**, **CFR citation**, **UEI**, **`agency_code`**.
Some sources are deliberately bounded or sampled (e.g. `lobbying_filings` is
2024+, `usaspending_recipients` is the top 100K by award dollars) — the data
dictionary documents the scope of each.

## Quickstart

Prerequisites: Python 3.12 and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/civictechdc/spicy-regs.git
cd spicy-regs
uv sync                       # install dependencies into .venv
uv run pytest                 # run the test suite
uv run ruff check .           # lint
```

The default checkout `source-readers` group installs the pinned wheels in `vendor/`
for BILLSTATUS, Unified Agenda, CourtListener bulk reads and PDF extraction.
Package installs opt in with
`spicy-regs[source-readers]`; base CLI and MCP installs remain independent.

The Unified Agenda reader uses SpicyDocs to fetch XML and read its fields;
SpicyRegs still chooses table columns and derives timetable dates. An unavailable,
malformed, or mismatched edition raises before yielding any of that edition's
rows. The two retained 2004 exports use an exact-hash, exact-position correction
with original and repaired digests retained. See the
[Agenda source-only rebuild guide](docs/unified-agenda-retained-rebuild.md).

Pinned originals also support separate local historical destinations:
[Federal Register issues and dated eCFR snapshots](docs/historical-regulations.md),
[complete Table III releases](docs/table3-historical-replay.md), and
[U.S. Code editions and native structure](docs/uscode-editions.md).
These commands produce receipt-bound local candidates; the published table
registry changes only when a separate publication selects their outputs.

CourtListener dump listing, source pins and CSV reading come from SpicyDocs.
The reader preserves quoted empty strings separately from nulls and refuses
invalid UTF-8 or malformed rows. Opinion tables deliberately map empty text to
null; the docket map preserves quoted empties in `court_id` and `docket_number`.
Resumed downloads require the original strong ETag, exact byte range and object
length; access refusals stop immediately. A missing strong ETag permits an
uninterrupted download but prevents a resume. These HTTP checks describe the
publisher's response, not independent proof of authenticity.
A failed read closes its source and discards staging output. Existing cached maps
and published tables change only when explicitly rebuilt.

No credentials are needed to run the tests, download the published Parquet, or
run the pipeline against the public Mirrulations mirror. Copy `.env.example` to
`.env` only if you want to publish output to Cloudflare R2 or ingest a source
that requires an API key.

## Working with the data locally

Download the published Parquet with the bundled CLI — no credentials:

```bash
uv run spicy-regs download                        # dockets, documents, comments
uv run spicy-regs download --types comments       # comments only
uv run spicy-regs download -o ./my-data           # custom output dir
```

Files land in `./spicy-regs-data/` by default. Then poke around:

```bash
uv run spicy-regs stats                # row counts + top agencies per file
uv run spicy-regs sample comments -n 5 # 5 random rows from comments
uv run spicy-regs search "climate"     # substring search across files
uv run spicy-regs agencies             # list every agency code
```

> Don't want to clone? Run it one-shot:
> `uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download --types comments`

For SQL-first exploration, [`docs/querying-python.md`](docs/querying-python.md)
walks through querying the bucket directly with DuckDB.

## Running the pipeline yourself

For bounded retained-source build/audit runs and explicit continuation, see
[the maintained operational runner](docs/operational-runner.md).

The pipeline reads raw JSON from the public Mirrulations S3 mirror, flattens it,
and writes Parquet to `./output/`. Scope your first run tight so it finishes in
minutes instead of hours:

```bash
# Smallest useful run: one agency, recent dockets + documents, no upload.
# The first run has no manifest yet, so it must be allowed to start empty.
uv run run-pipeline --agency EPA --skip-comments --since-year 2025 --allow-fresh-start
```

Comments require `--use-iceberg` and the `R2_CATALOG_*` credentials in `.env`.

What you get:

- `output/dockets.parquet` / `output/documents.parquet` — merged and deduplicated
- `output/manifest.parquet` — a Bloom filter of already-processed source keys,
  so the next run is incremental. Pass `--full-refresh` to rebuild from scratch.
  Without a manifest (locally or on R2) a run refuses to start unless given
  `--allow-fresh-start`.

Useful flags (`uv run run-pipeline --help` for the full list):

| Flag | What it does |
|---|---|
| `--agency EPA` | Process a single agency instead of all of them |
| `--since-year 2025` | Skip dockets older than the given year |
| `--only-comments` | Stage comments only (skip dockets + documents) |
| `--skip-comments` | Inverse — dockets + documents only (much faster) |
| `--max-workers 8` | Agencies processed in parallel (default 4) |
| `--full-refresh` | Ignore the existing manifest and rebuild from scratch |
| `--allow-fresh-start` | Start from an empty manifest when none exists (first run) |
| `--no-skip-upload` | Also publish to R2 (needs credentials in `.env`) |
| `--use-iceberg` | Route comments through the R2 Data Catalog (required unless `--skip-comments`) |
| `--chunk-size 50000` | Bounded-memory comment ingest for very large agencies |
| `--no-enrich-text` | Skip filling comment `text_content` from Mirrulations' pre-extracted attachment text |

### Rollups

Rollups are decoupled from the main ETL — each is its own console script and its
own cron workflow, so one failing source can't block the rest:

```bash
uv run run-rollup-feed-summary        # derived from the core R2 tables
uv run run-rollup-federal-register    # ingests an external API → its own Parquet
```

Each defaults to `--skip-upload`. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
full list and for what adding a new external source touches.

### Retained FEC input

`write_fec_committee_rows(records, destination, batch_size=2000)` in
`spicy_regs.transforms.build_fec_committees` accepts an iterator of raw OpenFEC
committee dictionaries. It reuses the existing column mapping, writes bounded
batches and replaces the destination only after the iterator finishes. It makes
no network request, preserves row order and duplicates, and publishes nothing.
Use a separate destination for a selected slice; the default rollup still merges
and deduplicates its full API input with the prior published table.

The caller verifies retained input bytes and keeps source references and coverage
alongside the output. Bulk master rows use different column names and omit some
API fields; they cannot be passed directly or treated as a complete replacement.

### Backfilling comment text

The ETL fills `text_content` inline only for comments it processes fresh — the
incremental manifest skips comments ingested before that feature landed, so they
stay `NULL`. Backfill from Mirrulations' pre-extracted text (read straight from
the bucket's `derived-data` prefix — no PDF download, no JSON re-ingest):

```bash
uv run spicy-regs download --types comments     # grab the published parquet
uv run backfill-comment-text                     # fill text_content in place
uv run backfill-comment-text --limit 5000        # cap work for a trial run
uv run backfill-comment-text --upload            # republish to R2 (needs credentials)
```

It's incremental and re-runnable — rows with a `text_extraction_status` are
skipped unless you pass `--overwrite`. Document text isn't published to
`derived-data`; backfill those with `uv run enrich-pdf-text --target documents`.
PDF extraction uses the `source-readers` extra (installed by default in a checkout).
For a package install, use `spicy-regs[source-readers]`. A page that fails to read
marks that PDF as `error`, with no partial text; blank pages remain valid.
SpicyRegs pins and checks pypdf `6.14.2` before parsing to reproduce its qualified
text and diagnostics. Backend recovery can change: `6.18.1` returns blank text
for the malformed `/Resources` fixture that raised on `6.14.2`. This is a
version-dependent recovery decision, not a claim that the newer backend is broken.
The reader is not a PDF conformance validator.
The shared reader refuses inputs over 64 MiB before parsing; this does not
bound decompression, output size, or processing time.

Each PDF attempt also saves `pdf_extraction_results_json`: an ordered entry for
every selected URL with its observed-byte SHA-256, status, page count, and error.
A row can be `ok` while one attachment failed; inspect this field before treating
its text as complete. A fetch that returned no bytes has a null digest/page count
and the generic error `fetch returned no bytes`. No response body is archived by
this enrichment step.

These are the latest PDF-attempt diagnostics. `--overwrite` replaces them; a
failed overwrite can leave older `text_content` intact, and derived-data backfill
preserves the PDF diagnostics while supplying text independently. Skipped rows
keep their prior results. Selection and aggregate counts remain per row.

Catalog setup adds this nullable field to existing document/comment tables and
refuses an incompatible column type. This requires DuckDB 1.5.3 or later. Setup
reopens the connection after adding the field so Iceberg writes see its current
schema. A conflicting schema update stops the run before data writes; rerunning
uses the current schema. The upgrade does not re-fetch files or publish the
public mirror; use the existing enrichment and publication commands for those
operations.

### Working on the data dictionary

```bash
uv run spicy-regs-dict check        # verify descriptions match the schema
uv run spicy-regs-dict generate     # regenerate table pages, catalog and MCP metadata
uv run --group docs mkdocs serve    # preview at 127.0.0.1:8000
```

Edit descriptions in `data_dictionary/descriptions.yaml`. Every table needs a
`label`, a `coverage` statement opening with its kind, and a `measured_on`
date; `check` fails on a missing or malformed one. `generate` also rewrites
`data_dictionary/catalog.json` and its digest — the machine-readable class
declaration other repos vendor — and `src/spicy_regs/table_metadata.json`, the
field meanings and coverage notes bundled with the MCP server. This lets a base
MCP installation describe datasets without installing the source readers.
It also writes `src/spicy_regs/table_qualification.json` from the
[output ledger](docs/research/fork-output-ledger-2026-09-21.md): each row's
audited pins, dates and disposition words, its statement and the ledger's
publisher, parsed by `spicy_regs.output_ledger`. `check` fails when that copy
differs from a fresh build or a row states an audit outside the recognized phrases.
CI fails if any generated output drifts from its source definitions.

The twenty-two hosted tables instead declare `columns_from: spicy_docs` and
take their per-column prose from the installed spicy-docs contract, so those
sentences live beside the code that fills each column. Declaring that marker
and an inline `columns:` together is refused at load.

One page explains ETL receipts: `docs/tables/etl_receipts.md`, whose text is
`data_dictionary/etl_receipts.md`. A table's notes mention its receipt in at
most one sentence, and only once the table has published in the native layout;
the header of `descriptions.yaml` states the rule and what to do when a table
converts, and `check` holds it.

> **Consumers must re-vendor.** `catalog.json` grew from 24 classes to 45 and
> `congress_bills` from 10 columns to 48, so every copy pinned by
> `catalog.json.sha256` is stale. `format_version` is still `3` — no field
> changed shape — but the `kind` vocabulary gained `sampled`, so a consumer
> that branches on `kind` needs a case for it before re-vendoring.

## Use it from an AI assistant

The read-only MCP server provides `list_sources(subject=None)`, `describe_table(table)`,
`query_sql(sql)`, `resolve_document_citations(...)`,
`read_receipt_fields(table, keys, fields)` and
`lookup_agency(namespace, identifier, on_date=None)`.

`read_receipt_fields` returns values a table keeps in its ETL receipts rather
than in its columns (a filing's source link, a value as the publisher wrote
it), for up to 100 rows by their identity. `describe_table` lists each table's
identity fields and the receipt fields it carries, which differ by table.

`lookup_agency` uses the pinned reviewed RefSpec mapping. Its exact namespaces
are `regulations.gov:agency` (for example, `OPM`) and
`federal_register_agency` (for example, `406`). It returns mapping evidence,
digests and documented abstentions without opening corpus tables. Labels do not
match identifiers, parent agencies are separate relationships, and an `on_date`
request remains `temporal_scope_unqualified` for an otherwise mapped agency.
REF-072 bridge and succession evidence is returned separately from REF-038
identity candidates. The owner's current-lineage policy ignores dates, so it does
not establish identity on a requested historical date or money attribution.

Document content renditions and separate attachments retain different roles.
`document_attachment_records_*` and `document_attachment_renditions` require
`attachment_records_json`, which comes from an explicitly read attachment
relationship. Older published document schemas report these views as unsupported.
In newly shaped rows, NULL means unread; `[]` means a validated complete empty
response. Restricted attachment records remain visible even without a file URL.

FEC subject tables are shaped at build time. Necessary response/array child views
appear in the same table list with `role: child_query` and their `source_tables`.
See [FEC tables and child rows](docs/fec-query-views.md) for grains, evidence and release limits.

`list_sources` with no argument shows the default table lists by subject
(`unassigned` holds tables the dictionary gives no subject), a preview and
count of declared tables without a loaded view (`unavailable_tables` and
`unavailable_table_count`), relationship and deferred view counts, the receipt
summary and FEC release status counts. The default excludes a designated set
of intentionally empty tables and sample-sized datasets from its lists and
unavailable count.
`list_sources(subject)` returns all loaded tables in that subject, including
those omitted by default, with their label, coverage kind (`true_range`,
`window`, `sampled`, `derived`, `not_a_range` or `empty`) and pinned row count.
A declared subject with no loaded tables returns none. An unknown subject returns
an error with valid names. The `views` page lists relationship views under their
shared summary, every unavailable table and view, deferred view names and the
FEC release; the `archive` page gives the bound archive release. Every reply carries
`generation`, a digest of the connection's pins: pages whose generations differ
came from different connections (the connection refreshes every five minutes). The whole
catalog in one reply had reached 39,135 of the 40,000-character reply budget
(2026-10-31); each page is now held 1,000 characters under it. Discovery checks
selected source schemas without restoring receipt-backed rows. The `views`
page's `deferred_views` names views whose exact receipt admission and output
schema remain pending;
`query_sql` validates those rows in a separate query connection. Matching FEC
release declarations do not mean deferred views have executed. Availability
reflects the current cached connection, which normally refreshes after five minutes; it does not certify
population completeness or data freshness. `describe_table` returns the loaded
columns, each with its dictionary meaning, plus declared row identifiers,
coverage and data-quality notes, joins and the live `publication` pin.
`schema_differences` shows when the loaded artifact differs from the supported
schema. Unavailable tables still return their dictionary metadata, declared
columns included, with `available: false`. `query_sql` returns the `publication`
pin of each table the query names. `describe_table` returns `qualification`: the
live pin, the output ledger's audited pin, date and disposition word, whether
the two pins match, and the ledger row's own statement, as separate fields. It is reported only when
the server reads the publisher the ledger names, and is `unknown_for_publisher`
otherwise. A disposition applies to its own pin and stated scope; it does not
upgrade relationships that `data_quality` calls heuristic. Read the dated coverage
notes and source evidence before interpreting joins or totals. These responses
describe this checkout's server; the hosted service changes after deployment.

To query locally built or audited outputs through the same MCP tools, set
`SPICY_REGS_DATA_DIR=/absolute/path/to/parquet-directory` before starting
`spicy-regs-mcp`. This reads only the named tables present in that directory,
loads no remote tables or catalog, and needs no network extensions. Responses
identify the local source directory. An invalid directory is refused rather
than silently switching sources. The SQL write guard also applies in local mode.

For an immutable DocSpec export, optionally set
`SPICY_REGS_DOCSPEC_EXPORTS=/absolute/path/bindings.json` with that explicit
local directory. The caller-trusted JSON document has `version: 1`, an absolute
`binary` path to DocSpec, a literal loopback `metadata_url` such as
`http://127.0.0.1:8080`, and `tables` mapping each application table name to
`{"file": "comments.parquet", "export": <complete canonical ExportPin>}`.
Each local file may be bound once. Only these named local files enter the connection; managed download/native
selection modes cannot share the directory. Admission compares the expected
pin and exact source release through the configured producer, then uses the
existing native `hash` command once to check the local file. Unchanged bindings
keep the admitted connection across refresh checks, with ordinary file-change
guards around queries. The producer stays running; hashing opens no Engine.

Replies identify `docspec_immutable_export` and retain the exact export and
release-level evidence reference. That reference is separate from SpicyRegs
ETL receipts, grants no SQL access to evidence files, and supplies no invented
row-to-receipt mapping or population-coverage claim. Callers authorize disclosure
of the pin and its references and retain the referenced evidence independently.
The first end-to-end qualification covers BIGINT/VARCHAR values; other application
type representations and remote distribution need their own qualification.
Default local-directory behavior is unchanged when this setting is absent.

For the FEC source inventory, selected retained collections and relationship
evidence, see [FEC integration](docs/fec-integration.md). The
[FEC gap register](docs/fec-gaps.md) tracks remaining work, owners and completion
checks; the [coverage census](docs/research/fec-coverage-2026-09-21.md) accounts
for every researched source family and official bulk group.

The [fork generation plan](docs/fork-generation.md) covers all rollups and their
dependencies. Its [local reuse inventory](docs/research/local-data-reuse-2026-09-21.md)
identifies existing source data and audited outputs available to seed the fork.

| Client | Setup |
|---|---|
| **Claude.ai** or any remote MCP client | Add `https://mcp.spicy-regs.dev/mcp` as a Custom Connector. Copy-paste setup for every client is at [mcp.spicy-regs.dev](https://mcp.spicy-regs.dev/). |
| **Claude Code** (plugin) | `/plugin marketplace add civictechdc/spicy-regs` then `/plugin install spicyregs@spicy-regs-local` |
| **Claude Code** (stdio, no deploy) | `claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp` |
| **Cursor / Continue / OpenAI / others** | See [`plugins/spicyregs/INSTALL.md`](plugins/spicyregs/INSTALL.md) for the full matrix, including a prompt-only fallback for assistants without MCP support. |

## Project layout

```
src/spicy_regs/
├── pipelines/        # Pipeline contract, the main ETL, and one module per rollup
│   └── rollups/      # Derived rollups + external-source ingests
├── sources/          # Readers/writers: Mirrulations S3, R2, Iceberg, external APIs
├── transforms/       # Merge, partition, enrich, and the rollup builders
├── manifest.py       # Incremental state (Bloom filter over processed source keys)
├── mcp_server.py     # Read-only SQL MCP server
├── data_dictionary.py# Generates the docs site from the schemas
└── cli.py            # Local data CLI (download / stats / sample / search)

mcp-server/           # MCP server engineering notes (the server itself is src/spicy_regs/mcp_server.py)
scripts/              # Operational tooling (dedupe, seed, freshness checks)
notebooks/            # Example analyses (runnable on Binder)
tests/                # Unit suite; integration tests are opt-in
.github/workflows/    # ETL cron, per-rollup crons, CI, deploys
```

The ETL is assembled from small composable pieces — `Reader → Transform →
Writer`, wired by a `Pipeline`. The
[architecture section of CONTRIBUTING.md](CONTRIBUTING.md#architecture-the-etl-building-blocks)
explains the contract, and `tests/test_example_pipeline.py` is a runnable
reference for adding your own.

The public [explorer metadata bundle](docs/explorer-metadata.md) publishes
descriptions, original sources, derived inputs and declared joins alongside
the data index. It refreshes independently of website deployments.

## License

[MIT](LICENSE) © Civic Tech DC.

The underlying data is public U.S. federal government information. Mirrulations
S3 is read with unsigned (anonymous) access, and the published Parquet/Iceberg
on R2 is openly readable.

## Contact

- **Slack** — [join the Civic Tech DC workspace](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)
  (open invite, anyone welcome), then say hello in
  [#spicy-regs](https://civictechdc.slack.com/archives/C09H576E6LU). This is the
  fastest way to reach us.
- **GitHub** — open an
  [issue](https://github.com/civictechdc/spicy-regs/issues/new/choose) for a bug,
  a feature idea, or a question about the data.
- **Email** — [eugene.kim@civictechdc.com](mailto:eugene.kim@civictechdc.com) for
  sponsorships, partnerships, or enterprise use cases.

## Acknowledgments

- [Mirrulations](https://github.com/MoravianUniversity/mirrulations) — the public
  regulations.gov mirror this pipeline reads from.
- [regulations.gov](https://www.regulations.gov) and the agencies that publish
  their dockets there.
- Everyone who has contributed code, issues, and analysis.

---

<div align="center">

<a href="https://www.civictechdc.org/">
  <img src="assets/civictechdc-logo.png" alt="Civic Tech DC" width="72" height="72">
</a>

Built and maintained by **[Civic Tech DC](https://www.civictechdc.org/)** — a
volunteer community using technology to serve the DC region.

**[Come build with us.](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)**

</div>

### Source-reference navigation

`src/spicy_regs/explorer_navigation.py` defines complete source keys for array and receipt-backed browser navigation. `table_joins.json` packages those recipes alongside scalar joins; the explorer metadata publisher binds them to the selected schemas. New meeting and nomination SQL relationship views compile from those same recipes. Add definitions here rather than matching field names in the browser.

The source reader preserves nomination partitions, independent meeting references, FCC document offers and Congressional Record passages. Native receipt migrations admit exact recorded prior schemas; original source fields remain recoverable from receipts paired with the selected generation. Missing keys refuse navigation, and legal ambiguity retains every recorded candidate. FEC routes expose retained observations without selecting current amendments or combining money amounts.

After changing source fields, run the focused navigation, Congress receipt and government shape checks, the dictionary check and generators, then full CI. Refresh only the affected rollup families. Metadata publication requires successful CI for the exact main revision and does not rescan unchanged comments or FEC records.

Scorecard acquisition, publication and recovery: [production operations](docs/scorecards-production.md).
