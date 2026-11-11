# MCP server internals

> Engineering notes for contributors. Not user-facing — for how to *use* the
> server see <https://docs.spicy-regs.dev>; for deploying it see
> [`../deploy/cloudflare/`](../deploy/cloudflare/).

This document explains the canonical MCP server implementation
(`src/spicy_regs/mcp_server.py`) and its operating boundaries. The active public
endpoint is <https://mcp.spicygov.ai/mcp>: a Cloudflare Worker forwards requests
to the Python server in Cloudflare Containers.

> **History:** there used to be a second, hand-mirrored copy at
> `mcp-server/api/index.py` — a dependency-light parallel of the canonical server
> for a Vercel deployment, kept in sync by `test_vercel_copy_in_sync`. The server
> now runs from the **canonical** module directly; the copy and its sync test
> were removed. Cloud Run was an earlier deployment. The active Cloudflare
> Dockerfile installs the runtime dependencies without the ETL dependency set;
> see `deploy/cloudflare/Dockerfile` and `deploy/cloudflare/worker/index.ts`.

## What must never be deleted

**The tool docstrings are not documentation — do not delete them.**
MCPServer reflects over `fn.__doc__` to build the tool descriptions sent to every
client during `list_tools`; the `tool` wrapper in `_tools` copies each
signature with `functools.wraps` and registers the docstring through
`inspect.cleandoc` as the description. MCPServer sends `__doc__` as written, and
Claude Code cuts a description at 2,048 characters (`CLAUDE_CODE_MAX_MCP_DESCRIPTION_LENGTH`)
with "… [truncated]": round 4 found `query_sql`'s 2,132 characters, 225 of them
indentation, cut mid-word for three personas. The cap counts the compact input
schema too: round 5 saw `query_sql`'s 1,728 + 322 = 2,050 cut at character 1,711
for three personas while `describe_table`'s 1,832 + 190 = 2,022 was not
(`round5/audit-oyelaran.md`). `tests/test_chaos_r4_server.py` holds every
registered description plus its compact input schema to 2,048 characters, with
no indented line; a long field description or enum spends the same budget. `describe_table`'s docstring is how a client learns
which tables are valid; `query_sql`'s is how it learns the available views.
Strip them and the server still runs, but every client goes blind. Verify with
`asyncio.run(build_server().list_tools())`.

**Inline `# type: ignore` / `# noqa` are directives, not comments.** `ty` and
`ruff` gate merges and both read them.

## Arguments are refused, never dropped (`_StrictTool`, round 6)

MCPServer's argument model ignores a key a tool does not declare, so zubair's
misspelled `offest` ran page 0 of a query and said nothing (round 6), and a
bound such as `max_rows` above 500 failed in pydantic's own text with a link to
its documentation. Each tool is a `_StrictTool`: its `run` refuses an
undeclared argument and states every argument error in one sentence, then the
arguments the tool takes ("offest is not an argument. query_sql takes
max_cell_chars, max_rows, sql."), and its schema says `additionalProperties:
false` without pydantic's per-field titles, which repeated each name against
the description cap. The tools are built with `Tool.from_function` and handed
to `MCPServer(tools=...)`, a public seam. A test of a new parameter can assert
its acceptance: a misspelled one is refused.

`lookup_agency`'s `namespace` is the closed set `vocabulary_mapping.Namespace`.
Rounds 4 and 5 answered another namespace with a success reply,
`status: unsupported_namespace`, on the reasoning that the lookup had run and
found nothing it could map. That was wrong: an agent took the status as an
answer about the identifier. With the two namespaces advertised in the schema,
another one is a malformed argument, and it is refused naming both.

`lookup_agency` names no organization itself (round 6, L7). `parent_labels`
gives each parent URN in the reply the name the vendored RefSpec rows state
for it (`vocabulary_mapping._names`, through `ontology.agencies`' public
readers): a preferred label first, then a publisher's name on a mapping's
evidence record, a bridge end or an event result; null where no row names it,
which is a registry change for the search lane. On 2026-10-03, 24 of 30
parents were named; the three eCFR and three Federal Hierarchy parents were
not, nor 9 of 10 event originals. The text says `current_lineage` is computed
for Federal Register ids only and points at each evidence row's `reasoning`
sentence for whether two names are one agency.

## Connection setup (`_build_connection`, `_get_connection`, `_apply_security_settings`)

- A local directory (`SPICY_REGS_DATA_DIR`) replaces R2 for the whole
  connection and never falls back to remote files; every reply's `source` names
  which one it read.
- `SET home_directory` **must precede** `INSTALL`/`LOAD`. DuckDB writes
  extensions under `<home_directory>/.duckdb`, and the default home is read-only
  or undefined on serverless hosts — hence `_resolve_home_directory` defaulting
  to the temp dir (`SPICY_REGS_HOME_DIR` overrides).
- Keep `LocalFileSystem` enabled. Disabling it breaks httpfs reads. Instead,
  set `allowed_paths` to the exact local paths or HTTPS URLs selected for the
  published views and set `enable_external_access=false`. DuckDB then refuses
  unrelated file access, including reads hidden in nested queries.
- `allow_persistent_secrets=false` prevents httpfs from consulting an unrelated
  on-disk secrets directory. Set it immediately after connecting, before binding
  any remote views: httpfs initializes the secret manager, after which DuckDB
  refuses even setting the same value again. The final restriction step checks
  the value before setting it. Load required extensions before restrictions.
- Load httpfs through `load_public_http(con, INTERACTIVE_HTTP_RETRIES)` before
  any view binds, so the build's own reads retry, and before the lock: DuckDB
  refuses `SET` once `lock_configuration` is on. DuckDB 1.5.5 retries a 429 on
  HEAD and GET, the first retry at once and then `wait * backoff**(k - 2)`
  before retry k. Its defaults give up after 0.5 s; the interactive policy
  allows 7 s of backoff per request, and the batch policy
  (`PUBLIC_HTTP_RETRIES`) about four minutes. `tests/test_duckdb_settings.py`
  measures the schedule against a local server.
- A legacy table (one no pointer pins) is skipped only when its read answers
  HTTP 404 (`duckdb.HTTPException.status_code`). Any other failure, a 429
  included, refuses the build: skipping a throttled read served a connection
  without the table.
- Apply memory and spill-directory settings before disabling external access;
  DuckDB refuses changes to `temp_directory` after that boundary is locked.
  A configured spill directory is an engine resource, not an allowed SQL input.
- Apply `_apply_security_settings` after trusted view construction has collected
  selected member paths and before the connection reaches any user query.
  `lock_configuration=true` stays last. Local and HTTPS controls, path escapes,
  and the deployed `/etc/os-release` reproduction are covered by retained
  experiments and the MCP regression tests.
- The public SQL server refuses configured direct Iceberg reads before setup: its dynamically
  discovered manifest and data paths have not been scoped to this file boundary.
  The fork currently serves published Parquet and configures no catalog on its
  Worker. ETL ingestion and mirror publication continue to use Iceberg. See
  `docs/research/mcp-chaos-2026-09-27.md` for the catalog comparison and adoption
  criteria; enabling unrestricted reads to make a catalog query pass is not a fix.

## Connection lifecycle (`_get_connection`, `_refreshed`)

A build reads each published table's Parquet footer over HTTPS: 3 JSON GETs and
461 Parquet requests (345 HEAD, 116 footer GETs) against r2.dev, 43 to 56 s, on
2026-09-28. Each build is a new DuckDB instance, so nothing it cached survives
into the next one.

- **Polled, not rebuilt.** Past `SPICY_REGS_CONNECTION_TTL` (300 s), the first
  caller re-reads the publication index and the rulemaking pointer and manifest
  (3 GETs, 0.45 s), plus the comments export receipt (one GET of about 45 KB),
  and compares them with the pins the connection holds
  (`_pinned_publication`). The views name immutable URLs, so unchanged pointers
  keep the connection; an idle TTL went from 464 requests to 3.
- **Moved pointers rebuild** through `_build_connection(publication)`, which
  pins exactly the pointers the poll read. The rebuild is a whole new instance:
  the security settings lock `allowed_paths`, so a locked instance cannot admit a
  new generation's URLs, and `CREATE OR REPLACE` of only the changed views is
  not possible.
- **Only the refreshing caller waits.** Other callers keep the current
  connection while a rebuild runs (`_refreshing`). A failed refresh, such as a
  429 on a pointer or a member, logs and keeps the current connection until the
  next TTL. Cold-start failures raise; request cancellation or deadline also
  raises during a refresh while preserving the older pinned cache.
- **Local mode rebuilds at every refresh**: its `current` link and member
  signatures are re-read at build.
- **Discovery has a separate cache.** `list_sources` and `describe_table` bind
  selected source schemas and compare release declarations without restoring
  financial or citation rows. Receipt-dependent views return `status: deferred`
  and `query_admission: pending_receipt_rows`; `list_sources.deferred_views`
  groups every deferred name by its identical reason/admission state. Discovery
  uses concise view labels; `describe_table` retains full registered meanings,
  identities, dependencies and purpose limits. Receipt families are listed once
  by name; `describe_table('etl_receipts')` carries their selected generation/hash/
  row pins and shared receipt fields. Their output schema is not invented. Release
  compatibility describes matching declarations, not completed row validation.
  `query_sql` builds its own connection and runs all exact admission guards
  before making those views queryable. Remote SQL with named relations binds
  only their declared source dependencies; receipt-dependent adapters still
  validate their exact selected receipts. An explicit `etl_receipts` query
  retains every captured family's receipts. Later queries for unbound sources
  or relationships rebuild against the same publication pins, preserving the
  previous connection if the replacement fails. Queries without named relations
  retain the complete catalog for introspection. Native local discovery still checks
  held-byte pins and policy schemas, and reports pending receipt joins.
- **Legacy comments files stay mutable.** Their views read the object current at
  each statement (the HEAD revalidates DuckDB's file cache), but the bound
  schema lasts until the pointers move. The build reads `comments-publication.json`
  and HEADs the two files that back views (`COMMENTS_EXPORT_TABLES`), not the 182
  the receipt lists; a moved receipt rebuilds the connection at the next poll.
  An invalid receipt reads as none (logged): it only labels two files' rows, so
  it does not refuse the connection. A failed read refuses it, as the index does.

## Concurrency: worker threads and a limiter

FastMCP (mcp 1.x) ran a sync tool on the event loop, so the server executed one
call at a time and a slow one, such as a 43 s build, stalled every request,
`GET /` included. MCPServer (mcp 2.x) runs sync tools on worker threads, and
`_tools` wraps each tool so that:

- it runs through `anyio.to_thread.run_sync` with one `CapacityLimiter` of
  `SPICY_REGS_TOOL_CONCURRENCY` tokens (default 2). A remote scan issues range
  requests from every DuckDB thread; one persona query peaked at 72 requests/s
  on four threads, and r2.dev throttles at hundreds per second across all the
  bucket's readers. Raise the limit once the bucket has a custom domain.
- each call takes its own `cursor()`, and the statement timer interrupts only
  that cursor (`tests/test_mcp_concurrency.py`).
- every failure is re-raised as a `ToolError`. MCPServer shows the client only a
  `ToolError`'s text; any other exception reaches it as a bare "Error executing
  tool". DuckDB's parser and binder messages and the read-only refusals are
  what a caller corrects a query from.

The Worker's `getContainer` has no instance name, so every request reaches one
container whatever `max_instances` says. More instances would each build their
own connection against the public bucket.

## The read-only statement guard

`query_sql` hands arbitrary SQL to `cursor.execute()`, so without a gate the
public endpoint accepts writes. `COPY ... TO`, `ATTACH`, and `EXPORT DATABASE`
all reach the container filesystem. Cloudflare gives this deployment disk-backed
spill space, but arbitrary writes could still exhaust it or corrupt local state.
The endpoint is public and unauthenticated, so that is an anonymous availability
lever, not a reason to relax the read-only guard.

Native file permissions constrain reads to selected data files. They do not
replace `_first_write_statement`, which classifies with DuckDB's own parser and
admits only `SELECT`. The outer `EXPLAIN` statement type hides its inner
statement: `EXPLAIN ANALYZE DELETE ...` executes the deletion, so all `EXPLAIN`
forms are refused until the inner statement can be reliably classified. The statement guard also refuses SQL that
changes configuration or writes permitted files.

**Do not swap the parser for a prefix regex.** The parser is what makes leading
comments (`/* c */ COPY ...`), `COPY` inside a string literal, and stacked
statements (`SELECT 1; DROP TABLE t`) classify correctly. The stacked case
matters most: `execute` runs every statement in the string but returns only the
last result, so a trailing write would otherwise land with nothing in the
response to show for it.

**The SELECT allowlist includes other read forms.** DuckDB folds
`DESCRIBE`, `SHOW`, `SUMMARIZE`, `VALUES`, `TABLE`, the FROM-first
shorthand (`FROM comments LIMIT 1`) and PRAGMA's table-returning forms
(`PRAGMA table_info('dockets')`, `PRAGMA show_tables`) into
`StatementType.SELECT`, so all of them still run; PRAGMA's state-changing
forms parse as `PRAGMA` or `SET` and are refused. `tests/test_mcp_server.py::test_read_forms_pass_the_guard` pins that
folding; if a DuckDB upgrade splits any of them into its own statement type,
that test fails rather than clients silently losing a query form.
`PRAGMA import_database` is expanded while DuckDB parses, reading the named
directory's `schema.sql` before the guard sees a statement: a file of SELECTs
passes the guard on an open connection, and only the locked connection's file
boundary refuses it (`test_import_database_is_refused_by_the_locked_connection_where_the_guard_cannot_see_it`).

Matching is on `StatementType.name`, not the enum member, because duckdb's type
stubs do not declare the members and `ty` gates merges — comparing names keeps
the check honest without a blanket type-ignore.

The two controls work together: `read_text`, `read_csv`, and `read_blob` are
SELECTs, but native file permissions refuse any file not selected for the views.
A result with duplicate column labels is rejected with alias guidance, because
a client reads each row's values by the names in `columns`. One-row lookahead
sets `truncated` only when `max_rows` actually omits a row. Validation refusals
raise tool errors so MCP reports `isError: true` consistently.

## The request timeout

**DuckDB has no `statement_timeout` parameter.** Setting one raises a catalog
error. `SPICY_REGS_STATEMENT_TIMEOUT` supplies a request budget instead: `790s`
by default in code; the Cloudflare Worker passes `600s`.

The budget begins before the tool enters the worker limiter. It covers cold
connection locks, input hashing, remote streams, receipt admission and SQL.
`runtime_bounds.RequestBound` carries one absolute deadline through these
readers. Streaming and receipt batch checkpoints raise `TimeoutError`; remote
HTTP timeouts use the remaining budget. Owned admission databases and each
request's query cursor register for interruption. A cancelled/failed build
closes its database and never enters the cache. Another request's cursor is
not interrupted.

The async wrapper bounds caller waiting and signals cancellation when the
client cancels. The worker stops at the next checkpoint or owned database
interruption; this does not forcibly preempt arbitrary Python or OS file I/O.
Direct internal statements outside an interactive request retain the original
cursor watchdog for test/audit callers. These application bounds do not measure
client startup, transport initialization or a platform deadline.

For serialized local audits, bound lock acquisition separately from the client
invocation and record attempt start, lock acquisition and terminal outcome in
the existing call log. The outer caller's timeout must cover both budgets plus
startup and log-delivery overhead: a `20000ms` Bash timeout cannot accommodate a
`120s` client deadline, even before lock waiting. Use the audit harness's
`600000ms` outer Bash budget, not a new production timeout setting. An outer
termination without a completed wire response establishes neither server query
duration nor whether server work continued or was cancelled. Test those caller
boundaries with a local held lock and fake transport, not a repeated heavy
public query.

## Iceberg and the comments mirror

Iceberg remains the ingestion and update store for comments. The MCP reads its
published Parquet mirror. Direct catalog configuration is refused before any
remote setup; dynamic manifest and data access has not been qualified against
the selected-file restrictions. The old best-effort attach and catalog view
were removed rather than retaining an unreachable, unrestricted fallback.

The 2026-09-27 comparison matched the catalog table UUID, snapshot and schema to
the mirror receipt, then matched the public object ETags and sizes to that
receipt. This supports current publication agreement, not a new source-content
audit. Legacy comments files still have mutable URLs and no immutable comments
generation is pinned; replies state the export receipt's identity and rows
beside the `legacy_unversioned` status (see Response size).

A table in `TABLES` whose Parquet is not published yet is skipped with a warning.
A missing managed generation member still refuses the connection.

## The rulemaking snapshot (`_spicy_rulemaking`)

The materialized rulemaking dataset (`rule_targets` through
`agency_lifecycle_stats`) is not in `publication.v2.json`: it publishes all its
tables at once under `materialized/rulemaking/snapshots/<id>/` and then
replaces `materialized/rulemaking/latest.json`. `_build_connection` reads that
pointer and the manifest it names once, beside the index, through
`publication.load_rulemaking_snapshot`, and pins the result in
`_spicy_rulemaking` the way `_spicy_publication` pins the index. The same view
loop then points each rulemaking view at its immutable snapshot key; there is no
second view path.

- **How often.** At each connection build, and at each refresh past
  `SPICY_REGS_CONNECTION_TTL` (see Connection lifecycle). A moved pointer is
  therefore served from the next refresh, while cursors on the old connection
  keep the snapshot they started with.
- **Refused, not guessed.** As `pipelines.materialized` reads its prior
  generation, the pointer and manifest must be one readable format version, of
  this dataset, naming one snapshot, and every public artifact must sit at
  `<snapshot prefix>/<name>.parquet`: the key is inlined into
  `read_parquet('…')`. An artifact without `visibility: public` is never
  served. A snapshot member that cannot be read refuses the connection, as a
  managed member does.
- **Blast radius.** A pointer or manifest that fails those checks refuses the
  whole connection, every table with it, exactly as an invalid
  `publication.v2.json` does through `load_index` today. Degrading to "rulemaking
  tables unavailable" would be possible but would make a broken publication look
  like an unpublished one; it is left as the index's behavior on purpose.
- **No schema pin.** The manifest records bytes, digest and rows but no columns,
  so the build cannot compare a view with a descriptor. `describe_table`
  compares it with the dictionary instead (`schema_matches_declared`), and
  `spicy-regs-dict check --source r2` holds each declared type to the file.
- **Local mode** never reads the pointer; a rulemaking table there is a loose
  Parquet file like any other.
- **Parsed once.** Each pinned record (`_spicy_publication`, `_spicy_rulemaking`,
  `_spicy_local_selection`) is JSON in the connection, read by several helpers
  per tool call; `_parsed_pin` parses each distinct document once and shares the
  result, which callers must not mutate.

## The congressional-tech archive release (`_spicy_archive`, L2-A4)

The archive's documents, meetings and witnesses are bound as nine flat views,
`archive_list` … `archive_recordings`, read from the release
`archive_release_pins.json` names as `bound` (see `archive_release.py` for the
pin's lifetime rule; its wire keys are published to `pins/archive-releases.json`
in the spicy-regs bucket, where the archive's pruner reads them).

- **Same admission path.** Archive views are `_RemoteSourceView`s admitted by
  `_admit_remote_source_views`, so `_remote_query_scope` decides: a query that
  names no `archive_*` view reads nothing from the archive, not even the
  manifest; one that names `archive_meetings` opens only that footer. Names are
  flat in `main` because `_tables_named` admits only `main`.
- **Read once.** The manifest (release id recomputed from `files[]`) and
  `schema.json` (checked against the manifest digest) are cached per process:
  a release prefix is create-only. TTL rebuilds re-read neither.
- **Columns by name.** Each view selects the columns `schema.json` publishes,
  so a file that lost one refuses at bind time; `list` and `detail` add
  `list_row` as `file_row_number` (DATA-MODEL §8.4). Sharded files refuse in
  `bound_files` until the archive publishes an offset rule.
- **Metadata cache stays on.** Archive URLs are classified `archive_release` in
  `_immutable_metadata_inputs`; without that, any unowned URL turns the HTTP
  and Parquet metadata caches off for the whole connection.
- **Blast radius.** Unlike the rulemaking pointer, an unreachable archive in an
  unscoped build (discovery) is recorded `unavailable` in `_spicy_archive` and
  the build proceeds: it is another host, and spicy-regs tables do not depend
  on it. That covers an unreadable release file too (`degrade`), such as a
  release pruned under the manifest this process cached: every archive view is
  retracted, never a partial set. A query naming an archive view refuses instead.
- **Trust.** Remote bytes read by HTTPS range are not hashed; `describe_table`
  states this (`publication.trust`). Local mode reads only
  `$SPICY_REGS_DATA_DIR/archive/releases/<release_id>/`, hashes every bound
  file at each build (linear in release size), and never falls back
  to the network. No download command fills that directory yet: copy the
  release's files, `manifest.json` included, from
  `<ORIGIN>/releases/<release_id>/` by hand; the binding then verifies them.
- **Recipes as published.** `describe_table` shows `schema.json` relationships
  and recipes unchanged, with `name_map` from release table names to views;
  those needing the producer=`spicy-regs` files (rosters, committees, members,
  bills, nominations) carry `available_here: false`, since spicy-regs serves
  those facts from its own tables. The meeting and witness document recipes
  join with `list_contains`, which is fine for one meeting but superlinear as a
  table-wide join; a maintained meetings-to-documents view must unnest
  `meeting_keys`/`witness_keys` into `(key, list_row)` pairs once and hash-join,
  as `scripts/compare_committee_meetings_archive.py` does. No such view exists
  yet; it lands with the committee_meetings retirement (ss-ikn9 stage 3).

## Declared joins (`_table_joins`)

`describe_table` lists the cross-table joins the table makes (`outgoing`) and
receives (`incoming`) from `table_joins.json`, which `spicy-regs-dict generate`
bundles from `spicy_regs.table_joins` and `check` refuses when stale. Each join
carries its kind (`complete`, `scope`, `design`, `empty`), the reason for a
partial one, and its measured baseline. `scripts/check_table_joins.py` holds the
live tables to each floor nightly in `check-rollup-freshness.yml`.

**The meaning text lives in the record (round 5).** `joins.basis` and
`qualification.basis` are the records' own top-level `basis`, which
`table_joins.joins_record()` and `output_ledger.qualification_record()` write
(`table_joins.BASIS`, `output_ledger.BASIS`); the server holds no copy. Round 5
found a persona reading a `complete` join as proof that the publisher paired
each key correctly (CHRG-118hhrg63377 with meeting 116369, the wrong hearing on
the same day), so the joins basis says `complete` checks only that each
non-null child key names a parent row; the qualification basis says a receipt
it names is the maintainer's retained evidence, not a public file. Editing
either text is a `spicy-regs-dict generate`, and `check` refuses a stale copy.

## Map-time status columns (FEC typed tables)

Some FEC typed columns are stamped when a row is mapped and never computed
afterwards: `filing_link_status` (`unresolved` on every row of 27 tables on
2026-10-03, with `filing_key` NULL), `entity_reference_status` on the two
summary tables, `fec_loan_guarantors.loan_link_status` and
`fec_filing_links.target_resolution_status`. The deciding happens elsewhere: in
the `<table>_filing_associations` and `fec_filing_reference_resolution` views,
or in a declared join. Since round 6 the dictionary says so from one template
per column (a YAML anchor in `descriptions.yaml`), and
`scripts/check_status_constants.py` lists, at release, every status column that
holds one value while its text does not say so.

**Debt: drop these stored constants when the FEC typed family is next
regenerated wholesale.** They state nothing the mapper did not already know and
read like outcomes. They stay for now because removing `filing_link_status`
rewrites all 27 tables of the family, the two largest (`fec_receipts`,
`fec_intercommittee_transactions`) among them, changes the SQL identity of the
qualified views that read them and forces a full release receipt, for a cleanup
that changes no result (`phase3-review.md`, round 6, 2026-10-03).

**Debt: both FEC mappers adopt spicy-docs' placeholder predicate when their
families are next regenerated.** Form 1's `affiliation_status`
(`fec_identity_observations.map_registry`) and `fec_relationships.value_status`
read a name as reported none only where it is `NONE` once stripped and
upper-cased. `schemas.fec_committee_history.is_placeholder_name` is the one
predicate, which the organization-links build has read since round 6 (its
second copy, `transforms/fec_placeholders.py`, is gone). Adopting it moves rows
from a stated name to reported none, so it waits for a regeneration under a
release receipt that declares them: on 2026-10-04, 2 of 12,311 Form 1 rows of
fec-query `784b23da` (`N/A`, `NA`) and 2,608 rows of fec-observations
`9c289dfe` (`BLANK` 1,574, `N/A` 674, `-` 126, `"NONE"` 102 and eleven rarer
spellings). Both statuses are receipt fields in the native-ETL design
(`fec_identity_context_fields.json`), so after the conversion they move in
`etl_receipts`, and the subject rows keep the name as filed.

## Citation lookup kinds (`resolve_document_citations`)

The tool accepts exactly the keys of `citation_resolution.SOURCE_TABLES`: the
kinds a writer puts in `document_citations.document_kind` (the print-citations
rollup's `govinfo_package` and `budget_volume`, and every held-field kind in
`citation_sources.TEXT_SOURCES`). The server compares case-insensitively, and
any other kind is a tool error naming the supported kinds and the closest one.
Before round 4 an unknown kind ran its SELECT and answered
`complete_held_selection` with no findings, which read as "this document cites
nothing"; three aliases (`house_activity_report(s)`, `budget_volumes`) that no
writer emits did the same with `source_read: read`. Raw SQL over
`document_citations` is not checked. The `document_kind` field description,
built from `SOURCE_TABLES` (`kind: table`), lists each kind once with the table
whose key `document_key` takes, so a new kind names its table without a
docstring edit; `govinfo_package` covers only the reports
`house_activity_reports` holds (no CHRG package has citation rows). Round 5
dropped the round-4 enum: it listed the kinds a second time against the client
budget above, and as a schema hint it never validated (the server's own refusal
does).

**Order and paging (round 5).** Rows come in `cite_kind` order, then
`CAST(span_start AS BIGINT)`, `target_key`, `rule_version` and `text_sha256`.
`span_start` is VARCHAR, as every column of the spicy-docs contract is, so the
old `ORDER BY span_start` was text order: on CRPT-118hrpt964 the first bill
citation (offset 18732) came 17th, and its four public-law rows sat at
positions 167 to 170 behind the default cap of 100. `CAST`, not `TRY_CAST`: a
span that is not an integer refuses the call instead of sorting somewhere.
`target_key` separates the rows a range citation writes at one span (273 groups
in print-citations 685f2e27) and `text_sha256`, part of the row identity, a
re-read text's rows, so the order is total and `offset` pages neither skip nor
repeat a row; a legacy file without the digest column sorts without it.
`cite_kind` (case-insensitive) selects one kind, `offset` skips rows, and
`coverage.cite_kind_counts` states the document's rows per kind, so a page
says what it leaves out. A `cite_kind` outside `citation_resolution.CITE_KINDS`
(spicy-docs' `DOCUMENT_CITATION_KINDS`, held equal by
`tests/test_citation_parity.py`) is refused naming the kinds and the ones the
document holds; any of those kinds with no rows is a complete, empty
selection, `case_docket_number` (held, never routed) included. Until the DRY
scout's S1, the check read `ROUTES`, which carried three kinds no writer emits
(`federal_register_document`, `federal_register_number`, `bioguide_id`; deleted)
and lacked `case_docket_number`.
`occurrence_selection.status` is `capped`, `last_page` (an offset page that
reached the end) or `complete_held_selection`.

**CFR part citations (round 6).** `2 CFR part 200` is keyed `2-200`, which
equals `cfr_ref` on every structural row of the part: 41 rows for 2-200
(subparts, subject groups, appendices, its table of contents), so every held
part citation read `ambiguous`. The `cfr_section` route's predicate keeps a
section row or the part's own granule (`cfr_sections.part_granule`, read from
the granule id by spicy-docs' grammar when the table is built), so the server
learns no CFR id grammar. A part held in two annual editions, or printed across
volumes (40 CFR part 60), still reads `ambiguous` with one candidate per
edition or volume, and the route's grain says so. `Route.predicate_columns`
names what a predicate reads: a selected table without one (a cfr-sections
generation built before the column, or a reverted pointer) is read as before,
every keyed row, instead of failing with `target_read_failure`. The probe is
the `SELECT * … LIMIT 0` this tool already uses for `text_sha256`, once per
batch.

**Pages and compaction (round 5, owner decision 2026-10-03).** `max_occurrences`
defaults to 25 and is at most 100; a larger request is refused with how to page
(the schema's `maximum` is only a hint to the client, so the refusal is the
server's own). The cap and the paging land together: before `cite_kind`,
`offset` and the counts, a smaller cap would have hidden more kinds. A reply
states a fixed list of occurrence fields once (`_compact_occurrences`, a reply
projection like `_query_reply_pins`): `OCCURRENCE_DOCUMENT_FIELDS`, fixed for a
document's text, in `occurrence_fields.shared`, and `OCCURRENCE_KIND_FIELDS`,
fixed for a kind's route and rule, in `occurrence_fields.by_cite_kind`; both
lists are in `occurrence_fields.hoisted`. `target_kind` or `normalized_key`
leaves an occurrence where it equals `cite_kind` or `target_key` (`same_as`).
An occurrence is `{**shared, **by_cite_kind[cite_kind], **occurrence}`;
`cite_kind` stays on every one. The list is fixed (coordinator answer 6), so a
page whose rows happen to agree on a per-occurrence field (all `found`) keeps
the same shape as the next. A listed field whose values differ in its scope (a
document holding rows of two texts) stays on each occurrence and is named in
`not_hoisted`: a stated-once value is never a guess. The acquisition queue is
built from the whole rows and then projected the same way (`_compact_queue`,
round 6 below). Nothing stored changes. Measured on the live bucket
(print-citations 685f2e27), characters per reply:

| Request | Deployed (d2b5f53) | Fixed lists (this tree) | Hoisting any agreeing field (794beed) |
|---|---|---|---|
| CRPT-118hrpt964, 100 rows | 113,534 | 47,041 | 31,644 |
| CRPT-118hrpt964, default page | 113,534 (100 rows) | 14,640 (25 rows) | 10,375 |
| CRPT-118hrpt964, all 222 rows | 283,615 (one 500-row call) | 127,422 (3 pages) | 97,989 |
| BUDGET-2025-APP, 100 rows | 148,361 | 80,933 | 65,838 |
| BUDGET-2025-APP, default page | 148,361 (100 rows) | 24,941 (25 rows) | 21,427 |
| BUDGET-2025-APP, all 4,456 rows | 893,849 for the first 500 | 4,151,031 (45 pages) | 2,998,213 |

The stable shape costs a third to a half more than hoisting any field a page
happens to agree on (`target_resolved`, `reason`, `rule_version`, ...). Without
the queue projection the 45 pages came to 5,461,495 under the earlier rule: the
queue repeated the document's pins on every requesting occurrence.

**What a reply no longer restates (round 6, owner decision 2026-10-03).** A
100-occurrence page of BUDGET-2027-APP's Statutes citations was 102,942
characters, 59% of it the acquisition queue. Every projection below is lossless
against the reply it sits in (`test_chaos_r6_server` rebuilds the whole queue):
- a queue item names its requesting occurrences by `requesting_spans`: each is
  the `span_start` of an occurrence whose `cite_kind` and `target_key` are the
  item's `target_kind` and `normalized_key`, and that occurrence states every
  field a request restated except `input_snapshot`, stated once in
  `shared_fields`;
- `QUEUE_KIND_FIELDS` (`target_snapshot`, `provider_route`, `status`) are
  stated once per target kind where the kind's items agree, a route's
  `native_identifier` leaves it where it equals `normalized_key`, and the
  queue's `coverage.resolution_coverage`, a copy of the reply's `coverage`,
  is left out;
- an occurrence leaves out `occurrence_key`, a digest of six fields it states
  (a fifth of each occurrence), and `target_rule` where it equals `cite_kind`
  (11 of the 13 held kinds on every row); `rule_version`, one per kind in every
  held document, joins `OCCURRENCE_KIND_FIELDS`. `same_as` lists only fields
  the rows hold, so a legacy file without `target_rule` is not given one.

The same page measured 49,291 characters on 2026-10-03, and the default
25-occurrence pages fell from 22,141 to 19,348 (BUDGET-2027-APP) and from
15,124 to 12,663 (CRPT-118hrpt964).

**Read statuses (round 5).** `source_read.status` separates three answers that
used to arrive as one `missing_digest` with no occurrences (`_source_read`):

- `not_held`: the kind's table has no row for the key. With citation rows (a
  document the table dropped) the occurrences read `unread_source`; with none,
  the call is refused (below).
- `not_read`: the table holds the document but records no read of it. A print
  kind's table records the read in the row: `text_sha256`, `pages_read`,
  `rule_set_version` and `citation_rows`. A held-field kind's table
  (`report_sections`, `bill_sections`, `comments`, ...) records none: the
  held-citations rollup reads only the fields an operator selects, at most 100
  a run. Its reads are recorded in `document_citation_reads` (`document_kind`,
  `document_key`, `text_sha256`, `rule_set_version`, `read_at`,
  `citation_rows`), which the held-citations pipeline publishes from its
  checkpoint beside `document_citations`. When that table is published, the
  record is the latest read (`read_at`) of the field's current text
  (`text_sha256` = `'sha256:' || sha256(field)`); until then, or with no such
  row, a held field with no citation rows is `not_read`, never an answer that
  it cites nothing. Round 5 also required the read to state its rule set, a
  rule written against a fixture before the table existed: every published
  read states no rule set and no time (13 of 13 on 2026-10-03), which the
  dictionary documents as a read recorded before the table existed, so every
  recorded read answered `not_read` (court opinion 11264529 read 0 rows).
  Round 6 dropped the filter. A read with no time sorts last, and among reads
  with none the one stating more rows first, so a disagreement refuses below
  instead of reading as none found.
- `read_none_found`: the read record states `citation_rows` 0. A read stating
  rows that `document_citations` does not hold refuses: the publication
  disagrees with itself.

Each reply defines the `target_status` words its occurrences use, and only
those, in `target_status_meaning` (`TARGET_STATUS_MEANINGS`); the tool text
points at it. The five definitions did not fit the tool text beside
everything else it must say (round 6).

With citation rows the status names the digest they are checked against:
`read`, `missing_digest` (a held row without one) or `ambiguous`. A key that
neither the table nor any citation row of the kind holds is refused; the error
names the table, says keys are exact and case-sensitive, and names any
spelling `document_citations` holds that differs only in case, under any kind.
That lookup reads `document_citations` alone, never a parent such as
`comments`. Round 5 found `crpt-118hrpt964` and `CHRG-119hhrg64503` answering
`missing_digest` with a complete, empty selection: the third time an empty
selection passed as an answer, after unknown kinds in round 4.
`coverage.partial` is true when the page was capped or offset, an occurrence
was not looked up (`coverage.reason_counts`), or the document was not read.

## Ledger qualification (`_qualification`)

`describe_table` reports the output ledger's audit for its table beside the
live pin. (`list_sources` did too, for every table, until the response-size
repair below.) The ledger is Markdown under `docs/research/`, which
the image does not ship, so `spicy-regs-dict generate` bundles
`table_qualification.json` (built by `spicy_regs.output_ledger`) and `check`
refuses a stale copy. `_ledger` reads it once per process.

- **Only for the ledger's publisher.** The record names the destination the
  ledger audits. A server reading any other base URL, or a local directory,
  reports `unknown_for_publisher` and no pins: the same pin on another bucket
  proves nothing about this ledger.
- **Pins compare by kind.** A family pin compares with the snapshot's
  `artifactDigest`; a base object's `verified at table digest` pin compares with
  its managed table's `sha256`; a `snapshot_…` pin compares with the rulemaking
  snapshot the connection pinned. A table neither pointer names has no live
  pin, and the reply says it cannot compare rather than guessing from a bare URL.
- **Separate fields, never one verified flag.** `ledger_disposition` is the
  row's word (`qualified`, `PARTIAL`, `FAILED`, `verified`) for `ledger_pin`
  only. `generation` says whether the live pin is one the ledger audited, so a
  `FAILED` generation that is still live reads as "current generation audited"
  plus `FAILED`. With no match, the ledger's latest audit is shown.
- **Closed vocabulary.** "Published at" is a publication statement, not an
  audit. A new or misspelled audit word fails the build instead of being read as
  either an audit or its absence; add it to `output_ledger` deliberately.

## Response size: a reply carries what its caller asked about

On 2026-09-28 a blind persona test (`docs/research/mcp-chaos-2026-09-28.md`)
found the replies too large for the clients reading them. `list_sources`
returned 163,699 characters: every table's publication pin, ledger audit and
relationship-view metadata. Every `query_sql` returned about 54,000 characters,
`SELECT 1` included, because it attached every table's pin
(`connection_publication`). Claude Code spilled each reply to a single-line
file its reader cannot page, so two of five personas never read a row.

- **`list_sources`** lists each table's name, label and coverage kind, and
  lists each relationship family's views once, under their shared summary.
  Since round 6 (owner decision 2026-10-03) the tables come in `subjects`
  groups by the dictionary's `subject` (`table_metadata.json`, implementer
  B's field), in the order subjects first appear in the display order, with
  the tables the dictionary gives no subject yet in a last group whose
  `subject` is null: two personas could not find the rulemaking tables among
  158 in one list. A group costs about 40 characters. Pins,
  audits and view dependencies live in `describe_table`.
  Since 2026-10-31 the entries are paged by subject (ss-ehe5): the whole
  catalog approached the reply budget. With no subject
  the reply names each subject's tables, without labels, coverage or rows
  (owner decision, ss-zt4p). The default has a 10,000-character bound;
  detailed pages retain their reply reserve. `list_sources(subject)` gives the
  full entries; the `views` and `archive` pages hold the rest.
  FEC release discovery reports the selected receipt, consumer identity and
  counts of the captured compatibility states. It retains the warning that raw
  financial qualification is not inferred. Available and unavailable view names
  remain discoverable; `describe_table` supplies a selected view's full release
  evidence and mismatch reasons. The summary is derived from the existing
  checks, not a separately maintained qualification model.
- **`query_sql`** echoes `sql` as its first key, so a reply read out of
  context, such as a client's spill file, names the statement it answers. It
  returns the pins of the tables the statement names. `_tables_named` walks
  DuckDB's unbound parse tree (`json_serialize_sql`). `cursor.get_table_names`
  binds the query and expands each view to the tables under it, which for these
  `read_parquet` views is none. The walk follows SQL scope: an unqualified name
  that a CTE in scope defines reads the CTE (a CTE body sees the CTEs before it,
  and itself only when recursive), names compare case-insensitively, and other
  schemas hold no published tables. Before this, a CTE named like a view pinned
  the view, and `FCC_FILINGS` pinned nothing.
- **Ordinary query provenance is compact, not weaker.** Preserve exact selected
  table/generation and release receipt pins, source population/as-of scope,
  named analytic purpose and essential interpretation warnings. Coverage kind
  alone does not explain financial eligibility: source-analysis eligibility,
  serving compatibility and a ledger audit are different checks. Keep the
  owner-defined limits; do not infer current/net money or group additivity in
  the server. Full dependency descriptors, consumer identity and acceptance
  evidence belong in the existing `describe_table` response, not every query.
  This is reply projection only: internal release admission, refusals and
  retained evidence are unchanged. No new evidence table or duplicate storage
  is needed.
- **Pins describe the selected connection**, not universal freshness. Managed
  `rows` come from the pinned index or snapshot manifest; snapshot `run_id` and
  `asserted_at` and dictionary coverage retain their own meanings. A later
  `describe_table` may run after a refresh: compare its exact generation and
  receipt pins with the query's before treating its detailed evidence as support
  for that earlier result. A mismatch is a different selection, not historical
  verification. Derived views embed `_publication_status` pins in
  `source_publication_json`, and `identity_candidates` hashes them into
  `candidate_id`; compact reply shaping must not change those internal pins.
- **`describe_table`** returns one column list. `declared_columns` repeated the
  column names and descriptions that `columns` already carried, which was about
  45% of the reply. DESCRIBE's null, key and default fields meant nothing for a
  Parquet view.
  Relationship metadata appears only in `metadata`. Full FEC release evidence
  appears once: under `publication.release_compatibility` for an available view,
  or `relationship.release_compatibility` for an unavailable view. Response
  shaping creates new dictionaries and leaves the stored relationship records,
  checks and publication provenance unchanged.
- **`describe_table(detail=false)` is the default since round 3 (2026-10-03).**
  The 2026-10-02 round-3 personas (`docs/research/mcp-chaos-2026-10-02.md`,
  round 3) found `bill_versions`' description at 24,600 bytes, 12,048 of them
  the joins' `measurement` records and 3,365 the bill family's ledger statements
  repeated on every table of the family. The default reply now omits exactly
  `joins[].measurement` and `qualification.ledger_statements` and names them in
  `detail.omitted`; every join still states its kind, reason, baseline counts
  and floor, and the qualification keeps its status, generation, pins,
  disposition and task ids. **A client that read `joins[].measurement` now
  needs `detail=true`**, which returns the whole record as before plus `detail`.
  Nothing else is truncated: a wide schema is still described whole.
- **A qualified FEC view's default description summarizes its release record
  (round 4, 2026-10-03).** The round-4 personas found
  `fec_receipts_net_receipts_decision` at 21,054 characters, 11,443 of them its
  dependency's whole storage descriptor (123 columns, 26 member files) and 1,703
  its 23 acceptance receipts. `fec_release.release_summary` keeps every pin and
  reason, each dependency's family and generation, and the receipts' count, and
  `detail.omitted` names `release_compatibility.dependencies[].descriptor` and
  `.acceptance_receipts` under `publication` or `relationship`. `detail=true`
  returns the stored record. Query replies use the same function narrowed to
  `QUERY_RELEASE_FIELDS`. This revises the 2026-10-02 "full release verification
  belongs in an explicit table description": the explicit description is now
  `detail=true`. Measured on the live 72 views: a median of 15,471 to 8,301
  characters (maximum 25,187 to 11,235; all 72, 1,130,695 to 609,034).
- **A query or citation reply past `REPLY_CHARS` is refused with its remedy
  (round 6, owner decision 2026-10-03).** Claude Code saves a result past
  25,000 tokens (`MAX_MCP_OUTPUT_TOKENS`) to a one-line file, and its reader
  showed 39,000 to 43,000 characters of each: round-6 personas lost the rows
  of 76,947- to 102,942-character replies. The budget is 40,000 characters of
  the reply's compact JSON text (`SPICY_REGS_REPLY_CHARS`; no per-call
  argument, which would spend every tool's description cap). A `query_sql`
  reply past it is a tool error that states its size, how many leading rows
  fit and how to ask again: ORDER BY a unique key with `LIMIT k`, then
  `LIMIT k OFFSET k`, comparing the publication pins between pages (a pointer
  can move between two calls), or `max_cell_chars`, naming the column holding
  most characters. A citation page past it states how many occurrences fit
  and the `max_occurrences` and `offset` to ask with; its size never falls as
  occurrences are added, so the count is found by halving over rows already
  resolved (about seven builds for 100, no read). The count leaves
  `RE_ASK_CHARS` (100) for the clause the reply echoes. Nothing partial is
  returned for size; the `max_rows` cut and `truncated` are unchanged. The
  owner chose refusal over cutting at the budget and continuing (the scout's
  recommendation): a caller that skips `truncated` cannot mistake a refusal
  for the whole answer. The cost is that the statement's work is thrown away
  and runs again; the server holds no cursor between calls.
- **`query_sql` states `columns` once and each row as an array in their
  order (round 6).** Column names repeated on every row were 32% of a
  399-row persona reply: ulrike's call 12 went from 76,949 characters to
  53,117, and its first 302 rows from 57,964 to 39,844. `truncated_cells`
  still names each cut cell's row and column.
- **The catalog replies are not row-shaped and are never refused or cut.**
  `test_chaos_r6_server` holds `list_sources` and every description, either
  detail, under the budget less room for live pins; on 2026-10-03 the live
  `list_sources` was 34,243 characters and the largest default description
  (`congress_bills`) 27,748.
- **`query_sql(max_cell_chars=N)`** cuts every text, list or struct cell longer
  than N characters to its first N (a list or struct as its compact JSON text)
  and lists each cut cell in `truncated_cells` as `{row, column, chars}` with the
  cell's full length, the same honesty rule as `truncated` for rows: a shortened
  value is never mistaken for the whole. Unset, nothing is cut and
  `truncated_cells` is empty. There is no server-side `offset`: the docstring
  points at `ORDER BY … LIMIT n OFFSET m`, which DuckDB already does without the
  server re-executing a statement to skip rows. Round 3 measured a 25-row
  `SELECT *` on `committee_meetings` at 204,218 bytes, 99.6% of it rows, and a
  43-row GAO result at 81,801 bytes that the client spilled to a file it could
  not page.
- **Each reply is sent once as data and once as its compact JSON text.** MCPServer's
  default text block is the structured result indented (`indent=2`), so the wire
  carried each reply twice, the second copy 1.1 to 1.7 times the first. Claude
  Code reads `structuredContent` (its spill files are compact JSON), and the MCP
  specification asks only for the serialized JSON in a text block, so `tool`
  returns a `CallToolResult` whose text is the compact JSON: `list_sources` went
  from 86,603 to 75,403 bytes and a FEC view's description from 59,148 to
  17,168 with the release summary above.
- **A managed pin states `published_at` (round 5): when the publisher moved the
  family's pointer to this generation.** It is the index's optional
  `publishedAt` family field (a UTC instant; `parse_index` refuses any other
  spelling), read with the index the build already fetches, so it costs no
  request. It is not "data as of": a generation published today can hold a
  source read last week, and a derived family can lag the parent it was built
  from. It is null until the publisher records it; the writer lands only after
  an image that reads the field is live, because the earlier `parse_index`
  required the family key set exactly and refuses the new key (a refresh would
  keep the old connection and the next cold start would raise). `_reply_pins` adds it for
  `describe_table`, `query_sql` and the citation reply's `publication`; it is
  not in `_publication_status`, whose pins derived views embed, nor in
  `list_sources`. Reading each member's Parquet key-value metadata for a time
  was measured and rejected: only `discovery_signals` records an `as_of`, and
  the reads doubled a cold build's requests to the bucket (+519, +5.2 s).
  `published_at_basis` (round 6) names what the value observed:
  `pointer_move`, stamped at the pointer write, or `last_object_write`, the
  newest object under the generation's prefix that
  `publication.backfill_published_at` wrote for entries published before the
  writer, the pointer having moved at or after it. The backfill ran once,
  applied at 2026-10-03T16:13:56Z (`PUBLISHED_AT_BACKFILLED_THROUGH`), and the
  writer stamps only later instants, so the basis is read from the value with
  no index change; another backfill moves the constant. Round 5's research
  note called a backfilled value a "lower bound", which a reader can take
  either way round; the basis says what was observed instead.
- **A derived table's pin states the parents its producer recorded (round 5,
  owner decision 2026-10-03; round 6).** A generation root (`artifact.json`)
  records `spec.parents`: each parent table's family, generation and bytes, or
  a storage version (an ETag, or a local copy's digest) for a parent no family
  pins. None recorded is not none: a read that bypassed the download helper is
  not recorded (scout C's read ledger is the pipeline half). `_input_lineage`
  adds `inputs`, one `{table, family, built_from, live, input_table_current}`
  per parent, and `inputs_current`; `built_from` and `live` are always pins of
  one kind.
  - A managed parent's are family generations; `input_table_current` compares
    the parent table's own `sha256` where both pins state one (else the
    generations), so a parent family that moved for another table does not
    mark this one stale. The name says so (coordinator answer 7): `built_from`
    can differ from `live` while `input_table_current` is true.
  - A parent in the table's own family is its previous output, read to carry
    rows forward (`generations._check_parents` requires the generation the
    build captured): it is `prior_generation`, never an input that can lag.
    Round 5 compared it with the table's live bytes, so `gao_reports` read
    stale against itself. Roots already published are immutable, so the
    server reads them this way whatever the writers record next.
  - A parent no family pins (the comments export) is compared with the export
    receipt the connection matched at build (`_export_pin`): the recorded
    sha256 with the receipt's sha256, or the ETag with its ETag. A file that
    moved since its receipt, or no receipt, gives `live: null`.
  - `inputs_current` is three-valued (`_all_current`): false if any input
    lags, else null if any is unknown, else true. Round 5 dropped unknowns
    from the vote, so a null beside a true read as true.

  A rulemaking snapshot table states its snapshot's sources
  (`_snapshot_lineage`): the manifest records `inputs.sources` per snapshot,
  and its stages record `depends_on` and `outputs` but not what each stage
  read, so one table's own sources cannot be derived. They are
  `snapshot_inputs` (`{table, built_from, live, input_table_current}`, table
  sha256s), and the table's `inputs_current` is true when every source is
  live, else null: never false for a source it may not have read (round 6
  review, item 3). Its `prior_generation` is the manifest's
  `previous_snapshot_id`. Per-stage sources would let each table state its
  own; that is the pipeline's change.

  A family whose root records no parents gets no `inputs` key. The root is read on the
  first reply that pins the generation, never at build, and kept per artifact
  digest (`_ROOT_PARENTS`; roots are immutable); a read that fails states
  `inputs: null` with `inputs_status: root_unavailable`, and the root is not
  read again for 60 s (`ROOT_RETRY_SECONDS`, `_ROOT_FAILED_AT`), so a failing
  bucket costs one GET a minute per generation, not one per reply. The read is `publication.read_pinned_root`: the
  image does not install rulespec-artifacts, so the server checks that the
  root at the pinned prefix names the pin, as it trusts pinned member URLs,
  and does not recompute its digest (`load_family_root` does, for lineage).
  A local download holds no roots: its pins state `root_unavailable`. Not in
  `list_sources`. On 2026-10-03, 10 of 52 families recorded parents, and
  bill-subjects (`congress_bills`) and member-vote-terms (`members`,
  `member_terms`) were built from parent bytes no longer live. Tests keep root
  reads off the network (`tests/conftest.py::no_generation_roots`).
- **`list_sources` states each table's pinned `rows`** (the index descriptor's or
  the snapshot manifest's count; null for a legacy table no pointer pins) so a
  declared table whose generation publishes no rows is visible at discovery
  without a describe call. It is the pinned generation's count, not a freshness
  claim: a later data run lands after the pin.
- **A comments export states its receipt's rows, labelled (round 4).** `comments`
  and `comments_index` are served from fixed URLs no pointer pins, so their
  `rows` were null while `comments-publication.json` stated them. A file whose
  ETag and size matched the receipt at build carries the receipt's `rows` with
  `rows_basis: comments_export_receipt`; a moved file carries `rows: null` with
  `rows_basis: export_receipt_does_not_match_object`. `describe_table` and
  `query_sql` pins add `export_receipt` (receipt digest, file digest, ETag,
  bytes, catalog snapshot). `catalog_snapshot_id` is a string: an Iceberg
  snapshot id passes 2^53, and a JavaScript client read 3797331542152182418 as
  a JSON number and cited 3797331542152182300 (round 5, oyelaran). The status
  stays `legacy_unversioned`: the facts
  join the reply in `_reply_pins` only, never `_publication_status`, whose pins
  the comment views embed in `source_publication_json`. A matching ETag proves
  the object is the one the receipt names, not that the receipt's count is
  right; that check belongs to the mirror export, before it writes the receipt.
  The match is measured at build: until the next poll sees a new receipt, a
  statement can read a newer file than the labelled count describes.

## Relationship-view column meanings (`view_columns`, `relationship_views.lineage`)

A derived view's column has one of three meanings in `describe_table`, in this
order: the spec's `column_descriptions` entry (an `SQLView` field; an
`ArrayRelationship.details` triple), the shared registry in `sql_views`
(`source_ordinal`, `target_key`, `source_table`, …), or the dictionary meaning
of the dependency column it projects unchanged. A column none of them covers has
`description: null`. The round-3 personas found 822 of 1,381 columns on the
non-FEC views carrying a sentence manufactured from the column name ("Term
match; its meaning and source grain are described by this view"); that fallback
is gone, and `tests/test_chaos_r3_server.py` describes every view over
dictionary-typed empty tables and refuses an undescribed column.

**Inheritance is by lineage, never by name.** `lineage.column_lineage` walks
DuckDB's own parse tree (`json_serialize_sql`) when a view is bound and records,
in the pinned relationship record's `column_lineage`, which output columns are a
bare projection (aliased or not, or a star expansion) of a dependency column,
through CTEs, subqueries, lateral joins and views over views. A UNION keeps a
position's origin only when every branch agrees; an aggregate, CASE, cast,
constant or struct field has none, so `fec_relationship_evidence.collection_id`
(computed from a locator) does not inherit `fec_source_records.collection_id`,
and `min(purpose)` does not inherit `purpose`. The serving connection runs no
EXPLAIN for this; the walk is a parse. A table function or VALUES list without
column aliases has unknown columns, and an unqualified name beside one resolves
to nothing rather than to a guess. The review of 2026-10-03 (`phase3-review.md`)
replaced an earlier name-based design with this one.

**Debt: the view meanings are not in `table_metadata.json`.** Generating the
relationship-view entries at `spicy-regs-dict generate` time would give one
dictionary file and let its decay lint cover view prose, but it needs the
generator (`data_dictionary.py`, another lane's file) to bind every view over
dictionary-typed tables, a placeholder FEC source pin for the 72 qualified
views, and a rule for text that lives in two places. Until then the specs hold
the view meanings and the server describes a view on request.

## DNS rebinding protection is off in `build_app`

Deliberate. The active deployment is reached via `mcp.spicygov.ai` and its
configured `workers.dev` address; the SDK's default localhost-only allowlist
would reject those hosts with **421**. `build_app` passes the setting to
`streamable_http_app`. This is a public, stateless, read-only endpoint, not a
privileged localhost service. The SQL and selected-file guards remain required.

## Other invariants

- `ICONS` uses a base64 `data:` URI, not an `https://` URL, so it works on both
  the stdio and HTTP transports. Generated by `scripts/gen_icon.py`.
- `_resolve_catalog_config` and `_resolve_r2_base_url` reject quotes,
  backslashes, and control characters outright rather than escaping them, because
  the values are inlined into `CREATE SECRET`/`ATTACH`/`read_parquet`, which take
  no bind parameters.
- `R2_CATALOG_NAMESPACE` uses `or DEFAULT` rather than `get`'s default argument so
  an env var set to the empty string falls back to `default` instead of `""`.
