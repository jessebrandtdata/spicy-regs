# Administration-policy archive observations

`administration_policy_statements` lets a legislative researcher find the
statements that a pinned community archive associates with congressional bills.
`administration_policy_bill_links` provides each explicit bill-link occurrence.
The maintained SpicyDocs administration-policy reader supplies the native facts;
these tables add queryable rows and generation-bound ETL receipts.

A statement can have no identified bill, several bills, or repeated bill links.
Every statement remains in the first table. Link rows preserve source order and
repetitions. `native_bill_id` preserves `<native bill>-<Congress>` from the
archive reader. The maintained bill identity code converts `bill_id` to
`<Congress>-<type>-<number>` for joins. It does not assert that every linked bill exists
in a particular published bill generation.

The source is `unitedstates/statements-of-administration-policy`, a community
archive. `rescinded` is its assertion. `source_record_json` retains every native
field, including unknown additions and external page URLs. Literal issue dates
and their parser status remain separate. An archived PDF URL is a declared
locator; this integration does not acquire PDF bodies or infer political positions.

## Build a selected candidate

Use the maintained environment with source-reader dependencies available:

```sh
uv run --frozen --no-sync python -m spicy_regs.sources.retained_collections \
  --manifest /absolute/path/to/selection.json \
  --output /absolute/path/to/new-candidate
```

The manifest uses `format: spicy-retained-collection-inputs`, `version: 1`, and
`sources`, a bounded nonempty list. Every selected entry has:

- `kind: administration-policy`, a full lowercase `commit`, and `administration`
  such as `47-Trump`;
- `path` to retained YAML or losslessly compressed `.gz` bytes (relative paths
  resolve against the manifest), with original decoded `sha256` and `bytes`;
- `url`, exactly the selected commit's metadata URL, and `observed_at`, the
  acquisition observation recorded in retained evidence.

The caller must qualify the declared commit, URL and acquisition time against
retained acquisition evidence. A digest verifies selected content; it alone
cannot prove where or when the bytes were acquired. A filename alone proves none
of those facts. This builder makes no network request.

The builder refuses mismatched input hash/size, duplicate commit/administration
selections, unsupported kinds, malformed source records, and byte-bound
violations. It checks code, manifest and originals again before publishing the
candidate through the maintained no-replace helper. Failure removes temporary
rows; existing candidates survive.

`statement_id` identifies an observation by metadata URL, decoded input hash and
source record index. `source_record_id` retains the archive reader's commit-pinned
locator. `link_id` adds the source link position. Changed captures and archive
commits produce separate observations; no cross-edition deduplication is implied.

Outputs are table directories containing Parquet rows, `etl_receipts.parquet`,
`selection.json` and `build.json`. The build receipt records exact inputs,
processing code/runtime identities, row counts and output hashes. Every output
row has a witness pointing to its source capture and native record position.
A table with no rows still has a schema-bearing empty Parquet subject and a
zero-row entry in `build.json`; release packaging must preserve that distinction.

## Refresh from the archive

`spicy_regs.pipelines.administration_policy` is the maintained caller around
that builder. `acquire` reads each selected administration's metadata file at
one full archive commit through the `CaptureEvidence` transport, so every file
lands in the central raw archive with a receipt, and writes a capture
selection (commit, URL, SHA-256, size and observation time per file). Given
the published statements table, it reads every administration of that table
too and compares bytes: an unchanged file keeps its published capture, so its
`statement_id` values stay as published, and a changed file takes the new
commit's capture in its place. A new administration file is named explicitly.

```sh
python -m spicy_regs.pipelines.administration_policy acquire \
  --commit "$ARCHIVE_COMMIT" --prior-statements "$PUBLISHED/administration_policy_statements.parquet" \
  --output-dir "$RUN_DIR" --captures-out captures.json
python -m spicy_regs.pipelines.administration_policy build \
  --captures captures.json --output-dir "$POLICY_OUTPUT"
```

`build` writes the builder manifest over the raw originals, builds the
candidate in `$POLICY_OUTPUT/candidate` and admits its tables and receipts as
`$POLICY_OUTPUT/generation`. The generation pins a source-evidence artifact
that names each selected file's commit, URL, digest and observation time. That
evidence carries no file body: the archive states no licence, so the bodies
stay in the raw archive. The refresh sequence through publication is in
[local-family-refresh.md](local-family-refresh.md).

## Qualification and release

Compare all selected statement JSON with the maintained reader, verify every
bill-link occurrence and foreign key, validate dataset receipts, and record
schema/coverage limits. This proves the selected metadata files. Publisher-wide
coverage, held PDF-body coverage and downstream bill completeness remain separate.

A local candidate does not change a published table. Submit source bindings to
the catalog owner and release candidates to the release owner. Public exports
must exclude local input paths from `selection.json` and `build.json`.

Other retained collections have separate source-native units. SEC, FERC and CFTC
comments are not regulations.gov comments merely because their files contain
comments. EDIS documents are investigation filings; direct Supreme Court term
renders are separate observations from CourtListener opinions. CBO-linked
BILLSTATUS archives belong to the bill source route. Inspector-general metadata
is independent of GAO/CRS and current Oversight.gov pages. See the G execution
ledger's `decisions.md` and `bindings-proposed.json` for current dispositions and
the evidence behind each decision.

## Finite native SEC captures

`spicy_regs.sources.retained_native_collections` builds `sec_rulemakings` and
`sec_comment_files` from an explicit offline selection. It uses the maintained
SpicyDocs page parsers and comment-file validator. It preserves form-letter
sections separately from comment occurrences, repeated listing declarations,
literal dates and commenter names, and every source capture witness. It does
not infer Regulations.gov identities or copy PDF bodies into table output.

The manifest has `format: spicy-retained-native-inputs`, `version: 1`, and a
bounded `sources` list. Each `sec-comments` unit names `docket`, ordered complete
`listings`, exact distinct `files`, and `expected_files`. Optional `rule` supplies
publisher-stated discovery. Without it, the selected listing must match the
maintained legacy URL grammar and the output records that derived route without
inventing a rule page. Capture declarations carry `path`, `requested_url`,
`resolved_url`, `status_code`, `content_type`, `observed_at`, decoded `bytes` and
`sha256`. The caller qualifies these declarations against retained acquisition
records; this builder checks bytes and source shape, not historical provenance
independently of those records.

```sh
python -B -m spicy_regs.sources.retained_native_collections \
  --manifest /absolute/path/to/selection.json \
  --output /absolute/path/to/new-candidate
```

The adapter refuses a nonterminal or reordered listing chain, wrong docket,
missing/extra/duplicate file selections, failed captures, changed input bytes,
invalid source formats, and manifest/code drift. Table rows and generation-bound
receipts use the maintained ETL writer with its reference validator for these
small finite selections; local publication refuses an existing
destination. Private `selection.json` and `build.json` contain local locators and
must remain outside public packaging. A complete selected docket is distinct
from publisher-wide history, source acquisition, public serving and release.
See `metadata/source-catalog-program/20261010/execution-20261012/G/` in the
canonical corpus for the exact finite selection and executed qualification.

## Finite native FERC originals

A `ferc-originals` unit names the recorded search `scope` (`docket`, `class`,
`type`), ordered `pages`, complete `files`, and `expected_files`. Each file names
its publisher `accession`, `file_id` GUID, literal `filename`, `declared_bytes`,
GET `metadata` capture and POST `original` capture. POST declarations also carry
`method` and the original UTF-8 `request_body`; those bytes remain in private
build evidence. The adapter supplies retained responses through the maintained
FERC reader's acquisition hook. Its normal search traversal checks pagination,
counts and unique references; its original-file validator checks public
availability, accession, GUID, size, media type and PDF structure. A transport
that refuses every request prevents acquisition.

The tables `ferc_search_documents`, `ferc_accession_files` and
`ferc_original_files` keep source identities separate from SEC and
Regulations.gov. Search and file-list rows retain the exact `source_page_json`
and a `record_index`. This preserves decimal numbers, unknown fields, literal
affiliations and the publisher's `acesssionNumber` spelling without conversion.
Original rows link each retained body hash and size to its stating search and
file-list witnesses. A selection must include every search-stated transmittal
and matching file-list row; missing originals and source failures refuse the
whole candidate. Failed acquisition records remain in their original evidence;
this successful-input selection does not manufacture successful rows for them.


## Finite native CFTC comments

A `cftc-comments` unit supplies its numeric `rule_id`, complete ordered
`listings`, explicit `scope`, `details`, `files`, and `expected_comments`.
`listing-only` retains the native listing fields and marks details
`not-selected`. `details-and-files` requires every listed comment detail and
every attachment those details state. Missing, extra, repeated, misidentified
or incomplete inputs refuse the candidate.

The maintained CFTC scoped listing walk checks pagination, stable stated totals
and unique comment IDs. Retained GET captures replace acquisition through the
same bounded validation hook used by FERC. Native listing and detail parsers
retain literal names, dates, organizations, citation text and letter links.
The tables `cftc_rule_observations`, `cftc_comments` and `cftc_comment_files`
retain those complete native values, scope and ordered source witnesses. Letter
rows also retain historical provider evidence; replay makes no new direct or
proxy acquisition claim. PDF validation reuses the source reader's maintained
shared PDF gate. These legacy portal selections do not establish coverage of
modern Regulations.gov records.

The shared offline hook compares POST request JSON through the maintained
lossless evidence codec. It allows whitespace and object-key-order differences
while preserving numeric and Boolean types; integer zero cannot substitute for
Boolean false.


## Finite EDIS document and retained ZIP

An `edis-document-bulk` unit supplies one public `document_id`, its GET
`document` and `attachments` XML captures, all `expected_attachment_ids`, and a
retained `archive` with exact artifact ID, byte identity, source URI and complete
`retained_member_expectations`. The ZIP lacks a retained acquisition observation
time. Its blob declaration therefore keeps `observed_at: null` and
`observation_time_state: not-recorded-in-retained-transfer-evidence`; it does not
manufacture HTTP capture fields or borrow a poll timestamp.

The maintained EDIS XML parsers retain native document and attachment values.
The maintained bulk reader validates the entire ZIP against all stated
attachments, including public index rows, complete membership, sizes, CRCs,
digests and PDF headers/trailers. It inflates members without extracting files.
A shared bounded byte reader verifies both captured inputs and retained blobs;
ZIP decoded member bytes also count against the cumulative read bound.

`edis_documents` and `edis_attachments` preserve complete native JSON and raw
XML in base64. `edis_attachment_files` preserves each literal index row, raw
index, member inventory and decoded PDF identity, with ordered document,
attachment and ZIP-member witnesses. The index's `337-1458 - Violation` and its
date strings remain literal. Separate investigation number/phase and date
fields stated in document XML remain independent native observations. These
rows qualify one selected document, not a complete investigation docket.

## Direct Supreme Court render observations

A `supreme-court-observations` unit supplies retained `indexes` with `term_year`,
`observation_key`, `blob` and `locator_binding`, plus selected PDF `files` and
explicit expected input totals. Every render remains separate. The maintained
term parser checks the page's stated term and dates; every native opinion,
missing link, decree release label, holding, revision link/date label and volume
`page_start` survives. The adapter infers no volume end page or CourtListener ID.

`supreme_court_index_observations` keeps each complete native index;
`supreme_court_opinion_observations` keeps each native row tied to that render.
`supreme_court_pdf_observations` validates selected retained probe bytes with the
maintained PDF reader and preserves every stating opinion/revision occurrence.
A selected resource must appear as a literal link in a selected index. The PDF
scope is the selected probes; it does not claim all index-linked files acquired.

These retained blobs lack local capture-completion clocks. `observed_at` stays null; safe
recorded HTTP Date, Last-Modified, media, length, ETag and status facts remain
separate from acquisition time. Session/cookie headers stay outside table data.
An optional `recorded_request_metadata` stores caller-qualified historical
requested URI, status, size and digest in `recorded_request_metadata_json` on
index and PDF observations. The adapter checks these facts against the selected
blob. An individual request-start instant stays separate from capture completion;
a session batch clock belongs to the manifest and remains unassigned to individual
renders. Recorded request facts do not establish a resolved URI or redirect
history. Source-native links remain evidence distinct from a newly captured
response. Public packaging excludes the
private manifest/build input paths as with other finite candidates.

## Inspector-general archive metadata

An `inspectors-general-metadata` unit supplies an immutable Git `commit` and a finite `reports` list, with each original metadata capture, archive member path and Git blob SHA-1. The adapter checks the exact commit URL, canonical archive path, retained SHA-256 and size, and Git blob identity. It reuses the maintained inspector-general metadata parser.

`inspectors_general_report_metadata` retains the complete parsed record and exact original JSON bytes in `source_record_base64`, including unknown fields and numeric spelling. Archive path year, the record's `year` and `published_on` remain separate. `declared_assets_json` and `declared_links_json` preserve what the metadata states; they do not establish that asset files were acquired or are present in an archive. Unreleased reports can preserve their landing link with no asset link. Scope stays metadata-only.
