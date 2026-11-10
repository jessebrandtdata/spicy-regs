# Changelog

All notable changes to the Spicy Regs data pipeline are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Entries link to the pull request that introduced the change.

## [Unreleased]

### Fixed

- **A table that kept its rows and columns is not refused for halving in size**
  (ss-kqzz). The `fec_committees` nightly was refused on 2026-11-09 for shrinking
  from 10.2 MB to 4.0 MB (ratio 0.392) with the same 89,974 rows and 16 columns:
  a writer change cut its row groups from 1,767 to 176 and its footer from
  3.03 MB to 0.31 MB. A table whose index shows it kept its prior's columns and
  at least its prior's rows in every partition may now shrink to 0.2 of its
  prior's bytes; a lost row or a changed column still faces the ordinary 0.5.
  The rule reads counts, names and types, never values. The 0.2 floor refuses
  rows kept with nearly all their values lost (that table with every value but
  its key nulled is 0.163 of itself), and one emptied column still publishes.

- **A split table's partitions are matched whatever order their columns are
  listed in** (ss-biqd). The stored index sorts each partition's columns and a
  new generation lists them in directory order, so a table split by
  `report_year` then `report_quarter` read as missing every partition its prior
  held: all 42 of `house_disbursements` on the live index. Its next publication
  would have been refused. A partition that is really missing still refuses, and
  the message names it as its directory spells it.

- **The four regulatory summaries build again, published without receipts** (ss-nxew).
  `agency_stats`, `agency_monthly_volume`, `discovery_signals` and `feed_summary`
  had a field policy installed on 2026-10-04 (64141879) but no receipt writer, so
  admission refused every build and the four stayed at their 2026-10-03
  generations. Their policies are removed. Each summary is rebuilt whole from
  published parents, and its generation pins the parents it read. The catalog's
  `etl_receipts.datasets` no longer lists the four; no field changed shape, and a
  consumer holding a copy re-vendors.

- **A new reading of an unchanged print replaces the old one** (ss-fmxo). When a
  derivation changed a print's text digest but its PDF bytes had not changed,
  `document_citations` and `bill_committee_actions` kept every finding under both
  digests (16,201 and 4,182 repeated rows on 19 reports in one run). The new
  reading now takes the old one's place when the checkpoint records the same
  source SHA. A real revision keeps both. Legacy checkpoints without that hash
  preserve ambiguous earlier readings and journal the unresolved source identity.

- **Uploads send one checksum per request** (ss-7lrn). R2 refused every upload
  carrying Content-MD5 beside a SHA-256 checksum ("You can only specify one
  non-default checksum at a time"), which stopped all publication from
  2026-10-31. A probe found R2 also answers NotImplemented to a SHA-256 checksum
  on UploadPart, so uploads send Content-MD5 alone, the shape R2 accepted before.
  Matching-layout multipart MD5s verify without a stored-body read. Unknown or
  mismatching layouts stream through the strong-digest check. Both test stores refuse what R2
  refuses.

### Added

- **`committee_report_cbo_letters`: every CBO letter a committee report
  reprints, one row per letter** (ss-uep5; owner answers 2026-10-31, Q1-Q4).
  - **Rows.** Keyed `(package_id, part_id, letter_seq)` and joined to
    `committee_reports` on `(package_id, part_id)`, the shape of
    `report_sections`. An omnibus report is one row per letter
    (CRPT-105hrpt149 has 11); a 1997-2011 letter's span runs through its
    enclosed estimate's "Estimate approved by ... Analysis." line, and
    `letter_date` is the letterhead's dateline, NULL where none is printed
    (every 118th-Congress letter).
  - **`committee_reports` moves to `legislative-documents/2`.** Its five
    per-letter columns (`letter_span_start`, `letter_span_end`,
    `letter_text_sha256`, `letter_end_rule`, `letter_signatory`) move to the
    new table, and it gains `cbo_letter_count` (NULL where no rule ran).
    `estimate_heading` (and its receipt-only rule) stays in place with a
    two-sided meaning: the part's first estimate heading where no located
    letter opens on it, NULL where one does, so none of the 144 live
    `estimate_heading` values is lost (115 stay on the part row, 29 move to
    their letter's row; checked against generation `670476640e83` over every
    live row's body). A prior published under `/1`
    restores in its exact shape (`legislative_policy_history.json`); the
    `cbo=` and `sections=` rule versions re-read every report once.
  - **Needs the SpicyDocs release that carries the reader** (0.67.x, the
    landing agent's). This branch was gated against a locally built wheel of
    spicy-docs `wf/cbo-letters` substituted in the working tree only; the
    vendored reader is unchanged here.

- **`gao_reports`: GAO's major-rule reports from 1996, and what each report's
  letter states of its rule** (owner decisions 2026-10-04; SpicyDocs 0.57.0's
  readers).
  - **Rows.** A run given finished walks of GAO's "Reports on Major Rules"
    (`GAO_MAJOR_RULE_RUN`) and of its index of 2000-12-15
    (`GAO_MAJOR_RULE_OLD_INDEX_RUN`), with the Month in Review walk
    (`GAO_LISTING_RUN`), adds the major-rule reports of 1996 to 2008 under the
    routes `gao_major_rule_listing` and `gao_major_rule_index`, and the AIMD-
    and GGD- products that listing also states as ordinary rows. A title the
    Month in Review cut is made whole, and a report GAO's listings state is
    labelled Federal Agency Major Rule Report whatever route holds its row.
  - **Columns.** `major_rule_agency`, `major_rule_rins` and
    `major_rule_fr_citations`, read from each report's letter in a capture
    named by `GAO_MAJOR_RULE_LETTERS`. The lists are not pairs. A blank is NULL
    and its reason is in the row's receipt, with each value's place in the
    letter. `gao_reports` moves to policy `government-sources/2`; a prior
    published under `/1` is read once and rewritten. Every retained row's
    `subject_version` changes because its subject gains three keys, even
    when their values are NULL; `record_id` and identity stay stable.
  - **Evidence.** Product pages, including old-index pages, known-page
    checks and refused pages, publish only hashes and request metadata.
    Their original local captures remain intact. GAO listing pages retain
    their bodies.
  - **Route changes.** Where a major-rule listing takes over an R-package
    row, its title replaces the package title and its route replaces
    `gao_r_package`; the package fills only cells the listing leaves NULL.
    A GovInfo row keeps its title and route but takes the major-rule label
    where GAO lists it as such.
  - **Product pages, built and off.** `GAO_PRODUCT_PAGES=true` reads the
    newest rows missing a count, a page count or subject terms from GAO's
    product pages through Zyte, a bounded number a run, the page outranking
    the R package. `rollup-gao-reports.yml` passes `false`, and the pass
    refuses to run under a reader rule before `gao-product-page-details/2`
    (SpicyDocs 0.57.0). When on, each run reads two pages whose values are
    known first and stops if either reads otherwise; a refusal writes
    nothing; a product is not read on its release day, and in its first
    week only where its page agrees with `gao_recommendations`. A read
    records what it replaced, and `GAO_PRODUCT_PAGES_UNDO` puts a day's or a
    rule's reads back. The rollup's cron moves from 17:00 to 17:20 UTC, after
    `rollup-gao-recommendations`.

- **`fcc_proceedings`: three ECFS proceeding fields added, four empty ones dropped**
  (ss-17vd). `date_nprm` and `date_reporting_and_order` are VARCHAR columns
  copied from the proceeding's selected ECFS document with no date parsing,
  like `date_created`. `rule_section` is published with leading and trailing
  whitespace stripped and a blank value as NULL (owner decision 2026-10-31):
  ECFS writes an empty string or a single space for "none" and pads some
  values; each receipt's `raw_record` keeps the value as written. The four
  comment-window columns (`comment_start_date`, `comment_end_date`,
  `reply_comment_start_date`, `reply_comment_end_date`) are removed: ECFS
  never filled them (0 of 21,724 live rows; 26,517 of 250,251 retained records
  carry the keys, all null). The table moves to policy `government-sources/2`
  once for both changes; receipts written under `/1` still validate against
  its frozen descriptor. Nothing is backfilled: each daily run re-walks every
  proceeding, so the next run republishes the whole table, and every row's
  `subject_version` changes then. The rollup's unused `fcc_since` dispatch
  input is removed; proceedings are always walked whole.

### Changed

- **`comments` and `comments_index` publish without ETL receipts** (ss-juz8,
  part of ss-vse3). The comments family's generation has no `etl_receipts.parquet`
  and its index entry no `etlReceipts`; the generation's two tables keep their
  columns, and no field moves, since the live receipts held none the table lacks.
  The mirror copies the pinned catalog table, checks the copy holds every row
  the snapshot does, and no longer rebinds, admits or replays its 26.5 million
  receipts: 138.5 of 169.6 measured minutes at full size. A receipt-only catalog
  commit no longer republishes. The last generation with receipts, the
  2026-10-10 one, is pinned in `docs/comments-receipt-archive.md`; the catalog
  keeps writing its own receipt table. Requires the installed comments policies
  to be marked receipts-retired first. `prepare_comments_selected_export.py
  --publish` (the 2026-10-10 v1 promotion) now refuses.
  **The fixed files change shape.** The first receipts-free publication is the
  first since 2026-10-03 to rewrite the bucket-root `comments.parquet` and its
  180 agency files, which still hold `attachments_json`, the attachment list as
  JSON text. They take the generation's shape: `attachments`, a list of
  structs, in the same position, every other column unchanged. A reader of
  those files that parses `attachments_json` must read `attachments` instead.

- **`federal_register` and `fr_docket_links` publish the fields their receipts
  held** (ss-vse3, step 1 of moving both tables off per-row receipts).
  `federal_register` gains, as its last columns, `html_url`, `pdf_url`,
  `body_html_url`, `source_sha256` and `regulations_dot_gov_info_json`;
  `fr_docket_links` gains `normalized_docket_candidates`, a native list like
  its others (the receipts held it as JSON text). The first
  scheduled run fills them for every row from the prior generation's receipts,
  the only copy for the 99% of rows whose API response was never retained.
  `source_sha256` names the retained API page a row was last read from and is
  NULL on a row not read since; it changes nightly on every row in the
  120-day re-read window, so compare generations without it. `pdf_url` stays
  NULL on about 33,500 rows where the Register serves no PDF. Policies move to
  `regulations-native-v3` and `-v2`; the earlier ones stay readable, so nothing
  is replayed. **The first night's `federal_register` job takes about 55 to 80
  minutes instead of 29**: the prior's schema differs, so the restore reads it
  by row once (28.7 and 56.2 minutes of restore on the two earlier hosted
  nights that did, 2026-10-31 and 11-01). The checks to run afterwards are in
  [docs/federal-register-receipt-archive.md](docs/federal-register-receipt-archive.md).

- **`fr_docket_links` and the rulemaking materializer read the published
  Federal Register tables, not their receipts** (ss-vse3, step 2). The links
  rollup fetches the pinned `federal_register` table instead of restoring it
  row by row (307 of that job's 937 s on 2026-11-09); the generation still
  records the table as its parent. The materializer reads `federal_register`
  and `fr_docket_links` as published and gives its stages the Register's lists
  as JSON text; its manifest records each one's publication pin and no receipt
  pair. The materializer is scheduled, not manual: `_regulations-refresh.yml`
  runs it inside the ETL refresh, so the first refresh after the merge takes
  this path unattended. Both tables still publish receipts. Run alone before
  the Register job has published its promoted columns, the links workflow
  refuses with a message saying so.

- **A rollup can declare that a published table no longer has receipts**
  (ss-vse3). `retired_receipt_datasets` on a rollup lets exactly those
  datasets leave the family's receipt ownership once, each keeping its table,
  and refuses receipts for them afterwards. Nothing declares one yet.

- Prepared native conversions publish by an operator-named receipt digest.
  `--publish-prepared` now requires `--expect-receipt-digest`, the
  `prepared receipt digest` line the preparation printed (never copied from
  `conversion.json`). The prepared seal moves to version 2 because it now
  carries the admission identity: `validationIdentity` (the code, runtime and
  platform the preparation started with) and `admissionKey`. A publisher under
  the same key keeps every byte, structure, membership, lineage and policy
  check and skips row admission and original restoration;
  `--full-verification` forces them, and any other key takes the full path.
  Version-1 seals still publish, always through the full path. A preparation
  whose code or checkout changed while it ran refuses to seal and resumes
  with `--resume-generation` from a clean checkout of current main plus
  `--expect-producer` naming the preparation's checkout. Publication reads
  the published generation back without downloading or admitting it again
  (owner decision, 2026-10-31): the public root and manifests are compared
  byte for byte, and each member by one HEAD against the hash storage
  computed, its SHA-256 checksum or, for a single-part object, its MD5 ETag.
  A member with neither (a multipart object stored without a checksum)
  streams through a SHA-256 from the public URL instead. Every upload now
  sends a SHA-256 checksum (`ChecksumAlgorithm=SHA256` and a checksum per
  part for multipart) beside Content-MD5. The receipt's
  `read_back.anonymous_read_rows` is replaced by `read_back.public_bytes`
  (members, bytes, `verified_by` counts per hash and `streamed_bytes`);
  `read_back.table_rows` still records the public pointer's counts.

- Native family conversion reads the replaced generation's source evidence
  before it downloads or hashes any table, so a family whose inherited
  evidence is not current completed-build evidence (another kind, an
  unfinished build) is refused at once, with the same message and exit 1,
  instead of after every table is held. `--publish-prepared` and
  `--resume-generation` check it before they hash retained tables. Evidence
  written before `evidence_version` 2 (unversioned or 1) no longer refuses a
  conversion (owner decision, 2026-10-17): it stays with the generation it
  describes, nothing is handed on from it, and the converted generation gets
  version 2 evidence of its own.

- Selected native inputs record their publication pin from the build's captured
  index alongside admitted subject, receipt, and processing hashes. Dynamic
  member reads now reach generation parent tracking. Local selections retain
  file witnesses; inputs with no published subject state that disposition.

- **A House member vote of 1990-2002 carries a `bioguide_id` where one member
  answers to the Clerk's label** (owner decisions, 2026-10-05; rule
  `house-name-crosswalk-v3`, `transforms/member_name_crosswalk.py`). Those
  files name a member by a label, a party letter and a state, so their
  `member_votes` rows key `name:` and held no id. At the votes merge each
  distinct (Congress, label, party, state) now resolves to the one member of
  that surname, state and party with a House term in that Congress, read from
  `members`, `member_terms` and `member_party_affiliations`. A printed first
  name chooses among several, by the crosswalk's first name or by the nickname
  its source states, which is read once `members` publishes that column and
  ignored until then. A row is filled only on a day inside that member's term
  and only once per member per roll call. Where several members still answer
  to a label, as a successor of the same surname does, the row takes the one
  seated on the vote's day; where the day seats several, a label that prints
  no first name takes the member left once those the same roll call lists
  under a first name are set aside. Any other row stays NULL, with its reason
  in the run's `member-name-crosswalk` journal event, which also records the
  rule version, the inputs, whether they state nicknames, and each
  resolution's rule and matched term. `member_key` does not change. Measured
  on the live tables of 2026-10-04: 3,173,468 of 3,175,523 rows resolve with
  `members` as published, and 3,175,521 with the source's nicknames;
  `member_vote_terms` matches a term for them on its next run. Run with the
  Clerk's ids hidden on the 108th-119th, the rule filled 6,567,134 of
  6,567,152 rows and differed from the Clerk on two, where the Clerk's file
  states a dead member's id (109-house-1-239 and -240). Receipt:
  `~/Work/corpora/fork-execution-2026-09-21/votes-person-crosswalk-2026-10-05/`.

- **SpicyDocs 0.52.0, adopting 0.51.0 with it** (main `b08ac1b`, wheel
  `374b8c20`). What each release entry asks of an importer, and what moves:

  - **Stages.** `congress_bills.stage` and `bill_actions.stage` read a bill's
    actions in publisher order, with `failed` and `vetoed` stages and a
    "Became law" label. Replayed on the live generation of 2026-09-29, 154,374
    of the 172,991 stages with actions move once each bill's Congress is
    re-read (92,981 `introduced` to `committee`, 49,264 `other_chamber` to
    `committee`, 5,682 `committee` to `other_chamber`). None emits a
    `stage_changed` event: a prior stage is re-derived from the bill's
    published actions under the running rule before events compare (owner,
    2026-09-28), so only a bill whose actions moved emits one.
  - **CBO for every Congress.** Each run reads CBO's keyless feed once for
    every Congress it is scoped to and merges its rows (`source` `cbo_feed`)
    with the BILLSTATUS rows, BILLSTATUS keeping every identity both state,
    against this run's rows and the prior ones. `title_bill_id`, `found_by`
    and `title_bill_id_rule` are appended. The published `laws` table names
    a law's bill for a title led by a law. Through this host's code, over
    the retained 108th-119th feeds and zips with the live table as prior
    (bill-family `a42bd610`, 12,740 rows), the merge adds 2,043 rows (913
    and 1,101 in the 112th and 113th, 29 elsewhere) and changes or drops no
    BILLSTATUS row. spicy-docs' receipt counts 2,048: the other five are two
    feed items titled by a 110th or 112th law, which `laws` (the 119th only,
    today) cannot name a bill for, and three 119th estimates the live
    BILLSTATUS record now lists itself.
  - **Votes.** `roll_call_votes` appends `clerk_body_element` and `vote_desc`.
    The 101st-107th House backfill (7,327 Clerk files, 1990-2002) is a
    dispatch of `rollup-roll-call-votes` with `chambers=house` and a per-run
    `max_votes` cap: replayed over the retained archive it adds 7,327 roll
    calls and 3,175,523 `name:`-keyed member rows with NULL `bioguide_id`. A
    `name:` key identifies a row within its vote, not a person, and
    `member_vote_terms` leaves it unresolved. The five votes vacated before
    any position was recorded publish a row with no member rows.
  - **Digests.** Every published digest is spelled `sha256:`, and spicy-docs
    compares them strictly. `enrich_pdf` writes each attempt's
    `source_sha256` prefixed, and a derived attachment's `sha256` arrives
    prefixed. The prior bare values are re-spelled once
    (`prior_repairs`, below); a bill-family run refuses a bare
    `bill_summaries.content_hash` rather than compare it.
  - **Native legal references.** The two tables are spicy-docs contracts:
    its shapers and its reading of each observation, with this repository's
    target lookup passed in as `resolve`. The first republish moves every
    row's `rule_version` to `native-legal-reference/003`, re-spells 14
    `target_candidates_json` values and re-versions 51 candidates in 31 rows,
    and nothing else (rebuilt from the retained manifest and compared).
  - **The Record.** `record_issues.package_id` is the part-1 whole-issue
    link's stem (`entire_issue_url_stem/2`); a one-time rebuild moves the 7
    `-bk{N}` ids of 368 held rows to the packages GovInfo holds.
  - **Comments.** The contract appends `subtype` and `duplicate_comments`.
    This host does not write them yet: `tests/test_contract_tables.py` names
    them as pending (`PENDING_RECORD_COLUMNS`), and the comments-fields branch
    adds the host columns and empties that entry.

- **A bill-family status read that settles nothing is recorded, not repeated
  every run.** A document the reader refuses, a bill whose rows leave its
  status in doubt, and a bill its folder's zip no longer holds are recorded
  with the zip and the reader (`bill_family_archives` metadata,
  `status_refusals.v1`), and the folder completes (`completed_archive_scopes.v4`,
  which reads v3 too). None reopens its folder until the zip moves or another
  reader runs. On the live generation of 2026-09-29, 26 bills of the
  108th-111th and 117th reopened ten folders on every run that scoped them.
  A pre-108th bill filled from the detail route records its reader too, and
  one another reader filled is re-read once, within the run's fetch cap; a
  detail refused after it was shaped waits for its list stamp or reader to
  move instead of costing a request every run.

- **One-time repairs for the adoption** (`python -m
  spicy_regs.pipelines.prior_repairs`), each a partial writer of its family,
  idempotent, with a `--dry-run` that writes nothing and a `restore` rollback.
  Dry runs against the live generations of 2026-09-29:
  `respell-bill-family-digests` 0 (no model rows are published);
  `respell-document-digests` 0; `respell-comment-digests` 366,541 digests in
  330,236 comments of 140 agencies, through the catalog (workflow
  `respell-comment-digests`); `backfill-found-by` 12,740 rows;
  `rebuild-record-issues` 7 package ids and 368 rules; `stage-suppression`
  154,374 stages, no events. A partial writer now carries a split sibling
  (`bill_sections`) member for member, so the bill family takes one.

- **The bill family skips an unchanged bill only when the running reader read
  it.** Each bill's status reader, this rollup's status rule (`status-v1`) and
  the SpicyDocs package digest, is recorded per bill in
  `bill_family_archives`' metadata. A SpicyDocs release that changes the code
  re-reads every bill of a Congress on that Congress's next run, and a
  version-only release re-reads none. Bumping `status-v1` does the same for a
  change to spicy-regs' own status pass. A re-read does not replace every older
  value. `congress_bills` merges column-wise, so a value the new reader leaves
  NULL keeps the old one. A bill whose cosponsor rows are refused keeps its
  prior rows, and CBO rows are replaced only on a listed outcome. Bills before
  the 108th, filled from the API detail route, record no reader.

  Expected effect: every bill with no recorded reader has its BILLSTATUS read
  once more on its Congress's next run, and no printing is fetched again. Runs
  36371298463 (the 113th, 10,637 bills) and 36375716128 (the 108th–112th,
  63,747 bills) measured 4.2 to 7.8 ms a bill all in, or 2.5 to 6.4 ms after
  the folder listings. The 119th nightly re-reads its 19,249 bills once, about
  1 to 2 minutes more. A 113th–118th run re-reads about 90,000 bills in about
  6 to 11 minutes of status pass. Only the 113th's 10,637 are extra because of
  this change: the 114th–118th lack the CBO and cosponsor outcomes and were due
  for a full re-read anyway. The 108th–112th's 63,747 re-read on their next
  scoped run.

- **SpicyDocs 0.50.1.** `house_communications` derives all four RIN columns of
  every read row from its retained `report_nature` on each run, not only
  `rin_occurrences_json`, so a row no run re-reads moves with the reader too.
  Replayed over the published table (5,006 rows): 2,040 rows' `rin_rule`
  becomes `report_nature_rin_label/2`, 2 go from `unmatched` to it, and 3 go to
  `unmatched`. Those 3 are the NOAA RINs read cut short (119-EC-1226, 1544 and
  1649), whose `rin` and `rin_matched_text` become NULL. 119-EC-602 and 1209
  gain `2120-AA64`. 1,559 rows' `rin_occurrences_json` is respelled compact
  and key-sorted, as the shaper spells it, with the same value. On each bill's
  re-read, `bill_cosponsors.source_xml` loses the whitespace after `</item>`,
  and each of the 108th–111th's 4,762 `cbo_cost_estimates` rows gains its http
  twin as a restatement (`stated_count` 2).

- **`proceedings.agency_code` follows RefSpec's agency registry for docket-less
  proceedings** (proceedings v11, RefSpec 0.1.0.dev21's registry view, batch
  1). A Register agency bridged to an organization a Regulations.gov code
  selects takes that code (Energy Department: DOE; Labor Department: DOL). A
  renamed agency takes its current successor's code whatever the document's
  date: a 1998 Health Care Finance Administration notice is CMS, and an Export
  Administration Bureau rule of any year is BIS. A split agency takes a code
  only when every current successor has one and they agree; none of batch 1's
  four splits does (INS, Customs Service, ICC, USIA). Expected effect over the
  live snapshot's 97,472 docket-less proceedings: 1,186 gain a code, 729 change
  code and none lose one, so 90,848 carry a code, up from 89,662. The changes
  are HHS to CMS 467, DOC to BIS 163, DOC to EAB 84, TREAS to IIO 11, HHS to
  ACL 2 and HHS to HHSIG 2. Docketed proceedings, and so lifecycles, do not
  change.

- **A defunct agency's department is the one RefSpec's registry view states**
  (view schema 1.1, RefSpec cd78e476; proceedings stay v11). The rule reads
  each renamed or split agency's roster parent from the view's
  `original_parents`, no longer from the `parent_id` its Register entry names.
  On the live Register the two agree for all 9 such agencies, so no code moves:
  none of 1,009,313 rows, and none of snapshot b22d81c4's proceedings replayed.

- **`document_attributes` and `docket_attributes` break a `modifyDate` tie by
  SpicyDocs' write-time rule** (document policy 1.3, spicy-docs 0.45.1). Among
  a record's mirror copies tied at the newest `modifyDate`, the copy written
  more than an hour after every other wins; otherwise the smallest SpicyDocs
  record digest wins. The higher attributes digest decided before. Expected
  effect, from the next attributes-sweep publish: about 8
  `document_attributes` rows mirror-wide. The 27 agencies measured on
  2026-09-27 hold 6 `open_for_comment` flips: false to true for
  DEA-2023-0148-0026, USA-2022-HQ-0007-0002, CMS-2023-0069-0001 and
  EPA-HQ-OMS-2021-0325-0002; true to false for EPA-R09-OAR-2018-0160-0007 and
  FWS-R4-ES-2023-0037-0001. No docket rows and no thin-table rows change.

## [2026.08.26]

The headline of this cycle: the MCP server moved off Vercel onto **Cloud Run**,
where it is load-tested to **100 concurrent users at ~0% errors**, and the
deployment itself became infrastructure-as-code under a new `deploy/` folder.
Along the way the **FCC** joined the corpus as its own two tables, and a pass
over "how do I actually use this data" turned up — and fixed — several
documented access paths that did not work.

### Added

- **FCC ECFS** ingestion — `fcc_proceedings` and `fcc_filings`, the FCC's docket
  and comment equivalents for the rulemaking that never reaches
  regulations.gov ([#149]).
- **Cloud Run** as the primary MCP host, captured as a reproducible deploy
  script plus a runbook for the billing-quota and org-policy hurdles
  ([#164]).
- **`deploy/` folder with Terraform IaC** for the R2 bucket, its
  `data.spicy-regs.dev` domain, CORS, the Iceberg catalog, and the edge cache
  rule — import-first, so it adopts existing production rather than recreating
  it ([#159]); Terraform state moved into a private R2 bucket over the `s3`
  backend with native locking ([#161]).
- **Cloudflare Containers** deploy, authored in [#159] and made actually
  deployable in [#162] (build context, generated Worker bindings); kept as the
  documented single-instance fallback.
- **Cloudflare purge-on-publish** (`sources/cloudflare.py`), a no-op without
  credentials and never able to fail a publish that already wrote its data
  ([#158]).
- Docs: `mcp-server/INTERNALS.md` as the committed home for server rationale
  ([#153]); a README reworked into a project front page ([#150]); a
  `getting_started.ipynb` covering the rollups, the non-core tables, and
  cross-source joins ([#156]).

### Changed

- **Performance:** the MCP server caches its DuckDB connection across tool
  calls instead of reinstalling extensions, reattaching the catalog, and
  recreating 20 views per request — **34.5s → 0.21s** warm ([#157]).
- **`.env` loads in CLI entry points, not at import time**, so importing the
  package no longer mutates `os.environ`; one AST test locks it in ([#155]).
- The public corpus hostname is `data.spicy-regs.dev` ([#150]).
- **MCP:** all code comments moved out of the server modules into
  `INTERNALS.md`, keeping the `@mcp.tool()` docstrings that FastMCP reflects
  over to build client-facing tool descriptions ([#153]).
- **Vercel:** pinned `mcp<2` and raised `maxDuration` to the real 800s Pro
  ceiling ([#151], [#152]) — both superseded when that deploy was retired
  ([#166]).

### Removed

- The **Vercel MCP deployment** and its dependency-light parallel copy of the
  server, now that `mcp.spicy-regs.dev` points at Cloud Run running the
  canonical `spicy_regs.mcp_server` ([#166]).

### Fixed

- **Parquet must be `no-cache`.** Edge-caching it corrupts DuckDB's concurrent
  byte-range reads — observed as decode and ETag-mismatch errors from ~c=10 on
  a revision that otherwise ran c=100 cleanly. Reverted the [#158] cache policy
  for Parquet ([#165]) and set the Terraform-managed cache rule to
  `enabled = false` so an apply cannot re-enable it ([#167]). Non-Parquet
  artifacts, which DuckDB never reads, stay cacheable.
- **The `spicy-regs` CLI was broken end to end** — Cloudflare 403s the default
  urllib User-Agent, so `download`, `stats`, `sample`, `search`, and `agencies`
  all failed; downloads now stream to a `.partial` sibling so a truncated write
  can't masquerade as a complete one. Also repointed the published query docs
  off a comments tree that no longer gets written, and added
  `--discover-from-derived` so the comment-text backfill can see the 99.8% of
  rows the `attachments_json` gate hid from it ([#156]).
- **Unit tests were downloading the production corpus** — a developer's `.env`
  reached "hermetic" tests through an import-time `load_dotenv()`, making them
  838s locally and green-by-accident in CI ([#154]).

[2026.08.26]: https://github.com/civictechdc/spicy-regs/releases/tag/2026.08.26
[#149]: https://github.com/civictechdc/spicy-regs/pull/149
[#150]: https://github.com/civictechdc/spicy-regs/pull/150
[#151]: https://github.com/civictechdc/spicy-regs/pull/151
[#152]: https://github.com/civictechdc/spicy-regs/pull/152
[#153]: https://github.com/civictechdc/spicy-regs/pull/153
[#154]: https://github.com/civictechdc/spicy-regs/pull/154
[#155]: https://github.com/civictechdc/spicy-regs/pull/155
[#156]: https://github.com/civictechdc/spicy-regs/pull/156
[#157]: https://github.com/civictechdc/spicy-regs/pull/157
[#158]: https://github.com/civictechdc/spicy-regs/pull/158
[#159]: https://github.com/civictechdc/spicy-regs/pull/159
[#161]: https://github.com/civictechdc/spicy-regs/pull/161
[#162]: https://github.com/civictechdc/spicy-regs/pull/162
[#164]: https://github.com/civictechdc/spicy-regs/pull/164
[#165]: https://github.com/civictechdc/spicy-regs/pull/165
[#166]: https://github.com/civictechdc/spicy-regs/pull/166
[#167]: https://github.com/civictechdc/spicy-regs/pull/167

## [2026.07.22]

The headline of this cycle: the pipeline went from mirroring regulations.gov and
the Federal Register to ingesting **ten complementary federal data sources**, so
the full rulemaking lifecycle — the organizations that engage in it and its
downstream context — can be queried from one place. Alongside the new sources,
this cycle hardened the rollups and made the published corpus edge-cacheable.

### Added

- **Federal Register** ingestion brought fully in-repo ([#125]).
- **Unified Agenda** (RegInfo) ingestion — `unified_agenda` ([#126]).
- **Congress.gov** bill ingestion — `congress_bills` ([#127]).
- **GovInfo CFR** section ingestion — `cfr_sections` ([#128]).
- **OpenFEC** committees ingestion — `fec_committees` ([#132]).
- **Senate Lobbying Disclosure (LDA)** filings ingestion — `lobbying_filings` ([#133]).
- **SAM.gov** entity registry ingestion — `sam_entities` ([#134]).
- **USASpending** recipients ingestion — `usaspending_recipients` ([#135]).
- **CourtListener** litigation ingestion — `court_dockets` ([#137]).
- **GAO + CRS** reports ingestion — `gao_reports`, `crs_reports` ([#138]).
- Docs: a Python query walkthrough ([#142]) and a refreshed table catalog for
  the eleven new data sources ([#139]).

### Changed

- **Performance:** made the R2 parquet corpus edge-cacheable and pruned docket
  scans ([#124]).
- **SAM.gov:** upgraded to full-coverage ingestion via a partitioned walk ([#141]).
- **OpenFEC:** switched to keyset pagination to walk all committees ([#136]).
- **Unified Agenda:** now fetches and parses the real `REGINFO_RIN_DATA` XML
  export ([#131]).
- **CFR:** use the GovInfo `/published` endpoint and derive section fields from
  IDs ([#130]).
- **Congress.gov:** drop the always-null `policy_area` (the list endpoint omits
  it) ([#129]).
- **CI:** schedule weekly comments-catalog compaction and serialize catalog
  writers ([#145]); forward `SAM_API_KEY` to rollup jobs ([#140]).

### Fixed

- Harden the external-source rollups ([#147]).
- Dedup the catalog comments view on read ([#143]).
- Sub-batch catalog dedup by key hash to avoid runner OOM ([#123]).
- Matrix sweep + full agency coverage to stop batch starvation ([#121]).
- Stop marking failed downloads as processed in the ETL manifest ([#120]).

[2026.07.22]: https://github.com/civictechdc/spicy-regs/releases/tag/2026.07.21
[#120]: https://github.com/civictechdc/spicy-regs/pull/120
[#121]: https://github.com/civictechdc/spicy-regs/pull/121
[#123]: https://github.com/civictechdc/spicy-regs/pull/123
[#124]: https://github.com/civictechdc/spicy-regs/pull/124
[#125]: https://github.com/civictechdc/spicy-regs/pull/125
[#126]: https://github.com/civictechdc/spicy-regs/pull/126
[#127]: https://github.com/civictechdc/spicy-regs/pull/127
[#128]: https://github.com/civictechdc/spicy-regs/pull/128
[#129]: https://github.com/civictechdc/spicy-regs/pull/129
[#130]: https://github.com/civictechdc/spicy-regs/pull/130
[#131]: https://github.com/civictechdc/spicy-regs/pull/131
[#132]: https://github.com/civictechdc/spicy-regs/pull/132
[#133]: https://github.com/civictechdc/spicy-regs/pull/133
[#134]: https://github.com/civictechdc/spicy-regs/pull/134
[#135]: https://github.com/civictechdc/spicy-regs/pull/135
[#136]: https://github.com/civictechdc/spicy-regs/pull/136
[#137]: https://github.com/civictechdc/spicy-regs/pull/137
[#138]: https://github.com/civictechdc/spicy-regs/pull/138
[#139]: https://github.com/civictechdc/spicy-regs/pull/139
[#140]: https://github.com/civictechdc/spicy-regs/pull/140
[#141]: https://github.com/civictechdc/spicy-regs/pull/141
[#142]: https://github.com/civictechdc/spicy-regs/pull/142
[#143]: https://github.com/civictechdc/spicy-regs/pull/143
[#145]: https://github.com/civictechdc/spicy-regs/pull/145
[#147]: https://github.com/civictechdc/spicy-regs/pull/147
