# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- **Active: `spicy_docs-0.68.0`**. Built from SpicyDocs fork main `7d140262`
  (tag `v0.68.0`); SHA-256
  `b9dcfaaa86cb9ee756c3269f83029eb3d41ecb846cef1d269ad4863d052893db`
  ([package receipt](spicy_docs-0.68.0.json); two builds from clean archives are
  byte-identical). It ports `CentralRawArchiveStore` (`storage/raw_archive.py`,
  including `put_stream` and `receipt_keys` selection in
  `iter_acquisition_receipts`) and the `Path | SourceNativeBlobStore` refusal
  store API (`reading/refusals.py`) onto `main` (ss-z50t), bounds
  `verify_archive` memory to one packed entry per original (ss-zznj), refuses
  the shared raw archive root under pytest unless `SPICYGOV_RAW_ROOT` is set
  (ss-m5bh), and refuses unnamed House `<xml>` vote envelopes (ss-xus7), on top
  of `0.67.1` (`held_original` for FEC ZIP runs, ss-6fuk). Because the release
  wheel natively ships `storage/raw_archive.py` and the refusal store API, no
  `+centralraw` overlay wheel or `vendor/central_raw_reader` source copy is
  needed. The superseded `0.67.0` and `0.67.1` wheels and their `+centralraw`
  overlays were removed when `0.68.0` was adopted.

- `spicy_docs-0.67.0+centralraw.02cc4431e606`: the previous overlay. The same shared raw
  archive overlay over `spicy_docs-0.67.0`, the consolidated Layer 1 release
  (SpicyDocs fork main `3e68ec10`, tag `v0.67.0`). Only `storage/raw_archive.py`
  and the blob destination in `reading/refusals.py` differ from the base; the
  overlay modules are unchanged, so the identity is unchanged. The
  [build receipt](spicy_docs-0.67.0+centralraw.02cc4431e606-py3-none-any.json)
  pins both changed modules and wheel SHA-256
  `e3eecedfe268c630de671bb5eaff6ce0898e74172dcd6bcbf5ff81252e4702f3`.
  `python scripts/package_central_raw_reader.py --check` reproduces the wheel,
  receipt and dependency pins from retained wheels without writing or downloading.
  `--base` selects another retained release wheel, `--provider` a reviewed source
  checkout for the archive module; `--write` explicitly updates the overlay after
  review. Keep both retained provider wheels. Tests verify unchanged reader
  modules, streaming captures, exact-byte reuse of raw and gzip storage, and
  interrupted-write cleanup.

- `spicy_docs-0.67.0`: built from SpicyDocs fork main `3e68ec10`; SHA-256
  `b4d147cf3ae2860509f0bda1cfba168e007cfe4f86798858983bdc5426c95545`; two builds
  from clean archives are byte-identical. It carries the CourtListener
  bankruptcy CSV dialect, the GPO symbol-font glyph table with unmapped-glyph
  routing (body text derivation 003; `ProcessingIssue.box`), Unified Agenda
  related RINs, the opinion Markdown converter, select-committee parent codes
  and SD bill files, the private-law version code, the CBO Common Crawl archive
  reader, FEC filing encoding decisions, `literal_sheets` number formats,
  `congress_sessions` party control and every CBO letter. Its `acquisition`
  extra adds `warcio`. See [package receipt](spicy_docs-0.67.0.json). Over the 13
  retained workbooks in the raw archive (the FEC IE tables among them), its
  `literal_sheets` equals the 0.66.9 gating build of the same reader change on
  every field of all 449,956 cells (number formats and date typing included; no
  refusals), and the FEC IE statistics, meetings, feed and source pages rebuild
  from their originals as they did under 0.66.9 (ss-j4vz).

- `spicy_docs-0.66.8+centralraw.02cc4431e606`: the previous overlay over
  `0.66.8`; SHA-256
  `1ae028e87696192b074103062bb819632b6a40209266606af4a1cb8e919d17ec`
  ([receipt](spicy_docs-0.66.8+centralraw.02cc4431e606-py3-none-any.json)). The
  0.66.8 base adds `nomination_id` on the nominations contract and on Senate PN
  entries of `roll_call_votes.documents_json` (ss-vhi2).

- `spicy_docs-0.66.8`: built from SpicyDocs fork main `c970f0ac`; SHA-256
  `1f7df1a064e7c478aad9d93549b5b08695da924f18ec93ca53d7dd27ebfd7961`; two builds
  from clean archives are byte-identical. See [package receipt](spicy_docs-0.66.8.json).

- `spicy_docs-0.66.7+centralraw.02cc4431e606`: the previous overlay over
  `0.66.7`; SHA-256
  `6b4344bee64a51ced4cff8c4d865e87be59ddf5ad2bf4bd22405c35d32f9304b`
  ([receipt](spicy_docs-0.66.7+centralraw.02cc4431e606-py3-none-any.json)). The
  0.66.7 base adds `cms_profile_for_policy`, which admits the retained CMS
  Part D and Medicaid releases under their declared acquisition policy 1.0.

- `spicy_docs-0.66.7`: built from SpicyDocs fork main `a87cc3d1`; SHA-256
  `ed0b69575364a0728fc3122ce20e557bd5f7ec4112c20ed4ecd92c65c7eab073`; two builds
  from clean archives are byte-identical, and the same method reproduces the
  0.66.6 wheel from `247bc77b`. See [package receipt](spicy_docs-0.66.7.json).

- `spicy_docs-0.66.6+centralraw.02cc4431e606`: the previous overlay over
  `0.66.6`; SHA-256
  `b9c65f65df036e37350a8f0bb2de768d04746ea5596c6f437f477747d0a6223c`
  ([receipt](spicy_docs-0.66.6+centralraw.02cc4431e606-py3-none-any.json)). The
  0.66.6 base adds the CMS, Grants.gov, OFAC and Treasury fiscal source
  profiles, executive order body captures and the source-native release reader
  that the CMS Part B/D and Medicaid, Grants, executive order, OFAC and Treasury
  tables read, and keeps the CourtListener traversal allocation of 0.66.3.

- `spicy_docs-0.66.6`: built from SpicyDocs fork main `247bc77b`; SHA-256
  `8b5d4d2144d269610ddf33595a1a4577cee217b1c51551fe313301022ee46471`; two builds
  from clean archives are byte-identical. A different build of the same version
  from `7cefbe92` (SHA-256 `af418de4…`) produced the live OFAC generation on an
  unmerged branch; main adds CMS, Grants.gov and body-capture shape checks and
  the Grants.gov profile registration over it. See [package receipt](spicy_docs-0.66.6.json).

- `spicy_docs-0.66.3+centralraw.02cc4431e606`: the previous overlay over
  `0.66.3`; SHA-256
  `b526dfae319cb5b5e2d91491b35bc3eb61f555b18ae02dde10bc376df943215b`
  ([receipt](spicy_docs-0.66.3+centralraw.02cc4431e606-py3-none-any.json)).

- `spicy_docs-0.66.3`: built from reviewed SpicyDocs commit `0950db0a`;
  SHA-256 `6df098b7f50f2e7d3b781e1987cb9ff5d37118334aa7ced23f6f603733916c96`. It bounds CourtListener requests
  and waits across a search and honors valid 429 Retry-After delays. Earlier
  wheels remain retained; see [package receipt](spicy_docs-0.66.3.json).

- `spicy_docs-0.66.2`: built from spicy-docs fork main at `bc371f52`; SHA-256
  `74094743267172439795e0ac53c5ee5a7798dabaeac794aac6f4699127884416`. It makes the
  OpenFEC traversal checkpoint append-only. Earlier wheels remain retained; see
  `spicy_docs-0.66.2.json`.

- `spicy_docs-0.66.0`: built from spicy-docs `simplify-20261016` at `3fc47e35`
  (base `743288d0`, 0.65.0); SHA-256
  `0ba69d888f5a8d8e918360407d4801d308317c465664d0f1d43a165839e428dc`. It adds
  bounded response reads, retained-capture reuse and resumable OpenFEC traversal.
  Earlier wheels remain retained; see `spicy_docs-0.66.0.json`.

- `spicy_docs-0.63.0`: complete release wheel built from the scorecard production
  refactor at `40927102`; SHA-256
  `7e5786e93d2bd8dc9773313b6b4dcc5f8d96513da02de734f9011097b44d82e4`.
  CPAC selects its GraphQL headers in the source reader; HRC reuses shared private
  qualification retention. Source shapes and existing publisher values remain
  unchanged. See [production operations](../docs/scorecards-production.md).
  Existing evidence pins and earlier wheels remain retained.

- `spicy_docs-0.65.0+centralraw.523052e8a6fe`: retained historical archive
  module provider; SHA-256
  `2049deea32da3460cbaf3eac78594dbcf9d092daa6bc2c24d22b762c834f74d4`.
  The current adapter reuses only `storage/raw_archive.py` from this capsule.
  Its older FEC client and other reader changes are not adopted.

- `spicy_docs-0.61.0`: tagged source `v0.61.0` at merge commit
  `76ec8628f608b1ed2080b8ef2a4a8c920af72c4f`. Its full tree equals the final
  CI-passed candidate `ceb3b7644f3d9af8ebf2d3f4c457873b15bb3e0f`.
  The exact qualified wheel was built at `9aac355e7fb9efc24d2a0717faef031e73377212`;
  the final guide-index repair changes no packaged runtime bytes, and the
  final rebuild matches that wheel. Independent source and installed-wheel
  reviews passed. The wheel has SHA-256
  `081cfb339b63d3ea6b396cea68f1ae3a2a5e339a10355325c64e07e295ae74ea`;
  see `spicy_docs-0.61.0.json` and the
  [adoption evidence](../docs/research/scorecards/work/integration/reader_adoption_new_american_print_20261005.json).
  This adds only the named printed 2008 Freedom Index fourth installment.
  The existing New American registry stays disabled with `hash_only`,
  unreviewed rights and no historical backfill. Deployment and data publication
  remain separate. Prior wheels and proofs remain retained; no PyPI upload is
  claimed.

- `spicy_docs-0.60.0`: tagged source `v0.60.0` at merge commit
  `ec18a2665e2e7764e624cf17a4e718ab4217ae3a`. Its full tree equals the
  independently reviewed candidate `2d62f887ae9e4972d7abaa6e821387fa3504fc70`.
  The wheel has SHA-256
  `4e0bae7cbda92c1af4adbee87000b283c880451b68c8ff14e630f431762cb3f4`;
  see `spicy_docs-0.60.0.json` for installed replay and review pins. The local
  consumer adds one named UTU lifetime article assessment and two separately
  attributed ASCF chamber originals. Source entries stay disabled with
  `hash_only` retention, unreviewed rights and no historical backfill. Consumer
  deployment and data publication require separate approval. No PyPI upload is
  claimed; prior wheels and proofs remain retained.

- `spicy_docs-0.58.0`: release commit
  `ca3a9bd84ca01e296c54856ed20ea71339b6ce57`, tag `v0.58.0`. The exact
  coordinated release wheel has SHA-256
  `952bae11f8353caa8a367dc62f98cae94692a15d6b70f5c04a33ae1a8154b144`;
  see `spicy_docs-0.58.0.json` for the retained source receipt pin. It adds
  source-stated member nicknames and the separately qualified scorecard readers.
  The members policy classifies `name_nickname` as nullable source input.
  Native members must be built with this wheel and policy together; earlier
  native members bundles refuse schema-mismatched resume. Existing native
  subjects and receipts need an explicitly approved paired rebuild rather
  than an implicit schema change. Scheduled member writes keep exact source
  strings and retain row-level input evidence. Reader installation, scheduled
  source publication, native conversion and serving qualification are separate
  stages. This wheel is vendored; no public registry publication is claimed.

- `spicy_docs-0.57.0`: the SpicyDocs 0.57.0 release, from release commit
  `080762574c16e5eef2f3b56cfac876f5e09b00a3`. SHA-256
  `4efc149a18f2222e53dee8e7588cc59fdbd33cadb9a9fcf1851e51329def1dc2`, 2,478,280 bytes;
  two builds from a clean archive of that commit are byte-identical, and every packaged
  source file matches the commit. It replaces the local `0.56.0+scorecards.7aed167baa10`
  build below, an overlay whose scorecard modules came from SpicyDocs `39dbd8a`, an ancestor
  of the release. The release is a superset: 454 of that build's 464 package files are
  byte-identical (every scorecard reader and the registry among them), none is removed, one
  is added (GAO's `report_requesters`, which nothing here imports yet) and ten move forward
  to later commits of the same files. `bill_stage`, `bill_family` and the bill table
  contract read a measure both chambers passed as `cleared` where its BILLSTATUS document
  lists an enrolled text (`stage_rule` `enrolled_text_listed`), so
  `transforms/build_bill_family.py` passes each bill's published `bill_versions` rows to the
  stage rule wherever it reads stored rows again; the wheel without that change puts a
  rebuilt bill back to `passed_both`. `bill_version_tables` declares the four values
  `bill_versions.source` takes, and its writer and the stage rule refuse any other, a native
  `printing_id` named `source` among them. GAO's `native`, `listing`, `month_in_review`,
  `major_rule_reports` and `product_metadata` read a topic-less page's unlabeled date beside
  the number that names the page (acquisition policy 1.3, page rule `/3`; no value read
  under the earlier versions changes), and `product_details`, which nothing here imports
  yet, refuses a page that may not be whole (rule `/2`). Table contract columns and
  identities, dependencies and entry points are unchanged; eight stage column descriptions
  change. Earlier wheels and receipts stay as the records of their adoptions. This is a
  release build, vendored rather than uploaded to a package index.

- `spicy_docs-0.56.0+scorecards.7aed167baa10`: reviewed Drum Major Institute
  readers on the previously qualified scorecard package. The baseline receipt
  remains in `spicy_docs-scorecards-8253a8a31276.json`. See the
  [adoption evidence](../docs/research/scorecards/work/integration/reader_adoption_dmi_20261005.json)
  for named source renditions, independent original-source review, installed
  replay and consumer checks. Data publication and scheduled refresh remain
  separate stages.
- `spicy_docs-0.56.0+scorecards.8253a8a31276`: SMART and CWA readers on
  the previously qualified scorecard package. The baseline receipt is retained
  as `spicy_docs-scorecards-ef0b64b3121f.json`. See the
  [adoption status](https://github.com/mikewolfd/spicy-regs/blob/main/docs/research/scorecards/work/integration/reader_adoption_smart_cwa_20261005.json)
  for installed replay, independent source checks and complete consumer gates.
- `spicy_docs-0.56.0+scorecards.ef0b64b3121f`: the released `0.56.0` baseline
  with the qualified AGC, CGS and Environment America readers from SpicyDocs
  `554dfba52246773a103601d3987d46a7755ef7ae`. The
  [package receipt](spicy_docs-scorecards.json) records identical repeated builds,
  exact module pins and unchanged baseline runtime files and dependencies.
  The [adoption evidence](../docs/research/scorecards/work/integration/reader_adoption_three_20261005.json)
  records fresh installed replay and consumer checks. Earlier wheels and
  receipts remain available for their historical inputs. Data publication and
  scheduled refresh are separate stages.

- `spicy_docs-0.56.0`: the SpicyDocs 0.56.0 release, from release commit
  `1e56604a28883ea90dda68351daca10a3149b96c`. SHA-256
  `36d28dc6e3255224eed395cfa9dd097fc13872aa53c1625551d78ba71010a144`, 2,415,227 bytes;
  two builds from a clean archive of that commit are byte-identical, and every packaged
  source file matches the commit. It replaces the local `0.54.0+scorecards.0bee5afcac95`
  build, an overlay whose scorecard modules came from SpicyDocs `7358ba8`, an ancestor of
  the release. The release is a superset: 447 of that build's 451 package files are
  byte-identical (every scorecard module among them), none is removed, five are added
  (GAO's `listing`, `major_rule_reports`, `major_rule_letters`, `major_rule_old_index` and
  `product_details`) and four move forward to later commits of the same files (Senate
  expenditure rows accept a table without geometry; GAO's `month_in_review`,
  `decision_pages` and `product_metadata` carry the listing refactor and the Congressional
  Review Act work, their exported names kept). Table contracts, dependencies and entry
  points are unchanged. This is a release build, vendored rather than uploaded to a
  package index.

- `spicy_docs-0.54.0+etl.cf9f88e8c4b7`: merged native ETL, source-reader, and scorecard work from
  SpicyDocs commit `cf9f88e8c4b7967b73edc190d326650306af9308`. Two archive
  builds produced identical wheels and every packaged source file matches the commit.
  The receipt in `spicy_docs-etl.json` records its digest. The complete source gate passed.

- `spicy_docs-0.54.0+scorecards.9ea43ea7845d`: additive scorecard adoption over the
  current main reader `0.54.0+chaos.a1b91ce28a2a`, preserving its newer source readers
  and body-text derivation exports. SHA-256
  `670d168db3ca15609485e363f2179a3fa6ad73572b4aadd4245e01d11c421168`,
  1,934,729 bytes. It includes the qualified scorecard readers, historical member
  crosswalk fields, structured Gemini page extraction and HRC's exact standalone
  rating `NA` to `N/A` rule. The raw HRC asset remains unchanged. Independent builds
  are byte-identical; the [merge build recipe](../docs/research/scorecards/work/integration/build_merge_reader_wheel.py)
  checks all package changes against the current main wheel. Consumer adoption is
  separate from a package-registry release or deployment.

- `spicy_docs-0.54.0+chaos.a1b91ce28a2a`: the round-6 bills-lane build (2026-10-03), from branch
  `chaos/2026-10-03-bills-lane-r6` at `a1b91ce28a2ac254d4f2301e5abb48a025dac198`: the round-5 build below plus round
  6's three spicy-docs branches. SHA-256 `4ec97919319016d128b28b4d95db0ec795a6465c468b061ff6e9b7c80bd4ef08`,
  1,865,157 bytes; two committed-archive builds produced identical bytes, and the body-text derivation digest read
  from the wheel equals its pin (`sha256:91fa3ee5…`, unchanged). It adds the `cleared` stage, stage rule F (a mention
  is no step; codes first) and a private law's signing date; the Register's regulations.gov link and four more stated
  fields (acquisition policy 1.4, raw schema 1.2); GAO's listing number split with a cut flag and the decision-page
  caption reader; the classification-table reader fix, the bare section key and `usc_place` on both OLRC tables, a
  law's last Statutes page (`laws-uslm-v3`), the committee codes a hearing's MODS states, and one placeholder
  predicate for FEC name fields. This is a vendored branch build, not a registry release. Build receipt:
  `mcp-chaos-2026-10-02/round6/wheel-r6/` (`README.md`, `wheel.json`, `smoke.out`).


- `spicy_docs-0.53.0+scorecards.fd38daf2d185`: local scorecard and historical identity candidate,
  over the reviewed FEC baseline `69964fe27c6ddc437e817041a3c5cca57a02723c`.
  SHA-256 `2caf2fc1cd4201bf57b0c772339a0b8837e2fd92702acee8ccbe3ea65e92337e`.
  Independent builds are byte-identical with pinned uv/uv_build and recorded
  Python versions; the explicit overlay preserves every
  baseline package file, including FEC candidate history. It adds scorecard
  readers, pinned GovTrack discovery, member crosswalk fields, and the requested
  existing Docling/OvisOCR2 extraction infrastructure, plus schema-validated Gemini
  page outcomes, retained paired input images, and explicit HRC source-fact
  schemas that separate vacant seats from members. IJM additionally supports its
  explicitly identified browser-rendered API JSON. Reconstruction remains at
  the native-only baseline; the receipt lists the scoped extraction tests. The `yaml` extra is now
  selected for discovery; PDF model configuration remains explicit at the host.
  See [build recipe](../docs/research/scorecards/work/integration/build_reader_wheel.py)
  and [hash/overlay receipt](../docs/research/scorecards/work/integration/reader_wheel_paired.json).
  This is local package adoption, not a registry release or source qualification.

- `spicy_docs-0.53.0+fec.69964fe27c6d`: reviewed FEC source at
  commit `69964fe27c6ddc437e817041a3c5cca57a02723c`.
  SHA-256 `824296eed3c333277feaa4dca03a2dc8faec437d803c374186d61c8761bf8c7b`. Repeated committed-archive
  builds are byte-identical. Production package files are identical to
  `0.53.0+fec.bcdde5431fac`; this repin includes the reviewed PostgreSQL
  rejection/cleanup and API shape-refusal regression tests. Build and comparison
  receipts: `hosted-release-20261001/reviewed-release/source/`.

- The superseded `spicy_docs-0.54.0+chaos.16c2285ef63e` (the round-5 build of `chaos/2026-10-03-bills-lane-r4` at
  `16c2285`, SHA-256 `21ece738…1ac8`, receipt `mcp-chaos-2026-10-02/round5/wheel-r5b/`) was removed on October 3,
  2026, when the round-6 build was adopted. Restore it with `git show b43e86f:vendor/<wheel> > vendor/<wheel>`.

- The superseded SpicyDocs 0.53.0 wheels were removed on October 3, 2026, when 0.54.0+chaos was adopted:
  `0.53.0`, `+printing.11189d72bd29`, `+fec.14b98e9db916`, `+fec.3fc8388b368b`, `+fec.69964fe27c6d`,
  `+fec.bcdde5431fac` and `+laws.f8431033f626` (fork main `d2b5f53` holds each, with its record in this file), and
  the development stand-in `+votes.8d7f3fe3b489` (the votes lister alone, receipt `mcp-chaos-2026-10-02/round5/wheel/`).
  Restore one with `git show d2b5f53:vendor/<wheel> > vendor/<wheel>`.

- Previously adopted `spicy_docs-0.52.0`: released from SpicyDocs `main` at `b08ac1b`, September 28, 2026:
  **1,757,380 bytes**, SHA-256 `374b8c208018d1964007b9e78c4c9ab6beedaafab2cca96fb93a4acbc2b26684`,
  byte-identical across the release's two builds and rechecked against its `SHA256SUMS` on
  adoption. It adopts 0.51.0 (never vendored here) with it: 0.51.0 reads bill stage in
  publisher order and adds two citation spellings and the administration-policy and
  Inspector General readers; 0.52.0 reads CBO's feed for every Congress, puts the native
  legal-reference tables under contract, spells every digest `sha256:`, reads the Clerk's
  archive whole, keys a Record issue on its first book's stem, and adds the comment fields and
  GAO's listing (its `docs/decisions.md` 0.51.0 and 0.52.0 entries). The extras used here are
  unchanged. Its two new extras are not installed, since no spicy-regs table reads through
  them yet: `record-speeches` (the pinned congressionalrecord fork) and `yaml` (the
  administration-policy reader's loader). The 0.50.1 wheel (`211bc6a`, `08a0afaa`) and its
  record are at `16b0683`. Vendored adoption only; no package-registry upload is asserted.

- `rulespec_artifacts-1.1.2`: exact dependency of SpicyDocs 0.39.2, built from Rulespec
  `23d5f2d98973b2a7f1bf7ce4c9c7a786a4f369ab` (`packages/rulespec-artifacts` of a whole-repo
  archive, since it force-includes repository files, with hatchling's default reproducible
  timestamp). **99,323 bytes**, SHA-256
  `7d547cd432b1d533b93dcfd3802e4a0e179de6e89301fd6faa8bb917b99830ef`. Code, schemas and
  canonical encoder equal 1.1.1's; it carries the platform-fixture corpus under its own
  version. The same method reproduces the 1.1.1 wheel (99,315 bytes, `63ad763f…c8c8`) byte
  for byte; 1.1.1's record is at `13f3831`.

- The unreferenced SpicyDocs 0.47.0, 0.48.0, 0.48.1 and 0.49.0 wheels were removed on
  September 27, 2026; restore one with `git show 8350352:vendor/<wheel> > vendor/<wheel>`.
  This file at `8350352` holds the 0.46.0, 0.47.0 (`f549c16`, `ff3d9bb0`) and 0.48.0 records;
  0.48.1 and 0.49.0 were vendored without one. Earlier wheels (SpicyDocs
  0.25.0–0.33.2, Rulespec Artifacts 1.0.14 and 1.1.0) were
  removed on September 25, 2026. Their build records are in this file at `e4840d9`; restore a
  wheel with `git show e4840d9:vendor/<wheel> > vendor/<wheel>`. Two were kept for replay:
  the published comment release under acquisition policy 1.2 (the six-agency cohort)
  replays with `spicy_docs-0.28.0` or earlier, and `spicy_docs-0.26.5`/`0.26.6` reproduce
  the generations built on them.

- `deltatrack-0.1.0`: built with `uv build --wheel` from `civictechdc/DeltaTrack`
  commit `c636448ba08d55bba7cb8c884aad0f5ac1ccf2f6`, 2026-09-19.
  SHA-256: `7f060e30af9702f4e45c305fa93c53d1e3e59bd70858d3c6717f6fc9cf825197`.
  DeltaTrack is a git dependency (SpicyDocs' `bill-diff` extra pins
  `git+https://github.com/civictechdc/DeltaTrack?rev=c636448…`), not a
  registry package, so it is vendored the same way as the other source-reader
  wheels here rather than resolved transitively. It exists for the bill-diff
  tables (`section_diffs`, `section_diff_items`, `financial_changes`) that
  `docs/research/table-contracts-2026-09-19.md` §5.4 describes, and the 0.21.x
  pin reaches them. `[tool.uv.sources]` alone was
  not enough: a uv source binds a dependency the project declares, and
  DeltaTrack arrives only through spicy-docs' `bill-diff` extra, so uv looked
  for it in the registry and failed the resolve. `deltatrack==0.1.0` is
  therefore also declared directly in both `source-readers` lists — the same
  shape `rulespec-artifacts` already had for the same reason.
- `uv.lock` records every wheel SHA-256 above. Replace the wheel and refresh the
  lock together, then run receiver tests against the installed wheel.

PDF enrichment (`transforms/pdf_text.py::extract_pdf_text`, the regulations.gov
attachment path) uses the narrow `pdf-pypdf` provider extra through
`source-readers`. SpicyRegs additionally pins pypdf `6.14.2` and checks that
version before parsing; package installs and checkouts therefore use the same
qualified PDF behavior.

The GovInfo *body* path is different: `extraction.body_text`'s PDF branch
(both `build_bill_family.py` and `build_committee_reports.py`) runs through
`body_text`'s default extractor, `DocumentExtractor(NativeText())`, whose
default reader opens PDFs with PyMuPDF — now installed via the `pdf` extra —
rather than through a pypdf adapter. This is not a licensing call; the user's
ruling is that results decide, not licensing (PyMuPDF is AGPL/commercial
dual-licensed; pypdfium2, which DeltaTrack itself uses, is BSD/Apache — the
same spicy-docs research doc measured it too and found it handles the two
genuinely GPO-numbered fixtures as well as PyMuPDF, but introduces false
hyphen rejoins on non-numbered layouts that spicy-docs's own extractor
declines; adopting it is a spicy-docs-side question, out of scope here). It is
a correctness one: `extraction/gpo_normalize.py`'s layout detector was derived
against PyMuPDF's line-grouped text, where a GPO gutter line number comes back
as its own physical line immediately after its content line. pypdf glues that
number onto the end of the content line instead (`Representa-1`), so the
detector's adjacency check never fires — measured on real GovInfo PDFs in
spicy-docs `docs/research/gpo-normalizer-vs-upstream-2026-09-19.md`: 0 of 6,
then 0 of 12, real gutter numbers correctly detected under pypdf, against 6 of
6 and 12 of 12 under PyMuPDF, on the same two documents. Under pypdf, three of
the normalizer's rules — layout detection, hyphen rejoin, and the
gutter-evidenced half of the bare-digit strip gate — are structurally
unreachable on every PDF-derived GovInfo body, not just occasionally
degraded. Routing this path through spicy-docs's default extractor instead of
`transforms/pdf_text.py::PypdfPageExtractor` (removed) is what restores it.
`pdf-pypdf` stays in `source-readers` only because `extract_pdf_text` still
needs it for the unrelated attachment path above.

CourtListener listing, pins and raw rows use the shared provider directly. Run
`uv run pytest tests/test_courtlistener_bulk.py tests/test_courtlistener_shared.py tests/test_court_scope.py tests/test_cluster_court_scope_backfill.py`
for this receiving change. Before replacing the provider wheel, also run the
BILLSTATUS, Unified Agenda and PDF reader tests (`test_bill_subjects.py`,
`test_unified_agenda*.py`, `test_pdf_text*.py`).
