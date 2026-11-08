# Navigate retained source relationships

The `spicy_regs.relationship_views` module installs read-only SQL views over
already loaded tables. It never acquires sources, rebuilds published data or
looks up targets during connection setup. The serving host calls
`install_relationship_views(connection, available_tables, publication=...)`
before restricting the connection, then uses the returned availability and
metadata for discovery. Views are regular in-memory DuckDB views so request
cursors can see them. The module uses only Python's standard library and the
injected DuckDB connection; it does not import SpicyDocs or pipeline modules.

The registry is the authoritative list of supported arrays. Each entry provides:

- `<name>_occurrences`: every array element, including duplicates, nulls and
  unsupported elements, with the source's full key, zero-based ordinal, field
  pointer, raw element JSON, parsing disposition and publication metadata.
- `<name>_pairs`: distinct valid source/target navigation pairs. These discard
  multiplicity intentionally; return to occurrences for source evidence.
- `<name>_field_states`: the held field's literal value and whether it is SQL
  NULL, JSON null, malformed, a non-array, empty or populated. A non-array never
  becomes a false empty source result. SQL NULL cannot recover the original
  publisher's absent/null distinction when an earlier shaper discarded it.

`target_status=not_checked` means a supported identifier was projected. It does
not assert that a target exists in the selected publication. The separate
lookup views below report `found`, `missing` or `ambiguous` only after an exact
lookup against their declared target inputs. `unsupported`
means this view does not supply a qualified target route. Publication JSON is
provided by the host separately for each input; JSON `null` means it was not
provided, not that immutable source identity was established. Locations point
to held array columns, not to unverified locations in an original capture.

## Supported source observations

| View family | Meaning and limits |
| --- | --- |
| `bill_related_bills` | Directed publisher-related bills. Each occurrence keeps the complete nested relationship-details list. No reciprocal relationship or identical text is inferred. |
| `member_fec_ids` | Source-listed candidate IDs, including presidential IDs, with community-crosswalk capture and roster context. No election cycle or committee authorization is inferred. |
| `meeting_*` | Bills, hearing jackets, committee objects, witnesses and offered documents are independent lists. Full meeting key is Congress/chamber/event ID. Jacket targets retain Congress/chamber. Canceled status stays visible. Witness names create no person identity; document URLs create no acquisition claim. |
| `vote_documents`, `vote_amendments` | Independent native blocks. Nomination suffixes and native Congress survive. PN and held bill shapes have routes; amendment/treaty routing remains unsupported pending positive native specimens. Empty amendment IDs do not suppress document references. |
| `fcc_filing_proceedings` | Held FCC proceeding-name memberships, preserving repetitions. The numeric IDs and participant roles discarded by earlier shapers cannot be reconstructed here. |
| `federal_register_rins`, `federal_register_dockets` | All held values under the complete document-number/publication-date key. Docket target keys remain literal source spellings: no free-text normalizer or unique-target claim. |
| `document_additional_rins`, `proceeding_*`, `comment_period_*` | Every held membership, independently expanded. Shared RINs do not collapse actions or pair different lists by position. |

`house_communication_rins` also preserves every held RIN finding under the full
Congress/type/number key, including source route, field digest, spans and
extraction rule. Older schemas without its held array are unsupported.

Comment references use a separate scalar-field path. `comment_document_references`
keeps the explicit `commentOnDocumentId` observation; its `_pairs` view includes
only nonblank string references. `comment_native_references` preserves the
separate object/original-document namespaces; neither is treated as a document
ID. `comment_reference_field_states` reports the native map's absent/null/empty
states. Legacy rows without that map remain `unread`. The source docket stays
null when the publisher supplied no docket, and there is no prefix inference.

## Target lookup and analytical views

| View | Grain and safeguards |
| --- | --- |
| `usc_citation_verdicts` | One row per U.S. Code (title, section) key that `document_citations` (settled `usc_section`), `law_code_sections` or `table3_records` names, with each source's count and whether the held OLRC release point prints it, spans it with a range stub (matched in Code order: integer stem, letters by length then spelling, the number after a dash, then any rest), or does not print it. Not printed never means absent: `reason` says only what was observed (`appendix_title`, `classified_as_new`, `not_printed`). Duplicate OLRC numberings stay one key with `release_point_rows` above one. |
| `usc_release_point_sections` | Keys only: one row per (title, section) the held OLRC release point prints, a range stub with its bounds in Code order. It is the `usc_section` target of `resolve_document_citations` (rule `selected-target-lookup/2`): `found` means the release point prints the section (`match_basis` `printed_section`) or a stub spans it (`range_stub`). `native_legal_references` still resolves against `law_code_sections` (`selected-target-lookup/1`), and its recorded candidates keep their links. |
| `agenda_item_editions` | Every exact RIN/edition/URL target stays separate. Duplicate edition rows are ambiguous; no latest-edition selection. |
| `agenda_related_rins` | One row per stated related RIN under the complete RIN/edition/`source_ordinal` key; repeats and self-references stay. `resolution_status` is `malformed` (the published RIN pattern, checked first), `same_edition`, `other_edition_only` or `unlisted`: a held agenda row, not a verified relationship. Native subjects only. |
| `uei_identifiers`, `sam_uei_registrations` | Literal UEIs remain namespaced by source and registrations retain native EFT, including NULL. Shape checks do not adjudicate entity identity. |
| `recipient_sam_entities` | One source recipient row with matching registration count. Grouping registrations before enrichment prevents monetary row multiplication. Original amounts and capture dates remain unchanged; parent/child totals may overlap. |
| `court_*_endpoints` | Native opinion, cluster and docket IDs stay typed. Source edges remain separate from each endpoint lookup. Duplicate exact targets are ambiguous, not silently chosen. No body acquisition or cross-provider court mapping. |
| `section_diff_endpoints`, `section_diff_version_endpoints` | Complete bill/version/provider keys on both sides. Section existence and text-digest agreement are separate. Added/removed sides may be absent. Engine revisions and all version candidates remain visible. |
| `fec_collection_cycles` | One row per collection. `cycle` is the FEC's own `bulk-downloads/<even year>/` directory in the captured URLs (2026 = 2025–2026); NULL with `not_stated` for API, legal, header and other files, and `ambiguous` when captures span two directories. No cycle is read from a file name or query parameter. Join `collection_id` to `fec_source_records`. |
| `fec_relationship_evidence` | Source locator resolves a collection/record companion without multiplying observations. All ambiguous candidates survive. Digest equality compares recorded digests; actual source bytes remain `not_checked`. Explicit empty-list observations remain empty-list observations. |
| `member_vote_party_affiliations` | One row per vote/member with native party candidates under half-open date intervals. Missing ends are not open-ended; gaps stay unknown, overlaps ambiguous, and term party never supplies a silent fallback. Source capture and affiliation ordinals remain in candidates. |
| `org_identity_candidates` | Existing name matches remain pending with matcher features, competing-name counts and original comment date bounds. Candidate IDs are scoped to a publication digest; unversioned sources get no falsely immutable candidate ID. Acting role is unknown; no identity or funds attribution is accepted. |

Offered artifact arrays (`document_artifacts`, `fcc_filing_artifacts`) retain
all native rendition fields and distinguish a URL from acquisition. Native
FR agencies/topics, Congress subject terms and LDA contacted-entity arrays
retain provider namespaces. No RefSpec equivalence is guessed from a matching
label. Empty LDA contacted-entity arrays create no agency edge.

Every advertised view includes schema-derived columns and coverage semantics:
source population is inherited, occurrence counts differ from unique pairs,
legacy schema absence is unsupported, and null/empty/malformed/unread states
remain explicit where the held data supports them. No counts or completeness
measurements are inferred during installation. These are connection views,
not independently published artifacts or an atomic cross-table snapshot.

## Scope of this delivery

This implements the thin held-array portion of T04/T06/T07/T08/T10/T11 and the
T03 explicit-field views. It does not complete target resolution or a public
population qualification. T10 native numeric IDs/participant detail need the
source projection repair. T11 free-text normalization and lifecycle date routing are not added by these
views. T14 entity-level enrichment, T15 selected native endpoints and T21 full
version/section lookup are implemented; population-wide source replay remains
separate. T09 provides companion navigation but does not read or hash evidence
bytes. T16 exposes selected held artifact families, not every source rendition.
T20 exposes native namespaced terms, not reviewed cross-source mappings. T22
exposes candidates without an acceptance/revocation workflow or a qualified
identity truth set. T25 adds explicit coverage semantics without materializing
new aggregate counts. T18 RIN arrays and T19 dated party selection are available
only when their new source columns/tables have actually been published; this
code does not backfill or publish those inputs.

Run focused validation with:

```sh
uv run --frozen --no-sync pytest tests/test_relationship_views*.py
uv run --frozen --no-sync ruff check src/spicy_regs/relationship_views tests/test_relationship_views*.py
```

The retained Senate fixture proves all native document blocks survive alongside
independent empty-ID amendment blocks, including `PN55-25`. The meeting fixture
checks independently retained jackets, witnesses and offered document objects.
See `tests/fixtures/relationship_views/README.md` for capture provenance and
limitations. Synthetic controls cover repeated pairs, malformed/null fields,
complete keys, old schemas and request-cursor visibility. These checks do not
establish publication, source-population completeness or deployed availability.

## Congress metadata retention decision

Decision `ss-2jix`, 2026-11-06: retain revisions of `amendments`, `nominations`
and `treaties` in their existing tables. Keep the other tables below at their
current logical grain. This first cohort answers how the publisher changed an
identified legislative item's description, purpose or reported status. Successive
captures do not establish a complete action history.

These are decisions for fresh generations. Originals and receipts stay unchanged.
Implementation must refuse incompatible prior table/policy inputs and explain
how to rebuild from selected originals; it must not relabel old rows as qualified
revisions. No new history table or `first_observed_at` is needed. This decision
changes no writer, query view or published data.

### Per-table meaning and current consumers

“Retain current” means retain the existing logical record or event grain; it
does not mean delete earlier original captures. A source update coordinate
remains useful evidence when it is not part of the table key.

| Table and disposition | Useful query and source coordinate | Current-record consumer and selection boundary |
| --- | --- | --- |
| `amendments`: **retain revisions** | How did an amendment's purpose, description or reported latest action change? Logical identity is `amendment_id`; `update_date` is a source update marker. Also identify the exact list/detail records supplying the fields, as specified below. Proposed/submitted dates remain legislative events. | Amendment watermark/detail-resume lookup, scorecard resolution's unique `amendment_id` index and Explorer amendment targets require one current amendment before indexing or joining. |
| `nominations`: **retain revisions** | How did a nomination part's description, organization, reported latest action or related committees/hearings change? Logical identity is `(congress, citation)`; keep number/part identity, `update_date` and the exact source-read coordinate below. `received_date` is receipt by Congress. | The index builder's `_held_rows`, unread nomination retry path and Explorer nomination targets require one current row per complete logical key. Independently walked committee/hearing lists belong to their recorded read, not automatically to the top-level update instant. |
| `treaties`: **retain revisions** | How did a treaty's titles, topic or resolution of ratification change? Logical identity is `(congress_received, number, suffix)`, spelled by `treaty_id`; retain `update_date` and the exact list/detail read coordinate. Transmission and entry-into-force dates remain events. | Index held/detail lookup and Explorer treaty targets require one current treaty. A record with no addressable detail remains explicitly list-only. |
| `committee_meetings`: **retain current** | What meeting is scheduled, and what documents or transcript jackets does its captured detail list? Use `(congress, chamber, event_id)` and the effective `update_date` beside list/detail evidence. A later transcript can update the record without creating another meeting. | `meeting_*` views, Explorer navigation and hearing-to-meeting joins use the full meeting key once. Keep unread detail, source omission and read-empty lists distinct. Repeating transcript-driven updates does not supply a source-backed reschedule history. |
| `committees`: **retain current** | Which committee does a code identify, and what historical names/authorities does its captured `history` state? Use `system_code`. List `update_date`, `detail_update_date` and nested `history[].updateDate` date different source statements; none dates the whole combined row. | Roster held lookup, meeting/referral navigation and bill/communication joins use one row per `system_code`; `is_current` filters present committees. Explicit publisher history remains available. Repeated list/detail counts are not additional committee histories. |
| `congress_bills`: **retain current** | What is the best retained bill index and its supported enrichment? Use `bill_id`; retain `update_date`, `update_date_including_text` and source/method evidence. These markers do not date every field in a coalesced row. | Bill-family checkpoints/restaging, scorecard resolution, related-bill views and bill joins expect one bill. Complementary list/API/BILLSTATUS inputs are coalesced; a newer date can coexist with an older incoming non-null value. This constructed current record is not a literal snapshot. Actions, printings and CRS summaries answer their separately stated historical questions. |
| `house_communications`: **retain current** | Which rule/report submission was received and where was it referred? Use `(congress, communication_type, number)`. API rows carry `update_date`; reconstructed printed-Record rows legitimately have NULL there and retain package/granule/text evidence. | `house_communication_rins`, Explorer committee/Record routes and regulatory joins use one communication. Preserve the source-route distinction and preference for the publisher's own decomposition. Record/referral dates are events; no API revision is invented for the printed era. |
| `laws`: **retain current** | Which law enacted a bill, and what authoritative citation/text was captured? Use `(congress, law_type, number)`. `update_date` and `update_date_including_text` date the Congress list, separately from PLAW input identity and approved date. | Law held/checkpoint lookup, `law_bills_from` and bill citation enrichment require one law per key. List refreshes are not new enactments or legal text versions; text correction evidence belongs to its source-specific processing. |
| `record_issues`: **retain current** | Which sections and offered files belong to this Congressional Record issue? Use `(volume, issue)` with `update_date` and detail evidence; `issue_date` dates the issue. | Index held/detail lookup and Explorer `record_package_id` navigation need one issue. A file-list refresh is not another printed issue; retained originals support a later source-specific correction question. |
| `bill_committees`: **retain current** | Which committees/subcommittees does the retained BILLSTATUS record name for a bill? Use `(bill_id, system_code)`; `snapshot_update_date` dates the parent BILLSTATUS record. | Referral/money-bill classification and bill-to-committee joins use one relationship per key. An unchanged relationship repeated under a later parent stamp is not a new referral. Parent evidence remains in receipts. |
| `bill_committee_activities`: **retain current event grain** | What named activities does the retained source list, including exact repeats? Use `(bill_id, system_code, activity_name, activity_date, occurrence)`; parent `snapshot_update_date` stays receipt evidence. Empty activity dates stay empty. | Bill-family merging and joins to `bill_committees` use the complete event/occurrence key. Repeating events for every parent refresh inflates counts. The current retained event union does not prove that the latest parent still lists an earlier event, or establish complete committee activity history. |

### Selected-cohort revision and current-selection rules

Each selected row is a **captured combination of publisher records**, not an
atomic publisher snapshot. Preserve each component's native identity, original
digest/locator and source update date in receipts. Keep separate list/detail
dates in the existing subject when supplied, and distinguish unread/omitted
detail from read-empty detail. Nomination relationship walks retain their own
witnesses and ordered occurrences. A later detail read or changed processing
rule must not claim a publisher change.

Use the existing logical key plus `update_date` and a deterministic
`source_revision_sha256` in each existing table. Define that digest from the
ordered, role-labelled canonical source records supplying the row: list,
optional detail, and independently walked related records where applicable.
Include source field states, order and multiplicity; exclude capture time,
transport formatting, processing rules and host enrichment. Keep exact
original-byte witnesses separately. The digest distinguishes source content;
it establishes neither publication time nor the order of same-date records.
Keep `update_date` as the shaper's effective source stamp, with its list/detail
basis explicit; do not replace it with the maximum of unrelated component dates.

Reuse the CRS approach where it fits: revisions stay in the existing table,
identical repeats coalesce, matching receipts keep exact lineage, and a current
record is selected before a logical-key join. This cohort needs the extra
content coordinate because separate source reads can differ without a new
top-level stamp. The maintained current selector must:

- Select the greatest valid effective publisher update date per logical key
  before watermarks, held-row maps, detail retries, scorecard indexing or target
  joins. Older retained inputs add history without winning by arrival order.
- At one date, allow detail to complete an otherwise identical list-only record
  only when overlapping source-stated fields agree. This is capture completion,
  not a publisher change; preserve the earlier capture's evidence.
- Preserve differing same-date contents. If incompatible candidates could be
  current, refuse selection explicitly and preserve the previous selected
  generation. Do not choose by digest, file order, capture time or population
  of fields. Historical reads can inspect the conflict and source witnesses.
- Refuse missing/unusable update dates before the shared non-null identity
  filter can discard rows. Preserve the source and refusal reason; do not
  substitute an action, receipt or capture date.

Change grouping **before** writing. Pooled listing currently qualifies coverage
by logical identity while keeping the greatest `updateDate`; `_walk` then builds
another logical-key dictionary. Keep coverage/count qualification by logical
identity while retaining distinct source revisions observed across passes.
Read detail only for a currently addressable candidate and bind its actual
list/detail witnesses; never attach today's detail to every older list version.
An older list-only revision remains a limited list record. This cannot recover
updates overwritten before capture or revisions published between polls.

Implement an amendment-builder change and a separate selected-index change for
nominations/treaties. Keep source composition local rather than adding a
universal history mode. Acceptance covers distinct revisions in one input and
across refreshes, unchanged replay, older-source replay, same-date conflicts,
missing dates, detail completion at an unchanged date, exact receipt witnesses
and stable current joins. Small retained samples and synthetic failure controls
establish these mechanisms without a full-corpus replay.

### Evidence and timing limits

The decision inspected SpicyRegs `5366faec6f4c5e50463c26235d3a7d30e52010f4`
and SpicyDocs `3ba96ae3d45ffab832b31e4e4b69befa46091bc0`. Maintained owners:
amendment composition (`src/spicy_regs/transforms/build_amendments.py`),
index pooling/resume (`src/spicy_regs/transforms/build_congress_index.py`),
bill merging (`src/spicy_regs/transforms/table_merge.py`),
source/receipt fields (`src/spicy_regs/congress_subjects.py`),
scorecard indexing (`src/spicy_regs/scorecards/resolution.py`) and
Explorer navigation (`src/spicy_regs/explorer_navigation.py`).
The retained audit at
`/Users/mikewolfd/Work/worktrees/crs-summary-revisions-20261105/update-field-audit/validation/`
already proves pooling, parent stamps and coalescing under its stated pins.
Those unchanged mechanisms were reread, not replayed. This decision establishes
no corpus completeness or publication qualification.

Introduction, proposal/submission, receipt, meeting, referral, transmission and
Record issue dates remain their named events. Congress.gov summary `updateDate`
may describe publication or republication; bill `textVersions[].date` describes
an action. A final-text-to-summary delay needs independently evidenced publication
of the relevant final text version. The earliest retained summary update alone
does not establish first availability.
