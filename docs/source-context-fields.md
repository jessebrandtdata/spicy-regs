# Source facts and recorded associations

The main tables carry the fields needed to understand a record and follow its declared relationships. Receipts retain the original observations, input pins, conversion details, and failures. Navigation uses main fields; evidence does not create an edge by itself.

- Comment periods retain independent proceeding, docket, and RIN memberships. New `evidence_occurrences` retain repeated references, source order, role, and the complete native document identity. A Federal Register reference includes its publication date. A Regulations.gov reference uses its document ID. Earlier periods without these details keep `evidence_occurrences` null until their maintained builder runs over the selected sources.
- Lifecycle events retain `source`, `dated_by`, `evidence_id`, and `joined_by` beside `event_date`. A missing date basis cannot establish a Federal Register publication date. Document and comment extraction results describe an attempt; an offered attachment, a captured body, and usable text are separate facts.
- Court PDF rows identify an opinion and a captured body. They retain the recorded parent publication and member pins, URLs, digests, extraction result, and extractor version. That recorded parent is separate from lookup against today's opinion table. A new captured cohort requires all exact selected parent members and exactly one matching opinion, cluster, offered URL, and native digest. SHA-1 alone never establishes a parent link.
- GAO decisions retain every B-number occurrence and any truncation flag. A shared B-number does not establish a GAO report ID. Senate expenditure records retain their package, file, body, page, grid, table, and row context. Payee and office names do not establish person identities.
- Scorecard subjects retain their recorded snapshot and capture associations while their logical business keys stay unchanged. Snapshots retain ordered capture roles, parsing and completeness facts. Resolution rows retain status, rules, candidates, and selected input pins. Valid unresolved, ambiguous, or conflicting decisions remain main rows with no resolved target. Only `resolved` decisions can supply an official-record link; current lookup remains a separate result.

## Reader and data release order

Release the reader before new data. Exact earlier declarations remain recorded in `src/spicy_regs/policy_migrations.json`, the ledger of every policy each dataset was published under. A selected historical file is read under its original schema and receipts. New writers use the new policy; they never relabel old bytes. A historical receipt-only dataset stays unavailable as a main table until a real main member is published. A real current empty main file remains a visible empty table.

The changed policies are `regulations-native-v2` for eight affected regulation datasets, `courts/2` for opinions and PDF extractions, `government-sources/3` for GAO reports and decisions, and `scorecards-etl-v2` / `scorecards-etl-ratings-v4` for scorecards. Unchanged Court and bill prepared families retain their selected producer and publication pins.

## Rebuild only affected families

Use the existing APIs with explicit selected files and generation IDs in a private output directory. These steps prepare local candidates; publication and service deployment remain separate.

| Data | Maintained entry points | Required input |
| --- | --- | --- |
| Existing regulation facts | `regulations_receipts.read_internal(ReceiptInput(...))`, then `build_local_generation(...)` | Exact matching subject/receipt pair; original source strings preserved |
| New period occurrences | `regulations_receipts.build_from_receipts(..., builder=build_comment_periods, outputs=["comment_periods"])` | Selected documents, Federal Register, proceedings and other builder inputs |
| Court retained facts | `court_receipts.restore_processing_input(...)`, then `write_court_rows(..., prior_receipts=...)` | Exact old subject/receipt pair and its original processing schema |
| New court captures | `build_court_pdf_extractions.prepare_captured_opinions(..., parent_subjects=...)` | Every selected opinion member, explicit bounded captures and recorded read snapshot |
| GAO retained facts | `government_receipts.processing_input(...)`, then its existing governed writer | Exact selected prior plus its retained source observations |
| Scorecard source facts | `scorecards.etl.read_indexed_family(...)`, then `write_family(...)` | Complete exact selected source family, including historical snapshot receipts |
| Scorecard resolutions | `build_scorecard_analysis(...)` | Exact main source/official members, receipt generations and input pins |

Do not run normal provider-refresh commands merely to promote retained fields. Rebuild period occurrences and scorecard resolutions when their interpretation changed. After preparation, verify complete identities, row counts, refusals, unresolved outcomes, and all affected forward/reverse relationships against matching new publications. Earlier measurements remain evidence only for their original pins.

## Current registry and operational-record retention

Decision `ss-pva3`, 2026-11-06: keep lobbying profiles, SAM registrations, FCC
proceedings and Regulations.gov dockets/documents/comments **current-only** in
their existing tables. Their useful supported queries concern one selected
profile, registration, docket or document. Captures and receipts retain earlier
source evidence; current-only tables do not establish historical validity.
This decision follows source meaning and the query need, including for fresh
greenfield generations. It does not rely on preserving an old storage layout.
No implementation ticket is selected for these families by this decision, and
no new table or `first_observed_at` is introduced.

| Family and useful query | Source identity and revision evidence | Current selection and historical limit |
| --- | --- | --- |
| `lobbying_registrants` and `lobbying_clients`: what profile do the selected LDA captures supply for the parties to this filing? | Registrant identity is `registrant_id`; client identity is LDA's `client_id`, not the registrant-local `registrant_client_number` or an organization identity. Registrants have `dt_updated`; clients have no such marker. Both retain `captured_at` and `captured_filing_uuid`, with page witnesses in receipts. | Before mapping finishes, `_profile_rank` retains the greatest registrant `dt_updated`, then capture time; clients rank only by capture time. Equal ranks keep the first seen. Native incremental merging replaces the matching profile with the fresh embedded profile. This is a current-profile selection policy, not a unique publisher revision identity or a source conflict adjudication. LDA embeds its current profile even in old filings. A captured profile is never evidence of the profile at historical filing time; filing-specific fields and immutable filing/amendment identities answer filing-history questions. |
| `sam_entities`: what active registration does the selected completed SAM year scope hold for this UEI/EFT? | Identity is `(uei, entity_eft_indicator)`, with NULL EFT distinct from a stated indicator. Source `entityRegistration.lastUpdateDate` orders different-dated versions before subject mapping, but is not a unique revision ID and is not a public subject column. Exact extract digest and record locator identify the retained evidence. Registration and expiration dates are separate events. | `registrations()` collapses identical records, selects strictly newer dates and refuses different records tied at one date. Unbounded complete active-registration-year extracts retire missing registrations in that scope; capped/partial reads do not establish retirement. A row is active as of its scope's last accepted read, not necessarily today. Existing recipient enrichment groups registrations by UEI before joining, preserving EFT multiplicity without multiplying recipient amounts. Absence from a complete active set is not an authoritative historical expiration instant. |
| `fcc_proceedings`: which original docket and source-stated rule dates should this filing navigate to? | Public identity is docket `name`; native `id_proceeding` identifies one ECFS source document. Its `date_last_modified` is used before mapping and is not a public subject column. Native document identity, source update marker and original response witness identify evidence; a docket name alone does not identify every source document or revision. | Duplicate names select earliest `date_proceeding_created`, then latest `date_last_modified`, most populated document and lowest native ID. This preference deliberately chooses the original docket over a newly recreated sparse document. It is not “latest modification across the name,” and alternates do not by themselves establish chronological versions. Filing navigation must preserve both native ID and name; a nonselected alternate remains an unmatched/ambiguous source observation, not an invented current target. |
| Regulations.gov `dockets`, `documents`, `comments`: what does the selected docket/document/comment record say, and what current content is available? | Native `docket_id`, `document_id` and `comment_id` remain their separate identities. `modify_date` versions publisher fields only. Documents/comments keep `posted_date` separately; it is not an update coordinate, and docket `modify_date` is not an opening date. Original responses and processing receipts identify source versus host changes. | Staging merge selects greatest `modify_date` per native ID; native incremental merge replaces matching logical identities. These paths have different refresh mechanics, not an implicit historical rule. Neither marker versions host text extraction, attachment processing, model output or enrichment. Docket/document/comment joins and counts use each selected identity once; attributes, explicit comment-on references, NULL docket meanings and source-specific status/removal observations remain separate. |

For SAM, reuse the accepted `ss-p08j` refusal evidence: different literal
records for UEI `U4A5NN4HRAC5`, NULL EFT, both dated `2026-10-14`, occur at
`/entityData/6392` and `/entityData/6796` in original SHA-256
`1583590cf3c005afc2038635320bf0183591ef15569efd895fdd5af35d2de74e`.
That acceptance verified the original; this decision did not reread the extract.
Preserve the refusal and prior selected output. A digest, capture date or scan
order cannot choose the authoritative winner. Do not reopen that accepted source
work or promote a deterministic tie-break into historical truth.

An as-of profile, registration or rulemaking metadata query would need more than
a date added to a key. It would need all source records retained before the
profile/SAM/FCC collapsing step, explicit treatment of omitted source markers,
a distinction between a complete active scope and partial absence, and selection
of one applicable parent before child joins. FCC would also need source document
identity distinguished from docket identity. Regulations.gov would need source
revisions separated from host processing results and coordinated selection of
dockets, documents, comments and attributes. These are requirements for a future
explicit historical product, not selected implementation work. The present
decision does not claim those queries are supported by current tables or that
all intervening versions can be recovered from retained captures.

Keep `ss-hqoi`'s historical **filing population**, amendment-aware totals ownership,
repeat-night proof and unresolved posting/conviction-date meanings unchanged;
that work does not promise historical embedded profiles. Keep `ss-17vd`'s FCC
date/rule-section publication and hosted readback scope unchanged. The separate
comments hold, deferred PDF/OCR work and other source-specific holds also stay
in place. Literal NULL, absent, empty and conflicting states keep their existing
meaning; no missing source value receives a fabricated date or historical state.

This decision inspected SpicyRegs `5366faec6f4c5e50463c26235d3a7d30e52010f4`
and SpicyDocs `3ba96ae3d45ffab832b31e4e4b69befa46091bc0`. Maintained owners:
profile collapse (`src/spicy_regs/transforms/build_lobbying_filings.py`),
SAM mapping/retirement (`src/spicy_regs/transforms/build_sam_entities.py`),
FCC preferred document (`src/spicy_regs/transforms/build_fcc_ecfs.py`),
regulation staging selection (`src/spicy_regs/transforms/merge_staging_files.py`)
and native refresh (`src/spicy_regs/native_merge.py`). Source-owned definitions
are `spicy_docs/sources/sam_extract.py` and `spicy_docs/schemas/regulations.py`
at the stated source pin. Existing bounded SAM conflict/retirement evidence in
`/Users/mikewolfd/Work/worktrees/crs-summary-revisions-20261105/update-field-audit/validation/`
was reused. This prose-only decision ran no application tests, acquisition,
corpus replay, scheduler change or publication check.
