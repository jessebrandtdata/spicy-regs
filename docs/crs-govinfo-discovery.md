# CRS and GovInfo discovery

A generated table does not establish acquisition completeness. These families
record `acquisition-coverage` in their retained source-evidence journal. The
maintained rollup also exposes `acquisition_status`; a committee or print run
with retryable or deferred work generates its candidate and then raises through
the existing deferred-failure path. CRS detail failures are likewise separate
from list acquisition. Publisher-stated missing root renditions remain a distinct
outcome. None of these outcomes establishes complete historical coverage.

## Choose the input for the required facts

| Family | Selected route and reusable input | Alternatives and remaining differences |
| --- | --- | --- |
| Bill summaries | Reuse pinned BILLSTATUS XML/archive originals for baseline and replay; use the existing bill-family folder refresh for changed status files. Preserve every summary and its action, version and update fields. The exact archive/member pins are in the retained rebuild's `crs/bill-status-archives.json`, `bill-status-archive-extension.json` and `bill-status-xml.json`. | Congress.gov `/summaries` supplies text and bill links but defaults to the last day without explicit dates. Per-bill summaries recover that bill's current supplied set. BILLSTATUS is a separate XML schema and current snapshot; neither route recovers overwritten revisions or establishes first publication. A bulk folder modification is not a summary publication event. |
| Standalone CRS products | Congress.gov `/crsreport` for metadata membership; explicit selected item reads add authors, formats and related materials. Reuse central originals listed by `crs/crs-files.json` and `crs-verified-inventory.json`, with exact identity verified by that retained rebuild. Ordinary windows use the selected prior discovery cutoff with overlap. Explicit reconciliation walks the native inventory without a date predicate. | Cached list pages are exact past observations, not current inventory. PDFs and numbered rendition URLs provide documents, not complete metadata membership; archived snapshots are useful only for the exact captured versions. No qualified same-population bulk substitute was found. The API date predicate is not an exact predicate over printed `updateDate`. |
| GovInfo documents | `/collections/{collection}/{start}/{end}` selects packages by native added/modified time; package summary, MODS and stated renditions retain full selected facts. `/published` supplies an independent bounded issue-date inventory. Reuse the committee/print retained rebuild's input inventories and central raw hashes. | Published issue dates, collection `lastModified`, MODS issued/ingested dates and file checksums answer different questions. Bulk BILLSTATUS is useful for bills; it does not replace CRPT, CHRG or BUDGET package/granule membership. Sitemaps advertise package URLs/last modification, not MODS multiplicity or body/granule completeness. Archive or cache membership is observation-specific; unavailable originals remain gaps. |

The retained rebuild is
`~/Work/worktrees/revision-retention-20261106/rebuild/`; its `README.md` and
`result.json` identify accepted candidate generations and explicit missing
inputs. Raw originals stay in `~/Work/corpora/spicyregs/raw/`. Query the selected
catalog by collection and hash before acquiring. Do not infer version identity
from a family name or capture date.

[Congress.gov summaries documentation](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/SummariesEndpoint.md),
[CRS fields](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/CRSReportEndpoint.md),
[GovInfo predicates](https://github.com/usgpo/api#collections-service) and
[available bulk collections](https://www.govinfo.gov/bulkdata) explain the native
routes. Current source behavior still requires bounded observation.

## Progress, retry and reconciliation

CRS and committee discovery read live progress from the prior **selected
generation's** pinned evidence journal. Imported row timestamps never establish
that progress. A crawl records its start time only after successful discovery
and output preparation; conversion and generation admission must still succeed
before that record can be selected. A later failed attempt's journal cannot move
the prior generation's cursor. Explicit historical windows and detail-only reads
carry the prior live cursor unchanged.

CRS reopens three UTC days before its completed discovery cutoff. The lower bound
opens one second before the selected date, so an exclusive native bound cannot
drop that day's first second. Every returned query member survives; a local
`updateDate` filter would apply a different predicate and could discard a late
indexed record. The reader keeps its existing complete-page/count checks and
pooled walks. These checks establish an observed query, not an atomic snapshot.

Use `CRS_REPORTS_SINCE=YYYY-MM-DD` for an explicit dated acquisition, or
`CRS_REPORTS_RECONCILE=true` for an unfiltered metadata inventory through the
ordinary `spicy_regs.pipelines.rollups.crs_reports` entrypoint. The two selections
are mutually exclusive. A native inventory captures currently advertised
metadata; it does not reconstruct historical editions. Retained completed detail
and witnesses remain attached at unchanged revision coordinates.

Committee discovery reopens 24 hours before its completed crawl cutoff. Without
qualified discovery state it declares the existing 30-day cold scope, regardless
of historical row dates. Set `COMMITTEE_REPORTS_SINCE` and
`COMMITTEE_REPORTS_UNTIL` to bound a reconciliation window through the maintained
committee entrypoint; explicit windows carry live progress unchanged. Every
unsettled discovered package is retained in the existing read table before the
body cap. Failed pages abort without committing progress. Failed bodies and
deferred packages remain retryable after the live cursor moves.

Print discovery enumerates its declared issue-date scope each run. It has no
live timestamp cursor. It now records cap-deferred identities in the existing
output checkpoints, so a subsequent inventory omission cannot erase that work.
Successful current checkpoints suppress body acquisition and extraction; changed
native revisions remain eligible.

Proposed operating policy: retain the existing refresh cadence, review an explicit
CRS metadata reconciliation monthly, and review bounded GovInfo issue inventories
against modification windows weekly for active issue scopes. These are review
cadences, not activated schedules. Late arrivals within overlap are re-asked;
arrivals outside it require reconciliation. No finite overlap promises arbitrary
publisher indexing delay. Older periods require explicitly selected inventory
work, not an automatic full-corpus scan.

## Bounded qualification and cost basis

The dated evidence is
`~/Work/worktrees/crs-depth-p1-20261106/evidence/ss-wx82/`. Its admission file
bounds the whole run, including inherited inputs, candidates, scratch and shared
capacity. `reusable-inputs.json` pins the existing manifests and records indexed
catalog lookups; `live-probe.json`, `crs-window-result.json` and their capture
journals retain exact URLs, hashes, observed times, counts and IDs.

On 2026-11-06 the bounded current CRS prefix advertised `R49505`, `R40977` and
`R49510`. One-second windows around the first two printed `updateDate` values each
returned zero; broader day windows returned both. This reverified the old warning
without asserting an internal cause or treating it as source lag. The comparison
records exact included/omitted IDs, and the prefix is explicitly incomplete.
A BUDGET issue inventory for 2025-05-01 through 2025-05-10 advertised
`BUDGET-2026-BUD`; the matching bounded modification-time query returned the same
ID and no extras. Its issue period and modification time differ legitimately.
Neither comparison establishes whole-source equality or an atomic snapshot.

Measured current probes used seven requests and 25,433 decoded response bytes;
a further maintained CRS window used one request and 11,437 bytes for 28 records.
An earlier instrumentation failure spent one request/1,545 bytes without retained
source facts and remains in its failed-attempt log. No bodies were downloaded.
The prior retained CRS baseline completed in 57 requests for 14,137 records.
Its reusable unique list bodies total 6,900,648 decoded bytes; these are dated
observations, not an estimate of today's complete population. A clean current CRS
inventory is estimated as `ceil(advertised_count / 250)` requests, plus retries or
pooled passes; body enrichment is separately bounded by its explicit item limit.

The retained initial BILLSTATUS archive selection totals 529,966,446 stored bytes;
its archive extension totals 52,546,531 bytes, and separately retained XML totals
938,298 bytes. Reuse costs zero source requests; decoded XML and conversion can
cost substantially more, so this task did not replay those archives. A selected
GovInfo inventory costs `ceil(count / page_size)` requests, then package reads
normally cost summary + MODS + each offered selected body. Granules require their
own enumeration and bodies. No byte estimate for an unmeasured full GovInfo crawl
is offered.

The bounded reuse probe (`reuse-measurements.json`) keeps two selected print
identities fixed, then changes one dated revision. Initial/unchanged/changed runs
read and extract respectively two/zero/one bodies; native list reads remain two
per run. The unchanged run reads zero body bytes and the single change reads
1,033 body bytes. Table merges still write their outputs. These small synthetic
controls prove skip behavior, not a production speedup.

The original-backed consumer probe (`consumer-qualification.json`) replays the
exact retained CRS window through `CrsReportsRollup.run`, generation admission
and native Parquet queries. It compares every selected source ID, update time,
title, version and status. `visual-review.json` links the original paths/hashes,
native locators, viewed literal-source renderings, findings and sample limits.

CRS same-coordinate conflict checks run before pooled walks can replace a repeated
observation and again against selected prior rows, including list-only rows.
Contradictory populated scalar facts refuse; omission preserves known values,
while explicit null/empty remains a clearing. Each distinct dated revision
actually observed during a shifting crawl survives for the admitted report IDs.
These checks keep the generic pooled reader unchanged.
