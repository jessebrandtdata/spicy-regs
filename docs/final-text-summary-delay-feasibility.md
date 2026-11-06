# Final-text-to-first-summary delay: research conclusion

The requested delay metric remains unavailable for the bounded retained-evidence
cohort reviewed on November 6, 2026. None of the selected bills supports an exact
delay or a finite availability interval. This is a completed feasibility
investigation for `ss-azn1`, not delivery of a measurement or evidence that every
historical bill is unmeasurable. No delay query was added.

The [pinned endpoint matrix](evidence/final-text-summary-delay-2026-11-06.json)
contains the frozen identities, literal dates, summary-text hashes, original
paths and member checksums, classification reasons, processing pins and review
images. The stable local evidence lives in
`~/Work/worktrees/crs-depth-p1-20261106/evidence/ss-azn1/`.

## What was selected

The frozen cohort combines the exact printing cases already qualified by
`ss-m4r1` with the legacy and current summary-clock controls from `ss-ze62`.
It tests whether those retained facts support the requested result. It is a
purposeful sample, not a representative sample of Congress or the publisher.
One exact printing candidate was selected from each original's offered text
list before any delay calculation; no delay was calculated.

“Final” means the final **bill text**, not passage, enactment, the public-law
rendition, or the newest action timestamp. The enrolled printing is the selected
final bill identity for enacted H.J. Res. 25. The other rows retain exact latest
offered candidates; their finalness remains unresolved. A current Congress can
produce more printings. The closed-Congress legacy snapshot does not prove an
exhaustive printing history. The report does not exclude failed bills in
principle; this evidence does not qualify their finalness.

CRS summaries describe action stages and do not necessarily identify a printed
version. A later-stage summary is a named candidate, not an automatic join to
the latest printing. Earlier-stage summaries are not silently reused. Doing so
would require reviewed evidence that they describe the selected final text.
These source distinctions are documented in the
[BILLSUM guide](https://github.com/usgpo/bulk-data/blob/main/Bills-Summary-XML-User-Guide.md#11-types-of-bill-summaries)
and the
[summary endpoint definitions](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/SummariesEndpoint.md).

## Endpoint-evidence matrix

| Bill | Exact final-printing candidate | Selected summary candidate | Available facts and unresolved endpoint |
| --- | --- | --- | --- |
| 119-H.J.Res.25 | `BILLS-119hjres25enr` | `49`, Public Law; action `2025-04-10`; update `2026-06-17T12:47:01Z` | Enrolled body and Public Law `119-5` are retained. Native issue/ingest day is `2025-03-28`; the body/action date is empty. Later metadata is not atomically bound to the older body. No first-publication event, stage-specific first-posting witness, or reviewed enrolled-to-law-summary correspondence establishes the pair. |
| 119-H.R.6028 | `BILLS-119hr6028eh` | `53`, Passed House; action `2026-06-08`; update `2026-06-12T20:14:08Z` | The qualified introduced body `...ih` is superseded by the offered engrossed printing in this snapshot. The introduced metadata's issue day `2025-11-12` and ingest day `2025-11-18` belong to `ih`. The `eh` body/metadata and first-publication evidence were not selected. They are not deemed absent. |
| 119-H.R.3426 | `BILLS-119hr3426rfs2` | None; only introduced stage `00` observed | The exact numbered reprint body is retained; native issue day is `2025-12-01`, ingest day `2025-12-02`. MODS calls it `rfs` with `reprint=false`; that literal label does not erase `rfs2`. Only stage `00` occurs in the selected summary block. Current-Congress finalness, exact correspondence and both first-publication endpoints remain unqualified. |
| 113-H.R.4200 | `BILLS-113hr4200rfs` | `81`, Passed House without amendment; action `2014-12-02`; item update `2014-12-02T11:35:43Z`; summary update `2014-12-03T19:28:27Z` | The retained legacy document offers a later referred-Senate printing. The passed-House summary says it repeats the unamended introduced summary; this does not establish an exact `rfs` body association. The `rfs` body/metadata were not selected. Neither source clock is first publication. |
| 119-S.1377 | `BILLS-119s1377rfh` | None; only introduced stage `00` observed | The source offers referred-House text dated by action `2026-09-10T04:00:00Z`, but its sole summary is introduced-Senate, updated `2026-02-10T00:13:00Z`. `lastSummaryUpdateDate` is absent. The `rfh` body/metadata were not selected. Finalness, correspondence and first publication remain unqualified. |

Dates above retain their source spelling and precision. Native issue/ingest
values have day precision with no stated time zone. Source summary clocks retain
seconds and `Z`. No day value was converted to a fabricated midnight instant.

## Classification and coverage

These are separate dimensions, not categories to add across rows:

| Dimension | Result in the frozen cohort |
| --- | --- |
| Frozen bill identities | 5 |
| Qualified enrolled final identity | 1 |
| Finalness unresolved | 4 |
| Eligible for a supported delay measurement | 0 |
| Final-printing/summary pairs without qualified exact correspondence | 5 |
| Absent summary files among selected originals | 0 |
| Only an earlier summary stage observed in the selected original | 2 |
| Named later summary candidate observed | 3 |
| Exact / bounded / unknown delay | 0 / 0 / 5 |

All selected bill XML files contain summaries. “Only an earlier stage observed”
means exactly that about these original members. It does not establish that a
later summary never existed or is absent from a current publisher response.
Missing candidate bodies and BILLSUM files were not requested in this
investigation, so no absence inference follows.

## Why the available dates do not identify the delay

The [Congress text definition](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/BillEndpoint.md#text-level)
associates `textVersions.date` with an action rather than printing. The same
guide says summary `updateDate` can represent publication or republication;
the [summary guide](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/SummariesEndpoint.md)
likewise includes republication in `lastSummaryUpdateDate`. Taking the minimum
retained update therefore selects an available update, not a proven first
publication. The legacy example demonstrates why the two literal clocks must
also remain separate.

The [BILLSUM attribute definition](https://github.com/usgpo/bulk-data/blob/main/Bills-Summary-XML-User-Guide.md#22-attributes)
assigns `orig-publish-date` to the bill-level `<item>` and dates first publication
of its summary **file**. Individual `<summary>` items have their own identities
and update dates. The guide's H.R. 910 example places a February file date
beside introduced and later reported-stage summaries. That example demonstrates
why the file attribute alone cannot date first publication of an arbitrary
later summary. No selected original BILLSUM file or before/after membership
history qualifies that attribute for this cohort, so it was not used.

The enrolled original appears with identical member bytes in two distinct
retained BILLSTATUS ZIPs. The retained ZIP receipts do not establish capture
instants or complete successful requested-resource responses. Distinct container
hashes cannot supply an availability bracket. An archive timestamp or a local
file time would be an observation, not a publication event.

The bill-body observations and later metadata observations from `ss-m4r1`
also do not provide both endpoints of an informative lag interval. Two later
presence observations, without supported earlier bounds, cannot establish a
finite delay range. Current republication must not replace or move a supported
earlier availability bound if such evidence is later found.

## What would change the conclusion

| Owner | Missing input or decision |
| --- | --- |
| SpicyDocs bill-source acquisition and literal reading | Retain the exact selected final printing, a source-backed publication event or qualified before/after availability observations, and original receipts that establish what was requested and whether each response was complete and successful. Broader metadata preservation alone does not establish first publication. |
| SpicyDocs CRS/BILLSUM acquisition and literal reading | Recover a stage-specific first-publication witness or complete requested absence/presence observations. Qualify BILLSUM file history and summary membership before assigning its file date to a particular stage. Preserve earlier evidence through later republication. |
| SpicyRegs legislative analysis and query | Review final-printing selection and the printing-to-summary-stage match, including unamended text reuse, enrolled versus enacted text and numbered reprints. Add a precision-aware query only when real endpoint evidence permits informative results; then validate exact/bounded math and missing/mismatched cases with separate synthetic controls. |

These are evidence gaps, not a claim that a new parser field would recover the
missing historical event. `ss-m4r1` and `ss-ze62` improve native evidence and date
interpretation; they do not supply the missing first-publication history.

## Review and resource evidence

`ADMISSION.json` declared a frozen cohort, zero publisher data requests, bounded
selected-record and decoded-byte reads, a 1 GiB resident-memory limit, 25 MB new
disk limit and one-hour stop. The offline probe read only six named original
records, including the repeated enrolled record: 151,218 decoded bytes, largest
record 31,195 bytes. It measured 1.09 seconds and 63,504,384 bytes peak resident
memory for that probe. These observations describe the probe, not the entire
human research session or a production-scale run. The catalog was queried by
indexed object digests; it was neither copied nor hashed. ZIP container pins
reuse retained qualified manifests; the probe read selected members with ZIP
CRC checks and computed new member digests. It did not scan or rehash whole ZIPs.

The agent actually viewed every selected summary's original-source render,
including original CDATA and native date lines. `VISUAL-REVIEW.md` and
`visual-originals.json` record exact paths, checksums, locators, images, findings
and sample limits. The exact prior bill-body and metadata visual review remains
in the sibling `ss-m4r1/` evidence directory. All sampled values are valid literal
source facts; no value correction is warranted. Unsupported finalness,
correspondence and publication events are explicit unresolved findings.

No source reader, table, schema, policy, package version or runtime environment
changed. No selected release, raw original, source-catalog selection, schedule,
hosted publication or full-corpus replay changed. Research validation checked
the matrix's identities, witnesses, classifications and local links. Application
tests and successful metric readback are not required or claimed for this
research-only conclusion.
