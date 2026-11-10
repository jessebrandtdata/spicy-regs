# Grants.gov opportunities and forecasts

`grants_opportunity_observations` keeps every admitted record in each explicitly
selected Grants.gov v2 daily XML export. One row identifies a source release,
the XML notice kind and the native opportunity ID. These are opportunities,
not USAspending awards.

The XML distinguishes `synopsis` and `forecast` records. A synopsis does not
prove an opportunity is currently posted or open. Export membership, a past
deadline or an archive date does not establish closed, deleted or withdrawn
status. The retained October 20, 2026 export contains historical dates, so it
must not be described as active-only. Different selected snapshots remain
distinct even when their native opportunity IDs match.

## Source meaning and values

SpicyDocs owns the complete ZIP/XML traversal, native identity, original bytes,
record order and source release admission. The domain mapper streams the admitted
records through the existing atomic Parquet writer. It refuses an incomplete
or rejected source population and publishes no generation on failure.

The table retains every direct XML field occurrence, in order, with its
expanded name, decoded text, attributes and XPath. XML decoding occurs once;
embedded HTML is not unescaped again. Repeated eligibility codes, funding
categories, instruments and Assistance Listing references remain ordered JSON
arrays. Assistance Listing numbers remain strings, including zeros.

Eligibility and funding-category labels reuse RefSpec's
`registry/grants_gov_codes.py` reader at
`f8857c8669a412bd6c254aa0b3a23cf05a52e04d`, with the official
[code page](https://www.grants.gov/api/status-codes) captured August 3, 2026
and pinned in `reference/refspec/grants-gov-codes.json`. Unknown codes remain
literal and receive a null label. “Other” and “Unrestricted” codes still retain
their additional eligibility text. The code list does not define opportunity
status or funding instrument semantics.

Dates are parsed only from valid `MMDDYYYY` text. Missing, blank and unparsed
values remain distinct in `value_statuses_json`; their exact wording remains
in `raw_fields_json`. Forecast estimated dates are separate from synopsis
dates. Capture time, export date, publisher update date and version are separate.

Amounts use exact `DECIMAL(38,2)` when representable, with no rounding.
The literal `none`, blanks, missing values and unrecognized text remain
distinct. A value outside this decimal range or scale remains raw with
`unrepresentable` status. `amount_unit` is null because the selected XML controls
do not establish a unit for every amount; no USD assumption is added.

## Local build and query

`acquire` reads the size and ETag the publisher states for one dated export,
then downloads it bound to that version through the maintained
`CaptureEvidence` transport, so the ZIP lands once in the central raw archive
with a receipt. An export republished between the two requests is refused. The
command writes a capture selection: each snapshot as its Grants.gov scope (the
export date, and the request URL, SHA-256, observation time and size of its
ZIP).

```sh
python -m spicy_regs.pipelines.grants_opportunities acquire \
  --snapshot-date 2026-11-09 --output-dir "$RUN_DIR" --captures-out captures.json
python -m spicy_regs.pipelines.grants_opportunities build \
  --captures captures.json --output-dir "$GRANTS_OUTPUT"
```

`build --captures` admits one source release per scope into
`$GRANTS_OUTPUT/source-releases` from the raw original (verified, never
written) and builds the generation. Admission is the slow step (5 minutes for
the 2026-11-09 export). Neither command changes a public selection; the
refresh sequence through publication is in
[local-family-refresh.md](local-family-refresh.md).

To build over releases admitted earlier and kept on disk, pass `--selection`
instead of `--captures`. It lists those releases and their exact retained ZIPs:

```json
{
  "sources": [{
    "release": "/local/source-release",
    "blobs": "/local/source-blobs",
    "logicalId": "<exact source logical ID>",
    "artifactDigest": "sha256:<exact release digest>",
    "verifierImplementationId": "<accepted source verifier ID>"
  }],
  "archives": {
    "sha256:<original ZIP digest>": "/local/retained-export.zip"
  }
}
```

### Release selection on refresh

`spicy_regs.pipelines.grants_opportunities.main` is the maintained refresh
caller. The capture selections or the `--selection` file given to `build` are
the complete input set for one new generation; `build` neither discovers the
latest export nor appends the preceding table. The builder already writes every
selected release. Keep this one selection mechanism rather than introducing
another revision key or history table. A release admitted again from its
capture by later code has a new digest, so a history that must keep an earlier
release's digest selects that kept release with `--selection`.

For a single-snapshot query, select the exact admitted release of interest. For
declared release history, retain the preceding selection file, add the next
admitted release to `sources`, and keep every earlier release pin and corresponding
`archives` entry in the next selection. Write that cumulative selection to a new
file and build into a fresh output directory. Keep the selection file and its
SHA-256 alongside the resulting generation and source evidence so its declared
coverage can be replayed. The existing evidence records each release's exact
logical ID, artifact digest, population outcome and compressed-original digest.
Do not replace a release pin merely because its snapshot date matches another.

A later-only selection intentionally produces later-only coverage. Selecting a
directory, retaining an old table, or possessing old original bytes does not add
them to the next generation. Report the selected snapshot dates **and release
digests**, plus any unselected, unadmitted or unavailable periods. Daily publisher
availability and the dates found within one export do not establish complete
historical coverage. Reuse the accepted full-export qualification in `ss-5bhb`;
this selection rule does not reopen it or acquire historical releases.

The accepted 2026-10-20 scope in `ss-5bhb` binds original ZIP SHA-256
`ee3dc41ec567dbea378f6b759fa071fb1679c69fab871dd1e49d987d7dc76508`
to source release
`sha256:125e9b3cfe0365ef6150954858f172eb4356435a18bed1e97a3ff97abbcdd615`
and domain generation
`sha256:b98a33ef40900f6dde4f49bfd9a41609178750d2af0207b00b52dbf1f0b0e5ab`.
These retained acceptance pins describe that selected export, not a continuous
daily history or a fresh readback of today's publication.

Compare observations using `(source_release_digest, notice_type, opportunity_id)`.
An unchanged cumulative selection retains the same source observations, while a
later snapshot may change a version/deadline or omit an opportunity. Omission
creates no withdrawal observation; source-stated lifecycle enrichment remains
owned by `ss-nguq`. `post_date` is the publisher's posting date for the notice and
`last_updated_date` is its stated update date; neither identifies the daily export
or proves when the notice first became publicly available.

The ordinary source-evidence writer retains the compressed original once per
distinct digest and records each source release and its population outcome.
No full decoded XML file is created. `build_generation` then admits the table.
Normal `sources.publication.publish_generation` can publish the resulting
generation with its evidence directory. Source profile packaging must be adopted
before the command can run from an installed SpicyDocs wheel.

Point `SPICY_REGS_DATA_DIR` at the generated `generation/` directory for the
existing SQL consumer:

```sql
SELECT opportunity_id, opportunity_number, version, notice_type, agency_name,
       close_date, close_date_explanation, eligibility_text,
       json_extract_string(source_locator_json, '$.request_url') AS archive_url,
       json_extract_string(source_locator_json, '$.source_xpath') AS record_path,
       source_release_digest
FROM grants_opportunity_observations
WHERE agency_code = 'DOS-SA'
  AND json_contains(eligibility_codes_json, '"25"')
  AND close_date >= DATE '2014-09-01'
ORDER BY snapshot_date, opportunity_id;
```

Use forecast estimated dates explicitly when searching forecasts. Include
`source_release_digest` in comparisons of multiple snapshots; do not count
revisions as distinct opportunities. A changed-version fixture demonstrates
refresh behavior. An absent record remains absent from that snapshot without
creating a withdrawal event.

Focused local fixtures prove mapping, refusal, ordinary publication and SQL
behavior. `ss-5bhb` records accepted complete selected-export qualification and
public delivery. The release-selection checks reuse that acceptance; they do not
establish automatic daily freshness, lifecycle status or complete history.
