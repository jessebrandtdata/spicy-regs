# Executive order text and publisher history

`executive_order_observations` holds every executive order the Federal Register
API serves, with its body text, the publisher's president attribution and its
literal history notes. The API serves no document published before 1994; its
first executive order is EO 12890, published 1994-01-05. Source identity is the
Federal Register document number plus publication date; the EO number alone is
not that identity: a republished order (`R1-2016-03141`) or a correction
(`C1-2009-31418`) can carry the number of the order it repeats. The API also
files records under executive orders with no number at all (22 of 1,574 on
2026-11-10: corrections, designations under EO 12958, CFIUS orders, the 2001
military order). They are kept, with a null `executive_order_number`.

## Coverage

A generation is built from one admitted SpicyDocs release whose scope is
`publishedFrom` 1994-01-01 through the day the listing was read, under
`presidentialDocumentType="executive_order"`. The build refuses any narrower
scope, any refused or discarded record, and any order without a body result. The
scope is written once, in the table file's key-value metadata:

```sql
SELECT value FROM parquet_kv_metadata('executive_order_observations.parquet')
WHERE key = 'spicy_regs.executive_orders.coverage';
-- {"publishedFrom":"1994-01-01","publishedThrough":"…","presidentialDocumentType":"executive_order",
--  "sourceReleaseDigest":"sha256:…"}
```

`publication_year` is the year of `publication_date`, not a completeness
marker. Signing dates stay separate: an order signed in December can be
published in January.

## Acquisition

`acquire` lists the scope through SpicyDocs' Federal Register owner, the same
fetcher and anonymous User-Agent as the `federal_register` family: 90-day
windows from 1994, read twice because the release requires two agreeing
traversals (268 requests in November 2026). Each page is saved in the raw archive
before it is parsed. The listing is admitted as a source release, and the run
refuses unless every record qualifies.

When no order was added, changed (another SpicyDocs record digest) or removed
since the generation named by `--read-index`, acquire stops there. Otherwise
it captures bodies through SpicyDocs' `FederalRegisterBodyAcquirer` (publisher
XML, falling back to the GovInfo HTML granule after an XML 404 or 410; at most
one request every 0.4 s) with the run's evidence transport, so every response is
in the raw archive before SpicyDocs validates it. An order the published
generation holds with an acquired body keeps that body result: its bytes are
restored from published source evidence when the raw archive lacks them, and
the publisher is not asked again. A body that failed is tried again on the next
run that builds. `capture_body` reuses a result already in
`<output-dir>/body-captures`, so rerunning acquire into the same directory
resumes without fetching those bodies again.

Why not read the orders from the `federal_register` family's pages: that
rollup re-reads only the last 120 days, but the publisher adds history notes to
old orders when later ones amend or revoke them. EO 13985, published
2021-01-25, read on 2026-11-10 "Revoked by: EO 14148, January 20, 2025"; the
public `federal_register` row for it holds no notes at all. Its pages are
unfiltered (1,000 documents a page), and it does not admit them as a release.
The EO listing is about 5 MB a run; making the nightly re-read every year to
keep notes current would cost far more.

## Build

`build` admits the same release again from the raw pages and lays each body
result out for SpicyDocs' offline loader, which replays its responses through
the source validator; nothing is fetched. A row's pointers
(`source_release_logical_id`, `source_release_digest`, `metadata_locator_json`
and `source_sha256`, the digest of the API page the row was read from) move only
when the publisher's record or the body result changes. An order the publisher
has not touched keeps its published row byte for byte; a corrected one gets a
new row. Every page and body a row points to is retained in the generation's
source evidence.

Ordinary body failures remain rows with `body_status='source_failed'` and null
text. On 2026-11-10 ten orders from 2015 to 2021 failed this way: SpicyDocs' XML
validator refuses their FRDOC marker, and an XML that answers 200 is never
replaced by the HTML fallback. A saved credential refusal aborts the build. A
failed request does not establish body absence. The shared `body_text`
extractor supplies searchable text and its cleanup and version evidence.

The president identifier and name are publisher attribution, not independently
established signer identity. History notes remain literal, including CRLF and
publisher typos. Nullable SQL note fields collapse missing and JSON null; their
states and the complete original record remain in `metadata_field_states_json`
and `raw_metadata_json`. No legal-effect, repeal or amendment status is derived.
Offered XML/HTML/PDF URLs remain visible even when a body attempt fails; an
offered URL does not prove it was fetched or is available.

## Refresh

A hosted run (`rollup-executive-orders.yml`, weekdays) does both steps in one
process and publishes nothing when nothing is new. The hand refresh is in
[local-family-refresh.md](local-family-refresh.md#executive-orders):

```sh
$UV python -m spicy_regs.pipelines.executive_orders acquire --read-index "$LIVE_INDEX" \
  --output-dir "$WORK/eo-acquire" --captures-out "$WORK/eo-captures.json"
$UV python -m spicy_regs.pipelines.executive_orders build --captures "$WORK/eo-captures.json" \
  --read-index "$LIVE_INDEX" --output-dir "$WORK/eo-build"
```

```sql
SELECT executive_order_number, publication_date, signing_date, president_name,
       disposition_notes, executive_order_notes, source_sha256
FROM executive_order_observations
WHERE executive_order_number = '14133' OR body_text ILIKE '%energy%';
```

Proclamation and memorandum text is not acquired. The `federal_register` table
holds those documents' metadata.
