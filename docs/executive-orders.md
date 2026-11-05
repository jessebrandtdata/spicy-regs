# Executive order text and publisher history

`executive_order_observations` provides EO-number lookup, text search and the
Federal Register's literal president attribution and history notes. Its scope is
one complete **publication year**. Signing dates remain separate: an order
signed in December can be published in January. Source identity is the Federal
Register document number plus publication date; the EO number alone is not that
identity.

The builder accepts a normally admitted SpicyDocs Federal Register release whose
scope is January 1 through December 31 with
`presidentialDocumentType="executive_order"`. All returned source records must
qualify, and every admitted order must have exactly one selected local body
result. Ordinary body failures remain rows with `body_status='source_failed'`
and null text. A saved credential refusal aborts the build and records its
status and exact acquisition receipt. Missing results, wrong identities and
omitted metadata records also refuse the build. A failed request does not
establish body absence.

SpicyDocs owns capture and validation through `FederalRegisterBodyAcquirer` and
`sources.federal_register.body_captures`. Successful retained bodies replay
locally through the same source validator, preserving original bytes, URL and
observation time. The domain build performs no acquisition or automatic retry.
The shared `body_text` extractor supplies searchable text and its cleanup/version
evidence. Successful and available refused responses stay in the source-evidence
artifact. For credential refusals, the status and saved receipt come from the
same manifest read even if a retry replaces the file. The generated Parquet
file is adopted without another retained copy.

The president identifier/name are publisher attribution, not independently
established signer identity. History notes remain literal, including CRLF and
publisher typos. Nullable SQL note fields collapse missing and JSON null; their
states and complete original record remain in `metadata_field_states_json` and
`raw_metadata_json`. No legal-effect, repeal or amendment status is derived.
Offered XML/HTML/PDF URLs remain visible even when a selected body attempt fails;
an offered URL does not prove it was fetched or is available.

## Local build and query

```sh
python -m spicy_regs.pipelines.executive_orders \
  --selection "$EO_SELECTION_JSON" --output-dir "$EO_OUTPUT"
```

The selection names the ordinary metadata release and explicit per-record body
results. The mapping is loaded once; each body result is read once by the build.

```json
{
  "source": {
    "release": "/local/source-release",
    "blobs": "/local/source-blobs",
    "logicalId": "<exact logical ID>",
    "artifactDigest": "sha256:<exact release digest>",
    "verifierImplementationId": "<accepted verifier implementation>"
  },
  "publication_year": 2025,
  "bodies": {
    "2025-00001@2025-01-03": "/local/body-results/2025-00001/result.json"
  }
}
```

Use the ordinary generation publisher and SQL reader:

```sql
SELECT executive_order_number, publication_date, signing_date, president_name,
       disposition_notes, executive_order_notes, source_release_digest
FROM executive_order_observations
WHERE executive_order_number = '14133' OR body_text ILIKE '%energy%';
```

Full-year real-source/body qualification and public delivery remain separate
acceptance steps. This implementation does not add proclamation or memorandum
text acquisition. The existing selected Federal Register metadata already
contains those document kinds; the retained 2025 metadata assessment is recorded
in ticket `ss-5zsa` and its preflight evidence. Body completeness for those kinds
has not been established.
