# Typed Federal Register source candidates

`build-federal-register-layer1` reads a bounded selection of retained original
API pages and writes a local typed candidate. It uses the installed SpicyDocs
reader to validate the requested date windows, field set, pagination and record
identity. It has no network fetch or publication step.

Use a source-reader installation of SpicyRegs and a manifest whose `query_scope`
matches the retained acquisition. Each `pages` entry names its exact request
`url` and original `sha256`. For example:

```json
{
  "query_scope": {
    "publishedFrom": "2025-01-01",
    "publishedThrough": "2025-03-31",
    "presidentialDocumentType": "executive_order"
  },
  "pages": [{"url": "<exact retained request URL>", "sha256": "sha256:<original digest>"}]
}
```

```sh
build-federal-register-layer1 --manifest selected-pages.json \
  --archive-root ~/Work/corpora/spicyregs/raw --output-dir new-candidate
```

The archive opens read-only. The output directory must be new and outside the
archive, including through symlinks. A complete staged result becomes visible
only after every table and the receipt are written. A failed write can be retried
at the same destination. The command
refuses missing or unused pages, repeated request cursors,
conflicting observations of one document number and publication date, unsupported
date or number spellings, and inputs exceeding the candidate row bound. Identical
observations collapse; the same number on distinct publication dates survives.
The default and maximum candidate bound is 10,000 observed records, including
duplicates. This route does not replace historical acquisition or scheduled
publishing.

`federal_register_documents.parquet` contains every requested source member.
Dates use `DATE`, page numbers and executive order numbers use `BIGINT`,
`significant` uses `BOOLEAN`, and source arrays use `VARCHAR[]`. Object array
elements contain JSON text; text array elements remain literal strings.
`president` and `regulations_dot_gov_info` contain JSON text. Source spelling
remains available in the original identified by each row's `source_sha256` and
`source_locator`.

Agency and CFR child tables retain the parent source key and zero-based position.
CFR `chapter`, `part` and `citation_url` use `VARCHAR` so mixed source number and
text values remain queryable in one column. The docket candidate table uses the
maintained plural docket reader. Those syntax matches remain distinct from the
publisher-stated docket id in `regulations_dot_gov_info`.

`inputs.json` pins the manifest, reader schema and selected source pages once per
table. Its field coverage distinguishes populated and null values from absent or
unrequested members. Optional source members may be absent; `member_names`
distinguishes their absence from explicit null in a queryable column. It describes
only the selected records; it does not prove
publisher-wide coverage, a current publisher snapshot or cross-family list
encoding behavior. No null object-array element is accepted by the current
Federal Register reader.
