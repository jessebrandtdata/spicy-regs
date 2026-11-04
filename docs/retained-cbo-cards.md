# Read retained CBO archive cards privately

`build-cbo-archive-cards` reads an explicit list of immutable acquisition receipts
from the central raw archive. It reuses SpicyDocs' WARC digest check and gated
CBO card parser. It performs no acquisition or publication.

Create a JSON manifest with `receipt_keys`, each a canonical
`acquisitions/sha256/xx/<digest>` path for a `cbo-publication-page` receipt.
Select these through the source catalog; do not substitute a filename or an
unverified decoded HTML copy for the receipt and original WARC member.

```sh
build-cbo-archive-cards --manifest pages.json \
  --archive-root ../corpora/spicyregs/raw --output-dir /tmp/private-cbo-candidate
```

The destination must be new and outside the raw archive. Inputs are bounded by
`--max-pages` and a 16 MiB aggregate original-byte limit. Missing or damaged
inputs and weak archive identity abort before an output appears. Common Crawl
record IDs must be nonempty UUID URIs of the form `<urn:uuid:...>` and match
the pinned receipt. A page must have an ASCII publication number and complete
archive coordinates before it can be selected, even if its card is absent. The latest
archive timestamp selects each publication's card; differing original digests
at one timestamp refuse, so receipt order cannot silently select a revision.
Capture count and earliest archive timestamp describe only the selected inputs.

The private `cbo_estimate_cards.parquet` has one row per accepted publication.
Its ordered windows retain their literal period headings and each printed
measure beside its integer millions value. `*`, blank and nonnumeric cells have
a null numeric value and retain the literal. Answers and footnotes remain
literal. Source digest and WARC record locator identify the original. `inputs.json`
retains the input list, full archive coordinates and a separate `card`, `absent`
or `refused` outcome with provenance. An absent card is an observed page result;
it does not establish that the publication has no estimate or archived PDF.

This command supports bounded retained-page use. It does not establish a complete
archive sweep, retrieve linked PDFs, create the PDF-only extraction path
(`ss-938z`), or register/publish a warehouse generation. Wider archive acquisition
and resume by WARC coordinates remain separate work. The original source review
is [SpicyDocs D12](../../spicy-docs/docs/research/cbo-archive-visual-review-2026-10-30.md).
