# Retained U.S. Code editions

The `uscode-editions` family holds selected editions of the Code from the Office
of the Law Revision Counsel (OLRC): the 2000 annual XHTML archive and the complete
release-point XML corpora at 119-102 and 119-138. Editions are added by explicit
decision, one at a time, after an independent audit of the new original; rows of
editions already published never change.

`build-uscode-editions --manifest selection.json --output fresh-directory`
builds the three tables from pinned original archives: annual XHTML archives, a
complete release-point XML archive, or an explicitly selected title XML archive.
It reads original ZIP files directly and uses the maintained SpicyDocs archive and
structure readers.

The manifest has format `spicy-uscode-edition-inputs`, version `1`, and a `sources`
array. Every source requires `path`, `sha256` (with the `sha256:` prefix), and
`kind`. Annual entries require `year`; `release-point-corpus` entries require
`release_point`; `release-point-title` entries also require `title`. Relative
paths resolve against the manifest directory. Select one pinned version of each
edition or title; conflicting or duplicate selections are refused.

## Tables

`uscode_editions` identifies every admitted archive member, including supporting
files. Annual archive year and native publication year remain separate. A carried
appendix therefore keeps its older native year. XML editions keep their exact
release point.

`uscode_edition_structure` preserves native annual itempaths and XML section and
chapter observations. Each row links to its member, original byte span or XML
position, literal identifier, label, status and complete reader observation.
Annual itempaths can describe ranges, stubs or nonsections; the adapter does not
expand them into invented sections.

`uscode_edition_sections` adds section content from the same selected originals.
It links each section occurrence to its edition and source position and retains
the complete content-reader observation. Annual named fields preserve byte
spans and separate statutory body, source credits and notes. XML fields preserve
positional XPath, ancestry and literal child text. Quoted sections nested in
notes retain explicit source context. Missing body fields yield null; explicit
empty fields yield empty text. Unknown XML children remain separate with issues.
See SpicyDocs' `docs/sources/uscode-section-bodies.md` for exact reader semantics.
These destinations do not replace enacted-law text or determine legal status;
original members retain exact markup.

## Evidence without per-row receipts

The tables publish no per-row ETL receipts (their policies are marked
`receipts_retired`). Each row is a cleaned one-to-one copy of a native
observation; its evidence is its edition row's `archive_sha256` and
`member_sha256`, the original in the shared raw archive, and the generation's
source-evidence journal, which names each original under its OLRC URL. The last
generation with receipts, `792cbaa7f8ea134201736db8e98ed9a5630408ab8465164fb894cc44147dd5b0`
(2026-10-14, the 2000 edition and 119-102), keeps them and is pinned in
`pins/receipt-archive.json`. Policy version `uscode-editions-public-v2` is
unchanged: retirement changed no column.

The build verifies original archive hashes, reader identity and selected input
stability, stages rows privately, and installs the output only after all archives
succeed. It refuses an existing output directory and retains a `rebuild.json`
containing input pins, reader pins (every `*.py` of SpicyDocs, rulespec-artifacts
and SpicyRegs, plus `pyproject.toml` and `uv.lock`), output hashes and scope; any
change to them makes a different build. No baseline or network access is used by
the build. Section content is written in batches of at most 64 records, the
others in batches of 2,000. Bodies and observations can be large, so a row bound
is not a byte or memory bound: supervise the build's memory.

## Adding an edition

Five steps, all run through `uv run --frozen --extra comments-reader`, from one
checkout that does not change between them: a changed `*.py` file makes the
audit refuse the build. The 119-138 addition on 2026-11-11 measured, peak RSS
under the watchdog: acquire 17 s and 0.6 GiB; build 91 s and 0.5 GiB; audit
127 s and 2.4 GiB; preparation 21 s and 5.7 GiB (it verifies the whole live
prior, receipts included); about 1.1 GB of scratch. A publish uploads about
0.98 GB: the three rewritten tables and the zip.

```sh
# 1. Capture the corpus zip into the shared raw archive (one request, ~110 MB).
python -m spicy_regs.sources.uscode_editions acquire --release-point 119-138 \
  --output-dir "$WORK/acquire" --selection-out "$WORK/selection.json"
# 2. Build the candidate from the raw archive's stored file.
python -m spicy_regs.sources.uscode_editions --manifest "$WORK/selection.json" --output "$WORK/candidate"
# 3. Audit it independently (docs/uscode-editions-audit.md).
python -m spicy_regs.sources.uscode_xml_audit "$WORK/candidate" --selection "$WORK/selection.json" \
  --output "$WORK/audit.json"
# 4. Union it after the live generation's rows; $ACQUIRED is acquire's evidence, $WORK/acquire/source-evidence/*/artifact.
python -m spicy_regs.uscode_edition_publication --prior "$LIVE_USCODE" --candidate "$WORK/candidate" \
  --acquisition "$ACQUIRED" --audit "$WORK/audit.json" --audit-sha256 "sha256:$(shasum -a 256 "$WORK/audit.json" | cut -d' ' -f1)" \
  --audit-bytes "$(stat -f %z "$WORK/audit.json")" --index "$LIVE_INDEX" --expected-prior "$PRIOR" \
  --output "$WORK/addition"
# 5. Publish (docs/local-family-refresh.md), from prepared.json's generation and evidence.
publish-local-generation --family uscode-editions --generation "$WORK/addition/generation" \
  --evidence "$EVIDENCE" --expect-prior "$PRIOR" --output "$WORK/publish-receipt"
```

`acquire` uses the SpicyDocs acquirer, which proves every member's native title
and release point before the bytes are kept, and commits the capture receipt
(URL, time, status, digest) to the raw archive. Its selection names the archive's
own stored file. `$LIVE_USCODE` and `$LIVE_INDEX` come from
`spicy-regs mirror --family uscode-editions` (about 600 MB); `$PRIOR` is the live
digest. The preparation requires the audit's `APPROVE` verdict and its
`evidence_sha256` pins over the build record, selection and every candidate
table; checks the prior against the index it read; refuses a duplicate identity
across prior and candidate; and compares the union with its inputs slice by
slice. It writes the union in zstd, as the builder does. It reads each accepted
original once, to retain it into the generation's evidence with the download
record of the `acquire` capture that fetched it (URL, time, HTTP status, capture
id) and the acquisition evidence's pin. A generation states exactly one
source-evidence input (`generations.py`, and publication holds every evidence
to the generation's prior), so the acquisition's artifact is cited from that
journal entry rather than passed as a second `--evidence`. The retention adds
one acquisition record to the raw archive per preparation run; it never
publishes.
