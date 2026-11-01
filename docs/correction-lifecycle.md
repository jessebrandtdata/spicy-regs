# Reprocess corrected print and Senate results

A successful source read records both the publisher's observed revision and the
processing inputs. Changing either makes the source eligible again. Existing
outputs without a checkpoint are re-read once; row presence alone cannot prove
that processing completed or that a source produced no findings.

Checkpoints are JSON records in the existing Parquet file's metadata under
`spicy_regs.read_checkpoints.<family>.v1`. The shared merge writes the rows and
metadata together. No public row column or table identity changes.

## Print citations and committee actions

The checkpoint includes the citation and action rule versions, relevant
vocabulary, body preference, and installed source-reader/PDF-library versions.
The public parent table's citation-only `rule_set_version` retains its meaning.

A report is skipped only when its parent, citation output, and action output
agree on the successful read. A budget volume requires agreement between its
parent and citation output. An interrupted publication that leaves mismatched
checkpoints therefore makes the source eligible on the next run.

A successful re-evaluation replaces findings for that document and text digest,
including a corrected result with no findings. A publisher's revision keeps its
earlier findings: offsets are evidence for one text, so changed source bytes
add rows under the new digest and leave the old ones. A new digest from unchanged
bytes is a new reading of the same print, not a revision, so it replaces the
reading its checkpoint names in every table keyed by text digest
(`document_citations`, `bill_committee_actions`). The checkpoint records the body's
`source_sha256` for this. One written before that field has only `last_modified`,
so an equal stamp stands in for that one read. The replaced digest is journaled as
`print-reading-replaced` in the run's source evidence, and the replaced rows stay
in the prior immutable generation. Consumers select the digest named by the
current parent row. Failed reads preserve the prior results and
checkpoint. Stale retained packages remain eligible outside the discovery window
when processing changes; unchanged processing does not establish fresh source
polling outside that window.

## Senate expenditures

Each source file's checkpoint includes package/granule modification values,
the shaping and reader versions, PDF-library version, page limit, observed page
count, and number of pages read. A read must return consecutive pages through
the requested prefix. A truncated stream is a failed read.

Successful processing replaces that file's results, including an empty result.
The replacement key is `(package_id, file_name)`; a failed sibling file keeps
its previous rows. Stale retained files can be corrected outside discovery;
unavailable or removed source files keep their previous results pending source
resolution. A checkpoint for 80 pages of a 1,335-page PDF establishes only that
prefix, not whole-report coverage.

## Verification and remaining publication limits

```sh
uv run --frozen pytest -q tests/test_print_citations.py tests/test_senate_expenditures.py tests/test_read_checkpoints.py tests/test_table_merge.py
```

Regressions cover unchanged bytes under corrected rules, successful empty
results, stable reruns, retained history, failed and truncated reads, and stale
processing outside discovery. Raw-source replays and independent review are in
`~/Work/corpora/supply-2026-09-02/receipts/correction-lifecycle-2026-09-21/`.

The existing uploader still publishes files sequentially and enforces its
per-file shrink guard. Checkpoint disagreement enables later repair; it does
not give consumers an atomic multi-table release. Deliberate publication must
resolve that existing limitation and validate corrected shrinkage.
