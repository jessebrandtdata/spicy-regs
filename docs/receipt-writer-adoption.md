# Receipt history and receipt lookup adoption

Receipt writers use the family history rules below. The carry changes preserve
the receipt schema and canonical digests; original receipt generations remain
visible within a newly admitted selection.

## History retained by maintained writers

`etl_receipts.inherit_receipt` retains ordered witnesses, prior receipt
identities and exact processing values. `rebind_receipt` gives a carried row the
new generation identity and receipt digest while retaining the original
generation and processing evidence. `resolve_receipt_witness` resolves earlier
`receipt.values.*` and `receipt-processing:` references through that retained
history. Repeated carries keep processing payloads flat and deduplicated.

Receipts are public, so a new receipt's witness `source_uri` is a public
address, a publication object key, a content digest or null, never a path on the
build machine (`etl_receipts.local_witness_uri`); a carried unchanged row keeps
its published bytes.
The receipt writer refuses a new receipt with a local path, and inheritance or
retirement drops such a path from the earlier witnesses it copies, keeping their
`source_id` and `sha256`.

`receipt_history.carry_receipt_history` reuses exact unchanged occurrences and
gives changed accepted rows a direct predecessor reference, without copying
earlier witnesses or processing values. `write_dataset(prior_receipts=...)`
exposes it to explicit callers. Following the owner's 2026-10-17 bulk receipt
writer decision, carry is active in these maintained paths:

- Government output migration (`transforms/government_receipts.migrate_outputs`),
  including lobbying's native prior merge.
- Court row and batch writers (`court_receipts`).
- The Congress row writer (`congress_receipts.write_congress_dataset`), batch
  writer (`votes_batch_receipts`) and `legislative_receipts.write_legislative_outputs`.
- Regulatory row and bulk writers for `transforms/regulations_receipts.CARRY_DATASETS`:
  every regulatory dataset except `comments` (2026-11-09).
- The FEC identity writer (`transforms/fec_identity_receipts.IdentityReceiptWriter`
  with `prior_receipts`), used by `build_fec_identity_rollup` (2026-11-09).

These paths retain unchanged receipts' original generation identities;
`coverage_reader.current_receipts` accepts them in the newly admitted selection.
Before 2026-11-09 the FEC identity and remaining regulatory writers inherited
every matched prior receipt even when nothing changed, so each refresh added
witnesses and prior references to every row (fec-committees averaged 55
witnesses and 27 prior receipts per row). Their published rows keep that
history until they change. Scorecard history is unchanged. Regulatory correction
retains both prior and fresh receipt inputs, in that order. This caller list does
not change the Comments rules below.

Legislative carry runs after combining every partition. It preserves unresolved
refusals and resolves one only when a current accepted row supplies the same
declared key or explicitly mapped raw key. The manifest still reports remaining
refusals. Fresh refusals block candidate publication; exact historical refusals
remain readable so a later successful input can resolve them. Changed processing
creates a fresh receipt even when the native subject remains unchanged.

Scheduled Congress and legislative writers leave admission to the finished
generation. Standalone writers admit the completed bundle once. Prior restore
admits the selected pair once, then decodes all-text retained source fields in
SQL; conversion checks, exact schema/footer recovery and physical row order
remain enforced. Unsupported source types use the logged row route.

For fresh Congress and legislative rows, the Python mappers remain authoritative.
The batch path applies them through an Arrow function and builds exact receipt
text and digests in SQL. The maintained flat Congress SQL mappings keep their
existing route. Control escapes, mapper refusals and unproven value spellings
return to the reference attempt; DuckDB or Arrow failures abort without retrying
the whole input by row. `tests/test_hybrid_receipts.py` compares subject and
receipt values against the independent row writers.

Boundary checks are in `tests/test_etl_receipts.py`,
`tests/test_receipt_history.py`, `tests/test_regulatory_base_bulk.py`,
`tests/test_votes_batch_current.py` and the family receipt tests. In particular,
the regulatory checks resolve original API witnesses after repeated row and
bounded rewrites and retain ordered evidence from multiple prior inputs.

## Catalog comments batching

Catalog comments use the batch writer only when the selected incoming scope has
no prior receipts. If prior receipts exist, the entire staged write uses the
current row writer, preserving all witnesses and retained processing history.
Processing reads can batch after complete receipt admission and source-to-native
reproduction; an unsupported proof returns the whole pair to the row reader for
its exact first error. Batch conversion preserves source order and count, routes
unsupported rows through the shared mapper, and retains refused attempts.

## Finding one receipt: the standard Parquet layout

Every receipt member is written by `etl_receipts.open_receipt_writer`: whole
65,536-row groups, a page index with 1,000-row pages, footer and page bounds on
the key columns (including `identity_json`), and dictionaries only on
low-cardinality columns. A reader finds one row's receipts by filtering on
`dataset` and `identity_json`, so the footer and page index prune to the pages
that can hold it; rows are not sorted. `read_receipt_fields` reads those groups
with DuckDB, and the browser reads the identity columns first, then the one row.
Measured 2026-11-09 on bill-family's largest dataset (3.7M receipts): ~2-5 MB a
cold lookup (ticket ss-e4nx). This replaced the earlier key-to-position sidecar
(`etl_receipts.keys.parquet`), which no family ever published; its 2026-10-05
qualification evidence stays under
`~/Work/corpora/receipt-index-qualification-20261005/`.

## Receipt-order experiment

The physical-sort proposal is closed without changing receipt admission. The
hosted FEC reader regression reproduced a derived candidate-list difference
when identical receipt rows were stored in a different order. The evidence
view now orders companion digests, with nulls last, while preserving repeated
and null candidates. Its rule is `fec-companion-location-v3`.

`tests/test_fec_receipt_order.py` exercises the receipt-backed serving path in
both physical orders. Navigation tests cover repeated and null candidates.
This makes the derived list deterministic without rewriting source observations
or adopting the proposed physical receipt sort.

## Replacement of the earlier draft stack

This mapping was checked against merged source
`fd4e80437722c63887c2dfb36c0d7336e4d78542` from
[#60](https://github.com/mikewolfd/spicy-regs/pull/60). It describes disposition
of the earlier proposals before the carry activation described above, not the
current caller list or approval to activate the remaining deferred policies.

| Earlier draft | Included replacement | Unique proposal disposition |
| --- | --- | --- |
| [#28](https://github.com/mikewolfd/spicy-regs/pull/28), `fddbcd41583d9b7cf033f9499c7099e1d759120d` | Shared prior-receipt API and standalone carry helper are included; maintained writers preserve full history. | Do not merge the original writer switch: immutable rebinding and direct-only inheritance remove the current resolution path for earlier witnesses and processing. The original regulatory correction also excludes the fresh receipt input. Retain this draft as reference for a future evidence-preserving design. |
| [#29](https://github.com/mikewolfd/spicy-regs/pull/29), `9a7a6416baf55aa479ea7009e182a5b493a01e31` | The bounded validator dispatcher is included, with the row validator available as the comparison path and parity checks in `tests/test_etl_bulk_validate.py`. | Superseded as a separate delivery PR; no separate merge is needed. |
| [#30](https://github.com/mikewolfd/spicy-regs/pull/30), `cf8ad73b340c753ac4d8ea57a7da5afb2bc4c021` | The exact same builder is included, with the explicit utility and private qualification harness developed in [#34](https://github.com/mikewolfd/spicy-regs/pull/34). | Do not merge the automatic local generation hook, remote sidecar parameter/membership changes, or mandatory above-threshold remote index. These activate publication behavior beyond the retained qualification. Retain the proposal as reference; the explicit utility is delivered separately. |

Close the earlier delivery drafts with this replacement/deferred-policy mapping
after integration of the wrap-up record. Preserve their branches and the
retained qualification evidence; closing a draft does not discard its proposal
or mark deferred activation complete.
