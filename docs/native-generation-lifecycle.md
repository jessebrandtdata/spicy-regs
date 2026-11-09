# Native generation selection and receipt history

Scheduled Congress, legislative and FEC identity builds use
`selected_generations.SelectedInputs`. A captured remote index selects the exact
subject and receipt members used throughout a publishable build. Explicit local
mode (`public_url=""`) selects immutable members pinned by the local
`.native-state/selection.json` file. A missing receipt or changed local member
refuses the read; the scheduler does not convert old published tables.

`remember_selection` advances a complete batch of local dataset selections with
one file replacement. The common generation runner selects all receipt-backed
datasets from the admitted immutable artifact, including families that supply
receipts through their own generation hook. Direct builder calls select only
after their complete native candidate passes validation. Each build uses a new
private directory; refused candidates retain their evidence without replacing
visible files. Public convenience copies are outputs, not incremental inputs. Successful empty partitioned tables
remain empty directories with receipt-only file state; they do not gain invented
partition keys.

Maintained writers carry history with `carry_receipt_history`: an unchanged
attempt keeps its prior receipt and a changed row references its predecessor
(see `receipt-writer-adoption.md`). `inherit_receipt` retains ordered witnesses,
prior receipt identities and digest-keyed exact source/processing values inside
the new receipt. These payloads are flat and deduplicated; repeated carries do
not nest prior receipt bodies. `resolve_receipt_witness` verifies and resolves
`receipt-processing:` references and
`receipt.values.<field>` references through the retained history. Explicit
removals use `retire_receipt` to preserve evidence without an orphan accepted
subject join. Original source files and capture evidence remain retained.

See [receipt writer adoption](receipt-writer-adoption.md) for the distinction
between included helpers and maintained writer activation, optional key indexes,
and replacement of the earlier receipt drafts.

Native subjects are authoritative for domain readers. Maintained source builders
may read their exact original source fields and processing checkpoints after the
shared reader validates subject identity, version and receipt membership. The
Congress reader supports all selected table members together and requires their
retained source schemas and footer metadata to agree. Missing source facts are
not synthesized by reversing native values into old source spellings.

Boundary checks live in `tests/test_subject_receipt_rollups.py`,
`tests/test_congress_receipts.py`, `tests/test_legislative_rollups.py`,
`tests/test_fec_identity_receipts.py` and `tests/test_etl_receipts.py`.
