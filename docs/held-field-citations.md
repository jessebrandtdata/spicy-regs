# Citations from selected held fields

`run-rollup-held-citations` reads explicit fields from selected immutable
publications and appends their findings to the existing `document_citations`
table. It publishes through the complete `print-citations` family, carrying
unchanged sibling tables forward. It does not fetch publisher bodies.

The selection manifest names full source keys and exact input generations:

```json
{
  "selections": [
    {"kind": "bill_section", "keys": ["119-hr-10534", "introduced-in-house", "govinfo", "5"]}
  ],
  "input_generations": {"bill_sections": "sha256:<exact selected generation>"}
}
```

Run a local candidate first:

```sh
uv run run-rollup-held-citations --selection selection.json --output-dir output/held-citations
```

The CLI defaults to skipping upload. Its input tables must belong to the named
managed generations. Publication also requires an existing complete
`print-citations` family and the exact prior citation table. A missing required
input stops the run.

| Kind | Full source identity | Literal field |
|---|---|---|
| `bill_section` | `bill_id, version_code, source, seq` | `bill_sections.body` |
| `report_section` | `package_id, part_id, last_modified, seq` | `report_sections.body` |
| `lobbying_activity` | `filing_uuid, activity_index` | `lobbying_activities.description` |
| `comment_inline` | `comment_id` | `comments.comment` |

Composite `document_key` values are compact JSON lists in the listed order.
Single comment IDs remain literal. Citation spans index the exact UTF-8-decoded
field text; comment offsets index its retained HTML, not rendered text or an
attachment. The field bytes and selection manifest are retained in the normal
source-evidence artifact. The source generation and text digest accompany each
read. MCP checks the current full-key field digest before resolving targets.

Each invocation permits at most 100 selected fields, 4 MiB per field and 32 MiB
total field text. Reads have a bounded timeout. A successful complete field,
including an empty result, replaces findings only for its source kind, full key,
text digest and selected rules. Failed, duplicate-parent, null or capped reads
preserve prior findings. Prior text versions remain distinct. Successful zeroes
and rule versions are checkpoints in the same Parquet artifact as the findings.

Bill numbers require a stated Congress; a document's publication period does
not fill it in. Committee names are excluded from default extraction because
reviewed report text exposed a truncated line-wrapped name. They require explicit
transform-level opt-in and remain unresolved evidence. The reviewed cohort also
contains executive orders and verbose Federal Register references outside the
current grammar. A normalized U.S. Code section key does not identify a specific
statutory note: the original `note` qualifier remains in matched text.

See `research/join-delivery-execution-2026-09-27.md` and its evidence receipts for
the reviewed source scopes, misses, selected publications and delivery status.
These bounded reads do not establish whole-document or corpus recall. FCC body
extraction remains disabled until a retained body route is qualified. Inline
comments require a managed source publication before the publication CLI will
accept them.

Communication selections now keep three source roles separate:
`communication_authority`, `communication_report_nature`, and
`communication_record_entry`. Their full key is Congress, communication type,
and number. Each role hashes and reads its own literal field; a null printed
entry remains unread even when the authority field is populated. The 2026-09-27
bounded replay of 119-ec-4554 and 119-ec-1278 retained exact spans and source pins
under `~/Work/corpora/supply-2026-09-02/receipts/communication-held-citations-2026-09-27/`.
Committee referrals remain the source's structured referrals, not inferred
citation matches; unresolved official/agency splits remain unresolved.

Court PDF text uses `court_opinion_derived_pdf`, keyed by the compact JSON pair
`[opinion_id, source_sha256]`. Its source is
`court_opinion_pdf_extractions.text_content`; the PDF digest in the key remains
separate from the derived text digest. Citations state `pdf` rendition and
`derived_pdf` derivation. Null/failed extraction text remains unread.

The retained 2026-09-27 three-body cohort qualified exact spans and preserved
prior citation rows. One body completed with zero supported findings. The generic
`case_docket_number` rule misclassified Governor Proclamation `No. 20-50`, so
this adapter excludes it and records that omission in each read checkpoint.
State reporters, short forms and other unsupported citation types remain outside
this qualification. Evidence and the preserving candidate are under
`~/.codex/artifacts/spicy-regs-court-citations-20260927/verification.json`;
publication is a separate step.
