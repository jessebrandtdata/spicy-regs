# Offline historical Table III replay

`table3_historical_records` preserves every record occurrence in a selected,
retained Office of the Law Revision Counsel (OLRC) Table III bulk member. It is
separate from `table3_records`, whose maintained laws build selects public-law
keys for requested Congresses and Congresses already held by the laws table.
Historical chapter-style keys remain native act identities in the new table.
Replaying a whole member establishes that member's contents, not publisher
completeness or suitability for replacing the current scoped table.

Run from the repository with an original archive digest, release point, and a
fresh local output directory:

```sh
uv run --frozen python -m spicy_regs.sources.table3_history \
  --archive /path/to/retained/archive \
  --sha256 sha256:ORIGINAL_SHA256 \
  --release-point 119-73 \
  --output /path/to/fresh/local-candidate
```

The producer reuses the SpicyDocs ZIP and Table III readers. It validates the
original archive digest, member grammar, release point, decoded bounds, CRC,
and every fragment before exposing subject data. Unknown vocabulary, malformed
late records, digest mismatch, wrong release point, or an empty result refuse
the replay. A failure retains `receipt.json` with the reason and exposes no
subject or ETL receipt member. An existing output directory is refused.

Successful output contains `table3_historical_records.parquet`,
`etl_receipts.parquet`, and `receipt.json`. `DatasetPolicy`, `ReceiptContext`,
and `write_dataset` apply the maintained receipt protocol. Each record carries
archive and decoded-member digests, release point, native act and record
context, and positions. Its ETL receipt witnesses the archive and member;
`source_position` and the witness locator preserve the exact source position.
`raw_source_row` retains all fields supplied by the maintained reader; the
original archive remains the authority for literal XML bytes.

The identity is `(member_sha256, release_point, act_key, act_record_position)`.
The act position runs across fragments sharing the same act key. Fragment and
fragment-record positions separately retain the physical occurrence. Publisher
sequence values remain fields, because they may reset or repeat. Different
member versions have different record identities. Different ZIP packaging of
identical member bytes retains the same record identity and distinct archive
witnesses. Meaningful blank act-section labels remain records.

The reader admits at most 128 MiB of compressed archive, 192 MiB of decoded
member, and 4 MiB per fragment. The shared output writer batches records. The
maintained ZIP reader holds the archive and decoded member in memory; these
bounds do not claim that process resident memory stays below their sum.

The pinned qualification evidence is retained under the canonical corpus's
`metadata/historical-rebuilds/20261010/table3/`: see `prove.py`, `proof.json`,
`registration-proposal.json`, and `receipt-bound-candidate/`. The proof compares
all held acts as full normalized row multisets, including multiplicities and
source order, and enumerates source-backed additions. It lists all literal
field differences separately. This is qualified content recovery after explicit
normalization, not literal page-versus-bulk row equality.

The observed blank-label discrepancy was a stale baseline omission. The
maintained page reader's earlier fix `6c1269f5` and current bulk mapper already
preserve those rows. Two independently retained page captures corroborate the
bulk sequence. This workflow acquires no new bytes, publishes nothing, and
retires no baseline.

The `table3-historical-public/2` recipe creates a fresh generation from the
recipe, archive digest and release point. Public archive witnesses use content
digests. The local `receipt.json` retains the physical source path and remains
private; select only the subject and fresh ETL receipt Parquet members for a
public artifact. Earlier path-bearing receipt history must not be inherited.

## Join placements by modern public law

Use the historical table when the SQL reader has a selected Table III member.
Its `act_key` is the publisher's search key. Modern public laws in the
`119-73` member run from the 85th through the 119th Congress. Date-and-chapter
keys from earlier Congresses remain available as original evidence. Some
chapters carry a session public-law number in `act_public_law`. That number
belongs to the chapter's historical naming system.

This `query_sql` example finds the Code placement of section 706 of Public
Law 88-352. It selects one archive and member, checks that the publisher's
Congress and act number agree with the search key, and forms the same
`congress-public-number` identity used by `laws.law_id`. The left join keeps
each placement even when the reader's `laws` table lacks the act's metadata.

```sql
WITH modern AS (
    SELECT *, replace(act_key, '-', '-public-') AS law_id
    FROM table3_historical_records
    WHERE release_point = '119-73'
      AND member_sha256 = 'sha256:985a9bb3dd328f98632530ce28bfbf2ae34f5b2eaad37b6088d983404c8ab681'
      AND source_sha256 = 'sha256:93e1f233e081e47fc3680c4b699151c6d66329988fe21add3b6e9e62746aeea7'
      AND regexp_full_match(act_key, '[1-9][0-9]{0,2}-[1-9][0-9]*')
      AND split_part(act_key, '-', 1) = congress
      AND act_key = act_number
)
SELECT m.law_id, l.title AS law_title, m.act_section, m.usc_title,
       m.usc_section_key, m.usc_place, m.act_record_position
FROM modern m
LEFT JOIN laws l ON l.law_id = m.law_id
WHERE m.law_id = '88-public-352' AND m.act_section = '706'
ORDER BY m.act_record_position
```

For other snapshots, take all three source pins from the selected replay.
Use `act_record_position` to order a law's placements. Each row represents
an occurrence, so `DISTINCT` over placement fields alone can discard repeated
source rows. Use `usc_place` to distinguish a Code section from a note or a
preceding placement.
