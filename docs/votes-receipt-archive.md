# Votes trio: leaving per-row receipts

The six tables of the nightly `refresh-congress-vote-terms.yml` publish without
ETL receipts (epic ss-vse3): `members`, `member_terms` and
`member_party_affiliations` (family `members`), `roll_call_votes` and
`member_votes` (family `roll-call-votes`) and `member_vote_terms` (family
`member-vote-terms`). This page records what the receipts held and where it
went, how the switch was shown to change nothing a reader sees, and the pin
and first night the switch needs.

## Why

On 2026-11-11 (run 42951404120) the three jobs took 1, 50 and 25 minutes. The
votes job spent 9 minutes restoring its prior through receipts and about 22
writing 10.6 million new ones, against 25 seconds of builder; the terms job
wrote 10.6 million more. Each night re-uploaded 3.47 GB and 1.21 GB of receipts
for tables of 33 MB and 19 MB. The same night's inputs, built the new way on a
workstation with upload skipped: 13 s, 60 s (House only) and 22 s.

## What the receipts held, and where it went

Each receipt held its row's builder output (`source_fields`, all text). The
mapping to the public table left these fields out; each is now a column,
appended after the existing ones (`congress_subjects.PROMOTED`):

| Table | Promoted columns | Why it is needed |
| --- | --- | --- |
| `members` | `roster`, `observed_at`, `other_names_json` | the roster file; the capture instant, the merge's version column; the alias patches as received: the native `other_names` cannot tell an omitted key from a null one, 2 of the 5 patches state a null, and the scorecard resolver reads an omitted `first` as the member's own |
| `member_terms` | `observed_at`, `party_affiliations_state` | version column; the scorecard links serialize whole term rows |
| `member_party_affiliations` | `input_sha256`, `start_status`, `end_status`, `source_path`, `observed_at` | the roster file's digest and the path in it: the pointer to the retained original |
| `roll_call_votes` | `source_url`, `match_rule`, `match_action_index`, `match_url`, `conflict_count` | the builder relinks every held roll call from its own columns, these included |

`member_votes` and `member_vote_terms` held nothing their tables lack.
`match_action_index` and `conflict_count` are integers, as the table's other
counts. The affiliation's `source_json` is not a column: its `party`, `start`
and `end` equal the table's `party`, `affiliation_start` and `affiliation_end`
on all 59 rows and `caucus` is a column; the original is at `source_path` in the
roster file `input_sha256` names, which the members job keeps as evidence.
`congress_acquisition`, the run journal copied into receipts, is the job's
retained journal.

Policies moved for the new columns (`congress-subjects/3` for `members` and
`roll_call_votes`, `/2` for `member_terms` and `member_party_affiliations`;
`policy_migrations.json` reads the earlier ones), and all six carry
`"receipts_retired": true`.

`member_vote_terms` leaves out a vote whose term the rule does not establish
(`unmatched`, `ambiguous`, `unresolved_member`, `undated`), as the receipt
route did with a `rejected` receipt (14 on 2026-11-11: 12 `unmatched`, four of
them two senators whose terms the crosswalk does not hold yet, and 2
`unresolved_member`). The
table is unchanged: such a vote is the `member_votes` row with no row here, the
builder logs every `term_match` count, and the job logs how many rows were left
out and the first keys. Publishing them would restore the dictionary's earlier
"one row for every `member_votes` row"; that is an owner decision, not part of
this switch.

## How a night runs

`SubjectTableRollup` runs the same builders on the same selected priors and
maps each output with `congress_tables.to_public`; nothing writes a receipt. A
prior is the published table read back in the builder's spelling
(`congress_tables.as_builder_input`). On the first night the live generation
still lists receipts for the tables with promoted fields, so those priors are
restored through them one last time, by row group (`SelectedPriors.last_receipts`;
4 s for `roll_call_votes` out of the 3.47 GB member), which fills the promoted
columns on every carried row. A row the mapping refuses fails the job and
nothing is published, as the receipt route refused a generation with a fresh
refusal. A reader of these tables still on receipts (committee rosters, and
scorecard analysis for `members`, `member_terms`, `roll_call_votes` and
`member_votes`) reads the published table the same way once its family lists
no receipts.

## Equality with the receipt route

Run on the night's real inputs with upload skipped (2026-11-11 09:27-09:31Z),
against that night's published generations, the last with receipts:

| Table | Rows | Every existing column, both ways, in order | Promoted columns against the receipts |
| --- | ---: | --- | --- |
| `members` | 12,770 | identical | equal except `observed_at` (a new capture) |
| `member_terms` | 45,535 | identical | equal except `observed_at` |
| `member_party_affiliations` | 59 | identical | equal except `observed_at` |
| `roll_call_votes` | 30,709 | identical | equal except `match_rule`/`match_action_index` on 4 rows |
| `member_votes` | 10,563,782 | identical | none |
| `member_vote_terms` | 10,563,768 | identical | none; the same 14 left out |

Keys are unique in every table. The four relinked roll calls keep their
`bill_id`: the bill family republished at 04:51Z with five more recorded-vote
references, which the receipt route's 03:15Z run had not read. The second
night's path was checked on the same tables: each new table read back as
builder input maps to itself, and `member_vote_terms` built from the read-back
inputs equals the published one. `tests/test_votes_receipt_switch.py` holds
the same comparisons on real rows of these generations.

What these checks cannot see: the Senate (the run read the House only, with
Senate rows carried; the hosted job reads both through its paid ladder); a
second night end to end with fresh reads; and text spelling, which the round
trip normalizes. `roll_call_votes`' document objects are read back stating
`nomination_id` null where 7,258 rows' text omitted it; the builder treats both
alike and the public structs are the same. Parquet layout (row groups,
encodings) is not compared.

## Pin and first night

`docs/pins/receipt-archive.json` pins the three generations of 2026-11-11, the
last with receipts:

| Family | Generation | Published | Receipts |
| --- | --- | --- | --- |
| `members` | `26d652e10178f3b079c0af1c3ca50dbce318868541c8b3e28af6a40e4223e780` | 03:15:15Z | 58,376 rows, 11.5 MB |
| `roll-call-votes` | `a77051b2e3b1e1c22ca48363b1a636c2969b886224c4cf833866a85cdc20a46c` | 04:05:38Z | 10,594,567 rows, 3.47 GB |
| `member-vote-terms` | `27ebf57b795752295e4225c79413c4ee652a52b49ade95a66c094baf4f0c9469` | 04:31:03Z | 10,563,784 rows, 1.21 GB |

Each job refuses at its start, in seconds, while a pin is stale
(`publication.require_live_receipt_pins`). If a nightly or a dispatch of
`rollup-members.yml`, `rollup-roll-call-votes.yml` or
`refresh-congress-vote-terms.yml` publishes before the merge, re-pin each
family it published:
`uv run --frozen --extra comments-reader python scripts/pin_receipt_archive.py <family>`.

After the first night (the 03:00Z refresh):

- `families.members`, `families."roll-call-votes"` and
  `families."member-vote-terms"` list no `etlReceipts`; row counts are the
  previous ones plus the night's new roll calls and positions.
- `roll_call_votes` has no NULL `source_url`, `match_rule` or `conflict_count`,
  `members` no NULL `roster` or `observed_at`.
- The votes job's log has `prior-scope dataset=roll_call_votes ranged=True`
  and `prior-published-input dataset=member_votes`; from the second night
  every own prior and `members`' tables are `prior-published-input`.
- The three jobs finish inside 15, 30 and 30 minutes.
- Each pinned generation's `etl_receipts.parquet` still answers a HEAD.
