# Recorded-vote references (the measured sample)

`recorded-votes-119-sample.json` is the 58 `recordedVotes` entries spicy-docs
measured on 2026-09-19 over 20 bills of the 119th Congress through the
Congress.gov `bill/{c}/{type}/{n}/actions` route — the measurement that sealed
`spicy_docs.interpretation.vote_matching`'s reference shape (six fields on 58 of
58 entries, `fullActionName` on none). Extracted from
`spicy-docs/docs/research/billtrax-raw-data-2026-09-19.json`
(SHA-256 `fe219d1ce3755809dec27121813269ee9ff732745a92fa8d297e9ca707281a9c`,
spicy-docs commit `2d5791e`), block `sources.rollCallReferences`: each bill's
`bill` label and its `votes` array, verbatim, with the per-bill field
inventories and action counts left out. The publisher's values are U.S.
government data in the public domain.

| Field | Value |
| --- | --- |
| Bills sampled | 20 |
| Entries | 58 |
| Distinct roll calls | 34 (28 House, 6 Senate) |
| Roll calls claimed by more than one bill | 0 |
| URL hosts | clerk.house.gov 49, www.senate.gov 9 |

Two things the sample shows that the tests rely on. Each bill's entries come
in pairs or more with the *same* roll number: the publisher attaches one
recorded vote to each floor action it settled, so a passage vote appears on
both the "On passage" and the "Motion to reconsider" actions. That is why 58
entries index to 34 roll calls, and why `bill_vote_references` is keyed with
`action_index`. And no roll call is claimed by two bills, so
`index_vote_references` settles the sample with zero conflicts — the count
`tests/test_roll_call_votes.py` asserts on every published row.

This is the JSON actions route's spelling (`rollNumber`, `sessionNumber`,
integers), which `read_recorded_vote` accepts beside the BILLSTATUS parser's
own snake-case attributes. An offline test over it establishes behavior for
this shape; it says nothing about coverage or what the publisher serves today.

# Roll-call vote bodies (one per chamber)

Two complete, unmodified publisher bodies, copied from spicy-docs'
`tests/fixtures/congress_votes/` (captured keyless 2026-09-19; see its README).
The Clerk body keeps its CRLF line endings, hence `-text` in `.gitattributes`.
They let the roll-call tests read a real `RollCallVote`, so `vote_day` is checked
against each chamber's own printed spelling rather than a stub's.

| Fixture | Publisher response | Printed date | SHA-256 |
| --- | --- | --- | --- |
| `clerk-roll240.xml` | `clerk.house.gov/evs/2025/roll240.xml` | `8-Sep-2025` | `0297b0c76d3c14452a91daf9828943e5669c00dcc408c07bc80870b9d8223542` |
| `senate-vote-119-1-00001.xml` | `www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00001.xml` | `January 9, 2025,  02:54 PM` | `9d71d78a54c83522babd743209ca4a1a27baa2df122d6830c50ec2aa512ea17e` |
| `clerk-roll001-1991.xml` | `clerk.house.gov/evs/1991/roll001.xml` | `3-Jan-1991` | `739d5d60f9ac7a83196d200b5e2ddd33a5a4a4d7666fc1411ff09bec7e2f20a9` |
| `clerk-roll300-2015.xml` | `clerk.house.gov/evs/2015/roll300.xml` | `4-Jun-2015` | `ba44f7befa0c90d61794aa9ca62a7a16c506b902468bb879067f28fa86563797` |

The last two were fetched keyless on 2026-09-28 into the Clerk archive survey
(`~/Work/corpora/fork-execution-2026-09-21/cbo-112-113/clerk-all/`, whose
`receipt.jsonl` records the same digests) and copied unmodified. 102-1-1, a
quorum call of 1991, is a file before 2003: it names no legislator by bioguide
`name-id`, so its 427 members key on `name:` with a NULL `bioguide_id`.
114-1-300 is one of the five votes the House vacated by unanimous consent
before any position was recorded: it lists no member, totals zero, files its
body under `<committee>` and says so in `<vote-desc>`.

`senate-vote-menu-119-1.xml` is spicy-docs' byte-exact head-and-tail excerpt of
the Senate's 119th-Congress first-session vote menu (captured keyless
2026-09-19; 19,841 bytes, SHA-256
`55c6f51697a46eac80bf151971bf6d9f428681e7eb1b54f828d7a62c3502789d`), copied
unmodified from `spicy-docs/tests/fixtures/congress_votes/`, whose README states
the kept spans. `tests/test_senate_vote_ladder.py` serves it from fake proxy rungs.
