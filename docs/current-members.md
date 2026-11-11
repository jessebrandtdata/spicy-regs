# Current member preparation

The member build reads both complete community-crosswalk rosters from the
reviewed fork commit named in `src/spicy_regs/sources/member_rosters.json`.
Each request uses that immutable URL and checks the response size and digest.
SpicyDocs owns transport, parsing, byte limits and source refusals. The source
evidence records the requested URL, capture and selected input pin.
Raw GitHub serves these JSON files as `text/plain`; the adapter admits that
media type only for its selected immutable URLs. It retains the real response
header and still requires valid roster JSON and the approved size and digest.

These inputs were generated with the community repository's unchanged JSON
serializer from the approved term-date patch. The patch changes the documented
term dates and the dependent party-affiliation end date. Names, identifiers,
record order and term membership retain the reviewed source values. See
[the upstream draft correction](https://github.com/unitedstates/congress-legislators/pull/1067)
and the generated fork commit in the selection file. The fork inputs remain a
community crosswalk with documented corrections; they do not establish an
independently complete official congressional roster.

`scripts/prepare_members_native.py --work NEW_DIRECTORY --expected-prior sha256:DIGEST` prepares the first
current native generation from these complete source files. It uses the
maintained member builder, field policies, source evidence and generation
admission. The captured published member artifact remains the
prior-generation input. The command requires `R2_PUBLIC_URL` and the expected
member artifact digest. It captures one published index, refuses a missing or
different member prior before building, and verifies that the result retains
that same member entry and prior-generation input. Its old processing rows are not used to manufacture
new source facts. The outputs are `members`, `member_terms` and
`member_party_affiliations`, which publish no per-row receipts
([votes trio](votes-receipt-archive.md)). The command writes
`members-prepared.json` in its private work directory. It performs no
publication.

The ordinary scheduled member build merges into the selected published
tables. Publication of the prepared generation requires the exact reviewed
current code and source wheel, compatible readers, source and field checks,
the captured member-family entry, and an admitted run. The shared publisher
checks the complete artifact and evidence before changing that family's
pointer. Other family updates can proceed independently.

The source wheel's additional scorecard catalog entries stay disabled and
unqualified. This member preparation does not adopt the separate scorecard
numeric-schema transition or receipt-history changes. See the named member
tests and `docs/native-generation-lifecycle.md` for source and receipt checks.
