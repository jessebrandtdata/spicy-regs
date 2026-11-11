# GovInfo package fixtures

The source-retention tests also use the complete `CRPT-119hrpt1` summary and
HTML body copied byte-for-byte from spicy-docs at commit
`5f4720f6338b82597163f946d0cfbcd9ebf7cdb6`. Their upstream fixture provenance
records acquisition on 2026-09-19. This local copy makes no new request.

| File | Publisher response | Bytes | SHA-256 |
| --- | --- | --- | --- |
| `summary-CRPT-119hrpt1.json` | [Package summary](https://api.govinfo.gov/packages/CRPT-119hrpt1/summary) | 1,800 | `818d6a4dc8678a6b2972eb2f0e4596be2c576087508bb49caeb5d8be588e5bc1` |
| `body-CRPT-119hrpt1.htm` | [HTML body](https://www.govinfo.gov/content/pkg/CRPT-119hrpt1/html/CRPT-119hrpt1.htm) | 13,953 | `d2575146c81d989831fd08e8f424eddb048346bfe78670db994c0a107b584ad9` |

Four complete, unchanged MODS responses and one summary. The first two MODS,
one per collection the `committee-reports` rollup publishes, exist so
`tests/test_committee_reports.py` can prove `bill_id` is read from the
package's own MODS — the metadata the transform already fetches for every
package, so the linkage costs no request — and that a hearing whose MODS
names bills only as body mentions publishes a NULL rather than one of them.
The other two are the multi-part shapes `tests/test_report_section_refresh.py`
reads (decision 29): `CRPT-119hrpt811`, whose root names only its part 1
(`CRPT-119hrpt811-pt1`, `granuleClass` `FIRSTPART`) and so publishes one row
keyed on that part, and `CRPT-119hrpt455`, whose two parts are both
constituents and so publish two rows replaced as a set. The `CRPT-119hrpt455`
summary lets the real acquirer be driven offline, over that MODS grown to seven
parts, to prove the body budget refuses such a report before any body request.
These U.S. government documents are public domain.

| File | Publisher response | Bytes | SHA-256 | Provenance |
| --- | --- | --- | --- | --- |
| `mods-CRPT-119hrpt1.xml` | [`packages/CRPT-119hrpt1/mods`](https://api.govinfo.gov/packages/CRPT-119hrpt1/mods) | 9,787 | `d73ea7b12140ca7e1ad08649092a9e14a432a9fce8948d8a4975e4f3cd43f9d2` | Copied byte-for-byte from spicy-docs `tests/fixtures/govinfo_bodies/` at tag v0.21.1 (`6f8d20e`), the release this repository pins; spicy-docs captured it 2026-09-19 with the key sent only as `X-Api-Key`. |
| `mods-CHRG-119hhrg63127.xml` | [`packages/CHRG-119hhrg63127/mods`](https://api.govinfo.gov/packages/CHRG-119hhrg63127/mods) | 33,283 | `0070c86ed50c50331a060d692957081d1feb41c04d82742c78765f3f77d66849` | Captured 2026-09-19 for this repository with `Accept-Encoding: identity` and the api.data.gov key sent only as `X-Api-Key`; the capture refused to write any body containing the key or an `api_key=` parameter, and this one contained neither. |
| `mods-CRPT-119hrpt811.xml` | [`packages/CRPT-119hrpt811/mods`](https://api.govinfo.gov/packages/CRPT-119hrpt811/mods) | 26,279 | `aba0227068f60977bb1ee98d58add159064bb2b38e7b235e70b8673de7b3e125` | The record the scheduled run of 2026-09-23 refused and the one of 2026-09-24 published as Part 1 (Last-Modified 2026-09-22 14:12:09 GMT). Copied byte-for-byte from `~/Work/corpora/fork-execution-2026-09-21/drift-audit-2026-09-24/reports/raw/`, captured 2026-09-24 with the key sent only as `X-Api-Key`; it contains no key and no `api_key=` parameter. |
| `mods-CRPT-119hrpt455.xml` | [`packages/CRPT-119hrpt455/mods`](https://api.govinfo.gov/packages/CRPT-119hrpt455/mods) | 24,549 | `290e09efb3f28d77e3e23cd379d8e826e8894ef444aea0fd383377a210d86c59` | Copied byte-for-byte from spicy-docs `tests/fixtures/govinfo_bodies/` at `549db06`, the 0.32.0 release this repository pins; spicy-docs captured it 2026-09-23 with the key sent only as `X-Api-Key`. It contains no key and no `api_key=` parameter. |
| `summary-CRPT-119hrpt455.json` | [`packages/CRPT-119hrpt455/summary`](https://api.govinfo.gov/packages/CRPT-119hrpt455/summary) | 1,399 | `1d483854f59a0c6d3c4bf521c4b2a6ec44391d16391faf3271a8b1884ecaceb0` | Copied byte-for-byte from the same spicy-docs directory and commit; captured 2026-09-23 by spicy-docs' live `acquire_parts` run, key sent only as `X-Api-Key`, and it contains none. |
| `summary-CHRG-79jhrg79716p11.json` | [`packages/CHRG-79jhrg79716p11/summary`](https://api.govinfo.gov/packages/CHRG-79jhrg79716p11/summary) | 1,492 | `0a6132ecd83157817e5dd057ef406cc3df28fb45ad819e8d286fcef815aa2529` | Part 11 of the 1946 Pearl Harbor hearings, whose 312,487,940-byte PDF is past the 24 MiB body bound. Copied byte-for-byte from the retained evidence of the scheduled run of 2026-09-26 (`source-evidence/blobs/sha256/0a6132ec…`, cited by generation `708f2bf4…`); it contains no key. |
| `mods-CHRG-79jhrg79716p11.xml` | [`packages/CHRG-79jhrg79716p11/mods`](https://api.govinfo.gov/packages/CHRG-79jhrg79716p11/mods) | 6,624 | `09b2ff255032c2e6bd4d1a94ec4c3896fa98c957e60e4f3db367b9536f0b1a5b` | Its MODS, copied the same way (`source-evidence/blobs/sha256/09b2ff25…`); it contains no key. `tests/test_report_section_refresh.py` drives the real acquirer over the two to prove a size refusal is final. |
| `mods-CHRG-119hhrg61487.xml` | [`metadata/pkg/CHRG-119hhrg61487/mods.xml`](https://www.govinfo.gov/metadata/pkg/CHRG-119hhrg61487/mods.xml) | 51,439 | `a0f298006bcbcb05ed1fdd4ba026b49bb189389df00c8a441031b4d808a21c3e` | House Armed Services, 2025-04-09: the one of the 185 held hearings whose `congCommittee` `authorityId` is upper-case (`HSAS00`). Copied byte-for-byte from the round-5 chaos scout's keyless read of 2026-10-03 (`corpora/mcp-chaos-2026-10-02/round5/scout-D/mods/`). It states no bill; `tests/test_committee_reports.py` adds one `COVER` element, copied from CHRG-119hhrg60951's, so a link row exists. |

`mods-CRPT-118srpt99.xml` is the one **reduced** file here: the Senate
Budget Committee's report on the 117th Congress, filed in the 118th, whose
MODS keys all 34 root `<bill>` elements -- H.R. 5376, the reconciliation act,
among them -- `congress="118"`. `tests/test_print_citations.py` uses it to
prove the transform keys the print's bills in the Congress the report states
it covers and counts the MODS disagreeing. It is byte-identical to spicy-docs'
`tests/fixtures/document_citations/mods-CRPT-118srpt99.xml`, written by the
same `build_fixtures.py` (receipt
`~/Work/corpora/supply-2026-09-02/receipts/fix-print-citations-2026-09-26/`)
from the 2026-09-26 drift audit's keyless read of
`https://www.govinfo.gov/metadata/pkg/CRPT-118srpt99/mods.xml` (32,601 bytes,
`sha256:775f4aac44924271b8d284cfb40372008cb942e5e5503ec60746d90c9f0002a9`,
`drift-qualification-2026-09-26/bills-citations/native/`): everything before
the first `<relatedItem>`, then `</mods>`, which drops the 62 constituent
records the MODS reader never reads.

| File | Bytes | SHA-256 |
| --- | --- | --- |
| `mods-CRPT-118srpt99.xml` | 11,679 | `824fe705c4ab450ca53f98ad4a53682daeb0a4d59c5b1a22835ae9493bfd481f` |

## What each one states about a bill

A GovInfo MODS carries the bills a package relates to as
`<extension><bill congress=".." type=".." number=".." context=".."/>`, and
the `context` attribute is the publisher's own statement of *how*:

- `CRPT-119hrpt1` (a four-page House Rules Committee report) lists four bills:
  H. Res. 53 as `PRIMARY` — the resolution the report accompanies — and
  H. Res. 53 again, S. 5 and H.R. 471 as `OTHER`. The `associatedBills`
  element beside them reads `H. Res. 53, 119th`. Measured live 2026-09-19 on
  the twelve newest reports the `committee-report/119` list route names, the
  `PRIMARY` bill agreed with the route's own `associatedBill[0]` on **12 of
  12**.
- `CHRG-119hhrg63127` (the House Homeland Security hearing *Worldwide Threats
  to the Homeland*) lists H.R. 1 and H.R. 5371, both as `BODY`: bills the
  transcript mentions, not a bill the hearing was held on. Over 50 distinct
  hearings of the `hearing/119` route, **no CHRG MODS carried a `PRIMARY`
  bill** — 10 carried `BODY` or `COVER` mentions, 31 entries in all, the most
  on any one hearing being 11 — and of the 12 hearings whose detail named a
  committee meeting, **none** of those meetings' `relatedItems.bills` named a
  bill. So
  `hearing_transcripts.bill_id` stays NULL, and this fixture is the case that
  proves a mention is not promoted to a linkage.

**Both measurements are retained**, with every request and every raw response,
at `~/Work/corpora/supply-2026-09-02/receipts/report-bill-linkage-2026-09-19/`
— 146 requests, with its own README stating the method and the per-package
verdicts. The restatements elsewhere in this repository (the transform
docstring and the two dictionary entries) cite that receipt rather than
re-deriving the numbers.

Offline tests establish behavior for these shapes; they do not establish
coverage or continuing live availability. Replace the CRPT file only by
re-copying from the spicy-docs release this repository pins.
