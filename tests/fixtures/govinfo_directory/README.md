# Congressional Directory interpretation fixtures

- `CDIR-2014-02-18-HOUSECOMMITTEES-4.htm` and `CDIR-2014-02-18.mods.xml`: byte-identical copies of the spicy-docs
  fixtures of the same names; their originals, digests and reductions are in
  `spicy-docs/tests/fixtures/govinfo_directory/README.md`.
- `legislators-113-joint.yaml`: the `JSEC`, `JSLC`, `JSPR` and `JSTX` entries of `unitedstates/congress-legislators`
  `committee-membership-current.yaml` at commit 53547039 (2014-02-13; original SHA-256
  `5961838ee03467b834d0e58db5ab6dc077401d4ddf51203ff9c55b7bb3f3e1a2`), re-serialized with `yaml.safe_dump`. It is
  another publisher's record, used only as the independent side of the joint-committee check.
- `references-113-joint.json`: SpicyRegs' own published rows for the 42 members on those lists and the six joint
  committee codes, selected from members generation `29718daf…` (`members.parquet`
  `ef0dace0…`, `member_terms.parquet` `dc8fd9f0…`, terms in force on 2014-02-18) and committee-rosters generation
  `5f031073…` (`committees.parquet` `75f27be7…`).
