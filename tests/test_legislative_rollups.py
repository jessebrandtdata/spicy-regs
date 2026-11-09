"""Exercise the owned rollup entry point and its selected receipt prior."""

import pyarrow.parquet as pq

from spicy_regs.pipelines.rollups.laws import LawsRollup


def test_laws_rollup_runs_native_generation_with_verified_local_prior(tmp_path, monkeypatch):
    from .test_laws import StubListingReader, StubUslm, StubOlrc, LISTED_119
    from spicy_regs.generations import verify_generation
    from spicy_regs.legislative_receipts import policy
    from spicy_regs.pipelines.rollups import laws
    from spicy_regs.transforms.build_laws import build_laws

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("LEGISLATIVE_PRIOR_BUNDLE", raising=False)
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    calls = []

    def builder(directory, **kwargs):
        uslm = StubUslm(unavailable={110, 109, 104})
        calls.append(uslm)
        return build_laws(directory, reader=StubListingReader({119: LISTED_119}), uslm=uslm, olrc=StubOlrc(), **kwargs)

    monkeypatch.setattr(laws, "build_laws", builder)
    first = tmp_path / "first"
    LawsRollup(output_dir=first).run()
    [generation] = (first / "generations").iterdir()
    artifact = verify_generation(generation)
    assert artifact.root["spec"]["etlReceipts"]["generationId"]
    # What the read of each law's PLAW established is published beside the row; which reader made it is not.
    published = pq.read_table(first / "laws.parquet")
    assert published.schema.equals(policy("laws").subject_schema)
    assert "uslm_reader_version" not in published.schema.names
    laws_read = {law["law_id"]: law for law in published.to_pylist()}
    assert (laws_read["119-public-1"]["uslm_outcome"], laws_read["119-public-1"]["uslm_reason"]) == ("captured", None)
    assert laws_read["119-public-1"]["law_text_outcome"] == "parsed"
    assert {(laws_read[f"119-public-{n}"]["uslm_outcome"], laws_read[f"119-public-{n}"]["uslm_reason"])
            for n in (104, 109, 110)} == {("unavailable", "source_unavailable")}
    LawsRollup(output_dir=first).run()
    assert 1 in [s.number for s in calls[0].selections]
    assert 1 not in [s.number for s in calls[1].selections]


def test_senate_rollup_returns_native_subject_and_file_outcomes_in_one_generation(tmp_path, monkeypatch):
    from .test_senate_expenditures import _Reader, _listing, _granule, PACKAGE, PART_ONE, _Acquirer, _Extractor
    from spicy_regs.generations import verify_generation
    from spicy_regs.legislative_receipts import policy
    from spicy_regs.pipelines.rollups import senate_expenditures
    from spicy_regs.transforms.build_senate_expenditures import build_senate_expenditures
    from spicy_regs.navigation_read_outcomes import FILE_POLICIES

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("LEGISLATIVE_PRIOR_BUNDLE", raising=False)

    def builder(directory, **kwargs):
        return build_senate_expenditures(
            directory,
            reader=_Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]}),
            acquirer=_Acquirer(),
            extractor=_Extractor(page_count=3),
            **kwargs,
        )

    monkeypatch.setattr(senate_expenditures, "build_senate_expenditures", builder)
    pipeline = senate_expenditures.SenateExpendituresRollup(skip_upload=True)
    direct = tmp_path / "direct"
    direct.mkdir()
    built = pipeline.build(direct)
    assert isinstance(built, tuple)
    assert {path.name for path in built} == {"senate_expenditures.parquet", "senate_expenditures_document_file_outcomes.parquet"}
    table = pq.read_table(direct / "senate_expenditures.parquet")
    assert table.num_rows > 0
    assert table.schema.equals(policy("senate_expenditures").subject_schema)
    outcomes = pq.read_table(direct / "senate_expenditures_document_file_outcomes.parquet")
    assert outcomes.schema.equals(FILE_POLICIES["senate_expenditures_document_file_outcomes"].subject_schema)
    assert outcomes.num_rows > 0
    assert sum(row["row_count"] for row in outcomes.to_pylist()) == table.num_rows
    assert all(row["original_byte_sha256"] and row["native_members"] for row in outcomes.to_pylist())

    scheduled = tmp_path / "scheduled"
    senate_expenditures.SenateExpendituresRollup(output_dir=scheduled, skip_upload=True).run()
    [generation] = (scheduled / "generations").iterdir()
    artifact = verify_generation(generation)
    assert artifact.root["spec"]["etlReceipts"]["generationId"]
    assert "senate_expenditures_document_file_outcomes.parquet" in artifact.root["spec"]["tables"]
    assert pq.read_table(scheduled / "senate_expenditures.parquet").equals(table)


def test_receipt_writers_use_the_standard_lookup_layout(tmp_path, monkeypatch):
    import pyarrow as pa
    from spicy_regs import etl_receipts
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, RECEIPT_STATISTICS, dataset_row_groups, open_receipt_writer

    monkeypatch.setattr(etl_receipts, "RECEIPT_ROW_GROUP", 2)  # whole groups whatever the caller writes
    rows = [{name: f"{dataset}-{n}-{name}" for name in RECEIPT_SCHEMA.names if name != "witnesses"}
            | {"dataset": dataset, "witnesses": []} for dataset in ("a", "b", "c") for n in range(2)]
    with open_receipt_writer(tmp_path / "r.parquet") as writer:
        for row in rows:
            writer.write_table(pa.Table.from_pylist([row], schema=RECEIPT_SCHEMA))
    with pq.ParquetFile(tmp_path / "r.parquet") as parquet:
        assert [parquet.metadata.row_group(i).num_rows for i in range(parquet.metadata.num_row_groups)] == [2, 2, 2]
        group = parquet.metadata.row_group(0)
        columns = {group.column(i).path_in_schema: group.column(i) for i in range(group.num_columns)}
        assert {name for name, column in columns.items() if column.is_stats_set} == set(RECEIPT_STATISTICS)
        assert {"processor", "identity_json"} <= set(RECEIPT_STATISTICS)
        assert all(column.has_offset_index for column in columns.values())  # the page index
        assert columns["identity_json"].has_column_index
        assert "PLAIN_DICTIONARY" not in columns["record_id"].encodings and "RLE_DICTIONARY" not in columns["record_id"].encodings
        assert dataset_row_groups(parquet, "b") == [1]
