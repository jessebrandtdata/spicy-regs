"""Published shaped inputs become exact native bundles, never alleged captures."""

from base64 import b64decode
from hashlib import sha256
import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import read_attempts
from spicy_regs.congress_receipts import policy
from spicy_regs.generations import verify_generation
from spicy_regs.scorecards.analysis_inputs import convert_published_official_input
from spicy_regs.scorecards.resolution import OFFICIAL_COLUMNS
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecard_analysis import COMPARISON_INPUTS, analysis_input_entries
from tests.generation_fakes import Store
from tests.test_scorecard_analysis import _inputs
from tests.test_scorecard_analysis_preflight import fixture_owner

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def seed_shaped_index(store, files, index=None):
    """Fixture for a captured historical index, not a new legacy publication.

    Production admission correctly refuses new registered tables without
    receipts. This test seeds the same prior shape that already exists live.
    All physical byte pins and source schemas are real fixture observations.
    """
    tables = {}
    for path in files:
        table = pq.ParquetFile(path)
        tables[path.name] = dict(columns=[[name, "VARCHAR"] for name in table.schema_arrow.names],
                                 rows=table.metadata.num_rows, byteSize=path.stat().st_size,
                                 sha256="sha256:" + sha256(path.read_bytes()).hexdigest())
    digest = sha256(json.dumps(tables, sort_keys=True).encode()).hexdigest()
    prefix = "generations/official-shaped/" + digest
    index = index or publication.empty_index()
    index["families"]["official-shaped"] = dict(
        artifactDigest="sha256:" + digest, logicalId="urn:spicy:artifact:test:" + digest,
        prefix=prefix, tables=tables,
    )
    for path in files:
        store.objects[prefix + "/" + path.name] = path.read_bytes()
    return index


def published_shaped(tmp_path, name="members", *, records=None):
    schema = pa.schema([(c, pa.string()) for c in OFFICIAL_COLUMNS[name]], metadata={b"retained-footer": b"literal"})
    records = records or [{"bioguide_id": "X000001", "name_first": "Alex", "name_last": "Example",
                           "fec_ids_json": "[]", "bioguide_previous_json": "null", "other_names_json": None}]
    path = tmp_path / (name + ".parquet")
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    store = Store()
    index = seed_shaped_index(store, [path])
    return path, index


def convert(tmp_path, path, index):
    return convert_published_official_input(index, "members", [path], tmp_path / "conversion",
                                           generation_id="explicit-conversion", published_url="https://data.example")


def test_published_shaped_conversion_restores_every_field_schema_footer_and_honest_witness(tmp_path):
    path, index = published_shaped(tmp_path)
    selected, proof = convert(tmp_path, path, index)
    assert proof["rows"] == 1
    assert proof["original_rows_sha256"] == proof["restored_rows_sha256"]
    assert proof["original_table_pin"] == publication.table_pin(index, path.name)
    assert proof["exact_schema_and_authored_footer"] is True
    assert proof["generated_footer_keys_added"] == []
    restored = selected.materialize("members", tmp_path / "again.parquet")
    assert pq.read_schema(restored).equals(pq.read_schema(path), check_metadata=True)
    assert pq.read_table(restored).to_pylist() == pq.read_table(path).to_pylist()
    attempts = list(read_attempts([selected.receipts], policy("members"), generation_id=selected.generation_id))
    assert {w["source_id"] for row in attempts for w in row["witnesses"]} == {
        "shaped-observation:members", "published-shaped-observation:members",
    }
    assert all(w["body_version"] is None for row in attempts for w in row["witnesses"])
    assert proof["source_acquisition_requests"] == 0
    assert all(w["source_uri"].startswith("https://data.example/generations/") for w in proof["witnesses"])


def test_conversion_reports_added_generated_arrow_schema_without_asserting_identical_storage(tmp_path):
    path, _ = published_shaped(tmp_path)
    pq.write_table(pq.read_table(path), path, store_schema=False)
    assert not pq.read_metadata(path).metadata
    index = seed_shaped_index(Store(), [path])
    _, proof = convert(tmp_path, path, index)
    assert proof["exact_schema_and_authored_footer"] is True
    assert proof["generated_footer_keys_added"] == ["ARROW:schema"]
    assert "exact_schema_and_footer" not in proof


def test_conversion_refuses_rewritten_original_footer_encoding_even_with_unchanged_decoded_schema(tmp_path):
    path, _ = published_shaped(tmp_path)
    expected_schema = pq.read_schema(path)
    encoded = pq.read_metadata(path).metadata[b"ARROW:schema"]
    assert encoded.endswith(b"=")
    alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    position = len(encoded.rstrip(b"=")) - 1
    altered = encoded[:position] + bytes([alphabet[alphabet.index(encoded[position]) ^ 1]]) + encoded[position + 1:]
    assert altered != encoded and b64decode(altered) == b64decode(encoded)
    body = path.read_bytes()
    assert body.count(encoded) == 1
    path.write_bytes(body.replace(encoded, altered))
    assert pq.read_schema(path).equals(expected_schema, check_metadata=True)
    index = seed_shaped_index(Store(), [path])
    with pytest.raises(ValueError, match="original footer"):
        convert(tmp_path, path, index)


@pytest.mark.parametrize("fault", ["pin", "population", "malformed-native-value"])
def test_published_conversion_refuses_changed_bytes_population_and_native_values(tmp_path, fault):
    path, index = published_shaped(tmp_path, records=[{
        "bioguide_id": "X000001", "fec_ids_json": "not json" if fault == "malformed-native-value" else "[]",
    }])
    if fault == "pin":
        source = pq.read_table(path)
        column = source.schema.get_field_index("name_first")
        pq.write_table(source.set_column(column, "name_first", pa.array(["changed"])), path)
    elif fault == "population":
        fixture_owner(index, path.name)["tables"][path.name]["rows"] += 1
    with pytest.raises(ValueError, match={"pin": "immutable pin", "population": "population differs",
                                          "malformed-native-value": "refused native values"}[fault]):
        convert(tmp_path, path, index)
    assert not (tmp_path / "scorecard_member_links.parquet").exists()
    if fault == "malformed-native-value":
        receipt = tmp_path / "conversion/etl_receipts.parquet"
        assert any(row["outcome"] == "refused" for row in pq.read_table(receipt).to_pylist())


def test_conversion_requires_all_selected_parts_and_uses_native_receipts_without_downgrade(tmp_path):
    path, index = published_shaped(tmp_path)
    with pytest.raises(ValueError, match="every selected table member"):
        convert_published_official_input(index, "members", [], tmp_path / "missing", generation_id="g",
                                         published_url="https://data.example")
    fixture_owner(index, path.name)["etlReceipts"] = {}
    with pytest.raises(ValueError, match="only explicitly selected published shaped"):
        convert(tmp_path, path, index)


def legacy_analysis_inputs(tmp_path):
    store, index = _inputs(tmp_path)
    # Vote and sponsorship comparison inputs stay native; only the existing resolver
    # inputs exercise the explicit historical shaped-conversion route.
    owner = index["families"]["test-inputs"]
    owner["tables"] = {name + ".parquet": owner["tables"][name + ".parquet"]
                       for name in ("member_votes", "bill_cosponsors")}
    owner["etlReceipts"]["datasets"] = ["member_votes", "bill_cosponsors"]
    directory = tmp_path / "shaped"
    directory.mkdir()
    files = []
    for name in OFFICIAL_COLUMNS:
        source = tmp_path / "inputs"
        parts = [source / (name + ".parquet")] if name != "congress_bills" else [
            source / "congress_bills-118.parquet", source / "congress_bills-119.parquet",
        ]
        table = pa.concat_tables([pq.ParquetFile(path).read() for path in parts])
        path = directory / (name + ".parquet")
        pq.write_table(table, path)
        files.append(path)
    index = seed_shaped_index(store, files, index)
    return store, index


def test_conversion_is_explicit_and_never_substitutes_for_missing_source_or_bad_native_receipts(tmp_path):
    _, index = legacy_analysis_inputs(tmp_path)
    with pytest.raises(publication.PublicationError, match="requires native receipts for: members"):
        analysis_input_entries(index)
    assert set(analysis_input_entries(index, convert_published_official_inputs=True)) == {
        "scorecards", "scorecard_members", "scorecard_items", *OFFICIAL_COLUMNS, *COMPARISON_INPUTS,
    }
    official = fixture_owner(index, "members.parquet")
    official["etlReceipts"] = {}
    with pytest.raises(publication.PublicationError, match="requires native receipts for: members"):
        analysis_input_entries(index, convert_published_official_inputs=True)
    official.pop("etlReceipts")
    fixture_owner(index, "scorecards.parquet").pop("etlReceipts")
    with pytest.raises(publication.PublicationError, match="requires native receipts for: scorecards"):
        analysis_input_entries(index, convert_published_official_inputs=True)


def test_published_partition_conversion_preserves_every_original_part_in_order(tmp_path):
    parts = tmp_path / "parts"
    schema = pa.schema([("bill_id", pa.string()), ("congress", pa.string())], metadata={b"footer": b"literal"})
    files = []
    rows = [{"bill_id": "118-hr-1", "congress": "118"}, {"bill_id": "119-hr-1", "congress": "119"}]
    for row in rows:
        path = parts / ("congress=" + row["congress"]) / "part-000000.parquet"
        path.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist([row], schema=schema), path)
        files.append(path)
    index = publication.empty_index()
    index["families"]["shaped-partitions"] = dict(
        artifactDigest="sha256:" + "1" * 64, logicalId="urn:spicy:artifact:test:partitions",
        prefix="generations/shaped-partitions/" + "1" * 64,
        tables={"congress_bills.parquet": dict(
            columns=[[name, "VARCHAR"] for name in schema.names], rows=2,
            byteSize=sum(path.stat().st_size for path in files), partitionColumns=["congress"],
            members=[dict(key=str(path.relative_to(parts.parent)), partition={"congress": row["congress"]},
                          rows=1, byteSize=path.stat().st_size, sha256="sha256:" + sha256(path.read_bytes()).hexdigest())
                     for path, row in zip(files, rows, strict=True)],
        )},
    )
    selected, proof = convert_published_official_input(index, "congress_bills", files, tmp_path / "conversion",
        generation_id="partition-conversion", published_url="https://data.example")
    restored = selected.materialize("congress_bills", tmp_path / "returned.parquet")
    assert pq.ParquetFile(restored).read().to_pylist() == rows
    assert pq.read_schema(restored).equals(schema, check_metadata=True)
    assert proof["rows"] == 2
    assert proof["original_rows_sha256"] == proof["restored_rows_sha256"]
    assert "tableDescriptorDigest" in proof["original_table_pin"]
    assert len(proof["witnesses"]) == 2


def test_complete_analysis_preparation_converts_published_inputs_and_keeps_original_parent_pins(tmp_path, monkeypatch):
    from spicy_regs.scorecards.operations import analysis as module
    store, index = legacy_analysis_inputs(tmp_path)

    from io import BytesIO

    class Client:
        def get_object(self, *, Bucket, Key):
            return {"Body": BytesIO(store.objects[Key])}

    monkeypatch.setattr(module.r2, "get_r2_client", Client)
    monkeypatch.setattr(module.publication, "_stored_index", lambda *args: (index, None))
    monkeypatch.setattr(module, "load_dotenv", lambda *args: None)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example")
    # The synthetic prior contains no acquisition evidence to inherit. This
    # replacement affects that external lookup, not conversion/admission.
    monkeypatch.setattr(module.CaptureEvidence, "inherit", lambda *args, **kwargs: None)
    output = tmp_path / "analysis"
    monkeypatch.setattr(sys, "argv", ["prepare_analysis.py", "--output", str(output),
                                      "--convert-published-official-inputs"])
    module.main()
    report = json.loads((output / "preparation.json").read_bytes())
    assert report["status"] == "prepared_not_published"
    assert len(report["published_official_conversions"]) == len(OFFICIAL_COLUMNS)
    generation = verify_generation(output / "generation")
    assert generation.pin.as_dict() == report["generation"]
    root = json.loads((output / "generation/artifact.json").read_bytes())
    assert root["spec"]["parents"]["members.parquet"] == publication.table_pin(index, "members.parquet")
    assert pq.read_table(output / "scorecard_member_links.parquet").to_pylist()[0]["bioguide_id"] == "X000001"
    assert pq.read_table(output / "scorecard_item_links.parquet").to_pylist()[0]["bill_id"] == "119-hr-1"
    assert report["source_network_requests"] == 0
    assert report["qualification"]["counts"]["scorecard_action_comparisons"] == 0
    assert report["qualification"]["comparison"]["scopes"] == []
    for proof in report["published_official_conversions"]:
        assert proof["original_rows_sha256"] == proof["restored_rows_sha256"]
        assert proof["original_table_pin"] == publication.table_pin(index, proof["dataset"] + ".parquet")
