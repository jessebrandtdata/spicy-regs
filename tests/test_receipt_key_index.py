"""Pinned position lookup preserves receipt bytes and refuses incomplete or stale sidecars."""
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.receipt_key_index import (
    KEY, FORMAT, COLUMNS, check_reader, lookup_receipts, verify_key_index,
)
from spicy_regs.sources.publication import file_identity
from tests.test_fec_receipt_adapter import fixture


def sidecar(receipts, destination, rows=None):
    identity = file_identity(receipts)
    count = pq.ParquetFile(receipts).metadata.num_rows
    receipt = {"sha256": identity["sha256"], "byteSize": identity["bytes"], "rows": count}
    if rows is None:
        rows = [{**{name: row[name] for name, _ in COLUMNS[:-1]}, "row_number": ordinal}
                for ordinal, row in enumerate(pq.read_table(receipts).to_pylist())]
        rows.sort(key=lambda r: (r["dataset"], r["record_id"] is None, r["record_id"] or "",
                                 r["outcome"], r["receipt_id"], r["row_number"]))
    schema = pa.schema([(name, pa.int64() if kind == "BIGINT" else pa.string()) for name, kind in COLUMNS],
                       metadata={b"spicy_receipt_key_index": json.dumps({"format": FORMAT,
                           "receiptSha256": receipt["sha256"], "receiptByteSize": receipt["byteSize"],
                           "receiptRows": count}).encode()})
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), destination, row_group_size=2)
    pin = file_identity(destination)
    return {"key": KEY, "format": FORMAT, "sha256": pin["sha256"], "byteSize": pin["bytes"],
            "rows": count, "receiptSha256": receipt["sha256"]}, receipt


def test_actual_fec_producer_lookup_duplicate_requests_and_absent_key(tmp_path):
    subject, receipts, _ = fixture(tmp_path)
    before = receipts.read_bytes()
    key = tmp_path / KEY
    descriptor, receipt = sidecar(receipts, key)
    expected = pq.read_table(receipts).to_pylist()[0]
    verify_key_index(receipts, key, descriptor, receipt)
    with duckdb.connect() as con:
        result = lookup_receipts(con, str(receipts), str(key), descriptor, receipt, dataset="fec_receipts",
                                 record_ids=[expected["record_id"], "sha256:" + "0" * 64, expected["record_id"]])
    assert result == [[expected], [], [expected]]
    assert receipts.read_bytes() == before


@pytest.mark.parametrize("damage", ["missing", "corrupt", "wrong-pin", "stale", "wrong-offset", "missing-map"])
def test_key_index_refuses_damaged_declarations_or_map(tmp_path, damage):
    _, receipts, _ = fixture(tmp_path)
    key = tmp_path / KEY
    descriptor, receipt = sidecar(receipts, key)
    if damage == "missing":
        key.unlink()
    elif damage == "corrupt":
        key.write_bytes(b"corrupt")
    elif damage == "wrong-pin":
        descriptor["sha256"] = "sha256:" + "0" * 64
    elif damage == "stale":
        receipt["sha256"] = "sha256:" + "0" * 64
    elif damage in {"wrong-offset", "missing-map"}:
        rows = pq.read_table(key).to_pylist()
        rows[0]["row_number"] = 17
        descriptor, receipt = sidecar(receipts, key, rows if damage == "wrong-offset" else [])
        with pytest.raises(ValueError, match="map|shape"):
            verify_key_index(receipts, key, descriptor, receipt)
        return
    with duckdb.connect() as con, pytest.raises(ValueError):
        check_reader(con, str(key), descriptor, receipt)


def test_runtime_position_mismatch_refuses_even_after_index_pin_is_verified(tmp_path):
    _, receipts, _ = fixture(tmp_path)
    key = tmp_path / KEY
    descriptor, receipt = sidecar(receipts, key)
    rows = pq.read_table(key).to_pylist()
    rows[0]["receipt_id"] = "sha256:" + "0" * 64
    descriptor, receipt = sidecar(receipts, key, rows)
    with duckdb.connect() as con, pytest.raises(ValueError, match="pointer differs"):
        lookup_receipts(con, str(receipts), str(key), descriptor, receipt, dataset="fec_receipts",
                        record_ids=[rows[0]["record_id"]])


def test_parse_index_admits_key_metadata_without_exposing_index_as_subject(tmp_path):
    from tests.test_mcp_etl_receipts import retained
    from spicy_regs.sources import publication as pub
    value, _, receipts = retained(tmp_path)
    key = tmp_path / KEY
    _, actual_receipts, _ = fixture(tmp_path)
    descriptor, _ = sidecar(actual_receipts, key)
    specification = value["families"]["members"]["etlReceipts"]
    descriptor["receiptSha256"] = specification["sha256"]
    descriptor["rows"] = specification["rows"]
    value["families"]["members"]["etlReceipts"]["keyIndex"] = descriptor
    assert pub.parse_index(json.dumps(value).encode()) == value
    assert pub.receipt_key_members(value)[0].key == KEY
    for bad in (None, {}, {**descriptor, "rows": True}, {**descriptor, "unknown": 1}):
        value["families"]["members"]["etlReceipts"]["keyIndex"] = bad
        with pytest.raises(pub.PublicationError):
            pub.parse_index(json.dumps(value).encode())


def test_indexed_native_selection_lookup_through_locked_server_cursor(tmp_path, monkeypatch):
    from tests.test_local_native_selection import selected_family
    from spicy_regs import mcp_server
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    selected = selected_family(tmp_path, monkeypatch)
    member = selected.native["members"]
    key = member.receipts.parent / KEY
    descriptor, receipt = sidecar(member.receipts, key)
    remember_selection(tmp_path, [SelectedDataset("members", member.subjects, member.receipts, "new-publisher",
                                                key, descriptor)])
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    con = mcp_server._build_connection()
    try:
        row = next(r for r in pq.read_table(member.receipts).to_pylist()
                   if r["dataset"] == "members" and r["outcome"] == "accepted")
        assert lookup_receipts(con.cursor(), str(member.receipts), str(key), descriptor, receipt,
                               dataset="members", record_ids=[row["record_id"]]) == [[row]]
    finally:
        con.close()
    key.unlink()
    with pytest.raises((ValueError, OSError, RuntimeError)):
        mcp_server._build_connection()


def test_explicit_index_adoption_builds_and_publishes_verified_generation(tmp_path):
    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.subject_catalog import descriptors
    from spicy_regs.generations import build_generation
    from spicy_regs.sources import publication as pub
    from tests.generation_fakes import Store
    subject, receipts, _ = fixture(tmp_path)
    descriptor, _ = sidecar(receipts, tmp_path / KEY)
    directory = tmp_path / "generation"
    policy = DatasetPolicy.from_descriptor(descriptors()["fec_receipts"])
    artifact = build_generation(directory, family="fec-query", files=[subject], expected_keys=[subject.name],
                                receipt_path=receipts, receipt_policies=[policy], receipt_generation_id="new-publisher",
                                receipt_index=(tmp_path / KEY, descriptor))
    assert artifact.root["spec"]["etlReceipts"]["keyIndex"] == descriptor
    store = Store()
    index = pub.publish_generation(directory, client=store, bucket="b", prior_index=pub.empty_index())
    assert index["families"]["fec-query"]["etlReceipts"]["keyIndex"] == descriptor
    assert KEY not in index["families"]["fec-query"]["tables"]
    assert pub.parse_index(json.dumps(index).encode()) == index


@pytest.mark.parametrize("damage", ["wrong_receipt", "wrong_position", "missing_receipt"])
def test_generation_index_adoption_refuses_unpaired_or_invalid_map(tmp_path, damage):
    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.subject_catalog import descriptors
    from spicy_regs.generations import build_generation
    subject, receipts, _ = fixture(tmp_path)
    invalid_rows = None
    if damage == "wrong_position":
        row = pq.read_table(receipts).to_pylist()[0]
        invalid_rows = [{**{name: row[name] for name, _ in COLUMNS[:-1]}, "row_number": 1}]
    descriptor, _ = sidecar(receipts, tmp_path / KEY, rows=invalid_rows)
    if damage == "wrong_receipt":
        descriptor["receiptSha256"] = "sha256:" + "0" * 64
    policy = DatasetPolicy.from_descriptor(descriptors()["fec_receipts"])
    with pytest.raises(ValueError):
        build_generation(tmp_path / "refused", family="fec-query", files=[subject], expected_keys=[subject.name],
                         receipt_path=None if damage == "missing_receipt" else receipts,
                         receipt_policies=[policy], receipt_generation_id="new-publisher",
                         receipt_index=(tmp_path / KEY, descriptor))


def many_receipts(tmp_path, monkeypatch):
    from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, write_dataset
    from spicy_regs import subject_catalog
    policy = DatasetPolicy("null_keys", pa.schema([("id", pa.string()), ("nullable", pa.string())]),
                           ("id", "nullable"), ("raw",), nullable_identity_fields=("nullable",))
    witness = {"source_id": "test", "source_uri": None, "sha256": "a" * 64, "locator": "/"}
    rows = [({"id": str(i) + "\x00\x1bΩ", "nullable": None if i % 2 else "null", "raw": str(i)},
             ReceiptContext("old", str(i), "test", [witness])) for i in range(205)]
    from spicy_regs import etl_receipts
    monkeypatch.setattr(etl_receipts, "RECEIPT_ROW_GROUP", 10)  # several groups from a small fixture
    _, receipts = write_dataset(rows, tmp_path / "many", policy, batch_size=10)
    monkeypatch.setattr(subject_catalog, "policies", lambda: {policy.dataset: policy})
    return receipts


def test_multigroup_null_control_identity_and_exact_original_positions(tmp_path, monkeypatch):
    receipts = many_receipts(tmp_path, monkeypatch)
    expected = pq.read_table(receipts).to_pylist()
    key = tmp_path / KEY
    descriptor, receipt = sidecar(receipts, key)
    assert pq.ParquetFile(receipts).metadata.num_row_groups > 1
    assert pq.ParquetFile(key).metadata.num_row_groups > 1
    verify_key_index(receipts, key, descriptor, receipt)
    requested = [expected[204]["record_id"], expected[10]["record_id"], expected[0]["record_id"]]
    with duckdb.connect() as con:
        assert lookup_receipts(con, str(receipts), str(key), descriptor, receipt, dataset="null_keys",
                               record_ids=requested) == [[expected[204]], [expected[10]], [expected[0]]]
    entries = pq.read_table(key).to_pylist()
    descriptor, receipt = sidecar(receipts, key, list(reversed(entries)))
    with pytest.raises(ValueError, match="map"):
        verify_key_index(receipts, key, descriptor, receipt)


def test_ambiguous_pointer_refuses_even_with_other_requested_key_absent(tmp_path):
    _, receipts, _ = fixture(tmp_path)
    key = tmp_path / KEY
    sidecar(receipts, key)
    entries = pq.read_table(key).to_pylist()
    descriptor, receipt = sidecar(receipts, key, [entries[0], entries[0]])
    descriptor["rows"] = receipt["rows"] = 2
    metadata = dict(pq.read_schema(key).metadata)
    binding = json.loads(metadata[b"spicy_receipt_key_index"])
    binding["receiptRows"] = 2
    metadata[b"spicy_receipt_key_index"] = json.dumps(binding).encode()
    table = pq.read_table(key).replace_schema_metadata(metadata)
    pq.write_table(table, key)
    pin = file_identity(key)
    descriptor.update(sha256=pin["sha256"], byteSize=pin["bytes"])
    with duckdb.connect() as con, pytest.raises(ValueError, match="ambiguous"):
        lookup_receipts(con, str(receipts), str(key), descriptor, receipt, dataset="fec_receipts",
                        record_ids=[entries[0]["record_id"], "sha256:" + "0" * 64])


def test_streamed_remote_sidecar_digest_size_and_cache(monkeypatch):
    from contextlib import contextmanager
    from hashlib import sha256
    import httpx
    from spicy_regs.receipt_key_index import _verify_bytes, _CHECKED
    content = b"index bytes" * 100
    requests = []
    class Response:
        def raise_for_status(self):
            pass
        def iter_bytes(self, chunk_size):
            assert chunk_size == 1024 * 1024
            yield content[:10]
            yield content[10:]
    @contextmanager
    def stream(method, location, timeout):
        requests.append(location)
        yield Response()
    monkeypatch.setattr(httpx, "stream", stream)
    _CHECKED.clear()
    descriptor = {"sha256": "sha256:" + sha256(content).hexdigest(), "byteSize": len(content)}
    _verify_bytes("https://fixture.test/immutable/index", descriptor)
    _verify_bytes("https://fixture.test/immutable/index", descriptor)
    assert len(requests) == 1
    with pytest.raises(ValueError, match="exceeds"):
        _verify_bytes("https://fixture.test/changed/index", {**descriptor, "byteSize": 1})
    with pytest.raises(ValueError, match="exact byte pin"):
        _verify_bytes("https://fixture.test/bad/index", {**descriptor, "sha256": "sha256:" + "0" * 64})


def test_full_reader_admission_memoizes_unchanged_signature_and_refuses_mutation(tmp_path):
    _, receipts, _ = fixture(tmp_path)
    key = tmp_path / KEY
    descriptor, receipt = sidecar(receipts, key)
    with duckdb.connect() as con:
        class Trace:
            def __init__(self):
                self.metadata_queries = 0
            def execute(self, sql, *args):
                self.metadata_queries += ('DESCRIBE' in sql or 'parquet_' in sql)
                con.execute(sql, *args)
                return self
            def __getattr__(self, name):
                return getattr(con, name)
        trace = Trace()
        check_reader(trace, str(key), descriptor, receipt)
        first = trace.metadata_queries
        check_reader(trace, str(key), descriptor, receipt)
        assert first == 3 and trace.metadata_queries == first
        key.write_bytes(b'corrupt')
        with pytest.raises(ValueError, match='corrupt or stale'):
            check_reader(trace, str(key), descriptor, receipt)
