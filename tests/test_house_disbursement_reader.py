"""Read a bounded qualified House witness without installing an unreleased source parser."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.schemas.house_disbursements import FAMILY, PARTITION_COLUMNS, SCHEMAS, TABLES
from tests.generation_fakes import download_generation
from tests.test_local_mcp import serve
from tests.test_mcp_server import _listed, _records, _tool_data

FIXTURES = Path(__file__).parent / "fixtures/house_disbursement_reader"


def test_house_witness_is_discoverable_and_queryable_after_managed_download(tmp_path, monkeypatch):
    provenance = json.loads((FIXTURES / "provenance.json").read_text())
    inputs = tmp_path / "tables"
    for source in provenance["files"]:
        fixture = FIXTURES / source["fixture"]
        assert "sha256:" + hashlib.sha256(fixture.read_bytes()).hexdigest() == source["fixture_sha256"]
        table = fixture.stem
        assert pq.ParquetFile(fixture).schema_arrow == SCHEMAS[table]
        target = inputs / table / "report_year=2022/report_quarter=2/part-000000.parquet"
        target.parent.mkdir(parents=True)
        target.write_bytes(fixture.read_bytes())
    generation = tmp_path / "generation"
    artifact = build_generation(
        generation, family=FAMILY, files=[inputs / table for table in TABLES.values()],
        expected_keys=[table + ".parquet" for table in TABLES.values()],
        schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
        partitioned={table + ".parquet": PARTITION_COLUMNS for table in TABLES.values()},
    )
    selected = download_generation(monkeypatch, generation, None, tmp_path / "selected", TABLES.values())
    con, server = serve(monkeypatch, selected)
    try:
        listed = {entry["table"] for entry in _listed(server)}
        assert set(TABLES.values()) <= listed
        result = _tool_data(server, "query_sql", {"sql": (
            "SELECT source_file_id, source_record_number, document, amount, row_kind "
            "FROM house_disbursements WHERE source_file_id = 'house-2022-q2-detail' "
            "AND source_record_number = 4033"
        )})
        witness, = _records(result)
        assert {key: witness[key] for key in ("source_file_id", "source_record_number", "document", "row_kind")} == {
            "source_file_id": "house-2022-q2-detail", "source_record_number": 4033,
            "document": "1570415", "row_kind": "transaction",
        }
        assert Decimal(str(witness["amount"])) == Decimal("-420.00")
        assert artifact.pin.artifact_digest in json.dumps(result)
        for table in TABLES.values():
            described = _tool_data(server, "describe_table", {"table": table})
            assert described["table"] == table
            assert "Earlier PDF reports are outside" in json.dumps(described)
    finally:
        con.close()
