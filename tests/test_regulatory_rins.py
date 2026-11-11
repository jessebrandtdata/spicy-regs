"""Pins multi-RIN handling end to end.

Every native RIN survives federal-register shaping and proceedings, ``Not
Assigned`` placeholders are dropped by the reader, and the documented all-RIN
join keeps dated identities instead of collapsing them.
"""

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.common import JsonReadStats, write_parquet_rows
from spicy_regs.ontology.rins import proceeding_rins
from spicy_docs.schemas.federal_register import project_federal_register_document

from spicy_regs.transforms.build_federal_register import build_federal_register
from spicy_regs.transforms.build_proceedings import build_proceedings


FIXTURE = Path(__file__).parent / "fixtures/regulatory_rins"


def native_record():
    pin = json.loads((FIXTURE / "pins.json").read_text())
    body = (FIXTURE / pin["fixture"]).read_bytes()
    assert hashlib.sha256(body).hexdigest() == pin["sha256"]
    return next(
        row
        for row in json.loads(body)["results"]
        if row["document_number"] == pin["selected_document_number"]
        and row["publication_date"] == pin["selected_publication_date"]
    )


def test_native_federal_register_keeps_every_rin_in_proceeding(tmp_path):
    native = native_record()
    assert native["regulation_id_numbers"] == ["3206-AO36", "3206-AO80"]
    shaped = project_federal_register_document(native)
    assert json.loads(shaped["regulation_id_numbers_json"] or "null") == native["regulation_id_numbers"]
    fr_path = build_federal_register(tmp_path, documents=lambda since: [native])
    [hosted] = pq.read_table(fr_path).to_pylist()
    assert hosted["rin"] == "3206-AO36"
    assert json.loads(hosted["regulation_id_numbers_json"] or "null") == native["regulation_id_numbers"]
    for table, key in (
        ("dockets", "docket_id"),
        ("documents", "document_id"),
        ("fr_docket_links", "document_number"),
        ("rule_targets", "docket_id"),
    ):
        write_parquet_rows(tmp_path / f"{table}.parquet", columns=(key,), rows=[])
    result = build_proceedings(tmp_path, run_id="native-rins", asserted_at="2026-09-21T00:00:00Z")
    [row] = pq.read_table(result).to_pylist()
    assert row["rin"] is None
    assert json.loads(row["rins_json"] or "null") == native["regulation_id_numbers"]
    assert json.loads(row["fr_document_ids_json"] or "null") == ["2026-17334@2026-08-25"]
    assert json.loads(row["docket_ids_json"] or "null") == []  # No invented parsed docket from freeform source labels.


@pytest.mark.parametrize(
    ("row", "expected", "malformed"),
    [
        ({"rin": "3206-AO36"}, {"3206-AO36"}, 0),
        ({"rin": "Not Assigned"}, set(), 0),
        ({"rin": "3206-AO36", "rins_json": "[]"}, set(), 0),
        ({"rin": "3206-AO36", "rins_json": "broken"}, set(), 1),
        ({"rin": "3206-AO36", "rins_json": "{}"}, set(), 1),
        ({"rins_json": '["3206-AO36"," 3206-ao80 ","Not Assigned",null,"3206-AO36"]'}, {"3206-AO36", "3206-AO80"}, 0),
    ],
)
def test_complete_rin_reader_and_legacy_limit(row, expected, malformed):
    """``rins_json`` wins over the scalar ``rin`` legacy fallback; malformed JSON counts one malformed row."""
    stats = JsonReadStats()
    assert proceeding_rins(row, stats) == expected
    assert stats.malformed_rows == malformed


def test_documented_all_rin_join_keeps_dated_identities():
    native = project_federal_register_document(native_record())
    # Explicit controls: a second publication date and repeated/placeholder RINs.
    other_date = {
        **native,
        "publication_date": "2026-08-26",
        "regulation_id_numbers_json": '["3206-AO36","3206-AO80","3206-AO80","Not Assigned",null]',
    }
    query = (
        (Path(__file__).parents[1] / "docs/regulatory-rins.md").read_text().split("```sql\n", 1)[1].split("```", 1)[0]
    )
    with duckdb.connect() as con:
        con.register("federal_register", pa.Table.from_pylist([native, other_date]))
        con.register(
            "house_communications",
            pa.Table.from_pylist(
                [
                    {"communication_id": "control-first", "rin": "3206-AO36"},
                    {"communication_id": "control-second", "rin": "3206-AO80"},
                    {"communication_id": "control-placeholder", "rin": "Not Assigned"},
                ]
            ),
        )
        rows = con.execute(query).fetchall()
    assert rows == [
        (communication, "2026-17334", day, rin)
        for communication, rin in [("control-first", "3206-AO36"), ("control-second", "3206-AO80")]
        for day in ["2026-08-25", "2026-08-26"]
    ]
    assert json.loads(native["regulation_id_numbers_json"] or "null") == ["3206-AO36", "3206-AO80"]
