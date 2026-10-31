"""Financial decisions and source witnesses survive the physical receipt split."""

from pathlib import Path
from decimal import Decimal
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext, validate_receipt_bundle
from spicy_regs.transforms.fec_subject_receipts import (
    write_fec_subjects,
    read_fec_with_receipts,
    register_internal_fec_table,
)
from spicy_regs.transforms.fec_financial_policy import bulk_observation_eligibility
from spicy_regs.transforms.fec_native_subjects import FIELD_RULES
from spicy_regs.relationship_views.fec_financial_meaning import financial_rule_sql


#: These explicit table lists exercise main-row namespace/status fields used by the maintained rules.
#: They are independent of the runtime registry so a missing field cannot make the test silently skip itself.
SOURCE_NAMESPACE_TABLES = frozenset(
    "fec_account_transfers fec_allocated_disbursements fec_allocation_bases fec_api_response_controls "
    "fec_bundled_contributions fec_candidate_api_observations fec_committee_master_observations "
    "fec_committee_observations fec_communication_costs fec_contribution_aggregates "
    "fec_coordinated_party_expenditures fec_debts fec_disbursements fec_electioneering_communications "
    "fec_filing_links fec_filing_report_observations fec_filing_text_observations fec_filings "
    "fec_inaugural_donations fec_independent_expenditures fec_intercommittee_transactions fec_legal_matters "
    "fec_loan_guarantors fec_loan_terms fec_loans fec_lobbyist_registrations "
    "fec_postgres_committee_history_observations fec_quality_notices fec_receipts "
    "fec_registration_statements fec_reported_financial_summaries".split()
)
AMOUNT_STATUS_TABLES = frozenset(
    "fec_communication_costs fec_historical_ie_statistics fec_inaugural_donations fec_legal_events "
    "fec_receipts".split()
)
RETURNED = {"source_namespace": SOURCE_NAMESPACE_TABLES, "amount_status": AMOUNT_STATUS_TABLES}


def returned_columns(table):
    return tuple(column for column, tables in RETURNED.items() if table in tables)


def declared_schema(table):
    """The producer's whole declared row: what a native conversion of this table is handed."""
    import base64

    from spicy_regs.fec_receipt_adapter import processing_declarations

    return pa.ipc.read_schema(pa.BufferReader(base64.b64decode(processing_declarations()[table]["arrow_schema"])))


def context(row, ordinal):
    witness = dict(source_id="source", source_uri=None, sha256="sha256:" + "a" * 64, locator=None, body_version=None)
    return ReceiptContext("generation-a", str(ordinal), "test-fec", [witness, witness])


def source_row():
    return dict(
        record_id="sha256:" + "b" * 64,
        identity_version="fec-typed-observation/1",
        mapping_version="fec-bulk-individual-receipt/1",
        value_mapping_version="fec-exact-financial-values/2",
        mapping_status="mapped",
        source_authority="official-fec",
        source_namespace="fec-bulk-individual-contributions",
        source_representation_role="snapshot",
        correction_operation="none",
        current_record_status="unqualified",
        amount=Decimal("123456789.123456789"),
        amount_status="exact",
        currency="USD",
        memo_indicator="",
        transaction_type="15",
    )


def write(tmp_path, rows=None):
    rows = rows or [source_row()]
    schema = pa.Table.from_pylist(rows).schema
    return write_fec_subjects(
        rows,
        tmp_path / "bundle",
        table="fec_receipts",
        input_schema=schema,
        generation_id="generation-a",
        context_for=context,
    ), schema


def test_financial_python_and_sql_match_prior_eligibility(tmp_path):
    ((subject, receipt), policy), schema = write(tmp_path)
    rows = list(read_fec_with_receipts([subject], [receipt], policy, generation_id="generation-a"))
    assert rows == [source_row()]
    assert bulk_observation_eligibility(rows[0]) == bulk_observation_eligibility(source_row())
    # fec_receipts publishes its amount status and source namespace; what qualifies a row stays in the receipt.
    [published] = pq.ParquetFile(subject).read().to_pylist()
    assert (published["amount_status"], published["source_namespace"]) == ("exact", "fec-bulk-individual-contributions")
    assert {n: published[n] for n in ("current_record_status", "source_representation_role", "mapping_status")} == {n: source_row()[n] for n in ("current_record_status", "source_representation_role", "mapping_status")}
    witnesses = pq.ParquetFile(receipt).read(columns=["witnesses"]).to_pylist()[0]["witnesses"]
    assert len(witnesses) == 2 and witnesses[0] == witnesses[1]
    with duckdb.connect() as con:
        register_internal_fec_table(
            con,
            table="fec_receipts",
            subject_paths=[subject],
            shared_receipt_paths=[receipt],
            policy=policy,
            input_schema=schema,
            generation_id="generation-a",
        )
        sql = financial_rule_sql("fec_receipts", "bulk_source_analysis", columns=schema.names)
        actual = con.execute(sql).fetchall()
        con.execute("DROP TABLE fec_receipts")
        con.register("fec_receipts", pa.Table.from_pylist([source_row()], schema=schema))
        assert con.execute(sql).fetchall() == actual


@pytest.mark.parametrize("fault", ["generation", "missing", "modified", "duplicate"])
def test_receipt_read_refuses_faults_before_yielding(tmp_path, fault):
    ((subject, receipt), policy), schema = write(tmp_path)
    receipts = [receipt]
    generation = "generation-a"
    if fault == "generation":
        generation = ""
    if fault == "missing":
        empty = tmp_path / "empty.parquet"
        pq.write_table(pq.ParquetFile(receipt).read().slice(0, 0), empty)
        receipts = [empty]
    if fault == "duplicate":
        receipts = [receipt, receipt]
    if fault == "modified":
        row = pq.ParquetFile(subject).read().to_pylist()[0]
        row["amount"] = Decimal("99")
        pq.write_table(pa.Table.from_pylist([row], schema=policy.subject_schema), subject)
    with pytest.raises(ValueError):
        list(read_fec_with_receipts([subject], receipts, policy, generation_id=generation))


def test_definition_table_has_main_context_and_exact_processing_readback(tmp_path):
    rows = [
        dict(
            record_id="definition",
            family="electronic",
            version="8.5",
            form="F3",
            workbook_sha256="sha256:" + "a" * 64,
            layout_json='{"fields":[]}',
        )
    ]
    schema = pa.Table.from_pylist(rows).schema
    (subject, receipt), policy = write_fec_subjects(
        rows,
        tmp_path / "bundle",
        table="fec_filing_definitions",
        input_schema=schema,
        generation_id="generation-a",
        context_for=context,
    )
    assert pq.read_table(subject)["record_id"].to_pylist() == ["definition"]
    assert "layout_json" not in pq.read_schema(subject).names
    assert list(read_fec_with_receipts([subject], [receipt], policy, generation_id="generation-a")) == rows
    validate_receipt_bundle({"fec_filing_definitions": [subject]}, [receipt], [policy], generation_id="generation-a")


def test_invalid_native_values_preserve_failed_inputs_without_subject(tmp_path):
    rows = [dict(record_id="legal", native_facts_json='{"citations":{"unknown":[]}}')]
    schema = pa.Table.from_pylist(rows).schema
    (subject, receipt), policy = write_fec_subjects(
        rows,
        tmp_path / "bundle",
        table="fec_legal_matters",
        input_schema=schema,
        generation_id="generation-a",
        context_for=context,
    )
    assert pq.ParquetFile(subject).metadata.num_rows == 0
    r = pq.ParquetFile(receipt).read().to_pylist()[0]
    assert r["outcome"] == "refused"
    assert r["subject_version"] is None
    assert "unknown" in r["processing_json"]


@pytest.mark.parametrize(
    ("table", "namespace"),
    # fec_receipts publishes its source namespace; fec_communication_costs keeps it in the receipt. The input
    # partition is checked on the mapper rows either way, so a table without the column cannot skip the check.
    [("fec_receipts", "fec-bulk-individual-contributions"), ("fec_communication_costs", "fec-bulk-communication-cost-csv")],
)
def test_subject_assembly_checks_input_partition_and_receipt_readback(tmp_path, table, namespace):
    from spicy_regs.transforms.assemble_fec_query import TypedInput, assemble_subject_table, _sha

    row = {**source_row(), "source_namespace": namespace}
    if table == "fec_communication_costs":
        del row["memo_indicator"]  # not a communication-cost field
    schema = pa.Table.from_pylist([row]).schema
    source = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_pylist([row], schema=schema), source)
    item = TypedInput(source, _sha(source), 1, row["source_namespace"])
    result = assemble_subject_table(
        [item],
        table=table,
        schema=schema,
        output=tmp_path / "assembled",
        generation_id="generation-a",
        context_for=context,
        check_resources=lambda: None,
        source_partitioned=True,
    )
    assert result["exact_mapper_rows_compared"] == 1
    assert result["subject_partition_columns"] == []
    assert result["source_partition_columns"] == ["source_namespace"]
    published = pq.ParquetFile(result["subject_path"]).read()
    if table in SOURCE_NAMESPACE_TABLES:
        assert published["source_namespace"].to_pylist() == [namespace]
    else:
        assert "source_namespace" not in published.schema.names
    wrong = TypedInput(source, _sha(source), 1, "wrong-namespace")
    with pytest.raises(ValueError, match="source namespace"):
        assemble_subject_table(
            [wrong],
            table=table,
            schema=schema,
            output=tmp_path / "wrong",
            generation_id="generation-a",
            context_for=context,
            check_resources=lambda: None,
            source_partitioned=True,
        )


def test_clean_subject_dictionary_cannot_trigger_legacy_evidence_fallback(tmp_path):
    from spicy_regs.relationship_views.fec_query_views import fec_query_views

    root = tmp_path / "package"
    root.mkdir()
    installed = Path(__file__).parents[1] / "src/spicy_regs/table_metadata.json"
    metadata = json.loads(installed.read_text())
    metadata["fec_receipts"]["columns"] = [
        c
        for c in metadata["fec_receipts"]["columns"]
        if c["column_name"] not in {"source_record_id", "source_sha256", "collection_id", "source_locator_json"}
    ]
    (root / "table_metadata.json").write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="receipt-validated processing"):
        fec_query_views(
            source_generation_pin="sha256:" + "a" * 64,
            population="test",
            as_of="test",
            namespace_evidence={},
            package_root=root,
        )


@pytest.mark.parametrize('child', [[], '', 7, ['wrong']])
def test_malformed_legal_child_retains_refusal_and_valid_sibling(tmp_path, child):
    rows = [dict(record_id='bad', native_facts_json=json.dumps({'ao_citations': [child]})),
            dict(record_id='good', native_facts_json=json.dumps({'ao_citations': []}))]
    schema = pa.Table.from_pylist(rows).schema
    (subject, receipt), _ = write_fec_subjects(
        rows, tmp_path / 'bundle', table='fec_legal_matters', input_schema=schema,
        generation_id='generation-a', context_for=context)
    assert [r['record_id'] for r in pq.read_table(subject).to_pylist()] == ['good']
    receipts = pq.read_table(receipt).to_pylist()
    assert sorted(r['outcome'] for r in receipts) == ['accepted', 'refused']
    refused = next(r for r in receipts if r['outcome'] == 'refused')
    assert 'ao_citations' in refused['processing_json']


def _held(receipt):
    """Each accepted receipt's conversion inputs: the fields the subject row does not carry."""
    from spicy_regs.etl_receipts import decode_exact_json

    rows = pq.ParquetFile(receipt).read().to_pylist()
    assert [row["outcome"] for row in rows] == ["accepted"] * len(rows)
    return [decode_exact_json(row["processing_json"])["fec_conversion_inputs"] for row in rows]


@pytest.mark.parametrize("table", sorted((SOURCE_NAMESPACE_TABLES | AMOUNT_STATUS_TABLES) & FIELD_RULES.keys()))
def test_returned_values_are_published_once_and_the_pair_restores_the_mapper_row(tmp_path, table):
    """Each listed table of this family, converted from its producer's whole declared row."""
    schema = declared_schema(table)
    stated = {"source_namespace": "fec-test-layout", "amount_status": "unsupported_spelling"}
    stated = {name: value for name, value in stated.items() if name in schema.names}
    row = {**dict.fromkeys(schema.names), "record_id": "sha256:" + "c" * 64, **stated}
    (subject, receipt), policy = write_fec_subjects(
        [row], tmp_path / "bundle", table=table, input_schema=schema, generation_id="generation-a", context_for=context
    )
    returned = returned_columns(table)
    kept = sorted(set(stated) - set(returned))  # the other of the two, where this table did not get it back
    assert returned
    published = pq.ParquetFile(subject).read()
    for name in returned:
        assert published.schema.field(name).type == pa.string()
        assert published[name].to_pylist() == [stated[name]]
    assert not set(kept) & set(published.schema.names)
    # Once: a returned value is not also a conversion input, and a kept one is nowhere but there.
    [held] = _held(receipt)
    assert not set(returned) & set(held)
    assert {name: held[name] for name in kept} == {name: stated[name] for name in kept}
    assert list(read_fec_with_receipts([subject], [receipt], policy, generation_id="generation-a")) == [row]
    with duckdb.connect() as con:
        register_internal_fec_table(
            con,
            table=table,
            subject_paths=[subject],
            shared_receipt_paths=[receipt],
            policy=policy,
            input_schema=schema,
            generation_id="generation-a",
        )
        assert [r[0] for r in con.execute(f'DESCRIBE "{table}"').fetchall()] == schema.names
        columns = ", ".join(stated)
        assert con.execute(f'SELECT {columns} FROM "{table}"').fetchall() == [tuple(stated.values())]
    # The receipt vouches for a returned value: a changed one no longer joins.
    changed = published.to_pylist()
    changed[0][returned[0]] = "changed"
    pq.write_table(pa.Table.from_pylist(changed, schema=published.schema), subject)
    with pytest.raises(ValueError):
        list(read_fec_with_receipts([subject], [receipt], policy, generation_id="generation-a"))


def _built_rows():
    """Rows from the real producers, by table: bulk individual receipts and one legal matter with its events."""
    from spicy_regs.transforms import fec_legal as legal
    from spicy_regs.transforms.fec_query import bulk_receipt
    from tests import test_fec_legal, test_fec_query

    receipts = []
    for ordinal, raw in enumerate(["5700", "1e3"]):
        source = test_fec_query.source_row()
        native = json.loads(source["metadata_json"])
        native["TRANSACTION_AMT"] = raw
        record = f"source/ordinal/{ordinal}"
        locator = {**json.loads(source["source_locator_json"]), "source_record_id": record, "ordinal": ordinal}
        source.update(source_record_id=record, source_locator_json=json.dumps(locator), metadata_json=json.dumps(native))
        receipts.append(bulk_receipt(source, test_fec_query.selection("snapshot"))[0])
    native = dict(
        type="admin_fines",
        no="42",
        name="Committee",
        committee_id="C00000001",
        final_determination_amount="1250.00",
        open_date="2025-02-03",
        dispositions=[{"disposition": "Dismissed", "penalty": "0.0000000001"}, {"disposition": "Paid", "penalty": "500"}],
    )
    tables, _ = legal.map_legal(test_fec_legal.source(native), test_fec_legal.SELECTED)
    return {"fec_receipts": receipts, legal.MATTERS: tables[legal.MATTERS], legal.EVENTS: tables[legal.EVENTS]}


@pytest.mark.parametrize("table", ["fec_receipts", "fec_legal_matters", "fec_legal_events"])
def test_real_builder_rows_publish_the_returned_values_and_restore_exactly(tmp_path, table):
    built = _built_rows()[table]
    schema = declared_schema(table)
    (subject, receipt), policy = write_fec_subjects(
        built, tmp_path / "bundle", table=table, input_schema=schema, generation_id="generation-a", context_for=context
    )
    returned = returned_columns(table)
    published = {row["record_id"]: row for row in pq.ParquetFile(subject).read().to_pylist()}
    assert len(published) == len(built) and all(not set(returned) & set(held) for held in _held(receipt))
    for row in built:
        assert {name: published[row["record_id"]][name] for name in returned} == {name: row[name] for name in returned}
    restored = read_fec_with_receipts([subject], [receipt], policy, generation_id="generation-a")
    assert sorted(restored, key=lambda row: row["record_id"]) == sorted(built, key=lambda row: row["record_id"])
    # What a reader gets from the columns, in the producers' own values.
    stated = {name: [row[name] for row in built] for name in returned}
    if table == "fec_receipts":
        # The second amount is spelled 1e3: no exact value, and the status beside the NULL says why.
        assert stated == {"source_namespace": ["fec-bulk-individual-contributions"] * 2, "amount_status": ["exact", "unsupported_spelling"]}
        assert [row["amount"] for row in built][1] is None
    elif table == "fec_legal_matters":
        assert stated == {"source_namespace": ["fec-openfec-admin_fines-no"]}
    else:
        assert {"exact", "excess_precision"} <= set(stated["amount_status"])


def test_subject_assembly_writes_captured_members_once_and_hashes_each_input_once(tmp_path, monkeypatch):
    """ss-3bkk: the subject lands in the captured member layout directly, with values equal to the flat write."""
    from spicy_regs.transforms import assemble_fec_query
    from spicy_regs.transforms.assemble_fec_query import TypedInput, assemble_subject_table, owned_input

    rows = [{**source_row(), "record_id": "sha256:" + f"{i:x}" * 64} for i in range(5)]
    schema = pa.Table.from_pylist(rows).schema
    inputs = []
    for name, part in (("a.parquet", rows[:3]), ("b.parquet", rows[3:])):
        pq.write_table(pa.Table.from_pylist(part, schema=schema), tmp_path / name)
        inputs.append(TypedInput(tmp_path / name, assemble_fec_query._sha(tmp_path / name), len(part),
                                 "fec-bulk-individual-contributions"))
    hashed = []
    original = assemble_fec_query._sha
    monkeypatch.setattr(assemble_fec_query, "_sha", lambda path: hashed.append(path) or original(path))

    def assemble(output, members, items):
        return assemble_subject_table(items, table="fec_receipts", schema=schema, output=tmp_path / output,
                                      generation_id="generation-a", context_for=context, check_resources=lambda: None,
                                      source_partitioned=True, subject_members=members, read_batch_rows=2)

    flat = assemble("flat", None, inputs)
    assert sorted(hashed) == sorted(item.path for item in inputs)
    members = [("source_namespace=n/part-000000.parquet", 3), ("source_namespace=n/part-000001.parquet", 0),
               ("source_namespace=n/part-000002.parquet", 2)]
    hashed.clear()
    signed = [owned_input(item.path, item.rows, item.source_namespace) for item in inputs]
    hashed.clear()
    split = assemble("split", members, signed)
    assert hashed == []  # a signed input was hashed by its caller; assembly checks only its signature
    root = Path(split["subject_path"])
    assert [(p.relative_to(root).as_posix(), pq.ParquetFile(p).metadata.num_rows)
            for p in sorted(root.rglob("*.parquet"))] == members
    assert split["subject_rows"] == split["exact_mapper_rows_compared"] == 5
    assert pa.concat_tables(pq.ParquetFile(p).read() for p in sorted(root.rglob("*.parquet"))).equals(
        pq.ParquetFile(flat["subject_path"]).read())
    with pytest.raises(ValueError, match="exceed the declared members"):
        assemble("short", members[:2], inputs)
