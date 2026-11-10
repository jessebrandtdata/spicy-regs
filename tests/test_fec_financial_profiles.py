"""Profiles preserve complete selected membership, exact amounts and unresolved ties."""

from copy import deepcopy
from datetime import date
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import data_dictionary, mcp_server
from spicy_regs.contract_types import described_schema
from spicy_regs.pipelines import fec_financial_profiles as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_fec_financial_profiles as module
from spicy_regs.transforms import fec_summaries
from spicy_regs.transforms.fec_financial_policy import reported_summary_measure
from spicy_regs.transforms.fec_query import CollectionSelection
from tests.generation_fakes import Store
from tests.test_fec_summaries import fields

PIN = "sha256:" + "a" * 64


def summary(kind="candidate", *, ordinal=0, entity=None, **values):
    mapping = fec_summaries.CANDIDATE_CSV if kind == "candidate" else fec_summaries.COMMITTEE_CSV
    native = fields(mapping)
    if kind == "candidate":
        native.update(
            Cand_Id=entity or "H6CA04230",
            Cand_Name="Example candidate",
            Coverage_Start_Date="01/01/2025",
            Coverage_End_Date="06/30/2026",
            Total_Receipt="9007199254740993.01",
            Total_Disbursement="12.30",
            Cash_On_Hand_COP="-0.01",
            Debt_Owed_By_Committee="0",
        )
    else:
        native.update(
            CMTE_ID=entity or "C00880898",
            CMTE_NM="Example committee",
            CVG_START_DT="20250101",
            CVG_END_DT="20260630",
            TTL_RECEIPTS="100.01",
            TTL_DISB="12.30",
            COH_COP="0",
            DEBTS_OWED_BY_CMTE="",
        )
    native.update(values)
    collection = f"bulk-{kind}_summary_2026-csv"
    row = {
        "collection_id": collection,
        "source_record_id": f"row/{ordinal}",
        "source_sha256": PIN,
        "source_locator_json": json.dumps(
            {"collection_id": collection, "source_record_id": f"row/{ordinal}", "ordinal": ordinal}
        ),
        "observed_at": "2026-09-12T14:42:28+00:00",
        "metadata_json": json.dumps(native),
    }
    selection = CollectionSelection(collection, PIN, PIN, "official-fec", 2026, "snapshot", PIN)
    return fec_summaries.map_summary(row, selection, mapping)[0]


def selected(tmp_path, rows):
    """Model the retained legacy parent descriptor; the new output uses today's publisher."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / module.PARENT
    pq.write_table(pa.Table.from_pylist(rows, schema=fec_summaries.SUMMARY_SCHEMA), source, store_schema=False)
    identity = publication.file_identity(source)
    snapshot = publication.empty_index()
    snapshot["families"]["fec-query"] = {
        "artifactDigest": PIN,
        "logicalId": "urn:fixture:retained-fec-query",
        "prefix": "generations/fec-query/" + PIN.removeprefix("sha256:"),
        "tables": {
            module.PARENT: {
                "sha256": identity["sha256"],
                "byteSize": identity["bytes"],
                "rows": len(rows),
                "columns": [list(pair) for pair in described_schema(fec_summaries.SUMMARY_SCHEMA)],
            }
        },
    }
    store = Store()
    raw = json.dumps(snapshot).encode()
    store.objects[publication.INDEX_V2_KEY] = raw
    index_path = tmp_path / "publication.json"
    index_path.write_bytes(raw)
    selection = {
        "scope_id": "retained-csv-2026",
        "cycle": 2026,
        "publication": {"path": str(index_path), "sha256": "sha256:" + hashlib.sha256(raw).hexdigest()},
        "table": {"path": str(source), "pin": publication.table_pin(snapshot, module.PARENT)},
        "collections": [
            {
                "entity_kind": kind,
                "collection_id": f"bulk-{kind}_summary_2026-csv",
                "source_sha256": PIN,
                "captured_at": "2026-09-12T14:42:28+00:00",
                "source_url": f"https://www.fec.gov/files/{kind}_summary_2026.csv",
                "expected_rows": sum(row["summary_type"] == kind + "-summary-csv/1" for row in rows),
            }
            for kind in ("candidate", "committee")
        ],
    }
    return selection, snapshot, store


def rows_for(tmp_path, rows, *, mutate=None):
    selection, snapshot, _ = selected(tmp_path, rows)
    if mutate:
        mutate(selection)
    output = tmp_path / module.OUTPUT
    module.build_profiles(selection, snapshot, output)
    return pq.read_table(output).to_pylist()


def test_exact_money_and_equivalent_committee_witnesses(tmp_path):
    candidate = summary()
    committee = summary("committee", CAND_ID="H6CA04230")
    other = summary("committee", ordinal=1, CAND_ID="S6CA00001")
    result = rows_for(tmp_path, [other, candidate, committee])
    assert len(result) == 2
    c, k = result
    assert c["receipts"] == "9007199254740993.010000000"
    assert c["cash_on_hand"] == "-0.010000000" and c["debts_owed"] == "0.000000000"
    assert c["coverage_status"] == "reported" and c["period_end"] == date(2026, 6, 30)
    assert k["selection_reason"] == "equivalent_witnesses" and k["witness_count"] == 2
    assert k["receipts"] == "100.010000000" and k["debts_owed_status"] == "source_empty"
    assert {w["candidate_native_id"] for w in json.loads(k["witnesses_json"])} == {"H6CA04230", "S6CA00001"}
    assert not reported_summary_measure(candidate, "Total_Receipt").current_financial_total_qualified


@pytest.mark.parametrize("change", [{"TTL_RECEIPTS": "100.02"}, {"CVG_END_DT": "20260629"}])
def test_conflicting_amount_or_overlapping_period_never_selects_latest(tmp_path, change):
    result = rows_for(tmp_path, [summary("committee"), summary("committee", ordinal=1, **change)])[0]
    assert result["selection_status"] == "ambiguous" and result["coverage_status"] == "ambiguous"
    assert result["receipts"] is None and result["receipts_status"] == "ambiguous"
    assert result["period_end"] is None and result["witness_count"] == 2
    assert len(json.loads(result["witnesses_json"])) == 2


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"Coverage_Start_Date": "", "Coverage_End_Date": ""}, "period_start:source_empty"),
        ({"Coverage_Start_Date": "02/31/2025"}, "period_start:invalid_date"),
        ({"Coverage_End_Date": "12/31/2026"}, "period_end_after_capture"),
        ({"Coverage_Start_Date": "01/01/2023"}, "period_outside_declared_cycle"),
        ({"Coverage_Start_Date": "07/01/2026"}, "period_start_after_end"),
    ],
)
def test_unqualified_period_keeps_exact_reported_money_and_entity(tmp_path, changes, reason):
    result = rows_for(tmp_path, [summary(**changes)])[0]
    assert result["entity_id"] == "H6CA04230" and result["selection_status"] == "selected"
    assert result["coverage_status"] == "unqualified" and reason in json.loads(result["coverage_reasons_json"])
    assert result["receipts_status"] == "exact"


def test_missing_refused_and_zero_are_not_interchangeable(tmp_path):
    result = rows_for(tmp_path, [summary(Total_Receipt=None, Total_Disbursement="", Cash_On_Hand_COP="1e6")])[0]
    assert [result[key + "_status"] for key in module.MEASURES["candidate"]] == [
        "source_null",
        "source_empty",
        "refused",
        "exact",
    ]
    assert result["cash_on_hand_raw"] == "1e6" and result["cash_on_hand"] is None
    assert result["debts_owed"] == "0.000000000"


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_sha256", "sha256:" + "b" * 64),
        ("value_mapping_version", "old"),
        ("source_authority", "unverified"),
        ("summary_type", "weball/1"),
        ("source_cycle", 2024),
    ],
)
def test_source_change_or_unsupported_mapping_refuses_without_fallback(tmp_path, field, value):
    row = summary()
    row[field] = value
    selection, snapshot, _ = selected(tmp_path, [row])
    with pytest.raises(ValueError, match="declared source or mapping"):
        module.build_profiles(selection, snapshot, tmp_path / module.OUTPUT)
    assert not (tmp_path / module.OUTPUT).exists()


@pytest.mark.parametrize("failure", ["membership", "mutation"])
def test_late_refusal_preserves_prior_destination_after_multiple_batches(tmp_path, monkeypatch, failure):
    records = [summary(ordinal=i, entity=f"H6CA{i:05}") for i in range(260)]
    selection, snapshot, _ = selected(tmp_path, records)
    destination = tmp_path / module.OUTPUT
    destination.write_bytes(b"previous accepted output")
    if failure == "membership":
        selection["collections"][0]["expected_rows"] += 1
    else:
        original = module.profile

        def changed(*args):
            result = original(*args)
            if result["entity_id"] == "H6CA00259":
                path = Path(selection["table"]["path"])
                path.touch()
            return result

        monkeypatch.setattr(module, "profile", changed)
    with pytest.raises(ValueError, match="differs from declaration|changed during reading"):
        module.build_profiles(selection, snapshot, destination)
    assert destination.read_bytes() == b"previous accepted output"


def test_ordinary_generation_publication_identity_and_sql(tmp_path, monkeypatch):
    selection, snapshot, store = selected(tmp_path / "inputs", [summary(), summary("committee")])
    output = tmp_path / "profiles"
    artifact, evidence = pipeline.build(selection, output)
    index = publication.publish_generation(
        output / "generation", client=store, bucket="fixture", prior_index=snapshot, evidence_directories=(evidence,)
    )
    assert index["families"]["fec-financial-profiles"]["artifactDigest"] == artifact.pin.artifact_digest
    root = json.loads((output / "generation" / "artifact.json").read_text())
    assert root["spec"]["parents"] == {module.PARENT: selection["table"]["pin"]}
    assert not (output / module.OUTPUT).exists()  # owned output moved, not copied
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        assert connection.execute(
            "SELECT entity_id, receipts, period_end FROM fec_financial_profiles WHERE entity_kind='candidate'"
        ).fetchall() == [("H6CA04230", "9007199254740993.010000000", date(2026, 6, 30))]
    metadata = data_dictionary.build_mcp_metadata(
        data_dictionary.load_descriptions(data_dictionary.DEFAULT_DESCRIPTIONS), data_dictionary.expected_schemas()
    )
    assert metadata[module.TABLE]["identity_columns"] == ["profile_id"]
    assert data_dictionary.expected_schemas()[module.TABLE] == described_schema(module.SCHEMA)
    changed = deepcopy(snapshot)
    changed["families"]["fec-query"]["artifactDigest"] = "sha256:" + "f" * 64
    with pytest.raises(ValueError, match="selected managed"):
        module.build_profiles(selection, changed, tmp_path / "refused.parquet")


def test_acquire_saves_both_summary_csvs_into_the_raw_archive_and_can_publish_them(tmp_path, monkeypatch):
    """FEC rewrites both files daily under one URL, so each day's bytes are kept before anything reads them."""
    import httpx
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    from spicy_regs.sources import r2

    bodies = {pipeline.summary_url(kind, 2026): f"{kind} summary row\n".encode() for kind in ("candidate", "committee")}
    transport = httpx.MockTransport(lambda request: httpx.Response(
        200, content=bodies[str(request.url)], headers={"Content-Type": "text/csv", "ETag": '"fixture"'}))
    record = pipeline.acquire([2026], tmp_path / "acquire", transport=transport)
    captures = {item["collection_id"]: item for item in record["captures"]}
    assert sorted(captures) == ["bulk-candidate_summary_2026-csv", "bulk-committee_summary_2026-csv"]
    archive = CentralRawArchiveStore(tmp_path / "central-raw", create=False)  # conftest's SPICYGOV_RAW_ROOT
    for item in captures.values():
        assert item["request_url"] == pipeline.summary_url(item["entity_kind"], 2026)
        body = bodies[item["request_url"]]
        assert (item["response_sha256"], item["byte_size"]) == ("sha256:" + hashlib.sha256(body).hexdigest(), len(body))
        with archive.open(item["response_sha256"]) as stream:
            assert stream.read() == body
    assert "published" not in record

    # A hosted run's raw archive is scratch: it sends the sealed evidence and an index of the captures to R2.
    store = Store()
    monkeypatch.setattr(r2, "verified_target_client", lambda: (store, "fixture"))
    monkeypatch.setattr(r2, "require_credentials", lambda _: None)
    published = pipeline.acquire([2026], tmp_path / "hosted", transport=transport, publish=True)["published"]
    for body in bodies.values():
        assert store.objects["source-evidence/blobs/sha256/" + hashlib.sha256(body).hexdigest()] == body
    assert published["index"].startswith("source-evidence/indexes/fec-summaries/")
    index = pq.read_table(pa.BufferReader(store.objects[published["index"]])).to_pylist()
    assert {row["collection_id"] for row in index} == set(captures)
    assert {row["artifact_digest"] for row in index} == {published["evidence"]}

    with pytest.raises(ValueError, match="even election cycle"):
        pipeline.summary_url("committee", 2025)
