"""Profiles map a captured CSV pair straight from the raw archive, keeping exact amounts and unresolved ties."""

import csv
from datetime import date
import hashlib
import io
import json
from pathlib import Path
import shutil

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import data_dictionary, mcp_server
from spicy_regs.contract_types import described_schema
from spicy_regs.pipelines import fec_financial_profiles as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_fec_financial_profiles as module
from spicy_regs.transforms import fec_summaries
from tests.generation_fakes import Store
from tests.published_paths import local_paths
from tests.test_fec_summaries import fields

PIN = "sha256:" + "a" * 64
CAPTURED = "2026-09-12T14:42:28+00:00"
FIXTURES = Path(__file__).parent / "fixtures" / "fec_summaries"


def candidate(**values):
    native = fields(fec_summaries.CANDIDATE_CSV)
    native.update(
        Cand_Id="H6CA04230",
        Cand_Name="Example candidate",
        Coverage_Start_Date="01/01/2025",
        Coverage_End_Date="06/30/2026",
        Total_Receipt="9007199254740993.01",
        Total_Disbursement="12.30",
        Cash_On_Hand_COP="-0.01",
        Debt_Owed_By_Committee="0",
    )
    return native | values


def committee(**values):
    native = fields(fec_summaries.COMMITTEE_CSV)
    native.update(
        CMTE_ID="C00880898",
        CMTE_NM="Example committee",
        CVG_START_DT="20250101",
        CVG_END_DT="20260630",
        TTL_RECEIPTS="100.01",
        TTL_DISB="12.30",
        COH_COP="0",
        DEBTS_OWED_BY_CMTE="",
    )
    return native | values


def real(kind):
    """Real rows of FEC's 2026-11-10 file: a name holding commas, and one committee linked to two candidates."""
    return (FIXTURES / f"{kind}_summary_2026.csv").read_bytes()


def csv_bytes(kind, rows):
    """A summary CSV as FEC writes it: a bare header in FEC's column order, then every data field quoted."""
    names = real(kind).decode().splitlines()[0].split(",")
    text = io.StringIO()
    text.write(",".join(names) + "\n")
    csv.DictWriter(text, names, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(rows)
    return text.getvalue().encode()


def captured(tmp_path, candidates=(), committees=(), *, observed=CAPTURED, cycle=2026, bodies=None, root=None):
    """Both CSVs in a raw archive (``root``, else one in ``tmp_path``), and their captures as ``acquire`` records them."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    store = CentralRawArchiveStore(root or tmp_path / "raw")
    captures = []
    for kind, rows in (("candidate", candidates), ("committee", committees)):
        body = (bodies or {}).get(kind) or csv_bytes(kind, rows)
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        store.put_blob(digest, len(body), [body])
        captures.append({"entity_kind": kind, "cycle": cycle, "collection_id": module.collection_id(kind, cycle),
                         "request_url": pipeline.summary_url(kind, cycle), "response_sha256": digest,
                         "observed_at": observed, "byte_size": len(body)})
    return captures, store


def rows_for(tmp_path, candidates=(), committees=(), **kwargs):
    captures, store = captured(tmp_path, candidates, committees, **kwargs)
    output = tmp_path / module.OUTPUT
    module.build_profiles(module.select(captures), store, output, selection_sha256=PIN)
    return pq.read_table(output).to_pylist()


def test_exact_money_and_equivalent_committee_witnesses(tmp_path):
    linked = committee(CAND_ID="H6CA04230")
    result = rows_for(tmp_path, [candidate()], [committee(CAND_ID="S6CA00001"), linked])
    assert len(result) == 2
    c, k = result
    assert c["receipts"] == "9007199254740993.010000000"
    assert c["cash_on_hand"] == "-0.010000000" and c["debts_owed"] == "0.000000000"
    assert c["coverage_status"] == "reported" and c["period_end"] == date(2026, 6, 30)
    assert c["scope_id"] == "fec-summary-csv-2026-captured-2026-09-12" and c["source_table_generation"] is None
    assert k["selection_reason"] == "equivalent_witnesses" and k["witness_count"] == 2
    assert k["receipts"] == "100.010000000" and k["debts_owed_status"] == "source_empty"
    witnesses = json.loads(k["witnesses_json"])
    assert {w["candidate_native_id"] for w in witnesses} == {"H6CA04230", "S6CA00001"}
    # Each witness keeps the byte locator fec-observations gives the same row, under its own header.
    locator = json.loads(witnesses[0]["source_locator_json"])
    assert locator["source_record_id"] == f"{k['source_sha256']}/original/{locator['ordinal']:020d}"
    assert locator["field_mapping"]["source_locator"]["ordinal"] == 0
    assert {w["selection_evidence_sha256"] for w in witnesses} == {PIN}


def test_real_rows_keep_quoted_commas_and_one_profile_per_linked_committee(tmp_path):
    rows = {kind: list(csv.DictReader(io.StringIO(real(kind).decode()))) for kind in ("candidate", "committee")}
    captures, store = captured(tmp_path, bodies={kind: real(kind) for kind in rows})
    output = tmp_path / module.OUTPUT
    summary = module.build_profiles(module.select(captures), store, output, selection_sha256=PIN)
    assert summary["source_rows"] == {"bulk-candidate_summary_2026-csv": 1, "bulk-committee_summary_2026-csv": 2,
                                      "bulk-candidate_summary_2026-csv:unmapped": 1,
                                      "bulk-committee_summary_2026-csv:unmapped": 1}
    c, k = pq.read_table(output).to_pylist()
    (row,) = rows["candidate"]
    assert (c["entity_id"], c["entity_name"]) == (row["Cand_Id"], "CARL, JERRY LEE, JR") == (row["Cand_Id"], row["Cand_Name"])
    assert (c["receipts_raw"], c["cash_on_hand_raw"], c["period_end_raw"]) == (
        row["Total_Receipt"], row["Cash_On_Hand_COP"], row["Coverage_End_Date"])
    assert k["entity_id"] == "C00285254" and k["selection_reason"] == "equivalent_witnesses" and k["witness_count"] == 2
    assert {w["candidate_native_id"] for w in json.loads(k["witnesses_json"])} == {r["CAND_ID"] for r in rows["committee"]}
    assert k["receipts_raw"] == rows["committee"][0]["TTL_RECEIPTS"] and k["receipts_status"] == "exact"


def test_profile_id_survives_a_refresh_and_tells_entities_kinds_and_cycles_apart(tmp_path):
    def by_entity(rows):
        return {(row["entity_kind"], row["entity_id"]): row for row in rows}

    september = by_entity(rows_for(tmp_path / "september", [candidate()], [committee()]))
    november = by_entity(rows_for(tmp_path / "november", [candidate(Total_Receipt="1.00")],
                                  [committee(TTL_RECEIPTS="2.00"), committee(CMTE_ID="C00000001")],
                                  observed="2026-11-10T16:58:18+00:00"))
    for key in september:
        assert september[key]["profile_id"] == november[key]["profile_id"]
        assert september[key]["scope_id"] != november[key]["scope_id"]
    assert len({row["profile_id"] for row in november.values()}) == len(november) == 3
    # The same native identifier under the other kind, and the same entity in another cycle, are other profiles.
    other_kind = by_entity(rows_for(tmp_path / "kind", [candidate(Cand_Id="C00880898")], [committee()]))
    assert other_kind["candidate", "C00880898"]["profile_id"] != other_kind["committee", "C00880898"]["profile_id"]
    other_cycle = by_entity(rows_for(tmp_path / "cycle", [candidate()], [committee()], cycle=2024))
    assert other_cycle["candidate", "H6CA04230"]["profile_id"] != september["candidate", "H6CA04230"]["profile_id"]


@pytest.mark.parametrize("kind", ["candidate", "committee"])
@pytest.mark.parametrize("damage", ["header-only", "cut at a row boundary"])
def test_a_file_that_lost_its_rows_refuses_unless_accepted(tmp_path, kind, damage):
    full = {"candidate": [candidate(Cand_Id=f"H6CA{i:05}") for i in range(20)],
            "committee": [committee(CMTE_ID=f"C{i:08}") for i in range(20)]}
    published = {"candidate": 20, "committee": 20}

    def build(directory, kept, **kwargs):
        rows = {**full, kind: full[kind][:kept]}
        captures, store = captured(directory, rows["candidate"], rows["committee"])
        return module.build_profiles(module.select(captures), store, directory / module.OUTPUT,
                                     selection_sha256=PIN, prior_rows=published, **kwargs)

    kept = 0 if damage == "header-only" else 10
    with pytest.raises(ValueError, match=f"The {kind} summary CSV holds {kept} rows.* --accept-shrink {kind}"):
        build(tmp_path / "refused", kept)
    assert not (tmp_path / "refused" / module.OUTPUT).exists()
    summary = build(tmp_path / "accepted", kept, accept_shrink=[kind])
    assert summary["accepted_shrink"] == [kind] and summary["published_rows"] == published
    # Losing exactly the stated share (2 of 20) still builds without acceptance.
    assert build(tmp_path / "within", 18)["profiles"] == {"selected": 38}


@pytest.mark.parametrize("change", [{"TTL_RECEIPTS": "100.02"}, {"CVG_END_DT": "20260629"}])
def test_conflicting_amount_or_overlapping_period_never_selects_latest(tmp_path, change):
    result = rows_for(tmp_path, committees=[committee(), committee(**change)])[0]
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
    result = rows_for(tmp_path, [candidate(**changes)])[0]
    assert result["entity_id"] == "H6CA04230" and result["selection_status"] == "selected"
    assert result["coverage_status"] == "unqualified" and reason in json.loads(result["coverage_reasons_json"])
    assert result["receipts_status"] == "exact"


def test_blank_refused_and_zero_are_not_interchangeable(tmp_path):
    result = rows_for(tmp_path, [candidate(Total_Receipt="", Total_Disbursement="0", Cash_On_Hand_COP="1e6")])[0]
    assert [result[key + "_status"] for key in module.MEASURES["candidate"]] == [
        "source_empty",
        "exact",
        "refused",
        "exact",
    ]
    assert result["receipts"] is None and result["disbursements"] == "0.000000000"
    assert result["cash_on_hand_raw"] == "1e6" and result["cash_on_hand"] is None


def test_a_wrong_file_or_selection_refuses_without_output(tmp_path):
    captures, store = captured(tmp_path, [candidate()], [committee()])
    output = tmp_path / module.OUTPUT
    # The committee file selected as the candidate CSV: its header lacks the candidate layout.
    swapped = [{**captures[0], **{key: captures[1][key] for key in ("response_sha256", "byte_size")}}, captures[1]]
    with pytest.raises(ValueError, match="lacks fields required"):
        module.build_profiles(module.select(swapped), store, output, selection_sha256=PIN)
    assert not output.exists()
    with pytest.raises(ValueError, match="exactly one candidate CSV and one committee CSV"):
        module.select(captures[:1])
    with pytest.raises(ValueError, match="one even FEC election cycle"):
        module.select([captures[0], {**captures[1], "cycle": 2024}])
    with pytest.raises(ValueError, match="declared CSV cycle"):
        module.select([captures[0], {**captures[1], "collection_id": "bulk-committee_summary_2024-csv"}])


def test_a_short_row_refuses_the_whole_build(tmp_path):
    captures, store = captured(tmp_path, [candidate()], [committee()])
    body = csv_bytes("committee", [committee()]) + b"C00000001,short\n"
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    store.put_blob(digest, len(body), [body])
    captures[1] |= {"response_sha256": digest, "byte_size": len(body)}
    with pytest.raises(ValueError, match="field count differs from its selected header"):
        module.build_profiles(module.select(captures), store, tmp_path / module.OUTPUT, selection_sha256=PIN)


def test_late_refusal_preserves_prior_destination_after_multiple_batches(tmp_path, monkeypatch):
    captures, store = captured(tmp_path, [candidate(Cand_Id=f"H6CA{i:05}") for i in range(260)])
    destination = tmp_path / module.OUTPUT
    destination.write_bytes(b"previous accepted output")
    original = module.profile

    def refused(rows, capture, selection):
        if rows[0]["candidate_native_id"] == "H6CA00259":
            raise ValueError("late refusal")
        return original(rows, capture, selection)

    monkeypatch.setattr(module, "profile", refused)
    with pytest.raises(ValueError, match="late refusal"):
        module.build_profiles(module.select(captures), store, destination, selection_sha256=PIN)
    assert destination.read_bytes() == b"previous accepted output"


def test_acquire_build_and_publish_one_generation_from_the_captured_pair(tmp_path, monkeypatch):
    """The capture record ``acquire`` writes is the build's whole input; the generation pins no other family."""
    bodies = {pipeline.summary_url("candidate", 2026): csv_bytes("candidate", [candidate()]),
              pipeline.summary_url("committee", 2026): csv_bytes("committee", [committee(), committee()])}
    transport = httpx.MockTransport(lambda request: httpx.Response(
        200, content=bodies[str(request.url)], headers={"Content-Type": "text/csv"}))
    record = pipeline.acquire([2026], tmp_path / "acquire", transport=transport)
    output = tmp_path / "profiles"
    artifact, evidence = pipeline.build(record["captures"], output)
    assert local_paths(output / "generation", evidence, also=(str(tmp_path),)) == []
    store = Store()
    index = publication.publish_generation(output / "generation", client=store, bucket="fixture",
                                           prior_index=publication.empty_index(), evidence_directories=(evidence,))
    assert index["families"]["fec-financial-profiles"]["artifactDigest"] == artifact.pin.artifact_digest
    root = json.loads((output / "generation" / "artifact.json").read_text())
    assert not root["spec"].get("parents")
    assert not (output / module.OUTPUT).exists()  # owned output moved, not copied
    events = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    cited = {event["sha256"] for event in events
             if event["event"] == "retained-file" and event.get("blob_store") == "central-raw"}
    assert cited == {item["response_sha256"] for item in record["captures"]}
    for digest in cited:
        assert store.objects["source-evidence/blobs/sha256/" + digest.removeprefix("sha256:")] in bodies.values()
    (selection,) = [event for event in events if event.get("stage") == "profile-selection"]
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        assert connection.execute(
            "SELECT entity_id, receipts, period_end, witness_count FROM fec_financial_profiles ORDER BY entity_kind"
        ).fetchall() == [("H6CA04230", "9007199254740993.010000000", date(2026, 6, 30), 1),
                         ("C00880898", "100.010000000", date(2026, 6, 30), 2)]
        witnesses = connection.execute("SELECT witnesses_json FROM fec_financial_profiles").fetchall()
    assert {json.loads(text)[0]["selection_evidence_sha256"] for (text,) in witnesses} == {selection["sha256"]}
    metadata = data_dictionary.build_mcp_metadata(
        data_dictionary.load_descriptions(data_dictionary.DEFAULT_DESCRIPTIONS), data_dictionary.expected_schemas()
    )
    assert metadata[module.TABLE]["identity_columns"] == ["profile_id"]
    assert data_dictionary.expected_schemas()[module.TABLE] == described_schema(module.SCHEMA)
    with pytest.raises(ValueError, match="fresh directory"):
        pipeline.build(record["captures"], output)


def test_a_hand_build_holds_each_file_to_the_mirrored_live_table(tmp_path):
    """``--read-index`` names a ``spicy-regs mirror`` snapshot; the live table beside it sets each file's floor."""
    raw = tmp_path / "central-raw"  # conftest's SPICYGOV_RAW_ROOT, which the build reads
    first, _ = captured(tmp_path, [candidate(Cand_Id=f"H6CA{i:05}") for i in range(20)], [committee()], root=raw)
    artifact, evidence = pipeline.build(first, tmp_path / "first")
    index = publication.publish_generation(tmp_path / "first" / "generation", client=Store(), bucket="fixture",
                                           prior_index=publication.empty_index(), evidence_directories=(evidence,))
    mirror = tmp_path / "live"
    shutil.copytree(tmp_path / "first" / "generation", mirror / index["families"][pipeline.FAMILY]["prefix"])
    read_index = mirror / "publication.v2" / "index.json"
    read_index.parent.mkdir()
    read_index.write_text(json.dumps(index))

    header_only, _ = captured(tmp_path, [], [committee()], root=raw, observed="2026-11-10T16:58:18+00:00")
    with pytest.raises(ValueError, match="The candidate summary CSV holds 0 rows.*20 the published"):
        pipeline.build(header_only, tmp_path / "refused", read_index=read_index)
    accepted, _ = pipeline.build(header_only, tmp_path / "accepted", read_index=read_index, accept_shrink=["candidate"])
    root = json.loads((tmp_path / "accepted" / "generation" / "artifact.json").read_text())
    assert root["spec"]["readSnapshot"]["families"][pipeline.FAMILY]["artifactDigest"] == artifact.pin.artifact_digest
    assert pq.read_table(tmp_path / "accepted" / "generation" / module.OUTPUT).num_rows == 1


def test_acquire_saves_both_summary_csvs_into_the_raw_archive_and_can_publish_them(tmp_path, monkeypatch):
    """FEC rewrites both files daily under one URL, so each day's bytes are kept before anything reads them."""
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
