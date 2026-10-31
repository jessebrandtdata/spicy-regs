"""Selected source scopes, native fields and evidence survive retained delivery."""

import hashlib
import json
from datetime import UTC, datetime
from io import BytesIO
from zipfile import ZipFile

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import MemberNotFoundError

from spicy_regs.transforms.build_fec_observations import build_fec_observations


def _query(tmp_path, *, name="committees", records=None, profile="committee", family="fec_committees"):
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    if records is None:
        records = [
            {
                "committee_id": "C00000001",
                "name": "Literal",
                "candidate_ids": ["H2AK01158", "H2AK01158"],
                "sponsor_candidate_list": [None],
                "future_field": {"null": None, "zero": 0, "empty": []},
            }
        ]
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")
    captures = []
    endpoint = {"committee": "committees", "candidate": "candidates", "filing": "filings", "audit": "audit-case"}[
        profile
    ]
    for index in range(max(1, len(records))):
        value = {
            "api_version": "1.0",
            "pagination": {
                "page": index + 1,
                "pages": len(records),
                "count": len(records),
                "is_count_exact": True,
                "per_page": 1,
            },
            "results": records[index : index + 1],
        }
        raw = json.dumps(value).encode()
        digest = "sha256:" + hashlib.sha256(raw).hexdigest()
        store.put_blob(digest, len(raw), (raw,))
        url = f"https://api.open.fec.gov/v1/{endpoint}/?per_page=1"
        if profile == "committee":
            url += "&sort=committee_id"
        if index:
            url += f"&page={index + 1}"
        captures.append(
            {"requestUrl": url, "observedAt": "2026-09-21T10:00:00Z", "responseSha256": digest, "byteSize": len(raw)}
        )
    return {
        "collection_id": name,
        "source_family": family,
        "profile": profile,
        "blob_root": "blobs",
        "captures": captures,
    }, records


def _manifest(tmp_path, items):
    path = tmp_path / "inputs.json"
    path.write_text(json.dumps({"version": 1, "collections": items}))
    return path


def _rows(paths):
    return [pq.read_table(path).to_pylist() for path in paths]


def test_raw_queries_keep_complete_metadata_empty_collections_and_parent_join(tmp_path):
    item, original = _query(tmp_path)
    empty, _ = _query(tmp_path, name="no-candidate-results", records=[], profile="candidate", family="fec_candidates")
    records, collections, relationships = _rows(
        build_fec_observations(_manifest(tmp_path, [item, empty]), tmp_path / "out", batch_size=1)
    )
    assert len(records) == 1
    record = records[0]
    assert json.loads(record["metadata_json"]) == original[0]
    assert record["source_sha256"] == item["captures"][0]["responseSha256"]
    assert record["observed_at"] == item["captures"][0]["observedAt"]
    assert [row["record_count"] for row in collections] == [1, 0]
    assert collections[1]["record_outcome"] == "empty"
    assert json.loads(collections[1]["requested_scope_json"])["captures"] == empty["captures"]
    assert all(row["artifact_sha256"] is None for row in collections)
    assert len(relationships) == 5
    for relationship in relationships:
        locator = json.loads(relationship["source_locator_json"])
        assert (locator["collection_id"], locator["source_record_id"]) == (
            record["collection_id"],
            record["source_record_id"],
        )
        assert locator["json_pointer"] == "/results/0"
    assert [r["object_id"] for r in relationships if r["relationship_type"] == "committee_candidate"] == [
        "H2AK01158",
        "H2AK01158",
    ]


def test_filing_negative_file_number_decimal_string_and_unknown_values_survive(tmp_path):
    item, _ = _query(
        tmp_path,
        name="filings",
        profile="filing",
        family="fec_reports",
        records=[{"sub_id": "123", "file_number": -24, "amount": "-0.0100", "unknown": False}],
    )
    records, _, relationships = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    assert records[0]["filing_id"] == "123"
    assert json.loads(records[0]["metadata_json"]) == {
        "sub_id": "123",
        "file_number": -24,
        "amount": "-0.0100",
        "unknown": False,
    }
    assert relationships == []


@pytest.mark.parametrize("mutation", ["truncated", "bad_digest", "duplicate", "late_failure"])
def test_incomplete_or_changed_input_installs_no_outputs(tmp_path, mutation):
    item, _ = _query(tmp_path, records=[{"committee_id": "C00000001"}, {"committee_id": "C00000002"}])
    items = [item]
    if mutation == "truncated":
        item["captures"].pop()
    elif mutation == "bad_digest":
        item["captures"][1]["byteSize"] += 1
    elif mutation == "duplicate":
        item, _ = _query(
            tmp_path,
            profile="candidate",
            family="fec_candidates",
            records=[{"candidate_id": "H2AK01158"}, {"candidate_id": "H2AK01158"}],
        )
        items = [item]
    else:
        bad, _ = _query(tmp_path, name="late-bad", records=[], profile="candidate", family="fec_candidates")
        bad["captures"][0]["responseSha256"] = "sha256:" + "0" * 64
        items.append(bad)
    with pytest.raises((ValueError, MemberNotFoundError)):
        build_fec_observations(_manifest(tmp_path, items), tmp_path / "out", batch_size=1)
    assert not (tmp_path / "out").exists()
    assert not list(tmp_path.glob(".fec-observations-*"))


def test_replay_has_equal_rows_and_refuses_existing_generation(tmp_path):
    item, _ = _query(tmp_path)
    manifest = _manifest(tmp_path, [item])
    first = build_fec_observations(manifest, tmp_path / "first")
    second = build_fec_observations(manifest, tmp_path / "second")
    assert _rows(first) == _rows(second)
    before = [path.read_bytes() for path in first]
    with pytest.raises(FileExistsError, match="new generation"):
        build_fec_observations(manifest, tmp_path / "first")
    assert before == [path.read_bytes() for path in first]


def test_pinned_release_reuses_verified_reader_and_refuses_different_pin(tmp_path):
    from rulespec_artifacts import Producer
    from spicy_docs.releases.format import VERIFIER_ID, VERIFIER_VERSION
    from spicy_docs.source_native import SourceNativeReleaseBuild, SourceNativeReleasePublisher
    from spicy_docs.sources.fec.profile import (
        FEC_COMMITTEE_CENSUS_PROFILE,
        committee_census_scope,
        iter_retained_committee_pages,
    )
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    item, original = _query(tmp_path)
    implementation = "git+https://example.com/test@" + "a" * 40
    producer = Producer(
        product="spicy-docs",
        implementation_id=implementation,
        verifier_id=VERIFIER_ID,
        verifier_version=VERIFIER_VERSION,
        verifier_implementation_id=implementation,
    )
    release = SourceNativeReleasePublisher(
        FEC_COMMITTEE_CENSUS_PROFILE,
        blob_store=LocalSourceNativeBlobStore(tmp_path / "release-blobs"),
        clock=lambda: datetime(2026, 9, 21, 12, tzinfo=UTC),
    ).publish(
        iter_retained_committee_pages(item["captures"], blob_source=LocalSourceNativeBlobStore(tmp_path / "blobs")),
        build=SourceNativeReleaseBuild(
            query_scope=committee_census_scope(item["captures"]), producer=producer, started_at="2026-09-21T11:00:00Z"
        ),
        destination=tmp_path / "release",
    )
    del item["captures"]
    item.update(
        release_path="release",
        blob_root="release-blobs",
        artifact_sha256=release.artifact.pin.artifact_digest,
        verifier_implementation_id=implementation,
    )
    records, collections, _ = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    assert json.loads(records[0]["metadata_json"]) == original[0]
    assert collections[0]["artifact_sha256"] == release.artifact.pin.artifact_digest
    item["artifact_sha256"] = "sha256:" + "0" * 64
    with pytest.raises(ValueError):
        build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


def _positional(tmp_path, raw, *, member=None, format="delimited"):
    from spicy_docs.sources.fec.row_profile import positional_row_scope
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")
    store.put_blob(digest, len(raw), (raw,))
    capture = {
        "requestUrl": "https://www.fec.gov/files/example.zip"
        if member
        else "https://docquery.fec.gov/dcdev/posted/123.fec",
        "responseSha256": digest,
        "byteSize": len(raw),
        "observedAt": "2026-09-21T11:00:00Z",
        "representation": "zip" if member else "opaque",
    }
    options = {"delimiter": ",", "quoting": "csv"} if format == "delimited" else {}
    scope = positional_row_scope(capture, format=format, encoding="utf-8", member=member, **options)
    return {
        "collection_id": "selected-file",
        "profile": "positional",
        "source_family": "fec_electioneering",
        "blob_root": "blobs",
        "scope": scope,
    }


def test_direct_zip_rows_preserve_header_blank_duplicate_negative_and_member_bytes(tmp_path):
    decoded = b'ID,AMOUNT,NOTE\r\nC00000001,-1.00,"literal, note"\r\n\r\nC00000001,-1.00,"literal, note"\r\n'
    buf = BytesIO()
    with ZipFile(buf, "w") as archive:
        archive.writestr("selected.csv", decoded)
        archive.writestr("other.csv", b"not selected")
    item = _positional(tmp_path, buf.getvalue(), member={"ordinal": 0, "name": "selected.csv"})
    records, collections, relations = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    assert len(records) == 4 and collections[0]["record_count"] == 4 and relations == []
    assert [json.loads(row["metadata_json"])["fields"] for row in records] == [
        ["ID", "AMOUNT", "NOTE"],
        ["C00000001", "-1.00", "literal, note"],
        [],
        ["C00000001", "-1.00", "literal, note"],
    ]
    for row in records:
        loc = json.loads(row["source_locator_json"])
        assert loc["member"] == {"ordinal": 0, "name": "selected.csv"}
        assert loc["sha256"] == "sha256:" + hashlib.sha256(decoded).hexdigest()
        assert row["source_sha256"] == item["scope"]["capture"]["responseSha256"]
    item["scope"]["member"]["name"] = "wrong.csv"
    with pytest.raises(ValueError, match="member identity"):
        build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


def test_zip_members_read_and_hash_their_original_once_per_contiguous_run(tmp_path, monkeypatch):
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    buf = BytesIO()
    with ZipFile(buf, "w") as archive:
        archive.writestr("first.csv", b"A,1\r\n")
        archive.writestr("second.csv", b"B,2\r\nC,3\r\n")
    first = _positional(tmp_path, buf.getvalue(), member={"ordinal": 0, "name": "first.csv"})
    second = {
        **_positional(tmp_path, buf.getvalue(), member={"ordinal": 1, "name": "second.csv"}),
        "collection_id": "second",
    }
    other = {**_positional(tmp_path, b"D,4\r\n"), "collection_id": "other"}
    again = {**second, "collection_id": "again"}
    opened = []
    real = LocalSourceNativeBlobStore.open

    def counted(self, blob_ref):
        opened.append(blob_ref)
        return real(self, blob_ref)

    monkeypatch.setattr(LocalSourceNativeBlobStore, "open", counted)
    together = _rows(build_fec_observations(_manifest(tmp_path, [first, second, other, again]), tmp_path / "run"))
    zip_ref, other_ref = ("sha256:" + hashlib.sha256(raw).hexdigest() for raw in (buf.getvalue(), b"D,4\r\n"))
    # One read per contiguous run: first+second share one; "again" follows another original.
    assert opened == [zip_ref, other_ref, zip_ref]
    alone = [
        _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / item["collection_id"]))
        for item in (first, second, other, again)
    ]
    for table in range(3):
        assert together[table] == [row for rows in alone for row in rows[table]]


def test_native_filing_body_remains_a_reference_and_statement_uses_header_version(tmp_path):
    header = "HDR\x1cFEC\x1c8.4\x1cTEST\x1c1\x1c\x1c\x1c\n"
    fields = [""] * 44
    fields[0], fields[1], fields[23], fields[24] = "F2N", "H2AK01158", "C00000001", "Literal committee"
    raw = (header + "\x1c".join(fields) + "\n[BEGINTEXT]\nLiteral narrative\n[ENDTEXT]\n").encode()
    item = _positional(tmp_path, raw, format="fec")
    records, _, relationships = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    bodies = [row for row in records if json.loads(row["metadata_json"]).get("kind") == "text"]
    assert len(bodies) == 1
    (body,) = json.loads(bodies[0]["embedded_bodies_json"])
    assert b"Literal narrative" in raw[body["byte_offset"] : body["byte_offset"] + body["byte_length"]]
    assert any(
        row["relationship_type"] == "principal_campaign_committee" and row["object_id"] == "C00000001"
        for row in relationships
    )


def test_rollup_defaults_to_local_generation_and_publishes_complete_family(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.fec_observations import FecObservationsRollup
    from spicy_regs.sources import r2, publication, cloudflare
    from tests.generation_fakes import Store

    item, _ = _query(tmp_path)
    manifest = _manifest(tmp_path, [item])
    store = Store()
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(publication, "load_index", lambda url: publication.empty_index())
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: None)
    FecObservationsRollup(manifest=manifest, output_dir=tmp_path / "outputs").run()
    assert store.writes == []
    [local_generation] = list((tmp_path / "outputs" / "generations").iterdir())
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    FecObservationsRollup(manifest=manifest, output_dir=tmp_path / "outputs", skip_upload=False).run()
    index = publication.parse_index(store.objects[publication.INDEX_V2_KEY])
    family = index["families"]["fec-observations"]
    assert set(family["tables"]) == {"fec_relationships.parquet", "fec_collections.parquet", "fec_source_records.parquet"}
    assert set(family["etlReceipts"]["datasets"]) == {"fec_source_records", "fec_collections", "fec_relationships"}
    assert store.writes[-1] == publication.INDEX_V2_KEY
    # The second run retains its own observation receipt; table bytes remain identical.
    assert local_generation.is_dir()
    for key in index["families"]["fec-observations"]["tables"]:
        remote_key = publication.single_member(index, key).path
        assert store.objects[remote_key] == (local_generation / key).read_bytes()


def test_selected_external_header_names_fields_and_emits_reported_bulk_relationship(tmp_path):
    header = _positional(tmp_path, b"CAND_ID,CAND_ELECTION_YR,FEC_ELECTION_YR,CMTE_ID,CMTE_TP,CMTE_DSGN,LINKAGE_ID\n")
    header["collection_id"] = "official-header"
    data = _positional(tmp_path, b"H2AK01158,2020,2024,C00000001,H,P,001\n")
    data["field_mapping"] = {
        "header_collection_id": "official-header",
        "header_row_ordinal": 0,
        "data_has_header": False,
        "relationship_family": "linkage",
        "cycle": 2024,
    }
    records, collections, relations = _rows(
        build_fec_observations(_manifest(tmp_path, [header, data]), tmp_path / "out")
    )
    mapped = json.loads(records[1]["metadata_json"])
    assert mapped["CAND_ELECTION_YR"] == "2020" and mapped["FEC_ELECTION_YR"] == "2024"
    assert mapped["LINKAGE_ID"] == "001"
    assert records[1]["candidate_id"] == "H2AK01158" and records[1]["committee_id"] == "C00000001"
    assert json.loads(records[1]["source_record_json"])["record"]["record"]["fields"] == [
        "H2AK01158",
        "2020",
        "2024",
        "C00000001",
        "H",
        "P",
        "001",
    ]
    (relation,) = relations
    assert relation["relationship_type"] == "principal_campaign_committee"
    assert relation["candidate_election_year"] == "2020" and relation["cycle"] == "2024"
    locator = json.loads(relation["source_locator_json"])
    assert locator["field_mapping"]["collection_id"] == "official-header"
    assert locator["field_mapping"]["source_sha256"] == header["scope"]["capture"]["responseSha256"]
    assert json.loads(collections[1]["collection_outcome_json"])["tableFieldMapping"] == data["field_mapping"]
    with pytest.raises(ValueError, match="earlier collection"):
        build_fec_observations(_manifest(tmp_path, [data, header]), tmp_path / "bad")


@pytest.mark.parametrize("raw", [b"A,A\n1,2\n", b"A,B\n1\n", b",B\n1,2\n"])
def test_header_mapping_refuses_ambiguous_names_or_wrong_width(tmp_path, raw):
    item = _positional(tmp_path, raw)
    item["field_mapping"] = {
        "header_collection_id": item["collection_id"],
        "header_row_ordinal": 0,
        "data_has_header": True,
    }
    with pytest.raises(ValueError, match="header"):
        build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


@pytest.mark.parametrize(
    "committee_field,candidate_field",
    [
        ("CMTE_ID", "CAND_ID"),
        ("Committee_Id", "Cand_Id"),
        ("COMMITTEE_ID", "CANDIDATE_ID"),
        ("committee_id", "cand_id"),
    ],
)
def test_named_source_ids_join_api_rows_without_changing_literal_source_fields(
    tmp_path, committee_field, candidate_field
):
    import duckdb

    source = f"{committee_field},{candidate_field},VALUE\nC00000001,H2AK01158,001\n,,\n"
    item = _positional(tmp_path, source.encode())
    item["field_mapping"] = {
        "header_collection_id": item["collection_id"],
        "header_row_ordinal": 0,
        "data_has_header": True,
    }
    candidate, _ = _query(
        tmp_path,
        name="candidate-query",
        profile="candidate",
        family="fec_candidates",
        records=[{"candidate_id": "H2AK01158", "name": "Source candidate"}],
    )
    paths = build_fec_observations(_manifest(tmp_path, [item, candidate]), tmp_path / "out")
    with duckdb.connect() as con:
        joined = con.execute(
            "SELECT b.committee_id, b.candidate_id, json_extract_string(a.metadata_json, '$.name') FROM read_parquet(?) b JOIN read_parquet(?) a USING (candidate_id) WHERE b.profile='positional' AND a.profile='candidate'",
            [str(paths[0]), str(paths[0])],
        ).fetchall()
    assert joined == [("C00000001", "H2AK01158", "Source candidate")]
    records = pq.read_table(paths[0]).to_pylist()
    assert json.loads(records[1]["source_record_json"])["record"]["record"]["fields"] == [
        "C00000001",
        "H2AK01158",
        "001",
    ]
    assert records[2]["committee_id"] == records[2]["candidate_id"] == ""


def test_source_aggregate_id_remains_literal_without_inferring_a_person_or_spender_role(tmp_path):
    item = _positional(tmp_path, b"CAND_ID,CAND_NM,spe_id\nP00000001,All candidates,C00000001\n")
    item["field_mapping"] = {
        "header_collection_id": item["collection_id"],
        "header_row_ordinal": 0,
        "data_has_header": True,
    }
    records, _, relationships = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    assert records[1]["candidate_id"] == "P00000001"
    assert records[1]["committee_id"] is None
    assert json.loads(records[1]["metadata_json"])["CAND_NM"] == "All candidates"
    assert relationships == []


@pytest.mark.parametrize("values", ["C00000001,C00000002", "C00000001,"])
def test_conflicting_identifier_aliases_refuse_instead_of_choosing_or_filling(tmp_path, values):
    item = _positional(tmp_path, f"CMTE_ID,COMMITTEE_ID\n{values}\n".encode())
    item["field_mapping"] = {
        "header_collection_id": item["collection_id"],
        "header_row_ordinal": 0,
        "data_has_header": True,
    }
    with pytest.raises(ValueError, match="conflicting source aliases"):
        build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


def _dictionary_mapping(tmp_path, item, html=None):
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    raw = (
        html
        or "<table><tr><td>Column name<td>Field name<td>Position<tr><td>CAND_ID<td>Candidate identifier<td>1<tr><td>TTL_RECEIPTS<td>Total receipts<td>2</table>"
    ).encode()
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    LocalSourceNativeBlobStore(tmp_path / "dictionary-blobs").put_blob(digest, len(raw), (raw,))
    capture = {
        "requestUrl": "https://www.fec.gov/campaign-finance-data/all-candidates-file-description/",
        "responseSha256": digest,
        "byteSize": len(raw),
        "observedAt": "2026-09-21T11:00:00Z",
        "representation": "opaque",
    }
    item["field_mapping"] = {
        "dictionary": {"capture": capture, "blob_root": "dictionary-blobs"},
        "data_has_header": False,
    }
    return raw


def test_official_html_dictionary_names_source_fields_and_retains_definition_coordinates(tmp_path):
    item = _positional(tmp_path, b"H2AK01158,-001.50\n,\n")
    raw = _dictionary_mapping(tmp_path, item)
    records, collections, relationships = _rows(build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out"))
    assert json.loads(records[0]["metadata_json"]) == {"CAND_ID": "H2AK01158", "TTL_RECEIPTS": "-001.50"}
    assert records[0]["candidate_id"] == "H2AK01158" and records[1]["candidate_id"] == ""
    assert json.loads(records[0]["source_record_json"])["record"]["record"]["fields"] == ["H2AK01158", "-001.50"]
    locator = json.loads(records[0]["source_locator_json"])["field_mapping"]
    assert locator["kind"] == "official-html-dictionary"
    assert locator["source_sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert locator["definitions_collection_id"] == item["collection_id"]
    definitions = json.loads(collections[0]["collection_outcome_json"])["tableFieldDefinitions"]
    assert [field["position"] for field in definitions["fields"]] == [1, 2]
    field = definitions["fields"][1]
    fragment = field["cells"][0]["fragments"][0]
    assert raw[fragment["byte_start"] : fragment["byte_end"]] == b"TTL_RECEIPTS"
    assert relationships == []


@pytest.mark.parametrize("mutation", ["digest", "width", "ambiguous", "embedded_header", "mixed_modes"])
def test_dictionary_mapping_failures_leave_no_output_generation(tmp_path, mutation):
    item = _positional(tmp_path, b"H2AK01158,-001.50\n" if mutation != "width" else b"H2AK01158\n")
    raw = _dictionary_mapping(tmp_path, item)
    mapping = item["field_mapping"]
    if mutation == "digest":
        mapping["dictionary"]["capture"]["byteSize"] += 1
    elif mutation == "ambiguous":
        _dictionary_mapping(tmp_path, item, raw.decode() * 2)
    elif mutation == "embedded_header":
        mapping["data_has_header"] = True
    elif mutation == "mixed_modes":
        mapping["header_collection_id"] = item["collection_id"]
    with pytest.raises(ValueError):
        build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "bad")
    assert not (tmp_path / "bad").exists()
