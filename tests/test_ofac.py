"""Literal reviewed OFAC excerpts, retained history and ordinary exact-ID lookup."""

from copy import deepcopy
import hashlib
from io import BytesIO
import json
from typing import Any
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr
from zipfile import ZipFile

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import data_dictionary, mcp_server
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import ofac as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_ofac as module
from tests.generation_fakes import Store
from tests.published_paths import local_paths

FIXTURE = json.loads((Path(__file__).parent / "fixtures/ofac/enhanced-excerpts.json").read_text())
EXCERPTS = FIXTURE["samples"]


class Reader:
    """Admitted-reader test double with synthetic pins, not a complete real list."""

    def __init__(self, selected="sdn", *, records=None, observation="2026-10-20T18:00:00Z"):
        records = deepcopy(EXCERPTS[selected]["records"] if records is None else records)
        self.body = json.dumps(records).encode()
        digest = "sha256:" + hashlib.sha256(self.body + observation.encode()).hexdigest()
        self.pin = ArtifactPin("urn:test:ofac:" + digest[7:], digest)
        capture = {
            **{key: records[0]["source_locator"][key] for key in ("request_url", "response_sha256")},
            "response_sha256": "sha256:" + hashlib.sha256(self.body).hexdigest(),
            "observed_at": observation,
            "byte_size": len(self.body),
        }
        self.collection_outcome: dict[str, Any] = {
            "requestedScope": {
                "selected_list": selected, "capture": capture,
                "member_name": records[0]["source_locator"]["member_name"],
            },
            "failedRecordCount": 0, "discardedObservationCount": 0,
            "discoveredRecordCount": len(records), "publishedRecordCount": len(records),
        }
        self.records: list[dict[str, Any]] = []
        for record in records:
            record["source_locator"].update({key: capture[key] for key in ("response_sha256", "observed_at")})
            self.records.append({"sourceRecordId": record["id"], "record": record})
        self.iterations = 0

    def iter_records(self):
        self.iterations += 1
        yield from deepcopy(self.records)


def read(paths):
    return [pq.read_table(path).to_pylist() for path in paths]


def child(node, *path):
    for name in path:
        node = next(item for item in node["content"]
                    if isinstance(item, dict) and item["name"] == module.NAMESPACE + name)
    return node


def test_reviewed_ids_aliases_sparse_addresses_and_metadata(tmp_path):
    readers = [Reader("sdn"), Reader("consolidated")]
    paths = module.build_ofac(readers, tmp_path)
    (sdn, consolidated), snapshots = read(paths)
    assert [reader.iterations for reader in readers] == [1, 1]
    assert [(row["entity_id"], row["identity_id"]) for row in (sdn, consolidated)] == [
        ("36", "4375"), ("9640", "1979"),
    ]
    assert json.loads(sdn["primary_names_json"]) == ["AEROCARIBBEAN AIRLINES"]
    assert json.loads(consolidated["primary_names_json"]) == ["ABU TEIR, Mohammed"]
    assert "AERO-CARIBBEAN" in sdn["names_json"]
    assert "ABOU TAYR, Mohammad Mahmoud" in consolidated["names_json"]
    assert "Non-SDN Palestinian Legislative Council List" in consolidated["list_memberships_json"]
    assert "Reject" in consolidated["sanction_types_json"] and "CUBA" in sdn["programs_json"]
    # The original empty address has primary/script fields but no country or parts.
    address = json.loads(consolidated["addresses_json"])[0]
    assert "country" not in json.dumps(address) and "Latin" in json.dumps(address)
    assert "1951-01-01" in consolidated["features_json"] and "1951-12-31" in consolidated["features_json"]
    assert [row["data_as_of_raw"] for row in snapshots] == [
        "2026-10-15T00:00:00", "2026-10-19T00:00:00-04:00",
    ]
    for name, row, snapshot in zip(("sdn", "consolidated"), (sdn, consolidated), snapshots, strict=True):
        assert json.loads(row["raw_entity_json"]) == EXCERPTS[name]["records"][-1]["element"]
        assert snapshot["entity_count"] == 1 and len(json.loads(snapshot["raw_metadata_json"])) == 3
    assert all(pq.read_schema(path).equals(schema) for path, schema in zip(paths, module.SCHEMAS.values(), strict=True))


def test_unknown_metadata_and_entity_values_survive(tmp_path):
    records = deepcopy(EXCERPTS["sdn"]["records"])
    extension = {"name": "{urn:future}unknown", "attributes": [["code", "0012"]],
                 "namespace_declarations": [["f", "urn:future"]], "path": [1, 4, 1, 99],
                 "content": [" leading ", {"name": "{urn:future}value", "attributes": [],
                                          "namespace_declarations": [], "path": [1, 4, 1, 99, 1],
                                          "content": ["repeat", "repeat"]}]}
    records[-1]["element"]["content"].append(extension)
    metadata = deepcopy(records[1])
    metadata["element"] = deepcopy(extension)
    metadata["kind"] = "extension"
    records.insert(2, metadata)
    rows, snapshots = read(module.build_ofac([Reader(records=records)], tmp_path))
    assert json.loads(rows[0]["raw_entity_json"])["content"][-1] == extension
    assert json.loads(snapshots[0]["raw_metadata_json"])[2]["record"]["element"] == extension


def test_reviewed_document_number_and_typed_relationship_remain_literal(tmp_path):
    records = deepcopy(EXCERPTS["sdn"]["records"][:-1] + FIXTURE["deeper_sdn"]["records"])
    (company, related), _ = read(module.build_ofac([Reader(records=records)], tmp_path))
    assert company["entity_id"] == "906" and company["identity_id"] == "4267"
    document = child(json.loads(company["identifiers_json"])[0], "identityDocument")
    assert child(document, "documentNumber")["content"] == ["01074897"]
    assert child(document, "issuingCountry")["attributes"] == [["refId", "11211"]]
    assert related["entity_id"] == "2686"
    relation = child(json.loads(related["relationships_json"])[0], "relationship")
    assert relation["attributes"] == [["id", "6659"]]
    assert child(relation, "type")["content"] == ["Acting for or on behalf of"]
    assert child(relation, "relatedEntity")["attributes"] == [["entityId", "4697"]]
    assert child(relation, "relatedEntity")["content"] == ["HIZBALLAH"]
    assert [json.loads(row["raw_entity_json"]) for row in (company, related)] == [
        record["element"] for record in FIXTURE["deeper_sdn"]["records"]
    ]


def test_refresh_add_change_and_absence_preserve_prior_observations(tmp_path):
    before = deepcopy(EXCERPTS["sdn"]["records"])
    removed = deepcopy(before[-1])
    removed["entity_id"] = "old"
    removed["element"]["attributes"] = [["id", "old"]]
    before.append(removed)
    after = deepcopy(EXCERPTS["sdn"]["records"])
    name = child(after[-1]["element"], "names", "name", "translations", "translation", "formattedFullName")
    name["content"] = ["CHANGED SOURCE NAME"]
    added = deepcopy(after[-1])
    added["entity_id"] = "new"
    added["element"]["attributes"] = [["id", "new"]]
    after.append(added)
    first = Reader(records=before)
    later = Reader(records=after, observation="2026-10-21T18:00:00Z")
    rows, snapshots = read(module.build_ofac([first, later], tmp_path))
    assert [(row["snapshot_id"], row["entity_id"]) for row in rows] == [
        (first.pin.artifact_digest, "36"), (first.pin.artifact_digest, "old"),
        (later.pin.artifact_digest, "36"), (later.pin.artifact_digest, "new"),
    ]
    assert rows[0]["entity_content_sha256"] != rows[2]["entity_content_sha256"]
    assert len(snapshots) == 2 and "delisted" not in module.ENTITY_SCHEMA.names
    assert json.loads(rows[0]["primary_names_json"]) == ["AEROCARIBBEAN AIRLINES"]


def test_moving_entity_position_does_not_claim_content_change(tmp_path):
    def move(node):
        node["path"] = [9, *node["path"]]
        for item in node["content"]:
            if isinstance(item, dict):
                move(item)

    shifted = deepcopy(EXCERPTS["sdn"]["records"])
    move(shifted[-1]["element"])
    rows, _ = read(module.build_ofac([
        Reader(), Reader(records=shifted, observation="2026-10-21T18:00:00Z"),
    ], tmp_path))
    assert rows[0]["entity_content_sha256"] == rows[1]["entity_content_sha256"]
    assert rows[0]["raw_entity_json"] != rows[1]["raw_entity_json"]


@pytest.mark.parametrize("defect", [
    "failed", "discarded", "short", "duplicate_entity", "capture", "identity", "scalar", "duplicate_release",
])
def test_refusal_does_not_publish_partial_tables(tmp_path, defect):
    reader = Reader()
    readers = [reader]
    if defect == "failed":
        reader.collection_outcome["failedRecordCount"] = 1
    elif defect == "discarded":
        reader.collection_outcome["discardedObservationCount"] = 1
    elif defect == "short":
        reader.records.pop()
    elif defect == "duplicate_entity":
        reader.records.append(deepcopy(reader.records[-1]))
        reader.collection_outcome.update(discoveredRecordCount=5, publishedRecordCount=5)
    elif defect == "capture":
        reader.records[-1]["record"]["source_locator"]["member_name"] = "wrong.xml"
    elif defect == "identity":
        reader.records[-1]["record"]["identity_id"] = "other"
    elif defect == "scalar":
        node = child(reader.records[-1]["record"]["element"], "generalInfo")
        node["content"].append(deepcopy(child(node, "entityType")))
    else:
        readers.append(reader)
    with pytest.raises(ValueError):
        module.build_ofac(readers, tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


def test_stream_failure_and_fresh_output_boundary(tmp_path):
    class Broken(Reader):
        def iter_records(self):
            yield from self.records
            raise OSError("source stream failed")

    with pytest.raises(OSError, match="source stream failed"):
        module.build_ofac([Broken()], tmp_path)
    assert not list(tmp_path.glob("*.parquet"))
    paths = module.build_ofac([Reader()], tmp_path)
    before = [path.read_bytes() for path in paths]
    with pytest.raises(ValueError, match="fresh"):
        module.build_ofac([Reader("consolidated")], tmp_path)
    assert [path.read_bytes() for path in paths] == before


def test_explicit_empty_snapshot_retains_metadata_and_typed_tables(tmp_path):
    records = EXCERPTS["sdn"]["records"][:-1]
    paths = module.build_ofac([Reader(records=records)], tmp_path)
    rows, snapshots = read(paths)
    assert rows == [] and snapshots[0]["entity_count"] == 0
    assert pq.read_schema(paths[0]).equals(module.ENTITY_SCHEMA)


def test_ordinary_publication_metadata_and_exact_identifier_lookup(tmp_path, monkeypatch):
    reader = Reader()
    archive = tmp_path / "fixture.zip"
    archive.write_bytes(reader.body)
    ref = reader.collection_outcome["requestedScope"]["capture"]["response_sha256"]
    output = tmp_path / "build"
    artifact, evidence = pipeline.build([reader], output, archives={ref: archive})
    index = publication.publish_generation(
        output / "generation", client=Store(), bucket="fixture",
        prior_index=publication.empty_index(), evidence_directories=(evidence,),
    )
    assert index["families"]["ofac-sanctions"]["artifactDigest"] == artifact.pin.artifact_digest
    metadata = build_bundle(index)
    for name in module.SCHEMAS:
        entry = metadata["tables"][name]
        assert entry["metadataStatus"] == entry["sourceStatus"] == "documented"
        assert entry["sources"][0]["id"] == "ofac"
        assert name in data_dictionary.expected_schemas()
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        actual = connection.execute("""
            SELECT e.entity_id, e.identity_id, e.primary_names_json, s.data_as_of_raw,
                   json_extract_string(e.source_locator_json, '$.source_xpath')
            FROM ofac_entity_observations e JOIN ofac_snapshots s USING(snapshot_id, selected_list)
            WHERE e.selected_list='sdn' AND e.entity_id='36'
        """).fetchall()
    assert actual == [("36", "4375", '["AEROCARIBBEAN AIRLINES"]', "2026-10-15T00:00:00", "/*[1]/*[4]/*[1]")]


def test_changed_archive_refuses_before_table_or_generation(tmp_path):
    reader = Reader()
    archive = tmp_path / "changed.zip"
    archive.write_bytes(b"!" * len(reader.body))
    ref = reader.collection_outcome["requestedScope"]["capture"]["response_sha256"]
    output = tmp_path / "build"
    with pytest.raises(ValueError, match="digest"):
        pipeline.build([reader], output, archives={ref: archive})
    assert not list(output.glob("*.parquet")) and not (output / "generation").exists()


@pytest.mark.parametrize("blocked_output", [0, 1])
def test_concurrent_output_is_preserved_and_install_can_retry(tmp_path, monkeypatch, blocked_output):
    targets = [tmp_path / f"{table}.parquet" for table in module.SCHEMAS]
    unrelated = tmp_path / "owner-notes"
    unrelated.write_bytes(b"unrelated")
    write_rows = module.write_rows

    def reserve_destination_after_staging(rows, path, schema, **kwargs):
        staged = write_rows(rows, path, schema, **kwargs)
        if path.name == targets[1].name:
            targets[blocked_output].write_bytes(b"foreign")
        return staged

    with monkeypatch.context() as patch:
        patch.setattr(module, "write_rows", reserve_destination_after_staging)
        with pytest.raises(FileExistsError):
            module.build_ofac([Reader()], tmp_path)
    assert targets[blocked_output].read_bytes() == b"foreign"
    assert not targets[1 - blocked_output].exists()
    assert unrelated.read_bytes() == b"unrelated"
    targets[blocked_output].unlink()
    entities, snapshots = read(module.build_ofac([Reader()], tmp_path))
    assert entities and snapshots[0]["entity_count"] == len(entities)
    assert unrelated.read_bytes() == b"unrelated"


def _xml(node):
    """One fixture element as XML in the export's default namespace."""
    name = node["name"].removeprefix(module.NAMESPACE)
    attributes = "".join(f" {key}={quoteattr(value)}" for key, value in node["attributes"])
    body = "".join(escape(item) if isinstance(item, str) else _xml(item) for item in node["content"])
    return f"<{name}{attributes}>{body}</{name}>"


def export_zip(selected="sdn"):
    """A small official-shaped export: the reviewed publication metadata and first entity of one list."""
    _root, metadata, _container, entity = EXCERPTS[selected]["records"]
    document = (f'<sanctionsData xmlns="{module.NAMESPACE[1:-1]}">{_xml(metadata["element"])}'
                f'<referenceValues/><entities>{_xml(entity["element"])}</entities></sanctionsData>')
    body = BytesIO()
    with ZipFile(body, "w") as archive:
        archive.writestr("SDN_ENHANCED.XML" if selected == "sdn" else "CONS_ENHANCED.XML", document)
    return body.getvalue()


def test_acquire_retains_the_export_in_raw_and_builds_from_its_capture(tmp_path, monkeypatch, capsys):
    """The ZIP lands in the raw archive bound to the official URL; the build reads it from there."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

    official = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ENHANCED.ZIP"
    signed = ("https://wc2h-sls-prod-public-published.s3.us-gov-west-1.amazonaws.com/Published/a/2026-11-09/b/"
              "SDN_ENHANCED.ZIP")
    archive = export_zip()
    digest = "sha256:" + hashlib.sha256(archive).hexdigest()
    requests = []

    def serve(request):
        requests.append(str(request.url).split("?")[0])
        if str(request.url) == official:
            return httpx.Response(302, headers={"location": signed + "?X-Amz-Signature=temporarysignature"})
        return httpx.Response(200, content=archive, headers={"content-type": "application/zip"})

    selection = pipeline.acquire(["sdn"], tmp_path / "acquire", transport=httpx.MockTransport(serve))
    assert requests == [official, signed]
    (scope,) = selection["releases"]
    assert scope["selected_list"] == "sdn" and scope["member_name"] == "SDN_ENHANCED.XML"
    assert {key: scope["capture"][key] for key in ("request_url", "response_sha256", "byte_size")} == {
        "request_url": official, "response_sha256": digest, "byte_size": len(archive)}
    raw = tmp_path / "central-raw"  # the archive every test is given
    assert CentralRawArchiveStore(raw, create=False).describe_blob(digest, len(archive))["encoding"] == "raw"
    receipts = [document for _path, document in iter_acquisition_receipts(raw)]
    files = [(row["source_url"], row.get("original_location"), row["sha256"])
             for document in receipts for row in document["files"]]
    # One receipt of the response as the redirect target sent it, one binding the same bytes to the official URL.
    assert sorted(files, key=lambda row: row[1] or "") == [
        (signed + "?X-Amz-Signature=<redacted>", None, digest[7:]), (official, official, digest[7:])]
    assert "temporarysignature" not in json.dumps(receipts)
    assert not list((tmp_path / "acquire").rglob("download-*"))

    captures = tmp_path / "captures.json"
    captures.write_text(json.dumps(selection))
    output = tmp_path / "build"
    monkeypatch.setattr("sys.argv", ["ofac", "build", "--captures", str(captures), "--output-dir", str(output)])
    pipeline.main()
    built = json.loads(capsys.readouterr().out)
    evidence = Path(built["sourceEvidence"])
    (entity,), (snapshot,) = read([output / "generation" / f"{table}.parquet" for table in module.SCHEMAS])
    assert (entity["entity_id"], entity["identity_id"]) == ("36", "4375")
    assert json.loads(entity["primary_names_json"]) == ["AEROCARIBBEAN AIRLINES"]
    assert json.loads(entity["source_locator_json"])["request_url"] == official
    assert (snapshot["data_as_of_raw"], snapshot["entity_count"]) == ("2026-10-15T00:00:00", 1)
    assert snapshot["observed_at"] == scope["capture"]["observed_at"]
    (retained,) = [event for line in (evidence / "journal.jsonl").read_text().splitlines()
                   if (event := json.loads(line))["event"] == "retained-file"]
    assert (retained["original_location"], retained["sha256"]) == (official, digest)
    assert local_paths(output / "generation", evidence, also=(str(tmp_path),)) == []
    index = publication.publish_generation(
        output / "generation", client=Store(), bucket="fixture",
        prior_index=publication.empty_index(), evidence_directories=(evidence,),
    )
    assert index["families"]["ofac-sanctions"]["artifactDigest"] == built["artifactDigest"]
