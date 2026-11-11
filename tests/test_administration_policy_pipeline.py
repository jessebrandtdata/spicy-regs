"""Administration-policy refresh: raw capture, unchanged files keep their observation, evidence-bound generation."""

from datetime import UTC, datetime, timedelta
import hashlib
import json
import shutil

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs import generation_retention
from spicy_regs.pipelines import administration_policy as pipeline
from spicy_regs.source_evidence import INPUT_ROLE
from spicy_regs.sources import publication
from tests.generation_fakes import Store
from tests.published_paths import local_paths

OCTOBER, NOVEMBER = "a" * 40, "b" * 40
ARCHIVE = "https://raw.githubusercontent.com/unitedstates/statements-of-administration-policy"
BIDEN = b"""- congress: 118
  bills: [s2]
  document_title: Statement on S. 2
  date_issued: '2024-03-05'
  url: https://example.test/sap/s2
"""
TRUMP = b"""- congress: 119
  bills: [hr1]
  document_title: Statement on H.R. 1
  date_issued: '2025-05-20'
  url: https://example.test/sap/hr1
"""
TRUMP_LATER = TRUMP + b"""- congress: 119
  bills: [hr4, s9]
  document_title: Statement on H.R. 4
  date_issued: '2025-07-15'
  url: https://example.test/sap/hr4
"""
FILES = {(OCTOBER, "46-Biden"): BIDEN, (OCTOBER, "47-Trump"): TRUMP,
         (NOVEMBER, "46-Biden"): BIDEN, (NOVEMBER, "47-Trump"): TRUMP_LATER}


def archive(requests):
    def serve(request):
        _owner, _repository, commit, _archive, name = request.url.path.strip("/").split("/")
        requests.append((commit, name))
        return httpx.Response(200, content=FILES[commit, name.removesuffix(".yaml")],
                              headers={"content-type": "text/plain"})

    return httpx.MockTransport(serve)


def rows(output, table):
    return pq.read_table(output / "generation" / f"{table}.parquet").to_pylist()


def test_refresh_keeps_unchanged_observations_and_binds_the_generation_to_its_sources(tmp_path):
    requests = []
    first = pipeline.acquire(OCTOBER, ["46-Biden", "47-Trump"], tmp_path / "acquire-1", transport=archive(requests))
    october, _, _ = pipeline.build_from_captures(first, tmp_path / "october")
    store = Store()
    live = publication.publish_generation(
        tmp_path / "october" / "generation", client=store, bucket="fixture", prior_index=publication.empty_index(),
        evidence_directories=tuple((tmp_path / "october").glob("source-evidence/*/artifact")))
    before = {row["administration"]: row["statement_id"] for row in rows(tmp_path / "october", pipeline.STATEMENTS)}

    del requests[:]
    selection = pipeline.acquire(
        NOVEMBER, [], tmp_path / "acquire-2", transport=archive(requests),
        prior_statements=tmp_path / "october" / "generation" / f"{pipeline.STATEMENTS}.parquet")
    assert requests == [(NOVEMBER, "46-Biden.yaml"), (NOVEMBER, "47-Trump.yaml")]
    biden, trump = selection["sources"]
    # The unchanged file keeps its October observation; the changed one is the new commit's.
    assert biden == first["sources"][0] and biden["url"] == f"{ARCHIVE}/{OCTOBER}/archive/46-Biden.yaml"
    assert (trump["commit"], trump["url"]) == (NOVEMBER, f"{ARCHIVE}/{NOVEMBER}/archive/47-Trump.yaml")
    assert (trump["sha256"], trump["bytes"]) == ("sha256:" + hashlib.sha256(TRUMP_LATER).hexdigest(), len(TRUMP_LATER))

    artifact, evidence, built = pipeline.build_from_captures(selection, tmp_path / "november")
    statements = rows(tmp_path / "november", pipeline.STATEMENTS)
    assert [(row["administration"], row["archive_commit"], row["document_title"]) for row in statements] == [
        ("46-Biden", OCTOBER, "Statement on S. 2"),
        ("47-Trump", NOVEMBER, "Statement on H.R. 1"),
        ("47-Trump", NOVEMBER, "Statement on H.R. 4"),
    ]
    assert statements[0]["statement_id"] == before["46-Biden"] != statements[1]["statement_id"]
    assert statements[1]["statement_id"] != before["47-Trump"]
    assert [row["bill_id"] for row in rows(tmp_path / "november", "administration_policy_bill_links")] == [
        "118-s-2", "119-hr-1", "119-hr-4", "119-s-9"]
    assert {table: output["rows"] for table, output in built["outputs"].items()} == {
        "administration_policy_statements": 3, "administration_policy_bill_links": 4}

    # The generation pins its evidence, and the evidence names and holds each selected file under its URL.
    (pinned,) = [item for item in artifact.root["inputs"] if item["role"] == INPUT_ROLE]
    assert pinned["artifactDigest"] == json.loads((evidence / "artifact.json").read_bytes())["artifactDigest"]
    events = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    assert [(event["administration"], event["url"], event["sha256"])
            for event in events if event["event"] == "administration-policy-source"] == [
        ("46-Biden", biden["url"], biden["sha256"]), ("47-Trump", trump["url"], trump["sha256"])]
    assert [(event["original_location"], event["sha256"], event["body_retained"])
            for event in events if event["event"] == "retained-file"] == [
        (biden["url"], biden["sha256"], True), (trump["url"], trump["sha256"], True)]
    assert (evidence / "blobs" / "sha256" / trump["sha256"].removeprefix("sha256:")).read_bytes() == TRUMP_LATER
    assert local_paths(tmp_path / "november" / "generation", evidence, also=(str(tmp_path),)) == []
    index = publication.publish_generation(
        tmp_path / "november" / "generation", client=store, bucket="fixture", prior_index=live,
        evidence_directories=(evidence,))
    assert index["families"]["administration-policy"]["artifactDigest"] == artifact.pin.artifact_digest != (
        october.pin.artifact_digest)


def test_two_captures_of_one_administration_are_refused(tmp_path):
    one = {"sources": [{"administration": "47-Trump", "commit": OCTOBER}]}
    other = {"sources": [{"administration": "47-Trump", "commit": NOVEMBER}]}
    assert pipeline.merge_selections([one, one]) == one
    with pytest.raises(ValueError, match="Two captures were selected for one administration"):
        pipeline.merge_selections([one, other])


def test_a_build_that_read_the_published_index_names_the_generation_it_replaces(tmp_path):
    """Recording the index a hand build read gives the publisher its staleness check and retention its chain."""
    family, transport = "administration-policy", archive([])
    first = pipeline.acquire(OCTOBER, ["46-Biden", "47-Trump"], tmp_path / "acquire-1", transport=transport)
    october, october_evidence, _ = pipeline.build_from_captures(first, tmp_path / "october")
    store = Store()
    live = publication.publish_generation(
        tmp_path / "october" / "generation", client=store, bucket="fixture", prior_index=publication.empty_index(),
        evidence_directories=(october_evidence,))
    # What `spicy-regs mirror` leaves: the generation under its prefix and the pointer that selected it.
    mirror = tmp_path / "mirror"
    held = mirror / live["families"][family]["prefix"]
    shutil.copytree(tmp_path / "october" / "generation", held)
    index = mirror / "publication.v2" / "snapshot.json"
    index.parent.mkdir()
    index.write_bytes(store.objects[publication.INDEX_V2_KEY])

    selection = pipeline.acquire(NOVEMBER, [], tmp_path / "acquire-2", transport=transport,
                                 prior_statements=held / f"{pipeline.STATEMENTS}.parquet")
    november, evidence, _ = pipeline.build_from_captures(selection, tmp_path / "november", read_index=index)
    read = november.root["spec"]["readSnapshot"]
    assert read["families"][family]["artifactDigest"] == october.pin.artifact_digest
    assert sorted(item["role"] for item in november.root["inputs"]) == ["prior-generation", "source-evidence"]
    assert local_paths(tmp_path / "november" / "generation", evidence, also=(str(tmp_path),)) == []

    # The publisher holds the family to the index the build read.
    moved = json.loads(json.dumps(read))
    moved["families"][family]["artifactDigest"] = "sha256:" + "0" * 64
    moved["families"][family]["prefix"] = f"generations/{family}/" + "0" * 64
    stale = Store()
    stale.put_object(Bucket="fixture", Key=publication.INDEX_V2_KEY, Body=json.dumps(moved).encode())
    with pytest.raises(publication.PublicationError, match="Family changed since the build read its inputs"):
        publication.publish_generation(tmp_path / "november" / "generation", client=stale, bucket="fixture",
                                       prior_index=read, evidence_directories=(evidence,))
    publication.publish_generation(tmp_path / "november" / "generation", client=store, bucket="fixture",
                                   prior_index=read, evidence_directories=(evidence,))

    # Retention walks readSnapshot from the current generation, so the replaced one is on the chain and kept.
    plan = generation_retention.plan(store, "fixture", notes=[], docspec={}, now=datetime.now(UTC) + timedelta(days=30))
    reasons = {entry["digest"]: entry["keep"] for entry in plan["families"][family]["generations"]}
    assert reasons == {november.pin.artifact_digest.removeprefix("sha256:"): ["current"],
                       october.pin.artifact_digest.removeprefix("sha256:"): ["last 3: 1 before current"]}
    assert plan["delete"] == []

    # The same build without the index records no predecessor: retention plans the replaced tables for deletion.
    unrecorded, unrecorded_evidence, _ = pipeline.build_from_captures(selection, tmp_path / "unrecorded")
    other = Store()
    before = publication.publish_generation(
        tmp_path / "october" / "generation", client=other, bucket="fixture",
        prior_index=publication.empty_index(), evidence_directories=(october_evidence,))
    publication.publish_generation(tmp_path / "unrecorded" / "generation", client=other, bucket="fixture",
                                   prior_index=before, evidence_directories=(unrecorded_evidence,))
    assert unrecorded.root["spec"]["readSnapshot"] == {}
    plan = generation_retention.plan(other, "fixture", notes=[], docspec={}, now=datetime.now(UTC) + timedelta(days=30))
    assert plan["delete"] == [live["families"][family]["prefix"]]
