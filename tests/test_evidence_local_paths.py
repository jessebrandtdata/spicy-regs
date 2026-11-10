"""Nothing a publish uploads names the build machine's folders (ss-81bv).

On 2026-11-10 the public bucket served owner home-folder paths in five families' evidence: ``retain_file``
journaled each file's absolute path, and builds journaled their own control files and folders. These tests hold
the journal refusal, the file-name default, the cut copies of control files, and used-input matching on durable
identities. Expectations are written out by hand.
"""

import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import LocalMemberSource

from spicy_regs.generations import build_generation
from spicy_regs.source_catalog.input_proof import input_proof, source_observation
from spicy_regs.source_evidence import (
    CaptureEvidence,
    EvidenceMemberSource,
    SourceEvidenceError,
    local_path_name,
    without_local_paths,
)
from tests.published_paths import machine_paths, published_machine_paths

URL = "https://download.example.gov/disbursements/2016Q1-detail.csv"


def journal(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]


def evidence_in(tmp_path, family="house-disbursements"):
    return CaptureEvidence(tmp_path / "output", family, archive_root=tmp_path / "central-raw")


def test_a_retained_file_is_journaled_by_its_name_not_its_folder(tmp_path):
    evidence = evidence_in(tmp_path)
    path = tmp_path / "work" / "normalized.csv"
    path.parent.mkdir()
    path.write_bytes(b"a,b\n1,2\n")
    receipt = evidence.retain_file(path, stage="house-normalized-input")
    assert (receipt["original_location"], receipt["name"]) == ("normalized.csv", "normalized.csv")
    assert str(tmp_path) not in (evidence.artifact_dir / "journal.jsonl").read_text()
    # What the input proof reads as this file's locations: its name and its retained member, nothing on this machine.
    (event,) = [row for row in journal(evidence) if row["event"] == "retained-file"]
    digest = hashlib.sha256(b"a,b\n1,2\n").hexdigest()
    assert source_observation(event)["locations"] == ["blobs/sha256/" + digest, "normalized.csv"]


@pytest.mark.parametrize("value", [
    "/Users/someone/Work/corpora/receipts/acquisition.json",
    "/home/runner/work/spicy-regs/spicy-regs/output/.builds/a/b.parquet",
    "/tmp/house-record-witness-u4dn8uou/record-body.htm",
    "/private/var/folders/xy/T/source-evidence-1/x.json",
    "/Volumes/External/raw/objects/sha256/a6/a6.csv",
    "file:///Users/someone/Work/a.html",
    "~/Work/corpora/a.json",
    "reviewed copy at /Users/someone/Work/a.json",
])
def test_a_journaled_local_path_is_refused_by_family_event_stage_and_field(tmp_path, value):
    evidence = evidence_in(tmp_path)
    with pytest.raises(SourceEvidenceError) as refused:
        evidence.event("closed-receipt-copy", stage="copy", predecessor_admission={"paths": ["ok", value]})
    assert str(refused.value) == (
        "house-disbursements: closed-receipt-copy(copy).predecessor_admission.paths[] names a local path, and the "
        "journal is published; record a URL, a logical name or a content digest"
    )
    assert [row["event"] for row in journal(evidence)] == ["run"]
    # The run remembers it: a caller that swallowed the error still cannot seal a complete build.
    with pytest.raises(SourceEvidenceError, match="evidence cannot seal as a complete build"):
        evidence.seal(outcome="build-complete")


def test_a_local_path_as_a_key_is_refused(tmp_path):
    evidence = evidence_in(tmp_path)
    with pytest.raises(SourceEvidenceError, match="a key of normalization-receipt.artifacts names a local path"):
        evidence.event("normalization-receipt", artifacts={"/Users/someone/Work/a.csv": {"bytes": 3}})


@pytest.mark.parametrize("value", [
    # Publishers' own text: a House PDF footer, a public comment's attachment link. No build runs on Windows.
    "https://www.house.gov/home/disbursements",
    "s3://congressional-tech-raw/tmp/x.json",
    "generations/house-disbursements/0d6125404263/members.json",
    "blobs/sha256/" + "a" * 64,
    "/bill/title",
    "/members/0/terms/2",
    "urn:spicy:artifact:spicy-regs-rollup-generation:abc",
    "house-2016-q1-detail.csv",
    "G:\\GSDD\\SOD\\72348\\72348.TXT",
    "file:///C:/Users/someone/Documents/comment.pdf",
])
def test_addresses_object_keys_and_locators_are_journaled_unchanged(tmp_path, value):
    evidence = evidence_in(tmp_path)
    evidence.event("selection", stage="house", location=value)
    assert journal(evidence)[-1]["location"] == value


def test_a_diagnostic_keeps_only_a_local_path_s_file_name(tmp_path):
    evidence = evidence_in(tmp_path)
    error = FileNotFoundError(2, "No such file or directory", "/Users/someone/Work/corpora/house/receipt.json")
    evidence.refusal(error, stage="house-read")
    assert journal(evidence)[-1]["message"] == "[Errno 2] No such file or directory: '<local>/receipt.json'"
    evidence.event("unretained", stage="gao", reason="cannot open /tmp/gao-x1/feed.xml, retrying")
    assert journal(evidence)[-1]["reason"] == "cannot open <local>/feed.xml, retrying"
    assert local_path_name("file:///Users/someone/a.csv, https://x.gov/home/a") == "file://<local>/a.csv, https://x.gov/home/a"


def test_a_used_input_located_on_this_machine_is_refused(tmp_path):
    evidence = evidence_in(tmp_path)
    with pytest.raises(SourceEvidenceError, match="house-disbursements: used input location names a local path"):
        evidence.declare_used_inputs([{"location": "/Users/someone/raw/a.csv", "sha256": "sha256:" + "0" * 64,
                                       "bytes": 1, "asset_kind": "csv"}], scope_id="house:used", complete=False)


def test_a_used_original_is_matched_by_its_publisher_url_through_seal_and_input_proof(tmp_path):
    """The used-input guarantee holds on durable keys: bytes, the publisher URL and the native identity."""
    body = b"organization,amount\nOffice,1.00\n"
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    raw = tmp_path / "central-raw-copy.csv"
    raw.write_bytes(body)

    # Accumulated declarations (the ``_finalize_used_inputs`` match) resolve against the URL the retain recorded.
    declared = evidence_in(tmp_path / "declared")
    declared.retain_file(raw, stage="house-csv-original", raw_source=True, original_location=URL, source_id=URL)
    declared.declare_used_inputs([{"location": URL, "sha256": digest, "bytes": len(body), "asset_kind": "csv"}],
                                 scope_id="house:used", complete=False)
    declared.seal(outcome="build-complete")
    (selection,) = [row["manifest"] for row in journal(declared) if row["event"] == "selected-inputs"]
    assert selection["used"] == [{"location": URL, "sha256": digest, "bytes": len(body), "asset_kind": "csv",
                                  "source_id": URL, "source_version": digest, "version_kind": "sha256"}]

    # A direct selection, as the House build makes, passes the input proof on the same keys.
    evidence = evidence_in(tmp_path)
    evidence.retain_file(raw, stage="house-csv-original", raw_source=True, original_location=URL, source_id=URL)
    used = [{"source_id": URL, "source_version": digest, "version_kind": "sha256", "asset_kind": "csv",
             "sha256": digest, "bytes": len(body), "location": URL}]
    with pytest.raises(SourceEvidenceError, match="selected-inputs.manifest.used\\[\\].location names a local path"):
        evidence.selected_inputs({"format": "spicy-selected-inputs", "version": 1, "complete": True,
                                  "scope": {"id": "house-csv:x", "finite": True},
                                  "used": [{**used[0], "location": str(raw)}], "available": [], "expected": []})
    evidence = evidence_in(tmp_path / "direct")
    evidence.retain_file(raw, stage="house-csv-original", raw_source=True, original_location=URL, source_id=URL)
    evidence.selected_inputs({"format": "spicy-selected-inputs", "version": 1, "complete": True,
                              "scope": {"id": "house-csv:x", "finite": True},
                              "used": used, "available": used, "expected": used})
    table = tmp_path / "house_disbursements.parquet"
    pq.write_table(pa.table({"amount": ["1.00"]}), table)
    artifact = build_generation(tmp_path / "generation", family="house-disbursements", files=[table],
                                expected_keys=[table.name], inputs=evidence.inputs())
    receipt, _ = input_proof(artifact, LocalMemberSource(tmp_path / "generation"),
                             ((EvidenceMemberSource(evidence.artifact_dir), evidence.artifact),))
    (generation,) = receipt["generations"]
    assert [row["location"] for row in generation["sources"]] == [URL]
    assert generation["captured_sources"][0]["locations"] == ["blobs/sha256/" + digest[7:], URL]
    assert str(tmp_path) not in json.dumps(receipt)


def test_derived_json_naming_a_local_path_is_refused_and_its_cut_copy_retained(tmp_path):
    evidence = evidence_in(tmp_path)
    control = tmp_path / "manifest.json"
    raw = json.dumps({"acquisition": "/Users/someone/Work/receipts/acquisition.json",
                      "acquisition_sha256": "sha256:" + "1" * 64, "url": URL}).encode()
    control.write_bytes(raw)
    with pytest.raises(SourceEvidenceError, match=r"input manifest.json\(house-manifest\) names a local path"):
        evidence.retain_file(control, stage="house-manifest")
    evidence = evidence_in(tmp_path / "bytes")
    with pytest.raises(SourceEvidenceError, match=r"derived JSON input \(profile-selection\) names a local path"):
        evidence.retain_bytes(raw, stage="profile-selection")
    # Publisher bytes are the publisher's: a public comment may quote a Windows or Mac path.
    evidence = evidence_in(tmp_path / "publisher")
    evidence.retain_bytes(b'{"comment": "see file:///C:/Users/someone/a.pdf"}', stage="comment", raw_source=True)

    evidence = evidence_in(tmp_path / "cut")
    receipt = evidence.retain_control(control, stage="house-manifest")
    assert (receipt["original_location"], receipt["local_sha256"]) == (
        "manifest.json", "sha256:" + hashlib.sha256(raw).hexdigest())
    member = evidence.artifact_dir / receipt["blob_member"]
    assert member.read_bytes() == (b'{"acquisition":"<local>/acquisition.json","acquisition_sha256":"sha256:'
                                   + b"1" * 64 + b'","url":"' + URL.encode() + b'"}')


def test_a_local_path_split_by_a_read_chunk_is_found(tmp_path):
    """The retained-file scan reads 1 MiB chunks; a root that straddles two is still seen."""
    control = tmp_path / "selection.json"
    head = b'{"a":"' + b"x" * (1024 * 1024 - 3 - 6 - 7) + b'","b":"'
    control.write_bytes(head + b'/Users/someone/Work/a.json"}')
    assert len(head) == 1024 * 1024 - 3
    with pytest.raises(SourceEvidenceError, match="names a local path"):
        evidence_in(tmp_path).retain_file(control, stage="selection")


def test_two_paths_with_one_file_name_cannot_be_cut_apart():
    with pytest.raises(ValueError, match="cannot be told apart"):
        without_local_paths({"/Users/someone/a/x.csv": 1, "/Users/someone/b/x.csv": 2})
    cut = without_local_paths({"/tmp/a/x.csv": ["/tmp/a/y.csv", 3, URL]})
    assert cut == {"<local>/x.csv": ["<local>/y.csv", 3, URL]}


def test_the_publication_guard_reads_every_published_kind_and_only_those(tmp_path_factory, monkeypatch):
    """The suite-wide guard (conftest) is not blind: a machine folder in each published file kind is found."""
    root = tmp_path_factory.mktemp("guard")
    folders = machine_paths(root)
    leak = str(root / "work" / "a.json")
    artifact = root / "output" / "source-evidence" / "run" / "artifact"
    (artifact / "blobs" / "sha256").mkdir(parents=True)
    (artifact / "journal.jsonl").write_text(json.dumps({"event": "retained-file", "original_location": leak}) + "\n")
    (artifact / "blobs" / "sha256" / ("b" * 64)).write_text(json.dumps({"selection": leak}))
    generation = root / "output" / "generation"
    generation.mkdir()
    (generation / "artifact.json").write_text("{}")
    (generation / "members.json").write_text("{}")
    pq.write_table(pa.table({"id": [1]}).replace_schema_metadata({"source": leak}), generation / "t.parquet")
    pq.write_table(pa.table({"witness": [leak]}), generation / "etl_receipts.parquet")
    proof = root / "output" / "generation-inputs" / "family" / "digest"
    proof.mkdir(parents=True)
    (proof / "inputs.json").write_text(json.dumps({"locations": [leak]}))
    # Local-only records are not published and are not read: a raw archive receipt and a run outcome.
    (root / "central-raw").mkdir()
    (root / "central-raw" / "receipt.json").write_text(json.dumps({"path": leak}))
    (artifact.parent / "run-outcome.json").write_text(json.dumps({"path": leak}))
    found = published_machine_paths(root, folders)
    assert sorted({name for name, _ in found}) == [
        "output/generation-inputs/family/digest/inputs.json",
        "output/generation/t.parquet",
        "output/source-evidence/run/artifact/blobs/sha256/" + "b" * 64,
        "output/source-evidence/run/artifact/journal.jsonl",
    ]
    # Receipt rows are read once the receipts stop naming build folders (published_paths.WHOLE_RECEIPTS).
    monkeypatch.setattr("tests.published_paths.WHOLE_RECEIPTS", 8 * 1024 * 1024)
    assert "output/generation/etl_receipts.parquet" in {name for name, _ in published_machine_paths(root, folders)}
