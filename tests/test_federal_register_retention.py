"""Main's existing evidence owner retains FR observations across runner/store loss."""

import hashlib
import json
import shutil
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest
import yaml
from rulespec_artifacts import LocalMemberSource, admit_artifact, iter_member_descriptors
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_docs.transport import acquisition

from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup
from spicy_regs.source_evidence import JOURNAL, EvidenceMemberSource, SourceEvidenceError, verify_evidence
from spicy_regs.sources import publication as pub
from spicy_regs.transforms.build_federal_register import _live_federal_register_documents
from tests.test_federal_register import _RAW_DOC


def runner(tmp_path, monkeypatch, raw, *, publish=False):
    monkeypatch.setenv("FR_SINCE", date.today().isoformat())

    @contextmanager
    def fetcher(_):
        yield lambda url: raw

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "runner/output/raw-source-originals"))
    return FederalRegisterRollup(output_dir=tmp_path / "runner" / "output", skip_upload=not publish)


def valid_page():
    return json.dumps({"count": 1, "total_pages": 1, "results": [
        dict(_RAW_DOC, publication_date=date.today().isoformat())
    ]}).encode()


@pytest.mark.parametrize("interrupt", [False, True])
def test_real_rollup_publishes_observations_with_safe_retry(tmp_path, monkeypatch, remote, interrupt):
    raw = valid_page()
    pipeline = runner(tmp_path, monkeypatch, raw, publish=True)
    if interrupt:
        def fail_metadata(key):
            if key.startswith("source-evidence/") and key.endswith("members.json"):
                raise OSError("interrupted after original upload")
        remote.before_put = fail_metadata
        with pytest.raises(OSError, match="interrupted"):
            pipeline.run()
        assert pub.INDEX_V2_KEY not in remote.objects
        remote.before_put = None
    else:
        pipeline.run()
    evidence = pipeline.source_evidence
    generation = next((pipeline.output_dir / "generations").iterdir())
    if interrupt:
        pub.publish_generation(generation, client=remote, bucket="spicy-regs", prior_index=pub.empty_index(),
                               evidence_directories=(evidence.artifact_dir,))
    native = verify_generation(generation)
    assert {"role": "source-evidence", **evidence.artifact.pin.as_dict()} in native.root["inputs"]
    digest = hashlib.sha256(raw).hexdigest()
    blob = "blobs/sha256/" + digest
    assert remote.writes.count("source-evidence/" + blob) == 1
    assert remote.objects["source-evidence/" + blob] == raw
    prefix = "source-evidence/" + evidence.artifact.pin.artifact_digest.removeprefix("sha256:")
    blobs = {m.object_key for m in iter_member_descriptors(evidence.artifact, LocalMemberSource(evidence.artifact_dir))
             if m.object_key is not None and m.object_key.startswith("blobs/sha256/")}
    pin = evidence.artifact.pin
    # The only remaining original and receipt copies are in the publication store.
    shutil.rmtree(pipeline.output_dir)
    remote_source = pub._EvidenceMembers(remote, "spicy-regs", prefix, None, blobs, set())
    assert verify_evidence(remote_source, expected_pin=pin).pin == pin
    events = [json.loads(line) for line in remote.objects[prefix + "/" + JOURNAL].splitlines()]
    [retained] = [event for event in events if event.get("event") == "retained-file"]
    assert retained["sha256"] == "sha256:" + digest and retained["byte_size"] == len(raw)
    assert retained["requested_url"].startswith("https://www.federalregister.gov/")
    assert "resolved_url" not in retained and "observed_at" not in retained
    assert all(not event["manifest"]["complete"] for event in events if event.get("event") == "selected-inputs")
    # The current main publisher moves only its maintained v2 pointer.
    assert remote.writes[-1] == pub.INDEX_V2_KEY


@pytest.mark.parametrize("raw", [b"not JSON", b'{"count":"invalid","results":[]}'])
def test_failed_rollup_body_and_journal_survive_workflow_audit_restore(tmp_path, monkeypatch, raw):
    pipeline = runner(tmp_path, monkeypatch, raw)
    with pytest.raises(ValueError):
        pipeline.run()
    evidence = pipeline.source_evidence
    assert not list((pipeline.output_dir / "generations").glob("*"))
    assert evidence.artifact.root["spec"]["outcome"] == "failed"
    relative = evidence.directory.relative_to(pipeline.output_dir)
    packed = shutil.make_archive(str(tmp_path / "audit"), "zip",
                                 root_dir=pipeline.output_dir)
    shutil.rmtree(pipeline.output_dir.parent)
    restored = tmp_path / "restored-audit"
    shutil.unpack_archive(packed, restored)
    source = EvidenceMemberSource(restored / relative / "artifact", archive_root=restored / "raw-source-originals")
    assert admit_artifact(source, expected_pin=evidence.artifact.pin).pin == evidence.artifact.pin
    digest = hashlib.sha256(raw).hexdigest()
    with source.open("blobs/sha256/" + digest) as stream:
        assert stream.read() == raw
    events = [json.loads(line) for line in (restored / relative / "artifact" / JOURNAL).read_bytes().splitlines()]
    assert any(event.get("event") == "retained-file" and event["sha256"] == "sha256:" + digest for event in events)
    assert json.loads((restored / relative / "run-outcome.json").read_bytes())["outcome"] == "failed"


def test_retention_failure_prevents_generation(tmp_path, monkeypatch):
    pipeline = runner(tmp_path, monkeypatch, valid_page())
    def unavailable(*args, **kwargs):
        raise OSError("original storage unavailable")
    monkeypatch.setattr(CentralRawArchiveStore, "put_blob", unavailable)
    with pytest.raises(SourceEvidenceError):
        pipeline.run()
    assert not list((pipeline.output_dir / "generations").glob("*"))
    assert json.loads((pipeline.source_evidence.directory / "run-outcome.json").read_bytes())["outcome"] == "failed"


@pytest.mark.parametrize("valid", [False, True])
def test_standalone_source_path_finishes_its_existing_owner(tmp_path, monkeypatch, valid):
    raw = b'{"count":0,"results":[]}' if valid else b"not JSON"
    pipeline = runner(tmp_path, monkeypatch, raw)
    read = _live_federal_register_documents(date.today(), output_dir=pipeline.output_dir)
    if valid:
        assert list(read) == []
    else:
        with pytest.raises(ValueError):
            list(read)
    outcome = json.loads(next(pipeline.output_dir.rglob("run-outcome.json")).read_bytes())
    assert outcome["outcome"] == ("complete" if valid else "failed")


def test_later_pages_keep_their_window_when_next_page_url_is_not_a_window_request(tmp_path, monkeypatch):
    """Run 41241935207: the API's next_page_url path has no ``.json``, so it does not parse as a window request."""
    today = date.today().isoformat()
    # The shape of a live next_page_url, 2026-11-01.
    next_url = ("https://www.federalregister.gov/api/v1/documents?"
                f"conditions%5Bpublication_date%5D%5Bgte%5D={today}&conditions%5Bpublication_date%5D%5Blte%5D={today}"
                "&format=json&order=newest&page=2&per_page=1000&search_after_cursor=WzE3OTMzMTg0MDAwMDAsIjIwMjYtMjIyNjkiXQ")
    documents = [dict(_RAW_DOC, publication_date=today, document_number=number) for number in ("2026-1", "2026-2")]
    pages = {
        False: {"count": 2, "total_pages": 2, "next_page_url": next_url, "results": documents[:1]},
        True: {"count": 2, "total_pages": 2, "results": documents[1:]},
    }
    pipeline = runner(tmp_path, monkeypatch, b"")

    @contextmanager
    def fetcher(_):
        yield lambda url: json.dumps(pages[url == next_url]).encode()

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    read = _live_federal_register_documents(date.today(), output_dir=pipeline.output_dir)
    assert [document["document_number"] for document in read] == ["2026-1", "2026-2"]


def test_workflow_always_retains_main_evidence_bodies_and_journal():
    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/_rollup.yml").read_text())
    audit = next(step for step in workflow["jobs"]["rollup"]["steps"]
                 if step.get("name") == "Retain invocation and transferred-input audit")
    assert audit["if"] == "always()"
    assert "output/source-evidence/" in audit["with"]["path"].splitlines()
    assert "output/raw-source-originals/" in audit["with"]["path"].splitlines()
