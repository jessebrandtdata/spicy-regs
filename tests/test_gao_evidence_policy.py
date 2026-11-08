"""GAO product-page bodies stay local through reads, refusals and evidence publication."""

import hashlib
import json
from types import SimpleNamespace

import pytest
from spicy_docs.sources.gao import major_rule_old_index
from spicy_docs.sources.gao.product_details import GaoProductDetailsError, GaoProductPageUnavailableError

from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import gao_listing, publication
from tests.generation_fakes import Store
from tests.test_gao_product_pages import Pages, _build, _capture, _page, _row

CONTACT = b"Named official: (202) 512-1234 official@gao.gov"


def _published(evidence):
    artifact = evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)
    store = Store()
    publication.publish_evidence(store, "bucket", evidence.artifact_dir, artifact)
    assert all(CONTACT not in body for body in store.objects.values())
    assert not any("source-evidence/blobs/sha256/" + hashlib.sha256(CONTACT).hexdigest() in key
                   for key in store.objects)
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


def test_old_index_keeps_listing_body_and_references_product_body_without_changing_local_capture(tmp_path, monkeypatch):
    local = tmp_path / "original.html"
    local.write_bytes(CONTACT)
    captures = [_capture("index", b"listing"), _capture("ogc-96-9", local.read_bytes())]
    index = SimpleNamespace(complete=True, pages=[SimpleNamespace(capture=c) for c in captures], reports=())
    monkeypatch.setattr(major_rule_old_index, "read_old_index_run", lambda *_: index)
    evidence = CaptureEvidence(tmp_path, "gao-reports")
    _, stated, _ = gao_listing._major_rule_reports((), None, tmp_path, evidence)
    assert stated.pages == {captures[1].requested_url: CONTACT}
    events = _published(evidence)
    index_event, page_event = [event for event in events if event["event"] == "capture"]
    assert index_event["body_retained"] is True
    assert page_event["body_retained"] is False and page_event["evidence_policy"] == "hash_only"
    assert page_event["sha256"] == captures[1].sha256 and page_event["byte_size"] == len(CONTACT)
    assert page_event["requested_url"] == captures[1].requested_url
    assert local.read_bytes() == CONTACT


@pytest.mark.parametrize("outcome", ["read", "refused", "unavailable", "known-refused", "acquisition"])
def test_enabled_product_pass_never_publishes_contact_bodies_or_refusal_messages(tmp_path, monkeypatch, outcome):
    capture = _capture("gao-26-1", CONTACT)
    known = None
    if outcome == "read":
        answer = _page("gao-26-1", 4, body=CONTACT)
    else:
        if outcome == "unavailable":
            answer = GaoProductPageUnavailableError(capture)
        else:
            answer = GaoProductDetailsError("Named official: official@gao.gov",
                                           "acquisition" if outcome == "acquisition" else "unread-section")
            setattr(answer, "capture", capture)
        if outcome == "known-refused":
            known = {"gao-04-49": answer}
    evidence = CaptureEvidence(tmp_path, "gao-reports")
    rows = _build(tmp_path, monkeypatch, [_row("gao-26-1")],
                  Pages({"gao-26-1": answer}, known=known), evidence=evidence, product_pages=True)
    assert rows["gao-26-1"]["recommendation_count"] == (4 if outcome == "read" else None)
    events = _published(evidence)
    captures = [event for event in events if event["event"] == "capture"]
    assert captures and all(event["evidence_policy"] == "hash_only" and not event["body_retained"]
                            for event in captures)
    assert any(event["sha256"] == capture.sha256 for event in captures)
    assert "official@gao.gov" not in json.dumps(events)


def test_factory_failure_root_finish_withholds_contact_capture_and_message(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from tests.test_gao_product_pages import module

    capture = _capture("gao-26-1", CONTACT)
    error = GaoProductDetailsError("Named official: official@gao.gov")
    setattr(error, "capture", capture)

    @contextmanager
    def failing_factory(evidence):
        raise error
        yield

    monkeypatch.setattr(module.gao_product_pages, "page_acquirer", failing_factory)
    evidence = CaptureEvidence(tmp_path, "gao-reports")
    with pytest.raises(GaoProductDetailsError):
        _build(tmp_path, monkeypatch, [_row("gao-26-1")], evidence=evidence, product_pages=True)
    evidence.finish(error)
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    assert "official@gao.gov" not in json.dumps(events)
    assert not any(CONTACT in path.read_bytes() for path in evidence.artifact_dir.rglob("*") if path.is_file())
