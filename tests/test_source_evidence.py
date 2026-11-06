"""Offline source captures, failure preservation and publication admission."""

from dataclasses import replace
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactInput, ArtifactVerificationError, admit_artifact
from spicy_docs.reading.refusals import RefusedResponse, attach_refused_response
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
from spicy_docs.sources.legislators import LegislatorsBudget, LegislatorsSourceError
from spicy_docs.transport.captured import CapturedBodyResponse, attached_capture
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.source_evidence import CaptureEvidence, EvidenceMemberSource, SourceEvidenceError, verify_evidence
from spicy_regs.sources import publication as pub
from spicy_regs.sources.retained import (
    RetainedGovInfoBodyAcquirer,
    RetainedLegislatorsAcquirer,
    RetainedGovInfoDiscoveryReader,
    RetainedCongressListingReader,
)
from spicy_regs.transforms.build_members import build_members
from spicy_regs.transforms.build_committee_reports import build_committee_reports, _event_id
from tests.generation_fakes import Store
from tests.test_committee_reports import BUDGET, CRPT_ID, CHRG_ID, FIXTURES, StubBodyAcquirer, StubHearings, _package
from tests.test_generation_publication import build

NOW = datetime(2026, 9, 19, tzinfo=UTC)
KEY = "test-credential-0123456789"


def capture(body=b"exact\r\nbytes\x00", **kwargs):
    return CapturedBodyResponse(
        "https://source.test/body",
        "https://source.test/body",
        200,
        "application/octet-stream",
        "2026-09-19T00:00:00Z",
        body,
        **kwargs,
    )


def journal(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]


def read_evidence_member(evidence, key):
    with EvidenceMemberSource(evidence.artifact_dir).open(key) as stream:
        return stream.read()


def bodies(evidence):
    source = EvidenceMemberSource(evidence.artifact_dir)
    result = []
    for key in source.keys():
        if key.startswith("blobs/sha256/"):
            with source.open(key) as stream:
                result.append(stream.read())
    return result


def test_gao_rss_contact_body_is_hash_only_while_source_values_survive(tmp_path):
    from spicy_regs.sources.gao_reports import GaoReportsReader, RSS_URL
    from tests.test_reference_source_failures import FEED

    raw = FEED.replace(b"Native text", b"Native text contact@example.test")
    evidence = CaptureEvidence(tmp_path, "gao-reports")
    reader = GaoReportsReader(evidence=evidence, transport=httpx.MockTransport(
        lambda _: httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": "application/rss+xml"})))
    [item] = list(reader.iter_records())
    assert item["description"] == "Native text contact@example.test"
    [event] = [e for e in journal(evidence) if e.get("event") == "capture"]
    assert event["requested_url"] == RSS_URL
    assert event["sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert event["byte_size"] == len(raw)
    assert event["evidence_policy"] == "hash_only" and event["body_retained"] is False
    assert "blob_member" not in event
    assert not (evidence.artifact_dir / "blobs" / "sha256" / hashlib.sha256(raw).hexdigest()).exists()
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin).pin == artifact.pin


def test_exact_capture_metadata_and_request_bytes_are_admitted(tmp_path):
    evidence = CaptureEvidence(tmp_path, "test")
    raw = capture(request_body=b'{"selection": 1}')
    evidence.capture(raw, stage="used")
    event = journal(evidence)[-1]
    assert event["sha256"] == raw.sha256
    assert event["byte_size"] == len(raw.body)
    assert event["observed_at"] == raw.observed_at
    assert event["recorded_at"] != raw.observed_at
    assert event["content_encoding"] == "identity"
    assert event["request_body"]["byte_size"] == len(raw.request_body)
    assert set(bodies(evidence)) == {raw.body, raw.request_body}
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin).pin == artifact.pin
    with pytest.raises(SourceEvidenceError, match="sealed"):
        evidence.capture(capture(b"late new bytes"), stage="late")
    assert b"late new bytes" not in bodies(evidence)
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)


@pytest.mark.parametrize("kind", ["echo", "request-echo", "401", "403", "refused"])
def test_credential_refused_bytes_never_enter_store_or_journal(tmp_path, kind):
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.credential = KEY
    response = replace(capture(), body=KEY.encode() if kind == "echo" else b"secret refusal")
    if kind == "request-echo":
        response = replace(response, request_body=KEY.encode())
    if kind in ("401", "403"):
        response = replace(response, status_code=int(kind))
    if kind == "refused":
        error = CredentialRefusedError("api_key=" + KEY)
        attach_refused_response(error, RefusedResponse("api_key=" + KEY, "http", b"secret refusal", "text/plain"))
        evidence.refusal(error, stage="capture")
    else:
        with pytest.raises(CredentialRefusedError) as raised:
            evidence.capture(response, stage="capture")
        refusal = journal(evidence)[-1]
        assert refusal["reason_code"] == ("access_refused" if kind in ("401", "403") else "credential_echo")
        assert refusal["status_code"] == response.status_code
        assert refusal["body_retained"] is False and refusal["response"] is None
        assert attached_capture(raised.value) is None
        evidence.refusal(raised.value, stage="capture")
    evidence.finish(CredentialRefusedError(KEY))
    assert bodies(evidence) == []
    for path in evidence.directory.rglob("*"):
        if path.is_file():
            assert KEY.encode() not in path.read_bytes()
            assert b"secret refusal" not in path.read_bytes()


@pytest.mark.parametrize("status", [401, 403])
def test_access_refusal_keeps_exact_status_and_source_binding_without_credentials(tmp_path, status):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    source = evidence.for_source("lcv", "hash_only", parser_version="test/1", policy_decision_id="test-policy")
    response = replace(
        capture(b"private-response-marker", request_body=b"private-request-marker"),
        status_code=status,
        requested_url="https://source.test/private-url-marker",
        resolved_url="https://source.test/private-url-marker",
    )
    with pytest.raises(CredentialRefusedError, match=f"Source access refused \\(HTTP {status}\\)") as raised:
        source.capture(response, stage="listing")
    assert attached_capture(raised.value) is None
    refusal = journal(evidence)[-1]
    assert refusal == {
        "event": "refusal",
        "recorded_at": refusal["recorded_at"],
        "stage": "listing",
        "error_type": "CredentialRefusedError",
        "reason_code": "access_refused",
        "status_code": status,
        "body_retained": False,
        "credential_refused": True,
        "message": None,
        "response": None,
        "publisher_id": "lcv",
        "evidence_policy": "hash_only",
        "parser_version": "test/1",
        "policy_decision_id": "test-policy",
    }
    evidence.finish(raised.value)
    assert admit_artifact(EvidenceMemberSource(evidence.artifact_dir)).root["spec"]["outcome"] == "failed"
    assert refusal in journal(evidence)
    assert bodies(evidence) == []
    for path in evidence.directory.rglob("*"):
        if path.is_file():
            assert b"private-" not in path.read_bytes()


def test_public_token_like_body_is_not_a_credential_echo(tmp_path):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    evidence.credential = KEY
    source = evidence.for_source("lcv", "hash_only", parser_version="test/1", policy_decision_id="test-policy")
    raw = capture(b'<script>window.publicConfig = {"api_key": "public-bootstrap-value"};</script>')
    receipt = source.capture(raw, stage="listing")
    assert receipt["sha256"] == raw.sha256
    assert receipt["body_retained"] is False and bodies(evidence) == []
    assert not any(row["event"] == "refusal" for row in journal(evidence))
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin).pin == artifact.pin


def test_owner_refusal_bytes_and_scrubbed_context_are_retained(tmp_path):
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.credential = KEY
    error = ValueError("bad page https://source.test/?api_key=" + KEY)
    attach_refused_response(
        error, RefusedResponse("https://source.test/?api_key=" + KEY, "parse", b"not JSON", "text/plain")
    )
    evidence.refusal(error, stage="listing")
    event = journal(evidence)[-1]
    assert event["response"]["sha256"] == "sha256:" + hashlib.sha256(b"not JSON").hexdigest()
    assert bodies(evidence) == [b"not JSON"]
    assert KEY not in json.dumps(event)


def roster(lis=True):
    return json.dumps(
        [
            {
                "id": {"bioguide": "A000001", **({"lis": "S001"} if lis else {})},
                "name": {"first": "Example", "last": "Person"},
                "terms": [{"type": "sen", "start": "2025-01-03", "state": "VT", "class": 1}],
            }
        ]
    ).encode()


def legislators(evidence, raw):
    return RetainedLegislatorsAcquirer(
        evidence=evidence,
        budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0),
        clock=lambda: NOW,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=httpx.ByteStream(raw), headers={"content-type": "application/json"}
            )
        ),
    )


def test_no_lis_postcondition_preserves_original_and_no_generation(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    raw = roster(lis=False)

    class Members(RollupPipeline):
        name = "members"
        outputs = ("members.parquet", "member_terms.parquet")
        retain_source_evidence = True

        def build(self, output_dir):
            with legislators(self.source_evidence, raw) as owner:
                return build_members(output_dir, acquirer=owner, evidence=self.source_evidence)

    pipeline = Members(output_dir=tmp_path)
    with pytest.raises(LegislatorsSourceError, match="no id.lis"):
        pipeline.run()
    evidence = pipeline.source_evidence
    assert evidence is not None
    assert raw in bodies(evidence)
    assert not (tmp_path / "generations").exists()
    assert any(row["event"] == "refusal" and "no id.lis" in row["message"] for row in journal(evidence))
    assert json.loads((evidence.directory / "run-outcome.json").read_bytes())["outcome"] == "failed"
    assert admit_artifact(EvidenceMemberSource(evidence.artifact_dir)).root["spec"]["outcome"] == "failed"


def test_both_roster_originals_reach_member_and_term_outputs(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    evidence = CaptureEvidence(tmp_path, "members")
    current_record = json.loads(roster())[0]
    current_record["id"].update(votesmart=123456, bioguide_previous=["Z000001"])
    current_record["other_names"] = [{"last": "Previous", "middle": None, "end": "2024-12-31"}]
    current = json.dumps([current_record]).encode()
    historical = roster(lis=False).replace(b"A000001", b"B000001")

    def respond(request):
        raw = current if "current" in request.url.path else historical
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": "application/json"})

    with RetainedLegislatorsAcquirer(
        evidence=evidence,
        budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0),
        clock=lambda: NOW,
        transport=httpx.MockTransport(respond),
    ) as owner:
        members, terms, affiliations = build_members(tmp_path, acquirer=owner, evidence=evidence)
    member_rows = pq.read_table(members).to_pylist()
    term_rows = pq.read_table(terms).to_pylist()
    assert {row["bioguide_id"] for row in member_rows} == {"A000001", "B000001"}
    assert {row["roster"] for row in member_rows} == {"current", "historical"}
    by_id = {row["bioguide_id"]: row for row in member_rows}
    assert by_id["A000001"]["votesmart_id"] == "123456"
    assert json.loads(by_id["A000001"]["bioguide_previous_json"]) == ["Z000001"]
    assert json.loads(by_id["A000001"]["other_names_json"]) == current_record["other_names"]
    assert all(by_id["B000001"][field] is None for field in
               ("votesmart_id", "bioguide_previous_json", "other_names_json"))
    assert len(term_rows) == 2
    assert all(row["observed_at"] == "2026-09-19T00:00:00Z" for row in member_rows + term_rows)
    assert set(bodies(evidence)) == {current, historical}


def govinfo(evidence, *, malformed_mods=False, credential_echo=False):
    def respond(request):
        if request.url.path.endswith("/summary"):
            raw = (FIXTURES / f"summary-{CRPT_ID}.json").read_bytes()
            kind = "application/json"
        elif request.url.path.endswith("/mods"):
            raw = b"not XML" if malformed_mods else (FIXTURES / f"mods-{CRPT_ID}.xml").read_bytes()
            kind = "application/xml"
        else:
            raw = KEY.encode() if credential_echo else (FIXTURES / f"body-{CRPT_ID}.htm").read_bytes()
            kind = "text/html"
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": kind})

    return RetainedGovInfoBodyAcquirer(
        evidence=evidence, api_key=KEY, budget=BUDGET, transport=httpx.MockTransport(respond), clock=lambda: NOW
    )


def test_later_mods_failure_keeps_successful_summary_and_exact_failure(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    with govinfo(evidence, malformed_mods=True) as owner:
        with pytest.raises(ValueError) as raised:
            owner.acquire(CRPT_ID)
    evidence.refusal(raised.value, stage=CRPT_ID)
    assert (FIXTURES / f"summary-{CRPT_ID}.json").read_bytes() in bodies(evidence)
    assert b"not XML" in bodies(evidence)
    assert all(row["observed_at"] == "2026-09-19T00:00:00Z" for row in journal(evidence) if row["event"] == "capture")


def test_keyless_body_echo_after_safe_metadata_is_not_retained(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    with govinfo(evidence, credential_echo=True) as owner:
        with pytest.raises(CredentialRefusedError) as raised:
            owner.acquire(CRPT_ID)
    evidence.refusal(raised.value, stage=CRPT_ID)
    assert len(bodies(evidence)) == 2  # safe summary and MODS preceded refused body
    assert all(KEY.encode() not in raw for raw in bodies(evidence))


class Discovery:
    def packages(self, url, *, max_pages=1):
        ids = [CRPT_ID] if "/CRPT/" in url else [CHRG_ID]
        yield SimpleNamespace(
            records=[{"packageId": key, "lastModified": "2026-09-18T12:00:00Z"} for key in ids],
            capture=capture(json.dumps(ids).encode()),
        )


class Hearings(StubHearings):
    def records(self, route, url, *, max_pages=1):
        for page in super().records(route, url, max_pages=max_pages):
            page.capture = capture(json.dumps(page.records).encode())
            yield page


def test_report_run_retains_used_listing_package_and_detail_then_unchanged_lineage(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    outputs = build_committee_reports(
        tmp_path,
        reader=Discovery(),
        acquirer=StubBodyAcquirer(),
        hearings=Hearings(),
        download_prior=lambda *args: False,
        evidence=evidence,
    )
    events = journal(evidence)
    assert len(outputs) == 6
    assert {row["stage"] for row in events if row["event"] == "capture"} >= {
        "CRPT:listing",
        "CHRG:listing",
        CRPT_ID + ":used",
        CHRG_ID + ":used",
        CHRG_ID + ":hearing-detail",
    }
    assert {row["package_id"] for row in events if row["event"] == "package-outcome"} == {CRPT_ID, CHRG_ID}
    resumed = CaptureEvidence(tmp_path, "committee-reports")
    owner = StubBodyAcquirer()
    previous = {path.name: path.read_bytes() for path in outputs}

    def download(key, path):
        path.write_bytes(previous[key])
        return True

    build_committee_reports(
        tmp_path, reader=Discovery(), acquirer=owner, hearings=Hearings(), download_prior=download, evidence=resumed
    )
    assert owner.requested == []
    assert {key for row in journal(resumed) if row["event"] == "package-selection" for key in row["unchanged"]} == {
        CRPT_ID,
        CHRG_ID,
    }


def test_capped_selection_and_detail_refusal_are_distinct(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    owner = StubBodyAcquirer()
    build_committee_reports(
        tmp_path,
        reader=Discovery(),
        acquirer=owner,
        hearings=Hearings(),
        max_packages=0,
        download_prior=lambda *args: False,
        evidence=evidence,
    )
    assert owner.requested == []
    assert {key for row in journal(evidence) if row["event"] == "package-selection" for key in row["deferred"]} == {
        CRPT_ID,
        CHRG_ID,
    }

    class WrongDetail:
        def records(self, *args, **kwargs):
            yield SimpleNamespace(records=[], capture=capture(b'{"hearings": []}'))

    assert _event_id(WrongDetail(), _package(CHRG_ID), evidence) == (None, "refused")
    assert b'{"hearings": []}' in bodies(evidence)


def test_storage_failure_aborts_report_build(tmp_path, monkeypatch):
    evidence = CaptureEvidence(tmp_path, "committee-reports")

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(evidence.store, "put_blob", fail)
    with pytest.raises(SourceEvidenceError, match="response bytes"):
        build_committee_reports(
            tmp_path,
            reader=Discovery(),
            acquirer=StubBodyAcquirer(),
            hearings=Hearings(),
            download_prior=lambda *args: False,
            evidence=evidence,
        )
    assert not list(tmp_path.glob("*.parquet"))


def test_listing_failure_after_a_page_retains_both_and_builds_nothing(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")

    class BrokenDiscovery(Discovery):
        def packages(self, url, *, max_pages=1):
            yield from super().packages(url, max_pages=max_pages)
            error = ValueError("malformed next page")
            attach_refused_response(error, RefusedResponse(url, "decode", b"bad page two", "application/json"))
            raise error

    with pytest.raises(ValueError, match="next page"):
        build_committee_reports(
            tmp_path,
            reader=BrokenDiscovery(),
            acquirer=StubBodyAcquirer(),
            hearings=Hearings(),
            download_prior=lambda *args: False,
            evidence=evidence,
        )
    assert json.dumps([CRPT_ID]).encode() in bodies(evidence)
    assert b"bad page two" in bodies(evidence)
    assert not list(tmp_path.glob("*.parquet"))


def test_successful_owner_package_retains_all_originals_and_native_output(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")

    class ReportsOnly(Discovery):
        def packages(self, url, *, max_pages=1):
            if "/CRPT/" in url:
                yield from super().packages(url, max_pages=max_pages)
            else:
                yield SimpleNamespace(records=[], capture=capture(b'{"count":0,"packages":[]}'))

    with govinfo(evidence) as owner:
        outputs = build_committee_reports(
            tmp_path,
            reader=ReportsOnly(),
            acquirer=owner,
            hearings=Hearings(),
            download_prior=lambda *args: False,
            evidence=evidence,
        )
    report = pq.read_table(outputs[0]).to_pylist()[0]
    original_body = (FIXTURES / f"body-{CRPT_ID}.htm").read_bytes()
    assert report["sha256"] == "sha256:" + hashlib.sha256(original_body).hexdigest()
    assert report["observed_at"] == "2026-09-19T00:00:00Z"
    assert report["package_id"] == CRPT_ID
    assert report["bill_id"] == "119-hres-53"
    for name in (f"summary-{CRPT_ID}.json", f"mods-{CRPT_ID}.xml", f"body-{CRPT_ID}.htm"):
        assert (FIXTURES / name).read_bytes() in bodies(evidence)


@pytest.mark.parametrize("change", ["none", "root", "oversize"])
def test_prior_root_is_bounded_and_matches_captured_pin(tmp_path, monkeypatch, change):
    directory, artifact = build(tmp_path)
    entry = {"prefix": "generations/test/pin", **artifact.pin.as_dict()}
    raw = (directory / "artifact.json").read_bytes()
    if change == "root":
        root = json.loads(raw)
        root["inputs"] = [{"role": "invented"}]
        raw = json.dumps(root).encode()
    elif change == "oversize":
        raw = b"x" * (pub.INDEX_LIMIT + 1)

    @contextmanager
    def stream(*args, **kwargs):
        yield httpx.Response(200, content=raw, request=httpx.Request("GET", "https://test.invalid"))

    monkeypatch.setattr(pub.httpx, "stream", stream)
    if change == "none":
        assert pub.load_family_root("https://test.invalid", entry) == (raw, artifact.root)
    else:
        with pytest.raises(pub.PublicationError):
            pub.load_family_root("https://test.invalid", entry)


def candidate(tmp_path, evidence, prior=None):
    path = tmp_path / "a.parquet"
    pq.write_table(pa.table({"id": ["one"]}), path)
    directory = tmp_path / "generation"
    artifact = build_generation(
        directory, family="test", files=[path], expected_keys=[path.name], read_snapshot=prior, inputs=evidence.inputs()
    )
    return directory, artifact


@pytest.mark.parametrize("failure", ["missing", "corrupt", "upload"])
def test_missing_corrupt_or_failed_evidence_never_advances_pointer(tmp_path, failure):
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.capture(capture(), stage="used")
    directory, _ = candidate(tmp_path, evidence)
    store = Store()
    if failure == "corrupt":
        store.corrupt_key = "journal.jsonl"
    if failure == "upload":

        def fail(key):
            if key.startswith("source-evidence/"):
                raise OSError("upload failed")

        store.before_put = fail
    with pytest.raises((pub.PublicationError, ArtifactVerificationError, OSError)):
        pub.publish_generation(
            directory,
            client=store,
            bucket="test",
            prior_index=pub.empty_index(),
            evidence_directories=() if failure == "missing" else (evidence.artifact_dir,),
        )
    assert pub.INDEX_V2_KEY not in store.objects
    assert not any(key.startswith("generations/") for key in store.writes)


@pytest.mark.parametrize("input_count", [0, 2])
def test_managed_prior_names_at_most_one_source_evidence_input(tmp_path, monkeypatch, input_count):
    old, old_artifact = build(tmp_path, keys=("a.parquet",))
    root = {**old_artifact.root, "inputs": [
        {"role": "source-evidence", "logicalId": "urn:fixture:old", "artifactDigest": "sha256:" + "0" * 64}
        for _ in range(input_count)
    ]}
    monkeypatch.setattr(pub, "load_family_root", lambda *_: ((old / "artifact.json").read_bytes(), root))
    prior = {"families": {"test": old_artifact.pin.as_dict()}}
    evidence = CaptureEvidence(tmp_path, "test")
    if input_count:
        with pytest.raises(SourceEvidenceError, match="more than one"):
            evidence.inherit(prior, public_url="https://test.invalid")
        assert not any(row["event"] == "lineage" for row in journal(evidence))
        with pytest.raises(SourceEvidenceError, match="retention failed"):
            evidence.seal(outcome="build-complete")
        return

    def unread(*_):
        raise AssertionError("a prior that names no source evidence has no journal to read")

    monkeypatch.setattr(pub, "load_evidence_journal", unread)
    evidence.inherit(prior, public_url="https://test.invalid")
    lineage = next(row for row in journal(evidence) if row["event"] == "lineage")
    assert lineage["evidence_status"].startswith("Prior generation names no source evidence")
    assert evidence.inherited_event("lineage") is None
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin).pin == artifact.pin


@pytest.mark.parametrize("entry_point", ["pinned_root", "admitted"])
def test_offline_prior_inheritance_names_at_most_one_source_evidence_input(tmp_path, monkeypatch, entry_point):
    """The at-most-one rule covers the offline entry points, and the refusal is sticky."""
    old, old_artifact = build(tmp_path, keys=("a.parquet",))
    pins = [
        {"role": "source-evidence", "logicalId": f"urn:fixture:old{n}", "artifactDigest": "sha256:" + str(n) * 64}
        for n in (0, 1)
    ]
    root = {**old_artifact.root, "inputs": pins}
    monkeypatch.setattr(pub, "family_root", lambda *_: root)
    prior = {"families": {"test": old_artifact.pin.as_dict()}}
    evidence = CaptureEvidence(tmp_path, "test")
    with pytest.raises(SourceEvidenceError, match="more than one"):
        if entry_point == "pinned_root":
            evidence.inherit_pinned_root(prior, (old / "artifact.json").read_bytes())
        else:
            evidence.inherit_admitted(prior, SimpleNamespace(pin=old_artifact.pin, root=root))
    assert not any(row["event"] == "lineage" for row in journal(evidence))
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        evidence.seal(outcome="build-complete")


def test_evidence_backed_prior_and_new_source_pin_survive_publication(tmp_path, monkeypatch):
    old_output = tmp_path / "old"
    old_output.mkdir()
    old_evidence = CaptureEvidence(old_output, "test")
    old_evidence.capture(capture(b"previous current source"), stage="used")
    old, old_artifact = candidate(old_output, old_evidence)
    store = Store()
    prior = pub.publish_generation(old, client=store, bucket="test", prior_index=pub.empty_index(),
                                   evidence_directories=(old_evidence.artifact_dir,))
    monkeypatch.setattr(
        pub, "load_family_root", lambda url, entry: ((old / "artifact.json").read_bytes(), old_artifact.root)
    )
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.inherit(prior, public_url="https://test.invalid")
    evidence.capture(capture(), stage="used")
    directory, artifact = candidate(tmp_path, evidence, prior)
    assert next(row for row in journal(evidence) if row["event"] == "lineage")["prior_inputs"] == old_artifact.root["inputs"]
    index = pub.publish_generation(
        directory, client=store, bucket="test", prior_index=prior, evidence_directories=(evidence.artifact_dir,)
    )
    assert index["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    assert store.writes[-1] == pub.INDEX_V2_KEY
    assert (
        next(item for item in artifact.root["inputs"] if item["role"] == "prior-generation")["artifactDigest"]
        == old_artifact.pin.artifact_digest
    )
    assert evidence.artifact is not None
    prefix = "source-evidence/" + evidence.artifact.pin.artifact_digest.removeprefix("sha256:")
    assert store.objects[prefix + "/journal.jsonl"] == (evidence.artifact_dir / "journal.jsonl").read_bytes()
    monkeypatch.setattr(
        pub, "load_family_root", lambda url, entry: ((directory / "artifact.json").read_bytes(), artifact.root)
    )
    resumed = CaptureEvidence(tmp_path, "test")
    resumed.inherit(index, public_url="https://test.invalid")
    lineage = next(row for row in journal(resumed) if row["event"] == "lineage")
    assert lineage["prior_inputs"] == artifact.root["inputs"]
    assert lineage["evidence_status"].startswith("Inherited pins")
    assert not any(row["event"] == "capture" for row in journal(resumed))
    assert resumed.prior_input is not None
    assert resumed.prior_input.artifact_digest == artifact.pin.artifact_digest


@pytest.mark.parametrize("tamper", [None, "journal.jsonl", "members.json"])
def test_the_prior_evidence_journal_is_read_through_its_pins(tmp_path, monkeypatch, tamper):
    store = Store()
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.inherit(pub.empty_index(), public_url=None)
    evidence.event("congress-index-selection", table="t", unevidenced=[["119", "ec", "1"]])
    directory, artifact = candidate(tmp_path, evidence)
    index = pub.publish_generation(
        directory,
        client=store,
        bucket="test",
        prior_index=pub.empty_index(),
        evidence_directories=(evidence.artifact_dir,),
    )
    assert evidence.inherited_event("congress-index-selection") is None, "no managed prior"
    if tamper:
        assert evidence.artifact is not None
        key = f"source-evidence/{evidence.artifact.pin.artifact_digest.removeprefix('sha256:')}/{tamper}"
        store.objects[key] += b"\n"

    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://test.invalid/")
        status, content = (200, store.objects[key]) if key in store.objects else (404, b"")
        yield httpx.Response(status, content=content, request=httpx.Request(method, url))

    monkeypatch.setattr(pub.httpx, "stream", stream)
    resumed = CaptureEvidence(tmp_path, "test")
    resumed.inherit(index, public_url="https://test.invalid")
    if tamper:
        with pytest.raises(pub.PublicationError, match="declared digest"):
            resumed.inherited_event("congress-index-selection", table="t")
        return
    selection = resumed.inherited_event("congress-index-selection", table="t")
    assert selection is not None and selection["unevidenced"] == [["119", "ec", "1"]]
    assert resumed.inherited_event("congress-index-selection", table="other") is None
    reads = sum(store.reads.values())
    resumed.inherited_event("congress-index-selection", table="t")
    assert sum(store.reads.values()) == reads, "the journal is read once"


def test_wrong_inherited_pin_refuses_generation_and_evidence_publication(tmp_path):
    old, old_artifact = build(tmp_path, keys=("a.parquet",))
    store = Store()
    prior = pub.publish_generation(old, client=store, bucket="test", prior_index=pub.empty_index())
    old_pointer = store.objects[pub.INDEX_V2_KEY]
    old_writes = list(store.writes)
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.prior_input = ArtifactInput("prior-generation", old_artifact.pin.logical_id, "sha256:" + "0" * 64)
    with pytest.raises(ValueError, match="lineage"):
        candidate(tmp_path, evidence, prior)
    # An internally valid table root cannot substitute a differently inherited
    # evidence artifact even when each artifact separately passes admission.
    inputs = [item for item in evidence.inputs() if item.role == "source-evidence"]
    inputs.append(ArtifactInput("prior-generation", old_artifact.pin.logical_id, old_artifact.pin.artifact_digest))
    directory = tmp_path / "different-lineage"
    build_generation(
        directory,
        family="test",
        files=[tmp_path / "a.parquet"],
        expected_keys=["a.parquet"],
        read_snapshot=prior,
        inputs=inputs,
    )
    with pytest.raises(pub.PublicationError, match="different inherited"):
        pub.publish_generation(
            directory, client=store, bucket="test", prior_index=prior, evidence_directories=(evidence.artifact_dir,)
        )
    assert store.objects[pub.INDEX_V2_KEY] == old_pointer
    assert store.writes == old_writes


def test_scheduled_pipeline_binds_and_uploads_nonhidden_evidence(tmp_path, monkeypatch):
    from spicy_regs.sources import r2, cloudflare

    class Scheduled(RollupPipeline):
        name = "test"
        output = "a.parquet"
        retain_source_evidence = True

        def build(self, output_dir):
            assert self.source_evidence is not None
            self.source_evidence.capture(capture(), stage="used")
            path = output_dir / self.output
            pq.write_table(pa.table({"id": ["one"]}), path)
            return path

    store = Store()
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setattr(pub, "load_index", lambda url: pub.empty_index())
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: None)
    pipeline = Scheduled(output_dir=tmp_path, skip_upload=False)
    pipeline.run()
    evidence = pipeline.source_evidence
    assert evidence is not None
    assert ".builds" not in evidence.directory.parts
    [generation] = list((tmp_path / "generations").iterdir())
    artifact = verify_generation(generation)
    assert evidence.artifact is not None
    assert artifact.root["inputs"] == [{"role": "source-evidence", **evidence.artifact.pin.as_dict()}]
    assert store.writes[-1] == pub.INDEX_V2_KEY
    assert json.loads((evidence.directory / "run-outcome.json").read_bytes())["outcome"] == "complete"


@pytest.mark.parametrize("failure", ["count-drift", "overflow"])
def test_owner_preyield_pagination_refusal_retains_both_pages_and_no_generation(tmp_path, monkeypatch, failure):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    second_url = "https://api.govinfo.gov/collections/CRPT/2026-01-01T00%3A00%3A00Z?offsetMark=two&pageSize=1000"
    first = json.dumps(
        {"count": 2 if failure == "count-drift" else 1, "packages": [{"packageId": CRPT_ID}], "nextPage": second_url}
    ).encode()
    second = json.dumps(
        {"count": 3 if failure == "count-drift" else 1, "packages": [{"packageId": "CRPT-119hrpt2"}]}
    ).encode()
    seen = []

    def respond(request):
        seen.append(str(request.url))
        raw = first if len(seen) == 1 else second
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": "application/json"})

    class Reports(RollupPipeline):
        name = "committee-reports"
        output = "committee_reports.parquet"
        retain_source_evidence = True

        def build(self, output_dir):
            assert self.source_evidence is not None
            with RetainedGovInfoDiscoveryReader(
                evidence=self.source_evidence,
                api_key=KEY,
                budget=PagedJsonBudget(4, 1024 * 1024, 10.0, 0.0),
                transport=httpx.MockTransport(respond),
                clock=lambda: NOW,
            ) as reader:
                return build_committee_reports(
                    output_dir,
                    reader=reader,
                    acquirer=StubBodyAcquirer(),
                    hearings=Hearings(),
                    download_prior=lambda *args: False,
                    evidence=self.source_evidence,
                )

    pipeline = Reports(output_dir=tmp_path)
    with pytest.raises(PagedJsonSourceError, match="declared count changed|more records"):
        pipeline.run()
    evidence = pipeline.source_evidence
    assert evidence is not None
    assert len(seen) == 2
    assert set(bodies(evidence)) == {first, second}
    captures = [row for row in journal(evidence) if row.get("stage") == "govinfo-listing-response"]
    assert len(captures) == 2
    assert captures[1]["sha256"] == "sha256:" + hashlib.sha256(second).hexdigest()
    assert not (tmp_path / "generations").exists()
    assert not list(tmp_path.glob("*.parquet"))
    assert json.loads((evidence.directory / "run-outcome.json").read_bytes())["outcome"] == "failed"


def test_congress_detail_preyield_overflow_retains_original(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    raw = json.dumps({"pagination": {"count": 0}, "hearing": {"jacketNumber": "63127", "congress": "119"}}).encode()
    with RetainedCongressListingReader(
        evidence=evidence,
        api_key=KEY,
        budget=PagedJsonBudget(4, 1024 * 1024, 10.0, 0.0),
        clock=lambda: NOW,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=httpx.ByteStream(raw), headers={"content-type": "application/json"}
            )
        ),
    ) as owner:
        assert _event_id(owner, _package(CHRG_ID), evidence) == (None, "refused")
    assert raw in bodies(evidence)
    assert any(row.get("stage") == "congress-listing-response" for row in journal(evidence))


def test_shared_congress_factory_selects_observer_without_budget_change(tmp_path):
    from spicy_regs.sources.congress_bills import listing_reader

    evidence = CaptureEvidence(tmp_path, "committee-reports")
    with listing_reader(KEY) as ordinary, listing_reader(KEY, evidence=evidence) as retained:
        assert isinstance(retained, RetainedCongressListingReader)
        assert retained.budget == ordinary.budget


def test_affiliation_refresh_replaces_digest_history_and_clears_empty(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    record = json.loads(roster())[0]
    record["terms"][0]["party_affiliations"] = [
        {"party": "Democrat", "start": "2025-01-03", "end": "2025-04-01"},
        {"party": "Republican", "start": "2025-03-01"},
    ]
    def run(records):
        raw = json.dumps(records).encode()
        with RetainedLegislatorsAcquirer(
            evidence=CaptureEvidence(tmp_path / ("populated" if records[0]["terms"][0]["party_affiliations"] else "empty"), "members"),
            budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0), clock=lambda: NOW,
            transport=httpx.MockTransport(lambda request: httpx.Response(
                200, stream=httpx.ByteStream(raw if "current" in request.url.path else b"[]"),
                headers={"content-type": "application/json"})),
        ) as owner:
            return build_members(tmp_path, acquirer=owner)[2]
    rows = pq.read_table(run([record])).to_pylist()
    assert [row["party"] for row in rows] == ["Democrat", "Republican"]
    assert rows[0]["affiliation_end"] > rows[1]["affiliation_start"]
    record["terms"][0]["party_affiliations"] = []
    assert pq.read_table(run([record])).num_rows == 0


def test_selected_response_header_is_retained_without_other_response_headers(tmp_path):
    evidence = CaptureEvidence(tmp_path, "fcc-filings")
    transport = evidence.transport(
        httpx.MockTransport(lambda _: httpx.Response(
            200, stream=httpx.ByteStream(b"native"), headers={
                "content-type": "application/json", "total": "594", "set-cookie": "private=secret", "x-api-key": "secret",
            })), stage="fcc", max_bytes=100, response_header_names=("total",),
    )
    with httpx.Client(transport=transport) as client:
        assert client.get("https://publicapi.fcc.gov/ecfs/filings").content == b"native"
    event = next(row for row in journal(evidence) if row["event"] == "capture")
    assert event["response_headers"] == [["total", "594"]]
    assert "secret" not in json.dumps(journal(evidence))
    evidence.finish()
    verify_evidence(evidence.artifact_dir)


@pytest.mark.parametrize("direct", [False, True])
def test_selected_header_credential_echo_is_never_retained(tmp_path, direct):
    evidence = CaptureEvidence(tmp_path, "fcc-filings")
    evidence.credential = KEY
    with pytest.raises(CredentialRefusedError):
        if direct:
            evidence.capture(capture(response_headers=(("total", KEY),)), stage="fcc")
        else:
            transport = evidence.transport(httpx.MockTransport(lambda _: httpx.Response(
                200, stream=httpx.ByteStream(b"native"), headers={"total": KEY},
            )), stage="fcc", max_bytes=100, response_header_names=("total",))
            with httpx.Client(transport=transport) as client:
                client.get("https://publicapi.fcc.gov/ecfs/filings")
    assert KEY not in json.dumps(journal(evidence))
    assert not any(row["event"] == "capture" for row in journal(evidence))
