"""A saved access refusal must abort the ordinary offline EO consumer."""

import hashlib
import json
from datetime import UTC, datetime

import httpx
import pytest
from spicy_docs.sources.federal_register.body_acquisition import FederalRegisterBodyAcquirer, FederalRegisterBodyBudget
from spicy_docs.sources.federal_register.body_captures import capture_body, load_body_capture
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.source_evidence import CaptureEvidence, EvidenceMemberSource
from spicy_regs.transforms.build_executive_orders import build_executive_orders
from tests.test_executive_orders import Reader, record

ORDER = record("94-290", "12890", "1994-01-05")


def xml(number):
    return f"<PRESDOCU><P>Example.</P><FRDOC>[FR Doc. {number} Filed 1-2-94; 8:45 am]</FRDOC></PRESDOCU>".encode()


def captured(directory, reader):
    """One acquired body result per record, served by a fake publisher."""
    paths = {}
    for item in reader.records:
        number = item["record"]["document_number"]
        with FederalRegisterBodyAcquirer(
            budget=FederalRegisterBodyBudget(2, 4096, 4096, 1, 0),
            transport=httpx.MockTransport(lambda request, number=number: httpx.Response(
                200, headers={"Content-Type": "application/xml"}, stream=httpx.ByteStream(xml(number)))),
        ) as client:
            path = directory / number / "result.json"
            capture_body(path, record=item["record"], source_pin=reader.pin.as_dict(), acquirer=client)
        paths[item["sourceRecordId"]] = path
    return paths


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("mixed", [False, True], ids=["all-refused", "mixed"])
def test_saved_access_refusal_aborts_and_preserves_previous_output(tmp_path, status, mixed):
    reader = Reader([ORDER, record("95-1001", "12945", "1995-01-20")] if mixed else [ORDER])
    paths = captured(tmp_path / "good", reader)
    output = tmp_path / "output"
    evidence = CaptureEvidence(output, "executive-orders")
    previous = build_executive_orders(reader, output, body_paths=paths, load_body=load_body_capture,
                                      evidence=evidence)
    before = previous.read_bytes()
    evidence.finish()
    selected = reader.records[-1]
    path = tmp_path / "denied" / "result.json"
    with FederalRegisterBodyAcquirer(
        budget=FederalRegisterBodyBudget(2, 4096, 4096, 1, 0),
        clock=lambda: datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        transport=httpx.MockTransport(lambda request: httpx.Response(status, content=b"Access denied")),
    ) as acquirer:
        with pytest.raises(CredentialRefusedError):
            capture_body(path, record=selected["record"], source_pin=reader.pin.as_dict(), acquirer=acquirer)
    saved = path.read_bytes()
    manifest = json.loads(saved)
    assert manifest["failure"]["credential_refused"] is True
    assert manifest["failure"]["status_code"] == status
    assert manifest["captures"] == []  # The provider deliberately excludes denied response bodies.
    paths[selected["sourceRecordId"]] = path
    retry_evidence = CaptureEvidence(output, "executive-orders")
    with pytest.raises(CredentialRefusedError, match="explicit retry") as refused:
        build_executive_orders(reader, output, body_paths=paths, load_body=load_body_capture,
                               evidence=retry_evidence)
    retry_evidence.finish(refused.value)
    assert previous.read_bytes() == before
    assert path.read_bytes() == saved
    events = [json.loads(line) for line in (retry_evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    retained = next(event for event in events if event.get("stage") == "executive-order-body-result"
                    and event.get("source_record_id") == selected["sourceRecordId"])
    assert retained["sha256"] == "sha256:" + hashlib.sha256(saved).hexdigest()
    denial = next(event for event in events if event.get("reason_code") == "access_refused")
    assert denial["status_code"] == status
    assert denial["body_retained"] is False


def test_overlapping_retry_retains_the_manifest_that_caused_the_refusal(tmp_path):
    reader = Reader([ORDER])
    identity = reader.records[0]["sourceRecordId"]
    path = tmp_path / "body" / "result.json"
    output = tmp_path / "build"
    statuses = iter((401, 403))

    with FederalRegisterBodyAcquirer(
        budget=FederalRegisterBodyBudget(2, 4096, 4096, 1, 0),
        transport=httpx.MockTransport(lambda request: httpx.Response(next(statuses), content=b"Access denied")),
    ) as acquirer:
        with pytest.raises(CredentialRefusedError, match="401"):
            capture_body(path, record=ORDER, source_pin=reader.pin.as_dict(), acquirer=acquirer)
        original = path.read_bytes()

        def load_then_retry(selected, *, record):
            result = load_body_capture(selected, record=record)
            # Let the actual retry writer replace the selection before the builder retains it.
            with pytest.raises(CredentialRefusedError, match="403"):
                capture_body(path, record=record, source_pin=reader.pin.as_dict(), acquirer=acquirer,
                             retry_failed=True)
            return result

        evidence = CaptureEvidence(output, "executive-orders")
        with pytest.raises(CredentialRefusedError, match="explicit retry") as refused:
            build_executive_orders(reader, output, body_paths={identity: path}, load_body=load_then_retry,
                                   evidence=evidence)
        evidence.finish(refused.value)

    assert json.loads(path.read_bytes())["failure"]["status_code"] == 403
    assert not (output / "executive_order_observations.parquet").exists()
    journal, = output.glob("source-evidence/*/artifact/journal.jsonl")
    events = [json.loads(line) for line in journal.read_text().splitlines()]
    denial, = [event for event in events if event.get("reason_code") == "access_refused"]
    assert denial["source_record_id"] == identity and denial["status_code"] == 401
    receipt, = [event for event in events if event.get("stage") == "executive-order-body-result"]
    assert receipt["source_record_id"] == identity
    with EvidenceMemberSource(journal.parent).open(receipt["blob_member"]) as retained:
        assert retained.read() == original


def test_real_provider_capture_uses_injected_aware_clock(tmp_path):
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    path = tmp_path / "result.json"
    reader = Reader([ORDER])
    with FederalRegisterBodyAcquirer(
        budget=FederalRegisterBodyBudget(2, 4096, 4096, 1, 0), clock=lambda: now,
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"Content-Type": "application/xml"}, stream=httpx.ByteStream(xml("94-290")))),
    ) as acquirer:
        capture_body(path, record=ORDER, source_pin=reader.pin.as_dict(), acquirer=acquirer)
    loaded = load_body_capture(path, record=ORDER)
    assert loaded.outcome == "acquired"
    assert loaded.captures[0].body == xml("94-290")
    assert datetime.fromisoformat(loaded.captures[0].observed_at.replace("Z", "+00:00")) == now
