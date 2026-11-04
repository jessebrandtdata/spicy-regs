"""A saved access refusal must abort the ordinary offline EO consumer."""

import hashlib
import json
from datetime import UTC, datetime

import httpx
import pytest
from spicy_docs.sources.federal_register.body_acquisition import FederalRegisterBodyAcquirer, FederalRegisterBodyBudget
from spicy_docs.sources.federal_register.body_captures import capture_body, load_body_capture
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_executive_orders import build_executive_orders
from tests.test_executive_orders import Reader, captured, record


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("mixed", [False, True], ids=["all-refused", "mixed"])
def test_saved_access_refusal_aborts_and_preserves_previous_output(tmp_path, status, mixed):
    reader = Reader([record(), record("2025-00002", "14134")] if mixed else [record()])
    paths = captured(tmp_path / "good", reader)
    output = tmp_path / "output"
    evidence = CaptureEvidence(output, "executive-orders")
    previous = build_executive_orders(reader, output, year=2025, body_paths=paths,
                                     load_body=load_body_capture, evidence=evidence)
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
            capture_body(path, record=selected["record"],
                         source_pin=reader.pin.as_dict(), acquirer=acquirer)
    saved = path.read_bytes()
    manifest = json.loads(saved)
    assert manifest["failure"]["credential_refused"] is True
    assert manifest["failure"]["status_code"] == status
    assert manifest["captures"] == []  # The provider deliberately excludes denied response bodies.
    paths[selected["sourceRecordId"]] = path
    retry_evidence = CaptureEvidence(output, "executive-orders")
    with pytest.raises(CredentialRefusedError, match="explicit retry") as refused:
        build_executive_orders(reader, output, year=2025, body_paths=paths,
                               load_body=load_body_capture, evidence=retry_evidence)
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
    from spicy_regs.pipelines.executive_orders import build

    candidate = tmp_path / "generation-attempt"
    with pytest.raises(CredentialRefusedError, match="explicit retry"):
        build(reader, candidate, year=2025, body_paths=paths)
    assert not (candidate / "generation").exists()
    assert previous.read_bytes() == before


def test_real_provider_capture_uses_injected_aware_clock(tmp_path):
    source = record()
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    body = b"<PRESDOCU><P>Example.</P><FRDOC>[FR Doc. 2025-00001 Filed 1-2-25; 8:45 am]</FRDOC></PRESDOCU>"
    path = tmp_path / "result.json"
    reader = Reader([source])
    with FederalRegisterBodyAcquirer(
        budget=FederalRegisterBodyBudget(2, 4096, 4096, 1, 0), clock=lambda: now,
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"Content-Type": "application/xml"}, stream=httpx.ByteStream(body))),
    ) as acquirer:
        capture_body(path, record=source, source_pin=reader.pin.as_dict(), acquirer=acquirer)
    loaded = load_body_capture(path, record=source)
    assert loaded.outcome == "acquired"
    assert loaded.captures[0].body == body
    assert datetime.fromisoformat(loaded.captures[0].observed_at.replace("Z", "+00:00")) == now
