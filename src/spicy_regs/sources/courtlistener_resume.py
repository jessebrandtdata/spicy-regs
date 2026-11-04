"""Caller-selected CourtListener Search recovery through the maintained checkpoint."""

from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import fcntl
import json
from pathlib import Path

import httpx

from spicy_docs.transport.resume import RetainedGetTraversal, ResumeError, write_json_atomic
from spicy_docs.transport.retry import retry_after_seconds


def _now():
    return datetime.now(UTC)


@dataclass(frozen=True)
class RetainedCourtSearch:
    """One explicit parent/edition selection; never substitute a newer parent on resume."""

    directory: Path
    selection: dict

    @contextmanager
    def open(self, *, url, max_page_bytes, live_transport, evidence, api_token):
        self.directory.mkdir(parents=True, exist_ok=True)
        with closing(live_transport), (self.directory / "run.lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ResumeError("Court retained search is already in use") from error
            retained = RetainedGetTraversal(
                self.directory,
                selection={**self.selection, "url": url, "max_page_bytes": max_page_bytes},
                first_url=url,
                max_page_bytes=max_page_bytes,
            )
            checkpoint = retained.progress
            prefix_size = len(checkpoint.state["pages"])
            paths = [retained.store / "sha256" / page["sha256"][7:] for page in checkpoint.state["pages"]]
            deadline_path = self.directory / "not-before.json"

            class LiveTransport(httpx.BaseTransport):
                def handle_request(self, request):
                    # Replay never enters here. A saved publisher deadline is a
                    # refusal before HTTP, not a new retry loop or quota guess.
                    if deadline_path.exists():
                        try:
                            value = json.loads(deadline_path.read_text())
                            deadline = datetime.fromisoformat(value["not_before"])
                            if deadline.tzinfo is None or value["selection_sha256"] != retained.selection_pin:
                                raise ValueError("invalid deadline selection")
                        except (KeyError, TypeError, ValueError) as error:
                            raise ResumeError("Court retained Retry-After deadline is invalid") from error
                        if _now() < deadline:
                            raise ResumeError("Court retained Retry-After deadline has not elapsed")
                    response = live_transport.handle_request(request)
                    observed = _now()
                    if response.status_code == 429:
                        delay = retry_after_seconds(response.headers.get("Retry-After"), now=observed)
                        if delay is not None:
                            try:
                                deadline = observed + timedelta(seconds=delay)
                            except OverflowError:
                                deadline = datetime.max.replace(tzinfo=UTC)
                            write_json_atomic(deadline_path, {
                                "selection_sha256": retained.selection_pin,
                                "observed_at": observed.isoformat(),
                                "not_before": deadline.isoformat(),
                            })
                    return response

            # The verified prefix must be outside the evidence wrapper: it
            # preserves original observation times and bypasses live pacing.
            transport = checkpoint.replay_transport(paths, LiveTransport())

            def retain(capture):
                if capture.method != "GET" or capture.request_body is not None or capture.content_encoding != "identity":
                    raise ResumeError("Court retained search requires identity-encoded GET captures")
                retained.writer.put(
                    [capture.body], max_bytes=max_page_bytes,
                    expected_digest=capture.sha256, expected_size=capture.byte_size,
                )

            def track(pages):
                for page in checkpoint.track(pages, retain_capture=retain, api_keys=(api_token or "",)):
                    if page.page_index < prefix_size:
                        evidence.capture(page.capture, stage="courtlistener-replayed-response")
                    yield page

            yield transport, track
