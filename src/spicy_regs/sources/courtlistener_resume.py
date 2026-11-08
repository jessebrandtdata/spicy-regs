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

#: Publisher's usage endpoint: https://wiki.free.law/c/courtlistener/help/api/rest/v4/api-usage
#: It is throttled under its own ``api_usage`` scope, so reading it spends no Search quota.
USAGE_URL = "https://www.courtlistener.com/api/rest/v4/api-usage/"
_MAX_USAGE_BYTES = 64 * 1024
_USAGE_FIELDS = {"rate": str, "used": int, "limit": int, "remaining": int, "window_seconds": int, "blocked": bool}


def _now():
    return datetime.now(UTC)


def _deferred(reason: str) -> ResumeError:
    return ResumeError(f"Court continuation deferred before live Search: {reason}. The retained checkpoint is unchanged")


def read_usage(api_token: str | None, transport: httpx.BaseTransport | None) -> bytes:
    """One authenticated usage read; any failure defers rather than guessing the allowance."""
    if not api_token:
        raise _deferred("CourtListener usage requires the API token")
    owned = transport is None
    transport = httpx.HTTPTransport() if transport is None else transport
    request = httpx.Request("GET", USAGE_URL, headers={
        "Authorization": f"Token {api_token}", "Accept": "application/json",
        "Accept-Encoding": "identity", "User-Agent": "spicy-regs-courtlistener-usage/1.0",
    }, extensions={"timeout": httpx.Timeout(60).as_dict()})
    try:
        response = transport.handle_request(request)
        try:
            if response.status_code != 200:
                raise _deferred(f"CourtListener usage returned HTTP {response.status_code}")
            body = bytearray()
            stream = response.stream
            if not isinstance(stream, httpx.SyncByteStream):
                raise TypeError("CourtListener usage needs a synchronous transport")
            for chunk in stream:
                body += chunk
                if len(body) > _MAX_USAGE_BYTES:
                    raise _deferred("CourtListener usage response is too large")
        finally:
            response.close()
    except httpx.HTTPError as error:
        raise _deferred("CourtListener usage could not be read") from error
    finally:
        if owned:
            transport.close()
    return bytes(body)


def fit_quota(body: bytes, wanted: int, *, request_interval: float) -> tuple[int, list[dict]]:
    """Live Search attempts, at most ``wanted``, that every user-scope window can absorb.

    Windows roll continuously. A window longer than the paced run cannot refill
    during it, so its ``remaining`` caps the run; a shorter one refills under the
    existing pacing. Blocked, exhausted or unreadable usage defers. A later 429's
    Retry-After stays authoritative over this point-in-time reading.
    """
    try:
        rows = json.loads(body)["current_usage"]
        windows = [{name: row[name] for name in (*_USAGE_FIELDS, "reset_at")}
                   for row in rows if row["scope"] == "user"]
        if not windows or any(
            type(window[name]) is not kind or (kind is int and window[name] < 0)
            for window in windows for name, kind in _USAGE_FIELDS.items()
        ) or any(window["window_seconds"] == 0 or not isinstance(window["reset_at"], (str, type(None)))
                 for window in windows):
            raise ValueError("unusable user windows")
    except (KeyError, TypeError, ValueError) as error:
        raise _deferred("CourtListener usage response is unusable") from error
    if any(window["blocked"] for window in windows):
        raise _deferred("the CourtListener account is blocked")
    for window in windows:
        if window["remaining"] == 0:
            raise _deferred(f"CourtListener {window['rate']} quota is exhausted until {window['reset_at']}")
    live = wanted
    # Longest first: each cap shortens the run, which can bring a shorter window into play.
    for window in sorted(windows, key=lambda window: -window["window_seconds"]):
        if request_interval * live < window["window_seconds"]:
            live = min(live, window["remaining"])
    return live, windows


@dataclass(frozen=True)
class RetainedCourtSearch:
    """One explicit parent/edition selection; never substitute a newer parent on resume."""

    directory: Path
    selection: dict

    @contextmanager
    def open(self, *, url, page_budget, traversal_budget, live_transport, usage_transport, evidence, api_token):
        """Yield the search transport, page tracker and the allocation this continuation may use.

        A live continuation first reads the account's usage once and shrinks the
        caller's allocation to what the quota can absorb, or defers without a
        Search request. A complete replay makes no request at all.
        """
        from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget

        max_page_bytes = page_budget.max_page_bytes
        self.directory.mkdir(parents=True, exist_ok=True)
        with closing(live_transport), (self.directory / "run.lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ResumeError("Court retained search is already in use") from error
            selection_path = self.directory / "selection.json"
            checkpoint_path = self.directory / "checkpoint.json"
            deadline_path = self.directory / "not-before.json"
            if any(path.is_symlink() for path in (selection_path, checkpoint_path, deadline_path)):
                raise ResumeError("Court retained search controls must be local files")
            # The provider writes the selection before the journal, then creates the capture store.
            # A selection alone can be a fresh setup interrupted before the first request.
            missing_selection = checkpoint_path.exists() and not selection_path.is_file()
            missing_checkpoint = not checkpoint_path.is_file() and (
                (self.directory / "captures").exists() or deadline_path.exists()
            )
            if missing_selection or missing_checkpoint:
                raise ResumeError("Court retained search controls are missing. Preserve the directory and recover its controls")
            retained = RetainedGetTraversal(
                self.directory,
                selection={**self.selection, "url": url, "max_page_bytes": max_page_bytes},
                first_url=url,
                max_page_bytes=max_page_bytes,
            )
            checkpoint = retained.progress
            prefix_size = len(checkpoint.state["pages"])
            capture_directory = retained.store / "sha256"
            paths = [capture_directory / page["sha256"][7:] for page in checkpoint.state["pages"]]
            # Bytes without a journal entry cannot establish their cursor or observation time.
            if set(capture_directory.iterdir()).difference(paths):
                raise ResumeError("Court retained search has captures outside its checkpoint. Preserve the directory")

            def check_deadline():
                # A saved publisher deadline is a refusal before HTTP, not a retry loop.
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

            # Replayed pages count toward the caller's allocation; only the rest can go live.
            traversal = traversal_budget
            wanted = traversal_budget.max_requests - prefix_size
            if not checkpoint.state["complete"] and wanted > 0:
                check_deadline()
                live, windows = fit_quota(
                    read_usage(api_token, usage_transport), wanted,
                    request_interval=page_budget.min_request_interval_seconds,
                )
                evidence.event("quota-preflight", stage="courtlistener-quota",
                               requested_live=wanted, allowed_live=live, windows=windows)
                if live < wanted:
                    traversal = CourtListenerTraversalBudget(prefix_size + live, traversal_budget.max_wait_seconds)

            class LiveTransport(httpx.BaseTransport):
                def handle_request(self, request):
                    # Replay never enters here.
                    check_deadline()
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

            yield transport, track, traversal
