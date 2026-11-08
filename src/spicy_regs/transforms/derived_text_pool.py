"""Bounded comment-level text reads shared by ETL and backfill."""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from queue import SimpleQueue
from threading import BoundedSemaphore, Event, Lock, Thread, local
from time import monotonic
from typing import Any

from loguru import logger

from spicy_regs.sources.derived_text import DerivedCommentText, DerivedFill, DerivedTextUnavailable, DocketListings

# One budget for all agencies/dockets using a pool. Pending results also occupy
# a slot until consumed, so slow staging cannot accumulate an unbounded queue.
MAX_TEXT_WORKERS = 16


@dataclass(frozen=True, slots=True)
class TextResult:
    record: dict
    status: str
    fill: DerivedFill | None = None
    error: DerivedTextUnavailable | None = None


def needs_comment_text(record: dict) -> bool:
    """Inline enrichment must preserve both existing text and PDF outcomes."""
    return bool(record.get("attachments_json")) and record.get("text_content") is None and not record.get(
        "text_extraction_status"
    )


class DerivedTextPool:
    """Ordered, bounded streams over one shared executor with worker-owned resources.

    Construct and close at the call site outside agency threads. Resource creation
    is serial, avoiding boto3's shared default-session race; each worker then owns
    one resource for its lifetime. Only immutable docket listings cross threads.
    """

    def __init__(
        self, resource_factory: Callable[[], Any], *, max_workers: int = 8, progress_seconds: float = 30,
        on_fill: Callable[[DerivedFill], None] | None = None,
    ) -> None:
        """``on_fill`` sees each fill before its row can be written (the ETL registers the objects it cites)."""
        if max_workers < 1:
            raise ValueError("text workers must be positive")
        self.max_workers = min(max_workers, MAX_TEXT_WORKERS)
        self._slots = BoundedSemaphore(self.max_workers * 2)
        self._local = local()
        self._stopped = Event()
        self._lock = Lock()
        self._counts: Counter[str] = Counter()
        self._started = monotonic()
        self._progress_seconds = progress_seconds
        self._on_fill = on_fill
        listings = DocketListings()
        fetchers: SimpleQueue[DerivedCommentText] = SimpleQueue()
        for _ in range(self.max_workers):
            fetchers.put(DerivedCommentText(resource_factory(), listings=listings))

        def initialize() -> None:
            self._local.fetcher = fetchers.get_nowait()

        self._executor = ThreadPoolExecutor(max_workers=self.max_workers, initializer=initialize)
        self._reporter = Thread(target=self._heartbeat, name="derived-text-progress", daemon=True)

    def __enter__(self) -> DerivedTextPool:
        self._reporter.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._stopped.set()
        if self._reporter.is_alive():
            self._reporter.join()
        self._report()

    def _heartbeat(self) -> None:
        while not self._stopped.wait(self._progress_seconds):
            self._report()

    def _report(self) -> None:
        with self._lock:
            counts = self._counts.copy()
        elapsed = monotonic() - self._started
        completed = sum(counts.values())
        logger.info(
            "Comment text: {} completed ({} derived, {} missing, {} failed, {} skipped), {:.1f}/s, {:.1f}s elapsed",
            completed, counts["derived"], counts["missing"], counts["failed"], counts["skipped"],
            completed / max(elapsed, 0.001), elapsed,
        )

    def _fill(self, record: dict, select: Callable[[dict], bool] | None) -> TextResult:
        if select is not None and not select(record):
            result = TextResult(record, "skipped")
        else:
            try:
                fill = self._local.fetcher.fill_for(
                    record.get("agency_code"), record.get("docket_id"), record.get("comment_id")
                )
                if fill is not None and self._on_fill is not None:
                    self._on_fill(fill)
                result = TextResult(record, "derived" if fill else "missing", fill)
            except DerivedTextUnavailable as error:
                result = TextResult(record, "failed", error=error)
        with self._lock:
            self._counts[result.status] += 1
        return result

    def map(
        self, records: Iterable[dict], *, select: Callable[[dict], bool] | None = None,
    ) -> Generator[TextResult, None, None]:
        """Preserve row order, with a global bound even when many agencies call at once.

        Never block for another slot while holding unconsumed futures: another
        agency may hold the remaining slots and need this consumer to progress.
        """
        pending: deque[Future[TextResult]] = deque()
        source = iter(records)
        exhausted = False
        try:
            while pending or not exhausted:
                while not exhausted and self._slots.acquire(blocking=not pending):
                    try:
                        record = next(source)
                    except StopIteration:
                        self._slots.release()
                        exhausted = True
                        break
                    except BaseException:
                        self._slots.release()
                        raise
                    try:
                        pending.append(self._executor.submit(self._fill, record, select))
                    except BaseException:
                        self._slots.release()
                        raise
                if pending:
                    future = pending.popleft()
                    try:
                        result = future.result()
                    finally:
                        self._slots.release()
                    yield result
        finally:
            for future in pending:
                # Running reads retain their slots until done, even if a caller
                # closes the generator early or another source raises.
                future.add_done_callback(lambda _: self._slots.release())
                future.cancel()
