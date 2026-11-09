"""CI log lines: one per timed phase, so a CI log attributes a run's wall time without a profiler, and annotations.

Logging only: the wrapped work runs unchanged and an exception propagates after its line.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from time import monotonic

from loguru import logger


@contextmanager
def phase_log(name: str, **fields: object) -> Iterator[None]:
    """Log ``receipt-phase <name> ... seconds=<s> status=<ok|failed>`` when the block ends."""
    started = monotonic()
    status = "failed"
    try:
        yield
        status = "ok"
    finally:
        details = " ".join(f"{key}={value}" for key, value in fields.items())
        logger.info("receipt-phase {} {} seconds={:.1f} status={}", name, details, monotonic() - started, status)


def annotate(level: str, title: str, message: str) -> None:
    """Log ``message`` and emit it as a GitHub Actions annotation (``warning``/``error``; plain text outside Actions)."""
    logger.log(level.upper(), "{}: {}", title, message)
    print(f"::{level} title={title}::{message}", flush=True)
