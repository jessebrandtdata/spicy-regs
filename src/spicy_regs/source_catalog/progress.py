"""Bounded phase diagnostics, separate from catalogue identities and stdout."""

from contextlib import contextmanager
from datetime import UTC, datetime
import json
import sys
from time import monotonic


@contextmanager
def phase(name):
    started = monotonic()

    def emit(status):
        print(json.dumps({'catalogPhase': name, 'status': status,
                          'at': datetime.now(UTC).isoformat(),
                          'elapsedSeconds': monotonic() - started}), file=sys.stderr, flush=True)

    emit('started')
    try:
        yield
    except BaseException:
        emit('failed')
        raise
    else:
        emit('completed')
