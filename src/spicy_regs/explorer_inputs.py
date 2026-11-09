"""Decide, before any build work, whether published explorer metadata already reflects its inputs.

Explorer metadata is a function of the verified ``main`` revision (``sourceRevision``) and the
mutable storage objects the publisher reads: the publication index, the separate publication
controls and the measurement companion. Everything else it reads is pinned by those objects.
The publisher records each such object's digest in the bundle's ``inputs``; the scheduled check
re-reads exactly those keys and calls :func:`unchanged`.

Standard library only: the check runs before the project's dependencies are installed.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping

KEY = "explorer-metadata.v1.json"
FORMAT = "spicy-regs-explorer-metadata"


def input_digest(raw: bytes | None) -> str | None:
    """The recorded identity of one read: its SHA-256, or None for an absent object."""
    return None if raw is None else "sha256:" + hashlib.sha256(raw).hexdigest()


def unchanged(previous: bytes | None, *, revision: str, read: Callable[[str], bytes | None]) -> tuple[bool, str]:
    """Whether the live metadata was built from this revision and the same input bytes.

    ``read(key)`` returns an object's current bytes, or None when it is absent; it raises when
    storage cannot answer. Only a complete, exact match is "unchanged". A missing, older or
    unreadable record, a different revision, any changed or newly present/absent input, or any
    read failure returns False with the reason, so the caller runs the full build.
    """
    try:
        value = json.loads(previous) if previous else None
    except (ValueError, UnicodeDecodeError):
        return False, "live metadata is absent or unreadable"
    if not isinstance(value, dict) or value.get("format") != FORMAT or value.get("version") != 1:
        return False, "live metadata is absent or another format"
    if not revision or value.get("sourceRevision") != revision:
        return False, "main revision differs from the live metadata"
    recorded = value.get("inputs")
    if not isinstance(recorded, Mapping) or not recorded:
        return False, "live metadata records no inputs"
    for key, digest in sorted(recorded.items()):
        if not isinstance(key, str) or not (digest is None or isinstance(digest, str)):
            return False, "live metadata input record is malformed"
        try:
            current = input_digest(read(key))
        except Exception as error:  # any storage doubt means rebuild, never skip
            return False, f"{key} could not be read ({type(error).__name__})"
        if current != digest:
            return False, f"{key} changed"
    return True, f"unchanged: revision {revision[:12]} and {len(recorded)} inputs match the live metadata"
