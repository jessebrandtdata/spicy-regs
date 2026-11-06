"""Dated GovInfo package identity and latest-read selection.

Package modification times identify retained metadata revisions. They do not
establish first publication or guarantee that each textual change has a new time.
"""

from datetime import datetime
import re
from typing import Any, Mapping


DATED_TABLES = frozenset({
    "budget_volumes", "house_activity_reports", "committee_reports", "report_sections",
    "committee_report_cbo_letters", "hearing_transcripts", "hearing_bill_links",
})
_INSTANT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\Z")


def revision_instant(value: Any) -> datetime:
    """Require a publisher timestamp with an explicit zone; never invent a date."""
    if not isinstance(value, str) or not _INSTANT.fullmatch(value):
        raise ValueError(f"GovInfo revision needs an explicit last_modified instant, got {value!r}")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def latest_read(prior: Mapping[str, Any], incoming: dict) -> dict:
    """Keep the latest source revision's checkpoint during an older-source replay.

    An incoming successful correction of the same source revision remains authoritative.
    Literal timestamp spelling breaks ties between equivalent publisher instants.
    """
    previous = prior.get("last_modified")
    modified = incoming.get("last_modified")
    if previous is not None:
        try:
            if modified is None or newer_revision(previous, modified):
                return dict(prior)
        except ValueError:
            # A refused discovery result cannot move a valid retained checkpoint.
            revision_instant(previous)
            return dict(prior)
    return incoming


def newer_revision(candidate: str, retained: str | None) -> bool:
    """Whether a validated source coordinate follows the retained coordinate."""
    return retained is None or (revision_instant(candidate), candidate) > (revision_instant(retained), retained)


def latest_package_rows(relation: str) -> str:
    """SQL selecting every part of the latest dated revision of each package.

    ``relation`` is a trusted SQL relation supplied by a maintained caller.
    """
    return (
        f"SELECT * FROM {relation} QUALIFY dense_rank() OVER (PARTITION BY package_id "
        "ORDER BY CAST(last_modified AS TIMESTAMPTZ) DESC, last_modified DESC) = 1"
    )
