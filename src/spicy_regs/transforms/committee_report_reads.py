"""Own-output checkpoints for the committee-report rollups, including successful reads that produce no link.

``RULE_VERSIONS`` is the rule set that invalidates a prior read for each
collection, ``complete`` is the completeness test over a checkpoint row, and
``settled`` adds the refusals no later run could turn into a read.
"""

from pathlib import Path

import pyarrow.parquet as pq
from spicy_docs.interpretation.cbo_estimates import CBO_ESTIMATE_RULE_VERSION
from spicy_docs.interpretation.hearing_bill_links import HEARING_BILL_LINK_RULE_VERSION
from spicy_docs.schemas.committee_report_tables import REPORT_SECTION_READER_VERSION

from spicy_regs.generations import spicy_docs_code

READS_TABLE = "committee_report_reads"
READ_COLUMNS = ("package_id", "last_modified", "outcome", "rule_version", "observed_at")
#: ``parts`` is this repository's own rule for reading a report: every part
#: its record states, one row each, a package's rows replaced as a set
#: (decision 29). ``per-part-001`` replaced ``held-001``, which held out a
#: report read at its part 1's stem, so every CRPT checkpoint reads once more.
#: The re-read corrects what the backfill cannot: a row published before
#: ``part_id`` is kept as ``(package_id, package_id)``, right for a report in
#: one part, but CRPT-119hrpt811's Part-1 row (published 2026-09-24) becomes
#: ``(CRPT-119hrpt811-pt1, 1)``, and CRPT-119hrpt494 gains its Part 2 and part
#: numbers, only when read again.
#:
#: ``body`` is this repository's rule for a body's text, in both collections:
#: ``placeholder-pdf-001`` reads the PDF a part offers in place of the
#: publisher's placeholder text, publishes ``body_completeness`` and
#: ``text_derivation``, and gives a PDF-derived report's sections their pages.
#: ``placeholder-pdf-002`` (spicy-docs 0.33.2) also knows the notice spelled
#: without "IN", which CRPT-119hrpt649 and CHRG-119jhrg60491 print, so every
#: body read under 001 is read again.
#: ``code`` is the installed SpicyDocs' code and data (:func:`spicy_docs_code`),
#: which derives a report's text and sections; it replaced the release string,
#: under which every one of the stack's version-only releases re-read all 141
#: reports for rows differing only in ``observed_at`` (708f2bf4 against 95810b26).
#: CHRG also moves with ``HEARING_BILL_LINK_RULE_VERSION`` (the date correction
#: moved it). Admitting the two historical CHRG volumes to the package grammar
#: needs no token: no checkpoint names either, so there is nothing to re-read,
#: and only discovery can select them. ``mods=hearing-mods-001``: spicy-docs
#: 0.54.0 reads a hearing's MODS event ids, witnesses and held date into three
#: appended columns, so each of the 185 held hearings is read once more.
#: ``mods=hearing-mods-002``: the round-6 build also reads the committee codes
#: the MODS states (``committee_system_codes_json``), so each is read again.
RULE_VERSIONS = {
    "CRPT": (
        f"code={spicy_docs_code()[:12]};cbo={CBO_ESTIMATE_RULE_VERSION};sections={REPORT_SECTION_READER_VERSION}"
        ";parts=per-part-001;body=placeholder-pdf-002"
    ),
    "CHRG": f"{HEARING_BILL_LINK_RULE_VERSION};body=placeholder-pdf-002;mods=hearing-mods-002",
}


def prior_reads(path: Path | None, priors: dict[str, Path | None]) -> dict[str, dict]:
    """Seed absent checkpoints from each package's latest retained revision."""
    import duckdb
    from spicy_regs.govinfo_revisions import latest_package_rows, newer_revision

    rows = {} if path is None else {row["package_id"]: row for row in pq.read_table(path).to_pylist()}
    for prior in priors.values():
        if prior is not None:
            current = latest_package_rows(f"read_parquet('{prior}')")
            for key, modified in duckdb.sql(f"SELECT DISTINCT package_id, last_modified FROM ({current})").fetchall():
                if key not in rows or newer_revision(modified, rows[key].get("last_modified")):
                    rows[key] = {"package_id": key, "last_modified": modified, "outcome": "pending", "rule_version": None}
    return rows


def complete(row: dict, collection: str, modified: str | None = None) -> bool:
    """Whether a recorded read is complete under this collection's rule version, optionally at a source stamp."""
    return (
        row.get("outcome") == "complete"
        and row.get("rule_version") == RULE_VERSIONS[collection]
        and (modified is None or modified == row.get("last_modified"))
    )


#: A read refused for a reason every later run would meet again until the
#: publisher changes the package: a body past the byte bound (the 1946 Pearl
#: Harbor hearing parts, 312 and 373 MB against 24 MiB), a record stating more
#: parts than the request budget, or no rendition the reader takes.
REFUSED_FINAL = "refused_final"


#: The basis of a size refusal whose response stated its length: ``size=<stated bytes>``.
SIZE_BASIS = "size="


def refusal_rule(collection: str, basis: str) -> str:
    """The token a final refusal is recorded under: the collection's rule and what refused it.

    ``basis`` is ``size=<bytes>`` for a body whose response stated a length past
    the byte bound, and otherwise the bounds that refused it.
    """
    return f"{RULE_VERSIONS[collection]};refused-under={basis}"


def settled(row: dict, collection: str, modified: str | None = None, *, bounds: str, max_body_bytes: int) -> bool:
    """Complete, or refused for good under the same rule; either way at ``modified`` when given.

    A size refusal is final on the size its response stated: it stands while that
    size is past ``max_body_bytes``, so a larger bound that still falls short (the
    1946 parts state 312,487,940 and 373,504,330 bytes) reads nothing, and one past
    it reads the package again. Any other final refusal stands while ``bounds`` is
    unchanged. Either way a new ``last_modified`` reads it again: GovInfo moves a
    package's stamp with its content, and a stated size is known only by asking.
    """
    if row.get("outcome") != REFUSED_FINAL:
        return complete(row, collection, modified)
    prefix = refusal_rule(collection, "")
    rule = row.get("rule_version") or ""
    if not rule.startswith(prefix):
        return False
    basis = rule.removeprefix(prefix)
    stated = basis.removeprefix(SIZE_BASIS)
    if basis.startswith(SIZE_BASIS) and stated.isdecimal():
        still_refused = int(stated) > max_body_bytes
    else:
        still_refused = basis == bounds
    return still_refused and (modified is None or modified == row.get("last_modified"))
