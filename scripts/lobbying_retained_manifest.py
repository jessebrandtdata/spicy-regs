"""Write the pinned retained-page manifest that ``run-rollup-lobbying-rebuild`` reads (ss-hqoi C1).

Read-only over the central raw archive and the source catalog. Which retained objects belong to the LDA family is
every object the catalog's ``family:lobbying-filings`` collection, the raw receipts or an acquisition receipt
attributes to lda.gov; an object is a filings page when its body is an API page whose results carry
``filing_uuid``. Each page's capture time is the latest ``observed_at`` a retained journal records for it (basis
``journal``). A page no journal records takes a day: the 2026-09-23 initial load's ``acquisition.json`` dates its
captures only by its directory (``date``); otherwise an acquisition receipt's association time
(``receipt-date``), else a date in the original location (``path-date``), each as the start of that UTC day.

Output is TSV ``sha256, bytes, captured_at, basis``, sorted by digest, pinned by its own sha256 when passed to the
rebuild. It refuses an attributed object missing from the archive, a body whose digest differs, or a filings page
with no capture time, so the page count is whatever the archive holds, not a remembered number.

    uv run --frozen python scripts/lobbying_retained_manifest.py --catalog <index.sqlite> pages.tsv
"""

from __future__ import annotations

import argparse
import collections
import gzip
import hashlib
import json
import re
import sqlite3
import sys
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root

COLLECTION = "family:lobbying-filings"
LDA_HOST = re.compile(r"https?://(?:www\.)?lda(?:\.senate)?\.gov/", re.I)
NEEDLES = ("lobbying", "lda.gov", "lda.senate.gov")
DATE_IN_PATH = re.compile(r"(20\d\d-?\d\d-?\d\d)")


class _Candidate:
    __slots__ = ("captured", "location", "receipt_url")

    def __init__(self) -> None:
        self.location: str | None = None
        self.receipt_url = False
        self.captured: str | None = None  # an acquisition association's observed_at


def _lines(path: Path) -> Iterator[dict]:
    with gzip.open(path, "rt") as stream:
        for line in stream:
            if any(needle in line for needle in NEEDLES):
                yield json.loads(line)


def candidates(raw: Path, catalog: Path) -> dict[str, _Candidate]:
    """Every retained object attributed to the LDA family, by digest, with the facts that date it."""
    found: dict[str, _Candidate] = collections.defaultdict(_Candidate)

    def add(sha: str, *, location: str | None = None, url: str | None = None, captured: str | None = None) -> None:
        item = found[sha.removeprefix("sha256:")]
        item.location = item.location or location
        item.receipt_url = item.receipt_url or bool(url and LDA_HOST.match(url))
        item.captured = item.captured or captured

    con = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)
    try:
        for sha, location in con.execute(
                "SELECT sha256, json_extract(data, '$.original_location') FROM files WHERE collection_id = ?",
                (COLLECTION,)):
            add(sha, location=location)
    finally:
        con.close()
    for row in _lines(raw / "files.jsonl.gz"):
        context = row.get("receipt_context") or {}
        add(row["sha256"], location=row.get("original_location"),
            url=row.get("receipt_source_url") or context.get("source_url"))
    for row in _lines(raw / "provisional-receipts.jsonl.gz"):
        add(row["raw_sha256"], location=row.get("source_reference"))
    for row in _lines(raw / "receipts.jsonl.gz"):
        add(row["sha256"], location=row.get("original_location"))
    for path in sorted((raw / "acquisitions").rglob("*")):
        if not path.is_file():
            continue
        text = path.read_bytes()
        if b"lda.gov" not in text and b"lda.senate.gov" not in text:
            continue
        receipt = json.loads(text)
        for item in receipt.get("files", []):
            if LDA_HOST.match(item.get("source_url") or ""):
                add(item["sha256"], location=item.get("original_location"), url=item["source_url"])
        for association in (receipt.get("receipt") or {}).get("acquisition_associations", []):
            if LDA_HOST.match(association.get("source_url") or "") and association.get("sha256"):
                add(association["sha256"], url=association["source_url"], captured=association.get("observed_at"))
    return dict(found)


def classify(sha: str) -> tuple[str, str, int, list[tuple[str, str]]]:
    """``(sha, kind, bytes, captures)``: a journal's ``(page, observed_at)`` pairs, an acquisition's ``(page, "")``."""
    with CentralRawArchiveStore(create=False).open(sha) as stream:
        body = stream.read()
    if hashlib.sha256(body).hexdigest() != sha:
        return sha, "unverified", len(body), []
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        if b'"event"' not in body.split(b"\n", 1)[0]:
            return sha, "other", len(body), []
        captures = []
        for line in body.decode().splitlines():
            event = json.loads(line)
            if event.get("event") == "capture" and event.get("sha256") and event.get("observed_at"):
                captures.append((event["sha256"].removeprefix("sha256:"), event["observed_at"]))
        return sha, "journal", len(body), captures
    if not isinstance(document, dict):
        return sha, "other", len(body), []
    results = document.get("results")
    if isinstance(results, list):
        first = next((record for record in results if isinstance(record, dict)), None)
        return sha, "filings" if first is not None and "filing_uuid" in first else "other", len(body), []
    if isinstance(document.get("captures"), list) and isinstance(document.get("windows"), list):
        return sha, "acquisition", len(body), [(c["sha256"], "") for c in document["captures"] if c.get("sha256")]
    return sha, "other", len(body), []


def _day(value: str) -> datetime:
    return datetime.fromisoformat(value[:10]).replace(tzinfo=timezone.utc)


def manifest(raw: Path, catalog: Path, *, workers: int = 8) -> tuple[list[str], collections.Counter]:
    found = candidates(raw, catalog)
    pages: dict[str, int] = {}
    observed: dict[str, datetime] = {}
    dated_by_load: dict[str, str] = {}
    refused = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for sha, kind, size, captures in pool.map(classify, sorted(found), chunksize=64):
            if kind == "unverified":
                refused.append(sha)
            elif kind == "filings":
                pages[sha] = size
            elif kind == "journal":
                for page, at in captures:
                    moment = datetime.fromisoformat(at).astimezone(timezone.utc)
                    observed[page] = max(observed.get(page, moment), moment)
            elif kind == "acquisition":
                # The load is dated only by its own directory name (lobbying-initial-load-2026-09-23).
                match = DATE_IN_PATH.search(Path(found[sha].location or "").parent.name)
                for page, _ in captures:
                    if match:
                        dated_by_load.setdefault(page, match.group(1))
    if refused:
        sys.exit(f"{len(refused)} attributed objects differ from their digest, e.g. {refused[0]}")
    lines, bases = ["sha256\tbytes\tcaptured_at\tbasis"], collections.Counter()
    for sha in sorted(pages):
        item = found[sha]
        if sha in observed:
            moment, basis = observed[sha], "journal"
        elif sha in dated_by_load:
            moment, basis = _day(dated_by_load[sha]), "receipt-date" if item.receipt_url else "date"
        elif item.captured:
            moment, basis = _day(item.captured), "receipt-date" if item.receipt_url else "date"
        elif match := DATE_IN_PATH.search(item.location or ""):
            moment, basis = _day(match.group(1)), "path-date"
        else:
            sys.exit(f"Retained filings page {sha} has no capture time")
        bases[basis] += 1
        lines.append(f"{sha}\t{pages[sha]}\t{moment.isoformat()}\t{basis}")
    return lines, bases


def main() -> None:
    parser = argparse.ArgumentParser(description="Write the pinned retained-page manifest for the lobbying rebuild.")
    parser.add_argument("--catalog", type=Path, required=True, help="the source catalog index.sqlite")
    parser.add_argument("--raw-root", type=Path, default=None, help="default: SPICYGOV_RAW_ROOT")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    raw = args.raw_root or default_raw_archive_root()
    lines, bases = manifest(raw, args.catalog)
    body = ("\n".join(lines) + "\n").encode()
    args.output.write_bytes(body)
    print(json.dumps({"pages": len(lines) - 1, "bases": dict(bases), "sha256": hashlib.sha256(body).hexdigest()}))


if __name__ == "__main__":
    main()
