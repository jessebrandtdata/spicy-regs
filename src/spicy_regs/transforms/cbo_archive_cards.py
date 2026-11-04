"""Private card candidates from explicitly pinned Common Crawl receipts; no acquisition.

Latest archive timestamp wins per publication. Conflicting originals at one timestamp
refuse; the order of receipt keys cannot choose a revision. The existing provider
owns WARC integrity, publication/card gates and literal-to-millions conversion.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

VERSION = "cbo-retained-cards/1"
MAX_INPUT_BYTES = 16 * 1024 * 1024


def _schema() -> pa.Schema:
    from spicy_docs.sources.cbo import CARD_MEASURES, CARD_QUESTIONS

    windows = [("period", pa.string())]
    for name in CARD_MEASURES:
        windows.extend([(name + "_literal", pa.string()), (name + "_millions", pa.int64())])
    return pa.schema(
        [
            *[
                (name, pa.string())
                for name in (
                    "publication_id",
                    "title",
                    "subtitle",
                    "markup",
                    "card_source",
                    "source_sha256",
                    "source_locator",
                    "archive_timestamp",
                    "earliest_timestamp",
                )
            ],
            ("captures", pa.int32()),
            ("windows", pa.list_(pa.struct(windows))),
            ("answers", pa.struct([(name, pa.string()) for name in CARD_QUESTIONS])),
            ("footnotes", pa.list_(pa.string())),
            ("pdf_urls", pa.list_(pa.string())),
        ]
    )


def build(manifest_path: Path, archive_root: Path, output_dir: Path, *, max_pages: int = 100) -> Path:
    """Read selected immutable page receipts, then promote one complete private candidate.

    Missing or damaged inputs and weak archive identity abort without an output.
    Genuine no-card pages and gated parser refusals have separate, sourced outcomes.
    The result is bounded retained-input proof, not archive-wide coverage or publication.
    """
    from spicy_docs.sources.cbo import (
        CARD_MEASURES,
        archived_publication_id,
        CboCardAbsent,
        CboSourceError,
        card_millions,
        parse_cbo_estimate_card,
    )
    from spicy_docs.sources.web_archive import cc_record_locator, read_warc_response
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

    destination = output_dir.resolve()
    if destination.is_relative_to(archive_root.resolve()):
        raise ValueError("CBO candidate output must be outside the raw archive")
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError(output_dir)
    manifest_bytes = manifest_path.read_bytes()
    keys = json.loads(manifest_bytes)["receipt_keys"]
    if not 1 <= max_pages <= 100 or not isinstance(keys, list) or not 1 <= len(keys) <= max_pages:
        raise ValueError("Select between one and max_pages explicit retained page receipts (maximum 100)")
    store = CentralRawArchiveStore(archive_root, create=False)
    captures = defaultdict(list)
    inputs = []
    observed_bytes = 0
    for _, document in iter_acquisition_receipts(archive_root, receipt_keys=keys):
        receipt = document["receipt"]
        if receipt.get("dataset_id") != "cbo-publication-page" or len(document["files"]) != 1:
            raise ValueError("Expected one CBO publication page original per receipt")
        item = document["files"][0]
        size = item["bytes"]
        if size > MAX_INPUT_BYTES - observed_bytes:
            raise ValueError("Retained CBO originals exceed the 16-MiB input bound")
        with store.open(item["sha256"]) as stream:
            body = stream.read(size + 1)
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        if len(body) != size or digest.removeprefix("sha256:") != item["sha256"]:
            raise ValueError("Retained CBO original differs from its receipt")
        observed_bytes += size
        response = read_warc_response(body)
        identifier = receipt.get("native_id")
        timestamp = receipt.get("archive_timestamp", "")
        if (
            receipt.get("authority") != "archive"
            or receipt.get("archive_operator") != "Common Crawl"
            or item.get("native_id", identifier) != identifier
            or archived_publication_id(response.target_uri) != identifier
            or response.target_uri != receipt.get("original_publisher_url")
            or not re.fullmatch(r"[0-9]{14}", timestamp)
            or datetime.fromisoformat(response.warc_date.replace("Z", "+00:00")).strftime("%Y%m%d%H%M%S") != timestamp
            or response.record_id != receipt.get("warc_record_id")
            or response.payload_digest != receipt.get("index_digest", "").removeprefix("sha1:")
            or response.payload_digest != receipt.get("payload_sha1")
            or response.http_status != 200
            or receipt.get("warc_length") != size
            or type(receipt.get("warc_offset")) is not int
            or receipt["warc_offset"] < 0
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", receipt.get("index_sha256", ""))
            or not receipt["warc_filename"].startswith("crawl-data/" + receipt["crawl_id"] + "/")
        ):
            raise ValueError("Weak or conflicting CBO archive identity")
        cc_record_locator(receipt["warc_filename"])
        provenance = {
            name: receipt[name]
            for name in (
                "authority",
                "archive_operator",
                "original_publisher_url",
                "archive_timestamp",
                "index_sha256",
                "index_digest",
                "crawl_id",
                "warc_filename",
                "warc_offset",
                "warc_length",
            )
        }
        provenance.update(
            source_sha256=digest,
            source_locator=response.record_id + "#payload",
            receipt_object=document["receipt_object"],
        )
        inputs.append(provenance)
        captures[identifier].append((timestamp, digest, response, provenance))
    rows, outcomes = [], []
    for identifier, versions in sorted(captures.items()):
        by_timestamp = defaultdict(set)
        for timestamp, digest, _, _ in versions:
            by_timestamp[timestamp].add(digest)
        if any(len(digests) != 1 for digests in by_timestamp.values()):
            raise ValueError(f"Conflicting CBO originals at one archive timestamp: {identifier}")
        # Exact duplicate originals may have repeated receipts. Digest/receipt order only
        # breaks that byte-identical tie, never a content or archive-time conflict.
        timestamp, digest, response, provenance = max(versions, key=lambda v: (v[0], v[1], v[3]["receipt_object"]))
        outcome = {
            **provenance,
            "publication_id": identifier,
            "captures": len(versions),
            "earliest_timestamp": min(v[0] for v in versions),
        }
        try:
            card = parse_cbo_estimate_card(
                response.payload, publication_id=identifier, archived_url=response.target_uri
            )
        except CboCardAbsent as absent:
            outcomes.append({**outcome, "outcome": "absent", "reason": str(absent)})
            continue
        except CboSourceError as refused:
            outcomes.append({**outcome, "outcome": "refused", "reason": str(refused)})
            continue
        windows = []
        for position, period in enumerate(card.periods):
            window = {"period": period}
            for name in CARD_MEASURES:
                literal = card.measures[name][position] if name in card.measures else None
                window.update({name + "_literal": literal, name + "_millions": card_millions(literal)})
            windows.append(window)
        rows.append(
            {
                **outcome,
                "title": card.title,
                "subtitle": card.subtitle,
                "markup": card.markup,
                "card_source": "archive-page",
                "windows": windows,
                "answers": dict(card.answers),
                "footnotes": list(card.footnotes),
                "pdf_urls": ["https://www.cbo.gov" + p for p in card.documents],
            }
        )
        outcomes.append({**outcome, "outcome": "card", "reason": None})
    table = pa.Table.from_pylist(rows, schema=_schema())
    report = {
        "version": VERSION,
        "manifest_sha256": "sha256:" + hashlib.sha256(manifest_bytes).hexdigest(),
        "selection": "latest archive timestamp per publication; conflicting same-time originals refuse",
        "inputs": inputs,
        "outcomes": outcomes,
        "rows": len(rows),
        "input_bytes": observed_bytes,
        "coverage": "explicit retained receipts only; no acquisition or archive-wide completeness claim",
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="." + destination.name + "-", dir=destination.parent) as temporary:
        staged = Path(temporary) / "candidate"
        staged.mkdir()
        pq.write_table(table, staged / "cbo_estimate_cards.parquet", compression="zstd")
        (staged / "inputs.json").write_text(json.dumps(report, indent=2) + "\n")
        destination.mkdir()
        try:
            staged.rename(destination)
        except BaseException:
            try:
                destination.rmdir()
            except OSError:
                pass
            raise
    return destination / "cbo_estimate_cards.parquet"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--archive-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()
    build(args.manifest, args.archive_root, args.output_dir, max_pages=args.max_pages)


if __name__ == "__main__":
    main()
