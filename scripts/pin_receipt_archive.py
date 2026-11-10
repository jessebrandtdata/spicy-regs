"""Pin a family's live generation as the last with receipts, for its tables whose receipts are retired (ss-vse3).

    uv run --frozen --extra comments-reader python scripts/pin_receipt_archive.py comment-attributes

Run it in the change that retires them, as late as possible before it merges: publication refuses the first
receipts-free generation unless docs/pins/receipt-archive.json pins the live one. For every dataset the family's live
receipts list that its installed policy marks retired, it writes the entry (family, digest, publication instant,
receipts key) and commits that generation's root as tests/fixtures/receipt_archive/<digest>.json.gz, which
tests/test_receipt_archive_pins.py checks the entry against; a root no entry names any more is removed. It refuses
when the family publishes no receipts: then its pin is already the last receipts generation and must not move.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PINS = ROOT / "docs/pins/receipt-archive.json"
ROOTS = ROOT / "tests/fixtures/receipt_archive"


def pin(family: str, base_url: str) -> dict[str, dict]:
    """Write the pins of ``family``'s retired datasets from the live index; return the entries written."""
    from spicy_regs.sources import publication
    from spicy_regs.subject_catalog import retired_receipt_datasets

    entry = publication.current_index(base_url)["families"][family]
    receipts = entry.get("etlReceipts")
    if receipts is None:
        raise SystemExit(f"{family} publishes no receipts: its last receipts generation is pinned already")
    datasets = sorted(set(receipts["datasets"]) & retired_receipt_datasets())
    if not datasets:
        raise SystemExit(f"{family}: none of {receipts['datasets']} is marked receipts_retired")
    raw, root = publication.load_family_root(base_url, entry)
    digest = entry["artifactDigest"].removeprefix("sha256:")
    if root["artifactDigest"] != entry["artifactDigest"]:
        raise SystemExit(f"{family}: the root read back is {root['artifactDigest']}, not the live {digest}")
    document = json.loads(PINS.read_text())
    written = {name: {"family": family, "artifactDigest": entry["artifactDigest"], "publishedAt": entry["publishedAt"],
                      "receipts": f"generations/{family}/{digest}/{receipts['key']}"} for name in datasets}
    document["archives"] = dict(sorted({**document["archives"], **written}.items()))
    PINS.write_text(json.dumps(document, indent=1) + "\n")
    (ROOTS / f"{digest}.json.gz").write_bytes(gzip.compress(raw, mtime=0))
    named = {item["artifactDigest"].removeprefix("sha256:") for item in document["archives"].values()}
    for stale in ROOTS.glob("*.json.gz"):
        if stale.name.removesuffix(".json.gz") not in named:
            stale.unlink()
    return written


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pin a family's live generation as its last with receipts.")
    parser.add_argument("family")
    parser.add_argument("--base-url", default="https://data.spicygov.ai")
    args = parser.parse_args(argv)
    print(json.dumps(pin(args.family, args.base_url), indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
