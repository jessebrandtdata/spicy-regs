"""Every table whose receipts are retired keeps its last receipts generation, and retention can see the pin (ss-ag1j).

Weekly retention keeps a family's last three generations and any generation a ``docs/`` file cites by eight or more
hex digits (``generation_retention.note_pins``). The last generation a family published with per-row receipts holds
the only complete copy of them, so ``docs/pins/receipt-archive.json`` cites it, in the same change that retires them.
Nothing fails if that pin is missing or a docs edit drops its digest, except this test.
"""

import gzip
import json
from pathlib import Path

import pytest

from spicy_regs import generation_retention as retention
from spicy_regs import subject_catalog

DIGEST = "ab" * 32


def _pin(family="federal-register", digest=DIGEST, **changed):
    return {"family": family, "artifactDigest": f"sha256:{digest}", "publishedAt": "2026-11-10T21:30:00+00:00",
            "receipts": f"generations/{family}/{digest}/etl_receipts.parquet", **changed}


def _document(archives):
    return json.dumps({"format": retention.RECEIPT_ARCHIVE_FORMAT, "version": 1, "archives": archives}).encode()


def test_every_retired_table_has_an_archive_pin_and_every_pin_a_retired_table():
    subject_catalog.retired_receipt_datasets.cache_clear()
    assert set(retention.receipt_archive_pins()) == subject_catalog.retired_receipt_datasets()


#: Each pinned generation's own root (artifact.json, gzipped, fetched once from the public bucket when it was pinned).
ROOTS = Path(__file__).parent / "fixtures/receipt_archive"


def test_each_pin_names_a_generation_that_holds_that_tables_receipts():
    """Checked against the pinned generation's own root, not against the pin file: the root's digest is recomputed
    from its content, so a pin edited to any other generation finds no root of that digest, or one that does not
    state receipts for the table.

    What it cannot see: that the bucket still holds that generation (retention deletes members it does not keep), and
    that it is the family's last generation with receipts (publication checks that when they are dropped).
    """
    from rulespec_artifacts import expected_artifact_digest, parse_canonical_json

    for dataset, pin in retention.receipt_archive_pins().items():
        digest = pin["artifactDigest"]
        path = ROOTS / (digest.removeprefix("sha256:") + ".json.gz")
        assert path.is_file(), f"{dataset}: commit the pinned generation's artifact.json as {path.name}"
        root = parse_canonical_json(gzip.decompress(path.read_bytes()))
        assert expected_artifact_digest(root) == digest, dataset
        receipts = root["spec"]["etlReceipts"]
        assert root["spec"]["family"] == pin["family"], dataset
        assert dataset in [policy["dataset"] for policy in receipts["policies"]], dataset
        assert pin["receipts"] == f"generations/{pin['family']}/{digest.removeprefix('sha256:')}/{receipts['key']}"


def test_retention_keeps_every_pinned_generation():
    pins = retention.receipt_archive_pins()
    generations = {pin["artifactDigest"].removeprefix("sha256:"): retention.Generation(pin["family"], digest)
                   for pin in pins.values() for digest in [pin["artifactDigest"].removeprefix("sha256:")]}
    named = retention.note_pins(retention._notes(), generations)
    pin_file = str(retention.RECEIPT_ARCHIVE.relative_to(retention.DOCS.parent))
    assert all(pin_file in named.get(digest, ()) for digest in generations)


def test_the_docs_scan_finds_a_pin_written_in_this_shape():
    """The mechanism on a synthetic entry, so it is proven before any table retires."""
    text = _document({"federal_register": _pin()}).decode()
    named = retention.note_pins([("docs/pins/receipt-archive.json", text)],
                                {DIGEST: retention.Generation("federal-register", DIGEST)})
    assert named == {DIGEST: {"docs/pins/receipt-archive.json"}}
    assert retention.receipt_archive_pins(_document({"federal_register": _pin()})) == {"federal_register": _pin()}


@pytest.mark.parametrize("pin", [
    _pin(digest="ab" * 16),
    _pin(receipts=f"generations/federal-register/{'cd' * 32}/etl_receipts.parquet"),
    _pin(family="Federal Register"),
    _pin(publishedAt="last night"),
    {key: value for key, value in _pin().items() if key != "publishedAt"},
])
def test_a_pin_that_would_not_keep_the_receipts_it_names_is_refused(pin):
    with pytest.raises(ValueError):
        retention.receipt_archive_pins(_document({"federal_register": pin}))
