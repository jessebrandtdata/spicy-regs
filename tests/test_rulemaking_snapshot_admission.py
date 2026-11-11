"""A rulemaking snapshot's declared receipt member is validated when the snapshot is admitted.

``publication.load_rulemaking_snapshot`` calls ``rulemaking_receipt`` on every load, so a misbound declaration
refuses the whole snapshot before any reader (the MCP connection, explorer metadata, the dictionary, aggregate
checks) builds a URL from it. Moved here from the retired receipt lookup's tests: the refusal outlived the lookup.
"""

from __future__ import annotations

import json

import pytest

from spicy_regs.sources import publication as pub

BASE = "https://example.invalid"
SNAPSHOT_ID = "snapshot_under_test"
PREFIX = f"materialized/rulemaking/snapshots/{SNAPSHOT_ID}"


def load(edit=None):
    manifest = {
        "dataset": "rulemaking", "format_version": 2, "snapshot_id": SNAPSHOT_ID, "run_id": "run-1",
        "asserted_at": "2026-10-05T00:00:00Z",
        "artifacts": {
            "proceedings.parquet": {"remote_key": f"{PREFIX}/proceedings.parquet", "sha256": "a" * 64,
                                    "bytes": 1, "rows": 2, "visibility": "public"},
            "etl_receipts.parquet": {"remote_key": f"{PREFIX}/etl_receipts.parquet", "sha256": "b" * 64,
                                     "bytes": 1, "rows": 2, "visibility": "internal"},
        },
        "etlReceipts": {"key": "etl_receipts.parquet", "generationId": "run-1", "policies": [{"dataset": "proceedings"}]},
    }
    if edit is not None:
        edit(manifest)
    pointer = {"dataset": "rulemaking", "format_version": 2, "snapshot_id": SNAPSHOT_ID,
               "manifest_key": f"{PREFIX}/manifest.json"}
    documents = {f"{BASE}/{pub.SNAPSHOT_POINTER}": pointer, f"{BASE}/{PREFIX}/manifest.json": manifest}
    return pub.load_rulemaking_snapshot(BASE, read=lambda url, **kwargs: json.dumps(documents[url]).encode())


def test_a_well_bound_snapshot_receipt_is_admitted_with_its_snapshot():
    snapshot = load()
    assert snapshot is not None and set(snapshot["tables"]) == {"proceedings.parquet"}
    receipt = pub.rulemaking_receipt(snapshot)
    assert receipt is not None and receipt["datasets"] == ["proceedings"]


@pytest.mark.parametrize("change", [
    lambda m: m["etlReceipts"].__setitem__("generationId", "another-run"),
    lambda m: m["etlReceipts"].__setitem__("key", "../other.parquet"),
    lambda m: m["artifacts"]["etl_receipts.parquet"].__setitem__("remote_key", "materialized/other/etl_receipts.parquet"),
    lambda m: m["artifacts"]["etl_receipts.parquet"].__setitem__("sha256", "missing"),
    lambda m: m["artifacts"]["etl_receipts.parquet"].__setitem__("visibility", "public"),
    lambda m: m["artifacts"].pop("etl_receipts.parquet"),
    lambda m: m["etlReceipts"].__setitem__("policies", [{"dataset": "proceedings"}, {"dataset": "proceedings"}]),
])
def test_a_misbound_snapshot_receipt_refuses_at_snapshot_admission(change):
    with pytest.raises(pub.PublicationError):
        load(change)
