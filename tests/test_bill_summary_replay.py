"""A replay-required CRS summary prior is rebuilt whole from every BILLSTATUS folder, or the run refuses (ss-aj7t)."""

import pyarrow.parquet as pq
import pytest

from spicy_regs.policy_migrations import PriorReplayRequired
from spicy_regs.transforms import build_bill_family as build
from tests.test_bill_family import StubBodyAcquirer, StubBulkAcquirer, _members_from, _prior_from
from tests.test_bill_family import scoped as fixture_scope

scoped = fixture_scope
SUMMARIES = "bill_publisher_summaries"


def replay_priors(published):
    """The earlier run's outputs, except a summary prior selected under a replay-required policy."""
    plain = _prior_from(published)

    def download(key, target):
        if key == f"{SUMMARIES}.parquet":
            raise PriorReplayRequired(SUMMARIES, "legislative-documents/2", "fixture: clocks need the XML")
        return plain(key, target)

    return {"download_prior": download, "download_members": _members_from(published)}


def build_run(directory, bulk, **priors):
    directory.mkdir()
    paths = build.build_bill_family(directory, bulk_acquirer=bulk, body_acquirer=StubBodyAcquirer(), **priors)
    return {path.stem: path for path in paths}


def rows(path):
    return sorted(pq.read_table(path).to_pylist(), key=str)


def test_a_nightly_scope_refuses_before_restoring_anything_and_names_the_one_dispatch(tmp_path, scoped):
    build_run(tmp_path / "first", StubBulkAcquirer(), download_prior=lambda key, target: False)
    priors, asked = replay_priors(tmp_path / "first"), []
    download = priors["download_prior"]
    priors["download_prior"] = lambda key, target: asked.append(key) or download(key, target)
    bulk = StubBulkAcquirer()
    with pytest.raises(ValueError, match=r"gh workflow run rollup-bill-family.yml .*-f congresses=108-119"):
        build_run(tmp_path / "second", bulk, **priors)
    assert asked == [f"{SUMMARIES}.parquet"] and bulk.calls == []


def test_a_full_scope_replay_rereads_unchanged_folders_and_bills_and_restates_every_summary(
    tmp_path, scoped, monkeypatch
):
    first = build_run(tmp_path / "first", StubBulkAcquirer(), download_prior=lambda key, target: False)
    assert rows(first[SUMMARIES])
    # An unchanged zip and bill would normally be skipped; the replay must read both.
    unchanged = build_run(tmp_path / "unchanged", StubBulkAcquirer(), **{
        "download_prior": _prior_from(tmp_path / "first"), "download_members": _members_from(tmp_path / "first")})
    assert rows(unchanged[SUMMARIES]) == rows(first[SUMMARIES])

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "108-119")
    monkeypatch.delenv("BILL_FAMILY_BILL_TYPES")
    bulk = StubBulkAcquirer()
    replayed = build_run(tmp_path / "replay", bulk, **replay_priors(tmp_path / "first"))
    assert len(bulk.zip_downloads) == 12 * 8, "every bulk folder is downloaded, unchanged or not"
    assert rows(replayed[SUMMARIES]) == rows(first[SUMMARIES]), "restated from the XML, not carried"
    assert rows(replayed["bill_versions"]) == rows(first["bill_versions"])


def test_rollup_retains_the_replaced_pair_then_writes_fresh_summary_receipts_only(tmp_path, monkeypatch):
    import json

    from spicy_regs.pipelines.rollups import subject_receipts
    from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from tests.test_subject_receipt_rollups import select_bundle

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "R2_PUBLIC_URL"):
        monkeypatch.delenv(key, raising=False)
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first = BillFamilyRollup()
    first.build_receipts(first_dir, build_bill_family, bulk_acquirer=StubBulkAcquirer(), body_acquirer=StubBodyAcquirer())
    select_bundle(monkeypatch, first_dir, first)
    # Treat the selected summary pair as replay-required, as a live /2 selection is.
    monkeypatch.setattr(subject_receipts, "replay_reason",
                        lambda policy: "fixture replay" if policy.dataset == SUMMARIES else None)

    def rollup(directory, *, evidence=True):
        directory.mkdir()
        pipeline = BillFamilyRollup()
        pipeline.source_evidence = (CaptureEvidence(directory, "bill-family", archive_root=tmp_path / "raw")
                                    if evidence else None)
        return pipeline, pipeline.build_receipts(directory, build_bill_family, bulk_acquirer=StubBulkAcquirer(),
                                                 body_acquirer=StubBodyAcquirer(), evidence=pipeline.source_evidence)

    with pytest.raises(ValueError, match="-f congresses=108-119"):
        rollup(tmp_path / "nightly")
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "108-119")
    monkeypatch.delenv("BILL_FAMILY_BILL_TYPES")
    with pytest.raises(ValueError, match="retains source evidence"):
        rollup(tmp_path / "no-evidence", evidence=False)
    pipeline, _ = rollup(tmp_path / "replay")

    events = [json.loads(line) for line in (pipeline.source_evidence.artifact_dir / "journal.jsonl").open()]
    kept = [e for e in events if e.get("stage") == "replaced-prior"]
    assert [(e["dataset"], e["role"]) for e in kept] == [(SUMMARIES, "subject"), (SUMMARIES, "receipts")]
    assert all(e["body_retained"] for e in kept)
    old = {r["receipt_id"] for r in pq.read_table(first_dir / "etl_receipts.parquet").to_pylist()
           if r["dataset"] == SUMMARIES}
    new = [r for r in pq.read_table(tmp_path / "replay" / "etl_receipts.parquet").to_pylist()
           if r["dataset"] == SUMMARIES]
    assert new and not old & {r["receipt_id"] for r in new}
    assert all("prior_receipt" not in r["diagnostic_json"] for r in new)
    assert rows(tmp_path / "replay" / f"{SUMMARIES}.parquet") == rows(first_dir / f"{SUMMARIES}.parquet")
