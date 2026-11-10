"""The hand-run publish command: its dry run on a disk store, its refusals and its public readback."""

import json
import shutil
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import local_publication as command
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import ARCHIVE_REFERENCE, CaptureEvidence
from spicy_regs.sources import publication, r2
from spicy_regs.sources.disk_store import DiskStore
from tests.generation_fakes import Store

FAMILY = "treasury-fiscal"


def generation(tmp_path, name, tables):
    """A complete-family generation of ``tables`` (table name to its ids) with evidence citing one raw original."""
    files = []
    for table, ids in tables.items():
        path = tmp_path / f"{name}-source" / f"{table}.parquet"
        path.parent.mkdir(exist_ok=True)
        pq.write_table(pa.table({"id": ids}), path)
        files.append(path)
    evidence = CaptureEvidence(tmp_path / f"{name}-build", FAMILY)
    evidence.retain_bytes(f"the original {name} was built from".encode(), stage="fixture-original", raw_source=True)
    directory = tmp_path / name
    artifact = build_generation(directory, family=FAMILY, files=files, expected_keys=[path.name for path in files],
                                inputs=evidence.inputs())
    evidence.finish()
    return directory, artifact.pin.artifact_digest.removeprefix("sha256:"), evidence.artifact_dir


def run(tmp_path, name, directory, evidence, *extra, family=FAMILY):
    command.main(["--generation", str(directory), "--family", family, "--output", str(tmp_path / name),
                  *(("--evidence", str(evidence)) if evidence else ()), *extra])
    return tmp_path / name


def test_dry_run_runs_the_publication_guards_on_a_disk_store(tmp_path, capsys, monkeypatch):
    first, first_digest, first_evidence = generation(tmp_path, "first", {"debt": ["a", "b"], "periods": ["p"]})
    store = DiskStore(tmp_path / "live", bucket="live")
    publication.publish_generation(first, client=store, bucket="live", prior_index=publication.empty_index(),
                                   evidence_directories=(first_evidence,))
    live = tmp_path / "publication.v2.json"
    live.write_bytes(store.objects[publication.INDEX_V2_KEY].read_bytes())
    second, second_digest, evidence = generation(
        tmp_path, "second", {"debt": ["a", "b", "c"], "periods": ["p"], "cells": ["x"]})
    dry = ("--dry-run", "--scratch-raw", "--live-index", str(live))
    refused = publication.PublicationError

    with pytest.raises(refused, match="is family treasury-fiscal, not the expected ofac-sanctions"):
        run(tmp_path, "accepted", second, evidence, *dry, family="ofac-sanctions")
    with pytest.raises(refused, match="published with the source evidence it pins"):
        run(tmp_path, "accepted", second, None, *dry)
    # A rehearsal build must say it is one: without --scratch-raw the dry run holds it to the shared archive.
    with pytest.raises(refused, match="Unset SPICYGOV_RAW_ROOT"):
        run(tmp_path, "accepted", second, evidence, "--dry-run", "--live-index", str(live))
    with pytest.raises(refused, match="Family membership changed; explicit migration is required"):
        run(tmp_path, "accepted", second, evidence, *dry)
    with pytest.raises(refused, match=f"Live treasury-fiscal is {first_digest}, not the expected"):
        run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "cells.parquet",
            "--expect-prior", second_digest)
    with monkeypatch.context() as loosened:
        loosened.setenv("R2_MIN_SIZE_RATIO", "0.01")
        with pytest.raises(refused, match="Unset R2_MIN_SIZE_RATIO: a hand publish keeps the default shrink guard"):
            run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "cells.parquet")
    # Every refusal above named the same --output and left nothing there, so the corrected command runs as typed.
    assert not (tmp_path / "accepted").exists()

    output = run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "cells.parquet",
                 "--expect-prior", first_digest[:12])
    receipt = json.loads((output / "dry-run.json").read_text())
    assert json.loads(capsys.readouterr().out) == receipt
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == FAMILY
    assert receipt["artifactDigest"] == "sha256:" + second_digest
    assert receipt["prefix"] == f"generations/{FAMILY}/{second_digest}"
    assert receipt["prior"]["artifactDigest"] == "sha256:" + first_digest
    assert {key: (table["rows"], table["priorRows"]) for key, table in receipt["tables"].items()} == {
        "cells.parquet": (1, None), "debt.parquet": (3, 2), "periods.parquet": (1, 1)}
    assert receipt["added_tables"] == ["cells.parquet"] and receipt["other_family_changes"] == []
    assert receipt["raw_archive"] == "scratch: not publishable" and len(receipt["evidence"]) == 1
    # periods.parquet is byte-identical to the live member, so the publisher copies it server-side.
    assert receipt["store_members_copied"] == 1 and receipt["store_bytes_written"] > 0
    # The store was a rehearsal: only the receipt remains, and the saved live index is as it was.
    assert [path.name for path in output.iterdir()] == ["dry-run.json"]
    assert not list(tmp_path.glob(".dry-run-store-*"))
    assert publication.parse_index(live.read_bytes())["families"][FAMILY]["artifactDigest"] == "sha256:" + first_digest
    with pytest.raises(SystemExit):
        run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "cells.parquet")


def test_publish_holds_the_target_and_raw_root_then_reads_back_publicly(tmp_path, monkeypatch):
    directory, digest, evidence = generation(tmp_path, "candidate", {"debt": ["a", "b"]})
    later, later_digest, later_evidence = generation(tmp_path, "later", {"debt": ["a", "b", "c"]})
    store = Store()
    endpoint = SimpleNamespace(endpoint_url=r2.VERIFIED_ENDPOINT)
    monkeypatch.setattr(store, "meta", endpoint, raising=False)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr("dotenv.load_dotenv", lambda path: None)
    shared = {"root": tmp_path / "central-raw"}  # the archive every test is given: the builds retained there
    monkeypatch.setattr("spicy_docs.storage.raw_archive.default_raw_archive_root", lambda: shared["root"])
    refused = publication.PublicationError

    with pytest.raises(refused, match="Unset SPICYGOV_RAW_ROOT"):
        run(tmp_path, "receipt", directory, evidence)
    monkeypatch.delenv("SPICYGOV_RAW_ROOT")
    # An evidence directory copied away from its build has no pointer: refused, not assumed shared.
    moved = tmp_path / "moved" / "artifact"
    shutil.copytree(evidence, moved)
    with pytest.raises(refused, match="is not beside the source-archive.json its build wrote"):
        run(tmp_path, "receipt", directory, moved)
    (moved.parent / ARCHIVE_REFERENCE).write_text(json.dumps({"root": str(tmp_path / "scratch-raw")}))
    with pytest.raises(refused, match="built on another raw archive than the shared one"):
        run(tmp_path, "receipt", directory, moved)
    # A pointer that names the shared archive is not enough: each cited original must be in it.
    shared["root"] = tmp_path / "another-shared"
    shared["root"].mkdir()
    (moved.parent / ARCHIVE_REFERENCE).write_text(json.dumps({"root": str(shared["root"])}))
    with pytest.raises(refused, match="1 cited original.s. are not in the shared raw archive"):
        run(tmp_path, "receipt", directory, moved)
    shared["root"] = tmp_path / "central-raw"

    with monkeypatch.context() as loosened:
        loosened.setenv("R2_ALLOW_SHRINK", "1")
        with pytest.raises(refused, match="Unset R2_ALLOW_SHRINK"):
            run(tmp_path, "receipt", directory, evidence)
    with pytest.raises(RuntimeError, match="requires R2 credentials"):
        run(tmp_path, "receipt", directory, evidence)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fixture")
    monkeypatch.setattr(endpoint, "endpoint_url", "https://other-account.r2.cloudflarestorage.com")
    with pytest.raises(ValueError, match="differs from the verified target"):
        run(tmp_path, "receipt", directory, evidence)
    assert not any(key.startswith("generations/") or key == publication.INDEX_V2_KEY for key in store.writes)
    assert not (tmp_path / "receipt").exists()

    monkeypatch.setattr(endpoint, "endpoint_url", r2.VERIFIED_ENDPOINT)
    served = []
    monkeypatch.setattr(publication, "load_index", lambda base: served.append(base) or publication.parse_index(
        store.objects[publication.INDEX_V2_KEY]))
    monkeypatch.setattr(publication, "load_family_root", lambda base, entry: publication.family_root(
        store.objects[entry["prefix"] + "/artifact.json"], entry))
    monkeypatch.setattr(publication, "verify_member", lambda base, member: len(store.objects[member.path]))
    receipt = json.loads((run(tmp_path, "receipt", directory, evidence) / "receipt.json").read_text())
    assert receipt["status"] == "published_public_readback" and receipt["prior"] is None
    assert receipt["artifactDigest"] == "sha256:" + digest and receipt["tables"]["debt.parquet"]["rows"] == 2
    assert receipt["public_index_url"] == "https://data.spicygov.ai/publication.v2.json"
    assert receipt["public_bytes_read"] == len(store.objects[receipt["prefix"] + "/debt.parquet"])
    assert served == ["https://data.spicygov.ai"]

    # A rerun after an uncertain result writes nothing: the family already is this generation.
    written = list(store.writes)
    again = json.loads((run(tmp_path, "again", directory, evidence) / "receipt.json").read_text())
    assert again["status"] == "already_published_public_readback" and store.writes == written

    # The family is live now. A build made before someone else's publish must not replace it unseen:
    # the publish needs the digest its dry run compared against, and that digest must still be live.
    with pytest.raises(refused, match="treasury-fiscal is live: pass --expect-prior"):
        run(tmp_path, "later-receipt", later, later_evidence)
    with pytest.raises(refused, match=f"Live treasury-fiscal is {digest}, not the expected prior {later_digest}"):
        run(tmp_path, "later-receipt", later, later_evidence, "--expect-prior", later_digest)
    assert store.writes == written
    replaced = json.loads((run(tmp_path, "later-receipt", later, later_evidence, "--expect-prior", digest[:12])
                           / "receipt.json").read_text())
    assert replaced["status"] == "published_public_readback"
    assert replaced["prior"]["artifactDigest"] == "sha256:" + digest
    assert replaced["tables"]["debt.parquet"] == {**replaced["tables"]["debt.parquet"], "rows": 3, "priorRows": 2}
