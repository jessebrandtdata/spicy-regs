"""The hand-run publish command: its dry run on a disk store, its refusals and its public readback."""

import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import local_publication as command
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import ARCHIVE_REFERENCE
from spicy_regs.sources import publication, r2
from spicy_regs.sources.disk_store import DiskStore
from tests.generation_fakes import Store

FAMILY = "treasury-fiscal"


def generation(tmp_path, name, tables):
    """A complete-family generation of ``tables`` (table name to its ids), built the ordinary way."""
    files = []
    for table, ids in tables.items():
        path = tmp_path / f"{name}-source" / f"{table}.parquet"
        path.parent.mkdir(exist_ok=True)
        pq.write_table(pa.table({"id": ids}), path)
        files.append(path)
    directory = tmp_path / name
    artifact = build_generation(directory, family=FAMILY, files=files, expected_keys=[path.name for path in files])
    return directory, artifact.pin.artifact_digest.removeprefix("sha256:")


def run(tmp_path, name, directory, *extra, family=FAMILY):
    command.main(["--generation", str(directory), "--family", family, "--output", str(tmp_path / name), *extra])
    return tmp_path / name


def test_dry_run_runs_the_publication_guards_on_a_disk_store(tmp_path, capsys):
    first, first_digest = generation(tmp_path, "first", {"debt": ["a", "b"]})
    store = DiskStore(tmp_path / "live", bucket="live")
    publication.publish_generation(first, client=store, bucket="live", prior_index=publication.empty_index())
    live = tmp_path / "publication.v2.json"
    live.write_bytes(store.objects[publication.INDEX_V2_KEY].read_bytes())
    second, second_digest = generation(tmp_path, "second", {"debt": ["a", "b", "c"], "cells": ["x"]})
    dry = ("--dry-run", "--live-index", str(live))

    with pytest.raises(publication.PublicationError, match="is family treasury-fiscal, not the expected ofac-sanctions"):
        run(tmp_path, "wrong-family", second, *dry, family="ofac-sanctions")
    with pytest.raises(publication.PublicationError, match="Family membership changed; explicit migration is required"):
        run(tmp_path, "no-migration", second, *dry)
    with pytest.raises(publication.PublicationError, match=f"Live treasury-fiscal is {first_digest}, not the expected"):
        run(tmp_path, "stale-prior", second, *dry, "--added-table", "cells.parquet", "--expect-prior", second_digest)

    output = run(tmp_path, "accepted", second, *dry, "--added-table", "cells.parquet",
                 "--expect-prior", first_digest[:12])
    receipt = json.loads((output / "dry-run.json").read_text())
    assert json.loads(capsys.readouterr().out) == receipt
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == FAMILY
    assert receipt["artifactDigest"] == "sha256:" + second_digest
    assert receipt["prefix"] == f"generations/{FAMILY}/{second_digest}"
    assert receipt["prior"]["artifactDigest"] == "sha256:" + first_digest
    assert {key: (table["rows"], table["priorRows"]) for key, table in receipt["tables"].items()} == {
        "cells.parquet": (1, None), "debt.parquet": (3, 2)}
    assert receipt["added_tables"] == ["cells.parquet"] and receipt["other_family_changes"] == []
    assert receipt["store_bytes_written"] > 0
    # The store was a rehearsal: only the receipt remains, and the saved live index is as it was.
    assert [path.name for path in output.iterdir()] == ["dry-run.json"]
    assert publication.parse_index(live.read_bytes())["families"][FAMILY]["artifactDigest"] == "sha256:" + first_digest


def test_publish_holds_the_target_and_raw_root_then_reads_back_publicly(tmp_path, monkeypatch):
    directory, digest = generation(tmp_path, "candidate", {"debt": ["a", "b"]})
    store = Store()
    endpoint = SimpleNamespace(endpoint_url=r2.VERIFIED_ENDPOINT)
    monkeypatch.setattr(store, "meta", endpoint, raising=False)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr("dotenv.load_dotenv", lambda path: None)
    shared = tmp_path / "shared-raw"
    monkeypatch.setattr("spicy_docs.storage.raw_archive.default_raw_archive_root", lambda: shared)
    evidence = tmp_path / "evidence" / "artifact"
    evidence.mkdir(parents=True)
    (evidence.parent / ARCHIVE_REFERENCE).write_text(json.dumps({"root": str(tmp_path / "scratch-raw")}))

    with pytest.raises(publication.PublicationError, match="Unset SPICYGOV_RAW_ROOT"):
        run(tmp_path, "scratch-env", directory)
    monkeypatch.delenv("SPICYGOV_RAW_ROOT")
    with pytest.raises(publication.PublicationError, match="built on another raw archive than the shared one"):
        run(tmp_path, "scratch-evidence", directory, "--evidence", str(evidence))
    with pytest.raises(RuntimeError, match="requires R2 credentials"):
        run(tmp_path, "no-credentials", directory)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fixture")
    monkeypatch.setattr(endpoint, "endpoint_url", "https://other-account.r2.cloudflarestorage.com")
    with pytest.raises(ValueError, match="differs from the verified target"):
        run(tmp_path, "other-account", directory)
    assert store.writes == []

    monkeypatch.setattr(endpoint, "endpoint_url", r2.VERIFIED_ENDPOINT)
    served = []
    monkeypatch.setattr(publication, "load_index", lambda base: served.append(base) or publication.parse_index(
        store.objects[publication.INDEX_V2_KEY]))
    monkeypatch.setattr(publication, "load_family_root", lambda base, entry: publication.family_root(
        store.objects[entry["prefix"] + "/artifact.json"], entry))
    monkeypatch.setattr(publication, "verify_member", lambda base, member: len(store.objects[member.path]))
    receipt = json.loads((run(tmp_path, "published", directory) / "receipt.json").read_text())
    assert receipt["status"] == "published_public_readback" and receipt["prior"] is None
    assert receipt["artifactDigest"] == "sha256:" + digest and receipt["tables"]["debt.parquet"]["rows"] == 2
    assert receipt["public_index_url"] == "https://data.spicygov.ai/publication.v2.json"
    assert receipt["public_bytes_read"] == len(store.objects[receipt["prefix"] + "/debt.parquet"])
    assert served == ["https://data.spicygov.ai"]

    # A rerun after an uncertain result writes nothing: the family already is this generation.
    written = list(store.writes)
    again = json.loads((run(tmp_path, "again", directory, "--expect-prior", "0" * 8) / "receipt.json").read_text())
    assert again["status"] == "already_published_public_readback" and store.writes == written
