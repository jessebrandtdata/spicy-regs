"""Pins RegulationsPipeline and the run-pipeline CLI.

The real reader → staging → merge composition runs against a fake in-memory S3
resource, alongside manifest incremental dedup, failed-key retry, chunked
comment commits, and publication ordering with the manifest published last.
"""

from json import dumps, loads
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import spicy_regs.pipelines.regulations as regulations
import spicy_docs.sources.mirrulations as mirrulations
from spicy_regs.manifest import Manifest, MissingManifestError
from spicy_regs.pipelines import Pipeline, RegulationsPipeline


from spicy_regs.pipelines.regulatory_publication import restore_checkpoint
from tests.regulatory_publication_fakes import install as install_publication


@pytest.fixture(autouse=True)
def native_publication(monkeypatch):
    remote = install_publication(monkeypatch)
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    return remote


PREFIX = "raw-data"
AGENCY = "EPA"


# --- contract --------------------------------------------------------------


def test_is_pipeline_subclass_with_name() -> None:
    assert issubclass(RegulationsPipeline, Pipeline)
    assert RegulationsPipeline.name == "regulations"


@pytest.mark.parametrize("chunk_size", [0, 1])
def test_reviewed_exclusions_are_checkpointed_without_catalog_writes(tmp_path, monkeypatch, chunk_size):
    """Both ingestion modes retire reviewed keys, without staging or merging their rows."""
    fixture_dir = Path(__file__).parent / "fixtures/reviewed_comments"
    store = {}
    for path in fixture_dir.glob("*.json"):
        raw = path.read_bytes()
        identity = loads(raw)["data"]["id"]
        store[_comment_key(identity, "CFTC-2026-0595", agency="CFTC")] = raw
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    monkeypatch.setattr(regulations.r2, "download", lambda *args, **kwargs: False)

    def forbidden(*args, **kwargs):
        pytest.fail("Reviewed exclusions reached catalog writes")

    monkeypatch.setattr(regulations.iceberg, "merge_comments", forbidden)
    output = tmp_path / "output"
    RegulationsPipeline(
        allow_fresh_start=True,
        agency="CFTC",
        output_dir=output,
        only_comments=True,
        use_iceberg=True,
        enrich_text=False,
        chunk_size=chunk_size,
        skip_upload=True,
    ).run()
    assert set(pl.read_parquet(output / "manifest.parquet")["key"].to_list()) == set(store)
    assert not list(output.glob("staging/comments/*.parquet"))


def test_invalid_comment_staging_refuses_before_any_dataset_merge(tmp_path, monkeypatch):
    """An unsafe docket outside the reviewed exclusions must protect every output."""
    comment = _comment_payload("EPA-2026-0001-0001", "EPA-2026-0001", "2026-01-01")
    comment["data"]["attributes"].update(docketId="../unsafe", title="Test of emission standards")
    store = {
        _docket_key("EPA-2026-0001"): dumps(_docket_payload("EPA-2026-0001", "2026-01-01")).encode(),
        _comment_key("EPA-2026-0001-0001", "EPA-2026-0001"): dumps(comment).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    monkeypatch.setattr(regulations.r2, "download", lambda *args, **kwargs: False)

    def forbidden(*args, **kwargs):
        pytest.fail("Dataset merge began before comment preflight")

    monkeypatch.setattr(regulations, "merge_staging_files", forbidden)
    monkeypatch.setattr(regulations.iceberg, "merge_comments", forbidden)
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="invalid coordinates"):
        RegulationsPipeline(
            allow_fresh_start=True,
            agency=AGENCY,
            output_dir=output,
            use_iceberg=True,
            enrich_text=False,
            skip_upload=True,
        ).run()
    assert not (output / "manifest.parquet").exists()


@pytest.mark.parametrize("sweep", [False, True])
def test_comments_without_the_catalog_refuse_before_the_manifest(tmp_path, monkeypatch, sweep):
    """Comments merge only into the catalog (decision 40); a run without it must not read anything."""
    monkeypatch.setattr(Manifest, "load", lambda *a, **k: pytest.fail("the manifest was read"))
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: pytest.fail("the mirror was read"))
    pipeline = RegulationsPipeline(agency=AGENCY, output_dir=tmp_path, batch_count=1 if sweep else None)
    with pytest.raises(ValueError, match="--use-iceberg"):
        pipeline.run_sweep() if sweep else pipeline.run()


# --- fake S3 ---------------------------------------------------------------


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        pass


class _FakeObj:
    def __init__(self, key: str, content: bytes) -> None:
        self.key = key
        self._content = content

    def get(self) -> dict:
        return {"Body": _FakeBody(self._content)}


class _FakeObjects:
    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store

    def filter(self, Prefix: str):  # noqa: N803 — mirrors boto3 kwarg
        for key, content in self._store.items():
            if key.startswith(Prefix):
                yield _FakeObj(key, content)


class _FakeBucket:
    def __init__(self, store: dict[str, bytes]) -> None:
        self.objects = _FakeObjects(store)


class _FakeS3Resource:
    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store

    def Bucket(self, name: str) -> _FakeBucket:  # noqa: N802 — mirrors boto3 API
        return _FakeBucket(self._store)

    def Object(self, name: str, key: str) -> _FakeObj:  # noqa: N802 — mirrors boto3 API
        return _FakeObj(key, self._store[key])


class _RaisingObj:
    """An S3 object whose body read fails — a transient download error."""

    def get(self) -> dict:
        class _Body:
            def read(self) -> bytes:
                raise OSError("connection reset by peer")

            def close(self) -> None:
                pass

        return {"Body": _Body()}


class _FlakyResource(_FakeS3Resource):
    """Fake S3 that fails ``read()`` for ``fail_keys`` while ``fail["active"]``.

    A shared mutable flag lets one test flip the failure off between runs, so the
    same key can fail on run 1 (excluded from the manifest) and succeed on run 2.
    """

    def __init__(self, store: dict[str, bytes], fail_keys: set[str], active: dict[str, bool]) -> None:
        super().__init__(store)
        self._fail_keys = fail_keys
        self._active = active

    def Object(self, name: str, key: str):  # noqa: N802 — mirrors boto3 API
        if self._active["active"] and key in self._fail_keys:
            return _RaisingObj()
        return _FakeObj(key, self._store[key])


def _docket_payload(docket_id: str, modify_date: str, agency: str = AGENCY) -> dict:
    return {
        "data": {
            "id": docket_id,
            "attributes": {
                "agencyId": agency,
                "title": f"Title {docket_id}",
                "docketType": "Rulemaking",
                "modifyDate": modify_date,
                "dkAbstract": "abstract",
            },
        }
    }


def _docket_key(docket_id: str, tag: str = "a", agency: str = AGENCY) -> str:
    return f"{PREFIX}/{agency}/{docket_id}/text-{docket_id}-{tag}/docket/{docket_id}.json"


def _comment_payload(comment_id: str, docket_id: str, posted_date: str, agency: str = AGENCY) -> dict:
    return {
        "data": {
            "id": comment_id,
            "attributes": {
                "docketId": docket_id,
                "agencyId": agency,
                "postedDate": posted_date,
                "modifyDate": posted_date,
                "comment": "a comment",
            },
        }
    }


def _comment_key(comment_id: str, docket_id: str, agency: str = AGENCY) -> str:
    return f"{PREFIX}/{agency}/{docket_id}/text-{comment_id}/comments/{comment_id}.json"


# --- composition -----------------------------------------------------------


def test_run_extracts_stages_and_merges(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2025-0002"): dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        allow_fresh_start=True,
        agency=AGENCY,
        output_dir=tmp_output,
        skip_comments=True,
        skip_upload=True,
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "EPA-2025-0002"]


def test_run_dedups_on_merge_keeping_latest_modify_date(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Same docket id seen twice with different modify dates -> one row, latest wins.
    store = {
        _docket_key("EPA-2024-0001", "old"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2024-0001", "new"): dumps(_docket_payload("EPA-2024-0001", "2024-09-09")).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        allow_fresh_start=True, agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_upload=True
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert df.height == 1
    assert df["modify_date"].to_list() == ["2024-09-09"]


def test_chunked_comments_commit_per_chunk(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With chunk_size set, comments ingest in bounded key-chunks and the catalog
    MERGE runs once per chunk (so a huge agency commits in pieces, never buffering
    everything in memory)."""
    # 5 comment files, chunk_size 2 -> chunks [2,2,1] -> 3 merges.
    store = {
        _comment_key(f"c{i}", "EPA-2026-0001"): dumps(
            _comment_payload(f"c{i}", "EPA-2026-0001", "2026-01-01T00:00:00Z")
        ).encode()
        for i in range(5)
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    merge_calls: list = []
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda sd, rt: merge_calls.append(1))

    RegulationsPipeline(
        allow_fresh_start=True,
        agency=AGENCY,
        output_dir=tmp_output,
        only_comments=True,
        use_iceberg=True,
        enrich_text=False,
        chunk_size=2,
        skip_upload=True,
    ).run()

    assert len(merge_calls) == 3  # ceil(5 / 2)


def test_run_with_no_records_is_noop(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource({}))

    RegulationsPipeline(
        allow_fresh_start=True, agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_upload=True
    ).run()

    assert not (tmp_output / "dockets.parquet").exists()


# --- incremental dedup -----------------------------------------------------


def _run(tmp_output: Path, **overrides: Any) -> None:
    kwargs: dict[str, Any] = dict(
        agency=AGENCY,
        output_dir=tmp_output,
        skip_comments=True,
        skip_upload=True,
    )
    kwargs.update(overrides)
    RegulationsPipeline(allow_fresh_start=True, **kwargs).run()


def test_second_run_skips_keys_already_in_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2025-0002"): dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    # First run stages + merges, and persists the manifest.
    _run(tmp_output)
    assert pl.read_parquet(tmp_output / "dockets.parquet").height == 2
    reloaded = Manifest.load(tmp_output)
    assert _docket_key("EPA-2024-0001") in reloaded

    # Second run: every key is already in the manifest, so nothing is staged
    # and the merge step must not run.
    merge_calls: list = []
    monkeypatch.setattr(regulations, "merge_staging_files", lambda *a, **k: merge_calls.append(1))
    _run(tmp_output)
    assert merge_calls == []


def test_full_refresh_reprocesses_despite_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = {_docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode()}
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    _run(tmp_output)  # seeds the manifest

    merge_calls: list = []
    real_merge = regulations.merge_staging_files
    monkeypatch.setattr(
        regulations,
        "merge_staging_files",
        lambda *a, **k: (merge_calls.append(1), real_merge(*a, **k))[1],
    )
    _run(tmp_output, full_refresh=True)
    assert merge_calls == [1]  # reprocessed even though the key is in the manifest


def test_run_without_a_manifest_refuses_before_reading_the_mirror(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No published manifest must stop the run, not re-read every agency's history."""
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: pytest.fail("the mirror was read"))

    with pytest.raises(MissingManifestError):
        RegulationsPipeline(agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_upload=True).run()
    with pytest.raises(MissingManifestError):
        RegulationsPipeline(
            agency=AGENCY, output_dir=tmp_output, only_comments=True, use_iceberg=True, chunk_size=2
        ).run()


# --- batching --------------------------------------------------------------


def _batches(agencies: list[str], monkeypatch: pytest.MonkeyPatch, count: int) -> list[list[str]]:
    monkeypatch.setattr(mirrulations, "discover_agencies", lambda: agencies)
    return [RegulationsPipeline(batch_number=n, batch_count=count)._agencies() for n in range(count)]


def test_batch_count_covers_every_discovered_agency_once(monkeypatch: pytest.MonkeyPatch) -> None:
    # 335 agencies were discovered on 2026-09-24; 15 fixed batches of 22 missed five.
    agencies = [f"A{i:03d}" for i in range(335)]
    batches = _batches(agencies, monkeypatch, 15)
    assert [agency for batch in batches for agency in batch] == agencies
    assert max(map(len, batches)) == 23


def test_derived_batch_size_above_the_ceiling_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    agencies = [f"A{i:03d}" for i in range(15 * regulations.MAX_DERIVED_BATCH_SIZE + 1)]
    with pytest.raises(RuntimeError, match="raise BATCH_COUNT"):
        _batches(agencies, monkeypatch, 15)


def test_batch_number_outside_the_batch_count_refuses() -> None:
    with pytest.raises(ValueError, match="outside"):
        RegulationsPipeline(agency=AGENCY, batch_number=15, batch_count=15)._agencies()


# --- failed-key handling ---------------------------------------------------


def test_failed_download_is_not_committed_to_manifest_and_retries_next_run(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient download failure must not be marked processed, so the next
    incremental run re-lists and re-downloads it."""
    good = _docket_key("EPA-2024-0001")
    flaky = _docket_key("EPA-2025-0002")
    store = {
        good: dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        flaky: dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode(),
    }
    active = {"active": True}
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FlakyResource(store, {flaky}, active))

    # Run 1: the flaky key fails every attempt -> only the good docket lands.
    _run(tmp_output)
    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert df["docket_id"].to_list() == ["EPA-2024-0001"]
    reloaded = Manifest.load(tmp_output)
    assert good in reloaded
    assert flaky not in reloaded  # excluded so the next run retries it

    # Run 2: failure cleared -> the previously-failed key is re-listed and lands.
    active["active"] = False
    _run(tmp_output)
    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "EPA-2025-0002"]


def test_parse_failure_is_unresolved_and_recovers(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unreadable bytes stay retryable, with attempt history retained by the host."""
    good = _docket_key("EPA-2024-0001")
    corrupt = _docket_key("EPA-2025-0002")
    store = {
        good: dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        corrupt: b"{ broken json",
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    _run(tmp_output)

    reloaded = Manifest.load(tmp_output)
    assert corrupt not in reloaded
    assert good in reloaded

    failed = pl.DataFrame(
        restore_checkpoint(tmp_output, "failed_keys"),
        schema={
            "agency": pl.String,
            "record_type": pl.String,
            "key": pl.String,
            "status": pl.String,
            "reason": pl.String,
            "attempted_at": pl.String,
            "attempts": pl.Int64,
        },
    )
    row = failed.filter(pl.col("key") == corrupt)
    assert row.height == 1
    assert row["status"].to_list() == ["unreadable"]
    assert row["attempts"].to_list() == [1]
    _run(tmp_output)
    assert pl.DataFrame(
        restore_checkpoint(tmp_output, "failed_keys"),
        schema={
            "agency": pl.String,
            "record_type": pl.String,
            "key": pl.String,
            "status": pl.String,
            "reason": pl.String,
            "attempted_at": pl.String,
            "attempts": pl.Int64,
        },
    )["attempts"].to_list() == [2]
    store[corrupt] = dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode()
    _run(tmp_output)
    assert corrupt in Manifest.load(tmp_output)
    assert pl.DataFrame(
        restore_checkpoint(tmp_output, "failed_keys"),
        schema={
            "agency": pl.String,
            "record_type": pl.String,
            "key": pl.String,
            "status": pl.String,
            "reason": pl.String,
            "attempted_at": pl.String,
            "attempts": pl.Int64,
        },
    ).is_empty()


def test_chunked_comments_exclude_failed_keys_from_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The chunked ingest path excludes transient failures from the manifest too."""
    keys = {f"c{i}": _comment_key(f"c{i}", "EPA-2026-0001") for i in range(3)}
    store = {
        keys[f"c{i}"]: dumps(_comment_payload(f"c{i}", "EPA-2026-0001", "2026-01-01T00:00:00Z")).encode()
        for i in range(3)
    }
    flaky = keys["c1"]
    active = {"active": True}
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FlakyResource(store, {flaky}, active))
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda sd, rt: None)

    pipe = RegulationsPipeline(
        allow_fresh_start=True,
        agency=AGENCY,
        output_dir=tmp_output,
        only_comments=True,
        use_iceberg=True,
        enrich_text=False,
        chunk_size=2,
        skip_upload=True,
    )
    staging_dir = tmp_output / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    manifest = Manifest.empty()
    with pipe._retention(tmp_output) as pipe._run_originals:  # run() owns this outside the test
        pipe._ingest_comments_chunked(AGENCY, tmp_output, staging_dir, manifest)

    recorded = set(pl.read_parquet(tmp_output / "manifest.parquet")["key"])
    assert flaky not in recorded  # excluded -> retried next run
    assert keys["c0"] in recorded
    assert keys["c2"] in recorded
    # The transient failure is surfaced in the local diagnostic.
    failed = pl.DataFrame(
        restore_checkpoint(tmp_output, "failed_keys"),
        schema={
            "agency": pl.String,
            "record_type": pl.String,
            "key": pl.String,
            "status": pl.String,
            "reason": pl.String,
            "attempted_at": pl.String,
            "attempts": pl.Int64,
        },
    )
    assert flaky in failed["key"].to_list()


# --- parallelism -----------------------------------------------------------


def test_processes_multiple_agencies_in_parallel(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENCIES", "EPA,FDA")
    store = {
        _docket_key("EPA-2024-0001", agency="EPA"): dumps(
            _docket_payload("EPA-2024-0001", "2024-01-01", agency="EPA")
        ).encode(),
        _docket_key("FDA-2024-0009", agency="FDA"): dumps(
            _docket_payload("FDA-2024-0009", "2024-02-02", agency="FDA")
        ).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        allow_fresh_start=True,
        output_dir=tmp_output,
        skip_comments=True,
        skip_upload=True,
        max_workers=2,
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "FDA-2024-0009"]


# --- upload ----------------------------------------------------------------


def test_run_does_not_advance_manifest_after_base_upload_failure(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed base-table publication must leave the remote retry checkpoint unchanged."""
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    store = {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    monkeypatch.setattr(regulations.r2, "preflight_uploads", lambda out, files: None)
    monkeypatch.setattr(
        "spicy_regs.sources.publication.publish_generation",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("base upload failed")),
    )
    uploaded: list[tuple[Path, str | None]] = []
    monkeypatch.setattr(
        regulations.r2,
        "upload_file",
        lambda path, remote_key=None: uploaded.append((path, remote_key)),
    )

    with pytest.raises(RuntimeError, match="base upload failed"):
        RegulationsPipeline(
            allow_fresh_start=True,
            agency=AGENCY,
            output_dir=tmp_output,
            skip_comments=True,
            skip_upload=False,
        ).run()

    assert (tmp_output / "manifest.parquet", "manifest.parquet") not in uploaded


def test_catalog_run_publishes_dockets_over_the_selected_generation(tmp_path, monkeypatch, native_publication):
    """A catalog run merges the day's dockets into the published generation, as it does documents.

    The deleted dockets catalog route exported only the day's rows from a never-seeded table:
    45 against 279,719 published, refused by the shrink guard (run 42574758742).
    """
    from tests.test_regulatory_base_families import _publish_source, _published_ids

    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    _publish_source(tmp_path, ["EPA-2024-0001"])
    store = {_docket_key("EPA-2025-0002"): dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode()}
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    monkeypatch.setattr(regulations.r2, "preflight_uploads", lambda out, files: None)
    monkeypatch.setattr(regulations.r2, "upload_file", lambda path, remote_key=None: None)
    RegulationsPipeline(
        allow_fresh_start=True, agency=AGENCY, output_dir=tmp_path / "output", skip_comments=True,
        use_iceberg=True, enrich_text=False, skip_upload=False,
    ).run()
    assert sorted(_published_ids(native_publication)) == ["EPA-2024-0001", "EPA-2025-0002"]


def test_run_preflight_failure_stops_all_publication(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every planned public object must pass its guard before the first write."""
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    store = {
        _comment_key("c1", "EPA-2024-0001"): dumps(
            _comment_payload("c1", "EPA-2024-0001", "2024-01-01T00:00:00Z")
        ).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    checked: list[Path] = []

    def fail_preflight(out: Path, files: list[Path]) -> None:
        checked.extend(files)
        raise RuntimeError("manifest guard failed")

    monkeypatch.setattr(regulations.r2, "preflight_uploads", fail_preflight)
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda sd, rt: 1)
    attempted: list[str] = []
    monkeypatch.setattr(
        regulations.r2,
        "upload_dataset",
        lambda *args, **kwargs: attempted.append("dataset"),
    )
    monkeypatch.setattr(
        regulations.r2,
        "upload_file",
        lambda *args, **kwargs: attempted.append("file"),
    )

    with pytest.raises(RuntimeError, match="manifest guard failed"):
        RegulationsPipeline(
            allow_fresh_start=True,
            agency=AGENCY,
            output_dir=tmp_output,
            only_comments=True,
            use_iceberg=True,
            enrich_text=False,
            skip_upload=False,
        ).run()

    assert tmp_output / "manifest.parquet" in checked
    assert attempted == []


def test_catalog_ingestion_defers_index_until_mirror(tmp_output, monkeypatch):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    store = {
        _comment_key("c1", "EPA-2024-0001"): dumps(
            _comment_payload("c1", "EPA-2024-0001", "2024-01-01T00:00:00Z")
        ).encode()
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda sd, rt: 1)
    uploaded = []
    monkeypatch.setattr(regulations.r2, "preflight_uploads", lambda output_dir, files: None)
    monkeypatch.setattr(regulations.r2, "upload_file", lambda path, remote_key=None: uploaded.append(path.name))
    RegulationsPipeline(
        allow_fresh_start=True,
        agency=AGENCY,
        output_dir=tmp_output,
        only_comments=True,
        use_iceberg=True,
        enrich_text=False,
        skip_upload=False,
    ).run()
    assert "comments_index.parquet" not in uploaded
    assert uploaded[-1] == "manifest.parquet"


# --- CLI -------------------------------------------------------------------


def test_cli_main_builds_and_runs_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    class _FakePipeline:
        def __init__(self, **kwargs) -> None:
            captured["kwargs"] = kwargs

        def run(self) -> None:
            captured["ran"] = True

    monkeypatch.setattr(regulations, "RegulationsPipeline", _FakePipeline)

    regulations.main(agency="EPA", skip_upload=True, since_year=2025)

    assert captured["ran"] is True
    assert captured["kwargs"]["agency"] == "EPA"
    assert captured["kwargs"]["skip_upload"] is True
    assert captured["kwargs"]["since_year"] == 2025


def test_cli_parses_the_scheduled_workflow_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    class _FakePipeline:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def run(self) -> None:
            pass

    monkeypatch.setattr(regulations, "RegulationsPipeline", _FakePipeline)
    monkeypatch.setattr(regulations, "load_dotenv", lambda *a, **k: None)

    with pytest.raises(SystemExit) as exited:
        regulations.app(["--batch-number", "14", "--batch-count", "15", "--no-allow-fresh-start", "--use-iceberg"])

    assert exited.value.code == 0
    assert (captured["batch_number"], captured["batch_count"], captured["allow_fresh_start"]) == (14, 15, False)


def test_scheduled_workflow_derives_batches_and_never_starts_fresh() -> None:
    import yaml

    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/etl-new-pipeline.yml").read_text())
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["allow_fresh_start"]["default"] is False
    assert "use_iceberg" not in inputs  # decision 40: comments ingest only through the catalog
    script = next(
        step["run"] for step in workflow["jobs"]["etl-new"]["steps"] if step.get("name") == "Run new pipeline"
    )
    assert '--batch-count "$BATCH_COUNT"' in script
    assert "--batch-size" not in script
    assert "--no-allow-fresh-start" in script
    assert "ARGS+=(--use-iceberg --defer-comments-publication)" in script


@pytest.mark.parametrize(
    ("event", "batch", "timeout", "outputs"),
    [
        ("schedule", "", "", {"timeout_minutes": "360", "batch": "all"}),
        ("workflow_dispatch", "all", "240", {"timeout_minutes": "240"}),
        ("workflow_dispatch", "14", "", {"timeout_minutes": "120", "batch": "14"}),
        ("workflow_dispatch", "14", "361", None),
        ("workflow_dispatch", "14", "060", None),
        ("workflow_dispatch", "15", "60", None),
    ],
)
def test_workflow_setup_validates_the_dispatch_inputs(tmp_path: Path, event, batch, timeout, outputs) -> None:
    """The setup step's shell, run as written: the timeout and batch it hands the ETL job."""
    import subprocess

    import yaml

    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/etl-new-pipeline.yml").read_text())
    assert workflow["jobs"]["etl-new"]["timeout-minutes"] == "${{ fromJSON(needs.setup.outputs.timeout_minutes) }}"
    output, bin_dir = tmp_path / "github_output", tmp_path / "bin"
    output.touch()
    bin_dir.mkdir()
    (bin_dir / "gh").write_text("#!/bin/sh\n")  # a scheduled sweep asks for the latest sweep: none here
    (bin_dir / "gh").chmod(0o755)
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "EVENT": event,
        "BATCH_NUMBER": batch,
        "TIMEOUT_MINUTES": timeout,
        "SKIP_UPLOAD": "false",
        "BATCH_COUNT": str(workflow["env"]["BATCH_COUNT"]),
        "GITHUB_OUTPUT": str(output),
    }
    [step] = workflow["jobs"]["setup"]["steps"]
    result = subprocess.run(["bash", "-c", step["run"]], env=env, capture_output=True, text=True)

    if outputs is None:
        assert result.returncode == 1
        return
    assert result.returncode == 0, result.stderr
    written = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert outputs.items() <= written.items()
    assert written["run"] == "true"


def test_sweep_reuses_manifest_and_discovers_agencies_once(tmp_output, monkeypatch):
    discoveries, loads, seen = [], [], []
    monkeypatch.delenv("AGENCIES", raising=False)
    monkeypatch.setattr(mirrulations, "discover_agencies", lambda: discoveries.append(1) or ["A", "B", "C"])
    manifest = Manifest.empty()
    monkeypatch.setattr(Manifest, "load", lambda *a, **kw: loads.append(1) or manifest)

    def run(self, *, manifest):
        seen.append((id(manifest), self._agencies()))
        if self.batch_number:
            assert "batch-0" in manifest
        manifest.record([f"batch-{self.batch_number}"])
        manifest.save(tmp_output)

    monkeypatch.setattr(RegulationsPipeline, "run", run)
    pipe = RegulationsPipeline(output_dir=tmp_output, batch_count=2, use_iceberg=True)
    pipe.run_sweep()
    assert discoveries == loads == [1]
    assert seen == [(id(manifest), ["A", "B"]), (id(manifest), ["C"])]
    assert pipe.batch_number is None


def test_failed_sweep_stops_before_later_batch_can_retire_pending_keys(tmp_output, monkeypatch):
    monkeypatch.setenv("AGENCIES", "A,B,C")
    manifest = Manifest.empty()
    monkeypatch.setattr(Manifest, "load", lambda *a, **kw: manifest)
    seen = []

    def run(self, *, manifest):
        seen.append(self.batch_number)
        manifest.record(["pending"])
        raise OSError("checkpoint publication failed")

    monkeypatch.setattr(RegulationsPipeline, "run", run)
    pipe = RegulationsPipeline(output_dir=tmp_output, batch_count=3, use_iceberg=True)
    with pytest.raises(OSError):
        pipe.run_sweep()
    assert seen == [0]
    assert not (tmp_output / "manifest.parquet").exists()
    assert pipe.batch_number is None


@pytest.mark.parametrize("defer", [False, True])
def test_manual_cli_finalizes_once_unless_workflow_owns_finalization(tmp_output, monkeypatch, defer):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    from spicy_regs.pipelines import comments_mirror

    calls = []
    monkeypatch.setattr(RegulationsPipeline, "run", lambda self: calls.append("ingest"))
    monkeypatch.setattr(comments_mirror, "publish_comments_mirror", lambda out: calls.append("mirror"))
    regulations.main(output_dir=tmp_output, use_iceberg=True, skip_upload=False, defer_comments_publication=defer)
    assert calls == (["ingest"] if defer else ["ingest", "mirror"])


def test_skip_upload_keeps_configured_remote_input_authority(tmp_output, monkeypatch, native_publication):
    from spicy_regs.pipelines.regulatory_publication import finish_checkpoints
    from spicy_regs.selected_generations import SelectedInputs
    from spicy_regs.sources import publication

    monkeypatch.setenv('R2_PUBLIC_URL', 'https://test.invalid')
    remote_root = tmp_output / 'publisher'
    inputs = SelectedInputs(remote_root, remote_root / 'selected')
    finish_checkpoints(remote_root, {'failed_keys': [], 'pending_comment_text': []}, publish=True, inputs=inputs)
    before = native_publication.objects[publication.INDEX_V2_KEY]
    pipeline = RegulationsPipeline(output_dir=tmp_output, agency=AGENCY, skip_upload=True, skip_comments=True, enrich_text=False, allow_fresh_start=True)
    observed = []
    monkeypatch.setattr(pipeline, '_run', lambda manifest: observed.append(pipeline._selected_inputs.index))
    pipeline.run()
    assert observed == [publication.parse_index(before)]
    assert native_publication.objects[publication.INDEX_V2_KEY] == before
