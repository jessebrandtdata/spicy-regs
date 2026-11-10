"""Actual Parquet families, interrupted writes and immutable reader snapshots."""

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactVerificationError, canonical_json_bytes, parse_canonical_json, stamp_root

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication as pub, r2
from tests.generation_fakes import Store, error


def build(tmp_path, name="one", family="test", keys=("a.parquet", "b.parquet"), value="one", table=None, **layout):
    """A generation holding ``table`` (by default one ``id`` row of ``value``) at each key, written with ``layout``."""
    source = tmp_path / (name + "-source")
    source.mkdir()
    files = []
    for key in keys:
        path = source / key
        pq.write_table(pa.table({"id": [value]}) if table is None else table, path, store_schema=False, **layout)
        files.append(path)
    target = tmp_path / name
    artifact = build_generation(target, family=family, files=files, expected_keys=keys)
    return target, artifact


def publish(store, path, prior=None):
    return pub.publish_generation(path, client=store, bucket="test", prior_index=prior or pub.empty_index())


#: The pointer publishing wrote before ``publication.v2.json`` was the only one. Nothing writes or reads it now.
RETIRED_POINTER = "publication.json"


def test_complete_generation_then_pointer_and_exact_readback(tmp_path):
    directory, artifact = build(tmp_path)
    store = Store()
    index = publish(store, directory)
    assert store.writes[-1] == pub.INDEX_V2_KEY
    assert index["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    for key in ("a.parquet", "b.parquet"):
        location, info = pub.single_member(index, key).path, pub.table_descriptor(index, key)
        assert info is not None
        assert store.objects[location] == (directory / key).read_bytes()
        assert info["rows"] == 1
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == index


def test_publishing_writes_only_the_version_2_pointer_and_leaves_a_retired_one_alone(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = publish(store, old)
    assert RETIRED_POINTER not in store.objects and RETIRED_POINTER not in store.writes
    store.objects[RETIRED_POINTER] = b"published long ago"
    current = publish(store, new, prior)
    publish(store, sibling, current)
    assert store.writes.count(pub.INDEX_V2_KEY) == 3
    assert store.objects[RETIRED_POINTER] == b"published long ago"
    assert RETIRED_POINTER not in store.writes[store.writes.index(pub.INDEX_V2_KEY) + 1 :]


@pytest.mark.parametrize("failure", ["a.parquet", "b.parquet", "artifact.json", "members.json", pub.INDEX_V2_KEY])
def test_interruption_never_changes_previous_complete_generation(tmp_path, failure):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    old_pointers = {pub.INDEX_V2_KEY: store.objects[pub.INDEX_V2_KEY]}
    old_members = {key: store.objects[pub.single_member(prior, key).path] for key in ("a.parquet", "b.parquet")}

    def interrupt(key):
        if key.endswith(failure):
            raise OSError("interrupted transfer")

    store.before_put = interrupt
    with pytest.raises(OSError, match="interrupted"):
        publish(store, new, prior)
    assert {key: store.objects[key] for key in old_pointers} == old_pointers
    for key, raw in old_members.items():
        assert store.objects[pub.single_member(prior, key).path] == raw


def test_corrupt_uploaded_bytes_never_become_published(tmp_path):
    directory, _ = build(tmp_path)
    store = Store()
    store.corrupt_key = "b.parquet"
    with pytest.raises(ArtifactVerificationError, match="digest"):
        publish(store, directory)
    assert pub.INDEX_V2_KEY not in store.objects


def test_late_shrink_refusal_uploads_nothing(tmp_path):
    directory, _ = build(tmp_path)
    store = Store()
    store.objects["b.parquet"] = b"x" * 100_000
    with pytest.raises(RuntimeError, match="shrink"):
        publish(store, directory)
    assert not store.writes


def test_persistent_contention_refuses_and_preserves_concurrent_writer(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    rivals = []

    def concurrent(key):
        if key == pub.INDEX_V2_KEY:  # semantically same, a new object version on every attempt
            rivals.append(json.dumps(prior, indent=len(rivals)).encode())
            store.objects[key] = rivals[-1]

    store.before_put = concurrent
    with pytest.raises(pub.PublicationError, match="concurrently"):
        publish(store, new, prior)
    assert store.objects[pub.INDEX_V2_KEY] == rivals[-1]
    assert len(rivals) == pub._POINTER_ATTEMPTS


def test_concurrent_sibling_family_is_merged_not_overwritten(tmp_path):
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = publish(store, old)
    rival = publish(Store(), sibling)["families"]["other"]

    def concurrent(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            index = pub.parse_index(store.objects[key])
            index["families"]["other"] = rival
            store.objects[key] = canonical_json_bytes(index)

    store.before_put = concurrent
    published = publish(store, new, prior)
    assert published["families"]["other"] == rival
    assert published["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == published


def test_conditional_request_conflict_is_retried_like_a_refused_pointer_write(tmp_path):
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)

    def conflict(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            raise error("ConditionalRequestConflict")

    store.before_put = conflict
    assert publish(store, new, prior)["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest


def test_concurrent_same_family_commit_refuses_as_stale(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    third, _ = build(tmp_path, "three", value="three")
    store = Store()
    prior = publish(store, old)
    other = Store()
    other.objects = dict(store.objects)
    publish(other, third, prior)
    rival = other.objects[pub.INDEX_V2_KEY]

    def concurrent(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            store.objects[key] = rival

    store.before_put = concurrent
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, new, prior)
    assert store.objects[pub.INDEX_V2_KEY] == rival


def test_stale_family_build_and_cross_family_takeover_refuse(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    other, _ = build(tmp_path, "other", family="other")
    store = Store()
    prior = publish(store, old)
    publish(store, new, prior)
    before = list(store.writes)
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, old, prior)
    with pytest.raises(pub.PublicationError, match="already belongs"):
        publish(store, other)
    assert store.writes == before


def test_independent_family_update_is_preserved(tmp_path):
    one, _ = build(tmp_path)
    two, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    first = publish(store, one)
    second = publish(store, two)
    assert second["families"]["test"] == first["families"]["test"]
    assert set(second["families"]) == {"test", "other"}


def test_missing_extra_changed_and_invalid_outputs_refuse(tmp_path):
    directory, _ = build(tmp_path)
    files = list(directory.glob("*.parquet"))
    with pytest.raises(ValueError, match="declared complete"):
        build_generation(tmp_path / "missing", family="test", files=files[:1], expected_keys=[p.name for p in files])
    with pytest.raises(ValueError, match="declared schema"):
        build_generation(
            tmp_path / "bad-schema",
            family="test",
            files=files,
            expected_keys=[p.name for p in files],
            schemas={"a": [("wrong", "VARCHAR")]},
        )
    (directory / "extra.parquet").write_bytes(files[0].read_bytes())
    with pytest.raises(ArtifactVerificationError, match="undeclared"):
        verify_generation(directory)


def test_successful_empty_table_is_a_member(tmp_path):
    path = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"id": pa.array([], type=pa.string())}), path)
    artifact = build_generation(tmp_path / "empty", family="test", files=[path], expected_keys=[path.name])
    assert artifact.root["spec"]["tables"][path.name]["rows"] == 0


def test_multipart_completion_is_conditional_and_replayable(tmp_path, monkeypatch):
    """Replaying an already-published generation returns the prior index and uploads nothing."""
    monkeypatch.setattr(pub, "PART_BYTES", 100)
    directory, _ = build(tmp_path)
    store = Store()
    prior = publish(store, directory)
    assert not store.uploads
    assert publish(store, directory, prior) == prior
    assert not store.uploads


def test_snapshot_keeps_all_downloads_on_one_generation(tmp_path, monkeypatch):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    calls = []

    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://test/")
        calls.append(key)
        if key not in store.objects:
            yield httpx.Response(404, request=httpx.Request(method, url))
            return
        if key == pub.INDEX_V2_KEY:
            raw = store.objects[key]
            publish(store, new, prior)
        else:
            raw = store.objects[key]
        yield httpx.Response(200, content=raw, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx, "stream", stream)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test")
    with pub.snapshot("https://test"):
        for key in ("a.parquet", "b.parquet"):
            target = tmp_path / key
            assert r2.download(key, target)
            assert pq.read_table(target).to_pylist() == [{"id": "one"}]
    assert calls.count(pub.INDEX_V2_KEY) == 1


def test_managed_download_refuses_corruption_without_replacing_prior(tmp_path, monkeypatch):
    directory, _ = build(tmp_path)
    store = Store()
    index = publish(store, directory)
    monkeypatch.setattr(pub, "load_index", lambda url: index)

    @contextmanager
    def stream(method, url, **kwargs):
        yield httpx.Response(200, content=b"corrupt", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx, "stream", stream)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test")
    target = tmp_path / "held.parquet"
    target.write_bytes(b"prior")
    with pytest.raises(RuntimeError, match="differs from its pin"):
        r2.download("a.parquet", target)
    assert target.read_bytes() == b"prior"


@pytest.mark.parametrize("raw", [b"{}", b'{"format":1,"format":2}', b"null", b"[]", b"x" * (pub.INDEX_LIMIT + 1)])
def test_invalid_index_refuses(raw):
    with pytest.raises(pub.PublicationError):
        pub.parse_index(raw)


def test_readable_footer_with_corrupt_body_is_refused(tmp_path):
    path = tmp_path / "a.parquet"
    pq.write_table(pa.table({"id": ["original", "data", "values"]}), path)
    raw = bytearray(path.read_bytes())
    footer_bytes = int.from_bytes(raw[-8:-4], "little")
    raw[4 : len(raw) - 8 - footer_bytes] = b"\0" * (len(raw) - 12 - footer_bytes)
    path.write_bytes(raw)
    assert pq.read_metadata(path).num_rows == 3
    with pytest.raises((OSError, pa.ArrowInvalid)):
        build_generation(tmp_path / "bad", family="test", files=[path], expected_keys=[path.name])


def test_rollup_ignores_stale_cached_prior_and_keeps_it_as_evidence(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    from spicy_regs.transforms.table_merge import merge_table
    from spicy_regs.sources import cloudflare

    class Ingest(RollupPipeline):
        name = "test"
        output = "a.parquet"

        def build(self, output_dir):
            # This custom path reproduces the cache idiom used by ingest
            # builders outside the shared published_table helper.
            prior = output_dir / "_a_prior.parquet"
            assert prior.exists() or r2.download(self.output, prior)
            return merge_table(
                output_dir,
                name="a",
                columns=("id",),
                identity=("id",),
                version_column=None,
                rows=[],
                remote_key=self.output,
            )

    old, _ = build(tmp_path, "current", keys=("a.parquet",), value="B")
    store = Store()
    index = publish(store, old)
    retained = tmp_path / "run"
    retained.mkdir()
    stale = retained / "_a_prior.parquet"
    pq.write_table(pa.table({"id": ["A"]}), stale)
    stale_bytes = stale.read_bytes()
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setattr(pub, "load_index", lambda url: index)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: None)

    def download(key, path):
        location = pub.single_member(pub.current_index("https://example.test"), key).path
        path.write_bytes(store.objects[location])
        return True

    monkeypatch.setattr(r2, "download", download)
    Ingest(output_dir=retained, skip_upload=False).run()
    current = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    location = pub.single_member(current, "a.parquet").path
    from io import BytesIO

    assert pq.read_table(BytesIO(store.objects[location])).to_pylist() == [{"id": "B"}]
    assert stale.read_bytes() == stale_bytes


def test_unchanged_members_are_copied_not_reuploaded(tmp_path):
    old, _ = build(tmp_path, value="one")
    # A partial-writer-shaped new generation: a changes, b is byte-identical,
    # so the new prefix differs but b's bytes already exist under the old one.
    source = tmp_path / "two-source"
    source.mkdir()
    pq.write_table(pa.table({"id": ["two"]}), source / "a.parquet", store_schema=False)
    pq.write_table(pa.table({"id": ["one"]}), source / "b.parquet", store_schema=False)
    new = tmp_path / "two"
    build_generation(
        new, family="test", files=[source / "a.parquet", source / "b.parquet"], expected_keys=("a.parquet", "b.parquet")
    )
    store = Store()
    prior = publish(store, old)
    publish(store, new, prior=prior)
    index = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    b_loc, b_info = pub.single_member(index, "b.parquet").path, pub.table_descriptor(index, "b.parquet")
    assert b_info is not None
    assert b_info["sha256"] == prior["families"]["test"]["tables"]["b.parquet"]["sha256"]
    assert store.objects[b_loc] == (new / "b.parquet").read_bytes()
    # b was copied server-side from the prior prefix, not uploaded as bytes;
    # a was uploaded under the new prefix.
    assert store.copies == [b_loc]
    a_loc = pub.single_member(index, "a.parquet").path
    assert a_loc in store.writes and a_loc not in store.copies


def test_a_changed_member_still_uploads_bytes(tmp_path):
    old, _ = build(tmp_path, value="one")
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    publish(store, new, prior=prior)
    index = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    for key in ("a.parquet", "b.parquet"):
        assert store.objects[pub.single_member(index, key).path] == (new / key).read_bytes()
    assert store.copies == []


def test_partial_writer_carries_exact_siblings_and_refuses_cold_start(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    from spicy_regs.sources import cloudflare

    class Partial(RollupPipeline):
        name = "partial"
        publication_family = "test"
        output = "a.parquet"

        def build(self, output_dir):
            path = output_dir / self.output
            pq.write_table(pa.table({"id": ["changed"]}), path)
            return path

    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    store = Store()
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(pub, "load_index", lambda url: pub.empty_index())
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: None)
    with pytest.raises(pub.PublicationError, match="complete test"):
        Partial(output_dir=tmp_path / "cold", skip_upload=False).run()
    assert not store.writes
    Partial(output_dir=tmp_path / "candidate", skip_upload=True).run()
    candidate = next((tmp_path / "candidate" / "generations").iterdir())
    assert verify_generation(candidate).root["spec"]["publicationStatus"] == "local-partial"
    with pytest.raises(pub.PublicationError, match="partial candidate"):
        publish(store, candidate)
    assert not store.writes and not store.objects
    old, _ = build(tmp_path)
    index = publish(store, old)
    monkeypatch.setattr(pub, "load_index", lambda url: index)

    def download(key, path):
        path.write_bytes(store.objects[pub.single_member(pub.current_index("https://example.test"), key).path])
        return True

    monkeypatch.setattr(r2, "download", download)
    Partial(output_dir=tmp_path / "warm", skip_upload=False).run()
    current = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert set(current["families"]) == {"test"}
    assert store.objects[pub.single_member(current, "b.parquet").path] == (old / "b.parquet").read_bytes()
    generation = next((tmp_path / "warm" / "generations").iterdir())
    artifact = verify_generation(generation)
    assert artifact.root["spec"]["carriedForward"] == {"b.parquet": index["families"]["test"]["artifactDigest"]}


def test_partial_writer_carries_a_split_sibling_member_for_member(tmp_path, monkeypatch):
    """A family with a split table (the bill family's ``bill_sections``) takes a partial writer too."""
    from spicy_regs.pipelines.rollups.base import RollupPipeline

    class Partial(RollupPipeline):
        name = "partial"
        publication_family = "test"
        output = "a.parquet"

        def build(self, output_dir):
            path = output_dir / self.output
            pq.write_table(pa.table({"id": ["changed"]}), path)
            return path

    source = tmp_path / "source"
    (source / "s" / "congress=118").mkdir(parents=True)
    (source / "s" / "congress=119").mkdir(parents=True)
    pq.write_table(pa.table({"id": ["one"]}), source / "a.parquet")
    for congress in ("118", "119"):
        pq.write_table(pa.table({"id": [congress], "congress": [congress]}),
                       source / "s" / f"congress={congress}" / "part-000000.parquet")
    old = tmp_path / "old"
    build_generation(old, family="test", files=[source / "a.parquet", source / "s"],
                     expected_keys=("a.parquet", "s.parquet"), partitioned={"s.parquet": ("congress",)})
    store = Store()
    index = publish(store, old)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setattr(pub, "load_index", lambda url: index)

    def download_members(key, directory):
        paths = []
        for member in pub.table_members(pub.current_index("https://example.test"), key):
            path = directory / member.key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(store.objects[member.path])
            paths.append(path)
        return paths

    monkeypatch.setattr(r2, "download_members", download_members)
    Partial(output_dir=tmp_path / "warm", skip_upload=True).run()
    artifact = verify_generation(next((tmp_path / "warm" / "generations").iterdir()))
    assert artifact.root["spec"]["carriedForward"] == {}, "a split table is carried member for member, not whole"
    assert artifact.root["spec"]["tables"]["s.parquet"]["partitionColumns"] == ["congress"]
    members = {path.relative_to(old).as_posix(): path.read_bytes() for path in (old / "s").rglob("*.parquet")}
    generation = next((tmp_path / "warm" / "generations").iterdir())
    assert {key: (generation / key).read_bytes() for key in members} == members, "each member's bytes, unchanged"


def test_two_writers_creating_the_index_at_once_both_publish(tmp_path):
    """Both read the index while it is absent; the second to create it loses that write, rereads and merges."""
    first, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    second, _ = build(tmp_path, "third", family="third", keys=("d.parquet",))
    store = Store()

    def rival(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            publish(store, second)

    store.before_put = rival
    published = publish(store, first)
    assert set(published["families"]) == {"other", "third"}
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == published


_INSTANT = "%Y-%m-%dT%H:%M:%SZ"


def _stamp(index: dict, family: str = "test") -> datetime:
    return datetime.strptime(index["families"][family]["publishedAt"], _INSTANT).replace(tzinfo=timezone.utc)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_the_pointer_write_stamps_when_it_moved_the_pointer(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    stamps = []
    prior = None
    for directory in (old, new):
        before = _now().replace(microsecond=0)
        prior = publish(store, directory, prior)
        stored = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
        assert stored == prior
        assert before <= _stamp(stored) <= _now()
        stamps.append(_stamp(stored))
    assert stamps[0] <= stamps[1]


def test_a_build_that_read_a_stamped_or_unstamped_index_still_publishes(tmp_path):
    """``publishedAt`` says when the pointer moved, not what a build read, so it never makes a family look changed.

    The build reads the index through ``parse_index`` (stamp kept), or before a backfill stamped it (stamp absent).
    """
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    third, latest = build(tmp_path, "three", value="three")
    store = Store()
    publish(store, old)
    read = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert "publishedAt" in read["families"]["test"]
    assert publish(store, new, read)["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    unstamped = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    del unstamped["families"]["test"]["publishedAt"]
    assert publish(store, third, unstamped)["families"]["test"]["artifactDigest"] == latest.pin.artifact_digest
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, new, read)


def test_replaying_the_current_generation_keeps_when_the_pointer_moved_to_it(tmp_path, monkeypatch):
    directory, _ = build(tmp_path)
    store = Store()
    prior = publish(store, directory)
    monkeypatch.setattr(pub, "_instant", lambda seconds=None: "2030-01-01T00:00:00Z")
    assert publish(store, directory, prior) == prior
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == prior


def _unstamped(store, *families: str) -> dict:
    """Leave ``store``'s version 2 as written before the writer stamped ``families``; return that index."""
    index = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    for family in families:
        del index["families"][family]["publishedAt"]
    store.objects[pub.INDEX_V2_KEY] = canonical_json_bytes(index)
    return index


def test_backfill_stamps_only_entries_lacking_the_instant_from_their_generation_s_latest_write(tmp_path):
    one, _ = build(tmp_path)
    other, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    publish(store, one)
    published = publish(store, other)
    index = _unstamped(store, "other")
    prefix = index["families"]["other"]["prefix"]
    for key in store.objects:
        if key.startswith(prefix + "/"):
            store.modified[key] = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    # The root is written first; the pointer moves after the last member and its read-back.
    store.modified[f"{prefix}/c.parquet"] = datetime(2026, 9, 1, 12, 15, 2, 750_000, tzinfo=timezone.utc)
    writes = list(store.writes)

    planned = pub.backfill_published_at(store, "test")
    assert planned["stamps"] == {"other": "2026-09-01T12:15:02Z"} and not planned["applied"]
    assert store.writes == writes

    applied = pub.backfill_published_at(store, "test", apply=True)
    assert applied["stamps"] == planned["stamps"] and applied["applied"]
    stored = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert stored["families"]["other"] == {**index["families"]["other"], "publishedAt": "2026-09-01T12:15:02Z"}
    assert stored["families"]["test"] == published["families"]["test"]

    writes = list(store.writes)
    again = pub.backfill_published_at(store, "test", apply=True)
    assert again["stamps"] == {} and not again["applied"] and store.writes == writes


def test_backfill_refuses_when_the_pointer_moved_since_it_read_it(tmp_path):
    one, _ = build(tmp_path)
    two, _ = build(tmp_path, "two", value="two")
    store = Store()
    _unstamped(store, *publish(store, one)["families"])
    prior = pub.parse_index(store.objects[pub.INDEX_V2_KEY])

    moved = []

    def rival(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            moved.append(publish(store, two, prior))

    store.before_put = rival
    with pytest.raises(pub.PublicationError, match="moved since"):
        pub.backfill_published_at(store, "test", apply=True)
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == moved[0]


def test_backfill_refuses_a_generation_without_its_root_or_an_absent_index(tmp_path):
    directory, _ = build(tmp_path)
    store = Store()
    index = _unstamped(store, *publish(store, directory)["families"])
    del store.objects[index["families"]["test"]["prefix"] + "/artifact.json"]
    writes = list(store.writes)
    with pytest.raises(pub.PublicationError, match="no root"):
        pub.backfill_published_at(store, "test", apply=True)
    del store.objects[pub.INDEX_V2_KEY]
    with pytest.raises(pub.PublicationError, match="absent"):
        pub.backfill_published_at(store, "test", apply=True)
    assert store.writes == writes


def test_the_backfill_command_writes_only_with_apply(tmp_path, monkeypatch, capsys):
    from scripts.backfill_published_at import main

    directory, _ = build(tmp_path)
    store = Store()
    _unstamped(store, *publish(store, directory)["families"])
    purged = []
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr("scripts.backfill_published_at.purge_urls", purged.extend)
    monkeypatch.setenv("R2_BUCKET_NAME", "test")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test/")
    writes = list(store.writes)
    assert main([]) == 0
    assert json.loads(capsys.readouterr().out)["applied"] is False and store.writes == writes and not purged
    assert main(["--apply"]) == 0
    assert json.loads(capsys.readouterr().out)["applied"] is True and store.writes == [*writes, pub.INDEX_V2_KEY]
    assert "publishedAt" in pub.parse_index(store.objects[pub.INDEX_V2_KEY])["families"]["test"]
    assert purged == [f"https://test/{pub.INDEX_V2_KEY}"]
    assert main(["--apply"]) == 0 and json.loads(capsys.readouterr().out)["applied"] is False and len(purged) == 1


def test_staging_complete_generations_keeps_both_unselected(tmp_path):
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    store = Store()
    for family, key in (("first", "a.parquet"), ("second", "b.parquet")):
        directory, artifact = build(tmp_path, name=family, family=family, keys=(key,))
        source = LocalMemberSource(directory)
        entry = pub._stage_verified_generation(
            artifact, source, client=store, bucket="test", index=pub.empty_index(),
            members=tuple(iter_member_descriptors(artifact, source)),
            upload_member=lambda prefix, member: pub._put_immutable(
                store, "test", prefix + "/" + member, directory / member),
        )
        assert store.objects[entry["prefix"] + "/" + key] == (directory / key).read_bytes()
        assert pub.INDEX_V2_KEY not in store.objects


@pytest.mark.parametrize("predating_ok", [False, True])
@pytest.mark.parametrize("change", [None, "outcome", "kind", "version", "unversioned"])
def test_inherited_evidence_journal_requires_current_completed_evidence(tmp_path, monkeypatch, change, predating_ok):
    """The inherited root is admitted before any member is requested: current kind, completed build, current version.

    A caller that inherits nothing (a native conversion) may let evidence from before version 2 pass as an empty journal.
    """
    base = "https://test.invalid"
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.inherit(pub.empty_index(), public_url=None)
    evidence.event("congress-index-selection", table="t", unevidenced=[])
    evidence.seal(outcome="build-complete")
    directory = evidence.artifact_dir
    root = parse_canonical_json((directory / "artifact.json").read_bytes())
    if change == "outcome":
        root["spec"]["outcome"] = "failed"
    elif change == "kind":
        root["kind"] = "spicy-regs-rollup-generation"
    elif change == "version":
        root["spec"]["evidence_version"] = 1
    elif change == "unversioned":
        del root["spec"]["evidence_version"]
    root = stamp_root(root)
    prefix = f"{pub.EVIDENCE_PREFIX}/{root['artifactDigest'].removeprefix('sha256:')}"
    objects = {
        f"{prefix}/artifact.json": canonical_json_bytes(root),
        **{f"{prefix}/{name}": (directory / name).read_bytes() for name in ("members.json", "journal.jsonl")},
    }
    requested = []

    def get(url, **_):
        requested.append(url.removeprefix(base + "/"))
        return objects.get(requested[-1])

    monkeypatch.setattr(pub, "_bounded_get", get)
    pin = {"logicalId": root["logicalId"], "artifactDigest": root["artifactDigest"]}
    if change is None:
        assert b"congress-index-selection" in pub.load_evidence_journal(base, pin, predating_ok=predating_ok)
        return
    if predating_ok and change in ("version", "unversioned"):
        assert pub.load_evidence_journal(base, pin, predating_ok=True) == b""
        assert requested == [f"{prefix}/artifact.json"]
        return
    message = (
        "Only completed-build evidence" if change in ("outcome", "kind") else "Unsupported source evidence version"
    )
    with pytest.raises(pub.PublicationError, match=message):
        pub.load_evidence_journal(base, pin, predating_ok=predating_ok)
    assert requested == [f"{prefix}/artifact.json"]


def test_a_generation_scoped_outcome_table_may_shrink_but_a_data_table_may_not(tmp_path, monkeypatch):
    """print_citations_document_file_outcomes recorded producer footers: 60,464 bytes became 25,137 with no row lost.

    Its size follows the writer that made the members (an ``ARROW:schema`` footer per member), so the byte-size guard
    exempts the per-generation file-outcome tables and nothing else.
    """
    from spicy_regs import etl_policy_registry

    monkeypatch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
    outcomes = "print_citations_document_file_outcomes.parquet"
    directory, _ = build(tmp_path, keys=("a.parquet", outcomes))
    store = Store()
    store.objects[outcomes] = b"x" * 100_000
    publish(store, directory)
    other, _ = build(tmp_path, "two", keys=("a.parquet", "b.parquet"))
    store = Store()
    store.objects["b.parquet"] = b"x" * 100_000
    with pytest.raises(RuntimeError, match="shrink"):
        publish(store, other)


@pytest.mark.parametrize("key, shrinks", [
    ("print_citations_document_file_outcomes.parquet", False),
    ("a.parquet", True),
])
def test_a_smaller_generation_over_a_published_prior_is_guarded_only_for_data_tables(tmp_path, monkeypatch, key, shrinks):
    """The live failure path: the prior generation's recorded size, not a probe of the remote object, is the comparison.

    The smaller generation also holds one row fewer. An outcome table's rows follow its generation's producer members,
    so it may lose rows and bytes; a data table that lost a row is compared by size.
    """
    from spicy_regs import etl_policy_registry

    monkeypatch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
    large, _ = build(tmp_path, keys=(key,), table=pa.table({"id": [os.urandom(50_000).hex(), "kept"]}))
    store = Store()
    prior = publish(store, large)
    small, _ = build(tmp_path, "two", keys=(key,), value="two")
    if shrinks:
        with pytest.raises(RuntimeError, match="shrink"):
            publish(store, small, prior)
    else:
        assert key in publish(store, small, prior)["families"]["test"]["tables"]


def _ids(rows: int) -> list[str]:
    return [f"C{number:08d}" for number in range(rows)]


def _smaller_over_prior(tmp_path, old: pa.Table, new: pa.Table):
    """Publish ``old`` at one row per row group, then build ``new`` in one group: the store, prior index and candidate.

    The fec_committees refusal in small: the writer's row-group count, not the data, sets most of the size (1,767
    groups became 176 and 10.2 MB became 4.0 MB with all 89,974 rows). The candidate is asserted under half the prior's
    bytes, so no caller passes or refuses for want of a real shrink past the 0.5 threshold.
    """
    large, _ = build(tmp_path, keys=("a.parquet",), table=old, row_group_size=1)
    small, artifact = build(tmp_path, "two", keys=("a.parquet",), table=new)
    assert (small / "a.parquet").stat().st_size * 2 < (large / "a.parquet").stat().st_size
    store = Store()
    return store, publish(store, large), small, artifact


@pytest.mark.parametrize("rows, publishes", [(400, True), (401, True), (399, False)])
def test_a_table_under_half_its_prior_bytes_publishes_only_if_it_kept_its_row_count(tmp_path, rows, publishes):
    """Ticket ss-kqzz. Equal or more rows with the same columns publish; one row fewer is the loss the guard is for."""
    store, prior, small, _ = _smaller_over_prior(tmp_path, pa.table({"id": _ids(400)}), pa.table({"id": _ids(rows)}))
    held = pub.table_descriptor(prior, "a.parquet")
    assert held is not None and (held["rows"], held["columns"]) == (400, [["id", "VARCHAR"]])
    if publishes:
        table = pub.table_descriptor(publish(store, small, prior), "a.parquet")
        assert table is not None and (table["rows"], table["columns"]) == (rows, [["id", "VARCHAR"]])
        assert table["byteSize"] == (small / "a.parquet").stat().st_size
        return
    writes = list(store.writes)
    with pytest.raises(RuntimeError, match=r"Refusing to upload a\.parquet: new file would shrink remote"):
        publish(store, small, prior)
    assert store.writes == writes


@pytest.mark.parametrize("columns, recorded", [
    ({"id": _ids(400)}, [["id", "VARCHAR"]]),  # removed
    ({"id": _ids(400), "n": [str(n) for n in range(400)]}, [["id", "VARCHAR"], ["n", "VARCHAR"]]),  # retyped
    ({"id": _ids(400), "m": list(range(400))}, [["id", "VARCHAR"], ["m", "BIGINT"]]),  # renamed: same width and types
])
def test_a_table_that_kept_its_rows_but_not_its_columns_is_still_compared_by_size(tmp_path, columns, recorded):
    """No earlier publication check compares an unregistered table's columns with its prior's; this guard owns them."""
    old = pa.table({"id": _ids(400), "n": list(range(400))})
    store, prior, small, artifact = _smaller_over_prior(tmp_path, old, pa.table(columns))
    held = pub.table_descriptor(prior, "a.parquet")
    assert held is not None and (held["rows"], held["columns"]) == (400, [["id", "VARCHAR"], ["n", "BIGINT"]])
    assert artifact.root["spec"]["tables"]["a.parquet"] == {"rows": 400, "columns": recorded}
    with pytest.raises(RuntimeError, match="shrink"):
        publish(store, small, prior)


@pytest.mark.parametrize("absent", [None, "rows", "columns"])
def test_a_prior_descriptor_recording_no_rows_or_columns_is_compared_by_size(tmp_path, absent):
    """``parse_index`` admits no such descriptor, so the index is edited by hand: the byte check applies, no KeyError.

    With nothing removed the same call stages, so the refusals come from the missing field alone.
    """
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    table = pa.table({"id": _ids(400)})
    store, prior, small, artifact = _smaller_over_prior(tmp_path, table, table)
    index = json.loads(json.dumps(prior))
    if absent is not None:
        del index["families"]["test"]["tables"]["a.parquet"][absent]
    source = LocalMemberSource(small)

    def stage():
        return pub._stage_verified_generation(
            artifact, source, client=store, bucket="test", index=index,
            members=tuple(iter_member_descriptors(artifact, source)),
            upload_member=lambda prefix, member: pub._put_immutable(
                store, "test", prefix + "/" + member, small / member),
        )

    if absent is None:
        assert stage()["tables"]["a.parquet"]["rows"] == 400
    else:
        with pytest.raises(RuntimeError, match="shrink"):
            stage()


def test_the_guard_cannot_see_values_emptied_in_rows_that_were_kept(tmp_path):
    """The stated limit of the rule: descriptors record counts, names and types, never values.

    Before ss-kqzz the size comparison refused this only because the bytes halved. If a check that reads values is
    added to publication, this expectation should flip.
    """
    store, prior, small, _ = _smaller_over_prior(tmp_path, pa.table({"id": _ids(400)}), pa.table({"id": [""] * 400}))
    table = pub.table_descriptor(publish(store, small, prior), "a.parquet")
    assert table is not None and (table["rows"], table["columns"]) == (400, [["id", "VARCHAR"]])
