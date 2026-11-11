"""Taking whole families out of the publication index: the plan, its refusals, the conditional write, the read-back."""

import json
from types import SimpleNamespace

import pytest
from rulespec_artifacts import canonical_json_bytes

from spicy_regs import generation_retention as retention
from spicy_regs import local_publication as command
from spicy_regs.sources import publication, r2
from tests.generation_fakes import Store
from tests.test_generation_publication import build, publish

refused = publication.PublicationError


@pytest.fixture
def live(tmp_path, monkeypatch):
    """Three published families on the verified target, read publicly from the same store, with no pins."""
    store, index = Store(), publication.empty_index()
    for name, keys in (("gone", ("a.parquet",)), ("also-gone", ("b.parquet",)), ("kept", ("c.parquet",))):
        index = publish(store, build(tmp_path, name, family=name, keys=keys)[0], index)
    monkeypatch.setattr(store, "meta", SimpleNamespace(endpoint_url=r2.VERIFIED_ENDPOINT), raising=False)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fixture")
    monkeypatch.setenv("R2_BUCKET_NAME", r2.VERIFIED_BUCKET)
    monkeypatch.delenv("CLOUDFLARE_API_TOKEN", raising=False)
    monkeypatch.setattr(publication, "load_index_bytes", lambda base: store.objects[publication.INDEX_V2_KEY])
    pins = {retention.DOCSPEC_PINS: {"format": retention.DOCSPEC_PINS_FORMAT, "version": 1, "pins": []},
            retention.ARCHIVE_PINS: {"version": 1, "generations": []}}
    monkeypatch.setattr(retention, "_read_pin_source", lambda source, client=None: json.dumps(pins[source]).encode())
    return SimpleNamespace(store=store, pins=pins)


def unpublish(tmp_path, name, *args):
    command.unpublish_main([*args, "--output", str(tmp_path / name)])
    return json.loads((tmp_path / name / "summary.json").read_text())


def index_bytes(live):
    return live.store.objects[publication.INDEX_V2_KEY]


def test_dry_run_writes_nothing_and_states_what_a_run_would_store(tmp_path, live):
    before, writes = index_bytes(live), list(live.store.writes)

    summary = unpublish(tmp_path, "dry", "gone", "also-gone")

    assert live.store.writes == writes and index_bytes(live) == before
    assert summary["status"] == "dry_run"
    assert summary["index_etag"] == live.store._etag(publication.INDEX_V2_KEY)
    assert summary["index_sha256"] == command._sha256(before)
    assert (summary["families_before"], summary["families_after"]) == (3, 1)
    gone = publication.parse_index(before)["families"]["gone"]
    assert summary["removed"]["gone"] == {
        "artifactDigest": gone["artifactDigest"], "prefix": gone["prefix"], "publishedAt": gone["publishedAt"],
        "tables": {"a.parquet": 1}, "rows": 1}
    planned = (tmp_path / "dry" / "planned.json").read_bytes()
    assert summary["write_sha256"] == command._sha256(planned)
    assert (tmp_path / "dry" / "before.json").read_bytes() == before


def test_execute_removes_only_the_named_families_and_keeps_every_other_entry_byte_for_byte(tmp_path, live):
    before = index_bytes(live)
    dry = unpublish(tmp_path, "dry", "gone", "also-gone")

    done = unpublish(tmp_path, "run", "gone", "also-gone", "--execute", "--expect-etag", dry["index_etag"])

    after = index_bytes(live)
    assert set(publication.parse_index(after)["families"]) == {"kept"}
    assert after == (tmp_path / "dry" / "planned.json").read_bytes()
    kept = canonical_json_bytes(publication.parse_index(before)["families"]["kept"])
    assert kept in after and canonical_json_bytes(publication.parse_index(after)["families"]["kept"]) == kept
    assert done["status"] == "removed" and done["write_sha256"] == dry["write_sha256"]
    assert done["read_back"]["stored"] == {"before": 3, "after": 1, "identical": 1}
    assert done["read_back"]["public_equals_stored"] and done["read_back"]["stored_equals_planned"]
    assert not live.store.deletes  # generations stay stored, so a family can be put back
    for name, entry in dry["removed"].items():
        assert f"{entry['prefix']}/artifact.json" in live.store.objects
        assert publication.restore_family(live.store, "b", name, publication.parse_index(before)["families"][name],
                                          expected=None)["families"][name] == publication.parse_index(
                                              before)["families"][name]
    assert index_bytes(live) == before


def test_a_stale_etag_refuses_and_writes_nothing(tmp_path, live):
    dry = unpublish(tmp_path, "dry", "gone")
    later = publication.parse_index(index_bytes(live))
    later["families"]["kept"]["publishedAt"] = "2026-11-11T00:00:00Z"
    live.store.objects[publication.INDEX_V2_KEY] = canonical_json_bytes(later)
    before, writes = index_bytes(live), list(live.store.writes)

    with pytest.raises(refused, match="not .*nothing was written"):
        unpublish(tmp_path, "run", "gone", "--execute", "--expect-etag", dry["index_etag"])
    assert index_bytes(live) == before and live.store.writes == writes
    assert not (tmp_path / "run").exists()


def test_the_conditional_write_refuses_an_index_changed_after_it_was_read(tmp_path, live):
    raw, etag = index_bytes(live), live.store._etag(publication.INDEX_V2_KEY)
    index = publication.parse_index(raw)
    removal = publication.plan_family_removal(raw, etag, ["gone"], roots=dict.fromkeys(index["families"], {}), held={})
    index["families"]["kept"]["publishedAt"] = "2026-11-11T00:00:00Z"
    live.store.objects[publication.INDEX_V2_KEY] = changed = canonical_json_bytes(index)

    with pytest.raises(refused, match="changed since it was read; nothing was written"):
        publication.write_family_removal(live.store, "b", removal)
    assert index_bytes(live) == changed


def test_execute_needs_the_etag_and_the_etag_needs_execute(tmp_path, live):
    for args in (("--execute",), ("--expect-etag", '"x"')):
        with pytest.raises(SystemExit):
            command.unpublish_main(["gone", *args, "--output", str(tmp_path / "x")])


def test_an_unknown_family_refuses(tmp_path, live):
    with pytest.raises(refused, match="Not in the publication index: missing"):
        unpublish(tmp_path, "dry", "gone", "missing")
    assert not (tmp_path / "dry").exists()


def test_a_family_another_current_generation_reads_as_a_parent_refuses(tmp_path, live):
    raw = index_bytes(live)
    index = publication.parse_index(raw)
    roots = {family: {"spec": {}} for family in index["families"]}
    roots["kept"] = {"spec": {"parents": {"a.parquet": {"family": "gone", "artifactDigest": "sha256:" + "0" * 64}}}}

    with pytest.raises(refused, match=r"gone is a parent of kept \(a.parquet\)"):
        publication.plan_family_removal(raw, "e", ["gone"], roots=roots, held={})
    with pytest.raises(refused, match="Current roots not read.*kept"):
        publication.plan_family_removal(raw, "e", ["gone"], roots={"also-gone": {}}, held={})
    assert publication.plan_family_removal(raw, "e", ["gone", "kept"], roots=roots, held={}).removed.keys() == {
        "gone", "kept"}


def test_a_pinned_family_refuses(tmp_path, live):
    digest = publication.parse_index(index_bytes(live))["families"]["gone"]["artifactDigest"]
    live.pins[retention.DOCSPEC_PINS]["pins"] = [
        {"family": "gone", "artifactDigest": digest, "role": "reader", "cites": "fixture"}]

    with pytest.raises(refused, match=r"gone is held by DocSpec reader pin \(fixture\)"):
        unpublish(tmp_path, "dry", "gone")
    with pytest.raises(refused, match="also-gone is held by a pin naming it"):
        publication.plan_family_removal(index_bytes(live), "e", ["also-gone"], roots={"gone": {}, "kept": {}},
                                        held={"also-gone": "a pin naming it"})


def test_the_read_back_fails_loudly_when_the_families_are_still_there(tmp_path, live, monkeypatch):
    dry = unpublish(tmp_path, "dry", "gone")
    put = live.store.put_object

    def lost(**kwargs):  # the write is acknowledged but the stored index keeps its bytes
        return {"ETag": live.store._etag(kwargs["Key"])} if kwargs["Key"] == publication.INDEX_V2_KEY else put(**kwargs)

    monkeypatch.setattr(live.store, "put_object", lost)
    with pytest.raises(refused, match="Still in the publication index: gone"):
        unpublish(tmp_path, "run", "gone", "--execute", "--expect-etag", dry["index_etag"])


def test_the_check_refuses_any_other_difference(live):
    before = index_bytes(live)
    index = publication.parse_index(before)
    del index["families"]["gone"]
    index["families"]["kept"]["publishedAt"] = "2026-11-11T00:00:00Z"
    with pytest.raises(refused, match="Entries changed besides the removal: kept"):
        publication.check_family_removal(before, canonical_json_bytes(index), ["gone"])
    del index["families"]["kept"]
    with pytest.raises(refused, match="added or lost besides the removal"):
        publication.check_family_removal(before, canonical_json_bytes(index), ["gone"])
