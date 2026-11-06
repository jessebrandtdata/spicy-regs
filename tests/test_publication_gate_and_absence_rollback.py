"""Observer gates follow staging; guarded bootstrap rollback preserves concurrent families."""

from copy import deepcopy

import pytest
from rulespec_artifacts import canonical_json_bytes

from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_generation_publication import build, publish


def test_public_gate_runs_after_staging_and_before_publication_time(tmp_path, monkeypatch):
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    prior_bytes = store.objects[pub.INDEX_V2_KEY]
    clock = {"now": "2026-11-06T10:00:00Z"}
    monkeypatch.setattr(pub, "_instant", lambda: clock["now"])
    gates = []

    def ready():
        assert store.objects[pub.INDEX_V2_KEY] == prior_bytes
        prefix = "generations/test/" + artifact.pin.artifact_digest.removeprefix("sha256:")
        for key in ("artifact.json", "members.json", "a.parquet", "b.parquet"):
            assert store.objects[f"{prefix}/{key}"] == (new / key).read_bytes()
        gates.append("staged")
        clock["now"] = "2026-11-06T10:05:00Z"

    result = pub.publish_generation(new, client=store, bucket="test", prior_index=prior, before_pointer=ready)

    assert gates == ["staged"]
    assert result["families"]["test"]["publishedAt"] == "2026-11-06T10:05:00Z"


def test_gate_failure_retains_staging_and_the_prior_pointer(tmp_path):
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    prior_bytes = store.objects[pub.INDEX_V2_KEY]

    def refused():
        raise RuntimeError("observer baseline unavailable")

    with pytest.raises(RuntimeError, match="observer baseline unavailable"):
        pub.publish_generation(new, client=store, bucket="test", prior_index=prior, before_pointer=refused)

    assert store.objects[pub.INDEX_V2_KEY] == prior_bytes
    prefix = "generations/test/" + artifact.pin.artifact_digest.removeprefix("sha256:")
    assert f"{prefix}/a.parquet" in store.objects
    assert not store.deletes


def test_one_shot_gate_and_fresh_timestamp_survive_a_sibling_race(tmp_path, monkeypatch):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    other, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = publish(store, old)
    sibling = publish(Store(), other)["families"]["other"]
    clock = {"now": "2026-11-06T10:00:00Z"}
    monkeypatch.setattr(pub, "_instant", lambda: clock["now"])
    calls, gates = [], []

    def ready_once():
        calls.append("callback")
        if not gates:
            gates.append("baseline and cooldown ready")
            clock["now"] = "2026-11-06T10:05:00Z"

    def sibling_race(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            current = pub.parse_index(store.objects[key])
            current["families"]["other"] = sibling
            store.objects[key] = canonical_json_bytes(current)
            clock["now"] = "2026-11-06T10:05:01Z"

    store.before_put = sibling_race
    result = pub.publish_generation(new, client=store, bucket="test", prior_index=prior, before_pointer=ready_once)

    assert calls == ["callback", "callback"]
    assert len(gates) == 1
    assert result["families"]["other"] == sibling
    assert result["families"]["test"]["publishedAt"] == "2026-11-06T10:05:01Z"


def test_guarded_rollback_restores_absence_and_retains_all_immutable_objects(tmp_path):
    first, _ = build(tmp_path)
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    current = publish(store, first)
    current = publish(store, sibling, current)
    expected = current["families"]["test"]
    objects = {key: value for key, value in store.objects.items() if key != pub.INDEX_V2_KEY}

    result = pub.restore_family(store, "test", "test", None, expected=expected)

    assert "test" not in result["families"]
    assert result["families"]["other"] == current["families"]["other"]
    assert {key: value for key, value in store.objects.items() if key != pub.INDEX_V2_KEY} == objects
    assert not store.deletes
    writes = list(store.writes)
    assert pub.restore_family(store, "test", "test", None, expected=expected) == result
    assert store.writes == writes


def test_absence_rollback_requires_a_recorded_release(tmp_path):
    first, _ = build(tmp_path)
    store = Store()
    publish(store, first)
    objects = dict(store.objects)

    with pytest.raises(pub.PublicationError, match="recorded expected release"):
        pub.restore_family(store, "test", "test", None, expected=None)

    assert store.objects == objects


@pytest.mark.parametrize("changed", ["artifactDigest", "publishedAt"])
def test_absence_rollback_refuses_a_different_expected_release(tmp_path, changed):
    first, _ = build(tmp_path)
    store = Store()
    current = publish(store, first)
    expected = deepcopy(current["families"]["test"])
    expected[changed] = "sha256:" + "f" * 64 if changed == "artifactDigest" else "2000-01-01T00:00:00Z"
    objects = dict(store.objects)

    with pytest.raises(pub.PublicationError, match="another generation"):
        pub.restore_family(store, "test", "test", None, expected=expected)

    assert store.objects == objects


def test_absence_rollback_preserves_a_concurrent_sibling_update(tmp_path):
    first, _ = build(tmp_path)
    other, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    current = publish(store, first)
    sibling = publish(Store(), other)["families"]["other"]

    def sibling_race(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            latest = pub.parse_index(store.objects[key])
            latest["families"]["other"] = sibling
            store.objects[key] = canonical_json_bytes(latest)

    store.before_put = sibling_race
    result = pub.restore_family(store, "test", "test", None, expected=current["families"]["test"])

    assert "test" not in result["families"]
    assert result["families"]["other"] == sibling
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == result
    assert not store.deletes


def test_absence_rollback_does_not_remove_a_concurrent_new_release(tmp_path):
    first, _ = build(tmp_path)
    next_release, _ = build(tmp_path, "two", value="two")
    store = Store()
    current = publish(store, first)
    newer = publish(Store(), next_release)["families"]["test"]

    def family_race(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            latest = pub.parse_index(store.objects[key])
            latest["families"]["test"] = newer
            store.objects[key] = canonical_json_bytes(latest)

    store.before_put = family_race
    with pytest.raises(pub.PublicationError, match="another generation"):
        pub.restore_family(store, "test", "test", None, expected=current["families"]["test"])

    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY])["families"]["test"] == newer
    assert not store.deletes
