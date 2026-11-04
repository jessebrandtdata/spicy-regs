"""Bounded registry metadata observation; no daemon, image build or layer pull."""

from copy import deepcopy
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "deploy/cloudflare/observe_image.py"
spec = importlib.util.spec_from_file_location("image_observer", SCRIPT)
assert spec is not None and spec.loader is not None
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


def arguments(tmp_path, selected, raw, config, runtime):
    values = {"selection": encoded(selected), "runtime": encoded(runtime), "manifest": raw, "config": config}
    for name, value in values.items():
        (tmp_path / name).write_bytes(value)
    return [
        "--selection",
        str(tmp_path / "selection"),
        "--selection-sha256",
        observer.sha(values["selection"]),
        "--runtime-consumer",
        str(tmp_path / "runtime"),
        "--runtime-consumer-sha256",
        observer.sha(values["runtime"]),
        "--output",
        str(tmp_path / "output.json"),
    ]


@pytest.fixture
def image():
    config = {
        "architecture": "amd64",
        "os": "linux",
        "config": {"Cmd": ["reader"]},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "1" * 64, "sha256:" + "2" * 64]},
    }
    config_raw = encoded(config)
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": observer.sha(config_raw),
            "size": len(config_raw),
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "size": 100 + i,
                "digest": "sha256:" + str(i + 3) * 64,
            }
            for i in range(2)
        ],
    }
    raw = encoded(manifest)
    digest = observer.sha(raw)
    runtime = {
        "image_digest": digest,
        "code_sha256": "sha256:" + "a" * 64,
        "package_versions": {"spicy-regs": "test", "duckdb": "test"},
    }
    selected = {
        "registry_image": observer.REGISTRY + "@" + digest,
        "registry_manifest_digest": digest,
        "config_digest": observer.sha(config_raw),
        "consumer": runtime,
    }
    return selected, raw, config_raw, deepcopy(runtime)


def test_exact_observation_needs_no_local_tag_or_repodigests(image):
    selected, raw, config, runtime = image
    report = observer.associate(selected, raw, config, runtime)
    assert report["status"] == "pass"
    assert report["ordered_manifest_layers"] == json.loads(raw)["layers"]
    assert report["ordered_rootfs_diff_ids"] == json.loads(config)["rootfs"]["diff_ids"]
    assert report["ordered_manifest_layers"][0]["digest"] != report["ordered_rootfs_diff_ids"][0]


@pytest.mark.parametrize(
    "change",
    [
        "manifest_bytes",
        "config_bytes",
        "runtime_image",
        "runtime_code",
        "runtime_package",
        "registry_account",
        "selected_config",
    ],
)
def test_mismatches_refuse_without_any_output(image, change, tmp_path):
    selected, raw, config, runtime = image
    if change == "manifest_bytes":
        raw += b"\n"
    elif change == "config_bytes":
        config += b"\n"
    elif change == "runtime_image":
        runtime["image_digest"] = "sha256:" + "9" * 64
    elif change == "runtime_code":
        runtime["code_sha256"] = "sha256:" + "9" * 64
    elif change == "runtime_package":
        runtime["package_versions"]["duckdb"] = "other"
    elif change == "registry_account":
        selected["registry_image"] = selected["registry_image"].replace(observer.ACCOUNT, "0" * 32)
    else:
        selected["config_digest"] = "sha256:" + "9" * 64
    argv = arguments(tmp_path, selected, raw, config, runtime)
    with pytest.raises(SystemExit) as refused:
        observer.main([*argv, "--manifest", str(tmp_path / "manifest"), "--config", str(tmp_path / "config")])
    assert refused.value.code == 2
    assert not (tmp_path / "output.json").exists()


def test_reordered_diffids_refuse_even_with_other_config_fields_unchanged(image):
    selected, raw, config, runtime = image
    changed = json.loads(config)
    changed["rootfs"]["diff_ids"].reverse()
    with pytest.raises(ValueError, match="config bytes differ"):
        observer.associate(selected, raw, encoded(changed), runtime)


@pytest.mark.parametrize("change", ["platform", "rootfs", "layer_count", "bool_size"])
def test_bad_internal_association_refuses_with_self_consistent_byte_pins(image, change):
    selected, raw, config, runtime = image
    manifest, cfg = json.loads(raw), json.loads(config)
    if change == "platform":
        cfg["architecture"] = "arm64"
    elif change == "rootfs":
        cfg["rootfs"] = None
    elif change == "layer_count":
        manifest["layers"].pop()
    else:
        manifest["layers"][0]["size"] = True
    config = encoded(cfg)
    manifest["config"].update(digest=observer.sha(config), size=len(config))
    raw = encoded(manifest)
    digest = observer.sha(raw)
    selected.update(
        config_digest=observer.sha(config),
        registry_manifest_digest=digest,
        registry_image=observer.REGISTRY + "@" + digest,
    )
    selected["consumer"]["image_digest"] = digest
    runtime["image_digest"] = digest
    with pytest.raises(ValueError):
        observer.associate(selected, raw, config, runtime)


def test_prepublication_runtime_is_explicit_and_remains_capture_scoped(image):
    selected, raw, config, runtime = image
    runtime["image_digest"] = None
    with pytest.raises(ValueError, match="another image"):
        observer.associate(selected, raw, config, runtime)
    result = observer.associate(selected, raw, config, runtime, prepublication=True)
    assert result["prepublication_runtime_capture"]
    assert "current serving measurement" in result["scope"]


def test_pull_credential_command_has_no_write_scope_or_docker_prerequisite(tmp_path, monkeypatch):
    wrangler = tmp_path / "node_modules/wrangler/bin/wrangler.js"
    wrangler.parent.mkdir(parents=True)
    actual_version = json.loads((SCRIPT.parent / "package-lock.json").read_text())["packages"]["node_modules/wrangler"][
        "version"
    ]
    (wrangler.parent.parent / "package.json").write_text(json.dumps({"version": actual_version}))
    target = tmp_path / "target.json"
    target.write_text(json.dumps({"account_id": observer.ACCOUNT}))
    calls = []

    def issue(args, env):
        calls.append(args)
        # Issued registry credentials, not the source OAuth profile's permission list.
        return encoded(
            {
                "account_id": observer.ACCOUNT,
                "registry_host": observer.HOST,
                "username": "read-user",
                "password": "secret-pull-token",
            }
        )

    monkeypatch.setattr(observer, "_credential_output", issue)
    credentials = observer.pull_credentials(wrangler, "read-profile", target)
    assert credentials["username"] == "read-user"
    assert len(calls) == 1 and "--pull" in calls[0]
    assert not any(arg in calls[0] for arg in ("--push", "containers:write", "cloudchamber:write", "docker", "login"))
    assert calls[0][calls[0].index("--expiration-minutes") + 1] == "5"


def test_http_reads_only_the_two_selected_controls_and_never_redirects(monkeypatch):
    seen = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self, limit):
            assert limit == observer.LIMIT + 1
            return b"original bytes"

    def open_request(request, **kwargs):
        seen.append(request)
        assert kwargs["timeout"] == 30
        return Response()

    monkeypatch.setattr(observer, "build_opener", lambda handler: SimpleNamespace(open=open_request))
    credentials = {"username": "read-user", "password": "secret-pull-token"}
    observer.registry_bytes("manifests", "sha256:" + "a" * 64, credentials)
    observer.registry_bytes("blobs", "sha256:" + "b" * 64, credentials)
    assert all(request.get_method() == "GET" for request in seen)
    assert [request.full_url for request in seen] == [
        f"https://{observer.HOST}/v2/{observer.REPOSITORY}/{kind}/sha256:" + value * 64
        for kind, value in [("manifests", "a"), ("blobs", "b")]
    ]
    with pytest.raises(ValueError, match="redirect refused"):
        observer._NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.invalid")


def test_receipt_promotion_refuses_foreign_output_and_leaves_no_temporary_file(tmp_path):
    output = tmp_path / "observed.json"
    output.write_bytes(b"foreign original")
    with pytest.raises(FileExistsError):
        observer.save(output, {"status": "pass"})
    assert output.read_bytes() == b"foreign original"
    assert list(tmp_path.iterdir()) == [output]


def test_ordinary_observer_uses_issued_pull_only_credential_without_oauth_or_docker(image, tmp_path, monkeypatch):
    selected, raw, config, runtime = image
    argv = arguments(tmp_path, selected, raw, config, runtime)
    secret = "already-issued-read-pull-only"
    credentials = {
        "account_id": observer.ACCOUNT,
        "registry_host": observer.HOST,
        "username": "no-write-principal",
        "password": secret,
    }
    monkeypatch.setattr(observer.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(encoded(credentials))))
    monkeypatch.setattr(
        observer.subprocess, "run", lambda *args, **kwargs: pytest.fail("Observer must not mint a token")
    )
    monkeypatch.setattr(observer, "pull_credentials", lambda *args: pytest.fail("No OAuth issuer is available"))
    calls = []

    def get(kind, digest, actual):
        calls.append((kind, digest))
        assert actual == credentials
        return raw if kind == "manifests" else config

    monkeypatch.setattr(observer, "registry_bytes", get)
    assert observer.main([*argv, "--pull-credentials-stdin", "--runtime-observed-at", "2026-11-04T23:00:00+00:00"]) == 0
    report = json.loads((tmp_path / "output.json").read_text())
    assert calls == [("manifests", selected["registry_manifest_digest"]), ("blobs", selected["config_digest"])]
    assert report["layer_downloads"] == report["registry_writes"] == 0
    assert not report["credential_retained"] and secret not in (tmp_path / "output.json").read_text()


def test_wrong_selected_config_refuses_before_any_blob_request(image, tmp_path, monkeypatch):
    selected, raw, config, runtime = image
    selected["config_digest"] = json.loads(raw)["layers"][0]["digest"]
    argv = arguments(tmp_path, selected, raw, config, runtime)
    credentials = {
        "account_id": observer.ACCOUNT,
        "registry_host": observer.HOST,
        "username": "read",
        "password": "secret",
    }
    monkeypatch.setattr(observer.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(encoded(credentials))))
    calls = []

    def get(kind, digest, actual):
        calls.append(kind)
        assert kind == "manifests", "A selected layer must not be fetched as a config"
        return raw

    monkeypatch.setattr(observer, "registry_bytes", get)
    with pytest.raises(SystemExit) as refused:
        observer.main([*argv, "--pull-credentials-stdin", "--runtime-observed-at", "2026-11-04T23:00:00+00:00"])
    assert refused.value.code == 2 and calls == ["manifests"]
    assert not (tmp_path / "output.json").exists()


def test_http_refusal_withholds_credential_or_server_error_details(monkeypatch):
    from email.message import Message
    from urllib.error import HTTPError

    def refused(*args, **kwargs):
        raise HTTPError("https://registry.invalid", 403, "sensitive-response-body", Message(), None)

    monkeypatch.setattr(observer, "build_opener", lambda handler: SimpleNamespace(open=refused))
    with pytest.raises(ValueError) as result:
        observer.registry_bytes("manifests", "sha256:" + "a" * 64, {"username": "read", "password": "secret"})
    assert str(result.value) == "Registry read refused with HTTP 403; details withheld"


@pytest.mark.parametrize(
    "raw",
    [b'{"field":1,"field":2}', b'{"field":NaN}', b"[]", b" " * (observer.LIMIT + 1)],
    ids=["duplicate-field", "nonfinite", "wrong-type", "oversize"],
)
def test_ambiguous_or_unbounded_controls_refuse(raw):
    with pytest.raises(ValueError):
        observer.decode(raw)
