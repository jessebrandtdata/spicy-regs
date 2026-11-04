"""Read one pinned image's manifest/config, without building or pulling layers."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

ACCOUNT = "174055408ff1560e60601c4d12c561c4"
HOST = "registry.cloudflare.com"
REPOSITORY = f"{ACCOUNT}/spicy-regs-reader"
REGISTRY = f"{HOST}/{REPOSITORY}"
LIMIT = 1024 * 1024
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
MANIFEST_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "Duplicate JSON field")
        result[key] = value
    return result


def decode(raw):
    require(isinstance(raw, bytes) and 0 < len(raw) <= LIMIT, "Control bytes outside limit")

    def nonfinite(_):
        raise ValueError("Nonfinite JSON value")

    result = json.loads(raw, object_pairs_hook=_pairs, parse_constant=nonfinite)
    require(isinstance(result, dict), "Control must be a JSON object")
    return result


def pinned(path, digest):
    with path.open("rb") as stream:
        raw = stream.read(LIMIT + 1)
    require(sha(raw) == digest, "Selected input bytes differ from their pin")
    return decode(raw)


def consumer(value, image, *, prepublication=False):
    require(
        isinstance(value, dict) and set(value) == {"image_digest", "code_sha256", "package_versions"},
        "Invalid runtime identity fields",
    )
    require(
        isinstance(value["code_sha256"], str) and DIGEST.fullmatch(value["code_sha256"]),
        "Invalid runtime code identity",
    )
    packages = value["package_versions"]
    require(
        isinstance(packages, dict)
        and packages
        and all(isinstance(k, str) and k and isinstance(v, str) and v for k, v in packages.items()),
        "Missing runtime package identity",
    )
    require(
        value["image_digest"] == image or (prepublication and value["image_digest"] is None),
        "Runtime selects another image",
    )
    return {**value, "image_digest": image}


def selection(value):
    image = value.get("registry_manifest_digest")
    require(isinstance(image, str) and DIGEST.fullmatch(image), "Invalid selected manifest digest")
    require(value.get("registry_image") == f"{REGISTRY}@{image}", "Selected registry/account/image differs")
    config = value.get("config_digest")
    require(isinstance(config, str) and DIGEST.fullmatch(config), "Invalid selected config digest")
    consumer(value.get("consumer"), image)
    return image


def manifest_control(selected, manifest_raw):
    image = selection(selected)
    require(sha(manifest_raw) == image, "Exact manifest bytes differ from selected digest")
    manifest = decode(manifest_raw)
    require(
        type(manifest.get("schemaVersion")) is int
        and manifest["schemaVersion"] == 2
        and manifest.get("mediaType") in MANIFEST_TYPES,
        "Unsupported image manifest; select one image, not an index",
    )
    descriptor = manifest.get("config", {})
    require(
        isinstance(descriptor, dict)
        and descriptor.get("digest") == selected["config_digest"]
        and descriptor.get("mediaType")
        in {"application/vnd.oci.image.config.v1+json", "application/vnd.docker.container.image.v1+json"}
        and type(descriptor.get("size")) is int
        and 0 < descriptor["size"] <= LIMIT,
        "Manifest config differs from selection or bound",
    )
    return manifest


def associate(selected, manifest_raw, config_raw, runtime, *, prepublication=False):
    image = selection(selected)
    manifest = manifest_control(selected, manifest_raw)
    descriptor = manifest["config"]
    require(
        sha(config_raw) == descriptor["digest"] and len(config_raw) == descriptor["size"],
        "Exact config bytes differ from manifest",
    )
    config = decode(config_raw)
    require((config.get("os"), config.get("architecture")) == ("linux", "amd64"), "Wrong image platform")
    layers = manifest.get("layers")
    rootfs = config.get("rootfs")
    require(isinstance(rootfs, dict) and rootfs.get("type") == "layers", "Missing image rootfs identity")
    diff_ids = rootfs.get("diff_ids")
    require(
        isinstance(layers, list) and layers and isinstance(diff_ids, list) and len(layers) == len(diff_ids),
        "Manifest/config layer counts differ",
    )
    require(
        all(
            isinstance(layer, dict)
            and isinstance(layer.get("digest"), str)
            and DIGEST.fullmatch(layer["digest"])
            and type(layer.get("size")) is int
            and layer["size"] >= 0
            and isinstance(layer.get("mediaType"), str)
            and layer["mediaType"]
            for layer in layers
        )
        and all(isinstance(digest, str) and DIGEST.fullmatch(digest) for digest in diff_ids),
        "Invalid ordered layer descriptors",
    )
    actual = consumer(runtime, image, prepublication=prepublication)
    require(actual == selected["consumer"], "Selected runtime code/packages differ from captured consumer")
    return {
        "status": "pass",
        "registry_image": selected["registry_image"],
        "registry_manifest_digest": image,
        "config_digest": descriptor["digest"],
        "platform": "linux/amd64",
        "consumer": actual,
        "ordered_manifest_layers": layers,
        "ordered_rootfs_diff_ids": diff_ids,
        "runtime_configuration": config.get("config"),
        "prepublication_runtime_capture": prepublication,
        "source_association": {
            k: selected[k] for k in ("source_commit", "source_tree", "uv_lock_sha256") if k in selected
        },
        "scope": "Exact manifest/config and their declared layer order, bound to pinned runtime inputs. "
        "No layer bodies, container execution, current serving measurement or source reconstruction.",
    }


def credential_target(credentials):
    require(
        credentials.get("account_id") == ACCOUNT and credentials.get("registry_host") == HOST,
        "Pull credentials target another registry/account",
    )
    require(
        all(isinstance(credentials.get(k), str) and credentials[k] for k in ("username", "password")),
        "Invalid pull credentials; values withheld",
    )
    return credentials


def _credential_output(command, env):
    """Bound both private CLI pipes; never put credential output in a file/error."""
    result = bytearray()
    observed = 0
    deadline = time.monotonic() + 60
    with subprocess.Popen(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
    ) as process:
        assert process.stdout is not None and process.stderr is not None
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            selector.register(process.stderr, selectors.EVENT_READ)
            try:
                while selector.get_map():
                    require(time.monotonic() < deadline, "Pull credential request timed out; details withheld")
                    for key, _ in selector.select(timeout=0.2):
                        chunk = os.read(key.fd, 65537 - observed)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        observed += len(chunk)
                        require(observed <= 65536, "Pull credential response exceeds private bound; details withheld")
                        if key.fileobj is process.stdout:
                            result.extend(chunk)
                require(
                    process.wait(timeout=max(0.1, deadline - time.monotonic())) == 0,
                    "Pull credential request refused; details withheld",
                )
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
    return bytes(result)


def pull_credentials(wrangler, profile, config):
    """Use the pinned CLI's existing credential route; request only pull authority."""
    package = json.loads((wrangler.parent.parent / "package.json").read_text())
    lock = json.loads((Path(__file__).parent / "package-lock.json").read_text())
    require(
        package["version"] == lock["packages"]["node_modules/wrangler"]["version"], "Wrangler version differs from lock"
    )
    target = json.loads(config.read_text())
    require(target.get("account_id") == ACCOUNT, "Operator account differs")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("CF_", "CLOUDFLARE_", "DOCKER_"))}
    env.update(
        CLOUDFLARE_ACCOUNT_ID=ACCOUNT,
        WRANGLER_SEND_METRICS="false",
        WRANGLER_LOG_PATH="/dev/null",
        WRANGLER_WRITE_LOGS="false",
        WRANGLER_LOG_SANITIZE="true",
        CI="1",
    )
    command = [
        "node",
        str(wrangler),
        "containers",
        "registries",
        "credentials",
        HOST,
        "--pull",
        "--expiration-minutes",
        "5",
        "--json",
        "--profile",
        profile,
        "--config",
        str(config),
        "--env-file",
        "/dev/null",
    ]
    try:
        raw = _credential_output(command, env)
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("Pull credential request unavailable; details withheld") from None
    return credential_target(decode(raw))


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Registry redirect refused")


def registry_bytes(kind, digest, credentials):
    require(kind in {"manifests", "blobs"} and DIGEST.fullmatch(digest), "Invalid registry read")
    request = Request(
        f"https://{HOST}/v2/{REPOSITORY}/{kind}/{digest}",
        method="GET",
        headers={
            "Authorization": "Basic "
            + base64.b64encode(f"{credentials['username']}:{credentials['password']}".encode()).decode(),
            "Accept": ", ".join(sorted(MANIFEST_TYPES)) if kind == "manifests" else "application/octet-stream",
            "Accept-Encoding": "identity",
        },
    )
    try:
        with build_opener(_NoRedirect()).open(request, timeout=30) as response:
            require(response.status == 200, "Registry read did not succeed")
            raw = response.read(LIMIT + 1)
    except HTTPError as error:
        raise ValueError(f"Registry read refused with HTTP {error.code}; details withheld") from None
    except (OSError, URLError):
        raise ValueError("Registry read unavailable; details withheld") from None
    require(0 < len(raw) <= LIMIT, "Registry control exceeds byte bound")
    return raw


def save(path, value):
    """Promote one complete receipt without replacing any existing file or symlink."""
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".image-observation-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write((json.dumps(value, indent=2) + "\n").encode())
            stream.flush()
            os.fsync(stream.fileno())
            os.link(temporary, path)
        finally:
            temporary.unlink()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--selection-sha256", required=True)
    parser.add_argument("--runtime-consumer", type=Path, required=True)
    parser.add_argument("--runtime-consumer-sha256", required=True)
    parser.add_argument(
        "--runtime-observed-at", help="Independent runtime capture time, with timezone; unknown stays absent"
    )
    parser.add_argument("--prepublication-runtime", action="store_true")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--wrangler", type=Path)
    parser.add_argument(
        "--pull-credentials-stdin",
        action="store_true",
        help="Use already-issued pull-only credentials; no OAuth permission check or credential issuance",
    )
    parser.add_argument("--profile")
    parser.add_argument("--operator-config", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        require(not args.output.exists() and not args.output.is_symlink(), "Output already exists")
        selected = pinned(args.selection, args.selection_sha256)
        image = selection(selected)
        runtime = pinned(args.runtime_consumer, args.runtime_consumer_sha256)
        require(
            consumer(runtime, image, prepublication=args.prepublication_runtime) == selected["consumer"],
            "Runtime input differs before registry access",
        )
        if args.runtime_observed_at is not None:
            require(
                datetime.fromisoformat(args.runtime_observed_at).utcoffset() is not None,
                "Runtime capture time needs a timezone",
            )
        if args.manifest is not None or args.config is not None:
            require(
                args.manifest is not None
                and args.config is not None
                and args.wrangler is None
                and not args.pull_credentials_stdin,
                "Retained mode needs both exact originals and no live credential request",
            )
            with args.manifest.open("rb") as stream:
                manifest_raw = stream.read(LIMIT + 1)
            with args.config.open("rb") as stream:
                config_raw = stream.read(LIMIT + 1)
            mode = "retained-originals"
        else:
            require(
                args.runtime_observed_at is not None, "Live observation needs an independently timed runtime capture"
            )
            if args.pull_credentials_stdin:
                require(args.wrangler is None, "Choose issued pull credentials or the pinned issuer")
                credentials = credential_target(decode(sys.stdin.buffer.read(LIMIT + 1)))
            else:
                require(
                    args.wrangler is not None and args.profile and args.operator_config is not None,
                    "Live read needs pinned Wrangler, named profile and account config",
                )
                credentials = pull_credentials(args.wrangler, args.profile, args.operator_config)
            manifest_raw = registry_bytes("manifests", image, credentials)
            manifest_control(selected, manifest_raw)  # Refuse wrong config/size/type before any blob request.
            config_raw = registry_bytes("blobs", selected["config_digest"], credentials)
            mode = "live-pull-only"
        report = associate(selected, manifest_raw, config_raw, runtime, prepublication=args.prepublication_runtime)
        report.update(
            mode=mode,
            selection_sha256=args.selection_sha256,
            runtime_capture_sha256=args.runtime_consumer_sha256,
            verified_at=datetime.now(timezone.utc).isoformat(),
            registry_observed_at=datetime.now(timezone.utc).isoformat() if mode == "live-pull-only" else None,
            input_provenance={
                "selection": str(args.selection),
                "runtime_consumer": str(args.runtime_consumer),
                "runtime_observed_at": args.runtime_observed_at,
            },
            originals={
                "manifest_base64": base64.b64encode(manifest_raw).decode(),
                "config_base64": base64.b64encode(config_raw).decode(),
            },
            layer_downloads=0,
            registry_writes=0,
            credential_retained=False,
        )
        save(args.output, report)
    except (OSError, ValueError, KeyError, TypeError) as error:
        # Remote credential responses and transport exceptions never enter the message.
        parser.exit(2, f"Image observation refused: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
