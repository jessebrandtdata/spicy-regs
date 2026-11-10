"""Produce the existing publication-input receipt from admitted generation evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .publication import count, sha


def validate_selected_inputs(value):
    """Validate an explicit build declaration; inventory presence never implies use.

    The declaration lives in the admitted source journal, so it cannot change
    independently of the table generation. Complete describes the declared used
    set only. Publisher-wide coverage still requires its finite expected set.
    """
    if (not isinstance(value, dict) or value.get("format") != "spicy-selected-inputs"
            or not isinstance(value.get("version"), int) or isinstance(value["version"], bool) or value["version"] != 1
            or not isinstance(value.get("complete"), bool)):
        raise ValueError("Unsupported selected-input manifest")
    scope = value.get("scope")
    if (not isinstance(scope, dict) or not isinstance(scope.get("id"), str) or not scope["id"].strip()
            or not isinstance(scope.get("finite"), bool) or value["complete"] and not scope["finite"]):
        raise ValueError("Complete selected inputs require an explicit finite scope")
    identities = {}
    for role in ("used", "available", "expected"):
        rows = value.get(role)
        if not isinstance(rows, list):
            raise ValueError(f"Selected inputs require a {role} inventory")
        seen = set()
        for row in rows:
            if not isinstance(row, dict) or not all(isinstance(row.get(k), str) and row[k].strip()
                    for k in ("source_id", "source_version", "version_kind", "asset_kind", "location")):
                raise ValueError("Selected inputs require native identity, version, kind and location")
            key = tuple(row[k] for k in ("source_id", "source_version", "version_kind", "asset_kind"))
            if key in seen:
                raise ValueError("Duplicate selected-input identity")
            seen.add(key)
            pin = sha(row["sha256"]), count(row["bytes"])
            if key in identities and identities[key] != pin:
                raise ValueError("Conflicting selected-input byte identity")
            identities[key] = pin
            if row["version_kind"] == "sha256" and sha(row["source_version"]) != pin[0]:
                raise ValueError("Selected source version differs from its byte identity")
    inventories = value.get("coverage_inventories", [])
    if not isinstance(inventories, list):
        raise ValueError("Coverage inventories must be an array")
    inventory_pins, inventory_roles = {}, set()
    for item in inventories:
        if (not isinstance(item, dict) or item.get("role") not in {"used", "available", "expected"} or item.get("format") != "parquet"
                or not isinstance(item.get("path"), str) or not item["path"]
                or not isinstance(item.get("complete"), bool)
                or not isinstance(item.get("columns"), dict) or not item["columns"].get("source_id")
                or not all(isinstance(item.get(k), str) and item[k] for k in ("version_kind", "asset_kind"))):
            raise ValueError("Invalid pinned coverage inventory")
        pin = sha(item["sha256"]), count(item["bytes"])
        if (item["role"], item["path"]) in inventory_roles:
            raise ValueError("Duplicate coverage inventory")
        inventory_roles.add((item["role"], item["path"]))
        if item["path"] in inventory_pins and inventory_pins[item["path"]] != pin:
            raise ValueError("Conflicting coverage inventory pin")
        inventory_pins[item["path"]] = pin
        if value["complete"] and item["role"] == "used":
            raise ValueError("External used inventories do not qualify a complete inline used set")
        inventory_scope = item.get("scope", scope)
        if (not isinstance(inventory_scope, dict)
                or not isinstance(inventory_scope.get("id"), str) or not inventory_scope["id"].strip()
                or not isinstance(inventory_scope.get("finite"), bool)
                or item["complete"] and not inventory_scope["finite"]):
            raise ValueError("Complete inventory requires its own named finite scope")
    return value


def source_observation(event):
    """Normalize one recorded original or derived input without implying use."""
    if (event.get("event") not in {"capture", "retained-file"} or not event.get("sha256")
            or event.get("stage") == "coverage-inventory"):
        return None
    locations = {event.get(key) for key in ("requested_url", "resolved_url", "original_location", "blob_member")}
    locations.discard(None)
    identities = {str(event[key]) for key in ("source_id", "native_id") if event.get(key) is not None}
    from spicy_regs.source_evidence import is_original

    derived = not is_original(event)
    return {"sha256": sha(event["sha256"]), "bytes": count(event["byte_size"]),
        "provenance": "retained-derived-input" if derived else "capture-observation", "stage": event.get("stage"),
        "location": event.get("resolved_url") or event.get("blob_member"),
        "source_ids": sorted(identities or locations), "locations": sorted(locations),
        "source_version": event["source_version"] if event.get("source_version") and event.get("version_kind") else sha(event["sha256"]),
        "version_kind": event["version_kind"] if event.get("source_version") and event.get("version_kind") else "sha256",
        "observed_at": event.get("observed_at"), "body_retained": event.get("body_retained", False)}


def _pin(raw):
    return {"sha256": "sha256:" + hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def input_proof(artifact, source, evidence=(), *, members=None):
    """Return normalized receipt and native control bytes, without source rereads.

    Callers admit the generation and source artifacts first. Only control files
    are read here; originals remain in their existing archive. The generic
    catalog closure remains conservative even with an explicit selected set:
    inherited generations and consumer-specific interpretation stay separate.
    """
    from spicy_regs.source_evidence import INPUT_ROLE, JOURNAL, PRIOR_ROLE
    from spicy_regs.sources.publication import member_table, table_members
    from rulespec_artifacts import iter_member_descriptors

    declared = [item for item in artifact.root["inputs"] if item["role"] == INPUT_ROLE]
    actual = [{"role": INPUT_ROLE, **item.pin.as_dict()} for _, item in evidence]
    if actual != declared:
        raise ValueError("Input proof source evidence differs from generation inputs")
    controls: dict[str, Any] = {}

    def control(owner, key, path):
        with owner.open(key) as stream:
            raw = stream.read()
        controls[path] = raw
        return {"path": path, **_pin(raw)}

    pins = [control(source, "artifact.json", "generation/artifact.json")]
    pins += [control(source, item["objectKey"], "generation/" + item["objectKey"])
             for item in artifact.root["memberManifests"]]
    spec = artifact.root["spec"]
    outputs = []
    verified_members = iter_member_descriptors(artifact, source) if members is None else members
    for member in verified_members:
        if member.object_key is None:
            raise ValueError("Published output lacks an object key")
        outputs.append({"table": member_table(member.object_key).removesuffix(".parquet"),
                        "object_key": member.object_key, "sha256": member.sha256, "bytes": member.byte_size})
    observations, derived_inputs, selections = [], [], []
    for number, (owner, admitted) in enumerate(evidence):
        prefix = f"evidence-{number}/"
        pins.append(control(owner, "artifact.json", prefix + "artifact.json"))
        pins += [control(owner, m.object_key, prefix + m.object_key) for m in admitted.manifests]
        pins.append(control(owner, JOURNAL, prefix + JOURNAL))
        for line in controls[prefix + JOURNAL].splitlines():
            event = json.loads(line)
            if event.get("event") == "selected-inputs":
                selections.append((validate_selected_inputs(event["manifest"]), owner))
            observed = source_observation(event)
            if observed is not None:
                (derived_inputs if observed["provenance"] == "retained-derived-input" else observations).append(observed)
    if len(selections) > 1:
        raise ValueError("Generation declares more than one selected-input manifest")
    selected, selected_owner = selections[0] if selections else (None, None)
    if selected:
        selected = json.loads(json.dumps(selected))
        for item in selected.get("coverage_inventories", []):
            original = "blobs/sha256/" + sha(item["sha256"]).removeprefix("sha256:")
            if item["path"] != original:
                raise ValueError("Coverage inventory lacks an admitted retained member")
            path = "inventories/" + sha(item["sha256"]).removeprefix("sha256:") + ".parquet"
            controls[path] = (selected_owner, original, sha(item["sha256"]), item["bytes"])
            pins.append({"path": path, "sha256": sha(item["sha256"]), "bytes": item["bytes"]})
            item["path"] = path
    if selected:
        for row in selected["used"]:
            if not any((sha(row["sha256"]), row["bytes"]) == (seen["sha256"], seen["bytes"])
                    and row["location"] in seen["locations"] and row["source_id"] in seen["source_ids"]
                    and (row["version_kind"] == "sha256" and sha(row["source_version"]) == seen["sha256"]
                         or (row["version_kind"], row["source_version"]) == (seen["version_kind"], seen["source_version"]))
                    for seen in observations):
                raise ValueError("Selected used original lacks an admitted capture with its identity, location and version")
    sources = ([{**row, "sha256": sha(row["sha256"]), "provenance": "build-consumption"}
                for row in selected["used"]] if selected else observations)
    parents, other_parents = [], []
    for key, parent in spec.get("parents", {}).items():
        if "family" not in parent or "etag" in parent:
            other_parents.append({"table": key, **parent,
                "identity_basis": "storage-etag-and-size" if "etag" in parent else "sha256-and-size",
                "fresh_cryptographic_read": False})
            continue
        for member in table_members(spec["readSnapshot"], key):
            parents.append({"table": key.removesuffix(".parquet"), "object_key": member.key,
                "family": parent["family"], "artifact_digest": parent["artifactDigest"],
                "sha256": member.sha256, "bytes": member.byte_size})
    generation = {"family": spec["family"], "artifact_digest": artifact.pin.artifact_digest,
        "complete": False, "conflicts": [], "outputs": outputs, "sources": sources, "parents": parents,
        "prior_generations": [{"family": spec["family"], "artifact_digest": row["artifactDigest"]}
                              for row in artifact.root["inputs"] if row["role"] == PRIOR_ROLE],
        "evidence_inputs": pins, "captured_sources": observations, "derived_inputs": derived_inputs,
        "selected_inputs": selected,
        "other_parents": other_parents,
        "limits": ["Generic input closure is not qualified by a capture or a build declaration alone.",
                   "No archive-wide availability or publisher-wide expected inventory is inferred."]}
    return {"format": "spicy-publication-inputs", "version": 1, "generations": [generation]}, controls


def save_input_proof(directory: Path, artifact, source, evidence=()):
    """Save deterministic controls and normalized receipt outside immutable table members."""
    from rulespec_artifacts import canonical_json_bytes

    receipt, controls = input_proof(artifact, source, evidence)
    for key, raw in {**controls, "inputs.json": canonical_json_bytes(receipt)}.items():
        path = directory / key
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(raw, tuple):
            owner, member, expected_sha, expected_size = raw
            temporary = path.with_suffix(".tmp")
            digest, size = hashlib.sha256(), 0
            try:
                with owner.open(member) as source, temporary.open("wb") as target:
                    while chunk := source.read(1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                        target.write(chunk)
                if ("sha256:" + digest.hexdigest(), size) != (expected_sha, expected_size):
                    raise ValueError("Retained coverage inventory differs from its declared pin")
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
            continue
        if path.exists() and path.read_bytes() != raw:
            raise ValueError("Existing generation input proof differs from its exact inputs")
        path.write_bytes(raw)
    return directory / "inputs.json"
