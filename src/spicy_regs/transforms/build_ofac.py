"""Retain OFAC entity observations and snapshot metadata without merging identities."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

NAMESPACE = "{https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/ENHANCED_XML}"
ENTITY_TABLE = "ofac_entity_observations"
SNAPSHOT_TABLE = "ofac_snapshots"
SECTIONS = {
    "names_json": "names",
    "list_memberships_json": "sanctionsLists",
    "programs_json": "sanctionsPrograms",
    "sanction_types_json": "sanctionsTypes",
    "legal_authorities_json": "legalAuthorities",
    "addresses_json": "addresses",
    "identifiers_json": "identityDocuments",
    "relationships_json": "relationships",
    "features_json": "features",
}
ENTITY_SCHEMA = pa.schema([
    ("snapshot_id", pa.string()),
    ("selected_list", pa.string()),
    ("entity_id", pa.string()),
    ("identity_id", pa.string()),
    ("entity_type_raw", pa.string()),
    ("entity_type_ref_id", pa.string()),
    ("primary_names_json", pa.string()),
    *((name, pa.string()) for name in SECTIONS),
    ("entity_content_sha256", pa.string()),
    ("source_record_id", pa.string()),
    ("source_locator_json", pa.string()),
    ("raw_entity_json", pa.string()),
])
SNAPSHOT_SCHEMA = pa.schema([
    ("snapshot_id", pa.string()),
    ("selected_list", pa.string()),
    ("observed_at", pa.string()),
    ("data_as_of_raw", pa.string()),
    ("entity_count", pa.int64()),
    ("source_release_logical_id", pa.string()),
    ("source_release_digest", pa.string()),
    ("source_selection_json", pa.string()),
    ("raw_metadata_json", pa.string()),
])
SCHEMAS = {ENTITY_TABLE: ENTITY_SCHEMA, SNAPSHOT_TABLE: SNAPSHOT_SCHEMA}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _children(node, *path):
    nodes = [node]
    for name in path:
        nodes = [
            child
            for parent in nodes
            for child in parent["content"]
            if isinstance(child, dict) and child["name"] == NAMESPACE + name
        ]
    return nodes


def _one(node, *path):
    nodes = _children(node, *path)
    if len(nodes) > 1:
        raise ValueError(f"OFAC scalar repeats: {'/'.join(path)}")
    return nodes[0] if nodes else None


def _text(node):
    if node is None:
        return None
    return "".join(item if isinstance(item, str) else _text(item) for item in node["content"])


def _attribute(node, name):
    return dict(node["attributes"]).get(name) if node is not None else None


def _content(node):
    """Exclude observation positions, so moving a source row is not a content change."""
    return {
        key: [_content(item) if isinstance(item, dict) else item for item in value]
        if key == "content" else value
        for key, value in node.items() if key != "path"
    }


def _entity(item, snapshot_id, scope):
    record = item["record"]
    node = record["element"]
    entity_id = record["entity_id"]
    identity_id = _text(_one(node, "generalInfo", "identityId"))
    if (
        node["name"] != NAMESPACE + "entity"
        or not entity_id
        or entity_id != _attribute(node, "id")
        or identity_id != record["identity_id"]
    ):
        raise ValueError("OFAC entity identity differs from its literal source")
    kind = _one(node, "generalInfo", "entityType")
    names = []
    for name in _children(node, "names", "name"):
        if _text(_one(name, "isPrimary")) == "true":
            for translation in _children(name, "translations", "translation"):
                if _text(_one(translation, "isPrimary")) == "true":
                    value = _text(_one(translation, "formattedFullName"))
                    if value is not None:
                        names.append(value)
    return {
        "snapshot_id": snapshot_id,
        "selected_list": scope["selected_list"],
        "entity_id": entity_id,
        "identity_id": identity_id,
        "entity_type_raw": _text(kind),
        "entity_type_ref_id": _attribute(kind, "refId"),
        "primary_names_json": _json(names),
        **{column: _json(_children(node, section)) for column, section in SECTIONS.items()},
        "entity_content_sha256": "sha256:" + hashlib.sha256(_json(_content(node)).encode()).hexdigest(),
        "source_record_id": item["sourceRecordId"],
        "source_locator_json": _json(record["source_locator"]),
        "raw_entity_json": _json(node),
    }


def _rows(readers, snapshots):
    releases = set()
    for reader in readers:
        snapshot_id = reader.pin.artifact_digest
        if snapshot_id in releases:
            raise ValueError("An OFAC source release was supplied twice")
        releases.add(snapshot_id)
        outcome = reader.collection_outcome
        if (
            outcome["failedRecordCount"]
            or outcome["discardedObservationCount"]
            or outcome["discoveredRecordCount"] != outcome["publishedRecordCount"]
        ):
            raise ValueError("OFAC requires a complete accepted snapshot")
        scope = outcome["requestedScope"]
        if scope["selected_list"] not in {"sdn", "consolidated"}:
            raise ValueError("Unknown OFAC selected list")
        metadata, entity_ids = [], set()
        records = 0
        data_as_of = None
        for item in reader.iter_records():
            record = item["record"]
            locator = record["source_locator"]
            if (
                record["selected_list"] != scope["selected_list"]
                or locator["response_sha256"] != scope["capture"]["response_sha256"]
                or locator["member_name"] != scope["member_name"]
                or locator["observed_at"] != scope["capture"]["observed_at"]
                or locator["request_url"] != scope["capture"]["request_url"]
            ):
                raise ValueError("OFAC observation differs from its selected capture")
            records += 1
            if record["kind"] == "entity":
                row = _entity(item, snapshot_id, scope)
                if row["entity_id"] in entity_ids:
                    raise ValueError("OFAC repeats an entity within one selected snapshot")
                entity_ids.add(row["entity_id"])
                yield row
            else:
                metadata.append(item)
                if record["element"]["name"] == NAMESPACE + "publicationInfo":
                    if data_as_of is not None:
                        raise ValueError("OFAC repeats publication metadata")
                    data_as_of = _text(_one(record["element"], "dataAsOf"))
        if records != outcome["publishedRecordCount"] or not data_as_of:
            raise ValueError("OFAC population or publication metadata is incomplete")
        snapshots.append({
            "snapshot_id": snapshot_id,
            "selected_list": scope["selected_list"],
            "observed_at": scope["capture"]["observed_at"],
            # Some publisher values have an offset and some do not. Preserve both.
            "data_as_of_raw": data_as_of,
            "entity_count": len(entity_ids),
            "source_release_logical_id": reader.pin.logical_id,
            "source_release_digest": reader.pin.artifact_digest,
            "source_selection_json": _json(scope),
            "raw_metadata_json": _json(metadata),
        })
    if not releases:
        raise ValueError("Declare at least one OFAC snapshot release")


def build_ofac(readers, output_dir: Path):
    """Read each release once; keep shared dictionaries once per snapshot."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destinations = [output_dir / (table + ".parquet") for table in SCHEMAS]
    if any(path.exists() for path in destinations):
        raise ValueError("Use fresh output paths for each OFAC generation")
    snapshots = []
    with TemporaryDirectory(dir=output_dir) as temporary:
        staging = Path(temporary)
        entities = write_rows(_rows(readers, snapshots), staging / destinations[0].name, ENTITY_SCHEMA, batch_size=128)
        metadata = write_rows(snapshots, staging / destinations[1].name, SNAPSHOT_SCHEMA)
        outputs = ((entities, destinations[0]), (metadata, destinations[1]))
        try:
            for source, destination in outputs:
                destination.hardlink_to(source)
        except BaseException:
            # Staging shares the destination filesystem. Roll back only files
            # this attempt installed, preserving a competing writer's output.
            for source, destination in outputs:
                try:
                    if not destination.is_symlink() and destination.samefile(source):
                        destination.unlink()
                except FileNotFoundError:
                    pass
            raise
    return tuple(destinations)
