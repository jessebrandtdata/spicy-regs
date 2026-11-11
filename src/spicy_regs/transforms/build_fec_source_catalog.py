"""Make the provider's official FEC source inventory queryable as public metadata.

This is a route inventory, not an assertion that every route has been acquired.
SpicyDocs owns the inventory; SpicyRegs exposes it alongside observed datasets.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from importlib.metadata import version
from importlib.resources import files
from pathlib import Path

import pyarrow as pa

from spicy_regs.transforms.parquet_rows import write_rows

OUTPUT = "fec_source_catalog.parquet"
COLUMNS = (
    "source_family",
    "title",
    "api_routes_json",
    "indexes_json",
    "bulk_families_json",
    "references_json",
    "source_metadata_json",
    "catalog_sha256",
    "provider_version",
    "catalogued_at",
    "coverage_note",
)
SCHEMA = pa.schema([(column, pa.string()) for column in COLUMNS])
COVERAGE_NOTE = (
    "Official source routes catalogued by the pinned provider. This row does not "
    "establish acquired records, complete historical coverage, acquired document "
    "bodies, or public table availability. Join selected collection evidence by "
    "source_family; catalogued_at is the catalog build time, not a source retrieval time."
)


def official_sources() -> bytes:
    """The installed provider's inventory: this table's one input, whose SHA-256 every row states as ``catalog_sha256``."""
    return files("spicy_docs.sources.fec").joinpath("official_sources.json").read_bytes()


def build_fec_source_catalog(output_dir: Path) -> Path:
    """Build one row per official family without fetching or interpreting records."""
    from spicy_docs.sources.fec.catalog import api_operations

    original = official_sources()
    sources = json.loads(original)
    operations = api_operations()
    digest = hashlib.sha256(original).hexdigest()
    provider_version = version("spicy-docs")
    catalogued_at = datetime.now(UTC).isoformat()
    seen: set[str] = set()

    def encode(value: object) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def rows():
        for source in sources:
            family = source["id"]
            if not isinstance(family, str) or not family or family in seen:
                raise ValueError("FEC source inventory has an empty or duplicate family ID")
            seen.add(family)
            routes = []
            for route in source.get("api", []):
                path = route["path"]
                if path not in operations:
                    raise ValueError(f"FEC source inventory route has no reader operation: {path}")
                routes.append({**route, "pagination_mode": operations[path]})
            yield {
                "source_family": family,
                "title": source["name"],
                "api_routes_json": encode(routes),
                "indexes_json": encode(source.get("indexes", [])),
                "bulk_families_json": encode(source.get("bulk_families", [])),
                "references_json": encode(source.get("references", [])),
                "source_metadata_json": encode(source),
                "catalog_sha256": digest,
                "provider_version": provider_version,
                "catalogued_at": catalogued_at,
                "coverage_note": COVERAGE_NOTE,
            }

    output_dir.mkdir(parents=True, exist_ok=True)
    return write_rows(rows(), output_dir / OUTPUT, SCHEMA)
