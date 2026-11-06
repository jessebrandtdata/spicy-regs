"""CRS detail fields and same-revision refresh rules; raw originals remain in capture storage."""
from __future__ import annotations

import json
import hashlib
from typing import Any

import pyarrow as pa

LIST_SOURCE_FIELDS = {
    "report_id": "id", "title": "title", "report_type": "contentType", "status": "status",
    "published_date": "publishDate", "update_date": "updateDate", "version": "version", "url": "url",
}


def merge_list_observation(prior: dict, report: dict) -> dict:
    """Merge one native coordinate: omission is unknown, explicit null/empty clears.

    Contradictory populated source facts refuse before a duplicate or pooled walk
    can overwrite them. The same rule applies to fresh pages and selected priors.
    """
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError

    for field in LIST_SOURCE_FIELDS.values():
        if (field in report and prior.get(field) not in (None, "") and report[field] not in (None, "")
                and prior[field] != report[field]):
            raise GovernmentShapeError("CRS incompatible same-coordinate list metadata; prior preserved")
    return {**prior, **report}


DETAIL_COLUMNS = (
    "summary", "authors_json", "topics_json", "formats_json", "related_materials_json",
    "detail_state", "detail_attempt_state", "detail_fields_json",
    "list_witness_json", "detail_witness_json", "detail_attempt_witness_json", "detail_error",
)
RECEIPT_COLUMNS = ("list_witness_json", "detail_witness_json", "detail_attempt_witness_json")
DETAIL_ARRAYS = {
    "authors": (("author", pa.string()),),
    "topics": (("topic", pa.string()),),
    "formats": (("format", pa.string()), ("url", pa.string())),
    "related_materials": (("url", pa.string()), ("congress", pa.int64()), ("number", pa.string()),
                          ("title", pa.string()), ("type", pa.string())),
    "detail_fields": (("name", pa.string()), ("state", pa.string())),
}
SOURCE_FIELDS = {"summary": "summary", "authors_json": "authors", "topics_json": "topics",
                 "formats_json": "formats", "related_materials_json": "relatedMaterials"}


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def detail_array(name: str, value: Any) -> list | None:
    """Preserve repeated positions and nulls, spelling numeric identifiers as text only."""
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError

    values = json.loads(value) if isinstance(value, str) else value
    if values is None:
        return None
    if not isinstance(values, list):
        raise GovernmentShapeError(f"CRS {name}: expected array")
    result = []
    for item in values:
        if item is None:
            result.append(None)
            continue
        if not isinstance(item, dict):
            raise GovernmentShapeError(f"CRS {name}: expected object member")
        item = dict(item)
        if "URL" in item:
            if "url" in item and item["url"] != item["URL"]:
                raise GovernmentShapeError("CRS related material states conflicting URL casing")
            item["url"] = item["URL"]
        shaped = {}
        for field, dtype in DETAIL_ARRAYS[name]:
            raw = item.get(field)
            if raw is not None and field == "number" and type(raw) is int:
                raw = str(raw)
            valid = raw is None or (type(raw) is int if pa.types.is_integer(dtype) else isinstance(raw, str))
            if not valid:
                raise GovernmentShapeError(f"CRS {name}.{field}: unsupported value type")
            shaped[field] = raw
        result.append(shaped)
    return result


def qualified_detail(report: dict, witness: list[dict]) -> dict:
    states = []
    result = {}
    for column, field in SOURCE_FIELDS.items():
        value = report.get(field)
        state = "missing" if field not in report else "null" if value is None else "empty" if value in ("", []) else "value"
        states.append({"name": field, "state": state})
        result[column] = value if column == "summary" else json_text(value) if field in report else None
    return {**result, "detail_state": "ok", "detail_attempt_state": "ok", "detail_error": None,
            "detail_fields_json": json_text(states),
            "detail_witness_json": json_text(witness), "detail_attempt_witness_json": json_text(witness)}


def check_detail_replacement(prior: dict, report: dict) -> None:
    """Same-coordinate explicit null/empty clears; contradictory populated facts refuse.

    Congress omits some optional fields. Omission is not an explicit clearing;
    removal of an already stated member at the same coordinate is ambiguous.
    """
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError

    if prior.get("detail_state") != "ok":
        return
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    witnesses = json.loads(prior["detail_witness_json"])
    digest = witnesses[-1]["sha256"]
    with CentralRawArchiveStore(create=False).open(digest) as stream:
        body = stream.read(2 * 1024 * 1024 + 1)
    if len(body) > 2 * 1024 * 1024 or "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        raise GovernmentShapeError("CRS prior detail original differs from its pinned witness")
    old = json.loads(body)["CRSReport"]
    for field, value in old.items():
        if field not in report or (report[field] != value and report[field] not in (None, "", [])):
            raise GovernmentShapeError(f"CRS incompatible same-coordinate detail: {field}")
