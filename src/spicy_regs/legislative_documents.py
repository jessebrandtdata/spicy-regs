"""Explicit subject shapes for legislative documents and their retained ETL inputs.

SpicyDocs continues to return source observations. These mappings run at the
SpicyRegs writer boundary, after source qualification and before receipt split.
The field registry documents every admitted input; an unreviewed field refuses.
"""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation, localcontext
from functools import cache
from importlib.resources import files
from typing import Any, Mapping

import pyarrow as pa


class LegislativeShapeError(ValueError):
    """An input cannot be represented without losing its stated meaning."""


@cache
def field_registry() -> dict[str, dict]:
    return json.loads(files("spicy_regs").joinpath("legislative_document_fields.json").read_text())


S = pa.string()
INT = pa.int64()
MONEY = pa.decimal128(38, 2)
TYPES = {
    "string": S,
    "int64": INT,
    "bool": pa.bool_(),
    "decimal128(38,2)": MONEY,
    "list<string>": pa.list_(S),
    "list<decimal128(38,2)>": pa.list_(MONEY),
}


def _struct_list(spelling: str, fields: list[tuple[str, pa.DataType]]) -> None:
    TYPES[spelling] = pa.list_(pa.struct(fields))


_struct_list("list<struct<bill_id:string,context:string>>", [("bill_id", S), ("context", S)])
_struct_list("list<struct<element:string,publisher_id:string,publisher_identifier:string,number:string,heading:string>>",
             [(name, S) for name in ("element", "publisher_id", "publisher_identifier", "number", "heading")])
_struct_list("list<struct<usc_key:string,detail:string>>", [("usc_key", S), ("detail", S)])
_struct_list(
    "list<struct<chamber:string,congress:int64,name:string,system_code:string>>",
    [("chamber", S), ("congress", INT), ("name", S), ("system_code", S)],
)
_struct_list(
    "list<struct<estimate_index:int64,title:string,description:string,pub_date:string>>",
    [("estimate_index", INT), ("title", S), ("description", S), ("pub_date", S)],
)
_struct_list(
    "list<struct<citation:string,congress:string,number:string,part:string,report_type:string>>",
    [(n, S) for n in ("citation", "congress", "number", "part", "report_type")],
)
TYPES["list<list<struct<amount:decimal128(38,2),text:string>>>"] = pa.list_(
    pa.list_(pa.struct([("amount", MONEY), ("text", S)]))
)
# Recorded target identity components follow the maintained resolver's routes.
# Values remain literal strings; complete identities and unresolved results both
# remain visible. The ordinals name positions in the retained ordered arrays.
NATIVE_TARGET_KEY_FIELDS = (
    "bill_id", "congress", "law_type", "number", "session", "seq", "package_id", "granule_id",
    "document_number", "publication_date", "rin", "agenda_edition", "report_id", "docket_id",
    "system_code", "cluster_id",
)
NATIVE_TARGET_KEYS = pa.list_(pa.struct(
    [("target_key_ordinal", INT), *((name, S) for name in NATIVE_TARGET_KEY_FIELDS)]
))
NATIVE_TARGET_SNAPSHOT = pa.struct([(name, S) for name in (
    "status", "sha256", "family", "artifact_digest", "generation_id", "receipt_sha256",
)])
NATIVE_CANDIDATE_FIELDS = [
    (name, INT if name in {"candidate_ordinal", "span_start", "span_end", "match_count"}
     else pa.bool_() if name == "target_resolved"
     else NATIVE_TARGET_KEYS if name == "candidate_keys"
     else NATIVE_TARGET_SNAPSHOT if name == "target_snapshot" else S)
    for name in (
        "candidate_ordinal", "cite_kind", "normalized_key", "matched_text", "span_start", "span_end",
        "target_kind", "target_key", "target_grain", "candidate_keys", "derivation_rule", "derivation_version",
        "document_key", "document_kind", "expected_cardinality", "match_count", "occurrence_key", "reason",
        "resolution_rule", "source_record_key", "source_status", "target_resolved", "target_snapshot",
        "target_status", "target_table_selected", "text_sha256", "match_basis", "error_type",
    )
]
# The registry uses this reviewed type name; Arrow carries the complete nested schema.
NATIVE_CANDIDATE_TYPE = "list<native-legal-target/2>"
_struct_list(NATIVE_CANDIDATE_TYPE, NATIVE_CANDIDATE_FIELDS)


def _native_candidates(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, list):
        raise LegislativeShapeError("target_candidates_json: expected list of objects")
    result = []
    for ordinal, candidate in enumerate(value):
        if candidate is None:
            result.append(None)
            continue
        if not isinstance(candidate, dict):
            raise LegislativeShapeError("target_candidates_json: expected list of objects")
        if "candidate_ordinal" in candidate:
            raise LegislativeShapeError("target_candidates_json: source supplied a derived ordinal")
        candidate = {**candidate, "candidate_ordinal": ordinal}
        if candidate.get("candidate_keys") is not None:
            keys = candidate["candidate_keys"]
            if not isinstance(keys, list):
                raise LegislativeShapeError("candidate_keys: expected list of identity objects")
            qualified_keys = []
            for i, inner_key in enumerate(keys):
                if inner_key is None:
                    qualified_keys.append(None)
                    continue
                if not isinstance(inner_key, dict):
                    raise LegislativeShapeError("candidate_keys: expected list of identity objects")
                if "target_key_ordinal" in inner_key:
                    raise LegislativeShapeError("candidate_keys: source supplied a derived ordinal")
                qualified_keys.append({**inner_key, "target_key_ordinal": i})
            candidate["candidate_keys"] = qualified_keys
        result.append(candidate)
    return result


@cache
def subject_schema(dataset: str) -> pa.Schema:
    spec = field_registry()[dataset]
    fields = [pa.field(f["target"], TYPES[f["target_type"]], nullable=f.get("nullable", True))
              for f in spec["fields"] if f["target"]]
    fields += [pa.field(name, TYPES[f["type"]]) for name, f in spec.get("derived_fields", {}).items()]
    if len({field.name for field in fields}) != len(fields):
        raise LegislativeShapeError(f"{dataset}: duplicate subject column")
    return pa.schema(fields)


def _object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Repeated JSON object property")
        result[name] = value
    return result


def _json(value: Any, field: str) -> Any:
    if value is None:
        return None
    if not isinstance(value, str):
        raise LegislativeShapeError(f"{field}: expected retained JSON text")
    try:
        return json.loads(value, parse_float=Decimal, object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, TypeError) as error:
        raise LegislativeShapeError(f"{field}: invalid retained JSON") from error


def _typed(value: Any, dtype: pa.DataType, field: str) -> Any:
    if value is None:
        return None
    if pa.types.is_string(dtype):
        if not isinstance(value, str):
            raise LegislativeShapeError(f"{field}: expected string")
        return value
    if pa.types.is_boolean(dtype):
        if type(value) is bool:
            return value
        if value in ("true", "false"):
            return value == "true"
        raise LegislativeShapeError(f"{field}: expected true or false")
    if pa.types.is_integer(dtype):
        if type(value) is int:
            return value
        if isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
            return int(value)
        raise LegislativeShapeError(f"{field}: expected integer without rounding")
    if pa.types.is_decimal(dtype):
        if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
            raise LegislativeShapeError(f"{field}: money must be exact decimal text or a JSON number")
        try:
            amount = Decimal(value)
            with localcontext() as ctx:
                ctx.prec = 80
                if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")):
                    raise LegislativeShapeError(f"{field}: money cannot be rounded")
            return amount
        except InvalidOperation as error:
            raise LegislativeShapeError(f"{field}: invalid decimal amount") from error
    if pa.types.is_list(dtype):
        if not isinstance(value, list):
            raise LegislativeShapeError(f"{field}: expected ordered list")
        return [_typed(v, dtype.value_type, field + "[]") for v in value]
    if pa.types.is_struct(dtype):
        if not isinstance(value, dict) or set(value) - set(dtype.names):
            raise LegislativeShapeError(f"{field}: unreviewed struct property")
        return {f.name: _typed(value.get(f.name), f.type, field + "." + f.name) for f in dtype}
    raise LegislativeShapeError(f"{field}: unsupported type {dtype}")


def _key(kind: str, values: list[Any]) -> str:
    if any(not isinstance(v, str) or not v for v in values):
        raise LegislativeShapeError(f"{kind}: every identity component must be a nonempty string")
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    return kind + ":" + hashlib.sha256(encoded).hexdigest()


def printing_id(bill_id: Any, version_code: Any, source: Any) -> str:
    """The native key of one printing: a digest of its bill, version code and ``source``, which it does not spell.

    One way: a reader holding native rows learns a printing's ``source`` only by keying each candidate and comparing.
    Public for that reader: the bill family's stage rule reads a row by its source (``build_bill_family``).
    """
    return _key("printing", [bill_id, version_code, source])


def body_version_id(value: Any) -> str | None:
    """An explicit content-version key, consistent across occurrences and readers."""
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", value):
        raise LegislativeShapeError("body version requires a complete SHA-256 digest")
    return "body:sha256:" + value.removeprefix("sha256:")


def bill_section_document_key(value: Any) -> str:
    """Translate an exact legacy held-field key to the native section identity."""
    try:
        parts = json.loads(value)
    except (ValueError, TypeError) as error:
        raise LegislativeShapeError("bill-section document key must be a JSON key tuple") from error
    if not isinstance(parts, list) or len(parts) != 4 or any(not isinstance(p, str) or not p for p in parts):
        raise LegislativeShapeError("bill-section document key requires bill, printing, source, and sequence")
    bill, version, source, seq = parts
    if re.fullmatch(r"printing:[0-9a-f]{64}", source):
        return json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    return json.dumps([bill, version, printing_id(bill, version, source), seq], ensure_ascii=False, separators=(",", ":"))


def court_opinion_document_key(value: Any) -> str:
    """Translate a legacy held-field key, ``[opinion_id, source_sha256]``, to the native opinion body identity.

    The native extraction table names a body by ``opinion_body_id`` alone, and that is the key the citation tool
    asks with. One function derives it (``court_subjects.opinion_body_id``); a key already in that form is kept.
    """
    from spicy_regs.court_subjects import opinion_body_id

    if isinstance(value, str) and re.fullmatch(r"court-opinion-body:[0-9a-f]{64}", value):
        return value
    try:
        parts = json.loads(value)
    except (ValueError, TypeError) as error:
        raise LegislativeShapeError("court-opinion document key must be a JSON key tuple or an opinion body id") from error
    if not isinstance(parts, list) or len(parts) != 2:
        raise LegislativeShapeError("court-opinion document key requires the opinion and its captured body digest")
    try:
        return opinion_body_id(*parts)
    except ValueError as error:
        raise LegislativeShapeError(f"court-opinion document key: {error}") from error


#: Held-field kinds whose published key differs from the key their rows were first written under. The citation
#: table and its read markers must translate the same kinds, so both read this one map.
HELD_KEY_TRANSLATIONS = {"bill_section": bill_section_document_key, "court_opinion_derived_pdf": court_opinion_document_key}


def native_document_key(kind: Any, key: Any) -> Any:
    """The key a held citation row or read marker is published under: translated for the kinds above, else as written."""
    translate = HELD_KEY_TRANSLATIONS.get(kind)
    return key if translate is None else translate(key)


def _identity_value(column: str, row: Mapping[str, Any]) -> str | None:
    value = row.get(column)
    if column in {"text_sha256", "input_sha256"}:
        return body_version_id(value)
    if column == "link_source":
        return _key("hearing-link", [row.get("package_id"), row.get("bill_id"), value])
    if column == "equivalent_xml_source":
        if value is None and row.get("equivalent_xml_version_code") is None:
            return None
        return printing_id(row.get("bill_id"), row.get("equivalent_xml_version_code"), value)
    prefix = column.removesuffix("source")
    return printing_id(row.get("bill_id"), row.get(prefix + "version_code"), value)


def map_subject(dataset: str, row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Map one qualified source-owner row; keep the untouched input for its receipt."""
    spec = field_registry()[dataset]
    # ``retired_fields`` are columns an earlier policy version's source rows carry and this one no longer maps; they
    # stay in the receipt's original row (committee_reports' per-letter columns, legislative-documents/1).
    allowed = {f["name"] for f in spec["fields"]} | set(spec.get("retired_fields", ()))
    if set(row) - allowed:
        raise LegislativeShapeError(f"{dataset}: unreviewed input columns: {sorted(set(row) - allowed)}")
    if spec["processing_only"]:
        for key in spec["identity_fields"]:
            if not isinstance(row.get(key), str) or not row[key]:
                raise LegislativeShapeError(f"{dataset}: missing processing scope {key}")
        return None
    subject = {}
    for field in spec["fields"]:
        name, target = field["name"], field["target"]
        if target is None:
            continue
        value = row.get(name)
        if field["decision"] == "stable-key":
            value = _identity_value(name, row)
        elif field["decision"] == "native-list":
            if name in {"match_path", "billstatus_action_code"}:
                value = None if value is None else ([] if value == "" else value.split("\x1f"))
            else:
                value = _json(value, name)
            if dataset == "law_sections" and name == "hierarchy_json" and value is not None:
                if not isinstance(value, list) or any(not isinstance(v, dict) for v in value):
                    raise LegislativeShapeError("hierarchy_json: expected list of ancestor objects")
                value = [{k: x for k, x in v.items() if k != "native_path"} for v in value]
            if name == "target_candidates_json":
                value = _native_candidates(value)
            elif name == "restatements_json" and value is not None:
                if not isinstance(value, list) or any(v is not None and not isinstance(v, dict) for v in value):
                    raise LegislativeShapeError(f"{name}: expected list of objects")
                value = [None if v is None else {k: x for k, x in v.items() if k != "url"} for v in value]
        subject[target] = _typed(value, TYPES[field["target_type"]], name)
    if dataset == "document_citations":
        subject["document_key"] = native_document_key(row.get("document_kind"), row.get("document_key"))
    for name, derived in spec.get("derived_fields", {}).items():
        value = row.get(derived["from"])
        if "property" in derived:
            obj = _json(value, derived["from"])
            if obj is not None and not isinstance(obj, dict):
                raise LegislativeShapeError("native attributes must be an object")
            value = None if obj is None else obj.get(derived["property"])
            subject[name] = _typed(value, S, name)
        else:
            subject[name] = body_version_id(value)
    for key in spec["identity_fields"]:
        if subject.get(key) is None or subject.get(key) == "":
            raise LegislativeShapeError(f"{dataset}: missing subject identity {key}")
    try:
        # Arrow enforces integer/decimal bounds as well as the complete shape.
        pa.Table.from_pylist([subject], schema=subject_schema(dataset))
    except (pa.ArrowException, OverflowError) as error:
        raise LegislativeShapeError(f"{dataset}: subject cannot be represented exactly") from error
    return subject


@cache
def _earlier_legislative_schema(dataset: str) -> pa.Schema | None:
    """The exact subject schema an earlier ``legislative_policy_history.json`` policy declared, if one is recorded."""
    from spicy_regs.etl_receipts import DatasetPolicy

    history = json.loads(files("spicy_regs").joinpath("legislative_policy_history.json").read_text())
    return DatasetPolicy.from_descriptor(history[dataset]).subject_schema if dataset in history else None


@cache
def _earlier_native_schema() -> pa.Schema:
    from spicy_regs.etl_receipts import DatasetPolicy

    history = json.loads(files("spicy_regs").joinpath("native_legal_policy_history.json").read_text())
    return DatasetPolicy.from_descriptor(history["native_legal_references"]).subject_schema


def recorded_subject(dataset: str, raw: Mapping[str, Any], subject: Mapping[str, Any]) -> dict[str, Any] | None:
    """Map a receipt's source row in the exact key spelling its selected subject used.

    Historical court citations kept the original opinion/digest tuple. New
    citations use its derived body ID. Admit either spelling only for that same
    retained tuple; callers still compare every other field with this result.
    Neither the selected public subject nor its original receipt is rewritten.
    """
    mapped = map_subject(dataset, raw)
    if mapped is not None and dataset == "native_legal_references":
        # Earlier native observations omitted these recorded main fields and
        # nested diagnostics. Verify against that exact reviewed shape, while
        # preserving the original receipt and selected public values unchanged.
        earlier = _earlier_native_schema()
        if set(subject) - {"raw_source_row"} == set(earlier.names):
            def historical(value, dtype):
                if value is None:
                    return None
                if pa.types.is_list(dtype):
                    return [historical(item, dtype.value_type) for item in value]
                if pa.types.is_struct(dtype):
                    return {f.name: historical(value.get(f.name), f.type) for f in dtype}
                return value
            mapped = {f.name: historical(mapped.get(f.name), f.type) for f in earlier}
    earlier_shape = _earlier_legislative_schema(dataset)
    if (mapped is not None and earlier_shape is not None
            and set(subject) - {"raw_source_row"} == set(earlier_shape.names)):
        # A subject written under the earlier policy: verify it in that exact shape. A column the current policy
        # retired is typed from the original row, the way the earlier mapping typed it; the rest map as today.
        current = mapped
        mapped = {f.name: current[f.name] if f.name in current else _typed(raw.get(f.name), f.type, f.name)
                  for f in earlier_shape}
    if mapped is not None and dataset == "document_citations":
        history = json.loads(files("spicy_regs").joinpath("citation_navigation_policy_history.json").read_text())
        from spicy_regs.etl_receipts import DatasetPolicy

        earlier = DatasetPolicy.from_descriptor(history[dataset]).subject_schema
        if set(subject) - {"raw_source_row"} == set(earlier.names):
            # Verify old values under their exact shape; the next writer can
            # promote the retained namespace without relabeling old receipts.
            mapped = {field.name: mapped[field.name] for field in earlier}
    if (mapped is not None and dataset == "document_citations"
            and raw.get("document_kind") == "court_opinion_derived_pdf"
            and subject.get("document_key") == raw.get("document_key")):
        mapped["document_key"] = raw["document_key"]
    return mapped
