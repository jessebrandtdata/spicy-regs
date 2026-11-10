"""Explicit native domain mapping for the regulations datasets.

No acquisition or publication occurs here. Unknown fields, malformed JSON and
unrepresented nested properties refuse before Arrow can silently drop them.
Exact converted inputs remain available to the shared receipt writer.
"""

from __future__ import annotations

import hashlib
import json
from functools import cache
from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa

from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS, RECEIPT_COLUMNS

S = pa.string()
STRINGS = pa.list_(S)
FORMAT = pa.struct([("url", S), ("format", S), ("size", pa.int64())])
FORMATS = pa.list_(FORMAT)
DISPLAY = pa.list_(pa.struct([("name", S), ("label", S), ("tooltip", S)]))
COMMENT_ATTACHMENTS = pa.list_(
    pa.struct(
        [("title", S), ("formats", FORMATS), ("attachment_id", S), ("restrict_reason", S), ("restrict_reason_type", S)]
    )
)
ATTACHMENT_RECORDS = pa.list_(
    pa.struct(
        [
            ("attachment_id", S),
            ("title", S),
            ("file_formats", FORMATS),
            ("restrict_reason", S),
            ("restrict_reason_type", S),
        ]
    )
)
AGENCIES = pa.list_(
    pa.struct(
        [
            ("id", pa.int64()),
            ("name", S),
            ("raw_name", S),
            ("parent_id", pa.int64()),
            ("slug", S),
        ]
    )
)
CFR_REFERENCES = pa.list_(pa.struct([("title", pa.int64()), ("part", S), ("chapter", S)]))
TIMETABLE = pa.list_(pa.struct([("action", S), ("date", S), ("fr_citation", S)]))
RELATED_RINS = pa.list_(pa.struct([("rin", S), ("rin_relation", S)]))
STAGE_EVENTS = pa.list_(
    pa.struct(
        [
            ("stage", S),
            ("event_kind", S),
            ("effective_date", S),
            ("document_id", S),
        ]
    )
)

# Each mapping is a deliberate field decision. No suffix-based classification.
NATIVE_FIELDS = {
    "comment_attributes": {
        "display_properties_json": ("display_properties", DISPLAY),
        "file_formats_json": ("file_formats", FORMATS),
    },
    "docket_attributes": {"display_properties_json": ("display_properties", DISPLAY)},
    "document_attributes": {"display_properties_json": ("display_properties", DISPLAY)},
    "comments": {"attachments_json": ("attachments", COMMENT_ATTACHMENTS)},
    "documents": {
        "attachments_json": ("attachments", FORMATS),
        "attachment_records_json": ("attachment_records", ATTACHMENT_RECORDS),
        "additional_rins": ("additional_rins", STRINGS),
    },
    "federal_register": {
        "agencies_json": ("agencies", AGENCIES),
        "docket_ids_json": ("docket_ids", STRINGS),
        "regulation_id_numbers_json": ("regulation_id_numbers", STRINGS),
        "cfr_references_json": ("cfr_references", CFR_REFERENCES),
        "topics_json": ("topics", STRINGS),
        "corrections_json": ("corrections", STRINGS),
        "agency_slugs": ("agency_slugs", STRINGS),
    },
    "fr_docket_links": {
        # The normalizer's readings of one printed label: a list like the table's others, never public as text.
        "normalized_docket_candidates_json": ("normalized_docket_candidates", STRINGS),
        "agency_slugs": ("agency_slugs", STRINGS),
        "docket_ids_json": ("docket_ids", STRINGS),
        "regulation_id_numbers_json": ("regulation_id_numbers", STRINGS),
    },
    "comment_periods": {
        "evidence_occurrences_json": ("evidence_occurrences", pa.list_(pa.struct([
            ("source", S), ("role", S), ("evidence_id", S), ("source_ordinal", pa.int64()),
            ("document_id", S), ("document_number", S), ("publication_date", S),
            ("open_date", S), ("close_date", S), ("rins", STRINGS), ("rin_values_json", S),
        ]))),
        "proceeding_ids_json": ("proceeding_ids", STRINGS),
        "rins_json": ("rins", STRINGS),
        "docket_ids_json": ("docket_ids", STRINGS),
        "opened_by_artifact_ids_json": ("opened_by_artifact_ids", STRINGS),
        "evidence_ids_json": ("evidence_ids", STRINGS),
    },
    "proceedings": {
        "docket_ids_json": ("docket_ids", STRINGS),
        "stage_events_json": ("stage_events", STAGE_EVENTS),
        "fr_document_numbers_json": ("fr_document_numbers", STRINGS),
        "fr_document_ids_json": ("fr_document_ids", STRINGS),
        "cfr_refs_json": ("cfr_refs", STRINGS),
        "cfr_target_iris_json": ("cfr_target_iris", STRINGS),
        "authority_refs_json": ("authority_refs", STRINGS),
        "rins_json": ("rins", STRINGS),
    },
    "rulemaking_lifecycles": {"specific_rins_json": ("specific_rins", STRINGS)},
    "unified_agenda": {
        "timetable_json": ("timetable", TIMETABLE),
        "cfr_references_json": ("cfr_references", STRINGS),
        "legal_authority_json": ("legal_authority", STRINGS),
        "related_rins_json": ("related_rins", RELATED_RINS),
    },
}
TYPE_OVERRIDES = {
    "federal_register": {"significant": pa.bool_(), "regulations_dot_gov_comments_count": pa.int64()},
    "cfr_sections": {"part_granule": pa.bool_()},
    "documents": {"withdrawn": pa.bool_()},
    "regulatory_agenda_items": {"linked_proceeding_count": pa.int64(), "observation_count": pa.int64()},
}
#: Columns that left receipts for the public table (ss-vse3), each appended after every column published before
#: it, in this order, so no earlier column changes position. ``source_sha256`` is new with them: the retained API
#: page a row was last read from, NULL on a row not read since.
PROMOTED_LAST = {
    "federal_register": ("html_url", "pdf_url", "body_html_url", "source_sha256", "regulations_dot_gov_info_json"),
    "fr_docket_links": ("normalized_docket_candidates",),
}
IDENTITIES = {
    "agency_lifecycle_stats": ("agency_code", "stratum"),
    "agency_monthly_volume": ("agency_code", "year", "month", "document_type"),
    "agency_stats": ("agency_code",),
    "agenda_item_proceedings": ("relationship_id",),
    "cfr_sections": ("granule_id",),
    "comment_attributes": ("comment_id",),
    "comment_periods": ("comment_period_id",),
    "comments": ("comment_id",),
    "comments_index": ("agency_code", "docket_id", "year", "month"),
    "discovery_signals": ("agency_code",),
    "docket_attributes": ("docket_id",),
    "dockets": ("docket_id",),
    "document_attributes": ("document_id",),
    "documents": ("document_id",),
    "federal_register": ("document_number", "publication_date"),
    "feed_summary": ("docket_id",),
    # Printed positions distinguish repeated labels; a link-only statement has
    # no position, so its literal docket ID distinguishes the relationship.
    "fr_docket_links": ("document_number", "publication_date", "docket_source_ordinal", "docket_id"),
    "lifecycle_events": ("lifecycle_event_id",),
    "proceedings": ("proceeding_id",),
    "regulatory_agenda_items": ("agenda_item_id",),
    "rule_targets": ("rule_target_id",),
    "rulemaking_lifecycles": ("proceeding_id",),
    "unified_agenda": ("rin", "agenda_edition"),
}
# Preserve witness-distinct source rows when their witness columns move to receipts.
DERIVED_IDENTITIES = {
    "rule_targets": ("rule_target_id", ("docket_id", "cfr_ref", "rin", "source")),
    "lifecycle_events": (
        "lifecycle_event_id",
        (
            "proceeding_id",
            "document_id",
            "stage",
            "event_date",
            "source",
            "dated_by",
            "evidence_id",
            "joined_by",
            "document_form",
            "anchor_role",
        ),
    ),
}
TYPES = {
    "VARCHAR": S,
    "BIGINT": pa.int64(),
    "INTEGER": pa.int32(),
    "DOUBLE": pa.float64(),
    "BOOLEAN": pa.bool_(),
    "DATE": pa.date32(),
    "TIMESTAMP WITH TIME ZONE": pa.timestamp("us", tz="UTC"),
    "VARCHAR[]": STRINGS,
}


class RegulationsShapeError(ValueError):
    """A source value cannot be represented without losing information."""


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RegulationsShapeError(f"repeated JSON key: {key}")
        result[key] = value
    return result


def _array(value, field):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(
                value,
                object_pairs_hook=_pairs,
                parse_constant=lambda v: (_ for _ in ()).throw(RegulationsShapeError(v)),
            )
        except (ValueError, TypeError) as error:
            raise RegulationsShapeError(f"{field}: malformed JSON: {error}") from error
    if value is None:
        return None
    if not isinstance(value, list):
        raise RegulationsShapeError(f"{field}: expected list, got {type(value).__name__}")
    return value


def _object(value, allowed, field):
    if not isinstance(value, Mapping) or set(value) - set(allowed):
        raise RegulationsShapeError(f"{field}: unexpected object fields or value: {value!r}")
    return value


def _text_number(value, field):
    # CFR parts/chapter labels include alpha suffixes and printed ranges. Keep
    # source strings exactly; integer JSON tokens have one unambiguous spelling.
    if value is None or isinstance(value, str):
        return value
    if type(value) is int:
        return str(value)
    raise RegulationsShapeError(f"{field}: expected string or integer")


def _formats(value, field, *, source_names=False):
    if value is None:
        return None
    result = []
    for item in value:
        if item is None:
            result.append(None)
            continue
        if source_names:
            _object(item, ("fileUrl", "format", "size"), field)
            item = {"url": item.get("fileUrl"), "format": item.get("format"), "size": item.get("size")}
        result.append(item)
    return result


def _native(dataset, field, value):
    if field == "agency_slugs":
        if value is None or isinstance(value, list):
            return value
        if not isinstance(value, str):
            raise RegulationsShapeError(f"{dataset}.agency_slugs: expected publisher slug string")
        return value.split(",")
    values = _array(value, f"{dataset}.{field}")
    if values is None:
        return None
    result = []
    for item in values:
        if item is None:
            result.append(None)
            continue
        if dataset == "federal_register" and field == "agencies_json":
            _object(item, ("id", "name", "raw_name", "parent_id", "slug", "url", "json_url"), field)
            item = {k: item.get(k) for k in ("id", "name", "raw_name", "parent_id", "slug")}
        elif dataset == "federal_register" and field == "cfr_references_json":
            _object(item, ("title", "part", "chapter", "citation_url"), field)
            item = {
                "title": item.get("title"),
                "part": _text_number(item.get("part"), field),
                "chapter": _text_number(item.get("chapter"), field),
            }
        elif dataset == "unified_agenda" and field == "related_rins_json":
            # Arrow would drop an unknown member silently; refuse it instead.
            _object(item, RELATED_RINS.value_type.names, field)
        elif dataset == "proceedings" and field == "stage_events_json":
            _object(item, ("stage", "event_kind", "effective_date", "evidence_id", "joined_by", "source"), field)
            item = {k: item.get(k) for k in ("stage", "event_kind", "effective_date")} | {
                "document_id": item.get("evidence_id")
            }
        elif dataset == "comments" and field == "attachments_json":
            _object(item, ("title", "formats", "attachment_id", "restrictReason", "restrictReasonType",
                           "restrict_reason", "restrict_reason_type"), field)
            item = dict(item)
            for source, target in (("restrictReason", "restrict_reason"), ("restrictReasonType", "restrict_reason_type")):
                if source in item:
                    if target in item and item[target] != item[source]:
                        raise RegulationsShapeError(f"{field}: conflicting {source} observations")
                    item[target] = item.pop(source)
        elif field == "attachment_records_json":
            _object(item, ("id", "type", "attributes", "links", "relationships"), field)
            if item.get("type") != "attachments":
                raise RegulationsShapeError(f"{field}: source record is not an attachment")
            attrs = _object(
                item.get("attributes") or {},
                ("title", "fileFormats", "restrictReason", "restrictReasonType",
                 "agencyNote", "authors", "docAbstract", "docOrder", "modifyDate", "publication"), field
            )
            item = {
                "attachment_id": item.get("id"),
                "title": attrs.get("title"),
                "file_formats": _formats(attrs.get("fileFormats"), field, source_names=True),
                "restrict_reason": attrs.get("restrictReason"),
                "restrict_reason_type": attrs.get("restrictReasonType"),
            }
        elif field == "file_formats_json":
            item = _formats([item], field, source_names=True)[0]
        result.append(item)
    return result


REGULATIONS_GOV_DETAIL_FIELDS = {
    "regulations_dot_gov_agency_id": S,
    "regulations_dot_gov_title": S,
    "regulations_dot_gov_regulation_id_number": S,
    "regulations_dot_gov_supporting_documents_count": pa.int64(),
    "regulations_dot_gov_supporting_documents": pa.list_(pa.struct([("document_id", S), ("title", S)])),
    "regulations_dot_gov_regulatory_plan_title": S,
}


@cache
def subject_schema(dataset):
    fields = []
    for name, type_name in SOURCE_COLUMNS[dataset]:
        if name in RECEIPT_COLUMNS[dataset]:
            continue
        target, dtype = NATIVE_FIELDS.get(dataset, {}).get(name, (name, TYPES[type_name]))
        dtype = TYPE_OVERRIDES.get(dataset, {}).get(name, dtype)
        fields.append(pa.field(target, dtype))
    if dataset in DERIVED_IDENTITIES:
        fields.insert(0, pa.field(DERIVED_IDENTITIES[dataset][0], S))
    if dataset == "federal_register":
        fields.extend(pa.field(name, dtype) for name, dtype in REGULATIONS_GOV_DETAIL_FIELDS.items())
    if dataset == "comments":
        fields.append(pa.field("comment_text", S))
    if dataset == "rule_targets":
        fields.append(pa.field("fr_document_ids", STRINGS))
    last = PROMOTED_LAST.get(dataset, ())
    fields = [f for f in fields if f.name not in last] + sorted((f for f in fields if f.name in last),
                                                                 key=lambda f: last.index(f.name))
    return pa.schema(fields)


def _validate(value, dtype, path):
    if value is None:
        return
    if pa.types.is_list(dtype):
        if not isinstance(value, list):
            raise RegulationsShapeError(f"{path}: expected list")
        for i, item in enumerate(value):
            _validate(item, dtype.value_type, f"{path}[{i}]")
    elif pa.types.is_struct(dtype):
        _object(value, [f.name for f in dtype], path)
        for f in dtype:
            _validate(value.get(f.name), f.type, f"{path}.{f.name}")
    elif pa.types.is_string(dtype) and not isinstance(value, str):
        raise RegulationsShapeError(f"{path}: expected string")
    elif pa.types.is_integer(dtype) and type(value) is not int:
        raise RegulationsShapeError(f"{path}: expected integer")
    elif pa.types.is_boolean(dtype) and type(value) is not bool:
        raise RegulationsShapeError(f"{path}: expected boolean")


def _shape_record(dataset, row):
    """Return native fields plus explicitly declared receipt-only input fields.

    An omitted source field stays omitted in the retained input; SQL NULL,
    JSON null, empty lists, repeated list items and original key spelling are
    distinguishable there. Comments retains raw_source_record once; other
    datasets retain raw_conversion_inputs. Subjects hold the mapped values.
    """
    known = {c for c, _ in SOURCE_COLUMNS[dataset]}
    if unknown := set(row) - known:
        raise RegulationsShapeError(f"{dataset}: undeclared fields {sorted(unknown)}")
    shaped = {c: row[c] for c in RECEIPT_COLUMNS[dataset] if c in row}
    # Comments readers normalize the one original input when replaying it.
    shaped["raw_source_record" if dataset == "comments" else "raw_conversion_inputs"] = dict(row)
    for name, _ in SOURCE_COLUMNS[dataset]:
        if name in RECEIPT_COLUMNS[dataset]:
            continue
        value = row.get(name)
        target = name
        if name in NATIVE_FIELDS.get(dataset, {}):
            target, _ = NATIVE_FIELDS[dataset][name]
            value = _native(dataset, name, value)
        elif pa.types.is_boolean(TYPE_OVERRIDES.get(dataset, {}).get(name, S)) and value is not None:
            if type(value) is not bool:
                if value not in ("true", "false", "True", "False"):
                    raise RegulationsShapeError(f"{dataset}.{name}: unknown boolean spelling")
                value = value.lower() == "true"
        elif dataset in {"regulatory_agenda_items", "federal_register"} and pa.types.is_integer(TYPE_OVERRIDES[dataset].get(name, S)) and value is not None:
            if isinstance(value, str) and value.isascii() and value.isdecimal():
                value = int(value)
            elif type(value) is not int:
                raise RegulationsShapeError(f"{dataset}.{name}: expected nonnegative integer")
            if value < 0:
                raise RegulationsShapeError(f"{dataset}.{name}: negative count")
        shaped[target] = value
    if dataset in DERIVED_IDENTITIES:
        key, columns = DERIVED_IDENTITIES[dataset]
        literal = json.dumps(
            [dataset, [(c, row.get(c)) for c in columns]], ensure_ascii=False, separators=(",", ":"), default=str
        )
        shaped[key] = hashlib.sha256(literal.encode()).hexdigest()
    if dataset == "rule_targets":
        references = _array(row.get("fr_references_json"), "rule_targets.fr_references_json")
        targets = []
        for reference in references or ():
            if reference is None:
                targets.append(None)
                continue
            if not isinstance(reference, Mapping) or not isinstance(reference.get("candidate_ids"), list):
                raise RegulationsShapeError("rule_targets.fr_references_json: expected native candidate list")
            candidates = reference["candidate_ids"]
            if any(not isinstance(candidate, str) for candidate in candidates):
                raise RegulationsShapeError("rule_targets.fr_references_json: invalid candidate identity")
            # Match the existing FederalRegisterIndex resolution rule: one
            # selected dated record resolves; missing/ambiguous references do not.
            targets.append(candidates[0] if len(candidates) == 1 else None)
        shaped["fr_document_ids"] = None if references is None else targets
    if dataset == "federal_register":
        raw_info = row.get("regulations_dot_gov_info_json")
        try:
            info = json.loads(raw_info, object_pairs_hook=_pairs,
                              parse_constant=lambda value: (_ for _ in ()).throw(RegulationsShapeError(value))) if raw_info is not None else {}
        except (TypeError, ValueError) as error:
            raise RegulationsShapeError("federal_register.regulations_dot_gov_info_json: invalid JSON") from error
        if info is None:
            info = {}
        _object(info, ("agency_id", "checked_regulationsdotgov_at", "comments_url", "docket_id", "document_id",
                       "regulation_id_number", "title", "comments_count", "supporting_documents_count",
                       "regulatory_plan", "supporting_documents"), "regulations_dot_gov_info_json")
        plan = info.get("regulatory_plan")
        if plan is not None:
            _object(plan, ("html_url", "title"), "regulations_dot_gov_info_json.regulatory_plan")
        for name in ("agency_id", "title", "regulation_id_number", "supporting_documents_count", "supporting_documents"):
            shaped["regulations_dot_gov_" + name] = info.get(name)
        shaped["regulations_dot_gov_regulatory_plan_title"] = None if plan is None else plan.get("title")
    if dataset == "comments":
        from spicy_regs.transforms.html_text import html_text

        comment = row.get("comment")
        if comment is not None and not isinstance(comment, str):
            raise RegulationsShapeError("comments.comment: expected string")
        shaped["comment_text"] = html_text(comment)
    schema = subject_schema(dataset)
    for field in schema:
        _validate(shaped.get(field.name), field.type, f"{dataset}.{field.name}")
    return shaped


def shape_record(dataset, row):
    """Shape one source row and check Arrow range/date rules with the reference schema."""
    shaped = _shape_record(dataset, row)
    pa.Table.from_pylist([shaped], schema=subject_schema(dataset))
    return shaped


def shape_records(dataset, rows):
    """Apply the same mapper and Arrow checks once for a bounded source batch.

    Unproven batches replay the row function, preserving its first exception
    when an earlier range failure precedes a later structural failure.
    """
    rows = list(rows)
    try:
        shaped = [_shape_record(dataset, row) for row in rows]
        pa.Table.from_pylist(shaped, schema=subject_schema(dataset))
    except (ValueError, TypeError, OverflowError, RecursionError, pa.ArrowException):
        return [shape_record(dataset, row) for row in rows]
    return shaped


def published_as_source(dataset: str, published, destination: Path) -> Path:
    """A published native table read back into the text shape the regulations builders read (``SOURCE_COLUMNS``).

    Each native list becomes the JSON text of its source column (``docket_ids`` as ``docket_ids_json``) and every
    other column its text; columns the table lacks are left out. It is the published value, not the publisher's
    spelling: an agency entry keeps ``id``, ``name``, ``raw_name``, ``parent_id`` and ``slug``, and a CFR part reads as
    text whether the Register wrote ``60`` or ``"60"``, which the regulations parsers take alike. ``published`` is one
    file or a table's members.
    """
    import duckdb
    import pyarrow.parquet as pq

    paths = [Path(published)] if isinstance(published, (str, Path)) else [Path(path) for path in published]
    held = set(pq.read_schema(paths[0]).names)
    native = {source: target for source, (target, _) in NATIVE_FIELDS[dataset].items() if source != target}
    columns = []
    for name, _ in SOURCE_COLUMNS[dataset]:
        target = native.get(name, name)
        if target not in held:
            continue
        columns.append(f"array_to_string({target}, ',') AS {name}" if name == "agency_slugs" else
                       f"CAST(to_json({target}) AS VARCHAR) AS {name}" if name in native else
                       f"CAST({target} AS VARCHAR) AS {name}")
    listed = ", ".join("'" + str(path).replace("'", "''") + "'" for path in paths)
    destination.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY (SELECT {', '.join(columns)} FROM read_parquet([{listed}])) "
               f"TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return destination
