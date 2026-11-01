"""Explicit native business fields for retained FEC financial/legal/agency rows.

The field registry is reviewed data, not a suffix-based runtime classifier.
Conversion inputs and qualification evidence are returned separately for the
shared receipt writer. Invalid structures refuse rather than lose properties.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .fec_query import AMOUNT_TYPE

_ROOT = Path(__file__).resolve().parents[1]
FIELD_RULES = json.loads((_ROOT / "fec_subject_fields.json").read_text())
FIELD_MAPS = json.loads((_ROOT / "fec_native_field_maps.json").read_text())
_TEXT = pa.string()
_TEXTS = pa.list_(_TEXT)
_ATTRIBUTES = pa.list_(pa.struct([("name", _TEXT), ("value", _TEXT)]))
_LEGAL_CITATION = pa.struct([("text", _TEXT), ("url", _TEXT)])
_LEGAL_SUBJECT = pa.struct(
    [("path", pa.list_(pa.int32())), ("node", pa.struct([("text", _TEXT), ("children", pa.list_(pa.int32()))]))]
)
_LEGAL_FACTS = {
    "ao_no": _TEXT,
    "case_serial": pa.int64(),
    "election_cycles": pa.list_(pa.int32()),
    "ao_citations": pa.list_(pa.struct([("name", _TEXT), ("no", _TEXT)])),
    "aos_cited_by": pa.list_(pa.struct([("name", _TEXT), ("no", _TEXT)])),
    "regulatory_citations": pa.list_(pa.struct([("title", pa.int32()), ("part", pa.int32()), ("section", pa.int32())])),
    "statutory_citations": pa.list_(pa.struct([("title", pa.int32()), ("section", _TEXT)])),
    "citations": pa.struct([("regulations", pa.list_(_LEGAL_CITATION)), ("us_code", pa.list_(_LEGAL_CITATION))]),
    "subject": pa.list_(_LEGAL_SUBJECT),
    "subjects": pa.list_(
        pa.struct([("primary_subject_id", _TEXT), ("secondary_subject_id", _TEXT), ("subject", _TEXT)])
    ),
    "non_monetary_terms": _TEXTS,
    "non_monetary_terms_respondents": _TEXTS,
}
_LEGAL_SCALARS = {
    **dict.fromkeys(
        "challenge_outcome civil_penalty_payment_status report_type mur_type committee_description committee_designation committee_type rm_id rm_number".split(),
        _TEXT,
    ),
    "report_year": pa.int32(),
    "is_open_for_comment": pa.bool_(),
}
_ORGANIZATION = pa.struct(
    [
        ("id", _TEXT),
        ("names", _TEXTS),
        ("abbreviations", _TEXTS),
        ("role", _TEXT),
        ("label", _TEXT),
        ("value", _TEXT),
        ("items", _TEXTS),
    ]
)
_CONTEXT = pa.struct([("ordinal", pa.int64()), ("tag", _TEXT), ("text", _TEXT), ("attributes", _ATTRIBUTES)])
_MEASURES = pa.list_(pa.struct([("native_field", _TEXT), ("value", AMOUNT_TYPE), ("unit", _TEXT)]))
_REPORT_MEASURES = pa.list_(
    pa.struct(
        [
            ("native_position", pa.int32()),
            ("native_label", _TEXT),
            ("value", AMOUNT_TYPE),
            ("quantity_kind", _TEXT),
            ("measure_role", _TEXT),
            ("period_basis", _TEXT),
        ]
    )
)
_TEXT_FRAGMENTS = pa.list_(pa.struct([("field_position", pa.int32()), ("text", _TEXT)]))
FILING_ASSOCIATION_FIELDS = {
    **dict.fromkeys(
        "filing_association_status filing_association_policy filing_association_source_generation_pin "
        "filing_association_target_generation_pin filing_association_record_id "
        "filing_association_referenced_key filing_association_target_generation_scope".split(), _TEXT
    ),
    "filing_association_target_count": pa.int64(),
    "filing_association_target_record_ids": _TEXTS,
}


def _object(value, allowed, label):
    if not isinstance(value, dict) or set(value) - set(allowed):
        raise ValueError(f"Unsupported {label} properties or shape")
    return value


def _decode(value):
    return json.loads(value) if isinstance(value, str) else value


def _list(value, mapper):
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("Expected native repeated value")
    return [None if item is None else mapper(item) for item in value]


def _select(item, fields, allowed=None):
    return {name: _object(item, allowed or fields, "nested value").get(name) for name in fields}


# C1 controls in filing text mean Windows punctuation was decoded as latin-1
# (FEC-1981885 read U+0092 for an apostrophe); cp1252 reads the intended text.
# The class holds the literal characters, so Python re and DuckDB's RE2 read it alike;
# the output scan also catches JSON-escaped C1 sequences (\u0080 to \u009f).
_C1_CONTROL = re.compile("[\x80-\x9f]")
_C1_OUTPUT_PATTERN = _C1_CONTROL.pattern + r"|\\u00[89][0-9a-fA-F]"
_C1_REMEDY = "decode the filing with spicy-docs filing_encoding (UTF-8, then cp1252), not latin-1"


def _holds_text(dtype):
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
        return True
    if pa.types.is_struct(dtype):
        return any(_holds_text(field.type) for field in dtype)
    if pa.types.is_map(dtype):
        return _holds_text(dtype.key_type) or _holds_text(dtype.item_type)
    if pa.types.is_dictionary(dtype):
        return _holds_text(dtype.value_type)
    return hasattr(dtype, "value_type") and _holds_text(dtype.value_type)


def refuse_c1_controls(tables, *, check_resources=lambda: None, threads=6):
    """Refuse a build whose assembled tables hold any C1 control in any text value.

    One output-level pass per build: each table (a Parquet file or a partitioned
    directory) is read once, only over columns that hold text at any depth, and
    nested values are matched through their VARCHAR rendering. _text_fragment
    keeps the cheap per-fragment refusal where filing text is mapped; only this
    scan sees the other tables (ss-f64m: live fec-query 784b23da holds 3 such
    rows, all in fec_filing_text_observations).
    """
    import duckdb

    found = {}
    with duckdb.connect(config={"threads": threads, "memory_limit": "1GiB",
                                "max_temp_directory_size": "0B"}) as con:
        for table in tables:
            check_resources()
            members = sorted(table.rglob("*.parquet")) if table.is_dir() else [table]
            columns = [field.name for field in pq.read_schema(members[0]) if _holds_text(field.type)]
            if not columns:
                continue
            matched = " OR ".join(
                'regexp_matches(CAST("{}" AS VARCHAR), \'{}\')'.format(name.replace('"', '""'), _C1_OUTPUT_PATTERN)
                for name in columns)
            [(rows,)] = con.execute(f"SELECT count(*) FILTER (WHERE {matched}) FROM read_parquet(?, hive_partitioning=false)",
                                    [[str(path) for path in members]]).fetchall()
            if rows:
                found[table.name.removesuffix(".parquet")] = rows
    if found:
        raise ValueError(f"FEC output carries C1 controls (rows by table: {found}); {_C1_REMEDY}")


def _text_fragment(item):
    fragment = _select(
        item,
        ["field_position", "text"],
        ["field_position", "text", "source_column", "source_pointer", "text_status", "body_reference_json"],
    )
    if isinstance(fragment["text"], str) and (match := _C1_CONTROL.search(fragment["text"])):
        raise ValueError(
            f"FEC filing text carries C1 control U+{ord(match.group()):04X} at character {match.start()}; {_C1_REMEDY}"
        )
    return fragment


def _scalar(value, dtype):
    if value is None:
        return None
    if pa.types.is_string(dtype):
        if isinstance(value, str):
            return value
        # Source legal IDs may be JSON integers. The original token stays in the receipt.
        if type(value) is int:
            return str(value)
    elif pa.types.is_boolean(dtype) and type(value) is bool:
        return value
    elif pa.types.is_integer(dtype) and type(value) is int:
        return value
    elif pa.types.is_integer(dtype) and isinstance(value, str) and value.isdecimal():
        return int(value)
    raise ValueError("Unsupported native scalar type")


def _subject_tree(value):
    if value is None:
        return None
    nodes = []

    def visit(items, parent):
        if not isinstance(items, list):
            raise ValueError("Legal subject children must be a list")
        for i, item in enumerate(items):
            path = [*parent, i]
            if item is None:
                nodes.append(dict(path=path, node=None))
                continue
            item = _object(item, {"text", "children"}, "legal subject")
            children = item.get("children")
            if children is not None and not isinstance(children, list):
                raise ValueError("Legal subject children must be a list")
            nodes.append(
                dict(
                    path=path,
                    node=dict(text=item.get("text"), children=None if children is None else list(range(len(children)))),
                )
            )
            if children is not None:
                visit(children, path)

    visit(value, [])
    return nodes


def _legal_facts(value):
    if value is None:
        return {name: None for name in _LEGAL_FACTS | _LEGAL_SCALARS}
    _object(value, _LEGAL_FACTS | _LEGAL_SCALARS, "legal facts")
    result = {}
    for name, dtype in (_LEGAL_FACTS | _LEGAL_SCALARS).items():
        item = value.get(name)
        if name == "subject":
            result[name] = _subject_tree(item)
        elif pa.types.is_list(dtype) and pa.types.is_struct(dtype.value_type):
            result[name] = _list(
                item,
                lambda v: {
                    f.name: _scalar(_object(v, dtype.value_type.names, name).get(f.name), f.type)
                    for f in dtype.value_type
                },
            )
        elif name == "citations":
            if item is not None:
                _object(item, {"regulations", "us_code"}, name)
            result[name] = (
                None
                if item is None
                else {
                    key: _list(item.get(key), lambda v: _select(v, ["text", "url"]))
                    for key in ("regulations", "us_code")
                }
            )
        elif pa.types.is_list(dtype):
            result[name] = _list(item, lambda v: _scalar(v, dtype.value_type))
        else:
            result[name] = _scalar(item, dtype)
    return result


def _organization(value):
    if "native_field" in value:
        _object(
            value,
            {"native_field", "label", "value", "items", "source_position", "links", "times", "numeric_content"},
            "agency organization",
        )
        return dict(
            id=None,
            names=None,
            abbreviations=None,
            role=value.get("native_field"),
            label=value.get("label"),
            value=value.get("value"),
            items=_list(value.get("items"), lambda v: v.get("text")),
        )
    _object(value, {"element", "id", "names", "abbreviations"}, "agency organization")
    return dict(
        id=value.get("id"),
        names=_list(value.get("names"), lambda v: _select(v, ["value"], ["value", "element"])["value"]),
        abbreviations=_list(value.get("abbreviations"), lambda v: _select(v, ["value"], ["value", "element"])["value"]),
        role=None,
        label=None,
        value=None,
        items=None,
    )


def native_fields(table):
    """Additional or reshaped business columns, explicitly declared by family."""
    if FIELD_RULES[table].get("observation_control"):
        return {"observation_ordinal": pa.int64(), **({"source_sha256": _TEXT} if table == "fec_agency_mapping_dispositions" else {})}
    if table == "fec_agency_mapping_dispositions":
        return {"source_sha256": _TEXT}
    if table == "fec_legal_matters":
        return _LEGAL_FACTS | _LEGAL_SCALARS
    if table == "fec_reported_financial_summaries":
        return {**dict.fromkeys(FIELD_MAPS["summary_attributes"].values(), _TEXT), "measures": _MEASURES}
    if table == "fec_contribution_aggregates":
        return dict.fromkeys(["size_of_contribution", "contributor_state", "state_name", "zip_3"], _TEXT)
    if table == "fec_filing_report_observations":
        return {"reported_measures": _REPORT_MEASURES}
    if table == "fec_filing_text_observations":
        return {"text_fragments": _TEXT_FRAGMENTS}
    if table == "fec_agency_reports":
        return {
            "organizations": pa.list_(_ORGANIZATION),
            **dict.fromkeys(FIELD_MAPS["agency_report_fields"].values(), _TEXT),
        }
    if table == "fec_report_metrics":
        return dict(
            definition_namespace=_TEXT,
            definition_path=_TEXTS,
            definition_schema_version=_TEXT,
            dimension_context=pa.list_(_CONTEXT),
            report_number=_TEXT,
        )
    if table == "fec_oversight_recommendations":
        return dict(significant=_TEXT, status_date=_TEXT, report_number=_TEXT, additional_details=_TEXT)
    if table == "fec_agency_report_text":
        return {"text_ordinal": pa.int64()}
    if table == "fec_legal_documents":
        return {"document_length": _TEXT}
    return {}


def subject_schema(table, input_schema):
    rule = FIELD_RULES[table]
    known = set(rule["keep"] + rule["receipt"] + rule["mapped"])
    if set(input_schema.names) - known:
        raise ValueError(f"Unclassified FEC columns in {table}: {set(input_schema.names) - known}")
    if rule["receipt_only"]:
        return pa.schema([])
    fields = {f.name: f.type for f in input_schema if f.name in rule["keep"]}
    fields.update(native_fields(table))
    # These are decision fields, available only on a financial/text mapper
    # schema which already declares a filing key. Inapplicable rows gain none.
    if "filing_key" in input_schema.names and "filing_key" in rule["keep"]:
        fields.update(FILING_ASSOCIATION_FIELDS)
    return pa.schema(list(fields.items()))


def prepare_subject(table, row):
    """Return subject and conversion inputs; retain exact inputs for receipt restoration."""
    rule = FIELD_RULES[table]
    known = set(rule["keep"] + rule["receipt"] + rule["mapped"])
    if set(row) - known:
        raise ValueError(f"Unclassified FEC columns in {table}: {set(row) - known}")
    result = {n: row[n] for n in rule["keep"] if n in row}
    inputs = {n: row[n] for n in rule["receipt"] + rule["mapped"] if n in row}
    if rule["receipt_only"]:
        return None, inputs
    if table == "fec_legal_matters":
        facts = _legal_facts(_decode(row.get("native_facts_json")))
        # An existing typed column is the producer's authoritative interpretation.
        for name, value in facts.items():
            if name not in result:
                result[name] = value
    elif table == "fec_reported_financial_summaries":
        values = _decode(row.get("attributes_json"))
        if values is not None:
            _object(values, {*FIELD_MAPS["summary_attributes"], "Link_Image"}, "financial summary attributes")
        for key, target in FIELD_MAPS["summary_attributes"].items():
            result[target] = None if values is None else values.get(key)
        result["measures"] = _list(
            row.get("measures"),
            lambda v: _select(
                v, ["native_field", "value", "unit"], ["native_field", "raw_value", "value", "value_status", "unit"]
            ),
        )
    elif table == "fec_contribution_aggregates":
        values = _decode(row.get("dimensions_json"))
        fields = dict(
            SIZE_OF_CONTRIBUTION="size_of_contribution",
            CONTRIB_STATE="contributor_state",
            STATE_NAME="state_name",
            ZIP_3="zip_3",
        )
        if values is not None:
            _object(values, fields, "contribution dimensions")
        result.update({target: None if values is None else values.get(key) for key, target in fields.items()})
    elif table == "fec_filing_report_observations":
        result["reported_measures"] = _list(
            row.get("reported_measures"),
            lambda v: _select(
                v,
                _REPORT_MEASURES.value_type.names,
                [*_REPORT_MEASURES.value_type.names, "raw_value", "value_status", "definition_cell"],
            ),
        )
    elif table == "fec_filing_text_observations":
        result["text_fragments"] = _list(row.get("text_fragments"), _text_fragment)
    elif table == "fec_agency_reports":
        result["organizations"] = _list(_decode(row.get("organizations_json")), _organization)
        meta = _decode(row.get("metadata_json"))
        result.update(dict.fromkeys(FIELD_MAPS["agency_report_fields"].values()))
        if meta is not None:
            _object(
                meta,
                "creation_dates fields fiscal_year links organizations package_attributes recommendation_body_indices representation schema_version title title_position".split(),
                "agency report metadata",
            )
            seen = set()
            for field in meta.get("fields", []):
                key = field["native_field"]
                if key not in FIELD_MAPS["agency_report_fields"] or key in seen:
                    raise ValueError("Unknown or repeated agency report scalar; requires a reviewed native list")
                seen.add(key)
                result[FIELD_MAPS["agency_report_fields"][key]] = field.get("value")
    elif table == "fec_report_metrics":
        definition = _decode(row.get("definition_json"))
        dimensions = _decode(row.get("dimensions_json"))
        if definition is not None:
            _object(
                definition,
                "namespace path schema_version source_format unit label native_field source_namespace".split(),
                "metric definition",
            )
        if dimensions is not None:
            _object(
                dimensions, "native_context numeric_content report_number source_position".split(), "metric dimensions"
            )
        definition = definition or {}
        dimensions = dimensions or {}
        result.update(
            definition_namespace=definition.get("namespace", definition.get("source_namespace")),
            definition_path=definition.get("path"),
            definition_schema_version=definition.get("schema_version"),
            report_number=dimensions.get("report_number"),
        )

        def context(value):
            _object(value, "element source_record_id tag text attributes".split(), "metric dimension context")
            attributes = value.get("attributes")
            if attributes is not None and not isinstance(attributes, dict):
                raise ValueError("Metric attributes must be named scalar attributes")
            return dict(
                ordinal=value.get("element"),
                tag=value.get("tag"),
                text=value.get("text"),
                attributes=None if attributes is None else [dict(name=k, value=v) for k, v in attributes.items()],
            )

        result["dimension_context"] = _list(dimensions.get("native_context"), context)
    elif table == "fec_oversight_recommendations":
        result.update(significant=row.get("significant_raw"), status_date=row.get("status_date_raw"))
        attrs = _decode(row.get("attributes_json"))
        if attrs is not None:
            _object(attrs, "cells description_cell report_number".split(), "recommendation attributes")
        attrs = attrs or {}
        result.update(report_number=attrs.get("report_number"), additional_details=None)
        details = [
            c.get("text")
            for c in attrs.get("cells", [])
            if c.get("attributes", {}).get("data-label") == "Additional Details"
        ]
        if len(details) > 1:
            raise ValueError("Repeated recommendation detail needs a reviewed native list")
        if details:
            result["additional_details"] = details[0]
    elif table == "fec_agency_report_text":
        ordinal = row.get("source_ordinal")
        if ordinal is None:
            locator = _decode(row.get("source_locator_json"))
            ordinal = locator.get("ordinal") if isinstance(locator, dict) else None
        if ordinal is not None and (type(ordinal) is not int or ordinal < 0):
            raise ValueError("Agency text reading order must be a nonnegative integer")
        result["text_ordinal"] = ordinal
    elif table == "fec_legal_documents":
        result["document_length"] = row.get("length_raw")
    return result, inputs
