"""Shared typed shapes for distinct House and Senate summary families."""

import pyarrow as pa

from spicy_regs.schemas.disbursement_pages import READING_FIELDS, SOURCE_FIELDS
from spicy_regs.schemas.disbursement_pages import PARTITION_COLUMNS as PARTITION_COLUMNS

PREFIXES = {
    ("house", "financial-summary"): "house_financial_summary",
    ("house", "mailing-summary"): "house_mailing_summary",
    ("house", "allowance-summary"): "house_allowance_summary",
    ("senate", "financial-summary"): "senate_financial_summary",
}
COMMON = SOURCE_FIELDS + READING_FIELDS + (("chamber", pa.string()), ("reading_profile", pa.string()))
ROW_CONTEXT = (
    ("summary_row_index", pa.int64()),
    ("source_table", pa.int64()),
    ("source_row", pa.int64()),
    ("source_line", pa.int64()),
    ("summary_kind", pa.string()),
    ("row_kind", pa.string()),
    ("organization", pa.string()),
    ("label", pa.string()),
    ("appropriation_title", pa.string()),
    ("appropriation_number", pa.string()),
    ("funding_period_literal", pa.string()),
    ("summary_period_start", pa.date32()),
    ("summary_period_end", pa.date32()),
    ("amended", pa.bool_()),
    ("issue_codes", pa.list_(pa.string())),
)
ROW_FIELDS = (
    ("values_json", pa.string()),
    ("raw_fields_json", pa.string()),
    ("source_row_json", pa.string()),
)
VALUE_FIELDS = (
    ("value_index", pa.int32()),
    ("value_name", pa.string()),
    ("value_decimal", pa.decimal128(38, 4)),
    ("value_scale", pa.int8()),
    ("value_unit", pa.string()),
    ("value_status", pa.string()),
    ("raw_value", pa.string()),
)
PAGE_FIELDS = (
    ("page_status", pa.string()),
    ("native_text", pa.string()),
    ("native_word_count", pa.int64()),
    ("image_count", pa.int64()),
    ("record_count", pa.int64()),
    ("page_reading_json", pa.string()),
    ("issue_codes", pa.list_(pa.string())),
)
ALLOWANCE_CONTEXT = (
    ("allowance_title", pa.string()),
    ("allowance_year", pa.int32()),
    ("printed_title_year", pa.int32()),
    ("printed_column_year", pa.int32()),
    ("state_literal", pa.string()),
    ("district_literal", pa.string()),
)
SUFFIX_FIELDS = {"rows": ROW_CONTEXT + ROW_FIELDS, "values": ROW_CONTEXT + VALUE_FIELDS, "pages": PAGE_FIELDS}
SCHEMAS = {
    prefix + "_" + suffix: pa.schema(
        COMMON + fields + (ALLOWANCE_CONTEXT if prefix == "house_allowance_summary" and suffix != "pages" else ())
    )
    for prefix in PREFIXES.values()
    for suffix, fields in SUFFIX_FIELDS.items()
}
TABLES = tuple(SCHEMAS)


def family_tables(chamber, profile):
    try:
        prefix = PREFIXES[chamber, profile]
    except KeyError as error:
        raise ValueError("Unsupported disbursement summary family") from error
    tables = tuple(prefix + "_" + suffix for suffix in SUFFIX_FIELDS)
    return prefix.replace("_", "-"), tables, {table: SCHEMAS[table] for table in tables}
