"""House PDF financial rows and page coverage, distinct from CSV occurrences."""

import pyarrow as pa

from spicy_regs.schemas.disbursement_pages import PAGE_FIELDS, READING_FIELDS, SOURCE_FIELDS
from spicy_regs.schemas.disbursement_pages import PARTITION_COLUMNS as PARTITION_COLUMNS

FAMILY = "house-disbursement-pages"
TABLES = ("house_disbursement_pdf_rows", "house_disbursement_pdf_pages")
COMMON_FIELDS = SOURCE_FIELDS + READING_FIELDS
RECORD_FIELDS = (
    ("source_line", pa.int64()),
    ("row_kind", pa.string()),
    ("organizational_group", pa.string()),
    ("organization", pa.string()),
    ("fiscal_or_legislative_year", pa.string()),
    ("program", pa.string()),
    ("cost_category", pa.string()),
    ("transaction_date", pa.date32()),
    ("data_source", pa.string()),
    ("document", pa.string()),
    ("vendor_name", pa.string()),
    ("service_start_date", pa.date32()),
    ("service_end_date", pa.date32()),
    ("description", pa.string()),
    ("amount", pa.decimal128(38, 2)),
    ("ytd_amount", pa.decimal128(38, 2)),
    ("qtd_amount", pa.decimal128(38, 2)),
    ("office_origin_json", pa.string()),
    ("raw_fields_json", pa.string()),
    ("word_indices", pa.list_(pa.int64())),
    ("normalizations_json", pa.string()),
    ("repairs_json", pa.string()),
    ("issue_codes", pa.list_(pa.string())),
)
FIELDS = {
    TABLES[0]: COMMON_FIELDS + RECORD_FIELDS,
    TABLES[1]: COMMON_FIELDS + PAGE_FIELDS + (("header_basis", pa.string()), ("header_json", pa.string())),
}
SCHEMAS = {name: pa.schema(fields) for name, fields in FIELDS.items()}
