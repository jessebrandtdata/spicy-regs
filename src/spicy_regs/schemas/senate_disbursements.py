"""Senate ledger and page table declarations, independent of acquisition."""

import pyarrow as pa

from spicy_regs.schemas.disbursement_pages import PAGE_FIELDS, READING_FIELDS, SOURCE_FIELDS
from spicy_regs.schemas.disbursement_pages import PARTITION_COLUMNS as PARTITION_COLUMNS

FAMILY = "senate-disbursements"
TABLES = ("senate_disbursements", "senate_disbursement_pages")
CONTEXT_FIELDS = (
    ("office", pa.string()),
    ("account_label", pa.string()),
    ("funding_year", pa.string()),
    ("funding_year_end", pa.string()),
    ("appropriation_title", pa.string()),
    ("context_basis", pa.string()),
    ("context_origin_json", pa.string()),
)
COMMON_FIELDS = SOURCE_FIELDS + CONTEXT_FIELDS + READING_FIELDS
RECORD_FIELDS = (
    ("source_line", pa.int64()),
    ("row_kind", pa.string()),
    ("document", pa.string()),
    ("date_posted", pa.date32()),
    ("payee", pa.string()),
    ("service_start", pa.date32()),
    ("service_end", pa.date32()),
    ("description", pa.string()),
    ("amount", pa.decimal128(38, 2)),
    ("group_notes", pa.string()),
    ("group_origin_json", pa.string()),
    ("payee_origin_json", pa.string()),
    ("raw_fields_json", pa.string()),
    ("word_indices", pa.list_(pa.int64())),
    ("issue_codes", pa.list_(pa.string())),
    ("repairs_json", pa.string()),
)
FIELDS = {TABLES[0]: COMMON_FIELDS + RECORD_FIELDS, TABLES[1]: COMMON_FIELDS + PAGE_FIELDS}
SCHEMAS = {name: pa.schema(fields) for name, fields in FIELDS.items()}
