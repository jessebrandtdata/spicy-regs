"""House CSV table types shared by builders, metadata generation and readers."""

import pyarrow as pa

FAMILY = "house-disbursements"
TABLES = {"detail": "house_disbursements", "summary": "house_disbursements_summary"}
PARTITION_COLUMNS = ("report_year", "report_quarter")
MONEY = pa.decimal128(38, 2)

# Column names and types are owned here; their prose is in descriptions.yaml.
COMMON_FIELDS = (
    ("source_file_id", pa.string()),
    ("source_url", pa.string()),
    ("source_sha256", pa.string()),
    ("source_record_number", pa.int64()),
    ("source_line_start", pa.int64()),
    ("source_line_end", pa.int64()),
    ("report_year", pa.int32()),
    ("report_quarter", pa.int32()),
    ("report_period_start", pa.date32()),
    ("report_period_end", pa.date32()),
    ("row_kind", pa.string()),
    ("issue_codes", pa.list_(pa.string())),
    ("correction_rules", pa.list_(pa.string())),
    ("correction_confidence", pa.list_(pa.string())),
    ("source_encoding", pa.string()),
    ("source_layout", pa.string()),
    ("normalization_version", pa.string()),
    ("normalization_receipt_sha256", pa.string()),
    ("audit_json", pa.string()),
)
DETAIL_FIELDS = (
    ("organization", pa.string()),
    ("fiscal_or_legislative_year", pa.string()),
    ("organization_code", pa.string()),
    ("program", pa.string()),
    ("program_code", pa.string()),
    ("cost_category", pa.string()),
    ("budget_object_class", pa.string()),
    ("sort_sequence", pa.string()),
    ("transaction_date", pa.date32()),
    ("data_source", pa.string()),
    ("document", pa.string()),
    ("vendor_name", pa.string()),
    ("vendor_id", pa.string()),
    ("service_start_date", pa.date32()),
    ("service_end_date", pa.date32()),
    ("description", pa.string()),
    ("budget_object_code", pa.string()),
    ("amount", MONEY),
)
SUMMARY_FIELDS = (
    ("organization", pa.string()),
    ("program", pa.string()),
    ("description", pa.string()),
    ("ytd_amount", MONEY),
    ("qtd_amount", MONEY),
)
FIELDS = {TABLES["detail"]: COMMON_FIELDS + DETAIL_FIELDS, TABLES["summary"]: COMMON_FIELDS + SUMMARY_FIELDS}
SCHEMAS = {name: pa.schema([(key, dtype) for key, dtype in fields]) for name, fields in FIELDS.items()}
