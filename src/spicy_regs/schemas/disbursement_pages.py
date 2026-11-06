"""Shared source identity and page coverage for congressional PDF readings."""

import pyarrow as pa

PARTITION_COLUMNS = ("report_year", "report_start_month")
SOURCE_FIELDS = (
    ("source_file_id", pa.string()),
    ("source_url", pa.string()),
    ("source_sha256", pa.string()),
    ("package_id", pa.string()),
    ("source_page", pa.int64()),
    ("printed_page", pa.string()),
    ("report_year", pa.int32()),
    ("report_start_month", pa.int32()),
    ("report_period_start", pa.date32()),
    ("report_period_end", pa.date32()),
)
READING_FIELDS = (("reader_version", pa.string()), ("reading_sha256", pa.string()))
PAGE_FIELDS = (
    ("page_status", pa.string()),
    ("native_text", pa.string()),
    ("native_word_count", pa.int64()),
    ("image_count", pa.int64()),
    ("record_count", pa.int64()),
    ("unassigned_line_count", pa.int64()),
    ("unassigned_lines_json", pa.string()),
    ("outside_ledger_word_indices", pa.list_(pa.int64())),
    ("issue_codes", pa.list_(pa.string())),
)
