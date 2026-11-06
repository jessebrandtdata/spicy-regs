"""Type House PDF observations through the shared PDF evidence lifecycle."""

from datetime import date
from pathlib import Path

from spicy_regs.schemas.house_disbursement_pages import SCHEMAS, TABLES
from spicy_regs.transforms.disbursement_pdf_tables import (
    build_pdf_tables,
    json_value,
    money,
    page_columns,
    pages,
    source_columns,
)


def _rows(source, entry, table):
    for page in pages(source, entry):
        common = source_columns(source, entry, page)
        if table == TABLES[1]:
            yield {
                **common,
                **page_columns(page),
                "header_basis": page["header_basis"],
                "header_json": json_value(page["header"]),
            }
            continue
        for row in page["records"]:
            yield {
                **common,
                "source_line": row["line"],
                **{
                    field: row[field]
                    for field in (
                        "row_kind",
                        "organizational_group",
                        "organization",
                        "fiscal_or_legislative_year",
                        "program",
                        "cost_category",
                        "data_source",
                        "document",
                        "vendor_name",
                        "description",
                        "word_indices",
                    )
                },
                **{
                    field: date.fromisoformat(row[field]) if row[field] else None
                    for field in ("transaction_date", "service_start_date", "service_end_date")
                },
                **{field: money(row[field]) for field in ("amount", "ytd_amount", "qtd_amount")},
                **{
                    field + "_json": json_value(row[field])
                    for field in ("office_origin", "raw_fields", "normalizations", "repairs")
                },
                "issue_codes": list(dict.fromkeys([*page["issues"], *row["issues"]])),
            }


def build_house_disbursement_pages(manifest_path: Path, output_dir: Path, *, evidence):
    return build_pdf_tables(
        manifest_path,
        output_dir,
        evidence=evidence,
        chamber="house",
        tables=TABLES,
        schemas=SCHEMAS,
        rows=_rows,
    )
