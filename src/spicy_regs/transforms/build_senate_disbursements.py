"""Type Senate ledger/page observations with shared PDF evidence validation."""

from datetime import date
from pathlib import Path

from spicy_regs.schemas.senate_disbursements import SCHEMAS, TABLES
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
        context = page["context"]
        common = {
            **source_columns(source, entry, page),
            **{
                field: context[field]
                for field in ("office", "account_label", "funding_year", "funding_year_end", "appropriation_title")
            },
            "context_basis": page.get("context_basis"),
            "context_origin_json": json_value(page.get("context_origin")),
        }
        if table == TABLES[1]:
            yield {**common, **page_columns(page)}
            continue
        for row in page["records"]:
            yield {
                **common,
                "source_line": row["line"],
                "row_kind": row["row_kind"],
                **{field: row[field] for field in ("document", "payee", "description")},
                **{
                    field: date.fromisoformat(row[field]) if row[field] else None
                    for field in ("date_posted", "service_start", "service_end")
                },
                "amount": money(row["amount"]),
                "group_notes": row.get("group_notes"),
                "group_origin_json": json_value(row["group_origin"]),
                "payee_origin_json": json_value(row.get("payee_origin")),
                "raw_fields_json": json_value(row["raw_fields"]),
                "word_indices": row["word_indices"],
                "issue_codes": list(dict.fromkeys([*page["issues"], *row["issues"]])),
                "repairs_json": json_value(row["repairs"]),
            }


def build_senate_disbursements(manifest_path: Path, output_dir: Path, *, evidence):
    return build_pdf_tables(
        manifest_path,
        output_dir,
        evidence=evidence,
        chamber="senate",
        tables=TABLES,
        schemas=SCHEMAS,
        rows=_rows,
    )
