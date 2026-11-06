"""Type reviewed summary cells with exact units and separate source occurrences."""

import json
from datetime import date
from functools import partial
from pathlib import Path

from spicy_regs.schemas.disbursement_summaries import FORMULA_COLUMNS, family_tables
from spicy_regs.transforms.disbursement_pdf_tables import (
    build_pdf_tables,
    exact_decimal,
    json_value,
    pages,
    source_columns,
)

MAILING_VALUES = {
    "total_distributed": (0, "items"),
    "average_distributed_per_household": (4, "items/household"),
    "total_cost": (2, "USD"),
    "average_cost_per_household": (4, "USD/household"),
}
PROFILE_VALUES = {"mailing-summary": MAILING_VALUES, "allowance-summary": {"allowance_amount": (2, "USD")}}


def _date(value):
    return date.fromisoformat(value) if value else None


def summary_rows(source, entry, table, *, chamber, profile, tables):
    for page in pages(source, entry):
        common = {**source_columns(source, entry, page), "chamber": chamber, "reading_profile": profile}
        if table == tables[2]:
            yield {
                **common,
                "page_status": page["status"],
                "native_text": page.get("text", page.get("native_text")),
                "native_word_count": len(page["words"]) if "words" in page else None,
                "image_count": page.get("image_count"),
                "record_count": len(page["records"]),
                "page_reading_json": json_value(page),
                "issue_codes": page["issues"],
            }
            continue
        for ordinal, row in enumerate(page["records"], 1):
            context = {
                **common,
                "summary_row_index": ordinal,
                "source_table": row.get("table"),
                "source_row": row.get("ruled_row", row.get("row")),
                "source_line": row.get("line"),
                "summary_kind": row.get("table_kind", row.get("grid_kind")),
                "row_kind": row.get("row_kind", "representative_summary"),
                "organization": page.get("context", {}).get("office"),
                "label": row.get(
                    "label", row["values"].get("representative_literal", row["values"].get("name_literal"))
                ),
                "appropriation_title": row.get("appropriation_title"),
                "appropriation_number": row.get("appropriation_number"),
                "funding_period_literal": row.get("funding_period_literal", row.get("funding_year_literal")),
                "summary_period_start": _date(row.get("period_start")),
                "summary_period_end": _date(row.get("period_end")),
                "amended": row.get("amended"),
                "issue_codes": list(dict.fromkeys([*page["issues"], *row["issues"]])),
            }
            if profile == "allowance-summary":
                context.update(
                    allowance_title=row["title"],
                    allowance_year=row["allowance_year"],
                    printed_title_year=row["printed_title_year"],
                    printed_column_year=row["printed_column_year"],
                    state_literal=row["values"]["state_literal"],
                    district_literal=row["values"]["district_literal"],
                )
            elif profile == "allowance-formula":
                context.update({name: row.get(source_key) for name, source_key, _ in FORMULA_COLUMNS})
                context["formula_effective_date"] = _date(row.get("effective_date"))
            if table == tables[0]:
                yield {
                    **context,
                    "values_json": json_value(row["values"]),
                    "raw_fields_json": json_value(row["raw_fields"]),
                    "source_row_json": json_value(row),
                }
                continue
            fields = PROFILE_VALUES.get(profile) or {key: (2, "USD") for key in row["values"]}
            if profile == "allowance-formula":
                # A formula's omitted bound or unsupplied external input is not
                # a blank numeric cell. Emit only its observed numeric fields.
                fields = {key: (spec["scale"], spec["unit"]) for key, spec in row["value_specs"].items()}
            for index, (field, (scale, unit)) in enumerate(fields.items(), 1):
                value = row["values"][field]
                raw = row["raw_fields"][field]
                yield {
                    **context,
                    "value_index": index,
                    "value_name": field,
                    "value_decimal": exact_decimal(value, scale),
                    "value_scale": scale,
                    "value_unit": unit,
                    "value_status": "parsed"
                    if value is not None
                    else "blank"
                    if raw is None or not raw.strip()
                    else "unresolved",
                    "raw_value": raw,
                }


def build_disbursement_summaries(manifest_path: Path, output_dir: Path, *, evidence):
    manifest = json.loads(manifest_path.read_text())
    chamber, profile = manifest["chamber"], manifest["reading_profile"]
    _, tables, schemas = family_tables(chamber, profile)
    return build_pdf_tables(
        manifest_path,
        output_dir,
        evidence=evidence,
        chamber=chamber,
        profile=profile,
        tables=tables,
        schemas=schemas,
        rows=partial(summary_rows, chamber=chamber, profile=profile, tables=tables),
    )
