"""Select source-owned page readers and validate their distinct row coordinates.

Custody, checkpoints and generation writing stay in the shared page pipeline.
These adapters preserve each reader's output without inventing ledger fields
for financial summaries or mailing tables.
"""

from importlib import import_module

PROFILES = ("ledger", "financial-summary", "mailing-summary", "allowance-summary")
READERS = {
    ("house", "ledger"): ("house_disbursements_pdf", "iter_house_disbursements_pages"),
    ("senate", "ledger"): ("senate_disbursements", "iter_senate_disbursements_pages"),
    ("house", "financial-summary"): ("house_financial_summaries", "iter_house_summary_pages"),
    ("senate", "financial-summary"): ("senate_disbursement_summaries", "iter_senate_summary_pages"),
    ("house", "mailing-summary"): ("house_mailing_summaries", "iter_house_mailing_summary_pages"),
    ("house", "allowance-summary"): ("house_allowance_summaries", "iter_house_allowance_summary_pages"),
}


def reader_module(chamber, profile):
    try:
        return "spicy_docs.reading." + READERS[chamber, profile][0]
    except KeyError as error:
        raise ValueError("No source-owned page reader for this chamber/profile") from error


def read_source_pages(source, body, profile):
    from datetime import date

    module = import_module(reader_module(source["chamber"], profile))
    reader = getattr(module, READERS[source["chamber"], profile][1])
    options = {}
    if "reading_pages" in source:
        options["pages"] = source["reading_pages"]
    if profile == "ledger" or (source["chamber"], profile) == ("house", "financial-summary"):
        options.update(
            report_period_start=date.fromisoformat(source["period_start"]),
            report_period_end=date.fromisoformat(source["period_end"]),
        )
    return reader(body, **options)


def record_kind(row):
    return row.get("row_kind") or row.get("table_kind") or "unresolved_summary_kind"


def validate_page_content(page, profile):
    """Check retained indices and row occurrence identity, without reinterpreting cells."""
    if "words" in page:
        if [word["index"] for word in page["words"]] != list(range(len(page["words"]))):
            raise ValueError("PDF reading repeats or omits a native word index")
        if "lines" in page:
            indices = [index for line in page["lines"] for index in line["word_indices"]]
            indices.extend(page["outside_ledger_word_indices"])
            if sorted(indices) != list(range(len(page["words"]))):
                raise ValueError("PDF reading omitted or repeated a native word")
        for row in page["records"]:
            if any(type(i) is not int or not 0 <= i < len(page["words"]) for i in row["word_indices"]):
                raise ValueError("PDF financial row names a nonexistent original word")
        money = [i for row in page["records"] for i in row.get("money_word_indices", [])]
        if len(money) != len(set(money)):
            raise ValueError("PDF financial summaries repeat an original monetary token")
    elif profile not in {"mailing-summary", "allowance-summary"}:
        raise ValueError("PDF page profile requires retained original words")
    if profile in {"mailing-summary", "allowance-summary"}:
        coordinates = [(row["table"], row["row"]) for row in page["records"]]
        first_row = 0 if profile == "allowance-summary" else 1
        for table, row in coordinates:
            if not 0 <= table < len(page["tables"]) or not first_row <= row < page["tables"][table]["row_count"]:
                raise ValueError("Summary row lies outside its retained source table")
        if profile == "allowance-summary":
            validate = import_module(reader_module("house", profile)).validate_allowance_record
            for record in page["records"]:
                validate(page, record)
    elif profile == "financial-summary" and "grids" in page:
        coordinates = [(row["table"], row["ruled_row"], row["line"]) for row in page["records"]]
    else:
        coordinates = [(row["line"],) for row in page["records"]]
    if coordinates != sorted(set(coordinates)):
        raise ValueError("PDF reading repeats or reorders a financial row")
