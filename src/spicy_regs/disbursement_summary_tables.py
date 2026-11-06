"""Disbursement summary table names shared by builders and import-light serving."""

PREFIXES = {
    ("house", "financial-summary"): "house_financial_summary",
    ("house", "mailing-summary"): "house_mailing_summary",
    ("house", "allowance-summary"): "house_allowance_summary",
    ("senate", "financial-summary"): "senate_financial_summary",
}
SUFFIXES = ("rows", "values", "pages")
TABLES = tuple(prefix + "_" + suffix for prefix in PREFIXES.values() for suffix in SUFFIXES)
