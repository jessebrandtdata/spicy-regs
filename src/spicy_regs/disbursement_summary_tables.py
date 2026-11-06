"""Disbursement summary table names shared by serving metadata and writers."""

PREFIXES = {
    ("house", "financial-summary"): "house_financial_summary",
    ("house", "mailing-summary"): "house_mailing_summary",
    ("house", "allowance-summary"): "house_allowance_summary",
    ("house", "allowance-formula"): "house_allowance_formula",
    ("senate", "financial-summary"): "senate_financial_summary",
}
SUFFIXES = ("rows", "values", "pages")
TABLES = tuple(prefix + "_" + suffix for prefix in PREFIXES.values() for suffix in SUFFIXES)
