"""Exact source-owned Google Books locators survive the public privacy filter."""

import builtins
from pathlib import Path

import pytest

from spicy_regs.source_catalog.export_completion import implementation
from spicy_regs.source_catalog.exporter import file_provenance, public_url
from spicy_regs.source_catalog.storage import digest


GOOGLE_PDF = (
    "https://books.google.com/books/download/Report_of_the_Secretary_of_the_Senate_fr.pdf?id=bjhOAQAAMAAJ&output=pdf"
)


@pytest.mark.parametrize(
    "url", [GOOGLE_PDF, GOOGLE_PDF.replace("id=bjhOAQAAMAAJ&output=pdf", "output=pdf&id=bjhOAQAAMAAJ")]
)
def test_observed_google_pdf_locator_preserves_exact_origin_without_inventing_capture(url):
    assert public_url(url) == url
    assert file_provenance({"source_url": url}, status="held", local=True) == {
        "basis": "inferred",
        "sourceUrls": [url],
        "availability": "local",
    }


@pytest.mark.parametrize(
    "url",
    [
        GOOGLE_PDF.replace("https:", "http:"),
        GOOGLE_PDF.replace("https:", "file:"),
        GOOGLE_PDF.replace("books.google.com", "user:secret@books.google.com"),
        GOOGLE_PDF.replace("books.google.com", "user@books.google.com"),
        GOOGLE_PDF.replace("books.google.com", "books.google.com:443"),
        GOOGLE_PDF.replace("books.google.com", "books.google.com.example.test"),
        GOOGLE_PDF.replace("books.google.com", "www.google.com"),
        GOOGLE_PDF.replace("/books/download/", "/books/redirect/"),
        GOOGLE_PDF.replace("/books/download/", "/books/download/../"),
        GOOGLE_PDF.replace("/books/download/", "/books/download/%2e%2e/"),
        GOOGLE_PDF.replace(".pdf?", ".html?"),
        GOOGLE_PDF.replace("bjhOAQAAMAAJ", "invalid"),
        GOOGLE_PDF.replace("output=pdf", "output=epub"),
        GOOGLE_PDF.replace("&output=pdf", ""),
        GOOGLE_PDF + "&token=secret",
        GOOGLE_PDF + "&redirect=https%3A%2F%2Fexample.test",
        GOOGLE_PDF + "&id=bjhOAQAAMAAJ",
        GOOGLE_PDF + "#page=1",
        GOOGLE_PDF + "#",
        GOOGLE_PDF.replace("Report_", "%00Report_", 1),
    ],
)
def test_google_pdf_exception_refuses_unqualified_paths_queries_and_credentials(url):
    assert public_url(url) is None
    assert file_provenance({"source_url": url}, status="held", local=True)["sourceUrls"] == []


def test_google_pdf_exception_uses_maintained_source_validator(monkeypatch):
    from spicy_docs.sources import disbursements_reports

    calls = []

    def refuse(value):
        calls.append(value)
        raise disbursements_reports.DisbursementsReportSourceError("source refuses locator")

    monkeypatch.setattr(disbursements_reports, "disbursements_report_locator", refuse)
    assert public_url(GOOGLE_PDF) is None
    assert calls == [GOOGLE_PDF]


def test_google_pdf_exception_requires_source_reader_dependency(monkeypatch):
    original = builtins.__import__

    def without_reader(name, *args, **kwargs):
        if name == "spicy_docs.sources.disbursements_reports":
            raise ImportError("source readers unavailable")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_reader)
    assert public_url(GOOGLE_PDF) is None
    assert public_url("https://example.test/report.pdf") == "https://example.test/report.pdf"


def test_export_completion_pins_the_source_locator_policy():
    from spicy_docs.sources import disbursements_reports

    source = Path(disbursements_reports.__file__).resolve()
    assert implementation()["sources"][str(source)] == digest(source)
