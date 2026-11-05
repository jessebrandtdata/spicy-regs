"""Independent values for the names preview and unchanged detailed pages."""
import duckdb
import pytest

from spicy_regs import mcp_server as server
from tests.test_mcp_server import _tool_data


@pytest.fixture
def discovery(monkeypatch):
    # These two labels/kinds are literal expectations from the retained public
    # source descriptions, not values calculated from the handler's reply.
    with duckdb.connect() as con:
        for name in ("dockets", "lobbying_filings", "unmapped_retained_table"):
            con.execute(f'CREATE TABLE "{name}" (id VARCHAR)')
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        monkeypatch.setattr(server, "TABLES", (
            "dockets", "lobbying_filings", "unmapped_retained_table",
            "missing_01", "missing_02", "missing_03", "missing_04", "missing_05",
            "missing_06", "missing_07", "missing_08", "missing_09", "missing_10",
        ))
        yield server.build_server()


def test_default_names_each_table_without_repeating_details(discovery):
    reply = _tool_data(discovery, "list_sources", {})
    assert reply["subjects"] == [
        {"subject": "rulemaking", "tables": ["dockets"]},
        {"subject": "lobbying", "tables": ["lobbying_filings"]},
        {"subject": "unassigned", "tables": ["unmapped_retained_table"]},
    ]


def test_subject_pages_preserve_independent_labels_kinds_and_unknown_values(discovery):
    assert _tool_data(discovery, "list_sources", {"subject": "rulemaking"})["subjects"] == [
        {"subject": "rulemaking", "tables": [
            {"table": "dockets", "label": "Rulemaking dockets", "coverage": "sampled", "rows": None},
        ]},
    ]
    assert _tool_data(discovery, "list_sources", {"subject": "lobbying"})["subjects"] == [
        {"subject": "lobbying", "tables": [
            {"table": "lobbying_filings", "label": "Lobbying disclosure filings", "coverage": "sampled", "rows": None},
        ]},
    ]
    # An unknown retained name gets no invented current label or source kind.
    assert _tool_data(discovery, "list_sources", {"subject": "unassigned"})["subjects"] == [
        {"subject": None, "tables": [
            {"table": "unmapped_retained_table", "label": None, "coverage": "derived", "rows": None},
        ]},
    ]


def test_default_preview_has_the_exact_count_and_full_views_page_keeps_every_name(discovery):
    default = _tool_data(discovery, "list_sources", {})
    views = _tool_data(discovery, "list_sources", {"subject": "views"})
    assert default["unavailable_tables"] == [
        "missing_01", "missing_02", "missing_03", "missing_04",
        "missing_05", "missing_06", "missing_07", "missing_08",
    ]
    assert default["unavailable_table_count"] == 10
    assert views["unavailable_tables"] == [
        "missing_01", "missing_02", "missing_03", "missing_04", "missing_05",
        "missing_06", "missing_07", "missing_08", "missing_09", "missing_10",
    ]
    assert default["generation"] == views["generation"]
