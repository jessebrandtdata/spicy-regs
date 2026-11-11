"""Actual legacy columns retain meanings without redeclaring native subject fields.

Since its receipts were retired (ss-vse3, 2026-11-11) ``lobbying_filings`` is native by design: no generation of it
is read as pre-native again, so its pre-native column meanings are gone (``mcp_server._legacy_column_meanings``).
"""
import asyncio
import copy
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from mcp.types import CallToolResult

from spicy_regs import mcp_server
from spicy_regs.etl_policy_registry import installed_policies


LEGACY = json.loads((Path(__file__).parent / 'fixtures/lobbying_filings_legacy_publication.json').read_text())
# ``url`` was one too until the receipts were retired (ss-vse3, 2026-11-11): it is the native table's last column again.
HISTORICAL = {'lobbying_activities_json', 'government_entities_json'}


def describe(monkeypatch, *, native=False, extra_legacy=False, local_native=False, detail=False):
    con = duckdb.connect()
    index = copy.deepcopy(LEGACY)
    entry = index['families']['lobbying-filings']
    if native:
        schema = installed_policies()['lobbying_filings'].subject_schema
        con.register('subjects', pa.Table.from_batches([], schema=schema))
        con.execute('CREATE TABLE lobbying_filings AS SELECT * FROM subjects')
        entry['etlReceipts'] = dict(key='etl_receipts.parquet', rows=0, sha256='sha256:' + '0' * 64,
                                   byteSize=0, generationId='native-fixture', datasets=['lobbying_filings'])
    else:
        columns = entry['tables']['lobbying_filings.parquet']['columns']
        con.execute('CREATE TABLE lobbying_filings (' + ','.join(f'"{name}" {dtype}' for name, dtype in columns) + ')')
    if extra_legacy:
        con.execute('ALTER TABLE lobbying_filings ADD COLUMN government_entities_json VARCHAR')
    monkeypatch.setattr(mcp_server, '_get_connection', lambda: con)
    monkeypatch.setattr(mcp_server, '_connection_index', lambda cursor: index)
    monkeypatch.setattr(mcp_server, '_input_lineage', lambda *args: {})
    if local_native:
        monkeypatch.setattr(mcp_server, '_connection_local_selection',
                            lambda cursor: {'native': {'lobbying_filings': {}}})
    mcp_server._table_metadata.cache_clear()
    result = asyncio.run(mcp_server.build_server().call_tool('describe_table', {'table': 'lobbying_filings', 'detail': detail}))
    con.close()
    assert isinstance(result, CallToolResult)
    assert not result.is_error
    return result.structured_content


def test_native_lobbying_subject_does_not_reintroduce_historical_fields(monkeypatch):
    result = describe(monkeypatch, native=True)
    assert HISTORICAL.isdisjoint(c['column_name'] for c in result['columns'])
    entity_description = next(c['description'] for c in result['columns'] if c['column_name'] == 'government_entities')
    assert 'each source ID once' in entity_description and 'activity rows retain every occurrence' in entity_description
    assert result['schema_matches_declared'] is True
    assert result['metadata']['identity_columns'] == ['filing_uuid']
    assert 'legacy_column_descriptions' not in result['metadata']
    reply = json.dumps(result, separators=(',', ':'))
    assert 'legacy_column_descriptions' not in mcp_server._table_metadata()['lobbying_filings']
    # A compact reply, not the full detail: 14,094 characters with the 28 government-sources/2 columns (ss-hqoi,
    # 2026-10-30); 12,000 bounded the 13 columns of /1.
    assert len(reply) < 16000


@pytest.mark.parametrize('layout', ['published-native', 'local-native'])
def test_historical_meaning_is_withheld_when_selected_native_has_extra_column(monkeypatch, layout):
    result = describe(monkeypatch, native=layout == 'published-native', extra_legacy=layout == 'published-native',
                      local_native=layout == 'local-native')
    assert next(c for c in result['columns'] if c['column_name'] == 'government_entities_json')['description'] is None


def test_documented_filing_types_are_detail_only(monkeypatch):
    """The LDA filing-type labels reach describe_table with detail=true and stay out of the compact reply."""
    compact = describe(monkeypatch)
    assert 'documented_values' not in compact['metadata']
    assert 'metadata.documented_values' in compact['detail']['omitted']
    full = describe(monkeypatch, detail=True)
    filing_type = full['metadata']['documented_values']['filing_type']
    assert filing_type['checked'] is True
    assert {'value': 'RR', 'label': 'Registration'} in filing_type['values']
    assert len(filing_type['values']) == 50


def test_labels_only_lists_say_they_gate_nothing():
    """The generated dictionary marks FEC party and BILLSTATUS action codes as unchecked, with the guide's caveat."""
    mcp_server._table_metadata.cache_clear()
    metadata = mcp_server._table_metadata()
    assert metadata['fec_candidate_history']['documented_values']['party']['checked'] is False
    action_code = metadata['bill_actions']['documented_values']['action_code']
    assert action_code['checked'] is False
    assert 'does not exist' in action_code['caveat']


@pytest.mark.parametrize('native', [False, True])
def test_later_policy_column_inventory_is_detail_only(monkeypatch, native):
    """Compact replies keep actual schema and meanings; detail keeps the policy grouping."""
    compact = describe(monkeypatch, native=native)
    full = describe(monkeypatch, native=native, detail=True)
    assert 'later_policy_columns' not in compact['metadata']
    assert 'metadata.later_policy_columns' in compact['detail']['omitted']
    assert 'metadata.later_policy_columns' not in full['detail']['omitted']
    expected = mcp_server._table_metadata()['lobbying_filings']['later_policy_columns']
    assert full['metadata']['later_policy_columns'] == expected
    assert compact['columns'] == full['columns']
    assert compact['schema_differences'] == full['schema_differences']
    names = {column['column_name'] for column in compact['columns']}
    missing = compact['schema_differences']['missing_columns']
    assert set(expected) <= names | set(missing)
    assert compact['metadata']['data_quality'] == full['metadata']['data_quality']
