"""Actual legacy columns retain meanings without redeclaring native subject fields."""
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
HISTORICAL = {'url', 'lobbying_activities_json', 'government_entities_json'}


def describe(monkeypatch, *, native=False, extra_url=False, local_native=False, detail=False):
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
    if extra_url:
        con.execute('ALTER TABLE lobbying_filings ADD COLUMN url VARCHAR')
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


def test_pinned_legacy_lobbying_columns_keep_original_meanings(monkeypatch):
    result = describe(monkeypatch)
    columns = {c['column_name']: c for c in result['columns']}
    assert result['publication']['artifact_digest'] == LEGACY['families']['lobbying-filings']['artifactDigest']
    assert 'page on lda.gov' in columns['url']['description']
    assert 'JSON array of lobbying activities' in columns['lobbying_activities_json']['description']
    assert 'JSON array of the distinct government entities' in columns['government_entities_json']['description']
    assert 'not an organization identity' in columns['client_id']['description']
    assert 'drop registrations (`RR`, `RA`)' in columns['filing_type']['description']
    assert result['metadata']['identity_columns'] == ['filing_uuid']
    assert HISTORICAL <= set(result['schema_differences']['unexpected_columns'])
    assert 'legacy_column_descriptions' not in result['metadata']
    assert len(json.dumps(result, separators=(',', ':'))) < 12000


def test_native_lobbying_subject_does_not_reintroduce_historical_fields(monkeypatch):
    result = describe(monkeypatch, native=True)
    assert HISTORICAL.isdisjoint(c['column_name'] for c in result['columns'])
    assert result['schema_matches_declared'] is True
    assert result['metadata']['identity_columns'] == ['filing_uuid']
    assert 'legacy_column_descriptions' not in result['metadata']
    reply = json.dumps(result, separators=(',', ':'))
    legacy = json.loads((Path(mcp_server.__file__).parent / 'table_metadata.json').read_text())
    assert not any(text in reply for text in legacy['lobbying_filings']['legacy_column_descriptions'].values())
    # A compact reply, not the full detail: 14,094 characters with the 28 government-sources/2 columns (ss-hqoi,
    # 2026-10-30); 12,000 bounded the 13 columns of /1.
    assert len(reply) < 16000


@pytest.mark.parametrize('layout', ['published-native', 'local-native'])
def test_historical_meaning_is_withheld_when_selected_native_has_extra_column(monkeypatch, layout):
    result = describe(monkeypatch, native=layout == 'published-native', extra_url=layout == 'published-native',
                      local_native=layout == 'local-native')
    assert next(c for c in result['columns'] if c['column_name'] == 'url')['description'] is None


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
