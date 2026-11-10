"""Explicit court subject shapes and lossless conversion of retained processing rows."""
from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from typing import Any

import pyarrow as pa


def opinion_body_id(opinion_id: str, source_sha256: str) -> str:
    """A stable business key for one opinion/body, independent of its extraction."""
    if not isinstance(opinion_id, str) or not re.fullmatch(r'[1-9][0-9]*', opinion_id):
        raise ValueError('Expected positive opinion identity')
    if not isinstance(source_sha256, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', source_sha256):
        raise ValueError('Expected complete captured body SHA-256')
    value = json.dumps([opinion_id, source_sha256], separators=(',', ':')).encode()
    return 'court-opinion-body:' + hashlib.sha256(value).hexdigest()


def _names(value: Any, *, prose: bool = False) -> list[str | None] | None:
    if value is None:
        return None
    if prose and isinstance(value, str):
        return [value]
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list) or any(v is not None and not isinstance(v, str) for v in value):
        raise ValueError('Expected a list of names, preserving null and empty elements')
    return value


#: What :func:`normalize_court_row` raises for a row it refuses (any other exception is a fault, not a refusal).
REFUSED = (ValueError, TypeError, KeyError, pa.ArrowException)


def normalize_court_row(dataset: str, row: Mapping[str, Any]) -> dict:
    """Convert only declared domain fields; retain every conversion input in receipt fields.

    The shared receipt writer records exceptions as refused attempts. This mapper
    never makes malformed lists empty or malformed numbers null.
    """
    schema = SUBJECT_SCHEMAS[dataset]
    allowed = set(schema.names) | set(RECEIPT_FIELDS[dataset]) | set(LEGACY_COLUMNS[dataset])
    if unknown := set(row) - allowed:
        raise ValueError(f'{dataset}: unclassified fields {sorted(unknown)}')
    result = {name: row[name] for name in RECEIPT_FIELDS[dataset] if name in row}
    originals = dict(row.get('conversion_inputs') or {})
    for field in schema:
        name = field.name
        value = row.get(name)
        if dataset == 'court_dockets' and name in ('parties', 'attorneys', 'firms'):
            legacy = name + '_json'
            if legacy in row and name in row:
                raise ValueError(f'Both legacy and native {name} supplied')
            value = _names(row[legacy] if legacy in row else value)
        elif dataset == 'court_opinion_clusters' and name == 'attorneys':
            originals.setdefault(name, value)
            value = _names(value, prose=True)
        elif dataset == 'court_opinion_pdf_extractions' and name == 'opinion_body_id':
            derived = opinion_body_id(row['opinion_id'], row['source_sha256']) if 'source_sha256' in row else value
            if value is not None and derived != value:
                raise ValueError('Body business identity differs from captured body')
            value = derived
        elif value is not None and pa.types.is_boolean(field.type):
            originals.setdefault(name, value)
            if type(value) is not bool:
                if not isinstance(value, str) or value.lower() not in ('t', 'f', 'true', 'false'):
                    raise ValueError(f'{name}: unsupported boolean {value!r}')
                value = value.lower() in ('t', 'true')
        elif value is not None and pa.types.is_integer(field.type):
            originals.setdefault(name, value)
            if type(value) is not int:
                if not isinstance(value, str) or not re.fullmatch(r'-?[0-9]+', value):
                    raise ValueError(f'{name}: unsupported integer {value!r}')
                value = int(value)
            if not -(2**63) <= value < 2**63:
                raise ValueError(f'{name}: integer outside BIGINT')
        elif value is not None and pa.types.is_floating(field.type):
            originals.setdefault(name, value)
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise ValueError(f'{name}: unsupported numeric value')
            value = float(value)
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f'{name}: publisher score outside [0,1]')
        result[name] = value
    if dataset == 'court_opinion_pdf_extractions':
        parent = json.loads(row.get('parent_opinion_publication_json') or '{}')
        if not isinstance(parent, dict):
            raise ValueError('Recorded parent publication must be an object')
        tables, receipts = parent.get('tables', {}), parent.get('etlReceipts', {})
        if not isinstance(tables, dict) or not isinstance(receipts, dict):
            raise ValueError('Recorded parent table and receipt declarations must be objects')
        member = tables.get('court_opinions.parquet', {})
        if not isinstance(member, dict):
            raise ValueError('Recorded parent opinion member must be an object')
        recorded = dict(parent_artifact_digest=parent.get('artifactDigest'),
                        parent_generation_id=receipts.get('generationId'),
                        parent_member_sha256=member.get('sha256'), parent_member_byte_size=member.get('byteSize'))
        for name, value in recorded.items():
            if row.get(name) is not None and result[name] != value:
                raise ValueError(f'{name}: differs from the recorded selected parent')
        result.update(recorded)
    for name in IDENTITIES[dataset]:
        if not isinstance(result[name], str) or not result[name]:
            raise ValueError(f'{dataset}: missing stable identity {name}')
    result['conversion_inputs'] = originals
    return result

# Source strings remain strings unless this table explicitly declares a native value.
SUBJECT_SCHEMAS = {
    # The four bulk tables publish no receipts (ss-vse3): the publisher's record fields their receipts held are
    # columns, appended after the contract's, with the export's edition and the SHA-256 of the export file each row
    # was read from (the pointer to the retained original).
    'court_citation_map': pa.schema([
        ('citing_opinion_id', pa.string()),
        ('cited_opinion_id', pa.string()),
        ('depth', pa.int64()),
        ('dump_date', pa.string()),
        ('source_sha256', pa.string()),
    ]),
    'court_citations': pa.schema([
        ('citation_id', pa.string()),
        ('cluster_id', pa.string()),
        ('volume', pa.string()),
        ('reporter', pa.string()),
        ('page', pa.string()),
        ('citation_type', pa.string()),
        ('date_created', pa.string()),
        ('date_modified', pa.string()),
        ('dump_date', pa.string()),
        ('source_sha256', pa.string()),
    ]),
    'court_docket_groups': pa.schema([
        ('cl_docket_id', pa.string()),
        ('parent_cl_docket_id', pa.string()),
        ('confidence_tier', pa.string()),
        ('group_size', pa.int64()),
    ]),
    'court_dockets': pa.schema([
        ('cl_docket_id', pa.string()),
        ('case_name', pa.string()),
        ('case_name_full', pa.string()),
        ('court_id', pa.string()),
        ('court', pa.string()),
        ('court_citation_string', pa.string()),
        ('docket_number', pa.string()),
        ('date_filed', pa.string()),
        ('date_terminated', pa.string()),
        ('date_argued', pa.string()),
        ('nature_of_suit', pa.string()),
        ('cause', pa.string()),
        ('jurisdiction_type', pa.string()),
        ('jury_demand', pa.string()),
        ('assigned_to', pa.string()),
        ('referred_to', pa.string()),
        ('parties', pa.list_(pa.string())),
        ('attorneys', pa.list_(pa.string())),
        ('firms', pa.list_(pa.string())),
        ('pacer_case_id', pa.string()),
        ('case_type', pa.string()),
        ('blocked', pa.bool_()),
        ('date_blocked', pa.string()),
    ]),
    'court_opinion_clusters': pa.schema([
        ('cluster_id', pa.string()),
        ('cl_docket_id', pa.string()),
        ('court_id', pa.string()),
        ('court_jurisdiction', pa.string()),
        ('court_is_federal', pa.bool_()),
        ('case_name', pa.string()),
        ('case_name_short', pa.string()),
        ('case_name_full', pa.string()),
        ('date_filed', pa.string()),
        ('date_filed_is_approximate', pa.bool_()),
        ('judges', pa.string()),
        ('nature_of_suit', pa.string()),
        ('precedential_status', pa.string()),
        ('citation_count', pa.int64()),
        ('scdb_id', pa.string()),
        ('scdb_decision_direction', pa.string()),
        ('scdb_votes_majority', pa.int64()),
        ('scdb_votes_minority', pa.int64()),
        ('procedural_history', pa.string()),
        ('attorneys', pa.list_(pa.string())),
        ('posture', pa.string()),
        ('syllabus', pa.string()),
        ('headnotes', pa.string()),
        ('summary', pa.string()),
        ('disposition', pa.string()),
        ('history', pa.string()),
        ('other_dates', pa.string()),
        ('cross_reference', pa.string()),
        ('correction', pa.string()),
        ('arguments', pa.string()),
        ('headmatter', pa.string()),
        ('blocked', pa.bool_()),
        ('date_blocked', pa.string()),
    ]),
    'court_opinion_pdf_extractions': pa.schema([
        ('opinion_body_id', pa.string()),
        ('opinion_id', pa.string()),
        ('cluster_id', pa.string()),
        ('text_content', pa.string()),
        ('source_url', pa.string()),
        ('resolved_url', pa.string()),
        ('source_sha256', pa.string()),
        ('native_sha1', pa.string()),
        ('actual_sha1', pa.string()),
        ('pdf_extraction_results_json', pa.string()),
        ('observed_at', pa.string()),
        ('extractor', pa.string()),
        ('extractor_version', pa.string()),
        ('parent_opinion_publication_json', pa.string()),
        ('sha1_matches', pa.bool_()),
        ('parent_artifact_digest', pa.string()),
        ('parent_generation_id', pa.string()),
        ('parent_member_sha256', pa.string()),
        ('parent_member_byte_size', pa.int64()),
    ]),
    'court_opinions': pa.schema([
        ('opinion_id', pa.string()),
        ('cluster_id', pa.string()),
        ('opinion_type', pa.string()),
        ('author_id', pa.string()),
        ('author_str', pa.string()),
        ('per_curiam', pa.bool_()),
        ('joined_by_str', pa.string()),
        ('page_count', pa.int64()),
        ('sha1', pa.string()),
        ('download_url', pa.string()),
        ('local_path', pa.string()),
        ('extracted_by_ocr', pa.bool_()),
        ('date_created', pa.string()),
        ('date_modified', pa.string()),
        ('dump_date', pa.string()),
        ('source_sha256', pa.string()),
    ]),
    # One opinion's body as Markdown (CommonMark + GFM tables + footnotes; citations as ``[text](opinion:ID)``),
    # converted by spicy_docs.sources.courtlistener.opinion_markdown from the first filled rendering; NULL when the
    # export holds none (text_source 'none'). opinion_block (opinion_id // 150000, decimal text) names the member.
    'court_opinion_texts': pa.schema([
        ('opinion_id', pa.string()),
        ('cluster_id', pa.string()),
        ('opinion_block', pa.string()),
        ('date_modified', pa.string()),
        ('text_source', pa.string()),
        ('markdown', pa.string()),
        ('converter_version', pa.string()),
    ]),
    'court_parentheticals': pa.schema([
        ('parenthetical_id', pa.string()),
        ('described_opinion_id', pa.string()),
        ('describing_opinion_id', pa.string()),
        ('text', pa.string()),
        ('score', pa.float64()),
        ('group_id', pa.string()),
        ('dump_date', pa.string()),
        ('source_sha256', pa.string()),
    ]),
}

#: Each table's policy version, named by the receipts and generations written under it. The four bulk tables moved
#: when their record fields became columns (ss-vse3); the others are unchanged.
POLICY_VERSIONS = {
    'court_citation_map': 'courts/2',
    'court_citations': 'courts/2',
    'court_docket_groups': 'courts/1',
    'court_dockets': 'courts/1',
    'court_opinion_clusters': 'courts/1',
    'court_opinion_pdf_extractions': 'courts/2',
    'court_opinions': 'courts/3',
    'court_opinion_texts': 'courts/1',
    'court_parentheticals': 'courts/2',
}

IDENTITIES = {
    'court_citation_map': ('citing_opinion_id', 'cited_opinion_id'),
    'court_citations': ('citation_id',),
    'court_docket_groups': ('cl_docket_id',),
    'court_dockets': ('cl_docket_id',),
    'court_opinion_clusters': ('cluster_id',),
    'court_opinion_pdf_extractions': ('opinion_body_id',),
    'court_opinions': ('opinion_id',),
    'court_opinion_texts': ('opinion_id',),
    'court_parentheticals': ('parenthetical_id',),
}

RECEIPT_FIELDS = {
    'court_citation_map': ('conversion_inputs', 'raw_source_record', 'source_observation'),
    'court_citations': ('conversion_inputs', 'raw_source_record', 'source_observation'),
    'court_docket_groups': ('edition', 'rule_version', 'match_basis', 'conversion_inputs', 'raw_source_record', 'source_observation'),
    'court_dockets': ('date_created', 'absolute_url', 'parties_json', 'attorneys_json', 'firms_json', 'conversion_inputs', 'raw_source_record', 'source_observation'),
    'court_opinion_clusters': ('source', 'slug', 'absolute_url', 'date_created', 'date_modified', 'ingest_source', 'conversion_inputs', 'raw_source_record', 'source_observation'),
    'court_opinion_pdf_extractions': ('conversion_inputs', 'raw_source_record', 'source_observation'),
    'court_opinions': ('conversion_inputs', 'raw_source_record', 'source_observation'),
    # A pointer to the converted value, never a copy of it: the export is the receipt's witness, the identity the
    # opinion id, source_column the rendering converted and source_sha256 that rendering's digest (owner, 2026-10-31).
    'court_opinion_texts': ('source_column', 'source_sha256', 'dump_date', 'conversion_inputs', 'raw_source_record', 'source_observation'),
    'court_parentheticals': ('conversion_inputs', 'raw_source_record', 'source_observation'),
}

LEGACY_COLUMNS = {
    'court_citation_map': ('citing_opinion_id', 'cited_opinion_id', 'depth', 'dump_date'),
    'court_citations': ('citation_id', 'cluster_id', 'volume', 'reporter', 'page', 'citation_type', 'date_created', 'date_modified', 'dump_date'),
    'court_docket_groups': ('cl_docket_id', 'parent_cl_docket_id', 'confidence_tier', 'group_size', 'edition', 'rule_version', 'match_basis'),
    'court_dockets': ('cl_docket_id', 'case_name', 'case_name_full', 'court_id', 'court', 'court_citation_string', 'docket_number', 'date_filed', 'date_terminated', 'date_argued', 'nature_of_suit', 'cause', 'jurisdiction_type', 'jury_demand', 'assigned_to', 'referred_to', 'parties_json', 'attorneys_json', 'firms_json', 'pacer_case_id', 'date_created', 'absolute_url', 'case_type'),
    'court_opinion_clusters': ('cluster_id', 'cl_docket_id', 'court_id', 'court_jurisdiction', 'court_is_federal', 'case_name', 'case_name_short', 'case_name_full', 'date_filed', 'date_filed_is_approximate', 'judges', 'nature_of_suit', 'precedential_status', 'citation_count', 'scdb_id', 'scdb_decision_direction', 'scdb_votes_majority', 'scdb_votes_minority', 'source', 'procedural_history', 'attorneys', 'posture', 'syllabus', 'headnotes', 'summary', 'disposition', 'history', 'other_dates', 'cross_reference', 'correction', 'arguments', 'headmatter', 'blocked', 'date_blocked', 'slug', 'absolute_url', 'date_created', 'date_modified', 'ingest_source'),
    'court_opinion_pdf_extractions': ('opinion_id', 'cluster_id', 'source_url', 'resolved_url', 'source_sha256', 'native_sha1', 'actual_sha1', 'sha1_matches', 'text_content', 'pdf_extraction_results_json', 'observed_at', 'extractor', 'extractor_version', 'parent_opinion_publication_json'),
    'court_opinions': ('opinion_id', 'cluster_id', 'opinion_type', 'author_id', 'author_str', 'per_curiam', 'joined_by_str', 'page_count', 'sha1', 'download_url', 'local_path', 'extracted_by_ocr', 'date_created', 'date_modified', 'dump_date'),
    'court_opinion_texts': ('opinion_id', 'cluster_id', 'opinion_block', 'date_modified', 'text_source', 'markdown', 'converter_version', 'source_column', 'source_sha256', 'dump_date'),
    'court_parentheticals': ('parenthetical_id', 'described_opinion_id', 'describing_opinion_id', 'text', 'score', 'group_id', 'dump_date'),
}
