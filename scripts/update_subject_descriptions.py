"""Reconcile subject prose with explicit native field policies, preserving source meanings."""
import json
from pathlib import Path
import re
from typing import Any
import yaml
import pyarrow as pa
from spicy_regs import data_dictionary as dd
from spicy_regs.subject_catalog import policies

ROOT = Path(__file__).resolve().parents[1]


def main(tables=None):
    original = yaml.safe_load(dd.DEFAULT_DESCRIPTIONS.read_text())['tables']
    for table, item in original.items():
        if item.get('columns_from') == 'spicy_docs':
            item['columns'] = dd.contract_column_prose(table)
    from spicy_regs.legislative_documents import field_registry
    field_prose = {(table, f['target']): f['meaning'] for table, spec in field_registry().items()
                   for f in spec['fields'] if f['target'] is not None}
    from spicy_regs.congress_subjects import INPUT_COLUMNS, target_name, FLATTEN
    for table, columns in INPUT_COLUMNS.items():
        for source in columns:
            prose = original.get(table, {}).get('columns', {}).get(source)
            if not prose:
                continue
            field_prose[table, target_name(table, source)] = prose
            for nested, (target, _) in FLATTEN.get((table, source), {}).items():
                field_prose[table, target] = f'{prose} This column retains the source property {nested}.'
    manual = {
        'evidence_occurrences': 'Every accepted source date window before membership lists reduce to sets. Includes source namespace, native document key, source row order, role, dates, and ordered RIN observations. A Register reference requires both document number and publication date; missing older observations remain null.',
        'parent_artifact_digest': 'Exact recorded court-opinions artifact used for this capture. A current opinion lookup is a separate check.',
        'parent_generation_id': 'Receipt generation of the recorded court-opinions parent, when declared. Null for older parents without native receipts.',
        'parent_member_sha256': 'SHA-256 of the exact recorded court-opinions member; never an identity inferred from a URL or native SHA-1 alone.',
        'parent_member_byte_size': 'Declared byte size of the recorded court-opinions member.',
        'observation_ordinal': 'Zero-based observation order within this dataset generation. Repeated observations remain separate; this is not a publisher record identifier.',
        'filing_association_status': 'The maintained association rule result. Resolved means a qualified filing key; unresolved reasons and not_evaluated never imply an edge or a current filing.',
        'filing_association_policy': 'Version of the maintained filing association rule used for this decision.',
        'filing_association_source_generation_pin': 'Exact selected source generation used for the filing association decision.',
        'filing_association_target_generation_pin': 'Exact separately selected filing-metadata generation for an external target. Null for a target in this same dataset generation (self); use the target scope to distinguish these cases.',
        'filing_association_target_generation_scope': 'Whether the filing target belongs to this same dataset generation (self) or a separately pinned generation (external). Self targets have no external generation pin; the scope alone does not establish a resolved match.',
        'filing_association_record_id': 'Identity of the admitted association witness; null when source context or a singular header witness is unavailable.',
        'filing_association_referenced_key': 'Qualified filing key stated by a supported source reference, before checking retained targets. Null when the reference cannot be qualified.',
        'filing_association_target_count': 'Number of observed filing-metadata witnesses for the qualified reference. Null means the target population is not established; no witness is selected as latest.',
        'filing_association_target_record_ids': 'Every matching filing observation identifier in the selected target population, preserving repeated witnesses. Null means the population is not established.',
        'source_ordinal': 'Source-record ordinal explicitly retained in the source locator; null when absent or invalid.',
        'subrecord_ordinal': 'Array-element ordinal explicitly retained in the source locator, preserving repeated references.',
        'subrecord_pointer': 'Literal source JSON pointer plus the explicitly recorded array field and element ordinal; null when unavailable.',
        'companion_status': 'Whether the retained locator supplies collection and source-record coordinates. It does not establish a found or unique companion.',
        'subject_endpoint_status': 'Subject lookup eligibility from its retained entity type, native identifier status and stated cycle. Unsupported or missing context produces no inferred edge.',
        'object_endpoint_status': 'Object lookup eligibility from reported value status, retained entity type, native identifier status and stated cycle. Names and reported-none values never create identifier edges.',
        'regulations_dot_gov_agency_id': 'Agency identifier stated by the linked Regulations.gov document metadata.',
        'regulations_dot_gov_title': 'Title stated by the linked Regulations.gov document metadata.',
        'regulations_dot_gov_regulation_id_number': 'Regulation identifier number stated by the linked Regulations.gov metadata; no inferred docket or rule match.',
        'regulations_dot_gov_supporting_documents_count': 'Supporting-document count stated by Regulations.gov for the linked document.',
        'regulations_dot_gov_supporting_documents': 'Supporting document identifiers and titles listed by Regulations.gov, preserving source order and repetition.',
        'regulations_dot_gov_regulatory_plan_title': 'Regulatory-plan title stated in the linked Regulations.gov metadata.',
        'body_version_id': 'Stable identity of the substantive captured document body, independent of extraction processing.',
        'from_body_version_id': 'Stable identity of the earlier document body in this comparison.',
        'to_body_version_id': 'Stable identity of the later document body in this comparison.',
        'proceedings': 'Source-listed FCC proceedings with their names and identifiers; repeated occurrences remain distinct.',
        'size_of_contribution': 'Publisher contribution-size category for this aggregate.',
        'contributor_state': 'Source contributor state qualifying this aggregate.',
        'state_name': 'Publisher state name qualifying this aggregate.',
        'zip_3': 'Three-digit postal prefix qualifying this aggregate.',
        'report_number': 'Source-stated report number, not an independently qualified report match.',
        'citation_product_id': 'Uppercase report number, falling back to the product-page ID only when the number is NULL. Blank numbers stay blank. This is a citation lookup key; report_id remains the record identity.',
        'native_element_id': 'Publisher element identifier attached to this legal reference.',
        'native_idref': 'Publisher identifier reference attached to this legal reference.',
        'native_class': 'Publisher classification attached to this legal reference.',
        'majority_requirement': 'Source-stated majority requirement for the vote.',
        'modify_date': 'Source-stated modification date for the vote record.',
        'tie_breaker_by_whom': 'Person or office identified by the source as casting the tie-breaking vote.',
        'tie_breaker_vote': 'Source-stated position of the tie-breaking vote.',
        'party_totals': 'Source-stated vote totals by party and position, preserving each listed group.',
        'fr_document_ids': 'Dated Federal Register document identities associated with this subject; no applicability inference is implied.',
        'rule_target_id': 'Stable identity of this rule-target assertion, preserving repeated observations independently.',
        'lifecycle_event_id': 'Stable identity of this lifecycle event within its lifecycle.',
        'opinion_body_id': 'Stable identity of this opinion and captured body version; independent of extraction processing.',
        'text_ordinal': 'Original order of this text fragment within its report, preserving gaps and repetition.',
        'date_kind': 'Whether the meeting dates represent one date, separately listed dates, or the two endpoints of a range.',
        'association_kind': 'Meaning of the stated organization-to-committee association; no shared legal identity is implied.',
        'district_number': 'District number stated for the candidate, as an integer; null when absent or unsupported.',
        'parts_count': 'Number of treaty parts stated by the source; zero and missing remain distinct.',
        'is_military': 'Whether the source identifies the nomination as military.',
        'definition_namespace': 'Namespace of the source definition identifying this reported metric.',
        'definition_path': 'Path of the source definition identifying this reported metric.',
        'definition_schema_version': 'Version of the source metric-definition schema, part of the metric meaning.',
        'dimension_context': 'Source-reported dimensions that qualify this metric value.',
        'released_date': 'Date the publisher released the decision.',
        'decided_date': 'Date the decision was made, distinct from its release date.',
        'decision_status': 'Publisher-stated decision status, retained as a business fact.',
        'outcome': 'Publisher-stated outcome of the decision; does not imply processing success.',
        'document_length': 'Document length stated by the source.',
        'significant': 'Whether the source marks this recommendation significant.',
        'status_date': 'Date associated with the source-stated recommendation status.',
        'additional_details': 'Additional substantive details supplied by the publisher.',
        'ao_no': 'Publisher advisory-opinion number.', 'case_serial': 'Publisher legal matter serial number.',
        'election_cycles': 'Election cycles stated for the legal matter; original order and repetitions are preserved.',
        'ao_citations': 'Advisory opinions cited by this legal matter, retaining source citation attributes.',
        'aos_cited_by': 'Advisory opinions that cite this matter, retaining the source direction.',
        'regulatory_citations': 'Source-listed regulatory citations; no applicability determination is implied.',
        'statutory_citations': 'Source-listed statutory citations; no applicability determination is implied.',
        'citations': 'Source-listed legal citations, grouped by citation kind with literal text and URLs.',
        'subject': 'Publisher subject hierarchy with source identifiers and ordered child links.',
        'subjects': 'Publisher subject identifiers in source order, preserving repeated entries.',
        'non_monetary_terms': 'Nonmonetary terms stated in the legal matter.',
        'non_monetary_terms_respondents': 'Respondents associated with the source-stated nonmonetary terms.',
        'record_id': 'Stable identity of this source-derived row.',
        'blocked': 'Whether the source marks the court docket blocked.',
        'date_blocked': 'Source-stated date when the docket was blocked.',
    }
    maps = json.loads((ROOT/'src/spicy_regs/fec_native_field_maps.json').read_text())
    for source,target in maps['summary_attributes'].items():
        field_prose['fec_reported_financial_summaries',target] = f'Publisher financial-summary field {source}; its literal value qualifies the reported summary.'
    for source,target in maps['agency_report_fields'].items():
        field_prose['fec_agency_reports',target] = f'Publisher report field {source}, retained as substantive report data.'
    field_prose['fec_relationships', 'collection_id'] = 'Collection identifier explicitly retained in this relationship\'s source locator; null when the coordinate is unavailable.'
    field_prose['fec_relationships', 'source_record_id'] = 'Individual source-record identifier explicitly retained in this relationship\'s locator. Together with collection_id and source_sha256 it identifies a source occurrence; no coordinate is invented.'
    field_prose['federal_register', 'source_sha256'] = (
        'SHA-256 of the retained Federal Register API page this row was last read from, as the run\'s source-evidence '
        'journal spells it (`sha256:<hex>`); the page is `source-evidence/blobs/sha256/<hex>`. NULL on a row no run '
        'has read since the column was added (2026-11). It is a pointer, not part of the record: the scheduled run '
        're-reads the last 120 days, so it changes every night on every row in that window (about 8,700) whether '
        'or not the record changed, and a comparison of two generations must leave it out. Where one run reads a '
        'document on two pages it names the first.')
    extras = {
        'attachments':'attachments_json', 'attachment_records':'attachments_json', 'fr_document_ids':'fr_document_ids_json',
        'is_original':'is_original_raw','agency_codes':'agency_codes_json','links':'links_json','dates':'dates_json',
        'printing_id':'source','equivalent_printing_id':'equivalent_xml_source','from_printing_id':'from_source','to_printing_id':'to_source',
        'vote_question_text':'question','vote_title':'vote_desc',
    }
    target = ROOT/'data_dictionary/subject_descriptions.json'
    overrides: dict[str, dict[str, Any]] = json.loads(target.read_text()) if tables is not None else {}
    if tables is not None and set(tables) - policies().keys():
        raise ValueError('Unknown subject-prose dataset selection')
    unresolved=[]
    for table, policy in policies().items():
        if tables is not None and table not in tables:
            continue
        if policy.receipt_only:
            continue
        old = original.get(table, {}).get('columns', {})
        columns={}
        for field in policy.subject_schema:
            name=field.name
            prose=old.get(name) or field_prose.get((table,name))
            if not prose and name in extras:
                prose=old.get(extras[name])
            if not prose and (pa.types.is_list(field.type) or pa.types.is_struct(field.type)):
                # The explicit policy already establishes native type and target;
                # this only transfers the prose for its former serialized spelling.
                prose=old.get(name+'_json')
            prose=prose or manual.get(name)
            if not prose and name.startswith('designated_agent_'):
                prose='Committee-designated agent '+name.removeprefix('designated_agent_').replace('_',' ')+', as reported by the source.'
            if not prose and name=='sponsor_candidate_list':
                prose='Source-listed sponsor candidate identifiers and names, preserving list order and repetitions.'
            if not prose:
                unresolved.append((table,name))
                continue
            if (table, name) == ('lobbying_filings', 'government_entities'):
                prose = (
                    "Government entities named on the filing, in order of first appearance. A rebuilt filing lists each "
                    "source ID once and uses the source name when the ID is absent. The activity rows retain every "
                    "occurrence. Earlier native generations may repeat entities at filing level. A missing activity "
                    "list yields null and an empty activity list yields an empty result."
                )
            elif pa.types.is_list(field.type) or pa.types.is_struct(field.type):
                prose=re.sub(r'JSON[- ](?:encoded|serialized|serialised)\s*', '', prose, flags=re.I)
                prose=prose.replace('JSON array','Native list').replace('JSON object','Native structure').replace('JSON list','Native list')
                prose+=' Stored as native nested values; list order, repeated values, null and empty collections remain distinct.'
            columns[name]=prose
        overrides[table] = {'columns': columns}
    # Carried prose may still name a list by its former serialized spelling, in its own table or another.
    for table, item in overrides.items():
        if tables is not None and table not in tables:
            continue
        item['columns'] = {name: dd.native_spelling(prose, table) for name, prose in item['columns'].items()}
    if unresolved:
        print('Unresolved field prose:',unresolved)
        raise SystemExit(1)
    target.write_text(json.dumps(overrides,indent=2,ensure_ascii=False)+'\n')
    print('Updated subject field prose',len(overrides))

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tables', nargs='+', help='Regenerate only these explicitly selected dataset descriptions.')
    main(parser.parse_args().tables)
