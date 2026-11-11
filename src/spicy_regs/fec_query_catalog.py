"""Explicit FEC dataset roles and meanings shared by discovery and documentation."""

FEC_SOURCE_CATEGORIES = {
    "fec_collections": "processing_evidence",
    "fec_source_catalog": "reference_data",
    "fec_collection_selection": "processing_evidence",
    "fec_filing_definition_evidence": "source_evidence",
    "fec_filing_definitions": "reference_data",
    "fec_filing_header_associations": "source_evidence",
    "fec_source_records": "source_data",
    "fec_agency_mapping_dispositions": "diagnostics",
    "fec_api_response_controls": "diagnostics",
    # The reference namespace (ss-j4vz Q1): rebuilt from whole originals, not query data.
    "fec_reference_filing_feed_items": "reference_data",
    "fec_reference_source_pages": "reference_data",
}


# All other typed FEC subject tables are query data; original evidence remains
# in fec_source_records and fec_collections. These roles do not certify coverage.
def table_category(name: str) -> str | None:
    if name in FEC_SOURCE_CATEGORIES:
        return FEC_SOURCE_CATEGORIES[name]
    if name.startswith("fec_"):
        return (
            "source_evidence"
            if name == "fec_record_evidence"
            else "query_data"
        )
    return None


CHILD_COLUMN_DESCRIPTIONS = {
    "native_label": "Literal source definition label for the reported measure.",
    "part": "Regulatory part identifier stated by the source citation.",
    "ordinal_path": "Ordered source-list positions identifying this subject in the parent hierarchy.",
    "child_ordinals": "Ordered child positions declared by this subject node in the parent hierarchy.",
    "parent_record_id": "record_id of the containing typed observation in the named source table and selected publication.",
    "matter_record_id": "record_id of the containing fec_legal_matters observation; distinct from matter_id.",
    "source_ordinal": "Zero-based position within the named parent array. Repeated source elements remain separate rows.",
    "source_field": "Parent column containing the array from which this row was expanded.",
    "source_pointer": "Path to this element in the named parent field; combine with parent identity and source_field.",
    "source_publication_json": "Publication metadata selected for the parent table; JSON null means no pin was supplied.",
    "rule_version": "Version of this child expansion rule; it does not qualify financial totals or current records.",
    "collection_status": "Parent collection state: source_null, json_null, empty, reported or unsupported_shape.",
    "parsing_status": "Element interpretation status. Unsupported or unavailable elements remain distinguishable from reported values; see the view-specific meanings.",
    "unsupported_value_json": "Literal unsupported element for diagnosis; the complete original remains at the parent evidence reference.",
    "raw_value_json": "Complete literal source array element, including nulls and unsupported values.",
    "subject_attributes_json": "Literal subject object excluding children; descendants are separate rows and source_pointer locates the original.",
    "citation_kind": "Native citation list or group name; preserves direction such as citations versus cited-by.",
    "citation_text": "Literal citation string or source text attribute; no legal applicability or resolved target is implied.",
    "advisory_opinion_number": "Literal advisory-opinion number in the source citation; not a resolved matter identity.",
    "subject_pointer": "Path identifying this subject node in the parent subject hierarchy.",
    "parent_subject_pointer": "Path of the containing subject node; null for a root subject.",
    "depth": "Zero-based depth in the source subject tree; zero identifies a root node.",
    "output_table": "Table name reported in the parent mapping outcome, not independently verified publication.",
    "output_count": "Nonnegative exact number of emitted rows reported for output_table; invalid values are null.",
    "date_role": "Meaning of the ordered date: range_start, range_end, single_date, listed_date or unspecified source role.",
    "meeting_date": "Exact four-digit source calendar date. A date in a range is an endpoint, not an expanded day.",
    "date_raw": "Literal parent date element before interpretation.",
    "cycle": "Literal source cycle value from this array; independent of other array positions.",
    "election_year": "Source election-year array element; it is not necessarily the capture cycle.",
    "election_district": "Literal election-district array element; positions are not paired with election years.",
    "candidate_id": "Literal source candidate reference; identifier spelling alone does not prove an existing candidate.",
    "source_fact_index": "Index of the source event containing this link in the retained capture context.",
    "url": "Source-reported link destination; no successful retrieval or verified target identity is implied.",
    "href": "Literal source link attribute before resolution against the captured page URL.",
    "title": "Literal title supplied for this source element.",
    "name": "Literal name supplied for this source element.",
    "section": "Literal section identifier supplied by the source citation; no independent resolution is implied.",
    "text": "Literal text of the selected source fragment or element, preserving the parent source ordering.",
}

CHILD_COLUMN_DESCRIPTIONS.update(
    {
        "control_row_count": "Number of source control observations grouped into this captured response; not a result-record count.",
        "repeated_field_rows": "Control rows beyond the first per response field; conflicting repeats prevent selecting an arbitrary value.",
        "query_completeness_status": "Whether source completeness declarations agree (source_declaration) or conflict (conflicting_declarations).",
        "completeness_status": "Capture-only limitation; this grouping never asserts complete source population coverage.",
        "body_status": "Source statement about the linked body, including deferred_pdf or linked_body_not_qualified; a URL does not prove acquisition.",
        "native_position": "Zero-based field position in the retained filing layout, distinct from this measure array ordinal.",
        "label": "Literal field label from the pinned filing definition; preserve source meaning rather than infer a financial metric.",
        "definition_cell": "Cell address in the retained definition workbook that supplies this field label.",
        "raw_value": "Literal original filing value before decimal interpretation.",
        "exact_value": "Exact decimal value rendered as a string, without rounding; NULL when interpretation was unsupported.",
        "value_status": "Original producer value interpretation, including exact, missing, empty or refusal states; a NULL amount is not zero.",
        "quantity_kind": "Source-specific quantity kind assigned by the reviewed filing mapper; an amount label alone does not authorize summation.",
        "measure_role": "Source-defined measure role, preserving totals and subtotals as distinct reported values.",
        "period_basis": "Period basis retained from the native report column; not automatically harmonized across forms or editions.",
        "field_position": "Zero-based position of this narrative field in the containing source record.",
        "fragment_source_column": "Source column containing this fragment at its retained evidence endpoint.",
        "fragment_source_pointer": "Exact fragment path in the source column, separate from the position in the typed text_fragments array.",
        "subject_kind": "Native subject hierarchy family, preserving the source grouping.",
        "parent_pointer": "Path of the containing subject node; NULL identifies a root node.",
        "subject": "Literal subject label supplied by the source; not a cross-source taxonomy mapping.",
        "primary_subject_id": "Source primary-subject identifier, preserved within its native hierarchy.",
        "secondary_subject_id": "Source secondary-subject identifier, preserved within its native hierarchy.",
        "text_status": "Source narrative interpretation status, including reported and unsupported fragment states; no successful extraction is inferred from row presence.",
    }
)


def child_column_descriptions(spec, described, descriptions):
    """Require explicit meanings, inheriting unchanged fields from their parent dictionary."""
    inherited = {}
    for table in spec.required:
        for name, meaning in descriptions.get(table, {}).get("columns", {}).items():
            inherited.setdefault(name, meaning)
    status = (
        "reported for a supported element shape; json_null for a literal null; unsupported_shape for "
        "another element shape; collection_unavailable when the parent collection is null or unsupported. "
        "Empty arrays emit no child rows."
    )
    overrides = {"parsing_status": status}
    if spec.name in {"fec_filing_report_measures", "fec_filing_text"}:
        overrides["source_pointer"] = (
            "Path in the parent row to this array element, including the source_field "
            "column name. For an unavailable collection it points to the collection itself."
        )
    declared = {**inherited, **CHILD_COLUMN_DESCRIPTIONS, **overrides, **spec.column_descriptions}
    missing = [row[0] for row in described if row[0] not in declared]
    if missing:
        raise ValueError(f"{spec.name} needs column meanings: {missing}")
    return {row[0]: declared[row[0]] for row in described}
