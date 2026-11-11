import json
from datetime import date

import pytest

from spicy_regs.transforms.regulations_shape import (
    IDENTITIES,
    SOURCE_COLUMNS,
    RECEIPT_COLUMNS,
    RegulationsShapeError,
    shape_record,
    subject_schema,
)


def test_null_empty_duplicates_and_raw_conversion_are_distinct():
    for value, native in [(None, None), ("null", None), ("[]", []), ('["A",null,"A"]', ["A", None, "A"])]:
        row = {"document_number": "2026-1", "publication_date": "2026-01-01", "docket_ids_json": value}
        shaped = shape_record("federal_register", row)
        assert shaped["docket_ids"] == native
        assert shaped["raw_conversion_inputs"]["docket_ids_json"] == value
        assert shaped["raw_conversion_inputs"]["docket_ids_json"] == value


def test_missing_conversion_is_not_invented_as_explicit_null():
    shaped = shape_record("documents", {"document_id": "D"})
    assert "attachments_json" not in shaped["raw_conversion_inputs"]
    assert "attachments_json" not in shaped["raw_conversion_inputs"]


@pytest.mark.parametrize("raw", ["", "{", "{}", '[{"part":1,"part":2}]', "NaN"])
def test_malformed_conversion_refuses_without_a_false_empty_list(raw):
    with pytest.raises(RegulationsShapeError):
        shape_record("federal_register", {"cfr_references_json": raw})


def test_unknown_nested_fields_refuse_instead_of_arrow_dropping_them():
    with pytest.raises(RegulationsShapeError):
        shape_record("documents", {"attachments_json": '[{"url":"u","unexpected":"source value"}]'})
    with pytest.raises(RegulationsShapeError):
        shape_record("dockets", {"unexpected": "source value"})
    agenda = {"rin": "1000-AA00", "agenda_edition": "202604"}
    related = [{"rin": "1000-AA01", "rin_relation": "Related to"}]
    assert shape_record("unified_agenda", agenda | {"related_rins_json": json.dumps(related)})["related_rins"] == related
    with pytest.raises(RegulationsShapeError):
        shape_record("unified_agenda", agenda | {"related_rins_json": json.dumps([related[0] | {"note": "x"}])})


def test_attachment_order_repeated_url_sizes_and_restrictions_survive():
    formats = [{"url": "u", "format": "pdf", "size": 5}, None, {"url": "u", "format": "pdf", "size": 8}]
    row = {
        "document_id": "D",
        "attachments_json": json.dumps(formats),
        "attachment_records_json": json.dumps(
            [
                {
                    "id": "a",
                    "type": "attachments",
                    "attributes": {"fileFormats": None, "restrictReasonType": "Restricted"},
                },
                {"id": "a", "type": "attachments", "attributes": {"fileFormats": []}},
            ]
        ),
    }
    shaped = shape_record("documents", row)
    assert shaped["attachments"] == formats
    assert [x["attachment_id"] for x in shaped["attachment_records"]] == ["a", "a"]
    assert shaped["attachment_records"][0]["file_formats"] is None
    assert shaped["attachment_records"][1]["file_formats"] == []
    assert shaped["attachment_records"][0]["restrict_reason_type"] == "Restricted"


def test_timetable_keeps_literal_month_precision_and_non_dates():
    values = [
        {"action": "NPRM", "date": "04/00/2026", "fr_citation": None},
        {"action": "Final", "date": "To Be Determined", "fr_citation": "91 FR 100"},
    ]
    shaped = shape_record(
        "unified_agenda",
        {
            "rin": "1000-AA00",
            "agenda_edition": "202604",
            "rin_status": "Withdrawn",
            "rule_stage": "Completed",
            "timetable_json": json.dumps(values),
        },
    )
    assert shaped["timetable"] == values
    assert shaped["rin_status"] == "Withdrawn"
    assert shaped["rule_stage"] == "Completed"


def test_cfr_part_strings_do_not_lose_alpha_suffixes_or_printed_ranges():
    shaped = shape_record(
        "federal_register",
        {
            "cfr_references_json": json.dumps(
                [
                    {"title": 40, "part": 60, "chapter": None, "citation_url": "url"},
                    {"title": 14, "part": "1203a", "chapter": "I"},
                    {"title": 40, "part": "80-85", "chapter": None},
                ]
            )
        },
    )
    assert [x["part"] for x in shaped["cfr_references"]] == ["60", "1203a", "80-85"]
    assert "citation_url" in shaped["raw_conversion_inputs"]["cfr_references_json"]


def test_domain_and_extraction_status_and_dates_are_main():
    shaped = shape_record(
        "documents",
        {
            "document_id": "D",
            "modify_date": "2026-01-01",
            "withdrawn": "true",
            "reason_withdrawn": "Agency withdrew proposal",
            "text_content": "held text",
            "text_extraction_status": "error",
        },
    )
    schema = subject_schema("documents")
    assert shaped["withdrawn"] is True
    assert shaped["reason_withdrawn"] == "Agency withdrew proposal"
    assert shaped["modify_date"] == "2026-01-01"
    assert shaped["text_extraction_status"] == "error"
    assert "text_extraction_status" in schema.names
    assert "text_content" in schema.names


def test_rule_targets_keep_witness_distinct_identity_and_source_namespace():
    base = {"docket_id": "D", "cfr_ref": "40-60", "rin": "1000-AA00"}
    a = shape_record("rule_targets", {**base, "source": "docket_rin"})
    b = shape_record("rule_targets", {**base, "source": "document_rin"})
    assert a["rule_target_id"] != b["rule_target_id"]
    assert "source" in subject_schema("rule_targets").names
    assert a["source"] == "docket_rin" and b["source"] == "document_rin"


def test_statistical_cells_keep_null_agency_and_estimand_fields():
    values = {
        "agency_code": None,
        "stratum": "all",
        "rules": 31,
        "finals": 9,
        "withdrawals": 2,
        "censored": 20,
        "suppressed": False,
        "median_days": None,
        "q1_days": 120,
        "censor_date": date(2026, 9, 27),
    }
    shaped = shape_record("agency_lifecycle_stats", values)
    assert all(shaped[k] == v for k, v in values.items())
    assert "pre_2008_coverage" in subject_schema("rulemaking_lifecycles").names


def test_each_assigned_table_has_explicit_columns_schema_and_identity():
    assert set(SOURCE_COLUMNS) == set(RECEIPT_COLUMNS) == set(IDENTITIES)
    retained_source_facts = {
        'comment_periods': {'unresolved_fr_references_json'},
        'comments': {'comment_reference_values_json', 'pdf_extraction_results_json'},
        'documents': {'pdf_extraction_results_json'},
        # The Register's literal president object: absent, JSON null and {} stay distinct (SpicyDocs 0.66.6 contract).
        # and its literal regulations.gov statement, public whole since regulations-native-v3 (ss-vse3).
        'federal_register': {'president_json', 'regulations_dot_gov_info_json'},
        'proceedings': {'fr_document_joins_json', 'unresolved_fr_references_json'},
        'regulatory_agenda_items': {'unresolved_fr_references_json'},
        'rule_targets': {'fr_references_json'},
    }
    for table in SOURCE_COLUMNS:
        schema = subject_schema(table)
        assert set(IDENTITIES[table]) <= set(schema.names)
        assert not set(RECEIPT_COLUMNS[table]) & set(schema.names)
        assert {f.name for f in schema if f.name.endswith("_json")} == retained_source_facts.get(table, set())
        assert len(set(schema.names)) == len(schema)


def test_wrong_native_values_do_not_coerce_to_strings_or_integers():
    with pytest.raises(RegulationsShapeError):
        shape_record("documents", {"attachments_json": '[{"size":"3"}]'})
    with pytest.raises(RegulationsShapeError):
        shape_record("regulatory_agenda_items", {"linked_proceeding_count": "1.0"})
    with pytest.raises(RegulationsShapeError):
        shape_record("dockets", {"docket_id": 42})


def test_rule_target_dated_references_preserve_repeats_without_promoting_ambiguity():
    refs = [
        {"candidate_ids": ["2026-1@2026-01-01"], "status": "dated"},
        {"candidate_ids": ["2026-1@2026-01-01"], "status": "single_candidate_in_input"},
        {"candidate_ids": ["2026-2@2026-01-01", "2026-2@2026-01-02"], "status": "ambiguous"},
    ]
    shaped = shape_record(
        "rule_targets", {"docket_id": "D", "source": "document_fr_doc", "fr_references_json": json.dumps(refs)}
    )
    assert shaped["fr_document_ids"] == ["2026-1@2026-01-01", "2026-1@2026-01-01", None]
    assert shaped["fr_references_json"] == json.dumps(refs)


def test_new_register_fields_keep_relations_counts_and_supporting_titles_native():
    info = {
        "agency_id": "EPA", "title": "Supporting material", "comments_count": 0,
        "docket_id": "EPA-X", "document_id": "EPA-X-1", "supporting_documents_count": 2,
        "supporting_documents": [{"document_id": "EPA-X-2", "title": "Study"},
                                 {"document_id": "EPA-X-2", "title": "Study"}],
        "regulatory_plan": {"html_url": "https://example.test/plan", "title": "Plan"},
    }
    raw = {"document_number": "2026-1", "publication_date": "2026-01-01",
           "action": "Final rule; correction.", "significant": "false", "corrections_json": '["C1-2026-1"]',
           "regulations_dot_gov_info_json": json.dumps(info), "regulations_dot_gov_comments_count": "0"}
    shaped = shape_record("federal_register", raw)
    assert shaped["significant"] is False
    assert shaped["regulations_dot_gov_comments_count"] == 0
    assert shaped["corrections"] == ["C1-2026-1"]
    assert shaped["regulations_dot_gov_supporting_documents"] == info["supporting_documents"]
    assert shaped["regulations_dot_gov_regulatory_plan_title"] == "Plan"
    assert shaped["raw_conversion_inputs"] == raw
    # The statement itself is a public column, whole: the plan's html_url is stated nowhere else (ss-vse3).
    assert shaped["regulations_dot_gov_info_json"] == json.dumps(info)
    assert subject_schema("federal_register").names[-5:] == [
        "html_url", "pdf_url", "body_html_url", "source_sha256", "regulations_dot_gov_info_json"]
    # The links' promoted field is a list like the table's others, not JSON text (ss-vse3).
    links = subject_schema("fr_docket_links")
    assert links.names[-2:] == ["link_source", "normalized_docket_candidates"]
    assert links.field("normalized_docket_candidates").type == links.field("docket_ids").type
    info["supporting_documents"][0]["unclassified"] = "retain, then refuse"
    with pytest.raises(RegulationsShapeError):
        shape_record("federal_register", raw | {"regulations_dot_gov_info_json": json.dumps(info)})


def test_plain_comment_text_and_source_restrictions_are_mapped_before_receipts():
    raw = {"comment_id": "C", "comment": "I&#39;m here.<br>Thanks &amp; good luck.",
           "attachments_json": '[{"title":"Study","formats":null,"restrictReasonType":"Copyrighted"}]'}
    shaped = shape_record("comments", raw)
    assert shaped["comment_text"] == "I'm here.\nThanks & good luck."
    assert shaped["attachments"] == [{"title": "Study", "formats": None, "restrict_reason_type": "Copyrighted"}]
    assert shaped["raw_source_record"] == raw
    assert "raw_conversion_inputs" not in shaped  # Comments keeps one replayable original input.


def test_the_installed_extracts_attachment_id_lands_in_the_comments_attachments_struct():
    """spicy-docs 0.85.0 ends each comment attachment entry with ``attachment_id``, NULL for a malformed id; the comments
    mapper accepts the key and the native struct keeps it, so nothing is refused or dropped."""
    import pyarrow as pa

    from spicy_regs.schemas import COMMENT

    record = {"data": {"id": "EPA-X-0001-0002", "attributes": {}}, "included": [
        {"id": "09000064b8f4997d", "type": "attachments", "attributes": {"title": "Letter", "fileFormats": [
            {"fileUrl": "https://downloads.regulations.gov/EPA-X-0001-0002/attachment_1.pdf", "format": "pdf",
             "size": 4}]}},
        {"id": 7, "type": "attachments", "attributes": {"title": "Study", "fileFormats": None,
                                                        "restrictReasonType": "Copyrighted"}},
    ]}
    extracted = COMMENT.extract(record)
    shaped = shape_record("comments", {"comment_id": "EPA-X-0001-0002", "attachments_json": extracted["attachments_json"]})
    stored = pa.Table.from_pylist([shaped], schema=subject_schema("comments")).to_pylist()[0]["attachments"]
    assert [entry["attachment_id"] for entry in stored] == ["09000064b8f4997d", None]
    assert [entry["restrict_reason_type"] for entry in stored] == [None, "Copyrighted"]


def test_agency_slugs_read_back_as_the_projection_spells_them(tmp_path):
    """``published_as_source`` joins the slugs with commas, as SpicyDocs' projection does, and the mapper splits them.

    Exact for what the projection writes (non-empty slugs, no comma, or no slug at all); the documented limit is held
    here so a change to it is deliberate: an empty list, which the projection never writes, comes back as ``[""]``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.transforms.regulations_shape import published_as_source, shape_records

    schema = subject_schema("federal_register")
    slugs = [["environmental-protection-agency", "air-and-radiation-office"], ["treasury-department"], None, []]
    rows = [dict.fromkeys(schema.names) | {"document_number": f"2026-{i}", "publication_date": "2026-01-01",
                                           "agency_slugs": value} for i, value in enumerate(slugs)]
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp_path / "published.parquet")
    text = published_as_source("federal_register", tmp_path / "published.parquet", tmp_path / "text.parquet")
    back = [row["agency_slugs"] for row in shape_records("federal_register", pq.read_table(text).to_pylist())]
    assert back == [*slugs[:3], [""]]
