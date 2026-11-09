"""Mapping tests cover source identity and the dates that must stay distinct."""

import json

import pytest

from spicy_regs.source_catalog.raw_mapping import REGISTRY, SourceMapper
from spicy_regs.source_catalog.raw_content import extract_metadata


def source(url=None, **context):
    return {"original_location": "/old/capture-2026-10-24/opaque-hash", "receipt_source_url": url,
            "receipt_context": context}


def test_native_congress_and_registry_family():
    mapper = SourceMapper()
    result = mapper.map_source(source("https://www.govinfo.gov/bulkdata/BILLSTATUS/113/hr/BILLSTATUS-113-hr.zip"))
    assert result["dataset_id"] == "family:bill-family"
    assert result["native_id"] == "BILLSTATUS-113-hr"
    assert [(row["kind"], row["label"]) for row in result["periods"]] == [("congress", "113")]
    assert result["acquired_at"] is None


def test_a_billsum_zip_is_the_bill_familys_with_its_native_congress():
    result = SourceMapper().map_source(source("https://www.govinfo.gov/bulkdata/BILLSUM/118/hr/BILLSUM-118-hr.zip"))
    assert result["dataset_id"] == "family:bill-family"
    assert result["native_id"] == "BILLSUM-118-hr"
    assert [(row["kind"], row["label"]) for row in result["periods"]] == [("congress", "118")]


def test_cfr_requested_edition_is_not_native_body_edition():
    result = SourceMapper().map_source(source("https://www.govinfo.gov/content/pkg/CFR-2025-title1-vol1/xml/CFR-2025-title1-vol1.xml"))
    assert result["dataset_id"] == "family:cfr-sections"
    assert result["periods"][0]["kind"] == "requested-edition"
    assert result["source_version"] is None


def test_ecfr_issue_is_separate_from_acquisition():
    result = SourceMapper().map_source(source("https://www.ecfr.gov/api/versioner/v1/full/2026-08-10/title-1.xml",
                                              acquired_at="2026-08-24T12:00:00Z"))
    assert result["source_version"] == "2026-08-10"
    assert result["acquired_at"] == "2026-08-24T12:00:00Z"
    assert result["periods"][0]["kind"] == "requested-issue-date"


def test_local_campaign_date_and_media_name_do_not_become_coverage():
    mapper = SourceMapper()
    result = mapper.map_source(source("https://www-senate-gov-msl3archive.akamaized.net/judiciary/judiciary020922_1/text/916.vtt"))
    assert result["dataset_id"] == "family:committee-meetings"
    assert result["periods"] == []
    assert result["acquired_at"] is None


def test_retained_comment_directory_maps_without_invented_dates():
    result = SourceMapper().map_source({"original_location": "/corpora/full-comments/acf-campaign/originals/ACF/hash.json"})
    assert result["dataset_id"] == "family:comments-mirror"
    assert result["periods"] == []


def test_requested_fec_cycle_and_credential_redaction():
    result = SourceMapper().map_source(source("https://api.open.fec.gov/v1/filings/?cycle=2024&api_key=secret&per_page=100"))
    assert result["dataset_id"] == "family:fec-query"
    assert result["periods"][0]["label"] == "2024"
    assert "secret" not in str(result)


def test_family_binding_wins_over_general_publisher_route():
    result = SourceMapper().map_source(source("https://www.gao.gov/products/gao-22-105", family="gao-recommendations"))
    assert result["dataset_id"] == "family:gao-recommendations"
    assert result["mapping_status"] == "mapped"
    assert any(row["dataset_id"] == "family:gao-reports" for row in result["evidence"])


def test_conflicting_explicit_bindings_require_review():
    record = source(dataset_id="family:members")
    record["dataset_id"] = "family:amendments"
    result = SourceMapper().map_source(record)
    assert result["dataset_id"] is None
    assert result["mapping_status"] == "conflict"
    assert result["candidate_dataset_ids"] == ["family:amendments", "family:members"]


def test_unknown_is_explicit_and_is_not_given_capture_year():
    result = SourceMapper().map_source(source())
    assert result["mapping_status"] == "unmapped"
    assert result["periods"] == []


def test_registry_source_name_reuses_collection_identity():
    mapper = SourceMapper({"collections": [{"id": "declared:example", "source_names": ["native-source"]}]})
    assert mapper.map_source(source(source_name="native-source"))["dataset_id"] == "declared:example"


def test_explicit_new_writer_fields_round_trip():
    result = SourceMapper().map_source(source(dataset_id="source:example", native_id="native-42",
                                              covered_period={"year": 2024}, source_version="v2",
                                              acquired_at="2026-10-24T12:00:00Z"))
    assert result["dataset_id"] == "source:example"
    assert result["native_id"] == "native-42"
    assert result["source_version"] == "v2"
    assert result["periods"][0]["kind"] == "declared-covered-period"
    assert result["acquired_at"] == "2026-10-24T12:00:00Z"


def test_federal_register_request_range_does_not_claim_completeness():
    url = "https://www.federalregister.gov/api/v1/documents.json?conditions%5Bpublication_date%5D%5Bgte%5D=2025-01-01&conditions%5Bpublication_date%5D%5Blte%5D=2025-12-31&page=2"
    result = SourceMapper().map_source(source(url))
    assert result["periods"][0]["kind"] == "requested-publication-range"
    assert "pagination completeness unknown" in result["periods"][0]["basis"]


def test_comment_native_posted_date_is_per_record_not_request_or_acquisition():
    original = json.dumps({"data": {"type": "comments", "id": "ACF-2023-0009-44294", "attributes": {
        "postedDate": "2023-12-04T05:00:00Z", "receiveDate": "2023-12-02T05:00:00Z",
        "modifyDate": "2023-12-05T02:20:24Z"}}}).encode()
    result = extract_metadata("family:comments-mirror", original)["periods"]
    assert result[0]["kind"] == "record-posted-date"
    assert result[0]["label"] == "2023-12-04"
    assert "observed record, not completeness" in result[0]["basis"]


def test_comment_errors_lists_and_invalid_dates_do_not_make_period_claims():
    mapping = {"dataset_id": "family:comments-mirror"}
    for value in ({"errors": [{"detail": "2023-12-04 unavailable"}]}, {"data": []},
                  {"data": {"type": "comments", "id": "X", "attributes": {"postedDate": "2023-99-99"}}},
                  {"data": {"type": "documents", "id": "X", "attributes": {"postedDate": "2023-12-04"}}}):
        assert extract_metadata(mapping["dataset_id"], json.dumps(value).encode())["periods"] == []


def test_top_level_writer_url_and_failed_response_status_are_preserved():
    result = SourceMapper().map_source({"source_url": "https://api.open.fec.gov/v1/filings/?cycle=2024",
                                        "http_status": 500})
    assert result["dataset_id"] == "family:fec-query"
    assert result["http_status"] == 500
    assert "success" not in result


def test_gzip_wrapped_native_package_extension_is_not_an_identity_component():
    result = SourceMapper().map_source(source("https://www.govinfo.gov/content/pkg/BILLS-119hr1ih.xml.gz"))
    assert result["native_id"] == "BILLS-119hr1ih"


def test_fr_issue_metadata_suffix_is_not_an_inferred_package_identity():
    from spicy_regs.source_catalog.raw_archive import merge_content

    # Literal local filename from verified sha256:04999d258b1340ac04735a663d8f1c5378789c91d5777bf26a6b05e1f2f1c802.
    raw = b'''<mods xmlns="http://www.loc.gov/mods/v3"><originInfo><dateIssued>1960-12-31</dateIssued>
      </originInfo><extension><collectionCode>FR</collectionCode><accessId>FR-1960-12-31</accessId></extension></mods>'''
    for name in ('FR-1960-12-31_mods.xml', 'FR-1960-12-31.mods.xml.gz'):
        record = {'original_location': '/old/raw/' + name}
        mapping = SourceMapper().map_source(record)
        assert mapping['native_id'] == 'FR-1960-12-31'
        assert record['original_location'].endswith(name)
        assert any(e['value'] == record['original_location'] for e in mapping['evidence'])
        merged = merge_content(mapping, extract_metadata('family:federal-register', raw), 'sha256:' + 'a' * 64)
        assert not merged['content_metadata'].get('conflict')
        assert any(p['label'] == '1960-12-31' and p.get('origin') == 'content' for p in merged['periods'])


def test_fr_metadata_suffix_normalization_preserves_explicit_receipt_claims_and_real_conflicts():
    from spicy_regs.source_catalog.raw_archive import merge_content

    record = {'original_location': '/old/raw/FR-1960-12-31_mods.xml',
              'receipt_context': {'native_id': 'FR-1960-12-30'}}
    mapping = SourceMapper().map_source(record)
    assert mapping['native_id'] == 'FR-1960-12-30'
    merged = merge_content(mapping, {'outcome': 'ok', 'native_id': 'FR-1960-12-31'}, 'sha256:' + 'a' * 64)
    assert merged['content_metadata']['conflict'] is True
    assert merged['field_conflicts']['receipt_content_native_id'] == ['FR-1960-12-30', 'FR-1960-12-31']
    literal = SourceMapper().map_source({'receipt_context': {'native_id': 'FR-1960-12-31_mods'}})
    assert literal['native_id'] == 'FR-1960-12-31_mods'
    unrelated = SourceMapper().map_source({'original_location': '/old/raw/FR-1960-12-31_mods-extra.xml'})
    assert unrelated['native_id'] == 'FR-1960-12-31_mods-extra'


def test_comment_acquisition_sidecar_is_not_classified_as_source_body():
    result = SourceMapper().map_source({"original_location": "/corpora/full-comments/acf-campaign/originals/ACF/hash.receipt.json"})
    assert result["dataset_id"] == "family:comments-mirror"
    assert result["role"] == "acquisition-receipt"
    assert result["periods"] == []


def test_run_directory_is_not_a_native_package_identifier():
    result = SourceMapper().map_source({"original_location": "/corpora/fr-audit-2026-09-23/raw/hash.json"})
    assert result["dataset_id"] == "family:federal-register"
    assert result["native_id"] is None
    assert result["periods"] == []


def test_conflicting_hash_matched_receipt_url_does_not_qualify_a_period():
    record = source("https://www.govinfo.gov/content/pkg/CREC-2016-02-04/html/CREC-2016-02-04-dailydigest.htm")
    record["original_location"] = "/old/PLAW-119pvtl1.xml"
    result = SourceMapper().map_source(record)
    assert result["mapping_status"] == "conflict"
    assert result["native_id"] is None
    assert result["periods"] == []
    assert {p["kind"] for p in result["candidate_periods"]} == {"congress", "publication-date"}


def test_congressional_meetings_and_communications_keep_native_congress():
    for endpoint, family in [("committee-meeting", "committee-meetings"),
                             ("house-communication", "house-communications")]:
        result = SourceMapper().map_source(source(f"https://api.congress.gov/v3/{endpoint}/119/house/1"))
        assert result["dataset_id"] == "family:" + family
        assert result["periods"][0]["label"] == "119"
        assert result["periods"][0]["kind"] == "congress"


def test_native_receipt_aliases_keep_field_provenance():
    record = {
        "original_location": "/old/request.body", "receipt_object": "acquisitions/sha256/ab/receipt",
        "receipt_context": {"requested_url": "https://api.open.fec.gov/v1/filings/?cycle=2024",
                            "observed_at": "2026-10-04T04:55:20.053512Z", "status_code": 200,
                            "source_record_key": "filings-page-1", "response_complete": True},
    }
    result = SourceMapper().map_source(record)
    assert result["dataset_id"] == "family:fec-query"
    assert result["native_id"] == "filings-page-1"
    assert result["acquired_at"] == record["receipt_context"]["observed_at"]
    assert result["http_status"] == 200
    assert result["response_complete"] is True
    assert result["source_validation"] is None
    assert {p["label"] for p in result["periods"]} == {"2024"}
    fact = next(e for e in result["evidence"] if e["target"] == "native_id")
    assert fact["origin"] == "receipt"
    assert "receipt_ref" not in fact  # The enclosing association owns this reference once.
    assert "association" not in fact
    assert fact["locator"] == "/receipt_context/source_record_key"


def test_status_aliases_keep_proxy_source_failures_and_refusals():
    result = SourceMapper().map_source(source(source_http_status=500, transport="zyte-native",
                                              capture_complete=True, outcome="refused"))
    assert result["http_status"] == 500
    assert result["transport"] == "zyte-native"
    assert result["response_complete"] is True
    assert result["source_validation"] == "refused"
    assert result["source_file_complete"] is None


def test_response_completeness_does_not_imply_whole_file_or_snapshot():
    record = source(complete_http_response=True, source_snapshot_complete=False, refused=False)
    record["partial"] = True
    result = SourceMapper().map_source(record)
    assert result["response_complete"] is True
    assert result["source_file_complete"] is False
    assert result["source_validation"] is None


def test_receipt_conflicts_stay_unknown_and_retain_both_claims():
    record = source(native_id="B", source_version="v2", observed_at="2026-10-24T02:00:00Z",
                    status_code=500, capture_complete=False, source_validation="refused")
    record.update(native_id="A", source_version="v1", acquired_at="2026-10-24T01:00:00Z",
                  http_status=200, response_complete=True, source_validation="accepted")
    result = SourceMapper().map_source(record)
    for target in ("native_id", "source_version", "acquired_at", "http_status",
                   "response_complete", "source_validation"):
        assert result[target] is None
        assert len(result["field_conflicts"][target]) == 2
        assert len([e for e in result["evidence"] if e["target"] == target]) == 2


def test_equivalent_timestamp_offsets_are_one_instant():
    record = source(observed_at="2026-10-24T01:00:00-04:00")
    record["acquired_at"] = "2026-10-24T05:00:00Z"
    result = SourceMapper().map_source(record)
    assert result["acquired_at"] == "2026-10-24T05:00:00Z"
    assert "acquired_at" not in result["field_conflicts"]


def test_invalid_receipt_types_and_timestamps_remain_unqualified():
    for timestamp in ("2026-10-24", "2026-10-24T12:00:00", "2026-02-30T12:00:00Z",
                      "2026-10-24T12:00:00+25:00", True):
        result = SourceMapper().map_source(source(observed_at=timestamp, http_status=True,
                                                  capture_complete="true", native_id=False,
                                                  source_validation="pass"))
        assert all(result[k] is None for k in ("acquired_at", "http_status", "response_complete",
                                               "native_id", "source_validation"))
        assert result["periods"] == []
        assert all(e["valid"] is False for e in result["evidence"])


def test_same_payload_does_not_share_observation_metadata():
    mapper = SourceMapper()
    first = mapper.map_source({"sha256": "same", "source_url": "https://api.open.fec.gov/v1/filings/?cycle=2024",
                               "acquired_at": "2026-10-24T12:00:00Z"})
    second = mapper.map_source({"sha256": "same", "receipt_context": {"same_bytes_as": "/old/request.body"}})
    assert first["periods"]
    assert second["acquired_at"] is None
    assert second["periods"] == []
    assert second["dataset_id"] is None


def test_requested_and_declared_periods_keep_their_meaning():
    result = SourceMapper().map_source(source(requested_period="2024", covered_period="2023",
                                              observed_at="2026-10-24T12:00:00Z"))
    assert {(p["kind"], p["label"]) for p in result["periods"]} == {
        ("declared-requested-period", "2024"), ("declared-covered-period", "2023"),
    }
    assert {e["locator"] for e in result["evidence"] if e["target"] == "periods"} == {
        "/receipt_context/requested_period", "/receipt_context/covered_period",
    }


def test_structured_receipt_period_keeps_searchable_date_and_evidence():
    result = SourceMapper().map_source(source(
        dataset_id="family:federal-register", native_id="94-9",
        period={"kind": "date", "label": "1994-01-04",
                "basis": "Companion publisher metadata publication_date."},
    ))
    assert [(p["kind"], p["label"]) for p in result["periods"]] == [("date", "1994-01-04")]
    assert "Companion publisher metadata publication_date." in result["periods"][0]["basis"]
    assert "declared scope; completeness unknown" in result["periods"][0]["basis"]
    evidence = [e for e in result["evidence"] if e["target"] == "periods"]
    assert evidence[0]["locator"] == "/receipt_context/period"
    assert evidence[0]["origin"] == "receipt"


def test_structured_period_list_preserves_each_native_kind():
    result = SourceMapper().map_source(source(period=[
        {"kind": "publication-date", "label": "1994-04-05", "basis": "Native FR package"},
        {"kind": "congress", "label": 103},
    ]))
    assert [(p["kind"], p["label"]) for p in result["periods"]] == [
        ("publication-date", "1994-04-05"), ("congress", "103"),
    ]
    assert {e["locator"] for e in result["evidence"] if e["target"] == "periods"} == {
        "/receipt_context/period/0", "/receipt_context/period/1",
    }


def test_structured_requested_and_covered_periods_remain_qualified():
    result = SourceMapper().map_source(source(
        requested_period={"kind": "publication-date", "label": "2024-01-04"},
        covered_period={"kind": "date", "label": "2023-01-04"},
    ))
    assert {(p["kind"], p["label"]) for p in result["periods"]} == {
        ("requested-publication-date", "2024-01-04"), ("declared-covered-date", "2023-01-04"),
    }


def test_malformed_structured_period_is_preserved_without_typed_period_claim():
    for declared in [{"kind": "date", "label": True}, {"kind": 1994, "label": "1994"},
                     [{"kind": "date", "label": "1994-01-04"}, {}]]:
        result = SourceMapper().map_source(source(period=declared))
        assert result["periods"][0]["kind"] == "declared-period"
        assert result["periods"][0]["label"] == json.dumps(declared, sort_keys=True)


def test_named_native_periods_keep_meanings_and_leaf_evidence():
    result = SourceMapper().map_source(source(
        period={"congress": "116", "meeting-date": "2019-03-12"},
        period_basis="Native source fields at the cited body locators; document scope only.",
    ))
    assert [(p["kind"], p["label"]) for p in result["periods"]] == [
        ("congress", "116"), ("meeting-date", "2019-03-12"),
    ]
    assert all("document scope only" in p["basis"] for p in result["periods"])
    assert {e["locator"] for e in result["evidence"] if e["target"] == "periods"} == {
        "/receipt_context/period/congress", "/receipt_context/period/meeting-date",
    }


def test_named_period_metadata_is_retained_as_evidence_not_periods():
    declared = {"congress": 108, "title": "Publisher scorecard title", "basis": "native HTML title"}
    result = SourceMapper().map_source(source(period=declared))
    assert [(p["kind"], p["label"]) for p in result["periods"]] == [("congress", "108")]
    evidence = next(e for e in result["evidence"] if e["target"] == "period_declaration")
    assert evidence["value"] == declared
    assert evidence["locator"] == "/receipt_context/period"
    assert "native HTML title" in result["periods"][0]["basis"]


def test_named_requested_and_covered_periods_remain_qualified():
    result = SourceMapper().map_source(source(
        requested_period={"congress": "119", "schedule-week": "2026-05-11"},
        covered_period={"document-date": "2019-01-30"},
    ))
    assert {(p["kind"], p["label"]) for p in result["periods"]} == {
        ("requested-congress", "119"), ("requested-schedule-week", "2026-05-11"),
        ("declared-covered-document-date", "2019-01-30"),
    }


def test_invalid_or_unknown_named_periods_stay_unqualified():
    for declared in [
        {"congress": True}, {"congress": "0"}, {"meeting-date": "2019-02-30"},
        {"congress": "116", "other": "not period evidence"}, {"title": "2024"},
        {"start": "2024-01-01", "end": "2024-12-31"},
    ]:
        result = SourceMapper().map_source(source(period=declared))
        assert result["periods"][0]["kind"] == "declared-period"
        assert result["periods"][0]["label"] == json.dumps(declared, sort_keys=True)


def test_named_congress_conflict_survives_as_candidates():
    from spicy_regs.source_catalog.raw_archive import merge_content

    mapping = SourceMapper().map_source(source(period={"congress": "118"}))
    result = merge_content(mapping, {
        "outcome": "ok", "primary_records": 1,
        "periods": [{"kind": "congress", "label": "119"}],
    }, "sha256:example")
    assert result["periods"] == []
    assert {p["label"] for p in result["candidate_periods"]} == {"118", "119"}


def test_conflicting_url_native_ids_are_not_selected_by_order():
    result = SourceMapper().map_source(source(
        requested_url="https://www.govinfo.gov/content/pkg/BILLS-119hr1ih/xml/BILLS-119hr1ih.xml",
        resolved_url="https://www.govinfo.gov/content/pkg/BILLS-119hr2ih/xml/BILLS-119hr2ih.xml",
    ))
    assert result["native_id"] is None
    assert result["field_conflicts"]["native_id"] == ["BILLS-119hr1ih", "BILLS-119hr2ih"]
    assert result["dataset_id"] == "family:bill-family"


def test_bad_calendar_dates_do_not_become_requested_periods():
    mapper = SourceMapper()
    for url in ("https://www.ecfr.gov/api/versioner/v1/full/2026-02-30/title-1.xml",
                "https://www.govinfo.gov/content/pkg/FR-2026-99-01/xml/FR-2026-99-01.xml",
                "https://lda.gov/api/v1/filings/?filing_dt_posted_after=2026-12-31&filing_dt_posted_before=2026-01-01"):
        assert mapper.map_source(source(url))["periods"] == []


def test_all_receipt_url_aliases_remove_credentials():
    result = SourceMapper().map_source(source(requested_url="https://user:secret@api.open.fec.gov/v1/filings/?cycle=2024&api_key=secret"))
    assert "secret" not in str(result)
    assert result["periods"][0]["label"] == "2024"


def test_package_metadata_filename_is_not_a_different_native_identity():
    record = source("https://www.govinfo.gov/content/pkg/PLAW-119publ1/premis/PLAW-119publ1.xml")
    record["original_location"] = "/old/PLAW-119publ1.premis.xml"
    result = SourceMapper().map_source(record)
    assert result["native_id"] == "PLAW-119publ1"
    assert result["field_conflicts"] == {}


def test_boolean_period_and_unqualified_status_are_not_normalized():
    result = SourceMapper().map_source(source(covered_period=True, status="captured"))
    assert result["periods"] == []
    assert result["http_status"] is None
    assert result["source_validation"] is None


def test_explicit_whole_file_completeness_conflict_with_partial_is_preserved():
    result = SourceMapper().map_source({"source_file_complete": True, "partial": True})
    assert result["source_file_complete"] is None
    assert result["field_conflicts"]["source_file_complete"] == [True, False]


def test_established_source_evidence_journal_is_receipt_evidence():
    artifact = "0809bac0b3548d35b0ff6a5972e235a46a0386d1fd59db32b0da829d022b9d34"
    record = {"original_location": f"/corpora/spicyregs/source-evidence/{artifact}/journal.jsonl",
              "receipt_context": {"artifact": "sha256:" + artifact, "family": "roll-call-votes",
                                  "member": "journal.jsonl", "declared_role": "evidence"}}
    result = SourceMapper().map_source(record)
    assert result["role"] == "acquisition-receipt"
    assert result["periods"] == []
    claim = next(e for e in result["evidence"] if e["target"] == "role")
    assert claim["rule"] == "source-evidence-acquisition-journal"
    assert claim["locator"] == "/original_location"


def test_generic_journal_name_does_not_establish_acquisition_evidence():
    mapper = SourceMapper()
    for location in ("/research/journal.jsonl", "/source-evidence/not-an-artifact/journal.jsonl",
                     "/research/" + "a" * 64 + "/journal.jsonl"):
        result = mapper.map_source({"original_location": location, "receipt_context": {"member": "journal.jsonl"}})
        assert result["role"] == "original-response"


def test_registry_object_binding_narrows_only_its_declared_family():
    digest = "a" * 64
    registry = {"object_datasets": [{"dataset_id": "dataset:narrow", "family": "fec-query", "sha256": [digest]}]}
    mapper = SourceMapper(registry)
    held = {"sha256": digest, "receipt_context": {"family": "fec-query"}}
    assert mapper.map_source(held)["dataset_id"] == "dataset:narrow"
    # Another digest of the family, and the same digest under another family, keep the family dataset.
    assert mapper.map_source({**held, "sha256": "b" * 64})["dataset_id"] == "family:fec-query"
    assert mapper.map_source({**held, "receipt_context": {"family": "other"}})["dataset_id"] == "family:other"
    # A receipt that names a dataset of its own still conflicts with the registry rather than being overridden.
    both = {**held, "receipt_context": {"family": "fec-query", "dataset_id": "dataset:own"}}
    assert mapper.map_source(both)["mapping_status"] == "conflict"
    with pytest.raises(ValueError, match="Ambiguous object dataset binding"):
        SourceMapper({"object_datasets": registry["object_datasets"] * 2})


def test_fec_research_originals_have_one_dataset_per_collection():
    """ss-wfop Q4: the imported FEC research receipts map to six datasets, one per collection."""
    registry = json.loads(REGISTRY.read_text())
    bindings = registry["object_datasets"]
    assert len(bindings) == 6 and {b["family"] for b in bindings} == {"fec-query"}
    declared = {c["id"] for c in registry["collections"]}
    assert {b["dataset_id"] for b in bindings} <= declared
    digests = [d for b in bindings for d in b["sha256"]]
    assert len(digests) == len(set(digests)) == 418


def test_unified_agenda_and_courtlistener_local_code_refs_match_installed_spicy_docs():
    """ss-k4yp, ss-olaa, ss-z50t: registry code_refs for spicy-docs 0.68.0 modules match installed spicy-docs."""
    import ast
    import hashlib
    import importlib.util
    from pathlib import Path

    spec = importlib.util.find_spec("spicy_docs")
    assert spec is not None and spec.origin is not None
    sd_root = Path(spec.origin).parent
    registry = json.loads(REGISTRY.read_text())

    targets = {
        "repo:spicy-docs/src/spicy_docs/sources/unified_agenda/records.py",
        "repo:spicy-docs/src/spicy_docs/sources/courtlistener/local.py",
        "repo:spicy-docs/src/spicy_docs/storage/raw_archive.py",
        "repo:spicy-docs/src/spicy_docs/schemas/federal_register.py",
        "repo:spicy-docs/src/spicy_docs/sources/federal_register/native.py",
        "repo:spicy-docs/src/spicy_docs/sources/courtlistener/search.py",
    }
    found = []

    def walk(obj):
        if isinstance(obj, dict):
            if obj.get("path") in targets:
                found.append(obj)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)

    def find_node(tree, symbol):
        parts = symbol.split(".")
        if len(parts) == 1:
            return next(
                n for n in ast.walk(tree)
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.name == parts[0]
            )
        cls_name, meth_name = parts
        cls_node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls_name)
        return next(m for m in cls_node.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == meth_name)

    walk(registry)
    assert len(found) == 13 + 132 + 134
    for ref in found:
        rel = ref["path"].removeprefix("repo:spicy-docs/src/spicy_docs/")
        source_file = sd_root / rel
        if "sha256" in ref:
            assert ref["sha256"] == hashlib.sha256(source_file.read_bytes()).hexdigest()
        tree = ast.parse(source_file.read_text())
        node = find_node(tree, ref["symbol"])
        assert (ref["start_line"], ref["end_line"]) == (node.lineno, node.end_lineno)

