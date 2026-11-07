"""Explicit earlier field policies, shared by source replay and receipt validation."""

from dataclasses import replace
from importlib.resources import files
import json

# Exact columns promoted from receipts to subjects, or added from the source, in government-sources/2.
EARLIER_COLUMNS = {
    "gao_reports": ("major_rule_agency", "major_rule_rins", "major_rule_fr_citations"),
    "gao_decisions": ("b_numbers_truncated",),
    "gao_recommendations": ("first_seen", "last_seen"),
    "lobbying_filings": (
        "filing_type_display", "filing_period_display", "filing_document_content_type", "expenses_method",
        "expenses_method_display", "termination_date", "posted_by_name", "registrant_address_1",
        "registrant_address_2", "registrant_city", "registrant_state", "registrant_zip", "registrant_country",
        "registrant_ppb_country", "registrant_different_address",
    ),
}


def earlier_policies(policy):
    from spicy_regs.etl_receipts import DatasetPolicy
    if policy.dataset == "senate_expenditures" and policy.policy_version == "legislative-documents/2":
        import pyarrow as pa

        current = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/senate_expenditures.json").read_text()))
        if policy.descriptor() != current.descriptor():
            raise ValueError("Current Senate funding policy differs from its exact declaration")
        history = json.loads(files("spicy_regs").joinpath("legislative_policy_history.json").read_text())
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        previous_schema = policy.subject_schema
        for name in ("funding_year", "funding_year_end"):
            at = previous_schema.get_field_index(name)
            field = previous_schema.field(at)
            if field.type != pa.string():
                raise ValueError("Current Senate funding designations must be strings")
            previous_schema = previous_schema.set(at, field.with_type(pa.int64()))
        expected = replace(policy, subject_schema=previous_schema, policy_version="legislative-documents/1")
        if earlier.descriptor() != expected.descriptor():
            raise ValueError("Senate funding migration changes fields beyond the two declared types")
        return (earlier,)
    if policy.dataset == "crs_reports" and policy.policy_version == "government-sources/3":
        from spicy_regs.crs_report_detail import DETAIL_COLUMNS, RECEIPT_COLUMNS

        earlier = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "crs_detail_policy_history.json").read_text()))
        previous_schema = policy.subject_schema
        for name in DETAIL_COLUMNS:
            if name not in RECEIPT_COLUMNS:
                previous_schema = previous_schema.remove(previous_schema.get_field_index(name.removesuffix("_json")))
        if (earlier.policy_version != "government-sources/2" or earlier.identity_fields != policy.identity_fields
                or earlier.receipt_fields != policy.receipt_fields
                or not earlier.subject_schema.equals(previous_schema, check_metadata=True)):
            raise ValueError("CRS detail policy changes previous metadata fields or revision identity")
        return (earlier, *earlier_policies(earlier))
    if policy.dataset == "crs_reports" and policy.policy_version == "government-sources/2":
        history = json.loads(files("spicy_regs").joinpath("government_policy_history.json").read_text())
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        at = policy.subject_schema.get_field_index("update_date")
        previous_schema = policy.subject_schema.set(at, policy.subject_schema.field(at).with_nullable(True))
        if (earlier.policy_version != "government-sources/1"
                or policy.identity_fields != (*earlier.identity_fields, "update_date")
                or policy.subject_schema.field(at).nullable
                or earlier.receipt_fields != policy.receipt_fields
                or not earlier.subject_schema.equals(previous_schema, check_metadata=True)):
            raise ValueError("CRS report revision policy changes fields beyond the dated identity")
        return (earlier,)
    if policy.dataset == "bill_publisher_summaries" and policy.policy_version == "legislative-documents/2":
        history = json.loads(files("spicy_regs").joinpath("legislative_policy_history.json").read_text())
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        at = policy.subject_schema.get_field_index("update_date")
        previous_schema = policy.subject_schema.set(at, policy.subject_schema.field(at).with_nullable(True))
        if (earlier.policy_version != "legislative-documents/1"
                or policy.identity_fields != (*earlier.identity_fields, "update_date")
                or policy.subject_schema.field(at).nullable
                or earlier.receipt_fields != policy.receipt_fields
                or not earlier.subject_schema.equals(previous_schema, check_metadata=True)):
            raise ValueError("CRS summary revision policy changes fields beyond the dated identity")
        return (earlier,)
    if policy.dataset == "scorecard_member_links" and policy.policy_version == "scorecards-etl-member-links-v4":
        previous = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs.scorecards").joinpath(
            "geography_policy_history.json").read_text()))
        without_evidence = policy.subject_schema.remove(policy.subject_schema.get_field_index("geography_evidence_json"))
        if (previous.policy_version != "scorecards-etl-member-links-v3"
                or previous.identity_fields != policy.identity_fields
                or previous.receipt_fields != policy.receipt_fields
                or not previous.subject_schema.equals(without_evidence, check_metadata=True)):
            raise ValueError("Geography evidence policy changes previous fields or identity")
        return (previous, *earlier_policies(previous))
    if policy.dataset == "scorecard_member_links" and policy.policy_version in {
        "scorecards-etl-member-links-v3", "scorecards-etl-v2",
    }:
        previous = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs.scorecards").joinpath(
            "member_link_policy_history.json").read_text()))
        context_history = json.loads(files("spicy_regs").joinpath("source_context_policy_history.json").read_text())
        original = DatasetPolicy.from_descriptor(context_history[policy.dataset])
        if policy.policy_version == "scorecards-etl-v2":
            if policy.descriptor() != previous.descriptor():
                raise ValueError("Historical member link policy differs from the exact declared policy")
            return (original,)
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs.scorecards").joinpath(
            "geography_policy_history.json").read_text()))
        without_geography = policy.subject_schema
        for field in ("normalized_state", "state_status", "normalized_district", "district_status", "correction_evidence_json"):
            without_geography = without_geography.remove(without_geography.get_field_index(field))
        if (policy.descriptor() != expected.descriptor()
                or previous.dataset != policy.dataset or original.dataset != policy.dataset
                or previous.policy_version != "scorecards-etl-v2"
                or previous.identity_fields != policy.identity_fields
                or previous.receipt_fields != policy.receipt_fields
                or previous.nullable_identity_fields != policy.nullable_identity_fields
                or previous.receipt_only != policy.receipt_only
                or not previous.subject_schema.equals(without_geography, check_metadata=True)):
            raise ValueError("Member geography policy changes the prior link fields or identity")
        return (previous, original)
    if policy.dataset == "gao_reports" and policy.policy_version == "government-sources/4":
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/gao_reports.json").read_text()))
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current GAO citation-key policy differs from the exact declared policy")
        history = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())
        prior = DatasetPolicy.from_descriptor(history["gao_reports_v3"])
        if (prior.dataset != policy.dataset or prior.policy_version != "government-sources/3"
                or prior.identity_fields != policy.identity_fields
                or prior.receipt_fields != policy.receipt_fields
                or prior.nullable_identity_fields != policy.nullable_identity_fields
                or not prior.subject_schema.equals(policy.subject_schema.remove(
                    policy.subject_schema.get_field_index("citation_product_id")), check_metadata=True)):
            raise ValueError("GAO citation-key migration changes prior fields or identity")
        return (prior, *earlier_policies(prior))
    if policy.dataset == "gao_reports" and policy.policy_version == "government-sources/3":
        history = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())
        if policy.descriptor() != history["gao_reports_v3"]:
            raise ValueError("Historical GAO policy differs from the exact declared policy")
        context = json.loads(files("spicy_regs").joinpath("source_context_policy_history.json").read_text())
        prior = DatasetPolicy.from_descriptor(context[policy.dataset])
        return (prior, *earlier_policies(prior))
    if policy.dataset == "document_citations" and policy.policy_version == "legislative-documents/2":
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/document_citations.json").read_text()))
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current citation policy differs from the exact declared policy")
        history = json.loads(files("spicy_regs").joinpath("citation_navigation_policy_history.json").read_text())
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        if earlier.policy_version != "legislative-documents/1" or earlier.identity_fields != policy.identity_fields:
            raise ValueError("Citation namespace promotion changes a historical observation identity")
        return (earlier,)
    if policy.dataset == "committee_reports" and policy.policy_version == "legislative-documents/2":
        history = json.loads(files("spicy_regs").joinpath("legislative_policy_history.json").read_text())
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        if earlier.policy_version != "legislative-documents/1" or earlier.identity_fields != policy.identity_fields:
            raise ValueError("Committee report letter move changes a historical observation identity")
        return (earlier,)
    if policy.policy_version == "legislative-documents/2" and policy.dataset in {
        "native_legal_references", "native_legal_reference_reads",
    }:
        history = json.loads(files("spicy_regs").joinpath("native_legal_policy_history.json").read_text())
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        if earlier.dataset != policy.dataset or earlier.policy_version != "legislative-documents/1":
            raise ValueError("Native legal history differs from its declared policy")
        if not earlier.receipt_only and earlier.identity_fields != policy.identity_fields:
            raise ValueError("Native legal migration changes a historical observation identity")
        return (earlier,)

    if policy.policy_version in {"fec-subject-receipts/3", "fec-identity-context-receipts/3"}:
        current = json.loads(files("spicy_regs").joinpath("etl_policies/" + policy.dataset + ".json").read_text())
        history = json.loads(files("spicy_regs").joinpath("fec_conversion_policy_history.json").read_text())
        if policy.descriptor() != current or policy.dataset not in history:
            return ()
        previous = DatasetPolicy.from_descriptor(history[policy.dataset])
        original = json.loads(files("spicy_regs").joinpath("fec_policy_history.json").read_text())
        return (previous, DatasetPolicy.from_descriptor(original[policy.dataset]))

    if policy.policy_version in {"fec-subject-receipts/2", "fec-identity-context-receipts/2"}:
        fec_history = json.loads(files("spicy_regs").joinpath("fec_policy_history.json").read_text())
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/" + policy.dataset + ".json").read_text()))
        if policy.descriptor() != expected.descriptor() or policy.dataset not in fec_history:
            # Fixture or partial writer schemas do not acquire historical policy
            # authority merely by sharing the current version label.
            return ()
        return (DatasetPolicy.from_descriptor(fec_history[policy.dataset]),)
    if policy.dataset == "fcc_filings" and policy.policy_version == "government-sources/3":
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/fcc_filings.json").read_text()))
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current FCC policy differs from the exact declared policy")
        history = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())
        return tuple(DatasetPolicy.from_descriptor(history[key]) for key in ("fcc_filings_v2", "fcc_filings"))
    if policy.dataset == "nominations" and policy.policy_version == "congress-subjects/3":
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/nominations.json").read_text()))
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current nominations policy differs from the exact declared policy")
        history = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())
        return tuple(DatasetPolicy.from_descriptor(history[key]) for key in ("nominations_v2", "nominations"))
    context_history = json.loads(files("spicy_regs").joinpath("source_context_policy_history.json").read_text())
    if policy.dataset in context_history and policy.policy_version in {
        "regulations-native-v2", "courts/2", "scorecards-etl-v2", "scorecards-etl-ratings-v4", "government-sources/3",
    }:
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/" + policy.dataset + ".json").read_text()))
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current source-context policy differs from the exact declared policy")
        prior = DatasetPolicy.from_descriptor(context_history[policy.dataset])
        if prior.dataset != policy.dataset or (not prior.receipt_only and prior.identity_fields != policy.identity_fields):
            raise ValueError("Source-context migration changes a historical business identity")
        return (prior, *earlier_policies(prior))
    history = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())
    if policy.dataset in history and policy.policy_version in {"congress-subjects/2", "government-sources/2", "navigation-read-outcomes/2"}:
        if policy.policy_version == "navigation-read-outcomes/2" or policy.dataset == "roll_call_votes":
            expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
                "etl_policies/" + policy.dataset + ".json").read_text()))
            if policy.descriptor() != expected.descriptor():
                if policy.policy_version == "navigation-read-outcomes/2":
                    raise ValueError("Current House outcome policy differs from the exact declared policy")
                raise ValueError(f"Current {policy.dataset} policy differs from the exact declared policy")
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        if earlier.identity_fields != policy.identity_fields or earlier.dataset != policy.dataset:
            raise ValueError("Navigation migration changes a historical identity")
        if policy.policy_version == "navigation-read-outcomes/2" and earlier.policy_version != "navigation-read-outcomes/1":
            raise ValueError("House outcome history differs from its declared policy")
        return (earlier,)

    if policy.dataset == "scorecard_member_ratings" and policy.policy_version in {
        "scorecards-etl-ratings-v3",
        "scorecards-etl-ratings-v2",
    }:
        from spicy_regs.etl_receipts import DatasetPolicy

        historical = json.loads(files("spicy_regs.scorecards").joinpath("historical_rating_policies.json").read_text())
        versions = ("scorecards-etl-ratings-v2", "scorecards-etl-v1")
        if set(historical) != set(versions):
            raise ValueError("Historical scorecard policies differ from the declared versions")
        policies = {version: DatasetPolicy.from_descriptor(historical[version]) for version in versions}
        if any(p.dataset != policy.dataset or p.policy_version != version for version, p in policies.items()):
            raise ValueError("Historical scorecard policy identity differs")
        if policy.policy_version == "scorecards-etl-ratings-v3":
            current = context_history[policy.dataset]
            expected = DatasetPolicy.from_descriptor(current)
            admitted = tuple(policies[version] for version in versions)
        else:
            expected = policies["scorecards-etl-ratings-v2"]
            admitted = (policies["scorecards-etl-v1"],)
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current scorecard policy differs from the exact declared policy")
        return admitted
    if policy.policy_version != "government-sources/2" or policy.dataset not in EARLIER_COLUMNS:
        return ()
    schema = policy.subject_schema
    for column in EARLIER_COLUMNS[policy.dataset]:
        index = schema.get_field_index(column)
        if index < 0:
            raise ValueError("Current government policy omits a declared promoted column")
        schema = schema.remove(index)
    return (replace(policy, subject_schema=schema, policy_version="government-sources/1"),)
