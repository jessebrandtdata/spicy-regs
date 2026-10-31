"""Remaining forms keep notice, transfer, allocation and loan-state grains."""

from datetime import date
from decimal import Decimal
import json

import pyarrow as pa
import pytest

from spicy_regs.transforms.fec_filing_financial import load_layout, map_filing_financial
from spicy_regs.transforms.fec_filing_forms import (
    FILING_REPORT_SCHEMA,
    filing_cover_mapping_for,
    filing_cover_mappings,
    map_filing_cover,
    remaining_filing_mapping_for,
    remaining_filing_mappings,
)
from spicy_regs.transforms.fec_query import CollectionSelection

PIN = "sha256:" + "a" * 64
GEN = "sha256:" + "b" * 64
SELECTION = CollectionSelection("filing", PIN, GEN, "official-fec", None, "snapshot", PIN)


def source(native, ordinal):
    return dict(
        collection_id="filing",
        source_record_id=str(ordinal),
        source_sha256=PIN,
        source_locator_json=json.dumps(
            dict(collection_id="filing", source_record_id=str(ordinal), ordinal=ordinal, member=None)
        ),
        metadata_json=json.dumps(native),
    )


def map_record(version, form, fields=(), *, width=None, header_version=None):
    mapping = remaining_filing_mapping_for(version, form)
    assert mapping is not None
    layout = load_layout(mapping)
    count = len(layout["fields"]) if width is None else width
    values = dict.fromkeys(map(str, range(count)), "")
    values.update(fields)
    values["0"] = form
    native = dict(kind="record", record_type=form, field_count=count, fields=values)
    header = dict(kind="header", format_version=header_version or version, fields=["HDR", "FEC", version])
    row, evidence = map_filing_financial(
        source(native, 1), SELECTION, mapping, layout=layout, header_row=source(header, 0)
    )
    assert pa.Table.from_pylist([row], schema=mapping.schema).to_pylist() == [row]
    return row, evidence


@pytest.mark.parametrize("mapping", remaining_filing_mappings())
def test_every_physical_definition_position_has_exactly_one_destination(mapping):
    layout = load_layout(mapping)
    assert layout["source"]["sha256"] == mapping.workbook_sha256
    assert len(mapping.schema.names) == len(set(mapping.schema.names))


@pytest.mark.parametrize("version,offset", [("3.00", 12), ("5.1", 12), ("8.1", 17), ("8.5", 17)])
def test_contribution_notice_is_not_an_additive_receipt(version, offset):
    row, evidence = map_record(version, "F65", {str(offset): "123.45", str(offset - 1): "20240229"})
    assert row["amount"] == Decimal("123.45") and row["transaction_date"] == date(2024, 2, 29)
    assert row["financial_grain"] == "contribution-notice"
    assert row["aggregation_status"] == "overlap-unqualified"
    assert row["filing_key"] is None and row["current_record_status"] == "unqualified"
    assert [e["role"] for e in evidence] == ["primary", "filing_header"]


def test_bundler_period_and_semiannual_amounts_remain_independent():
    row, _ = map_record("8.1", "SA3L", {"6": "Native bundler", "20": "1500", "21": "2500", "25": ""})
    assert row["bundler_organization"] == "Native bundler"
    assert row["bundled_amount"] == 1500 and row["semiannual_bundled_amount"] == 2500
    assert "amount" not in row and row["donor_committee_id"] == ""
    assert row["financial_grain"] == "reported-bundler-period-aggregate"
    assert row["aggregation_status"] == "overlap-unqualified"


@pytest.mark.parametrize("form,amount_position,account_position", [("SASI1", 15, 36), ("SBSI2", 14, 33)])
def test_special_account_preserves_native_reference_without_inventing_account_identity(
    form, amount_position, account_position
):
    row, _ = map_record("3.00", form, {str(amount_position): "-10", str(account_position): "001-X"})
    assert row["amount"] == -10 and row["source_nonfederal_account"] == "001-X"
    assert row["reported_account_form"] == form
    assert row["aggregation_status"] == "overlap-unqualified"


@pytest.mark.parametrize("version,fields", [("5.00", {"7": "30", "8": "100"}), ("8.5", {"8": "100", "9": "30"})])
def test_account_transfer_total_and_event_component_do_not_swap_between_versions(version, fields):
    row, _ = map_record(version, "H3", fields)
    assert row["transferred_amount"] == 30 and row["total_amount"] == 100
    assert "amount" not in row and row["financial_grain"] == "reported-account-transfer"


def test_dictionary_placeholder_tail_is_not_silently_trimmed():
    row, _ = map_record("3.00", "H3", {"7": "30", "8": "100"}, width=11)
    assert row["mapping_status"] == "partial"
    assert json.loads(row["mapping_reason_json"])["absent_positions"] == list(range(11, 38))


def test_levin_transfer_total_and_activity_breakdown_are_separate():
    row, _ = map_record("5.3", "H5", {"4": "10", "5": "20", "6": "30", "7": "40", "8": "100"})
    assert row["total_amount"] == 100 and row["voter_registration_amount"] == 10
    assert row["voter_id_amount"] == 20 and row["gotv_amount"] == 30 and row["generic_campaign_amount"] == 40
    assert "amount" not in row


@pytest.mark.parametrize("version,start", [("5.3", 13), ("8.4", 19)])
def test_levin_payment_shares_are_not_relabeled_nonfederal_or_summed(version, start):
    row, _ = map_record(version, "H6", {str(start): "100", str(start + 1): "60", str(start + 2): "40"})
    assert row["total_amount"] == 100 and row["federal_share"] == 60 and row["levin_share"] == 40
    assert "nonfederal_share" not in row and "amount" not in row


def test_allocation_percentages_stay_literal_and_are_not_money():
    row, _ = map_record("8.5", "H1", {"7": "028.00", "8": "72", "9": "X"})
    assert row["federal_percentage"] == "028.00" and row["nonfederal_percentage"] == "72"
    assert row["administrative_ratio_applies"] == "X"
    assert "amount" not in row


def test_loan_terms_keep_principal_draw_balance_collateral_and_dates_distinct():
    row, _ = map_record(
        "8.5",
        "SC1/10",
        {
            "3": "native-ref",
            "10": "1000",
            "13": "ON DEMAND",
            "16": "200",
            "17": "800",
            "21": "3000",
            "25": "9000",
            "15": "20240101",
        },
    )
    assert row["loan_amount"] == 1000 and row["credit_draw_amount"] == 200 and row["total_balance"] == 800
    assert row["collateral_value"] == 3000 and row["future_income_estimated_value"] == 9000
    assert row["original_loan_date"] == date(2024, 1, 1) and row["due_date_terms"] == "ON DEMAND"
    assert row["back_reference_transaction_id"] == "native-ref" and row["filing_link_status"] == "unresolved"
    assert "amount" not in row


def test_extra_blank_invalid_amount_and_missing_money_are_distinct():
    row, _ = map_record("8.5", "F65", {"17": "1e3", "20": ""}, width=21)
    assert row["amount"] is None and row["amount_raw"] == "1e3" and row["amount_status"] == "unsupported_spelling"
    assert json.loads(row["mapping_reason_json"])["extra_positions"] == {"20": ""}
    row, _ = map_record("8.5", "F65", width=17)
    assert row["amount_status"] == "source_missing"
    row, _ = map_record("8.5", "F65")
    assert row["amount_status"] == "source_empty"


@pytest.mark.parametrize(
    "version,form",
    [
        ("8.50", "F65"),
        ("8.5 ", "F65"),
        ("8.5", "SA3L"),
        ("8.1", "SB3L"),
        ("3.00", "SASI2"),
        ("8.5", "H4"),
        ("8.5", "F3XN"),
        ("8.5", "SC1"),
    ],
)
def test_unreviewed_forms_and_spelling_are_not_routed_by_prefix(version, form):
    assert remaining_filing_mapping_for(version, form) is None


def test_header_layout_mismatch_refuses_before_typed_values():
    with pytest.raises(ValueError, match="header"):
        map_record("8.5", "F65", header_version="8.4")


def cover(version, form, updates=(), *, width=None, bodies=()):
    mapping = filing_cover_mapping_for(version, form)
    assert mapping is not None
    layout = load_layout(mapping)
    count = len(layout["fields"]) if width is None else width
    values = dict.fromkeys(map(str, range(count)), "")
    values.update(updates)
    values["0"] = form
    for body in bodies:
        del values[str(body["field_index"])]
    native = dict(kind="record", record_type=form, field_count=count, fields=values, embedded_bodies=list(bodies))
    row, evidence = map_filing_cover(
        source(native, 1),
        SELECTION,
        mapping,
        layout=layout,
        header_row=source(dict(kind="header", format_version=version, fields=["HDR", "FEC", version]), 0),
    )
    assert pa.Table.from_pylist([row], schema=FILING_REPORT_SCHEMA).to_pylist() == [row]
    return row, evidence


@pytest.mark.parametrize("mapping", filing_cover_mappings())
def test_cover_definitions_cover_all_physical_positions(mapping):
    load_layout(mapping)
    assert len(mapping.schema.names) == len(set(mapping.schema.names))


def test_report_measures_keep_repeated_labels_and_time_columns_distinct():
    row, evidence = cover(
        "8.5", "F3A", {"1": "C00123456", "15": "20240101", "16": "20240331", "23": "100", "62": "900", "29": "250"}
    )
    measures = {m["native_position"]: m for m in row["reported_measures"]}
    assert measures[23]["value"] == 100 and measures[62]["value"] == 900
    assert measures[23]["measure_role"] == "reported-total"
    assert measures[29]["quantity_kind"] == "reported-balance"
    assert row["period_start"] == date(2024, 1, 1) and row["period_end"] == date(2024, 3, 31)
    assert row["form_type"] == "F3A" and row["correction_operation"] == "none"
    assert row["filing_key"] is None and row["current_record_status"] == "unqualified"
    assert row["aggregation_status"] == "overlap-unqualified"
    assert all(m["period_basis"] == "native-report-column-not-harmonized" for m in row["reported_measures"])
    assert evidence[0]["target_table"] == "fec_filing_report_observations"


def test_report_year_is_not_money_and_repeated_totals_are_not_merged():
    row, _ = cover("8.5", "F3XN", {"23": "120", "74": "2024", "75": "300"})
    measures = {m["native_position"]: m for m in row["reported_measures"]}
    assert 74 not in measures
    assert "native_fields" not in row
    assert measures[23]["value"] == 120 and measures[75]["value"] == 300


def test_candidate_statement_retains_candidate_filer_and_literal_other_identifiers():
    row, _ = cover("8.5", "F2A", {"1": "H0AA00001", "2": "Native Name", "23": "C00123456"})
    assert row["reporting_candidate_id"] == "H0AA00001" and row["reporting_committee_id"] is None
    assert "native_fields" not in row
    assert row["reported_measures"] == [] and row["filing_link_status"] == "unresolved"


def test_retained_lowercase_supplement_is_explicit_and_never_normalized():
    row, _ = cover("8.1", "f2s", {"1": "H0AA00001", "2": "C00123456"})
    assert row["form_type"] == "f2s" and row["report_record_role"] == "supplement"
    assert row["reporting_candidate_id"] == "H0AA00001"
    assert "native_fields" not in row
    assert filing_cover_mapping_for("8.5", "f2s") is None


def test_special_account_summary_keeps_account_identity_and_totals_separate():
    row, _ = cover(
        "8.5",
        "SL",
        {
            "1": "C00123456",
            "3": "001",
            "4": "Native Account",
            "5": "20240101",
            "6": "20240331",
            "11": "300",
            "19": "400",
            "28": "700",
        },
    )
    assert row["account_native_id"] == "001" and row["account_name"] == "Native Account"
    assert row["report_record_role"] == "account-summary"
    measures = {m["native_position"]: m for m in row["reported_measures"]}
    assert measures[11]["value"] == 300 and measures[28]["value"] == 700
    assert measures[19]["quantity_kind"] == "reported-balance" and measures[19]["value"] == 400
    assert row["period_start"] == date(2024, 1, 1) and row["filing_link_status"] == "unresolved"


def test_v2_report_dictionary_ambiguity_refuses_shifted_date_and_money_interpretation():
    row, _ = cover("2.00", "F3N", {"1": "C00123456", "19": "19980701", "20": "19980930", "21": "159925.73"}, width=93)
    assert row["reporting_committee_id"] == "C00123456"
    assert row["reported_measures"] == [] and row["period_start"] is None and row["period_end"] is None
    assert row["definition_applicability_status"] == "source-layout-ambiguous"
    assert row["mapping_status"] == "partial"
    assert "native_fields" not in row


def test_report_absent_blank_body_and_extra_fields_survive_without_body_processing():
    row, _ = cover("8.5", "F3N", {"93": "extra"}, width=94, bodies=[dict(field_index=23, body_id="literal-body")])
    assert "native_fields" not in row
    reasons = json.loads(row["mapping_reason_json"])
    assert reasons["body_positions"] == [23]
    assert reasons["extra_positions"] == {"93": "extra"}
    assert row["reported_measures"][0]["value_status"] == "source_body_reference"
    row, _ = cover("8.5", "F3N", width=23)
    assert row["reported_measures"][0]["value_status"] == "source_missing"


@pytest.mark.parametrize("version,form", [("8.5", "F1X"), ("8.4", "F3PN"), ("v8.5", "F3N"), ("8.5", "F65")])
def test_cover_selection_requires_reviewed_form_and_version(version, form):
    assert filing_cover_mapping_for(version, form) is None


from spicy_regs.transforms.fec_filing_forms import (  # noqa: E402
    FILING_TEXT_SCHEMA,
    filing_text_mapping_for,
    filing_text_mappings,
    map_filing_text,
)
from spicy_regs.transforms.fec_typed_batch import typed_batch  # noqa: E402


@pytest.mark.parametrize("mapping", filing_text_mappings())
def test_narrative_definition_covers_every_native_field(mapping):
    assert load_layout(mapping)["source"]["sha256"] == mapping.workbook_sha256
    assert not mapping.fields.amounts and not mapping.fields.dates


def narrative(native, *, version="8.5", read_body=lambda _: "  exact\ntext  ", header=None):
    mapping = filing_text_mapping_for(version, "TEXT") if native["kind"] == "record" else None
    result, evidence = map_filing_text(
        source(native, 1),
        SELECTION,
        header_row=header or source(dict(kind="header", format_version=version), 0),
        read_body=read_body,
        mapping=mapping,
        layout=load_layout(mapping) if mapping else None,
    )
    assert typed_batch([result], FILING_TEXT_SCHEMA).to_pylist() == [result]
    return result, evidence


def text_record(version="8.5", body=True):
    mapping = filing_text_mapping_for(version, "TEXT")
    count = len(load_layout(mapping)["fields"])
    position = next(int(p) for n, p in mapping.fields.text if n == "native_text")
    fields = dict.fromkeys(map(str, range(count)), "")
    fields.update({"0": "TEXT", "1": "001"})
    native: dict = dict(kind="record", record_type="TEXT", field_count=count, fields=fields)
    if body:
        del fields[str(position)]
        native["embedded_bodies"] = [
            dict(sha256=PIN, byte_offset=12, byte_length=20, encoding="utf-8", delimiter="\x1c", field_index=position)
        ]
    return native


@pytest.mark.parametrize("version,position", [("5.00", 3), ("5.3", 3), ("8.1", 5), ("8.5", 5)])
def test_narrative_body_and_field_pointers_preserve_text_and_unresolved_identity(version, position):
    result, evidence = narrative(text_record(version), version=version)
    fragment = result["text_fragments"][0]
    assert fragment["text"] == "  exact\ntext  "
    assert fragment["field_position"] == position
    assert fragment["source_pointer"] == "/embedded_bodies/0"
    assert json.loads(fragment["body_reference_json"])["field_index"] == position
    assert "native_fields" not in result
    assert result["filing_key"] is None and result["current_record_status"] == "unqualified"
    assert not {"currency", "amount_kind", "value_mapping_version", "correction_operation"} & result.keys()
    assert [e["role"] for e in evidence] == ["primary", "filing_header"]


def test_standalone_narrative_has_no_invented_dictionary_or_financial_interpretation():
    body = dict(sha256=PIN, byte_offset=0, byte_length=20, encoding="utf-8")
    result, _ = narrative(dict(kind="text", embedded_bodies=[body]))
    assert result["definition_set_id"] is None and "native_fields" not in result
    assert result["text_fragments"][0]["field_position"] is None
    assert result["filing_header_record_id"] == "0"


@pytest.mark.parametrize("empty,width,status", [(True, 6, "source_empty"), (False, 5, "source_missing")])
def test_narrative_empty_and_absent_are_distinct(empty, width, status):
    native = text_record(body=False)
    if not empty:
        del native["fields"]["5"]
    native["field_count"] = width
    result, _ = narrative(native, read_body=lambda _: pytest.fail("Inline text needs no body read"))
    assert result["text_fragments"][0]["text_status"] == status
    assert result["text_fragments"][0]["text"] == ("" if empty else None)


def test_narrative_read_refusal_propagates_without_empty_replacement():
    def refuse(_):
        raise ValueError("missing retained original")

    with pytest.raises(ValueError, match="missing retained original"):
        narrative(text_record(), read_body=refuse)
    with pytest.raises(ValueError, match="return source text"):
        narrative(text_record(), read_body=lambda _: None)


@pytest.mark.parametrize("case", ["header", "digest", "position", "mixed"])
def test_narrative_refuses_mismatched_header_body_and_positions(case):
    native = text_record()
    header = source(dict(kind="header", format_version="8.5"), 0)
    if case == "header":
        header["source_sha256"] = GEN
    if case == "digest":
        native["embedded_bodies"][0]["sha256"] = GEN
    if case == "position":
        native["embedded_bodies"][0]["field_index"] = 4
    if case == "mixed":
        native["fields"]["5"] = "duplicate"
    with pytest.raises(ValueError):
        narrative(native, header=header)


def test_narrative_uses_source_owned_reader_for_quoted_and_multiline_text(tmp_path):
    import hashlib
    from spicy_docs.sources.fec.filings import filing_body, filing_records

    original = b'HDR,FEC,5.1\nTEXT,SA,A1,"literal comma, quote ""yes""\nand newline"\n'
    digest = hashlib.sha256(original).hexdigest()
    (tmp_path / "sha256").mkdir()
    (tmp_path / "sha256" / digest).write_bytes(original)
    header, native = list(filing_records(store=tmp_path, sha256="sha256:" + digest))
    rows = [source(header, 0), source(native, 1)]
    for row in rows:
        row["source_sha256"] = "sha256:" + digest
    selection = CollectionSelection("filing", "sha256:" + digest, GEN, "official-fec", None, "snapshot", PIN)
    mapping = filing_text_mapping_for("5.1", "TEXT")
    result, _ = map_filing_text(
        rows[1],
        selection,
        header_row=rows[0],
        mapping=mapping,
        layout=load_layout(mapping),
        read_body=lambda body: filing_body(store=tmp_path, body=body),
    )
    assert result["text_fragments"][0]["text"] == 'literal comma, quote "yes"\nand newline'
    assert typed_batch([result], FILING_TEXT_SCHEMA).to_pylist() == [result]


def test_narrative_selector_does_not_guess_a_version_or_normalize_record_spelling():
    assert filing_text_mapping_for("8.50", "TEXT") is None
    assert filing_text_mapping_for("8.5", "text") is None
    with pytest.raises(ValueError, match="source-owned TEXT"):
        narrative(dict(kind="header", format_version="8.5"))


from spicy_regs.transforms.fec_filing_forms import map_filing_physical_text  # noqa: E402


@pytest.mark.parametrize("text", ['[ENDTEXT]"', '"HDR","FEC","5.1"', "Debt reported as $5,883.78.  Native text\t", ""])
def test_physical_filing_text_remains_searchable_without_repair_or_financial_interpretation(text):
    native: dict = dict(
        kind="row",
        fields=[text] if text else [],
        source=dict(sha256=PIN, byte_offset=17, byte_length=30, encoding="utf-8"),
    )
    row = source(native, 3)
    row["source_locator_json"] = json.dumps({**json.loads(row["source_locator_json"]), **native["source"]})
    result, evidence = map_filing_physical_text(row, SELECTION)
    assert result["text_fragments"][0]["text"] == text
    assert result["declared_format_version"] is None and result["filing_header_record_id"] is None
    assert result["reporting_committee_id"] is None
    assert not {"currency", "amount_kind", "value_mapping_version", "native_fields"} & result.keys()
    assert json.loads(result["mapping_reason_json"])["filing_syntax"] == "syntax-uninterpreted"
    assert result["filing_key"] is None and result["definition_set_id"] is None
    assert [e["role"] for e in evidence] == ["primary"]
    assert typed_batch([result], FILING_TEXT_SCHEMA).to_pylist() == [result]


def test_physical_filing_text_refuses_unpinned_or_delimited_fields():
    native: dict = dict(
        kind="row", fields=["a", "b"], source=dict(sha256=PIN, byte_offset=17, byte_length=30, encoding="utf-8")
    )
    row = source(native, 3)
    row["source_locator_json"] = json.dumps({**json.loads(row["source_locator_json"]), **native["source"]})
    with pytest.raises(ValueError, match="physical filing line"):
        map_filing_physical_text(row, SELECTION)
    native["fields"] = ["text"]
    native["source"]["byte_offset"] = 99
    row["metadata_json"] = json.dumps(native)
    with pytest.raises(ValueError, match="physical filing line"):
        map_filing_physical_text(row, SELECTION)


def test_report_native_values_resolve_through_evidence_without_duplicate_array():
    mapping = filing_cover_mapping_for("8.5", "F3N")
    layout = load_layout(mapping)
    values = dict.fromkeys(map(str, range(len(layout["fields"]))), "")
    values.update({"0": "F3N", "1": "C00123456", "23": "100.00", "93": "undefined-extra"})
    native = dict(kind="record", record_type="F3N", field_count=94, fields=values, embedded_bodies=[])
    original = source(native, 1)
    before = original["metadata_json"]
    result, evidence = map_filing_cover(
        original, SELECTION, mapping, layout=layout, header_row=source(dict(kind="header", format_version="8.5"), 0)
    )
    assert "native_fields" not in result and "native_fields" not in FILING_REPORT_SCHEMA.names
    assert original["metadata_json"] == before
    witness = next(e for e in evidence if e["role"] == "primary")
    assert witness["source_record_id"] == original["source_record_id"]
    assert witness["collection_id"] == original["collection_id"]
    assert json.loads(original["metadata_json"])["fields"]["93"] == "undefined-extra"
    assert result["reported_measures"][0]["raw_value"] == "100.00"


# FEC-1981885 bytes "PAC\x92s" and FEC-1998705 "\x93Agreement\x94": latin-1 reads C1, cp1252 the punctuation.
@pytest.mark.parametrize("body", [b"AFC PAC\x92s bank account", b"this \x93Agreement\x94"])
def test_release_refuses_c1_controls_in_mapped_text(body):
    """Mapped text read as latin-1 carries C1 controls and refuses at the subject boundary; cp1252 passes."""
    from spicy_regs.transforms.fec_native_subjects import prepare_subject

    row = {name: None for name in FILING_TEXT_SCHEMA.names}
    latin, _ = narrative(text_record(), read_body=lambda _: body.decode("latin-1"))
    row.update(latin)
    with pytest.raises(ValueError, match=r"C1 control U\+009[234]"):
        prepare_subject("fec_filing_text_observations", row)
    readable, _ = narrative(text_record(), read_body=lambda _: body.decode("cp1252"))
    row.update(readable)
    subject, _ = prepare_subject("fec_filing_text_observations", row)
    assert subject["text_fragments"][0]["text"] == body.decode("cp1252")


def test_output_scan_refuses_c1_in_any_table_at_any_depth(tmp_path):
    """The once-per-build scan reads every text column, nested or partitioned, and names each table with a match."""
    import pyarrow.parquet as pq
    from spicy_regs.transforms.fec_native_subjects import refuse_c1_controls

    nested = pa.list_(pa.struct([("name", pa.string()), ("value", pa.string())]))
    schema = pa.schema([("record_id", pa.string()), ("amount", pa.int64()), ("attributes", nested)])

    def table(path, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        return path

    # Neighbours of the C1 range (U+007F, U+00A0, the cp1252 reading U+2019) stay admitted.
    clean = [dict(record_id="r1", amount=1, attributes=[dict(name="memo", value="PAC’s \x7f\xa0")])]
    planted = [*clean, dict(record_id="r2", amount=2, attributes=[dict(name="memo", value="PAC\x92s")]),
               dict(record_id="r\x93", amount=3, attributes=None)]
    receipts = table(tmp_path / "fec_receipts" / "source_namespace=a" / "part-000000.parquet", clean)
    table(tmp_path / "fec_receipts" / "source_namespace=b" / "part-000000.parquet", planted)
    numbers = tmp_path / "fec_numbers.parquet"
    pq.write_table(pa.table({"amount": [1]}), numbers)
    flat = table(tmp_path / "fec_legal_events.parquet", clean)

    refuse_c1_controls([flat, numbers, receipts.parent])  # clean partition only
    with pytest.raises(ValueError, match=r"C1 controls \(rows by table: \{'fec_receipts': 2\}\)"):
        refuse_c1_controls([flat, numbers, tmp_path / "fec_receipts"])
