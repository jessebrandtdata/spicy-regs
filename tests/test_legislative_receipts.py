"""Receipts bind actual legislative producer writes and incremental reads."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext, read_with_receipts, select_receipts
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import (
    RAW,
    admit_bundle,
    build_with_receipts,
    write_legislative_outputs,
    policy,
    restore_prior,
    split_source_row,
)


def row(dataset, **values):
    return {f["name"]: None for f in field_registry()[dataset]["fields"]} | (
        {"last_modified": "2026-09-18T12:00:00Z"} if dataset in {"budget_volumes", "house_activity_reports",
        "committee_reports", "report_sections", "committee_report_cbo_letters", "hearing_transcripts", "hearing_bill_links"}
        else {}) | values


def retained(directory, dataset, rows, metadata=None):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{dataset}.parquet"
    schema = pa.schema([(f["name"], pa.string()) for f in field_registry()[dataset]["fields"]], metadata=metadata)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def test_receipt_split_keeps_invalid_input_without_subject():
    context = ReceiptContext("g", "a", "test", [{"source_id": "x", "sha256": "a" * 64}])
    source = row("budget_volumes", package_id="x", associated_bills_json="not-json")
    subject, receipt = split_source_row("budget_volumes", source, context)
    assert subject is None and receipt["outcome"] == "refused"
    assert "not-json" in receipt["processing_json"]


def test_roundtrip_preserves_checkpoint_only_rows_and_exact_parquet_metadata(tmp_path):
    source = row(
        "laws",
        congress="119",
        law_type="public",
        number="1",
        law_id="119-public-1",
        uslm_outcome="request_failed",
        uslm_reason="timeout",
        uslm_reader_version="laws-uslm-v3",
        uslm_citable_as_json="[]",
    )
    law = retained(tmp_path / "source", "laws", [source], {b"checkpoints": b'{"rule":"r1","complete":false}'})
    checkpoint = row("committee_report_reads", package_id="CRPT-119hrpt1", outcome="complete", rule_version="rule-1")
    read = retained(tmp_path / "source", "committee_report_reads", [checkpoint])
    manifest = write_legislative_outputs([law, read], tmp_path / "bundle", generation_id="g1")
    assert not (tmp_path / "bundle/committee_report_reads.parquet").exists()
    assert manifest["subjects"]["committee_report_reads"] == []
    # The outcome a reader judges the row by is published; which reader made the attempt stays in the receipt.
    [published] = pq.read_table(tmp_path / "bundle/laws.parquet").to_pylist()
    assert (published["uslm_outcome"], published["uslm_reason"]) == ("request_failed", "timeout")
    assert "uslm_reader_version" not in published
    restored = restore_prior(tmp_path / "bundle", tmp_path / "prior")
    assert pq.read_table(restored["laws"]).to_pylist() == [source]
    assert pq.read_schema(restored["laws"]).metadata == pq.read_schema(law).metadata
    assert pq.read_table(restored["committee_report_reads"]).to_pylist() == [checkpoint]


@pytest.mark.parametrize("bulk", [False, True])
def test_crs_clock_policy_roundtrips_source_fields_and_refuses_earlier_priors(tmp_path, bulk):
    from dataclasses import replace

    from spicy_regs.etl_receipts import decode_exact_json, write_dataset
    from spicy_regs.legislative_receipts import input_policy, mapped_record

    dataset = "bill_publisher_summaries"
    current = policy(dataset)
    original = row(
        dataset, bill_id="113-hr-4200", summary_version_code="00", action_date="2014-03-11",
        action_desc="Introduced in House", update_date="2014-03-11T04:00:00Z",
        last_summary_update_date="2014-09-08T21:32:48Z", source_schema_version="1.0.0",
        summary_html="<p>Original summary.</p>", summary_chars="24",
    )
    initial = retained(tmp_path / "initial-input", dataset, [original])
    write_legislative_outputs([initial], tmp_path / "initial", generation_id="initial", bulk=bulk)
    initial_receipts = tmp_path / "initial/etl_receipts.parquet"
    before = initial_receipts.read_bytes()
    revised = {**original, "update_date": "2014-09-10T12:00:00Z",
               "last_summary_update_date": "2014-09-10T12:00:00Z",
               "summary_html": "<p>Revised summary.</p>", "summary_chars": "23"}
    new_input = retained(tmp_path / "new-input", dataset, [original, revised])
    write_legislative_outputs([new_input], tmp_path / "new", generation_id="new",
                              prior_receipts=[initial_receipts], bulk=bulk)
    published = pq.read_table(tmp_path / f"new/{dataset}.parquet")
    assert published.schema.equals(current.subject_schema)
    assert published.num_rows == 2
    assert [(r["update_date"], r["last_summary_update_date"], r["source_schema_version"])
            for r in published.to_pylist()] == [
                (r["update_date"], r["last_summary_update_date"], r["source_schema_version"])
                for r in (original, revised)]
    restored = restore_prior(tmp_path / "new", tmp_path / "new-restored", bulk=bulk)
    assert pq.read_table(restored[dataset]).to_pylist() == [original, revised]
    receipts = pq.read_table(tmp_path / "new/etl_receipts.parquet").to_pylist()
    accepted = [r for r in receipts if r["dataset"] == dataset and r["outcome"] == "accepted"]
    assert all(r["policy_version"] == "legislative-documents/3" for r in accepted)
    assert {decode_exact_json(r["processing_json"])[RAW]["last_summary_update_date"]
            for r in accepted} == {original["last_summary_update_date"], revised["last_summary_update_date"]}
    write_legislative_outputs([new_input], tmp_path / "repeated", generation_id="repeated",
                              prior_receipts=[tmp_path / "new/etl_receipts.parquet"], bulk=bulk)
    assert pq.read_table(tmp_path / "repeated/etl_receipts.parquet").to_pylist() == receipts
    assert initial_receipts.read_bytes() == before

    # Earlier processing omitted the legacy clock. Neither NULL-filling nor
    # an invented receipt predecessor can qualify a correction from originals.
    old_schema = current.subject_schema
    for name in ("last_summary_update_date", "source_schema_version"):
        old_schema = old_schema.remove(old_schema.get_field_index(name))
    earlier = replace(current, subject_schema=old_schema, policy_version="legislative-documents/2")
    old_raw = {k: v for k, v in original.items() if k in old_schema.names}
    old_native = {k: v for k, v in mapped_record(dataset, original).items() if k in old_schema.names}
    subject, old_receipts = write_dataset(
        [(old_native | {RAW: old_raw}, ReceiptContext("old", "old/0", "old-reader",
            [{"source_id": "retained-old-reader-fixture", "sha256": "a" * 64}]))],
        tmp_path / "old", earlier,
    )
    assert subject is not None
    old_bytes = old_receipts.read_bytes()
    # /2 is identified, so a run can see it, but it is replay-required: it is never carried (ss-aj7t).
    from spicy_regs.policy_migrations import replay_reason

    selected = input_policy(dataset, [subject])
    assert selected.policy_version == "legislative-documents/2" and replay_reason(selected)
    with pytest.raises(ValueError, match="incompatible clock policy"):
        write_legislative_outputs([new_input], tmp_path / "incompatible", generation_id="incompatible",
                                  prior_receipts=[old_receipts], bulk=bulk)
    assert not (tmp_path / "incompatible").exists()
    # A replay rebuilt every row from originals: the /2 receipts are left behind and no predecessor is invented.
    write_legislative_outputs([new_input], tmp_path / "replayed", generation_id="replayed",
                              prior_receipts=[old_receipts], replayed={dataset}, bulk=bulk)
    replayed = [r for r in pq.read_table(tmp_path / "replayed/etl_receipts.parquet").to_pylist()
                if r["dataset"] == dataset]
    assert {r["policy_version"] for r in replayed} == {"legislative-documents/3"} and len(replayed) == 2
    assert all("prior_receipt" not in r["diagnostic_json"] for r in replayed)
    assert old_receipts.read_bytes() == old_bytes


def test_summary_clock_consumer_preserves_unknown_and_literal_source_states(tmp_path):
    from pathlib import Path
    import duckdb

    dataset = "bill_publisher_summaries"
    item_date = "2014-03-11T04:00:00Z"
    states = [("1.0.0", None, item_date, "absent"), ("1.0.0", "", item_date, "empty"),
              ("1.0.0", "not-a-date", item_date, "malformed"),
              ("1.0.0", "2014-09-08T21:32:48Z", item_date, "valid"),
              ("3.0.0", None, item_date, "valid"), ("3.0.0", None, "", "empty"),
              ("3.0.0", None, "not-a-date", "malformed"),
              ("4.0.0", "2014-09-08T21:32:48Z", item_date, "unknown_schema")]
    rows = [row(dataset, bill_id=f"113-hr-{index + 1}", summary_version_code="00", action_date="2014-03-11",
                update_date=update, last_summary_update_date=literal,
                source_schema_version=schema, summary_html="retained source text", summary_chars="20")
            for index, (schema, literal, update, _) in enumerate(states)]
    source = retained(tmp_path / "input", dataset, rows)
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="states")
    sql = (Path(__file__).parents[1] / "docs/bill-summary-clocks.md").read_text().split("```sql\n")[1].split("```")[0]
    with duckdb.connect() as con:
        con.read_parquet(tmp_path / f"bundle/{dataset}.parquet").create_view(dataset)
        results = con.execute(sql).fetchall()
        columns = [column[0] for column in con.description]
    decoded = [dict(zip(columns, result, strict=True)) for result in results]
    assert [(r["source_schema_version"], r["last_summary_update_date"], r["update_date"], r["summary_update_status"])
            for r in decoded] == [state for state in states if state[2] != ""]
    from spicy_regs.etl_receipts import decode_exact_json
    refused = [r for r in pq.read_table(tmp_path / "bundle/etl_receipts.parquet").to_pylist()
               if r["dataset"] == dataset and r["outcome"] == "refused"]
    assert len(refused) == 1
    assert decode_exact_json(refused[0]["processing_json"])[RAW]["update_date"] == ""
    assert decoded[3]["summary_update_field"] == "lastSummaryUpdateDate"
    assert decoded[4]["summary_update_field"] == "updateDate"
    assert all(r["summary_update_instant"] is None for r in decoded[5:])


@pytest.mark.parametrize(
    "dataset,source,published",
    [
        (
            "laws",
            dict(congress="119", law_type="private", number="1", law_id="119-private-1", law_text_outcome="parsed",
                 uslm_outcome="captured_partial", uslm_reason="statutes_citation_not_stated"),
            dict(uslm_outcome="captured_partial", uslm_reason="statutes_citation_not_stated",
                 law_text_outcome="parsed"),
        ),
        (
            "document_citations",
            dict(document_kind="budget_volume", document_key="BUDGET-2027", cite_kind="bill_number", target_key="hr-1",
                 span_start="4", text_sha256="sha256:" + "c" * 64, target_resolved="false",
                 target_rule="bill_number:unstated", rule_version="003"),
            dict(target_resolved=False, target_rule="bill_number:unstated"),
        ),
        (
            "section_diff_items",
            dict(bill_id="119-hr-1", from_version_code="ih", from_source="govinfo", to_version_code="enr",
                 to_source="govinfo", seq="0", text_diff_json='["a"]', text_diff_truncated="true", similarity="0.5"),
            dict(text_diff_truncated=True),
        ),
    ],
)
def test_returned_columns_are_published_and_the_receipt_keeps_the_whole_original(tmp_path, dataset, source, published):
    """Owner decision, 2026-10-05: these fields are subject columns again; the receipt's original record is unchanged."""
    source = row(dataset, **source)
    assert published.keys() <= source.keys()
    written = retained(tmp_path / "source", dataset, [source])
    write_legislative_outputs([written], tmp_path / "bundle", generation_id="g1")
    subject = tmp_path / f"bundle/{dataset}.parquet"
    schema = pq.read_schema(subject)
    assert schema.equals(policy(dataset).subject_schema)
    assert {name: schema.field(name).type for name in published} == {
        name: pa.bool_() if isinstance(value, bool) else pa.string() for name, value in published.items()
    }
    [row_published] = pq.read_table(subject).to_pylist()
    assert {name: row_published[name] for name in published} == published
    receipts = select_receipts(tmp_path / "bundle/etl_receipts.parquet", tmp_path / "selected.parquet", dataset=dataset)
    [joined] = read_with_receipts([subject], [receipts], policy(dataset), generation_id="g1")
    # The receipt restores every source field in its original spelling, the returned ones included.
    assert joined[RAW] == source
    assert {name: joined[name] for name in published} == published
    restored = restore_prior(tmp_path / "bundle", tmp_path / "prior")
    assert pq.read_table(restored[dataset]).to_pylist() == [source]


def test_wrong_generation_and_missing_receipt_refuse(tmp_path):
    source = retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0")])
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g1")
    receipts = select_receipts(
        tmp_path / "bundle/etl_receipts.parquet", tmp_path / "selected.parquet", dataset="table3_records"
    )
    with pytest.raises(ValueError, match="generation"):
        list(
            read_with_receipts(
                [tmp_path / "bundle/table3_records.parquet"], [receipts], policy("table3_records"), generation_id=""
            )
        )
    pq.write_table(pq.read_table(receipts).slice(0, 0), receipts)
    with pytest.raises(ValueError, match="Missing"):
        list(
            read_with_receipts(
                [tmp_path / "bundle/table3_records.parquet"], [receipts], policy("table3_records"), generation_id="g1"
            )
        )


def test_invalid_input_bundle_cannot_become_a_qualified_prior(tmp_path):
    source = retained(
        tmp_path / "source", "budget_volumes", [row("budget_volumes", package_id="x", associated_bills_json="{}")]
    )
    manifest = write_legislative_outputs([source], tmp_path / "bundle", generation_id="g1")
    assert manifest["refused_rows"] == {"budget_volumes": 1}
    assert pq.read_table(tmp_path / "bundle/budget_volumes.parquet").num_rows == 0
    with pytest.raises(ValueError, match="conversion refusals"):
        restore_prior(tmp_path / "bundle", tmp_path / "prior")


def test_changed_subject_refuses_before_incremental_reader(tmp_path):
    source = retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0")])
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g1")
    subject = tmp_path / "bundle/table3_records.parquet"
    table = pq.read_table(subject)
    changed = table.to_pylist()[0] | {"status": "changed"}
    pq.write_table(pa.Table.from_pylist([changed], schema=table.schema), subject)
    with pytest.raises(ValueError, match="receipt"):
        restore_prior(tmp_path / "bundle", tmp_path / "prior")


def test_actual_laws_builder_reuses_only_verified_receipt_prior(tmp_path, monkeypatch):
    # Existing retained publisher fixtures exercise the actual source-owner
    # shapers, Table III safeguards, merge writer, and USLM retry decision.
    from .test_laws import StubListingReader, StubUslm, StubOlrc, LISTED_119
    from spicy_regs.transforms.build_laws import build_laws

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    first_uslm = StubUslm(unavailable={110, 109, 104})
    first = build_with_receipts(
        build_laws,
        tmp_path / "run1",
        tmp_path / "bundle1",
        generation_id="g1",
        builder_kwargs={"reader": StubListingReader({119: LISTED_119}), "uslm": first_uslm, "olrc": StubOlrc()},
    )
    second_uslm = StubUslm(unavailable={110, 109, 104})
    second = build_with_receipts(
        build_laws,
        tmp_path / "run2",
        tmp_path / "bundle2",
        generation_id="g2",
        prior_bundle=tmp_path / "bundle1",
        builder_kwargs={"reader": StubListingReader({119: LISTED_119}), "uslm": second_uslm, "olrc": StubOlrc()},
    )
    assert (
        set(first["datasets"])
        == set(second["datasets"])
        == {"laws", "law_code_sections", "table3_records", "law_sections"}
    )
    assert all(n == 0 for n in second["refused_rows"].values())
    assert 1 in [s.number for s in first_uslm.selections]
    assert 1 not in [s.number for s in second_uslm.selections]


def test_actual_committee_producer_keeps_empty_read_checkpoints(tmp_path, monkeypatch):
    from .test_committee_reports import StubDiscovery, StubBodyAcquirer, StubHearings
    from spicy_regs.transforms.build_committee_reports import build_committee_reports

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    kwargs = {"reader": StubDiscovery(), "acquirer": StubBodyAcquirer(), "hearings": StubHearings()}
    first = build_with_receipts(
        build_committee_reports, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    second = build_with_receipts(
        build_committee_reports,
        tmp_path / "run2",
        tmp_path / "bundle2",
        generation_id="g2",
        prior_bundle=tmp_path / "bundle1",
        builder_kwargs=kwargs,
    )
    assert set(first["datasets"]) == set(second["datasets"])
    assert "committee_report_reads" in second["datasets"]
    assert not (tmp_path / "bundle2/committee_report_reads.parquet").exists()
    assert not any(second["refused_rows"].values())
    admit_bundle(tmp_path / "bundle2", tmp_path / "generation")


def test_actual_native_reference_reader_emits_main_scopes_and_preserves_originals(tmp_path):
    from .test_native_legal_references import FIXTURES
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.transforms.native_legal_references import build_native_legal_references

    def builder(directory, *, download_prior):
        return build_native_legal_references(
            FIXTURES / "manifest.json",
            directory,
            evidence=CaptureEvidence(directory, "native-legal-references"),
            download_prior=download_prior,
        )

    manifest = build_with_receipts(builder, tmp_path / "source", tmp_path / "bundle", generation_id="g")
    assert not any(manifest["refused_rows"].values())
    assert pq.read_metadata(tmp_path / "bundle/native_legal_references.parquet").num_rows == 33
    assert manifest["subjects"]["native_legal_reference_reads"] == ["native_legal_reference_reads.parquet"]
    assert sorted(pq.read_table(tmp_path / "bundle/native_legal_reference_reads.parquet")["occurrence_count"].to_pylist()) == [2, 31]
    restored = restore_prior(tmp_path / "bundle", tmp_path / "prior")
    assert pq.read_metadata(restored["native_legal_reference_reads"]).num_rows == 2


def test_actual_senate_producer_preserves_incremental_read_limits(tmp_path):
    from .test_senate_expenditures import _Reader, _listing, _granule, PACKAGE, PART_ONE, _Acquirer, _Extractor
    from spicy_regs.transforms.build_senate_expenditures import build_senate_expenditures

    kwargs = {
        "reader": _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]}),
        "acquirer": _Acquirer(),
        "extractor": _Extractor(page_count=3),
    }
    first = build_with_receipts(
        build_senate_expenditures, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    second = build_with_receipts(
        build_senate_expenditures,
        tmp_path / "run2",
        tmp_path / "bundle2",
        generation_id="g2",
        prior_bundle=tmp_path / "bundle1",
        builder_kwargs=kwargs,
    )
    assert not any(first["refused_rows"].values()) and not any(second["refused_rows"].values())
    assert pq.read_table(tmp_path / "bundle1/senate_expenditures.parquet").equals(
        pq.read_table(tmp_path / "bundle2/senate_expenditures.parquet")
    )


def test_actual_print_producer_and_native_citations(tmp_path):
    from .test_print_citations import _Reader, _listing, _Acquirer, _package, _NoRosters, CRPT_ID, REPORT_PAGES
    from spicy_regs.transforms.build_print_citations import build_print_citations

    kwargs = {
        "reader": _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT of the COMMITTEE")]}),
        "acquirer": _Acquirer({CRPT_ID: _package(CRPT_ID, pages=REPORT_PAGES)}),
        "rosters": _NoRosters(),
    }
    manifest = build_with_receipts(
        build_print_citations, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    assert not any(manifest["refused_rows"].values())
    citations = pq.read_table(tmp_path / "bundle1/document_citations.parquet")
    assert citations.num_rows > 0
    # Every finding the rules wrote says whether its key was settled and how it was reached.
    assert citations.schema.field("target_resolved").type == pa.bool_()
    assert set(citations["target_resolved"].to_pylist()) <= {True, False}
    assert all(isinstance(rule, str) and rule for rule in citations["target_rule"].to_pylist())
    assert pq.read_metadata(tmp_path / "bundle1/bill_committee_actions.parquet").num_rows > 0


def test_actual_bill_producer_preserves_partitioned_sections_and_names_other_owners(tmp_path, monkeypatch):
    from .test_bill_family import StubBulkAcquirer, StubBodyAcquirer
    from spicy_regs.transforms.build_bill_family import build_bill_family

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    kwargs = {"bulk_acquirer": StubBulkAcquirer(), "body_acquirer": StubBodyAcquirer()}
    first = build_with_receipts(
        build_bill_family, tmp_path / "run1", tmp_path / "bundle1", generation_id="g1", builder_kwargs=kwargs
    )
    assert first["unowned_outputs"]
    assert first["subjects"]["bill_sections"]
    assert not any(first["refused_rows"].values())
    # The producer's own comparison rows: whether a word diff was cut is published, and unstated where none was made.
    items = pq.read_table(tmp_path / "bundle1/section_diff_items.parquet")
    assert items.num_rows and items.schema.field("text_diff_truncated").type == pa.bool_()
    assert set(items["text_diff_truncated"].to_pylist()) == {None}
    restored = restore_prior(tmp_path / "bundle1", tmp_path / "prior")
    assert restored["bill_sections"].is_dir()
    admit_bundle(tmp_path / "bundle1", tmp_path / "generation")
    with pytest.raises(ValueError, match="integrated family prior"):
        build_with_receipts(
            build_bill_family,
            tmp_path / "run2",
            tmp_path / "bundle2",
            generation_id="g2",
            prior_bundle=tmp_path / "bundle1",
            builder_kwargs=kwargs,
        )


def test_bulk_restore_admits_once_and_matches_row_restore_for_multiple_datasets(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from spicy_regs import etl_bulk

    sources = [
        retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0", status="escape \x1b")]),
        retained(tmp_path / "source", "bill_versions", [row("bill_versions", bill_id="119-hr-1", version_code="ih", source="govinfo")]),
    ]
    write_legislative_outputs(sources, tmp_path / "bundle", generation_id="g")
    expected = restore_prior(tmp_path / "bundle", tmp_path / "row", bulk=False)
    calls = []
    original = etl_bulk._validated_bundle

    @contextmanager
    def admitted(*args, **kwargs):
        calls.append(1)
        with original(*args, **kwargs) as con:
            yield con

    monkeypatch.setattr(etl_bulk, "_validated_bundle", admitted)
    actual = restore_prior(tmp_path / "bundle", tmp_path / "bulk")
    assert calls == [1]
    for dataset in actual:
        assert pq.read_table(actual[dataset]).equals(pq.read_table(expected[dataset]), check_metadata=True)


def test_wide_bulk_restore_preserves_source_order_values_and_metadata_across_batches(tmp_path):
    """The bill refresh must yield bounded source batches before parsing and sorting the next one."""
    from spicy_regs import etl_bulk

    text = 'Peña "quoted" \\ path \x1b\n' * 50
    versions = [row("bill_versions", bill_id=f"119-hr-{i + 1}", version_code="ih", source="govinfo",
                    label=f"{i}: {text}", requested_url=None if i % 7 == 0 else f"https://example.test/{i}",
                    resolved_url="" if i % 3 == 0 else f"https://example.test/held/{i}") for i in range(4203)]
    reads = [row("committee_report_reads", package_id=f"CRPT-119hrpt{i}", outcome="complete",
                 rule_version="reader-1", last_modified=None if i % 2 else "") for i in range(2103)]
    sources = [
        retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0")]),
        retained(tmp_path / "source", "bill_versions", versions, {b"retained-source": b"wide-version-window"}),
        retained(tmp_path / "source", "committee_report_reads", reads, {b"retained-source": b"read-window"}),
    ]
    write_legislative_outputs(sources, tmp_path / "bundle", generation_id="g")
    # Physical row groups need not coincide with decoding batches or global subject ordinals.
    path = tmp_path / "bundle/bill_versions.parquet"
    pq.write_table(pq.read_table(path), path, row_group_size=257)
    with etl_bulk.bulk_limits(threads=4, memory_limit="256MB"):
        actual = restore_prior(tmp_path / "bundle", tmp_path / "restored")
    for source in sources:
        assert pq.read_table(actual[source.stem]).equals(pq.read_table(source), check_metadata=True)


@pytest.mark.parametrize("bulk", [False, True])
def test_retained_source_still_must_reproduce_the_admitted_subject(tmp_path, bulk):
    from spicy_regs.etl_receipts import _digest, decode_exact_json, exact_json

    source = retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0", status="original")])
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g")
    path = tmp_path / "bundle/etl_receipts.parquet"
    table = pq.read_table(path)
    receipts = table.to_pylist()
    receipt = next(receipt for receipt in receipts if receipt["outcome"] == "accepted")
    processing = decode_exact_json(receipt["processing_json"])
    processing[RAW]["status"] = "changed input, unchanged subject"
    receipt["processing_json"] = exact_json(processing)
    receipt["receipt_id"] = _digest({key:value for key, value in receipt.items() if key != "receipt_id"})
    pq.write_table(pa.Table.from_pylist(receipts, schema=table.schema), path)
    with pytest.raises(ValueError, match="disagrees with its native subject"):
        restore_prior(tmp_path / "bundle", tmp_path / "prior", bulk=bulk)


@pytest.mark.parametrize("empty", [False, True])
def test_bulk_partition_restore_matches_row_paths_metadata_and_emptiness(tmp_path, empty):
    source = tmp_path / "source/bill_sections"
    source.mkdir(parents=True)
    if not empty:
        for index in range(2):
            retained(source / f"congress={118 + index}", "bill_sections", [row("bill_sections",
                bill_id=f"{118 + index}-hr-1", version_code="ih", source="govinfo", seq="0", text="a \x1b b")],
                {b"partition-check": str(index).encode()})
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g")
    expected = restore_prior(tmp_path / "bundle", tmp_path / "row", bulk=False)["bill_sections"]
    actual = restore_prior(tmp_path / "bundle", tmp_path / "bulk")["bill_sections"]
    files = sorted(path.relative_to(expected) for path in expected.rglob("*.parquet"))
    assert files == sorted(path.relative_to(actual) for path in actual.rglob("*.parquet"))
    for member in files:
        assert pq.ParquetFile(actual/member).read().equals(pq.ParquetFile(expected/member).read(), check_metadata=True)


@pytest.mark.parametrize("validate", [True, False])
def test_writer_admits_only_finished_bundle_when_requested(tmp_path, monkeypatch, validate):
    from spicy_regs import etl_receipts, legislative_receipts

    source = retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0")])
    calls = []
    original = etl_receipts.validate_receipt_bundle
    def admit(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)
    monkeypatch.setattr(etl_receipts, "validate_receipt_bundle", admit)
    monkeypatch.setattr(legislative_receipts, "validate_receipt_bundle", admit)
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g", validate=validate)
    assert len(calls) == int(validate)


def test_carry_resolves_refusals_across_partitions_and_preserves_unmatched(tmp_path):
    from spicy_regs.etl_receipts import decode_exact_json

    first = tmp_path / "first-source" / "budget_volumes"
    bad_x = row("budget_volumes", package_id="X", associated_bills_json="bad")
    bad_y = row("budget_volumes", package_id="Y", associated_bills_json="bad")
    unchanged = row("budget_volumes", package_id="Z")
    retained(first / "a", "budget_volumes", [bad_x, unchanged])
    retained(first / "b", "budget_volumes", [bad_y])
    write_legislative_outputs([first], tmp_path / "one", generation_id="one")
    old = pq.read_table(tmp_path / "one/etl_receipts.parquet").to_pylist()
    before = [r for r in old if r["dataset"] == "budget_volumes"]
    second = tmp_path / "second-source" / "budget_volumes"
    retained(second / "a", "budget_volumes", [unchanged])
    retained(second / "z", "budget_volumes", [bad_x | {"associated_bills_json": "[]"}])
    manifest = write_legislative_outputs([second], tmp_path / "two", generation_id="two",
                                          prior_receipts=[tmp_path / "one/etl_receipts.parquet"])
    after = [r for r in pq.read_table(tmp_path / "two/etl_receipts.parquet").to_pylist()
             if r["dataset"] == "budget_volumes"]
    accepted_before = next(r for r in before if r["outcome"] == "accepted")
    assert accepted_before in after
    unresolved = next(r for r in before if decode_exact_json(r["processing_json"])[RAW]["package_id"] == "Y")
    assert [r for r in after if r["outcome"] == "refused"] == [unresolved]
    assert manifest["refused_rows"] == {"budget_volumes": 1}
    assert manifest["fresh_refused_rows"] == {"budget_volumes": 0}
    restored = restore_prior(tmp_path / "two", tmp_path / "restored")
    assert sum(pq.read_metadata(p).num_rows for p in restored["budget_volumes"].rglob("*.parquet")) == 2
    third = retained(tmp_path / "third-source", "budget_volumes", [unchanged, bad_x | {"associated_bills_json": "[]"},
                                                                  bad_y | {"associated_bills_json": "[]"}])
    resolved = write_legislative_outputs([third], tmp_path / "three", generation_id="three",
                                         prior_receipts=[tmp_path / "two/etl_receipts.parquet"])
    assert resolved["refused_rows"] == resolved["fresh_refused_rows"] == {"budget_volumes": 0}
    restore_prior(tmp_path / "three", tmp_path / "restored-three")


def test_processing_change_with_unchanged_subject_receives_fresh_evidence(tmp_path):
    from spicy_regs.etl_receipts import decode_exact_json

    initial = row("laws", congress="119", law_type="public", number="1", law_id="119-public-1",
                  uslm_reader_version="reader-1")
    source = retained(tmp_path / "source1", "laws", [initial])
    write_legislative_outputs([source], tmp_path / "one", generation_id="one")
    revised = retained(tmp_path / "source2", "laws", [initial | {"uslm_reader_version": "reader-2"}])
    write_legislative_outputs([revised], tmp_path / "two", generation_id="two",
                              prior_receipts=[tmp_path / "one/etl_receipts.parquet"])
    first, second = [next(r for r in pq.read_table(tmp_path / name / "etl_receipts.parquet").to_pylist()
                         if r["dataset"] == "laws" and r["outcome"] == "accepted") for name in ("one", "two")]
    assert first["subject_version"] == second["subject_version"]
    assert first["processing_json"] != second["processing_json"]
    assert second["generation_id"] == "two"
    assert decode_exact_json(second["diagnostic_json"])["prior_receipt"]["receipt_id"] == first["receipt_id"]


@pytest.mark.parametrize("force_fallback", [False, True])
def test_reference_restore_loads_and_admits_receipts_once(tmp_path, monkeypatch, force_fallback):
    from contextlib import contextmanager
    from spicy_regs import etl_bulk, etl_receipts

    sources = [retained(tmp_path / "source", "table3_records", [row("table3_records", act_key="119-1", seq="0")]),
               retained(tmp_path / "source", "laws", [row("laws", congress="119", law_type="public", number="1")])]
    write_legislative_outputs(sources, tmp_path / "bundle", generation_id="g")
    calls = []
    original = etl_receipts._load_receipts
    def load(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)
    monkeypatch.setattr(etl_receipts, "_load_receipts", load)
    if force_fallback:
        @contextmanager
        def unproven(*args, **kwargs):
            raise etl_bulk.NotBulkEligible("forced reference admission")
            yield
        monkeypatch.setattr(etl_bulk, "_validated_bundle", unproven)
    restored = restore_prior(tmp_path / "bundle", tmp_path / "prior", bulk=force_fallback)
    assert len(calls) == 1
    for source in sources:
        assert pq.read_table(source).equals(pq.read_table(restored[source.stem]), check_metadata=True)


#: The seven per-letter columns committee_reports carried under legislative-documents/1 (ss-uep5 moved them).
# The five columns that moved; estimate_heading and its rule stay on the part row with a part-level meaning.
LETTER_COLUMNS = ("letter_span_start", "letter_span_end", "letter_text_sha256", "letter_end_rule", "letter_signatory")


def _committee_reports_v1_registry():
    """The legislative-documents/1 committee_reports spec: today's fields with the five letter columns where they were.

    The letter columns kept their names, decisions and types when they moved, so the letters table's own spec is what
    they were; the recorded /1 policy pins that this rebuild is the shape the earlier writer published.
    """
    registry = field_registry()
    letters = {f["name"]: f for f in registry["committee_report_cbo_letters"]["fields"]}
    fields = []
    for field in registry["committee_reports"]["fields"]:
        if field["name"] == "cbo_letter_count":
            continue
        fields.append(field | {"nullable": True} if field["name"] == "last_modified" else field)
        if field["name"] == "estimate_heading_rule":
            fields.extend(letters[name] for name in LETTER_COLUMNS)
    spec = {k: v for k, v in registry["committee_reports"].items() if k != "retired_fields"} | {"fields": fields}
    return registry | {"committee_reports": spec}


def test_greenfield_report_build_refuses_prior_policy_but_preserves_original_source(tmp_path, monkeypatch):
    """A fresh dated build keeps source evidence and refuses automatic old-policy migration."""

    from spicy_regs import legislative_documents, legislative_receipts
    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.policy_migrations import earlier_policy

    current = policy("committee_reports")
    assert current.policy_version == "legislative-documents/3"
    old_registry = _committee_reports_v1_registry()
    with monkeypatch.context() as patch:
        patch.setattr(legislative_documents, "field_registry", lambda: old_registry)
        patch.setattr(legislative_receipts, "field_registry", lambda: old_registry)
        # The schema is cached per dataset; read it uncached while the earlier registry is in place.
        uncached = legislative_documents.subject_schema.__wrapped__
        for module in (legislative_documents, legislative_receipts):
            patch.setattr(module, "subject_schema", uncached)
        patch.setattr(legislative_receipts, "policy", lambda name, _p=policy: (
            DatasetPolicy("committee_reports", legislative_documents.subject_schema(name), ("package_id", "part_id"),
                          current.receipt_fields, policy_version="legislative-documents/1")
            if name == "committee_reports" else _p(name)))
        recorded = earlier_policy("committee_reports", "legislative-documents/1")
        assert legislative_documents.subject_schema("committee_reports").equals(recorded.subject_schema)
        source = {f["name"]: None for f in old_registry["committee_reports"]["fields"]} | dict(
            package_id="CRPT-105hrpt149", part_id="CRPT-105hrpt149", last_modified="2026-09-18T12:00:00Z", report_states_estimate="true",
            estimate_heading="COMMITTEE COST ESTIMATE", estimate_heading_rule="committee_cost_estimate",
            letter_span_start="5402", letter_span_end="16376", letter_text_sha256="sha256:" + "a" * 64,
            letter_end_rule="named_director+estimate_approval", letter_signatory="JUNE E. O'NEILL")
        directory = tmp_path / "source"
        directory.mkdir()
        written = directory / "committee_reports.parquet"
        pq.write_table(pa.Table.from_pylist([source], schema=pa.schema(
            [(name, pa.string()) for name in source])), written)
        write_legislative_outputs([written], tmp_path / "v1", generation_id="g1")
    v1_subject = pq.read_table(tmp_path / "v1/committee_reports.parquet")
    assert v1_subject.schema.equals(recorded.subject_schema)
    assert v1_subject.to_pylist()[0]["letter_span_start"] == 5402
    # The new dated build deliberately has no automatic old-policy migration.
    with pytest.raises(ValueError, match="Subject schema differs from policy"):
        restore_prior(tmp_path / "v1", tmp_path / "prior")
    # Its immutable original can be used for an explicit fresh build.
    assert pq.read_table(written).to_pylist() == [source]
    write_legislative_outputs([written], tmp_path / "v2", generation_id="g2")
    [published] = pq.read_table(tmp_path / "v2/committee_reports.parquet").to_pylist()
    assert not set(LETTER_COLUMNS) & published.keys()
    assert published["estimate_heading"] == "COMMITTEE COST ESTIMATE"
    assert published["cbo_letter_count"] is None and published["report_states_estimate"] is True
    receipts = select_receipts(tmp_path / "v2/etl_receipts.parquet", tmp_path / "selected.parquet",
                               dataset="committee_reports")
    [joined] = read_with_receipts([tmp_path / "v2/committee_reports.parquet"], [receipts], current, generation_id="g2")
    assert joined[RAW] == source


def test_dated_print_refresh_carries_original_revision_receipt_and_witness(tmp_path):
    from tests.test_print_citations import _Reader, _Acquirer, _NoRosters, _package, _listing, BUDGET_ID, BUDGET_PAGES
    from spicy_regs.transforms.build_print_citations import build_print_citations

    old, new = "2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z"
    reader = _Reader({"BUDGET": [_listing(BUDGET_ID, "Mid-Session Review", old)]})
    acquirer = _Acquirer({BUDGET_ID: _package(BUDGET_ID, pages=BUDGET_PAGES, last_modified=old)})
    kwargs = {"reader": reader, "acquirer": acquirer, "rosters": _NoRosters()}
    build_with_receipts(build_print_citations, tmp_path / "run1", tmp_path / "bundle1",
                        generation_id="g1", builder_kwargs=kwargs)
    original = [row for row in pq.read_table(tmp_path / "bundle1/etl_receipts.parquet").to_pylist()
                if row["dataset"] == "budget_volumes" and row["outcome"] == "accepted"]
    assert len(original) == 1
    reader = _Reader({"BUDGET": [_listing(BUDGET_ID, "Mid-Session Review", new)]})
    acquirer.packages[BUDGET_ID] = _package(BUDGET_ID, pages=BUDGET_PAGES, last_modified=new)
    build_with_receipts(build_print_citations, tmp_path / "run2", tmp_path / "bundle2",
                        generation_id="g2", prior_bundle=tmp_path / "bundle1",
                        builder_kwargs=kwargs | {"reader": reader})
    receipts = pq.read_table(tmp_path / "bundle2/etl_receipts.parquet").to_pylist()
    assert original[0] in receipts
    selected = select_receipts(tmp_path / "bundle2/etl_receipts.parquet", tmp_path / "selected.parquet",
                               dataset="budget_volumes")
    restored = list(read_with_receipts([tmp_path / "bundle2/budget_volumes.parquet"], [selected],
                                      policy("budget_volumes"), generation_id="g2"))
    assert {row[RAW]["last_modified"] for row in restored} == {old, new}
