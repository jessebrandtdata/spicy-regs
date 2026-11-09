"""Exact House EC identity, complete granule scope and retained-source resume."""
import hashlib
import json
import shutil
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.govinfo.bodies import parse_granule_identity
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.congress_receipts import restore_processing_input, write_congress_dataset
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.house_record_enrichment import enrich_house_record

PACKAGE = "CREC-2025-01-06"
SENTENCE = "A letter from the Secretary of State, transmitting a report; to the Committee on Foreign Affairs."


def published_fetch(evidence, root, *, changed=False):
    """Serve a sealed evidence artifact the way publish_evidence lays it out on R2.

    Metadata lives under ``source-evidence/<digest>/``; blobs live once under the
    shared ``source-evidence/blobs/sha256/<hex>`` prefix and nowhere else.
    """
    digest = root["artifactDigest"][7:]

    def fetch(url, limit):
        if "/source-evidence/blobs/" in url:
            body = (evidence.artifact_dir / url.split("/source-evidence/", 1)[1]).read_bytes()
            return body + b"changed" if changed else body
        relative = url.split("/source-evidence/" + digest + "/", 1)[1]
        if relative.startswith("blobs/"):
            raise AssertionError("per-artifact blob keys are never published: " + url)
        return (evidence.artifact_dir / relative).read_bytes()
    return fetch


def fixture(tmp_path, *, issue_congress="119", numbers=("1",), ids=("a",), entries=None, failure=None, package=PACKAGE):
    columns = TABLE_CONTRACTS["house_communications"].columns
    rows = [dict.fromkeys(columns) | {"communication_id":"119-ec-" + number, "congress":"119",
            "communication_type":"ec", "number":number, "congressional_record_date":"2025-01-06",
            "update_date":"2025-01-07", "source_route":"congress-gov-detail", "detail_read":"true",
            "abstract":"Original API observation", "report_nature":"Original API report"} for number in numbers]
    output = tmp_path / "house_communications.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(name, pa.string()) for name in columns])), output)
    issue = tmp_path / "issue-source.parquet"
    pq.write_table(pa.Table.from_pylist([{"congress":issue_congress, "issue_date":"2025-01-06", "package_id":package,
                                        "update_date":"2025-01-07"}]), issue)
    pin = {"dataset":"record_issues", "generationId":"selected-issues",
           "subjects":[{"sha256":"sha256:" + "a" * 64}], "receipts":{"sha256":"sha256:" + "b" * 64},
           "processing":{"sha256":"sha256:" + hashlib.sha256(issue.read_bytes()).hexdigest(), "byteSize":issue.stat().st_size}}
    class Reader:
        calls = 0
        def granules(self, url, max_pages):
            self.calls += 1
            if failure == "listing":
                raise ValueError("incomplete enumeration")
            yield SimpleNamespace(records=[{"granuleId":package + "-PgH" + value,
                  "granuleClass":"HOUSE", "title":"EXECUTIVE COMMUNICATIONS, ETC."} for value in ids])
    class Acquirer:
        calls = []
        def acquire_granule(self, package, granule, **kwargs):
            self.calls.append(granule)
            if failure == granule.rsplit("PgH", 1)[-1]:
                raise ValueError("body failed")
            text = (entries or {}).get(granule.rsplit("PgH", 1)[-1], "EC-1. " + SENTENCE)
            raw = ("<html><body><pre>" + text + "</pre></body></html>").encode()
            url = "https://www.govinfo.gov/content/pkg/" + package + "/html/" + granule + ".htm"
            capture = CapturedBodyResponse(url, url, 200, "text/html", "2025-01-07T00:00:00Z", raw)
            return SimpleNamespace(identity=parse_granule_identity(package, granule), format="htm",
                                   body_capture=capture, body=SimpleNamespace(media_type="text/html", byte_size=len(raw)))
    def download(key, destination):
        assert key == "record_issues.parquet"
        shutil.copyfile(issue, destination)
        return True
    evidence = CaptureEvidence(tmp_path, "house-communications")
    reader, acquirer = Reader(), Acquirer()
    acquirer.calls = []
    args = {"download_prior":download, "selected_input":lambda name:pin, "evidence":evidence,
            "reader":reader, "acquirer":acquirer}
    return output, rows, args, pin


def events(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


def test_exact_identity_keeps_api_observations_and_receipt_roundtrip(tmp_path):
    output, before, args, _ = fixture(tmp_path, numbers=("1", "2"))
    enrich_house_record(output, **args)
    rows = pq.read_table(output).to_pylist()
    assert rows[0]["record_package_id"] == PACKAGE
    assert rows[0]["record_granule_id"] == PACKAGE + "-PgHa"
    assert rows[0]["record_entry_text"] == SENTENCE
    assert rows[1]["record_package_id"] is None
    for name in ("abstract", "report_nature", "source_route", "update_date"):
        assert [row[name] for row in rows] == [row[name] for row in before]
    subject, receipts = write_congress_dataset(output, tmp_path / "native", dataset="house_communications", generation_id="new")
    assert subject is not None
    restored = tmp_path / "restored.parquet"
    restore_processing_input((subject,), receipts, restored, dataset="house_communications", generation_id="new")
    assert pq.read_table(restored).to_pylist() == rows
    from spicy_regs.explorer_navigation import declarations, target_keys
    link = next(item for item in declarations() if item["id"] == "house_communication_record")
    assert target_keys(link["targets"][0], None, rows[0]) == [PACKAGE]
    journal = events(args["evidence"])
    matched = next(row for row in journal if row.get("event") == "house-record-result" and row.get("outcome") == "matched")
    assert matched["witnesses"][0]["input"]["generationId"] == "selected-issues"
    assert matched["witnesses"][0]["bodies"][0]["sha256"].startswith("sha256:")


def test_wrong_congress_is_not_inferred_from_date_or_number(tmp_path):
    output, _, args, _ = fixture(tmp_path, issue_congress="118")
    enrich_house_record(output, **args)
    assert args["reader"].calls == 0
    assert pq.read_table(output)["record_package_id"].to_pylist() == [None]


@pytest.mark.parametrize("value,day", [("2025-01-06", "2025-01-06"), ("2025-01-06T04:00:00Z", "2025-01-06"),
    ("2025-01-06T23:30:00-05:00", "2025-01-06"), ("2025-01-06T04:00:00.123+00:00", "2025-01-06")])
def test_scope_calendar_day_validates_date_and_datetime_without_timezone_shift(value, day):
    from spicy_regs.transforms.house_record_enrichment import _calendar_day
    assert _calendar_day(value) == day


@pytest.mark.parametrize("value", ["2025-01-06junk", "2025-01-06T04:00:00Zjunk", "2025-02-29",
    "2025-02-29T04:00:00Z", "2025-01-06T25:00:00Z", "2025-01-06T04:00:00+25:00",
    "2025-01-06T04:00:00+01:99", "20250106", 20250106])
def test_scope_calendar_day_refuses_invalid_full_source_values(value):
    from spicy_regs.transforms.house_record_enrichment import _calendar_day
    with pytest.raises(ValueError):
        _calendar_day(value)


@pytest.mark.parametrize("invalid", ["issue-junk", "api-junk", "package-other-day"])
def test_invalid_or_conflicting_source_dates_cannot_qualify_record_locator(tmp_path, invalid):
    output, _, args, pin = fixture(tmp_path)
    issue = tmp_path / "issue-source.parquet"
    values = pq.read_table(issue).to_pylist()
    if invalid == "issue-junk":
        values.append(values[0] | {"issue_date":"2025-01-06T04:00:00Zjunk"})
    elif invalid == "package-other-day":
        values[0]["issue_date"] = "2025-01-06T04:00:00Z"
        values[0]["package_id"] = "CREC-2025-01-07"
    else:
        rows = pq.read_table(output).to_pylist()
        rows[0]["congressional_record_date"] = "2025-01-06T04:00:00Zjunk"
        pq.write_table(pa.Table.from_pylist(rows, schema=pq.read_schema(output)), output)
    pq.write_table(pa.Table.from_pylist(values), issue)
    pin["processing"] = {"sha256":"sha256:" + hashlib.sha256(issue.read_bytes()).hexdigest(), "byteSize":issue.stat().st_size}
    enrich_house_record(output, **args)
    assert pq.read_table(output)["record_package_id"].to_pylist() == [None]
    journal = events(args["evidence"])
    assert any(row.get("outcome") == "refused" for row in journal)
    if invalid != "api-junk":
        refused = next(row for row in journal if row.get("event") == "house-record-package" and row.get("outcome") == "refused")
        assert refused["issue"] in values and refused["input"] == pin
        assert next(row for row in journal if row.get("event") == "house-record-result")["complete_scope"] is False


@pytest.mark.parametrize("entries", [None, {"a":"EC-1. " + SENTENCE + "\nEC-1. " + SENTENCE},
                                       {"a":"EC-1. " + SENTENCE, "b":"EC-1. " + SENTENCE.replace("a report", "another report")}])
def test_repeated_printed_occurrences_are_ambiguous_without_deduplication(tmp_path, entries):
    output, _, args, _ = fixture(tmp_path, ids=("a", "b") if entries is None or "b" in entries else ("a",), entries=entries)
    enrich_house_record(output, **args)
    assert pq.read_table(output)["record_package_id"].to_pylist() == [None]
    assert next(row for row in events(args["evidence"]) if row.get("event") == "house-record-result")["outcome"] == "ambiguous"


@pytest.mark.parametrize("failure", ["listing", "b"])
def test_partial_scope_never_certifies_a_unique_match(tmp_path, failure):
    output, _, args, _ = fixture(tmp_path, ids=("a", "b"), failure=failure)
    enrich_house_record(output, **args)
    assert pq.read_table(output)["record_package_id"].to_pylist() == [None]
    journal = events(args["evidence"])
    assert any(row.get("event") == "house-record-package" and row.get("outcome") == "failed" for row in journal)
    assert next(row for row in journal if row.get("event") == "house-record-result")["complete_scope"] is False


@pytest.mark.parametrize("package,outcome", [(None, "unread"), ("", "unread"), (123, "refused"),
                                             ("CREC-2025-99-99", "refused")])
def test_issue_without_valid_package_keeps_other_matching_issue_scope_incomplete(tmp_path, package, outcome):
    output, _, args, pin = fixture(tmp_path)
    issue = tmp_path / "issue-source.parquet"
    values = pq.read_table(issue).to_pylist()
    values.append(values[0] | {"package_id":package})
    # A numeric malformed identity needs its own Arrow-typed fixture rather
    # than coercing the valid source package string to a different value.
    if isinstance(package, int):
        values = [values[-1]]
    pq.write_table(pa.Table.from_pylist(values), issue)
    pin["processing"] = {"sha256":"sha256:" + hashlib.sha256(issue.read_bytes()).hexdigest(), "byteSize":issue.stat().st_size}
    enrich_house_record(output, **args)
    assert pq.read_table(output)["record_package_id"].to_pylist() == [None]
    journal = events(args["evidence"])
    missing = next(row for row in journal if row.get("event") == "house-record-package" and row.get("outcome") == outcome)
    assert missing["issue"]["package_id"] == package
    assert missing["input"] == pin
    result = next(row for row in journal if row.get("event") == "house-record-result")
    assert result["complete_scope"] is False
    assert result["outcome"] == "unread"
    if package is None or package == "":
        assert result["qualified_occurrences"] == 1


def test_successfully_empty_and_bounded_unread_are_distinct(tmp_path):
    output, _, args, _ = fixture(tmp_path, entries={"a":"No executive communications printed."})
    enrich_house_record(output, **args)
    assert any(row.get("event") == "house-record-package" and row.get("outcome") == "empty" for row in events(args["evidence"]))
    args["evidence"] = CaptureEvidence(tmp_path, "house-communications")
    enrich_house_record(output, **args, max_packages=0)
    assert any(row.get("event") == "house-record-package" and row.get("outcome") == "unread" for row in events(args["evidence"]))


@pytest.mark.parametrize("changed", [False, True])
def test_resume_checks_declared_retained_body_and_never_recaptures(tmp_path, changed):
    output, _, args, pin = fixture(tmp_path)
    enrich_house_record(output, **args)
    old = args["evidence"]
    artifact = old.seal(outcome="build-complete")
    root = artifact.root
    current = CaptureEvidence(tmp_path, "house-communications")
    current._prior_evidence = ("https://evidence.test", {"artifactDigest":root["artifactDigest"], "logicalId":root["logicalId"]})
    args["evidence"] = current
    # Other changes in the selected issue family do not change this issue's marker.
    pin["generationId"] = "new-selected-issues"
    table = pq.read_table(output)
    from spicy_regs.transforms.table_merge import set_column
    for name in ("record_package_id", "record_granule_id", "record_entry_text"):
        table = set_column(table, name, [None])
    pq.write_table(table, output)
    fetch = published_fetch(old, root, changed=changed)
    with patch("spicy_regs.sources.publication.load_evidence_journal", return_value=(old.artifact_dir / "journal.jsonl").read_bytes()):
        enrich_house_record(output, **args, fetch_retained=fetch)
    assert len(args["acquirer"].calls) == 1
    assert pq.read_table(output)["record_package_id"].to_pylist() == [None if changed else PACKAGE]
    assert any(row.get("event") == "house-record-package" and
               (row.get("outcome") == "refused" if changed else row.get("acquisition") == "retained-source-reuse")
               for row in events(current))
    current.seal(outcome="build-complete")


def test_locator_prose_describes_api_enrichment(tmp_path):
    from spicy_regs.data_dictionary import contract_column_prose, load_descriptions

    descriptions = contract_column_prose("house_communications")
    assert "API row" in descriptions["record_package_id"]
    assert "original abstract" in descriptions["record_entry_text"]
    published = load_descriptions()["house_communications"]
    for field in ("record_package_id", "record_granule_id", "record_entry_text"):
        assert published["columns"][field] == descriptions[field]
    assert "Bounded reads of selected Congressional Record issues" in published["data_quality"]


def test_existing_index_builder_invokes_enrichment_after_api_shaping(tmp_path):
    from tests.test_congress_index import FixtureReader
    from spicy_regs.transforms.build_congress_index import INDEX_SPECS, build_index_table

    package = "CREC-2026-09-17"
    _, _, args, pin = fixture(tmp_path, entries={"a":"EC-4752. " + SENTENCE}, package=package)
    issue = tmp_path / "issue-source.parquet"
    pq.write_table(pa.Table.from_pylist([{"congress":"119", "issue_date":"2026-09-17", "package_id":package,
                                        "update_date":"2026-09-17"}]), issue)
    pin["processing"] = {"sha256":"sha256:" + hashlib.sha256(issue.read_bytes()).hexdigest(), "byteSize":issue.stat().st_size}
    download = args["download_prior"]
    (tmp_path / "builder").mkdir()
    output = build_index_table(tmp_path / "builder", INDEX_SPECS["house_communications"], reader=FixtureReader(),
        congresses=[119], evidence=args["evidence"], max_details=3, selected_input=args["selected_input"],
        record_reader=args["reader"], record_acquirer=args["acquirer"],
        download_prior=lambda key, dest: download(key, dest) if key == "record_issues.parquet" else False)
    row = next(row for row in pq.read_table(output).to_pylist() if row["number"] == "4752")
    assert row["record_package_id"] == package
    assert row["source_route"] == "congress-gov-detail"
    assert row["abstract"] != SENTENCE


def test_byte_bound_advances_later_packages_without_rereading_qualified_scope(tmp_path):
    output, _, args, pin = fixture(tmp_path, numbers=("1", "2"))
    packages = (PACKAGE, "CREC-2025-01-07")
    table = pq.read_table(output).to_pylist()
    table[1]["congressional_record_date"] = "2025-01-07"
    pq.write_table(pa.Table.from_pylist(table, schema=pq.read_schema(output)), output)
    issue = tmp_path / "issue-source.parquet"
    pq.write_table(pa.Table.from_pylist([{"congress":"119", "issue_date":package[5:], "package_id":package,
                                        "update_date":package[5:]} for package in packages]), issue)
    pin["processing"] = {"sha256":"sha256:" + hashlib.sha256(issue.read_bytes()).hexdigest(), "byteSize":issue.stat().st_size}
    class Reader:
        def granules(self, url, **kwargs):
            package = url.split("/packages/")[1].split("/")[0]
            yield SimpleNamespace(records=[{"granuleId":package + "-PgH1", "granuleClass":"HOUSE",
                                           "title":"EXECUTIVE COMMUNICATIONS, ETC."}])
    class Acquirer:
        calls = []
        def acquire_granule(self, package, granule, **kwargs):
            self.calls.append(package)
            number = packages.index(package) + 1
            raw = ("<html><body><pre>EC-" + str(number) + ". " + SENTENCE + "</pre></body></html>").encode()
            if len(raw) > kwargs["max_bytes"]:
                raise ValueError("source body exceeds remaining allowance")
            url = "https://www.govinfo.gov/content/pkg/" + package + "/html/" + granule + ".htm"
            capture = CapturedBodyResponse(url, url, 200, "text/html", "2025-01-07T00:00:00Z", raw)
            return SimpleNamespace(identity=parse_granule_identity(package, granule), format="htm", body_capture=capture,
                                   body=SimpleNamespace(media_type="text/html", byte_size=len(raw)))
    args.update(reader=Reader(), acquirer=Acquirer())
    allowance = len(("<html><body><pre>EC-1. " + SENTENCE + "</pre></body></html>").encode())
    enrich_house_record(output, **args, max_bytes=allowance)
    assert pq.read_table(output)["record_package_id"].to_pylist() == [packages[0], None]
    old = args["evidence"]
    root = old.seal(outcome="build-complete").root
    args["evidence"] = CaptureEvidence(tmp_path, "house-communications")
    args["evidence"]._prior_evidence = ("https://evidence.test", {"artifactDigest":root["artifactDigest"], "logicalId":root["logicalId"]})
    with patch("spicy_regs.sources.publication.load_evidence_journal", return_value=(old.artifact_dir / "journal.jsonl").read_bytes()):
        enrich_house_record(output, **args, max_bytes=allowance,
                            fetch_retained=lambda *args: pytest.fail("unchanged qualified scope reread its bodies"))
    assert pq.read_table(output)["record_package_id"].to_pylist() == list(packages)
    assert args["acquirer"].calls == list(packages)
    current = args["evidence"]
    next_root = current.seal(outcome="build-complete").root
    table = pq.read_table(output).to_pylist()
    for field in ("record_package_id", "record_granule_id", "record_entry_text"):
        table[0][field] = None
    pq.write_table(pa.Table.from_pylist(table, schema=pq.read_schema(output)), output)
    args["evidence"] = CaptureEvidence(tmp_path, "house-communications")
    args["evidence"]._prior_evidence = ("https://evidence.test", {"artifactDigest":next_root["artifactDigest"], "logicalId":next_root["logicalId"]})
    fetch = published_fetch(old, root)
    with patch("spicy_regs.sources.publication.load_evidence_journal", return_value=(current.artifact_dir / "journal.jsonl").read_bytes()):
        enrich_house_record(output, **args, max_bytes=allowance, fetch_retained=fetch)
    assert pq.read_table(output)["record_package_id"].to_pylist() == list(packages)
    assert args["acquirer"].calls == list(packages)
    args["evidence"].seal(outcome="build-complete")


@pytest.mark.parametrize("change", ["entry", "locator", "rule"])
def test_cached_interpretation_must_equal_the_checked_body_and_rule(tmp_path, change):
    from spicy_regs.transforms.table_merge import set_column

    output, _, args, _ = fixture(tmp_path)
    enrich_house_record(output, **args)
    old = args["evidence"]
    checkpoint = next(row for row in events(old) if row.get("event") == "house-record-package")
    root = old.seal(outcome="build-complete").root
    if change == "entry":
        checkpoint["entries"][0]["entry_text"] = "An invented passage"
    elif change == "locator":
        checkpoint["bodies"][0]["granule_id"] = PACKAGE + "-PgHwrong"
    else:
        checkpoint["rule"] = "an unreviewed interpretation"
    table = pq.read_table(output)
    for field in ("record_package_id", "record_granule_id", "record_entry_text"):
        table = set_column(table, field, [None])
    pq.write_table(table, output)
    current = CaptureEvidence(tmp_path, "house-communications")
    current._prior_evidence = ("https://evidence.test", {"artifactDigest":root["artifactDigest"], "logicalId":root["logicalId"]})
    args["evidence"] = current
    fetch = published_fetch(old, root)
    with patch.object(current, "inherited_event", return_value=checkpoint):
        enrich_house_record(output, **args, fetch_retained=fetch)
    assert pq.read_table(output)["record_package_id"].to_pylist() == [None]
    assert len(args["acquirer"].calls) == 1


def test_actual_receipt_build_injects_selected_issue_pin_through_house_wrapper(tmp_path, monkeypatch):
    from tests.test_congress_index import FixtureReader
    from spicy_regs.pipelines.rollups.congress_index import HouseCommunicationsRollup
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from spicy_regs.transforms.build_congress_index import build_house_communications

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    package = "CREC-2026-09-17"
    _, _, args, _ = fixture(tmp_path, entries={"a":"EC-4752. " + SENTENCE}, package=package)
    issue = tmp_path / "selected-record-issue.parquet"
    schema = pa.schema([(name, pa.string()) for name in TABLE_CONTRACTS["record_issues"].columns])
    value = dict.fromkeys(schema.names) | {"volume":"172", "issue":"147", "congress":"119", "session":"2",
        "issue_date":"2026-09-17T04:00:00Z", "package_id":package, "update_date":"2026-09-17T00:00:00Z", "detail_read":"true"}
    pq.write_table(pa.Table.from_pylist([value], schema=schema), issue)
    subject, receipts = write_congress_dataset(issue, tmp_path / "selected-native-issues", dataset="record_issues", generation_id="record-pinned")
    assert subject is not None
    remember_selection(tmp_path, [SelectedDataset("record_issues", (subject,), receipts, "record-pinned")])
    pipeline = HouseCommunicationsRollup(output_dir=tmp_path)
    pipeline.source_evidence = args["evidence"]
    output = pipeline.build_receipts(tmp_path, build_house_communications, reader=FixtureReader(), congresses=[119],
        max_details=3, evidence=pipeline.source_evidence, record_reader=args["reader"], record_acquirer=args["acquirer"])
    restored = SelectedPriors(tmp_path / "verify-restored", root=tmp_path).get("house_communications")
    row = next(row for row in pq.read_table(restored).to_pylist() if row["number"] == "4752")
    assert {path.name for path in output} == {"house_communications.parquet", "house_communications_detail_reads.parquet", "house_record_enrichment_results.parquet"}
    assert row["record_package_id"] == package
    assert row["congressional_record_date"] == "2026-09-17"
    package_witness = next(row for row in events(pipeline.source_evidence)
                           if row.get("event") == "house-record-package" and row.get("outcome") == "read")
    assert package_witness["issue"]["issue_date"] == "2026-09-17T04:00:00Z"
    matched = next(row for row in events(pipeline.source_evidence)
                   if row.get("event") == "house-record-result" and row.get("outcome") == "matched")
    assert matched["witnesses"][0]["input"]["generationId"] == "record-pinned"
    assert matched["congressional_record_date"] == "2026-09-17"
    assert matched["record_calendar_day"] == "2026-09-17"
    assert matched["witnesses"][0]["input"]["processing"]["sha256"].startswith("sha256:")
