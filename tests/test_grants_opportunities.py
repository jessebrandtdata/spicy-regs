"""Grants literals, snapshot changes and the ordinary local SQL consumer."""

from copy import deepcopy
from datetime import date
from decimal import Decimal
import hashlib
from io import BytesIO
import json
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape
from zipfile import ZipFile

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import data_dictionary, mcp_server
from spicy_regs.pipelines import grants_opportunities as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_grants_opportunities as module
from tests.generation_fakes import Store
from tests.published_paths import local_paths


class Reader:
    """Small admitted-reader fixture; the real source workflow is checked separately."""

    def __init__(self, rows, *, snapshot="2026-10-20"):
        self.body = json.dumps(rows).encode()
        digest = "sha256:" + hashlib.sha256(self.body + snapshot.encode()).hexdigest()
        self.pin = ArtifactPin("urn:test:grants:" + digest[7:], digest)
        capture = {
            "request_url": "https://www.grants.gov/xml-extract/fixture.zip",
            "response_sha256": "sha256:" + hashlib.sha256(self.body).hexdigest(),
            "observed_at": "2026-10-20T12:00:00Z",
            "byte_size": len(self.body),
        }
        self.collection_outcome: dict[str, Any] = {
            "requestedScope": {"snapshot_date": snapshot, "capture": capture, "member_name": "fixture.xml"},
            "failedRecordCount": 0,
            "discardedObservationCount": 0,
            "discoveredRecordCount": len(rows),
            "publishedRecordCount": len(rows),
        }
        self.records: list[dict[str, Any]] = []
        for ordinal, (kind, fields) in enumerate(rows):
            native = dict(fields)
            source_xpath = f"/Grants/Opportunity{kind.title()}Detail_1_0[{ordinal + 1}]"
            self.records.append(
                {
                    "sourceRecordId": f"grants-gov:{kind}:{native['OpportunityID']}",
                    "record": {
                        "snapshot_date": snapshot,
                        "notice_type": kind,
                        "opportunity_id": native["OpportunityID"],
                        "version": native.get("Version"),
                        "ordinal": ordinal,
                        "fields": [
                            {
                                "tag": module.NAMESPACE + key,
                                "text": value,
                                "attributes": {},
                                "source_xpath": source_xpath + "/" + key,
                            }
                            for key, value in fields
                        ],
                        "source_locator": {
                            **{key: capture[key] for key in ("request_url", "response_sha256", "observed_at")},
                            "member_name": "fixture.xml",
                            "member_sha256": "sha256:" + "b" * 64,
                            "source_xpath": source_xpath,
                        },
                    },
                }
            )

    def iter_records(self):
        yield from deepcopy(self.records)


def synopsis(*, version="Synopsis 2", close="10202026"):
    return (
        "synopsis",
        [
            ("OpportunityID", "00123"),
            ("OpportunityNumber", "EXAMPLE-01"),
            ("Version", version),
            ("AgencyCode", "EX"),
            ("AgencyName", "Example Agency"),
            ("OpportunityTitle", "Research &amp; learning"),
            ("EligibleApplicants", "25"),
            ("EligibleApplicants", "99"),
            ("EligibleApplicants", "ZZ"),
            ("AdditionalInformationOnEligibility", "Only applicants described in the notice."),
            ("CFDANumbers", "01.001"),
            ("CFDANumbers", "01.010"),
            ("FundingInstrumentType", "G"),
            ("FundingInstrumentType", "CA"),
            ("CategoryOfFundingActivity", "ST"),
            ("CategoryOfFundingActivity", "UNKNOWN"),
            ("AwardCeiling", "900719925474099.01"),
            ("AwardFloor", "0"),
            ("EstimatedTotalProgramFunding", "none"),
            ("PostDate", "01012011"),
            ("CloseDate", close),
            ("CloseDateExplanation", "Extensions depend on the notice."),
            ("ArchiveDate", "12312014"),
            ("LastUpdatedDate", "10192026"),
            ("Description", "<p>Exact decoded XML text</p>"),
            ("UnknownFutureField", "preserved"),
        ],
    )


def forecast():
    return (
        "forecast",
        [
            ("OpportunityID", "00456"),
            ("Version", "Forecast 1"),
            ("AgencyCode", "EX"),
            ("EstimatedSynopsisCloseDate", "rolling"),
            ("EstimatedSynopsisCloseDateExplanation", "Until exhausted"),
            ("EstimatedAwardDate", "02302027"),
            ("AwardCeiling", ""),
            ("AwardFloor", "1.001"),
        ],
    )


def test_literals_multiplicity_typed_values_and_no_status_invention(tmp_path):
    reader = Reader([synopsis(), forecast()])
    path = module.build_grants_opportunities([reader], tmp_path)
    first, second = pq.read_table(path).to_pylist()
    assert first["opportunity_id"] == "00123"
    assert first["title"] == "Research &amp; learning"  # No second HTML decoding.
    assert json.loads(first["assistance_listing_numbers_json"]) == ["01.001", "01.010"]
    assert json.loads(first["eligibility_codes_json"]) == ["25", "99", "ZZ"]
    labels = json.loads(first["eligibility_labels_json"])
    assert "Additional Information" in labels[0] and "clarification" in labels[1] and labels[2] is None
    assert first["award_ceiling"] == Decimal("900719925474099.01")
    assert first["award_floor"] == Decimal("0") and first["amount_unit"] is None
    assert first["archive_date"] == date(2014, 12, 31) and first["notice_type"] == "synopsis"
    assert first["snapshot_date"] == date(2026, 10, 20)
    assert first["source_release_digest"] == reader.pin.artifact_digest
    assert json.loads(first["raw_fields_json"]) == reader.records[0]["record"]["fields"]
    assert json.loads(first["value_statuses_json"])["estimated_total_program_funding"] == "publisher_none"
    assert second["notice_type"] == "forecast" and second["close_date"] is None
    statuses = json.loads(second["value_statuses_json"])
    assert statuses["close_date"] == "missing"
    assert statuses["estimated_synopsis_close_date"] == statuses["estimated_award_date"] == "unparsed"
    assert statuses["award_ceiling"] == "blank" and statuses["award_floor"] == "unrepresentable"
    assert second["estimated_synopsis_close_date_explanation"] == "Until exhausted"
    assert "status" not in module.SCHEMA.names
    assert pq.read_schema(path).equals(module.SCHEMA)


def test_changed_version_and_omission_keep_original_observations(tmp_path):
    first = Reader([synopsis(), forecast()])
    changed = Reader([synopsis(version="Synopsis 3", close="11302026")], snapshot="2026-10-21")
    path = module.build_grants_opportunities([first, changed], tmp_path)
    rows = pq.read_table(path).to_pylist()
    assert [(row["version"], row["close_date"]) for row in rows if row["notice_type"] == "synopsis"] == [
        ("Synopsis 2", date(2026, 10, 20)),
        ("Synopsis 3", date(2026, 11, 30)),
    ]
    assert len({row["source_release_digest"] for row in rows}) == 2
    # The absent forecast is an observation in the earlier snapshot, never a withdrawal event.
    assert len(rows) == 3 and rows[1]["version"] == "Forecast 1"


@pytest.mark.parametrize("defect", ["incomplete", "late_count", "identity", "archive", "scalar", "duplicate_release"])
def test_refusal_keeps_existing_table(tmp_path, defect):
    original = Reader([synopsis()])
    path = module.build_grants_opportunities([original], tmp_path)
    before = path.read_bytes()
    reader = Reader([synopsis(), forecast()])
    readers = [reader]
    if defect == "incomplete":
        reader.collection_outcome["failedRecordCount"] = 1
    elif defect == "late_count":
        reader.collection_outcome["publishedRecordCount"] = reader.collection_outcome["discoveredRecordCount"] = 3
    elif defect == "identity":
        reader.records[-1]["record"]["opportunity_id"] = "other"
    elif defect == "archive":
        reader.records[-1]["record"]["source_locator"]["member_name"] = "other.xml"
    elif defect == "scalar":
        reader.records[-1]["record"]["fields"].append(reader.records[-1]["record"]["fields"][0])
    else:
        readers.append(reader)
    with pytest.raises(ValueError):
        module.build_grants_opportunities(readers, tmp_path)
    assert path.read_bytes() == before


def test_stream_failure_is_not_a_partial_snapshot(tmp_path):
    class BrokenReader(Reader):
        def iter_records(self):
            yield self.records[0]
            raise OSError("source read failed")

    reader = BrokenReader([synopsis(), forecast()])
    with pytest.raises(OSError, match="source read failed"):
        module.build_grants_opportunities([reader], tmp_path)
    assert not (tmp_path / module.OUTPUT).exists()


def test_explicit_complete_empty_release_keeps_empty_schema(tmp_path):
    path = module.build_grants_opportunities([Reader([])], tmp_path)
    assert pq.read_table(path).num_rows == 0
    assert pq.read_schema(path).equals(module.SCHEMA)


def test_ordinary_publication_and_local_agency_eligibility_deadline_query(tmp_path, monkeypatch):
    reader = Reader([synopsis(), forecast()])
    archive = tmp_path / "fixture.zip"
    archive.write_bytes(reader.body)
    ref = reader.collection_outcome["requestedScope"]["capture"]["response_sha256"]
    output = tmp_path / "build"
    artifact, evidence = pipeline.build([reader], output, archives={ref: archive})
    index = publication.publish_generation(
        output / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["grants-opportunities"]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        actual = connection.execute(f"""
            SELECT opportunity_id, version, close_date, award_ceiling,
                   json_extract_string(source_locator_json, '$.request_url')
            FROM {module.TABLE}
            WHERE agency_code='EX' AND json_contains(eligibility_codes_json, '\"25\"')
              AND close_date >= DATE '2026-10-01'
        """).fetchall()
    assert actual == [
        (
            "00123",
            "Synopsis 2",
            date(2026, 10, 20),
            Decimal("900719925474099.01"),
            "https://www.grants.gov/xml-extract/fixture.zip",
        )
    ]
    assert module.TABLE in data_dictionary.expected_schemas()


def test_changed_archive_refuses_before_table_or_generation(tmp_path):
    reader = Reader([synopsis()])
    archive = tmp_path / "changed.zip"
    archive.write_bytes(b"!" * len(reader.body))
    ref = reader.collection_outcome["requestedScope"]["capture"]["response_sha256"]
    output = tmp_path / "build"
    with pytest.raises(ValueError, match="digest"):
        pipeline.build([reader], output, archives={ref: archive})
    assert not (output / module.OUTPUT).exists()
    assert not (output / "generation").exists()


def export_zip(snapshot, rows):
    """A small daily export in the publisher's layout: one XML member named for its date."""
    records = "".join(
        f"<Opportunity{kind.title()}Detail_1_0>"
        + "".join(f"<{tag}>{escape(text)}</{tag}>" for tag, text in fields)
        + f"</Opportunity{kind.title()}Detail_1_0>"
        for kind, fields in rows
    )
    body = BytesIO()
    with ZipFile(body, "w") as archive:
        archive.writestr(f"GrantsDBExtract{snapshot.replace('-', '')}v2.xml",
                         f'<Grants xmlns="{module.NAMESPACE[1:-1]}">{records}</Grants>')
    return body.getvalue()


def test_acquire_retains_the_export_in_raw_and_builds_from_its_capture(tmp_path, monkeypatch, capsys):
    """The download is bound to the version the publisher states, kept in raw, and built from there."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

    official = "https://prod-grants-gov-chatbot.s3.amazonaws.com/extracts/GrantsDBExtract20261109v2.zip"
    archive = export_zip("2026-11-09", [synopsis(), forecast()])
    digest = "sha256:" + hashlib.sha256(archive).hexdigest()
    stated = {"content-type": "application/zip", "etag": '"fixture-version-7"', "content-length": str(len(archive))}
    requests = []

    def serve(request):
        requests.append((request.method, str(request.url), request.headers.get("if-match")))
        return httpx.Response(200, headers=stated, content=b"" if request.method == "HEAD" else archive)

    selection = pipeline.acquire(["2026-11-09"], tmp_path / "acquire", transport=httpx.MockTransport(serve))
    assert requests == [("HEAD", official, None), ("GET", official, '"fixture-version-7"')]
    (scope,) = selection["releases"]
    assert scope["snapshot_date"] == "2026-11-09" and scope["member_name"] == "GrantsDBExtract20261109v2.xml"
    assert {key: scope["capture"][key] for key in ("request_url", "response_sha256", "byte_size")} == {
        "request_url": official, "response_sha256": digest, "byte_size": len(archive)}
    raw = tmp_path / "central-raw"  # the archive every test is given
    assert CentralRawArchiveStore(raw, create=False).describe_blob(digest, len(archive))["encoding"] == "raw"
    assert [(row["source_url"], row["sha256"]) for _path, document in iter_acquisition_receipts(raw)
            for row in document["files"]] == [(official, digest[7:])]

    captures = tmp_path / "captures.json"
    captures.write_text(json.dumps(selection))
    output = tmp_path / "build"
    monkeypatch.setattr("sys.argv", ["grants", "build", "--captures", str(captures), "--output-dir", str(output)])
    pipeline.main()
    built = json.loads(capsys.readouterr().out)
    evidence = Path(built["sourceEvidence"])
    # An admitted release yields its records in source-record order: the forecast precedes the synopsis.
    second, first = pq.read_table(output / "generation" / module.OUTPUT).to_pylist()
    assert (first["opportunity_id"], first["notice_type"], first["version"]) == ("00123", "synopsis", "Synopsis 2")
    assert (first["snapshot_date"], first["close_date"]) == (date(2026, 11, 9), date(2026, 10, 20))
    assert first["title"] == "Research &amp; learning" and first["award_ceiling"] == Decimal("900719925474099.01")
    assert (second["opportunity_id"], second["notice_type"]) == ("00456", "forecast")
    assert json.loads(first["source_locator_json"])["request_url"] == official
    (retained,) = [event for line in (evidence / "journal.jsonl").read_text().splitlines()
                   if (event := json.loads(line))["event"] == "retained-file"]
    assert (retained["original_location"], retained["sha256"]) == (official, digest)
    assert local_paths(output / "generation", evidence, also=(str(tmp_path),)) == []
    index = publication.publish_generation(
        output / "generation", client=Store(), bucket="fixture",
        prior_index=publication.empty_index(), evidence_directories=(evidence,),
    )
    assert index["families"]["grants-opportunities"]["artifactDigest"] == built["artifactDigest"]


def test_acquire_refuses_an_export_that_changes_after_its_version_was_stated(tmp_path):
    archive = export_zip("2026-11-09", [synopsis()])

    def serve(request):
        version = '"stated"' if request.method == "HEAD" else '"republished"'
        return httpx.Response(200, content=b"" if request.method == "HEAD" else archive, headers={
            "content-type": "application/zip", "etag": version, "content-length": str(len(archive))})

    with pytest.raises(ValueError, match="ETag does not match its selected version"):
        pipeline.acquire(["2026-11-09"], tmp_path / "acquire", transport=httpx.MockTransport(serve))
