"""Every executive order the API serves: multi-year builds, kept rows, the nothing-new exit and the public table."""

from copy import deepcopy
from datetime import date
import hashlib
import json
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow.parquet as pq
import pytest
import yaml
from rulespec_artifacts import ArtifactPin

from spicy_docs.sources.federal_register.body_captures import load_body_capture
from spicy_docs.sources.federal_register.native import federal_register_source_record_id
from spicy_regs import data_dictionary, mcp_server
from spicy_regs.local_publication import LOCAL_FAMILIES
from spicy_regs.pipelines import executive_orders as pipeline
from spicy_regs.pipelines.raw_releases import open_raw
from spicy_regs.pipelines.rollups import source_refreshes
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from spicy_regs.transforms import build_executive_orders as module
from tests.generation_fakes import Store
from tests.published_paths import local_paths

REPO_ROOT = Path(__file__).resolve().parents[1]
THROUGH = date(1995, 6, 30)


def record(number, eo, published, **changes):
    year, month, day = published.split("-")
    return {
        "document_number": number,
        "publication_date": published,
        "signing_date": published,
        "type": "Presidential Document",
        "subtype": "Executive Order",
        "executive_order_number": eo,
        "title": f"Order {eo}",
        "president": {"identifier": "william-j-clinton", "name": "William J. Clinton"},
        "disposition_notes": None,
        "executive_order_notes": "See: EO 12866",
        "full_text_xml_url": f"https://www.federalregister.gov/documents/full_text/xml/{year}/{month}/{day}/{number}.xml",
        "pdf_url": f"https://www.govinfo.gov/content/pkg/FR-{published}/pdf/{number}.pdf",
        **changes,
    }


class Publisher:
    """A fake Federal Register: its executive-order listing and each order's XML, every request recorded."""

    def __init__(self, records):
        self.records = list(records)
        self.listed, self.bodies = [], []

    def fetch(self, url):
        query = parse_qs(urlsplit(url).query)
        assert query["conditions[presidential_document_type][]"] == ["executive_order"]
        first, last = query["conditions[publication_date][gte]"][0], query["conditions[publication_date][lte]"][0]
        self.listed.append(url)
        found = sorted((row for row in self.records if first <= row["publication_date"] <= last),
                       key=lambda row: row["publication_date"], reverse=True)
        return json.dumps({"count": len(found), "total_pages": 1, "results": found}).encode()

    def serve(self, request):
        number = request.url.path.rsplit("/", 1)[-1].removesuffix(".xml")
        self.bodies.append(number)
        body = (f"<PRESDOCU><P>Order text {number}.</P>"
                f"<FRDOC>[FR Doc. {number} Filed 1-2-95; 8:45 am]</FRDOC></PRESDOCU>").encode()
        return httpx.Response(200, headers={"Content-Type": "application/xml"}, content=body)

    def acquire(self, directory, prior=None):
        return pipeline.acquire(directory, prior=prior or {}, through=THROUGH, fetch=self.fetch,
                                transport=httpx.MockTransport(self.serve))


def built_rows(selection, directory, prior=None):
    evidence = CaptureEvidence(directory, pipeline.FAMILY)
    table = pipeline.retain_and_build(selection, directory, evidence, prior=prior or {})
    evidence.finish()
    return table, {row["source_record_id"]: row for row in pq.read_table(table).to_pylist()}


ORDERS = [record("94-290", "12890", "1994-01-05"), record("95-1001", "12945", "1995-01-20")]


def test_every_year_is_built_and_an_untouched_order_keeps_its_published_row(tmp_path):
    publisher = Publisher(ORDERS)
    first = publisher.acquire(tmp_path / "acquire-1")
    # 1994-01-01 through 1995-06-30 is seven 90-day windows, read twice; one body each.
    assert len(publisher.listed) == 14 and sorted(publisher.bodies) == ["94-290", "95-1001"]
    assert first["changes"] == {"added": ["94-290@1994-01-05", "95-1001@1995-01-20"], "changed": [], "removed": []}
    table, rows = built_rows(first, tmp_path / "build-1")
    assert {key: (row["publication_year"], row["executive_order_number"]) for key, row in rows.items()} == {
        "94-290@1994-01-05": (1994, "12890"), "95-1001@1995-01-20": (1995, "12945")}
    assert json.loads(pq.read_schema(table).metadata[module.COVERAGE_KEY.encode()]) == {
        "publishedFrom": "1994-01-01", "publishedThrough": "1995-06-30", "presidentialDocumentType": "executive_order",
        "sourceReleaseDigest": rows["94-290@1994-01-05"]["source_release_digest"]}
    for row in rows.values():
        assert row["source_sha256"] == json.loads(row["metadata_locator_json"])["evidenceBlobRef"]
        assert row["body_status"] == "acquired" and row["body_text"].startswith("Order text")

    # The publisher adds a 1995 order and adds a history note to the 1994 one.
    publisher.records = [record("94-290", "12890", "1994-01-05", disposition_notes="Revoked by: EO 13000"),
                         ORDERS[1], record("95-3000", "12950", "1995-02-07")]
    publisher.listed, publisher.bodies = [], []
    second = publisher.acquire(tmp_path / "acquire-2", prior=rows)
    assert second["changes"] == {"added": ["95-3000@1995-02-07"], "changed": ["94-290@1994-01-05"], "removed": []}
    assert publisher.bodies == ["95-3000"]  # published bodies are reused, never fetched again
    _, again = built_rows(second, tmp_path / "build-2", prior=rows)
    assert again["95-1001@1995-01-20"] == rows["95-1001@1995-01-20"]
    corrected, before = again["94-290@1994-01-05"], rows["94-290@1994-01-05"]
    assert corrected["disposition_notes"] == "Revoked by: EO 13000"
    assert corrected["source_release_digest"] != before["source_release_digest"]
    assert corrected["source_sha256"] != before["source_sha256"]
    assert (corrected["body_sha256"], corrected["body_observed_at"]) == (before["body_sha256"], before["body_observed_at"])
    assert again["95-3000@1995-02-07"]["executive_order_number"] == "12950"

    # Nothing changed since the second build: no body request, and nothing to build.
    publisher.listed, publisher.bodies = [], []
    unchanged = publisher.acquire(tmp_path / "acquire-3", prior=again)
    assert unchanged["changes"] == {"added": [], "changed": [], "removed": []} and "bodies" not in unchanged
    assert publisher.bodies == [] and len(publisher.listed) == 14
    with pytest.raises(ValueError, match="nothing new"):
        built_rows(unchanged, tmp_path / "build-3", prior=again)


def test_a_listing_the_release_refuses_is_never_nothing_new(tmp_path):
    publisher = Publisher([record("94-290", "12890", "1994-01-05", subtype="Proclamation")])
    with pytest.raises(ValueError, match="complete admitted population"):
        publisher.acquire(tmp_path / "acquire")
    assert publisher.bodies == []


def test_a_withdrawn_order_leaves_and_a_missing_body_result_refuses(tmp_path):
    publisher = Publisher(ORDERS)
    _, rows = built_rows(publisher.acquire(tmp_path / "acquire-1"), tmp_path / "build-1")
    publisher.records = ORDERS[1:]
    selection = publisher.acquire(tmp_path / "acquire-2", prior=rows)
    assert selection["changes"] == {"added": [], "changed": [], "removed": ["94-290@1994-01-05"]}
    _, kept = built_rows(selection, tmp_path / "build-2", prior=rows)
    assert kept == {"95-1001@1995-01-20": rows["95-1001@1995-01-20"]}
    del selection["bodies"]["95-1001@1995-01-20"]
    with pytest.raises(ValueError, match="body result is missing"):
        built_rows(selection, tmp_path / "build-3", prior=rows)


def test_hand_build_publishes_the_table_readers_query(tmp_path, monkeypatch):
    publisher = Publisher(ORDERS)
    artifact, evidence = pipeline.build(publisher.acquire(tmp_path / "acquire"), tmp_path / "build")
    assert local_paths(tmp_path / "build" / "generation", evidence, also=(str(tmp_path),)) == []
    index = publication.publish_generation(tmp_path / "build" / "generation", client=Store(), bucket="fixture",
                                           prior_index=publication.empty_index(), evidence_directories=(evidence,))
    entry = index["families"]["executive-orders"]
    assert entry["artifactDigest"] == artifact.pin.artifact_digest
    assert list(entry["tables"]) == ["executive_order_observations.parquet"]
    assert [name for name, _ in entry["tables"]["executive_order_observations.parquet"]["columns"]] == [
        "publication_year", "executive_order_number", "document_number", "publication_date", "signing_date",
        "title", "president_identifier", "president_name", "president_json", "disposition_notes",
        "executive_order_notes", "metadata_field_states_json", "body_status", "body_text", "body_sha256",
        "text_sha256", "body_format", "body_url", "body_observed_at", "text_derivation", "text_derivation_version",
        "text_cleanup_json", "body_result_json", "offered_renditions_json", "source_record_id",
        "source_release_logical_id", "source_release_digest", "metadata_locator_json", "raw_metadata_json",
        "source_sha256"]
    assert module.TABLE in data_dictionary.expected_schemas() and "executive-orders" in LOCAL_FAMILIES
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "build" / "generation")
    with mcp_server._build_connection() as connection:
        assert connection.execute(
            f"SELECT publication_year, president_name FROM {module.TABLE} "
            "WHERE executive_order_number = '12945' AND body_text ILIKE '%order text%'"
        ).fetchall() == [(1995, "William J. Clinton")]
    # Every listing page and body the rows came from is an evidence member bound for R2.
    events = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    kept = {event["sha256"] for event in events if event.get("blob_store") == "central-raw"}
    rows = pq.read_table(tmp_path / "build" / "generation" / module.OUTPUT).to_pylist()
    assert {row["source_sha256"] for row in rows} | {row["body_sha256"] for row in rows} <= kept


def test_hosted_run_reuses_published_bodies_and_ends_green_when_nothing_is_new(tmp_path, monkeypatch):
    publisher = Publisher(ORDERS)
    first, _ = built_rows(publisher.acquire(tmp_path / "acquire"), tmp_path / "october")

    def held(self, directory):
        directory.mkdir()
        (directory / module.OUTPUT).write_bytes(first.read_bytes())
        return [directory / module.OUTPUT]

    publisher.records = [*ORDERS, record("95-3000", "12950", "1995-02-07")]
    publisher.bodies = []
    monkeypatch.setattr(source_refreshes.ExecutiveOrdersRollup, "published_tables", held)
    monkeypatch.setattr(source_refreshes.ExecutiveOrdersRollup, "fetch", staticmethod(publisher.fetch))
    monkeypatch.setattr(source_refreshes.ExecutiveOrdersRollup, "transport", httpx.MockTransport(publisher.serve))
    source_refreshes.ExecutiveOrdersRollup(output_dir=tmp_path / "first").run()
    (generation,) = (tmp_path / "first" / "generations").iterdir()
    rows = pq.read_table(generation / module.OUTPUT).to_pylist()
    assert sorted(row["executive_order_number"] for row in rows) == ["12890", "12945", "12950"]
    assert publisher.bodies == ["95-3000"]

    first.write_bytes((generation / module.OUTPUT).read_bytes())
    source_refreshes.ExecutiveOrdersRollup(output_dir=tmp_path / "unchanged").run()
    assert not (tmp_path / "unchanged" / "generations").exists() or not any(
        (tmp_path / "unchanged" / "generations").iterdir())


def test_rollup_has_a_console_script_and_a_weekday_workflow():
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["run-rollup-executive-orders"] == "spicy_regs.pipelines.rollups.source_refreshes:executive_orders_app"
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "rollup-executive-orders.yml").read_text())
    assert workflow[True]["schedule"] == [{"cron": "29 14 * * 1-5"}]
    assert workflow[True]["workflow_dispatch"]["inputs"]["skip_upload"]["default"] is True
    run = workflow["jobs"]["run"]
    assert (run["uses"], run["with"]["command"]) == ("./.github/workflows/_rollup.yml", "run-rollup-executive-orders")
    assert run["with"]["skip_upload"] == "${{ github.event_name == 'workflow_dispatch' && inputs.skip_upload || false }}"


class Reader:
    """An admitted release stand-in for the transform's refusals."""

    def __init__(self, rows):
        rows = deepcopy(rows)
        self.body = json.dumps(rows).encode()
        self.ref = "sha256:" + hashlib.sha256(self.body).hexdigest()
        self.pin = ArtifactPin("urn:test:executive-orders", self.ref)
        self.records: list[dict[str, Any]] = [
            {"sourceRecordId": federal_register_source_record_id(row), "record": row} for row in rows
        ]
        self.collection_outcome: dict[str, Any] = {
            "requestedScope": pipeline.query_scope(THROUGH),
            "failedRecordCount": 0,
            "discardedObservationCount": 0,
            "publishedRecordCount": len(rows),
            "discoveredRecordCount": len(rows),
        }

    def iter_records(self):
        yield from deepcopy(self.records)

    def iter_record_evidence(self):
        for index, item in enumerate(self.records):
            yield {"sourceRecordId": item["sourceRecordId"], "evidenceBlobRef": self.ref,
                   "jsonPointer": f"/results/{index}"}

    def read_evidence(self, ref, *, max_bytes):
        assert ref == self.ref and len(self.body) <= max_bytes
        return self.body


@pytest.mark.parametrize(
    "defect",
    ["one_year_scope", "failed_metadata", "late_count", "missing_body", "extra_body", "wrong_type", "wrong_locator"],
)
def test_refusal_preserves_previous_table(tmp_path, monkeypatch, defect):
    publisher = Publisher(ORDERS[:1])
    selection = publisher.acquire(tmp_path / "acquire")
    paths = pipeline.materialize_bodies(selection["bodies"], tmp_path / "bodies", store=open_raw())
    reader = Reader(ORDERS[:1])
    output = tmp_path / "build"
    evidence = CaptureEvidence(output, "executive-orders")
    path = module.build_executive_orders(reader, output, body_paths=paths, load_body=load_body_capture,
                                         evidence=evidence)
    before = path.read_bytes()
    if defect == "one_year_scope":
        reader.collection_outcome["requestedScope"]["publishedFrom"] = "1995-01-01"
    elif defect == "failed_metadata":
        reader.collection_outcome["failedRecordCount"] = 1
    elif defect == "late_count":
        reader.collection_outcome["publishedRecordCount"] = reader.collection_outcome["discoveredRecordCount"] = 2
    elif defect == "missing_body":
        paths.clear()
    elif defect == "extra_body":
        paths["other"] = next(iter(paths.values()))
    elif defect == "wrong_type":
        reader.records[0]["record"]["subtype"] = "Proclamation"
    else:
        original = list(reader.iter_record_evidence())
        original[0]["sourceRecordId"] = "wrong"
        monkeypatch.setattr(reader, "iter_record_evidence", lambda: iter(original))
    with pytest.raises(ValueError):
        module.build_executive_orders(reader, output, body_paths=paths, load_body=load_body_capture,
                                      evidence=evidence)
    assert path.read_bytes() == before
    evidence.finish()
