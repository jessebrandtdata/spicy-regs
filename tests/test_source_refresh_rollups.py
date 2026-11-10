"""The hosted refreshes of the once hand-run families: wiring, raw retention and the no-change exit."""

from datetime import UTC, datetime, timedelta
import hashlib
import json
import shutil
import tomllib
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs import data_dictionary as dd
from spicy_regs.pipelines import administration_policy, treasury_fiscal
from spicy_regs.pipelines.raw_releases import restore_originals
from spicy_regs.pipelines.rollups import source_refreshes as rollups
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from tests.generation_fakes import Store
from tests import test_administration_policy_pipeline as policy_fixture
from tests.published_paths import local_paths
from tests.test_grants_opportunities import export_zip as grants_zip, forecast, synopsis
from tests.test_ofac import export_zip as ofac_zip

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
TREASURY = Path(__file__).parent / "fixtures" / "treasury"
ROLLUPS = {
    "treasury_fiscal_app": rollups.TreasuryFiscalRollup,
    "administration_policy_app": rollups.AdministrationPolicyRollup,
    "ofac_sanctions_app": rollups.OfacSanctionsRollup,
    "grants_opportunities_app": rollups.GrantsOpportunitiesRollup,
    "house_disbursements_app": rollups.HouseDisbursementsRollup,
}


@pytest.mark.parametrize("app, rollup", ROLLUPS.items(), ids=lambda value: getattr(value, "name", value))
def test_each_refresh_has_a_console_script_and_a_dispatch_only_workflow(app, rollup):
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    command = f"run-rollup-{rollup.name}"
    assert scripts[command] == f"spicy_regs.pipelines.rollups.source_refreshes:{app}"
    assert all(key.removesuffix(".parquet") in dd.TABLES for key in rollup.outputs or (rollup.output,))
    workflow = yaml.safe_load((WORKFLOWS / f"rollup-{rollup.name}.yml").read_text())
    # A schedule starts running the day it reaches main: none until a hosted rehearsal has passed.
    assert set(workflow[True]) == {"workflow_dispatch"}
    assert workflow[True]["workflow_dispatch"]["inputs"]["skip_upload"]["default"] is True
    run = workflow["jobs"]["run"]
    assert (run["uses"], run["secrets"], run["with"]["command"]) == ("./.github/workflows/_rollup.yml", "inherit", command)
    declared = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())[True]["workflow_call"]["inputs"]
    assert set(run["with"]) <= set(declared)
    assert run["with"]["skip_upload"] == "${{ github.event_name == 'workflow_dispatch' && inputs.skip_upload || false }}"


def generations(output):
    return sorted((output / "generations").iterdir()) if (output / "generations").exists() else []


def published(output):
    """The one generation and evidence artifact a run left, as a publish would upload them."""
    (generation,) = generations(output)
    (evidence,) = output.glob("source-evidence/*/artifact")
    assert local_paths(generation, evidence, also=(str(output.parent),)) == []
    # The ordinary publisher admits the evidence, uploads every blob it names, then the generation.
    store = Store()
    index = publication.publish_generation(generation, client=store, bucket="fixture",
                                           prior_index=publication.empty_index(), evidence_directories=(evidence,))
    events = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    for event in events:
        if event.get("blob_store") == "central-raw":
            assert "source-evidence/" + event["blob_member"] in store.objects
    assert len(index["families"]) == 1
    return generation, events


def publish_as_prior(monkeypatch, match):
    """Make the published generation's evidence answer ``inherited_event`` from ``match(event, fields)``."""
    monkeypatch.setattr(CaptureEvidence, "inherited_event", lambda self, event, **fields: match(event, fields))


def test_ofac_keeps_its_exports_in_evidence_and_skips_published_ones(tmp_path, monkeypatch):
    archive = ofac_zip()
    digest = "sha256:" + hashlib.sha256(archive).hexdigest()

    def serve(request):
        if request.url.host == "sanctionslistservice.ofac.treas.gov":
            name = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(302, headers={"location": (
                "https://wc2h-sls-prod-public-published.s3.us-gov-west-1.amazonaws.com/Published/a/" + name)})
        return httpx.Response(200, content=archive if "SDN" in request.url.path else ofac_zip("consolidated"),
                              headers={"content-type": "application/zip"})

    monkeypatch.setattr(rollups.OfacSanctionsRollup, "transport", httpx.MockTransport(serve))
    rollups.OfacSanctionsRollup(output_dir=tmp_path / "first").run()
    generation, events = published(tmp_path / "first")
    entities = pq.read_table(generation / "ofac_entity_observations.parquet").to_pylist()
    assert sorted(row["entity_id"] for row in entities) == ["36", "9640"]
    # The transport's capture is the retained original: its bytes are an evidence member bound for R2.
    assert digest in [event["sha256"] for event in events
                      if event["event"] == "capture" and event.get("blob_store") == "central-raw"]
    assert [event["collection_outcome"]["requestedScope"]["selected_list"] for event in events
            if event["event"] == "ofac-source-release"] == ["sdn", "consolidated"]

    publish_as_prior(monkeypatch, lambda event, fields: fields.get("stage") == "ofac-source-archive")
    rollups.OfacSanctionsRollup(output_dir=tmp_path / "unchanged").run()
    assert generations(tmp_path / "unchanged") == []
    (outcome,) = (tmp_path / "unchanged").glob("source-evidence/*/run-outcome.json")
    assert json.loads(outcome.read_bytes())["outcome"] == "complete"


def test_grants_takes_the_newest_export_and_skips_a_published_date(tmp_path, monkeypatch):
    newest = (datetime.now(UTC).date() - timedelta(days=2)).isoformat()
    archive = grants_zip(newest, [synopsis(), forecast()])
    stated = {"content-type": "application/zip", "etag": '"export-1"', "content-length": str(len(archive))}
    probed = []

    def serve(request):
        if newest.replace("-", "") not in request.url.path:
            probed.append(request.url.path)
            return httpx.Response(403)
        return httpx.Response(200, headers=stated, content=b"" if request.method == "HEAD" else archive)

    monkeypatch.setattr(rollups.GrantsOpportunitiesRollup, "transport", httpx.MockTransport(serve))
    rollups.GrantsOpportunitiesRollup(output_dir=tmp_path / "first").run()
    generation, events = published(tmp_path / "first")
    rows = pq.read_table(generation / "grants_opportunity_observations.parquet").to_pylist()
    assert sorted((row["opportunity_id"], row["snapshot_date"].isoformat()) for row in rows) == [
        ("00123", newest), ("00456", newest)]
    assert len(probed) >= 2  # today and yesterday hold no export yet
    assert ["sha256:" + hashlib.sha256(archive).hexdigest()] == [
        event["sha256"] for event in events if event["event"] == "capture" and event.get("blob_store") == "central-raw"]

    publish_as_prior(monkeypatch, lambda event, fields: {
        "collection_outcome": {"requestedScope": {"snapshot_date": newest}}})
    rollups.GrantsOpportunitiesRollup(output_dir=tmp_path / "unchanged").run()
    assert generations(tmp_path / "unchanged") == []

    monkeypatch.setattr(rollups.GrantsOpportunitiesRollup, "transport",
                        httpx.MockTransport(lambda request: httpx.Response(403)))
    with pytest.raises(RuntimeError, match="holds no daily export from the last 7 days"):
        rollups.GrantsOpportunitiesRollup(output_dir=tmp_path / "stalled").run()


def test_administration_policy_rebuilds_at_the_head_commit_and_skips_an_unchanged_archive(tmp_path, monkeypatch):
    requests = []
    transport = policy_fixture.archive(requests)
    october = administration_policy.acquire(policy_fixture.OCTOBER, ["46-Biden", "47-Trump"], tmp_path / "acquire",
                                            transport=transport)
    administration_policy.build_from_captures(october, tmp_path / "october")

    def held(self, directory):
        shutil.copytree(tmp_path / "october" / "generation", directory)
        return sorted(directory.glob("administration_policy_*.parquet"))

    monkeypatch.setattr(rollups.AdministrationPolicyRollup, "published_tables", held)
    monkeypatch.setattr(rollups.AdministrationPolicyRollup, "transport", transport)
    monkeypatch.setattr(administration_policy, "head_commit", lambda: policy_fixture.NOVEMBER)
    rollups.AdministrationPolicyRollup(output_dir=tmp_path / "first").run()
    generation, events = published(tmp_path / "first")
    statements = pq.read_table(generation / "administration_policy_statements.parquet").to_pylist()
    assert [(row["administration"], row["archive_commit"], row["document_title"]) for row in statements] == [
        ("46-Biden", policy_fixture.OCTOBER, "Statement on S. 2"),
        ("47-Trump", policy_fixture.NOVEMBER, "Statement on H.R. 1"),
        ("47-Trump", policy_fixture.NOVEMBER, "Statement on H.R. 4"),
    ]
    assert pq.read_table(generation / "etl_receipts.parquet").num_rows == 7  # three statements, four bill links
    assert [event["commit"] for event in events if event["event"] == "administration-policy-head"] == [
        policy_fixture.NOVEMBER]
    changed = "sha256:" + hashlib.sha256(policy_fixture.TRUMP_LATER).hexdigest()
    assert changed in [event["sha256"] for event in events
                       if event["event"] == "capture" and event.get("blob_store") == "central-raw"]

    monkeypatch.setattr(administration_policy, "head_commit", lambda: "c" * 40)
    policy_fixture.FILES.update({("c" * 40, name): policy_fixture.FILES[policy_fixture.OCTOBER, name]
                                 for name in ("46-Biden", "47-Trump")})
    rollups.AdministrationPolicyRollup(output_dir=tmp_path / "unchanged").run()
    assert generations(tmp_path / "unchanged") == []


def test_treasury_adds_listed_debt_days_to_the_published_generation(tmp_path, monkeypatch):
    report = b"%PDF-1.7 retained statement"
    later = (TREASURY / "debt_to_penny-2026-10-16.json").read_bytes().replace(b"2026-10-16", b"2026-10-19")
    listed = [{"record_date": "2026-10-19"}]

    def serve(request):
        if request.url.path.endswith(".pdf"):
            return httpx.Response(200, content=report, headers={"content-type": "application/pdf"})
        if request.url.path.endswith("/publishedfiles"):
            return httpx.Response(200, json=[{"report_date": "2026-09-30", "report_group_id": "5", "path": (
                "/static-data/published-reports/mts/MonthlyTreasuryStatement_202609.pdf")}])
        wanted = request.url.params["filter"]
        if wanted == "record_date:gt:2026-10-16":
            return httpx.Response(200, json={"data": listed, "meta": {"total-count": len(listed)}})
        if wanted == "record_date:eq:2026-10-19":
            return httpx.Response(200, content=later, headers={"content-type": "application/json"})
        name = f"{request.url.path.rsplit('/', 1)[-1]}-{wanted.removeprefix('record_date:eq:')}.json"
        return httpx.Response(200, content=(TREASURY / name).read_bytes(), headers={"content-type": "application/json"})

    transport = httpx.MockTransport(serve)
    selection = treasury_fiscal.acquire(
        [("mts_table_1", datetime(2026, 9, 30).date()), ("mts_table_2", datetime(2026, 9, 30).date()),
         ("debt_to_penny", datetime(2026, 10, 16).date())], [datetime(2026, 9, 30).date()],
        tmp_path / "acquire", transport=transport)
    treasury_fiscal.build_from_captures(selection, tmp_path / "october")

    def held(self, directory):
        shutil.copytree(tmp_path / "october" / "generation", directory)
        return sorted(directory.glob("treasury_*.parquet"))

    monkeypatch.setattr(rollups.TreasuryFiscalRollup, "published_tables", held)
    monkeypatch.setattr(rollups.TreasuryFiscalRollup, "transport", transport)
    rollups.TreasuryFiscalRollup(output_dir=tmp_path / "first").run()
    generation, events = published(tmp_path / "first")
    debt = pq.read_table(generation / "treasury_debt_observations.parquet").to_pylist()
    assert [row["record_date"].isoformat() for row in debt] == ["2026-10-16", "2026-10-19"]
    before = pq.read_table(tmp_path / "october" / "generation" / "treasury_statement_periods.parquet").num_rows
    assert pq.read_table(generation / "treasury_statement_periods.parquet").num_rows == before > 0
    # Every page behind the generation, old and new, and the report control are evidence members again.
    kept = {event["sha256"] for event in events if event["event"] in ("retained-file", "capture")}
    assert {capture["response_sha256"] for scope in selection["releases"] for capture in scope["captures"]} <= kept
    assert {"sha256:" + hashlib.sha256(later).hexdigest(), "sha256:" + hashlib.sha256(report).hexdigest()} <= kept

    del listed[:]
    rollups.TreasuryFiscalRollup(output_dir=tmp_path / "unchanged").run()
    assert generations(tmp_path / "unchanged") == []


def test_published_evidence_restores_the_originals_a_runner_lacks(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    page = b'{"data": []}'
    digest = "sha256:" + hashlib.sha256(page).hexdigest()
    requests = []

    def serve(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=page)

    store = CentralRawArchiveStore(tmp_path / "runner-raw")
    transport = httpx.MockTransport(serve)
    assert restore_originals(store, [digest, digest], "https://data.example", max_bytes=64, transport=transport) == 1
    assert requests == [f"https://data.example/source-evidence/blobs/sha256/{digest[7:]}"]
    assert store.describe_blob(digest, len(page))["encoding"] == "raw"
    assert restore_originals(store, [digest], "https://data.example", max_bytes=64, transport=transport) == 0
    with pytest.raises(ValueError, match="differs from its address"):
        restore_originals(store, ["sha256:" + "0" * 64], "https://data.example", max_bytes=64, transport=transport)


def test_house_rebuilds_from_restored_originals_when_the_house_lists_a_quarter(tmp_path, monkeypatch):
    """A runner's raw archive starts empty: the published originals come back from R2 evidence, the new quarter
    from the House, and the published quarter rebuilds value for value."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    from spicy_regs.pipelines import house_disbursements as house
    from tests import test_house_disbursements as fixture

    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    house.build(fixture.normalized(tmp_path), tmp_path / "first")
    workstation = CentralRawArchiveStore(tmp_path / "raw", create=False)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "runner-raw"))

    def download_members(key, directory):
        shutil.copytree(tmp_path / "first" / "generation" / key.removesuffix(".parquet"),
                        directory / key.removesuffix(".parquet"))

    monkeypatch.setattr(rollups.r2, "download_members", download_members)
    q2 = {kind: fixture.ROOT_URL + f"q2-{kind}.csv" for kind in ("detail", "summary")}
    bodies: dict[str, bytes] = {q2["detail"]: (b"ORGANIZATION,PROGRAM,SORT SUBTOTAL DESCRIPTION,SORT SEQUENCE,TRANSACTION DATE,"
                             b"DATA SOURCE,DOCUMENT,VENDOR NAME,PERFORM START DT,PERFORM END DT,DESCRIPTION,AMOUNT\r\n"
                             b"Office,Program,Travel,DETAIL,2-Apr-20,AP,00032,Payee,2-Apr-20,3-Apr-20,Travel,5.00\r\n"),
              q2["summary"]: b"ORGANIZATION,PROGRAM,DESCRIPTION,YTD AMOUNT,QTD AMOUNT\r\nOffice,Program,Travel,5.00,5.00\r\n"}
    requests = []

    def serve(request):
        requests.append(str(request.url))
        if "/source-evidence/blobs/sha256/" in request.url.path:
            with workstation.open("sha256:" + request.url.path.rsplit("/", 1)[-1]) as stream:
                return httpx.Response(200, content=stream.read())
        return httpx.Response(200, content=bodies[str(request.url)], headers={"Content-Type": "text/csv"})

    monkeypatch.setattr(rollups.HouseDisbursementsRollup, "transport", httpx.MockTransport(serve))
    fixture._listing(monkeypatch, (fixture.ROOT_URL + "detail.csv", "detail", "2020-01-01", "2020-03-31"))
    rollups.HouseDisbursementsRollup(output_dir=tmp_path / "unchanged").run()
    assert generations(tmp_path / "unchanged") == [] and requests == []

    fixture._listing(monkeypatch, *((url, kind, "2020-04-01", "2020-06-30") for kind, url in q2.items()))
    rollups.HouseDisbursementsRollup(output_dir=tmp_path / "hosted").run()
    assert sum("/source-evidence/blobs/" in url for url in requests) == 2 and set(q2.values()) <= set(requests)
    generation, events = published(tmp_path / "hosted")
    for table in house.TABLES.values():
        after = fixture._table_rows(generation, table)
        assert {row["report_quarter"] for row in after} == {1, 2}
        assert [{**row, "normalization_receipt_sha256": None} for row in after if row["report_quarter"] == 1] == [
            {**row, "normalization_receipt_sha256": None}
            for row in fixture._table_rows(tmp_path / "first" / "generation", table)]
    stages = {event.get("stage") for event in events}
    assert {"house-acquisition-journal", "house-csv-original"} <= stages
    assert sum(event.get("blob_store") == "central-raw" for event in events) == 4
