"""A selected native printing can gain date evidence without changing body or descendants."""

import json
from dataclasses import replace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.legislative_documents import map_subject, subject_schema
from spicy_regs.relationship_views.bill_publication import BILL_PUBLICATION_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.transforms.bill_publication import enrich_bill_publication, refresh_bill_publication
from tests.test_bill_family import _priors, StubBodyAcquirer, StubBulkAcquirer
from tests.test_bill_family import scoped as fixture_scope
from tests.test_bill_family_order import _run, _rows

scoped = fixture_scope

PACKAGE = "BILLS-119hjres25enr"
URL = f"https://www.govinfo.gov/content/pkg/{PACKAGE}/xml/{PACKAGE}.xml"


def capture(url, body, observed="2026-11-06T18:00:00Z"):
    return CapturedBodyResponse(url, url, 200, "application/xml", observed, body)


class Metadata:
    def __init__(self, *, issued="2025-03-26", changed=False, observed="2026-11-06T18:00:00Z"):
        summary = json.dumps({"packageId": PACKAGE, "collectionCode": "BILLS", "dateIssued": issued,
                              "lastModified": "2026-07-10T15:22:16Z"}).encode()
        self.captures = (
            capture(f"https://api.govinfo.gov/packages/{PACKAGE}/summary", summary, observed),
            capture(f"https://api.govinfo.gov/packages/{PACKAGE}/mods", f'''<mods xmlns="http://www.loc.gov/mods/v3">
                <extension><accessId>{PACKAGE}</accessId><collectionCode>BILLS</collectionCode>
                <dateIngested>2025-03-27</dateIngested></extension>
                <originInfo><dateIssued encoding="w3cdtf">{issued}</dateIssued></originInfo></mods>'''.encode(), observed),
            capture(f"https://api.govinfo.gov/packages/{PACKAGE}/summary", summary + (b" " if changed else b""), observed),
        )
        self.requested = []

    def acquire_bill_metadata(self, package_id):
        self.requested.append(package_id)
        return self.captures


def row():
    return dict.fromkeys(TABLE_CONTRACTS["bill_versions"].columns) | {
        "bill_id": "119-hjres-25", "version_code": "enrolled-bill", "source": "govinfo", "version_date": "",
        "package_id": PACKAGE, "sha256": "sha256:690ca61f34b0927bf8da34addcf721e4fd704c22c98cf99aaccfbd26e31889e4",
        "resolved_url": URL, "observed_at": "2026-09-12T12:29:18.332855+00:00", "section_count": "4",
    }


def test_metadata_only_refresh_native_consumer_keeps_empty_enrolled_action_date_and_body_identity(tmp_path):
    path = tmp_path / "bill_versions.parquet"
    schema = pa.schema([(name, pa.string()) for name in TABLE_CONTRACTS["bill_versions"].columns])
    pq.write_table(pa.Table.from_pylist([row()], schema=schema), path)
    source = Metadata()
    assert refresh_bill_publication(path, package_ids=(PACKAGE,), metadata_source=source)[0]["status"] == "enriched_body_reused"
    [enriched] = pq.read_table(path).to_pylist()
    assert {key: enriched[key] for key in row() if not key.startswith("publication_")} == {
        key: value for key, value in row().items() if not key.startswith("publication_")
    }
    native = map_subject("bill_versions", enriched)
    con = duckdb.connect()
    con.register("bill_versions", pa.Table.from_pylist([native], schema=subject_schema("bill_versions")))
    installed = install_sql_views(con, ["bill_versions"], BILL_PUBLICATION_VIEWS)
    assert installed["bill_text_publication_dates"]["status"] == "available"
    actual = con.execute("SELECT action_version_date, body_version_id, native_value, native_precision, evidence_url "
                         "FROM bill_text_publication_dates WHERE native_field='SUMMARY/dateIssued'").fetchone()
    assert actual == ("", "body:" + enriched["sha256"], "2025-03-26", "day", source.captures[0].resolved_url)
    before = path.read_bytes()
    assert refresh_bill_publication(path, package_ids=(PACKAGE,), metadata_source=source)[0]["status"] == "unchanged_evidence_reused"
    assert path.read_bytes() == before
    assert source.requested == [PACKAGE, PACKAGE]


def test_changed_and_older_metadata_preserve_selection_and_exact_row():
    [enriched], _ = enrich_bill_publication([row()], metadata_source=Metadata(), max_packages=1)
    updates, outcomes = enrich_bill_publication([enriched], metadata_source=Metadata(changed=True), max_packages=1)
    assert updates == [] and outcomes[0]["status"] == "summary_changed_during_acquisition"
    updates, outcomes = enrich_bill_publication([enriched], metadata_source=Metadata(issued="2025", observed="2026-10-01T00:00:00Z"), max_packages=1)
    assert updates == [] and outcomes[0]["status"] == "older_observation_preserved"


def test_wrong_printing_metadata_and_unbounded_selection_refuse_before_update():
    source = Metadata()
    source.captures = (source.captures[0], replace(source.captures[1], body=source.captures[1].body.replace(
        PACKAGE.encode(), b"BILLS-119hjres25ih")), source.captures[2])
    with pytest.raises(ValueError, match="accessId"):
        enrich_bill_publication([row()], metadata_source=source, max_packages=1)
    with pytest.raises(ValueError, match="max_packages"):
        enrich_bill_publication([row()], metadata_source=source, max_packages=0)


def test_ordinary_builder_enriches_held_body_without_reextracting_sections(tmp_path, scoped):
    from spicy_regs.transforms.build_bill_family import build_bill_family

    prior = _run(tmp_path / "prior", StubBulkAcquirer(), StubBodyAcquirer())
    target = "BILLS-119hr6028ih"

    class SelectedMetadata(Metadata):
        def acquire_bill_metadata(self, package_id):
            assert package_id == target
            return tuple(replace(capture, requested_url=capture.requested_url.replace(PACKAGE, target),
                                 resolved_url=capture.resolved_url.replace(PACKAGE, target),
                                 body=capture.body.replace(PACKAGE.encode(), target.encode())) for capture in self.captures)

    output = tmp_path / "refresh"
    output.mkdir()
    body = StubBodyAcquirer()
    paths = {path.stem: path for path in build_bill_family(
        output, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, max_version_fetches=0,
        publication_package_ids=(target,), publication_metadata_acquirer=SelectedMetadata(),
        **_priors(tmp_path / "prior"),
    )}
    assert body.requested == []
    assert _rows(paths, "bill_sections") == _rows(prior, "bill_sections")
    old = next(row for row in _rows(prior, "bill_versions") if row["package_id"] == target)
    new = next(row for row in _rows(paths, "bill_versions") if row["package_id"] == target)
    assert new["sha256"] == old["sha256"] and new["observed_at"] == old["observed_at"]
    assert new["publication_evidence_status"] == "stable_summary_bracket_body_unverified"
