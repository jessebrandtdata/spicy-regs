"""Exercise CMS source admission and the domain build together, entirely offline."""

import json
import sys
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from rulespec_artifacts import ArtifactPin, LocalMemberSource, Producer
from spicy_docs.source_native import SourceNativeReleaseBuild, SourceNativeReleasePublisher, SourceNativeReleaseReader
from spicy_docs.sources.cms import CATALOG_URL, CmsError, cms_selection, page_url, resolve_release
from spicy_docs.sources.cms_profile import CMS_PROFILE, cms_scope, iter_retained_pages
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.pipelines import cms_drug_spending, cms_medicaid, cms_part_b, cms_part_d
from tests.test_cms_part_b import EXCERPTS
from tests.test_cms_part_d import raw
from tests.published_paths import local_paths

NOW = datetime(2026, 10, 20, tzinfo=UTC)
IMPLEMENTATION = "git+https://example.test/cms@" + "a" * 40
PRODUCER = Producer(
    product="spicy-docs",
    implementation_id=IMPLEMENTATION,
    verifier_id="urn:spicy-regs:source-native-release-verifier",
    verifier_version="2.0",
    verifier_implementation_id=IMPLEMENTATION,
)
API = "https://data.cms.gov/data-api/v1/dataset/"
SERIES = "11111111-1111-1111-1111-111111111111"
RELEASE = "22222222-2222-2222-2222-222222222222"


def make_release(tmp_path, program, policy, *, catalog_padding=0):
    title = {
        "d": "Medicare Part D Spending by Drug",
        "m": "Medicaid Spending by Drug",
        "b": "Medicare Part B Spending by Drug",
    }[program]
    series_id, dataset_id = API + SERIES + "/data", API + SERIES + "/data-viewer"
    api = API + RELEASE + "/data"
    catalog = {
        "conformsTo": {"identifier": "https://resources.data.gov/dcat-us/3.0.0"},
        "datasetSeries": [{"@id": series_id, "title": title, "last": {"@id": dataset_id}}],
        "dataset": [
            {
                "@id": dataset_id,
                "inSeries": [{"@id": series_id}],
                "temporal": [{"startDate": "2024-01-01"}],
                "modified": "2026-06-25",
                "distribution": [
                    {"format": "API", "accessURL": api, "describedBy": {"downloadURL": API + RELEASE + "/dictionary"}}
                ],
            }
        ],
        "padding": "x" * catalog_padding,
    }
    if program == "b":
        rows = EXCERPTS["rows"]
    else:
        rows = [raw(), raw("Maker")]
        if program == "m":
            for row in rows:
                row.pop("Tot_Benes_2024")
                row.pop("Avg_Spnd_Per_Bene_2024")
    dictionary = {"data": {"fields": [{"name": n} for n in rows[0]]}}

    def capture(url, value):
        return CapturedBodyResponse(url, url, 200, "application/json", NOW.isoformat(), json.dumps(value).encode())

    selected = resolve_release(catalog, series_title=title)
    controls = [
        capture(CATALOG_URL, catalog),
        capture(selected["dictionary_url"], dictionary),
        capture(api + "/stats", {"found_rows": len(rows), "total_rows": len(rows)}),
    ]
    selection = cms_selection(*controls, series_title=title)
    pages = [capture(page_url(selection, size=2), rows)]
    blobs = LocalSourceNativeBlobStore(tmp_path / "blobs")
    for c in controls + pages:
        blobs.put_blob(c.sha256, c.byte_size, [c.body])
    scope = cms_scope(selection, controls, pages)
    # This reproduces the historical version on the old source code; the source
    # policy body is unchanged for the two pre-Part-B series.
    profile = replace(CMS_PROFILE, acquisition_policy_version=policy)
    published = SourceNativeReleasePublisher(profile, blob_store=blobs, clock=lambda: NOW).publish(
        iter_retained_pages(scope, blob_source=blobs),
        destination=tmp_path / "release",
        build=SourceNativeReleaseBuild(
            query_scope=scope, producer=PRODUCER, started_at=NOW.isoformat().replace("+00:00", "Z")
        ),
    )
    return published, blobs, controls


def open_reader(published, blobs, policy):
    from spicy_docs.sources.cms_profile import cms_profile_for_policy

    return SourceNativeReleaseReader(
        LocalMemberSource(published.root),
        blob_source=blobs,
        profile=cms_profile_for_policy(policy),
        expected_pin=published.artifact.pin,
        accepted_verifier_implementation_ids=frozenset({IMPLEMENTATION}),
    )


@pytest.mark.parametrize(
    ("program", "policy", "pipeline"),
    [
        ("d", "1.0", cms_part_d),
        ("m", "1.0", cms_medicaid),
        ("b", "1.1", cms_part_b),
        ("d", "1.1", cms_part_d),
        ("m", "1.1", cms_medicaid),
    ],
)
def test_admitted_release_through_pipeline(tmp_path, program, policy, pipeline):
    published, blobs, _ = make_release(tmp_path, program, policy)
    reader = open_reader(published, blobs, policy)
    artifact, evidence = pipeline.build(reader, tmp_path / "output", year=2024)
    assert artifact.pin.artifact_digest and evidence.exists()
    assert local_paths(tmp_path / "output" / "generation", evidence, also=(str(tmp_path),)) == []
    assert reader.collection_outcome["acquisitionPolicyVersion"] == policy
    assert reader.pin == published.artifact.pin


@pytest.mark.parametrize(
    ("program", "policy", "pipeline"),
    [("d", "1.0", cms_part_d), ("m", "1.0", cms_medicaid), ("b", "1.1", cms_part_b), ("b", None, cms_part_b)],
)
def test_selection_cli_uses_explicit_policy(tmp_path, monkeypatch, capsys, program, policy, pipeline):
    published, blobs, _ = make_release(tmp_path, program, policy or "1.1")
    pin = published.artifact.pin
    selection = tmp_path / "selection.json"
    selection.write_text(
        json.dumps(
            {
                "benefit_year": 2024,
                "source": {
                    "release": str(published.root),
                    "blobs": str(blobs.root),
                    "logicalId": pin.logical_id,
                    "artifactDigest": pin.artifact_digest,
                    "verifierImplementationId": IMPLEMENTATION,
                    **({"acquisitionPolicyVersion": policy} if policy is not None else {}),
                },
            }
        )
    )
    monkeypatch.setattr(sys, "argv", ["cms", "--selection", str(selection), "--output-dir", str(tmp_path / "output")])
    cms_drug_spending.main(pipeline.build, "test")
    assert json.loads(capsys.readouterr().out)["artifactDigest"].startswith("sha256:")


def test_old_policy_rejects_wrong_version_and_part_b(tmp_path):
    from spicy_docs.sources.cms_profile import cms_profile_for_policy

    published, blobs, _ = make_release(tmp_path / "part-d", "d", "1.0")
    with pytest.raises(ValueError, match="policy version"):
        open_reader(published, blobs, "1.1")
    with pytest.raises(CmsError, match="Unsupported"):
        cms_profile_for_policy("1.2")
    b_release, b_blobs, _ = make_release(tmp_path / "part-b", "b", "1.0")
    with pytest.raises(ValueError, match="does not include Part B"):
        open_reader(b_release, b_blobs, "1.0")


def test_wrong_policy_digest_pin_and_verifier_refuse(tmp_path):
    from spicy_docs.sources.cms_profile import cms_profile_for_policy

    published, blobs, _ = make_release(tmp_path, "d", "1.0")
    profile = cms_profile_for_policy("1.0")
    wrong_policy = replace(profile, acquisition_policy=lambda scope: {"changed": scope})

    def load(selected_profile=profile, pin=published.artifact.pin, verifier=IMPLEMENTATION):
        return SourceNativeReleaseReader(
            LocalMemberSource(published.root),
            blob_source=blobs,
            profile=selected_profile,
            expected_pin=pin,
            accepted_verifier_implementation_ids=frozenset({verifier}),
        )

    with pytest.raises(ValueError, match="policy digest"):
        load(wrong_policy)
    with pytest.raises(ValueError):
        load(pin=ArtifactPin(published.artifact.pin.logical_id, "sha256:" + "0" * 64))
    with pytest.raises(ValueError, match="verifier implementation"):
        load(verifier="other")


def test_catalog_over_source_bound_refuses(tmp_path):
    from spicy_docs.sources.cms_profile import CMS_PROFILE

    published, blobs, _ = make_release(tmp_path, "d", "1.1")
    reader = open_reader(published, blobs, "1.1")
    scope = reader.collection_outcome["requestedScope"]
    scope["captures"][0]["byte_size"] = 64 * 1024 * 1024
    assert CMS_PROFILE.validate_query_scope(scope) == scope
    scope["captures"][0]["byte_size"] = 64 * 1024 * 1024 + 1
    with pytest.raises(CmsError, match="inventory"):
        CMS_PROFILE.validate_query_scope(scope)


def test_large_admitted_catalog_streamed_to_evidence(tmp_path):
    published, blobs, controls = make_release(tmp_path, "d", "1.1", catalog_padding=24 * 1024 * 1024)
    reader = open_reader(published, blobs, "1.1")
    assert controls[0].byte_size > 24 * 1024 * 1024
    artifact, evidence = cms_part_d.build(reader, tmp_path / "output", year=2024)
    retained = evidence / "blobs" / "sha256" / controls[0].sha256.removeprefix("sha256:")
    assert artifact.pin.artifact_digest and retained.read_bytes() == controls[0].body


def test_stream_corruption_refuses_without_generation(tmp_path):
    published, blobs, controls = make_release(tmp_path, "d", "1.1")
    reader = open_reader(published, blobs, "1.1")
    path = blobs.root / "sha256" / controls[0].sha256.removeprefix("sha256:")
    path.write_bytes(b"!" * controls[0].byte_size)
    with pytest.raises(ValueError, match="digest"):
        cms_part_d.build(reader, tmp_path / "output", year=2024)
    assert not (tmp_path / "output" / "generation").exists()
