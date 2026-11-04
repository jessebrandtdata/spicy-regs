"""Operational guards for an additive HRC publication candidate."""

from copy import deepcopy
from hashlib import sha256
from uuid import uuid4

import pytest

from prepare_hrc import canonical, check_public_evidence, check_scopes, checked_source
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.source_evidence import CaptureEvidence
from validate_hrc_candidate import value_readback


@pytest.fixture(autouse=True)
def deny_shared_archive(monkeypatch, tmp_path):
    """Apply to this research collection root, including integration markers."""
    from pathlib import Path
    from spicy_docs.storage import raw_archive

    original = raw_archive.CentralRawArchiveStore

    def forbidden_default():
        raise AssertionError('Synthetic research captures require an explicit temporary archive')

    def isolated(root, *args, **kwargs):
        assert Path(root).resolve().is_relative_to(tmp_path.resolve()), 'Shared archive writer denied'
        return original(root, *args, **kwargs)

    monkeypatch.delenv('SPICYGOV_CATALOG_REQUEST', raising=False)
    monkeypatch.setenv('SPICYGOV_RAW_ROOT', str(tmp_path / 'central-raw'))
    monkeypatch.setattr(raw_archive, 'default_raw_archive_root', forbidden_default)
    monkeypatch.setattr(raw_archive, 'CentralRawArchiveStore', isolated)


def test_missing_explicit_archive_cannot_fall_back_to_shared_writer(tmp_path):
    with pytest.raises(AssertionError, match='explicit temporary archive'):
        CaptureEvidence(tmp_path, 'scorecards')


@pytest.mark.parametrize('outcome', ['complete', 'refused', 'exception'])
def test_offline_capture_lifecycle_stays_isolated(tmp_path, outcome):
    archive = tmp_path / 'central-raw'
    evidence = CaptureEvidence(tmp_path, 'scorecards', archive_root=archive)
    scope = evidence.for_source('hrc', 'full', parser_version='fixture/1', policy_decision_id='explicit-fixture-only')
    scope.capture(CapturedBodyResponse('https://example.org/hrc.pdf', 'https://example.org/hrc.pdf',
                  200, 'application/pdf', '2026-10-03T22:43:07Z', b'%PDF-fixture'), stage='pdf')
    if outcome == 'refused':
        scope.refusal(ValueError('synthetic source refusal'), stage='pdf')
    evidence.finish(RuntimeError('synthetic interruption') if outcome == 'exception' else None)
    assert list((archive / 'acquisitions/sha256').glob('*/*'))


def source_receipt(body):
    return {
        "requested_url": "https://example.org/hrc.pdf",
        "resolved_url": "https://example.org/hrc.pdf",
        "status_code": 200,
        "content_type": "application/pdf",
        "observed_at": "2026-10-03T22:43:07Z",
        "byte_size": len(body),
        "sha256": "sha256:" + sha256(body).hexdigest(),
    }


def test_retained_pdf_requires_recorded_status_and_exact_bytes():
    body = b"%PDF-1.7\nsource facts\n%%EOF"
    receipt = source_receipt(body)
    assert checked_source(body, receipt).body == body
    without_status = {key: value for key, value in receipt.items() if key != "status_code"}
    without_status["status"] = "complete_http_capture"
    with pytest.raises(ValueError, match="HTTP evidence"):
        checked_source(body, without_status)
    with pytest.raises(ValueError, match="HTTP evidence"):
        checked_source(body + b"changed", receipt)


def scopes():
    prior = {
        "scorecard_publishers": [{"publisher_id": "lcv", "name": "LCV"}],
        "scorecards": [{"publisher_id": "lcv", "scorecard_id": "lcv:2025", "snapshot_id": "old"}],
        "scorecard_member_ratings": [{"scorecard_id": "lcv:2025", "value_text": "0", "capture_id": "old"}],
    }
    reference = {
        "scorecard_publishers": [{"publisher_id": "hrc", "name": "HRC"}],
        "scorecards": [{"publisher_id": "hrc", "scorecard_id": "hrc:118-final", "snapshot_id": "reference"}],
        "scorecard_member_ratings": [
            {
                "scorecard_id": "hrc:118-final",
                "value_text": "N/A",
                "capture_id": "reference",
                "source_path": f"pdf:page=22;observation={uuid4()};/members/4/record/ratings/0",
            }
        ],
    }
    current = {name: deepcopy(prior[name] + reference[name]) for name in prior}
    current["scorecard_member_ratings"][-1]["capture_id"] = "new-capture"
    current["scorecard_member_ratings"][-1]["source_path"] = (
        f"pdf:page=22;observation={uuid4()};/members/4/record/ratings/0"
    )
    return prior, current, reference


def test_only_opaque_hrc_observation_identity_may_differ():
    prior, current, reference = scopes()
    editions, kept = check_scopes(prior, current, reference)
    assert editions == {"hrc:118-final"}
    assert set(kept.values()) == {1}


@pytest.mark.parametrize("field,value", [("value_text", "100"), ("capture_id", "different-old-capture")])
def test_unselected_scope_preserves_every_field(field, value):
    prior, current, reference = scopes()
    current["scorecard_member_ratings"][0][field] = value
    with pytest.raises(ValueError, match="Unselected source rows changed"):
        check_scopes(prior, current, reference)


@pytest.mark.parametrize("value", ["NA", "100", ""])
def test_hrc_admitted_na_matches_the_qualified_reference(value):
    prior, current, reference = scopes()
    current["scorecard_member_ratings"][-1]["value_text"] = value
    with pytest.raises(ValueError, match="qualified source reference"):
        check_scopes(prior, current, reference)


def test_locator_comparison_preserves_page_and_source_record():
    prior, current, reference = scopes()
    current["scorecard_member_ratings"][-1]["source_path"] = current["scorecard_member_ratings"][-1][
        "source_path"
    ].replace("page=22", "page=23")
    with pytest.raises(ValueError, match="qualified source reference"):
        check_scopes(prior, current, reference)
    assert canonical([{"notes_text": str(uuid4())}], omit_observation_ids=True) != canonical(
        [{"notes_text": str(uuid4())}], omit_observation_ids=True
    )


def test_hash_only_pdf_and_derived_asset_have_no_public_bodies(tmp_path):
    evidence = CaptureEvidence(tmp_path, "scorecards", archive_root=tmp_path / "central-raw")
    scope = evidence.for_source("hrc", "hash_only", parser_version="fixture/1", policy_decision_id="hash-only")
    pdf = b"%PDF-1.7\nPRIVATE_ORIGINAL_PDF_BYTES\n%%EOF"
    asset = b'{"private_model_output":"PRIVATE_DERIVED_OBSERVATION_BYTES"}'
    scope.capture(
        CapturedBodyResponse(
            "https://example.org/hrc.pdf",
            "https://example.org/hrc.pdf",
            200,
            "application/pdf",
            "2026-10-03T22:43:07Z",
            pdf,
        ),
        stage="original-pdf",
    )
    scope.retain_bytes(asset, stage="hrc-qualified-observations")
    evidence.inputs()
    _, count = check_public_evidence(evidence.artifact_dir)
    assert count == 1
    public = b"".join(path.read_bytes() for path in evidence.artifact_dir.rglob("*") if path.is_file())
    assert b"PRIVATE_ORIGINAL_PDF_BYTES" not in public
    assert b"PRIVATE_DERIVED_OBSERVATION_BYTES" not in public


def test_full_body_evidence_cannot_qualify_for_this_deployment(tmp_path):
    evidence = CaptureEvidence(tmp_path, "scorecards", archive_root=tmp_path / "central-raw")
    scope = evidence.for_source("hrc", "full", parser_version="fixture/1", policy_decision_id="explicit-fixture-only")
    scope.capture(
        CapturedBodyResponse(
            "https://example.org/hrc.pdf",
            "https://example.org/hrc.pdf",
            200,
            "application/pdf",
            "2026-10-03T22:43:07Z",
            b"%PDF-fixture",
        ),
        stage="pdf",
    )
    evidence.inputs()
    with pytest.raises(ValueError, match="body member"):
        check_public_evidence(evidence.artifact_dir)


def readback_fixture(raw_rating="NA", raw_result="NA"):
    asset = {
        "members": [
            {"record": {"ratings": [{"value_text": raw_rating}], "item_results": [{"result_text": raw_result}]}}
        ]
    }
    tables = {
        "scorecard_member_ratings": [
            {
                "source_path": "pdf:page=1;/members/0/record/ratings/0",
                "value_text": "N/A",
                "value_number": None,
                "publisher_member_key": "member",
                "metric_id": "period",
            }
        ],
        "scorecard_member_item_results": [
            {"source_path": "pdf:page=1;/members/0/record/item_results/0", "result_text": raw_result}
        ],
    }
    return asset, tables


def test_raw_na_normalizes_in_ratings_only():
    asset, tables = readback_fixture()
    report = value_readback(asset, tables)
    assert report["normalization_count"] == 1
    assert asset["members"][0]["record"]["ratings"][0]["value_text"] == "NA"
    tables["scorecard_member_item_results"][0]["result_text"] = "N/A"
    with pytest.raises(ValueError, match="permitted rating mapping"):
        value_readback(asset, tables)


@pytest.mark.parametrize("raw", [" NA", "NA ", "na", "N.A."])
def test_no_additional_rating_normalization_is_permitted(raw):
    asset, tables = readback_fixture(raw_rating=raw)
    with pytest.raises(ValueError, match="permitted rating mapping"):
        value_readback(asset, tables)


def test_raw_readback_requires_every_observation_once():
    asset, tables = readback_fixture()
    tables["scorecard_member_ratings"] = []
    with pytest.raises(ValueError, match="omits source"):
        value_readback(asset, tables)
    asset, tables = readback_fixture()
    tables["scorecard_member_ratings"] *= 2
    with pytest.raises(ValueError, match="duplicate source"):
        value_readback(asset, tables)
