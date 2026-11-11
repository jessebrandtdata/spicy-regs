"""A prepared publication reuses exact admitted bytes and repeats proof, never the source writer."""

import json
from pathlib import Path

import duckdb
import pytest
from rulespec_artifacts import canonical_json_bytes

from spicy_regs import native_conversion as conversion
from spicy_regs.sources import publication
from tests.test_native_conversion import (
    BASE, BUCKET, CFR, COMMON, DOCKET, MAIN, PINNED_WHEEL, STATE, WHEEL,
    bucket as _bucket, cfr_on_its_receipt_route as _cfr_on_its_receipt_route,
    convert, prepared_digest, publish_old, publish_old_with_evidence, publish_split_sections, stored,
)

bucket = _bucket  # Expose the existing hermetic fixture to pytest in this module.
#: cfr-sections on its former receipt route, this file's example family (autouse; tests/test_native_conversion.py).
cfr_on_its_receipt_route = _cfr_on_its_receipt_route


def publish_prepared(path, family="cfr-sections", **kwargs):
    kwargs.setdefault("expected_receipt_digest", prepared_digest(path))
    return conversion.publish_prepared(path, allowed=[family], expected_main=MAIN, expected_spicy_docs=WHEEL,
                                       expect_bucket=BUCKET, state=lambda _: dict(STATE), **kwargs)


@pytest.mark.parametrize("family", ["cfr-sections", "courtlistener", "bill-family"])
def test_prepared_publication_reuses_exact_generation_without_running_writer(tmp_path, monkeypatch, bucket, family):
    if family == "bill-family":
        old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    else:
        tables = {"cfr_sections": CFR} if family == "cfr-sections" else {"court_dockets": [DOCKET]}
        old = publish_old(bucket, monkeypatch, tmp_path, family, tables)
    work = tmp_path / "work"
    dry = convert(family, work)
    generation = Path(dry["generation"]["directory"])
    original = {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()}

    def no_writer(*args, **kwargs):
        raise AssertionError("Prepared publication must never run the source writer")

    monkeypatch.setattr(conversion, "_convert_rollup", no_writer)
    monkeypatch.setattr(conversion, "_convert_court", no_writer)
    done = publish_prepared(work / conversion.RECEIPT, family)
    assert done["generation"] == dry["generation"]
    assert done["tables"] == dry["tables"]
    assert done["published"]["entry"]["artifactDigest"] == dry["generation"]["artifactDigest"]
    assert {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()} == original
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"][family] == old


@pytest.mark.parametrize("damage", [
    "unsealed", "refused", "published", "attempted", "source", "proof", "resealed-proof",
    "snapshot", "retained", "subject", "artifact", "missing-artifact",
])
def test_prepared_changes_or_incomplete_proofs_refuse_before_upload(tmp_path, monkeypatch, bucket, damage):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    receipt = convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    if damage in {"unsealed", "refused", "published", "attempted", "source", "proof", "resealed-proof"}:
        if damage == "unsealed":
            receipt.pop("prepared")
        elif damage == "refused":
            receipt["refused"] = []
        elif damage == "published":
            receipt["published"] = {}
        elif damage == "attempted":
            receipt["publish_attempt"] = {}
        elif damage == "source":
            receipt["source"]["checkout"] = "1a" * 20
        else:
            receipt["tables"]["cfr_sections"]["rows_only_in_retained"] = 7
        if damage not in {"unsealed", "proof"}:
            receipt["prepared"]["receiptDigest"] = conversion._prepared_digest(receipt)
        path.write_text(json.dumps(receipt))
    else:
        generation = Path(receipt["generation"]["directory"])
        target = {"snapshot": work / "captured-publication.v2.json", "retained": work / "retained/cfr_sections.parquet",
                  "subject": generation / "cfr_sections.parquet", "artifact": generation / "artifact.json",
                  "missing-artifact": generation / "artifact.json"}[damage]
        if damage == "missing-artifact":
            target.unlink()
        else:
            target.write_bytes(target.read_bytes() + b"changed")
    before = list(bucket.writes)
    with pytest.raises((conversion.ConversionRefused, ValueError)):
        publish_prepared(path)
    assert bucket.writes == before and stored(bucket) == old


def test_prepared_allow_runtime_and_target_remain_explicit(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    before = list(bucket.writes)
    for changes in ({"allowed": []}, {"expect_bucket": "other"}, {"state": lambda _: STATE | {"uncommitted": ["changed"]}},
                    {"state": lambda _: STATE | {"main": "ff" * 20}}, {"state": lambda _: STATE | {"spicy_docs": "other"}}):
        options: dict = dict(allowed=["cfr-sections"], expect_bucket=BUCKET, state=lambda _: dict(STATE)) | changes
        with pytest.raises(conversion.ConversionRefused):
            conversion.publish_prepared(path, expected_main=MAIN, expected_spicy_docs=WHEEL,
                                        expected_receipt_digest=prepared_digest(path), **options)
    assert bucket.writes == before


BULK_FAILURES = [
    pytest.param(duckdb.InvalidInputException("date32 outside Python date range"), True, id="invalid-input"),
    pytest.param(duckdb.OutOfMemoryException("injected"), False, id="operational"),
]


@pytest.mark.parametrize("step", ["publish", "resume"])
@pytest.mark.parametrize("failure, refused", BULK_FAILURES)
def test_prepared_steps_report_a_bulk_failure_by_its_kind(tmp_path, monkeypatch, bucket, step, failure, refused):
    """Invalid input data refuses the step; an operational failure does not. Nothing is uploaded either way."""
    from spicy_regs import etl_bulk

    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    generation = Path(convert("cfr-sections", work)["generation"]["directory"])
    if step == "resume":
        (work / conversion.RECEIPT).unlink()

    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(etl_bulk, "_check_receipts", fail)
    run = {
        "publish": lambda: publish_prepared(work / conversion.RECEIPT, full_verification=True),
        "resume": lambda: conversion.resume_prepared(
            generation,
            work=work,
            allowed=["cfr-sections"],
            expected_main=MAIN,
            expected_spicy_docs=WHEEL,
            expected_producer=MAIN,
            state=lambda _: dict(STATE),
        ),
    }[step]
    before = list(bucket.writes)
    if refused:
        with pytest.raises(
            conversion.ConversionRefused, match=r"cannot be verified: BulkInputError: .*no row fallback"
        ):
            run()
    else:
        with pytest.raises(etl_bulk.BulkValidationError, match="no row fallback"):
            run()
    assert bucket.writes == before


@pytest.mark.parametrize("step", ["publish", "resume"])
def test_prepared_steps_refuse_non_current_inherited_evidence_before_hashing_retained_tables(
    tmp_path, monkeypatch, bucket, step
):
    """The evidence the replaced generation hands on is read first: a runtime that cannot inherit it stops early.

    The runtime moves to another evidence version after the dry run, so the prior's evidence is no longer readable.
    """
    from spicy_regs import source_evidence

    publish_old_with_evidence(tmp_path, monkeypatch, bucket, "courtlistener", "court_dockets", [DOCKET])
    work = tmp_path / "work"
    generation = Path(convert("courtlistener", work)["generation"]["directory"])
    if step == "resume":
        (work / conversion.RECEIPT).unlink()
        identity = json.loads((generation / "artifact.json").read_bytes())["producer"]["implementationId"]
        locked = {"version": WHEEL, "wheels": [{"hash": PINNED_WHEEL["locked_sha256"]}]}
        monkeypatch.setattr(conversion, "_historical_producer", lambda *_: (identity, locked))
    monkeypatch.setattr(source_evidence, "EVIDENCE_VERSION", source_evidence.EVIDENCE_VERSION + 1)

    def hashed(*args, **kwargs):
        raise AssertionError("Retained tables were hashed before the inherited evidence was checked")

    monkeypatch.setattr(conversion, "_retained_inputs", hashed)
    run = {
        "publish": lambda: publish_prepared(work / conversion.RECEIPT, "courtlistener"),
        "resume": lambda: conversion.resume_prepared(
            generation,
            work=work,
            allowed=["courtlistener"],
            expected_main=MAIN,
            expected_spicy_docs=WHEEL,
            expected_producer=MAIN,
            state=lambda _: dict(STATE),
        ),
    }[step]
    before = list(bucket.writes)
    with pytest.raises(
        publication.PublicationError if step == "resume" else conversion.ConversionRefused,
        match="Inherited source evidence refused: .*Rebuild the family",
    ):
        run()
    assert bucket.writes == before


@pytest.mark.parametrize("version", ["absent", 1])
def test_prepared_publication_of_a_family_whose_evidence_predates_version_2_hands_nothing_on(
    tmp_path, monkeypatch, bucket, version
):
    """The prepared route reads the earlier evidence the way the dry run did: nothing handed on, version 2 written."""
    remainder = {"table": "treaties", "unevidenced": [["119-1"]], "shape_version": 2}
    publish_old_with_evidence(
        tmp_path,
        monkeypatch,
        bucket,
        "treaties",
        "treaties",
        [{"treaty_id": "119-1"}],
        evidence_version=None if version == "absent" else version,
        unversioned=version == "absent",
        congress_index_selection=remainder,
    )
    work = tmp_path / "work"
    dry = convert("treaties", work)

    done = publish_prepared(work / conversion.RECEIPT, "treaties")

    assert dry["lineage"]["handed_on_events"] == done["lineage"]["handed_on_events"] == 0
    assert done["published"]["entry"]["artifactDigest"] == dry["generation"]["artifactDigest"]


def test_prepared_publication_reports_the_local_mirror_in_its_receipt(tmp_path, monkeypatch, bucket):
    """Owner 2026-10-31: a mirror outcome is reported in the publish result; a failure never changes the exit."""
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    root = tmp_path / "mirror"
    root.write_bytes(b"not a directory")  # every mirror write fails
    monkeypatch.setenv(publication.PUBLICATION_MIRROR, str(root))
    done = publish_prepared(work / conversion.RECEIPT, "cfr-sections")
    mirror = done["publish_attempt"]["mirror"]
    assert done["publish_attempt"]["outcome"] == "published"
    assert mirror["prefix"] == done["published"]["entry"]["prefix"] and "error" in mirror
    assert json.loads((work / conversion.RECEIPT).read_bytes())["publish_attempt"]["mirror"] == mirror


@pytest.mark.parametrize("when", ["before-upload", "pointer-retry"])
def test_prepared_exact_prior_includes_timestamp_on_every_pointer_attempt(tmp_path, monkeypatch, bucket, when):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    changed = old | {"publishedAt": "2001-01-01T00:00:00Z"}

    def change(key):
        if key == publication.INDEX_V2_KEY:
            index = publication.parse_index(bucket.objects[key])
            index["families"]["cfr-sections"] = changed
            bucket.objects[key] = canonical_json_bytes(index)

    before = list(bucket.writes)
    if when == "before-upload":
        change(publication.INDEX_V2_KEY)
    else:
        bucket.before_put = change
    with pytest.raises(conversion.ConversionRefused):
        publish_prepared(work / conversion.RECEIPT)
    assert stored(bucket) == changed
    if when == "before-upload":
        assert bucket.writes == before


def test_prepared_cli_and_second_publication_refusal(tmp_path, monkeypatch, bucket, capsys):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda _: dict(STATE))
    work = tmp_path / "work"
    convert("cfr-sections", work)
    args = ["--publish-prepared", str(work / conversion.RECEIPT), *COMMON, "--expect-bucket", BUCKET,
            "--expect-receipt-digest", prepared_digest(work / conversion.RECEIPT)]
    assert conversion.main(args) == 0
    assert "published and read back" in capsys.readouterr().out
    before = list(bucket.writes)
    assert conversion.main(args) == 1
    assert bucket.writes == before


def _prepared_with_history(tmp_path, monkeypatch, bucket, history):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    receipt = convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    if history == "published":
        publish_prepared(path)
    else:
        receipt["publish_attempt"] = {"outcome": "attempted", "error": None}
        path.write_text(json.dumps(receipt))
    return path


def _assert_no_new_attempt(state, path):
    assert "no new publication attempt" in state
    assert "read-only" in state and str(path) in state
    assert conversion.NOTHING_PUBLISHED not in state


@pytest.mark.parametrize("history", ["attempted", "published"])
@pytest.mark.parametrize("refusal", ["journal", "main", "wheel", "environment"])
def test_prepared_cli_prior_history_never_claims_an_unchanged_pointer(
    tmp_path, monkeypatch, bucket, capsys, history, refusal,
):
    path = _prepared_with_history(tmp_path, monkeypatch, bucket, history)
    state = dict(STATE)
    if refusal == "main":
        state["main"] = "be" * 20
    elif refusal == "wheel":
        state["spicy_docs_wheel"] = STATE["spicy_docs_wheel"] | {"installed_differs": ["changed.py"]}
    monkeypatch.setattr(conversion, "source_state", lambda _: state)
    args = ["--publish-prepared", str(path), *COMMON, "--expect-bucket", BUCKET,
            "--expect-receipt-digest", prepared_digest(path)]
    if refusal == "environment":
        env = tmp_path / "conflicting.env"
        env.write_text("R2_PUBLIC_URL=https://another.invalid\n")
        args += ["--env-file", str(env)]
    before = (list(bucket.writes), dict(bucket.reads), path.read_bytes())
    assert conversion.main(args) == 1
    _assert_no_new_attempt(capsys.readouterr().err, path)
    assert (bucket.writes, bucket.reads, path.read_bytes()) == before


@pytest.mark.parametrize("history", ["attempted", "published"])
@pytest.mark.parametrize("refusal", ["journal", "main", "wheel"])
def test_prepared_api_prior_history_requires_read_only_reconciliation(tmp_path, monkeypatch, bucket, history, refusal):
    path = _prepared_with_history(tmp_path, monkeypatch, bucket, history)
    state = dict(STATE)
    if refusal == "main":
        state["main"] = "be" * 20
    elif refusal == "wheel":
        state["spicy_docs"] = "other"
    before = (list(bucket.writes), dict(bucket.reads), path.read_bytes())
    with pytest.raises(conversion.ConversionRefused) as failure:
        conversion.publish_prepared(path, allowed=["cfr-sections"], expected_main=MAIN, expected_spicy_docs=WHEEL,
                                    expect_bucket=BUCKET, state=lambda _: state,
                                    expected_receipt_digest=prepared_digest(path))
    _assert_no_new_attempt(failure.value.state, path)
    assert (bucket.writes, bucket.reads, path.read_bytes()) == before


def test_prepared_current_attempt_retains_its_confirmed_not_moved_state(tmp_path, monkeypatch, bucket, capsys):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    monkeypatch.setattr(conversion, "source_state", lambda _: dict(STATE))

    intercepted = []
    before = list(bucket.writes)

    def refuse(*args, **kwargs):
        assert isinstance(kwargs["conversion_proof"], publication.NativeConversionProof)
        intercepted.append(True)
        raise publication.PublicationError("injected publication refusal")

    monkeypatch.setattr(publication, "_publish_verified_generation", refuse)
    assert conversion.main(["--publish-prepared", str(path), *COMMON, "--expect-bucket", BUCKET,
                            "--expect-receipt-digest", prepared_digest(path)]) == 1
    state = capsys.readouterr().err
    assert conversion.NOTHING_PUBLISHED in state and "no new publication attempt" not in state
    assert json.loads(path.read_bytes())["publish_attempt"]["outcome"] == "not moved"
    assert intercepted == [True]
    assert bucket.writes == before
    assert stored(bucket) == old


@pytest.mark.parametrize("publisher", ["other-identity", "forced-full"])
def test_prepared_native_admission_is_repeated_unless_the_seal_holds_this_identity(
        tmp_path, monkeypatch, bucket, publisher):
    """A publisher under another admission key, or asked for it, repeats the real admission and its refusal."""
    from spicy_regs import conversion_reads, generations

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)

    intercepted = []
    open_reader = conversion_reads._open_family_reader

    def refuse(*args, **kwargs):
        # Repeat the real byte-bound receipt/subject admission, then fail inside its owned lifetime.
        with open_reader(*args, **kwargs):
            intercepted.append(kwargs["generation_id"])
            raise ValueError("native admission refused this selected bundle")

    monkeypatch.setattr(conversion_reads, "_open_family_reader", refuse)
    if publisher == "other-identity":
        identity = generations.validation_identity()
        monkeypatch.setattr(generations, "validation_identity", lambda: identity | {"runtime": "another runtime"})
    before = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="native admission refused"):
        publish_prepared(work / conversion.RECEIPT, full_verification=publisher == "forced-full")
    assert len(intercepted) == 1
    assert bucket.writes == before and stored(bucket) == old


def test_prepared_rechecks_code_after_qualification_before_upload(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    states = iter([dict(STATE), STATE | {"main": "be" * 20}])
    before = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="Main or the pinned wheel moved") as failure:
        conversion.publish_prepared(work / conversion.RECEIPT, allowed=["cfr-sections"], expected_main=MAIN,
                                    expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: next(states),
                                    expected_receipt_digest=prepared_digest(work / conversion.RECEIPT))
    _assert_no_new_attempt(failure.value.state, work / conversion.RECEIPT)
    assert bucket.writes == before


def test_prepared_pointer_retry_preserves_another_familys_new_entry(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    other = publish_old(bucket, monkeypatch, tmp_path / "sibling", "nominations", {"nominations": [{"citation": "PN1-119"}]})
    changed = other | {"artifactDigest": "sha256:" + "cf" * 32, "prefix": "generations/nominations/" + "cf" * 32}
    work = tmp_path / "work"
    convert("cfr-sections", work)
    raced = []

    def change(key):
        if key == publication.INDEX_V2_KEY and not raced:
            raced.append(True)
            index = publication.parse_index(bucket.objects[key])
            index["families"]["nominations"] = changed
            bucket.objects[key] = canonical_json_bytes(index)

    bucket.before_put = change
    done = publish_prepared(work / conversion.RECEIPT)
    assert raced and done["published"]["entry"]["artifactDigest"] == done["generation"]["artifactDigest"]
    assert publication.current_index(BASE)["families"]["nominations"] == changed
