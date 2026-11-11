"""Prepared publication by receipt digest (ss-7lrn).

The operator names the seal by the digest its preparation printed; a publisher under the seal's admission key relies
on that preparation's row admission and restoration, and still hashes every member and original. Each test checks
its claim through something other than the mechanism under test: validator spies, bucket writes, receipt bytes,
and the index entry the publication itself would write.
"""

import json
from pathlib import Path

import pytest
from rulespec_artifacts import iter_member_descriptors

from spicy_regs import conversion_reads, etl_bulk, generations
from spicy_regs import native_conversion as conversion
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.sources import publication
from tests.test_native_conversion import (
    BASE, BUCKET, CFR, MAIN, STATE, WHEEL, bucket as _bucket, cfr_on_its_receipt_route as _cfr_on_its_receipt_route,
    convert, publish_old, stored,
)

bucket = _bucket
#: cfr-sections on its former receipt route, this file's example family (autouse; tests/test_native_conversion.py).
cfr_on_its_receipt_route = _cfr_on_its_receipt_route


def prepare(tmp_path, monkeypatch, bucket):
    """A sealed cfr-sections preparation; the operator digest is the run's returned value, not the file's."""
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    done = convert("cfr-sections", work)
    return old, work / conversion.RECEIPT, done["prepared"]["receiptDigest"]


def publish(path, digest, **options):
    return conversion.publish_prepared(path, allowed=["cfr-sections"], expected_main=MAIN, expected_spicy_docs=WHEEL,
                                       expect_bucket=BUCKET, state=lambda _: dict(STATE),
                                       expected_receipt_digest=digest, **options)


def count_validators(monkeypatch):
    """Record every row-level validation entry: the family reader and the bulk bundle check."""
    calls = []
    for owner, name in ((conversion_reads, "_open_family_reader"), (etl_bulk, "_validated_bundle")):
        real = getattr(owner, name)

        def counted(*args, _real=real, _name=name, **kwargs):
            calls.append(_name)
            return _real(*args, **kwargs)

        monkeypatch.setattr(owner, name, counted)
    return calls


def forbid_validators(monkeypatch):
    def validator(*args, **kwargs):
        raise AssertionError("No row validator may run for a receipt or bytes that already fail their digests")

    monkeypatch.setattr(conversion_reads, "_open_family_reader", validator)
    monkeypatch.setattr(etl_bulk, "_validated_bundle", validator)


def flip_one_byte(path: Path) -> None:
    raw = path.read_bytes()
    path.write_bytes(raw[:100] + bytes([raw[100] ^ 1]) + raw[101:])


@pytest.mark.parametrize("target", ["generation-member", "retained-original"])
def test_a_flipped_byte_refuses_without_calling_the_validator(tmp_path, monkeypatch, bucket, target):
    old, path, digest = prepare(tmp_path, monkeypatch, bucket)
    receipt = json.loads(path.read_text())
    flip_one_byte(Path(receipt["generation"]["directory"]) / "cfr_sections.parquet" if target == "generation-member"
                  else path.parent / "retained" / "cfr_sections.parquet")
    forbid_validators(monkeypatch)
    before, writes = path.read_bytes(), list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused):
        publish(path, digest)
    assert path.read_bytes() == before and bucket.writes == writes and stored(bucket) == old


@pytest.mark.parametrize("edit", ["restoration", "outcomes"])
def test_an_edited_then_resealed_receipt_refuses_on_the_operator_digest(tmp_path, monkeypatch, bucket, edit):
    """The seal's own digest is recomputable by anyone who edits the receipt; the operator's digest is not."""
    old, path, digest = prepare(tmp_path, monkeypatch, bucket)
    receipt = json.loads(path.read_text())
    if edit == "restoration":  # a restoration the preparation never observed
        receipt["tables"]["cfr_sections"]["rows_only_in_retained"] = 7
    else:  # an outcome the sealed path would otherwise carry forward unread
        receipt["receipt_outcomes"]["refused"] = 1
    receipt["prepared"]["receiptDigest"] = conversion._prepared_digest(receipt)
    path.write_text(json.dumps(receipt))
    forbid_validators(monkeypatch)
    before, writes = path.read_bytes(), list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="not the one the operator named"):
        publish(path, digest)
    assert path.read_bytes() == before and bucket.writes == writes and stored(bucket) == old


@pytest.mark.parametrize("digest", ["", "sha256:" + "AB" * 32, "ab" * 32])
def test_a_malformed_operator_digest_refuses(tmp_path, monkeypatch, bucket, digest):
    old, path, _ = prepare(tmp_path, monkeypatch, bucket)
    with pytest.raises(conversion.ConversionRefused, match="expect-receipt-digest"):
        publish(path, digest)
    assert stored(bucket) == old


def test_the_same_identity_publishes_without_row_validation(tmp_path, monkeypatch, bucket):
    old, path, digest = prepare(tmp_path, monkeypatch, bucket)
    calls = count_validators(monkeypatch)
    done = publish(path, digest)
    assert calls == []
    assert done["publish_attempt"]["verification"]["mode"] == "sealed-admission"
    assert done["published"]["entry"]["artifactDigest"] == done["generation"]["artifactDigest"]
    conversion.rollback(path, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old


@pytest.mark.parametrize("difference", ["platform", "spicy-docs-code", "version-1-seal", "forced"])
def test_another_identity_or_an_older_seal_runs_the_full_path(tmp_path, monkeypatch, bucket, difference):
    old, path, digest = prepare(tmp_path, monkeypatch, bucket)
    options = {}
    if difference == "version-1-seal":
        # A seal written before this change carries no identity and must still publish, through every check.
        receipt = json.loads(path.read_text())
        receipt["prepared"] = {key: value for key, value in receipt["prepared"].items()
                               if key not in {"validationIdentity", "admissionKey", "receiptDigest"}} | {"version": 1}
        receipt["prepared"]["receiptDigest"] = digest = conversion._prepared_digest(receipt)
        path.write_text(json.dumps(receipt))
    elif difference == "forced":
        options["full_verification"] = True
    else:
        identity = generations.validation_identity()
        # Any platform other than the one running the test; a literal would equal the real identity on a
        # runner that happens to be that platform (GitHub's Linux x86_64 runners), and the seal would then
        # correctly be trusted instead of exercising the full path.
        other_platform = {"system": identity["platform"]["system"] + "-other", "machine": "other"}
        changed = {"platform": other_platform} if difference == "platform" else {"spicy_docs_code": "0" * 64}
        monkeypatch.setattr(generations, "validation_identity", lambda: identity | changed)
    calls = count_validators(monkeypatch)
    done = publish(path, digest, **options)
    assert done["publish_attempt"]["verification"]["mode"] == "full"
    assert "_open_family_reader" in calls and "_validated_bundle" in calls
    conversion.rollback(path, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old


def _family_rows(family):
    """One rollup, one regulatory base and one court family: the three converters a seal can stand in for."""
    if family == "documents":
        from tests.test_regulatory_prepared_conversion import rows
        return "documents", rows("documents")
    if family == "court-opinion-clusters":
        from tests.test_court_prepared_conversion import clusters
        return "court_opinion_clusters", clusters()
    return "cfr_sections", CFR


@pytest.mark.parametrize("family", ["cfr-sections", "documents", "court-opinion-clusters"])
def test_sealed_and_full_paths_reach_identical_entries_and_proofs(tmp_path, monkeypatch, bucket, family):
    table, rows = _family_rows(family)
    old = publish_old(bucket, monkeypatch, tmp_path, family, {table: rows})
    done = convert(family, tmp_path / "work")
    path, digest = tmp_path / "work" / conversion.RECEIPT, done["prepared"]["receiptDigest"]
    reached, calls = {}, count_validators(monkeypatch)
    for full in (False, True):
        calls.clear()
        with ConversionReadOperation() as operation:
            verified = conversion._verify_prepared(
                path, allowed=[family], expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET,
                expected_receipt_digest=digest, remote="origin", state=lambda _: dict(STATE), expected_producer=None,
                full_verification=full, read_operation=operation)
            artifact, source = operation.verified_source(verified.built.generation)
            reached[verified.verification["mode"]] = {
                "proof": verified.conversion_proof, "policies": dict(verified.built.policies),
                "receipt_only": verified.built.receipt_only_tables, "added": verified.built.added_tables,
                "entry": publication.generation_entry(artifact, list(iter_member_descriptors(artifact, source))),
            }
        # The full path restores originals through the family reader; the sealed path does not.
        assert ("_open_family_reader" in calls) == full
    assert set(reached) == {"sealed-admission", "full"}
    assert reached["sealed-admission"] == reached["full"]
    assert reached["full"]["proof"] is not None
    sealed = path.read_bytes()
    for full in (False, True):
        path.write_bytes(sealed)  # each publication starts from the untouched sealed receipt
        done = conversion.publish_prepared(path, allowed=[family], expected_main=MAIN, expected_spicy_docs=WHEEL,
                                           expect_bucket=BUCKET, state=lambda _: dict(STATE),
                                           expected_receipt_digest=digest, full_verification=full)
        assert done["publish_attempt"]["verification"]["mode"] == ("full" if full else "sealed-admission")
        assert publication._generation_entry(done["published"]["entry"]) == reached["full"]["entry"]
        conversion.rollback(path, expect_bucket=BUCKET)
        assert publication.current_index(BASE)["families"][family] == old


def test_a_corrupted_upload_fails_read_back_and_keeps_the_rollback_record(tmp_path, monkeypatch, bucket):
    old, path, digest = prepare(tmp_path, monkeypatch, bucket)
    real = ConversionReadOperation.verify_public_generation

    def corrupted(operation, base, index, family, directory, *storage):
        key = f"{index['families'][family]['prefix']}/cfr_sections.parquet"
        raw = bucket.objects[key]
        bucket.objects[key] = raw[:100] + bytes([raw[100] ^ 1]) + raw[101:]
        return real(operation, base, index, family, directory, *storage)

    monkeypatch.setattr(ConversionReadOperation, "verify_public_generation", corrupted)
    with pytest.raises(conversion.ConversionRefused, match="could not be read back") as refusal:
        publish(path, digest)
    assert "IS published" in refusal.value.state
    receipt = json.loads(path.read_text())
    assert receipt["publish_attempt"]["outcome"] == "published" and receipt["published"]
    assert receipt["publish_attempt"]["verification"]["mode"] == "sealed-admission"
    conversion.rollback(path, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old


def test_the_cli_prints_the_digest_and_requires_it_back(tmp_path, monkeypatch, bucket, capsys):
    from tests.test_native_conversion import COMMON

    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda _: dict(STATE))
    work = tmp_path / "work"
    assert conversion.main(["cfr-sections", "--work", str(work), *COMMON]) == 0
    printed = [line.removeprefix("prepared receipt digest ") for line in capsys.readouterr().out.splitlines()
               if line.startswith("prepared receipt digest ")]
    assert len(printed) == 1 and conversion.RECEIPT_DIGEST.fullmatch(printed[0])
    path = work / conversion.RECEIPT
    with pytest.raises(SystemExit):
        conversion.main(["--publish-prepared", str(path), *COMMON, "--expect-bucket", BUCKET])
    with pytest.raises(SystemExit):
        conversion.main(["--publish-prepared", str(path), *COMMON, "--expect-bucket", BUCKET,
                         "--expect-receipt-digest", printed[0].upper()])
    assert conversion.main(["--publish-prepared", str(path), *COMMON, "--expect-bucket", BUCKET,
                            "--expect-receipt-digest", printed[0]]) == 0


@pytest.mark.parametrize("moved", ["validator-code", "checkout", "hosted-main-only"])
def test_a_seal_names_only_the_code_its_run_started_with(tmp_path, monkeypatch, bucket, moved):
    """Code changed on disk after the writer and admission ran refuses the seal; hosted main moving does not."""
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    started = generations.validation_identity()
    calls = []

    def state(_):
        calls.append(None)
        if len(calls) == 1 or moved == "validator-code":
            return dict(STATE)
        return STATE | ({"checkout": "f" * 40} if moved == "checkout" else {"main": "e" * 40})

    real_seal = conversion._seal_prepared

    def seal_after_a_change(*args):
        if moved == "validator-code":  # a pull or SpicyDocs re-sync lands after the run imported its modules
            monkeypatch.setattr(generations, "validation_identity", lambda: started | {"spicy_regs": "0" * 64})
        return real_seal(*args)

    monkeypatch.setattr(conversion, "_seal_prepared", seal_after_a_change)
    work = tmp_path / "work"
    if moved == "hosted-main-only":
        done = convert("cfr-sections", work, state=state)
        assert done["prepared"]["validationIdentity"] == started and len(calls) == 2
        return
    with pytest.raises(conversion.ConversionRefused, match="stays unsealed: resume it with --resume-generation from "
                       f"a clean checkout of current main and --expect-producer {MAIN} "):
        convert("cfr-sections", work, state=state)
    receipt = json.loads((work / conversion.RECEIPT).read_bytes())
    assert "prepared" not in receipt and "refused" not in receipt and receipt["generation"]
    # The unsealed receipt resumes, pinned to the preparation's checkout, without rerunning the writer.
    identity = json.loads((Path(receipt["generation"]["directory"]) / "artifact.json").read_bytes())
    monkeypatch.setattr(conversion, "_seal_prepared", real_seal)
    monkeypatch.setattr(generations, "validation_identity", lambda: started)
    monkeypatch.setattr(conversion, "_historical_producer", lambda *_: (
        identity["producer"]["implementationId"], {"version": WHEEL, "wheels": [{"hash": STATE["spicy_docs_wheel"]["locked_sha256"]}]}))
    resumed = conversion.resume_prepared(Path(receipt["generation"]["directory"]), work=work, allowed=["cfr-sections"],
                                         expected_main=MAIN, expected_spicy_docs=WHEEL, expected_producer=MAIN,
                                         state=lambda _: dict(STATE))
    assert resumed["prepared"]["validationIdentity"] == started
