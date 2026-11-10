"""Persisted scorecard receipt joins, exact source replay, and incremental refresh."""

from copy import deepcopy
import json
from dataclasses import replace
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import RECEIPT_SCHEMA
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_family, verify_family_readback, write_family
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import edition, tables, provider, evidence, run, combine


def test_native_generation_keeps_exact_provider_rows_and_only_domain_subjects(tmp_path, monkeypatch):
    from spicy_regs import etl_receipts
    from spicy_regs.scorecards import etl

    before = tables(edition("2025"))
    directory = tmp_path / "candidate"
    validations = []
    validate = etl.validate_receipt_bundle

    def validate_family(subjects, *args, **kwargs):
        validations.append(set(subjects))
        return validate(subjects, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(etl_receipts, "validate_receipt_bundle", lambda *a, **kw: pytest.fail("Repeated component check"))
        patch.setattr(etl, "validate_receipt_bundle", validate_family)
        files = write_family(directory, before)
    assert validations == [set(before)]
    assert read_family(directory, SOURCE_NAMES) == before
    assert not list(directory.glob(".scorecard-etl-*"))
    assert (directory / "scorecard_snapshots.parquet").exists()
    for path in files:
        names = pq.read_schema(path).names
        assert {"capture_id", "source_url", "source_path"} <= set(names)
    target = tmp_path / "generation"
    artifact = build_generation(
        target,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        **generation_options(directory, SOURCE_NAMES),
    )
    admitted = verify_generation(target)
    assert admitted.pin == artifact.pin
    assert admitted.root["spec"]["etlReceipts"]["rows"] == sum(map(len, before.values()))


def test_corrupt_private_component_refuses_before_replacing_complete_family(tmp_path, monkeypatch):
    from spicy_regs.scorecards import etl

    directory = tmp_path / "candidate"
    write_family(directory, tables(edition("2025")))
    previous = {path.name: path.read_bytes() for path in directory.iterdir()}
    write = etl.write_dataset

    def corrupt_component(*args, **kwargs):
        assert kwargs["validate"] is False
        subject, receipt = write(*args, **kwargs)
        if subject is not None and subject.name == "scorecards.parquet":
            table = pq.read_table(subject)
            rows = table.to_pylist()
            rows[0]["title"] = "Changed after receipt creation"
            pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), subject)
        return subject, receipt

    monkeypatch.setattr(etl, "write_dataset", corrupt_component)
    with pytest.raises(ValueError, match="Missing, ambiguous or reused subject receipt: scorecards"):
        write_family(directory, tables(edition("2026")))
    assert {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()} == previous
    retained = list(directory.glob(".scorecard-etl-*/etl_receipts.parquet"))
    assert len(retained) == 1


def test_exact_readback_visits_shuffled_rows_and_observed_snapshots_without_materializing(tmp_path, monkeypatch):
    from spicy_regs.scorecards import etl

    before = combine(tables(edition("2025")), tables(edition("2024")))
    directory = tmp_path / "candidate"
    write_family(directory, {name: list(reversed(rows)) for name, rows in before.items()})
    monkeypatch.setattr(etl, "read_receipt_bundle", lambda **kwargs: pytest.fail("Second raw family materialized"))
    counts = verify_family_readback(directory, before)
    assert counts == {name: len(rows) for name, rows in before.items()}
    assert counts["scorecard_snapshots"] == 2


@pytest.mark.parametrize("change", ["capture", "literal", "null", "type", "missing", "additional", "duplicate"])
def test_exact_readback_refuses_source_changes_even_when_persisted_receipts_are_valid(tmp_path, change):
    before = tables(edition("2025"))
    directory = tmp_path / "candidate"
    write_family(directory, before)
    expected = deepcopy(before)
    rows = expected["scorecard_member_ratings"]
    if change == "capture":
        rows[0]["capture_id"] = "a different original capture"
    elif change == "literal":
        rows[0]["value_text"] += " "
    elif change == "null":
        rows[0]["rank_text"] = "" if rows[0]["rank_text"] is None else None
    elif change == "type":
        rows[0]["value_number"] = 80
    elif change == "missing":
        rows.pop()
    elif change == "additional":
        rows.append(dict(rows[0], publisher_member_key="absent-member"))
    else:
        rows.append(deepcopy(rows[0]))
    with pytest.raises(ValueError):
        verify_family_readback(directory, expected)


def test_exact_readback_refuses_duplicate_processing_only_source_identity(tmp_path):
    before = tables(edition("2025"))
    before["scorecard_snapshots"].append(deepcopy(before["scorecard_snapshots"][0]))
    directory = tmp_path / "candidate"
    with pytest.raises(ValueError, match="Duplicate or ambiguous"):
        write_family(directory, before)


@pytest.mark.parametrize("corruption", ["subject", "receipt", "generation", "duplicate", "missing"])
def test_inconsistent_native_evidence_refuses_before_internal_read(tmp_path, corruption):
    directory = tmp_path / "candidate"
    write_family(directory, tables(edition("2025")))
    kwargs = {}
    if corruption == "subject":
        path = directory / "scorecards.parquet"
        table = pq.read_table(path)
        rows = table.to_pylist()
        rows[0]["title"] = "changed"
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    elif corruption in {"receipt", "duplicate"}:
        path = directory / "etl_receipts.parquet"
        rows = pq.read_table(path).to_pylist()
        if corruption == "receipt":
            rows[0]["processor"] = "changed"
        else:
            rows.append(deepcopy(rows[0]))
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path)
    elif corruption == "generation":
        kwargs["generation_id"] = ""
    else:
        (directory / "etl_receipts.parquet").unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        read_family(directory, SOURCE_NAMES, generation_id=kwargs.get("generation_id"))


@pytest.mark.parametrize("local_receipts", [False, True])
def test_native_prior_reconstructs_validation_and_preserves_unselected_scope(tmp_path, monkeypatch, local_receipts):
    old = combine(tables(edition("2025")), tables(edition("2024")))
    first = tmp_path / "first"
    files = write_family(first, old)
    generation = tmp_path / "generation"
    build_generation(
        generation,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        **generation_options(first, SOURCE_NAMES),
    )
    store = Store()
    index = pub.publish_generation(generation, client=store, bucket="test", prior_index=pub.empty_index())

    def fetch(url, member, target, key=None):
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example")
    second = tmp_path / "second"
    second.mkdir()
    e = evidence(second)
    e.read_snapshot = index

    def download(key, target):
        return fetch(None, pub.single_member(index, key), target)

    selected = edition("2025")
    options = {}
    if local_receipts:
        monkeypatch.delenv("R2_PUBLIC_URL")
        options["download_prior_receipts"] = lambda target: fetch(
            None, pub.receipt_members(index, dataset="scorecards")[0], target
        )
    run(second, provider([selected], {selected.scorecard_id: tables}), e, download_prior=download, **options)
    actual = read_family(second / "build", SOURCE_NAMES)
    from spicy_regs.etl_receipts import subject_identity
    from spicy_regs.scorecards.etl import POLICIES

    key = subject_identity(POLICIES["scorecards"], {"scorecard_id": "lcv:2024"})[0]
    before_receipt = next(
        r
        for r in pq.read_table(first / "etl_receipts.parquet").to_pylist()
        if r["dataset"] == "scorecards" and r["record_id"] == key
    )
    after_receipt = next(
        r
        for r in pq.read_table(second / "build/etl_receipts.parquet").to_pylist()
        if r["dataset"] == "scorecards" and r["record_id"] == key
    )
    assert after_receipt == before_receipt
    for name, rows in actual.items():
        if name != "scorecard_publishers":
            assert [r for r in rows if r["scorecard_id"] == "lcv:2024"] == [
                r for r in old[name] if r["scorecard_id"] == "lcv:2024"
            ]


def test_conversion_failure_retains_raw_input_without_subject_generation(tmp_path):
    raw = tables(edition("2025"))
    raw["scorecard_member_ratings"][0]["value_number"] = "NaN"
    with pytest.raises(ValueError, match="conversion refused"):
        write_family(tmp_path / "candidate", raw)
    assert not list((tmp_path / "candidate").glob("*.parquet"))
    receipts = list((tmp_path / "candidate").glob(".scorecard-etl-*/etl_receipts.parquet"))
    assert len(receipts) == 1
    from spicy_regs.etl_receipts import _unpack

    failed = [r for r in pq.read_table(receipts[0]).to_pylist() if r["outcome"] == "error"]
    assert len(failed) == 1
    assert _unpack(json.loads(failed[0]["processing_json"]))["raw_source"]["value_number"] == "NaN"


def test_conversion_refusal_does_not_read_or_change_prior_receipts(tmp_path):
    raw = tables(edition("2025"))
    raw["scorecard_member_ratings"][0]["value_number"] = "1e2"
    prior = tmp_path / "prior.parquet"
    prior_bytes = b"Prior receipt carry must not run for a refused conversion."
    prior.write_bytes(prior_bytes)
    candidate = tmp_path / "candidate"
    with pytest.raises(ValueError, match="conversion refused"):
        write_family(candidate, raw, prior_receipts=prior)
    assert prior.read_bytes() == prior_bytes
    assert not list(candidate.glob("*.parquet"))
    assert not list(candidate.glob(".scorecard-etl-*/rebound-etl-receipts.parquet"))
    retained = list(candidate.glob(".scorecard-etl-*/etl_receipts.parquet"))
    assert len(retained) == 1
    from spicy_regs.etl_receipts import _unpack

    failures = [r for r in pq.read_table(retained[0]).to_pylist() if r["outcome"] == "error"]
    assert len(failures) == 1
    assert _unpack(json.loads(failures[0]["processing_json"]))["raw_source"]["value_number"] == "1e2"


@pytest.mark.parametrize("late_error", [False, True])
def test_streamed_multibatch_family_keeps_late_failure_receipts_and_prior(tmp_path, monkeypatch, late_error):
    from spicy_regs import etl_receipts

    base = tables(edition("2025"))["scorecard_member_ratings"][0]
    sizes = []
    original_subjects = etl_receipts._subjects

    def subjects(policy, rows):
        sizes.append(len(rows))
        return original_subjects(policy, rows)

    monkeypatch.setattr(etl_receipts, "_subjects", subjects)

    def source_rows():
        for index in range(4001):
            if index and index % 2000 == 0:
                # The next input batch is read only after the preceding batch reaches the shared writer.
                assert sizes == [2000] * (index // 2000)
            yield dict(base, publisher_member_key=f"member-{index}")
        if late_error:
            yield dict(base, publisher_member_key="invalid-member", value_number="1e2")

    candidate = tmp_path / "candidate"
    prior = tmp_path / "prior.parquet"
    prior_bytes = b"A late conversion refusal must not read or change prior receipts."
    prior.write_bytes(prior_bytes)
    raw = {"scorecard_member_ratings": source_rows()}
    if late_error:
        with pytest.raises(ValueError, match="conversion refused"):
            write_family(candidate, raw, prior_receipts=prior)
        assert prior.read_bytes() == prior_bytes
        assert not list(candidate.glob("*.parquet"))
        assert not (candidate / "scorecard-etl-build.json").exists()
        receipts = list(candidate.glob(".scorecard-etl-*/etl_receipts.parquet"))
        assert len(receipts) == 1
        rows = pq.read_table(receipts[0]).to_pylist()
        assert sum(row["outcome"] == "accepted" for row in rows) == 4001
        failures = [row for row in rows if row["outcome"] == "error"]
        assert len(failures) == 1
        held = etl_receipts._unpack(json.loads(failures[0]["processing_json"]))
        assert held["raw_source"]["value_number"] == "1e2"
    else:
        write_family(candidate, raw)
        monkeypatch.setattr(etl_receipts, "_subjects", original_subjects)
        assert read_family(candidate, ("scorecard_member_ratings",)) == {
            "scorecard_member_ratings": [dict(base, publisher_member_key=f"member-{index}") for index in range(4001)]
        }
    assert sizes == [2000, 2000, 1]


def test_installed_scorecard_policy_refuses_receiptless_new_generation(tmp_path):
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.scorecards.etl import POLICIES

    installed = installed_policies()
    assert all(installed[name].descriptor() == policy.descriptor() for name, policy in POLICIES.items())
    files = write_family(tmp_path / "candidate", tables(edition("2025")))
    with pytest.raises(ValueError, match="require ETL receipts"):
        build_generation(tmp_path / "refused", family="scorecards", files=files, expected_keys=[p.name for p in files])


def historical_rating_mapper(current):
    """Construct the old writer's Decimal subjects for historical-policy fixtures."""

    def mapped(name, raw):
        row = current(name, raw)
        if name == "scorecard_member_ratings" and row["value_number"] is not None:
            row["value_number"] = Decimal(row["value_number"])
        return row

    return mapped


@pytest.mark.parametrize("numeric", [None, "+080.125"])
@pytest.mark.parametrize("historical_version", ["v1", "v2"])
def test_historical_rating_policy_is_validated_before_exact_current_rewrite(
    tmp_path, monkeypatch, numeric, historical_version
):
    from spicy_regs.scorecards import etl
    from spicy_regs.contract_types import described_schema
    from spicy_regs import etl_policy_registry

    before = tables(edition("2024"))
    before["scorecard_member_ratings"][0]["value_number"] = numeric
    first = tmp_path / "historical"
    rating = etl.LEGACY_RATING_POLICY if historical_version == "v1" else etl.LEGACY_RATING_POLICY_V2
    historical = dict(etl.POLICIES, scorecard_member_ratings=rating)
    with monkeypatch.context() as old_runtime:
        old_runtime.setattr(etl, "POLICIES", historical)
        old_runtime.setattr(etl, "map_source_row", historical_rating_mapper(etl.map_source_row))
        old_runtime.setattr(etl_policy_registry, "installed_policies", lambda: historical)
        files = etl.write_family(first, before)
        artifact = build_generation(
            tmp_path / "old-generation",
            family="scorecards",
            files=files,
            expected_keys=[p.name for p in files],
            **etl.generation_options(first, SOURCE_NAMES),
        )
    selected = etl.admitted_read_policies(SOURCE_NAMES, descriptors=artifact.root["spec"]["etlReceipts"]["policies"])
    assert selected["scorecard_member_ratings"].descriptor() == rating.descriptor()
    published_columns = {
        name: described_schema(p.subject_schema)
        for name, p in historical.items()
        if name in SOURCE_NAMES and not p.receipt_only
    }
    assert etl.admitted_read_policies(SOURCE_NAMES, columns=published_columns) == selected
    indexed = {
        "tables": {name + ".parquet": {"columns": columns} for name, columns in published_columns.items()},
        "etlReceipts": {"generationId": artifact.root["spec"]["etlReceipts"]["generationId"]},
    }
    assert etl.read_indexed_family(first, SOURCE_NAMES, indexed) == before
    assert verify_generation(tmp_path / "old-generation").pin == artifact.pin
    assert etl.read_source_generation(tmp_path / "old-generation") == before
    from spicy_regs.local_data import local_selection
    from spicy_regs.selected_generations import remember_generation

    old_workspace = tmp_path / "old-native-workspace"
    remember_generation(old_workspace, tmp_path / "old-generation", artifact)
    from spicy_regs.etl_receipts import selected_subject_policy

    local = local_selection(old_workspace)
    native_rating = local.native["scorecard_member_ratings"]
    assert native_rating.generation_id == artifact.root["spec"]["etlReceipts"]["generationId"]
    # Selected reads validate the original historical schema without relabeling it.
    # Rewriting under the current schema still requires explicit source restoration.
    assert selected_subject_policy(etl.POLICIES["scorecard_member_ratings"], native_rating.subjects) == rating
    assert pq.ParquetFile(native_rating.subjects[0]).schema_arrow.equals(rating.subject_schema)
    with pytest.raises(ValueError, match="Subject schema differs from policy"):
        etl.read_family(first, SOURCE_NAMES)
    restored = etl.read_family(first, SOURCE_NAMES, policies=selected)
    assert restored == before
    second = tmp_path / "current"
    etl.write_family(second, restored, prior_receipts=first / "etl_receipts.parquet")
    assert etl.read_family(second, SOURCE_NAMES) == before
    indexed["tables"] = {
        name + ".parquet": {"columns": described_schema(etl.POLICIES[name].subject_schema)}
        for name in SOURCE_NAMES
        if not etl.POLICIES[name].receipt_only
    }
    indexed["etlReceipts"]["generationId"] = etl.generation_options(second, SOURCE_NAMES)["receipt_generation_id"]
    assert etl.read_indexed_family(second, SOURCE_NAMES, indexed) == before
    old = next(
        r
        for r in pq.read_table(first / "etl_receipts.parquet").to_pylist()
        if r["dataset"] == "scorecard_member_ratings"
    )
    new = next(
        r
        for r in pq.read_table(second / "etl_receipts.parquet").to_pylist()
        if r["dataset"] == "scorecard_member_ratings"
    )
    assert old["policy_version"] == rating.policy_version
    assert old["record_id"] == new["record_id"] and old["witnesses"] == new["witnesses"]
    assert old["subject_version"] != new["subject_version"]
    assert new["policy_version"] == "scorecards-etl-ratings-v4"
    from spicy_regs.etl_receipts import _digest, decode_exact_json

    if True:  # Adding recorded snapshot/capture fields changes every historical subject.
        assert new["policy_version"] == "scorecards-etl-ratings-v4"
        assert new["generation_id"] != old["generation_id"]
        assert decode_exact_json(new["diagnostic_json"]) == {
            "prior_receipt": {
                "receipt_id": old["receipt_id"],
                "generation_id": old["generation_id"],
                "processing_sha256": _digest(decode_exact_json(old["processing_json"])),
            }
        }


@pytest.mark.parametrize("change", ["version", "schema", "classification", "duplicate"])
def test_historical_policy_selection_refuses_unknown_declarations(change):
    from spicy_regs.scorecards import etl

    descriptors = [p.descriptor() for name, p in etl.POLICIES.items() if name in SOURCE_NAMES]
    rating = next(p for p in descriptors if p["dataset"] == "scorecard_member_ratings")
    if change == "version":
        rating["policy_version"] = "unreviewed"
    elif change == "schema":
        schema = etl.POLICIES["scorecard_member_ratings"].subject_schema
        schema = schema.set(
            schema.get_field_index("value_number"), schema.field("value_number").with_type(pa.decimal128(38, 20))
        )
        rating.update(replace(etl.POLICIES["scorecard_member_ratings"], subject_schema=schema).descriptor())
    elif change == "classification":
        rating["receipt_fields"].remove("raw_source")
    else:
        descriptors[-1] = descriptors[0]
    with pytest.raises(ValueError, match="supported exact"):
        etl.admitted_read_policies(SOURCE_NAMES, descriptors=descriptors)


def test_decimal_to_text_cast_cannot_reinterpret_historical_receipt_hashes(tmp_path, monkeypatch):
    from spicy_regs.scorecards import etl
    from spicy_regs.etl_receipts import validate_receipt_bundle

    before = tables(edition("2024"))
    before["scorecard_member_ratings"][0]["value_number"] = "80.125"
    with monkeypatch.context() as old_runtime:
        old_runtime.setattr(etl, "POLICIES", dict(etl.POLICIES, scorecard_member_ratings=etl.LEGACY_RATING_POLICY))
        old_runtime.setattr(etl, "map_source_row", historical_rating_mapper(etl.map_source_row))
        etl.write_family(tmp_path / "historical", before)
    directory = tmp_path / "historical"
    path = directory / "scorecard_member_ratings.parquet"
    table = pq.read_table(path)
    current = etl.POLICIES["scorecard_member_ratings"].subject_schema
    # A schema cast cannot add the newly promoted context; model an attempted
    # unqualified null fill as well as the historical decimal-to-text cast.
    for field in current:
        if field.name not in table.schema.names:
            table = table.append_column(field, pa.nulls(len(table), type=field.type))
    pq.write_table(table.cast(current, safe=True), path)
    subjects = {
        name: [] if etl.POLICIES[name].receipt_only else [directory / (name + ".parquet")] for name in SOURCE_NAMES
    }
    # Casting the old physical file cannot replace a receipt-checked source
    # reconstruction: its padded decimal text differs from the source lexeme.
    policies = [etl.POLICIES[name] for name in SOURCE_NAMES]
    with pytest.raises(ValueError, match="Missing, ambiguous or reused subject receipt"):
        validate_receipt_bundle(subjects, [directory / "etl_receipts.parquet"], policies)


def test_unchanged_scorecard_refresh_preserves_complete_receipts(tmp_path):
    from hashlib import file_digest
    from spicy_regs.etl_receipts import decode_exact_json, rebind_receipt

    before = tables(edition("2024"))
    original = tmp_path / "original"
    write_family(original, before, generation_id="original-build")
    original_path = original / "etl_receipts.parquet"
    rows = pq.read_table(original_path).to_pylist()
    # Model already retained history without discarding any of its exact values.
    for generation in ("old-refresh-1", "old-refresh-2", "old-refresh-3"):
        rows = [rebind_receipt(row, generation_id=generation) if row["outcome"] == "accepted" else row for row in rows]
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), original_path)
    with original_path.open("rb") as stream:
        original_hash = file_digest(stream, "sha256").hexdigest()
    assert any("prior_receipts" in decode_exact_json(row["diagnostic_json"]) for row in rows)
    prior = original_path
    for refresh in range(4):
        candidate = tmp_path / f"refresh-{refresh}"
        write_family(candidate, before, generation_id=f"current-refresh-{refresh}", prior_receipts=prior)
        prior = candidate / "etl_receipts.parquet"
        assert pq.read_table(prior).to_pylist() == rows
        assert read_family(candidate, SOURCE_NAMES) == before
    with original_path.open("rb") as stream:
        assert file_digest(stream, "sha256").hexdigest() == original_hash


@pytest.mark.parametrize("change", ["schema", "classification", "identity"])
@pytest.mark.parametrize("version", ["v2", "v3"])
def test_shared_historical_admission_requires_exact_current_descriptor(change, version):
    from spicy_regs.policy_migrations import earlier_policies
    from spicy_regs.scorecards import etl

    current = etl.POLICIES["scorecard_member_ratings"] if version == "v3" else etl.LEGACY_RATING_POLICY_V2
    if change == "schema":
        field = current.subject_schema.field("value_number")
        changed = replace(
            current,
            subject_schema=current.subject_schema.set(
                current.subject_schema.get_field_index("value_number"), field.with_type(pa.decimal128(38, 20))
            ),
        )
    elif change == "classification":
        changed = replace(current, receipt_fields=tuple(f for f in current.receipt_fields if f != "raw_source"))
    else:
        changed = replace(current, identity_fields=tuple(reversed(current.identity_fields)))
    assert earlier_policies(current) and earlier_policies(changed) == ()


@pytest.mark.parametrize("change", ["subject", "processing"])
def test_changed_scorecard_row_references_only_direct_predecessor(tmp_path, change):
    from spicy_regs.etl_receipts import decode_exact_json, _digest

    raw = tables(edition("2025"))
    first = tmp_path / "first"
    write_family(first, raw)
    before = pq.read_table(first / "etl_receipts.parquet").to_pylist()
    if change == "subject":
        raw["scorecard_member_ratings"][0]["value_number"] = "81"
        raw["scorecard_member_ratings"][0]["value_text"] = "81%"
    else:
        raw["scorecard_member_ratings"][0]["source_path"] = "$[2]"
    second = tmp_path / "second"
    write_family(second, raw, prior_receipts=first / "etl_receipts.parquet")
    after = pq.read_table(second / "etl_receipts.parquet").to_pylist()
    old = next(r for r in before if r["dataset"] == "scorecard_member_ratings")
    new = next(r for r in after if r["dataset"] == "scorecard_member_ratings")
    assert decode_exact_json(new["diagnostic_json"]) == {
        "prior_receipt": {
            "receipt_id": old["receipt_id"],
            "generation_id": old["generation_id"],
            "processing_sha256": _digest(decode_exact_json(old["processing_json"])),
        }
    }
    assert new["subject_version"] != old["subject_version"]
    # A public context change also remains visible in the exact raw_source receipt.
    assert (new["processing_json"] != old["processing_json"]) is (change == "subject")
    fresh = tmp_path / "fresh-without-prior"
    write_family(fresh, raw)
    fresh_rating = next(
        r
        for r in pq.read_table(fresh / "etl_receipts.parquet").to_pylist()
        if r["dataset"] == "scorecard_member_ratings"
    )
    assert new["witnesses"] == fresh_rating["witnesses"]
    assert [r for r in before if r is not old] == [r for r in after if r is not new]
    assert read_family(second, SOURCE_NAMES) == raw
