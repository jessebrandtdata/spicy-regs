"""Recorded native legal navigation survives typing and exact historical replay."""
import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.citation_resolution import NATIVE_RESOLUTION_RULE, NATIVE_ROUTES
from spicy_regs.legislative_documents import (
    LegislativeShapeError, NATIVE_TARGET_KEY_FIELDS, map_subject, recorded_subject,
)
from spicy_regs.legislative_receipts import restore_prior, write_legislative_outputs
from tests.test_legislative_documents import row
from tests.test_native_legal_references import FIXTURES, run


def observation(candidates, **extra):
    return row("native_legal_references", scope_id="scope", input_sha256="sha256:" + "a" * 64,
               occurrence_index="3", source_family="uscode", source_record_key="/us/usc/t5/s552",
               source_locator="retained:uslm", source_path="/*[1]/*[2]", element_tag="sourceCredit",
               text="The original passage", interpretation_status="partial_text_findings",
               rule_version="native-legal-reference/003", target_candidates_json=json.dumps(candidates), **extra)


@pytest.mark.parametrize("kind", NATIVE_ROUTES)
def test_every_maintained_route_keeps_its_complete_identity_and_recorded_pin(kind):
    route = NATIVE_ROUTES[kind]
    keys = {name: f"literal-{name}" for name in route.identity}
    pin = {"status": "published", "family": "selected-family", "generation_id": "original-generation",
           "artifact_digest": "sha256:" + "b" * 64, "receipt_sha256": "sha256:" + "c" * 64}
    candidate = dict(cite_kind=kind, target_kind=kind, target_key="retained-key", target_resolved=True,
                     candidate_keys=[keys, keys], expected_cardinality=route.expected_cardinality,
                     target_table_selected=route.table, target_status="found", match_count=2,
                     target_snapshot=pin, resolution_rule=NATIVE_RESOLUTION_RULE,
                     derivation_rule="retained-rule", derivation_version="004", occurrence_key="original:0")
    result = map_subject("native_legal_references", observation([candidate, None, candidate]))
    assert result is not None
    assert result["source_path"] == "/*[1]/*[2]" and result["text"] == "The original passage"
    assert result["rule_version"] == "native-legal-reference/003"
    assert result["interpretation_status"] == "partial_text_findings"
    first, null, repeat = result["target_candidates"]
    assert null is None and first["candidate_ordinal"] == 0 and repeat["candidate_ordinal"] == 2
    assert first["target_snapshot"]["receipt_sha256"] == pin["receipt_sha256"]
    assert first["resolution_rule"] == candidate["resolution_rule"]
    assert first["target_status"] == "found" and first["target_resolved"] is True
    assert first["match_count"] == 2
    assert [key["target_key_ordinal"] for key in first["candidate_keys"]] == [0, 1]
    for key in first["candidate_keys"]:
        assert {name: key[name] for name in route.identity} == keys
        assert all(key[name] is None for name in set(NATIVE_TARGET_KEY_FIELDS) - set(route.identity))


def test_typed_key_union_is_exactly_the_native_route_identities():
    """native_legal_references records candidates under NATIVE_ROUTES, so its typed keys are their identities."""
    assert set(NATIVE_TARGET_KEY_FIELDS) == {name for route in NATIVE_ROUTES.values() for name in route.identity}


@pytest.mark.parametrize("status,reason,count,keys", [
    ("missing", None, 0, []), ("ambiguous", None, 2, [{"number": "1"}, {"number": "2"}]),
    ("unsupported", "unsupported_citation_kind", None, []),
    ("not_checked", "target_snapshot_unavailable", None, []),
    ("not_checked", "candidate_limit", 103, [{"congress": "119"}]),
    ("not_checked", "target_identity_incomplete", 1, [{"congress": None}]),
    ("not_checked", "target_read_failure", None, []),
])
def test_recorded_outcomes_and_refusals_are_preserved_without_a_new_lookup(status, reason, count, keys):
    candidate = dict(target_status=status, reason=reason, match_count=count, candidate_keys=keys,
                     source_status="current_text", target_resolved=False, error_type="TimeoutError",
                     match_basis="page_range", text_sha256="sha256:" + "d" * 64)
    mapped = map_subject("native_legal_references", observation([candidate]))
    assert mapped is not None
    [result] = mapped["target_candidates"]
    for name in ("target_status", "reason", "match_count", "source_status", "target_resolved",
                 "error_type", "match_basis", "text_sha256"):
        assert result[name] == candidate[name]
    assert len(result["candidate_keys"]) == len(keys)


@pytest.mark.parametrize("candidates,status", [
    ([], "unsupported_href"), ([], "no_qualified_text_findings"), (None, None),
])
def test_unsupported_empty_and_unread_observations_remain_visible(candidates, status):
    raw = observation(candidates) | {"interpretation_status": status, "href": "opaque:retained"}
    result = map_subject("native_legal_references", raw)
    assert result is not None
    assert result["href"] == "opaque:retained" and result["text"] == raw["text"]
    assert result["interpretation_status"] == status
    assert result["target_candidates"] == candidates


@pytest.mark.parametrize("candidate", [
    {"unexpected": "value"}, {"candidate_keys": [{"unreviewed_key": "value"}]},
    {"target_snapshot": {"unreviewed_pin": "value"}}, {"candidate_ordinal": 4},
    {"candidate_keys": [{"target_key_ordinal": 4}]},
])
def test_unreviewed_candidate_properties_refuse(candidate):
    with pytest.raises(LegislativeShapeError):
        map_subject("native_legal_references", observation([candidate]))


def test_main_read_scope_keeps_zero_empty_unknown_and_selected_shapes():
    raw = row("native_legal_reference_reads", scope_id="empty", source_family="ecfr",
              source_record_key="title-1", input_sha256="sha256:" + "a" * 64, occurrence_count="0",
              source_bytes="17", selected_shapes_json='["AUTH","SOURCE"]', unsupported_shapes_json='[]',
              source_locator="retained:xml", read_status="complete_selected_shapes", rule_version="rule-3",
              manifest_sha256="sha256:" + "b" * 64)
    result = map_subject("native_legal_reference_reads", raw)
    assert result is not None
    assert result["occurrence_count"] == 0 and result["source_bytes"] == 17
    assert result["edition"] is None and result["unsupported_shapes"] == []
    assert result["selected_shapes"] == ["AUTH", "SOURCE"]
    assert result["body_version_id"] == "body:sha256:" + "a" * 64
    assert result["input_sha256"] == raw["input_sha256"]
    assert result["manifest_sha256"] == raw["manifest_sha256"]


def test_exact_earlier_receipt_only_reads_and_stripped_subjects_restore_before_upgrade(tmp_path, monkeypatch):
    from spicy_regs import legislative_receipts as receipts
    from spicy_regs.legislative_documents import field_registry
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection

    files, _ = run(FIXTURES / "manifest.json", tmp_path / "source")
    from spicy_regs.policy_migrations import earlier_policy
    old = {name: earlier_policy(name, "legislative-documents/1")
           for name in ("native_legal_references", "native_legal_reference_reads")}  # the order ``files`` has
    current_policy, current_map = receipts.policy, receipts.map_subject

    def old_map(dataset, raw):
        if dataset not in old:
            return current_map(dataset, raw)
        if old[dataset].receipt_only:
            return None
        return recorded_subject(dataset, raw, dict.fromkeys(old[dataset].subject_schema.names))

    with monkeypatch.context() as patch:
        patch.setattr(receipts, "policy", lambda name: old.get(name) or current_policy(name))
        patch.setattr(receipts, "map_subject", old_map)
        manifest = write_legislative_outputs(files, tmp_path / "old-bundle", generation_id="old-generation")
    assert manifest["subjects"]["native_legal_reference_reads"] == []
    selections = [SelectedDataset(name, tuple(tmp_path / "old-bundle" / p for p in paths),
                                  tmp_path / "old-bundle/etl_receipts.parquet", "old-generation")
                  for name, paths in manifest["subjects"].items()]
    remember_selection(tmp_path / "state", selections)
    from spicy_regs.local_data import local_selection
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.sources import publication
    import duckdb
    selected = local_selection(tmp_path / "state")
    assert "native_legal_reference_reads" not in selected.files
    assert "native_legal_references" in selected.files
    local_native = {item.dataset: {"subjects": item.subjects, "receipts": item.receipts,
                                   "generation_id": item.generation_id} for item in selections}
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, publication.empty_index(), "unused", local_native=local_native)
        reads = list(adapter.selected_rows("native_legal_reference_reads"))
        assert [r["raw_source_row"] for r in reads] == pq.read_table(files[1]).to_pylist()
        assert "scope_id" not in reads[0]  # Original observed receipt, no invented main row.
        index = publication.empty_index()
        shared = tmp_path / "old-bundle/etl_receipts.parquet"
        identity = publication.file_identity(shared)
        index["families"]["native-legal-references"] = {
            "prefix": "generations/native/" + "a" * 64, "tables": {},
            "etlReceipts": {"key": "etl_receipts.parquet", "sha256": identity["sha256"],
                            "byteSize": identity["bytes"], "rows": pq.read_metadata(shared).num_rows,
                            "generationId": "old-generation", "datasets": list(old)},
        }
        remote = ReceiptAdapter(con, index, "https://never-download.invalid")
        fetched = []
        def fetch(member, destination):
            import shutil
            fetched.append(member.key)
            assert member.key == "etl_receipts.parquet"
            shutil.copyfile(shared, destination)
        monkeypatch.setattr(remote, "_fetch", fetch)
        assert list(remote.selected_rows("native_legal_reference_reads")) == reads
        assert fetched == ["etl_receipts.parquet"]
    prior = SelectedPriors(tmp_path / "selected", root=tmp_path / "state", public_url="")
    restored = [prior.get(name) for name in old]
    for original, recovered in zip(files, restored, strict=True):
        assert pq.read_table(original).equals(pq.read_table(recovered), check_metadata=True)
    direct = restore_prior(tmp_path / "old-bundle", tmp_path / "direct")
    assert set(direct) == set(old)
    upgraded = write_legislative_outputs(restored, tmp_path / "new-bundle", generation_id="new-generation")
    assert not any(upgraded["refused_rows"].values())
    assert pq.read_metadata(tmp_path / "new-bundle/native_legal_reference_reads.parquet").num_rows == 2
    assert not field_registry()["native_legal_reference_reads"]["processing_only"]


def test_actual_hosted_family_writes_main_scopes_and_reuses_selected_originals(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.native_legal_references import NativeLegalReferencesRollup
    from spicy_regs.source_evidence import CaptureEvidence

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    work = tmp_path / "work"
    work.mkdir()
    pipeline = NativeLegalReferencesRollup(manifest=FIXTURES / "manifest.json", output_dir=work)
    pipeline.source_evidence = CaptureEvidence(work, pipeline.name)
    outputs = pipeline.build(work)
    assert {p.name for p in outputs} == {
        "native_legal_references.parquet", "native_legal_reference_reads.parquet",
        "native_legal_references_document_file_outcomes.parquet",
    }
    assert not pipeline.receipt_only_tables
    assert sorted(pq.read_table(work / "native_legal_reference_reads.parquet")["occurrence_count"].to_pylist()) == [2, 31]
    before = {p.name: pq.read_table(p) for p in outputs}
    again = NativeLegalReferencesRollup(manifest=FIXTURES / "manifest.json", output_dir=work)
    again.source_evidence = CaptureEvidence(work, again.name)
    repeated = again.build(work)
    assert {p.name for p in repeated} == set(before)
    for name in ("native_legal_references.parquet", "native_legal_reference_reads.parquet"):
        assert before[name].equals(pq.read_table(work / name))
