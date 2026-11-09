import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_fec_identity_rollup import BUILDERS
from spicy_regs.transforms.fec_identity_context_fields import REGISTRY
from spicy_regs.transforms.fec_identity_receipts import (
    IdentityReceiptWriter,
    read_identity_rows,
    read_identity_processing,
)

WITNESS = {
    "source_id": "held-input",
    "source_uri": None,
    "sha256": "sha256:" + "a" * 64,
    "locator": None,
    "body_version": None,
}


def test_every_table_subject_or_processing_roundtrip(tmp_path):
    output = tmp_path / "all"
    with IdentityReceiptWriter(output, generation_id="g1", tables=REGISTRY) as writer:
        for name, rules in REGISTRY.items():
            row = dict.fromkeys(rules["input_fields"])
            for key in rules["identity_fields"]:
                if key in row:
                    row[key] = ("2024" if key == "cycle" else WITNESS["sha256"] if key == "source_sha256" else name + key)
            if name == "fec_reference_source_pages":
                row.update(content_status="body_extracted", text="Public document")
            writer.emit(name, row, input_witness=WITNESS, source_input={"original": None})
    for name, rules in REGISTRY.items():
        if rules["receipt_only"]:
            assert not (output / (name + ".parquet")).exists()
            assert len(list(read_identity_processing(output, name, generation_id="g1"))) == 1
        else:
            stored = pq.read_table(output / (name + ".parquet"))
            assert stored.num_rows == 1
            assert stored.schema.names == list(rules["subject_fields"])
            assert len(list(read_identity_rows(output, name, generation_id="g1"))) == 1
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert len(receipts) == len(REGISTRY)


def test_native_list_nulls_and_multiple_context_witnesses_survive(tmp_path):
    output = tmp_path / "identity"
    table = "fec_committee_observations"
    row = {
        "record_id": "source-observation",
        "candidate_ids_json": '["H4NC05146","N/A",null,"H4NC05146"]',
        "cycles_json": "[2024,true,null,2024]",
    }
    evidence = [
        {"collection_id": "part-a", "witness_sha256": "sha256:" + "b" * 64, "role": "item_fields"},
        {"collection_id": "part-b", "witness_sha256": "sha256:" + "c" * 64, "role": "item_fields"},
    ]
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        writer.emit(table, row, input_witness=WITNESS, evidence=evidence)
    subject = pq.read_table(output / (table + ".parquet")).to_pylist()[0]
    assert subject["candidate_ids"] == ["H4NC05146", "N/A", None, "H4NC05146"]
    assert subject["cycles"] == [2024, None, None, 2024]
    assert "cycles_json" not in subject
    receipt = pq.read_table(output / "etl_receipts.parquet").to_pylist()[0]
    assert [w["source_id"] for w in receipt["witnesses"]] == ["held-input", "part-a", "part-b"]
    internal = list(read_identity_rows(output, table, generation_id="g1"))[0]
    assert internal["cycles_json"] == row["cycles_json"]
    assert internal["conversion_diagnostics"]["cycles"][0]["ordinal"] == 1
    with pytest.raises(ValueError):
        list(read_identity_rows(output, table, generation_id=""))


def test_subject_tamper_missing_receipt_duplicate_identity_and_existing_output_refuse(tmp_path):
    table = "fec_quality_notices"
    output = tmp_path / "notices"
    row = {
        "record_id": "notice",
        "notice_kind": "publisher_false_fictitious_filings_list",
        "notice_scope": "source_listed_committee",
        "exclusion_status": "no_automatic_exclusion",
    }
    with pytest.raises(ValueError):
        with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
            writer.emit(table, row, input_witness=WITNESS)
            writer.emit(table, row, input_witness=WITNESS)
    assert not output.exists()
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        writer.emit(table, row, input_witness=WITNESS)
    with pytest.raises(FileExistsError):
        with IdentityReceiptWriter(output, generation_id="g2", tables=[table]):
            pass
    internal = list(read_identity_rows(output, table, generation_id="g1"))[0]
    assert internal["exclusion_status"] == "no_automatic_exclusion"
    schema = pq.read_schema(output / (table + ".parquet"))
    subject = pq.read_table(output / (table + ".parquet")).to_pylist()[0]
    subject["notice_kind"] = "tampered"
    import pyarrow as pa

    pq.write_table(pa.Table.from_pylist([subject], schema=schema), output / (table + ".parquet"))
    with pytest.raises(ValueError):
        list(read_identity_rows(output, table, generation_id="g1"))


def test_real_mapping_build_and_generation_admission(tmp_path):
    import hashlib
    import pyarrow as pa
    from spicy_regs.transforms.build_fec_identity_context import build_fec_identity_context
    from spicy_regs.transforms.fec_identity_receipts import seal_identity_context
    from spicy_regs.generations import verify_generation
    from tests.test_fec_candidate_observations import entry, row, GEN

    source = tmp_path / "source.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [row({"candidate_id": "H2AK01158", "candidate_status": "C", "cycles": [2024, 2024, None]})]
        ),
        source,
    )
    spec = {
        "version": 1,
        "generation_id": "build-g1",
        "tables": [
            "fec_candidate_api_observations",
            "fec_api_response_controls",
            "fec_source_records",
            "fec_record_evidence",
        ],
        "inputs": [
            {
                "mode": "source_records",
                "table": "fec_source_records",
                "path": str(source),
                "rows": 1,
                "sha256": "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest(),
                "source_generation_pin": GEN,
                "jobs": [{"kind": "candidate_api", "entry": entry()}],
            }
        ],
    }
    manifest = tmp_path / "selection.json"
    manifest.write_text(json.dumps(spec))
    output = tmp_path / "result"
    build_fec_identity_context(manifest, output)
    subject = pq.read_table(output / "fec_candidate_api_observations.parquet").to_pylist()[0]
    assert subject["candidate_status"] == "C"
    assert subject["cycles"] == [2024, 2024, None]
    assert (output / "fec_api_response_controls.parquet").exists()
    seal_identity_context(output, tmp_path / "generation")
    artifact = verify_generation(tmp_path / "generation")
    assert artifact.root["spec"]["etlReceipts"]["generationId"] == "build-g1"
    assert artifact.root["spec"]["publicationStatus"] == "local-partial"
    spec["inputs"][0]["sha256"] = "sha256:" + "f" * 64
    manifest.write_text(json.dumps(spec))
    with pytest.raises(ValueError, match="selected regular-file bytes"):
        build_fec_identity_context(manifest, tmp_path / "bad")
    assert not (tmp_path / "bad").exists()


def test_notice_consumer_preserves_financial_guard_and_refuses_wrong_generation(tmp_path):
    from tests.test_fec_financial_policy import bulk, row as financial_fixture
    from spicy_regs.transforms.fec_identity_consumers import quality_notice_effect_with_receipts
    from spicy_regs.transforms.fec_financial_policy import quality_notice_effect

    table = "fec_quality_notices"
    notice = financial_fixture(
        "notice",
        notice_kind="publisher_false_fictitious_filings_list",
        notice_scope="source_listed_committee",
        exclusion_status="no_automatic_exclusion",
        committee_id="C00000001",
    )
    # The generic financial fixture includes financial-only fields that are not notice fields.
    notice = {k: v for k, v in notice.items() if k in REGISTRY[table]["input_fields"]}
    financial = bulk(reporting_committee_id="C00000001")
    output = tmp_path / "notice"
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        writer.emit(table, notice, input_witness=WITNESS)
    actual = quality_notice_effect_with_receipts(
        financial, directory=output, notice_id=notice["record_id"], generation_id="g1"
    )
    assert actual == quality_notice_effect(financial, notice)
    assert not actual["automatic_exclusion"]
    assert not actual["donor_identity_inferred"]
    with pytest.raises(ValueError):
        quality_notice_effect_with_receipts(
            financial, directory=output, notice_id=notice["record_id"], generation_id=""
        )


def test_existing_catalog_producer_has_actual_receipt_writing_entrypoint(tmp_path):
    from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup

    result = build_fec_identity_rollup("fec_source_catalog", tmp_path / "catalog", generation_id="g1")
    assert (result / "fec_source_catalog.parquet").exists()
    rows = list(read_identity_rows(result, "fec_source_catalog", generation_id="g1"))
    assert rows and all(row["source_family"] for row in rows)
    assert all("source_metadata_json" in row for row in rows)


def test_organization_links_state_their_match_grade_and_sponsor_comparison_in_the_subject(tmp_path):
    """Owner decision 2026-10-05: the three columns a reader judges a link by are subject columns, not receipt fields.

    The rows come from the real builder, because a row made from the registry's own input list cannot show a
    builder column the registry never classified.
    """
    import duckdb
    import pyarrow as pa

    from spicy_regs.etl_receipts import _unpack
    from spicy_regs.relationship_views import install_relationship_views
    from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup
    from spicy_regs.transforms.build_org_committee_links import COLUMNS
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    from tests.test_org_committee_links import _comment, _committee, _history, _write

    table = "org_committee_links"
    returned = ("match_method", "confidence", "sponsor_name_match")
    policy = dataset_policy(table)
    assert [policy.subject_schema.field(name).type for name in returned] == [pa.string()] * 3
    assert not set(returned) & set(policy.receipt_fields)
    assert {name for name, _ in COLUMNS} == set(REGISTRY[table]["input_fields"])

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    posted = {"posted_date": "2025-03-01T05:00:00Z"}
    _write(
        inputs / "comments.parquet",
        [
            _comment("C-1", "American Physical Therapy Association", **posted),
            _comment("C-2", "National Association of Realtors", **posted),
            _comment("C-3", "Pipeline Safety Trust", **posted),
        ],
    )
    _write(
        inputs / "fec_committees.parquet",
        [
            _committee("C00000001", "Pipeline Safety Trust"),
            _committee("C00000002", "AMERICAN PHYSICAL THERAPY ASSOCIATION PHYSICAL THERAPY POLITICAL ACTION COMMITTEE"),
            _committee("C00030718", "NATIONAL ASSOCIATION OF REALTORS POLITICAL ACTION COMMITTEE"),
        ],
    )
    _write(
        inputs / "fec_committee_history.parquet",
        [
            _history("C00000002", "2026", "INTERNATIONAL BROTHERHOOD OF TEAMSTERS - DRIVE"),
            _history("C00030718", "2026", "NATIONAL ASSOCIATION OF REALTORS"),
        ],
    )
    output = build_fec_identity_rollup(
        table, tmp_path / "links", generation_id="g1", inputs=sorted(inputs.glob("*.parquet"))
    )
    expected = {
        "American Physical Therapy Association": ("prefix", "medium", "differs"),
        "National Association of Realtors": ("core", "high", "agrees"),
        "Pipeline Safety Trust": ("exact", "high", "not_stated"),
    }
    stored = pq.read_table(output / (table + ".parquet"))
    assert stored.schema.equals(policy.subject_schema)
    assert {row["organization"]: tuple(row[name] for name in returned) for row in stored.to_pylist()} == expected
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert [receipt["outcome"] for receipt in receipts] == ["accepted"] * 3
    held = [_unpack(json.loads(receipt["processing_json"])) for receipt in receipts]
    assert all(not {*returned, "connected_organization_name"} & set(fields) for fields in held)
    # FEC's stated sponsor is published beside the comparison that reads it (owner decision 2, 2026-10-03).
    assert {row["organization"]: row["connected_organization_name"] for row in stored.to_pylist()} == {
        "American Physical Therapy Association": "INTERNATIONAL BROTHERHOOD OF TEAMSTERS - DRIVE",
        "National Association of Realtors": "NATIONAL ASSOCIATION OF REALTORS",
        "Pipeline Safety Trust": None,
    }

    # The pair restores every column of the builder's file its receipts witness, the returned ones included.
    import hashlib
    witness = receipts[0]["witnesses"][0]
    assert witness["source_uri"] is None  # the retained builder file is found by content, not by a machine path
    [retained] = [path for path in (tmp_path / "links.inputs").glob("*.parquet")
                  if "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest() == witness["sha256"]]
    built = pq.read_table(retained).to_pylist()
    restored = [
        {name: row["conversion_inputs"][name] if name in row["conversion_inputs"] else row[name] for name, _ in COLUMNS}
        for row in read_identity_rows(output, table, generation_id="g1")
    ]
    assert restored == built and len(built) == 3

    # The candidate view reads the native table and projects the grade from it.
    with duckdb.connect() as con:
        con.from_parquet(str(output / (table + ".parquet"))).create_view(table)
        view = install_relationship_views(con, [table])["org_identity_candidates"]
        assert (view["status"], view["metadata"]["rule_version"]) == ("available", "native-name-candidates/2")
        viewed = con.execute(
            "SELECT organization, match_method, confidence, sponsor_name_match FROM org_identity_candidates"
        ).fetchall()
        assert {row[0]: row[1:] for row in viewed} == expected

    # Each receipt still vouches for the returned values: a changed grade no longer joins.
    changed = stored.to_pylist()
    changed[0]["confidence"] = "changed"
    pq.write_table(pa.Table.from_pylist(changed, schema=stored.schema), output / (table + ".parquet"))
    with pytest.raises(ValueError, match="subject receipt"):
        list(read_identity_rows(output, table, generation_id="g1"))


def test_committee_increment_requires_exact_prior_receipts_before_producer(tmp_path, monkeypatch):
    import shutil
    import importlib
    from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup
    from spicy_regs.transforms.build_fec_committees import _shape

    table = "fec_committees"
    prior = tmp_path / "prior"
    row = _shape(
        {"committee_id": "C00000001", "cycles": [2024, 2024], "candidate_ids": None, "first_file_date": "2024-01-02"}
    )
    with IdentityReceiptWriter(prior, generation_id="prior-g", tables=[table]) as writer:
        writer.emit(table, row, input_witness=WITNESS)
    calls = []

    def producer(stage, **kwargs):
        calls.append(kwargs)
        held = pq.read_table(stage / "_fec_prior.parquet").to_pylist()[0]
        assert held["cycles_json"] == "[2024, 2024]"
        assert held["candidate_ids_json"] == "null"
        assert held["first_file_date"] == "2024-01-02"
        result = stage / "fec_committees.parquet"
        shutil.copyfile(stage / "_fec_prior.parquet", result)
        return result

    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    monkeypatch.setattr(module, "build_fec_committees", producer)
    with pytest.raises(ValueError, match="prior receipt bundle"):
        build_fec_identity_rollup(table, tmp_path / "absent", generation_id="g")
    with pytest.raises(ValueError):
        build_fec_identity_rollup(
            table, tmp_path / "wrong", generation_id="g", prior_bundle=prior, prior_generation_id=""
        )
    assert not calls
    # A refused call leaves no custody directory, so the corrected retry to the same output is not blocked.
    assert not (tmp_path / "absent.inputs").exists() and not (tmp_path / "wrong.inputs").exists()
    output = build_fec_identity_rollup(
        table, tmp_path / "next", generation_id="next-g", prior_bundle=prior, prior_generation_id="prior-g"
    )
    assert calls == [{"capture_dir": (tmp_path / "next.inputs" / "raw").resolve()}]
    subject = pq.read_table(output / "fec_committees.parquet").to_pylist()[0]
    assert subject["cycles"] == [2024, 2024]
    assert subject["candidate_ids"] is None
    from spicy_regs.etl_receipts import resolve_receipt_witness

    [before] = pq.read_table(prior / "etl_receipts.parquet").to_pylist()
    [after] = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert after["witnesses"][: len(before["witnesses"])] == before["witnesses"]
    reference = next(w for w in after["witnesses"] if w["body_version"] == before["receipt_id"])
    assert resolve_receipt_witness(after, reference)


def _stub_producer(monkeypatch, table, *, fail=False):
    """Replace one table's producer with a stub that files a raw original in ``capture_dir``, then builds or fails."""
    import importlib

    import pyarrow as pa

    module, function = BUILDERS[table]
    calls = []

    def producer(stage, **kwargs):
        calls.append(kwargs)
        if "capture_dir" in kwargs:
            (kwargs["capture_dir"]).mkdir(parents=True, exist_ok=True)
            (kwargs["capture_dir"] / "original.bin").write_bytes(b"raw acquisition evidence")
        if fail:
            raise RuntimeError("producer failed")
        built = stage / (table + ".parquet")
        pq.write_table(pa.table({c: pa.array([], pa.string()) for c in REGISTRY[table]["input_fields"]}), built)
        return built

    monkeypatch.setattr(importlib.import_module("spicy_regs.transforms." + module), function, producer)
    return calls


ACQUIRING_TABLES = ["fec_committees", "fec_candidate_history", "fec_committee_history"]


def _rollup(table, output, **options):
    from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup

    if table == "fec_committees":
        options.setdefault("full_walk", True)
    return build_fec_identity_rollup(table, output, generation_id="g", **options)


@pytest.mark.parametrize("fail", [False, True], ids=["success", "failure"])
@pytest.mark.parametrize("table", ACQUIRING_TABLES)
def test_acquiring_producers_get_durable_storage(tmp_path, monkeypatch, table, fail):
    calls = _stub_producer(monkeypatch, table, fail=fail)

    def run(output, **options):
        if fail:
            with pytest.raises(RuntimeError, match="producer failed"):
                _rollup(table, output, **options)
        else:
            _rollup(table, output, **options)
        return calls[-1]["capture_dir"]

    def recorded(name):
        return json.loads((tmp_path / f"{name}.inputs" / "acquisition.json").read_text())

    default = run(tmp_path / "default")
    assert default == (tmp_path / "default.inputs" / "raw").resolve()
    assert recorded("default") == {"route": "refresh", "capture_dir": str(default)}
    monkeypatch.setenv("FEC_CAPTURE_DIR", str(tmp_path / "from-env"))
    assert run(tmp_path / "env") == (tmp_path / "from-env").resolve()
    assert recorded("env") == {"route": "refresh", "capture_dir": str((tmp_path / "from-env").resolve())}
    assert run(tmp_path / "explicit", capture_dir=tmp_path / "chosen") == (tmp_path / "chosen").resolve()
    assert recorded("explicit") == {"route": "refresh", "capture_dir": str((tmp_path / "chosen").resolve())}
    monkeypatch.setenv("FEC_CAPTURE_DIR", "")
    assert run(tmp_path / "blank-env") == (tmp_path / "blank-env.inputs" / "raw").resolve()
    # The raw file outlives the build and its staging directory, on success and on failure alike.
    for directory in (default, tmp_path / "from-env", tmp_path / "chosen"):
        assert (directory / "original.bin").read_bytes() == b"raw acquisition evidence"
    assert not list(tmp_path.glob(".fec-identity-producer-*"))
    before = len(calls)
    with pytest.raises(ValueError, match="inside the receipt bundle"):
        _rollup(table, tmp_path / "inside", capture_dir=tmp_path / "inside" / "raw")
    assert len(calls) == before and not (tmp_path / "inside.inputs").exists()


def test_selection_and_resume_routes_are_table_specific_and_name_their_originals(tmp_path, monkeypatch):
    import hashlib

    selection = tmp_path / "attempt" / "selection.json"
    selection.parent.mkdir()
    selection.write_bytes(b'{"retained": "selection"}')
    resume = tmp_path / "committees-attempt"
    resume.mkdir()
    (resume / "checkpoint.json").write_text("{}")
    history = _stub_producer(monkeypatch, "fec_candidate_history")
    _rollup("fec_candidate_history", tmp_path / "rebuilt", selection=selection)
    # A rebuild acquires nothing, so no raw storage is allocated; the bundle names the selection it consumed.
    assert history == [{"selection": selection.resolve()}]
    digest = "sha256:" + hashlib.sha256(selection.read_bytes()).hexdigest()
    assert json.loads((tmp_path / "rebuilt.inputs" / "acquisition.json").read_text()) == {
        "route": "selection",
        "selection": {"path": str(selection.resolve()), "sha256": digest},
    }
    assert not (tmp_path / "rebuilt.inputs" / "raw").exists()
    committees = _stub_producer(monkeypatch, "fec_committees")
    _rollup("fec_committees", tmp_path / "resumed", resume=resume)
    capture = (tmp_path / "resumed.inputs" / "raw").resolve()
    assert committees == [{"full_walk": True, "resume": resume.resolve(), "capture_dir": capture}]
    assert json.loads((tmp_path / "resumed.inputs" / "acquisition.json").read_text()) == {
        "route": "resume",
        "capture_dir": str(capture),
        "resume": str(resume.resolve()),
    }
    # None means "not given", exactly as the command line passes it.
    _rollup("fec_committees", tmp_path / "unset", selection=None)
    _rollup("fec_candidate_history", tmp_path / "unset-resume", resume=None)
    calls = len(history) + len(committees)
    for table, name, options, match in (
        ("fec_committees", "selection-on-committees", {"selection": selection}, "only the candidate and committee"),
        ("fec_source_catalog", "selection-on-catalog", {"selection": selection}, "only the candidate and committee"),
        ("fec_candidate_history", "resume-on-candidates", {"resume": resume}, "only a committee walk"),
        ("fec_committee_history", "resume-on-committee-history", {"resume": resume}, "only a committee walk"),
        ("fec_candidate_history", "selection-and-capture", {"selection": selection, "capture_dir": tmp_path}, "acquires nothing"),
        ("fec_committee_history", "selection-and-evidence", {"selection": selection, "evidence": object()}, "acquires nothing"),
        ("fec_candidate_history", "missing-selection", {"selection": tmp_path / "nowhere.json"}, "missing or unreadable"),
        ("fec_committees", "missing-resume", {"resume": tmp_path / "typo"}, "not a retained committee attempt"),
    ):
        with pytest.raises(ValueError, match=match):
            _rollup(table, tmp_path / name, **options)
        # A refused call leaves no custody directory, so the corrected retry to the same output works.
        assert not (tmp_path / f"{name}.inputs").exists()
    assert len(history) + len(committees) == calls


def test_committee_history_parent_restores_the_rows_its_builder_wrote(tmp_path, monkeypatch):
    """org-committee-links reads this parent through SelectedPriors, which refused it as undeclared (2026-10-05)."""
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    blank = dict.fromkeys(REGISTRY["fec_committee_history"]["input_fields"])
    written = [
        {**blank, "committee_id": "C00030718", "cycle": "2026", "name": "REALTORS PAC", "connected_organization_name": ""},
        {**blank, "committee_id": "C00030718", "cycle": "2024", "zip": "060111", "party": "Rep"},
    ]
    bundle = tmp_path / "bundle"
    with IdentityReceiptWriter(bundle, generation_id="g1", tables=["fec_committee_history", "fec_candidate_history"]) as writer:
        for row in written:
            writer.emit("fec_committee_history", row, input_witness=WITNESS)
        candidate = dict.fromkeys(REGISTRY["fec_candidate_history"]["input_fields"])
        candidate.update({key: "2024" if key == "cycle" else key for key in REGISTRY["fec_candidate_history"]["identity_fields"]})
        writer.emit("fec_candidate_history", candidate, input_witness=WITNESS)
    published = pq.read_table(bundle / "fec_committee_history.parquet")
    assert published["cycle"].to_pylist() == [2026, 2024]
    # The published party is folded; the restored parent below is the FEC's own spelling.
    assert published["party"].to_pylist() == [None, "REP"]
    remember_selection(tmp_path, [
        SelectedDataset(name, (bundle / (name + ".parquet"),), bundle / "etl_receipts.parquet", "g1")
        for name in ("fec_committee_history", "fec_candidate_history")
    ])
    priors = SelectedPriors(tmp_path / "selected", root=tmp_path, public_url="")
    restored = pq.read_table(priors.get("fec_committee_history"))
    assert restored.to_pylist() == written
    assert set(map(str, restored.schema.types)) == {"string"}
    # A parent with no declared reconstruction still refuses rather than hand a reader converted values.
    with pytest.raises(ValueError, match="No processing reconstruction declared for fec_candidate_history"):
        priors.get("fec_candidate_history")


def test_registration_statements_publish_the_source_namespace_that_tells_form_1_from_form_2(tmp_path):
    """Owner decision 2026-10-05: source_namespace is a subject column where its published value varies.

    Statements of organization (Form 1) and of candidacy (Form 2) share this table. The rows come from the real
    registry mapper, and the pair restores every field it wrote.
    """
    import duckdb
    import pyarrow as pa

    from spicy_regs.etl_receipts import decode_exact_json
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.transforms import fec_identity_observations as identity
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    from tests.test_fec_identity_observations import SELECTION, registry

    table = identity.STATEMENTS
    policy = dataset_policy(table)
    assert policy.subject_schema.field("source_namespace").type == pa.string()
    assert "source_namespace" not in policy.receipt_fields
    # fec_filings is not on the list: every published row there is fec-openfec-file-number, kept in the receipt.
    filings = dataset_policy(identity.FILINGS)
    assert "source_namespace" not in filings.receipt_fields and "source_namespace" in filings.subject_schema.names

    built = []
    for ordinal, mapping in enumerate((identity.FORM1, identity.FORM2)):
        source = registry(mapping)
        record = f"row/{ordinal}"
        locator = {**json.loads(source["source_locator_json"]), "source_record_id": record, "ordinal": ordinal}
        source.update(source_record_id=record, source_locator_json=json.dumps(locator))
        built.append(identity.map_registry(source, SELECTION, mapping)[0])
    expected = {built[0]["record_id"]: "fec-bulk-form1", built[1]["record_id"]: "fec-bulk-form2"}
    assert len(expected) == 2 and {row["record_id"]: row["source_namespace"] for row in built} == expected

    output = tmp_path / "statements"
    with IdentityReceiptWriter(output, generation_id="g1", tables=[table]) as writer:
        for row in built:
            writer.emit(table, row, input_witness=WITNESS)
    stored = pq.read_table(output / (table + ".parquet"))
    assert stored.schema.equals(policy.subject_schema)
    assert dict(zip(stored["record_id"].to_pylist(), stored["source_namespace"].to_pylist())) == expected
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert [receipt["outcome"] for receipt in receipts] == ["accepted"] * 2
    # Once: the receipt binds the subject row by version and does not hold a second copy of the value.
    assert all("source_namespace" not in decode_exact_json(receipt["processing_json"]) for receipt in receipts)

    fields = REGISTRY[table]["input_fields"]
    restored = [
        {name: row["conversion_inputs"][name] if name in row["conversion_inputs"] else row[name] for name in fields}
        for row in read_identity_rows(output, table, generation_id="g1")
    ]
    by_id = {row["record_id"]: row for row in built}
    assert {row["record_id"]: row for row in restored} == {key: {name: row.get(name) for name in fields} for key, row in by_id.items()}

    # The serving adapter restores the processing relation with the value read once, from the subject.
    selected = {table: {"subjects": [str(output / (table + ".parquet"))], "receipts": str(output / "etl_receipts.parquet"),
                        "generation_id": "g1"}}
    with duckdb.connect() as con:
        relation = ReceiptAdapter(con, {"families": {}}, "unused", local_native=selected).restore(table)
        assert [r[0] for r in con.execute(f'DESCRIBE "{relation}"').fetchall()].count("source_namespace") == 1
        assert dict(con.execute(f'SELECT record_id, source_namespace FROM "{relation}"').fetchall()) == expected

    # A changed namespace no longer joins its receipt.
    changed = stored.to_pylist()
    changed[0]["source_namespace"] = "changed"
    pq.write_table(pa.Table.from_pylist(changed, schema=stored.schema), output / (table + ".parquet"))
    with pytest.raises(ValueError, match="subject receipt"):
        list(read_identity_rows(output, table, generation_id="g1"))
