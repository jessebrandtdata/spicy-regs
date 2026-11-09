"""Coverage bridge uses selected receipts, with exact original source values."""

from contextlib import ExitStack
import copy
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from unittest.mock import patch
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_regs import coverage_reader as bridge
from spicy_regs import etl_bulk, regulations_bulk
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _digest, decode_exact_json, exact_json, selected_subject_policy
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import write_legislative_outputs
from spicy_regs.pipelines.rollups import subject_receipts
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
from spicy_regs.sources.publication import empty_index
from spicy_regs.transforms import regulations_receipts
from spicy_regs.transforms.regulations_receipts import (
    RESTORE_DATASETS,
    ReceiptInput,
    materialize_internal,
    write_held_dataset,
)
from tests.test_federal_register_bulk_restore import _source as federal_register_source
from tests.test_fr_docket_links_bulk_restore import _source as fr_docket_links_source


def member(path):
    return {
        "path": str(path.resolve()),
        "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
        "byteSize": path.stat().st_size,
    }


def fixture(tmp_path, dataset, values):
    raw = {f["name"]: None for f in field_registry()[dataset]["fields"]} | values
    source = tmp_path / (dataset + ".parquet")
    schema = pa.schema([(f["name"], pa.string()) for f in field_registry()[dataset]["fields"]])
    pq.write_table(pa.Table.from_pylist([raw], schema=schema), source)
    manifest = write_legislative_outputs([source], tmp_path / "native", generation_id="coverage-test")
    request = {
        "dataset": dataset,
        "generationId": "coverage-test",
        "subjects": [member(tmp_path / "native" / p) for p in manifest["subjects"][dataset]],
        "receipts": member(tmp_path / "native" / "etl_receipts.parquet"),
        "destination": str(tmp_path / "restored"),
    }
    return raw, request


def test_restores_source_spelling_and_native_pin_separately(tmp_path):
    raw, request = fixture(
        tmp_path,
        "bill_versions",
        {"bill_id": "hr1-119", "version_code": "is", "source": "GovInfo original label", "version_date": "2025-01-03"},
    )
    result = bridge.restore(request)
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]
    assert "printing_id" in dict(result["nativeSchema"]) and "source" not in dict(result["nativeSchema"])
    assert "source" in dict(result["processingSchema"])
    assert result["selection"] == {"subjects": request["subjects"], "receipts": request["receipts"]}


def test_restores_receipt_only_reads_without_a_public_subject(tmp_path):
    raw, request = fixture(
        tmp_path,
        "document_citation_reads",
        {
            "document_kind": "bill_section",
            "document_key": '["hr1-119","is","GovInfo original label","1"]',
            "text_sha256": "sha256:" + "a" * 64,
        },
    )
    assert not request["subjects"]
    result = bridge.restore(request)
    assert result["receiptOnly"] and result["nativeSchema"] == []
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]


def empty_legislative_request(tmp_path, dataset):
    schema = pa.schema([(f["name"], pa.string()) for f in field_registry()[dataset]["fields"]])
    source = tmp_path / (dataset + ".parquet")
    pq.write_table(pa.Table.from_batches([], schema=schema), source)
    manifest = write_legislative_outputs([source], tmp_path / "native", generation_id="empty-coverage")
    request = {
        "dataset": dataset,
        "generationId": "empty-coverage",
        "subjects": [member(tmp_path / "native" / path) for path in manifest["subjects"][dataset]],
        "receipts": member(tmp_path / "native/etl_receipts.parquet"),
        "destination": str(tmp_path / "restored"),
    }
    return source, request


@pytest.mark.parametrize("dataset", [
    "bill_summaries", "diff_summaries", "financial_changes", "section_classifications",
])
def test_declared_empty_legislative_subject_restores_with_retained_file_state(tmp_path, dataset):
    source, request = empty_legislative_request(tmp_path, dataset)
    result = bridge.restore(request)
    assert result["rows"] == 0
    assert result["selection"] == {key: request[key] for key in ("subjects", "receipts")}
    assert pq.read_table(result["urls"][0]).equals(pq.read_table(source), check_metadata=True)


@pytest.mark.parametrize("change", ["no-subject", "no-file-state", "wrong-schema"])
def test_empty_subject_still_requires_selected_schema_and_retained_file_state(tmp_path, change):
    _, request = empty_legislative_request(tmp_path, "bill_summaries")
    if change == "no-subject":
        request["subjects"] = []
        expected = "no receipt context"
    elif change == "no-file-state":
        rewrite_receipts(request, lambda table: table.slice(0, 0))
        expected = "Missing or duplicate retained source file state"
    else:
        subject = Path(request["subjects"][0]["path"])
        pq.write_table(pa.table({"wrong": pa.array([], type=pa.string())}), subject)
        request["subjects"] = [member(subject)]
        expected = "schema"
    with pytest.raises(ValueError, match=expected):
        bridge.restore(request)


def test_nonempty_subject_still_requires_its_receipts(tmp_path):
    _, request = fixture(tmp_path, "bill_versions", {"bill_id": "hr1-119", "version_code": "is", "source": "govinfo"})
    rewrite_receipts(request, lambda table: pa.Table.from_pylist(
        [row for row in table.to_pylist() if row["dataset"] != "bill_versions"], schema=table.schema,
    ))
    with pytest.raises(ValueError, match="no receipt context"):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


@pytest.mark.parametrize("reverse", [False, True])
def test_mixed_empty_and_nonempty_subjects_require_receipts_before_creating_output(tmp_path, reverse):
    _, request = empty_legislative_request(tmp_path, "bill_summaries")
    empty = Path(request["subjects"][0]["path"])
    nonempty = tmp_path / "nonempty.parquet"
    pq.write_table(pa.Table.from_pylist([{}], schema=pq.read_schema(empty)), nonempty)
    request["subjects"].append(member(nonempty))
    if reverse:
        request["subjects"].reverse()
    with pytest.raises(ValueError, match="no receipt context"):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


def test_empty_subject_cannot_borrow_another_legislative_datasets_file_state(tmp_path):
    _, request = empty_legislative_request(tmp_path, "bill_summaries")
    request["dataset"] = "diff_summaries"
    with pytest.raises(ValueError):
        bridge.restore(request)
    assert not list(Path(request["destination"]).rglob("processing/*.parquet"))


@pytest.mark.parametrize("dataset", ["members", "dockets", "fec_committees"])
def test_nonlegislative_empty_subject_still_requires_receipt_context(tmp_path, dataset):
    policy = subject_receipts.dataset_policy(dataset)
    subject, receipts = tmp_path / "empty.parquet", tmp_path / "receipts.parquet"
    pq.write_table(pa.Table.from_batches([], schema=policy.subject_schema), subject)
    pq.write_table(pa.Table.from_batches([], schema=RECEIPT_SCHEMA), receipts)
    request = {"dataset": dataset, "generationId": "empty-other-route", "subjects": [member(subject)],
               "receipts": member(receipts), "destination": str(tmp_path / "restored")}
    with pytest.raises(ValueError, match="no receipt context"):
        bridge.checked_request(request, subject_receipts.dataset_policy)
    assert not Path(request["destination"]).exists()


@pytest.mark.parametrize("change", ["sha", "size"])
def test_refuses_changed_inputs(tmp_path, change):
    _, request = fixture(tmp_path, "bill_versions", {"bill_id": "hr1-119", "version_code": "is", "source": "govinfo"})
    if change == "sha":
        request["receipts"]["sha256"] = "sha256:" + "f" * 64
    else:
        request["receipts"]["byteSize"] += 1
    with pytest.raises(ValueError):
        bridge.restore(request)


def test_schema_cli_describes_both_native_policy_families():
    for dataset, version in [("bill_versions", "legislative-documents/2"), ("bill_actions", "congress-subjects/1")]:
        result = json.loads(
            subprocess.check_output([sys.executable, "-m", "spicy_regs.coverage_reader", "--schema", dataset])
        )
        assert result["nativeSchema"] and result["policyVersion"] == version
        assert result["policy"]["dataset"] == dataset and result["implementationSha256"].startswith("sha256:")


def test_congress_native_source_fields_restore_using_its_maintained_reader(tmp_path):
    from spicy_regs.congress_receipts import write_congress_dataset

    source = tmp_path / "source.parquet"
    raw = {"bill_id": "hr1-119", "subjects_json": "[]", "url": "https://example.gov/original-bill"}
    pq.write_table(pa.Table.from_pylist([raw]), source)
    subject, receipts = write_congress_dataset(
        source, tmp_path / "native", dataset="congress_bills", generation_id="congress-coverage"
    )
    request = {
        "dataset": "congress_bills",
        "generationId": "congress-coverage",
        "subjects": [member(subject)],
        "receipts": member(receipts),
        "destination": str(tmp_path / "restored"),
    }
    result = bridge.restore(request)
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]
    assert result["policyVersion"] == "congress-subjects/1"


def rewrite_receipts(request, edit):
    """Rewrite the fixture receipts through `edit(table) -> table` and re-pin the member."""
    path = Path(request["receipts"]["path"])
    pq.write_table(edit(pq.read_table(path)), path)
    request["receipts"] = member(path)


def replaced(table, name, values):
    return table.set_column(
        table.schema.get_field_index(name), name, pa.array(values, type=table.schema.field(name).type)
    )


def bill_versions_request(tmp_path):
    return fixture(tmp_path, "bill_versions", {"bill_id": "hr1-119", "version_code": "is", "source": "govinfo"})[1]


def test_refuses_undeclared_policy_receipts(tmp_path):
    request = bill_versions_request(tmp_path)
    rewrite_receipts(request, lambda t: replaced(t, "policy_version", ["legislative-documents/0"] * t.num_rows))
    with pytest.raises(ValueError, match="Unsupported coverage receipt policy"):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


def test_declared_earlier_policy_receipts_are_admitted_and_undeclared_ones_refused(tmp_path):
    """Carried receipts keep the version they were written under; the call is direct because relabelling a row
    breaks its digest, which a full restore would refuse."""
    from spicy_regs.congress_receipts import write_congress_dataset

    source = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_pylist([{"congress": "119", "chamber": "house", "event_id": "7"}]), source)
    _, receipts = write_congress_dataset(
        source, tmp_path / "native", dataset="committee_meetings", generation_id="congress-coverage"
    )
    policy = bridge.dataset_policy("committee_meetings")
    assert policy.policy_version == "congress-subjects/2"
    table = pq.read_table(receipts)
    assert set(table["policy_version"].to_pylist()) == {"congress-subjects/2"}

    def relabelled(name, version):
        path = tmp_path / name
        pq.write_table(replaced(table, "policy_version", [version] + table["policy_version"].to_pylist()[1:]), path)
        return path

    bridge.current_receipts(receipts, "committee_meetings", policy)
    bridge.current_receipts(
        relabelled("declared.parquet", "congress-subjects/1"), "committee_meetings", policy
    )
    with pytest.raises(ValueError, match="Unsupported coverage receipt policy"):
        bridge.current_receipts(
            relabelled("undeclared.parquet", "congress-subjects/0"), "committee_meetings", policy
        )
    # A carried receipt keeps the generation that first wrote it; the pinned receipt bytes bind it, not its label.
    carried = tmp_path / "carried.parquet"
    pq.write_table(replaced(table, "generation_id", ["earlier-generation"] + table["generation_id"].to_pylist()[1:]), carried)
    bridge.current_receipts(carried, "committee_meetings", policy)
    emptied = tmp_path / "emptied.parquet"
    pq.write_table(replaced(table, "generation_id", [""] + table["generation_id"].to_pylist()[1:]), emptied)
    with pytest.raises(ValueError, match="lack a generation"):
        bridge.current_receipts(emptied, "committee_meetings", policy)


def test_refuses_receipts_missing_a_current_field(tmp_path):
    request = bill_versions_request(tmp_path)
    rewrite_receipts(request, lambda t: t.drop_columns(["policy_version"]))
    with pytest.raises(ValueError, match="Unsupported coverage receipt format"):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


def test_other_dataset_rows_are_ignored_and_carried_generations_restore(tmp_path):
    raw, request = fixture(tmp_path, "bill_versions", {"bill_id": "hr1-119", "version_code": "is", "source": "govinfo"})
    path = Path(request["receipts"]["path"])
    original = path.read_bytes()

    def append_unrelated(table):
        row = table.slice(0, 1).to_pylist()[0] | {"dataset": "unrelated", "generation_id": "other"}
        return pa.concat_tables([table, pa.Table.from_pylist([row], schema=table.schema)])

    rewrite_receipts(request, append_unrelated)
    result = bridge.restore(request)
    assert pq.read_table(result["urls"][0]).to_pylist() == [raw]

    path.write_bytes(original)
    request["receipts"] = member(path)
    request["destination"] = str(tmp_path / "mixed")
    # A receipt carried from an earlier generation restores: the file hash pins the bytes, not the label.
    def carried(table):
        rows = table.to_pylist()
        rows[0]["generation_id"] = "earlier"
        rows[0]["receipt_id"] = _digest({k: v for k, v in rows[0].items() if k != "receipt_id"})
        return pa.Table.from_pylist(rows, schema=table.schema)

    rewrite_receipts(request, carried)
    assert pq.read_table(bridge.restore(request)["urls"][0]).to_pylist() == [raw]


@pytest.mark.parametrize(
    ("column", "message"),
    [
        ("policy_version", "Unsupported coverage receipt policy"),
        ("generation_id", "lack a generation"),
    ],
)
def test_null_policy_or_generation_refuses(tmp_path, column, message):
    request = bill_versions_request(tmp_path)
    rewrite_receipts(request, lambda t: replaced(t, column, [None] + t.column(column).to_pylist()[1:]))
    with pytest.raises(ValueError, match=message):
        bridge.restore(request)
    assert not Path(request["destination"]).exists()


def test_malformed_request_refused_before_hashing(tmp_path):
    request = bill_versions_request(tmp_path)
    cases = [
        ({k: v for k, v in request.items() if k != "destination"}, "Unsupported coverage restoration request"),
        (request | {"extra": 1}, "Unsupported coverage restoration request"),
        (request | {"generationId": ""}, "Missing coverage receipt generation"),
        (request | {"subjects": {}}, "Coverage subjects must be explicit"),
    ]
    with patch.object(
        bridge, "checked_member", side_effect=AssertionError("hashed before the request was admitted")
    ) as hashed:
        for bad, message in cases:
            with pytest.raises(ValueError, match=message):
                bridge.restore(bad)
        hashed.assert_not_called()


def test_one_subject_requests_refuse_other_counts(tmp_path):
    request = bill_versions_request(tmp_path)
    with pytest.raises(ValueError, match="Coverage subjects must be explicit"):
        bridge.checked_request(request | {"subjects": request["subjects"] * 2}, bridge.dataset_policy, one_subject=True)


def test_existing_destination_is_refused_before_hashing(tmp_path):
    request = bill_versions_request(tmp_path)
    Path(request["destination"]).mkdir()
    with patch.object(bridge, "checked_member", side_effect=AssertionError("hashed before the destination check")):
        with pytest.raises(ValueError, match="new absolute directory"):
            bridge.restore(request)


def test_identity_covers_every_policy_file_the_earlier_receipt_chains_read():
    """current_receipts admits what the chains declare, so each file they read must be part of the packaged identity."""
    root = bridge.PACKAGE_ROOT
    source = (root / "earlier_receipt_policies.py").read_text()
    reads = re.findall(r'files\("([\w.]+)"\)\s*\.joinpath\(\s*"([^"]+)"', source)
    assert reads and len(reads) == source.count(".joinpath(")
    covered = set(bridge._identity_files())
    assert root / "earlier_receipt_policies.py" in covered
    for package, name in reads:
        folder = root.joinpath(*package.split(".")[1:])
        # "etl_policies/" + dataset + ".json" names the directory: every policy file in it is read by some chain.
        files = sorted((folder / name).glob("*.json")) if name.endswith("/") else [folder / name]
        assert files and all(file.is_file() for file in files)
        assert set(files) <= covered, name


@pytest.mark.parametrize(
    "changed",
    [
        "earlier_receipt_policies.py",
        "navigation_policy_history.json",
        "fec_policy_history.json",
        "scorecards/historical_rating_policies.json",
        "etl_policies/gao_reports.json",
    ],
)
def test_identity_changes_with_the_bytes_of_the_earlier_receipt_chains(tmp_path, monkeypatch, changed):
    copy = tmp_path / "package"
    for file in bridge._identity_files():
        if file != Path(bridge.__file__):
            target = copy / file.relative_to(bridge.PACKAGE_ROOT)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(file, target)
    original = bridge.implementation_identity()
    monkeypatch.setattr(bridge, "PACKAGE_ROOT", copy)
    assert bridge.implementation_identity() == original
    with (copy / changed).open("ab") as stream:
        stream.write(b"\n")
    assert bridge.implementation_identity() != original


# The selected regulatory restore admits and scopes its pair once, whichever route restores it.
@pytest.fixture
def route_warnings():
    messages = []
    sink = logger.add(lambda message: messages.append(message.record["message"]), level="WARNING")
    yield messages
    logger.remove(sink)


def _regulatory_input(tmp_path, dataset):
    if dataset == "federal_register":
        rows = [{"document_number": "2026-00001", "publication_date": "2026-06-30"}]
        return federal_register_source(tmp_path, rows)[0]
    if dataset == "fr_docket_links":
        return fr_docket_links_source(tmp_path)[0]
    schema = pa.schema([("document_id", pa.string()), ("title", pa.string())], metadata={b"dump_date": b"2026-06-30"})
    source = tmp_path / "source.parquet"
    rows = [{"document_id": "D-1", "title": "one"}, {"document_id": "D-2", "title": "two"}]
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source)
    subject, receipts = write_held_dataset("documents", source, tmp_path / "native", generation_id="g")
    return ReceiptInput("documents", (subject,), receipts, "g")


def _restore_request(tmp_path, selected, name="restored"):
    return {
        "dataset": selected.dataset,
        "generationId": selected.generation_id,
        "subjects": [member(path) for path in selected.subjects],
        "receipts": member(selected.receipts),
        "destination": str(tmp_path / name),
    }


REGULATORY_RESTORES = ["federal_register", "fr_docket_links", "documents"]


def test_regulatory_policies_agree_between_the_selected_prior_reader_and_the_materializers():
    assert RESTORE_DATASETS == {"dockets", "documents", "federal_register", "fr_docket_links"}
    for dataset in sorted(RESTORE_DATASETS):
        assert bridge.dataset_policy(dataset) == regulations_receipts.policy(dataset)


@pytest.mark.parametrize("dataset", REGULATORY_RESTORES)
def test_regulatory_restore_admits_and_scopes_the_pair_once_under_the_selected_policy(
    tmp_path, route_warnings, dataset
):
    selected = _regulatory_input(tmp_path, dataset)
    admissions = [
        patch.object(module, "validate_receipt_bundle", wraps=module.validate_receipt_bundle)
        for module in (subject_receipts, regulations_bulk)
    ]
    scopings = [
        patch.object(module, name, wraps=getattr(module, name))
        for module, name in ((subject_receipts, "select_pinned_receipts"), (regulations_bulk, "select_receipts"),
                             (regulations_receipts, "select_receipts"))
    ]
    with ExitStack() as stack:
        admitted = [stack.enter_context(call) for call in admissions]
        scoped = [stack.enter_context(call) for call in scopings]
        restored = bridge.restore(_restore_request(tmp_path, selected))
    assert [call.call_count for call in admitted] == [0, 1]
    assert [call.call_count for call in scoped] == [1, 0, 0]
    [(_, args, _)] = admitted[1].mock_calls
    assert args[2] == [selected_subject_policy(bridge.dataset_policy(dataset), selected.subjects)]
    assert route_warnings == []
    assert restored["rows"] == pq.ParquetFile(selected.subjects[0]).metadata.num_rows


@pytest.mark.parametrize("dataset", REGULATORY_RESTORES)
def test_restored_regulatory_processing_member_is_the_member_each_materializer_route_writes(
    tmp_path, route_warnings, dataset
):
    selected = _regulatory_input(tmp_path, dataset)
    restored = bridge.restore(_restore_request(tmp_path, selected))
    [processing] = restored["processingMembers"]
    # Without ``scoped`` the materializer selects the shared receipts itself: the earlier two-pass route.
    unscoped = materialize_internal(selected, tmp_path / "unscoped.parquet", bulk=True)
    assert processing["sha256"] == bridge.file_hash(unscoped)
    row = materialize_internal(selected, tmp_path / "row.parquet", bulk=False)
    assert pq.read_table(unscoped).equals(pq.read_table(row), check_metadata=True)
    assert route_warnings == []


@pytest.mark.parametrize("dataset", REGULATORY_RESTORES)
def test_forced_batch_ineligibility_logs_the_row_route_and_restores_identically(
    tmp_path, monkeypatch, route_warnings, dataset
):
    selected = _regulatory_input(tmp_path, dataset)
    bulk = bridge.restore(_restore_request(tmp_path, selected, "bulk"))
    assert route_warnings == []

    def forced(*args, **kwargs):
        raise etl_bulk.NotBulkEligible("forced for the test")

    monkeypatch.setattr(regulations_bulk, "_materialize_selected", forced)
    row = bridge.restore(_restore_request(tmp_path, selected, "row"))
    rows = sum(pq.ParquetFile(path).metadata.num_rows for path in selected.subjects)
    assert route_warnings == [f"regulatory restore route=row dataset={dataset} rows={rows} reason=forced for the test"]
    assert pq.read_table(row["urls"][0]).equals(pq.read_table(bulk["urls"][0]), check_metadata=True)
    assert row["rows"] == bulk["rows"]


def test_a_subject_schema_other_than_the_declared_one_logs_the_row_route_before_it_refuses(tmp_path, route_warnings):
    selected = _regulatory_input(tmp_path, "federal_register")
    subject = pq.read_table(selected.subjects[0])
    pq.write_table(subject.append_column("undeclared", pa.nulls(subject.num_rows, pa.string())), selected.subjects[0])
    with pytest.raises(ValueError, match="Subject schema differs from policy"):
        materialize_internal(selected, tmp_path / "refused.parquet", bulk=True)
    assert route_warnings == [
        f"regulatory restore route=row dataset=federal_register rows={subject.num_rows} reason=subject-schema-differs"
    ]


def _corrupt(selected, change):
    """Rewrite the selected pair's last accepted receipt (or add one) so admission must refuse it."""
    rows = pq.read_table(selected.receipts).to_pylist()
    last = next(row for row in reversed(rows) if row["outcome"] == "accepted")

    def sealed(row):
        row["receipt_id"] = _digest({key: value for key, value in row.items() if key != "receipt_id"})

    def processing(row, edit):
        values = decode_exact_json(row["processing_json"])
        edit(values)
        row["processing_json"] = exact_json(values)

    if change == "orphan":  # a second accepted receipt whose subject row does not exist
        extra = copy.deepcopy(last)
        extra["attempt_id"] += "-orphan"
        processing(extra, lambda values: values["raw_conversion_inputs"].update(document_number="2099-99999"))
        extra["identity_json"] = extra["identity_json"].replace("2026-00001", "2099-99999")
        sealed(extra)
        rows.append(extra)
    elif change == "ambiguous":
        extra = copy.deepcopy(last)
        extra["attempt_id"] += "-another"
        sealed(extra)
        rows.append(extra)
    elif change == "missing":
        rows.remove(last)
    elif change == "digest":
        last["receipt_id"] = "sha256:" + "0" * 64
    elif change == "undeclared-policy":
        last["policy_version"] = "regulations-native-v0"
        sealed(last)
    elif change == "raw":
        processing(last, lambda values: values["raw_conversion_inputs"].update(title="changed"))
        sealed(last)
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), selected.receipts)


def _restore_through_selected_priors(tmp_path, selected, route, monkeypatch):
    if route == "row":
        def forced(*args, **kwargs):
            raise etl_bulk.NotBulkEligible("forced for the test")

        monkeypatch.setattr(regulations_bulk, "_materialize_selected", forced)
    priors = SelectedPriors(tmp_path / "selected", index=empty_index(), public_url="https://example.invalid")
    monkeypatch.setattr(priors.selected, "select", lambda _: selected)
    try:
        return priors.get(selected.dataset)
    except Exception as error:  # the refusal is the observation
        return f"{type(error).__name__}: {error}"


FR_ROWS = [
    {"document_number": "2026-00001", "publication_date": "2025-06-30", "title": "first"},
    {"document_number": "2026-00002", "publication_date": "2026-06-30", "title": "second"},
]


@pytest.mark.parametrize("change", ["orphan", "ambiguous", "missing", "digest", "undeclared-policy", "raw"])
def test_a_corrupted_selected_pair_refuses_identically_on_the_bulk_and_the_row_route(tmp_path, monkeypatch, change):
    """Removing get()'s own admission must not let any corruption through on either restore route."""
    outcomes = {}
    for route in ("bulk", "row"):
        directory = tmp_path / route
        directory.mkdir()
        selected = federal_register_source(directory, FR_ROWS)[0]
        _corrupt(selected, change)
        with monkeypatch.context() as patch_route:
            outcomes[route] = _restore_through_selected_priors(directory, selected, route, patch_route)
    assert isinstance(outcomes["bulk"], str), "the bulk route restored a corrupted pair"
    assert outcomes["bulk"] == outcomes["row"]


@pytest.mark.parametrize("change", ["ambiguous", "missing", "digest", "undeclared-policy"])
def test_a_corrupted_pair_of_a_dataset_without_a_batch_route_refuses_through_the_row_reader(tmp_path, monkeypatch, change):
    """cfr_sections has no batch restore: the row reader's own admission is the only one."""
    source = tmp_path / "cfr.parquet"
    pq.write_table(pa.table({"granule_id": ["CFR-2025-title1-vol1-sec1-1"], "last_modified": ["2025-01-01"]}), source)
    subject, receipts = write_held_dataset("cfr_sections", source, tmp_path / "cfr", generation_id="cfr1")
    selected = ReceiptInput("cfr_sections", (subject,), receipts, "cfr1")
    assert isinstance(_restore_through_selected_priors(tmp_path / "ok", selected, "bulk", monkeypatch), Path)
    _corrupt(selected, change)
    outcome = _restore_through_selected_priors(tmp_path / "bad", selected, "bulk", monkeypatch)
    assert isinstance(outcome, str), "a corrupted pair was restored"
