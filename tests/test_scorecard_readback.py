"""Reuse fully verified scorecard rows synchronously; corrupt or late reads refuse."""

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.generations import build_generation
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, read_family, write_family
from spicy_regs.scorecards.retained import check_preserved
from spicy_regs.sources import publication as pub
from spicy_regs.transforms import build_scorecards as transform
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import combine, edition, evidence, journal, prior, provider, run, tables


def native_prior(tmp_path, monkeypatch, selected_evidence):
    old = combine(tables(edition("2024")), tables(edition("2025")))
    directory = tmp_path / "old"
    files = write_family(directory, old)
    generation = tmp_path / "old-generation"
    build_generation(generation, family="scorecards", files=files, expected_keys=tuple(p.name for p in files),
                     **generation_options(directory, SOURCE_NAMES))
    store = Store()
    index = pub.publish_generation(generation, client=store, bucket="test", prior_index=pub.empty_index())
    selected_evidence.read_snapshot = index

    def fetch(url, member, target, key=None):
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    return old, lambda key, target: fetch(None, pub.single_member(index, key), target), lambda target: fetch(
        None, pub.receipt_members(index, dataset="scorecards")[0], target
    )


@pytest.mark.parametrize("native", [False, True])
def test_readback_validator_gets_exact_prior_and_persisted_rows_after_validation(tmp_path, monkeypatch, native):
    selected = edition("2025")
    e = evidence(tmp_path)
    options = {}
    if native:
        old, download, receipt_download = native_prior(tmp_path, monkeypatch, e)
        options["download_prior_receipts"] = receipt_download
        # Native admission must use the receipt reader, never eager raw subject
        # materialization. The wrapper still permits every real streamed read.
        original = pq.ParquetFile

        class StreamedOnly:
            def __init__(self, *args, **kwargs):
                self.value = original(*args, **kwargs)

            def __getattr__(self, name):
                return getattr(self.value, name)

            def __enter__(self):
                self.value.__enter__()
                return self

            def __exit__(self, *args):
                return self.value.__exit__(*args)

            def read(self, *args, **kwargs):
                pytest.fail("Native subject eagerly materialized instead of receipt-checked reconstruction")

        monkeypatch.setattr(pq, "ParquetFile", StreamedOnly)
    else:
        old = combine(tables(edition("2024")), tables(edition("2025")))
        download = prior(e, old, tmp_path)
    p = provider([selected], {selected.scorecard_id: tables})
    validated = []
    original_validate = p.validate

    def validate(rows):
        original_validate(rows)
        validated.append(rows)

    p.validate = validate
    calls = []

    def assess(previous, current):
        assert validated[-1] is current
        assert previous == old
        assert current == read_family(tmp_path / "build", SOURCE_NAMES)
        assert not any(row["event"] == "scorecard-refresh" for row in journal(e))
        counts = check_preserved(previous, current, scorecard_ids={selected.scorecard_id}, publisher_ids={"lcv"})
        assert counts["scorecard_member_ratings"] == 2
        calls.append((previous, current))

    run(tmp_path, p, e, download_prior=download, validate_readback=assess, **options)
    assert len(calls) == 1
    assert any(row["event"] == "scorecard-refresh" for row in journal(e))


def test_readback_validator_failure_aborts_before_success(tmp_path):
    selected = edition("2025")
    e = evidence(tmp_path)
    calls = []

    def reject(previous, current):
        calls.append((previous, current))
        raise ValueError("Preservation assessment refused")

    with pytest.raises(ValueError, match="assessment refused"):
        run(tmp_path, provider([selected], {selected.scorecard_id: tables}), e,
            download_prior=lambda *args: False, validate_readback=reject)
    assert len(calls) == 1
    assert not any(row["event"] == "scorecard-refresh" for row in journal(e))


@pytest.mark.parametrize("corruption", ["subject", "receipt", "generation", "late-raw"])
def test_bad_persisted_family_never_reaches_readback_validator(tmp_path, monkeypatch, corruption):
    original_write = transform.write_family

    def corrupt(directory, *args, **kwargs):
        if corruption == "late-raw":
            changed = deepcopy(args[0])
            changed["scorecard_snapshots"][0]["observed_at"] = "2040-01-01T00:00:00Z"
            return original_write(directory, changed, *args[1:], **kwargs)
        result = original_write(directory, *args, **kwargs)
        if corruption == "generation":
            state = directory / "scorecard-etl-build.json"
            value = json.loads(state.read_text())
            value["generation_id"] = ""
            state.write_text(json.dumps(value))
        else:
            path = directory / ("scorecards.parquet" if corruption == "subject" else "etl_receipts.parquet")
            arrow = pq.read_table(path)
            rows = arrow.to_pylist()
            rows[0]["title" if corruption == "subject" else "processor"] = "source corruption"
            pq.write_table(pa.Table.from_pylist(rows, schema=arrow.schema), path)
        return result

    monkeypatch.setattr(transform, "write_family", corrupt)
    called = []
    selected = edition("2025")
    e = evidence(tmp_path)
    with pytest.raises(ValueError):
        run(tmp_path, provider([selected], {selected.scorecard_id: tables}), e,
            download_prior=lambda *args: False, validate_readback=lambda *rows: called.append(rows))
    assert called == []
    assert not any(row["event"] == "scorecard-refresh" for row in journal(e))


def test_prior_pin_corruption_refuses_before_callback(tmp_path, monkeypatch):
    e = evidence(tmp_path)
    _, download, receipts = native_prior(tmp_path, monkeypatch, e)

    def changed(key, target):
        download(key, target)
        if key == "scorecards.parquet":
            target.write_bytes(target.read_bytes() + b"source drift")
        return True

    selected = edition("2025")
    called = []
    with pytest.raises(transform.ScorecardRefreshError, match="immutable pin"):
        run(tmp_path, provider([selected], {selected.scorecard_id: tables}), e,
            download_prior=changed, download_prior_receipts=receipts,
            validate_readback=lambda *rows: called.append(rows))
    assert called == []


def preparation_fixture(tmp_path, monkeypatch, *, fault=None):
    from hashlib import sha256
    from tests.test_scorecard_prepare import PREPARE

    selected = edition("2025")
    previous = combine(tables(edition("2024")), tables(selected))
    current = combine(
        {name: [row for row in rows if name == "scorecard_publishers" or row["scorecard_id"] == "lcv:2024"]
         for name, rows in previous.items()}, tables(selected)
    )
    fake = SimpleNamespace(entries={selected.scorecard_id: dict(edition_object=selected)},
                           reference_publishers={}, complete=lambda: None)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(dict(schema_version="1", entries=[dict(adapter="lcv", edition=dict(publisher_id="lcv"))])))
    index = tmp_path / "index.json"
    index.write_text(json.dumps(pub.empty_index()))
    monkeypatch.setattr(PREPARE, "RetainedScorecardBatch", lambda *args, **kwargs: fake)
    monkeypatch.setattr(PREPARE, "installed_provider", lambda: SimpleNamespace(get_adapter=lambda name: None))
    monkeypatch.setattr(PREPARE, "generation_options", lambda *args: {})
    calls = []

    def build(directory, *, validate_readback, validate_acquisitions, **options):
        validate_acquisitions()
        # The real retained batch populates these observations during acquire,
        # rather than at construction. Read them only at the verified boundary.
        fake.reference_publishers["lcv"] = current["scorecard_publishers"][0]
        altered = deepcopy(current)
        if fault == "unselected-scope":
            altered["scorecard_member_ratings"][0]["value_text"] = "changed prior row"
        elif fault == "publisher":
            altered["scorecard_publishers"][0]["name"] = "not the qualified publisher"
        if fault != "missing-assessment":
            validate_readback(previous, altered)
        return ()

    def generation(*args, **kwargs):
        calls.append("generation")
        raise RuntimeError("verified assessment reached copied-generation boundary")

    monkeypatch.setattr(PREPARE, "build_scorecards", build)
    monkeypatch.setattr(PREPARE, "build_generation", generation)
    return PREPARE, SimpleNamespace(output=tmp_path / "candidate", private_observations=tmp_path / "private",
        plan=plan_path, plan_sha256=sha256(plan_path.read_bytes()).hexdigest(), index=index,
        corpus=tmp_path, public_url="https://data.example"), calls


def test_preparation_assesses_verified_rows_before_separate_generation_admission(tmp_path, monkeypatch):
    prepare, args, calls = preparation_fixture(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError, match="copied-generation boundary"):
        prepare.prepare(args)
    assert calls == ["generation"]


@pytest.mark.parametrize("fault", ["unselected-scope", "publisher", "missing-assessment"])
def test_preparation_preservation_reference_and_missing_assessment_refuse_before_generation(tmp_path, monkeypatch, fault):
    prepare, args, calls = preparation_fixture(tmp_path, monkeypatch, fault=fault)
    with pytest.raises(ValueError):
        prepare.prepare(args)
    assert calls == []


def test_complete_preparation_uses_late_publisher_reference_and_real_generation_checks(tmp_path, monkeypatch):
    from hashlib import sha256
    from tests.test_scorecard_prepare import PREPARE
    from spicy_regs.generations import verify_generation

    selected = edition("2025")
    p = provider([selected], {selected.scorecard_id: tables})
    adapter = p.get_adapter("lcv")

    class Batch:
        parser_version = adapter.parser_version
        entries = {selected.scorecard_id: dict(edition_object=selected)}

        def __init__(self):
            self.reference_publishers = {}
            self.acquired = False

        def list_scorecards(self, context):
            return adapter.list_scorecards(context)

        def acquire_scorecard(self, edition, context):
            result = adapter.acquire_scorecard(edition, context)
            self.reference_publishers["lcv"] = result.tables["scorecard_publishers"][0]
            self.acquired = True
            return result

        def complete(self):
            assert self.acquired

    batch = Batch()
    monkeypatch.setattr(PREPARE, "RetainedScorecardBatch", lambda *args, **kwargs: batch)
    monkeypatch.setattr(PREPARE, "installed_provider", lambda: p)
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps(dict(schema_version="1", entries=[dict(adapter="lcv", edition=dict(publisher_id="lcv"))])))
    index = tmp_path / "index.json"
    index.write_text(json.dumps(pub.empty_index()))
    args = SimpleNamespace(output=tmp_path / "candidate", private_observations=tmp_path / "private",
        plan=plan, plan_sha256=sha256(plan.read_bytes()).hexdigest(), index=index,
        corpus=tmp_path, public_url="https://offline.example")
    report = PREPARE.prepare(args)
    assert report["status"] == "prepared_not_published"
    assert report["counts"]["scorecard_members"] == 2
    assert report["counts"]["scorecard_member_ratings"] == 2
    assert report["publisher_or_model_bodies_in_public_evidence"] == 0
    assert report["source_network_requests"] == 0
    assert all(count == 0 for count in report["preserved_row_counts"].values())
    generation = verify_generation(args.output / "generation")
    assert generation.pin.as_dict() == report["generation"]


def carried_public_readback(tmp_path, monkeypatch, corruption=None, *, no_ratings=False):

    from spicy_regs.scorecards.operations import readback as readback
    previous = tables(edition("2024"))
    first = tmp_path / "prior"
    write_family(first, previous, generation_id="original-source-generation")
    incoming = tables(edition("2025"))
    if no_ratings:
        incoming["scorecard_member_ratings"] = []
    current = combine(previous, incoming)
    candidate = tmp_path / "candidate"
    files = write_family(
        candidate, current, generation_id="current-container-generation", prior_receipts=first / "etl_receipts.parquet"
    )
    generation = tmp_path / "generation"
    artifact = build_generation(
        generation,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        **generation_options(candidate, SOURCE_NAMES),
    )
    store = Store()
    index = pub.publish_generation(generation, client=store, bucket="test", prior_index=pub.empty_index())
    monkeypatch.setattr(pub, "load_index", lambda *args: index)

    def fetch(url, member, path, *args, **kwargs):
        path.write_bytes(store.objects[member.path])
        if member.key == "etl_receipts.parquet" and corruption:
            arrow = pq.read_table(path)
            rows = arrow.to_pylist()
            row = next(
                row for row in rows if row["dataset"] == "scorecard_member_ratings" and row["outcome"] == "accepted"
            )
            row["generation_id" if corruption == "origin" else "outcome"] = (
                "changed-origin" if corruption == "origin" else "refused"
            )
            # Isolate semantic readback checks after a transport claims pinned bytes.
            pq.write_table(pa.Table.from_pylist(rows, schema=arrow.schema), path)
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    output = tmp_path / "public-readback"
    output.mkdir()
    args = SimpleNamespace(output=output, public_url="https://offline.example", mcp_url="https://offline.example/mcp")
    prepared = dict(
        generation=artifact.pin.as_dict(),
        generation_directory=str(generation),
        counts={name: len(rows) for name, rows in current.items()},
    )
    authenticated = dict(family_entry=index["families"]["scorecards"])
    return readback, args, prepared, authenticated


@pytest.mark.parametrize("corruption", [None, "origin", "count"])
def test_public_readback_compares_carried_origin_distribution_to_exact_candidate(tmp_path, monkeypatch, corruption):
    readback, args, prepared, authenticated = carried_public_readback(tmp_path, monkeypatch, corruption)
    if corruption:
        with pytest.raises(ValueError, match="origins or outcome counts|differs from its immutable pin"):
            readback.public_files(args, prepared, authenticated)
    else:
        connection, _ = readback.public_files(args, prepared, authenticated)
        try:
            origins = connection.execute(
                "SELECT DISTINCT generation_id FROM etl_receipts ORDER BY generation_id"
            ).fetchall()
            assert origins == [("current-container-generation",), ("original-source-generation",)]
        finally:
            connection.close()
        assert {member["origin"] for member in json.loads((args.output / "public-files.json").read_bytes())["members"]} == {"downloaded"}


@pytest.mark.parametrize("mode,corruption,discovery", [
    ("full", fault, "counts") for fault in (None, "origin", "count", "missing-pin", "stale-pin", "field-state", "native-value")
] + [("source-adoption", fault, "counts") for fault in (None, "native-value", "missing-scope", "stale-pin", "native-scorecard", "no-ratings", "no-source-columns")]
  + [(mode, None, shape) for mode in ("full", "source-adoption") for shape in ("names", "entries", "flat")])
def test_hosted_readback_counts_all_source_origins_and_binds_family_among_other_pins(tmp_path, monkeypatch, mode, corruption, discovery):
    import asyncio
    from contextlib import asynccontextmanager
    import duckdb

    readback, args, prepared, authenticated = carried_public_readback(tmp_path, monkeypatch, no_ratings=corruption == "no-ratings")
    args.mode = mode
    args.accepted_scopes = ["lcv:2025"] + (["absent:scope"] if corruption == "missing-scope" else [])
    connection, family = readback.public_files(args, prepared, authenticated)
    server = duckdb.connect()
    for key in family["tables"]:
        server.read_parquet(str(args.output / "members" / key)).create_view(key.removesuffix(".parquet"))
    if corruption == "no-source-columns":
        removed = {"capture_id", "source_url", "source_path"}
        family = deepcopy(family)
        family["tables"]["scorecard_member_ratings.parquet"]["columns"] = [
            column for column in family["tables"]["scorecard_member_ratings.parquet"]["columns"]
            if column[0] not in removed
        ]
        columns = ",".join(column[0] for column in family["tables"]["scorecard_member_ratings.parquet"]["columns"])
        for database in (connection, server):
            database.read_parquet(str(args.output / "members/scorecard_member_ratings.parquet")).create_view("_ratings")
            database.execute("CREATE OR REPLACE VIEW scorecard_member_ratings AS SELECT " + columns + " FROM _ratings")
    receipts = pq.read_table(args.output / "members/etl_receipts.parquet")
    rows = receipts.to_pylist()
    rows.append(dict(rows[0], dataset="members", generation_id="unrelated-family-generation"))
    server.register("_hosted_receipts", pa.Table.from_pylist(rows, schema=receipts.schema))
    server.execute("CREATE VIEW etl_receipts AS SELECT * FROM _hosted_receipts")
    receipts_pin = dict(
        artifact_digest=family["artifactDigest"],
        generation_id=family["etlReceipts"]["generationId"],
        sha256=family["etlReceipts"]["sha256"],
        rows=family["etlReceipts"]["rows"],
        datasets=family["etlReceipts"]["datasets"],
    )

    @asynccontextmanager
    async def stream(*args, **kwargs):
        yield None, None

    class Session:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def initialize(self):
            pass

        async def call_tool(self, tool, arguments):
            if mode == "source-adoption":
                assert tool != "read_receipt_fields"
                assert arguments.get("table") != "etl_receipts"
                assert "FROM etl_receipts" not in arguments.get("sql", "")
            if tool == "list_sources" and "subject" in arguments:
                assert discovery == "counts", "names and full replies must not cause extra subject requests"
                assert arguments["subject"] == "unassigned", "readback pages only until the family's tables are listed"
                body = dict(subjects=[dict(subject=None, tables=[dict(table=key.removesuffix(".parquet"))
                                                                 for key in family["tables"]])])
            elif tool == "list_sources":
                names = [key.removesuffix(".parquet") for key in family["tables"]]
                if discovery == "counts":
                    body = dict(subjects=[dict(subject="unassigned", table_count=len(names), rows=0),
                                          dict(subject="congress", table_count=1, rows=0)])
                elif discovery == "flat":
                    body = dict(tables=[dict(table=name) for name in names])
                else:
                    rows = names if discovery == "names" else [dict(table=name) for name in names]
                    body = dict(subjects=[dict(subject="unassigned", tables=rows),
                                          dict(subject="congress", tables=["laws"] if discovery == "names" else [dict(table="laws")])])
            elif tool == "describe_table":
                body: dict[str, object] = dict(available=True)
                if arguments["table"] == "scorecard_member_ratings":
                    body.update(
                        publication=dict(artifact_digest=family["artifactDigest"]),
                        columns=[
                            dict(column_name=name, column_type=kind)
                            for name, kind in family["tables"]["scorecard_member_ratings.parquet"]["columns"]
                        ],
                    )
            elif tool == "read_receipt_fields":
                from spicy_regs import receipt_lookup
                from spicy_regs.mcp_server import _jsonify
                local = {"selected_tables": ["scorecard_member_ratings"], "receipt_members": {
                    family["prefix"] + "/" + family["etlReceipts"]["key"]:
                        str(args.output / "members/etl_receipts.parquet")}}
                body = receipt_lookup.read_fields(server, index={"families": {"scorecards": family}},
                    local=local, base_url=args.public_url,
                    entry={"columns": [dict(column_name=name, column_type=kind)
                           for name, kind in family["tables"]["scorecard_member_ratings.parquet"]["columns"]]},
                    plain=_jsonify, **arguments)
                if corruption == "field-state":
                    body["keys"][0]["fields"][arguments["fields"][0]]["state"] = "unread"
            else:
                cursor = server.execute(arguments["sql"])
                columns = [column[0] for column in cursor.description]
                records = [readback._jsonify(dict(zip(columns, row, strict=True))) for row in cursor.fetchall()]
                if corruption == "native-value" and "WHERE scorecard_id=" in arguments["sql"]:
                    records[0]["value_text"] = "changed source rating"
                if mode == "source-adoption" and "WHERE c.scorecard_id IN" in arguments["sql"]:
                    if corruption == "native-value":
                        records[0]["published_value"] = "changed source rating"
                if corruption == "native-scorecard" and "SELECT * FROM scorecards WHERE" in arguments["sql"]:
                    records[0]["title"] = "changed native scorecard"
                pins = {
                    key.removesuffix(".parquet"): dict(artifact_digest=family["artifactDigest"])
                    for key in family["tables"]
                }
                if mode == "source-adoption" and corruption == "stale-pin":
                    pins["scorecards"]["artifact_digest"] = "sha256:" + "f" * 64
                if "FROM etl_receipts" in arguments["sql"]:
                    assert sum(row["rows"] for row in records) == family["etlReceipts"]["rows"]
                    assert {row["generation_id"] for row in records} == {
                        "original-source-generation",
                        "current-container-generation",
                    }
                    source_pin = deepcopy(receipts_pin)
                    if corruption == "missing-pin":
                        source_pin = None
                    elif corruption == "stale-pin":
                        source_pin["artifact_digest"] = "sha256:" + "f" * 64
                    elif corruption == "origin":
                        records[0]["generation_id"] = "changed-origin"
                    elif corruption == "count":
                        records[0]["rows"] += 1
                    families = {"members": dict(artifact_digest="sha256:" + "e" * 64)}
                    if source_pin is not None:
                        families["scorecards"] = source_pin
                    pins = dict(etl_receipts=dict(status="managed_receipts", families=families))
                body = dict(columns=columns, rows=records, truncated=False, publication=pins)
            return SimpleNamespace(model_dump=lambda **kwargs: dict(structuredContent=body))

    monkeypatch.setattr(readback, "streamable_http_client", stream)
    monkeypatch.setattr(readback, "ClientSession", Session)
    try:
        if corruption and corruption not in {"no-ratings", "no-source-columns"}:
            with pytest.raises(ValueError, match="selected scorecards family|outcomes differ|source fields or read states|Hosted rows differ|every accepted scorecard|different source generation"):
                asyncio.run(readback.hosted(args, connection, family))
        else:
            calls = asyncio.run(readback.hosted(args, connection, family))
            if mode == "full":
                assert calls[-1]["label"] == "source_receipt_counts"
            else:
                assert calls[-1]["label"] == "accepted_scope_native_values"
                native = json.loads((args.output / "accepted_scorecards.json").read_bytes())["structuredContent"]
                assert [row["scorecard_id"] for row in native["rows"]] == ["lcv:2025"]
                sample = json.loads((args.output / "accepted_scope_native_values.json").read_bytes())["structuredContent"]
                assert [row["scorecard_id"] for row in sample["rows"]] == ([] if corruption == "no-ratings" else ["lcv:2025"])
                if corruption == "no-source-columns":
                    assert not {"capture_id", "source_url", "source_path"} & set(sample["columns"])
                else:
                    assert {"capture_id", "source_url", "source_path"} <= set(sample["columns"])
    finally:
        server.close()
        connection.close()


@pytest.mark.parametrize("changed", [False, True])
def test_public_readback_reuses_exact_local_members_and_refuses_changed_bytes(tmp_path, monkeypatch, changed):
    readback, args, prepared, authenticated = carried_public_readback(tmp_path, monkeypatch)
    retained = Path(prepared["generation_directory"])
    args.local_inputs = [retained]
    if changed:
        path = retained / "scorecards.parquet"
        path.write_bytes(path.read_bytes() + b"changed")
    requested = []

    def unavailable(url, member, target, **kwargs):
        requested.append(member.key)
        return False

    monkeypatch.setattr(pub, "fetch_member", unavailable)
    if changed:
        with pytest.raises(ValueError, match="unavailable or differs from its immutable pin"):
            readback.public_files(args, prepared, authenticated)
        assert requested == ["scorecards.parquet"]
    else:
        connection, family = readback.public_files(args, prepared, authenticated)
        connection.close()
        assert not requested
        assert all((args.output / "members" / key).is_symlink() for key in family["tables"])
        assert {member["origin"] for member in json.loads((args.output / "public-files.json").read_bytes())["members"]} == {"retained_verified"}


@pytest.mark.parametrize("scopes", [None, [], ["lcv:2025", "lcv:2025"],
                                   pytest.param([f"scope:{index}" for index in range(501)], id="too-many")])
def test_source_adoption_requires_explicit_accepted_scopes(scopes):
    from spicy_regs.scorecards.operations.readback import adoption_scopes

    with pytest.raises(ValueError, match="exact distinct accepted scopes"):
        adoption_scopes({"accepted_scopes": scopes})
