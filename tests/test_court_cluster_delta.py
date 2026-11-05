"""A bounded additive search precedes all full-prior processing."""

from datetime import date
from contextlib import nullcontext
import hashlib
import importlib
import json

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import LocalMemberSource, canonical_json_bytes, iter_member_descriptors
from spicy_docs.sources.courtlistener.search import OPINION_STATUSES, CourtListenerTraversalBudget, search_url

from spicy_regs.court_receipts import build_court_generation, local_receipt_selection, write_court_rows
from spicy_regs.generations import verify_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import courtlistener, publication

delta = importlib.import_module("spicy_regs.transforms.catch_up_court_opinion_clusters")


def digest(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def selected(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICY_REGS_DISK_FLOOR_GIB", "0")
    monkeypatch.setattr(courtlistener, "MIN_REQUEST_INTERVAL_SECONDS", 0)
    witness = {
        "source_id": "original",
        "source_uri": None,
        "sha256": "sha256:" + "a" * 64,
        "locator": None,
        "body_version": None,
    }
    rows = [
        {
            "cluster_id": "2",
            "case_name": "prior literal \\u001f",
            "attorneys": [None, "", "one"],
            "date_created": "2026-06-30T08:00:00Z",
            "ingest_source": "bulk",
        },
        {"cluster_id": "1", "case_name": "older", "date_filed": "2025-01-01"},
        {"cluster_id": "3", "citation_count": "unsupported"},
    ]
    subject = write_court_rows(
        delta.DATASET, rows, tmp_path / "source", witnesses=[witness], generation_id="original-generation"
    )
    prior = tmp_path / "prior"
    artifact = build_court_generation(prior, family=delta.FAMILY, files=[subject])
    members = iter_member_descriptors(artifact, LocalMemberSource(prior))
    snapshot = publication.empty_index()
    snapshot["families"][delta.FAMILY] = publication.generation_entry(artifact, members)
    work = tmp_path / "work"
    evidence = CaptureEvidence(work, delta.FAMILY)
    evidence.inherit_pinned_root(snapshot, canonical_json_bytes(artifact.root))
    edition = tmp_path / "edition.json"
    edition.write_text(json.dumps({"source_edition": "2026-06-30", "prior": artifact.pin.artifact_digest}))
    courts = tmp_path / "courts.csv"
    courts.write_text("id,jurisdiction\nca9,F\n")
    # Only the already maintained parser is replaced here; this fixture's tiny
    # courts bytes remain pinned, and the caller must select their edition.
    monkeypatch.setattr(delta, "court_jurisdictions", lambda **kwargs: {"ca9": "F"})
    kwargs = dict(
        prior_directory=prior,
        expected_prior=artifact.pin.artifact_digest,
        source_edition=date(2026, 6, 30),
        source_edition_evidence=edition,
        source_edition_sha256=digest(edition),
        courts_local_file=courts,
        courts_sha256=digest(courts),
        search_through=6,
        search_budget=CourtListenerTraversalBudget(2, 0),
        evidence=evidence,
    )
    return work, kwargs


def transport(monkeypatch, *pages):
    queued = iter(pages)
    calls = []

    def handle(request):
        calls.append(request)
        response = next(queued)
        return httpx.Response(
            response if isinstance(response, int) else 200,
            json=response if isinstance(response, dict) else {"detail": "refused"},
        )

    # SpicyDocs 0.66.6 refuses keyless live search; the catch-up requires the token before any work.
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "fixture-token")
    monkeypatch.setattr(
        delta,
        "CourtListenerOpinionSearchReader",
        lambda **kwargs: courtlistener.CourtListenerOpinionSearchReader(
            transport=httpx.MockTransport(handle), api_token="fixture-token", **kwargs
        ),
    )
    return calls


def page(ids, *, total=None, next_url=None):
    return {
        "count": len(ids) if total is None else total,
        "next": next_url,
        "previous": None,
        "results": [{"cluster_id": identity, "court_id": "ca9", "caseName": "new", "attorney": []} for identity in ids],
    }


def test_disjoint_append_preserves_prior_values_receipts_and_ordinary_admission(selected, monkeypatch):
    work, kwargs = selected
    prior = kwargs["prior_directory"]
    original = {name: (prior / name).read_bytes() for name in (delta.OUTPUT, "etl_receipts.parquet")}
    old_rows = pq.read_table(prior / delta.OUTPUT).to_pylist()
    old_receipts = pq.read_table(prior / "etl_receipts.parquet").to_pylist()
    calls = transport(monkeypatch, page([6, 4]))  # Search order is filing date, not cluster ID.
    monkeypatch.setattr(
        "spicy_regs.court_receipts.restore_processing_input",
        lambda *args, **kwargs: pytest.fail("Native append must not restore the prior"),
    )
    result = delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert pq.read_table(result).to_pylist()[: len(old_rows)] == old_rows
    assert pq.read_table(result)["cluster_id"].to_pylist() == ["2", "1", "6", "4"]
    receipts, _ = local_receipt_selection(result)
    assert pq.read_table(receipts).to_pylist()[: len(old_receipts)] == old_receipts
    assert all((prior / name).read_bytes() == value for name, value in original.items())
    assert calls[0].url.params["q"] == "cluster_id:[3 TO 6]"
    assert not (work / delta.OUTPUT).exists()  # No public local selection before admission.
    generation = work / "generation"
    build_court_generation(
        generation,
        family=delta.FAMILY,
        files=[result],
        read_snapshot=kwargs["evidence"].read_snapshot,
        inputs=kwargs["evidence"].inputs(),
    )
    assert verify_generation(generation).root["spec"]["tables"][delta.OUTPUT]["rows"] == 4


@pytest.mark.parametrize("failure", ["http", "budget", "range"])
def test_incomplete_search_refuses_before_conversion_or_prior_copy(selected, monkeypatch, failure):
    work, kwargs = selected
    kwargs["search_budget"] = CourtListenerTraversalBudget(1, 0)
    next_url = search_url(kind="o", q="cluster_id:[3 TO 6]", opinion_statuses=OPINION_STATUSES) + "&cursor=next"  # a continuation keeps its selection
    calls = transport(
        monkeypatch,
        429 if failure == "http" else page([7]) if failure == "range" else page([4], total=2, next_url=next_url),
    )
    for name in ("finish_court_output", "_append_native"):
        monkeypatch.setattr(delta, name, lambda *args, **kwargs: pytest.fail("Source must complete first"))
    with pytest.raises((ValueError, httpx.HTTPError)):
        delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert len(calls) == 1
    assert not list(work.glob(".clusters-delta-*.parquet"))
    events = [json.loads(line) for line in (kwargs["evidence"].artifact_dir / "journal.jsonl").read_text().splitlines()]
    assert any(event["event"] == "refusal" for event in events)


@pytest.mark.parametrize("pin", ["expected_prior", "source_edition_sha256", "courts_sha256"])
def test_wrong_input_pins_refuse_before_search(selected, monkeypatch, pin):
    work, kwargs = selected
    kwargs[pin] = "sha256:" + "0" * 64
    calls = transport(monkeypatch, page([4]))
    with pytest.raises(ValueError, match="differs|differ"):
        delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert calls == []


def test_overlap_is_refused_without_replacing_prior(selected, tmp_path):
    _, kwargs = selected
    prior = kwargs["prior_directory"] / delta.OUTPUT
    original = prior.read_bytes()
    fresh = tmp_path / "overlap.parquet"
    pq.write_table(pq.read_table(prior).slice(0, 1), fresh)
    with pytest.raises(ValueError, match="overlaps"):
        delta._append_native(prior, fresh, tmp_path / "combined.parquet", guard=lambda: None)
    assert prior.read_bytes() == original


@pytest.mark.parametrize("member", ["subject", "receipts", "edition"])
def test_input_mutation_during_search_refuses_before_conversion(selected, monkeypatch, member):
    work, kwargs = selected
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "fixture-token")
    target = {
        "subject": kwargs["prior_directory"] / delta.OUTPUT,
        "receipts": kwargs["prior_directory"] / "etl_receipts.parquet",
        "edition": kwargs["source_edition_evidence"],
    }[member]

    def changed(self):
        yield {"cluster_id": 4}
        target.write_bytes(target.read_bytes() + b"changed")

    monkeypatch.setattr(delta.CourtListenerOpinionSearchReader, "iter_records", changed)
    monkeypatch.setattr(
        delta, "finish_court_output", lambda *args, **kwargs: pytest.fail("Changed input must refuse before conversion")
    )
    with pytest.raises(ValueError, match="changed"):
        delta.catch_up_court_opinion_clusters(work, **kwargs)
    assert not list(work.glob(".clusters-delta-*.parquet"))


def test_catch_up_requires_token_before_reading_its_prior(selected, monkeypatch):
    """SpicyDocs 0.66.6 refuses keyless live search; the weekly catch-up says so before any work."""
    work, kwargs = selected
    monkeypatch.delenv("COURTLISTENER_API_TOKEN", raising=False)
    monkeypatch.setattr(delta.publication, "table_owner", lambda *_: pytest.fail("prior read before the token check"))
    with pytest.raises(courtlistener.CourtListenerError, match="requires COURTLISTENER_API_TOKEN"):
        delta.catch_up_court_opinion_clusters(work, **kwargs)


def test_bulk_build_requires_token_before_reading_unless_search_is_skipped(tmp_path, monkeypatch):
    build = importlib.import_module("spicy_regs.transforms.build_court_opinion_clusters")
    monkeypatch.delenv("COURTLISTENER_API_TOKEN", raising=False)
    monkeypatch.setattr(build.r2, "download", lambda *_: pytest.fail("prior read before the token check"))
    with pytest.raises(courtlistener.CourtListenerError, match="requires COURTLISTENER_API_TOKEN"):
        build.build_court_opinion_clusters(tmp_path)


@pytest.fixture
def local_cli(selected, tmp_path, monkeypatch):
    """Exercise the real console app and generation admission; replace only remote inputs."""
    from spicy_regs.pipelines.rollups.court_opinion_clusters import app

    work, kwargs = selected
    snapshot = kwargs["evidence"].read_snapshot
    artifact = verify_generation(kwargs["prior_directory"])
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.invalid")
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "fixture-token")
    monkeypatch.setattr(publication, "snapshot", lambda _: nullcontext(snapshot))
    monkeypatch.setattr(publication, "load_family_root", lambda *args: (canonical_json_bytes(artifact.root), artifact.root))
    monkeypatch.setattr("dotenv.load_dotenv", lambda: None)
    retained = tmp_path / "retained-search"
    options = {key: value for key, value in kwargs.items() if key not in {"search_budget", "evidence"}}
    options.update(output_dir=work, retained_search_directory=retained, max_requests=2, max_wait_seconds=0)

    def run(**changes):
        arguments = ["catch-up"]
        for key, value in (options | changes).items():
            arguments.extend(["--" + key.replace("_", "-"), str(value)])
        try:
            app(arguments)
        except SystemExit as error:
            assert error.code == 0

    return run, retained, work, kwargs


@pytest.mark.parametrize("option", ["output_dir", "retained_search_directory"])
@pytest.mark.parametrize("nested", [False, True], ids=["prior", "inside-prior"])
def test_catch_up_preserves_prior_when_writable_directory_aliases_it(local_cli, monkeypatch, tmp_path, option, nested):
    run, _, _, options = local_cli
    prior = options["prior_directory"]
    alias = tmp_path / "prior-alias"
    alias.symlink_to(prior, target_is_directory=True)
    calls = transport(monkeypatch, 503)
    with pytest.raises(ValueError, match="outside the selected prior"):
        run(**{option: alias / "new-child" if nested else alias})
    assert not calls
    assert verify_generation(prior).pin.artifact_digest == options["expected_prior"]


@pytest.mark.parametrize("child", ["source-evidence", ".builds", "generation-inputs/court-opinion-clusters"])
def test_catch_up_refuses_when_an_output_child_points_to_the_prior(local_cli, monkeypatch, child):
    run, _, work, kwargs = local_cli
    prior = kwargs["prior_directory"]
    output = work / "aliased-output"
    target = output / child
    target.parent.mkdir(parents=True, exist_ok=True)
    target.symlink_to(prior, target_is_directory=True)
    calls = transport(monkeypatch, page([4]))

    with pytest.raises(ValueError, match="output must be outside the selected prior"):
        run(output_dir=output)

    assert not calls
    assert verify_generation(prior).pin.artifact_digest == kwargs["expected_prior"]


@pytest.mark.parametrize("lost", ["checkpoint", "selection", "both", "page-record"])
def test_catch_up_refuses_when_a_retained_control_is_lost(local_cli, monkeypatch, lost):
    run, retained, work, kwargs = local_cli
    next_url = search_url(kind="o", q="cluster_id:[3 TO 6]", opinion_statuses=OPINION_STATUSES) + "&cursor=next"
    transport(monkeypatch, page([4], total=2, next_url=next_url), 429)
    with pytest.raises(ValueError, match="incomplete"):
        run()
    checkpoint = retained / "checkpoint.json"
    if lost == "page-record":
        checkpoint.write_bytes(checkpoint.read_bytes().splitlines(keepends=True)[0])
    if lost in {"checkpoint", "both"}:
        checkpoint.unlink()
    if lost in {"selection", "both"}:
        (retained / "selection.json").unlink()
    remaining = {path: path.read_bytes() for path in retained.rglob("*") if path.is_file()}
    calls = transport(monkeypatch, page([4]))

    with pytest.raises(ValueError, match="controls are missing|captures outside its checkpoint"):
        run()

    assert not calls
    assert {path: path.read_bytes() for path in retained.rglob("*") if path.is_file()} == remaining
    assert verify_generation(kwargs["prior_directory"]).pin.artifact_digest == kwargs["expected_prior"]
    assert not (work / "generations").exists()


def test_catch_up_can_resume_an_initialization_interrupted_before_the_journal(local_cli, monkeypatch):
    from spicy_docs.transport import resume

    run, retained, _, _ = local_cli
    calls = transport(monkeypatch, page([4]))

    def interrupted(*args, **kwargs):
        raise OSError("interrupted before writing the checkpoint header")

    with monkeypatch.context() as patch:
        patch.setattr(resume, "TraversalCheckpoint", interrupted)
        with pytest.raises(OSError, match="interrupted before writing"):
            run()
    assert (retained / "selection.json").is_file()
    assert not (retained / "checkpoint.json").exists()
    assert not calls

    run()

    assert len(calls) == 1


def test_normal_cli_retains_interruption_and_resumes_only_missing_page(local_cli, monkeypatch, tmp_path):
    from types import SimpleNamespace
    from spicy_docs.transport import capture

    run, retained, work, kwargs = local_cli
    clock, sleeps = [0.0], []

    def sleep(delay):
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr(capture, "time", SimpleNamespace(monotonic=lambda: clock[0], sleep=sleep))
    monkeypatch.setattr(courtlistener, "MIN_REQUEST_INTERVAL_SECONDS", 12)
    first_url = search_url(kind="o", q="cluster_id:[3 TO 6]", opinion_statuses=OPINION_STATUSES)
    next_url = first_url + "&cursor=next"
    original_subject = (kwargs["prior_directory"] / delta.OUTPUT).read_bytes()
    original_receipts = pq.read_table(kwargs["prior_directory"] / "etl_receipts.parquet").to_pylist()
    calls = transport(monkeypatch, page([4], total=2, next_url=next_url), 429)
    with pytest.raises(ValueError, match="incomplete"):
        run(max_wait_seconds=30)
    assert len(calls) == 2
    assert sleeps == [12]
    assert not (work / "generations").exists()
    records = [json.loads(line) for line in (retained / "checkpoint.json").read_text().splitlines()]
    first = next(record["page"] for record in records if "page" in record)
    assert first["next_url"] == next_url
    assert (retained / "captures" / "sha256" / first["sha256"][7:]).is_file()
    assert not any(record.get("complete") for record in records)

    calls = transport(monkeypatch, page([6], total=2))
    run(max_wait_seconds=30)
    assert [str(request.url) for request in calls] == [next_url]
    assert sleeps == [12]  # Retained prefix is replayed without live pacing.
    records = [json.loads(line) for line in (retained / "checkpoint.json").read_text().splitlines()]
    assert next(record["page"] for record in records if "page" in record) == first
    assert any(record.get("complete") for record in records)
    (generation,) = (work / "generations").iterdir()
    rows = pq.read_table(generation / delta.OUTPUT).to_pylist()
    assert [row["cluster_id"] for row in rows] == ["2", "1", "4", "6"]
    assert pq.read_table(generation / "etl_receipts.parquet").to_pylist()[:len(original_receipts)] == original_receipts
    assert (kwargs["prior_directory"] / delta.OUTPUT).read_bytes() == original_subject
    replay = [json.loads(line) for path in (work / "source-evidence").glob("*/artifact/journal.jsonl")
              for line in path.read_text().splitlines() if 'courtlistener-replayed-response' in line]
    assert len(replay) == 1 and replay[0]["observed_at"] == first["observed_at"]

    # An independent clean invocation produces exactly the same native values.
    calls = transport(monkeypatch, page([4], total=2, next_url=next_url), page([6], total=2))
    clean = tmp_path / "clean"
    run(output_dir=clean, retained_search_directory=tmp_path / "clean-search", max_wait_seconds=30)
    (clean_generation,) = (clean / "generations").iterdir()
    assert len(calls) == 2
    assert sleeps == [12, 12]
    assert pq.read_table(clean_generation / delta.OUTPUT).to_pylist() == rows


@pytest.mark.parametrize("change", ["parent", "edition", "query", "status", "missing", "corrupt"])
def test_normal_cli_changed_selection_or_capture_refuses_before_live_or_append(local_cli, monkeypatch, change):
    run, retained, work, kwargs = local_cli
    prior_bytes = {name: (kwargs["prior_directory"] / name).read_bytes()
                   for name in (delta.OUTPUT, "etl_receipts.parquet")}
    next_url = search_url(kind="o", q="cluster_id:[3 TO 6]", opinion_statuses=OPINION_STATUSES) + "&cursor=next"
    transport(monkeypatch, page([4], total=2, next_url=next_url), 429)
    with pytest.raises(ValueError, match="incomplete"):
        run()
    changes = {}
    if change == "parent":
        changes["expected_prior"] = "sha256:" + "0" * 64
    elif change == "edition":
        changes["source_edition"] = "2026-09-30"
    elif change == "query":
        changes["search_through"] = 7
    elif change == "status":
        monkeypatch.setattr("spicy_docs.sources.courtlistener.search.OPINION_STATUSES", ("Published",))
    else:
        (body,) = (retained / "captures" / "sha256").iterdir()
        if change == "missing":
            body.unlink()
        else:
            body.write_bytes(b"changed")
    calls = transport(monkeypatch, page([6], total=2))
    monkeypatch.setattr(delta, "_append_native", lambda *a, **k: pytest.fail("Refused source cannot copy prior"))
    monkeypatch.setattr(delta, "finish_court_output", lambda *a, **k: pytest.fail("Refused source cannot convert delta"))
    with pytest.raises((ValueError, FileNotFoundError)):
        run(**changes)
    assert calls == [] and not (work / "generations").exists()
    assert all((kwargs["prior_directory"] / name).read_bytes() == value for name, value in prior_bytes.items())


def test_normal_cli_keeps_saved_retry_after_before_live_dispatch(local_cli, monkeypatch):
    from datetime import UTC, datetime, timedelta
    from spicy_regs.sources import courtlistener_resume

    run, retained, work, _ = local_cli
    clock = [datetime(2026, 10, 19, 12, tzinfo=UTC)]
    monkeypatch.setattr(courtlistener_resume, "_now", lambda: clock[0])
    calls = []
    responses = iter([
        httpx.Response(429, headers={"Retry-After": "60"}, json={"detail": "throttled"}),
        httpx.Response(200, json=page([4])),
    ])

    def handle(request):
        calls.append(request)
        clock[0] += timedelta(seconds=7)  # Response latency must not shorten Retry-After.
        return next(responses)

    monkeypatch.setattr(delta, "CourtListenerOpinionSearchReader", lambda **kwargs:
                        courtlistener.CourtListenerOpinionSearchReader(
                            transport=httpx.MockTransport(handle), api_token="fixture-token", **kwargs))
    with pytest.raises(ValueError, match="incomplete"):
        run()
    saved = (retained / "not-before.json").read_bytes()
    assert json.loads(saved)["not_before"] == "2026-10-19T12:01:07+00:00"
    clock[0] = datetime(2026, 10, 19, 12, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="deadline has not elapsed"):
        run()
    assert len(calls) == 1 and not (work / "generations").exists()
    assert (retained / "not-before.json").read_bytes() == saved
    clock[0] += timedelta(seconds=7)
    run()
    assert len(calls) == 2


def test_normal_cli_does_not_checkpoint_invalid_page_or_share_live_checkpoint(local_cli, monkeypatch):
    import fcntl

    run, retained, work, _ = local_cli
    calls = transport(monkeypatch, page([4, 4]))
    with pytest.raises(ValueError, match="repeated identity"):
        run()
    assert len(calls) == 1
    records = [json.loads(line) for line in (retained / "checkpoint.json").read_text().splitlines()]
    assert not any("page" in record for record in records)
    calls = transport(monkeypatch, page([4]))
    with (retained / "run.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="already in use"):
            run()
    assert not calls and not (work / "generations").exists()


@pytest.mark.parametrize("fault", ["duplicate", "range", "count", "foreign-cursor"])
def test_normal_cli_bad_resumed_membership_preserves_good_prefix(local_cli, monkeypatch, fault):
    run, retained, work, kwargs = local_cli
    next_url = search_url(kind="o", q="cluster_id:[3 TO 6]", opinion_statuses=OPINION_STATUSES) + "&cursor=next"
    original = {name: (kwargs["prior_directory"] / name).read_bytes()
                for name in (delta.OUTPUT, "etl_receipts.parquet")}
    transport(monkeypatch, page([4], total=2, next_url=next_url), 429)
    with pytest.raises(ValueError, match="incomplete"):
        run()
    checkpoint = retained / "checkpoint.json"
    before = [record["page"] for line in checkpoint.read_text().splitlines()
              if "page" in (record := json.loads(line))]
    invalid = {
        "duplicate": page([4], total=2), "range": page([7], total=2), "count": page([6], total=3),
        "foreign-cursor": page([6], total=2, next_url="https://foreign.invalid/search/"),
    }[fault]
    calls = transport(monkeypatch, invalid)
    monkeypatch.setattr(delta, "finish_court_output", lambda *a, **k: pytest.fail("Incomplete source cannot convert"))
    monkeypatch.setattr(delta, "_append_native", lambda *a, **k: pytest.fail("Incomplete source cannot append"))
    with pytest.raises(ValueError):
        run()
    after = [record["page"] for line in checkpoint.read_text().splitlines()
             if "page" in (record := json.loads(line))]
    assert after == before
    assert [str(request.url) for request in calls] == [next_url]
    assert all((kwargs["prior_directory"] / name).read_bytes() == value for name, value in original.items())
    assert not (work / "generations").exists()


def test_normal_cli_restarts_in_a_fresh_process_after_keyboard_interrupt(local_cli, tmp_path):
    """Only source/publication responses are fixtures; checkpoint and admission are real."""
    import os
    import subprocess
    import sys

    _, retained, work, kwargs = local_cli
    options = {key: str(value) for key, value in kwargs.items() if key not in {"search_budget", "evidence"}}
    options.update(output_dir=str(work), retained_search_directory=str(retained), max_requests="2", max_wait_seconds="0")
    first_url = search_url(kind="o", q="cluster_id:[3 TO 6]", opinion_statuses=OPINION_STATUSES)
    next_url = first_url + "&cursor=next"
    fixture = tmp_path / "process-input.json"
    fixture.write_text(json.dumps({"options": options, "snapshot": kwargs["evidence"].read_snapshot,
                                  "first_url": first_url, "next_url": next_url,
                                  "first": page([4], total=2, next_url=next_url), "last": page([6], total=2)}))
    program = '''
from contextlib import nullcontext
import importlib, json, sys
from pathlib import Path
import httpx
from rulespec_artifacts import canonical_json_bytes
from spicy_regs.generations import verify_generation
from spicy_regs.sources import courtlistener, publication
from spicy_regs.pipelines.rollups.court_opinion_clusters import app
import dotenv
fixture, phase, log = sys.argv[1:]
config = json.loads(Path(fixture).read_text())
delta = importlib.import_module("spicy_regs.transforms.catch_up_court_opinion_clusters")
snapshot = config["snapshot"]
artifact = verify_generation(Path(config["options"]["prior_directory"]))
publication.snapshot = lambda _: nullcontext(snapshot)
publication.load_family_root = lambda *args: (canonical_json_bytes(artifact.root), artifact.root)
dotenv.load_dotenv = lambda: None
delta.court_jurisdictions = lambda **kwargs: {"ca9": "F"}
courtlistener.MIN_REQUEST_INTERVAL_SECONDS = 0
def handle(request):
    with Path(log).open("a") as output:
        output.write(str(request.url) + "\\n")
    if phase == "interrupt":
        if str(request.url) == config["next_url"]:
            raise KeyboardInterrupt()
        assert str(request.url) == config["first_url"]
        return httpx.Response(200, json=config["first"])
    assert str(request.url) == config["next_url"], "successful page requested again"
    return httpx.Response(200, json=config["last"])
original = courtlistener.CourtListenerOpinionSearchReader
delta.CourtListenerOpinionSearchReader = lambda **kwargs: original(transport=httpx.MockTransport(handle), **kwargs)
arguments = ["catch-up"]
for key, value in config["options"].items():
    arguments.extend(["--" + key.replace("_", "-"), value])
app(arguments)
'''
    env = {**os.environ, "COURTLISTENER_API_TOKEN": "fixture-token", "R2_PUBLIC_URL": "https://fixture.invalid"}
    prior_bytes = {name: (kwargs["prior_directory"] / name).read_bytes()
                   for name in (delta.OUTPUT, "etl_receipts.parquet")}
    log = tmp_path / "requests.jsonl"
    interrupted = subprocess.run([sys.executable, "-B", "-c", program, str(fixture), "interrupt", str(log)],
                                 env=env, capture_output=True, text=True)
    assert interrupted.returncode == 130  # Cyclopts turns KeyboardInterrupt into this process exit.
    assert not (work / "generations").exists()
    prefix = [json.loads(line)["page"] for line in (retained / "checkpoint.json").read_text().splitlines()
              if '"page"' in line]
    assert len(prefix) == 1
    resumed = subprocess.run([sys.executable, "-B", "-c", program, str(fixture), "resume", str(log)],
                             env=env, capture_output=True, text=True)
    assert resumed.returncode == 0, resumed.stderr
    assert log.read_text().splitlines() == [first_url, next_url, next_url]
    (generation,) = (work / "generations").iterdir()
    assert pq.read_table(generation / delta.OUTPUT)["cluster_id"].to_pylist() == ["2", "1", "4", "6"]
    assert all((kwargs["prior_directory"] / name).read_bytes() == value for name, value in prior_bytes.items())
    records = [json.loads(line) for line in (retained / "checkpoint.json").read_text().splitlines()]
    assert next(record["page"] for record in records if "page" in record) == prefix[0]
    assert any(record.get("complete") for record in records)
