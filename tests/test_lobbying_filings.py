"""Hermetic tests for the Senate LDA lobbying-filings ingest (no network).

The request walk itself is spicy-docs' ``LdaFilingsReader``; its retries,
credential headers, page-shape refusals, count checks and traversal bounds are
pinned by spicy-docs' own ``test_lda_and_courtlistener_search`` and
``test_paged_json``. What remains here is what this repository owns on top of
it: the raw-filing → published-schema mapping (``_shape``, including the nested
activity/government-entity projections), the filtered-request semantics
(bounded date window, cold-start upper bound, ``max_records``) and that a
failed or malformed read aborts the rollup before anything is written or
published.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from importlib import import_module
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.sources.lda import API
from spicy_docs.transport import retry as spicy_retry

from spicy_regs.transforms.build_lobbying_filings import (
    COLUMNS,
    LobbyingFilingsError,
    _bounded_until,
    _shape,
)

# ``transforms/__init__`` shadows the module name with the public function, so
# import the module itself for attribute access (its ``MAX_REQUESTS_PER_PAGE``).
bld = import_module("spicy_regs.transforms.build_lobbying_filings")

_RAW_FILING = {
    "filing_uuid": "7866327b-c892-4430-b9f0-1f0f679c58c6",
    "filing_type": "Q1",
    "filing_year": 2024,
    "filing_period": "first_quarter",
    "dt_posted": "2024-01-02T10:13:41-05:00",
    "income": "30000.00",
    "expenses": None,
    "filing_document_url": "https://lda.senate.gov/filings/public/filing/7866327b/print/",
    "registrant": {"id": 35707, "name": "SMITH GARSON"},
    "client": {"id": 58116, "client_id": 58116, "name": "E-COM 9-1-1 DISPATCH CENTER"},
    "lobbying_activities": [
        {
            "general_issue_code": "TEC",
            "general_issue_code_display": "Telecommunications",
            "description": "Emergency dispatch technology funding.",
            "government_entities": [{"id": 2, "name": "HOUSE OF REPRESENTATIVES"}],
        },
        {
            "general_issue_code": "BUD",
            "general_issue_code_display": "Budget/Appropriations",
            "description": "Appropriations.",
            # Same chamber lobbied again — must remain a repeated occurrence.
            "government_entities": [
                {"id": 2, "name": "HOUSE OF REPRESENTATIVES"},
                {"id": 1, "name": "SENATE"},
            ],
        },
    ],
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_FILING)
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_FILING)
    assert row["filing_uuid"] == "7866327b-c892-4430-b9f0-1f0f679c58c6"
    assert row["filing_type"] == "Q1"
    # Integer scalars stringify (schema is all-VARCHAR).
    assert row["filing_year"] == "2024"
    assert row["registrant_id"] == "35707"
    assert row["client_id"] == "58116"
    assert row["registrant_name"] == "SMITH GARSON"
    assert row["client_name"] == "E-COM 9-1-1 DISPATCH CENTER"
    assert row["income"] == "30000.00"
    assert row["expenses"] is None
    assert row["url"] == "https://lda.senate.gov/filings/public/filing/7866327b/print/"
    # Activities project to issue codes + descriptions.
    acts = json.loads(row["lobbying_activities_json"])
    assert [a["general_issue_code"] for a in acts] == ["TEC", "BUD"]
    assert acts[0]["general_issue_code_display"] == "Telecommunications"
    # One entity per filing; activity rows retain each source occurrence.
    ents = json.loads(row["government_entities_json"])
    assert {e["name"] for e in ents} == {"HOUSE OF REPRESENTATIVES", "SENATE"}
    assert [e["id"] for e in ents] == [2, 1]
    activities = bld._activity_rows(_RAW_FILING)
    assert [[e["id"] for e in json.loads(a["government_entities_json"])] for a in activities] == [[2], [2, 1]]


def test_filing_entity_identity_uses_id_then_name_and_retains_null():
    entries = [{"id": 2, "name": "First"}, {"id": 2, "name": "Later"},
               {"name": "Unnamed ID"}, {"name": "Unnamed ID"}, None, None]
    filing = {"lobbying_activities": [{"government_entities": entries}]}
    assert bld._government_entities(filing) == [
        {"id": 2, "name": "First"}, {"id": None, "name": "Unnamed ID"}, None,
    ]
    assert len(json.loads(bld._activity_rows(filing)[0]["government_entities_json"])) == len(entries)


def test_shape_handles_missing_nested():
    row = _shape({"filing_uuid": "x"})
    assert row["registrant_name"] is None
    assert row["client_id"] is None
    assert row["lobbying_activities_json"] == "null"
    assert row["government_entities_json"] == "null"


def _page(uuids: list[str], next_url: str | None, *, count: int | None = None) -> dict:
    return {
        "count": len(uuids) if count is None else count,
        "next": next_url,
        "previous": None,
        "results": [{"filing_uuid": u} for u in uuids],
    }


def _json_response(payload, *, status: int = 200) -> httpx.Response:
    """A streamed JSON response — the owner's capture reads bodies via ``iter_raw``."""
    return httpx.Response(
        status,
        stream=httpx.ByteStream(json.dumps(payload).encode()),
        headers={"content-type": "application/json"},
    )


def _mock_http(monkeypatch, handler):
    """Route every httpx client (spicy-docs' capture builds one per reader) through ``handler``."""
    original = httpx.Client
    calls = []

    def record(request):
        calls.append(request)
        return handler(request, len(calls))

    def make_client(**kwargs):
        # spicy-docs' capture always passes its own ``transport`` (None here);
        # drop it so the mock transport is the one the requests flow through.
        kwargs.pop("transport", None)
        return original(transport=httpx.MockTransport(record), **kwargs)

    monkeypatch.setattr(httpx, "Client", make_client)
    return calls


def _no_retry_delay(monkeypatch):
    """Deterministic retries: no jitter, sleeps recorded. Returns the sleep log."""
    sleeps = []
    monkeypatch.setattr(spicy_retry.random, "uniform", lambda *_: 0.0)
    monkeypatch.setattr(spicy_retry.time, "sleep", sleeps.append)
    # Page pacing waits on the same clock; these tests are about retries, so run unpaced.
    monkeypatch.setattr(bld, "KEYLESS_INTERVAL_SECONDS", 0.0)
    return sleeps


def _no_prior(monkeypatch):
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "download", lambda *args: False)


def test_lagging_window_is_capped_to_thirty_days():
    assert _bounded_until(
        date(2026, 4, 1),
        None,
        today=date(2026, 7, 20),
    ) == date(2026, 5, 1)
    assert _bounded_until(
        date(2026, 7, 1),
        None,
        today=date(2026, 7, 20),
    ) == date(2026, 7, 20)


def test_cold_start_uses_today_filter_without_archive_year_floor(tmp_path, monkeypatch):
    _no_prior(monkeypatch)

    def respond(request, number):
        assert request.url.params["filing_dt_posted_before"] == date.today().isoformat()
        assert "filing_year" not in request.url.params
        assert "filing_dt_posted_after" not in request.url.params
        assert request.url.params["ordering"] == "dt_posted"
        return _json_response(_page([], None))

    calls = _mock_http(monkeypatch, respond)
    bld.build_lobbying_filings(tmp_path)
    assert len(calls) == 1


def test_fetch_sends_bounded_date_window(tmp_path, monkeypatch):
    _no_prior(monkeypatch)

    def respond(request, number):
        assert request.url.params["filing_dt_posted_after"] == "2026-04-01"
        assert request.url.params["filing_dt_posted_before"] == "2026-05-01"
        assert request.url.params["ordering"] == "dt_posted"
        return _json_response(_page([], None))

    _mock_http(monkeypatch, respond)
    bld.build_lobbying_filings(tmp_path, since=date(2026, 4, 1), until=date(2026, 5, 1))


def test_a_filing_year_reads_the_whole_year_whatever_the_prior_watermark(tmp_path, monkeypatch):
    """The history backfill: the stored posted-date watermark must not narrow a year to its last days."""
    from spicy_regs.transforms.build_lobbying_filings import _SCHEMA

    _no_prior(monkeypatch)
    pq.write_table(pa.Table.from_pylist([_shape(_RAW_FILING)], schema=_SCHEMA), tmp_path / "_lda_prior.parquet")

    def respond(request, number):
        assert request.url.params["filing_year"] == "2010"
        assert "filing_dt_posted_after" not in request.url.params
        assert "filing_dt_posted_before" not in request.url.params
        return _json_response(_page([], None))

    calls = _mock_http(monkeypatch, respond)
    bld.build_lobbying_filings(tmp_path, filing_year=2010)
    assert len(calls) == 1


def test_max_records_stops_the_walk_mid_page(tmp_path, monkeypatch):
    """An explicit record cap is a prefix of the walk: the next page is never requested."""
    _no_prior(monkeypatch)
    calls = _mock_http(
        monkeypatch,
        lambda request, number: _json_response(
            _page(["a", "b", "c", "d", "e"], f"{API}/filings/?page=2", count=100)
        ),
    )
    out, *_ = bld.build_lobbying_filings(tmp_path, max_records=3)
    assert len(calls) == 1
    assert [row["filing_uuid"] for row in pq.read_table(out).to_pylist()] == ["a", "b", "c"]


def _tied_walks(walks: list[tuple[list[str], list[str]]]):
    """Each pass of a two-page walk tied on dt_posted, declaring 4: ``(page 1, page 2)`` uuids per pass."""

    def handler(request, number):
        walk = walks[min((number - 1) // 2, len(walks) - 1)]
        if "page=2" in str(request.url):
            return _json_response(_page(walk[1], None, count=4))
        return _json_response(_page(walk[0], f"{API}/filings/?page=2", count=4))

    return handler


def test_a_walk_that_keeps_repeating_one_filing_and_skipping_another_refuses(tmp_path, monkeypatch):
    """Every pass serves the declared 4 records, but "b" twice and "c" never (the retained 1999 walk's shape)."""
    from spicy_docs.reading.paged_json import DEFAULT_POOL_PASSES, IncompleteWalkError

    _no_prior(monkeypatch)
    monkeypatch.setattr(bld, "KEYLESS_INTERVAL_SECONDS", 0.0)
    calls = _mock_http(monkeypatch, _tied_walks([(["a", "b"], ["b", "d"])]))
    with pytest.raises(IncompleteWalkError, match="pooled 3 of 4 declared records after 4 passes"):
        bld.build_lobbying_filings(tmp_path)
    assert len(calls) == 2 * DEFAULT_POOL_PASSES
    assert not list(tmp_path.rglob("lobbying_filings.parquet"))


def test_walks_that_skip_different_filings_pool_to_the_declared_total(tmp_path, monkeypatch):
    """lda.gov reorders a tie per request (ss-hqoi probe): a second walk serves the filing the first skipped."""
    _no_prior(monkeypatch)
    monkeypatch.setattr(bld, "KEYLESS_INTERVAL_SECONDS", 0.0)
    calls = _mock_http(monkeypatch, _tied_walks([(["a", "b"], ["b", "d"]), (["c", "a"], ["d", "a"])]))
    filings, *_ = bld.build_lobbying_filings(tmp_path)
    assert sorted(pq.read_table(filings, columns=["filing_uuid"]).column(0).to_pylist()) == ["a", "b", "c", "d"]
    assert len(calls) == 4


def test_the_pass_budget_bounds_the_pooled_walks(tmp_path, monkeypatch):
    """``LDA_POOL_PASSES`` reaches the walk: a tie that settles only on its fifth walk needs a budget above four."""
    from spicy_docs.reading.paged_json import IncompleteWalkError

    from spicy_regs.pipelines.rollups import lobbying_filings as rollup

    _no_prior(monkeypatch)
    monkeypatch.setattr(bld, "KEYLESS_INTERVAL_SECONDS", 0.0)
    short = (["a", "b"], ["b", "d"])
    _mock_http(monkeypatch, _tied_walks([short, short, short, short, (["c", "a"], ["d", "a"])]))
    with pytest.raises(IncompleteWalkError, match="after 3 passes"):
        bld.build_lobbying_filings(tmp_path, max_passes=3)
    with pytest.raises(ValueError):  # zero is refused, not read as the default
        bld.build_lobbying_filings(tmp_path, max_passes=0)
    seen = {}
    monkeypatch.setattr(rollup, "build_lobbying_filings", lambda output_dir, **kwargs: seen.update(kwargs) or ())
    monkeypatch.setenv("LDA_POOL_PASSES", "8")
    rollup.LobbyingFilingsRollup(output_dir=tmp_path).build(tmp_path)
    assert seen["max_passes"] == 8


@pytest.mark.parametrize(
    ("failure_page", "failure", "have_prior"),
    [(1, 400, False), (1, 400, True), (2, 400, False), (2, 400, True), (2, 503, True), (2, "transport", True)],
)
def test_reader_failure_aborts_actual_rollup_before_output_or_publication(
    tmp_path, monkeypatch, failure_page, failure, have_prior
):
    from spicy_regs.pipelines.rollups.lobbying_filings import LobbyingFilingsRollup
    from spicy_regs.sources import publication, r2
    from spicy_regs.transforms.build_lobbying_filings import OUTPUT, _SCHEMA

    prior = tmp_path / OUTPUT
    if have_prior:
        pq.write_table(pa.Table.from_pylist([_shape(_RAW_FILING)], schema=_SCHEMA), prior)
    original = prior.read_bytes() if have_prior else None

    def download(key, target):
        assert key == OUTPUT
        if original is None:
            return False
        target.write_bytes(original)
        return True

    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.example")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "synthetic-test-key")
    monkeypatch.setattr(r2, "download", download)
    monkeypatch.setattr(publication, "load_index", lambda base: publication.empty_index())
    monkeypatch.setattr(
        publication, "publish_generation", lambda *args, **kwargs: pytest.fail("failed read was published")
    )
    # Three requests per page: the initial attempt plus two retries.
    monkeypatch.setattr(bld, "MAX_REQUESTS_PER_PAGE", 3)
    sleeps = _no_retry_delay(monkeypatch)

    def respond(request, number):
        if failure_page == 2 and number == 1:
            return _json_response({"count": 2, "next": f"{API}/filings/?page=2", "results": [_RAW_FILING]})
        if failure == "transport":
            raise httpx.ReadTimeout("interrupted", request=request)
        return _json_response({"detail": "failure"}, status=failure)

    calls = _mock_http(monkeypatch, respond)
    retried = failure in {503, "transport"}
    expected_error = httpx.HTTPError if failure == 503 else ConnectionError if retried else PagedJsonSourceError
    with pytest.raises(expected_error):
        LobbyingFilingsRollup(output_dir=tmp_path, skip_upload=False).run()
    assert len(calls) == failure_page + (2 if retried else 0)
    assert sleeps == ([0.0, 0.0] if retried else [])
    assert not (tmp_path / "generations").exists()
    assert not list((tmp_path / ".builds").rglob(OUTPUT))
    assert not list(tmp_path.rglob("_*_new.parquet"))
    if have_prior:
        assert prior.read_bytes() == original
        assert pq.read_table(prior).to_pylist()[0]["filing_uuid"] == _RAW_FILING["filing_uuid"]
        assert next((tmp_path / ".builds").rglob("_lda_prior.parquet")).read_bytes() == original
    else:
        assert not prior.exists()


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({"detail": "not a result page"}, PagedJsonSourceError),
        ({"count": 0, "results": [], "next": 2}, PagedJsonSourceError),
        ({"count": -1, "results": [], "next": None}, PagedJsonSourceError),
        ({"count": 1, "results": [{}], "next": None}, LobbyingFilingsError),
        ({"count": 1, "results": [{"filing_uuid": ""}], "next": None}, LobbyingFilingsError),
        ({"count": 12, "results": [], "next": None}, PagedJsonSourceError),
    ],
)
def test_malformed_success_cannot_become_empty_or_partial_output(tmp_path, monkeypatch, payload, error):
    _no_prior(monkeypatch)
    _mock_http(monkeypatch, lambda request, number: _json_response(payload))
    with pytest.raises(error):
        bld.build_lobbying_filings(tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


_FILING_WITH_ACTIVITIES = {
    "filing_uuid": "f-1",
    "lobbying_activities": [
        {
            "general_issue_code": "SCI",
            "general_issue_code_display": "Science/Technology",
            "description": "Quantum computing policy, including S. 3597.",
            "foreign_entity_issues": "Bluefors OY has interest in this issue area.",
            "lobbyists": [
                {"lobbyist": {"id": 67914, "first_name": "GARY", "last_name": "GALLANT"},
                 "covered_position": None, "new": False},
                {"lobbyist": {"id": 70001, "first_name": "ANA", "last_name": "RUIZ"},
                 "covered_position": "Legislative Assistant, Sen. X", "new": True},
            ],
            "government_entities": [{"id": 2, "name": "HOUSE OF REPRESENTATIVES"}, {"id": 1, "name": "SENATE"}],
        },
        {"general_issue_code": "TRD", "description": "Export controls.", "lobbyists": [], "government_entities": []},
    ],
}


def test_each_activity_and_each_named_lobbyist_is_a_row_at_its_position(tmp_path, monkeypatch):
    """Decision 47: the lobbyists the filings table dropped become rows, keyed by position in the filing."""
    _no_prior(monkeypatch)
    _mock_http(monkeypatch, lambda request, number: _json_response(
        {"count": 1, "next": None, "previous": None, "results": [_FILING_WITH_ACTIVITIES]}))
    filings, activities, lobbyists, *_ = bld.build_lobbying_filings(tmp_path, since=date(2026, 9, 1), until=date(2026, 9, 2))

    assert [r["filing_uuid"] for r in pq.read_table(filings).to_pylist()] == ["f-1"]
    acts = pq.read_table(activities).to_pylist()
    assert [(a["activity_index"], a["general_issue_code"]) for a in acts] == [(0, "SCI"), (1, "TRD")]
    assert acts[0]["government_entities"] == [{"id": "2", "name": "HOUSE OF REPRESENTATIVES"},
                                                               {"id": "1", "name": "SENATE"}]
    people = pq.read_table(lobbyists).to_pylist()
    assert [(p["activity_index"], p["lobbyist_index"], p["lobbyist_id"], p["last_name"], p["new"]) for p in people] == [
        (0, 0, "67914", "GALLANT", False), (0, 1, "70001", "RUIZ", True)]
    assert people[1]["covered_position"] == "Legislative Assistant, Sen. X"


def test_the_rollup_declares_its_two_new_tables_as_an_explicit_migration():
    from spicy_regs.pipelines.rollups.lobbying_filings import LobbyingFilingsRollup

    assert set(LobbyingFilingsRollup.outputs) - set(LobbyingFilingsRollup.added_tables) == {"lobbying_filings.parquet"}


def _remote_native_family(tmp_path, monkeypatch):
    import hashlib
    from contextlib import contextmanager
    from types import SimpleNamespace
    from spicy_regs.sources import publication

    first, night = tmp_path / 'first', tmp_path / 'night'
    first.mkdir()
    night.mkdir()
    _no_prior(monkeypatch)
    _mock_http(monkeypatch, lambda request, number: _json_response(
        {'count': 1, 'next': None, 'results': [_FILING_WITH_ACTIVITIES]}))
    subjects = bld.build_lobbying_filings(first, receipt_generation_id='g0',
                                         since=date(2026, 9, 1), until=date(2026, 9, 2))
    members = {path.name: path for path in (*subjects, first / 'etl_receipts.parquet')}
    bodies = {name: path.read_bytes() for name, path in members.items()}
    descriptors = {name: {'sha256': 'sha256:' + hashlib.sha256(bodies[name]).hexdigest(),
                         'byteSize': len(bodies[name]), 'rows': pq.read_metadata(path).num_rows}
                   for name, path in members.items()}
    index = publication.empty_index()
    index['families']['lobbying-filings'] = {
        'prefix': 'generations/lobbying-filings/' + 'a' * 64,
        'tables': {path.name: descriptors[path.name] for path in subjects},
        'etlReceipts': {'key': 'etl_receipts.parquet', **descriptors['etl_receipts.parquet'],
                        'generationId': 'g0', 'datasets': [path.stem for path in subjects]},
    }
    calls = []

    @contextmanager
    def stream(method, url, **kwargs):
        name = url.rsplit('/', 1)[1]
        calls.append(name)
        yield SimpleNamespace(status_code=200, iter_bytes=lambda: iter([bodies[name]]))

    monkeypatch.setenv('R2_PUBLIC_URL', 'https://fixture.invalid')
    monkeypatch.setattr(publication, 'current_index', lambda _: index)
    monkeypatch.setattr(publication.httpx, 'stream', stream)
    monkeypatch.setattr(bld.r2, 'download', lambda *args: pytest.fail('Native subject downloaded outside shared selection'))
    return subjects, night, index, calls


def test_remote_native_family_transfers_and_selects_receipts_once(tmp_path, monkeypatch):
    from spicy_regs import etl_receipts
    from spicy_regs.transforms import government_receipts as gr

    subjects, night, _, calls = _remote_native_family(tmp_path, monkeypatch)
    scans, admitted = [], []
    scan = etl_receipts._select_receipt_batches
    validate = gr.validate_receipt_bundle

    def counted(parquet, destinations):
        scans.append(tuple(destinations))
        return scan(parquet, destinations)

    def admission(subjects, *args, **kwargs):
        admitted.extend(subjects)
        return validate(subjects, *args, **kwargs)

    monkeypatch.setattr(etl_receipts, '_select_receipt_batches', counted)
    monkeypatch.setattr(gr, 'validate_receipt_bundle', admission)
    monkeypatch.setattr(gr, 'select_receipts', lambda *args, **kwargs: pytest.fail('Repeated family scan'))
    result = bld.build_lobbying_filings(night, receipt_generation_id='g1',
                                      since=date(2026, 9, 1), until=date(2026, 9, 2))
    names = [path.stem for path in subjects]
    assert calls.count('etl_receipts.parquet') == 1
    assert sorted(calls) == sorted([path.name for path in subjects] + ['etl_receipts.parquet'])
    assert scans == [tuple(names)]
    assert admitted == names
    assert all(pq.read_table(old).equals(pq.read_table(new)) for old, new in zip(subjects, result, strict=True))
    validate({path.stem: [path] for path in result}, [night / 'etl_receipts.parquet'],
             [gr.POLICIES[name] for name in names], generation_id='g1')


@pytest.mark.parametrize('failure', ['wrong_pin', 'during_scan', 'after_admission'])
def test_remote_native_family_refuses_wrong_pin_or_mutation(tmp_path, monkeypatch, failure):
    from spicy_regs import etl_receipts
    from spicy_regs.transforms import government_receipts as gr

    _, night, index, _ = _remote_native_family(tmp_path, monkeypatch)

    def mutate():
        receipt, = night.glob('.selected-priors-*/.members/**/etl_receipts.parquet')
        replacement = receipt.with_suffix('.replacement')
        replacement.write_bytes(receipt.read_bytes())
        replacement.replace(receipt)

    if failure == 'wrong_pin':
        index['families']['lobbying-filings']['etlReceipts']['sha256'] = 'sha256:' + '0' * 64
    elif failure == 'during_scan':
        scan = etl_receipts._select_receipt_batches

        def changed(parquet, destinations):
            scan(parquet, destinations)
            mutate()

        monkeypatch.setattr(etl_receipts, '_select_receipt_batches', changed)
    else:
        validate = gr.validate_receipt_bundle

        def changed(*args, **kwargs):
            validate(*args, **kwargs)
            mutate()

        monkeypatch.setattr(gr, 'validate_receipt_bundle', changed)
    with pytest.raises((ValueError, RuntimeError), match='differs from its pin|changed during'):
        bld.build_lobbying_filings(night, receipt_generation_id='g1',
                                  since=date(2026, 9, 1), until=date(2026, 9, 2))
    assert not (night / gr.BUILD_METADATA).exists()


@pytest.mark.parametrize('member', ['family_receipts', 'scoped_receipts'])
def test_remote_native_family_migration_mutation_cannot_complete_or_be_adopted(tmp_path, monkeypatch, member):
    from spicy_regs.transforms import government_receipts as gr

    subjects, night, index, calls = _remote_native_family(tmp_path, monkeypatch)
    merge = gr.native_merge
    producer_bytes = {}

    def changed(policy, *args, **kwargs):
        result = merge(policy, *args, **kwargs)
        if policy.dataset == 'lobbying_activity_lobbyists':
            producer_bytes.update({path.name: (night / path.name).read_bytes() for path in subjects})
            if member == 'scoped_receipts':
                receipt = kwargs['prior'][1]
            else:
                receipt, = night.glob('.selected-priors-*/.members/**/etl_receipts.parquet')
            replacement = receipt.with_suffix('.replacement')
            replacement.write_bytes(receipt.read_bytes())
            replacement.replace(receipt)
        return result

    monkeypatch.setattr(gr, 'native_merge', changed)
    with pytest.raises(ValueError, match='changed during'):
        bld.build_lobbying_filings(night, receipt_generation_id='failed',
                                  since=date(2026, 9, 1), until=date(2026, 9, 2))
    assert producer_bytes
    assert not (night / gr.BUILD_METADATA).exists()
    assert not (night / 'etl_receipts.parquet').exists()
    assert {name: (night / name).read_bytes() for name in producer_bytes} == producer_bytes

    # The retry must select the publication again, not adopt the failed candidate.
    monkeypatch.setattr(gr, 'native_merge', merge)
    index['families']['lobbying-filings']['etlReceipts']['sha256'] = 'sha256:' + '0' * 64
    with pytest.raises(RuntimeError, match='differs from its pin'):
        bld.build_lobbying_filings(night, receipt_generation_id='retry',
                                  since=date(2026, 9, 1), until=date(2026, 9, 2))
    assert calls.count('etl_receipts.parquet') == 2
    assert not (night / gr.BUILD_METADATA).exists()


def test_native_nightly_receipts_only_the_window_and_equals_the_full_row_merge(tmp_path, monkeypatch):
    import copy
    import shutil
    import duckdb
    from spicy_regs import government_bulk
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, decode_exact_json, failure_receipt, validate_receipt_bundle
    from spicy_regs.transforms import government_receipts as gr
    from spicy_regs.transforms.table_merge import merge_local_prior

    _no_prior(monkeypatch)
    first_dir, night_dir = tmp_path / "first", tmp_path / "night"
    first_dir.mkdir()
    night_dir.mkdir()
    old_filings: list[dict] = []
    for key in ("a", "b"):
        filing = copy.deepcopy(_FILING_WITH_ACTIVITIES)
        filing.update(filing_uuid=key, dt_posted="2026-10-01T10:00:00Z")
        old_filings.append(filing)
    current_filings = old_filings
    _mock_http(monkeypatch, lambda request, number: _json_response(
        {"count": len(current_filings), "next": None, "previous": None, "results": current_filings}))
    first = bld.build_lobbying_filings(first_dir, receipt_generation_id="g0", since=date(2026, 10, 1), until=date(2026, 10, 2))
    before = {path.stem: pq.read_table(path) for path in first}
    # The actual cold output equals the maintained row writer using the same retained inputs and context.
    for path in first:
        retained = first_dir / ".government-inputs/g0" / path.name
        gr._write_by_row(retained, tmp_path / "row-subject.parquet", tmp_path / "row-receipt.parquet", path.stem,
                         generation_id="g0", digest=gr._digest(retained))
        assert pq.read_table(path).equals(pq.read_table(tmp_path / "row-subject.parquet"))
        actual = pq.read_table(first_dir / "etl_receipts.parquet", filters=[("dataset", "=", path.stem)])
        assert actual.equals(pq.read_table(tmp_path / "row-receipt.parquet"))

    prior_receipts = pq.read_table(first_dir / "etl_receipts.parquet").to_pylist()
    context = ReceiptContext("g0", "qualified-refusal", "p", [{"source_id": "held", "sha256": "a" * 64}])
    failures = [failure_receipt(gr.POLICIES["lobbying_filings"], context, outcome="refused",
                               raw_fields={"raw_record": {"filing_uuid": key, "income": "invalid"}})
                for key in ("b", "unanswered")]
    prior_receipts.extend(failures)
    prior_path = first_dir / "etl_receipts.parquet"
    pq.write_table(pa.Table.from_pylist(prior_receipts, schema=RECEIPT_SCHEMA), prior_path)
    metadata = json.loads((first_dir / gr.BUILD_METADATA).read_text())
    metadata["receipt_sha256"] = gr._digest(prior_path)
    (night_dir / gr.BUILD_METADATA).write_text(json.dumps(metadata))

    def download(key, target):
        shutil.copyfile(first_dir / key, target)
        return True

    monkeypatch.setattr(bld.r2, "download", download)
    fresh_filings = [copy.deepcopy(old_filings[1]), copy.deepcopy(old_filings[0])]
    fresh_filings[0]["lobbying_activities"][0]["description"] = "Updated description"
    fresh_filings[1].update(filing_uuid="c", dt_posted="2026-10-02T11:00:00Z")
    current_filings = fresh_filings
    monkeypatch.setattr(government_bulk, "restore_legacy", lambda *a, **k: pytest.fail("native nightly restored its full prior"))
    written = {}
    original_write = government_bulk.write_dataset

    def record_write(source, *args, **kwargs):
        written[kwargs["dataset"]] = pq.ParquetFile(source).metadata.num_rows
        return original_write(source, *args, **kwargs)

    monkeypatch.setattr(government_bulk, "write_dataset", record_write)
    second = bld.build_lobbying_filings(night_dir, receipt_generation_id="g1", since=date(2026, 10, 1), until=date(2026, 10, 3))
    assert pq.read_table(second[0], columns=["filing_uuid"]).column(0).to_pylist() == ["c", "a", "b"]
    assert written == {"lobbying_filings": 2, "lobbying_activities": 4, "lobbying_activity_lobbyists": 4,
                       "lobbying_foreign_entities": 0, "lobbying_affiliated_organizations": 0,
                       "lobbying_conviction_disclosures": 0, "lobbying_registrants": 0, "lobbying_clients": 0}
    orders = {"lobbying_filings": "dt_posted DESC, filing_uuid", "lobbying_activities": "filing_uuid, CAST(activity_index AS INTEGER)",
              "lobbying_activity_lobbyists": "filing_uuid, CAST(activity_index AS INTEGER), CAST(lobbyist_index AS INTEGER)",
              "lobbying_foreign_entities": "filing_uuid, CAST(foreign_entity_index AS INTEGER)",
              "lobbying_affiliated_organizations": "filing_uuid, CAST(affiliated_organization_index AS INTEGER)",
              "lobbying_conviction_disclosures": "filing_uuid, CAST(conviction_disclosure_index AS INTEGER)",
              "lobbying_registrants": "CAST(registrant_id AS BIGINT)", "lobbying_clients": "CAST(client_id AS BIGINT)"}
    for path in second:
        dataset = path.stem
        prior = first_dir / ".government-inputs/g0" / path.name
        fetched = night_dir / ".government-inputs/g1" / path.name
        oracle = tmp_path / "full-merge.parquet"
        with duckdb.connect() as con:
            merge_local_prior(con, columns=gr._legacy_schema(dataset).names, identity=gr.POLICIES[dataset].identity_fields,
                              order_by=orders[dataset], prior_file=prior, new_file=fetched, out_file=oracle)
        gr._write_by_row(oracle, tmp_path / "expected.parquet", tmp_path / "expected-r.parquet", dataset,
                         generation_id="oracle", digest=gr._digest(oracle))
        assert pq.read_table(path).equals(pq.read_table(tmp_path / "expected.parquet"))
        assert pq.read_table(first_dir / path.name).equals(before[dataset])
    receipts = pq.read_table(night_dir / "etl_receipts.parquet").to_pylist()
    old_by_id = {row["record_id"]: row for row in prior_receipts if row["outcome"] == "accepted"}
    for row in receipts:
        if row["outcome"] == "accepted" and row["record_id"] in old_by_id:
            old = old_by_id[row["record_id"]]
            if row["subject_version"] == old["subject_version"]:
                assert row == old
            else:
                assert decode_exact_json(row["diagnostic_json"])["prior_receipt"]["receipt_id"] == old["receipt_id"]
    assert [row for row in receipts if row["outcome"] == "refused"] == [failures[1]]
    validate_receipt_bundle({path.stem: [path] for path in second}, [night_dir / "etl_receipts.parquet"],
                            [gr.POLICIES[path.stem] for path in second])
    counts = json.loads((night_dir / gr.BUILD_METADATA).read_text())["counts"]
    assert counts["lobbying_filings"] == {"inputs": 2, "subjects": 3, "refused": 0}

    # Reusing the build directory must select g1's published output, not the
    # staged g0 files that remain while the native merge consumes them.
    current_filings = []
    second_subjects = [pq.read_table(path) for path in second]
    third = bld.build_lobbying_filings(night_dir, receipt_generation_id="g2", since=date(2026, 10, 1), until=date(2026, 10, 3))
    assert written == dict.fromkeys(orders, 0)
    assert all(pq.read_table(path).equals(expected) for path, expected in zip(third, second_subjects, strict=True))
    third_receipts = pq.read_table(night_dir / "etl_receipts.parquet").to_pylist()
    assert [row for row in third_receipts if row["outcome"] == "accepted"] == [row for row in receipts if row["outcome"] == "accepted"]
    assert [row for row in third_receipts if row["outcome"] == "refused"] == [failures[1]]
    counts = json.loads((night_dir / gr.BUILD_METADATA).read_text())["counts"]
    assert counts["lobbying_filings"] == {"inputs": 0, "subjects": 3, "refused": 0}
    validate_receipt_bundle({path.stem: [path] for path in third}, [night_dir / "etl_receipts.parquet"],
                            [gr.POLICIES[path.stem] for path in third])

    # A failed raw-window build leaves the admitted staged prior available for
    # retry even though the public paths now hold its unfinished source rows.
    with monkeypatch.context() as interrupted:
        def fail_before_migration(*args, **kwargs):
            raise RuntimeError("interrupted before native migration")

        interrupted.setattr(gr, "migrate_outputs", fail_before_migration)
        with pytest.raises(RuntimeError, match="interrupted before native migration"):
            bld.build_lobbying_filings(night_dir, receipt_generation_id="unfinished", since=date(2026, 10, 1), until=date(2026, 10, 3))
    retried = bld.build_lobbying_filings(night_dir, receipt_generation_id="retried", since=date(2026, 10, 1), until=date(2026, 10, 3))
    assert all(pq.read_table(path).equals(expected) for path, expected in zip(retried, second_subjects, strict=True))


# --- government-sources/2: the filing's own fields, and the retained-page rebuild (ss-hqoi) ---------------------

#: Values as the retained pages spell them (visual review, work/lobbying-wf-20261030/visual_sample.json): an
#: upper-case method whose display is the bare letter, a termination date, no second address line, no flag.
_FILING_V2_FIELDS = {
    "filing_type_display": "Year-End Termination",
    "filing_period_display": "Year-End (July 1 - Dec 31)",
    "filing_document_content_type": "text/html",
    "expenses_method": "A",
    "expenses_method_display": "A",
    "termination_date": "2024-06-10",
    "posted_by_name": "Amy Luft",
    "registrant_address_1": "700 NE 2nd Street",
    "registrant_address_2": None,
    "registrant_city": "Meridian",
    "registrant_state": "ID",
    "registrant_zip": "83642",
    "registrant_country": "United States of America",
    "registrant_ppb_country": None,
    "registrant_different_address": None,
}


def test_shape_keeps_the_v2_fields_as_filed():
    row = _shape({**_RAW_FILING, **_FILING_V2_FIELDS, "registrant_address_2": "", "registrant_different_address": True})
    assert {key: row[key] for key in _FILING_V2_FIELDS} == {
        **_FILING_V2_FIELDS, "registrant_address_2": "", "registrant_different_address": "True"}
    assert _shape(_RAW_FILING)["termination_date"] is None


def test_earlier_policy_is_the_published_v1_descriptor():
    """A live government-sources/1 prior is still read under its own exact policy; nothing is relabeled."""
    from spicy_regs.transforms import government_receipts as gr

    (earlier,) = gr.EARLIER_POLICIES["lobbying_filings"]
    assert earlier.policy_version == "government-sources/1"
    assert gr.POLICIES["lobbying_filings"].policy_version == "government-sources/2"
    assert earlier.subject_schema.names == [
        name for name in gr.POLICIES["lobbying_filings"].subject_schema.names if name not in _FILING_V2_FIELDS]


def test_window_onto_an_earlier_policy_prior_refuses(tmp_path, monkeypatch):
    """A /2 window merged onto a /1 native prior would publish its rows with the new columns NULL: refuse instead."""
    from spicy_regs.transforms import government_receipts as gr
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError

    _no_prior(monkeypatch)
    filing = {**_FILING_WITH_ACTIVITIES, "dt_posted": "2026-10-01T10:00:00Z"}
    _mock_http(monkeypatch, lambda request, number: _json_response(
        {"count": 1, "next": None, "previous": None, "results": [filing]}))
    bld.build_lobbying_filings(tmp_path, receipt_generation_id="g0", since=date(2026, 10, 1), until=date(2026, 10, 2))
    # The prior on disk is read back as one written under the earlier policy, as the live /1 generation is.
    earlier = gr.EARLIER_POLICIES["lobbying_filings"][0]
    monkeypatch.setattr(gr, "_prior_policy", lambda dataset, path: earlier if dataset == "lobbying_filings"
                        else gr.POLICIES[dataset])
    with pytest.raises(GovernmentShapeError, match="government-sources/1.*rebuild the family"):
        bld.build_lobbying_filings(tmp_path, receipt_generation_id="g1", since=date(2026, 10, 1),
                                   until=date(2026, 10, 2))
    v1 = tmp_path / "v1.parquet"
    pq.write_table(earlier.subject_schema.empty_table(), v1)
    monkeypatch.undo()
    assert gr._prior_policy("lobbying_filings", v1) is earlier


class _Store:
    """The central raw archive's ``open`` over in-memory originals, keyed by digest."""

    def __init__(self, bodies: dict[str, bytes]):
        self.bodies = bodies

    def open(self, sha256):
        import io
        from contextlib import nullcontext

        return nullcontext(io.BytesIO(self.bodies[sha256]))


def _retained(tmp_path, captures: list[tuple[str, list[dict]]]):
    """Pages as retained bodies plus their pinned manifest: ``(captured_at, filings)`` per page."""
    import hashlib

    bodies, lines = {}, ["sha256\tbytes\tcaptured_at\tbasis"]
    for captured_at, results in captures:
        body = json.dumps({"count": len(results), "next": None, "previous": None, "results": results}).encode()
        digest = hashlib.sha256(body).hexdigest()
        bodies[digest] = body
        lines.append(f"{digest}\t{len(body)}\t{captured_at}\tjournal")
    manifest = tmp_path / "pages.tsv"
    manifest.write_text("\n".join(lines) + "\n")
    return manifest, hashlib.sha256(manifest.read_bytes()).hexdigest(), _Store(bodies)


def test_retained_rebuild_takes_each_filing_from_its_latest_capture(tmp_path, monkeypatch):
    """Two captures of one filing: the later wins, and its activities and lobbyists come with it, not the earlier's."""
    import copy

    from spicy_regs.sources import publication

    # Even beside a published family, a rebuild selects and stages no prior.
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.invalid")
    monkeypatch.setattr(publication, "current_index", lambda _: pytest.fail("A rebuild selects no prior"))
    monkeypatch.setattr(bld.r2, "download", lambda *args: pytest.fail("A rebuild stages no prior"))
    older: dict[str, Any] = {**copy.deepcopy(_FILING_WITH_ACTIVITIES), "dt_posted": "2026-10-01T10:00:00Z", **_FILING_V2_FIELDS,
             "registrant": {"id": 14143, "name": "FARM CREDIT BANK OF TEXAS", "contact_telephone": "+1 202-642-1416",
                            "dt_updated": "2026-07-30T15:51:39.285204-04:00"},
             "client": {"id": 119518, "client_id": 12, "name": "FARM CREDIT BANK OF TEXAS",
                        "client_self_select": True, "client_government_entity": None, "effective_date": "2009-02-01"}}
    newer = copy.deepcopy(older)
    newer["registrant"]["name"] = "FARM CREDIT BANK OF TEXAS, ACA"
    newer["client"]["client_self_select"] = False
    newer["lobbying_activities"] = newer["lobbying_activities"][:1]
    newer["lobbying_activities"][0]["lobbyists"] = newer["lobbying_activities"][0]["lobbyists"][:1]
    newer["posted_by_name"] = "Amy Luft, Counsel"
    # The later capture's lists win with the filing, as the activities do.
    older["foreign_entities"] = [{"name": "OLD HOLDINGS", "contribution": "1.00", "country": "KR"}]
    newer["foreign_entities"] = [{"name": "SK HOLDINGS", "contribution": "0.00", "ownership_percentage": "100.00",
                                  "country": "KR", "country_display": "South Korea"}, None]
    newer["affiliated_organizations"] = [{"name": "DCP MIDSTREAM, LLC", "url": None, "state": "CO", "zip": "80202"}]
    newer["conviction_disclosures"] = [{"lobbyist": {"id": 29268, "first_name": "C", "middle_name": "JAMES",
                                                     "last_name": "PATTI", "prefix_display": None},
                                        "date": "2002-05-07", "description": "Violation of 18 USC 1341"}]
    other = {**copy.deepcopy(older), "filing_uuid": "f-2", "lobbying_activities": [], "foreign_entities": [],
             "affiliated_organizations": None}
    manifest, digest, store = _retained(tmp_path, [
        ("2026-10-19T00:00:00+00:00", [newer]),
        ("2026-09-29T12:00:00-04:00", [older, other]),
    ])
    out = tmp_path / "build"
    out.mkdir()
    filings, activities, lobbyists, foreign, affiliated, convictions, registrants, clients = bld.rebuild_lobbying_filings(
        out, manifest=manifest, manifest_sha256=digest, store=store, receipt_generation_id="r0")
    rows = {row["filing_uuid"]: row for row in pq.read_table(filings).to_pylist()}
    assert set(rows) == {"f-1", "f-2"}
    assert rows["f-1"]["posted_by_name"] == "Amy Luft, Counsel"
    assert rows["f-1"]["termination_date"] == date(2024, 6, 10)
    assert rows["f-1"]["expenses_method_display"] == "A"
    assert rows["f-1"]["registrant_different_address"] is None
    assert [(a["filing_uuid"], a["activity_index"]) for a in pq.read_table(activities).to_pylist()] == [("f-1", 0)]
    assert [(p["filing_uuid"], p["lobbyist_index"]) for p in pq.read_table(lobbyists).to_pylist()] == [("f-1", 0)]
    # A null list entry keeps its position as an all-null row; an empty or null list has no rows. Whole rows are
    # compared, so a dropped column or a repeated entry fails here.
    def child(path, **values):
        return {column: None for column in pq.read_schema(path).names} | values

    assert pq.read_table(foreign).to_pylist() == [
        child(foreign, filing_uuid="f-1", foreign_entity_index=0, name="SK HOLDINGS", contribution=Decimal("0"),
              ownership_percentage=Decimal("100"), country="KR", country_display="South Korea"),
        child(foreign, filing_uuid="f-1", foreign_entity_index=1)]
    assert pq.read_table(affiliated).to_pylist() == [
        child(affiliated, filing_uuid="f-1", affiliated_organization_index=0, name="DCP MIDSTREAM, LLC", state="CO",
              zip="80202")]
    assert pq.read_table(convictions).to_pylist() == [
        child(convictions, filing_uuid="f-1", conviction_disclosure_index=0, lobbyist_id="29268", first_name="C",
              middle_name="JAMES", last_name="PATTI", date=date(2002, 5, 7), description="Violation of 18 USC 1341")]
    # The shapers emit one row per entry before the merge's identity dedup, so a repeated entry fails here too.
    shapers = {output: shape for output, _, shape in bld._TABLES}
    assert [(r["foreign_entity_index"], r["name"]) for r in shapers[bld.FOREIGN_ENTITIES_OUTPUT](newer)] == [
        ("0", "SK HOLDINGS"), ("1", None)]
    assert [(r["affiliated_organization_index"], r["name"])
            for r in shapers[bld.AFFILIATED_ORGANIZATIONS_OUTPUT](newer)] == [("0", "DCP MIDSTREAM, LLC")]
    assert [(r["conviction_disclosure_index"], r["last_name"])
            for r in shapers[bld.CONVICTION_DISCLOSURES_OUTPUT](newer)] == [("0", "PATTI")]
    # Every source field of an entry is a column (the 2026-10-31 key scan of all retained pages); the lists' keys:
    place = ("city", "state", "state_display", "country", "country_display",
             "ppb_city", "ppb_state", "ppb_state_display", "ppb_country", "ppb_country_display")
    assert pq.read_schema(foreign).names == [
        "filing_uuid", "foreign_entity_index", "name", "contribution", "ownership_percentage", "address", *place]
    assert pq.read_schema(affiliated).names == [
        "filing_uuid", "affiliated_organization_index", "name", "url", "address_1", "address_2", "address_3",
        "address_4", *place[:3], "zip", *place[3:]]
    assert pq.read_schema(convictions).names == [
        "filing_uuid", "conviction_disclosure_index", "lobbyist_id", "prefix", "first_name", "nickname", "middle_name",
        "last_name", "suffix", "date", "description"]
    # One profile per LDA id, the one captured last: the 10-19 page's, though f-2 (captured 09-29) names it too.
    (registrant,) = pq.read_table(registrants).to_pylist()
    assert (registrant["registrant_id"], registrant["name"], registrant["captured_filing_uuid"]) == (
        "14143", "FARM CREDIT BANK OF TEXAS, ACA", "f-1")
    assert str(registrant["captured_at"]) == "2026-10-19 00:00:00+00:00"
    # The LDA publishes the registrant's business telephone on every filing (owner round 9 Q2); the contact name stays out.
    assert registrant["contact_telephone"] == "+1 202-642-1416" and "contact_name" not in registrant
    (client,) = pq.read_table(clients).to_pylist()
    assert (client["client_id"], client["registrant_client_number"], client["registrant_id"]) == ("119518", "12", "14143")
    assert (client["client_self_select"], client["client_government_entity"]) == (False, None)
    assert client["effective_date"] == date(2009, 2, 1)
    receipts = pq.read_table(out / "etl_receipts.parquet").to_pylist()
    assert {r["policy_version"] for r in receipts if r["dataset"] == "lobbying_filings"} == {"government-sources/2"}
    assert not list(out.rglob(".selected-priors-*")) and not list(out.glob("_*.parquet"))


@pytest.mark.parametrize("fault", ["page", "manifest", "offset"])
def test_retained_rebuild_refuses_unpinned_bytes(tmp_path, monkeypatch, fault):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    captured = "2026-10-19T00:00:00" if fault == "offset" else "2026-10-19T00:00:00+00:00"
    manifest, digest, store = _retained(tmp_path, [(captured, [_FILING_WITH_ACTIVITIES])])
    if fault == "page":
        (sha,) = store.bodies
        store.bodies[sha] = store.bodies[sha].replace(b"f-1", b"f-9")
    if fault == "manifest":
        digest = "0" * 64
    out = tmp_path / "build"
    out.mkdir()
    with pytest.raises(LobbyingFilingsError, match="pinned|offset"):
        bld.rebuild_lobbying_filings(out, manifest=manifest, manifest_sha256=digest, store=store,
                                     receipt_generation_id="r0")
    assert not (out / "lobbying_filings.parquet").exists()


def test_the_rebuild_command_seals_evidence_and_admits_the_whole_family(tmp_path, monkeypatch):
    """``run-rollup-lobbying-rebuild`` end to end: pages from the central raw archive, no prior, no request.

    The tie between two pages captured at one time goes to the larger digest and is counted, not assumed absent.
    """
    import copy

    from loguru import logger as loguru_logger
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    from spicy_regs.generations import verify_generation
    from spicy_regs.pipelines.rollups import lobbying_filings as rollup

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    filing = {**copy.deepcopy(_FILING_WITH_ACTIVITIES), "dt_posted": "2026-10-01T10:00:00Z", **_FILING_V2_FIELDS,
              "registrant": {"id": 14143, "name": "FARM CREDIT BANK OF TEXAS"},
              "client": {"id": 119518, "client_id": 12, "name": "FARM CREDIT BANK OF TEXAS"}}
    renamed = {**copy.deepcopy(filing), "posted_by_name": "Amy Luft, Counsel"}
    manifest, digest, fixture = _retained(tmp_path, [("2026-09-23T00:00:00+00:00", [filing]),
                                                     ("2026-09-23T00:00:00+00:00", [renamed])])
    archive = CentralRawArchiveStore()  # the per-test SPICYGOV_RAW_ROOT (conftest)
    for body in fixture.bodies.values():
        archive.put_stream(iter([body]), max_bytes=len(body))
    monkeypatch.setenv("LDA_RETAINED_MANIFEST", str(manifest))
    monkeypatch.setenv("LDA_RETAINED_MANIFEST_SHA256", digest)
    monkeypatch.setattr(bld.r2, "download", lambda *args: pytest.fail("A rebuild stages no prior"))
    messages: list[str] = []
    sink = loguru_logger.add(lambda message: messages.append(str(message)), level="INFO")
    try:
        rollup.LobbyingRetainedRebuild(output_dir=tmp_path / "out", skip_upload=True).run()
    finally:
        loguru_logger.remove(sink)

    [generation] = (tmp_path / "out" / "generations").iterdir()
    spec = verify_generation(generation).root["spec"]
    assert spec["etlReceipts"]["generationId"]
    assert set(rollup.LobbyingRetainedRebuild.outputs) <= set(spec["tables"])
    winner = max(fixture.bodies, key=lambda sha: sha)
    expected = json.loads(fixture.bodies[winner])["results"][0]["posted_by_name"]
    (row,) = pq.read_table(tmp_path / "out" / "lobbying_filings.parquet").to_pylist()
    assert row["posted_by_name"] == expected
    assert any("1 repeat occurrences tied on capture time" in message for message in messages)
    evidence = [path for path in (tmp_path / "out").rglob("artifact.json")]
    assert evidence, "the rebuild seals its source evidence"
