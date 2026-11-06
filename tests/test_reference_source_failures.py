"""Source refusals preserve prior CRS, GAO, FCC and USAspending output bytes."""

import importlib
import json
from datetime import date
from decimal import Decimal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.reading.paged_json import DEFAULT_POOL_PASSES, IncompleteWalkError, PagedJsonSourceError
from spicy_docs.sources.gao.rss import GaoFeedSourceError
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_docs.transport import retry
from spicy_regs.sources import crs_reports as crs, gao_reports as gao
from spicy_regs.transforms import build_fcc_ecfs as fcc

usa = importlib.import_module("spicy_regs.transforms.build_usaspending_recipients")

KEY = "fixture-key-never-in-url"
DAY = date(2026, 9, 8)
CRS = {"id": "R1", "updateDate": "2026-09-08T00:00:00Z", "title": "One"}
FCC_FILING = {
    "id_submission": "f1",
    "date_received": "2026-09-08T23:56:03Z",
    "date_submission": "2026-09-08T23:55:00Z",
    "express_comment": 1,
}
FCC_PROCEEDING = {"name": "17-108", "id_proceeding": 1, "date_proceeding_created": "2026-09-08T10:00:00Z"}
RECIPIENT = {"id": "r1-R", "name": "ONE", "amount": 1.25}
FEED = b'<rss version="2.0"><channel><title>Reports</title><item><title>One</title><link>https://www.gao.gov/products/gao-26-107974</link><description>Native text</description></item></channel></rss>'
EMPTY_FEED = b'<rss version="2.0"><channel><title>Reports</title></channel></rss>'
REFUSALS = (ValueError, ConnectionError, httpx.HTTPError, CredentialRefusedError)


class Transport(httpx.MockTransport):
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []
        super().__init__(self.handle)

    def handle(self, request):
        self.calls.append(request)
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        status = value if isinstance(value, int) else 200
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        content_type = "application/rss+xml" if request.url.path.endswith(".xml") else "application/json"
        headers = {"content-type": content_type}
        if isinstance(value, dict) and "_fixture_total" in value:
            headers["total"] = str(value["_fixture_total"])
        return httpx.Response(status, stream=httpx.ByteStream(raw), headers=headers)


@pytest.fixture(autouse=True)
def bounded_requests(monkeypatch):
    monkeypatch.setattr(retry.random, "uniform", lambda *_: 0)
    for module in (crs, fcc, usa):
        monkeypatch.setattr(module, "_MAX_REQUESTS_PER_PAGE", 1)
    monkeypatch.setattr(gao, "_MAX_REQUESTS", 1)


def crs_page(records, *, total=None, next_url=None):
    return {"CRSReports": records, "pagination": {"count": len(records) if total is None else total, "next": next_url}}


def docketed(*names, **fields):
    """A filing as a docket-scoped selection serves it: ``proceedings`` names every docket it was filed in."""
    return {**FCC_FILING, **fields, "proceedings": [{"name": name} for name in names]}


def fcc_page(kind, records, *, counted=None):
    """An ECFS page with the aggregation ECFS answers beside every response; ``counted`` records carry its field."""
    key, field = ("filing", "express_comment") if kind == "fcc-filings" else ("proceeding", "bureau_name")
    if counted is None:
        counted = len(records) if kind == "fcc-filings" else 0
    buckets = [{"key": 1, "doc_count": counted}] if counted else []
    return {
        key: records,
        **({"_fixture_total": counted} if kind == "fcc-filings" else {}),
        "aggregations": {field: {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": buckets}},
    }


def usa_page(records, *, total=None, next_page=None):
    return {
        "results": records,
        "page_metadata": {
            "total": len(records) if total is None else total,
            "hasNext": next_page is not None,
            "next": next_page,
        },
    }


class FccFetch:
    """One FCC selection against the transform's adopted fetch; ``api_key`` stays mutable for the missing-key case.

    The fetch function is captured at construction so tests that monkeypatch
    ``build_fcc_ecfs._fetch_fcc`` still reach the real walk through this handle.
    """

    def __init__(self, *, endpoint: str, **kwargs):
        self.endpoint = endpoint
        self.kwargs = kwargs
        self._fetch = fcc._fetch_fcc

    @property
    def api_key(self) -> str | None:
        return self.kwargs.get("api_key")

    @api_key.setter
    def api_key(self, value: str | None) -> None:
        self.kwargs["api_key"] = value

    def iter_records(self):
        return self._fetch(self.endpoint, **self.kwargs)


def selected(kind, transport, **kwargs):
    if kind == "crs":
        return crs.CrsReportsReader(api_key=KEY, transport=transport, **kwargs)
    if kind == "gao":
        return gao.GaoReportsReader(transport=transport, **kwargs)
    if kind == "usa":
        return usa._iter_recipient_rows(transport=transport, **kwargs)
    endpoint = "filings" if kind == "fcc-filings" else "proceedings"
    return FccFetch(endpoint=endpoint, since=DAY, until=DAY, api_key=KEY, transport=transport, **kwargs)


def records(kind, transport, **kwargs):
    source = selected(kind, transport, **kwargs)
    return iter(source) if kind == "usa" else source.iter_records()


@pytest.mark.parametrize("kind", ["crs", "gao", "usa", "fcc-filings", "fcc-proceedings"])
@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
def test_http_refusal_is_not_empty_success(kind, status):
    transport = Transport(status)
    with pytest.raises(REFUSALS) as error:
        list(records(kind, transport))
    assert KEY not in str(error.value)
    assert len(transport.calls) == 1


@pytest.mark.parametrize("kind", ["crs", "gao", "usa", "fcc-filings", "fcc-proceedings"])
def test_transport_failure_is_not_empty_success(kind):
    with pytest.raises(REFUSALS):
        list(records(kind, Transport(httpx.ReadTimeout("fixture interruption"))))


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("crs", {}),
        ("crs", {"CRSReports": []}),
        ("crs", crs_page([{}])),
        ("crs", crs_page([CRS, CRS])),
        ("crs", crs_page([], total=1)),
        ("crs", crs_page([{"id": "R1", "title": "No update date"}])),
        ("usa", {}),
        ("usa", {"results": []}),
        ("usa", usa_page([{}])),
        ("usa", usa_page([RECIPIENT, RECIPIENT])),
        ("usa", usa_page([], total=1)),
        ("usa", {"results": [], "page_metadata": {"total": 0, "hasNext": True, "next": None}}),
        ("fcc-filings", fcc_page("fcc-filings", [FCC_FILING, FCC_FILING])),
        ("fcc-filings", {"filing": [FCC_FILING]}),
        ("fcc-filings", fcc_page("fcc-filings", [{**FCC_FILING, "id_submission": ""}])),
        ("fcc-filings", fcc_page("fcc-filings", [{**FCC_FILING, "express_comment": 1}], counted=2)),
        ("fcc-proceedings", {"proceeding": [FCC_PROCEEDING]}),
        ("gao", b"<html><body>Denied</body></html>"),
        ("gao", b"<rss>"),
        ("gao", b'<!DOCTYPE rss [<!ENTITY x "boom">]><rss version="2.0"><channel/></rss>'),
        ("gao", b'<rss version="2.0"><channel><item><title>No identity</title></item></channel></rss>'),
    ],
)
def test_malformed_or_incomplete_source_is_refused(kind, payload):
    # A walk short of its count is pooled over further passes before it refuses.
    with pytest.raises(REFUSALS):
        list(records(kind, Transport(*[payload] * DEFAULT_POOL_PASSES)))


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("crs", crs_page([])),
        ("usa", usa_page([])),
        ("gao", EMPTY_FEED),
        ("fcc-filings", fcc_page("fcc-filings", [])),
        ("fcc-proceedings", fcc_page("fcc-proceedings", [])),
    ],
)
def test_confirmed_empty_selection_is_valid(kind, payload):
    assert list(records(kind, Transport(payload))) == []


def test_crs_follows_continuation_and_never_stops_at_an_old_row():
    next_url = f"{crs.API_BASE}/crsreport?offset=2&limit=2&format=json"
    older = {**CRS, "id": "R-old", "updateDate": "2026-01-01T00:00:00Z"}
    newer = {**CRS, "id": "R-new"}
    transport = Transport(crs_page([older, CRS], total=3, next_url=next_url), crs_page([newer], total=3))
    rows = list(selected("crs", transport, since=DAY, per_page=2).iter_records())
    assert [x["id"] for x in rows] == ["R-old", "R1", "R-new"]
    assert transport.calls[0].url.params["fromDateTime"] == "2026-09-07T23:59:59Z"
    assert str(transport.calls[1].url) == next_url
    assert all(x.headers["x-api-key"] == KEY and "api_key" not in x.url.params for x in transport.calls)


def test_crs_page_bound_refuses_partial_walk(monkeypatch):
    monkeypatch.setattr(crs, "_MAX_PAGES", 1)
    next_url = f"{crs.API_BASE}/crsreport?offset=1&limit=1"
    with pytest.raises(PagedJsonSourceError, match="page bound"):
        list(selected("crs", Transport(crs_page([CRS], total=2, next_url=next_url)), per_page=1).iter_records())


@pytest.mark.parametrize("per_page,limit", [(None, 1_000), (19, 19), (6_000, 5_000)])
def test_fcc_filings_delegate_each_selection_to_the_source_traversal(monkeypatch, per_page, limit):
    from spicy_docs.sources.fcc_ecfs import FccEcfsReader

    calls = []

    def iter_filings(_reader, **kwargs):
        calls.append(kwargs)
        yield FCC_FILING

    monkeypatch.setattr(FccEcfsReader, "iter_filings", iter_filings)
    monkeypatch.setattr(
        fcc, "_fetch_proceedings_window", lambda *_args, **_kwargs: pytest.fail("filings used the proceedings walk")
    )
    transport = Transport()
    rows = list(
        fcc._fetch_fcc(
            "filings",
            since=DAY,
            until=date(2026, 9, 9),
            api_key=KEY,
            per_page=per_page,
            proceedings=("17-108", "23-320"),
            transport=transport,
        )
    )
    assert calls == [
        {"received_from": "2026-09-08", "received_to": "2026-09-09", "proceeding": name, "limit": limit}
        for name in ("17-108", "23-320")
    ]
    assert rows == [FCC_FILING, FCC_FILING] and all(row is not FCC_FILING for row in rows)
    assert transport.calls == []


def test_usa_short_nonterminal_page_continues_and_preserves_amount():
    transport = Transport(usa_page([RECIPIENT], total=2, next_page=2), usa_page([{**RECIPIENT, "id": "r2-R"}], total=2))
    rows = list(records("usa", transport, per_page=2))
    assert [x["id"] for x in rows] == ["r1-R", "r2-R"]
    assert rows[0]["amount"] == Decimal("1.25")
    assert json.loads(transport.calls[1].content)["page"] == 2


def test_usa_top_page_bound_is_an_explicit_selection():
    transport = Transport(usa_page([RECIPIENT], total=20, next_page=2))
    assert len(list(records("usa", transport, per_page=1, max_pages=1))) == 1
    assert len(transport.calls) == 1


@pytest.mark.parametrize(
    "second_page",
    [
        usa_page([{**RECIPIENT, "id": "r2-R"}], total=3),  # count changed
        usa_page([RECIPIENT], total=2),  # repeated identity across pages
        usa_page([], total=2),  # terminal count is short
        usa_page([], total=2, next_page=3),  # empty nonterminal page
        usa_page([{**RECIPIENT, "id": "r2-R"}], total=2, next_page=4),  # skipped page
    ],
)
def test_usa_refuses_cross_page_inconsistency(second_page):
    transport = Transport(usa_page([RECIPIENT], total=2, next_page=2), second_page)
    rows = records("usa", transport, per_page=1)
    assert next(rows)["id"] == RECIPIENT["id"]
    with pytest.raises(REFUSALS):
        list(rows)
    assert len(transport.calls) == 2


def test_fcc_named_proceedings_keep_each_selection_and_shared_filing():
    # A filing can legitimately appear in both selected proceedings. The fetch
    # preserves both observations; table merging owns identity deduplication.
    shared = docketed("17-108", "23-320")
    second = docketed("23-320", id_submission="f2")
    transport = Transport(fcc_page("fcc-filings", [shared]), fcc_page("fcc-filings", [shared, second]))
    reader = selected("fcc-filings", transport, proceedings=("17-108", "23-320"))
    assert list(reader.iter_records()) == [shared, shared, second]
    assert [r.url.params["proceedings.name"] for r in transport.calls] == ["17-108", "23-320"]


def test_fcc_second_proceeding_failure_preserves_prior_output(monkeypatch, tmp_path):
    module = importlib.import_module("spicy_regs.transforms.build_fcc_ecfs")
    filed = docketed("17-108")
    table = pa.Table.from_pylist([module._shape_filing(filed)], schema=module._FILING_SCHEMA)
    prior = tmp_path / "_fcc_filings_prior.parquet"
    output = tmp_path / "fcc_filings.parquet"
    pq.write_table(table, prior)
    pq.write_table(table, output)
    before = prior.read_bytes(), output.read_bytes()
    transport = Transport(fcc_page("fcc-filings", [filed]), 500)
    reader = selected("fcc-filings", transport, proceedings=("17-108", "23-320"))
    monkeypatch.setattr(module, "_fetch_fcc", lambda *args, **kwargs: reader.iter_records())

    with pytest.raises(REFUSALS):
        module.build_fcc_filings(tmp_path)

    assert (prior.read_bytes(), output.read_bytes()) == before
    assert [r.url.params["proceedings.name"] for r in transport.calls] == ["17-108", "23-320"]


def test_gao_validates_whole_feed_before_applying_selected_item_bound():
    assert list(selected("gao", Transport(FEED), max_records=1).iter_records()) == [
        {
            "product_id": "gao-26-107974",
            "title": "One",
            "link": "https://www.gao.gov/products/gao-26-107974",
            "description": "Native text",
            "pub_date": None,
        }
    ]
    bad = FEED.replace(b"</channel>", b"<item><title>Missing identity</title></item></channel>")
    with pytest.raises(GaoFeedSourceError):
        list(selected("gao", Transport(bad), max_records=1).iter_records())


CASES = {
    "crs": (
        "build_crs_reports",
        "build_crs_reports",
        "CrsReportsReader",
        "_crs_prior.parquet",
        "crs_reports.parquet",
        "_SCHEMA",
        "_shape",
        CRS,
    ),
    "gao": (
        "build_gao_reports",
        "build_gao_reports",
        "GaoReportsReader",
        "_gao_prior.parquet",
        "gao_reports.parquet",
        "_SCHEMA",
        "_shape",
        {"product_id": "gao-26-107974", "link": "https://www.gao.gov/products/gao-26-107974", "title": "Prior"},
    ),
    "usa": (
        "build_usaspending_recipients",
        "build_usaspending_recipients",
        "_iter_recipient_rows",
        "_usaspending_prior.parquet",
        "usaspending_recipients.parquet",
        "_SCHEMA",
        "_shape",
        RECIPIENT,
    ),
    "fcc-filings": (
        "build_fcc_ecfs",
        "build_fcc_filings",
        "_fetch_fcc",
        "_fcc_filings_prior.parquet",
        "fcc_filings.parquet",
        "_FILING_SCHEMA",
        "_shape_filing",
        FCC_FILING,
    ),
    "fcc-proceedings": (
        "build_fcc_ecfs",
        "build_fcc_proceedings",
        "_fetch_fcc",
        "_fcc_proceedings_prior.parquet",
        "fcc_proceedings.parquet",
        "_PROCEEDING_SCHEMA",
        "_shape_proceeding",
        FCC_PROCEEDING,
    ),
}


@pytest.mark.parametrize("kind", CASES)
@pytest.mark.parametrize("failure", ["http", "malformed", "partial", "missing-key"])
def test_failed_build_preserves_prior_and_existing_output_bytes(monkeypatch, tmp_path, kind, failure):
    if failure == "missing-key" and kind not in ("crs", "fcc-filings", "fcc-proceedings"):
        pytest.skip("source is keyless")
    module_name, builder_name, reader_name, prior_name, output_name, schema_name, shape_name, native = CASES[kind]
    module = importlib.import_module("spicy_regs.transforms." + module_name)
    table = pa.Table.from_pylist([getattr(module, shape_name)(native)], schema=getattr(module, schema_name))
    prior, output = tmp_path / prior_name, tmp_path / output_name
    pq.write_table(table, prior)
    pq.write_table(table, output)
    before = (prior.read_bytes(), output.read_bytes())
    if failure == "partial":
        if kind == "crs":
            source = selected(
                kind,
                Transport(crs_page([CRS], total=2, next_url=f"{crs.API_BASE}/crsreport?offset=1&limit=1"), 500),
                per_page=1,
            )
        elif kind == "usa":
            source = selected(kind, Transport(usa_page([RECIPIENT], total=2, next_page=2), 500), per_page=1)
        elif kind.startswith("fcc"):
            source = selected(
                kind,
                Transport(fcc_page(kind, [native], counted=2 if kind == "fcc-filings" else 0), 500),
                per_page=1,
            )
        else:
            source = selected(
                kind, Transport(FEED.replace(b"</channel>", b"<item><title>No identity</title></item></channel>"))
            )
    elif failure == "missing-key":
        source = selected(kind, Transport())
        source.api_key = ""
    else:
        source = selected(kind, Transport(500 if failure == "http" else b"not a source response"))
    if kind.startswith("fcc"):
        monkeypatch.setattr(module, reader_name, lambda *args, **kwargs: source.iter_records())
    else:
        monkeypatch.setattr(module, reader_name, lambda **_: source)
    with pytest.raises(REFUSALS):
        getattr(module, builder_name)(tmp_path)
    assert (prior.read_bytes(), output.read_bytes()) == before
    assert not list(tmp_path.glob("*_new.parquet"))
    if failure == "partial" and kind.startswith("fcc"):
        assert len(source.kwargs["transport"].calls) == 2


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("crs", crs_page([])),
        ("usa", usa_page([])),
        ("gao", EMPTY_FEED),
        ("fcc-filings", fcc_page("fcc-filings", [])),
    ],
)
@pytest.mark.parametrize("have_prior", [False, True])
def test_empty_success_keeps_prior_rows_or_builds_zero(monkeypatch, tmp_path, kind, payload, have_prior):
    module_name, builder_name, reader_name, prior_name, output_name, schema_name, shape_name, native = CASES[kind]
    module = importlib.import_module("spicy_regs.transforms." + module_name)
    expected = [getattr(module, shape_name)(native)] if have_prior else []
    if have_prior:
        pq.write_table(pa.Table.from_pylist(expected, schema=getattr(module, schema_name)), tmp_path / prior_name)
    source = selected(kind, Transport(payload))
    if kind.startswith("fcc"):
        monkeypatch.setattr(module, reader_name, lambda *args, **kwargs: source.iter_records())
    else:
        monkeypatch.setattr(module, reader_name, lambda **_: source)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    out = getattr(module, builder_name)(tmp_path)
    if isinstance(out, tuple):
        # gao-reports also writes gao_decisions, which an empty feed leaves empty.
        out, *others = out
        assert all(pq.read_table(other).num_rows == 0 for other in others)
    assert out.name == output_name
    result = pq.read_table(out)
    from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS, map_subject
    from spicy_regs.transforms.government_receipts import BUILD_METADATA
    generation = json.loads((tmp_path / BUILD_METADATA).read_text())["generation_id"]
    assert result.to_pylist() == [map_subject(out.stem, row, generation_id=generation) for row in expected]
    assert result.schema == SUBJECT_SCHEMAS[out.stem]


def test_fcc_proceedings_refuse_an_empty_whole_walk_and_keep_the_output(monkeypatch, tmp_path):
    output = tmp_path / fcc.PROCEEDINGS_OUTPUT
    output.write_bytes(b"previous generation")
    source = selected("fcc-proceedings", Transport(fcc_page("fcc-proceedings", [])))
    monkeypatch.setattr(fcc, "_fetch_fcc", lambda *args, **kwargs: source.iter_records())
    with pytest.raises(fcc.FccEcfsError, match="empty whole walk"):
        fcc.build_fcc_proceedings(tmp_path)
    assert output.read_bytes() == b"previous generation"


def test_crs_pools_a_shifted_walk_until_its_declared_count():
    """A pass that repeats one report and skips another still serves the declared rows; pooling catches it.

    Neither walk is clean, so the pool of both settles. CRS ignores ``sort``, so the second walk moves its page
    boundaries by page size alone.
    """
    second = {**CRS, "id": "R2"}
    transport = Transport(crs_page([CRS, CRS], total=2), crs_page([second, second], total=2))
    assert sorted(x["id"] for x in selected("crs", transport).iter_records()) == ["R1", "R2"]
    assert [call.url.params["limit"] for call in transport.calls] == ["250", "237"]


def test_fcc_pools_a_window_until_its_aggregate_count():
    """ECFS states no total; the aggregation it answers beside the rows counts the window.

    Neither walk is clean, so the pool of both settles; the second walk takes a smaller page, so its boundaries
    fall on other filings.
    """
    second = {**FCC_FILING, "id_submission": "f2", "express_comment": 0}
    first = {**FCC_FILING, "express_comment": 1}
    transport = Transport(
        fcc_page("fcc-filings", [first, first], counted=2), fcc_page("fcc-filings", [second, second], counted=2)
    )
    assert sorted(x["id_submission"] for x in selected("fcc-filings", transport).iter_records()) == ["f1", "f2"]
    assert [call.url.params["limit"] for call in transport.calls] == ["1000", "948"]


def test_fcc_walks_cycle_through_three_page_sizes():
    """Every walk after the first moves its page boundaries; the cycle repeats once all three sizes are spent."""
    dirty = fcc_page("fcc-filings", [FCC_FILING, FCC_FILING], counted=2)
    transport = Transport(*[dirty] * DEFAULT_POOL_PASSES)
    with pytest.raises(IncompleteWalkError):
        list(records("fcc-filings", transport))
    assert [call.url.params["limit"] for call in transport.calls] == ["1000", "948", "889", "1000"][
        :DEFAULT_POOL_PASSES
    ]


def test_fcc_filings_keep_filters_and_explicit_page_size_through_source_traversal():
    filings = [docketed("17-108"), docketed("17-108", id_submission="f2")]
    transport = Transport(fcc_page("fcc-filings", filings))
    rows = list(
        fcc._fetch_fcc(
            "filings",
            since=DAY,
            until=date(2026, 9, 9),
            api_key=KEY,
            per_page=19,
            proceedings=("17-108",),
            transport=transport,
        )
    )
    assert rows == filings and len(transport.calls) == 1
    request = transport.calls[0]
    assert request.url.params["date_received"] == "[gte]2026-09-08[lte]2026-09-10"
    assert request.url.params["proceedings.name"] == "17-108"
    assert request.url.params["limit"] == "19"
    assert request.url.params["sort"] == "date_submission,ASC"
    assert request.headers["x-api-key"] == KEY and KEY not in str(request.url)


def test_fcc_filings_missing_flag_uses_query_total_without_defaulting_false():
    missing_flag = {key: value for key, value in FCC_FILING.items() if key != "express_comment"}
    transport = Transport(fcc_page("fcc-filings", [missing_flag]))
    assert list(records("fcc-filings", transport)) == [missing_flag]
    assert len(transport.calls) == 1


def _proceeding(name, **fields):
    """An ECFS proceeding document carrying the bureau its window's count is aggregated over."""
    return {
        **FCC_PROCEEDING,
        "name": name,
        "bureau": {"code": "WCB", "name": "Wireline"},
        "total_filing_count": 1,
        **fields,
    }


def test_fcc_proceedings_split_full_spans_but_refuse_a_full_single_day(monkeypatch):
    monkeypatch.setattr(fcc, "MAX_RESULT_WINDOW", 2)
    first = _proceeding("17-108", id_proceeding=1)
    second = _proceeding("23-320", id_proceeding=2, date_proceeding_created="2026-09-09T10:00:00Z")
    transport = Transport(
        fcc_page("fcc-proceedings", [first, second], counted=2),
        fcc_page("fcc-proceedings", [first], counted=1),
        fcc_page("fcc-proceedings", [second], counted=1),
    )
    rows = list(
        fcc._fetch_fcc("proceedings", since=DAY, until=date(2026, 9, 9), api_key=KEY, per_page=2, transport=transport)
    )
    assert rows == [first, second]
    assert [call.url.params["date_proceeding_created"] for call in transport.calls] == [
        "[gte]2026-09-08[lte]2026-09-10",
        "[gte]2026-09-08[lte]2026-09-09",
        "[gte]2026-09-09[lte]2026-09-10",
    ]
    assert all(call.url.params["offset"] == "0" for call in transport.calls)

    same_day = [first, {**second, "date_proceeding_created": "2026-09-08T11:00:00Z"}]
    full_day = Transport(fcc_page("fcc-proceedings", same_day, counted=2))
    with pytest.raises(fcc.FccEcfsError, match="proceedings reaches the result ceiling on 2026-09-08"):
        list(fcc._fetch_fcc("proceedings", since=DAY, until=DAY, api_key=KEY, per_page=2, transport=full_day))
    assert len(full_day.calls) == 1


@pytest.mark.parametrize("window", [20, 34])
def test_fcc_proceedings_split_a_window_a_smaller_walk_could_not_exhaust(monkeypatch, window):
    """A proceedings window the first walk exhausts but a later, smaller walk would stop short of is split at once.

    Otherwise a second walk would meet the result ceiling and refuse as if the window had grown. ``per_page=19``
    cycles 19, 18 and 17 rows a page, and a window is within reach only below ``min(window // size * size)``: 17 at
    20 and 18 at 34. The last size alone (``window // 17 * 17``) gives 34 at 34, where 18 rows would be pooled
    instead, so that case pins the reach to the smallest multiple of any walk size.
    """
    monkeypatch.setattr(fcc, "MAX_RESULT_WINDOW", window)
    created = ["2026-09-08T10:00:00Z"] * 9 + ["2026-09-09T10:00:00Z"] * 9
    documents = [
        _proceeding(f"26-{number}", id_proceeding=number, date_proceeding_created=day)
        for number, day in enumerate(created)
    ]
    transport = Transport(
        fcc_page("fcc-proceedings", documents, counted=18),  # 18 of 19 a page: exhausted, but an 18-row walk is not
        fcc_page("fcc-proceedings", documents[:9], counted=9),
        fcc_page("fcc-proceedings", documents[9:], counted=9),
    )
    rows = list(
        fcc._fetch_fcc("proceedings", since=DAY, until=date(2026, 9, 9), api_key=KEY, per_page=19, transport=transport)
    )
    assert rows == documents
    assert [call.url.params["date_proceeding_created"] for call in transport.calls] == [
        "[gte]2026-09-08[lte]2026-09-10",
        "[gte]2026-09-08[lte]2026-09-09",
        "[gte]2026-09-09[lte]2026-09-10",
    ]
    assert all(call.url.params["limit"] == "19" and call.url.params["offset"] == "0" for call in transport.calls)


@pytest.mark.parametrize(
    "field,before,after",
    [
        # Measured 2026-09-23: 14-58 carried total 10,777, recent_filings 39 over days 30, beside
        # total_filing_count 4,642 and recent_filing_count 106.
        ("total", 10_777, 10_778),
        ("recent_filings", 39, 40),
        ("days", 30, 31),
        ("total_filing_count", 4_642, 4_643),
        ("recent_filing_count", 106, 107),
        ("last_30_days", "2026-09-23T01:31:31.756Z", "2026-09-23T02:00:00.000Z"),
    ],
)
def test_fcc_proceedings_pool_by_document_not_by_filing_activity(field, before, after):
    """A proceeding's filing activity moves between walks while its document does not; the pool keys the document."""
    held, other, skipped = (_proceeding(name, **{field: before}) for name in ("17-108", "23-320", "24-1"))
    busier = {**held, field: after}
    transport = Transport(
        fcc_page("fcc-proceedings", [held, held, other], counted=3),
        fcc_page("fcc-proceedings", [skipped, skipped, busier], counted=3),
    )
    rows = list(records("fcc-proceedings", transport))
    assert sorted(row["name"] for row in rows) == ["17-108", "23-320", "24-1"]
    assert next(row for row in rows if row["name"] == "17-108")[field] == after, "the latest observation"
    # Proceedings keep the 250-row default (and its shifted second walk) while filings default to 1,000.
    assert [call.url.params["limit"] for call in transport.calls] == ["250", "237"]


def test_fcc_proceedings_edited_between_walks_refuse_rather_than_settle_on_a_surplus():
    """Any other change reads as another document: the pool overfills its count and only a clean walk could settle it."""
    held, other, skipped = _proceeding("17-108"), _proceeding("23-320"), _proceeding("24-1")
    closed = {**held, "date_closed": "2026-09-09T00:00:00Z"}
    dirty = fcc_page("fcc-proceedings", [held, held, other], counted=3)
    transport = Transport(
        dirty, fcc_page("fcc-proceedings", [skipped, skipped, closed], counted=3), *[dirty] * (DEFAULT_POOL_PASSES - 2)
    )
    with pytest.raises(IncompleteWalkError, match="pooled 4 records, more than the 3 declared"):
        list(records("fcc-proceedings", transport))


def test_fcc_proceedings_publish_one_row_per_docket_name(monkeypatch, tmp_path):
    """ECFS holds more than one document for some dockets; the original (or last edited) is published."""
    original = {
        "name": "13-84",
        "id_proceeding": 1012202662,
        "date_proceeding_created": "2013-03-27T15:50:47.000-04:00",
        "description": "RF exposure",
        "total_filing_count": 994,
    }
    recreated = {
        "name": "13-84",
        "id_proceeding": 1789995656032,
        "date_proceeding_created": "2026-09-21T13:00:56.032Z",
        "filingStatus": "OPENALL",
    }
    edited = {
        "name": "24-89",
        "id_proceeding": 1710787523643,
        "date_proceeding_created": "2024-03-18T16:24:02.000Z",
        "date_closed": "2024-12-09T05:00:00.000Z",
        "date_last_modified": "2024-12-09T19:33:20.191Z",
    }
    unedited = {
        "name": "24-89",
        "id_proceeding": 1710787523643,
        "date_proceeding_created": "2024-03-18T16:24:02.000Z",
        "filingStatus": "OPENALL",
    }
    nameless = {"id_proceeding": "0621042517382", "date_proceeding_created": "2017-06-21T21:12:23.075Z"}
    documents = [recreated, unedited, nameless, original, edited]
    monkeypatch.setattr(fcc, "_fetch_fcc", lambda *args, **kwargs: iter(documents))
    rows = pq.read_table(fcc.build_fcc_proceedings(tmp_path)).to_pylist()
    assert [(row["name"], row["id_proceeding"], row["date_closed"]) for row in rows] == [
        ("24-89", "1710787523643", "2024-12-09T05:00:00.000Z"),
        ("13-84", "1012202662", None),
    ]
    assert not list(tmp_path.glob(".*partial"))
