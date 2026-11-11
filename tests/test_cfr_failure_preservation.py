"""Refuse incomplete CFR source walks before replacing an existing table, leaving the output byte-identical.

A volume that cannot be downloaded or scanned keeps that package's prior rows.
"""

import importlib
import json
from pathlib import Path
from xml.etree import ElementTree

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.sources.cfr.acquisition import CfrAcquirer, CfrAcquisitionBudget
from spicy_docs.transport import retry
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs.sources import cfr_sections as cfr

build = importlib.import_module("spicy_regs.transforms.build_cfr_sections")
FIXTURES = Path(__file__).parent / "fixtures" / "cfr"
PACKAGE_ID = "CFR-2025-title1-vol1"
PACKAGE = {"packageId": PACKAGE_ID, "lastModified": "2026-09-11T17:30:58Z", "title": "General Provisions"}
GRANULE = {"granuleId": f"{PACKAGE_ID}-sec1-1", "title": "Definitions.", "granuleClass": "CONTENT"}
KEY = "test-key-never-in-query"


def page(key, records, *, count=None, next_page=None):
    """Build a paged-JSON envelope for ``key`` whose ``count`` defaults to the record count."""
    return {key: records, "count": len(records) if count is None else count, "nextPage": next_page}


class Transport(httpx.MockTransport):
    """Queued-body mock transport.

    An ``int`` body becomes an error response with that status, ``bytes`` are
    served verbatim, and anything else is JSON-encoded.
    """

    def __init__(self, *bodies):
        self.bodies = iter(bodies)
        self.calls = []
        super().__init__(self.handle)

    def handle(self, request):
        self.calls.append(request)
        body = next(self.bodies)
        if isinstance(body, int):
            return httpx.Response(
                body,
                stream=httpx.ByteStream(b'{"error":"source refused"}'),
                headers={"content-type": "application/json"},
            )
        if isinstance(body, bytes):
            return httpx.Response(200, stream=httpx.ByteStream(body), headers={"content-type": "application/json"})
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )


@pytest.fixture(autouse=True)
def no_delay(monkeypatch):
    """Zero retry jitter and cap requests per page so refusals are deterministic and immediate."""
    monkeypatch.setattr(retry.random, "uniform", lambda *_: 0)
    monkeypatch.setattr(cfr, "_MAX_REQUESTS_PER_PAGE", 1)


def reader(transport):
    """A ``cfr.CfrSectionsReader`` over ``transport`` pinned to the 2025 edition."""
    return cfr.CfrSectionsReader(api_key=KEY, since_year=2025, until_year=2025, transport=transport)


VOLUME_URL = f"https://www.govinfo.gov/bulkdata/CFR/2025/title-1/{PACKAGE_ID}.xml"
VOLUME = (FIXTURES / "annual-title1-vol1.xml").read_bytes()


def volumes(*responses):
    """A volume acquirer serving queued ``(status, content_type, body)`` responses, and its request log."""
    queued = iter(responses)
    calls = []

    def handle(request):
        calls.append(str(request.url))
        status, content_type, body = next(queued)
        return httpx.Response(status, stream=httpx.ByteStream(body), headers={"content-type": content_type})

    budget = CfrAcquisitionBudget(
        max_requests=build._VOLUME_REQUESTS,
        max_bytes=build.MAX_VOLUME_BYTES,
        timeout_seconds=5,
        min_request_interval_seconds=0,
    )
    return CfrAcquirer(budget=budget, transport=httpx.MockTransport(handle)), calls


def test_replays_native_granules_and_keeps_all_package_fields():
    """Pins header-only API key, native granule replay, full package retention, and the ``offsetMark=*`` start."""
    native = json.loads((FIXTURES / "govinfo-package-granules.json").read_bytes())
    # Keep the two native rows, explicitly make this a synthetic terminal fixture.
    native.update(count=2, nextPage=None)
    package = {**PACKAGE, "dateIssued": "2025-01-01", "congress": None, "source-extra": ["preserved"]}
    transport = Transport(page("packages", [package]), native)
    rows = list(reader(transport).iter_records())
    assert len(rows) == 2
    for row, original in zip(rows, native["granules"], strict=True):
        assert {key: row[key] for key in original} == original
        assert row["_package"] == package
    assert all(call.headers["x-api-key"] == KEY for call in transport.calls)
    assert all("api_key" not in call.url.params for call in transport.calls)
    assert all("offset" not in call.url.params for call in transport.calls)
    assert transport.calls[1].url.params["offsetMark"] == "*"


def test_follows_publisher_opaque_continuation_exactly():
    next_url = f"{cfr.API_BASE}/packages/{PACKAGE_ID}/granules?offsetMark=opaque%2Bcursor&pageSize=1"
    second = {**GRANULE, "granuleId": f"{PACKAGE_ID}-sec1-2"}
    transport = Transport(
        page("packages", [PACKAGE]),
        page("granules", [GRANULE], count=2, next_page=next_url),
        page("granules", [second], count=2),
    )
    assert len(list(reader(transport).iter_records())) == 2
    assert str(transport.calls[2].url) == next_url


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"error": "denied"},
        {"packages": []},
        {"packages": [], "count": None},
        {"packages": {}, "count": 0},
        {"packages": [None], "count": 1},
        page("packages", [{}]),
        page("packages", [{"packageId": " GPO-CFR-INDEX-2025"}]),
        page("packages", [], count=1),
        page("packages", [PACKAGE], count=True),
        b"not JSON",
    ],
)
def test_bad_package_page_refuses(payload):
    """SpicyDocs' discovery walk refuses an uncounted page and a missing, padded or repeated id."""
    with pytest.raises(PagedJsonSourceError):
        list(reader(Transport(payload)).iter_records())


@pytest.mark.parametrize("package_id", ["FR-2025-01-01", "GPO-CFR-INDEX-25"])
def test_a_package_outside_the_collection_refuses(package_id):
    with pytest.raises(cfr.CfrSectionsError, match="outside its collection"):
        list(reader(Transport(page("packages", [{"packageId": package_id}]))).iter_records())


def test_listed_index_package_is_skipped_with_one_log_line():
    """GovInfo's CFR listing includes its annual index, which is not a title volume."""
    from loguru import logger

    listing = json.loads((FIXTURES / "govinfo-published-cfr-index.json").read_bytes())
    volume_id = "CFR-2025-title10-vol1"
    assert [p["packageId"] for p in listing["packages"]] == [volume_id, "GPO-CFR-INDEX-2025"]
    granule = {**GRANULE, "granuleId": f"{volume_id}-sec1-1"}
    transport = Transport(listing, page("granules", [granule]))
    messages: list[str] = []
    sink = logger.add(messages.append, level="INFO", format="{message}")
    try:
        rows = list(reader(transport).iter_records())
    finally:
        logger.remove(sink)
    assert [row["_package_id"] for row in rows] == [volume_id]
    assert len(transport.calls) == 2 and "GPO-CFR-INDEX" not in str(transport.calls[1].url)
    assert [m for m in messages if "skipped" in m] == ["CFR: skipped 1 listed index package(s): GPO-CFR-INDEX-2025\n"]


@pytest.mark.parametrize(
    "payload",
    [
        page("granules", [{}]),
        page("granules", [None]),
        {"granules": [], "count": None},
        page("granules", [], count=1),
        page("granules", [GRANULE], count=2),
        page("granules", [GRANULE, GRANULE]),
    ],
)
def test_bad_granule_page_refuses(payload):
    """SpicyDocs' discovery walk refuses an uncounted page and a missing, padded or repeated id."""
    with pytest.raises(PagedJsonSourceError):
        list(reader(Transport(page("packages", [PACKAGE]), payload)).iter_records())


def test_a_granule_of_another_package_refuses():
    other = page("granules", [{**GRANULE, "granuleId": "CFR-2024-title1-vol1-sec1-1"}])
    with pytest.raises(cfr.CfrSectionsError, match="differs from its requested package"):
        list(reader(Transport(page("packages", [PACKAGE]), other)).iter_records())


def test_missing_or_repeated_continuation_cannot_finish_a_partial_selection():
    next_url = f"{cfr.API_BASE}/packages/{PACKAGE_ID}/granules?offsetMark=next&pageSize=1"
    transport = Transport(
        page("packages", [PACKAGE]),
        page("granules", [GRANULE], count=2, next_page=next_url),
        page("granules", [], count=2, next_page=next_url),
    )
    with pytest.raises(PagedJsonSourceError, match="repeats|repeated"):
        list(reader(transport).iter_records())


def test_page_bound_refuses(monkeypatch):
    monkeypatch.setattr(cfr, "_MAX_PAGES", 1)
    next_url = f"{cfr.API_BASE}/packages/{PACKAGE_ID}/granules?offsetMark=next"
    transport = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE], count=2, next_page=next_url))
    with pytest.raises(PagedJsonSourceError, match="page bound"):
        list(reader(transport).iter_records())


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
def test_http_failures_are_not_empty_selections(status):
    """An HTTP status must raise (never an empty selection), make one call, and keep the API key out of the error."""
    transport = Transport(status)
    with pytest.raises((CredentialRefusedError, PagedJsonSourceError, httpx.HTTPError)) as refused:
        list(reader(transport).iter_records())
    assert KEY not in str(refused.value)
    assert len(transport.calls) == 1


def test_explicit_empty_selection_is_valid():
    assert list(reader(Transport(page("packages", []))).iter_records()) == []
    assert list(reader(Transport(page("packages", [PACKAGE]), page("granules", []))).iter_records()) == []


@pytest.mark.parametrize(
    ("failure", "refusal"),
    [
        ("missing-key", cfr.CfrSectionsError),
        ("http", CredentialRefusedError),
        ("partial", PagedJsonSourceError),
        ("missing-identity", PagedJsonSourceError),
    ],
)
def test_source_failure_preserves_prior_output_byte_for_byte(monkeypatch, tmp_path, failure, refusal):
    """Every failure mode leaves the published output and ``_cfr_prior.parquet`` unchanged.

    No ``_cfr_new.parquet`` is left behind either.
    """
    prior_row = build._shape({**GRANULE, "_package_id": PACKAGE_ID})
    prior_file = tmp_path / "_cfr_prior.parquet"
    output = tmp_path / build.OUTPUT
    table = pa.Table.from_pylist([prior_row], schema=build._SCHEMA)
    pq.write_table(table, prior_file)
    pq.write_table(table, output)
    before = output.read_bytes()
    prior_before = prior_file.read_bytes()
    if failure == "missing-key":
        selected = cfr.CfrSectionsReader(api_key="")
    else:
        payload = {
            "http": 403,
            "partial": page("granules", [GRANULE], count=2),
            "missing-identity": page("granules", [{}]),
        }[failure]
        selected = reader(Transport(page("packages", [PACKAGE]), payload))
    monkeypatch.setattr(build, "CfrSectionsReader", lambda **_: selected)
    with pytest.raises(refusal):
        build.build_cfr_sections(tmp_path, since_year=2025)
    assert output.read_bytes() == before
    assert prior_file.read_bytes() == prior_before
    assert not (tmp_path / "_cfr_new.parquet").exists()


def test_explicit_empty_build_preserves_prior_rows(monkeypatch, tmp_path):
    prior_row = build._shape({**GRANULE, "_package_id": PACKAGE_ID})
    pq.write_table(pa.Table.from_pylist([prior_row], schema=build._SCHEMA), tmp_path / "_cfr_prior.parquet")
    selected = reader(Transport(page("packages", [])))
    monkeypatch.setattr(build, "CfrSectionsReader", lambda **_: selected)
    output = build.build_cfr_sections(tmp_path, since_year=2025)
    assert pq.read_table(output).to_pylist() == [prior_row]


def test_native_ancestry_disproves_section_prefix_inference():
    """Pins that ``part``/``cfr_ref`` are not prefix-inferred from a section id.

    Native eCFR ancestry puts N=19-8.1 under part 241, so the ID prefix would be wrong.
    """
    root = ElementTree.fromstring((FIXTURES / "ecfr-api-title14-numbering.xml").read_bytes())
    native_part = root.find(".//*[@TYPE='PART']")
    assert native_part is not None
    native_section = native_part.find(".//*[@TYPE='SECTION']")
    assert native_section is not None
    assert native_part.attrib["N"] == "241"
    assert native_section.attrib["N"] == "19-8.1"
    # This corresponding ID is pinned in the independent public-table audit.
    row = build._shape({"granuleId": "CFR-2025-title14-vol4-sec19-8-1"})
    assert row["part"] is None
    assert row["section"] == "19-8-1"
    assert row["cfr_ref"] is None
    # Current eCFR ancestry is not assigned to an annual edition; the edition's own
    # volume places the section, and agrees.
    [placed] = build.place_sections([row], (FIXTURES / "ancestry" / "CFR-2025-title14-vol4.xml").read_bytes())
    assert (placed["part"], placed["section"], placed["cfr_ref"]) == ("241", "19-8.1", None)


def test_granule_modification_date_takes_precedence_over_package_date():
    row = build._shape(
        {**GRANULE, "lastModified": "2025-01-02T00:00:00Z", "_package_last_modified": "2026-09-11T17:30:58Z"}
    )
    assert row["last_modified"] == "2025-01-02T00:00:00Z"


@pytest.mark.parametrize("identity", [None, "", "  ", [], 12])
def test_shaper_refuses_missing_or_malformed_identity(identity):
    with pytest.raises(cfr.CfrSectionsError):
        build._shape({"granuleId": identity})


def test_fresh_annual_records_are_never_placed_in_false_part_19():
    """Pins that granule token ``19-8-1`` is section 19-8.1 of Part 241, never Part 19, with no citation.

    ``19-8-10`` is not in the volume excerpt, so it keeps its token and an unknown part.
    """
    fixture = json.loads((FIXTURES / "govinfo-title14-counterexamples.json").read_bytes())
    package = fixture["package"]
    assert package["packageId"] == "CFR-2025-title14-vol4"
    assert package["lastModified"] == "2025-06-17T21:29:08Z"
    expected = {
        "CFR-2025-title14-vol4-sec19-8-1": ("241", "19-8.1", "Purpose."),
        "CFR-2025-title14-vol4-sec19-8-10": (None, "19-8-10", "Staff review."),
    }
    assert {row["granuleId"] for row in fixture["granules"]} == set(expected)
    shaped = [
        build._shape({**raw, "_package_id": package["packageId"], "_package_last_modified": package["lastModified"]})
        for raw in fixture["granules"]
    ]
    xml = (FIXTURES / "ancestry" / "CFR-2025-title14-vol4.xml").read_bytes()
    for row in build.place_sections(shaped, xml):
        assert (row["part"], row["section"], row["heading"]) == expected[row["granule_id"]]
        assert row["cfr_ref"] is None
        assert row["edition_year"] == "2025" and row["title"] == "14"
        assert row["last_modified"] == package["lastModified"]


def build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=None, *, marked=False, replace_all=False):
    """Run the build over ``walk`` and ``acquirer``; return output rows by granule.

    ``prior_rows=None`` means no prior table at all; ``marked`` stamps the prior
    with the placement marker a generation built by this rule carries.
    """
    if prior_rows is None:
        monkeypatch.setattr(build.r2, "download", lambda *_: False)
    else:
        table = pa.Table.from_pylist(prior_rows, schema=build._SCHEMA)
        pq.write_table(
            table.replace_schema_metadata(build.PLACEMENT_MARKER if marked else None), tmp_path / "_cfr_prior.parquet"
        )
    monkeypatch.setattr(build, "CfrSectionsReader", lambda **_: reader(walk))
    output = build.build_cfr_sections(tmp_path, since_year=2025, replace_all=replace_all, acquirer=acquirer)
    return {row["granule_id"]: row for row in pq.read_table(output).to_pylist()}


def is_marked(tmp_path):
    [(key, value)] = build.PLACEMENT_MARKER.items()
    return (pq.read_schema(tmp_path / build.OUTPUT).metadata or {}).get(key.encode()) == value.encode()


def placed_prior(stamp=PACKAGE["lastModified"], heading="Prior heading."):
    """A prior row for GRANULE as this rule places it, stamped with its package's lastModified."""
    shaped = build._shape({**GRANULE, "_package_id": PACKAGE_ID, "_package_last_modified": stamp})
    return {**shaped, "part": "1", "section": "1", "cfr_ref": "1-1.1", "heading": heading}


def test_held_rows_gain_part_granule_from_their_ids_without_a_volume_read(monkeypatch, tmp_path):
    """A prior published before ``part_granule`` existed gains it on an unchanged package: no volume is read."""
    part = {"granuleId": f"{PACKAGE_ID}-part1", "title": "GENERAL PROVISIONS", "granuleClass": "NODE"}
    held = [placed_prior(), build._shape({**part, "_package_id": PACKAGE_ID,
                                          "_package_last_modified": PACKAGE["lastModified"]})]
    before = [column for column in build.COLUMNS if column != "part_granule"]
    table = pa.Table.from_pylist([{c: row[c] for c in before} for row in held])
    pq.write_table(table.replace_schema_metadata(build.PLACEMENT_MARKER), tmp_path / "_cfr_prior.parquet")
    acquirer, calls = volumes()
    rows = build_with(monkeypatch, tmp_path, Transport(page("packages", [PACKAGE]), page("granules", [GRANULE, part])),
                      acquirer)
    assert calls == []
    assert rows[part["granuleId"]]["part_granule"] == "true"
    assert rows[GRANULE["granuleId"]]["part_granule"] == "false"
    assert rows[GRANULE["granuleId"]]["heading"] == "Prior heading."  # the held row, not a re-read
    assert is_marked(tmp_path)


def test_build_places_section_granules_from_one_volume_download(monkeypatch, tmp_path):
    second = {**GRANULE, "granuleId": f"{PACKAGE_ID}-sec1-2"}
    acquirer, calls = volumes((200, "application/xml", VOLUME))
    walk = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE, second]))
    rows = build_with(monkeypatch, tmp_path, walk, acquirer)
    assert calls == [VOLUME_URL]
    placed = rows[GRANULE["granuleId"]]
    assert (placed["part"], placed["section"], placed["cfr_ref"]) == ("1", "1", "1-1.1")
    # § 1.2 is not in the excerpt: nothing to place, so it keeps its identifier values.
    unplaced = rows[second["granuleId"]]
    assert (unplaced["part"], unplaced["section"], unplaced["cfr_ref"]) == (None, "1-2", None)
    assert is_marked(tmp_path)


def test_build_downloads_no_volume_for_a_package_without_section_granules(monkeypatch, tmp_path):
    node = {"granuleId": f"{PACKAGE_ID}-part1", "title": "PART 1—DEFINITIONS", "granuleClass": "NODE"}
    acquirer, calls = volumes()
    rows = build_with(monkeypatch, tmp_path, Transport(page("packages", [PACKAGE]), page("granules", [node])), acquirer)
    assert calls == []
    assert rows[node["granuleId"]]["cfr_ref"] == "1-1"


FAILURES = pytest.mark.parametrize(
    "responses",
    [
        [(404, "text/html", b"not found")],
        [(500, "text/plain", b"down")] * build._VOLUME_REQUESTS,
        [(200, "text/html", b"<html>not a volume</html>")],
        [(200, "application/xml", b"<html>not a volume</html>")],
        [(200, "application/xml", VOLUME[: len(VOLUME) // 2])],
        # Another title's volume: it scans, but the validator refuses it before placement.
        [(200, "application/xml", (FIXTURES / "ancestry" / "CFR-2025-title14-vol4.xml").read_bytes())],
    ],
    ids=["unavailable", "server-error", "not-xml", "not-a-volume", "truncated", "another-title"],
)


@FAILURES
def test_failed_volume_keeps_the_package_prior_rows(monkeypatch, tmp_path, capsys, responses):
    """The package changed since the prior table, but its volume failed: the prior row stands unchanged."""
    prior_row = placed_prior(stamp="2025-01-02T00:00:00Z")
    other_id = "CFR-2025-title2-vol1"
    other = {"packageId": other_id, "lastModified": "2026-09-11T17:30:58Z"}
    node = {"granuleId": f"{other_id}-part1", "title": "PART 1", "granuleClass": "NODE"}
    walk = Transport(page("packages", [PACKAGE, other]), page("granules", [GRANULE]), page("granules", [node]))
    acquirer, calls = volumes(*responses)
    rows = build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=[prior_row], marked=True)
    assert calls == [VOLUME_URL] * len(responses)
    assert rows[GRANULE["granuleId"]] == prior_row
    # Another package in the same run is still refreshed, and the table stays marked.
    assert rows[node["granuleId"]]["cfr_ref"] == "2-1"
    assert is_marked(tmp_path)
    assert f"::warning title=cfr_sections::{PACKAGE_ID} volume failed; its prior rows stand" in capsys.readouterr().out


@FAILURES
def test_failed_volume_with_no_prior_table_refuses_the_run(monkeypatch, tmp_path, responses):
    acquirer, _calls = volumes(*responses)
    walk = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE]))
    with pytest.raises(cfr.CfrSectionsError, match="no prior table to keep"):
        build_with(monkeypatch, tmp_path, walk, acquirer)
    assert not (tmp_path / build.OUTPUT).exists()
    assert not (tmp_path / "_cfr_new.parquet").exists()


def test_failed_volume_of_a_package_new_to_the_prior_is_annotated_as_an_error(monkeypatch, tmp_path, capsys):
    other_id = "CFR-2025-title2-vol1"
    other_row = {**build._shape({"granuleId": f"{other_id}-part1", "_package_id": other_id}), "heading": "Kept."}
    acquirer, _calls = volumes((404, "text/html", b"not found"))
    walk = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE]))
    rows = build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=[other_row], marked=True)
    assert rows == {other_row["granule_id"]: other_row}
    out = capsys.readouterr().out
    assert f"::error title=cfr_sections::{PACKAGE_ID} volume failed and it has no prior rows" in out


def test_access_refusal_aborts_the_run_instead_of_keeping_prior_rows(monkeypatch, tmp_path):
    acquirer, _calls = volumes((403, "text/html", b"denied"))
    walk = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE]))
    with pytest.raises(CredentialRefusedError):
        build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=[placed_prior(stamp="2025-01-02T00:00:00Z")])
    assert not (tmp_path / build.OUTPUT).exists() and not (tmp_path / "_cfr_new.parquet").exists()


def test_default_volume_client_is_bounded():
    budget = build._volume_acquirer().budget
    assert budget.max_bytes == build.MAX_VOLUME_BYTES == 64 * 1024 * 1024
    assert budget.max_requests == build._VOLUME_REQUESTS
    assert budget.min_request_interval_seconds > 0


def test_unchanged_package_keeps_its_prior_rows_without_a_download(monkeypatch, tmp_path):
    """The package stamp matches a marked prior: no volume request, prior row untouched.

    GovInfo's granule listing carries no lastModified, so each row's
    last_modified is its package's and the unchanged check is per package.
    """
    assert "lastModified" not in GRANULE
    prior_row = placed_prior()
    acquirer, calls = volumes()
    walk = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE]))
    rows = build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=[prior_row], marked=True)
    assert calls == []
    assert rows == {GRANULE["granuleId"]: prior_row}
    assert is_marked(tmp_path)


@pytest.mark.parametrize("change", ["package-stamp", "new-granule", "unmarked-prior", "replace-all"])
def test_changed_or_unproven_package_is_downloaded_and_re_placed(monkeypatch, tmp_path, change):
    """A new package stamp or granule, a prior without the placement marker, or replace_all re-places the package.

    The stamp is the package's lastModified (the granule listing has none).
    """
    prior_row = placed_prior(stamp="2025-01-02T00:00:00Z" if change == "package-stamp" else PACKAGE["lastModified"])
    second = {**GRANULE, "granuleId": f"{PACKAGE_ID}-sec1-2"}
    listed = [GRANULE, second] if change == "new-granule" else [GRANULE]
    acquirer, calls = volumes((200, "application/xml", VOLUME))
    walk = Transport(page("packages", [PACKAGE]), page("granules", listed))
    rows = build_with(
        monkeypatch,
        tmp_path,
        walk,
        acquirer,
        prior_rows=[prior_row],
        marked=change != "unmarked-prior",
        replace_all=change == "replace-all",
    )
    assert calls == [VOLUME_URL]
    placed = rows[GRANULE["granuleId"]]
    assert (placed["part"], placed["cfr_ref"], placed["last_modified"]) == ("1", "1-1.1", PACKAGE["lastModified"])
    assert placed["heading"] == GRANULE["title"]
    assert set(rows) == {row["granuleId"] for row in listed}
    assert is_marked(tmp_path)


def test_unmarked_prior_stays_unmarked_while_any_package_keeps_unplaced_rows(monkeypatch, tmp_path):
    acquirer, _calls = volumes((404, "text/html", b"not found"))
    walk = Transport(page("packages", [PACKAGE]), page("granules", [GRANULE]))
    build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=[placed_prior()], marked=False)
    assert not is_marked(tmp_path)



def test_a_failed_volume_without_prior_rows_does_not_hold_the_marker_back(monkeypatch, tmp_path):
    """Every prior package re-placed, and a new package's volume refused: no unplaced row remains, so the output is
    marked and the next night places only changed packages (CFR-2026-title2-vol01 failed so every night)."""
    new_id = "CFR-2025-title1-vol2"
    new = {"packageId": new_id, "lastModified": PACKAGE["lastModified"], "title": "General Provisions"}
    new_granule = {**GRANULE, "granuleId": f"{new_id}-sec1-1"}
    acquirer, calls = volumes((200, "application/xml", VOLUME), (404, "text/html", b"not found"))
    walk = Transport(page("packages", [PACKAGE, new]), page("granules", [GRANULE]), page("granules", [new_granule]))
    rows = build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=[placed_prior()], marked=False)
    assert len(calls) == 2
    assert set(rows) == {GRANULE["granuleId"]}
    assert is_marked(tmp_path)

def test_prior_index_package_rows_are_dropped(monkeypatch, tmp_path):
    """GPO-CFR-INDEX-2025's rows are not CFR sections; the walk skips the package and the table drops them."""
    index = build._shape(
        {"granuleId": "GPO-CFR-INDEX-2025-1", "_package_id": "GPO-CFR-INDEX-2025", "dateIssued": "2025"}
    )
    acquirer, _calls = volumes()
    rows = build_with(
        monkeypatch,
        tmp_path,
        Transport(page("packages", [])),
        acquirer,
        prior_rows=[index, placed_prior()],
        marked=True,
    )
    assert set(rows) == {GRANULE["granuleId"]}
    assert is_marked(tmp_path)


def test_an_unchanged_title_41_package_takes_compound_parts_from_its_prior_without_a_volume(monkeypatch, tmp_path):
    """The prior's cut keys (41-60) are corrected in place: an unchanged package's rows are never re-read."""
    package_id, stamp = "CFR-2025-title41-vol1", "2026-09-11T17:30:58Z"
    granules = [{"granuleId": f"{package_id}-part60", "title": "1-OBLIGATIONS OF CONTRACTORS AND SUBCONTRACTORS",
                 "granuleClass": "NODE"},
                {"granuleId": f"{package_id}-part60-id334", "title": "2-AFFIRMATIVE ACTION PROGRAMS",
                 "granuleClass": "NODE"},
                {"granuleId": f"{package_id}-part60-id334-subpartA", "title": "General", "granuleClass": "NODE"}]
    prior = [build._shape({**g, "_package_id": package_id, "_package_last_modified": stamp}) for g in granules]
    assert {row["cfr_ref"] for row in prior} == {"41-60"}
    walk = Transport(page("packages", [{"packageId": package_id, "lastModified": stamp}]), page("granules", granules))
    acquirer, calls = volumes()
    rows = build_with(monkeypatch, tmp_path, walk, acquirer, prior_rows=prior, marked=True)
    assert calls == []
    assert {gid: row["cfr_ref"] for gid, row in rows.items()} == {
        f"{package_id}-part60": "41-60-1", f"{package_id}-part60-id334": "41-60-2",
        f"{package_id}-part60-id334-subpartA": "41-60-2"}
    assert is_marked(tmp_path)
