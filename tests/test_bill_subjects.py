"""Hermetic tests for the per-bill subject enrichment (no network).

Covers the pieces with real logic: the route each Congress takes (the bill
family's own BILLSTATUS rows, a folder's bulk zip read through spicy-docs, the
Congress.gov page walk), and the three outcomes the transform depends on — an
answer, a definitive "not held", and a failure that must leave the bill for the
next run rather than pin an empty or truncated answer to it.
"""

from __future__ import annotations

import importlib
import io
import json
from datetime import datetime
import zipfile

import httpx

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from loguru import logger
from spicy_docs.sources.congress.bill_acquisition import BillSourceUnavailableError
from spicy_docs.sources.congress.bill_status import BillSourceError
from spicy_docs.sources.congress.bulk_status import read_bulk_status_archive
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.sources.bill_subjects import (
    API_HOURLY_BUDGET,
    CARRIER_API,
    CARRIER_BULKDATA,
    DELAY_SECONDS,
    BillSubjects,
    BillSubjectsFetcher,
    FetchCounts,
    assignment,
)
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS
from spicy_regs.transforms.congress_scope import current_congress
from spicy_regs.transforms.enrich_bill_subjects import (
    DEADLINE_SECONDS,
    COLUMNS,
    MAX_API_BILLS_PER_RUN,
    _pending_bills,
    _shape,
    enrich_bill_subjects,
)

# A BILLSTATUS record in the shape GPO actually serves: <policyArea> appears
# both as a direct child of <bill> and again inside <subjects>, and a term can
# repeat across containers.
_BILLSTATUS = """<?xml version="1.0" encoding="UTF-8"?>
<billStatus>
  <version>3.0.0</version>
  <bill>
    <number>{number}</number>
    <title>Synthetic bill for subject receiver tests</title>
    <congress>{congress}</congress>
    <type>HR</type>
    <policyArea><name>Environmental Protection</name></policyArea>
    <subjects>
      <legislativeSubjects>
        <item><name>Air quality</name></item>
        <item><name>  Congressional  oversight </name></item>
        <item><name>Air quality</name></item>
      </legislativeSubjects>
      <policyArea>
        <name>Environmental Protection</name>
        <updateDate>2024-01-02T00:00:00Z</updateDate>
      </policyArea>
    </subjects>
  </bill>
</billStatus>
"""

# The superseded schema the publisher still serves for a few reserved numbers
# (117 hr 9 and 13-17 on 2026-09-23); the shared reader refuses it by name.
_BILLSTATUS_1_0_0 = """<billStatus><bill><billType>HR</billType><billNumber>{number}</billNumber>
<congress>{congress}</congress><title>Reserved for the Speaker.</title></bill></billStatus>"""

#: An older Congress, whose zip is settled, and the previous one, whose zip can lag its bill list.
OLD = 110
RECENT = current_congress() - 1

_FAMILY_COLUMNS = ("bill_id", "congress", "bill_type", "bill_number", "schema_version", "policy_area", "subjects_json")


def _keyless(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _bill(bill_id, *, family=None):
    """One congress_bills row; ``family`` is the (policy_area, subjects) BILLSTATUS filled it with."""
    congress, bill_type, number = bill_id.split("-")
    row = dict(bill_id=bill_id, congress=congress, bill_type=bill_type, bill_number=number)
    if family is not None:
        policy_area, subjects = family
        subjects_json = None if subjects is None else json.dumps(subjects)
        row |= dict(schema_version="3.0.0", policy_area=policy_area, subjects_json=subjects_json)
    return row


def _write_bills(path, rows):
    """Write a congress_bills.parquet fixture with the family columns this transform reads."""
    schema = pa.schema([(c, pa.string()) for c in _FAMILY_COLUMNS])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _zip(congress, members, *, named=None):
    """A BILLSTATUS folder zip: ``members`` maps bill number to XML template; ``named`` adds raw entry names."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for number, template in members.items():
            archive.writestr(f"BILLSTATUS-{congress}hr{number}.xml", template.format(congress=congress, number=number))
        for name, body in (named or {}).items():
            archive.writestr(name, body)
    return buffer.getvalue()


def _write_prior(path, rows):
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in COLUMNS])), path)


class _Folders:
    """Serves retained folder zips through the real spicy-docs archive reader, counting reads."""

    def __init__(self, zips=None, errors=None):
        self.zips = zips or {}
        self.errors = errors or {}
        self.read: list[tuple[int, str]] = []

    def __call__(self, congress, bill_type):
        self.read.append((congress, bill_type))
        if (congress, bill_type) in self.errors:
            raise self.errors[(congress, bill_type)]
        return read_bulk_status_archive(self.zips[(congress, bill_type)], congress=congress, bill_type=bill_type)


class _StubFetcher:
    """Answers the API route from a canned map; anything unmapped is a transport failure."""

    def __init__(self, answers=None):
        self.answers = answers or {}
        self.asked: list[str] = []

    def subjects_for(self, congress, bill_type, bill_number):
        key = f"{congress}-{bill_type}-{bill_number}"
        self.asked.append(key)
        return self.answers.get(key)

    def close(self):
        pass


def _rows(path):
    return {row["bill_id"]: row for row in pq.read_table(path).to_pylist()}


# -- the published shape and cleanup ------------------------------------------


def test_shape_produces_exact_schema():
    row = _shape("118-hr-1", "Health", ("Medicare",), CARRIER_API, "2026-08-22T00:00:00+00:00")
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 6
    assert row["subjects_json"] == '["Medicare"]'
    assert row["subject_count"] == "1"
    assert row["carrier"] == CARRIER_API


def test_assignment_preserves_literal_names_nulls_blanks_and_repetition():
    result = assignment("  Health ", ["B", " A  a ", None, "", "B", "A a"], CARRIER_BULKDATA)
    assert result == BillSubjects("  Health ", ("B", " A  a ", None, "", "B", "A a"), CARRIER_BULKDATA)
    assert assignment(" ", [], CARRIER_API).policy_area == " "
    with pytest.raises(ValueError, match="text or null"):
        assignment("Health", [{"name": "unsupported"}], CARRIER_API)


def test_every_result_lands_in_exactly_one_bucket():
    counts = FetchCounts()
    for result in (
        BillSubjects("Health", ("Medicare",), CARRIER_API),
        BillSubjects(None, ("Medicare",), CARRIER_API),
        BillSubjects(None, (), CARRIER_API),
        BillSubjects(None, (), CARRIER_API, held=False),
        BillSubjects("Health", (), CARRIER_BULKDATA),
        None,
    ):
        counts.record(result)
    assert (counts.with_policy_area, counts.subjects_only, counts.unassigned, counts.not_held) == (2, 1, 1, 1)
    assert (counts.answered, counts.failed) == (5, 1)
    assert counts.policy_areas == {"Health": 2}


# -- routes: the family's rows, a folder zip, or waiting ----------------------


def test_family_rows_are_copied_without_any_request(tmp_path):
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [_bill("119-hr-1", family=("Health", [" Medicare ", "Drug safety", "Medicare"]))],
    )
    folders, fetcher = _Folders(), _StubFetcher()
    out = enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=fetcher)
    row = _rows(out)["119-hr-1"]
    assert (row["policy_area"], json.loads(row["subjects_json"]), row["subject_count"], row["carrier"]) == (
        "Health",
        [" Medicare ", "Drug safety", "Medicare"],
        "3",
        CARRIER_BULKDATA,
    )
    assert folders.read == [] and fetcher.asked == []


def test_a_changed_family_row_is_copied_again_and_an_unchanged_one_keeps_its_time(tmp_path):
    """119-hr-181 on 2026-09-23: published with no subjects, the family's later read lists two."""
    earlier = "2026-08-22T04:40:39+00:00"
    _write_prior(
        tmp_path / "_bill_subjects_prior.parquet",
        [
            _shape("119-hr-181", "Environmental Protection", (), CARRIER_BULKDATA, earlier),
            _shape("119-hr-2", "Health", ("Medicare",), CARRIER_BULKDATA, earlier),
            _shape("119-hr-3", None, (), CARRIER_BULKDATA, earlier),
        ],
    )
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [
            _bill("119-hr-181", family=("Environmental Protection", ["Endangered and threatened species"])),
            _bill("119-hr-2", family=("Health", ["Medicare"])),
            _bill("119-hr-3", family=("Taxation", [])),  # CRS assigned a policy area after the first read
        ],
    )
    rows = _rows(enrich_bill_subjects(tmp_path, read_folder=_Folders(), fetcher=_StubFetcher()))
    assert json.loads(rows["119-hr-181"]["subjects_json"]) == ["Endangered and threatened species"]
    assert rows["119-hr-3"]["policy_area"] == "Taxation"
    assert rows["119-hr-181"]["enriched_at"] != earlier and rows["119-hr-3"]["enriched_at"] != earlier
    assert rows["119-hr-2"]["enriched_at"] == earlier


def test_a_family_row_without_a_subject_list_is_no_answer(tmp_path):
    earlier = "2026-08-22T04:40:39+00:00"
    _write_prior(
        tmp_path / "_bill_subjects_prior.parquet",
        [_shape("119-hr-1", "Health", ("Medicare",), CARRIER_BULKDATA, earlier)],
    )
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [_bill("119-hr-1", family=("Taxation", None)), _bill("119-hr-2", family=("Health", None))],
    )
    rows = _rows(enrich_bill_subjects(tmp_path, read_folder=_Folders(), fetcher=_StubFetcher()))
    assert set(rows) == {"119-hr-1"}
    assert (rows["119-hr-1"]["policy_area"], rows["119-hr-1"]["enriched_at"]) == ("Health", earlier)


def test_a_folder_the_family_has_not_read_comes_from_its_zip_once(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"{OLD}-hr-{n}") for n in (1, 2, 3, 4)])
    folders = _Folders({(OLD, "hr"): _zip(OLD, {1: _BILLSTATUS, 2: _BILLSTATUS, 3: _BILLSTATUS_1_0_0})})
    fetcher = _StubFetcher({f"{OLD}-hr-3": BillSubjects("Health", (), CARRIER_API)})
    rows = _rows(enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=fetcher))
    assert folders.read == [(OLD, "hr")]
    assert json.loads(rows[f"{OLD}-hr-1"]["subjects_json"]) == [
        "Air quality", "  Congressional  oversight ", "Air quality",
    ]
    assert rows[f"{OLD}-hr-2"]["policy_area"] == "Environmental Protection"
    # A file the reader refuses goes to the API; a bill a settled zip does not list is not held.
    assert fetcher.asked == [f"{OLD}-hr-3"]
    assert (rows[f"{OLD}-hr-3"]["policy_area"], rows[f"{OLD}-hr-3"]["carrier"]) == ("Health", CARRIER_API)
    assert (rows[f"{OLD}-hr-4"]["policy_area"], rows[f"{OLD}-hr-4"]["subjects_json"]) == (None, "[]")


def test_a_bill_missing_from_a_recent_congress_zip_is_asked_again(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"{RECENT}-hr-1"), _bill(f"{RECENT}-hr-2")])
    zips = {(RECENT, "hr"): _zip(RECENT, {1: _BILLSTATUS})}
    out = enrich_bill_subjects(tmp_path, read_folder=_Folders(zips), fetcher=_StubFetcher())
    assert set(_rows(out)) == {f"{RECENT}-hr-1"}
    zips[(RECENT, "hr")] = _zip(RECENT, {1: _BILLSTATUS, 2: _BILLSTATUS})
    again = _Folders(zips)
    out = enrich_bill_subjects(tmp_path, read_folder=again, fetcher=_StubFetcher())
    assert again.read == [(RECENT, "hr")]
    assert _rows(out)[f"{RECENT}-hr-2"]["policy_area"] == "Environmental Protection"


def test_a_bill_a_zip_may_hold_under_an_unreadable_name_goes_to_the_api(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"{OLD}-hr-1"), _bill(f"{OLD}-hr-2")])
    zip_body = _zip(OLD, {1: _BILLSTATUS}, named={f"BILLSTATUS-{OLD}hr0002.xml": b"<billStatus/>"})
    fetcher = _StubFetcher({f"{OLD}-hr-2": BillSubjects("Taxation", (), CARRIER_API)})
    rows = _rows(enrich_bill_subjects(tmp_path, read_folder=_Folders({(OLD, "hr"): zip_body}), fetcher=fetcher))
    assert fetcher.asked == [f"{OLD}-hr-2"]
    assert (rows[f"{OLD}-hr-1"]["carrier"], rows[f"{OLD}-hr-2"]["carrier"]) == (CARRIER_BULKDATA, CARRIER_API)


def test_a_list_level_bill_in_a_folder_the_family_reads_waits_for_the_family(tmp_path):
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [_bill(f"{RECENT}-hr-1", family=("Health", [])), _bill(f"{RECENT}-hr-2")],
    )
    folders, fetcher = _Folders(), _StubFetcher()
    out = enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=fetcher)
    assert set(_rows(out)) == {f"{RECENT}-hr-1"}
    assert folders.read == [] and fetcher.asked == []


def test_a_list_level_bill_a_settled_family_read_skipped_goes_to_the_api(tmp_path):
    """The family skips a member the reader refuses; in a settled Congress nothing else leaves a bill list-level."""
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [_bill(f"{OLD}-hr-1", family=("Health", [])), _bill(f"{OLD}-hr-2")],
    )
    folders = _Folders()
    fetcher = _StubFetcher({f"{OLD}-hr-2": BillSubjects("Taxation", (), CARRIER_API)})
    rows = _rows(enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=fetcher))
    assert folders.read == [] and fetcher.asked == [f"{OLD}-hr-2"]
    assert rows[f"{OLD}-hr-2"]["carrier"] == CARRIER_API


def test_folders_are_read_newest_first_up_to_the_cap(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("117-hr-1"), _bill("118-hr-1")])
    zips = {(congress, "hr"): _zip(congress, {1: _BILLSTATUS}) for congress in (117, 118)}
    first = _Folders(zips)
    enrich_bill_subjects(tmp_path, max_folders=1, read_folder=first, fetcher=_StubFetcher())
    assert first.read == [(118, "hr")]
    second = _Folders(zips)
    out = enrich_bill_subjects(tmp_path, max_folders=1, read_folder=second, fetcher=_StubFetcher())
    assert second.read == [(117, "hr")]
    assert set(_rows(out)) == {"117-hr-1", "118-hr-1"}


def _unavailable():
    url = "https://www.govinfo.gov/bulkdata/BILLSTATUS/110/hr/BILLSTATUS-110-hr.zip"
    return BillSourceUnavailableError(
        CapturedBodyResponse(url, url, 404, "text/html", "2026-09-23T00:00:00Z", body=b"")
    )


@pytest.mark.parametrize(
    "error,congress,pinned",
    [
        (_unavailable(), OLD, True),
        (_unavailable(), RECENT, False),  # a new Congress's folder can 404 before its first bill posts
        (BillSourceError("BILLSTATUS archive exceeds its entry bound"), OLD, False),
        (httpx.ConnectError("private transport detail"), OLD, False),
    ],
)
def test_only_a_settled_unpublished_folder_pins_not_held(tmp_path, error, congress, pinned):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"{congress}-hr-1")])
    folders = _Folders(errors={(congress, "hr"): error})
    rows = _rows(enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=_StubFetcher()))
    assert (f"{congress}-hr-1" in rows) is pinned
    if pinned:
        assert (rows[f"{congress}-hr-1"]["policy_area"], rows[f"{congress}-hr-1"]["carrier"]) == (
            None,
            CARRIER_BULKDATA,
        )


def test_a_credential_refusal_from_a_folder_stops_the_run(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("118-hr-1")])
    refused = _Folders(errors={(118, "hr"): CredentialRefusedError("refused")})
    with pytest.raises(CredentialRefusedError):
        enrich_bill_subjects(tmp_path, read_folder=refused, fetcher=_StubFetcher())


# -- routes: the Congress.gov API below the 108th -----------------------------


def test_only_bills_below_the_108th_congress_reach_the_api(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("107-hr-1"), _bill("108-hr-1")])
    fetcher = _StubFetcher({"107-hr-1": BillSubjects("Health", (), CARRIER_API)})
    folders = _Folders({(108, "hr"): _zip(108, {1: _BILLSTATUS})})
    out = enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=fetcher)
    assert fetcher.asked == ["107-hr-1"]
    assert folders.read == [(108, "hr")]
    assert {bill_id: row["carrier"] for bill_id, row in _rows(out).items()} == {
        "107-hr-1": CARRIER_API,
        "108-hr-1": CARRIER_BULKDATA,
    }


def test_without_a_key_api_bills_wait_rather_than_fail(tmp_path, monkeypatch):
    _keyless(monkeypatch)
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("107-hr-1")])
    out = enrich_bill_subjects(tmp_path, read_folder=_Folders())
    assert _rows(out) == {}


def test_a_capped_api_run_resumes_where_it_stopped(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"100-hr-{n}") for n in range(1, 6)])
    answers = {f"100-hr-{n}": BillSubjects("Health", ("Medicare",), CARRIER_API) for n in range(1, 6)}
    first = _StubFetcher(answers)
    enrich_bill_subjects(tmp_path, max_bills=2, fetcher=first)
    second = _StubFetcher(answers)
    out = enrich_bill_subjects(tmp_path, max_bills=2, fetcher=second)
    assert len(first.asked) == len(second.asked) == 2
    assert set(second.asked).isdisjoint(first.asked)
    assert pq.ParquetFile(out).metadata.num_rows == 4
    assert pq.ParquetFile(out).schema_arrow.names == list(COLUMNS)


@pytest.mark.parametrize(("day", "max_bills", "asked"), [
    (5, None, ["100-hr-1"]), (6, None, ["100-hr-2", "100-hr-1"]),
    (6, 1, ["100-hr-2"]),  # a full queue cannot crowd the weekly retry out
])
def test_a_recorded_refusal_is_asked_weekly_and_first(tmp_path, monkeypatch, day, max_bills, asked):
    import spicy_regs.transforms.enrich_bill_subjects as module

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 2 + day, 21, tzinfo=tz)  # 2026-11-02 is a Monday

    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setitem(module.RECORDED_REFUSALS, "100-hr-2", "fixture refusal")
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("100-hr-2"), _bill("100-hr-1")])
    fetcher = _StubFetcher({"100-hr-1": BillSubjects("Health", (), CARRIER_API)})
    messages: list[str] = []
    sink = logger.add(messages.append, level="INFO", format="{message}")
    try:
        out = enrich_bill_subjects(tmp_path, fetcher=fetcher, max_bills=max_bills)
    finally:
        logger.remove(sink)
    assert fetcher.asked == asked
    assert set(_rows(out)) == ({"100-hr-1"} & set(asked))  # a refusal never becomes a row
    assert any("1 recorded refusals" in m and "100-hr-2 (fixture refusal)" in m for m in messages)


def test_a_failed_fetch_leaves_the_bill_un_enriched_for_the_next_run(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("100-hr-1"), _bill("100-hr-2")])
    out = enrich_bill_subjects(tmp_path, fetcher=_StubFetcher({"100-hr-1": BillSubjects("Health", (), CARRIER_API)}))
    assert set(_rows(out)) == {"100-hr-1"}
    recovered = _StubFetcher({"100-hr-2": BillSubjects("Taxation", (), CARRIER_API)})
    out = enrich_bill_subjects(tmp_path, fetcher=recovered)
    assert recovered.asked == ["100-hr-2"]
    assert set(_rows(out)) == {"100-hr-1", "100-hr-2"}


def test_a_definitive_miss_is_recorded_and_not_asked_again(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("100-hr-1")])
    enrich_bill_subjects(tmp_path, fetcher=_StubFetcher({"100-hr-1": BillSubjects(None, (), CARRIER_API, held=False)}))
    second = _StubFetcher({"100-hr-1": BillSubjects("Health", (), CARRIER_API)})
    enrich_bill_subjects(tmp_path, fetcher=second)
    assert second.asked == []


def test_a_bill_with_a_prior_row_is_selected_again_only_when_the_family_filled_it(tmp_path):
    """An empty answer is final for the zip and API routes; only the family's own row is re-read every run."""
    _write_prior(
        tmp_path / "_bill_subjects_prior.parquet",
        [
            _shape("118-hr-1", None, (), CARRIER_API, "2026-08-22T00:00:00+00:00"),
            _shape("118-hr-2", None, (), CARRIER_BULKDATA, "2026-08-22T00:00:00+00:00"),
            _shape("100-hr-1", None, (), CARRIER_API, "2026-08-22T00:00:00+00:00"),
        ],
    )
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [_bill("118-hr-1"), _bill("118-hr-2", family=("Health", [])), _bill("100-hr-1"), _bill("100-hr-2")],
    )
    pending = _pending_bills(
        tmp_path / "congress_bills.parquet", tmp_path / "_bill_subjects_prior.parquet", have_prior=True
    )
    assert [(bill.bill_id, bill.prior is not None) for bill in pending] == [("118-hr-2", True), ("100-hr-2", False)]


def test_bills_below_the_api_floor_or_without_a_number_are_never_selected(tmp_path):
    bills = tmp_path / "congress_bills.parquet"
    _write_bills(bills, [_bill("92-hr-1"), _bill("93-hr-1"), dict(_bill("118-hr-2"), bill_number=None)])
    pending = _pending_bills(bills, tmp_path / "_absent_prior.parquet", have_prior=False)
    assert [bill.bill_id for bill in pending] == ["93-hr-1"]


def test_a_missing_bill_table_fails_loudly(tmp_path):
    with pytest.raises(RuntimeError, match="congress_bills.parquet"):
        enrich_bill_subjects(tmp_path, fetcher=_StubFetcher())


def test_a_run_cannot_outspend_the_documented_hourly_budget_or_the_job():
    """Congress.gov states 5,000 requests an hour; the rollup workflow kills a job at 30 minutes."""
    assert 3600 / DELAY_SECONDS < API_HOURLY_BUDGET
    assert MAX_API_BILLS_PER_RUN < API_HOURLY_BUDGET
    assert DEADLINE_SECONDS <= 30 * 60 - 10 * 60  # merge, upload and one in-flight request keep ten minutes


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_the_runs_own_fetcher_carries_the_run_deadline(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_GOV_API_KEY", "fixture-key")
    built = []

    class Recording(_StubFetcher):
        def __init__(self, **kwargs):
            super().__init__()
            built.append(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    # The transforms facade exports a function under this module's name, so patch the module object.
    module = importlib.import_module("spicy_regs.transforms.enrich_bill_subjects")
    monkeypatch.setattr(module, "BillSubjectsFetcher", Recording)
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("100-hr-1")])
    clock = _Clock()
    clock.now = 50.0
    enrich_bill_subjects(tmp_path, deadline_seconds=1_000, clock=clock)
    assert [(kwargs["deadline"], kwargs["clock"]) for kwargs in built] == [(1_050.0, clock)]


def test_a_folder_not_started_before_the_deadline_is_left_for_the_next_run(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("117-hr-1"), _bill("118-hr-1")])
    zips = {(congress, "hr"): _zip(congress, {1: _BILLSTATUS}) for congress in (117, 118)}
    clock = _Clock()

    class Slow(_Folders):
        def __call__(self, congress, bill_type):
            clock.now += 400
            return super().__call__(congress, bill_type)

    first = Slow(zips)
    out = enrich_bill_subjects(tmp_path, deadline_seconds=300, read_folder=first, fetcher=_StubFetcher(), clock=clock)
    assert first.read == [(118, "hr")]
    assert set(_rows(out)) == {"118-hr-1"}
    clock.now = 0.0
    second = Slow(zips)
    out = enrich_bill_subjects(tmp_path, deadline_seconds=300, read_folder=second, fetcher=_StubFetcher(), clock=clock)
    assert second.read == [(117, "hr")]
    assert set(_rows(out)) == {"117-hr-1", "118-hr-1"}


def test_the_api_loop_stops_at_its_deadline_and_the_next_run_resumes(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"100-hr-{n}") for n in range(1, 6)])
    clock = _Clock()

    class Slow(_StubFetcher):
        def subjects_for(self, congress, bill_type, bill_number):
            clock.now += 400  # a bill costs 400 s on this clock
            return super().subjects_for(congress, bill_type, bill_number)

    answers = {f"100-hr-{n}": BillSubjects("Health", (), CARRIER_API) for n in range(1, 6)}
    first = Slow(answers)
    out = enrich_bill_subjects(tmp_path, deadline_seconds=1_000, fetcher=first, clock=clock)
    assert len(first.asked) == 3  # started at 0, 400 and 800 s; none at 1,200
    assert len(_rows(out)) == 3
    clock.now = 0.0
    second = Slow(answers)
    out = enrich_bill_subjects(tmp_path, deadline_seconds=1_000, fetcher=second, clock=clock)
    assert len(second.asked) == 2 and set(second.asked).isdisjoint(first.asked)
    assert len(_rows(out)) == 5


# -- the Congress.gov fetch ---------------------------------------------------


@pytest.fixture
def timed_transport(monkeypatch):
    """The API fetcher over a MockTransport on a fake clock; each action is (seconds it takes, response kwargs)."""
    clock = _Clock()
    sleeps: list[float] = []

    def sleep(seconds):
        sleeps.append(round(seconds, 2))
        clock.now += seconds

    monkeypatch.setattr("spicy_regs.sources.bill_subjects.time.sleep", sleep)
    clients = []

    def create(*actions, deadline=None):
        calls = []

        def respond(request):
            calls.append(request)
            seconds, response = actions[min(len(calls), len(actions)) - 1]
            clock.now += seconds
            return httpx.Response(**response)

        client = httpx.Client(transport=httpx.MockTransport(respond))
        clients.append(client)
        fetcher = BillSubjectsFetcher(api_key=_KEY, client=client, deadline=deadline, clock=clock)
        return fetcher, calls, clock, sleeps

    yield create
    for client in clients:
        client.close()


def test_requests_are_paced_from_their_start_not_slept_after(timed_transport):
    """Measured 2026-09-23: a round trip takes 1.36 s, longer than the 0.75 s interval, so it costs no sleep."""
    empty = {"status_code": 200, "json": _page(0, [])}
    fetcher, _calls, _clock, sleeps = timed_transport((1.36, empty), (0.1, empty), (0.1, empty))
    for number in (1, 2, 3):
        assert fetcher.subjects_for("100", "hr", str(number)) is not None
    assert sleeps == [0.65]


def test_a_later_page_is_not_started_after_the_deadline(timed_transport):
    """A bill whose first page ends past the deadline is no answer, never a truncated list."""
    first = {"status_code": 200, "json": _page(400, [f"S{n}" for n in range(250)], "Taxation")}
    fetcher, calls, _clock, _sleeps = timed_transport((61.0, first), deadline=60.0)
    assert fetcher.subjects_for("100", "hr", "1") is None
    assert len(calls) == 1


def test_a_retry_is_not_started_after_the_deadline(timed_transport):
    fetcher, calls, clock, _sleeps = timed_transport((61.0, {"status_code": 503, "json": {}}), deadline=60.0)
    assert fetcher.subjects_for("100", "hr", "1") is None
    assert len(calls) == 1
    assert clock.now < 60.0 + 61.0 + 30  # one in-flight request and at most one backoff past the deadline


def test_the_api_fetcher_refuses_to_run_without_a_key(monkeypatch):
    _keyless(monkeypatch)
    with pytest.raises(ValueError, match="api.data.gov key"):
        BillSubjectsFetcher()


def _api_fetcher(monkeypatch, pages):
    fetcher = BillSubjectsFetcher(api_key="test", delay=0)
    calls: list[int] = []

    def fake_get_json(url, *, params):
        calls.append(params["offset"])
        return pages[params["offset"]]

    monkeypatch.setattr(fetcher, "_get_json", fake_get_json)
    return fetcher, calls


def _page(count, names, policy_area=None):
    subjects: dict[str, object] = {"legislativeSubjects": [{"name": name} for name in names]}
    if policy_area:
        subjects["policyArea"] = {"name": policy_area}
    return {"pagination": {"count": count}, "subjects": subjects}


def test_one_call_carries_both_fields(monkeypatch):
    """policyArea and legislativeSubjects arrive together — no second request."""
    fetcher, calls = _api_fetcher(monkeypatch, {0: _page(2, ["Medicare", "Drug safety"], "Health")})
    assert fetcher.subjects_for("100", "HR", "1") == BillSubjects("Health", ("Medicare", "Drug safety"), CARRIER_API)
    assert calls == [0]


def test_a_fat_bill_walks_offsets_until_the_count_is_met(monkeypatch):
    fetcher, calls = _api_fetcher(monkeypatch, {0: _page(3, ["A", "B"], "Taxation"), 2: _page(3, ["C"])})
    result = fetcher.subjects_for("100", "HR", "1")
    assert result == BillSubjects("Taxation", ("A", "B", "C"), CARRIER_API)
    assert calls == [0, 2]


def test_a_failed_later_page_publishes_nothing_rather_than_a_truncated_list(monkeypatch):
    fetcher, _ = _api_fetcher(monkeypatch, {0: _page(400, ["A"], "Taxation"), 1: None})
    assert fetcher.subjects_for("100", "HR", "1") is None


def test_the_page_cap_is_a_refusal_not_a_silent_stop(monkeypatch):
    pages = {offset: _page(1_001, [f"S{offset + n}" for n in range(250)]) for offset in (0, 250, 500, 750)}
    fetcher, calls = _api_fetcher(monkeypatch, pages)
    messages: list[str] = []
    sink = logger.add(messages.append, level="ERROR", format="{message}")
    try:
        assert fetcher.subjects_for("100", "hr", "1") is None
    finally:
        logger.remove(sink)
    assert calls == [0]  # the first page's stated count already decides the refusal
    assert any("100-hr-1" in m and "1001 subjects stated" in m for m in messages)


def test_a_list_that_fits_the_page_cap_is_walked_to_its_end(monkeypatch):
    pages = {offset: _page(1_000, [f"S{offset + n}" for n in range(250)]) for offset in (0, 250, 500, 750)}
    fetcher, calls = _api_fetcher(monkeypatch, pages)
    result = fetcher.subjects_for("100", "hr", "1")
    assert result is not None and len(result.subjects) == 1_000
    assert calls == [0, 250, 500, 750]


def test_an_empty_page_before_the_count_is_met_is_a_refusal(monkeypatch):
    fetcher, calls = _api_fetcher(monkeypatch, {0: _page(3, ["A"]), 1: _page(3, [])})
    assert fetcher.subjects_for("100", "hr", "1") is None
    assert calls == [0, 1]


_KEY = "fixture-congress-key-0123456789"


@pytest.fixture
def api_transport(monkeypatch):
    """The API carrier over a MockTransport of ``httpx.Response`` kwargs; the last repeats, backoff does not sleep."""
    monkeypatch.setattr("spicy_regs.sources.bill_subjects.time.sleep", lambda _seconds: None)
    clients = []

    def create(*actions):
        calls = []

        def respond(request):
            calls.append(request)
            return httpx.Response(**actions[min(len(calls), len(actions)) - 1])

        client = httpx.Client(transport=httpx.MockTransport(respond))
        clients.append(client)
        return BillSubjectsFetcher(api_key=_KEY, delay=0, client=client), calls

    yield create
    for client in clients:
        client.close()


def test_the_api_key_travels_only_in_the_header(api_transport):
    fetcher, calls = api_transport({"status_code": 200, "json": _page(1, ["Medicare"])})
    assert fetcher.subjects_for("100", "HR", "1") == BillSubjects(None, ("Medicare",), CARRIER_API)
    [request] = calls
    assert "api_key" not in request.url.params
    assert _KEY not in str(request.url)
    assert request.headers["X-Api-Key"] == _KEY


def test_a_404_is_a_definitive_miss(api_transport):
    fetcher, calls = api_transport({"status_code": 404, "json": {}})
    assert fetcher.subjects_for("100", "hr", "1") == BillSubjects(None, (), CARRIER_API, held=False)
    assert len(calls) == 1


@pytest.mark.parametrize("status", [401, 403])
def test_api_access_refusal_stops_without_an_empty_result_or_retry(api_transport, status):
    fetcher, calls = api_transport({"status_code": status, "json": {"error": "refused"}})
    with pytest.raises(CredentialRefusedError) as refused:
        fetcher.subjects_for("100", "hr", "1")
    assert len(calls) == 1
    assert _KEY not in str(refused.value)


def test_a_failed_request_logs_neither_the_key_nor_the_query(api_transport):
    """Checks the query string too: the key is header-only, so its absence alone cannot catch a URL in a log line."""
    fetcher, calls = api_transport({"status_code": 400, "json": {"error": "bad request"}})
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        assert fetcher.subjects_for("100", "hr", "1") is None
    finally:
        logger.remove(sink)
    assert len(messages) == len(calls) > 1
    assert all("api.congress.gov/v3/bill/100/hr/1/subjects" in m and "HTTP 400" in m for m in messages)
    assert not any(_KEY in m or "?" in m or "offset=" in m for m in messages)


def test_a_redirect_is_not_followed_so_the_key_header_stays_on_the_api_host(api_transport):
    fetcher, calls = api_transport({"status_code": 302, "headers": {"location": "https://elsewhere.example/collect"}})
    assert fetcher.subjects_for("100", "hr", "1") is None
    assert {request.url.host for request in calls} == {"api.congress.gov"}


def test_the_job_outlasts_the_enrichment_deadline_by_the_native_restore_and_write():
    """The job took 22 to 27 of its 30 minutes before receipts; restoring congress_bills alone is 5.5 more."""
    from pathlib import Path

    import yaml

    from spicy_regs.transforms.enrich_bill_subjects import DEADLINE_SECONDS

    workflow = Path(__file__).resolve().parents[1] / ".github/workflows/rollup-bill-subjects.yml"
    job_seconds = yaml.safe_load(workflow.read_text())["jobs"]["run"]["with"]["timeout_minutes"] * 60
    assert job_seconds >= DEADLINE_SECONDS + 30 * 60
