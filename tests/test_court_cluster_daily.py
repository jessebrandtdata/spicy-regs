"""The daily court catch-up keeps its place in the published table and stays inside the account's quota (ss-jran)."""

import bz2
import hashlib
import json
import os
import re
import shutil
from contextlib import nullcontext
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import cast

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import LocalMemberSource, canonical_json_bytes, iter_member_descriptors

from spicy_regs.generations import verify_generation
from spicy_regs.local_data import file_state_from_stat
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import courtlistener, courtlistener_resume, publication
from spicy_regs.transforms.build_court_opinion_clusters import EXPORT_EVENT
from tests.test_court_cluster_delta import (  # noqa: F401
    DAY_SPENT, NOON, SOON, delta, local_cli, page, selected, transport, usage,
)

EDITION = date(2026, 6, 30)


class Search:
    """A publisher holding indexed cluster ids: it answers an id range newest first, ``PAGE_SIZE`` a page.

    ``answers`` replaces the reply to the numbered attempts it names (1 is the first request of the test), with
    a response or a function of the request. ``arrivals`` adds newly created ids as a numbered attempt arrives.
    """

    def __init__(self, ids, answers=None, arrivals=None):
        self.ids = sorted(ids, reverse=True)
        self.answers = answers or {}
        self.arrivals = arrivals or {}
        self.asked: list[str] = []

    def __call__(self, request):
        params = request.url.params
        self.asked.append(params["q"] + (" +" + params["cursor"] if "cursor" in params else ""))
        self.ids = sorted({*self.ids, *self.arrivals.get(len(self.asked), ())}, reverse=True)
        if len(self.asked) in self.answers:
            answer = self.answers[len(self.asked)]
            return answer(request) if callable(answer) else answer
        asked = re.fullmatch(r"cluster_id:\[(\d+) TO (\d+|\*)\]", params["q"])
        assert asked is not None, params["q"]
        low, high = asked.groups()
        held = [identity for identity in self.ids if identity >= int(low) and (high == "*" or identity <= int(high))]
        start = int(params.get("cursor", 0))
        more = start + delta.PAGE_SIZE < len(held)
        return httpx.Response(200, json=page(
            held[start:start + delta.PAGE_SIZE], total=len(held),
            next_url=str(request.url.copy_set_param("cursor", start + delta.PAGE_SIZE)) if more else None,
        ))


def refused(seconds):
    return httpx.Response(429, headers={"Retry-After": str(seconds)}, json={"detail": "throttled"})


def overcounted(request):
    """A first page, of the window's two lowest ids, that claims nine clusters: more than any window here has ids."""
    low = int(request.url.params["q"].removeprefix("cluster_id:[").split(" TO ")[0])
    return httpx.Response(200, json=page([low + 1, low], total=9,
                                         next_url=str(request.url.copy_set_param("cursor", 2))))


#: Ten clusters above the published id 2, so ids 3 to 10 may be read (two ids are left below the proven 12).
TEN = [4, 6, 7, 8, 9, 11, 12, 13, 14, 15]


def events(evidence, name):
    lines = (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()
    return [event for event in map(json.loads, lines) if event["event"] == name]


@pytest.fixture
def daily(selected, monkeypatch):  # noqa: F811
    """The real daily route over a two-row published prior (ids 2 and 1); only the publisher and R2 are fakes.

    Two results a page, two pages a window, four requests a day and a two-id margin below the newest proven id
    keep every expectation countable by hand. The 75-second pacing is real; the clock it sleeps on is not, and
    ``sleeps`` records each wait.
    """
    from spicy_docs.transport import capture

    work, kwargs = selected
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.invalid")
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "fixture-token")
    for name, value in (("PAGE_SIZE", 2), ("WINDOW_PAGES", 2), ("DAILY_REQUESTS", 4), ("FRONTIER_MARGIN", 2)):
        monkeypatch.setattr(delta, name, value)
    state = SimpleNamespace(
        work=work, evidence=kwargs["evidence"], usage=usage(), fetched=[], sleeps=[], clock=0.0,
        generations={kwargs["expected_prior"].removeprefix("sha256:"): kwargs["prior_directory"]},
    )

    def sleep(seconds):
        state.sleeps.append(seconds)
        state.clock += seconds

    clock = SimpleNamespace(monotonic=lambda: state.clock, sleep=sleep)
    monkeypatch.setattr(capture, "time", clock)
    monkeypatch.setattr(delta, "time", clock)
    monkeypatch.setattr(courtlistener_resume, "read_usage", lambda token, transport: json.dumps(state.usage).encode())

    def fetch(base, member, path, *args, **options):
        digest, key = member.path.split("/")[-2:]
        shutil.copyfile(state.generations[digest] / key, path)
        state.fetched.append(key)
        with path.open("rb") as stream:
            sha256 = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        return publication.DownloadedMember(sha256, path.stat().st_size, file_state_from_stat(os.stat(path)))

    monkeypatch.setattr(publication, "fetch_member", fetch)

    def serve(search):
        monkeypatch.setattr(delta, "CourtListenerOpinionSearchReader", courtlistener.CourtListenerOpinionSearchReader)
        monkeypatch.setattr(delta, "_live_transport", lambda: httpx.MockTransport(search))
        return search

    state.serve = serve
    state.run = lambda above=2, **options: delta.daily_catch_up(
        work, evidence=state.evidence, source_edition=EDITION, above=above, **options)
    return state


def test_a_day_appends_whole_id_windows_without_restoring_the_prior(daily, monkeypatch):
    search = daily.serve(Search(TEN))
    monkeypatch.setattr("spicy_regs.court_receipts.restore_processing_input",
                        lambda *args, **options: pytest.fail("The daily append must not restore the prior"))
    candidate, fault = daily.run()
    # One request states the backlog (10 above id 2); the other three are the pages of two windows of four ids.
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 6]", "cluster_id:[7 TO 10]", "cluster_id:[7 TO 10] +2"]
    assert fault is None
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "6", "4", "9", "8", "7"]
    (stated,) = events(daily.evidence, delta.CATCH_UP_EVENT)
    # ``through`` is the end of the last whole window. Id 10 lists nothing, and ten clusters above 2 prove it assigned.
    assert {name: stated[name] for name in ("source_edition", "above", "through", "declared_backlog", "clusters",
                                             "windows", "requests", "max_requests")} == {
        "source_edition": "2026-06-30", "above": 2, "through": 10, "declared_backlog": 10, "clusters": 5,
        "windows": 2, "requests": 4, "max_requests": 4}
    assert daily.fetched == [delta.OUTPUT, "etl_receipts.parquet"]
    assert not list((daily.work / ".catchup-prior").iterdir())  # The downloaded prior is released once appended.


def test_exhausted_quota_is_nothing_to_do_before_any_search_or_download(daily, monkeypatch):
    search = daily.serve(Search(TEN))
    daily.usage = usage(*DAY_SPENT, ("125/day", 0, 86400))  # the helper's next slot frees at 15:03:52 on 2026-11-08
    monkeypatch.setattr(courtlistener_resume, "_now", lambda: datetime(2026, 11, 8, 12, tzinfo=UTC))
    with pytest.raises(delta.NothingToCatchUp, match="quota leaves 0 Search requests today"):
        daily.run()
    assert search.asked == [] and daily.fetched == []
    (preflight,) = events(daily.evidence, "quota-preflight")
    assert (preflight["requested_live"], preflight["allowed_live"], preflight["not_before"]) == (4, 0, None)


@pytest.mark.parametrize(("ids", "listed"), [([1, 2], 0), ([3, 4], 2)], ids=["none", "only-the-newest"])
def test_no_readable_backlog_is_nothing_to_do_after_one_request_and_no_download(daily, ids, listed):
    search = daily.serve(Search(ids))
    with pytest.raises(delta.NothingToCatchUp, match=f"lists {listed} clusters above id 2; the newest 2 wait"):
        daily.run()
    assert search.asked == ["cluster_id:[3 TO *]"] and daily.fetched == []


def test_no_window_reaches_past_the_ids_the_backlog_proves_assigned(daily):
    # Four clusters above id 2 prove ids to 6 assigned, wherever they are; two ids stay unread below that.
    search = daily.serve(Search([3, 4, 5, 6]))
    candidate, _ = daily.run()
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 4]"]
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "4", "3"]
    (stated,) = events(daily.evidence, delta.CATCH_UP_EVENT)
    assert (stated["through"], stated["declared_backlog"], stated["clusters"]) == (4, 4, 2)


def test_a_refused_attempt_is_waited_out_and_counted_against_the_day(daily):
    search = daily.serve(Search([4, 6, 7, 9, 10, 11, 12, 13, 14, 15], answers={2: refused(30)}))
    candidate, fault = daily.run()
    # The refused attempt is one of the day's four, so the second window has one page: ids 7 and 8.
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 6]", "cluster_id:[3 TO 6]", "cluster_id:[7 TO 8]"]
    # 75 s after the backlog request; the publisher's 30 s; the rest of the 75 s since the refused attempt; 75 s
    # between the two windows.
    assert daily.sleeps == [75.0, 30.0, 45.0, 75.0]
    assert fault is None
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "6", "4", "7"]
    (stated,) = events(daily.evidence, delta.CATCH_UP_EVENT)
    assert (stated["requests"], stated["through"]) == (4, 8)


@pytest.mark.parametrize(("answer", "is_fault"), [(refused(80_000), False), (httpx.Response(500), True),
                                                  (overcounted, True)],
                         ids=["quota-ran-out", "publisher-fault", "more-clusters-than-ids"])
def test_a_window_that_cannot_finish_publishes_the_whole_windows_before_it(daily, monkeypatch, answer, is_fault):
    monkeypatch.setattr(courtlistener, "_MAX_REQUESTS_PER_PAGE", 1)
    monkeypatch.setattr(delta, "DAILY_REQUESTS", 6)  # attempts to spare: the stop is the answer's, not the day's end
    at = 3 if answer is overcounted else 4  # the second window's first page, or its second
    search = daily.serve(Search(TEN, answers={at: answer}))
    candidate, fault = daily.run()
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 6]", "cluster_id:[7 TO 10]",
                            "cluster_id:[7 TO 10] +2"][:at]
    # The second window's first page is dropped with it: Search orders by filing date, not id.
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "6", "4"]
    assert (fault is not None) is is_fault
    if answer is overcounted:
        assert str(fault) == "CourtListener lists 9 clusters in the 4 ids above 6"
    (stated,) = events(daily.evidence, delta.CATCH_UP_EVENT)
    assert (stated["through"], stated["clusters"], stated["declared_backlog"]) == (6, 2, 10)


@pytest.mark.parametrize(("answer", "error", "message"), [
    (refused(80_000), delta.NothingToCatchUp, "requests ended before a window above id 2 listed a cluster"),
    (httpx.Response(500), ValueError, "incomplete"),
    (overcounted, ValueError, "lists 9 clusters in the 4 ids above 2"),
], ids=["quota-ran-out", "publisher-fault", "more-clusters-than-ids"])
def test_a_first_window_that_cannot_finish_appends_nothing(daily, monkeypatch, answer, error, message):
    monkeypatch.setattr(courtlistener, "_MAX_REQUESTS_PER_PAGE", 1)
    daily.serve(Search(TEN, answers={2: answer}))
    monkeypatch.setattr(delta, "_append_native", lambda *args, **options: pytest.fail("Nothing whole to append"))
    with pytest.raises(error, match=message) as raised:
        daily.run()
    assert raised.type is error or error is ValueError
    assert not events(daily.evidence, delta.CATCH_UP_EVENT)


def test_waiting_time_running_out_ends_the_day_without_a_fault(daily, monkeypatch):
    monkeypatch.setattr(delta, "DAILY_REQUESTS", 6)
    monkeypatch.setattr(delta, "RETRY_WAIT_SECONDS", 0.0)  # no spare waiting: the 200 s below comes out of the pacing
    search = daily.serve(Search(TEN, answers={2: refused(200)}))
    candidate, fault = daily.run()
    # The second window's first page is answered 350 s in; its second would be paced past the 375 s the day has.
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 6]", "cluster_id:[3 TO 6]", "cluster_id:[7 TO 10]"]
    assert daily.sleeps == [75.0, 200.0, 75.0]
    assert fault is None
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "6", "4"]


def test_the_backlog_request_and_the_walk_share_one_retry_after_allowance(daily, monkeypatch):
    monkeypatch.setattr(delta, "DAILY_REQUESTS", 6)
    search = daily.serve(Search(TEN, answers={1: refused(1000), 4: refused(1000)}))
    candidate, fault = daily.run()
    # 1,000 of the run's 1,800 seconds went on the backlog request, so the second 1,000 cannot be waited out.
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO *]", "cluster_id:[3 TO 6]", "cluster_id:[7 TO 10]"]
    assert daily.sleeps == [1000.0, 75.0, 75.0]
    assert fault is None
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "6", "4"]


def test_an_empty_stretch_widens_the_window_and_a_crowded_one_narrows_it(daily, monkeypatch):
    monkeypatch.setattr(delta, "WINDOW_PAGES", 1)
    monkeypatch.setattr(delta, "DAILY_REQUESTS", 10)
    search = daily.serve(Search(range(9, 21)))  # twelve clusters above id 2: ids to 12 may be read
    candidate, fault = daily.run()
    # Two ids a window. Nothing in 3-4, so 5-8 is tried; 9-16 is cut to the readable 9-12, which lists four, more
    # than one page, and is left after its first; 9-10 and 11-12 fit.
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 4]", "cluster_id:[5 TO 8]", "cluster_id:[9 TO 12]",
                            "cluster_id:[9 TO 10]", "cluster_id:[11 TO 12]"]
    assert fault is None
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "10", "9", "12", "11"]
    (stated,) = events(daily.evidence, delta.CATCH_UP_EVENT)
    assert (stated["through"], stated["requests"], stated["windows"]) == (12, 6, 4)


def test_a_backlog_wholly_above_the_ids_it_proves_fails_the_run_instead_of_repeating_it(daily):
    # Eight clusters above id 2, all far above id 8: every readable id was read and lists nothing.
    search = daily.serve(Search(range(1000, 1008)))
    with pytest.raises(ValueError, match="lists 8 clusters above id 2 but none in ids 3 to 8") as raised:
        daily.run()
    assert raised.type is ValueError  # not NothingToCatchUp: tomorrow would ask the same three requests
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 6]", "cluster_id:[7 TO 8]"]
    assert not events(daily.evidence, delta.CATCH_UP_EVENT)


def test_a_table_above_the_id_its_journal_states_refuses_before_any_window(daily):
    search = daily.serve(Search(TEN))
    with pytest.raises(ValueError, match="holds a cluster above the id its journal says"):
        daily.run(above=1)  # the published table holds id 2
    assert search.asked == ["cluster_id:[2 TO *]"]


def test_a_run_keeping_step_with_yesterdays_waits_for_the_first_slot(daily, monkeypatch):
    monkeypatch.setattr(courtlistener_resume, "_now", lambda: SOON)
    daily.usage = usage(*DAY_SPENT, ("125/day", 5, 86400))  # too few for today's 4 and the docket job's 7
    search = daily.serve(Search(TEN))
    daily.run()
    assert daily.sleeps[0] == pytest.approx(232.762401)  # until the slot yesterday's first request leaves
    assert search.asked[:2] == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 6]"]


def test_a_dispatch_can_cap_the_days_requests(daily):
    search = daily.serve(Search(TEN))
    candidate, fault = daily.run(max_requests=2)
    assert search.asked == ["cluster_id:[3 TO *]", "cluster_id:[3 TO 4]"]
    assert pq.read_table(candidate)["cluster_id"].to_pylist() == ["2", "1", "4"]
    (stated,) = events(daily.evidence, delta.CATCH_UP_EVENT)
    assert (stated["max_requests"], stated["requests"], stated["through"]) == (2, 2, 4)


@pytest.mark.parametrize("cap", [1, 5, 0, "3", 2.0])
def test_a_cap_outside_two_to_the_daily_allowance_refuses_before_the_usage_read(daily, monkeypatch, cap):
    monkeypatch.setattr(courtlistener_resume, "read_usage", lambda *args: pytest.fail("usage read before the cap check"))
    with pytest.raises(ValueError, match="takes 2 to 4 requests"):
        daily.run(max_requests=cap)


@pytest.mark.parametrize(("stated", "expected"), [
    ({"catchup-selection": {"source_edition": "2026-09-30", "above": 10987420, "through": 10991197}},
     (date(2026, 9, 30), 10991197)),  # the generation published 2026-10-19
    ({"export-selection": {"source_edition": "2026-12-31", "through": 11200000}}, (date(2026, 12, 31), 11200000)),
    ({}, None),
    ({delta.CATCH_UP_EVENT: {"source_edition": "2026-09-30", "through": "10991197"}}, None),
    ({delta.CATCH_UP_EVENT: {"source_edition": "September", "through": 5}}, None),
])
def test_held_selection_reads_the_edition_and_floor_the_prior_journal_states(stated, expected):
    assert delta.held_selection(cast(CaptureEvidence, SimpleNamespace(inherited_event=stated.get))) == expected
    assert delta.held_selection(None) is None


@pytest.fixture
def hosted(local_cli, daily, monkeypatch):  # noqa: F811
    """The scheduled command, day after day: each run's prior is the generation the run before it admitted.

    The first prior is made the way the live one was (an explicit catch-up, ids 4 and 6 above the export's 2).
    """
    import spicy_docs.sources.courtlistener.bulk as bulk
    from spicy_regs.pipelines.rollups.court_opinion_clusters import CourtOpinionClustersRollup

    run_explicit, _, work, _ = local_cli
    monkeypatch.setattr(bulk, "list_bulk_dumps", lambda: [])
    daily.newest = EDITION
    monkeypatch.setattr(bulk, "latest_dump_date", lambda objects, dataset: daily.newest)
    held = {}

    def journal(base, pin, **options):
        for root in (work / "source-evidence").glob("*/artifact/artifact.json"):  # sealed runs only
            if json.loads(root.read_text())["artifactDigest"] == pin["artifactDigest"]:
                return (root.parent / "journal.jsonl").read_bytes()
        raise AssertionError("the prior generation's evidence was not retained")

    def publish():
        """Make the newest admitted generation the published one, as an upload would; return its cluster ids."""
        directory = max((work / "generations").iterdir(), key=lambda path: path.stat().st_mtime_ns)
        artifact = verify_generation(directory)
        entry = publication.generation_entry(artifact, iter_member_descriptors(artifact, LocalMemberSource(directory)))
        held["snapshot"] = publication.empty_index()
        held["snapshot"]["families"][delta.FAMILY] = entry
        held["root"] = (canonical_json_bytes(artifact.root), artifact.root)
        daily.generations[artifact.pin.artifact_digest.removeprefix("sha256:")] = directory
        return pq.read_table(directory / delta.OUTPUT)["cluster_id"].to_pylist()

    def run(search):
        daily.serve(search)
        CourtOpinionClustersRollup(output_dir=work, skip_upload=True).run()
        return search.asked

    transport(monkeypatch, page([6, 4]))
    run_explicit()
    monkeypatch.setattr(publication, "snapshot", lambda _: nullcontext(held["snapshot"]))
    monkeypatch.setattr(publication, "load_family_root", lambda *args: held["root"])
    monkeypatch.setattr(publication, "load_evidence_journal", journal)
    daily.published, daily.day = publish, run
    assert publish() == ["2", "1", "6", "4"]
    return daily


def test_each_day_resumes_above_the_published_table_and_never_rereads(hosted):
    # Day 1: six clusters above id 6 prove ids to 12 assigned, so 7 to 10 are read, in one window of two pages.
    # Cluster 13 is created while that window is being walked; it is above every id the run may read.
    day1 = Search(range(4, 13), arrivals={3: [13]})
    assert hosted.day(day1) == ["cluster_id:[7 TO *]", "cluster_id:[7 TO 10]", "cluster_id:[7 TO 10] +2"]
    assert hosted.published() == ["2", "1", "6", "4", "10", "9", "8", "7"]
    # Day 2 starts above 10, where day 1 ended: nothing at or below it is asked for again. Three more clusters
    # have been created, so ids to 14 are read, the late cluster 13 among them.
    assert hosted.day(Search(range(4, 17))) == ["cluster_id:[11 TO *]", "cluster_id:[11 TO 14]", "cluster_id:[11 TO 14] +2"]
    assert hosted.published() == ["2", "1", "6", "4", "10", "9", "8", "7", "14", "13", "12", "11"]
    # Day 3: only the two newest clusters are above 14. One request, a successful run, the same generation.
    generations = sorted((hosted.work / "generations").iterdir())
    assert hosted.day(Search(range(4, 17))) == ["cluster_id:[15 TO *]"]
    assert sorted((hosted.work / "generations").iterdir()) == generations and len(generations) == 3
    outcomes = [json.loads(path.read_text())["spec"]["outcome"]
                for path in (hosted.work / "source-evidence").glob("*/artifact/artifact.json")]
    assert sorted(outcomes) == ["build-complete", "build-complete", "build-complete", "no-op"]


def test_a_day_without_quota_succeeds_and_leaves_the_published_generation(hosted, monkeypatch):
    hosted.usage = usage(*DAY_SPENT, ("125/day", 0, 86400))
    monkeypatch.setattr(courtlistener_resume, "_now", lambda: NOON)
    generations = sorted((hosted.work / "generations").iterdir())
    assert hosted.day(Search(range(4, 17))) == []
    assert sorted((hosted.work / "generations").iterdir()) == generations
    assert hosted.fetched == []


def test_a_publisher_fault_after_a_whole_window_is_published_and_still_reported(hosted, monkeypatch):
    monkeypatch.setattr(courtlistener, "_MAX_REQUESTS_PER_PAGE", 1)
    monkeypatch.setattr(delta, "DAILY_REQUESTS", 6)
    search = Search(range(4, 17), answers={4: httpx.Response(500)})  # ten clusters above id 6: ids to 14 are read
    with pytest.raises(ValueError, match="incomplete"):
        hosted.day(search)
    assert search.asked == ["cluster_id:[7 TO *]", "cluster_id:[7 TO 10]", "cluster_id:[7 TO 10] +2", "cluster_id:[11 TO 14]"]
    assert hosted.published() == ["2", "1", "6", "4", "10", "9", "8", "7"]


def test_a_newer_export_is_rebuilt_from_bulk_and_spends_no_quota(hosted, monkeypatch):
    from spicy_regs.pipelines.rollups import court_opinion_clusters as rollup

    hosted.newest = date(2026, 9, 30)
    built = []
    monkeypatch.setattr(rollup, "build_court_opinion_clusters", lambda output_dir, **options: built.append(options))
    monkeypatch.setattr(delta, "daily_catch_up", lambda *args, **options: pytest.fail("a new export is rebuilt"))
    monkeypatch.setattr(courtlistener_resume, "read_usage", lambda *args: pytest.fail("a rebuild reads no usage"))
    pipeline = rollup.CourtOpinionClustersRollup(output_dir=hosted.work, skip_upload=True)
    pipeline.source_evidence = CaptureEvidence(hosted.work, delta.FAMILY)
    with publication.snapshot("https://fixture.invalid") as index:
        pipeline.source_evidence.inherit(index, public_url="https://fixture.invalid")
    pipeline.build(hosted.work / "rebuild")
    assert built == [{"evidence": pipeline.source_evidence}]


def test_the_bulk_build_needs_no_token_makes_no_search_and_states_what_it_holds(tmp_path, monkeypatch):
    from spicy_regs.sources import r2
    from spicy_regs.transforms import build_court_opinion_clusters as build

    monkeypatch.setenv("SPICY_REGS_DISK_FLOOR_GIB", "0")
    monkeypatch.delenv("COURTLISTENER_API_TOKEN", raising=False)
    monkeypatch.setattr(r2, "download", lambda *args, **options: False)
    monkeypatch.setattr(courtlistener.CourtListenerOpinionSearchReader, "iter_records",
                        lambda self: pytest.fail("the bulk build searched"))
    dump = tmp_path / "opinion-clusters.csv.bz2"
    dump.write_bytes(bz2.compress(b'id,case_name\n"7","Seven"\n"12","Twelve"\n"9","Nine"\n'))
    evidence = CaptureEvidence(tmp_path / "work", delta.FAMILY)
    output = build.build_court_opinion_clusters(
        tmp_path, dump_date=date(2026, 9, 30), local_file=dump, skip_court_scope=True, evidence=evidence)
    assert pq.read_table(output)["cluster_id"].to_pylist() == ["7", "12", "9"]
    (stated,) = events(evidence, EXPORT_EVENT)
    assert (stated["source_edition"], stated["through"]) == ("2026-09-30", 12)
    prior = cast(CaptureEvidence, SimpleNamespace(inherited_event={EXPORT_EVENT: stated}.get))
    assert delta.held_selection(prior) == (date(2026, 9, 30), 12)
