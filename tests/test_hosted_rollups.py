"""Wiring tests for the rollups that publish the contract-hosted tables.

Nothing here runs a rollup: these assert the wiring that is easy to get subtly
wrong and that no other test would catch until a cron failed at 02:00 — the
declared outputs, the console scripts, the workflows, and the rule that every
hosted table has exactly one writer.
"""

from __future__ import annotations

import json
import re
import subprocess
import tomllib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs import data_dictionary as dd
from spicy_regs.pipelines.rollups.amendments import AmendmentsRollup
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
from spicy_regs.pipelines.rollups.committee_reports import CommitteeReportsRollup
from spicy_regs.pipelines.rollups.congress_index import (
    CommitteeMeetingsRollup,
    HouseCommunicationsRollup,
    NominationsRollup,
    RecordIssuesRollup,
    TreatiesRollup,
)
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.pipelines.rollups.print_citations import PrintCitationsRollup
from spicy_regs.pipelines.rollups.press_releases import PressReleasesRollup
from spicy_regs.pipelines.rollups.roll_call_votes import RollCallVotesRollup
from spicy_regs.pipelines.rollups.senate_expenditures import SenateExpendituresRollup
from spicy_regs.pipelines.rollups.scorecards import ScorecardsRollup
from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup

# A8/A9 (laws and rosters)
from spicy_regs.pipelines.rollups.committee_rosters import CommitteeRostersRollup
from spicy_regs.pipelines.rollups.fec_candidate_history import FecCandidateHistoryRollup
from spicy_regs.pipelines.rollups.fec_committee_history import FecCommitteeHistoryRollup
from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup
from spicy_regs.pipelines.rollups.gao_recommendations import GaoRecommendationsRollup
from spicy_regs.pipelines.rollups.gao_reports import GaoReportsRollup
from spicy_regs.pipelines.rollups.laws import LawsRollup
from spicy_regs.pipelines.rollups.native_legal_references import NativeLegalReferencesRollup
from spicy_regs.transforms.build_bill_family import (
    ARCHIVE_COLUMNS,
    ARCHIVES_TABLE,
    BACKFILL_COLUMNS,
    BACKFILL_WALK_COLUMNS,
    BACKFILL_WALKS_TABLE,
    BACKFILLS_TABLE,
    VOTE_REFERENCE_COLUMNS,
    VOTE_REFERENCES_TABLE,
)

#: The bill family's published outputs that are not contract tables, each with
#: the column tuple its transform writes.
from spicy_regs.navigation_read_outcomes import POLICIES as NAVIGATION_OUTCOME_POLICIES
from spicy_regs.transforms.committee_report_reads import READS_TABLE, READ_COLUMNS

BILL_OWN_TABLES = {
    ARCHIVES_TABLE: ARCHIVE_COLUMNS,
    VOTE_REFERENCES_TABLE: VOTE_REFERENCE_COLUMNS,
    BACKFILLS_TABLE: BACKFILL_COLUMNS,
    BACKFILL_WALKS_TABLE: BACKFILL_WALK_COLUMNS,
}

OWN_TABLES = BILL_OWN_TABLES | {READS_TABLE: READ_COLUMNS}

#: Contract tables the regulations ETL writes in its document and docket passes, published by the regulations
#: refresh's base families like ``documents`` itself, not by a standalone rollup (decisions 65-67).
ETL_TABLES = {"document_attributes", "docket_attributes", "comment_attributes"}

#: Tables of this repository's own that a hosted rollup publishes beside its contracts: gao-reports' typed product
#: table, whose schema is the transform's Arrow schema (``build_gao_reports._SCHEMA``), and print-citations' held-field
#: reads, rebuilt from its citation table's checkpoints (``held_citations.READS_COLUMNS``).

HOSTED_OWN_TABLES = {"gao_reports", "document_citation_reads", *NAVIGATION_OUTCOME_POLICIES}


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"

HOSTED_ROLLUPS = (
    BillFamilyRollup,
    PressReleasesRollup,
    AmendmentsRollup,
    RollCallVotesRollup,
    MembersRollup,
    CommitteeReportsRollup,
    # A8/A9 (laws and rosters)
    LawsRollup,
    CommitteeRostersRollup,
    # The Congress.gov index tables (gaps A5, A7, A10).
    HouseCommunicationsRollup,
    CommitteeMeetingsRollup,
    RecordIssuesRollup,
    TreatiesRollup,
    NominationsRollup,
    # The two PDF-only families (spicy-docs 0.23.0), one rollup per
    # acquisition pass.
    PrintCitationsRollup,
    SenateExpendituresRollup,
    # SpicyDocs 0.42.0's federal_register contract.
    FederalRegisterRollup,
    # SpicyDocs 0.43.0's fec_committee_history contract (decision 53).
    FecCommitteeHistoryRollup,
    # SpicyDocs 0.53.0's gao_recommendations contract, accumulated daily.
    GaoRecommendationsRollup,
    GaoReportsRollup,
    # The fec-publish branch's fec_candidate_history contract (decision 53's shape).
    FecCandidateHistoryRollup,
)

#: Rollups that host contracts but run on demand over an explicit retained-input manifest, so they have no schedule
#: or workflow: SpicyDocs 0.52.0's native legal-reference tables.
MANIFEST_ROLLUPS = (NativeLegalReferencesRollup,)

# Scorecards use a dedicated workflow with qualified weekly source selection.
SCORECARD_ROLLUPS = (ScorecardsRollup,)


def _declared_keys(rollup) -> tuple[str, ...]:
    return rollup.outputs or (rollup.output,)


@pytest.mark.parametrize(
    "rollup", HOSTED_ROLLUPS + MANIFEST_ROLLUPS + SCORECARD_ROLLUPS + (ScorecardAnalysisRollup,), ids=lambda r: r.name
)
def test_every_output_is_a_published_table(rollup):
    for key in _declared_keys(rollup):
        assert key.endswith(".parquet")
        assert key.removesuffix(".parquet") in dd.TABLES, key


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_an_ingesting_rollup_primes_no_base_table(rollup):
    """These fetch from a publisher, so nothing is primed and nothing is fatal on absence.

    This says only that the rollup declares no hard ``inputs``. It deliberately
    does **not** stand in for "reads nothing else": two of these do read a
    published table at merge time, and they satisfy an ``inputs == ()``
    assertion by construction while doing it. What makes those reads safe is
    checked in ``test_a_soft_input_is_an_ingest_output_its_writer_produces_first``,
    against the ``soft_inputs`` they must declare.
    """
    assert rollup.inputs == ()


def _scheduled_workflow(rollup) -> Path:
    """Resolve the scheduled producer, including shared refresh workflows."""
    command = f"run-rollup-{rollup.name}"
    matches = []
    for workflow in WORKFLOWS.glob("*.yml"):
        document = yaml.safe_load(workflow.read_text())
        if not document.get(True, {}).get("schedule"):
            continue
        if any(job.get("with", {}).get("command") == command for job in document["jobs"].values()):
            matches.append(workflow)
    assert len(matches) == 1, f"{command} must have one scheduled producer, found {matches}"
    return matches[0]


def _cron_minutes(workflow: Path) -> int:
    """The daily minute-of-day the workflow's schedule fires at."""
    crons = re.findall(r"cron: '(\d+) (\d+) \* \* \*'", workflow.read_text())
    assert len(crons) == 1, f"{workflow.name} declares {len(crons)} daily crons, expected 1"
    minute, hour = (int(part) for part in crons[0])
    return hour * 60 + minute


def _writers_of(remote_key: str) -> list[type[RollupPipeline]]:
    return [rollup for rollup in HOSTED_ROLLUPS if remote_key in (rollup.outputs or (rollup.output,))]


@pytest.mark.parametrize(
    ("rollup", "soft_input"),
    [(rollup, key) for rollup in HOSTED_ROLLUPS for key in rollup.soft_inputs],
    ids=lambda value: value if isinstance(value, str) else value.name,
)
def test_a_soft_input_is_an_ingest_output_its_writer_produces_first(rollup, soft_input):
    """The two things that make a merge-time read of a published table safe.

    The rollup contract (``pipelines/rollups/base.py``) permits reading an
    *ingest* rollup's output — one with no upstream dependency inside this
    repository, so a stale copy costs one cron's lag rather than racing — and
    forbids reading a derived rollup's. Both halves are checked here rather
    than asserted in prose: the writer ingests (declares no ``inputs``), and
    this rollup's cron **fires after** the writer's on the same day.

    Start order is all this asserts, and all a cron can give. It is not a
    guarantee that the writer has *finished*: the bill family's workflow
    budgets 180 minutes and these readers start 20 and 60 minutes after it, so
    a long family run is still writing when they begin, and they read the
    previous run's output. That is the ordering preference the rollup contract
    describes, not a barrier — which is exactly why both reads are
    ``soft_inputs``, tolerant of an absent or older table, rather than
    ``inputs``.
    """
    writers = _writers_of(soft_input)
    assert writers, f"{soft_input} is declared a soft input but no rollup publishes it"

    reader_at = _cron_minutes(_scheduled_workflow(rollup))
    earlier = []
    for writer in writers:
        assert writer.inputs == (), (
            f"{soft_input} is written by {writer.name}, which reads base tables — "
            "a derived rollup's output may not be a soft input"
        )
        writer_at = _cron_minutes(_scheduled_workflow(writer))
        if writer_at < reader_at:
            earlier.append((writer.name, reader_at - writer_at))

    assert earlier, (
        f"rollup-{rollup.name} reads {soft_input} but every writer of it "
        f"({[w.name for w in writers]}) fires later in the day, so it always reads yesterday's"
    )


def test_the_bill_family_declares_all_classified_subject_outputs():
    assert "bill_cosponsors.parquet" in BillFamilyRollup.outputs
    assert set(BillFamilyRollup.outputs) == {
        p.dataset + ".parquet" for p in BillFamilyRollup.receipt_policies if not p.receipt_only
    }
    assert BillFamilyRollup.outputs[0] == "congress_bills.parquet"
    assert {f"{name}.parquet" for name in BILL_OWN_TABLES if not dd.subject_policies()[name].receipt_only} <= set(
        BillFamilyRollup.outputs
    )
    # The property the freshness checker uses resolves to the first key.
    assert BillFamilyRollup(output_dir=None).output == "congress_bills.parquet"


@pytest.mark.parametrize("table", sorted(OWN_TABLES), ids=str)
def test_a_non_contract_output_is_declared_once_in_each_place_that_needs_it(table):
    """Their columns are this repository's: the transform owns the tuple it writes and the dictionary reads it.

    ``expected_schemas()`` imports the tuple, so ``DERIVED_SCHEMAS`` must not
    restate it; this checks the wiring (the right tuple on the right table).
    The MCP server lists the table literally because it must stay installable
    without the source-readers group. Same shape as
    ``test_mcp_server_tables_match_dictionary``.
    """
    from spicy_regs import mcp_server

    from spicy_regs.congress_subjects import INPUT_COLUMNS
    from spicy_regs.native_types import described_schema

    from spicy_regs.legislative_documents import field_registry

    source_columns = INPUT_COLUMNS.get(table)
    if source_columns is None:
        source_columns = [field["name"] for field in field_registry()[table]["fields"]]
    assert list(source_columns) == list(OWN_TABLES[table])
    policy = dd.subject_policies()[table]
    if not policy.receipt_only:
        assert dd.expected_schemas()[table] == described_schema(policy.subject_schema)
    assert (table in dd.TABLES) == (not dd.subject_policies()[table].receipt_only)
    assert (table in mcp_server.TABLES) == (not dd.subject_policies()[table].receipt_only)
    assert table not in dd.CONTRACT_TABLES, "it is this repository's own table, not a hosted contract"


def test_every_hosted_table_has_exactly_one_writer():
    """Two rollups writing one table would overwrite each other's columns.

    ``congress_bills`` was the exception until plan A1 retired its archive-wide
    list writer (decision 31): its run of 2026-09-23 replaced 3,044 BILLSTATUS
    ``update_date`` instants with same-day dates and 3,095 congress.gov page
    URLs with API resource URLs.
    """
    written: dict[str, list[str]] = {}
    for rollup in HOSTED_ROLLUPS + MANIFEST_ROLLUPS + SCORECARD_ROLLUPS:
        for key in _declared_keys(rollup):
            written.setdefault(key.removesuffix(".parquet"), []).append(rollup.name)

    assert set(written) == (
        (set(dd.CONTRACT_TABLES) - ETL_TABLES | set(OWN_TABLES) | HOSTED_OWN_TABLES) & set(dd.TABLES)
    ), "every contract, plus the bill family's own two tables, must be published by exactly one rollup"
    doubled = {table: names for table, names in written.items() if len(names) > 1}
    assert not doubled, f"tables with more than one writer: {doubled}"


def test_update_date_keeps_the_larger_value(tmp_path):
    """The contract's sentence for ``update_date``, as the merge applies it.

    Rows from the drift audit of 2026-09-23 (``drift-audit-2026-09-23/bill-family``):
    the retired list writer left ``119-hconres-100`` with the list route's
    same-day date and ``119-hr-10432`` with a later day than BILLSTATUS then
    stated. A BILLSTATUS re-read restores the first's instant, which sorts after
    the date; a re-read stating the older stamp never moves the second back.
    Outside the two shapes the table holds, VARCHAR order is not time order, so
    a ``9999`` sentinel or a spaced instant in the prior loses to the fresh value
    rather than pinning the row forever.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    contract = TABLE_CONTRACTS["congress_bills"]

    def row(bill, update_date):
        return {c: None for c in contract.columns} | {"bill_id": bill, "congress": "119", "update_date": update_date}

    schema = pa.schema([(c, pa.string()) for c in contract.columns])
    prior = [
        row("119-hconres-100", "2026-09-19"),
        row("119-hr-10432", "2026-09-22"),
        row("119-hr-1", "2026-05-08"),
        row("119-hr-2", "9999-12-31"),
        row("119-hr-3", "2026-09-22 23:35:29"),
    ]
    pq.write_table(pa.Table.from_pylist(prior, schema=schema), prior_scratch_path(tmp_path, "congress_bills"))
    fresh = [
        row("119-hconres-100", "2026-09-19T23:35:29Z"),
        row("119-hr-10432", "2026-09-18T05:23:22Z"),
        row("119-hr-2", "2026-09-20T01:00:00Z"),
        row("119-hr-3", "2026-09-20T01:00:00Z"),
    ]
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=lambda *_: False, prior_present=True)
    got = {r["bill_id"]: r["update_date"] for r in pq.read_table(out).to_pylist()}
    assert got == {
        "119-hconres-100": "2026-09-19T23:35:29Z",
        "119-hr-10432": "2026-09-22",
        "119-hr-1": "2026-05-08",
        "119-hr-2": "2026-09-20T01:00:00Z",
        "119-hr-3": "2026-09-20T01:00:00Z",
    }


def test_url_source_follows_the_url_the_merge_keeps(tmp_path):
    """Decision 1: an inherited url carries the label ``inherited``; a stated one carries its writer's."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    contract = TABLE_CONTRACTS["congress_bills"]

    def row(bill, **values):
        base = {c: None for c in contract.columns}
        number = bill.split("-")[-1]
        base.update(bill_id=bill, congress="118", bill_type="hr", bill_number=number, update_date="2026-01-01")
        return base | values

    api = "https://api.congress.gov/v3/bill/118/hr/{}?format=json"
    prior = [
        row("118-hr-1", url=api.format(1), url_source="congress_api_list"),  # fresh states none: inherited
        row("118-hr-2", url=api.format(2), url_source="congress_api_list"),  # fresh states its own
        row("118-hr-3", url=api.format(3), url_source="congress_api_list"),  # untouched: keeps its label
        row("118-hr-4", url=api.format(4)),  # predates the label: stays NULL when untouched
    ]
    schema = pa.schema([(c, pa.string()) for c in contract.columns])
    pq.write_table(pa.Table.from_pylist(prior, schema=schema), prior_scratch_path(tmp_path, "congress_bills"))
    page = "https://www.congress.gov/bill/118th-congress/house-bill/2"
    fresh = [
        row("118-hr-1", update_date="2026-02-01"),
        row("118-hr-2", update_date="2026-02-01", url=page, url_source="billstatus"),
        row("118-hr-5", update_date="2026-02-01"),  # no url anywhere: no label
    ]
    out = merge_contract_table(tmp_path, "congress_bills", fresh, download_prior=lambda *_: False, prior_present=True)
    got = {r["bill_id"]: (r["url"], r["url_source"]) for r in pq.read_table(out).to_pylist()}
    assert got == {
        "118-hr-1": (api.format(1), "inherited"),
        "118-hr-2": (page, "billstatus"),
        "118-hr-3": (api.format(3), "congress_api_list"),
        "118-hr-4": (api.format(4), None),
        "118-hr-5": (None, None),
    }


def test_only_congress_bills_merges_column_wise():
    """Coalescing is for rows from sources stating different columns; elsewhere a NULL is a value."""
    from spicy_regs.transforms.table_merge import COALESCED_TABLES

    assert COALESCED_TABLES == {"congress_bills"}


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_each_rollup_has_a_console_script_and_a_workflow(rollup):
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    command = f"run-rollup-{rollup.name}"
    assert command in scripts, f"{command} is not registered in pyproject.toml"

    workflow = WORKFLOWS / f"rollup-{rollup.name}.yml"
    assert workflow.exists(), f"missing {workflow.name}"
    document = yaml.safe_load(workflow.read_text())
    assert document["jobs"]["run"]["with"]["command"] == command
    assert document["jobs"]["run"]["uses"] == "./.github/workflows/_rollup.yml"


def test_scorecard_commands_are_registered_and_inputs_remain_explicit():
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    for rollup, module in ((ScorecardsRollup, "scorecards"), (ScorecardAnalysisRollup, "scorecard_analysis")):
        assert scripts[f"run-rollup-{rollup.name}"] == f"spicy_regs.pipelines.rollups.{module}:app"
    assert ScorecardsRollup.inputs == ()
    assert set(ScorecardAnalysisRollup.inputs) == {
        "scorecards.parquet",
        "scorecard_members.parquet",
        "scorecard_items.parquet",
        "scorecard_member_item_results.parquet",
        "scorecard_metric_items.parquet",
        "scorecard_methodologies.parquet",
        "bill_cosponsors.parquet",
        "members.parquet",
        "member_terms.parquet",
        "member_votes.parquet",
        "congress_bills.parquet",
        "amendments.parquet",
        "roll_call_votes.parquet",
    }


def test_scorecard_schedule_selects_only_qualified_weekly_publishers():
    from spicy_regs.scorecards.registry import load_registry, select_sources

    workflow = yaml.safe_load((WORKFLOWS / "rollup-scorecards.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers) == {"workflow_dispatch", "schedule"}
    assert triggers["schedule"] == [{"cron": "15 6 * * 1"}]
    selected = select_sources(load_registry())
    assert {source.publisher_id for source in selected} == {"lcv", "afscme"}
    proof = json.loads((REPO_ROOT / "docs/research/scorecards/work/integration/live_refresh_20261004.json").read_text())
    scopes = {row["publisher_id"]: row for row in proof["scopes"]}
    assert set(scopes) == {source.publisher_id for source in selected}
    for source in selected:
        assert source.cadence == "weekly"
        assert source.historical_backfill is False
        assert source.evidence_policy == "hash_only"
        assert source.qualification_id == scopes[source.publisher_id]["qualification_id"]
        assert scopes[source.publisher_id]["completeness_status"] == "complete"
        assert scopes[source.publisher_id]["edition"]["is_current"] is True


def test_scorecard_workflow_preserves_manual_defaults_and_bounded_publication():
    workflow = yaml.safe_load((WORKFLOWS / "rollup-scorecards.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    defaults = {key: value["default"] for key, value in triggers["workflow_dispatch"]["inputs"].items()}
    assert defaults == {
        "skip_upload": True,
        "publishers": "",
        "editions": "",
        "zyte_publishers": "",
        "zyte_browser_publishers": "",
        "historical_backfill": False,
        "force": False,
    }
    assert workflow["concurrency"] == {"group": "scorecards-publication", "cancel-in-progress": False}
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["run"]
    assert job["runs-on"] == "ubuntu-latest"
    assert type(job["timeout-minutes"]) is int
    assert 0 < job["timeout-minutes"] <= 180
    steps = {step.get("name"): step for step in job["steps"] if "name" in step}
    build = steps["Build or publish scorecard family"]
    assert (
        build["env"]["SCORECARD_SKIP_UPLOAD"]
        == "${{ github.event_name == 'schedule' && 'false' || inputs.skip_upload }}"
    )
    assert build["env"]["SCORECARD_PUBLISHERS"] == "${{ inputs.publishers }}"
    assert build["env"]["SCORECARD_HISTORY"] == "${{ inputs.historical_backfill }}"
    assert build["env"]["SCORECARD_FORCE"] == "${{ inputs.force }}"
    assert steps["Install pinned dependencies"]["run"] == "uv sync --frozen"
    audit = steps["Retain invocation and permitted evidence"]
    assert audit["if"] == "always()"
    assert audit["with"]["path"].splitlines() == ["output/invocation.json", "output/source-evidence/"]
    outputs = steps["Retain build-only outputs and failures"]
    # Scheduled success publishes; failed runs and manual build-only runs retain outputs.
    assert outputs["if"] == "failure() || (success() && inputs.skip_upload)"
    assert outputs["with"]["path"] == "output/"
    for artifact in (audit, outputs):
        assert artifact["uses"] == "actions/upload-artifact@v4"
        assert artifact["with"]["retention-days"] == 30


@pytest.mark.parametrize(
    ("skip_upload", "selectors", "extra_args"),
    [
        ("true", {}, []),  # Manual dispatch defaults to a local build.
        ("false", {}, []),  # Scheduled refresh publishes the registry-selected sources.
        (
            "false",
            {
                "SCORECARD_PUBLISHERS": "lcv,afscme",
                "SCORECARD_EDITIONS": "lcv:2025,edition with spaces;$literal",
                "SCORECARD_ZYTE_PUBLISHERS": "lcv",
                "SCORECARD_ZYTE_BROWSER_PUBLISHERS": "ijm",
                "SCORECARD_HISTORY": "true",
                "SCORECARD_FORCE": "true",
            },
            [
                "--publisher",
                "lcv",
                "--publisher",
                "afscme",
                "--edition",
                "lcv:2025",
                "--edition",
                "edition with spaces;$literal",
                "--zyte-publisher",
                "lcv",
                "--zyte-browser-publisher",
                "ijm",
                "--historical-backfill",
                "--force",
            ],
        ),
    ],
    ids=["manual-build-default", "scheduled-publication", "explicit-manual-options"],
)
def test_scorecard_workflow_executes_quoted_arguments_without_running_rollup(skip_upload, selectors, extra_args):
    workflow = yaml.safe_load((WORKFLOWS / "rollup-scorecards.yml").read_text())
    build = next(
        step for step in workflow["jobs"]["run"]["steps"] if step.get("name") == "Build or publish scorecard family"
    )
    # A shell function records argv and prevents network, publication or environment changes.
    stub = "uv() { printf '%s\\n' \"$@\"; }\n"
    result = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", stub + build["run"]],
        env={
            "SCORECARD_SKIP_UPLOAD": skip_upload,
            "SCORECARD_PUBLISHERS": "",
            "SCORECARD_EDITIONS": "",
            "SCORECARD_ZYTE_PUBLISHERS": "",
            "SCORECARD_ZYTE_BROWSER_PUBLISHERS": "",
            "SCORECARD_HISTORY": "false",
            "SCORECARD_FORCE": "false",
            **selectors,
        },
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.stderr == ""
    assert result.stdout.splitlines() == [
        "run",
        "--frozen",
        "run-rollup-scorecards",
        "--output-dir",
        "output",
        "--skip-upload" if skip_upload == "true" else "--no-skip-upload",
        *extra_args,
    ]


@pytest.mark.parametrize("rollup", HOSTED_ROLLUPS, ids=lambda r: r.name)
def test_each_rollup_has_one_scheduled_producer(rollup):
    _scheduled_workflow(rollup)


def test_no_two_rollup_workflows_share_a_cron_minute():
    """Spacing keeps six jobs from contending for a runner at the same instant."""
    slots: dict[str, list[str]] = {}
    for workflow in WORKFLOWS.glob("rollup-*.yml"):
        for cron in re.findall(r"cron: '([^']+)'", workflow.read_text()):
            slots.setdefault(cron, []).append(workflow.name)
    new = {f"rollup-{rollup.name}.yml" for rollup in HOSTED_ROLLUPS}
    for cron, names in slots.items():
        if len(names) > 1 and new.intersection(names):
            pytest.fail(f"new rollup shares cron {cron!r} with {names}")


def test_the_reusable_workflow_declares_every_input_the_callers_pass():
    """A caller passing an input the reusable workflow has not declared fails at run time."""
    reusable = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())
    declared = set(reusable[True]["workflow_call"]["inputs"])
    for rollup in HOSTED_ROLLUPS:
        passed = set(yaml.safe_load((WORKFLOWS / f"rollup-{rollup.name}.yml").read_text())["jobs"]["run"]["with"])
        assert passed <= declared, f"rollup-{rollup.name}.yml passes undeclared {sorted(passed - declared)}"


def test_member_and_cosponsor_family_growth_is_explicit():
    assert MembersRollup.added_tables == ("member_party_affiliations.parquet",)
    # Run 41270152327 refused a generation holding this table before it was declared.
    assert "committee_report_cbo_letters.parquet" in CommitteeReportsRollup.added_tables
    assert "committee_report_cbo_letters.parquet" in CommitteeReportsRollup.outputs
    assert BillFamilyRollup.added_tables == (
        "bill_cosponsors.parquet",
        "bill_committee_activities.parquet",
        "cbo_feed_items.parquet",
        "bill_family_document_file_outcomes.parquet",
    )
    for rollup in (MembersRollup, BillFamilyRollup):
        assert set(rollup.added_tables) <= set(rollup.outputs)


def test_bulk_workflows_pass_the_required_mode_to_the_actual_process():
    reusable = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())
    assert reusable[True]["workflow_call"]["inputs"]["require_bulk"]["default"] is False
    step = next(step for step in reusable["jobs"]["rollup"]["steps"] if step.get("name") == "Run rollup")
    assert step["env"]["SPICY_REGS_REQUIRE_BULK"] == "${{ inputs.require_bulk && 'true' || 'false' }}"
    assert 'uv run --frozen "$COMMAND" "${ARGS[@]}"' in step["run"]
    for filename in ["rollup-lobbying-filings.yml", "rollup-bill-family.yml", "rollup-roll-call-votes.yml",
                     "rollup-member-vote-terms.yml", "rollup-court-citations.yml", "rollup-court-opinion-clusters.yml"]:
        workflow = yaml.safe_load((WORKFLOWS / filename).read_text())
        assert workflow["jobs"]["run"]["with"]["require_bulk"] is True
    refresh = yaml.safe_load((WORKFLOWS / "refresh-congress-vote-terms.yml").read_text())
    assert refresh["jobs"]["votes"]["with"]["require_bulk"] is True
    assert refresh["jobs"]["terms"]["with"]["require_bulk"] is True
    assert "require_bulk" not in refresh["jobs"]["members"]["with"]
    regulatory = yaml.safe_load((WORKFLOWS / "_regulations-refresh.yml").read_text())
    assert regulatory["jobs"]["base-families"]["with"]["require_bulk"] is True
    assert "require_bulk" not in regulatory["jobs"]["derived"]["with"]
    etl = yaml.safe_load((WORKFLOWS / "etl-new-pipeline.yml").read_text())
    run = next(step for step in etl["jobs"]["etl-new"]["steps"] if step.get("name") == "Run new pipeline")
    assert run["env"]["SPICY_REGS_REQUIRE_BULK"] == "true"
    converter = yaml.safe_load((WORKFLOWS / "convert-native-family.yml").read_text())
    assert converter["jobs"]["convert"]["env"]["SPICY_REGS_REQUIRE_BULK"] == "true"


def test_votes_refresh_waits_for_its_member_crosswalk_writer():
    jobs = yaml.safe_load((WORKFLOWS / "refresh-congress-vote-terms.yml").read_text())["jobs"]

    def predecessors(name):
        needs = jobs[name].get("needs", [])
        return {parent for dependency in needs for parent in ({dependency} | predecessors(dependency))}

    assert "members" in predecessors("votes")
    assert {"members", "votes"} <= predecessors("terms")


def test_votes_disk_probe_is_enabled_and_retained_before_audit_upload():
    workflow = yaml.safe_load((WORKFLOWS / "refresh-congress-vote-terms.yml").read_text())
    for job in ["votes", "terms"]:
        assert workflow["jobs"][job]["with"]["free_disk"] is True
    assert workflow["jobs"]["votes"]["with"]["timeout_minutes"] == 30
    for filename in ["rollup-roll-call-votes.yml", "rollup-member-vote-terms.yml"]:
        manual = yaml.safe_load((WORKFLOWS / filename).read_text())
        assert manual["jobs"]["run"]["with"]["free_disk"] is True
    steps = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())["jobs"]["rollup"]["steps"]
    report = next(i for i, step in enumerate(steps) if step.get("name") == "Report the lowest free disk seen")
    audit = next(i for i, step in enumerate(steps) if step.get("name") == "Retain invocation and transferred-input audit")
    assert report < audit and "always()" in steps[report]["if"]
    assert "output/disk-watch.log" in steps[audit]["with"]["path"]
    assert "lowest sampled free disk" in steps[report]["run"]
