"""Exact links read one immutable input set, including partitioned bill tables."""

from copy import deepcopy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.scorecards.etl import ANALYSIS_NAMES, generation_options, read_family
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
from spicy_regs.scorecards.resolution import OFFICIAL_COLUMNS
from spicy_regs.sources import publication as pub
from spicy_regs.transforms.build_scorecard_analysis import _read
from tests.generation_fakes import Store

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def test_old_member_artifact_reports_missing_votesmart_column(tmp_path):
    path = tmp_path / "members.parquet"
    old_columns = [column for column in OFFICIAL_COLUMNS["members"] if column != "votesmart_id"]
    pq.write_table(pa.Table.from_pylist([], schema=pa.schema([(c, pa.string()) for c in old_columns])), path)
    with pytest.raises(ValueError, match=r"members\.parquet lacks required columns: votesmart_id"):
        _read([path], OFFICIAL_COLUMNS["members"])


def _inputs(tmp_path, *, row_overrides=None, published=frozenset()):
    """Published scorecards and official inputs; the ``published`` official tables as a family without receipts.

    Those are mapped to their public tables (``congress_tables.to_public``) as a family whose receipts are retired
    publishes them (ss-vse3); every other official table is written with its receipts, as before.
    """
    source = tmp_path / "inputs"
    source.mkdir()
    provenance = {
        "scorecard_id": "test:2025",
        "snapshot_id": "s1",
        "capture_id": "c1",
        "source_url": "https://publisher.example/2025",
        "source_path": "/table/1",
    }
    rows = {
        "scorecards": [{**provenance, "year_text": "2025"}],
        "scorecard_members": [
            {
                **provenance,
                "publisher_member_key": "p1",
                "member_name": "Alex Example",
                "state": "CA",
                "district": "1",
                "chamber_text": "House",
            }
        ],
        "scorecard_items": [
            {
                **provenance,
                "item_id": "i1",
                "item_kind_text": "cosponsorship",
                "congress_text": "119",
                "bill_citation_text": "H.R. 1",
            }
        ],
        "members": [{"bioguide_id": "X000001", "name_first": "Alex", "name_last": "Example"}],
        "member_terms": [
            {
                "bioguide_id": "X000001",
                "term_index": "0",
                "term_type": "rep",
                "term_start": "2025-01-03",
                "term_end": "2027-01-03",
                "term_state": "CA",
                "term_district": "1",
            }
        ],
        "scorecard_member_item_results": [],
        "scorecard_metric_items": [],
        "scorecard_methodologies": [],
        "bill_cosponsors": [],
        "member_votes": [],
        "amendments": [],
        "roll_call_votes": [],
    }
    rows.update(row_overrides or {})
    from spicy_regs.congress_receipts import policy, write_congress_dataset
    from spicy_regs.etl_receipts import combine_receipts
    from spicy_regs.scorecards.etl import write_family
    from spicy_regs.scorecards.subject_shapes import SOURCE_COLUMNS
    import shutil

    scorecard_names = ('scorecards', 'scorecard_members', 'scorecard_items', 'scorecard_member_item_results', 'scorecard_metric_items', 'scorecard_methodologies')
    scorecard_tables = {name: [{column: row.get(column) for column in SOURCE_COLUMNS[name]}
                              for row in rows[name]] for name in scorecard_names}
    scorecard_build = source / 'scorecards-native'
    source_files = write_family(scorecard_build, scorecard_tables)
    scorecard_generation = tmp_path / 'published-scorecards'
    build_generation(scorecard_generation, family='scorecards', files=source_files,
                     expected_keys=[p.name for p in source_files],
                     **generation_options(scorecard_build, scorecard_names))
    store = Store()
    index = pub.publish_generation(scorecard_generation, client=store, bucket='test', prior_index=pub.empty_index())

    files, receipts, plain = [], [], []
    official_names = (*OFFICIAL_COLUMNS, "member_votes", "bill_cosponsors")
    for name in official_names:
        partitions = [('118', [{'bill_id': '118-hr-1', 'congress': '118'}]),
                      ('119', [{'bill_id': '119-hr-1', 'congress': '119'}])] if name == 'congress_bills' else [(None, rows[name])]
        if name == 'congress_bills' and name in rows:
            partitions = [('117', rows[name])]
        for partition, records in partitions:
            raw = source / (name + ('-' + partition if partition else '') + '.parquet')
            extra_columns = {"member_votes": ("vote_id", "member_key", "position", "position_normalized"),
                             "bill_cosponsors": ("bill_id", "cosponsor_index", "bioguide_id", "sponsorship_date", "sponsorship_withdrawn_date")}
            schema = pa.schema([(c, pa.string()) for c in dict.fromkeys((*OFFICIAL_COLUMNS.get(name, extra_columns.get(name, ())), *(records[0] if records else ())))])
            pq.write_table(pa.Table.from_pylist(records, schema=schema), raw)
            if name in published:
                from spicy_regs.congress_tables import to_public

                (source / 'plain').mkdir(exist_ok=True)
                to_public(name, raw, source / 'plain' / (name + '.parquet'))
                plain.append(source / 'plain' / (name + '.parquet'))
                continue
            subject, receipt = write_congress_dataset(raw, source / (raw.stem + '-native'),
                dataset=name, generation_id='official-v1')
            receipts.append(receipt)
            assert subject is not None
            if partition:
                target = source / name / ('congress=' + partition) / 'part-000000.parquet'
                target.parent.mkdir(parents=True)
                shutil.copyfile(subject, target)
            else:
                files.append(subject)
        if name == 'congress_bills':
            files.append(source / name)
    artifact = tmp_path / 'published-inputs'
    build_generation(artifact, family='test-inputs', files=files,
        expected_keys=tuple(p.name if p.is_file() else p.name + '.parquet' for p in files),
        partitioned={'congress_bills.parquet': ('congress',)},
        receipt_path=combine_receipts(receipts, source / 'official-receipts.parquet'),
        receipt_policies=[policy(name) for name in official_names if name not in published],
        receipt_generation_id='official-v1')
    index = pub.publish_generation(artifact, client=store, bucket='test', prior_index=index)
    if plain:
        build_generation(tmp_path / 'published-plain', family='test-plain-inputs', files=plain,
                         expected_keys=[path.name for path in plain])
        index = pub.publish_generation(tmp_path / 'published-plain', client=store, bucket='test', prior_index=index)
    return store, index


def _serve(monkeypatch, store):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example")
    calls = []

    def fetch(url, member, target, key):
        calls.append(member.path)
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    return calls


def test_analysis_pins_every_partition_and_keeps_publisher_identities(tmp_path, monkeypatch):
    store, index = _inputs(tmp_path)
    calls = _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    parents = rollup._prime(output, index)
    # Every main member, including both bill partitions; source receipts and
    # only the two official datasets requiring original processing context.
    assert len(calls) == len(rollup.inputs) + 1 + 1 + 2
    assert set(rollup._official_receipts) == {"members", "member_terms"}

    assert "tableDescriptorDigest" in parents["congress_bills.parquet"]
    assert "sha256" not in parents["congress_bills.parquet"]
    members, items, comparisons = rollup.build(output)
    assert pq.read_table(comparisons).num_rows == 0
    assert json.loads((output / "scorecard-analysis-qualification.json").read_text())["comparison"]["scopes"] == []
    member = pq.read_table(members).to_pylist()[0]
    item = pq.read_table(items).to_pylist()[0]
    assert member["publisher_member_key"] == "p1"
    assert member["bioguide_id"] == "X000001"
    assert item["bill_id"] == "119-hr-1"
    assert item["vote_id"] is None
    assert json.loads(item["input_pins_json"])["congress_bills"] == parents["congress_bills.parquet"]
    item_receipt = read_family(output, ANALYSIS_NAMES)["scorecard_item_links"][0]
    assert json.loads(item_receipt["input_pins_json"])["congress_bills"] == parents["congress_bills.parquet"]
    sealed = tmp_path / "analysis-generation"
    build_generation(
        sealed,
        family="scorecard-analysis",
        files=[members, items, comparisons],
        expected_keys=rollup.outputs,
        parents=parents,
        read_snapshot=index,
        **generation_options(output, ANALYSIS_NAMES),
    )
    verify_generation(sealed)
    # A warm read validates bytes and needs no network.
    rollup._prime(output, index)
    assert len(calls) == len(rollup.inputs) + 1 + 1 + 2


def test_analysis_keeps_member_context_receipt_checked(tmp_path, monkeypatch):
    store, index = _inputs(tmp_path)
    _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    rollup._prime(output, index)
    assert set(rollup._official_receipts) == {"members", "member_terms"}
    members, _, _ = rollup.build(output)
    assert pq.read_table(members).to_pylist()[0]["bioguide_id"] == "X000001"
    receipts, _ = rollup._official_receipts["member_terms"]
    receipts.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="receipt changed"):
        rollup._prime(output, index)


def test_lcv_comparison_flows_through_pinned_analysis_family(tmp_path, monkeypatch):
    context = dict(scorecard_id="lcv:2025", snapshot_id="s1", capture_id="c1",
                   source_url="https://publisher.example/2025", source_path="CSV/row[1]")
    store, index = _inputs(tmp_path, row_overrides={
        "scorecards": [context | dict(publisher_id="lcv", year_text="2025")],
        "scorecard_members": [context | dict(publisher_member_key="p1", member_name="Alex Example",
            identifiers_json='[{"scheme":"bioguide","value":"X000001"}]', chamber_text="House")],
        "scorecard_items": [context | dict(item_id="i1", item_kind_text="Scorecard Vote", item_date_text="2025",
            congress_text="119", chamber_text="House", session_text="1", roll_number_text="24", publisher_position_text="No")],
        "scorecard_member_item_results": [context | dict(item_id="i1", publisher_member_key="p1",
            result_id="reported", metric_id="annual", participation_id="scorecard", result_text="excused")],
        "scorecard_metric_items": [context | dict(item_id="i1", metric_id="annual", participation_id="scorecard",
                                                  publisher_position_text="No")],
        "roll_call_votes": [dict(vote_id="119-house-1-24", vote_date="29-Jan-2025")],
        "member_votes": [dict(vote_id="119-house-1-24", member_key="X000001", bioguide_id="X000001",
                              position="Not Voting", position_normalized="not_voting")],
    })
    _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    parents = rollup._prime(output, index)
    files = rollup.build(output)
    [row] = pq.read_table(files[2]).to_pylist()
    assert row["comparison_status"] == "not_voting"
    assert row["publisher_credit_text"] == "excused"
    assert row["official_position_text"] == "Not Voting"
    assert json.loads(row["input_pins_json"])["member_votes"] == parents["member_votes.parquet"]
    sealed = tmp_path / "generation"
    build_generation(sealed, family="scorecard-analysis", files=files, expected_keys=rollup.outputs,
                     parents=parents, read_snapshot=index, **generation_options(output, ANALYSIS_NAMES))
    assert "scorecard_action_comparisons.parquet" in verify_generation(sealed).root["spec"]["tables"]


def test_analysis_preserves_unresolved_people_and_independent_actions(tmp_path, monkeypatch):
    provenance = {
        "scorecard_id": "test:2025",
        "snapshot_id": "s1",
        "capture_id": "c1",
        "source_url": "https://publisher.example/2025",
        "source_path": "/table/1",
    }
    source_people = [
        {
            **provenance,
            "publisher_member_key": "named-participant",
            "member_name": "Unresolved named participant",
            "chamber_text": "Senate",
            "identifiers_json": '[{"scheme":"ocd-person","value":"publisher-supplied-value"}]',
        }
    ]
    items = [
        {
            **provenance,
            "item_id": "committee-motion",
            "item_kind_text": "committee_action",
            "title": "Senate Finance Committee: motion reported without a bill",
            "chamber_text": "Senate",
            "congress_text": "119",
            "session_text": "1",
            "roll_number_text": "1",
        },
        {
            **provenance,
            "item_id": "cosponsor",
            "item_kind_text": "cosponsorship",
            "congress_text": "119",
            "bill_citation_text": "H.R. 1",
        },
        *[
            {
                **provenance,
                "item_id": f"vote-{roll}",
                "item_kind_text": "roll_call",
                "congress_text": "119",
                "chamber_text": "House",
                "session_text": "1",
                "roll_number_text": str(roll),
                "bill_citation_text": "H.R. 1",
            }
            for roll in (1, 2)
        ],
    ]
    originals = deepcopy((source_people, items))
    store, index = _inputs(
        tmp_path,
        row_overrides={
            "scorecard_members": source_people,
            "scorecard_items": items,
            "roll_call_votes": [{"vote_id": f"119-house-1-{roll}", "bill_id": "119-hr-1"} for roll in (1, 2)],
        },
    )
    calls = _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    rollup._prime(output, index)
    acquired = list(calls)
    members, links, _ = rollup.build(output)
    # Analysis uses only the selected inputs; it does not acquire official data again.
    assert calls == acquired
    assert pq.read_table(members).num_rows == 1
    [main_person] = pq.read_table(members).to_pylist()
    assert main_person["resolution_status"] == "unresolved" and main_person["bioguide_id"] is None
    reconstructed = read_family(output, ANALYSIS_NAMES)
    person = reconstructed["scorecard_member_links"][0]
    assert person["bioguide_id"] is None
    assert person["resolution_status"] == "unresolved"
    observed = json.loads(person["source_context_json"])
    assert observed["member_name"] == source_people[0]["member_name"]
    assert observed["identifiers_json"] == source_people[0]["identifiers_json"]
    [main_motion] = [row for row in pq.read_table(links).to_pylist() if row["item_id"] == "committee-motion"]
    assert main_motion["resolution_status"] == "unresolved"
    assert main_motion["vote_id"] is None and main_motion["bill_id"] is None
    linked = {row["item_id"]: row for row in reconstructed["scorecard_item_links"]}
    assert linked["committee-motion"]["vote_id"] is None
    assert linked["committee-motion"]["bill_id"] is None
    assert "committee_action_not_mapped_to_floor_roll" in linked["committee-motion"]["reason"]
    assert linked["cosponsor"]["bill_id"] == "119-hr-1"
    assert linked["cosponsor"]["vote_id"] is None
    assert {linked[f"vote-{roll}"]["vote_id"] for roll in (1, 2)} == {"119-house-1-1", "119-house-1-2"}
    assert {linked[f"vote-{roll}"]["bill_id"] for roll in (1, 2)} == {"119-hr-1"}
    assert (source_people, items) == originals


@pytest.mark.parametrize("corruption", ["bytes", "missing-family"])
def test_analysis_refuses_inputs_that_do_not_match_the_index(tmp_path, monkeypatch, corruption):
    store, index = _inputs(tmp_path)
    _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    rollup._prime(output, index)
    if corruption == "bytes":
        (output / "congress_bills/congress=119/part-000000.parquet").write_bytes(b"different")
    else:
        index = pub.empty_index()
    with pytest.raises(pub.PublicationError):
        rollup._prime(output, index)
    with pytest.raises(pub.PublicationError, match="Prime verified"):
        rollup.build(output)
    assert not (output / "scorecard_member_links.parquet").exists()


def test_split_parent_pin_rejects_a_changed_member_or_size(tmp_path):
    _, index = _inputs(tmp_path)
    key = "congress_bills.parquet"
    pin = pub.table_pin(index, key)
    output = tmp_path / "derived.parquet"
    pq.write_table(pa.table({"id": ["one"]}), output)
    for number, field in enumerate(("member", "byteSize", "unmanaged")):
        changed = deepcopy(index)
        parent = deepcopy(pin)
        if field == "member":
            changed["families"]["test-inputs"]["tables"][key]["members"][0]["sha256"] = "sha256:" + "0" * 64
        elif field == "byteSize":
            parent["byteSize"] += 1
        else:
            parent.pop("family")
            parent.pop("artifactDigest")
        with pytest.raises(ValueError, match="[Pp]arent"):
            build_generation(
                tmp_path / f"refused-{number}",
                family="derived",
                files=[output],
                expected_keys=[output.name],
                parents={key: parent},
                read_snapshot=changed,
            )


@pytest.mark.parametrize('missing', ['source', 'official'])
def test_analysis_requires_native_receipts_for_every_input(tmp_path, monkeypatch, missing):
    store, index = _inputs(tmp_path)
    _serve(monkeypatch, store)
    output = tmp_path / 'analysis'
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    rollup._prime(output, index)
    if missing == 'source':
        rollup._scorecard_source_receipt = None
    else:
        rollup._official_receipts.pop('member_terms')
    with pytest.raises(ValueError, match='require.*native receipts'):
        rollup.build(output)
    assert not (output / 'scorecard_member_links.parquet').exists()
