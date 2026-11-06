"""Tables stored as several files: the version-2 index, readers, building, publishing and the audit (multi-file design)."""

import argparse
import json
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any, ClassVar

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import canonical_json_bytes
from spicy_docs.schemas.tables import bill_congress

from spicy_regs import cli, mcp_server
from spicy_regs.generation_audit import Declaration, PublicBase, _consistency, _Run, audit
from spicy_regs.generations import build_generation
from spicy_regs.local_data import local_selection, verify_local_members
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import publication as pub, r2
from spicy_regs.transforms.table_merge import (
    Partitioning,
    merge_partitioned_table,
    prior_members_path,
    published_members,
)
from tests.generation_fakes import Store
from tests.test_generation_audit import public_base
from tests.test_generation_mcp import connection_fixture
from tests.test_local_mcp import serve
from tests.test_mcp_server import _records, _tool_data

DIGEST = "sha256:" + "b" * 64
PREFIX = f"generations/bills/{'b' * 64}"

# Synthetic tables exercise storage mechanics independently of installed subject policies.
COLUMNS = [["bill_id", "VARCHAR"], ["congress", "VARCHAR"], ["body", "VARCHAR"]]
DECLARED = {"fixture_bill_sections": Declaration(tuple(map(tuple, COLUMNS)), ("bill_id",), "test")}


def _member(congress: str, rows: int, size: int) -> dict:
    return {"key": f"fixture_bill_sections/congress={congress}/part-000000.parquet", "sha256": "sha256:" + congress[-1] * 64,
            "byteSize": size, "rows": rows, "partition": {"congress": congress}}


def _index(**table) -> dict:
    members = [_member("118", 2, 10), _member("119", 1, 5)]
    split = {"byteSize": 15, "rows": 3, "columns": COLUMNS, "partitionColumns": ["congress"], "members": members}
    single = {"sha256": "sha256:" + "c" * 64, "byteSize": 7, "rows": 1, "columns": [["bill_id", "VARCHAR"]]}
    return {"format": "spicy-regs-publication", "version": 2, "families": {"bills": {
        "prefix": PREFIX, "logicalId": "urn:spicy-regs:family:bills", "artifactDigest": DIGEST,
        "tables": {"fixture_bill_sections.parquet": split | table, "fixture_congress_bills.parquet": single}}}}


def _parse(index: dict) -> dict:
    return pub.parse_index(json.dumps(index).encode())


def test_a_split_table_resolves_to_its_members_and_a_single_table_to_itself():
    index = _parse(_index())
    assert pub.table_members(index, "fixture_bill_sections.parquet") == (
        pub.Member(f"{PREFIX}/fixture_bill_sections/congress=118/part-000000.parquet", "sha256:" + "8" * 64, 10, 2),
        pub.Member(f"{PREFIX}/fixture_bill_sections/congress=119/part-000000.parquet", "sha256:" + "9" * 64, 5, 1),
    )
    assert pub.single_member(index, "fixture_congress_bills.parquet") == pub.Member(
        f"{PREFIX}/fixture_congress_bills.parquet", "sha256:" + "c" * 64, 7, 1)
    with pytest.raises(pub.PublicationError, match="2 files; read it through table_members"):
        pub.single_member(index, "fixture_bill_sections.parquet")
    # A member's key within its generation is the layout a download keeps; a legacy file's key is its own path.
    assert [m.key for m in pub.table_members(index, "fixture_bill_sections.parquet")] == [
        "fixture_bill_sections/congress=118/part-000000.parquet", "fixture_bill_sections/congress=119/part-000000.parquet"]
    assert pub.table_members(index, "legacy.parquet")[0].key == "legacy.parquet"
    with pytest.raises(pub.PublicationError, match="not under a generation prefix"):
        _ = pub.Member("generations/bills/short/x.parquet", DIGEST, 1, 1).key


@pytest.mark.parametrize("broken", [
    {"rows": 4},  # members' rows sum to 3
    {"byteSize": 14},
    {"partitionColumns": ["session"]},  # not a declared column
    {"members": [_member("118", 2, 10), {**_member("119", 1, 5), "partition": {"congress": "118"}}]},
    {"members": [_member("118", 2, 10), {**_member("119", 1, 5), "key": "fixture_bill_sections/congress=119/x.parquet"}]},
    {"members": [_member("118", 3, 15), _member("118", 0, 0)]},  # one key twice
    {"members": [_member("../118", 3, 15)]},  # its key spells the value; only the value grammar refuses
    {"columns": [*COLUMNS, ["../x", "VARCHAR"]], "partitionColumns": ["../x"],  # a column spells a key directory
     "members": [{**_member("118", 3, 15), "key": "fixture_bill_sections/../x=118/part-000000.parquet",
                  "partition": {"../x": "118"}}]},
])
def test_a_split_table_whose_members_do_not_add_up_or_name_their_partition_refuses(broken):
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        _parse(_index(**broken))


def test_only_version_2_may_list_a_split_table():
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        _parse({**_index(), "version": 1})


def test_partition_directory_order_comes_from_declared_columns_after_canonical_json():
    index = _index(columns=[*COLUMNS, ["session", "VARCHAR"]], partitionColumns=["session", "congress"])
    table = index["families"]["bills"]["tables"]["fixture_bill_sections.parquet"]
    for member in table["members"]:
        member["partition"]["session"] = "1"
        member["key"] = member["key"].replace("/congress=", "/session=1/congress=")
    parsed = pub.parse_index(json.dumps(index, sort_keys=True).encode())
    assert parsed == index
    # The object may be reordered; the explicit directory declaration may not.
    table["partitionColumns"].reverse()
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        _parse(index)


def test_readers_read_only_the_version_2_index(monkeypatch):
    """A bucket serving only the retired ``publication.json`` has no pointer: its tables resolve as legacy."""
    retired: dict[str, Any] = {**_index(), "version": 1}
    retired["families"]["bills"]["tables"].pop("fixture_bill_sections.parquet")
    served = {"publication.json": retired, pub.INDEX_V2_KEY: _index()}
    requested = []

    def get(url, **_):
        requested.append(key := url.rsplit("/", 1)[1])
        return json.dumps(served[key]).encode() if key in served else None

    monkeypatch.setattr(pub, "_bounded_get", get)
    assert "fixture_bill_sections.parquet" in pub.load_index("https://test")["families"]["bills"]["tables"]
    del served[pub.INDEX_V2_KEY]
    assert pub.load_index("https://test") == pub.empty_index()
    assert requested == [pub.INDEX_V2_KEY, pub.INDEX_V2_KEY]


def test_the_mcp_serves_one_view_over_every_member_without_a_hive_column(tmp_path, monkeypatch):
    """Members sit under ``congress=<N>/`` directories; the view's columns are still exactly the declared ones."""
    index = _parse(_index())
    locations = {}
    for congress, bills in (("118", ["118-hr1", "118-hr2"]), ("119", ["119-s5"])):
        path = tmp_path / f"congress={congress}" / "part-000000.parquet"
        path.parent.mkdir()
        pq.write_table(pa.table({"bill_id": bills, "congress": [congress] * len(bills), "body": ["text"] * len(bills)}),
                       path)
        locations[f"{PREFIX}/fixture_bill_sections/congress={congress}/part-000000.parquet"] = path
    single = tmp_path / "fixture_congress_bills.parquet"
    pq.write_table(pa.table({"bill_id": ["119-s5"]}), single)
    locations[f"{PREFIX}/fixture_congress_bills.parquet"] = single
    built = connection_fixture(monkeypatch, index, locations, tables=())
    mcp_server._build_connection()
    [con] = built
    assert [row[0] for row in con.inner.execute("DESCRIBE fixture_bill_sections").fetchall()] == ["bill_id", "congress", "body"]
    assert con.inner.execute("SELECT congress, count(*) FROM fixture_bill_sections GROUP BY 1 ORDER BY 1").fetchall() == [
        ("118", 2), ("119", 1)]
    con.inner.close()


# --------------------------------------------------------------------------- #
# Producing and publishing a split table (multi-file design §4.2-§4.4).
# --------------------------------------------------------------------------- #
SPLIT = {"fixture_bill_sections.parquet": ["congress"]}
KEYS = ["fixture_bill_sections.parquet", "fixture_congress_bills.parquet"]


def _sections(directory, by_congress: dict[str, list[str]]) -> None:
    """Write one ``congress=<N>/part-000000.parquet`` member per congress, its rows holding that congress."""
    for congress, bills in by_congress.items():
        path = directory / "fixture_bill_sections" / f"congress={congress}" / "part-000000.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"bill_id": bills, "congress": [congress] * len(bills),
                                 "body": [f"text of {bill}" for bill in bills]}), path)


def _outputs(directory, by_congress=None, *, bills=("119-s5",)) -> list:
    """A bills family's outputs under ``directory``: split sections (unless already there) and one single table."""
    if by_congress is not None:
        _sections(directory, by_congress)
    pq.write_table(pa.table({"bill_id": list(bills)}), directory / "fixture_congress_bills.parquet")
    return [directory / "fixture_bill_sections", directory / "fixture_congress_bills.parquet"]


def _publish(tmp_path, store, name, files, prior=None):
    directory = tmp_path / name
    build_generation(directory, family="bills", files=files, expected_keys=KEYS, read_snapshot=prior,
                     partitioned=SPLIT)
    return pub.publish_generation(directory, client=store, bucket="test", prior_index=prior or pub.empty_index())


def _serve(monkeypatch, store) -> None:
    """Answer public GETs from the fake bucket, as r2.dev does; the index is read the way readers read it."""
    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://test/")
        request = httpx.Request(method, url)
        yield (httpx.Response(200, content=store.objects[key], request=request) if key in store.objects
               else httpx.Response(404, request=request))

    monkeypatch.setattr(httpx, "stream", stream)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test")
    monkeypatch.setattr(pub, "load_index", lambda url: pub.parse_index(store.objects[pub.INDEX_V2_KEY]))


def test_a_split_table_publishes_in_version_2_and_an_unchanged_partition_is_copied_not_uploaded(tmp_path,
                                                                                                  monkeypatch):
    store = Store()
    first = _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1", "118-hr2"],
                                                                           "119": ["119-s5"]}))
    split = first["families"]["bills"]["tables"]["fixture_bill_sections.parquet"]
    assert (split["rows"], split["partitionColumns"]) == (3, ["congress"])
    assert [(m["key"], m["rows"], m["partition"]) for m in split["members"]] == [
        ("fixture_bill_sections/congress=118/part-000000.parquet", 2, {"congress": "118"}),
        ("fixture_bill_sections/congress=119/part-000000.parquet", 1, {"congress": "119"})]

    # The builder's nightly shape: fetch the prior partitions, rewrite only the sitting Congress.
    _serve(monkeypatch, store)
    rebuilt = tmp_path / "out2"
    fetched = r2.download_members("fixture_bill_sections.parquet", rebuilt)
    assert [path.relative_to(rebuilt).as_posix() for path in fetched] == [m["key"] for m in split["members"]]
    _sections(rebuilt, {"119": ["119-s5", "119-s6"]})
    store.copies.clear()
    second = _publish(tmp_path, store, "second", _outputs(rebuilt), prior=first)

    prefix = second["families"]["bills"]["prefix"]
    assert sorted(store.copies) == [f"{prefix}/fixture_bill_sections/congress=118/part-000000.parquet",
                                    f"{prefix}/fixture_congress_bills.parquet"]
    assert f"{prefix}/fixture_bill_sections/congress=119/part-000000.parquet" in store.writes
    assert second["families"]["bills"]["tables"]["fixture_bill_sections.parquet"]["rows"] == 4


def test_the_audit_reconciles_a_split_table_member_by_member(tmp_path):
    store = Store()
    first = _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1"], "119": ["119-s5"]}))
    _publish(tmp_path, store, "second", _outputs(tmp_path / "out2", {"118": ["118-hr1"], "119": ["119-s5", "119-s6"]}),
             prior=first)

    report = audit(public_base(store, tmp_path / "public"), family="bills", table="fixture_bill_sections",
                   declarations=DECLARED)

    table = report["sections"]["publication"]["tables"]["fixture_bill_sections.parquet"]
    assert (table["index_equals_manifest"], table["index_equals_root"], table["observed_bytes_equal_index"]) == (
        True, True, True)
    assert len(table["manifest"]) == 2
    conservation = report["sections"]["conservation"]["fixture_bill_sections"]
    assert (conservation["rows"], conservation["prior_rows"], conservation["bytes_equal"]) == (3, 2, False)
    assert (conservation["identities_added"], conservation["added_sample"]) == (1, [{"bill_id": "119-s6"}])
    assert report["findings"] == []


def test_the_audit_finds_one_split_member_the_index_misstates_and_retains_version_2(tmp_path):
    """Only the second member's pin is wrong, so a check reading the first member alone would pass."""
    store = Store()
    _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1"], "119": ["119-s5"]}))
    index = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    index["families"]["bills"]["tables"]["fixture_bill_sections.parquet"]["members"][1]["sha256"] = "sha256:" + "0" * 64
    store.objects[pub.INDEX_V2_KEY] = canonical_json_bytes(index)
    retained = tmp_path / "retained"

    report = audit(public_base(store, tmp_path / "public"), family="bills", table="fixture_bill_sections",
                   declarations=DECLARED, retain=retained)

    table = report["sections"]["publication"]["tables"]["fixture_bill_sections.parquet"]
    assert (table["index_equals_manifest"], table["index_equals_root"], table["observed_bytes_equal_index"]) == (
        False, True, False)
    assert {f["code"] for f in report["findings"]} >= {"index-equals-manifest-false",
                                                       "observed-bytes-equal-index-false"}
    assert (retained / pub.INDEX_V2_KEY).read_bytes() == store.objects[pub.INDEX_V2_KEY]
    assert not (retained / "publication.json").exists()


def test_the_audit_rereads_the_etag_of_every_member_of_both_generations():
    """ETag stability is checked per stored file, so a split member of the current or prior generation is not skipped."""
    current = _parse(_index())["families"]["bills"]
    prior: dict[str, Any] = {**current, "prefix": f"generations/bills/{'a' * 64}"}
    locations = [f"{entry['prefix']}/{member['key']}" for entry in (current, prior)
                 for member in entry["tables"]["fixture_bill_sections.parquet"]["members"]]
    moved = {locations[1], locations[2]}
    base = PublicBase("https://data.test", client=httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, headers={"etag": '"moved"' if request.url.path[1:] in moved else '"e"'}))))
    base.receipts = [{"key": location, "etag": '"e"', "complete": True} for location in locations]
    run = _Run(samples=1, secrets={})

    result = _consistency(base, current, prior, ["fixture_bill_sections.parquet"], run)

    assert result["etag_stable"] == {location: location not in moved for location in locations}
    assert sorted(f["location"] for f in run.findings) == sorted(moved)


def test_the_shrink_guard_sums_a_split_tables_members(tmp_path):
    """Every partition remains, but the one holding nearly all the table's bytes shrinks."""
    store = Store()
    many = [f"119-hr{n}" for n in range(3000)]
    first = _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1"], "119": many}))
    with pytest.raises(RuntimeError, match="shrink"):
        _publish(tmp_path, store, "second", _outputs(tmp_path / "out2", {"118": ["118-hr1"], "119": ["119-hr1"]}),
                 prior=first)


def test_a_split_table_keeps_every_partition_its_prior_generation_holds(tmp_path):
    """A forgotten partition refuses even when the rest outweighs the shrink ratio; part files and new ones may change."""
    store = Store()
    many = [f"119-hr{n}" for n in range(3000)]
    first = _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1"], "119": many}))
    with pytest.raises(pub.PublicationError, match="lacks partitions its prior generation holds: congress=118"):
        _publish(tmp_path, store, "second", _outputs(tmp_path / "out2", {"119": many}), prior=first)

    out = tmp_path / "out3"
    _sections(out, {"118": ["118-hr1"], "120": ["120-hr1"]})
    (out / "fixture_bill_sections" / "congress=119").mkdir()
    for part, bills in enumerate((many[:1500], many[1500:])):
        pq.write_table(pa.table({"bill_id": bills, "congress": ["119"] * len(bills), "body": ["b"] * len(bills)}),
                       out / "fixture_bill_sections" / "congress=119" / f"part-{part:06d}.parquet")
    third = _publish(tmp_path, store, "third", _outputs(out), prior=first)
    members = third["families"]["bills"]["tables"]["fixture_bill_sections.parquet"]["members"]
    assert [member["key"].split("/", 1)[1] for member in members] == [
        "congress=118/part-000000.parquet", "congress=119/part-000000.parquet", "congress=119/part-000001.parquet",
        "congress=120/part-000000.parquet"]


@pytest.mark.parametrize(("layout", "message"), [
    ({"fixture_bill_sections/congress=118/part-000000.parquet": "119"}, "differ from the partition"),
    ({"fixture_bill_sections/congress=118/data.parquet": "118"}, "is not <col>=<value>"),
    ({"fixture_bill_sections/session=1/part-000000.parquet": "118"}, "is not <col>=<value>"),
    ({"fixture_bill_sections/part-000000.parquet": "118"}, "is not <col>=<value>"),
])
def test_a_split_output_whose_layout_or_rows_disagree_with_its_partitions_refuses(tmp_path, layout, message):
    out = tmp_path / "out"
    for relative, congress in layout.items():
        (out / relative).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"bill_id": ["x"], "congress": [congress], "body": ["b"]}), out / relative)
    with pytest.raises(ValueError, match=message):
        build_generation(tmp_path / "artifact", family="bills", files=_outputs(out), expected_keys=KEYS,
                         partitioned=SPLIT)


def test_members_of_one_split_table_must_share_their_columns(tmp_path):
    out = tmp_path / "out"
    _sections(out, {"118": ["118-hr1"]})
    other = out / "fixture_bill_sections" / "congress=119" / "part-000000.parquet"
    other.parent.mkdir(parents=True)
    pq.write_table(pa.table({"bill_id": ["119-s5"], "congress": ["119"]}), other)
    with pytest.raises(ValueError, match="differ in columns"):
        build_generation(tmp_path / "artifact", family="bills", files=_outputs(out), expected_keys=KEYS,
                         partitioned=SPLIT)


# --------------------------------------------------------------------------- #
# The CLI and local mode read a split table (multi-file design §7, Readers).
# --------------------------------------------------------------------------- #
def _download_split(tmp_path, monkeypatch):
    """Publish a bills family with a split table into the fake bucket, then download both tables with the CLI."""
    store = Store()
    _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1", "118-hr2"], "119": ["119-s5"]}))
    _serve(monkeypatch, store)
    monkeypatch.delenv("SPICY_REGS_R2_URL", raising=False)
    output = tmp_path / "data"
    cli.cmd_download(argparse.Namespace(output_dir=output, types=["fixture_bill_sections", "fixture_congress_bills"], force=False))
    return output, (output / "current").resolve(strict=True)


def test_a_split_table_downloads_verifies_and_is_read_locally_without_a_hive_column(tmp_path, monkeypatch, capsys):
    output, batch = _download_split(tmp_path, monkeypatch)
    metadata = json.loads((batch / "download.json").read_text())
    prefix = metadata["publication"]["families"]["bills"]["prefix"]
    members = [f"fixture_bill_sections/congress={n}/part-000000.parquet" for n in ("118", "119")]
    assert metadata["selected"] == {
        "fixture_bill_sections": {"key": [f"{prefix}/{key}" for key in members], "status": "managed"},
        "fixture_congress_bills": {"key": f"{prefix}/fixture_congress_bills.parquet", "status": "managed"}}

    selection = local_selection(output)
    assert selection.files["fixture_bill_sections"] == (batch / "fixture_bill_sections", "managed")
    assert selection.paths("fixture_bill_sections") == tuple(batch / key for key in members)
    assert set(verify_local_members(selection)) == {str(batch / key) for key in [*members, "fixture_congress_bills.parquet"]}

    con, server = serve(monkeypatch, output)
    try:
        assert [row[0] for row in con.execute("DESCRIBE fixture_bill_sections").fetchall()] == ["bill_id", "congress", "body"]
        result = _tool_data(server, "query_sql", {"sql": "SELECT congress, count(*) AS n FROM fixture_bill_sections "
                                                         "GROUP BY 1 ORDER BY 1"})
        assert _records(result) == [{"congress": "118", "n": 2}, {"congress": "119", "n": 1}]
        # The view names the verified member files, so a file added to the directory later is never read.
        extra = batch / "fixture_bill_sections" / "congress=119" / "part-000001.parquet"
        extra.write_bytes((batch / members[1]).read_bytes())
        count = _tool_data(server, "query_sql", {"sql": "SELECT count(*) AS n FROM fixture_bill_sections"})
        extra.unlink()
        assert _records(count) == [{"n": 3}]
    finally:
        con.close()

    capsys.readouterr()
    args = argparse.Namespace(output_dir=output, data_type="fixture_bill_sections", agency=None, n=3, query="118-hr2", limit=5)
    cli.cmd_stats(args)
    stats = capsys.readouterr().out
    assert "FIXTURE_BILL_SECTIONS" in stats and "Rows: 3" in stats and "Columns: bill_id, congress, body\n" in stats
    cli.cmd_sample(args)
    cli.cmd_search(args)
    assert "3 total rows; managed" in (out := capsys.readouterr().out) and "118-hr2" in out


@pytest.mark.parametrize(("damage", "message"), [
    ("tampered", "differs from its generation pin"),
    ("extra", "differs from its published members"),
    ("symlink", "differs from its published members"),
    ("missing", "differs from its published members"),
])
def test_a_split_download_whose_files_differ_from_its_members_refuses(tmp_path, monkeypatch, damage, message):
    output, batch = _download_split(tmp_path, monkeypatch)
    member = batch / "fixture_bill_sections" / "congress=119" / "part-000000.parquet"
    raw = member.read_bytes()
    if damage == "tampered":
        member.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
    elif damage == "extra":
        member.with_name("part-000001.parquet").write_bytes(raw)
    elif damage == "symlink":
        (tmp_path / "elsewhere.parquet").write_bytes(raw)
        member.unlink()
        member.symlink_to(tmp_path / "elsewhere.parquet")
    else:
        member.unlink()
    with pytest.raises(RuntimeError, match=message):
        verify_local_members(local_selection(output))


# --------------------------------------------------------------------------- #
# Building a split table from its published prior, through the rollup (multi-file design §4.3).
# --------------------------------------------------------------------------- #
class _SplitFamily(RollupPipeline):
    """A family storing ``sections`` one file per Congress, the way the bill family stores ``fixture_bill_sections``."""

    name: ClassVar[str] = "split-family"
    outputs: ClassVar[tuple[str, ...]] = ("sections.parquet", "bills.parquet")
    partitioned: ClassVar[Mapping[str, tuple[str, ...]]] = {"sections.parquet": ("congress",)}
    fresh: ClassVar[tuple[dict, ...]] = ()

    def build(self, output_dir):
        sections = merge_partitioned_table(
            output_dir, name="sections", columns=("bill_id", "congress", "body"), identity=("bill_id",),
            version_column=None, rows=[{**row, "congress": bill_congress(row["bill_id"])} for row in self.fresh],
            partitioning=Partitioning("congress", "bill_id", bill_congress),
            prior=published_members(output_dir, "sections"),
        )
        bills = output_dir / "bills.parquet"
        pq.write_table(pa.table({"bill_id": ["119-s-5"]}), bills)
        return sections, bills


def _public(monkeypatch, store) -> None:
    """Answer the public base's GETs from the fake bucket, as r2.dev does."""
    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://example.test/")
        request = httpx.Request(method, url)
        yield (httpx.Response(200, content=store.objects[key], request=request) if key in store.objects
               else httpx.Response(404, request=request))

    monkeypatch.setattr(httpx, "stream", stream)


def test_a_rollup_declaring_a_split_table_publishes_its_members_and_copies_an_untouched_one(tmp_path, remote,
                                                                                             monkeypatch):
    """The nightly shape: every prior member fetched by digest, the sitting Congress rewritten, the rest copied."""
    _public(monkeypatch, remote)
    first = type("First", (_SplitFamily,), {"fresh": ({"bill_id": "118-hr-1", "body": "a"},
                                                      {"bill_id": "119-s-5", "body": "b"})})
    first(output_dir=tmp_path / "first", skip_upload=False).run()
    published = pub.parse_index(remote.objects[pub.INDEX_V2_KEY])["families"]["split-family"]
    split = published["tables"]["sections.parquet"]
    assert (split["partitionColumns"], [m["key"] for m in split["members"]]) == (
        ["congress"], ["sections/congress=118/part-000000.parquet", "sections/congress=119/part-000000.parquet"])
    with pytest.raises(pub.PublicationError, match="read it through table_members"):
        r2.download("sections.parquet", tmp_path / "one-file.parquet")

    remote.copies.clear()
    second = type("Second", (_SplitFamily,), {"fresh": ({"bill_id": "119-s-6", "body": "c"},)})
    second(output_dir=tmp_path / "second", skip_upload=False).run()

    entry = pub.parse_index(remote.objects[pub.INDEX_V2_KEY])["families"]["split-family"]
    members = {m["key"]: m for m in entry["tables"]["sections.parquet"]["members"]}
    assert members["sections/congress=118/part-000000.parquet"]["sha256"] == split["members"][0]["sha256"]
    assert members["sections/congress=119/part-000000.parquet"]["rows"] == 2
    assert f"{entry['prefix']}/sections/congress=118/part-000000.parquet" in remote.copies
    assert f"{entry['prefix']}/sections/congress=119/part-000000.parquet" in remote.writes


def test_the_bill_family_declares_bill_sections_split_by_congress():
    from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup

    assert BillFamilyRollup.partitioned == {"bill_sections.parquet": ("congress",)}
    assert "bill_sections.parquet" in BillFamilyRollup.outputs


def test_a_prior_split_table_is_read_member_by_member_and_an_unpublished_one_is_a_cold_start(tmp_path, monkeypatch):
    store = Store()
    _publish(tmp_path, store, "first", _outputs(tmp_path / "out1", {"118": ["118-hr1"], "119": ["119-s5"]}))
    _serve(monkeypatch, store)
    paths = published_members(tmp_path / "build", "fixture_bill_sections")
    assert paths is not None
    assert [path.relative_to(prior_members_path(tmp_path / "build", "fixture_bill_sections")).as_posix() for path in paths] == [
        "fixture_bill_sections/congress=118/part-000000.parquet", "fixture_bill_sections/congress=119/part-000000.parquet"]
    assert published_members(tmp_path / "build", "never_published") is None
    monkeypatch.delenv("R2_PUBLIC_URL")
    assert published_members(tmp_path / "unconfigured", "fixture_bill_sections") is None
