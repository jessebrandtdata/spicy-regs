"""Mirror retry/no-op behavior, the agency-first physical layout, and the published members' one typed footer."""

from dataclasses import asdict, replace
import hashlib
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comments_mirror as mirror
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies
from spicy_regs.transforms.write_staging import write_staging





def build(con, root, resources):
    resources.configure(con, root / "spill")
    from tempfile import TemporaryDirectory
    from pathlib import Path

    with TemporaryDirectory(dir=root) as work:
        staging = Path(work) / "staging"
        stage_comment_agencies(con, "SELECT * FROM source", staging, resources=resources)
        partitions = sort_comment_agencies(staging, root, resources=resources)
    flat = assemble_comments(con, partitions, root, con.sql("SELECT * FROM source").columns, resources=resources)
    index = iceberg._build_comments_index(con, COMMENT, root, source_sql="SELECT * FROM source")
    return {"comments": flat, "partitions": partitions, "index": index}


def test_partitioning_preserves_values_nulls_schema_and_order(tmp_path):
    text = "wide source value " * 150_000
    source = pa.table({"comment_id": ["z", "a", "b", "c"], "agency_code": ["EPA", "EPA", "012", "EPA"],
                       "docket_id": ["EPA-1", "EPA-1", None, None],
                       "posted_date": ["2026-09-01", "2026-09-01", None, None],
                       "text_content": [text, "new", None, "unknown docket"]})
    stale = tmp_path / "comments/agency/agency_code=STALE/part-0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"stale")
    with duckdb.connect() as con:
        con.register("source", source)
        result = build(con, tmp_path, ExportResources("128MB", 1, row_group_bytes="1MB"))
        flat = pq.read_table(result["comments"])
        assert flat.schema == source.schema
        assert sorted(flat.to_pylist(), key=lambda r: r["comment_id"]) == sorted(source.to_pylist(), key=lambda r: r["comment_id"])
        epa = pq.ParquetFile(result["partitions"] / "agency_code=EPA/part-0.parquet").read()
        assert epa["comment_id"].to_pylist() == ["a", "z", "c"]
        assert "agency_code" not in epa.column_names
        assert not stale.exists()
        assert (result["partitions"] / "agency_code=012/part-0.parquet").exists()


@pytest.mark.parametrize("agency", [None, "__HIVE_DEFAULT_PARTITION__", "../bad"])
def test_invalid_partition_preserves_previous_files(tmp_path, agency):
    old = tmp_path / "comments/agency/agency_code=EPA/part-0.parquet"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    with duckdb.connect() as con:
        con.register("source", pa.table({"comment_id": ["a"], "agency_code": pa.array([agency], type=pa.string()),
                                         "docket_id": ["EPA-1"], "posted_date": ["2026-09-01"]}))
        with pytest.raises(ValueError, match="invalid coordinates"):
            build(con, tmp_path, ExportResources("64MB", 1))
    assert old.read_bytes() == b"old"
    assert not list(tmp_path.glob("comments-build-*"))


@pytest.fixture
def publication(tmp_path, monkeypatch):
    snapshot = iceberg.CatalogPairSnapshot("table-uuid", 41, 0, iceberg.CatalogSnapshot("receipts-uuid", 51, 0))
    state: dict = {"snapshot": snapshot, "objects": {}, "index": {"format": "spicy-regs-publication", "version": 2, "families": {}}, "receipt": None, "builds": 0, "uploads": [], "fail": None, "agency": "EPA", "text": "first"}
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setattr(mirror, "resolve_r2_base_url", lambda: "https://public.example")
    monkeypatch.setattr(mirror, "MIN_EXPECTED_ROWS", 1)
    state['catalog_prepared'] = 0
    def prepare_catalog(_):
        state['catalog_prepared'] += 1
    monkeypatch.setattr(mirror, '_prepare_catalog', prepare_catalog)
    monkeypatch.setattr(iceberg, "catalog_snapshot", lambda _: state["snapshot"])
    monkeypatch.setattr(iceberg, "rows_unchanged_since", lambda rt, pinned: iceberg.catalog_snapshot(rt) == pinned)
    monkeypatch.setattr(mirror.r2, "read_json_object", lambda _: state["receipt"])
    monkeypatch.setattr(mirror.r2, "object_version", lambda key: state["objects"].get(key))
    monkeypatch.setattr(mirror.publication, "public_object_version", lambda url, **_: state["objects"].get(url.removeprefix("https://public.example/")))
    state["objects"]["comments.parquet"] = {"etag": "prior", "bytes": 99}
    monkeypatch.setattr(mirror, "validate_export", lambda *a, **kw: mirror.PublicPredecessor(state["objects"]["comments.parquet"]["etag"], frozenset({"EPA"})))
    monkeypatch.setattr(mirror.r2, "preflight_uploads", lambda *a: None)
    monkeypatch.setattr(mirror.publication, "current_index", lambda _: state["index"])
    monkeypatch.setattr(mirror.r2, "get_r2_client", lambda: object())
    def publish(directory, **kwargs):
        from spicy_regs.generations import verify_generation
        artifact = verify_generation(directory)
        entry = {"artifactDigest": artifact.root["artifactDigest"], "tables": artifact.root["spec"]["tables"],
                 "etlReceipts": artifact.root["spec"]["etlReceipts"]}
        state["index"] = {**state["index"], "families": {"comments": entry}}
        return state["index"]
    monkeypatch.setattr(mirror.publication, "publish_generation", publish)

    def export(root, rt, **kw):
        state["builds"] += 1
        root.mkdir(parents=True, exist_ok=True)
        from spicy_regs.etl_receipts import ReceiptContext
        from spicy_regs.transforms.regulations_receipts import write_records
        from uuid import uuid4
        pair = root / ".catalog-pairs" / uuid4().hex
        generation = "snapshot-" + str(state["snapshot"].snapshot_id)
        row = dict.fromkeys(COMMENT.schema)
        row.update(comment_id="a", agency_code=state["agency"], text_content=state["text"])
        witness = dict(source_id="fixture", source_uri=None, sha256="sha256:" + "a" * 64,
                       locator=None, body_version=None)
        subject, receipts = write_records("comments", [(row, ReceiptContext(generation, "row-a", "fixture", [witness]))], pair)
        marker = pair / "generation.json"
        marker.write_text(json.dumps(dict(generation_id=generation, dataset="comments", snapshot=asdict(state["snapshot"]))))
        with duckdb.connect() as con:
            con.register("source", pq.read_table(subject))
            result = build(con, root, ExportResources("64MB", 1))
        return {**result, "receipts": receipts, "generation": marker}


    def upload(path, remote_key=None, **kw):
        key = remote_key or path.name
        if state["fail"] == key:
            raise OSError("interrupted upload")
        data = path.read_bytes()
        state["objects"][key] = {"etag": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
        state["uploads"].append(key)
        if key == mirror.RECEIPT_KEY:
            state["receipt"] = json.loads(data)

    def verify(path, key, base_url):
        if state["fail"] == "readback":
            raise OSError("readback failed")
        return {**state["objects"][key], "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def partitions(root, files):
        for path in files:
            upload(path, path.relative_to(root).as_posix())
        upload(root / "comments_index.parquet")

    monkeypatch.setattr(iceberg, "export_public_comments", export)
    monkeypatch.setattr(mirror.r2, "upload_file", upload)
    monkeypatch.setattr(mirror.r2, "upload_comment_partitions", partitions)
    monkeypatch.setattr(mirror.r2, "verify_public_file", verify)
    return state, tmp_path


def test_exact_retry_uses_metadata_and_text_only_change_rebuilds(publication):
    state, root = publication
    assert mirror.publish_comments_mirror(root)
    uploads = list(state["uploads"])
    assert uploads[-1] == mirror.RECEIPT_KEY
    assert state["receipt"]["source"] == asdict(state["snapshot"])
    assert not mirror.publish_comments_mirror(root)
    assert state["builds"] == 1 and state["uploads"] == uploads
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["text"] = "repaired attachment text"
    assert mirror.publish_comments_mirror(root)
    assert state["builds"] == 2
    assert pq.read_table(root / "comments.parquet")["text_content"].to_pylist() == [state["text"]]


@pytest.mark.parametrize("change", ["missing", "replaced", "schema", "table", "format", "inventory"])
def test_changed_publication_rebuilds(publication, change):
    state, root = publication
    mirror.publish_comments_mirror(root)
    key = "comments/agency/agency_code=EPA/part-0.parquet"
    if change == "missing":
        del state["objects"][key]
    elif change == "replaced":
        state["objects"][key] = {"etag": "different", "bytes": state["objects"][key]["bytes"]}
    elif change == "schema":
        state["snapshot"] = replace(state["snapshot"], schema_id=1)
    elif change == "table":
        state["snapshot"] = replace(state["snapshot"], table_uuid="new-table")
    elif change == "format":
        state["receipt"]["format_version"] = 0
    elif change == "inventory":
        del state["receipt"]["files"][key]
    assert mirror.publish_comments_mirror(root)
    assert state["builds"] == 2


@pytest.mark.parametrize("failure", ["comments_index.parquet", "readback"])
def test_failed_publication_does_not_advance_receipt_and_retry_finishes(publication, failure):
    state, root = publication
    mirror.publish_comments_mirror(root)
    original = state["receipt"]
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["fail"] = failure
    with pytest.raises(OSError):
        mirror.publish_comments_mirror(root)
    assert state["receipt"] is original
    state["fail"] = None
    assert mirror.publish_comments_mirror(root)
    assert state["receipt"]["source"]["snapshot_id"] == 42


def test_skip_upload_builds_without_changing_receipt(publication):
    state, root = publication
    mirror.publish_comments_mirror(root)
    original, uploads = state["receipt"], list(state["uploads"])
    assert mirror.publish_comments_mirror(root, skip_upload=True)
    assert state["builds"] == 2 and state["uploads"] == uploads
    assert state["receipt"] is original
    assert json.loads((root / "comments-build.json").read_text())["source"] == asdict(state["snapshot"])


def test_moving_catalog_refuses_before_upload(publication, monkeypatch):
    state, root = publication
    values = iter([state["snapshot"], replace(state["snapshot"], snapshot_id=42)])
    monkeypatch.setattr(iceberg, "catalog_snapshot", lambda _: next(values))
    with pytest.raises(RuntimeError, match="Catalog changed"):
        mirror.publish_comments_mirror(root)
    assert state["uploads"] == []


def test_compaction_during_export_publishes_the_pinned_snapshot(publication, monkeypatch):
    """The catalog moved past the pinned snapshot by compaction alone (``iceberg._compacted_only``)."""
    state, root = publication
    checked = []
    monkeypatch.setattr(iceberg, "rows_unchanged_since", lambda rt, s: checked.append(s) or True)
    assert mirror.publish_comments_mirror(root)
    assert checked == [state["snapshot"]]
    assert state["receipt"]["source"] == asdict(state["snapshot"])


def test_force_can_recover_from_an_unreadable_receipt(publication, monkeypatch):
    state, root = publication
    monkeypatch.setattr(mirror.r2, "read_json_object", lambda _: pytest.fail("force read corrupt receipt"))
    assert mirror.publish_comments_mirror(root, force=True)
    assert state["receipt"] is not None


def test_agency_move_clears_old_url_and_keeps_a_valid_receipt(publication):
    state, root = publication
    mirror.publish_comments_mirror(root)
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["agency"] = "FDA"
    assert mirror.publish_comments_mirror(root)
    old = "comments/agency/agency_code=EPA/part-0.parquet"
    new = "comments/agency/agency_code=FDA/part-0.parquet"
    assert pq.ParquetFile(root / old).metadata.num_rows == 0
    assert state["receipt"]["files"][old]["rows"] == 0
    assert state["receipt"]["files"][new]["rows"] == 1
    assert not mirror.publish_comments_mirror(root)


_EXPORT = iceberg.export_public_comments  # the publication fixture fakes it; the member tests below run the real one
_SUBMITTER = ("subtype", "duplicate_comments")


def _comment(comment_id, agency, subtype, count, comment=None):
    row = dict.fromkeys(COMMENT.schema)
    row.update(comment_id=comment_id, agency_code=agency, docket_id=f"{agency}-2025-1", posted_date="2025-01-15T00:00:00Z",
               modify_date="2025-01-15T00:00:00Z", subtype=subtype, duplicate_comments=count, comment=comment)
    return row


def _footer(path) -> dict[str, tuple[str, str, str]]:
    """Each column's Arrow type, Parquet physical type and DuckDB type (DocSpec's lens), in footer order."""
    parquet = pq.ParquetFile(path)
    leaves = {parquet.metadata.schema.column(i).name: parquet.metadata.schema.column(i).physical_type
              for i in range(parquet.metadata.num_columns)}
    with duckdb.connect() as con:
        described = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    return {field.name: (str(field.type), leaves.get(field.name, "GROUP"), described[i][1])
            for i, field in enumerate(parquet.schema_arrow)}



@pytest.fixture(params=["populated", "null"])
def published(request, publication, monkeypatch):
    """Publish a native catalog pair through the real export.

    EPA covers reported and all-null submitter values; CMS is all-null, and
    DOT's empty compatibility partition clears its previous fixed URL.
    """
    state, root = publication
    catalog = root / "catalog"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{catalog / 'catalog.duckdb'}' AS {iceberg._CATALOG_ALIAS}")
        return con

    monkeypatch.setattr(iceberg, "_connect", connect)
    rows = {"EPA": [_comment("EPA-1", "EPA", "Mass Mail Campaign", 15851, "I&#39;m writing<br/>&amp; asking."),
                    _comment("EPA-2", "EPA", "Public Comment", 0)],
            "CMS": [_comment("CMS-1", "CMS", None, None), _comment("CMS-2", "CMS", None, None)]}
    if request.param == "null":
        for agency_rows in rows.values():
            for row in agency_rows:
                row.update(subtype=None, duplicate_comments=None)
    for agency, agency_rows in rows.items():
        write_staging(agency, COMMENT.name, agency_rows, catalog / "staging", COMMENT.schema)
    iceberg.merge_comments(catalog / "staging", COMMENT)
    scans = []
    from spicy_regs.sources import regulatory_catalog
    original_export_pair = regulatory_catalog.export_pair
    def capture_export(con, rt, directory, **kwargs):
        scans.append(kwargs["snapshot"])
        return original_export_pair(con, rt, directory, **kwargs)
    monkeypatch.setattr(regulatory_catalog, "export_pair", capture_export)
    monkeypatch.setattr(iceberg, "_read_pair_snapshot", lambda con, rt: state["snapshot"])
    monkeypatch.setattr(iceberg, "export_public_comments", _EXPORT)
    monkeypatch.setattr(mirror, "validate_export", lambda *a, **kw: mirror.PublicPredecessor("prior", frozenset({"EPA", "DOT"})))
    assert mirror.publish_comments_mirror(root, resources=ExportResources("64MB", 1))
    return request.param, state, root, scans


def test_duplicate_comments_is_int32_in_every_published_member(published):
    """spicy-docs 0.52.0 types it INTEGER; DocSpec refuses a BIGINT footer."""
    _, state, root, _ = published
    tables = [key for key in state["receipt"]["files"] if key != "comments_index.parquet"]
    assert len(tables) == 4
    for key in tables:
        assert _footer(root / key)["duplicate_comments"] == ("int32", "INT32", "INTEGER"), key


def test_submitter_fields_are_typed_where_every_value_is_null(published):
    """An untyped NULL column is refused: an agency (or a snapshot) that states neither field still writes them typed."""
    shape, state, root, _ = published
    stated = shape == "populated"
    flat = {row["comment_id"]: (row["subtype"], row["duplicate_comments"]) for row in pq.read_table(root / "comments.parquet").to_pylist()}
    assert flat == {"EPA-1": ("Mass Mail Campaign", 15851) if stated else (None, None),
                    "EPA-2": ("Public Comment", 0) if stated else (None, None),
                    "CMS-1": (None, None), "CMS-2": (None, None)}
    for agency in ("CMS", "DOT") if stated else ("CMS", "DOT", "EPA"):
        path = root / f"comments/agency/agency_code={agency}/part-0.parquet"
        assert all(value is None for row in pq.read_table(path, columns=list(_SUBMITTER)).to_pylist() for value in row.values())
        footer = _footer(path)
        assert footer["subtype"] == ("string", "BYTE_ARRAY", "VARCHAR"), agency
        assert footer["duplicate_comments"] == ("int32", "INT32", "INTEGER"), agency


def test_every_member_is_republished_together_from_the_pinned_snapshot(published):
    """DocSpec refuses a member set whose footers disagree; the index must count the members' own snapshot."""
    _, state, root, scans = published
    receipt = state["receipt"]
    assert scans == [state["snapshot"]] and receipt["source"] == asdict(state["snapshot"])
    agencies = {code: f"comments/agency/agency_code={code}/part-0.parquet" for code in ("CMS", "DOT", "EPA")}
    assert receipt["files"].keys() == {"comments.parquet", "comments_index.parquet", *agencies.values()}
    flat = _footer(root / "comments.parquet")
    from spicy_regs.transforms.regulations_receipts import policy
    assert list(flat) == policy("comments").subject_schema.names
    del flat["agency_code"]  # the partition key, carried by each agency file's path
    for key in agencies.values():
        assert list(_footer(root / key).items()) == list(flat.items()), key
    # Every member counts the pinned rows; none sees the write that landed after the pin.
    with duckdb.connect() as con:
        index = dict(con.execute("SELECT agency_code, sum(row_count)::INTEGER FROM read_parquet(?) GROUP BY 1",
                                 [str(root / "comments_index.parquet")]).fetchall())
    assert index == {"CMS": 2, "EPA": 2}
    assert {code: receipt["files"][key]["rows"] for code, key in agencies.items()} == {**index, "DOT": 0}
    assert receipt["files"]["comments.parquet"]["rows"] == sum(index.values())


def test_comment_text_is_the_body_read_as_plain_text_in_every_member(published):
    """The publisher serves the body as HTML; the mirror adds its text and keeps the publisher's bytes beside it."""
    _, state, root, _ = published
    flat = {row["comment_id"]: (row["comment"], row["comment_text"])
            for row in pq.read_table(root / "comments.parquet", columns=["comment_id", "comment", "comment_text"]).to_pylist()}
    assert flat["EPA-1"] == ("I&#39;m writing<br/>&amp; asking.", "I'm writing\n& asking.")
    assert flat["EPA-2"] == (None, None)
    epa = pq.read_table(root / "comments/agency/agency_code=EPA/part-0.parquet", columns=["comment_id", "comment_text"])
    assert dict(zip(*epa.to_pydict().values())) == {"EPA-1": "I'm writing\n& asking.", "EPA-2": None}
    assert all(_footer(root / key)["comment_text"] == ("string", "BYTE_ARRAY", "VARCHAR")
               for key in state["receipt"]["files"] if key != "comments_index.parquet")


def _member(path, rows: int, row_group: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"comment_id": [str(i) for i in range(rows)]}), path, row_group_size=row_group)
    return {"rows": rows}


def test_every_receipt_admission_in_the_mirror_is_bounded_by_its_resources(published, monkeypatch):
    """The paired export's admission and the generation's each insert byte-sized slices under the mirror's limits.

    Hosted run 42738947666 admitted the exported subjects as one statement at the bulk default (4 threads, 4GB)
    and ran out of memory encoding an 89 MiB comment beside 2.8 GiB of other rows (ss-7flp).
    """
    from spicy_regs import etl_bulk
    _, _, root, _ = published
    admissions = []
    scan = etl_bulk._scan

    def recorded(con, *args, **kwargs):
        limits = con.execute("SELECT current_setting('threads'), current_setting('memory_limit')").fetchone()
        admissions.append((kwargs.get("bounded_insert", False), *limits))
        return scan(con, *args, **kwargs)

    monkeypatch.setattr(etl_bulk, "_scan", recorded)
    assert mirror.publish_comments_mirror(root, resources=ExportResources("512MB", 2), skip_upload=True)
    # At least the export's receipts and subjects, then the generation's receipts, comments and index.
    assert len(admissions) >= 5
    assert set(admissions) == {(True, 2, "488.2 MiB")}


def test_a_receipt_whose_agency_rows_do_not_sum_to_the_monolith_is_refused(tmp_path):
    files = {"comments.parquet": _member(tmp_path / "comments.parquet", 3, 2),
             "comments_index.parquet": _member(tmp_path / "comments_index.parquet", 1, 1),
             "comments/agency/agency_code=EPA/part-0.parquet":
                 _member(tmp_path / "comments/agency/agency_code=EPA/part-0.parquet", 2, 1)}
    with pytest.raises(RuntimeError, match="agency files hold 2 rows; the export validated 3"):
        mirror.check_receipt_rows(tmp_path, files, 3)
    files["comments/agency/agency_code=CMS/part-0.parquet"] = _member(
        tmp_path / "comments/agency/agency_code=CMS/part-0.parquet", 1, 1)
    mirror.check_receipt_rows(tmp_path, files, 3)
    with pytest.raises(RuntimeError, match="comments.parquet states 3 rows; the export validated 4"):
        mirror.check_receipt_rows(tmp_path, files, 4)
    files["comments_index.parquet"]["rows"] = 2
    with pytest.raises(RuntimeError, match="comments_index.parquet states 2 rows and its row groups hold 1"):
        mirror.check_receipt_rows(tmp_path, files, 3)


def test_a_refused_receipt_uploads_nothing(publication, monkeypatch):
    """The row check runs on the local files before any upload, so a refusal leaves the public files untouched."""
    state, root = publication
    monkeypatch.setattr(mirror, "check_receipt_rows", lambda *a: (_ for _ in ()).throw(RuntimeError("Refusing the comments receipt: test")))
    with pytest.raises(RuntimeError, match="Refusing the comments receipt"):
        mirror.publish_comments_mirror(root)
    assert state["uploads"] == [] and state["receipt"] is None


@pytest.mark.parametrize('changed_table', ['subjects', 'receipts'])
@pytest.mark.parametrize('when', ['before-export', 'paired-export'])
def test_export_refuses_a_catalog_moved_from_the_pinned_snapshot(tmp_path, monkeypatch, changed_table, when):
    from spicy_regs.sources import regulatory_catalog
    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        regulatory_catalog.ensure_native(con, COMMENT)
        return con
    monkeypatch.setattr(iceberg, "_connect", connect)
    pinned = iceberg.CatalogPairSnapshot("table-uuid", 41, 0, iceberg.CatalogSnapshot("receipts-uuid", 51, 0))
    moved = (replace(pinned, snapshot_id=42) if changed_table == 'subjects'
             else replace(pinned, receipts=replace(pinned.receipts, snapshot_id=52)))
    snapshots = iter([moved] if when == 'before-export' else [pinned, moved])
    monkeypatch.setattr(iceberg, "_read_pair_snapshot", lambda con, rt: next(snapshots))
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda *a: pytest.fail("scanned a snapshot other than the pinned one"))
    with pytest.raises(RuntimeError, match="changed before.*export"):
        iceberg.export_public_comments(tmp_path, COMMENT, snapshot=pinned)
    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize("fault", ["missing", "tampered", "generation", "snapshot"])
def test_receipt_fault_refuses_before_any_publication(publication, monkeypatch, fault):
    state, root = publication
    export = iceberg.export_public_comments
    def broken(*args, **kwargs):
        result = export(*args, **kwargs)
        if fault == "missing":
            result["receipts"].unlink()
        elif fault == "tampered":
            result["receipts"].write_bytes(b"invalid receipt bytes")
        else:
            metadata = json.loads(result["generation"].read_text())
            if fault == "generation":
                metadata["generation_id"] = ""
            else:
                metadata["snapshot"]["snapshot_id"] += 1
            result["generation"].write_text(json.dumps(metadata))
        return result
    monkeypatch.setattr(iceberg, "export_public_comments", broken)
    with pytest.raises((ValueError, OSError)):
        mirror.publish_comments_mirror(root)
    assert state["uploads"] == []
    assert state["index"]["families"] == {}


def test_mirror_selects_native_catalog_before_pinning_snapshot(publication, tmp_path, monkeypatch):
    state, _ = publication
    def snapshot(_):
        assert state['catalog_prepared'] == 1
        return state['snapshot']
    monkeypatch.setattr(iceberg, 'catalog_snapshot', snapshot)
    mirror.publish_comments_mirror(tmp_path, skip_upload=True)
    assert state['catalog_prepared'] == 1


def test_rejected_only_receipt_append_republishes_unchanged_subjects(publication, monkeypatch):
    """A source attempt is retained even when it produces no changed subject row."""
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, failure_receipt
    from spicy_regs.transforms.regulations_receipts import policy

    state, root = publication
    assert mirror.publish_comments_mirror(root)
    before_subject = (root / 'comments.parquet').read_bytes()
    before_rows = state['receipt']['generation']['etlReceipts']['rows']
    original_export = iceberg.export_public_comments

    def export_with_rejection(*args, **kwargs):
        result = original_export(*args, **kwargs)
        generation = json.loads(result['generation'].read_text())['generation_id']
        context = ReceiptContext(generation, 'rejected-attempt', 'fixture',
            [{'source_id': 'comments', 'source_uri': None, 'sha256': 'sha256:' + 'b' * 64,
              'locator': 'older source revision', 'body_version': None}])
        failed = failure_receipt(policy('comments'), context, outcome='rejected',
                                 raw_fields={'raw_source_record': {'comment_id': 'a', 'comment': 'older'}})
        rows = pq.read_table(result['receipts']).to_pylist()
        pq.write_table(pa.Table.from_pylist([*rows, failed], schema=RECEIPT_SCHEMA), result['receipts'])
        return result

    monkeypatch.setattr(iceberg, 'export_public_comments', export_with_rejection)
    state['snapshot'] = replace(state['snapshot'], receipts=replace(state['snapshot'].receipts, snapshot_id=52))
    assert mirror.publish_comments_mirror(root)
    assert state['builds'] == 2
    assert (root / 'comments.parquet').read_bytes() == before_subject
    assert state['receipt']['source']['snapshot_id'] == 41
    assert state['receipt']['source']['receipts']['snapshot_id'] == 52
    assert state['receipt']['generation']['etlReceipts']['rows'] == before_rows + 1
    assert not mirror.publish_comments_mirror(root)


def test_receipt_only_write_during_export_refuses_before_upload(publication, monkeypatch):
    state, root = publication
    original_export = iceberg.export_public_comments

    def changed(*args, **kwargs):
        result = original_export(*args, **kwargs)
        state['snapshot'] = replace(state['snapshot'], receipts=replace(state['snapshot'].receipts, snapshot_id=52))
        return result

    monkeypatch.setattr(iceberg, 'export_public_comments', changed)
    with pytest.raises(RuntimeError, match='Catalog changed during export'):
        mirror.publish_comments_mirror(root)
    assert state['uploads'] == []
    assert state['index']['families'] == {}
