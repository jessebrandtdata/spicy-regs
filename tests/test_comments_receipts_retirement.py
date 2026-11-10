"""Comments leave per-row ETL receipts (ss-juz8): the receipts-free export equals the receipt route's.

One small catalog is written through the ETL's own path (staging, ``merge_comments``, a text fill, a rejected and a
refused attempt), then exported twice from the same snapshot: through the paired export the mirror used until now
(``regulatory_catalog.export_pair``: receipts copied, rebound, admitted, every row replayed) and through the
receipts-free export, which must not read or write a receipt. The published comments, agency files and index must
be equal, and the receipt route's own receipts must attest every row the receipts-free route publishes.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow.parquet as pq
import pytest

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comments_mirror as mirror
from spicy_regs.pipelines.comments_generation import RETIRED_RECEIPT_DATASETS, build_comments_generation
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.sources import regulatory_catalog as native
from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies
from spicy_regs.transforms.regulations_receipts import policy
from spicy_regs.transforms.write_staging import write_staging

RESOURCES = ExportResources("128MB", 1, row_group_rows=2, row_group_bytes="1MB")
PINNED = iceberg.CatalogPairSnapshot("comments-uuid", 41, 0, iceberg.CatalogSnapshot("receipts-uuid", 51, 0))
ATTACHMENT = ('[{"title": "Letter", "formats": [{"url": "https://downloads.regulations.gov/EPA-1-0003/attachment_1.pdf",'
              ' "format": "pdf", "size": 52011}], "attachment_id": "EPA-1-0003-A1", "restrictReason": null,'
              ' "restrictReasonType": null}]')


def comment(comment_id, agency="EPA", **values):
    row = dict.fromkeys(COMMENT.schema)
    row.update(comment_id=comment_id, agency_code=agency, docket_id=f"{agency}-1", posted_date="2026-10-02T04:00:00Z",
               modify_date="2026-10-02T04:00:00Z", title=f"Comment {comment_id}")
    row.update(values)
    return row


#: Hand-checked rows: what each must publish, column by column, on both routes.
ROWS = [
    # Withdrawn by its submitter: the publisher keeps the record and clears its body; no attachment list is stated.
    comment("EPA-1-0001", title="Withdrawn Comment", comment=None, attachments_json="[]"),
    # A body in HTML and an empty text; its attachment list absent (NULL), not empty.
    comment("EPA-1-0002", comment="I&#39;m writing<br/>&amp; asking.", text_content="", subtype="Mass Mail Campaign",
            duplicate_comments=15851),
    # An attachment whose text Mirrulations derived.
    comment("EPA-1-0003", comment="See attached.", attachments_json=ATTACHMENT, text_content="Attached letter text",
            text_extraction_status="derived", pdf_extraction_results_json='{"source": "derived"}',
            duplicate_comments=0),
    # Empty body, no text yet; numeric-looking agency, no docket or date (NULL coordinates are published as such).
    comment("012-0004", agency="012", comment="", docket_id=None, posted_date=None, modify_date="2026-10-03T00:00:00Z"),
    comment("FAA-1-0005", agency="FAA", comment="Plain text, é😀 kept", organization="Acme Air"),
]


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    """A file-backed catalog the ETL wrote, with accepted, rejected, refused and initialization receipts."""
    path = tmp_path / "catalog.duckdb"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{path}' AS {iceberg._CATALOG_ALIAS}")
        return con

    monkeypatch.setattr(iceberg, "_connect", connect)
    staging = tmp_path / "staging"
    for agency in {row["agency_code"] for row in ROWS}:
        write_staging(agency, COMMENT.name, [row for row in ROWS if row["agency_code"] == agency], staging,
                      COMMENT.schema)
    assert iceberg.merge_comments(staging, COMMENT) == len(ROWS)
    with connect() as con:
        # A later text fill replaces one row, carrying its earlier receipt as history.
        iceberg.upsert_comment_text(con, COMMENT, "EPA", _fill(tmp_path))
    # An older observation of a published comment is kept only as a rejected attempt.
    older = tmp_path / "older"
    write_staging("EPA", COMMENT.name, [{**ROWS[1], "comment": "older text", "modify_date": "2020-01-01"}], older,
                  COMMENT.schema)
    assert iceberg.merge_comments(older, COMMENT) == 0
    # A row its mapper refuses changes nothing; the refused attempt is retained.
    refused = tmp_path / "refused"
    write_staging("EPA", COMMENT.name, [comment("EPA-1-0009", attachments_json="not-json")], refused, COMMENT.schema)
    with pytest.raises(ValueError, match="conversion refused"):
        iceberg.merge_comments(refused, COMMENT)
    with connect() as con:
        outcomes = dict(con.execute(f"SELECT outcome, count(*) FROM {native.receipts_table()} GROUP BY 1").fetchall())
    assert outcomes == {"accepted": len(ROWS), "observed": 1, "rejected": 1, "refused": 1}
    return connect


def _fill(tmp_path):
    import pyarrow as pa
    path = tmp_path / "fill.parquet"
    pq.write_table(pa.table({"comment_id": ["EPA-1-0001"], "_new_text": pa.array([None], pa.string()),
                             "_new_status": ["empty"]}), path)
    return path


def receipt_route(connect, output: Path):
    """The mirror's export as it stood at a9b29898: the paired export, then the same staging, sort and assembly."""
    output.mkdir()
    with TemporaryDirectory(dir=output) as work:
        work = Path(work)
        con = connect()
        RESOURCES.configure(con, work / "spill")
        selected = native.export_pair(con, COMMENT, output / ".catalog-pairs" / "comments",
                                      generation_id="catalog-receipt-route")
        stage_comment_agencies(con, f"SELECT * FROM read_parquet('{selected.subjects[0]}')", work / "staging",
                               resources=RESOURCES)
        con.close()
        partitions = sort_comment_agencies(work / "staging", output, resources=RESOURCES)
        with duckdb.connect() as con:
            RESOURCES.configure(con, work / "spill")
            monolith = assemble_comments(con, partitions, output, policy("comments").subject_schema.names,
                                         resources=RESOURCES)
            con.from_parquet(str(monolith)).create_view("comments_export")
            index = iceberg._build_comments_index(con, COMMENT, output, source_sql="SELECT * FROM comments_export")
    return {"comments": monolith, "index": index, "partitions": partitions, "receipts": selected.receipts}


class NoReceiptReads:
    """A catalog connection that fails any statement naming the shared receipt table."""

    def __init__(self, con):
        self._con = con

    def _check(self, sql):
        if "etl_receipts" in sql:
            pytest.fail(f"the receipts-free export read the receipt table: {sql[:120]}")

    def execute(self, sql, *args, **kwargs):
        self._check(sql)
        return self._con.execute(sql, *args, **kwargs)

    def sql(self, sql, *args, **kwargs):
        self._check(sql)
        return self._con.sql(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


def receipts_free_route(connect, output: Path, monkeypatch):
    """The export the mirror runs now, with every receipt reader and writer made to fail."""
    from spicy_regs import etl_bulk, etl_receipts

    with monkeypatch.context() as guarded:
        guarded.setattr(iceberg, "_connect", lambda: NoReceiptReads(connect()))
        guarded.setattr(iceberg, "_read_pair_snapshot", lambda con, rt, **kw: PINNED)  # metadata, not rows
        guarded.setattr(iceberg, "_receipt_metadata", lambda *a, **kw: pytest.fail("read receipt metadata"))
        for module, names in ((native, ("export_pair", "initialized", "require_initialized", "processing_table",
                                        "rebind_receipt", "read_with_receipts", "ensure_native")),
                              (etl_receipts, ("rebind_receipt", "read_with_receipts", "write_dataset",
                                              "combine_receipts", "validate_receipt_bundle")),
                              (etl_bulk, ("_scan",))):
            for name in names:
                guarded.setattr(module, name, lambda *a, _name=name, **kw: pytest.fail(f"called {_name}"))
        result = iceberg.export_public_comments(output, COMMENT, snapshot=PINNED, resources=RESOURCES)
    assert result.keys() == {"comments", "index", "partitions"}
    assert not list(output.rglob("etl_receipts.parquet")) and not (output / ".catalog-pairs").exists()
    assert not list(output.glob("comments-export-*")), "the export's working copy outlived it"
    return result


def table(path):
    return pq.read_table(path)


def test_the_receipts_free_export_equals_the_receipt_routes_table(catalog, tmp_path, monkeypatch):
    from spicy_regs.etl_receipts import validate_receipt_bundle

    expected = receipt_route(catalog, tmp_path / "receipt-route")
    actual = receipts_free_route(catalog, tmp_path / "receipts-free", monkeypatch)

    # Same rows, same order, same schema: the declared policy's, nothing appended (no field left the receipts).
    assert table(actual["comments"]).schema.names == policy("comments").subject_schema.names
    assert table(actual["comments"]).equals(table(expected["comments"]))
    agencies = sorted(path.relative_to(actual["partitions"]) for path in actual["partitions"].rglob("*.parquet"))
    assert agencies == sorted(path.relative_to(expected["partitions"])
                              for path in expected["partitions"].rglob("*.parquet"))
    for agency in agencies:
        assert table(actual["partitions"] / agency).equals(table(expected["partitions"] / agency)), agency
    keys = [("agency_code", "ascending"), ("docket_id", "ascending"), ("year", "ascending"), ("month", "ascending")]
    assert table(actual["index"]).sort_by(keys).equals(table(expected["index"]).sort_by(keys))
    # Every published row is the one its receipt attests: the receipt route's own receipts admit the new table.
    validate_receipt_bundle({"comments": [actual["comments"]]}, [expected["receipts"]], [policy("comments")],
                            generation_id="catalog-receipt-route")

    rows = {row["comment_id"]: row for row in table(actual["comments"]).to_pylist()}
    assert set(rows) == {row["comment_id"] for row in ROWS}  # the rejected and the refused attempts publish nothing
    withdrawn = rows["EPA-1-0001"]
    assert (withdrawn["comment"], withdrawn["comment_text"], withdrawn["attachments"]) == (None, None, [])
    assert (withdrawn["text_content"], withdrawn["text_extraction_status"]) == (None, "empty")  # the fill
    html = rows["EPA-1-0002"]
    assert (html["comment"], html["comment_text"]) == ("I&#39;m writing<br/>&amp; asking.", "I'm writing\n& asking.")
    assert (html["text_content"], html["attachments"], html["duplicate_comments"]) == ("", None, 15851)
    attached = rows["EPA-1-0003"]
    assert attached["attachments"] == [{
        "title": "Letter", "attachment_id": "EPA-1-0003-A1", "restrict_reason": None, "restrict_reason_type": None,
        "formats": [{"url": "https://downloads.regulations.gov/EPA-1-0003/attachment_1.pdf", "format": "pdf",
                     "size": 52011}]}]
    assert (attached["text_content"], attached["text_extraction_status"], attached["pdf_extraction_results_json"]) == (
        "Attached letter text", "derived", '{"source": "derived"}')
    empty = rows["012-0004"]
    assert (empty["comment"], empty["comment_text"], empty["text_content"]) == ("", "", None)
    assert (empty["agency_code"], empty["docket_id"], empty["posted_date"]) == ("012", None, None)
    assert rows["FAA-1-0005"]["comment_text"] == "Plain text, é😀 kept"


def test_a_comments_generation_leaves_receipts_once_declared_and_never_takes_them_back(catalog, tmp_path, monkeypatch):
    """Publication against the fake bucket: the prior generation with receipts, then the receipts-free one.

    The installed policies stand in for the receipts kit's marker on the receipts-free side, as the Federal Register
    switch's test does; with them as shipped, the build itself refuses.
    """
    from spicy_regs import etl_policy_registry
    from spicy_regs.etl_receipts import combine_receipts, write_dataset
    from spicy_regs.generations import build_generation
    from spicy_regs.pipelines.comments_generation import _index_records
    from spicy_regs.sources.publication import PublicationError, empty_index, publish_generation
    from tests.generation_fakes import Store

    store = Store()
    expected = receipt_route(catalog, tmp_path / "receipt-route")
    # The prior: the mirror's generation as main built it, the shared member holding both datasets' receipts.
    with TemporaryDirectory(dir=tmp_path) as work:
        index, index_receipts = write_dataset(_index_records(expected["index"], "catalog-receipt-route", {
            "source_id": "comments", "source_uri": None, "sha256": None, "locator": None,
            "body_version": "catalog-receipt-route"}), Path(work) / "index", policy("comments_index"))
        receipts = combine_receipts([expected["receipts"], index_receipts], Path(work) / "etl_receipts.parquet")
        assert index is not None
        build_generation(tmp_path / "prior", family="comments", files=[expected["comments"], index],
                         expected_keys=("comments.parquet", "comments_index.parquet"), receipt_path=receipts,
                         receipt_policies=[policy("comments"), policy("comments_index")],
                         receipt_generation_id="catalog-receipt-route")
    prior = publish_generation(tmp_path / "prior", client=store, bucket="b", prior_index=empty_index())
    before = prior["families"]["comments"]
    assert before["etlReceipts"]["datasets"] == ["comments", "comments_index"]

    result = receipts_free_route(catalog, tmp_path / "receipts-free", monkeypatch)
    with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
        build_comments_generation(tmp_path / "as-shipped", result)
    installed = etl_policy_registry.installed_policies()
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {
        name: value for name, value in installed.items() if name not in RETIRED_RECEIPT_DATASETS})
    generation = build_comments_generation(tmp_path / "receipts-free", result)

    with pytest.raises(PublicationError, match="drops required ETL receipts"):
        publish_generation(generation, client=store, bucket="b", prior_index=prior)
    with pytest.raises(PublicationError, match="keeps its table and publishes no receipts"):
        publish_generation(generation, client=store, bucket="b", prior_index=prior,
                           retired_receipt_datasets=RETIRED_RECEIPT_DATASETS | {"comment_attributes"})
    after = publish_generation(generation, client=store, bucket="b", prior_index=prior,
                               retired_receipt_datasets=RETIRED_RECEIPT_DATASETS)["families"]["comments"]
    assert "etlReceipts" not in after
    assert {key: value["rows"] for key, value in after["tables"].items()} == {
        "comments.parquet": len(ROWS), "comments_index.parquet": table(expected["index"]).num_rows}
    assert after["tables"]["comments.parquet"]["columns"] == before["tables"]["comments.parquet"]["columns"]
    # The archive is untouched: the prior generation's receipt member is still stored under its own prefix.
    assert before["prefix"] + "/etl_receipts.parquet" in store.objects
    assert mirror.RETIRED_RECEIPT_DATASETS is RETIRED_RECEIPT_DATASETS
