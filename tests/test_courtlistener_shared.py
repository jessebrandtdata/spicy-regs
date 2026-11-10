"""SR04: installed source reader, table policies and failed-attempt cleanup.

Pins the CourtListener bulk reader end to end — exact retained bytes and
jurisdictions, strict CSV refusals replacing the old lossy admission, and
compressed-offset resume verified against the publisher's ETag and byte range —
plus the local tables' parity with the frozen mapping and that a failure after
flush leaves the prior output and the original error untouched.
"""

from __future__ import annotations

import bz2
import importlib
import io
import subprocess
import sys
from collections.abc import Generator
from datetime import date
from email.message import Message
from hashlib import sha256
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.courtlistener import http as owner_http
from spicy_docs.sources.courtlistener.bulk import CourtListenerBulkReader
from spicy_docs.sources.courtlistener.csv import CourtListenerCsvError

from tests._courtlistener_oracle import old_cluster, old_rows
from spicy_regs.transforms.build_court_opinion_clusters import _shape_bulk
from spicy_regs.transforms.court_scope import CourtScope, build_docket_court_map, court_jurisdictions

FIXTURE = Path(__file__).parent / "fixtures/courtlistener_bulk/courts-2026-06-30.csv.bz2"
DUMP_DATE = date(2026, 6, 30)


@pytest.fixture(autouse=True)
def unpaced(monkeypatch):
    """The reader waits CourtListener's published interval between pages; these tests run unpaced."""
    from spicy_regs.sources import courtlistener

    monkeypatch.setattr(courtlistener, "MIN_REQUEST_INTERVAL_SECONDS", 0.0)


def _dump(tmp_path: Path, body: bytes) -> Path:
    path = tmp_path / "dump.csv.bz2"
    path.write_bytes(bz2.compress(body))
    return path


def _encode_cell(value: str | None) -> str:
    # Independent publisher-dialect re-encoder, not a second source decoder.
    return "" if value is None else '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _encode_records(records: list[dict]) -> bytes:
    return (
        ",".join(records[0])
        + "\n"
        + "".join(",".join(_encode_cell(value) for value in row.values()) + "\n" for row in records)
    ).encode()


def test_retained_courts_exact_bytes_and_all_jurisdictions():
    compressed = FIXTURE.read_bytes()
    original = bz2.decompress(compressed)
    assert sha256(compressed).hexdigest() == "d5a7a5aa902cb4cdb1b99eb3e6a160867a77ce4b2401682291081499537ea8be"
    assert sha256(original).hexdigest() == "110a1578a24788b73a9d351992051b40cb8fcf1e95dee72dd5a42054f413e757"
    reader = CourtListenerBulkReader("courts", local_file=FIXTURE)
    rows = list(reader.iter_records())
    assert len(rows) == 3361
    assert all(len(row) == 20 for row in rows)
    assert sum(value is None for row in rows for value in row.values()) == 16096
    assert sum(value == "" for row in rows for value in row.values()) == 11808
    assert _encode_records(rows) == original
    assert reader.compressed_bytes == 81180
    assert reader.decompressed_bytes == 765809
    assert not reader.stopped_early
    previous = {row["id"]: row.get("jurisdiction") or "" for row in old_rows(original) if row.get("id")}
    assert court_jurisdictions(local_file=FIXTURE) == previous


@pytest.mark.parametrize("empty", [None, ""])
def test_cluster_metadata_matches_frozen_mapping_through_empty_and_null_fields(tmp_path, empty):
    records = [
        {
            "id": "11",
            "cluster_id": "101",
            "docket_id": "71",
            "type": "010combined",
            "plain_text": empty,
            "html_with_citations": '<p>Opinion \\"text\\"</p>',
            "case_name": "Doe, Inc.",
            "slug": "doe",
            "syllabus": empty,
        }
    ]
    body = _encode_records(records)
    [old] = old_rows(body)
    [new] = CourtListenerBulkReader("opinions", local_file=_dump(tmp_path, body)).iter_records()
    assert new == records[0]
    expected = {**old_cluster(old), "syllabus": empty, "raw_source_record": None}
    assert _shape_bulk(new) == expected
    assert _shape_bulk(new)["syllabus"] == empty


def test_docket_map_preserves_quoted_empty_source_fields(tmp_path):
    body = b'id,court_id,docket_number\n"1","",""\n"2",,\n"3","dcd",""\n"","dcd","ignored"\n'
    previous = old_rows(body)
    assert previous[0]["court_id"] is None and previous[0]["docket_number"] is None
    out = build_docket_court_map(tmp_path, dump_date=DUMP_DATE, local_file=_dump(tmp_path, body))
    assert pq.read_table(out).to_pylist() == [
        {"cl_docket_id": "1", "court_id": "", "docket_number": ""},
        {"cl_docket_id": "2", "court_id": None, "docket_number": None},
        {"cl_docket_id": "3", "court_id": "dcd", "docket_number": ""},
    ]
    scope = CourtScope.from_map(out, {"dcd": "FD"})
    assert scope.for_docket("1") == scope.for_docket("2") == (None, None, None)
    assert scope.for_docket("3") == ("dcd", "FD", "t")


def test_literal_unquoted_backslash_is_a_named_source_correction(tmp_path):
    body = b'id,notes\n"1",\\N\n'
    assert old_rows(body)[0]["notes"] == "N"
    [row] = CourtListenerBulkReader("courts", local_file=_dump(tmp_path, body)).iter_records()
    assert row["notes"] == "\\N"


@pytest.mark.parametrize(
    "body",
    [
        b'id,notes\n"1",\xff\n',
        b'id,notes\n"1",a,discarded\n',
        b'id,id\n"1","2"\n',
        b'id,notes\n"1","unfinished',
    ],
)
def test_strict_source_refusals_replace_lossy_old_admission(tmp_path, body):
    assert old_rows(body)  # Old decoding admitted a row after replacement or silent loss.
    with pytest.raises(CourtListenerCsvError):
        list(CourtListenerBulkReader("courts", local_file=_dump(tmp_path, body)).iter_records())


class _Response(io.BytesIO):
    def __init__(self, body: bytes, *, url: str, offset=0, fail_after=None, status=200):
        super().__init__(body)
        self.seek(offset)
        self.fail_after = fail_after
        self.status = status
        self.bytes_read = 0
        self.url = url
        self.headers = Message()
        self.headers["ETag"] = f'"{sha256(body).hexdigest()}"'
        self.headers["Content-Length"] = str(len(body) - offset)
        if status == 206:
            self.headers["Content-Range"] = f"bytes {offset}-{len(body) - 1}/{len(body)}"

    def geturl(self):
        return self.url

    def read(self, size=-1):
        if self.fail_after is not None and self.tell() >= self.fail_after:
            raise OSError("connection reset")
        result = super().read(min(size, 1024) if size >= 0 else 1024)
        self.bytes_read += len(result)
        return result


def _network(monkeypatch, payload, *, fail_after=None, resume_status=206, resume_mutation=None):
    handles = []
    ranges = []

    def urlopen(request, **kwargs):
        header = request.get_header("Range")
        offset = int(header.removeprefix("bytes=").removesuffix("-")) if header else 0
        ranges.append(offset)
        assert request.get_header("Accept-encoding") == "identity"
        assert request.get_header("If-match") == (f'"{sha256(payload).hexdigest()}"' if header else None)
        response = _Response(
            payload,
            url=request.full_url,
            offset=offset,
            fail_after=fail_after if not header else None,
            status=resume_status if header else 200,
        )
        if header and resume_mutation == "etag":
            response.headers.replace_header("ETag", '"changed-object"')
        if header and resume_mutation == "range":
            response.headers.replace_header("Content-Range", f"bytes {offset + 1}-{len(payload) - 1}/{len(payload)}")
        handles.append(response)
        return response

    monkeypatch.setattr(owner_http.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(owner_http.time, "sleep", lambda _: None)
    return handles, ranges


def test_public_reader_resumes_exact_compressed_offset_and_closes(monkeypatch):
    records = [{"id": str(i), "notes": f"row {i}"} for i in range(4000)]
    body = _encode_records(records)
    payload = bz2.compress(body)
    assert len(payload) > 4096
    handles, ranges = _network(monkeypatch, payload, fail_after=2048)
    reader = CourtListenerBulkReader("courts", dump_date=DUMP_DATE)
    assert list(reader.iter_records()) == records
    assert ranges == [0, 2048]
    assert reader.resumes == 1 and reader.compressed_bytes == len(payload)
    assert all(handle.closed for handle in handles)


def test_public_reader_refuses_incorrect_resume_and_closes(monkeypatch):
    payload = bz2.compress(_encode_records([{"id": str(i), "notes": f"row {i}"} for i in range(4000)]))
    handles, _ = _network(monkeypatch, payload, fail_after=2048, resume_status=200)
    with pytest.raises(RuntimeError, match="not 206"):
        list(CourtListenerBulkReader("courts", dump_date=DUMP_DATE).iter_records())
    assert all(handle.closed for handle in handles)


@pytest.mark.parametrize("mode", ["record", "compressed", "malformed", "consumer"])
def test_public_reader_bounds_and_failure_close_the_source(monkeypatch, mode):
    body = b'id,notes\n"1","one"\n"2","two"\n'
    if mode == "malformed":
        body += b'"3",extra,column\n'
    payload = bz2.compress(body)
    handles, _ = _network(monkeypatch, payload)
    reader = CourtListenerBulkReader(
        "courts",
        dump_date=DUMP_DATE,
        max_record_characters=8 if mode == "record" else 1024,
        max_compressed_bytes=len(payload) // 2 if mode == "compressed" else None,
    )
    iterator = cast(Generator[dict, None, None], reader.iter_records())
    if mode in {"record", "malformed"}:
        with pytest.raises(CourtListenerCsvError):
            list(iterator)
    elif mode == "consumer":
        assert next(iterator)["id"] == "1"
        iterator.close()
    else:
        assert list(iterator) == []
        assert reader.compressed_bytes == len(payload) // 2
    assert reader.stopped_early
    assert handles and all(handle.closed for handle in handles)


def test_concatenated_members_and_truncated_archive(tmp_path):
    path = tmp_path / "dump.bz2"
    path.write_bytes(bz2.compress(b'id,notes\n"1","one"\n') + bz2.compress(b'"2","two"\n'))
    assert [row["id"] for row in CourtListenerBulkReader("courts", local_file=path).iter_records()] == ["1", "2"]
    path.write_bytes(path.read_bytes()[:-8])
    with pytest.raises(EOFError, match="incomplete bzip2"):
        list(CourtListenerBulkReader("courts", local_file=path).iter_records())


@pytest.mark.parametrize("kind", ["clusters"])
@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_source_failure_after_flush_preserves_output_and_original_error(
    tmp_path, monkeypatch, kind, cleanup_failure, ample_disk_space
):
    module = importlib.import_module(f"spicy_regs.transforms.build_court_opinion_{kind}")
    monkeypatch.setattr(module, "BATCH_ROWS", 1)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    output = tmp_path / module.OUTPUT
    output.write_bytes(b"previous output stays untouched")
    dump = _dump(tmp_path, b'id,cluster_id,docket_id,plain_text\n"1","10","20","body"\n"2",bad,row,extra,column\n')
    closed = []
    original_writer = module.pq.ParquetWriter

    class Writer:
        def __init__(self, *args, **kwargs):
            self.writer = original_writer(*args, **kwargs)

        def write_table(self, table):
            self.writer.write_table(table)

        def close(self):
            self.writer.close()
            closed.append(True)
            if cleanup_failure:
                raise OSError("cleanup also failed")

    monkeypatch.setattr(module.pq, "ParquetWriter", Writer)
    kwargs = {"skip_court_scope": True} if kind == "clusters" else {}
    with pytest.raises(CourtListenerCsvError, match="expected 4 columns, got 5"):
        getattr(module, f"build_court_opinion_{kind}")(tmp_path, local_file=dump, **kwargs)
    assert closed == [True]
    assert output.read_bytes() == b"previous output stays untouched"
    assert not (tmp_path / f"_{kind}_new.parquet").exists()


@pytest.mark.parametrize(
    ("prior_created", "named_edition", "reads_export"),
    [
        ("2026-06-30 08:16:36.759906+00", False, False),
        ("2026-03-31 07:00:00+00", False, True),
        ("2026-06-30 08:16:36.759906+00", True, True),
    ],
    ids=["prior-holds-the-export", "prior-holds-an-older-export", "edition-named"],
)
def test_a_prior_holding_the_newest_export_reads_no_dump_and_the_build_never_searches(
    tmp_path, monkeypatch, prior_created, named_edition, reads_export, ample_disk_space
):
    """A build re-streamed 7 GiB of unchanged dumps for a prior that held them; it reads neither, and never Search."""
    import spicy_docs.sources.courtlistener.bulk as bulk

    monkeypatch.delenv("COURTLISTENER_API_TOKEN", raising=False)  # the build makes no Search request, so needs none
    from spicy_docs.sources.courtlistener.listing import BulkObject

    module = importlib.import_module("spicy_regs.transforms.build_court_opinion_clusters")
    prior = {**_shape_bulk({"id": "5", "docket_id": "1", "date_created": prior_created}), "court_id": "dcd"}
    from spicy_regs.court_receipts import write_court_rows
    witness = dict(source_id='fixture', source_uri=None, sha256='sha256:' + 'a' * 64,
                   locator=None, body_version=None)
    prior_path = write_court_rows('court_opinion_clusters', [prior], tmp_path, witnesses=[witness])
    assert module.held_export(prior_path) == (5, prior_created[:10])
    monkeypatch.setattr(bulk, "list_bulk_dumps", lambda: [])
    monkeypatch.setattr(bulk, "latest_dump_date", lambda objects, dataset: DUMP_DATE)
    listed = BulkObject("bulk-data/opinion-clusters-2026-06-30.csv.bz2", 1, '"etag"', "2026-06-30T12:00:00Z")
    monkeypatch.setattr(bulk, "find_dump", lambda objects, dataset, day: listed)
    monkeypatch.setattr(module, "court_jurisdictions", lambda **kwargs: {"dcd": "FD"})
    docket_map = tmp_path / "map.parquet"
    pq.write_table(pa.table({"cl_docket_id": ["1"], "court_id": ["dcd"]}), docket_map)
    monkeypatch.setattr(module, "build_docket_court_map", lambda *a, **k: docket_map)
    exported = []

    def export(self):
        exported.append(True)
        yield {"id": "8", "docket_id": "1", "date_created": "2026-06-30 08:00:00+00"}

    monkeypatch.setattr(CourtListenerBulkReader, "iter_records", export)
    monkeypatch.setattr(
        "spicy_regs.sources.courtlistener.CourtListenerOpinionSearchReader.iter_records",
        lambda self: pytest.fail("the bulk build searched; the daily catch-up owns Search (ss-jran)"),
    )
    out = module.build_court_opinion_clusters(tmp_path, dump_date=DUMP_DATE if named_edition else None)

    from spicy_regs.court_receipts import read_court_rows
    rows = {row["cluster_id"]: row for row in read_court_rows(out, dataset="court_opinion_clusters")}
    assert bool(exported) is reads_export
    assert sorted(rows) == (["5", "8"] if reads_export else ["5"])
    assert rows["5"]["ingest_source"] == "bulk"


@pytest.mark.parametrize("kind", ["clusters"])
@pytest.mark.parametrize("failure", ["shape", "write"])
def test_receiving_failure_closes_source_iterator(tmp_path, monkeypatch, kind, failure, ample_disk_space):
    module = importlib.import_module(f"spicy_regs.transforms.build_court_opinion_{kind}")
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    closed = []

    def records(self):
        try:
            yield {"id": "1"}
            yield {"id": "2"}
        finally:
            closed.append(True)

    def fail(*args, **kwargs):
        raise ValueError("shaping failed")

    monkeypatch.setattr(CourtListenerBulkReader, "iter_records", records)
    if failure == "shape":
        monkeypatch.setattr(module, "_shape_bulk" if kind == "clusters" else "_shape", fail)
    else:
        monkeypatch.setattr(module.CourtListenerTableWriter, "add", fail)
    kwargs = {"skip_court_scope": True} if kind == "clusters" else {}
    (tmp_path / "unused").touch()
    with pytest.raises(ValueError, match="shaping failed"):
        getattr(module, f"build_court_opinion_{kind}")(tmp_path, local_file=tmp_path / "unused", **kwargs)
    assert closed == [True]
    assert not (tmp_path / module.OUTPUT).exists()
    assert not (tmp_path / f"_{kind}_new.parquet").exists()


def test_base_imports_do_not_require_source_readers():
    script = """
import importlib.abc
import sys
class NoSourceReaders(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "spicy_docs" or fullname.startswith("spicy_docs."):
            raise ModuleNotFoundError("source-readers deliberately absent", name=fullname)
sys.meta_path.insert(0, NoSourceReaders())
import spicy_regs.cli
import spicy_regs.mcp_server
import spicy_regs.transforms
import spicy_regs.pipelines.rollups.court_opinion_clusters
import spicy_regs.pipelines.rollups.court_citations
import spicy_regs.pipelines.rollups.court_opinions
assert not any(name == "spicy_docs" or name.startswith("spicy_docs.") for name in sys.modules)
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_docket_map_failure_after_flush_discards_only_failed_attempt(tmp_path, monkeypatch, cleanup_failure):
    module = importlib.import_module("spicy_regs.transforms.court_scope")
    monkeypatch.setattr(module, "BATCH_ROWS", 1)
    previous = tmp_path / "docket_courts-2026-03-31.parquet"
    previous.write_bytes(b"prior map")
    dump = _dump(tmp_path, b'id,court_id,docket_number\n"1","dcd","20"\n"2",bad,row,extra\n')
    original_writer = module.pq.ParquetWriter
    closed = []

    class Writer:
        def __init__(self, *args, **kwargs):
            self.writer = original_writer(*args, **kwargs)

        def write_table(self, table):
            self.writer.write_table(table)

        def close(self):
            self.writer.close()
            closed.append(True)
            if cleanup_failure:
                raise OSError("footer also failed")

    monkeypatch.setattr(module.pq, "ParquetWriter", Writer)
    with pytest.raises(CourtListenerCsvError, match="expected 3 columns, got 4"):
        build_docket_court_map(tmp_path, dump_date=DUMP_DATE, local_file=dump)
    assert closed == [True]
    assert previous.read_bytes() == b"prior map"
    assert not (tmp_path / "docket_courts-2026-06-30.parquet").exists()
    assert not list(tmp_path.glob("*.partial.parquet"))


def test_large_source_field_matches_old_reader_and_restores_test_limit(tmp_path):
    import csv

    original_limit = csv.field_size_limit(1024)
    try:
        body = _encode_records([{"id": "1", "notes": "large body " * 20000}])
        old = old_rows(body)
        assert csv.field_size_limit() == 1024
        reader = CourtListenerBulkReader("opinions", local_file=_dump(tmp_path, body))
        assert list(reader.iter_records()) == old
    finally:
        csv.field_size_limit(original_limit)


def test_docket_receipt_uses_shared_listing_pin_including_exact_etag(tmp_path, monkeypatch):
    from spicy_regs.transforms.court_scope import write_map_receipt
    import json

    listing = b"""<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>com-courtlistener-storage</Name><Prefix>bulk-data/</Prefix>
<IsTruncated>false</IsTruncated><Contents>
<Key>bulk-data/dockets-2026-06-30.csv.bz2</Key><Size>123</Size>
<ETag>"publisher-etag"</ETag><LastModified>2026-06-30T09:00:00Z</LastModified>
</Contents></ListBucketResult>"""
    requests = []

    def urlopen(request, **kwargs):
        requests.append(request.full_url)
        return io.BytesIO(listing)

    monkeypatch.setattr(owner_http.urllib.request, "urlopen", urlopen)
    path = tmp_path / "map.parquet"
    path.write_bytes(b"local map result")
    reader = CourtListenerBulkReader("dockets", dump_date=DUMP_DATE)
    receipt = json.loads(write_map_receipt(path, reader=reader, dump_date=DUMP_DATE, rows=1).read_text())
    assert len(requests) == 1
    assert requests[0].startswith("https://com-courtlistener-storage.s3.amazonaws.com/?")
    assert receipt["source"]["bytes"] == 123
    assert receipt["source"]["etag"] == '"publisher-etag"'
    assert receipt["source"]["last_modified"] == "2026-06-30T09:00:00Z"
    assert receipt["source"]["listing_object_count"] == 1


@pytest.mark.parametrize("kind", ["clusters"])
@pytest.mark.parametrize("row_count", [0, 1, 3])
def test_local_table_build_matches_frozen_mapping_by_id(tmp_path, monkeypatch, kind, row_count, ample_disk_space):
    module = importlib.import_module(f"spicy_regs.transforms.build_court_opinion_{kind}")
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    monkeypatch.setattr(module, "BATCH_ROWS", 2)
    records = [
        {"id": "11", "cluster_id": "101", "docket_id": "71", "case_name": "Doe", "plain_text": "", "syllabus": None},
        {
            "id": "12",
            "cluster_id": "102",
            "docket_id": "72",
            "case_name": "Roe",
            "plain_text": "line one\nline two",
            "syllabus": "",
        },
    ]
    records.append({**records[0], "id": "13", "cluster_id": "103", "docket_id": "73"})
    raw = _encode_records(records[:row_count]) if row_count else (",".join(records[0]) + "\n").encode()
    kwargs = {"skip_court_scope": True} if kind == "clusters" else {}
    output = getattr(module, f"build_court_opinion_{kind}")(
        tmp_path,
        local_file=_dump(tmp_path, raw),
        dump_date=DUMP_DATE,
        **kwargs,
    )
    from spicy_regs.court_subjects import SUBJECT_SCHEMAS
    expected = [old_cluster(row) for row in old_rows(raw)]
    for row, original in zip(expected, records[:row_count], strict=True):
        row["syllabus"] = original["syllabus"]
    expected = [{k: row[k] for k in SUBJECT_SCHEMAS["court_opinion_clusters"].names} for row in expected]
    key = "cluster_id"
    assert {row[key]: row for row in pq.read_table(output).to_pylist()} == {row[key]: row for row in expected}
    parquet = pq.ParquetFile(output)
    assert parquet.schema_arrow == SUBJECT_SCHEMAS["court_opinion_clusters"]
    assert parquet.metadata.num_rows == row_count


@pytest.mark.parametrize("mutation,match", [("etag", "ETag differs"), ("range", "exact remaining bytes")])
def test_resumed_object_change_refuses_before_accepting_more_bytes(monkeypatch, mutation, match):
    payload = bz2.compress(_encode_records([{"id": str(i), "notes": f"row {i}"} for i in range(4000)]))
    handles, ranges = _network(monkeypatch, payload, fail_after=2048, resume_mutation=mutation)
    with pytest.raises(owner_http.BulkTransferError, match=match):
        list(CourtListenerBulkReader("courts", dump_date=DUMP_DATE).iter_records())
    assert ranges == [0, 2048]
    assert handles[-1].bytes_read == 0  # The refused response body was not read.
    assert all(handle.closed for handle in handles)


@pytest.mark.parametrize("status", [401, 403])
def test_access_refusal_closes_without_retry(monkeypatch, status):
    from urllib.error import HTTPError
    from spicy_docs.transport.credentials import CredentialRefusedError

    body = io.BytesIO(b"access refused")
    calls = []

    def urlopen(request, **kwargs):
        calls.append(request.full_url)
        raise HTTPError(request.full_url, status, "access refused", Message(), body)

    monkeypatch.setattr(owner_http.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(owner_http.time, "sleep", lambda _: pytest.fail("retried access refusal"))
    with pytest.raises(CredentialRefusedError, match=f"HTTP {status}"):
        list(CourtListenerBulkReader("courts", dump_date=DUMP_DATE).iter_records())
    assert len(calls) == 1
    assert body.closed


def test_cluster_refused_update_keeps_valid_native_prior(tmp_path, monkeypatch, ample_disk_space):
    from spicy_regs.court_receipts import write_court_rows, local_receipt_selection
    module = importlib.import_module('spicy_regs.transforms.build_court_opinion_clusters')
    witness = dict(source_id='fixture', source_uri=None, sha256='sha256:' + 'a' * 64,
                   locator=None, body_version=None)
    prior = write_court_rows('court_opinion_clusters',
        [module._shape_bulk({'id': '5', 'case_name': 'Held', 'citation_count': '2'})],
        tmp_path, witnesses=[witness])
    before = pq.read_table(prior).to_pylist()[0]
    dump = _dump(tmp_path, b'id,docket_id,case_name,citation_count\n"5","101","Rejected","invalid"\n"6","102","New","1"\n')
    result = module.build_court_opinion_clusters(tmp_path, local_file=dump, dump_date=DUMP_DATE,
                                               skip_court_scope=True)
    rows = {row['cluster_id']: row for row in pq.read_table(result).to_pylist()}
    assert rows['5'] == before
    assert rows['6']['case_name'] == 'New'
    receipts = pq.read_table(local_receipt_selection(result)[0]).to_pylist()
    failed = [r for r in receipts if r['outcome'] == 'refused']
    assert len(failed) == 1 and 'Rejected' in failed[0]['processing_json']
