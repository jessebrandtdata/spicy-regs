"""The ordinary retained-card entry uses pinned archive evidence, never acquisition."""

import hashlib
import io
import json

import pyarrow.parquet as pq
import pytest
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from spicy_docs.sources.web_archive import payload_sha1, read_warc_response
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from tests.test_bill_family_cbo import CARD, NOTICE
from spicy_regs.transforms.cbo_archive_cards import build


def held(tmp_path, pages):
    archive = tmp_path / "raw"
    store = CentralRawArchiveStore(archive)
    keys = []
    for identifier, payload, timestamp in pages:
        url = f"https://www.cbo.gov/publication/{identifier}"
        stream = io.BytesIO()
        writer = WARCWriter(stream, gzip=True)
        record = writer.create_warc_record(
            url,
            "response",
            payload=io.BytesIO(payload),
            http_headers=StatusAndHeaders("200 OK", [("Content-Type", "text/html")], protocol="HTTP/1.1"),
            warc_headers_dict={"WARC-Date": timestamp},
        )
        writer.write_record(record)
        body = stream.getvalue()
        digest = hashlib.sha256(body).hexdigest()
        store.put_blob(digest, len(body), [body])
        response = read_warc_response(body)
        receipt = {
            "authority": "archive",
            "archive_operator": "Common Crawl",
            "dataset_id": "cbo-publication-page",
            "native_id": identifier,
            "original_publisher_url": url,
            "archive_timestamp": timestamp.translate(str.maketrans("", "", "-:TZ")),
            "index_sha256": "sha256:" + "1" * 64,
            "index_digest": payload_sha1(payload),
            "payload_sha1": payload_sha1(payload),
            "warc_filename": "crawl-data/CC-MAIN-2026-43/segments/example/warc/example.warc.gz",
            "crawl_id": "CC-MAIN-2026-43",
            "warc_offset": 0,
            "warc_length": len(body),
            "warc_record_id": response.record_id,
        }
        key = store.record_receipt(files=[{"sha256": digest, "bytes": len(body)}], receipt=receipt)
        keys.append(key["object"])
    manifest = tmp_path / "pages.json"
    manifest.write_text(json.dumps({"receipt_keys": keys}))
    return manifest, archive


def test_positive_absent_and_refused_preserve_provenance_and_values(tmp_path):
    manifest, archive = held(
        tmp_path,
        [
            ("62720", CARD, "2026-10-17T05:18:39Z"),
            ("62589", NOTICE, "2026-10-17T06:19:02Z"),
            ("62721", CARD, "2026-10-17T05:18:39Z"),
        ],
    )
    output = tmp_path / "candidate"
    build(manifest, archive, output)
    cards = pq.read_table(output / "cbo_estimate_cards.parquet").to_pylist()
    assert len(cards) == 1 and cards[0]["publication_id"] == "62720"
    windows = cards[0]["windows"]
    assert [w["period"] for w in windows] == ["2026", "2026-2031", "2026-2036"]
    assert [(w["revenues_literal"], w["revenues_millions"]) for w in windows] == [
        ("-5", -5),
        ("*", None),
        ("1,234", 1234),
    ]
    report = json.loads((output / "inputs.json").read_text())
    outcomes = {r["publication_id"]: r for r in report["outcomes"]}
    assert {k: v["outcome"] for k, v in outcomes.items()} == {"62720": "card", "62589": "absent", "62721": "refused"}
    assert all(v["source_sha256"] and v["receipt_object"] and v["source_locator"] for v in outcomes.values())
    assert "canonical" in outcomes["62721"]["reason"]


def test_missing_receipt_or_input_writes_no_candidate(tmp_path):
    manifest, archive = held(tmp_path, [("62720", CARD, "2026-10-17T05:18:39Z")])
    digest = json.loads((archive / json.loads(manifest.read_text())["receipt_keys"][0]).read_text())["files"][0][
        "sha256"
    ]
    (archive / "objects/sha256" / digest[:2] / digest).unlink()
    with pytest.raises(FileNotFoundError):
        build(manifest, archive, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_latest_timestamp_selected_and_conflicting_tie_refuses(tmp_path):
    manifest, archive = held(
        tmp_path,
        [("62720", CARD, "2026-10-17T05:18:39Z"), ("62720", CARD.replace(b"1,234", b"2,345"), "2026-10-18T05:18:39Z")],
    )
    build(manifest, archive, tmp_path / "candidate")
    row = pq.read_table(tmp_path / "candidate/cbo_estimate_cards.parquet").to_pylist()[0]
    assert row["captures"] == 2 and row["earliest_timestamp"] == "20261017051839"
    assert row["archive_timestamp"] == "20261018051839" and row["windows"][2]["revenues_millions"] == 2345
    other = tmp_path / "tie"
    other.mkdir()
    manifest, archive = held(
        other,
        [("62720", CARD, "2026-10-17T05:18:39Z"), ("62720", CARD.replace(b"1,234", b"2,345"), "2026-10-17T05:18:39Z")],
    )
    with pytest.raises(ValueError, match="Conflicting"):
        build(manifest, archive, other / "candidate")
    assert not (other / "candidate").exists()


def test_raw_destination_and_existing_output_refuse(tmp_path):
    manifest, archive = held(tmp_path, [("62720", CARD, "2026-10-17T05:18:39Z")])
    with pytest.raises(ValueError, match="outside"):
        build(manifest, archive, archive / "derived")
    out = tmp_path / "candidate"
    out.mkdir()
    (out / "foreign").write_text("keep")
    with pytest.raises(FileExistsError):
        build(manifest, archive, out)
    assert (out / "foreign").read_text() == "keep"


@pytest.mark.parametrize(
    "field,value",
    [
        ("native_id", "1"),
        ("archive_timestamp", "20250101000000"),
        ("index_digest", "A" * 32),
        ("warc_offset", -1),
        ("warc_offset", None),
        ("warc_offset", True),
        ("warc_length", None),
        ("warc_length", ""),
        ("warc_filename", None),
        ("warc_filename", ""),
        ("crawl_id", None),
        ("crawl_id", ""),
        ("native_id", None),
        ("native_id", ""),
    ],
)
def test_weak_receipt_identity_refuses_before_output(tmp_path, field, value):
    manifest, archive = held(tmp_path, [("62720", CARD, "2026-10-17T05:18:39Z")])
    document = json.loads((archive / json.loads(manifest.read_text())["receipt_keys"][0]).read_text())
    document["receipt"][field] = value
    key = CentralRawArchiveStore(archive).record_receipt(files=document["files"], receipt=document["receipt"])["object"]
    manifest.write_text(json.dumps({"receipt_keys": [key]}))
    with pytest.raises(ValueError, match="identity"):
        build(manifest, archive, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_failed_write_can_retry_without_partial_output(tmp_path, monkeypatch):
    manifest, archive = held(tmp_path, [("62720", CARD, "2026-10-17T05:18:39Z")])
    original = pq.write_table

    def fail(*args, **kwargs):
        raise OSError("injected disk failure")

    monkeypatch.setattr(pq, "write_table", fail)
    with pytest.raises(OSError, match="disk failure"):
        build(manifest, archive, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()
    monkeypatch.setattr(pq, "write_table", original)
    build(manifest, archive, tmp_path / "candidate")


@pytest.mark.parametrize("record_id", ["", " ", "not-a-record-id", "<urn:uuid:not-a-uuid>"])
def test_missing_or_malformed_warc_record_locator_refuses(tmp_path, record_id):
    import gzip
    import re

    manifest, archive = held(tmp_path, [("62720", CARD, "2026-10-17T05:18:39Z")])
    document = json.loads((archive / json.loads(manifest.read_text())["receipt_keys"][0]).read_text())
    original = (archive / document["files"][0]["object"]).read_bytes()
    replacement = b"" if record_id == "" else b"WARC-Record-ID: " + record_id.encode() + b"\r\n"
    changed, count = re.subn(rb"(?im)^WARC-Record-ID:[^\r\n]*\r?\n", replacement, gzip.decompress(original), count=1)
    assert count == 1
    body = gzip.compress(changed, mtime=0)
    digest = hashlib.sha256(body).hexdigest()
    store = CentralRawArchiveStore(archive)
    store.put_blob(digest, len(body), [body])
    document["receipt"].update(warc_record_id=record_id.strip(), warc_length=len(body))
    key = store.record_receipt(files=[{"sha256": digest, "bytes": len(body)}], receipt=document["receipt"])["object"]
    manifest.write_text(json.dumps({"receipt_keys": [key]}))
    with pytest.raises(ValueError, match="identity"):
        build(manifest, archive, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_matching_missing_publication_identity_is_not_an_absent_or_refused_card(tmp_path):
    import gzip
    import re

    manifest, archive = held(tmp_path, [("62720", CARD, "2026-10-17T05:18:39Z")])
    document = json.loads((archive / json.loads(manifest.read_text())["receipt_keys"][0]).read_text())
    original = (archive / document["files"][0]["object"]).read_bytes()
    changed, count = re.subn(
        rb"(?im)^WARC-Target-URI:[^\r\n]*", b"WARC-Target-URI: https://www.cbo.gov/", gzip.decompress(original), count=1
    )
    assert count == 1
    body = gzip.compress(changed, mtime=0)
    digest = hashlib.sha256(body).hexdigest()
    store = CentralRawArchiveStore(archive)
    store.put_blob(digest, len(body), [body])
    document["receipt"].update(native_id=None, original_publisher_url="https://www.cbo.gov/", warc_length=len(body))
    key = store.record_receipt(files=[{"sha256": digest, "bytes": len(body)}], receipt=document["receipt"])["object"]
    manifest.write_text(json.dumps({"receipt_keys": [key]}))
    with pytest.raises(ValueError, match="identity"):
        build(manifest, archive, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()
