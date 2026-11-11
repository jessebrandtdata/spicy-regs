"""Edition replay preserves native history and refuses partial or unpinned inputs."""

import hashlib
import io
import json
import zipfile

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import uscode_editions as reader
from spicy_docs.sources.uscode import UsCodeSourceError
from spicy_docs.sources.uscode.annual import ANNUAL_FIELDS


def annual(year, title="1", tail=""):
    values = (
        f"{year} Main Edition",
        "NATIVE-ID",
        str(year),
        "20150101",
        "January 1, 2015",
        f"TITLE {title}",
        title,
        "positive-law",
        "fixture",
        "20150101",
    )
    comments = "".join(f"<!-- {name}:{value} -->" for name, value in zip(ANNUAL_FIELDS, values, strict=True))
    return (
        "<!DOCTYPE html><html><head>" + comments + "</head><body>"
        "<!-- documentid:one usckey:K --><!-- itempath:/1/[Secs. 1 to 3 -->"
        "<!-- field-start:repealedhead --><h3>Native stub</h3><!-- field-end:repealedhead -->" + tail + "</body></html>"
    ).encode()


XML = b"""<uscDoc xmlns="http://xml.house.gov/schemas/uslm/1.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
<meta><dc:title>Title 1</dc:title><dc:type>USCTitle</dc:type><docNumber>1</docNumber>
<docPublicationName>Online@119-103</docPublicationName></meta>
<main><chapter identifier="/us/usc/t1/ch1"><num>1</num><heading>Chapter</heading>
<section identifier="/us/usc/t1/s1"><num>1</num><heading>Section</heading><content>Text.</content>
</section></chapter></main></uscDoc>"""


def zipped(*members):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members:
            archive.writestr(name, body)
    return out.getvalue()


def manifest(tmp_path, *items):
    sources = []
    for i, (selection, body) in enumerate(items):
        path = tmp_path / f"{i}.zip"
        path.write_bytes(body)
        sources.append({**selection, "path": path.name, "sha256": "sha256:" + hashlib.sha256(body).hexdigest()})
    path = tmp_path / "selection.json"
    path.write_text(json.dumps({"format": "spicy-uscode-edition-inputs", "version": 1, "sources": sources}))
    return path


def rows(output, table):
    return pq.read_table(output / table / (table + ".parquet")).to_pylist()


def test_annual_editions_keep_carried_year_sidecars_and_unexpanded_native_labels(tmp_path):
    source = zipped(("2016usc01.htm", annual(2016)), ("2016usc50a.htm", annual(2015, "50")), ("usc.css", b"body{}"))
    pin = manifest(tmp_path, ({"kind": "annual", "year": 2016}, source))
    output = tmp_path / "output"
    result = reader.build(pin, output)
    editions = rows(output, "uscode_editions")
    assert [(r["requested_year"], r["native_publication_year"], r["carried_forward"]) for r in editions[:2]] == [
        ("2016", "2016", "false"),
        ("2016", "2015", "true"),
    ]
    assert editions[2]["member_role"] == "supporting-file"
    records = rows(output, "uscode_edition_structure")
    assert len(records) == 2
    observation = json.loads(records[0]["source_observation_json"])
    assert observation["bracketed"] and observation["parts"][0]["range_end"] == "3"
    assert records[0]["native_status"] is None
    content = rows(output, "uscode_edition_sections")
    assert len(content) == 2 and all(r["body_text"] is None for r in content)
    assert {r["edition_id"] for r in content} == {r["edition_id"] for r in records}
    assert result["baseline_inputs"] == []
    # Receipts are retired: the candidate is the three tables and the build's own selection and record.
    assert sorted(p.relative_to(output).as_posix() for p in output.rglob("*") if p.is_file()) == sorted(
        ["progress.jsonl", "rebuild.json", "selection.json", *(f"{name}/{name}.parquet" for name in reader.SCHEMAS)])
    assert all(editions[0]["archive_sha256"] == result["inputs"][0]["sha256"] for _ in editions)
    repeated = reader.build(pin, tmp_path / "second")
    assert repeated["generation"] == result["generation"]
    assert repeated["outputs"] == result["outputs"]


def test_release_point_xml_keeps_own_edition_and_native_positions(tmp_path):
    pin = manifest(
        tmp_path,
        ({"kind": "release-point-title", "release_point": "119-103", "title": "01"}, zipped(("usc01.xml", XML))),
    )
    output = tmp_path / "output"
    reader.build(pin, output)
    edition = rows(output, "uscode_editions")[0]
    assert edition["release_point"] == "119-103" and edition["native_publication_year"] is None
    records = rows(output, "uscode_edition_structure")
    assert {r["observation_kind"] for r in records} == {"xml-section", "xml-chapter"}
    assert {r["native_identifier"] for r in records} == {"/us/usc/t1/s1", "/us/usc/t1/ch1"}
    assert all(r["source_position"].startswith("/*[1]") for r in records)
    content = rows(output, "uscode_edition_sections")
    assert len(content) == 1 and content[0]["body_text"] == "Text."
    assert content[0]["native_identifier"] == "/us/usc/t1/s1"
    assert content[0]["source_context"] == "main-body-candidate"


def test_late_invalid_member_cannot_activate_partial_output(tmp_path):
    pin = manifest(
        tmp_path,
        ({"kind": "annual", "year": 2016}, zipped(("2016usc01.htm", annual(2016)), ("2016usc02.htm", b"not a title"))),
    )
    with pytest.raises(UsCodeSourceError):
        reader.build(pin, tmp_path / "output")
    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".uscode-editions-*"))


def test_hash_drift_duplicate_selection_and_existing_output_refuse(tmp_path):
    source = zipped(("2016usc01.htm", annual(2016)))
    pin = manifest(tmp_path, ({"kind": "annual", "year": 2016}, source))
    (tmp_path / "0.zip").write_bytes(source + b"changed")
    with pytest.raises(ValueError, match="hash differs"):
        reader.build(pin, tmp_path / "output")
    pin = manifest(tmp_path, ({"kind": "annual", "year": 2016}, source), ({"kind": "annual", "year": 2016}, source))
    with pytest.raises(ValueError, match="duplicate"):
        reader.build(pin, tmp_path / "output")
    assert not (tmp_path / "output").exists()
    (tmp_path / "output").mkdir()
    with pytest.raises(FileExistsError):
        reader.build(pin, tmp_path / "output")


def test_changed_transitive_reader_refuses_candidate(tmp_path, monkeypatch):
    source = zipped(("2016usc01.htm", annual(2016)))
    pin = manifest(tmp_path, ({"kind": "annual", "year": 2016}, source))
    original = reader._file_digest
    reads = 0

    def changed_digest(path):
        nonlocal reads
        if path.name == "zip_archive.py":
            reads += 1
            if reads > 1:
                return "sha256:" + "0" * 64
        return original(path)

    monkeypatch.setattr(reader, "_file_digest", changed_digest)
    with pytest.raises(ValueError, match="reader changed"):
        reader.build(pin, tmp_path / "output")
    assert reads == 2
    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".uscode-editions-*"))


def test_late_unclosed_content_field_cannot_install_candidate(tmp_path):
    source = zipped(("2016usc01.htm", annual(2016, tail="<!-- field-start:statute -->Unclosed")))
    pin = manifest(tmp_path, ({"kind": "annual", "year": 2016}, source))
    with pytest.raises(UsCodeSourceError, match="unclosed"):
        reader.build(pin, tmp_path / "output")
    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".uscode-editions-*"))


def test_content_conversion_never_writes_more_than_64_rows_and_keeps_every_occurrence(tmp_path, monkeypatch):
    # Cross two full batches with differently sized body values and a final short batch.
    bodies = ["body-" + str(i) + "-" + "x" * (i % 7 * 1000) for i in range(129)]
    sections = "".join(
        '<section identifier="/us/usc/t1/s' + str(i + 1) + '"><num>' + str(i + 1) + "</num>"
        "<heading>Section " + str(i + 1) + "</heading><content>" + text + "</content></section>"
        for i, text in enumerate(bodies)
    )
    original = XML.replace(
        b'<section identifier="/us/usc/t1/s1"><num>1</num><heading>Section</heading><content>Text.</content>\n</section>',
        sections.encode(),
    )
    pin = manifest(
        tmp_path,
        ({"kind": "release-point-title", "release_point": "119-103", "title": "01"}, zipped(("usc01.xml", original))),
    )
    observed = []
    real_writer = pq.ParquetWriter

    class RecordingWriter:
        def __init__(self, path, *args, **kwargs):
            self.path = Path(path)
            self.writer = real_writer(path, *args, **kwargs)

        def __enter__(self):
            self.writer.__enter__()
            return self

        def __exit__(self, *args):
            return self.writer.__exit__(*args)

        def write_table(self, table, *args, **kwargs):
            if "uscode_edition_sections" in self.path.parts:
                observed.append(table.num_rows)
            return self.writer.write_table(table, *args, **kwargs)

    monkeypatch.setattr(pq, "ParquetWriter", RecordingWriter)
    output = tmp_path / "candidate"
    reader.build(pin, output)
    assert observed and max(observed) <= 64
    assert sum(observed) == len(bodies)
    content = rows(output, "uscode_edition_sections")
    assert [row["body_text"] for row in content] == bodies
    assert [row["native_identifier"] for row in content] == ["/us/usc/t1/s" + str(i + 1) for i in range(129)]
    assert len({row["observation_id"] for row in content}) == 129
    assert all(row["source_context"] == "main-body-candidate" for row in content)
    assert not (output / "etl_receipts.parquet").exists()
    assert str(tmp_path) not in json.dumps(content)
