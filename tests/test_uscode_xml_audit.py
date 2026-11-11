"""Complete tiny XML originals exercise the body audit invariants."""

import io
import json
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import uscode_editions as builder
from spicy_regs.sources import uscode_xml_audit as auditor


XML = b"""<uscDoc xmlns="http://xml.house.gov/schemas/uslm/1.0" xmlns:dc="http://purl.org/dc/elements/1.1/"
xmlns:h="http://www.w3.org/1999/xhtml" identifier="/us/usc/t1">
<meta><dc:title>Title 1</dc:title><dc:type>USCTitle</dc:type><docNumber>1</docNumber>
<docPublicationName>Online@119-103</docPublicationName><property role="is-positive-law">true</property></meta>
<main><chapter identifier="/us/usc/t1/ch1"><num>1</num><heading>Chapter</heading><heading>Second heading</heading>
<section identifier="/us/usc/t1/s1"><num>1</num><heading>Outer</heading>
<content>A<note>X</note>Z</content><subsection><num>(a)</num><content>Nested</content><notes>omit</notes></subsection>
<notes><note><quotedContent><section identifier="/us/usc/t1/s9"><num>9</num><content>Quoted</content></section></quotedContent></note></notes>
</section><section identifier="/us/usc/t1/s1"><num>1</num><h:table><h:tr><h:td>Table</h:td></h:tr></h:table></section>
<section identifier="/us/usc/t1/s2"><num>2</num><content/></section>
<section identifier="/us/usc/t1/s3" status="repealed"><num>3</num></section>
<section identifier="/us/usc/t1/s4"><num>4</num><mystery>Unknown</mystery></section>
</chapter></main></uscDoc>"""


def fixture(tmp_path):
    original = tmp_path / "original.zip"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("usc01.xml", XML)
    original.write_bytes(stream.getvalue())
    selection = tmp_path / "selection.json"
    selection.write_text(
        json.dumps(
            {
                "format": "spicy-uscode-edition-inputs",
                "version": 1,
                "sources": [
                    {
                        "kind": "release-point-title",
                        "release_point": "119-103",
                        "title": "01",
                        "path": original.name,
                        "sha256": auditor.sha(original),
                    },
                ],
            }
        )
    )
    candidate = tmp_path / "candidate"
    builder.build(selection, candidate)
    return candidate, selection


def snapshot(root):
    return {str(p.relative_to(root)): auditor.sha(p) for p in root.rglob("*") if p.is_file()}


def records(candidate, table):
    return pq.read_table(candidate / table / (table + ".parquet")).to_pylist()


def corrupt(candidate, table, change):
    path = candidate / table / (table + ".parquet")
    values = records(candidate, table)
    change(values)
    pq.write_table(pa.Table.from_pylist(values, schema=builder.SCHEMAS[table]), path)
    path = candidate / "rebuild.json"
    value = json.loads(path.read_text())
    value["outputs"][table].update(rows=len(values), sha256=auditor.sha(candidate / table / (table + ".parquet")))
    path.write_text(json.dumps(value))


def test_complete_xml_fixture_is_read_only_and_preserves_native_occurrences(tmp_path):
    candidate, selection = fixture(tmp_path)
    before = snapshot(tmp_path)
    result = auditor.audit(candidate, selection)
    assert result["verdict"] == "APPROVE" and result["complete_original_members"] == 1
    assert result["rows"] == {"uscode_editions": 1, "uscode_edition_structure": 7, "uscode_edition_sections": 6}
    assert snapshot(tmp_path) == before
    content = records(candidate, "uscode_edition_sections")
    assert content[0]["native_identifier"] == "/us/usc/t1/s9"  # nested section closes first
    assert content[0]["source_context"] == "notes-or-quoted-content"
    assert [r["body_text"] for r in content] == ["Quoted", "AZ\n(a)Nested", "Table", "", None, None]
    assert content[1]["native_identifier"] == content[2]["native_identifier"]
    assert content[1]["observation_id"] != content[2]["observation_id"]
    assert json.loads(content[-1]["source_observation_json"])["issues"] == [
        "unclassified-child:" + auditor.U + "mystery"
    ]
    chapter = records(candidate, "uscode_edition_structure")[-1]
    assert chapter["observation_kind"] == "xml-chapter"
    assert [x["text"] for x in json.loads(chapter["source_observation_json"])["headings"]] == [
        "Chapter",
        "Second heading",
    ]


@pytest.mark.parametrize(
    "column,value",
    [
        ("body_text", "wrong"),
        ("source_context", "wrong"),
        ("source_position", "/*[9]"),
        ("native_identifier", None),
        ("source_kind", "annual"),
        ("observation_id", "wrong"),
    ],
)
def test_each_content_subject_field_is_checked(tmp_path, column, value):
    candidate, selection = fixture(tmp_path)
    corrupt(candidate, "uscode_edition_sections", lambda values: values[0].update({column: value}))
    with pytest.raises(AssertionError):
        auditor.audit(candidate, selection)


@pytest.mark.parametrize("change", ["order", "duplicate", "missing", "observation"])
def test_complete_order_multiplicity_and_native_observation_are_checked(tmp_path, change):
    candidate, selection = fixture(tmp_path)

    def mutate(values):
        if change == "order":
            values.reverse()
        elif change == "duplicate":
            values.append(values[-1])
        elif change == "missing":
            values.pop()
        else:
            value = json.loads(values[0]["source_observation_json"])
            value["fields"][0]["text"] = "changed"
            values[0]["source_observation_json"] = json.dumps(value)

    corrupt(candidate, "uscode_edition_sections", mutate)
    with pytest.raises((AssertionError, StopIteration)):
        auditor.audit(candidate, selection)


APPENDIX = XML.replace(
    b"<dc:type>USCTitle</dc:type><docNumber>1</docNumber>", b"<dc:type>USCTitleAppendix</dc:type><docNumber>5a</docNumber>"
).replace(b'identifier="/us/usc/t1"', b'identifier="/us/usc/t5a"')


def corpus_fixture(tmp_path, members):
    original = tmp_path / "corpus.zip"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body in members:
            z.writestr(name, body)
    original.write_bytes(stream.getvalue())
    selection = tmp_path / "selection.json"
    spec = {"kind": "release-point-corpus", "release_point": "119-103", "path": original.name,
            "sha256": auditor.sha(original)}
    selection.write_text(json.dumps({"format": "spicy-uscode-edition-inputs", "version": 1, "sources": [spec]}))
    candidate = tmp_path / "candidate"
    builder.build(selection, candidate)
    return candidate, selection


def test_a_whole_release_point_corpus_is_audited_member_by_member(tmp_path):
    candidate, selection = corpus_fixture(tmp_path, [("usc01.xml", XML), ("usc05A.xml", APPENDIX)])
    result = auditor.audit(candidate, selection)
    assert result["status"] == "verified-complete-selected-xml119-103-corpus"
    assert result["complete_original_members"] == 2
    assert result["rows"] == {"uscode_editions": 2, "uscode_edition_structure": 14, "uscode_edition_sections": 12}
    # The report pins every file it read, as the additive preparation requires.
    assert str(candidate / "rebuild.json") in result["evidence_sha256"]
    editions = records(candidate, "uscode_editions")
    assert [e["title"] for e in editions] == ["01", "05a"]
    assert json.loads(editions[1]["metadata_json"])["document_type"] == "USCTitleAppendix"


def test_a_corpus_member_of_another_title_refuses(tmp_path):
    candidate, selection = corpus_fixture(tmp_path, [("usc01.xml", XML), ("usc05A.xml", APPENDIX)])
    corrupt(candidate, "uscode_editions", lambda values: values[1].update(title="05"))
    with pytest.raises(AssertionError, match="complete edition fields"):
        auditor.audit(candidate, selection)


@pytest.mark.parametrize("change", ["release_point", "title", "kind", "year", "multi"])
def test_selected_xml_scope_refuses_before_candidate_reads(tmp_path, change):
    selection = tmp_path / "selection.json"
    spec = dict(kind="release-point-title", release_point="119-103", title="01", path="absent", sha256="absent")
    if change != "multi":
        spec[change] = "different"
    selection.write_text(
        json.dumps(
            {
                "format": "spicy-uscode-edition-inputs",
                "version": 1,
                "sources": [spec, spec] if change == "multi" else [spec],
            }
        )
    )
    with pytest.raises(AssertionError):
        auditor.audit(tmp_path / "absent", selection)


@pytest.mark.parametrize(
    "bad", [b'<!DOCTYPE x [<!ENTITY y "z">]><x/>', b"<x/>", XML.replace(b"Online@119-103", b"Online@119-102")]
)
def test_native_identity_or_dtd_refuses(bad):
    with pytest.raises(AssertionError):
        auditor.native_observations(bad)


@pytest.mark.parametrize(
    "bad",
    [
        XML.decode().encode("utf-16le"),
        ('<!DOCTYPE uscDoc [<!ENTITY x "expanded">]>' + XML.decode()).encode("utf-16le"),
        b'<?xml version="1.0" encoding="ISO-8859-1"?>' + XML,
    ],
)
def test_non_utf8_encoding_and_hidden_dtd_refuse(bad):
    with pytest.raises(AssertionError, match="non-UTF8"):
        auditor.native_observations(bad)


def test_report_existing_input_or_candidate_paths_refuse_without_writes(tmp_path):
    candidate, selection = fixture(tmp_path)
    before = snapshot(tmp_path)
    for output in [selection, candidate / "new-report.json"]:
        assert auditor.main([str(candidate), "--selection", str(selection), "--output", str(output)]) == 1
    assert snapshot(tmp_path) == before
    report = tmp_path / "report.json"
    assert auditor.main([str(candidate), "--selection", str(selection), "--output", str(report)]) == 0
    assert json.loads(report.read_text())["verdict"] == "APPROVE"
