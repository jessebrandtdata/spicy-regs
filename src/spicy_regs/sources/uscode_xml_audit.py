"""Independent complete audit of one selected USLM release point: a whole-corpus zip or one title's zip.

Source expectations use ElementTree and literal XML, never the maintained
content/structure readers. Shared annual-audit helpers check only hashes,
public output and executing reader identities. The tables publish no per-row
receipts, so every row is checked against the original member itself. A
supervised actual run is required; tiny fixtures and static review are not
source qualification.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.sources.uscode_editions import MAX_ARCHIVE_BYTES, MAX_MEMBER_BYTES, SCHEMAS
from spicy_regs.sources.uscode_editions_audit import (
    _reserve_report,
    identity,
    public,
    reader_pins,
    require,
    rows,
    sha,
)

U = "{http://xml.house.gov/schemas/uslm/1.0}"
DC = "{http://purl.org/dc/elements/1.1/}"
DT = "{http://purl.org/dc/terms/}"
XSI = "{http://www.w3.org/2001/XMLSchema-instance}"
#: The builder's own bounds. One member's whole ElementTree is held at a time: the largest, Title 42 at
#: 119-102, is 113.7 MB of XML, so the process needs several times that; supervise its RSS externally.
TITLE_MEMBER = re.compile(r"usc(?P<title>[0-9]{2})(?P<appendix>[aA]?)\.xml")
KINDS = ("release-point-corpus", "release-point-title")
BODY_TAGS = {
    U + n
    for n in (
        "content",
        "subsection",
        "paragraph",
        "subparagraph",
        "clause",
        "subclause",
        "item",
        "subitem",
        "chapeau",
        "continuation",
        "table",
    )
} | {"{http://www.w3.org/1999/xhtml}table"}
NONBODY_TAGS = {U + n for n in ("notes", "note", "quotedContent", "sourceCredit")}


def text(node):
    return "".join(node.itertext())


def identifier_piece(raw):
    """Interpret the documented native coordinate shapes without reader calls."""
    result = dict(
        raw=raw,
        kind=None,
        title=None,
        appendix=None,
        section=None,
        section_part=None,
        chapter=None,
        range_start=None,
        range_end=None,
    )
    match = re.fullmatch(r"/us/usc/t([0-9]+[aA]?)/(.*)", raw)
    if not match:
        return result
    title, tail = match.groups()
    result.update(title=title, appendix=title.endswith(("a", "A")))
    components = tail.split("/")
    coordinate = None
    section = components[0]
    if section.startswith("s") and len(section) > 1 and not section[1:].lower().startswith(("t", "ch", "p", "d")):
        if len(components) == 1:
            result.update(kind="section", section=section[1:])
            coordinate = section[1:]
        elif len(components) == 2 and components[1]:
            result.update(kind="section-part", section=section[1:], section_part=components[1])
            coordinate = section[1:]
    if result["kind"] is None and all(components) and components[-1].startswith("ch") and len(components[-1]) > 2:
        coordinate = components[-1][2:]
        result.update(kind="chapter", chapter=coordinate)
    if coordinate is not None:
        ends = coordinate.split("...")
        if len(ends) == 2 and all(ends):
            result.update(range_start=ends[0], range_end=ends[1])
    return result


def native_metadata(root, title, release_point):
    """The title's native identity, held to the title its member name selects and the selected release point."""
    doc_number = title.lstrip("0")
    require(root.tag == U + "uscDoc", "native root differs")
    require(sum(e.tag == U + "uscDoc" for e in root.iter()) == 1, "nested native document")
    meta = [e for e in root if e.tag == U + "meta"]
    require(len(meta) == 1, "native meta count differs")

    def value(tag, required=False):
        found = [text(e).strip() for e in meta[0] if e.tag == tag]
        require(len(found) <= 1, "repeated native metadata")
        result = (found[0] or None) if found else None
        require(not required or result is not None, "missing native metadata")
        return result

    document_type = "USCTitleAppendix" if title.endswith("a") else "USCTitle"
    require(value(U + "docNumber", True) == doc_number, "native title differs")
    require(value(U + "docPublicationName", True) == f"Online@{release_point}", "native release point differs")
    require(value(DC + "type", True) == document_type, "native document type differs")
    stated_id = root.attrib.get("identifier")
    require(stated_id in (None, f"/us/usc/t{doc_number}"), "native root identifier differs")
    properties = [
        text(e).strip() for e in meta[0] if e.tag == U + "property" and e.attrib.get("role") == "is-positive-law"
    ]
    require(len(properties) <= 1, "repeated positive-law property")
    body_present = any(text(e).strip() for e in root if e.tag in (U + "main", U + "appendix"))
    return dict(
        source="release-point-title",
        title=title,
        doc_number=doc_number,
        release_point=f"Online@{release_point}",
        document_type=document_type,
        heading=value(DC + "title", True),
        publisher=value(DC + "publisher"),
        converter=value(DC + "creator"),
        created=value(DT + "created"),
        positive_law=properties[0] if properties else None,
        schema_location=root.attrib.get(XSI + "schemaLocation"),
        identifier=stated_id,
        identity_basis=["doc-number:native", "release-point:native", "document-type:native"]
        + (["identifier:native"] if stated_id is not None else []),
        body_present=body_present,
    )


def native_observations(body, title="01", release_point="119-103"):
    """Return literal metadata and postorder structure/body observations of one title member."""
    require(len(body) <= MAX_MEMBER_BYTES, "selected XML member bound")
    decoded = body.decode("utf-8-sig")
    require("\x00" not in decoded, "non-UTF8 XML unsupported")
    declaration = re.match(r"""\s*<\?xml\b[^?]*encoding\s*=\s*["']([^"']+)["']""", decoded)
    require(declaration is None or declaration[1].lower() in ("utf-8", "utf8"), "non-UTF8 XML declaration")
    require("<!DOCTYPE" not in decoded and "<!ENTITY" not in decoded, "DTD/entity XML unsupported")
    root = ET.fromstring(decoded)
    structure, content = [], []

    def body_text(node):
        if node.tag in NONBODY_TAGS:
            return ""
        return (node.text or "") + "".join(body_text(c) + (c.tail or "") for c in node)

    def visit(node, ancestors, path):
        require(len(ancestors) < 256, "XML depth bound")
        element = dict(tag=node.tag, attributes=dict(node.attrib), source_xpath=path)
        for index, child in enumerate(node, 1):
            visit(child, ancestors + [element], path + f"/*[{index}]")
        if node.tag in (U + "section", "section", U + "chapter", "chapter"):

            def named_fields(tag):
                return [
                    dict(
                        element=dict(tag=c.tag, attributes=dict(c.attrib), source_xpath=path + f"/*[{i}]"), text=text(c)
                    )
                    for i, c in enumerate(node, 1)
                    if c.tag in (U + tag, tag)
                ]

            structure.append(
                dict(
                    element=element,
                    ancestors=ancestors,
                    identifier_pieces=[identifier_piece(x) for x in node.attrib.get("identifier", "").split()],
                    numbers=named_fields("num"),
                    headings=named_fields("heading"),
                )
            )
        if node.tag == U + "section":
            fields = [
                dict(
                    name=c.tag,
                    text=text(c),
                    byte_span=None,
                    element=dict(tag=c.tag, attributes=dict(c.attrib), source_xpath=path + f"/*[{i}]"),
                    depth=0,
                )
                for i, c in enumerate(node, 1)
            ]
            bodies = [c for c in node if c.tag in BODY_TAGS]
            recognized = BODY_TAGS | NONBODY_TAGS | {U + "num", U + "heading"}
            content.append(
                dict(
                    element=element,
                    ancestors=ancestors,
                    fields=fields,
                    body_text="\n".join(body_text(c) for c in bodies) if bodies else None,
                    native_text=text(node),
                    context="notes-or-quoted-content"
                    if any(a["tag"] in {U + "notes", U + "note", U + "quotedContent"} for a in ancestors)
                    else "main-body-candidate",
                    issues=["unclassified-child:" + c.tag for c in node if c.tag not in recognized],
                )
            )

    visit(root, [], "/*[1]")
    return native_metadata(root, title, release_point), structure, content


def audit(candidate: Path, selection: Path):
    """Read one exact XML candidate and its original completely; never write or repair either."""
    candidate, selection = Path(candidate).resolve(), Path(selection).resolve()
    raw = selection.read_bytes()
    require(len(raw) <= 1024**2, "selection byte bound")
    selected = json.loads(raw)
    require(
        selected.get("format") == "spicy-uscode-edition-inputs" and selected.get("version") == 1,
        "unsupported selection",
    )
    require(isinstance(selected.get("sources"), list) and len(selected["sources"]) == 1, "one XML archive required")
    spec = selected["sources"][0]
    kind, point, title = spec.get("kind"), spec.get("release_point"), spec.get("title")
    require(
        kind in KINDS
        and isinstance(point, str)
        and re.fullmatch(r"[0-9]{2,3}-[0-9]+[a-z]?", point) is not None
        and spec.get("year") is None
        and (title is None if kind == "release-point-corpus" else isinstance(title, str) and TITLE_MEMBER.fullmatch(f"usc{title}.xml") is not None),
        "unsupported selected XML scope",
    )
    source = Path(spec["path"])
    source = source if source.is_absolute() else selection.parent / source
    require(source.stat().st_size <= MAX_ARCHIVE_BYTES, "selected archive byte bound")
    require(sha(source) == spec["sha256"], "original archive differs")
    require((candidate / "selection.json").read_bytes() == raw, "candidate selection differs")
    rebuild = json.loads((candidate / "rebuild.json").read_text())
    require(rebuild["manifest_sha256"] == sha(selection), "manifest digest differs")
    require(rebuild["baseline_inputs"] == [], "unexpected baseline input")
    code = reader_pins()
    require(rebuild["reader_sha256"] == code, "reader identity differs")
    generation = identity("uscode-editions-public-v2", sha(selection), code)
    require(rebuild["generation"] == generation, "generation differs")
    require(set(rebuild["outputs"]) == set(SCHEMAS), "output table inventory differs")
    evidence = [selection, source, Path(__file__), candidate / "rebuild.json", candidate / "selection.json"]
    for table, entry in rebuild["outputs"].items():
        require(entry["path"] == f"{table}/{table}.parquet", "output path differs")
        path = candidate / entry["path"]
        require(sha(path) == entry["sha256"], "subject hash differs")
        parquet = pq.ParquetFile(path)
        require(
            parquet.schema_arrow == SCHEMAS[table] and parquet.metadata.num_rows == entry["rows"],
            "subject schema/count differs",
        )
        evidence.append(path)
    before = {str(p): sha(p) for p in evidence}
    counts = Counter()
    editions = rows(candidate / rebuild["outputs"]["uscode_editions"]["path"])
    observed = {t: rows(candidate / rebuild["outputs"][t]["path"]) for t in ("uscode_edition_structure", "uscode_edition_sections")}

    def checked(table, row):
        public(row, table)
        counts[table] += 1

    with zipfile.ZipFile(source) as archive:
        members = [m for m in archive.infolist() if not m.is_dir()]
        require(bool(members), "empty original archive")
        if kind == "release-point-title":
            require(len(members) == 1, "a title archive holds exactly one member")
        for index, member in enumerate(members, 1):
            match = TITLE_MEMBER.fullmatch(member.filename.rsplit("/", 1)[-1])
            if match is None:
                raise AssertionError("member is not a title: " + member.filename)
            code_title = match["title"] + match["appendix"].lower()
            require(kind == "release-point-corpus" or code_title == title, "title member differs from the selection")
            require(member.file_size <= MAX_MEMBER_BYTES, "decoded member bound")
            body = archive.read(member)
            metadata, structure, content = native_observations(body, code_title, point)
            member_sha = "sha256:" + hashlib.sha256(body).hexdigest()
            edition_id = identity((kind, None, point, title), spec["sha256"], index, member.filename, member_sha)
            edition = next(editions)
            require(json.loads(edition["metadata_json"]) == metadata, "complete native metadata differs")
            require(
                {k: v for k, v in edition.items() if k != "metadata_json"}
                == dict(
                    edition_id=edition_id,
                    source_kind=kind,
                    requested_year=None,
                    release_point=point,
                    archive_sha256=spec["sha256"],
                    member_name=member.filename,
                    member_sha256=member_sha,
                    member_bytes=str(len(body)),
                    title=code_title,
                    native_publication_year=None,
                    carried_forward=None,
                    member_role="title",
                ),
                "complete edition fields differ at " + member.filename,
            )
            checked("uscode_editions", edition)
            del body
            for table, expected in [("uscode_edition_sections", content), ("uscode_edition_structure", structure)]:
                for native in expected:
                    row = next(observed[table])
                    position = native["element"]["source_xpath"]
                    xml_kind = "xml-section" if native["element"]["tag"] in (U + "section", "section") else "xml-chapter"
                    common = dict(
                        observation_id=identity(
                            edition_id, xml_kind if table == "uscode_edition_structure" else "section-content", position
                        ),
                        edition_id=edition_id,
                        source_position=position,
                        native_identifier=native["element"]["attributes"].get("identifier"),
                    )
                    if table == "uscode_edition_structure":
                        common.update(
                            observation_kind=xml_kind,
                            native_label=" ".join(n["text"] for n in native["numbers"]),
                            native_status=native["element"]["attributes"].get("status"),
                        )
                    else:
                        common.update(source_kind=kind, source_context=native["context"], body_text=native["body_text"])
                    where = member.filename + position
                    require(json.loads(row["source_observation_json"]) == native, "complete native observation differs at " + where)
                    require(
                        {k: v for k, v in row.items() if k != "source_observation_json"} == common,
                        "complete subject fields differ at " + where,
                    )
                    checked(table, row)
            del structure, content
    require(next(editions, None) is None, "extra edition rows")
    require(all(next(stream, None) is None for stream in observed.values()), "extra native rows")
    require(all(counts[t] == rebuild["outputs"][t]["rows"] for t in SCHEMAS), "verified row counts differ")
    require({str(p): sha(p) for p in evidence} == before, "input/candidate evidence drift")
    require(reader_pins() == code, "reader drift")
    scope = f"xml{point}-" + ("corpus" if kind == "release-point-corpus" else f"title{title}")
    return dict(
        status=f"verified-complete-selected-{scope}",
        verdict="APPROVE",
        generation=generation,
        source_sha256=spec["sha256"],
        selection_sha256=sha(selection),
        audit_sha256=sha(__file__),
        complete_original_members=len(members),
        rows=dict(counts),
        evidence_sha256=before,
        independence="All source expectations use independent ElementTree, native attributes/text and positional child paths; maintained helpers only declare schemas.",
        scope=f"Every member of one selected release-point archive ({scope}): every native metadata/heading/hierarchy/body/null/field/occurrence of every row. No publication or retirement proof.",
        limits="UTF8 XML without DTD/entities, within the builder's archive and member bounds; one member's whole tree at a time, not a memory bound. Rendering is documented descendant/body text, not facsimile or legal meaning. External supervision required.",
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        report = _reserve_report(args.candidate, args.selection, args.output)
    except Exception as error:
        print(f"Cannot reserve fresh audit report: {error}", file=sys.stderr)
        return 1
    with report:
        try:
            pa.set_cpu_count(1)
            pa.set_io_thread_count(1)
            result, exit_code = audit(args.candidate, args.selection), 0
        except Exception as error:
            result, exit_code = (
                dict(
                    status="xml-audit-failed",
                    verdict="REQUEST CHANGES",
                    error_type=type(error).__name__,
                    error=str(error),
                    audit_sha256=sha(__file__),
                ),
                1,
            )
        report.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
