"""Independently qualify one selected retained annual U.S. Code archive.

No acquisition/content reader is called. Expectations come from raw ZIP bytes,
native comments, an independent field stack, and standard HTML parser events.
Maintained helpers supply declared schemas only; the tables publish no
per-row receipts, so each row is checked against the original itself.
The library returns evidence without writing files; main reserves fresh reports.
"""

import argparse
import collections
import hashlib
import inspect
import json
import re
import sys
import zipfile
from html.parser import HTMLParser
from pathlib import Path
from importlib import metadata as distribution_metadata
import platform

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.sources.uscode_editions import SCHEMAS

# Explicit retained annual selection; admitting a year does not qualify its
# contents. The one-archive manifest, native year and all-field checks below
# still establish the scope of each executed audit.
SUPPORTED_ANNUAL_YEARS = frozenset(range(1994, 2025))

HEADER = (
    "AUTHORITIES-PUBLICATION-NAME",
    "AUTHORITIES-PUBLICATION-ID",
    "AUTHORITIES-PUBLICATION-YEAR",
    "AUTHORITIES-LAWS-ENACTED-THROUGH-DATE",
    "SEARCHABLE-LAWS-ENACTED-THROUGH-DATE",
    "AUTHORITIES-USC-TITLE-NAME",
    "AUTHORITIES-USC-TITLE-ENUM",
    "AUTHORITIES-USC-TITLE-STATUS",
    "CONVERSION-PROGRAM",
    "CONVERSION-DATETIME",
)
META = (
    "publication_name",
    "publication_id",
    "publication_year",
    "laws_enacted_through",
    "laws_enacted_through_stated",
    "title_name",
    "title_enum",
    "title_status",
    "conversion_program",
    "conversion_datetime",
)
LOCAL = re.compile(
    r"(?:/Users/|/home/|/tmp/|/private/|/Volumes/|file://|[A-Za-z]:\\|https?://(?:localhost|127\.0\.0\.1))", re.I
)
AUTH_QUERY = re.compile(r"(?:[?&]|\b)(?:api[_-]?key|access[_-]?token|authorization|credential|secret)\s*=", re.I)
BAD_KEYS = {
    "path",
    "local_path",
    "internal_path",
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "credentials",
    "secret",
    "secrets",
    "access_token",
}


def require(condition, label):
    if not condition:
        raise AssertionError(label)


def sha(path):
    with Path(path).open("rb") as f:
        return "sha256:" + hashlib.file_digest(f, "sha256").hexdigest()


def identity(*values):
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


def rows(path):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=32, use_threads=False):
        yield from batch.to_pylist()


def public(value, where):
    """Visit every key/value, including recursively decoded JSON strings."""
    if isinstance(value, dict):
        for key, item in value.items():
            require(key.lower().replace("-", "_") not in BAD_KEYS, f"private field {where}/{key}")
            public(key, where + "/key")
            if key == "source_uri" and item is not None:
                require(bool(re.fullmatch(r"sha256:[0-9a-f]{64}", item)), "non-digest archive source_uri")
            public(item, where + "/" + key)
    elif isinstance(value, (list, tuple)):
        for item in value:
            public(item, where)
    elif isinstance(value, str):
        # Decode valid JSON first: serialization escapes are not publisher values.
        if value.strip().startswith(("{", "[", '"')):
            try:
                decoded = json.loads(value)
            except (ValueError, TypeError):
                pass
            else:
                if decoded != value:
                    public(decoded, where + "/decoded")
                    return
        require(not LOCAL.search(value), f"local locator in {where}")
        require(not AUTH_QUERY.search(value), f"credential query/value in {where}")


def comments(body, lo=0, hi=None):
    """Offsets always refer to original bytes, never replacement-decoded text."""
    hi = len(body) if hi is None else hi
    cursor = lo
    while True:
        start = body.find(b"<!--", cursor, hi)
        if start < 0:
            return
        stop = body.find(b"-->", start + 4, hi)
        require(stop >= 0, "unclosed native comment")
        end = stop + 3
        yield start, end, body[start + 4 : stop]
        cursor = end


def decode(raw, issues):
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        if "invalid_utf8" not in issues:
            issues.append("invalid_utf8")
        return raw.decode("utf-8", "replace")


class Events(HTMLParser):
    """Separate token stream for documented readable-field rendering."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.events = []
        self.first_heading = None

    def handle_data(self, data: str):
        self.events.append(("text", data))

    def handle_starttag(self, tag, attrs):
        self.events.append(("start", tag))
        if self.first_heading is None and tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self.first_heading = attrs

    def handle_endtag(self, tag):
        self.events.append(("end", tag))


def html_view(text, readable):
    parser = Events()
    parser.feed(text)
    parser.close()
    output = []
    for kind, value in parser.events:
        if kind == "text":
            output.append(value)
        elif readable and (
            (kind == "start" and value == "br")
            or (kind == "end" and value in {"p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6"})
        ):
            output.append("\n")
        elif readable and kind == "start" and value in {"td", "th"}:
            output.append("\t")
    return "".join(output), [list(pair) for pair in (parser.first_heading or [])]


def parts(label):
    result = []
    token = re.compile(r"[0-9][0-9A-Za-z.‐‑‒–—―−\x96\x97-]*\Z")
    for raw in label.split(","):
        item = dict(raw=raw, kind="unparsed", section=None, range_start=None, range_end=None)
        value = raw.strip()
        ends = re.fullmatch(r"(\S+)\s+to\s+(\S+)", value)
        if ends and token.fullmatch(ends[1]) and token.fullmatch(ends[2]):
            item.update(kind="range", range_start=ends[1], range_end=ends[2])
        elif token.fullmatch(value):
            item.update(kind="section", section=value)
        result.append(item)
    return result


def observation(body, item, doc, stop):
    issues = []
    start, end, payload = item
    itempath = decode(payload.split(b":", 1)[1].rstrip(), issues)
    comment = decode(body[start:end], issues)
    fields = []
    doc_comment = None
    if doc is not None:
        ds, de, dp = doc
        doc_comment = decode(body[ds:de], issues)
        text = decode(dp, issues)
        markers = list(re.finditer(r"(?<!\S)([A-Za-z][A-Za-z0-9_-]*):", text))
        for i, marker in enumerate(markers):
            limit = markers[i + 1].start() if i + 1 < len(markers) else len(text)
            fields.append([marker[1], text[marker.end() : limit].strip()])
    keys = [v for k, v in fields if k == "usckey"]
    if len(keys) != 1:
        issues.append("missing_usckey" if not keys else "repeated_usckey")
    elif not keys[0]:
        issues.append("empty_usckey")
    tail = itempath.rsplit("/", 1)[-1].strip()
    label = re.fullmatch(r"\[?(Secs?\.)\s+(.*)", tail, re.S)
    heading = None
    for hs, he, hp in comments(body, end, stop):
        match = re.fullmatch(rb"\s*field-start:([A-Za-z0-9_-]*head)\s*", hp)
        if not match:
            continue
        name = match[1]
        closing = re.compile(rb"<!--\s*field-end:" + re.escape(name) + rb"\s*-->").search(body, he, stop)
        if closing is None:
            issues.append("heading_field_unclosed")
        else:
            literal = decode(body[he : closing.start()], issues)
            text, attrs = html_view(literal, False)
            heading = dict(
                field_name=name.decode("ascii"),
                html=literal,
                text=text,
                attributes=attrs,
                byte_span=[he, closing.start()],
            )
        break
    return dict(
        itempath=itempath,
        itempath_comment=comment,
        itempath_span=[start, end],
        document_comment=doc_comment,
        document_comment_span=[doc[0], doc[1]] if doc else None,
        document_fields=fields,
        usckey=keys[0] if len(keys) == 1 else None,
        section_label=label[1] if label else None,
        bracketed=tail.startswith("["),
        parts=parts(label[2]) if label else [],
        heading=heading,
        issues=issues,
    )


def native_items(body):
    document = item = own_doc = None
    for position in comments(body):
        payload = position[2]
        marker = re.fullmatch(rb"\s*(documentid|itempath):(.*)", payload, re.S)
        if not marker:
            continue
        if item is not None:
            yield observation(body, item, own_doc, position[0]), item[1], position[0]
            item = None
        if marker[1] == b"documentid":
            document = position
        else:
            item, own_doc = position, document
            document = None
    if item is not None:
        yield observation(body, item, own_doc, len(body)), item[1], len(body)


def native_fields(body, start, stop):
    stack, output, issues = [], [], []
    for cs, ce, payload in comments(body, start, stop):
        field = re.fullmatch(rb"\s*field-(start|end):([^\s>]+)\s*", payload)
        if not field:
            continue
        name = field[2].decode("ascii")
        if field[1] == b"start":
            stack.append((name, ce, len(stack)))
        else:
            require(stack and stack[-1][0] == name, "native crossed/unpaired field markers")
            _, begin, depth = stack.pop()
            text, _ = html_view(decode(body[begin:cs], issues), True)
            output.append(dict(name=name, text=text, byte_span=[begin, cs], element=None, depth=depth))
    require(not stack, "native unclosed field markers")
    output.sort(key=lambda item: item["byte_span"])
    statutes = [f["text"] for f in output if f["name"] == "statute" and f["depth"] == 0]
    return output, "\n".join(statutes) if statutes else None, issues


def metadata(body):
    header_end = body.lower().find(b"<body")
    require(header_end > 0, "title lacks body marker")
    found = {}
    for _, _, payload in comments(body, 0, header_end):
        key, separator, value = payload.strip().partition(b":")
        if separator and key.decode("ascii", "replace") in HEADER:
            name = key.decode("ascii")
            require(name not in found, "repeated native identity")
            found[name] = value.decode("ascii").strip()
    require(set(found) == set(HEADER), "missing native identity")
    return dict(
        source="annual-title",
        **{column: found[key] for key, column in zip(HEADER, META)},
        identity_basis=["publication-year:native", "title-enum:native"],
    )


def reader_pins() -> dict[str, str]:
    # Independently hash the executing package trees, including the auditor.
    import spicy_docs
    import spicy_regs
    import rulespec_artifacts

    code = {}
    for package in (spicy_docs, spicy_regs, rulespec_artifacts):
        package_root = Path(inspect.getfile(package)).parent
        for path in package_root.rglob("*.py"):
            code[f"{package.__name__}/{path.relative_to(package_root).as_posix()}"] = sha(path)
    from spicy_regs.sources import uscode_editions

    checkout = Path(inspect.getfile(uscode_editions)).resolve().parents[3]
    if all((checkout / name).is_file() for name in ("pyproject.toml", "uv.lock")):
        for name in ("pyproject.toml", "uv.lock"):
            code[f"checkout/{name}"] = sha(checkout / name)
    code["runtime/python"] = platform.python_version()
    for name in ("spicy-docs", "rulespec-artifacts", "pyarrow"):
        code[f"distribution/{name}"] = distribution_metadata.version(name)
    return code


def audit(candidate: Path, selection: Path, *, annual_year: int) -> dict:
    """Return complete qualification evidence without modifying any input/output.

    Candidate, selection, sources and reader packages must remain unchanged.
    The caller supplies external resource supervision; bounds here limit member
    buffers and Arrow batches, not aggregate process memory.
    """
    require(
        type(annual_year) is int and annual_year in SUPPORTED_ANNUAL_YEARS,
        "supported audit scope is one retained annual archive from 1994 through 2024",
    )
    candidate, selection = Path(candidate).resolve(), Path(selection).resolve()
    rebuild_bytes = (candidate / "rebuild.json").read_bytes()
    selection_bytes = selection.read_bytes()
    rebuild = json.loads(rebuild_bytes)
    selected = json.loads(selection_bytes)
    selection_sha = "sha256:" + hashlib.sha256(selection_bytes).hexdigest()
    rebuild_sha = "sha256:" + hashlib.sha256(rebuild_bytes).hexdigest()
    require((candidate / "selection.json").read_bytes() == selection_bytes, "candidate selection differs")
    require(rebuild["manifest_sha256"] == selection_sha, "selection digest differs")
    require(
        selected.get("format") == "spicy-uscode-edition-inputs" and selected.get("version") == 1,
        "unsupported selection format",
    )
    require(len(selected["sources"]) == 1, "audit scope is one annual archive")
    spec = selected["sources"][0]
    require(
        spec["kind"] == "annual"
        and type(spec["year"]) is int
        and spec["year"] == annual_year
        and spec.get("release_point") is None
        and spec.get("title") is None,
        "annual selection/expected year differs",
    )
    source = Path(spec["path"])
    if not source.is_absolute():
        source = selection.parent / source
    require(sha(source) == spec["sha256"], "original archive differs")
    require(rebuild["baseline_inputs"] == [], "unexpected baseline input")
    code = reader_pins()
    require(code == rebuild["reader_sha256"], "isolated reader package identity differs from candidate")
    require(
        rebuild["generation"] == identity("uscode-editions-public-v2", selection_sha, code),
        "generation differs from selected manifest and reader recipe",
    )
    require(set(rebuild["outputs"]) == set(SCHEMAS), "missing/extra output tables")
    evidence_paths = [candidate / "rebuild.json", candidate / "selection.json", selection, source, Path(__file__)]
    for name, entry in rebuild["outputs"].items():
        require(entry["path"] == f"{name}/{name}.parquet", "unexpected output path")
        evidence_paths.append(candidate / entry["path"])
        path = candidate / entry["path"]
        require(sha(path) == entry["sha256"], "output digest differs " + name)
        parquet = pq.ParquetFile(path)
        require(parquet.schema_arrow == SCHEMAS[name], "schema differs " + name)
        require(parquet.metadata.num_rows == entry["rows"], "row count differs " + name)
    evidence_before = {str(path): sha(path) for path in evidence_paths}
    counts = collections.Counter()
    categories = collections.Counter()
    edition_rows = list(rows(candidate / rebuild["outputs"]["uscode_editions"]["path"]))
    require(len({e["edition_id"] for e in edition_rows}) == len(edition_rows), "duplicate edition IDs")
    member_map = {}

    def checked(table, row):
        public(row, table)
        counts[table] += 1

    with zipfile.ZipFile(source) as archive:
        members = [info for info in archive.infolist() if not info.is_dir()]
        require(
            [x.filename for x in members] == [e["member_name"] for e in edition_rows],
            "complete original ZIP inventory differs",
        )
        require(bool(members), "empty original archive")
        native_years = []
        for index, (info, edition) in enumerate(zip(members, edition_rows), 1):
            require(info.file_size <= 128 * 1024**2, "member bound")
            body = archive.read(info)
            member_sha = "sha256:" + hashlib.sha256(body).hexdigest()
            require(
                edition["member_sha256"] == member_sha and edition["member_bytes"] == str(len(body)),
                "decoded member hash/size differs",
            )
            expected_id = identity(
                ("annual", annual_year, None, None), spec["sha256"], index, info.filename, member_sha
            )
            require(edition["edition_id"] == expected_id, "edition identity differs")
            match = re.fullmatch(
                r"\d{4}usc(?P<title>\d{2})(?P<appendix>[aA]?)\.htm", info.filename.rsplit("/", 1)[-1], re.I
            )
            require(
                edition["archive_sha256"] == spec["sha256"]
                and edition["source_kind"] == "annual"
                and edition["requested_year"] == str(annual_year)
                and edition["release_point"] is None,
                "edition scope differs",
            )
            if match:
                native = metadata(body)
                native_years.append(native["publication_year"])
                enum = native["title_enum"].lower()
                title = match["title"].lstrip("0")
                require(enum in (title, title + match["appendix"].lower()), "native title/member routing differs")
                require(json.loads(edition["metadata_json"]) == native, "native member metadata differs")
                require(
                    edition["title"] == native["title_enum"]
                    and edition["native_publication_year"] == native["publication_year"]
                    and edition["carried_forward"] == str(native["publication_year"] != str(annual_year)).lower()
                    and edition["member_role"] == "title",
                    "native edition fields differ",
                )
            else:
                require(
                    edition["member_role"] == "supporting-file"
                    and all(
                        edition[k] is None
                        for k in ("title", "native_publication_year", "carried_forward", "metadata_json")
                    ),
                    "sidecar metadata differs",
                )
            member_map[edition["edition_id"]] = info
            checked("uscode_editions", edition)
            del body

        require(str(annual_year) in native_years, "archive has no title of expected year")
        for table in ("uscode_edition_structure", "uscode_edition_sections"):
            observed = rows(candidate / rebuild["outputs"][table]["path"])
            for edition in edition_rows:
                if edition["member_role"] != "title":
                    continue
                body = archive.read(member_map[edition["edition_id"]])
                for native, start, stop in native_items(body):
                    if table == "uscode_edition_sections" and native["section_label"] is None:
                        continue
                    row = next(observed)
                    position = "bytes:" + ":".join(str(v) for v in native["itempath_span"])
                    require(
                        row["edition_id"] == edition["edition_id"]
                        and row["source_position"] == position
                        and row["native_identifier"] == native["usckey"],
                        "native positional occurrence/member/identifier differs",
                    )
                    if table == "uscode_edition_structure":
                        expected = native
                        require(
                            row["observation_kind"] == "annual-itempath"
                            and row["native_label"] == native["section_label"]
                            and row["native_status"] is None,
                            "native structure columns differ",
                        )
                        kind = "annual-itempath"
                    else:
                        fields, body_text, issues = native_fields(body, start, stop)
                        expected = dict(
                            structure=native,
                            fields=fields,
                            body_text=body_text,
                            issues=list(dict.fromkeys(native["issues"] + issues)),
                        )
                        require(
                            row["source_kind"] == "annual"
                            and row["source_context"] == "annual-section-itempath"
                            and row["body_text"] == body_text,
                            "content columns/body rendering differ",
                        )
                        kind = "section-content"
                        categories[
                            "null_body" if body_text is None else "empty_body" if body_text == "" else "nonempty_body"
                        ] += 1
                        categories["bracketed"] += int(native["bracketed"])
                        categories["range_pieces"] += sum(p["kind"] == "range" for p in native["parts"])
                        categories["fields"] += len(fields)
                        categories["invalid_utf8"] += int("invalid_utf8" in expected["issues"])
                    require(
                        json.loads(row["source_observation_json"]) == expected,
                        "complete independent observation differs at " + edition["member_name"] + "/" + position,
                    )
                    require(
                        row["observation_id"] == identity(edition["edition_id"], kind, position),
                        "observation identity differs",
                    )
                    checked(table, row)
                del body
            require(next(observed, None) is None, "extra native positions/rows")
    require(all(counts[k] == rebuild["outputs"][k]["rows"] for k in SCHEMAS), "verified table counts differ")
    require(
        sha(source) == spec["sha256"] and sha(selection) == rebuild["manifest_sha256"],
        "source/selection drift during audit",
    )
    require({str(path): sha(path) for path in evidence_paths} == evidence_before, "audit evidence changed during reads")
    require(sha(candidate / "rebuild.json") == rebuild_sha, "rebuild changed during audit")
    require(
        sha(selection) == selection_sha and sha(candidate / "selection.json") == selection_sha,
        "selection changed during audit",
    )
    require(
        all(sha(candidate / entry["path"]) == entry["sha256"] for entry in rebuild["outputs"].values()),
        "final output digest differs from rebuild",
    )
    # Recalculate independent reader pins after the source checks.
    require(reader_pins() == code, "reader packages changed during audit")
    result = dict(
        status=f"verified-complete-selected-annual{annual_year}-archive",
        verdict="APPROVE",
        generation=rebuild["generation"],
        source_sha256=spec["sha256"],
        selection_sha256=selection_sha,
        rebuild_sha256=rebuild_sha,
        audit_sha256=sha(__file__),
        rows=dict(counts),
        content_categories=dict(categories),
        complete_original_members=len(edition_rows),
        evidence_sha256=evidence_before,
        scope=f"All member identities, original annual itempath positions, document pairing, headings, hierarchy labels, range/stub pieces, named field spans/nesting/rendered text, nullable body text, issues, public field/value closure including decoded JSON. One complete selected {annual_year} archive only; no whole-corpus, publication or deletion proof.",
        independence="Expected source observations use only raw ZIP bytes, independent comment/field scan and standard HTML parser events. Maintained helpers declare output schemas only.",
        limits="Text rendering verified against the documented transformation, not legal interpretation or facsimile typography. Public closure checks declared schemas plus local-locator and credential-key exclusion; not an assertion that all publisher prose is free of sensitive content. One original member and Arrow batch of 32 at a time; no temporary row store.",
    )
    return result


def _reserve_report(candidate: Path, selection: Path, output: Path):
    """Reject candidate/input aliases before exclusive fresh report creation."""
    candidate, selection = candidate.resolve(), selection.resolve()
    if output.exists() or output.is_symlink():
        raise FileExistsError("audit report already exists")
    output = output.resolve()
    require(not output.is_relative_to(candidate), "report must be outside candidate")
    selected = json.loads(selection.read_text())
    inputs = [selection.resolve(), Path(__file__).resolve()]
    for spec in selected.get("sources", []):
        source = Path(spec["path"])
        inputs.append((source if source.is_absolute() else selection.parent / source).resolve())
    require(output not in inputs, "report aliases an audit input")
    # Existing files (including hard links and symlinks) always refuse; mode x
    # also closes the creation race. No failure handler reopens this path.
    return output.open("x", encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--annual-year", required=True, type=int)
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
            result = audit(args.candidate, args.selection, annual_year=args.annual_year)
            exit_code = 0
        except Exception as error:
            result = dict(
                status="audit-failed",
                verdict="REQUEST CHANGES",
                annual_year=args.annual_year,
                error_type=type(error).__name__,
                error=str(error),
                audit_sha256=sha(__file__),
            )
            exit_code = 1
        report.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
