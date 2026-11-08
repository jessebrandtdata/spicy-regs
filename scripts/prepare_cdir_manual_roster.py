"""One-off: read the 118th Congressional Directory's committee rosters from its PDF into a reviewed input (ss-ehci).

CDIR-2024-04-25 serves no granules and no member records, and its package text interleaves the two printed
columns, so the rosters are read from the PDF's own layout: ``pdftotext -layout`` (poppler) keeps each printed
column at its position. Every roster entry becomes one record with its PDF page and layout-text line, the printed
text, and the column it is printed in; a vacancy or text that is not a seat becomes a record of its kind. Nothing
is resolved here: the committee-rosters build interprets these records like any directory edition's.

    uv run --frozen python scripts/prepare_cdir_manual_roster.py CDIR-2024-04-25.pdf \\
        --output src/spicy_regs/directory_manual/CDIR-2024-04-25.json

The output records the PDF's SHA-256, the tool version and each page's layout-text SHA-256, and its own
``entries_sha256`` (canonical JSON of the entries), which ``directory_rosters.MANUAL_EDITIONS`` pins. A build
refuses the input unless the retained PDF it acquires has that SHA-256. Review notes live in ``review``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

PACKAGE_ID = "CDIR-2024-04-25"
CONGRESS = "118"
DATE_ISSUED = "2024-04-25"
PDF_SHA256 = "c9bed6a33130ea49fbe606b732bf35e93ea2ccfa6c163039645e855cc6db2758"

SECTIONS = (
    ("STANDING COMMITTEES OF THE SENATE", "senate"),
    ("SELECT AND SPECIAL COMMITTEES", None),  # its chamber is on the next line ("OF THE SENATE" / "OF THE HOUSE")
    ("STANDING COMMITTEES OF THE HOUSE", "house"),
    ("JOINT COMMITTEES", "joint"),
)
END = "CONGRESSIONAL ADVISORY BOARDS"
HEADER = re.compile(r"^\s*(?:\d+\s+Congressional Directory|(?:Committees of the (?:Senate|House)|Joint Committees|"
                    r"Select and Special Committees[\w ]*)\s+\d+)\s*$")  # fmt: skip
FOOTER = re.compile(
    r"^\s*(?:\d+|S—The Capitol;.*|HVC—.*|SD—.*|SR—.*|H or HT—.*|House Office Building[;.].*|\*\s+\*\s+\*)\s*$"
)
#: "(ST)" or "(ST-DD)"; the printer also drops a parenthesis ("Gillibrand NY)", "Merkley (OR") and lowercases one
#: state ("(md-01)"), all read as the entry they are.
STATE = re.compile(
    r"\((?P<state>[A-Za-z]{2})(?:-(?P<district>[0-9A-Z]*)\s*)?\)"
    r"|(?<=\s)(?P<state2>[A-Z]{2})\)|\((?P<state3>[A-Z]{2})$"
)
#: The older style "Name, of State." (printed once, CDIR-2024-04-25 p. 707).
OLD_STYLE = re.compile(r"^(?P<name>.+?), of (?P<state>[A-Z][A-Za-z ]+)\.$")
STATE_CODES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA", "Colorado": "CO",
    "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}  # fmt: skip
ROLE = re.compile(r"\b(?P<role>(?:VICE |CO-)?CHAIR(?:MAN|WOMAN|PERSON)?|RANKING (?:MINORITY )?MEMBER|EX OFFICIO)\b")
ADDRESS = re.compile(
    r"^(?:Room |HVC|\(ph\)|https?://|Meets |\[|Majority:|Minority:)|\(ph\)|CHOB|Building|\bmeets\b"
    r"|^upon the call of the Chair\.$"
)
STAFF = re.compile(r"^(?:COMMITTEE |SUBCOMMITTEE |MAJORITY |MINORITY )?STAFF\b|\bSTAFF$")
# Column boundary on the layout text: the right column starts at 40-57 on every roster page read.
RIGHT_COLUMN = 30


def layout_pages(pdf: Path, first: int, last: int) -> list[str]:
    text = subprocess.run(
        ["pdftotext", "-layout", "-f", str(first), "-l", str(last), str(pdf), "-"],
        check=True, capture_output=True, text=True,
    ).stdout  # fmt: skip
    return text.split("\f")[: last - first + 1]


def section_pages(pdf: Path) -> tuple[int, int]:
    """The first page of the Senate's standing committees and the last page before the advisory boards."""
    pages = subprocess.run(["pdftotext", "-layout", str(pdf), "-"], check=True, capture_output=True,
                           text=True).stdout.split("\f")  # fmt: skip
    first = next(i for i, page in enumerate(pages) if re.search(r"^\s*STANDING COMMITTEES OF THE SENATE", page, re.M))
    last = next(i for i, page in enumerate(pages) if i > first and re.search(rf"^\s*{END}", page, re.M))
    return first + 1, last  # 1-based first page; ``last`` (0-based) is the 1-based page before the boards


def fragments(line: str) -> list[tuple[int, str]]:
    """Text runs separated by three or more spaces, with their start columns."""
    return [(m.start(), m.group().strip()) for m in re.finditer(r"\S(?:.*?\S)?(?=\s{3,}|\s*$)", line)]


def parse_entry(text: str) -> dict | None:
    """``Name (ST[-DD]) [ROLE]``, the role printed after the state or (once) between name and state."""
    old = OLD_STYLE.match(text)
    if old and old["state"] in STATE_CODES:
        return {"name": old["name"], "state": STATE_CODES[old["state"]], "district": None, "role": None}
    state = STATE.search(text)
    if not state:
        return None
    before, after = text[: state.start()].strip(), text[state.end() :].strip(" ,")
    role = None
    inline = ROLE.search(before)
    if inline:
        role, before = inline["role"], before[: inline.start()].strip()
    if after:
        trailing = ROLE.fullmatch(after)
        if not trailing:
            return None
        role = trailing["role"]
    name = before.rstrip(" ,.")
    if not name or not re.match(r"[A-Z]", name):
        return None
    code = (state["state"] or state["state2"] or state["state3"]).upper()
    return {"name": name, "state": code, "district": state["district"] or None, "role": role}


def is_caps(text: str) -> bool:
    letters = [c for c in text if c.isalpha()]
    return bool(letters) and all(c.isupper() for c in letters)


def read(pdf: Path) -> dict:
    first, last = section_pages(pdf)
    pages = layout_pages(pdf, first, last)
    entries: list[dict] = []
    page_digests = {}
    chamber = committee = sub = None
    hint = None
    mode = "top"  # top | members | sub | staff
    order = 0
    pending: list[str] = []

    def emit(kind, page, line_no, side, text, parsed=None):
        nonlocal order
        record = {"kind": kind, "page": page, "line": line_no, "chamber": chamber, "committee": committee,
                  "subcommittee": sub, "side": side, "printed": text, "member_chamber": hint}  # fmt: skip
        if parsed:
            order += 1
            record |= parsed | {"order": order}
        entries.append(record)

    def start(new_committee=None, new_sub=None):
        nonlocal committee, sub, order, hint
        if new_committee is not None:
            committee, sub, hint = new_committee, None, None
        if new_sub is not None:
            sub = new_sub
        order = 0

    for offset, page_text in enumerate(pages):
        page = first + offset
        page_digests[str(page)] = hashlib.sha256(page_text.encode()).hexdigest()
        lines = page_text.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = " ".join(line.split())
            line_no = i + 1
            i += 1
            if not stripped or HEADER.match(line) or FOOTER.match(line):
                continue
            section = next(((h, c) for h, c in SECTIONS if stripped.startswith(h)), None)
            if section:
                chamber = section[1] or ("senate" if "SENATE" in " ".join(lines[i : i + 2]) else "house")
                mode, committee, sub, pending = "top", None, None, []
                continue
            if stripped in ("OF THE SENATE", "OF THE HOUSE"):
                continue
            if stripped.startswith("[") and stripped not in ("[VACANT]",) and not stripped.startswith("[VACANT]"):
                while not stripped.endswith("]") and i < len(lines):  # a bracketed note, possibly wrapped
                    stripped = " ".join(lines[i].split())
                    i += 1
                continue
            if stripped == "SUBCOMMITTEES":
                mode = "sub"
                continue
            if STAFF.search(stripped) and is_caps(stripped):
                mode = "staff"
                continue
            if stripped in ("SENATE", "HOUSE"):
                hint = stripped[0]
                continue
            runs = fragments(line)
            parsed = [(col, text, parse_entry(text)) for col, text in runs]
            seats = [p for p in parsed if p[2] or p[1].startswith("[VACANT]")]
            if ADDRESS.search(stripped) and not seats:
                continue  # an office, telephone, web or meeting line
            if seats and mode != "staff" and committee is not None:
                if pending:  # a subcommittee title read just above
                    start(new_sub=" ".join(pending))
                    pending = []
                for col, text, entry in parsed:
                    side = "minority" if col >= RIGHT_COLUMN else "majority"
                    wrapped = ROLE.fullmatch(text)
                    prior = next((e for e in reversed(entries) if e["kind"] == "seat" and e["committee"] == committee
                                 and e["subcommittee"] == sub and e["side"] in ((side, "chair") if side == "majority"
                                 else (side,))), None)  # fmt: skip
                    if entry is None and wrapped and prior is not None and not prior["role"]:
                        # A role printed on the line below its entry ("Fleischmann (TN-03)" / "CHAIR", p. 662).
                        prior["role"], prior["role_wrapped_line"] = wrapped["role"], line_no
                        if side == "majority" and re.match(r"CHAIR|CO-CHAIR", wrapped["role"]):
                            prior["side"] = "chair"
                        continue
                    if entry is None and text.startswith("[VACANT]"):
                        emit("vacancy", page, line_no, side, text)
                    elif entry is None:
                        emit("unread_text", page, line_no, side, text)
                    else:
                        if entry["role"] and re.match(r"CHAIR|CO-CHAIR", entry["role"]) and side == "majority":
                            side = "chair"
                        emit("seat", page, line_no, side, text, entry)
                continue
            if is_caps(stripped) and mode in ("members", "sub") and committee is not None:
                pending.append(stripped)  # a subcommittee title, possibly wrapped
                continue
            if not is_caps(stripped) and not ADDRESS.search(stripped) and "—" not in stripped:
                # A committee title (centered, mixed case, possibly wrapped) is followed by its office lines or
                # its first entries; anything else is staff or notes text.
                title = [stripped]
                j = i
                while j < len(lines) and lines[j].strip() and not ADDRESS.search(lines[j].strip()) \
                        and not STATE.search(lines[j]) and not is_caps(lines[j].strip()):  # fmt: skip
                    title.append(" ".join(lines[j].split()))
                    j += 1
                k = j
                while k < len(lines) and not lines[k].strip():
                    k += 1
                follower = lines[k].strip() if k < len(lines) else ""
                joined = " ".join(title)
                # Titles carry no sentence punctuation; staff lines ("Moore, Lowell Schwartz, Brandon Yoder. ...") do.
                titled = not re.search(r"[.:;—]|,$", joined) and len(line) - len(line.lstrip()) >= 4
                if titled and (ADDRESS.search(follower) or STATE.search(follower)) and chamber:
                    start(new_committee=joined)
                    mode, sub, pending = "members", None, []
                    i = j
                    continue
            if mode == "staff":
                continue
            if committee is not None and mode in ("members", "sub"):
                emit("unread_text", page, line_no, "line", stripped)
    return {"first_page": first, "last_page": last, "page_layout_sha256": page_digests, "entries": entries}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    body = args.pdf.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    if digest != PDF_SHA256:
        raise SystemExit(f"{args.pdf} is not {PACKAGE_ID}'s PDF (sha256 {digest})")
    version = subprocess.run(["pdftotext", "-v"], capture_output=True, text=True).stderr.splitlines()[0]
    read_result = read(args.pdf)
    entries = read_result.pop("entries")
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    document = {
        "package_id": PACKAGE_ID,
        "congress": CONGRESS,
        "date_issued": DATE_ISSUED,
        "source": {"pdf_sha256": digest, "pdf_bytes": len(body), "tool": f"{version} -layout", **read_result},
        "entries_sha256": hashlib.sha256(canonical).hexdigest(),
        "review": [],
        "entries": entries,
    }
    if args.output.exists():  # keep the review notes written by hand
        document["review"] = json.loads(args.output.read_text())["review"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # One entry per line, so a review diff shows each changed entry.
    head = json.dumps({k: v for k, v in document.items() if k != "entries"}, indent=1, ensure_ascii=False)
    lines = ",\n".join(json.dumps(entry, sort_keys=True, ensure_ascii=False) for entry in entries)
    args.output.write_text(head[:-2] + ',\n "entries": [\n' + lines + "\n ]\n}\n")
    kinds: dict[str, int] = {}
    for entry in entries:
        kinds[entry["kind"]] = kinds.get(entry["kind"], 0) + 1
    print(f"{PACKAGE_ID}: pages {read_result['first_page']}-{read_result['last_page']}, {kinds}, "
          f"entries_sha256 {document['entries_sha256']}")  # fmt: skip


if __name__ == "__main__":
    main()
