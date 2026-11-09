"""Committee seats for earlier Congresses from one GovInfo Congressional Directory edition each (ss-ehci).

spicy-docs reads what an edition prints (``spicy_docs.sources.govinfo.directory_rosters``). This module interprets
it into ``committee_assignments`` rows:

- **Committee.** A printed title maps to a Congress.gov ``system_code`` through the committee's own names (its
  current name, and each history entry's Library of Congress and official names) in force on the edition's
  ``dateIssued``, within the same chamber and parent. A name matching no code, or more than one, is an
  ``unmapped_committee`` disposition, never a guess. Party bodies printed among the select committees (campaign
  committees, steering and policy committees, conferences) are ``not_a_committee``.
- **Member.** A printed name and state resolve against the edition's MODS ``congMember`` records, then member
  terms in force on ``dateIssued``. A misprint resolved against them (a wrong state, a surname off by two letters, a
  state code in the name), a hand-reviewed broken entry (:data:`REVIEWED_ENTRIES`) and an entry printed in the other
  party's column say ``entry_label = corrected`` beside ``printed_entry``; a member printed after leaving says
  ``after_departure``; a body with no Congress.gov code (:data:`DIRECTORY_DEFINED`) says ``no_congress_gov_code``.
- **Rank and role.** Rank is print order within the printed column, the chair first on the majority side. A
  printed role is normalized (Chairman becomes Chair) with ``position_basis = printed``. Where a list prints no
  minority leader, the first minority-column entry is labeled Ranking Member with ``position_basis = inferred``
  (owner decision 2026-11-08).
- **The 118th.** Its only edition serves no granules, so a reviewed reading of its PDF (:data:`MANUAL_EDITIONS`,
  ``congress_basis = directory-manual``) is reconciled with archived chamber files (:func:`reconcile`).

Every printed entry ends as a row or a ``committee_directory_entries`` row (owner decision 2026-11-08: drop nothing).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol, cast

from spicy_docs.schemas.roster_tables import shape_directory_assignment, shape_directory_entry
from spicy_docs.sources.govinfo.directory_rosters import (
    DirectoryEdition,
    DirectoryMember,
    DirectoryRosterError,
    PrintedDisposition,
    PrintedGranule,
    PrintedSeat,
    normalize_name,
    normalize_role,
    parse_directory_granule,
    read_directory_mods,
    roster_granules,
)
from spicy_docs.sources.govinfo.discovery import package_granules_url

#: One edition per Congress: the latest each Congress has (owner decision 2026-11-08: the 115th has two,
#: CDIR-2018-07-27 and CDIR-2018-10-01, and the later is used). The 118th's only edition, CDIR-2024-04-25, serves no
#: granules and no member records (measured 2026-11-08); it is a :data:`MANUAL_EDITIONS` entry. The 119th belongs to
#: the chamber files.
DIRECTORY_EDITIONS: Mapping[int, str] = {
    113: "CDIR-2014-02-18",
    114: "CDIR-2016-02-12",
    115: "CDIR-2018-10-01",
    116: "CDIR-2020-07-22",
    117: "CDIR-2022-10-26",
}

#: Words that carry no identity in a committee title.
_NOISE = frozenset(
    "committee subcommittee on the of and select special permanent house senate united states us joint s for "
    "department departments".split()
)
#: Joint committees, printed in the House's joint granule, are the ``j*`` codes.
JOINT_CHAMBER = "joint"
#: The codes congressional-tech's committee normalization keeps for the four joint committees, preferred when the
#: Congress.gov list carries more than one code with the same name (Joint Economic: jsec00, jhje00, jjec00).
JOINT_CANONICAL = frozenset({"jsec00", "jslc00", "jstx00", "jspr00"})
#: Party organizations printed among the select committees; they are not committees of record.
_PARTY_BODY = re.compile(r"(?i)\b(?:campaign committee|steering|policy committee|conference|republican|democratic)\b")
_PARTY = {"Democrat": "D", "Republican": "R", "Independent": "I", "Libertarian": "L"}
_SUFFIX = frozenset({"jr", "sr", "ii", "iii", "iv"})


def _title_key(text: str) -> frozenset[str]:
    """Identity words of a title, without footnote marks (``\\1\\``, ``*``) or a printed acronym (``(EMDC)``)."""
    text = re.sub(r"\\\d+\\|\*|\([A-Z]{2,}\)", " ", text.replace("&", " and "))
    return frozenset(word for word in normalize_name(text).split() if word not in _NOISE)


@dataclass(frozen=True, slots=True)
class _Name:
    chamber: str  # house | senate | joint
    parent: str | None
    key: frozenset[str]
    system_code: str
    in_force: bool | None  # a dated history name in force on the date; None for the undated current list name


#: Printed titles no Congress.gov name of the code states, reviewed by hand: ``(parent, printed title) -> code``.
#: Each was checked against an independent roster of the same weeks (congress-legislators
#: committee-membership-current.yaml at 53547039, 2014-02-13), and the code was in force on the edition's date.
REVIEWED_TITLES: Mapping[tuple[str | None, str], str] = {
    # Ten of its eleven printed seats are SSFR01's; the eleventh there is an ex officio member CDIR does not print.
    ("ssfr00", "European Affairs"): "ssfr01",
    # SSCM27 ("Tourism, Competitiveness, and Innovation", 2009-03-12..2015-02-13); membership Jaccard 0.71, the
    # highest of any Commerce subcommittee.
    ("sscm00", "Competitiveness, Innovation, and Export Promotion"): "sscm27",
    # The Library committee's printed names; JSLC00's one name is "Joint Committee on the Library".
    (None, "Joint Committee on the Library of Congress"): "jslc00",
    (None, "Joint Committee of Congress on the Library"): "jslc00",
    # SSJU04, the Judiciary immigration subcommittee under every name it has held since 1995; the 114th's name is
    # not in its history.
    ("ssju00", "Immigration and the National Interest"): "ssju04",
}
#: Printed bodies with no Congress.gov code: ``(parent, printed title) -> (directory-defined code, kind)``. Their rows
#: say ``entry_label = no_congress_gov_code``. The 114th House Financial Services task force has no code on
#: Congress.gov's committee list or in the Clerk's MemberData.xml of 2016-03-05 (measured 2026-11-08).
DIRECTORY_DEFINED: Mapping[tuple[str | None, str], str] = {
    ("hsba00", "Task Force to Investigate Terrorism Financing"): "hsba00-tf-terrorism-financing",
}


class CommitteeNames:
    """Congress.gov committee names, by chamber and parent, judged on one date.

    The history's dates are the publisher's, and they lag: CDIR-2014-02-18 prints the House Intelligence
    subcommittee "Terrorism, Human Intelligence, Analysis and Counterintelligence", whose history entry ends
    2013-01-02. So a match is tried in tiers, all within the printed chamber and parent, the first with exactly one
    code winning: a reviewed title (:data:`REVIEWED_TITLES`); a name in force on the date; any name of a code
    active on the date (one with a history entry covering it); any name ever held; then a strict subset or superset
    of an active code's name ("Science and Space" for SSCM24 "Space, Science, and Competitiveness"; a name of two
    words or more inside a longer title). Nothing looser is tried: a title no tier settles is a disposition.
    """

    def __init__(self, committees: Iterable[Mapping[str, Any]], date: str):
        self.names: list[_Name] = []
        self.active: set[str] = set()
        self.codes: set[str] = set()
        for row in committees:
            code = str(row["system_code"]).lower()
            self.codes.add(code)
            chamber = str(row.get("chamber") or "").lower()
            parent = (row.get("parent_system_code") or None) and str(row["parent_system_code"]).lower()
            history = _history(row)
            if not history:
                self.active.add(code)
            self.names.append(_Name(chamber, parent, _title_key(str(row.get("name") or "")), code, None))
            for entry in history:
                start = (entry.get("startDate") or "")[:10]
                end = (entry.get("endDate") or "")[:10]
                in_force = not ((start and start > date) or (end and end < date))
                if in_force:
                    self.active.add(code)
                for field_name in ("libraryOfCongressName", "officialName"):
                    if entry.get(field_name):
                        self.names.append(_Name(chamber, parent, _title_key(entry[field_name]), code, in_force))

    def resolve(self, chamber: str, parent: str | None, title: str) -> tuple[str | None, str]:
        """``(system_code, how)``, or ``(None, why)`` when no tier settles on one code."""
        key = _title_key(title)
        if not key:
            return None, "empty title"
        printed = " ".join(title.split())
        reviewed = REVIEWED_TITLES.get((parent, printed))
        if reviewed in self.codes:
            return reviewed, "reviewed title"
        if (parent, printed) in DIRECTORY_DEFINED:
            return DIRECTORY_DEFINED[(parent, printed)], "no Congress.gov code"
        pool = [n for n in self.names if n.parent == parent and (parent is not None or n.chamber == chamber)]
        equal = [n for n in pool if n.key == key]
        tiers = (
            ("name in force", {n.system_code for n in equal if n.in_force}),
            ("name of an active code", {n.system_code for n in equal if n.system_code in self.active}),
            ("earlier name", {n.system_code for n in equal}),
            # The printed title is a shortened name, or adds words to a name of at least two (a one-word name such as
            # "Water" would match any title holding the word).
            ("subset of an active code's name", {
                n.system_code for n in pool
                if n.system_code in self.active and (key < n.key or (len(n.key) >= 2 and n.key < key))
            }),
        )  # fmt: skip
        why = "no name"
        for how, codes in tiers:
            if len(codes) > 1 and codes & JOINT_CANONICAL:
                codes = codes & JOINT_CANONICAL
            if len(codes) == 1:
                return next(iter(codes)), how
            if codes:
                why = f"ambiguous {how}: {sorted(codes)}"
        return None, why


def _history(row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """The committee's history entries, from the contract's JSON column or the published typed column."""
    value = row.get("history_json", row.get("history"))
    if isinstance(value, str):
        value = json.loads(value)
    return [entry for entry in value or [] if isinstance(entry, Mapping)]


@dataclass(frozen=True, slots=True)
class _Person:
    bioguide_id: str
    chamber: str  # H or S
    state: str
    party: str | None
    names: tuple[str, ...]
    last_name: str
    first_name: str | None = None
    #: The member record's surname as written ("Van Hollen"), or the earlier one it was matched by: the row's
    #: ``name_last``, not the printed name's last word (ss-d2ma).
    record_last: str | None = None


class MemberIndex:
    """Members by (chamber, state), resolved from a printed name.

    The printed name and state are tried first. When they settle on no one, a correction is tried, and a row it
    yields says ``entry_label = corrected`` beside its printed entry: the name with a trailing state code removed
    ("Jamaal Bowman, NY"); a surname within two letters of one serving in that state ("Thom Tills" for Tillis); the
    exact full name serving in another state ("James B. Renacci" printed of South Carolina).
    """

    def __init__(self, people: Iterable[_Person]):
        self.by_state: dict[tuple[str, str], list[_Person]] = {}
        self.by_chamber: dict[str, list[_Person]] = {}
        for person in people:
            self.by_state.setdefault((person.chamber, person.state), []).append(person)
            self.by_chamber.setdefault(person.chamber, []).append(person)

    def _in_state(self, printed: str, pool: Sequence[_Person]) -> tuple[_Person | None, str]:
        tokens = _tokens(printed)
        core = " ".join(tokens)
        exact = {p.bioguide_id: p for p in pool if printed in p.names or core in p.names}
        if len(exact) == 1:
            return next(iter(exact.values())), "exact name"
        last = {
            p.bioguide_id: p for p in pool if p.last_name and (core == p.last_name or core.endswith(" " + p.last_name))
        }
        if len(last) == 1:
            person = next(iter(last.values()))
            if tokens and not any(n.split()[0] == tokens[0] for n in person.names if n) and self._near(tokens, pool):
                # The surname is another member's, the first name is not ("Vicente Gonzales" is not Tony Gonzales).
                return None, "surname matches a member whose first name differs"
            return person, "last name in state"
        if len(last) > 1 and tokens:
            first = [p for p in last.values() if any(n.split()[0] == tokens[0] for n in p.names if n)]
            if len(first) == 1:
                return first[0], "last and first name in state"
        return None, f"{len(exact)} exact, {len(last)} last-name candidates"

    def resolve(self, name: str, state: str, chambers: Sequence[str]) -> tuple[_Person | None, str]:
        person, how = self.resolve_labeled(name, state, chambers)[:2]
        return person, how

    def resolve_labeled(self, name: str, state: str, chambers: Sequence[str]) -> tuple[_Person | None, str, str | None]:
        """``(person, how, label)``; ``label`` is ``corrected`` when a printed value had to be corrected."""
        printed = normalize_name(name)
        pool = [person for chamber in chambers for person in self.by_state.get((chamber, state), [])]
        person, why = self._in_state(printed, pool)
        if person is not None:
            return person, why, None
        stripped = re.sub(rf"\s{state.lower()}$", "", printed)
        if stripped != printed:
            person, how = self._in_state(stripped, pool)
            if person is not None:
                return person, f"{how}, state code removed from the printed name", "corrected"
        tokens = _tokens(printed)
        near = self._near(tokens, pool)
        if len(near) == 1:
            return near[0], "surname within two letters, first name equal", "corrected"
        elsewhere = {p.bioguide_id: p for chamber in chambers for p in self.by_chamber.get(chamber, [])
                     if len(tokens) >= 2 and any(n.split()[:1] == tokens[:1] and n.split()[-1:] == tokens[-1:]
                                                 for n in p.names if n)}  # fmt: skip
        if len(elsewhere) == 1:
            return next(iter(elsewhere.values())), "same first and last name serving for another state", "corrected"
        return None, why, None

    @staticmethod
    def _near(tokens: Sequence[str], pool: Sequence[_Person]) -> list[_Person]:
        """Members whose surname is within two letters of the printed one and whose first name is the printed one."""
        if len(tokens) < 2:
            return []
        found = {}
        for p in pool:
            width = len(p.last_name.split())
            if p.last_name and len(tokens) > width and _edits(" ".join(tokens[-width:]), p.last_name) <= 2 \
                    and any(n.split()[0] == tokens[0] for n in p.names if n):  # fmt: skip
                found[p.bioguide_id] = p
        return list(found.values())


def _tokens(printed: str) -> list[str]:
    """A printed name's words without suffixes or stray print marks ("Garret Graves T4")."""
    return [token for token in printed.split() if token not in _SUFFIX and not any(c.isdigit() for c in token)]


def _edits(a: str, b: str) -> int:
    """Levenshtein distance, for short surnames."""
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        current = [i]
        for j, cb in enumerate(b, 1):
            current.append(min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (ca != cb)))
        previous = current
    return previous[-1]


def mods_people(
    members: Iterable[DirectoryMember], records: Mapping[str, Mapping[str, Any]] | None = None
) -> list[_Person]:
    """The edition's own members; ``records`` (the members table) supplies each one's surname as written."""
    records = records or {}
    return [
        _Person(
            m.bioguide_id,
            m.chamber,
            m.state,
            m.party or None,
            m.names,
            m.last_name,
            record_last=records.get(m.bioguide_id, {}).get("name_last") or None,
        )
        for m in members
        if m.chamber in ("H", "S")
    ]


def term_people(
    terms: Iterable[Mapping[str, Any]], members: Mapping[str, Mapping[str, Any]], date: str, *, departed: bool = False
) -> list[_Person]:
    """Members serving on ``date`` from SpicyRegs member terms, with every name form the members table holds.

    ``departed`` instead returns members whose term in that seat ended before ``date`` and who held none on it: the
    people an edition may still print after they left (Tom Udall on the 117th Senate Appropriations list).
    """
    people: dict[tuple[str, str], _Person] = {}
    serving = {str(t["bioguide_id"]) for t in terms if _in_force(t, date)}
    for term in terms:
        end = str(term.get("term_end") or "")
        if departed:
            if not end or end > date or str(term["bioguide_id"]) in serving:
                continue
        elif not _in_force(term, date):
            continue
        bioguide = str(term["bioguide_id"])
        member = members.get(bioguide, {})
        last, first, nick = member.get("name_last") or "", member.get("name_first") or "", member.get("name_nickname")
        forms = {normalize_name(f"{first} {last}")}
        if nick:
            forms.add(normalize_name(f"{nick} {last}"))
        lasts = {normalize_name(last): last}
        for other in _other_names(member):
            if other.get("last"):
                lasts.setdefault(normalize_name(other["last"]), other["last"])
                forms.add(normalize_name(f"{other.get('first') or first} {other['last']}"))
        chamber = "S" if term.get("term_type") == "sen" else "H"
        party = _PARTY.get(str(term.get("term_party") or ""), term.get("term_party") or None)
        for surname, written in lasts.items():
            people[(bioguide, surname)] = _Person(
                bioguide,
                chamber,
                str(term.get("term_state") or ""),
                party,
                tuple(sorted(forms)),
                surname,
                first or None,
                written or None,
            )
    return list(people.values())


def _in_force(term: Mapping[str, Any], date: str) -> bool:
    start, end = str(term.get("term_start") or ""), str(term.get("term_end") or "")
    return bool(start and start <= date and (not end or date < end))


def _other_names(member: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    value = member.get("other_names_json", member.get("other_names"))
    if isinstance(value, str):
        value = json.loads(value)
    return [entry for entry in value or [] if isinstance(entry, Mapping)]


@dataclass(frozen=True, slots=True)
class Disposition:
    """A printed line that published no row, and why."""

    congress: str
    granule_id: str
    line: int | str
    context: str
    kind: str  # vacancy | unread_text | not_a_committee | unresolved_seat | duplicate_seat
    text: str
    side: str = "line"
    note: str | None = None


@dataclass
class EditionResult:
    rows: list[dict] = field(default_factory=list)
    dispositions: list[Disposition] = field(default_factory=list)
    #: ``committee_directory_entries`` rows, one per disposition.
    entries: list[dict] = field(default_factory=list)
    #: Every corrected or relabeled entry: ``(granule, line, side) -> (printed, reason)``.
    corrections: dict[tuple[str, str, str], tuple[str, str]] = field(default_factory=dict)
    #: Each printed list's code and the tier that chose it (or why none did): ``(granule, title, code, how)``.
    mappings: list[tuple[str, str, str | None, str]] = field(default_factory=list)


def _member_chamber(seat: PrintedSeat, granule_chamber: str) -> tuple[str, ...]:
    if granule_chamber == "house":
        return ("H",)
    if granule_chamber == "senate":
        return ("S",)
    return (seat.member_chamber,) if seat.member_chamber else ("H", "S")


#: Printed entries the reader could not seat because the print itself is broken, corrected by hand:
#: ``(granule, line) -> (name, state, reason, printed text)``. Applied only when the reader left exactly that text
#: unread on that line, so a shifted line cannot seat another entry. The row says ``entry_label = corrected``;
#: ``printed_entry`` keeps what was printed.
REVIEWED_ENTRIES: Mapping[tuple[str, int], tuple[str, str, str, str]] = {
    ("CDIR-2018-10-29-HOUSECOMMITTEES", 3235): (
        "Jenniffer Gonzalez-Colon",
        "PR",
        'printed "nniffer Gonzalez-Colon, of Puerto" with "Rico." lost at the page break',
        "nniffer Gonzalez-Colon, of Puerto",
    ),
    # Agriculture's Rural Development and Energy subcommittee, majority column, between Daines and Hyde-Smith. The
    # htm (sha256 1ff00c09270918a02b0be80f7c07412a340646261d7fdd1ca4be2b39822ee4ca) prints it at line 100; the granule
    # PDF (sha256 158d749196866fb5c11d0329b73a15ae852b6b8edb7fa365fb3a6db8025949d5) prints it on PDF page 2 (printed
    # page 346).
    ("CDIR-2018-10-29-SENATECOMMITTEES", 100): (
        "Deb Fischer",
        "NE",
        'printed "Deb Fischer, of Nebraksa." with the state misspelled',
        "Deb Fischer, of Nebraksa.",
    ),
}


def interpret_edition(
    edition: DirectoryEdition,
    granules: Sequence[PrintedGranule],
    *,
    committees: Iterable[Mapping[str, Any]],
    terms: Sequence[Mapping[str, Any]],
    members: Mapping[str, Mapping[str, Any]],
    observed_at: str,
    congress_basis: str = "directory",
) -> EditionResult:
    """Rows and dispositions for one edition's printed seats: every printed entry ends as one or the other."""
    date, congress = edition.date_issued, edition.congress
    names = CommitteeNames(committees, date)
    by_mods = MemberIndex(mods_people(edition.members, members))
    by_terms = MemberIndex(term_people(terms, members, date))
    departed = MemberIndex(term_people(terms, members, date, departed=True))
    result = EditionResult()
    for granule in granules:
        seats = list(granule.seats)
        for printed in granule.dispositions:
            reviewed = REVIEWED_ENTRIES.get((printed.granule_id, printed.line))
            if reviewed and printed.kind == "unread_text" and printed.text == reviewed[3]:
                committee, _, sub = printed.context.partition("/")
                listed = [s for s in granule.seats if s.committee == committee and (s.subcommittee or "") == sub]
                # Its print position: the order of the next entry printed below it in its column (ties break by line).
                below = [s.order for s in listed if s.side == printed.side and s.line > printed.line]
                seats.append(replace(
                    listed[0], line=printed.line, name=reviewed[0], state=reviewed[1], printed_role=None, role=None,
                    side=printed.side, order=min(below, default=10_000 + printed.line),
                ))  # fmt: skip
                result.corrections[(printed.granule_id, str(printed.line), printed.side)] = (printed.text, reviewed[2])
                continue
            committee = printed.context.partition("/")[0]
            if printed.kind == "unread_text" and _is_party_body(committee):
                # A party officer line left unread ("Recruitment.--Susan W. Brooks, of Indiana."): as its seats would be.
                result.dispositions.append(
                    Disposition(congress, printed.granule_id, printed.line, printed.context, "not_a_committee",
                                printed.text, printed.side, "a party organization, not a committee")
                )  # fmt: skip
                continue
            result.dispositions.append(
                Disposition(congress, printed.granule_id, printed.line, printed.context, printed.kind, printed.text,
                            printed.side)
            )  # fmt: skip
        lists: dict[tuple[str, str | None], list[PrintedSeat]] = {}
        for seat in sorted(seats, key=lambda s: (s.committee, s.subcommittee or "", s.order, s.line)):
            lists.setdefault((seat.committee, seat.subcommittee), []).append(seat)
        for (title, sub_title), listed in lists.items():
            _interpret_list(result, names, (by_mods, by_terms, departed), edition, granule.chamber, title, sub_title,
                            listed, observed_at, congress_basis)  # fmt: skip
    _drop_duplicates(result)
    result.entries = [_entry_row(d, edition, congress_basis, observed_at) for d in result.dispositions]
    return result


def _entry_row(disposition: Disposition, edition: DirectoryEdition, congress_basis: str, observed_at: str) -> dict:
    kind = {"unmapped_committee": "unresolved_seat", "unmatched_member": "unresolved_seat"}.get(
        disposition.kind, disposition.kind
    )
    return shape_directory_entry(
        congress=edition.congress, congress_basis=congress_basis, granule_id=disposition.granule_id,
        line=disposition.line, side=disposition.side, date_issued=edition.date_issued, kind=kind,
        context=disposition.context, text_printed=disposition.text, note=disposition.note, observed_at=observed_at,
    )  # fmt: skip


def _committee(
    names: CommitteeNames, chamber: str, title: str, sub_title: str | None
) -> tuple[str | None, str | None, str]:
    """``(system_code, parent_system_code, why)`` for one printed list."""
    top, how = names.resolve(chamber, None, title)
    if top is None and chamber != JOINT_CHAMBER:
        top, how = names.resolve(JOINT_CHAMBER, None, title)  # a joint list printed in a chamber granule
    if top is None or sub_title is None:
        return top, None, how
    sub, sub_how = names.resolve(chamber, top, sub_title)
    return sub, top, sub_how


def _ranks(seats: Sequence[PrintedSeat], skip: set[int]) -> dict[int, int | None]:
    """Each printed seat's rank: its place in its printed column, the chair first on the majority side.

    Counted over every printed entry, so an entry that resolves to no member still holds its place. Another line
    printed above the columns (a vice chair), or an entry in the other party's column (``skip``), has no rank.
    """
    ranks: dict[int, int | None] = {}
    counts: Counter[str] = Counter()
    for index, seat in enumerate(seats):
        if index in skip or (seat.side == "chair" and seat.role not in ("Chair", "Co-Chair")):
            ranks[index] = None
            continue
        side = "minority" if seat.side == "minority" else "majority"
        counts[side] += 1
        ranks[index] = counts[side]
    return ranks


def _resolve(indexes, seat: PrintedSeat, chambers: Sequence[str]) -> tuple[_Person | None, str, str | None]:
    by_mods, by_terms, departed = indexes
    for index in (by_mods, by_terms):
        person, how, label = index.resolve_labeled(seat.name, seat.state, chambers)
        if person is not None and label is None:
            return person, how, None
    for index in (by_mods, by_terms):
        person, how, label = index.resolve_labeled(seat.name, seat.state, chambers)
        if person is not None:
            return person, how, label
    person, how = departed.resolve(seat.name, seat.state, chambers)
    if person is not None:
        return person, f"{how}; no term in force on the edition's date", "after_departure"
    return None, how, None


def _interpret_list(result, names, indexes, edition, chamber, title, sub_title, seats, observed_at, basis) -> None:
    congress = edition.congress
    context = f"{title}/{sub_title}" if sub_title else title
    granule_id = seats[0].granule_id
    if _is_party_body(title):
        result.mappings.append((granule_id, context, None, "not a committee of record"))
        for seat in seats:
            result.dispositions.append(
                Disposition(congress, seat.granule_id, seat.line, context, "not_a_committee",
                            f"{seat.name} ({seat.state})", seat.side, "a party organization, not a committee")
            )  # fmt: skip
        return
    system_code, parent, how = _committee(names, chamber, title, sub_title)
    result.mappings.append((granule_id, context, system_code, how))
    if system_code is None:
        for seat in seats:
            result.dispositions.append(
                Disposition(congress, seat.granule_id, seat.line, context, "unmapped_committee",
                            f"{seat.name} ({seat.state})", seat.side, f"no committee code: {how}")
            )  # fmt: skip
        return
    people = [_resolve(indexes, seat, _member_chamber(seat, chamber)) for seat in seats]
    crossed = _crossed_columns(seats, people) if chamber != JOINT_CHAMBER else set()
    ranks = _ranks(seats, crossed)
    inferred = _inferred_ranking(seats, people, crossed)
    for index, (seat, (person, why, label)) in enumerate(zip(seats, people, strict=True)):
        if person is None:
            note = "; the first minority entry, so no Ranking Member is inferred" if index == inferred else ""
            result.dispositions.append(
                Disposition(congress, seat.granule_id, seat.line, context, "unmatched_member",
                            f"{seat.name} ({seat.state})", seat.side, f"no member: {why}{note}")
            )  # fmt: skip
            continue
        position, position_basis = seat.role, ("printed" if seat.role else None)
        if index == inferred:
            position, position_basis = "Ranking Member", "inferred"
        if index in crossed:
            label = "corrected"
            result.corrections[(seat.granule_id, str(seat.line), seat.side)] = (
                f"{seat.name} ({seat.state}) printed in the {seat.side} column",
                f"party {person.party} is the other column's; no rank",
            )
        elif label is not None:
            result.corrections[(seat.granule_id, str(seat.line), seat.side)] = (f"{seat.name} ({seat.state})", why)
        elif (seat.granule_id, str(seat.line), seat.side) in result.corrections:
            label = "corrected"  # a reviewed entry (REVIEWED_ENTRIES)
        if system_code in DIRECTORY_DEFINED.values():
            label = label or "no_congress_gov_code"
        result.rows.append(
            shape_directory_assignment(
                seat, congress=congress, date_issued=edition.date_issued,
                chamber="senate" if person.chamber == "S" else "house", system_code=system_code,
                parent_system_code=parent,
                member={"bioguide_id": person.bioguide_id, "name_last": person.record_last or _surname(seat.name),
                        "name_first": None, "party": person.party, "state": person.state or None},
                rank=ranks[index], position=position, position_basis=position_basis, observed_at=observed_at,
                congress_basis=basis, entry_label=label,
            )
        )  # fmt: skip
        reviewed = REVIEWED_ENTRIES.get((seat.granule_id, seat.line))
        if reviewed and label == "corrected":
            result.rows[-1]["printed_entry"] = result.corrections[(seat.granule_id, str(seat.line), seat.side)][0]


def _is_party_body(title: str) -> bool:
    """A party organization's list (campaign, steering and policy committees, conferences), not a committee."""
    return bool(_PARTY_BODY.search(title)) and not re.search(r"(?i)\bethics\b", title)


def _crossed_columns(seats: Sequence[PrintedSeat], people: Sequence[tuple]) -> set[int]:
    """Entries printed in the other party's column: a minority-column member of the chair's party, or the reverse.

    CDIR-2024-04-25 prints Gary C. Peters (D) at the head of the 118th Senate Appropriations Military Construction
    subcommittee's minority column. Independents, and lists whose chair is unknown, are left as printed.
    """
    chair = next((p for s, (p, _, _) in zip(seats, people, strict=True) if s.side == "chair" and s.role == "Chair"),
                 None)  # fmt: skip
    if chair is None or chair.party not in ("D", "R"):
        return set()
    crossed = set()
    for index, (seat, (person, _, _)) in enumerate(zip(seats, people, strict=True)):
        if person is None or person.party not in ("D", "R") or seat.side == "chair":
            continue
        if (seat.side == "minority") == (person.party == chair.party):
            crossed.add(index)
    return crossed


def _inferred_ranking(seats: Sequence[PrintedSeat], people: Sequence[tuple], crossed: set[int]) -> int | None:
    """The index of the first printed minority-column entry, when the list prints no minority leader of its own.

    A printed "Ranking Member" is that leader, and so is any role printed in the minority column (the 118th Senate
    Ethics committee prints James Lankford there as Vice Chairman). So is a line printed above the columns whose
    member's party differs from the chair's (earlier editions print Senate Ethics and Intelligence vice chairmen
    there); a vice chair of the chair's own party is not (House committees print majority vice chairs). Nothing is
    inferred from a list that prints a member in the other party's column: its print order is not reliable there.
    """
    if crossed or any(seat.role == "Ranking Member" or (seat.side == "minority" and seat.role) for seat in seats):
        return None  # a printed minority leader, or columns printed out of party order
    chair = next((p for s, (p, _, _) in zip(seats, people, strict=True) if s.side == "chair" and s.role == "Chair"),
                 None)  # fmt: skip
    for seat, (person, _, _) in zip(seats, people, strict=True):
        if seat.side == "chair" and seat.role not in ("Chair", "Co-Chair"):
            if person is None or chair is None or person.party != chair.party:
                return None
    return next(
        (i for i, seat in enumerate(seats) if seat.side == "minority" and seat.role is None and i not in crossed), None
    )


def _surname(printed: str) -> str | None:
    tokens = [t for t in re.sub(r",.*$", "", printed).split() if normalize_name(t) not in _SUFFIX]
    return tokens[-1] if tokens else None


def _drop_duplicates(result: EditionResult) -> None:
    """One row per (congress, system_code, bioguide_id): the first printed; a repeat is a ``duplicate_seat`` entry."""
    seen: set[tuple[str, str, str]] = set()
    kept = []
    # Where a seat is printed twice, the printing that needed no correction is the seat (CDIR-2024-04-25 prints Juan
    # Ciscomani in both columns of one House Appropriations subcommittee).
    for row in sorted(result.rows, key=lambda r: r["entry_label"] == "corrected"):
        key = (row["congress"], row["system_code"], row["bioguide_id"])
        if key in seen:
            result.dispositions.append(
                Disposition(row["congress"], row["source_granule"], row["source_line"], row["committee_name"],
                            "duplicate_seat", row["printed_entry"], "line",
                            f"printed again on {row['system_code']}; the first printing is the seat")
            )  # fmt: skip
            continue
        seen.add(key)
        kept.append(row)
    result.rows = kept


# ---------------------------------------------------------------- acquisition and the build step


@dataclass(frozen=True, slots=True)
class DirectoryCapture:
    """One edition as read: its MODS facts, its printed roster granules, and when the MODS was captured."""

    edition: DirectoryEdition
    granules: tuple[PrintedGranule, ...]
    observed_at: str


class DirectorySource(Protocol):
    """What the build needs of an edition reader."""

    def edition(self, package_id: str) -> DirectoryCapture: ...


class GovInfoDirectorySource:
    """Reads an edition from GovInfo: package metadata, the granule list, then each roster granule's ``htm``.

    ``1 + 3 + 3G`` requests for ``G`` roster granules (five per edition measured), each through the acquirers the
    caller supplies; the rollup supplies the ``Retained*`` ones, so every response lands in the central raw store.
    """

    def __init__(self, acquirer: Any, reader: Any):
        self.acquirer, self.reader = acquirer, reader

    def edition(self, package_id: str) -> DirectoryCapture:
        _, mods, _ = self.acquirer.acquire_package_metadata(package_id)
        edition = read_directory_mods(mods.body)
        if edition.package_id != package_id:
            raise DirectoryRosterError(f"{package_id}: MODS names {edition.package_id}")
        listing = [
            record
            for page in self.reader.granules(package_granules_url(package_id, page_size=1000))
            for record in page.records
        ]
        chosen = roster_granules(listing)
        if set(chosen.values()) != {"house", "senate", "joint"}:
            raise DirectoryRosterError(f"{package_id}: roster granules for {sorted(set(chosen.values()))} only")
        granules = tuple(
            parse_directory_granule(
                self.acquirer.acquire_granule(package_id, granule_id, prefer=("htm",)).body_capture.body,
                granule_id,
                chamber,
            )
            for granule_id, chamber in chosen.items()
        )
        return DirectoryCapture(edition, granules, mods.observed_at)


@dataclass(frozen=True, slots=True)
class ManualEdition:
    """A reviewed one-off reading of an edition's PDF (``scripts/prepare_cdir_manual_roster.py``)."""

    package_id: str
    resource: str  # under spicy_regs/
    entries_sha256: str
    pdf_sha256: str


#: The 118th's only edition serves no granules or members and its package text interleaves the two printed columns,
#: so its rosters were read from the PDF's layout once and reviewed (owner decision 2026-11-08). The input is pinned
#: by its entries' digest, and a build reads it only beside the retained PDF it was made from.
MANUAL_EDITIONS: Mapping[int, ManualEdition] = {
    118: ManualEdition(
        "CDIR-2024-04-25",
        "directory_manual/CDIR-2024-04-25.json",
        "d225832f90f065cd10941290c1b8dfd0e1e4f24829d3042c60bbbad4e21dd5a6",
        "c9bed6a33130ea49fbe606b732bf35e93ea2ccfa6c163039645e855cc6db2758",
    ),
}
#: Internet Archive captures of the chamber files read beside a manual edition, nearest its date
#: (owner decision 2026-11-08): the House file published 2024-05-01, the Senate file updated 2024-09-11 (the only
#: Senate capture nearer than 2023-03-24).
ARCHIVED_CHAMBER_FILES: Mapping[int, Mapping[str, str]] = {
    118: {"house": "20240501161500", "senate": "20240911154349"},
}


def manual_edition(spec: ManualEdition) -> tuple[DirectoryEdition, tuple[PrintedGranule, ...]]:
    """The edition and its printed granules (one per chamber section) from the reviewed input, refused if altered."""
    from importlib.resources import files

    document = json.loads(files("spicy_regs").joinpath(spec.resource).read_text(encoding="utf-8"))
    entries = document["entries"]
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    if (
        hashlib.sha256(canonical).hexdigest() != spec.entries_sha256
        or document["source"]["pdf_sha256"] != spec.pdf_sha256
    ):
        raise DirectoryRosterError(f"{spec.package_id}: the manual roster input does not match its pin")
    granules = {chamber: PrintedGranule(spec.package_id, chamber) for chamber in ("senate", "house", "joint")}
    for entry in entries:
        granule = granules[entry["chamber"]]
        # A manual edition's locator is PDF page:line, where a granule's is its line number.
        line = cast(Any, f"{entry['page']}:{entry['line']}")
        if entry["kind"] == "seat":
            role = entry["role"]
            hint = entry["member_chamber"] or {"house": "H", "senate": "S"}.get(entry["chamber"])
            granule.seats.append(PrintedSeat(
                spec.package_id, line, entry["committee"], entry["subcommittee"], entry["name"], entry["state"],
                role, normalize_role(role), entry["side"], entry["order"], hint,
            ))  # fmt: skip
        else:
            context = f"{entry['committee']}/{entry['subcommittee']}" if entry["subcommittee"] else entry["committee"]
            granule.dispositions.append(
                PrintedDisposition(spec.package_id, line, str(context), entry["kind"], entry["printed"], entry["side"])
            )
    edition = DirectoryEdition(spec.package_id, document["congress"], document["date_issued"], ())
    return edition, tuple(granules.values())


class ManualDirectorySource:
    """A manual edition, read only beside its retained PDF: SUMMARY, MODS and PDF through the caller's acquirer."""

    def __init__(self, acquirer: Any):
        self.acquirer = acquirer

    def edition(self, package_id: str) -> DirectoryCapture:
        spec = next(spec for spec in MANUAL_EDITIONS.values() if spec.package_id == package_id)
        pdf = self.acquirer.acquire(package_id, prefer=("pdf",))
        if pdf.body_capture.sha256 != f"sha256:{spec.pdf_sha256}":
            raise DirectoryRosterError(f"{package_id}: the PDF served ({pdf.body_capture.sha256}) is not the one read")
        edition, granules = manual_edition(spec)
        return DirectoryCapture(edition, granules, pdf.body_capture.observed_at)


@dataclass
class Reconciliation:
    """A manual edition's seats beside the archived chamber files', for one Congress."""

    rows: list[dict]
    report: dict


def reconcile(directory_rows: Sequence[dict], file_rows: Sequence[dict]) -> Reconciliation:
    """One row per seat: the chamber file's where it lists the seat, else the directory's; every difference reported.

    The chamber files state rank and leadership for their own Congress, so their row stands where both list a seat.
    A seat only one source lists is kept from that source and labeled by its ``congress_basis``. The report lists,
    per committee the chamber files cover, the seats each source alone lists and the chairs and ranking members
    that differ, and the committees only one source lists.
    """
    by_file = {(r["system_code"], r["bioguide_id"]): r for r in file_rows}
    by_directory = {(r["system_code"], r["bioguide_id"]): r for r in directory_rows}
    covered = {r["system_code"] for r in file_rows}
    rows = list(by_file.values()) + [r for k, r in by_directory.items() if k not in by_file]
    seats: dict[str, dict[str, list[str]]] = {}
    for code in sorted(covered | {r["system_code"] for r in directory_rows}):
        mine = {b for c, b in by_directory if c == code}
        theirs = {b for c, b in by_file if c == code}
        if code in covered and mine and mine != theirs:
            seats[code] = {"directory_only": sorted(mine - theirs), "chamber_file_only": sorted(theirs - mine)}

    def leaders(rows_of: Mapping, title: str) -> dict[str, str]:
        out = {}
        for (code, bioguide), row in rows_of.items():
            position = normalize_role(row.get("position")) or ""
            if position == title or (title == "Ranking Member" and position == "Ranking"):
                out[code] = bioguide
        return out

    differences = {}
    for title in ("Chair", "Ranking Member"):
        a, b = leaders(by_directory, title), leaders(by_file, title)
        differences[title] = {code: {"directory": a[code], "chamber_file": b[code]}
                              for code in sorted(set(a) & set(b)) if a[code] != b[code]}  # fmt: skip
    directory_codes = {r["system_code"] for r in directory_rows}
    report = {
        "seats_directory": len(by_directory), "seats_chamber_file": len(by_file), "seats_both": len(by_directory.keys() & by_file.keys()),
        "seats_published": len(rows),
        "committees_directory_only": sorted(directory_codes - covered),
        "committees_chamber_file_only": sorted(covered - directory_codes),
        "seat_differences": seats, "leader_differences": differences,
    }  # fmt: skip
    return Reconciliation(rows, report)


class EditionSources:
    """Every edition through one GovInfo acquirer: a manual edition beside its PDF, any other from its granules."""

    def __init__(self, acquirer: Any, reader: Any):
        self.granules = GovInfoDirectorySource(acquirer, reader)
        self.manual = ManualDirectorySource(acquirer)

    def edition(self, package_id: str) -> DirectoryCapture:
        if any(spec.package_id == package_id for spec in MANUAL_EDITIONS.values()):
            return self.manual.edition(package_id)
        return self.granules.edition(package_id)


def directory_congresses_pending(prior_assignments: Path | None, reread: Iterable[int] = ()) -> list[int]:
    """Congresses with an edition (or a manual one) and none of its rows published yet, plus any named again."""
    done: set[int] = set()
    if prior_assignments is not None:
        import duckdb

        columns = {
            row[0] for row in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{prior_assignments}')").fetchall()
        }
        if "congress_basis" in columns:
            done = {
                int(congress)
                for (congress,) in duckdb.sql(
                    f"SELECT DISTINCT congress FROM read_parquet('{prior_assignments}') "
                    "WHERE congress_basis IN ('directory', 'directory-manual')"
                ).fetchall()
            }
    return sorted((set(DIRECTORY_EDITIONS) | set(MANUAL_EDITIONS)) - (done - set(reread)))


def read_rows(path: Path | None) -> list[dict]:
    if path is None:
        return []
    import duckdb

    relation = duckdb.sql(f"SELECT * FROM read_parquet('{path}')")
    return [dict(zip(relation.columns, row, strict=True)) for row in relation.fetchall()]
