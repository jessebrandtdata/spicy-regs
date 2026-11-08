"""Committee seats for earlier Congresses from one GovInfo Congressional Directory edition each (ss-ehci).

spicy-docs reads what an edition prints (``spicy_docs.sources.govinfo.directory_rosters``). This module interprets
it into ``committee_assignments`` rows:

- **Committee.** A printed title maps to a Congress.gov ``system_code`` through the committee's own names (its
  current name, and each history entry's Library of Congress and official names) in force on the edition's
  ``dateIssued``, within the same chamber and parent. A name matching no code, or more than one, is an
  ``unmapped_committee`` disposition, never a guess. Party bodies printed among the select committees (campaign
  committees, steering and policy committees, conferences) are ``not_a_committee``.
- **Member.** A printed name and state resolve first against the edition's own MODS ``congMember`` records, then
  against member terms in force on ``dateIssued`` (the MODS omits a few members, e.g. Heinrich and Leger Fernandez in
  the 117th and Duffy in the 113th). An unresolved entry is an ``unmatched_member`` disposition.
- **Rank and role.** Rank is print order within the printed column, the chair first on the majority side. A
  printed role is normalized (Chairman becomes Chair) with ``position_basis = printed``. Where a list prints no
  Ranking Member and no other minority leader line, the first minority-column entry is labeled Ranking Member with
  ``position_basis = inferred`` (owner decision 2026-11-08).

Every printed entry ends as a row or a disposition; :func:`interpret_edition` returns both.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from spicy_docs.schemas.roster_tables import shape_directory_assignment
from spicy_docs.sources.govinfo.directory_rosters import (
    DirectoryEdition,
    DirectoryMember,
    DirectoryRosterError,
    PrintedGranule,
    PrintedSeat,
    normalize_name,
    parse_directory_granule,
    read_directory_mods,
    roster_granules,
)
from spicy_docs.sources.govinfo.discovery import package_granules_url

#: One edition per Congress: the latest each Congress has (owner decision 2026-11-08: the 115th has two,
#: CDIR-2018-07-27 and CDIR-2018-10-01, and the later is used). The 118th's only edition, CDIR-2024-04-25, serves no
#: granules and no member records (measured 2026-11-08), so it is not read here; the 119th belongs to the chamber
#: files.
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
        reviewed = REVIEWED_TITLES.get((parent, " ".join(title.split())))
        if reviewed in self.codes:
            return reviewed, "reviewed title"
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


class MemberIndex:
    """Members by (chamber, state), resolved from a printed name."""

    def __init__(self, people: Iterable[_Person]):
        self.by_state: dict[tuple[str, str], list[_Person]] = {}
        for person in people:
            self.by_state.setdefault((person.chamber, person.state), []).append(person)

    def resolve(self, name: str, state: str, chambers: Sequence[str]) -> tuple[_Person | None, str]:
        printed = normalize_name(name)
        tokens = [token for token in printed.split() if token not in _SUFFIX]
        core = " ".join(tokens)
        pool = [person for chamber in chambers for person in self.by_state.get((chamber, state), [])]
        exact = {p.bioguide_id: p for p in pool if printed in p.names or core in p.names}
        if len(exact) == 1:
            return next(iter(exact.values())), "exact name"
        last = {
            p.bioguide_id: p for p in pool if p.last_name and (core == p.last_name or core.endswith(" " + p.last_name))
        }
        if len(last) == 1:
            return next(iter(last.values())), "last name in state"
        if len(last) > 1 and tokens:
            first = [p for p in last.values() if any(n.split()[0] == tokens[0] for n in p.names if n)]
            if len(first) == 1:
                return first[0], "last and first name in state"
        return None, f"{len(exact)} exact, {len(last)} last-name candidates"


def mods_people(members: Iterable[DirectoryMember]) -> list[_Person]:
    return [
        _Person(m.bioguide_id, m.chamber, m.state, m.party or None, m.names, m.last_name)
        for m in members
        if m.chamber in ("H", "S")
    ]


def term_people(
    terms: Iterable[Mapping[str, Any]], members: Mapping[str, Mapping[str, Any]], date: str
) -> list[_Person]:
    """Members serving on ``date`` from SpicyRegs member terms, with every name form the members table holds."""
    people: dict[tuple[str, str], _Person] = {}
    for term in terms:
        start, end = str(term.get("term_start") or ""), str(term.get("term_end") or "")
        if not (start and start <= date and (not end or date < end)):
            continue
        bioguide = str(term["bioguide_id"])
        member = members.get(bioguide, {})
        last, first, nick = member.get("name_last") or "", member.get("name_first") or "", member.get("name_nickname")
        forms = {normalize_name(f"{first} {last}")}
        if nick:
            forms.add(normalize_name(f"{nick} {last}"))
        lasts = {normalize_name(last)}
        for other in _other_names(member):
            if other.get("last"):
                lasts.add(normalize_name(other["last"]))
                forms.add(normalize_name(f"{other.get('first') or first} {other['last']}"))
        chamber = "S" if term.get("term_type") == "sen" else "H"
        party = _PARTY.get(str(term.get("term_party") or ""), term.get("term_party") or None)
        for surname in lasts:
            people[(bioguide, surname)] = _Person(
                bioguide,
                chamber,
                str(term.get("term_state") or ""),
                party,
                tuple(sorted(forms)),
                surname,
                first or None,
            )
    return list(people.values())


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
    line: int
    context: str
    kind: str  # vacancy | unread_text | unmapped_committee | not_a_committee | unmatched_member | duplicate_seat
    text: str


@dataclass
class EditionResult:
    rows: list[dict] = field(default_factory=list)
    dispositions: list[Disposition] = field(default_factory=list)
    #: Each printed list's code and the tier that chose it (or why none did): ``(granule, title, code, how)``.
    mappings: list[tuple[str, str, str | None, str]] = field(default_factory=list)


def _member_chamber(seat: PrintedSeat, granule_chamber: str) -> tuple[str, ...]:
    if granule_chamber == "house":
        return ("H",)
    if granule_chamber == "senate":
        return ("S",)
    return (seat.member_chamber,) if seat.member_chamber else ("H", "S")


def interpret_edition(
    edition: DirectoryEdition,
    granules: Sequence[PrintedGranule],
    *,
    committees: Iterable[Mapping[str, Any]],
    terms: Iterable[Mapping[str, Any]],
    members: Mapping[str, Mapping[str, Any]],
    observed_at: str,
) -> EditionResult:
    """Rows and dispositions for one edition's printed seats."""
    date, congress = edition.date_issued, edition.congress
    names = CommitteeNames(committees, date)
    by_mods = MemberIndex(mods_people(edition.members))
    by_terms = MemberIndex(term_people(terms, members, date))
    result = EditionResult()
    for granule in granules:
        for printed in granule.dispositions:
            result.dispositions.append(
                Disposition(congress, printed.granule_id, printed.line, printed.context, printed.kind, printed.text)
            )
        lists: dict[tuple[str, str | None], list[PrintedSeat]] = {}
        for seat in granule.seats:
            lists.setdefault((seat.committee, seat.subcommittee), []).append(seat)
        for (title, sub_title), seats in lists.items():
            _interpret_list(result, names, by_mods, by_terms, edition, granule.chamber, title, sub_title, seats,
                            observed_at)  # fmt: skip
    _drop_duplicates(result)
    return result


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


def _ranks(seats: Sequence[PrintedSeat]) -> dict[int, int | None]:
    """Each printed seat's rank: its place in its printed column, the chair first on the majority side.

    Counted over every printed entry, so an entry that resolves to no member still holds its place. Another line
    printed above the columns (a vice chair) has no rank.
    """
    ranks: dict[int, int | None] = {}
    counts: Counter[str] = Counter()
    for index, seat in enumerate(seats):
        if seat.side == "chair" and seat.role not in ("Chair", "Co-Chair"):
            ranks[index] = None
            continue
        side = "minority" if seat.side == "minority" else "majority"
        counts[side] += 1
        ranks[index] = counts[side]
    return ranks


def _interpret_list(result, names, by_mods, by_terms, edition, chamber, title, sub_title, seats, observed_at) -> None:
    congress = edition.congress
    context = f"{title}/{sub_title}" if sub_title else title
    granule_id = seats[0].granule_id
    if _PARTY_BODY.search(title) and not re.search(r"(?i)\bethics\b", title):
        result.mappings.append((granule_id, context, None, "not a committee of record"))
        for seat in seats:
            result.dispositions.append(
                Disposition(congress, seat.granule_id, seat.line, context, "not_a_committee", seat.name)
            )
        return
    system_code, parent, how = _committee(names, chamber, title, sub_title)
    result.mappings.append((granule_id, context, system_code, how))
    if system_code is None:
        for seat in seats:
            result.dispositions.append(
                Disposition(congress, seat.granule_id, seat.line, context, "unmapped_committee", f"{seat.name}: {how}")
            )
        return
    people = []
    for seat in seats:
        chambers = _member_chamber(seat, chamber)
        person, why = by_mods.resolve(seat.name, seat.state, chambers)
        if person is None:
            person, why = by_terms.resolve(seat.name, seat.state, chambers)
        people.append((person, why))
    ranks = _ranks(seats)
    inferred = _inferred_ranking(seats, people)
    for index, (seat, (person, why)) in enumerate(zip(seats, people, strict=True)):
        if person is None:
            note = " (the first minority entry: its Ranking Member is not inferred)" if index == inferred else ""
            result.dispositions.append(
                Disposition(congress, seat.granule_id, seat.line, context, "unmatched_member",
                            f"{seat.name} ({seat.state}): {why}{note}")
            )  # fmt: skip
            continue
        position, basis = seat.role, ("printed" if seat.role else None)
        if index == inferred:
            position, basis = "Ranking Member", "inferred"
        result.rows.append(
            shape_directory_assignment(
                seat,
                congress=congress,
                date_issued=edition.date_issued,
                chamber="senate" if person.chamber == "S" else "house",
                system_code=system_code,
                parent_system_code=parent,
                member={"bioguide_id": person.bioguide_id, "name_last": _surname(seat.name),
                        "name_first": None, "party": person.party},
                rank=ranks[index],
                position=position,
                position_basis=basis,
                observed_at=observed_at,
            )
        )  # fmt: skip


def _inferred_ranking(seats: Sequence[PrintedSeat], people: Sequence[tuple[_Person | None, str]]) -> int | None:
    """The index of the first printed minority-column entry, when the list prints no minority leader of its own.

    A printed "Ranking Member" is that leader. So is a line printed above the columns whose member's party differs
    from the chair's (the Senate Ethics and Intelligence committees print their minority leader as Vice Chairman);
    a vice chair of the chair's own party is not (House committees print majority vice chairs there).
    """
    if any(seat.role == "Ranking Member" for seat in seats):
        return None
    chair = next((p for s, (p, _) in zip(seats, people, strict=True) if s.side == "chair" and s.role == "Chair"), None)
    for seat, (person, _) in zip(seats, people, strict=True):
        if seat.side == "chair" and seat.role not in ("Chair", "Co-Chair"):
            if person is None or chair is None or person.party != chair.party:
                return None
    return next((index for index, seat in enumerate(seats) if seat.side == "minority" and seat.role is None), None)


def _surname(printed: str) -> str | None:
    tokens = [t for t in re.sub(r",.*$", "", printed).split() if normalize_name(t) not in _SUFFIX]
    return tokens[-1] if tokens else None


def _drop_duplicates(result: EditionResult) -> None:
    """One row per (congress, system_code, bioguide_id): the first printed; a repeat is a disposition."""
    seen: set[tuple[str, str, str]] = set()
    kept = []
    for row in result.rows:
        key = (row["congress"], row["system_code"], row["bioguide_id"])
        if key in seen:
            result.dispositions.append(
                Disposition(row["congress"], row["source_granule"], int(row["source_line"]), row["system_code"],
                            "duplicate_seat", row["bioguide_id"])
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


def directory_congresses_pending(prior_assignments: Path | None, reread: Iterable[int] = ()) -> list[int]:
    """Congresses with an edition and no directory rows published yet, plus any named for re-reading."""
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
                    f"SELECT DISTINCT congress FROM read_parquet('{prior_assignments}') WHERE congress_basis = 'directory'"
                ).fetchall()
            }
    return sorted(set(DIRECTORY_EDITIONS) - (done - set(reread)))


def read_rows(path: Path | None) -> list[dict]:
    if path is None:
        return []
    import duckdb

    relation = duckdb.sql(f"SELECT * FROM read_parquet('{path}')")
    return [dict(zip(relation.columns, row, strict=True)) for row in relation.fetchall()]
