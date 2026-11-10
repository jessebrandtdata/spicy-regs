"""Read values a table keeps in its ETL receipts, by table, row key and field: the logic of ``read_receipt_fields``.

A receipt holds the fields its table's policy declares and, for most families, a mapping of the row's fields as
the family's mapper received them. What that gives differs by table and can shrink with a policy, so the fields on
offer are worked out per call from the installed policy and the bundled ``receipt_fields.json``
(``receipt_field_declarations``); nothing here assumes a receipt holds a row's whole original record. The receipt
codec and the row identity digest are ``etl_receipts``' own, imported when a lookup runs: that module needs
pyarrow, which describing a table does not.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from collections.abc import Callable, Mapping, Sequence
from functools import lru_cache
from importlib.resources import files
from typing import Any, NamedTuple

from spicy_regs.relationship_views.core import DETAIL_STATES
from spicy_regs.subject_catalog import descriptors

TOOL = "read_receipt_fields"
MAX_KEYS = 100
#: The most receipts a lookup reads to find its keys, and the most subject rows it searches for a key no receipt
#: holds (owner decision 2026-10-05). Counted after pruning: only row groups whose footer bounds admit the table and
#: one of the keys' ``identity_json`` are read (receipts carry those bounds since the standard layout, 2026-11-09),
#: so a table of any size is served while its keys fall in a few 64k-row groups; a group whose footer gives no
#: identity bounds admits every key. Row groups that mix policy versions count against it too (:func:`_stated_identities`).
#: Measured 2026-10-05 on the public bucket: sam_entities' 797,525 receipts cost one lookup 28.6 MiB in 800 range
#: requests; 2026-11-09 on a bill-family slice in the standard layout, DuckDB read ~1.5 MB per cold lookup.
SCAN_ROW_BOUND = 2_000_000

#: A found field's states. The first four are the field-state views' own (``DETAIL_STATES``), so a row and field
#: both can answer read the same; the fifth is a NULL no declared read marker governs.
UNREAD, NOT_STATED, STATED_EMPTY, STATED = DETAIL_STATES
NULL_UNMARKED = "null_unmarked"
STATE_MEANINGS = {
    STATED: "The receipt holds a value for the field.",
    STATED_EMPTY: "The field was read and the receipt holds an empty value: empty text, list or object.",
    UNREAD: "The table's read marker says this row's field was not read, so no value is known.",
    NOT_STATED: "The table's read marker says the field was read and the source stated no value.",
    NULL_UNMARKED: "The receipt holds no value, and nothing recorded says whether the field was read for this row.",
}
FOUND, AMBIGUOUS, RECEIPT_MISSING, NOT_IN_TABLE = "found", "ambiguous", "receipt_missing", "not_in_table"
RECEIPT_MEANINGS = {
    FOUND: "One accepted receipt holds this key.",
    AMBIGUOUS: "More than one accepted receipt holds this key (receipts_found); none is chosen.",
    RECEIPT_MISSING: "The table holds this key and no accepted receipt does: a fault in this publication.",
    NOT_IN_TABLE: "Neither the table nor an accepted receipt holds this key. Keys are exact and case-sensitive.",
}

#: One receipt member read by row number, so a range of rows selects row groups without reading a column.
_ROWS = "read_parquet(?, file_row_number = true, hive_partitioning = false)"
_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)\Z")


@lru_cache(maxsize=1)
def _declared() -> dict[str, dict]:
    """The bundled declaration (``receipt_field_declarations.record``), read once per process."""
    return json.loads(files("spicy_regs").joinpath("receipt_fields.json").read_text(encoding="utf-8"))["tables"]


def retired(table: str) -> str | None:
    """Why ``table`` has no receipt to read once its receipts are retired (``subject_catalog.RECEIPTS_RETIRED``)."""
    from spicy_regs.subject_catalog import retired_receipt_datasets

    if table not in retired_receipt_datasets():
        return None
    return (f"{table} no longer publishes per-row ETL receipts: what they held is in the table's own columns, which "
            f"query_sql reads. Its last generation with receipts is kept as an archive "
            f"(docs/pins/receipt-archive.json).")


def carried_fields(table: str) -> dict[str, str | None] | None:
    """Each field ``table``'s receipts carry, with the mapping that holds it (``None``: a receipt field itself).

    Read from the installed policy on every call: its receipt fields are the fields, and a mapping's declared
    contents are on offer only while the policy still keeps that mapping. ``None`` for a name with no installed
    row identity (not a table, or processing evidence only) and for a table whose receipts are retired.
    """
    policy = descriptors().get(table)
    if policy is None or policy["receipt_only"] or not policy["identity_fields"] or retired(table):
        return None
    fields: dict[str, str | None] = dict.fromkeys(policy["receipt_fields"])
    for container, names in _declared().get(table, {}).get("containers", {}).items():
        if container in policy["receipt_fields"]:
            for name in names:
                fields.setdefault(name, container)
    return fields


def _meanings(table: str, names: Sequence[str], columns: Mapping[str, Any]) -> dict[str, str | None]:
    """The dictionary's meaning of each field: its subject column's, else its own, else none stated."""
    own = _declared().get(table, {}).get("meanings", {})
    return {name: (columns.get(name) or {}).get("description") or own.get(name) for name in names}


def describe(table: str, entry: Mapping[str, Any], *, available: bool) -> tuple[dict[str, Any], dict[str, str]] | None:
    """What ``describe_table`` adds for ``table``: what to ask the tool for, and the meanings it leaves to detail.

    ``entry`` is the table's dictionary entry. The fields named are those no subject column of the same name
    carries: a column's value is in the table, and its description is already in the reply (a receipt that also
    holds the column's value as its mapper received it answers for it by the column's name all the same). Nor is
    a field the table holds under another column name (the declaration's ``renamed``, such as a list received as
    ``<column>_json``): the receipt keeps it as received and still answers for it by name, and the table has its
    value. A field the dictionary gives no meaning has none here. ``None`` where the table has no installed row
    identity. A table whose receipts are retired says so, with no fields and nothing available.
    """
    if (reason := retired(table)) is not None:
        return {"tool": TOOL, "available": False, "retired": reason}, {}
    fields = carried_fields(table)
    if fields is None:
        return None
    columns = {column["column_name"]: column for column in entry.get("columns", [])}
    renamed = _declared().get(table, {}).get("renamed", {})
    names = [name for name in fields if name not in columns and renamed.get(name) not in columns]
    return (
        {"tool": TOOL, "available": available,
         "identity_fields": {name: columns.get(name, {}).get("column_type")
                             for name in descriptors()[table]["identity_fields"]},
         "fields": names},
        {name: meaning for name, meaning in _meanings(table, names, {}).items() if meaning},
    )


class Member(NamedTuple):
    """One table's receipt member in this connection, and the generation that pins it."""

    location: str
    sha256: str | None
    family: str | None
    generation_id: str
    artifact_digest: str | None
    snapshot_id: str | None = None
    manifest_key: str | None = None


def selected_member(index: Mapping, local: Mapping | None, base_url: str, table: str,
                    snapshot: Mapping | None = None) -> Member | None:
    """The receipt member paired with the subjects this connection selects.

    A native build root names its member itself; a download holds only its selected families' members; a remote
    connection reads the member under the family's generation prefix. ``None`` where the table's family has no
    receipts in this connection, or a download selected a sibling table of the family and not this one.
    """
    from spicy_regs.sources.publication import rulemaking_receipt, table_owner

    native = (local or {}).get("native", {}).get(table)
    if native is not None:
        return Member(native["receipts"], None, None, native["generation_id"], None)
    owner = table_owner(index, f"{table}.parquet")
    if owner is None and local is None and snapshot is not None and f"{table}.parquet" in snapshot["tables"]:
        receipt = rulemaking_receipt(snapshot)
        if receipt is not None and table in receipt["datasets"]:
            return Member(f"{base_url.rstrip('/')}/{receipt['remote_key']}", "sha256:" + receipt["sha256"],
                          None, receipt["generationId"], None, snapshot["snapshot_id"], snapshot["manifest_key"])
    if owner is None or table not in owner[1].get("etlReceipts", {}).get("datasets", ()):
        return None
    family, entry = owner
    receipt = entry["etlReceipts"]
    path = f"{entry['prefix']}/{receipt['key']}"
    if local is None:
        location = f"{base_url}/{path}"
    elif path in local.get("receipt_members", {}) and table in local.get("selected_tables", ()):
        location = local["receipt_members"][path]
    else:
        return None
    return Member(location, receipt["sha256"], family, receipt["generationId"], entry["artifactDigest"])


def tables_with_receipts(index: Mapping, local: Mapping | None, base_url: str,
                         snapshot: Mapping | None = None) -> list[str]:
    """Every table this connection can answer for: an installed row identity and a selected receipt member."""
    return sorted(table for table in descriptors()
                  if carried_fields(table) is not None
                  and selected_member(index, local, base_url, table, snapshot) is not None)


class _Group(NamedTuple):
    """One row group of a receipt member: its rows' position and the footer's bounds on three columns."""

    start: int
    rows: int
    datasets: tuple[str | None, str | None]
    versions: tuple[str | None, str | None]
    identities: tuple[str | None, str | None] = (None, None)

    @property
    def end(self) -> int:
        return self.start + self.rows


#: Each pinned member's row groups by (location, sha256): a pinned member is immutable, so one footer read serves
#: the process. A member with no pin (a native build root's) is read on every call. A concurrent first use may
#: read twice.
_FOOTERS: dict[tuple[str, str], tuple[_Group, ...]] = {}
#: What a pinned member's receipts state for a dataset (:func:`_stated_identities`), by (location, sha256, dataset).
_STATED: dict[tuple, dict[str, tuple[str, list]]] = {}


def _cached(cache: dict, key: tuple, pinned: bool, read: Callable[[], Any]) -> Any:
    """``read()``, kept under ``key`` for a pinned member; another thread may empty the cache at any point."""
    if not pinned:
        return read()
    value = cache.get(key)
    if value is None:
        if len(cache) >= 256:  # generations move daily; a long-lived process forgets old ones
            cache.clear()
        value = cache[key] = read()
    return value


def _footer(cursor: Any, member: Member) -> tuple[_Group, ...]:
    """The member's row groups in file order, from its footer alone."""

    def read() -> tuple[_Group, ...]:
        bounds = ", ".join(
            f"any_value(stats_{bound}_value) FILTER (path_in_schema = '{column}')"
            for column in ("dataset", "policy_version", "identity_json") for bound in ("min", "max"))
        groups, start = [], 0
        for _, rows, *stats in cursor.execute(
            f"SELECT row_group_id, any_value(row_group_num_rows), {bounds} FROM parquet_metadata(?) "
            "WHERE path_in_schema IN ('dataset', 'policy_version', 'identity_json') "
            "GROUP BY row_group_id ORDER BY row_group_id", [member.location],
        ).fetchall():
            groups.append(_Group(start, rows, (stats[0], stats[1]), (stats[2], stats[3]), (stats[4], stats[5])))
            start += rows
        return tuple(groups)

    return _cached(_FOOTERS, (member.location, member.sha256 or ""), member.sha256 is not None, read)


def _within(bounds: tuple[str | None, str | None], value: str) -> bool:
    """Whether footer ``bounds`` admit ``value``; a group that states none admits everything."""
    low, high = bounds
    return low is None or high is None or low <= value <= high


def _runs(groups: Sequence[_Group], indexes: Sequence[int]) -> list[tuple[int, int]]:
    """Row ranges covering ``indexes``' groups, adjacent groups merged."""
    runs: list[tuple[int, int]] = []
    for index in sorted(indexes):
        group = groups[index]
        if runs and runs[-1][1] == group.start:
            runs[-1] = (runs[-1][0], group.end)
        else:
            runs.append((group.start, group.end))
    return runs


def _stated_identities(cursor: Any, member: Member, groups: Sequence[_Group], candidates: Sequence[int],
                       dataset: str) -> dict[str, tuple[str, list]]:
    """One accepted receipt's record id and decoded identity for every policy_version the dataset's receipts state.

    Admission accepts the current policy and registered earlier policies (``etl_receipts.receipt_policies``),
    and a carried receipt keeps its version (``rebind_receipt``). A dataset can therefore state several versions;
    every version present is read. A group whose footer states one version holds no other; a group stating a
    range is read for the versions this dataset's rows hold, refused past :data:`SCAN_ROW_BOUND` rows before any
    is read: this read is what refuses a lookup whose versions identify a row differently, so it is never skipped.
    """
    from spicy_regs.etl_receipts import _unpack

    def read() -> dict[str, tuple[str, list]]:
        versions = {groups[i].versions[0] for i in candidates
                    if groups[i].versions[0] is not None and groups[i].versions[0] == groups[i].versions[1]}
        mixed = [i for i in candidates if groups[i].versions[0] is None or groups[i].versions[0] != groups[i].versions[1]]
        if (rows := sum(groups[i].rows for i in mixed)) > SCAN_ROW_BOUND:
            raise ValueError(
                f"{TOOL} cannot answer keys for {dataset}: its receipts' row groups mix policy versions across "
                f"{rows:,} receipts, over the {SCAN_ROW_BOUND:,}-receipt bound, and every version must be read to "
                "check it identifies a row as this server does. Ask again once this generation's receipts are "
                "rewritten, or see describe_table's notes for which receipt values a row's own columns give.")
        for start, end in _runs(groups, mixed):
            versions.update(version for (version,) in cursor.execute(
                f"SELECT DISTINCT policy_version FROM {_ROWS} WHERE file_row_number >= ? AND file_row_number < ? "
                "AND dataset = ?", [member.location, start, end, dataset]).fetchall())
        stated = {}
        for version in sorted(versions - {None}):
            row = cursor.execute(
                f"SELECT record_id, identity_json FROM {_ROWS} WHERE file_row_number >= ? AND file_row_number < ? "
                "AND dataset = ? AND policy_version = ? AND outcome = 'accepted' LIMIT 1",
                [member.location, groups[candidates[0]].start, groups[candidates[-1]].end, dataset, version],
            ).fetchone()
            if row is not None:
                stated[version] = (row[0], _unpack(json.loads(row[1])))
        return stated

    if not candidates:
        return {}
    return _cached(_STATED, (member.location, member.sha256 or "", dataset), member.sha256 is not None, read)


def _kind(dtype: Any) -> str:
    """The supported native identity type. Any other type refuses: a key typed as text would
    digest to another record id than the writer's, and every lookup would read as a miss."""
    import pyarrow as pa

    if pa.types.is_integer(dtype):
        return "integer"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "text"
    if pa.types.is_timestamp(dtype) and dtype.unit == "us" and dtype.tz == "UTC":
        return "timestamp"
    raise ValueError(f"{TOOL} types text, whole-number and UTC timestamp row identities only; this table's has a {dtype} field.")


def _identity_words(policy: Any) -> str:
    """The table's identity fields and their types, as a refusal names them."""
    return ", ".join(
        f"{name} ({_kind(policy.subject_schema.field(name).type)}"
        + (", may be null" if name in policy.nullable_identity_fields else "") + ")"
        for name in policy.identity_fields)


def _refuse_other_identity(policy: Any, stated: Mapping[str, tuple[str, list]]) -> None:
    """Refuse where this server would not compute the record ids a version present was written with.

    A version that names or types the row identity differently from the installed policy is refused in those
    words. One that names and types it alike is then held to the end-to-end statement: its sampled receipt's
    stored record id must equal the id this server computes from that receipt's own identity. That catches what
    names and types cannot show, such as another digest, another encoding of a value, or a renamed dataset. A
    differing version alone is not refused: the record id digests the dataset and its identity pairs, so an
    equal identity definition finds its receipts under any version.
    """
    from spicy_regs.etl_receipts import subject_identity

    for version, (record_id, pairs) in stated.items():
        names = tuple(name for name, _ in pairs)
        typed = names == policy.identity_fields and all(
            value is None or type(value).__name__ == {"integer": "int", "text": "str", "timestamp": "datetime"}[
                _kind(policy.subject_schema.field(name).type)]
            for name, value in pairs)
        if not typed:
            held = ", ".join(f"{name} ({'null' if value is None else 'integer' if type(value) is int else 'timestamp' if isinstance(value, datetime) else 'text'})"
                             for name, value in pairs)
            raise ValueError(
                f"{policy.dataset} receipts were written under policy_version {version!r}, whose row identity is "
                f"{held}; this server's policy ({policy.policy_version!r}) identifies a row by "
                f"{_identity_words(policy)}. Every key would miss, so nothing is looked up until the server and "
                "the publication agree.")
        if subject_identity(policy, dict(pairs))[0] != record_id:
            raise ValueError(
                f"{policy.dataset} receipts written under policy_version {version!r} hold record ids this server "
                f"does not reproduce: a receipt's own row identity ({_identity_words(policy)}) computes to another "
                "id here than the one stored. Every key would miss, so nothing is looked up until the server and "
                "the publication agree.")


def _typed_keys(policy: Any, keys: Sequence[Any]) -> list[dict[str, Any]]:
    """Each key typed by the installed identity schema, or a refusal naming the first that does not parse.

    Text is taken exactly as given: never trimmed, folded or read as a number. An integer may be given as a number
    or as its plain decimal text. An offset timestamp is normalized to UTC without losing precision.
    A key is refused, never answered as a miss, when it is not an object of exactly
    the identity fields, gives a field another type, or gives null where the policy allows none.
    """
    import pyarrow as pa

    typed = []
    for position, key in enumerate(keys, 1):
        problem, values = None, {}
        if not isinstance(key, Mapping):
            problem = "is not an object"
        elif extra := sorted(set(key) - set(policy.identity_fields)):
            problem = f"names {', '.join(map(repr, extra))}, which is not an identity field"
        elif missing := [name for name in policy.identity_fields if name not in key]:
            problem = f"gives no {', '.join(missing)}"
        else:
            for name in policy.identity_fields:
                value, dtype = key[name], policy.subject_schema.field(name).type
                if value is None:
                    if name not in policy.nullable_identity_fields:
                        problem = f"gives null for {name}"
                elif _kind(dtype) == "text":
                    if not isinstance(value, str):
                        problem = f"gives {name} as {type(value).__name__}; it is text, so quote it"
                elif _kind(dtype) == "timestamp":
                    try:
                        if not isinstance(value, str) or not re.fullmatch(
                            r"[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}"
                            r"(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])", value,
                        ):
                            raise ValueError
                        value = datetime.fromisoformat(value).astimezone(timezone.utc)
                        pa.scalar(value, type=dtype)
                    except (ValueError, OverflowError, pa.ArrowException):
                        problem = f"gives {name} as {key[name]!r}; use an ISO timestamp with an offset and at most six fractional digits"
                else:
                    if (isinstance(value, str) and _INTEGER.match(value)) or (
                            isinstance(value, float) and value.is_integer()):
                        value = int(value)
                    try:
                        if type(value) is not int:
                            raise ValueError
                        pa.scalar(value, type=dtype)
                    except (ValueError, OverflowError, pa.ArrowException):
                        problem = f"gives {name} as {key[name]!r}; it is a whole number ({dtype})"
                if problem:
                    break
                values[name] = value
        if problem:
            raise ValueError(f"Key {position} of {len(keys)} {problem}. A key for {policy.dataset} is an object "
                             f"giving exactly {_identity_words(policy)}; text is exact and case-sensitive.")
        typed.append(values)
    return typed


def _accepted(cursor: Any, member: Member, groups: Sequence[_Group], candidates: Sequence[int],
              dataset: str, keys: Sequence[tuple[str, str]]) -> dict[str, list[str]]:
    """Each ``(record_id, identity_json)`` key's accepted receipts, as their ``processing_json``.

    ``candidates`` are the groups whose footer bounds admit the dataset and a key's identity (:func:`_key_groups`).
    One scan of those groups' row ranges, filtered on dataset, identity and record id: DuckDB reads the filter
    columns and then ``processing_json`` only where a row matches. A record id digests its dataset and identity,
    and the identity filter is what the footer and page bounds prune on. O(candidate receipts), not O(dataset).
    """
    found: dict[str, list[str]] = {}
    if not candidates or not keys:
        return found
    runs = _runs(groups, candidates)
    ids = sorted({record_id for record_id, _ in keys})
    identities = sorted({identity for _, identity in keys})
    for record_id, processing_json in cursor.execute(
        f"SELECT record_id, processing_json FROM {_ROWS} WHERE ("
        + " OR ".join("(file_row_number >= ? AND file_row_number < ?)" for _ in runs)
        + f") AND dataset = ? AND outcome = 'accepted' AND identity_json IN ({', '.join('?' * len(identities))})"
        + f" AND record_id IN ({', '.join('?' * len(ids))})",
        [member.location, *(bound for run in runs for bound in run), dataset, *identities, *ids],
    ).fetchall():
        found.setdefault(record_id, []).append(processing_json)
    return found


def _key_groups(groups: Sequence[_Group], candidates: Sequence[int], identities: Sequence[str]) -> list[int]:
    """The dataset's groups whose footer bounds admit one of ``identities``; a group stating none admits all."""
    return [i for i in candidates if any(_within(groups[i].identities, identity) for identity in identities)]


def _held_keys(cursor: Any, policy: Any, keys: Sequence[Mapping[str, Any]]) -> set[tuple]:
    """Which of ``keys`` the subject table holds, read only for keys no accepted receipt holds.

    Refused past SCAN_ROW_BOUND rows: nothing says the table is ordered by its identity, so the search may read
    every row's identity columns.
    """
    table = policy.dataset
    [(rows,)] = cursor.execute(f'SELECT count(*) FROM "{table}"').fetchall()
    if rows > SCAN_ROW_BOUND:
        raise ValueError(
            f"No accepted receipt holds {len(keys)} of the keys asked for, and telling a missing receipt from a key "
            f"{table} does not hold would search its {rows:,} rows, over the {SCAN_ROW_BOUND:,}-row bound. First "
            f"such key: {json.dumps(keys[0], ensure_ascii=False, default=str)}. Check the key's spelling (keys are exact and "
            f"case-sensitive) with query_sql on {table}, and ask again without it.")
    names = ", ".join(f'"{name}"' for name in policy.identity_fields)
    match = " AND ".join(f'"{name}" IS NOT DISTINCT FROM ?' for name in policy.identity_fields)
    return set(cursor.execute(
        f'SELECT DISTINCT {names} FROM "{table}" WHERE ' + " OR ".join(f"({match})" for _ in keys),
        [key[name] for key in keys for name in policy.identity_fields]).fetchall())


def _empty(name: str, value: Any) -> bool:
    """Whether a held value is empty: empty text, list or object, or a ``*_json`` field's text of an empty one.

    The second form is the field-state views' own reading of a held list column (``relationship_views.core``).
    """
    if isinstance(value, (str, list, tuple, Mapping)) and not value:
        return True
    if isinstance(value, str) and name.endswith("_json"):
        try:
            parsed = json.loads(value)
        except ValueError:
            return False
        return isinstance(parsed, (list, dict)) and not parsed
    return False


def _field_states(table: str, fields: Sequence[str], where: Mapping[str, str | None], processing: Mapping,
                  plain: Callable[[Any], Any]) -> dict[str, dict[str, Any]]:
    """Each requested field's state in one decoded receipt, and its value where the receipt states one.

    A read marker speaks for a NULL only where the table declares one for the field and for this row; any other
    NULL is ``null_unmarked``. A field whose mapping the receipt does not hold is refused, not answered as NULL:
    a publication written under a policy that no longer keeps the mapping does not carry the field.
    """

    def held(name: str) -> Any:
        container = where[name]
        if container is None:
            return processing.get(name)
        mapping = processing.get(container)
        if not isinstance(mapping, Mapping):
            raise LookupError(container)
        return mapping.get(name)

    markers = [marker for marker in _declared().get(table, {}).get("read_markers", ())
               if {*marker["columns"], *marker["where"]} <= set(where)]
    states = {}
    for name in fields:
        value, read, empty_unread = held(name), None, False
        for marker in markers:
            if name in marker["fields"] and all(held(column) == wanted for column, wanted in marker["where"].items()):
                read = all(held(column) is not None if marker["read_value"] is None
                           else held(column) == marker["read_value"] for column in marker["columns"])
                empty_unread = marker["empty_unread"]
                break
        if value is None:
            states[name] = {"state": NULL_UNMARKED if read is None else NOT_STATED if read else UNREAD}
        elif _empty(name, value):
            states[name] = ({"state": UNREAD} if read is False and empty_unread
                            else {"state": STATED_EMPTY, "value": plain(value)})
        else:
            states[name] = {"state": STATED, "value": plain(value)}
    return states


def read_fields(cursor: Any, *, table: str, keys: Sequence[Any], fields: Sequence[str], index: Mapping,
                local: Mapping | None, base_url: str, entry: Mapping[str, Any],
                plain: Callable[[Any], Any], snapshot: Mapping | None = None) -> dict[str, Any]:
    """The tool's reply: one entry per key in request order, under the facts every entry shares.

    ``entry`` is the table's dictionary entry and ``plain`` the server's JSON conversion. Every refusal is a
    ValueError whose text says what is valid.
    """
    if (reason := retired(table)) is not None:
        raise ValueError(f"{reason} There is no receipt to read.")
    where = carried_fields(table)
    member = selected_member(index, local, base_url, table, snapshot) if where is not None else None
    if where is None or member is None:
        served = tables_with_receipts(index, local, base_url, snapshot)
        reason = (f"{table!r} is not a table with a row identity" if where is None
                  else f"{table} has no receipts in this connection (nothing here publishes them yet)")
        raise ValueError(f"{reason}, so there is no receipt to read. Tables whose receipts this connection holds: "
                         f"{', '.join(served) or 'none'}.")
    if len(keys) > MAX_KEYS:
        raise ValueError(f"keys holds {len(keys)} keys; {TOOL} reads at most {MAX_KEYS} a call. Ask in batches.")
    fields = list(dict.fromkeys(fields))
    if unknown := [name for name in fields if name not in where]:
        raise ValueError(f"{table} receipts do not carry {', '.join(map(repr, unknown))}. They carry: "
                         f"{', '.join(where)}. describe_table gives each one's meaning with detail=true.")
    from spicy_regs.etl_receipts import _unpack, subject_identity
    from spicy_regs.subject_catalog import policies

    policy = policies()[table]
    typed = _typed_keys(policy, keys)
    groups = _footer(cursor, member)
    candidates = [i for i, group in enumerate(groups) if _within(group.datasets, table)]
    identities = [subject_identity(policy, key) for key in typed]
    key_groups = _key_groups(groups, candidates, [identity for _, _, identity in identities])
    receipts = sum(groups[i].rows for i in key_groups)
    # Decided from the footer alone, before any row is read.
    if receipts > SCAN_ROW_BOUND:
        raise ValueError(
            f"{TOOL} cannot answer these keys for {table}: the row groups whose bounds admit them hold "
            f"{receipts:,} receipts, over the {SCAN_ROW_BOUND:,}-receipt bound (this generation's receipts "
            "do not order or bound the table by identity). Ask for fewer keys, or see describe_table's notes for "
            "which of its receipt values a row's own columns give.")
    stated = _stated_identities(cursor, member, groups, candidates, table)
    _refuse_other_identity(policy, stated)
    record_ids = [record_id for record_id, _, _ in identities]
    found = _accepted(cursor, member, groups, key_groups, table,
                      list(dict.fromkeys((record_id, identity) for record_id, _, identity in identities)))
    unheld = [key for key, record_id in zip(typed, record_ids, strict=True) if record_id not in found]
    in_table = _held_keys(cursor, policy, list({tuple(key.values()): key for key in unheld}.values())) if unheld else set()
    entries, absent = [], set()
    for key, record_id in zip(typed, record_ids, strict=True):
        held = found.get(record_id, [])
        if not held:
            outcome = RECEIPT_MISSING if tuple(key[name] for name in policy.identity_fields) in in_table else NOT_IN_TABLE
            entries.append({"key": plain(key), "receipt": outcome})
        elif len(held) > 1:
            entries.append({"key": plain(key), "receipt": AMBIGUOUS, "receipts_found": len(held)})
        else:
            try:
                states = _field_states(table, fields, where, _unpack(json.loads(held[0])), plain)
            except LookupError as error:
                absent.add(error.args[0])
                continue
            entries.append({"key": plain(key), "receipt": FOUND, "fields": states})
    if absent:
        gone = [name for name in fields if where[name] in absent]
        raise ValueError(
            f"{table} receipts do not carry {', '.join(map(repr, gone))} in this generation: they hold no "
            f"{', '.join(sorted(absent))}, which this server's policy expects. They carry: "
            f"{', '.join(name for name, container in where.items() if not {name, container} & absent)}.")
    columns = {column["column_name"]: column for column in entry.get("columns", [])}
    markers = _declared().get(table, {}).get("read_markers", ())
    return {
        "table": table,
        "identity_fields": {name: columns.get(name, {}).get("column_type") or _kind(policy.subject_schema.field(name).type)
                            for name in policy.identity_fields},
        "receipts": {
            "family": member.family, "generation_id": member.generation_id,
            "artifact_digest": member.artifact_digest,
            **({"snapshot_id": member.snapshot_id, "manifest_key": member.manifest_key,
                "member_sha256": member.sha256} if member.snapshot_id is not None else {}),
            "policy_version": next(iter(stated)) if len(stated) == 1 else sorted(stated) or None,
        },
        "fields": {name: {"meaning": meaning, "kept_in": where[name],
                          **({"read_marker": marker["basis"]} if (marker := next(
                              (m for m in markers if name in m["fields"]), None)) else {})}
                   for name, meaning in _meanings(table, fields, columns).items()},
        "receipt_meaning": {word: RECEIPT_MEANINGS[word] for word in RECEIPT_MEANINGS
                            if any(item["receipt"] == word for item in entries)},
        "state_meaning": {word: STATE_MEANINGS[word] for word in STATE_MEANINGS
                          if any(state["state"] == word for item in entries
                                 for state in item.get("fields", {}).values())},
        "keys": entries,
    }


def refuse_oversized(reply: dict[str, Any], limit: int, size: Callable[[Any], int]) -> None:
    """Refuse a reply past ``limit`` characters, saying how many of its leading keys fit.

    ``size`` measures a value as the client is sent it. Entries are independent, so the count comes from one
    measurement each; nothing partial is returned for size.
    """
    total = size(reply)
    if total <= limit:
        return
    entries = reply["keys"]
    room, fit = limit - size({**reply, "keys": []}) + 1, 0  # the first entry has no comma
    for item in entries:
        room -= size(item) + 1
        if room < 0:
            break
        fit += 1
    over = f"This reply would be {total:,} characters, over the {limit:,}-character reply limit; nothing is returned."
    widths = {name: sum(size(item["fields"][name]) for item in entries if "fields" in item) for name in reply["fields"]}
    widest = max(widths, key=widths.__getitem__)
    fewer = f"{widest} holds {round(100 * widths[widest] / max(sum(widths.values()), 1))}% of the values' characters"
    if not fit:
        raise ValueError(f"{over} Not even the first key fits: ask for fewer fields ({fewer}).")
    raise ValueError(f"{over} The first {fit} of its {len(entries)} keys fit: ask for those, then the rest, or for "
                     f"fewer fields ({fewer}).")
