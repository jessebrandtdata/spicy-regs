"""What a table's ETL receipts hold inside a mapping, what each receipt-only field means, and which fields a
builder's read marker governs: the declaration ``read_receipt_fields`` reads, bundled as ``receipt_fields.json``.

Every entry is derived from a family's own field registry, a builder's own read rule or the dictionary's prose.
Those import pyarrow and spicy-docs, which the MCP server's image does not install, so ``spicy-regs-dict generate``
writes the bundle and ``check`` refuses a stale one. A table's declared receipt fields are not listed: the server
reads them from the installed policy at run time, and offers a field held in a mapping only while that policy still
declares the mapping, so a policy that stops keeping one takes its fields off offer without a rebuild of this file.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

RECORD = Path(__file__).with_name("receipt_fields.json")


def _government(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS

    return {"raw_record": tuple(LEGACY_COLUMNS[table])}


def _congress(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.congress_subjects import INPUT_COLUMNS

    return {"source_fields": tuple(INPUT_COLUMNS[table])}


def _legislative(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.legislative_documents import field_registry

    return {"raw_source_row": tuple(field["name"] for field in field_registry()[table]["fields"])}


def _regulations(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS

    return {"raw_conversion_inputs": tuple(name for name, _ in SOURCE_COLUMNS[table])}


def _fec_identity(table: str) -> dict[str, tuple[str, ...]]:
    # normalize_record keeps the original of each converted scalar and nothing else. build_fec_committees' whole
    # row in the same mapping is that writer's own exception, declared nowhere, so it is not offered.
    from spicy_regs.transforms.fec_identity_context_fields import REGISTRY

    return {"conversion_inputs": tuple(REGISTRY[table]["native_scalars"])}


def _fec_subject(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.transforms.fec_native_subjects import FIELD_RULES

    return {"fec_conversion_inputs": (*FIELD_RULES[table]["receipt"], *FIELD_RULES[table]["mapped"])}


def _scorecards(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.scorecards.subject_shapes import BOOLEAN_FIELDS, DECIMAL_FIELDS, DOMAIN_COLUMNS, INTEGER_FIELDS

    converted = BOOLEAN_FIELDS | DECIMAL_FIELDS | INTEGER_FIELDS
    return {"conversion_inputs": tuple(name for name in DOMAIN_COLUMNS[table] if name in converted)}


def _administration_policy(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.sources.retained_collections import POLICIES

    POLICIES[table]  # Refuse unknown datasets even under a known version prefix.
    return {}  # The producer declares no receipt-only mappings.


def _retained_native(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.sources.retained_native_collections import POLICIES

    POLICIES[table]
    return {}  # Every retained field is already a subject column.


def _historical_regulations(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.transforms.historical_regulations import POLICIES

    POLICIES[table]
    # selection_metadata is offered as a whole mapping by the policy. The
    # producer has no maintained nested-field registry; do not invent one here.
    return {}


def _uscode_editions(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.sources.uscode_editions import POLICIES

    POLICIES[table]
    return {}


def _table3_history(table: str) -> dict[str, tuple[str, ...]]:
    from spicy_regs.sources.table3_history import IDENTITY_FIELDS, POLICY, POSITION_FIELDS

    if table != POLICY.dataset:
        raise KeyError(table)
    return {"source_position": tuple(dict.fromkeys((
        *IDENTITY_FIELDS, "member_name", *POSITION_FIELDS, "record_native_id",
    )))}


#: Each policy family, by the prefix its policies' versions share, with the registry that names the fields its
#: receipts hold inside a mapping. Courts keep conversion inputs too, under keys their mapper builds and no registry
#: declares, so only their declared receipt fields are offered.
FAMILIES: tuple[tuple[str, str, Callable[[str], dict[str, tuple[str, ...]]]], ...] = (
    ("government-sources/", "government sources", _government),
    ("congress-subjects/", "congress", _congress),
    ("legislative-documents/", "legislative documents", _legislative),
    ("regulations-native-", "regulations", _regulations),
    ("fec-identity-context-receipts/", "FEC identity", _fec_identity),
    ("fec-subject-receipts/", "FEC subject", _fec_subject),
    ("scorecards-etl-", "scorecards", _scorecards),
    ("courts/", "courts", lambda table: {}),
    ("navigation-read-outcomes/", "source read outcomes", lambda table: {}),
    ("administration-policy-", "administration policy", _administration_policy),
    ("retained-native-collections-", "retained native collections", _retained_native),
    ("historical-regulations-", "historical regulations", _historical_regulations),
    ("uscode-editions-", "Code editions", _uscode_editions),
    ("table3-historical", "historical Table III", _table3_history),
    ("crs-report-files/", "CRS report files", lambda table: {}),
)


def container_fields(policy) -> dict[str, tuple[str, ...]]:
    """The fields ``policy``'s family registry says its receipts hold inside a mapping, by that mapping's name.

    A mapping the policy does not declare as a receipt field is left out: the registry names what a mapper
    receives, and only the policy says what a receipt keeps of it. An unknown family raises.
    """
    for prefix, _, registry in FAMILIES:
        if policy.policy_version.startswith(prefix):
            return {container: names for container, names in registry(policy.dataset).items()
                    if container in policy.receipt_fields and names}
    raise ValueError(f"{policy.dataset}: no receipt field registry for policy_version {policy.policy_version!r}")


@dataclass(frozen=True)
class ReadMarker:
    """A builder's own record of whether a row's fields were read: ``columns`` say so, for ``fields``.

    The row was read when every column holds ``read_value`` (``None``: any value but NULL). ``where`` names the
    rows the marker speaks for, by a field's value; it speaks for every row when empty. ``empty_unread`` says an
    empty list on a row not read is unread too, as ``relationship_views.core.DETAIL_STATES`` defines it.
    """

    table: str
    columns: tuple[str, ...]
    fields: tuple[str, ...]
    basis: str
    read_value: str | None = None
    where: tuple[tuple[str, str], ...] = ()
    empty_unread: bool = False


def read_markers() -> tuple[ReadMarker, ...]:
    """Every read marker a builder or view already acts on, from its own definition.

    ``detail_read`` governs the list a field-state view reads with it and the lists a Congress.gov index shaper
    leaves NULL for a read detail that states none (``build_congress_index._stale_sql``'s derivation). A roll
    call's read columns are stated by every captured file of its chamber, so a NULL in any says the row predates
    their read: they govern themselves, and nothing declares which other columns the same file backs. The
    committee-report and citation read records (``committee_report_reads``, ``document_citation_reads``) are rows
    of another dataset's receipts, and are not wired here.
    """
    from spicy_regs.relationship_views.congress import CONGRESS_RELATIONSHIPS
    from spicy_regs.relationship_views.regulatory import REGULATORY_RELATIONSHIPS
    from spicy_regs.transforms.build_congress_index import DETAIL_READ, INDEX_SPECS
    from spicy_regs.transforms.build_roll_call_votes import _READ_COLUMNS

    detail: dict[str, list[str]] = {}
    for view in (*CONGRESS_RELATIONSHIPS, *REGULATORY_RELATIONSHIPS):
        if view.detail_read_column is not None:
            if view.detail_read_column != DETAIL_READ:
                raise ValueError(f"{view.name}: its read column is not the index builder's {DETAIL_READ}")
            detail.setdefault(view.source_table, []).append(view.source_field)
    for table, spec in INDEX_SPECS.items():
        if spec.detail_route is not None:
            unstated = spec.shape({}, {})
            detail.setdefault(table, []).extend(
                column for column, value in unstated.items() if value is None and column.endswith("_json"))
    markers = [
        ReadMarker(table, (DETAIL_READ,), tuple(dict.fromkeys(fields)), read_value="true", empty_unread=True,
                   basis=f"{DETAIL_READ} is 'true' on a row whose Congress.gov detail record was read.")
        for table, fields in detail.items()
    ]
    markers.extend(
        ReadMarker("roll_call_votes", columns, columns, where=(("chamber", chamber),),
                   basis=f"Every captured {chamber} vote file states these columns ('' at least), so a NULL in "
                         "any says the row was published before they were read. It speaks for these columns "
                         "only, not for the row's other file-backed fields.")
        for chamber, columns in _READ_COLUMNS.items())
    return tuple(markers)


def _prose() -> dict[str, dict[str, str]]:
    """The dictionary's sentence per source field: descriptions.yaml or the spicy-docs contract, then a legislative
    document field's registered meaning. Read from the file because ``load_descriptions`` replaces each table's
    columns with its subject columns, dropping the fields kept only in receipts."""
    import yaml

    from spicy_regs import data_dictionary as dd
    from spicy_regs.legislative_documents import field_registry

    tables = yaml.safe_load(dd.DEFAULT_DESCRIPTIONS.read_text(encoding="utf-8"))["tables"]
    prose = {
        table: dd.contract_column_prose(table) if entry.get("columns_from") == dd.COLUMNS_FROM_SPICY_DOCS
        else dict(entry.get("columns") or {})
        for table, entry in tables.items()
    }
    for table, spec in field_registry().items():
        for field in spec["fields"]:
            prose.setdefault(table, {}).setdefault(field["name"], field["meaning"])
    return prose


def renamed_fields(policy, carried: set[str]) -> dict[str, str]:
    """Each carried field whose value the table holds under another column name, with that column.

    A list a mapper received serialized as ``<column>_json`` is published as ``<column>``, and FCC filings' lists
    have legacy names of their own (``FCC_LEGACY_ARRAYS``, the government mapper's map). The receipt still holds
    the field as received; the table has its value, so ``describe_table`` does not offer it as something the
    table lacks. Other families' mappers rename through code this does not read: their renamed fields stay on
    offer until they are declared here.
    """
    from spicy_regs.transforms.government_source_shapes import FCC_LEGACY_ARRAYS, LEGACY_COLUMNS

    subject = set(policy.subject_schema.names)
    renamed = {name: name.removesuffix("_json") for name in carried - subject if name.endswith("_json")}
    if policy.dataset in LEGACY_COLUMNS:
        renamed |= FCC_LEGACY_ARRAYS
    return {name: column for name, column in sorted(renamed.items()) if name in carried - subject and column in subject}


def record(policies: Mapping | None = None) -> dict:
    """The bundled declaration for every table with a row identity, from the installed policies.

    Per table: ``containers`` (mapping name to the fields it holds), ``renamed`` (``renamed_fields``), ``meanings``
    for the fields no subject column carries (a column's meaning is the dictionary's, which the server already
    bundles), and ``read_markers``. A marker naming a field the table's receipts do not carry raises: it would
    govern nothing and say so nowhere.
    """
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.subject_catalog import retired_receipt_datasets

    policies = installed_policies() if policies is None else policies
    # A table whose receipts are retired has none to read: its fields are columns.
    policies = {name: policy for name, policy in policies.items() if name not in retired_receipt_datasets()}
    prose = _prose()
    markers: dict[str, list[ReadMarker]] = {}
    for marker in read_markers():
        markers.setdefault(marker.table, []).append(marker)
    tables = {}
    for table, policy in sorted(policies.items()):
        if policy.receipt_only or not policy.identity_fields:
            continue
        containers = container_fields(policy)
        carried = {*policy.receipt_fields, *(name for names in containers.values() for name in names)}
        entry: dict = {}
        if containers:
            entry["containers"] = {container: list(names) for container, names in containers.items()}
        if renamed := renamed_fields(policy, carried):
            entry["renamed"] = renamed
        meanings = {name: " ".join(text.split()) for name in sorted(carried - set(policy.subject_schema.names))
                    if (text := prose.get(table, {}).get(name))}
        if meanings:
            entry["meanings"] = meanings
        for marker in markers.pop(table, ()):
            named = {*marker.columns, *marker.fields, *(name for name, _ in marker.where)}
            if named - carried:
                raise ValueError(f"{table}: a read marker names fields its receipts do not carry: "
                                 f"{sorted(named - carried)}")
            entry.setdefault("read_markers", []).append({
                "columns": list(marker.columns), "read_value": marker.read_value, "fields": list(marker.fields),
                "where": dict(marker.where), "empty_unread": marker.empty_unread, "basis": marker.basis,
            })
        tables[table] = entry
    if markers:
        raise ValueError(f"Read markers name tables with no receipts to read: {sorted(markers)}")
    return {
        "basis": "Derived by spicy_regs.receipt_field_declarations from the installed policies, each family's field "
                 "registry, the builders' read rules and the dictionary; regenerate with spicy-regs-dict generate.",
        "tables": tables,
    }


def record_bytes() -> bytes:
    """The declaration as ``receipt_fields.json`` holds it."""
    return json.dumps(record(), indent=1, ensure_ascii=False).encode("utf-8") + b"\n"


def record_errors() -> list[str]:
    """Refuse a bundled declaration that differs from a fresh build: the server would offer fields, meanings or
    read markers the registries no longer state."""
    if not RECORD.is_file() or RECORD.read_bytes() != record_bytes():
        return [f"{RECORD.name} is stale relative to the installed policies and field registries; "
                "run 'uv run spicy-regs-dict generate'"]
    return []
