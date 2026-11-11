"""Source-occurrence and distinct navigation views, using only held tables.

Install before applying the serving connection's SQL restrictions. Installation
binds schemas but does not read source rows or look up targets. This module has
no pipeline or spicy_docs import dependency.
"""

from typing import Any, Iterable, Mapping
from dataclasses import replace

from .affiliations import AFFILIATION_VIEWS, NATIVE_AFFILIATION_VIEWS
from .agenda import AGENDA_VIEWS
from .artifacts_topics import ARTIFACT_SQL_VIEWS, ARTIFACT_TOPIC_RELATIONSHIPS, NATIVE_BILL_SUBJECTS
from .courts import COURT_VIEWS
from .bill_publication import BILL_PUBLICATION_VIEWS
from .legislative_diffs import NATIVE_DIFF_VIEWS as DIFF_VIEWS
from .diffs import DIFF_VIEWS as LEGACY_DIFF_VIEWS
from .entities import ENTITY_VIEWS
from .fec import FEC_VIEWS
from .fec_native_document_query import FEC_NATIVE_DOCUMENT_QUERY_VIEWS
from .fec_document_query import FEC_DOCUMENT_QUERY_VIEWS
from .fcc_native import FCC_NATIVE_VIEWS
from .lobbying_native import LOBBYING_NATIVE_VIEWS
from .regulations_native import ARRAYS as NATIVE_REGULATORY_ARRAYS
from .lifecycle_dates import LIFECYCLE_DATE_VIEWS
from .identity_candidates import IDENTITY_VIEWS, NATIVE_IDENTITY_VIEWS
from .sql_views import (annotate_views, compatible_sql_variant, install_sql_variants,
                        install_sql_views, view_columns)
from .comments import install_comment_references
from .congress import CONGRESS_RELATIONSHIPS, NATIVE_CONGRESS_RELATIONSHIPS, NATIVE_COMMUNICATION_RINS
from .core import ArrayRelationship, install_arrays
from .regulatory import REGULATORY_RELATIONSHIPS
from .reference import REFERENCE_VIEWS
from .uscode import USCODE_VIEWS

RELATIONSHIP_VIEWS = (*CONGRESS_RELATIONSHIPS, *REGULATORY_RELATIONSHIPS, *ARTIFACT_TOPIC_RELATIONSHIPS)
FEC_QUERY_VIEWS = tuple(
    replace(spec, role="child_query", category="query_data") for spec in FEC_NATIVE_DOCUMENT_QUERY_VIEWS
)
SQL_RELATIONSHIP_VIEWS = (
    *BILL_PUBLICATION_VIEWS,
    *ARTIFACT_SQL_VIEWS,
    *ENTITY_VIEWS,
    *COURT_VIEWS,
    *DIFF_VIEWS,
    *FEC_VIEWS,
    *AGENDA_VIEWS,
    *IDENTITY_VIEWS,
    *AFFILIATION_VIEWS,
    *FCC_NATIVE_VIEWS,
    *LOBBYING_NATIVE_VIEWS,
    *LIFECYCLE_DATE_VIEWS,
    *USCODE_VIEWS,
    *REFERENCE_VIEWS,
)


def describe_relationship_views(
    connection: Any, available_tables: Iterable[str], publication: Mapping[str, object] | None = None,
) -> dict[str, dict[str, Any]]:
    """Document maintained definitions against selected inputs without binding SQL.

    Empty input sets reuse the installers' declaration paths: they perform no
    schema reads or view creation. Variant selection and output schemas remain
    pending until the separate query connection admits the actual inputs.
    """
    from .regulations_native import native_regulations_views

    available = set(available_tables)
    arrays = (*RELATIONSHIP_VIEWS, *NATIVE_CONGRESS_RELATIONSHIPS, NATIVE_COMMUNICATION_RINS, NATIVE_BILL_SUBJECTS)
    results = install_arrays(connection, (), RELATIONSHIP_VIEWS, publication)
    results.update(install_comment_references(connection, (), publication))
    specs = (*SQL_RELATIONSHIP_VIEWS, *LEGACY_DIFF_VIEWS, *FEC_QUERY_VIEWS,
             *FEC_DOCUMENT_QUERY_VIEWS, *NATIVE_IDENTITY_VIEWS, *NATIVE_AFFILIATION_VIEWS,
             *native_regulations_views(available))
    # Alternative definitions are documentation, never a schema verdict.
    versions: dict[str, list[str]] = {}
    dependencies: dict[str, set[str]] = {}
    for spec in arrays:
        documented = install_arrays(connection, (), [spec], publication)
        for name, entry in documented.items():
            versions.setdefault(name, [])
            dependencies.setdefault(name, set()).add(spec.source_table)
            if spec.rule_version not in versions[name]:
                versions[name].append(spec.rule_version)
            results.setdefault(name, entry)
    for spec in specs:
        versions.setdefault(spec.name, [])
        dependencies.setdefault(spec.name, set()).update(spec.required)
        if spec.rule_version not in versions[spec.name]:
            versions[spec.name].append(spec.rule_version)
        if spec.name not in results:
            results.update(install_sql_views(connection, (), [spec], publication, bind=False))
    reachable = set(available)
    while True:
        added = {name for name, entry in results.items() if set(entry['dependencies']) <= reachable} - reachable
        if not added:
            break
        reachable.update(added)
    for name, entry in results.items():
        metadata = entry['metadata']
        metadata['definition_basis'] = 'Maintained declarations with this connection\'s selected input pins; no runtime schema verdict.'
        if name in versions:
            metadata['declared_rule_versions'] = versions[name]
            metadata['declared_dependencies'] = sorted(dependencies[name])
        if name in reachable:
            entry.update(status='deferred', reason='Relationship schema and query admission are deferred until a query connection is requested.')
            metadata['query_admission'] = 'pending_relationship_binding'
    annotate_views(results)
    for entry in results.values():
        entry['metadata']['availability_basis'] = 'Declared dependencies only; runtime schema binding and query admission remain pending.'
    return results


def install_relationship_views(
    connection: Any,
    available_tables: Iterable[str],
    publication: Mapping[str, object] | None = None,
    *, selected_names: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return per-view availability, meaning, source identity and publication pins."""
    available = set(available_tables)
    from spicy_regs.subject_catalog import descriptors
    from importlib.resources import files
    import json
    declared = descriptors()
    metadata = json.loads(files("spicy_regs").joinpath("table_metadata.json").read_text())

    def native_columns(table):
        # A generation written under an earlier admitted policy (columns short of today's) is still native; views
        # that need a newer column report it missing themselves (install_sql_views) instead of the whole table
        # falling back to the legacy receipt-column views (ss-cdaa).
        entry = metadata[table]
        return {column["column_name"] for column in entry["columns"]} - set(entry.get("later_policy_columns", ()))

    native = {table for table in available if table in declared and not declared[table]["receipt_only"]
              and native_columns(table) <= {row[0] for row in connection.execute(f'DESCRIBE "{table}"').fetchall()}}
    def requested(spec):
        return selected_names is None or spec.name in selected_names

    lobbying_variants = tuple(spec for spec in LOBBYING_NATIVE_VIEWS if requested(spec)
                              if compatible_sql_variant(connection, available, [spec], publication)[0] is not None)
    lobbying_replacements = {spec.name for spec in lobbying_variants}
    congress = tuple(spec for spec in CONGRESS_RELATIONSHIPS if spec.source_table not in native)
    congress += tuple(spec for spec in NATIVE_CONGRESS_RELATIONSHIPS if spec.source_table in native)
    native_arrays = tuple(spec for spec in (NATIVE_COMMUNICATION_RINS, NATIVE_BILL_SUBJECTS)
                          if spec.source_table in native)
    replacements = {spec.name for spec in native_arrays}
    arrays = tuple(spec for spec in (*congress, *REGULATORY_RELATIONSHIPS, *ARTIFACT_TOPIC_RELATIONSHIPS)
                   if spec.name not in replacements
                   and not any(name in lobbying_replacements for name in spec.names)
                   and not (spec.source_table in native and spec.source_table == "fcc_filings"))
    arrays += native_arrays
    if selected_names is not None:
        arrays = tuple(spec for spec in arrays if set(spec.names) & selected_names)
    results = install_arrays(connection, available, arrays, publication)
    from .comments import NAMES as COMMENT_NAMES
    if selected_names is None or set(COMMENT_NAMES) & selected_names:
        results.update(install_comment_references(connection, available, publication))
    sql = tuple(spec for spec in SQL_RELATIONSHIP_VIEWS
                if spec not in (*DIFF_VIEWS, *LOBBYING_NATIVE_VIEWS)) + lobbying_variants
    native_sql = tuple(spec for spec in (*NATIVE_IDENTITY_VIEWS, *NATIVE_AFFILIATION_VIEWS)
                       if set(spec.required) <= native)
    replaced = {spec.name for spec in native_sql}
    sql = tuple(spec for spec in sql if spec.name not in replaced) + native_sql
    sql += tuple(spec for spec in DIFF_VIEWS if set(spec.required) <= native)
    sql += tuple(spec for spec in LEGACY_DIFF_VIEWS if not set(spec.required) <= native)
    results.update(install_sql_views(connection, available, filter(requested, sql), publication))
    legal_names = {"fec_legal_citations", "fec_legal_subjects"}
    results.update(install_sql_views(connection, available,
                                   [spec for spec in FEC_QUERY_VIEWS if spec.name not in legal_names and requested(spec)], publication))
    legal_variants = [(next(s for s in FEC_QUERY_VIEWS if s.name == name),
                       replace(next(s for s in FEC_DOCUMENT_QUERY_VIEWS if s.name == name),
                               role="child_query", category="query_data"))
                      for name in sorted(legal_names) if selected_names is None or name in selected_names]
    results.update(install_sql_variants(connection, available, legal_variants, publication))
    # Legacy array installation already reports incompatible columns. If no
    # lobbying variant binds, keep that explicit disposition rather than hiding it.
    if not lobbying_variants:
        for variants in ([spec] for spec in LOBBYING_NATIVE_VIEWS):
            name = variants[0].name
            if requested(variants[0]) and results.get(name, {}).get("status") != "available":
                results.update(install_sql_variants(connection, available, [variants], publication))
    # Field-read states and lifecycle processing evidence live in shared receipts.
    removed = {base + "_field_states" for base, table, *_ in NATIVE_REGULATORY_ARRAYS if table in native}
    if "comments" in native:
        removed.add("comment_reference_field_states")
    if "rulemaking_lifecycles" in native:
        removed.add("lifecycle_date_evidence")
    for name in removed:
        results.pop(name, None)
    from .regulations_native import native_regulations_views
    results.update(install_sql_views(connection, available, filter(requested, native_regulations_views(native)), publication))
    annotate_views(results)
    return results


__all__ = ["ArrayRelationship", "RELATIONSHIP_VIEWS", "install_relationship_views", "view_columns"]
