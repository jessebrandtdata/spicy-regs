"""Explicit offline build, query, validation, gap report and export operations."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from .exporter import export
from .importer import build
from .storage import connect
from .coverage import coverage
from .refresh import compare, write_report


def _identity_scope(fact: dict) -> str | None:
    """Locate the primary record identified by a scalar or composite identity."""
    locator = fact.get("locator")
    if not isinstance(locator, str) or (locator and not locator.startswith("/")):
        return None
    fields = fact.get("source_fields")
    if fields:
        if not isinstance(fields, list) or not all(isinstance(field, str) for field in fields):
            return None
        if all(field.startswith(locator + "/") for field in fields):
            # Native bill XML keeps the identity in <form> and the version on
            # its containing <bill>/<resolution>, as recorded by raw_content.
            if fact.get("namespace") == "congress-bill" and locator in {"/bill/form", "/resolution/form"}:
                return locator.rsplit("/", 1)[0]
            return locator
    return locator.rsplit("/", 1)[0] if locator else None


def _identity_version_matches(data: str, native_id: str, source_version: str) -> bool:
    """Require both filters to describe one receipt identity or native record.

    Mapping scalars can come from different origins after content enrichment.
    Content locators and namespaces establish their association; mere presence
    in the same retained response does not. This predicate runs before paging.
    """
    mapping = json.loads(data).get("mapping", {})
    evidence = [
        fact
        for fact in mapping.get("evidence", [])
        if isinstance(fact, dict) and fact.get("target") in {"native_id", "source_version"}
    ]
    if mapping.get("native_id") == native_id and mapping.get("source_version") == source_version:
        # Preserve legacy scalar-only catalogs and pairs normalized from one
        # file's receipt. Never pair a receipt scalar with a content scalar.
        if not evidence or all(
            any(
                fact.get("origin") == "receipt"
                and fact.get("valid") is not False
                and fact.get("target") == field
                and (
                    fact.get("value") == value
                    or isinstance(fact.get("value"), int)
                    and not isinstance(fact["value"], bool)
                    and str(fact["value"]) == value
                )
                for fact in evidence
            )
            for field, value in (("native_id", native_id), ("source_version", source_version))
        ):
            return True
    content = [fact for fact in evidence if fact.get("origin") == "content" and fact.get("valid") is not False]
    identities = [
        (fact, _identity_scope(fact))
        for fact in content
        if fact.get("target") == "native_id"
        and fact.get("rule") == "primary-native-identity"
        and isinstance(fact.get("namespace"), str)
        and fact["namespace"]
    ]
    for version in content:
        locator = version.get("locator")
        if (
            version.get("target") != "source_version"
            or version.get("value") != source_version
            or version.get("rule") != "native-document-version"
            or not isinstance(locator, str)
            or not locator.startswith("/")
        ):
            continue
        scope = locator.rsplit("/", 1)[0]
        candidates = {
            (fact["namespace"], fact.get("value"))
            for fact, identity_scope in identities
            if identity_scope == scope
            and (version.get("namespace") is None or fact["namespace"] == version["namespace"])
        }
        # An untagged version cannot choose between package/capture and record
        # identities in the same scope. Missing locators remain unqualified.
        if len(candidates) == 1 and next(iter(candidates))[1] == native_id:
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("build")
    create.add_argument("--corpus", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--config", type=Path)
    create.add_argument(
        "--temp-store",
        choices=("file", "memory"),
        default="file",
        help="SQLite temporary storage; memory trades RAM for less temporary disk use",
    )
    create.add_argument("--catalog-input", type=Path, help="Pinned catalog inventory overlay; corpus stays unchanged")
    create.add_argument(
        "--table-inputs-input", type=Path, help="Pinned table dependency overlay; corpus stays unchanged"
    )
    create.add_argument("--discovery", type=Path, action="append", default=[])
    create.add_argument("--publication-observation", type=Path, action="append", default=[])
    create.add_argument("--publication-reconciliation", type=Path)
    maintenance = commands.add_parser("refresh-metadata", help="Prepare metadata updates over unchanged raw holdings")
    maintenance.add_argument("--index", type=Path, required=True)
    maintenance.add_argument("--output", type=Path, required=True)
    maintenance.add_argument("--config", type=Path)
    maintenance.add_argument("--publication-reconciliation", type=Path)
    maintenance.add_argument("--expected-generation", required=True)
    maintenance.add_argument('--include-acquisitions', action='store_true',
                             help='Import committed receipt additions over the unchanged base snapshot')
    maintenance.add_argument('--refresh-content', action='append', default=[],
                             help='Refresh a named collection when its content reader changes')
    maintenance.add_argument('--refresh-object-list', type=Path,
                             help='Replay only listed canonical SHA-256 identities in the named collections')
    maintenance.add_argument('--reuse-unchanged', action='store_true',
                             help='Return the accepted generation without creating a candidate when inputs match')
    maintenance.add_argument('--receipt-snapshot', type=Path)
    maintenance.add_argument('--receipt-snapshot-sha256')
    freeze = commands.add_parser('freeze-receipts', help='Authenticate a base and freeze its receipt keys and custody')
    for flag in ('index', 'output'):
        freeze.add_argument('--' + flag, type=Path, required=True)
    freeze.add_argument('--expected-generation', required=True)
    freeze.add_argument('--expected-index-sha256')
    reconcile = commands.add_parser("reconcile-publication")
    reconcile.add_argument("--index", type=Path, required=True)
    reconcile.add_argument("--output", type=Path, required=True)
    reconcile.add_argument("--selection", type=Path, required=True)
    reconcile.add_argument("--expected-generation")
    reconcile.add_argument("--require-verified", action="store_true")
    parity = commands.add_parser("check-publication")
    parity.add_argument("--index", type=Path, required=True)
    parity.add_argument("--selection", type=Path, required=True)
    parity.add_argument('--target-only', action='store_true', help='Require exact selected descriptors while preserving provenance uncertainty')
    publish = commands.add_parser("export")
    publish.add_argument("--index", type=Path, required=True)
    publish.add_argument("--output", type=Path, required=True)
    publish.add_argument("--internal", action="store_true")
    publish.add_argument("--page-size", type=int, default=5000)
    publish.add_argument("--completion-cache", type=Path, help="Own reusable public export completion in this selection cache")
    select = commands.add_parser("select", help="Select a prepared local catalog and public export")
    for flag in ("candidate", "reconciliation", "staged-public", "cache", "public", "raw-root"):
        select.add_argument("--" + flag, type=Path, required=True)
    select.add_argument("--expected-generation", required=True)
    select.add_argument("--allow-inventory-change", action="store_true",
                        help="Accept reviewed raw inventory changes pinned by the prepared candidate")
    select.add_argument('--receipt-snapshot', type=Path)
    select.add_argument('--receipt-snapshot-sha256')
    refresh = commands.add_parser("compare-inventory")
    refresh.add_argument("--previous", type=Path, required=True)
    refresh.add_argument("--current", type=Path, required=True)
    refresh.add_argument("--output", type=Path, required=True)
    refresh.add_argument("--max-age-days", type=int, required=True)
    refresh.add_argument("--owner", required=True)
    refresh.add_argument("--previous-completion", type=Path)
    refresh.add_argument("--current-completion", type=Path)
    for name in ["query", "validate", "gap-report", "matrix", "coverage", "holdings"]:
        command = commands.add_parser(name)
        command.add_argument("--index", type=Path, required=True)
        command.add_argument("--collection")
        command.add_argument("--scope", choices=("active", "retained", "all"), default="active",
                             help="Current usable sources, saved evidence, or the complete custody inventory")
        if name == "query":
            command.add_argument("--limit", type=int, default=100)
            command.add_argument("--offset", type=int, default=0)
            command.add_argument("--sha256")
            command.add_argument("--name", help="Literal filename substring, including saved files with --scope retained")
            command.add_argument("--native-id")
            command.add_argument("--source-version")
            command.add_argument("--period", help="Exact retained native/requested period label")
    args = parser.parse_args()
    if args.command == "build":
        result = build(
            args.corpus,
            args.output,
            config=args.config,
            discovery=args.discovery,
            publication_observations=args.publication_observation,
            catalog_input=args.catalog_input,
            table_inputs_input=args.table_inputs_input,
            temp_store=args.temp_store,
            publication_reconciliation=args.publication_reconciliation,
        )
    elif args.command == "refresh-metadata":
        from .maintenance import refresh_metadata

        result = refresh_metadata(args.index, args.output, config=args.config,
                                  publication_reconciliation=args.publication_reconciliation,
                                  expected_generation=args.expected_generation,
                                  include_acquisitions=args.include_acquisitions,
                                  refresh_content=tuple(args.refresh_content), reuse_unchanged=args.reuse_unchanged,
                                  refresh_object_list=args.refresh_object_list,
                                  receipt_snapshot=args.receipt_snapshot,
                                  receipt_snapshot_sha256=args.receipt_snapshot_sha256)
    elif args.command == 'freeze-receipts':
        from .maintenance import freeze_receipts

        result = freeze_receipts(args.index, args.output, expected_generation=args.expected_generation,
                                 expected_index_sha256=args.expected_index_sha256)
    elif args.command == "reconcile-publication":
        from .publication import reconcile

        result = reconcile(args.index, args.output, args.selection, expected_generation=args.expected_generation,
                           require_verified=args.require_verified)
    elif args.command == "check-publication":
        from .publication import check_publication

        result = check_publication(args.index, args.selection)
        print(json.dumps(result, indent=2))
        if not (result['selectionMatches'] and result['targetMatches'] if args.target_only else result['verified']):
            raise SystemExit(1)
        return
    elif args.command == "export":
        members = None
        if args.completion_cache:
            if args.internal:
                parser.error('--completion-cache requires a public export')
            from .export_completion import export_completed

            manifest, members = export_completed(args.index, args.output, args.completion_cache, page_size=args.page_size)
        else:
            manifest = export(args.index, args.output, internal=args.internal, page_size=args.page_size)
        result = {
            "generation": manifest["generation"],
            "totals": manifest["totals"],
            "manifest": str(args.output / "manifest.json"),
        }
        if members is not None:
            result["files"] = members
    elif args.command == "select":
        from .selection import select

        result = select(args.candidate, args.reconciliation, args.staged_public, args.cache,
                        args.public, args.raw_root, expected_generation=args.expected_generation,
                        allow_inventory_change=args.allow_inventory_change,
                        receipt_snapshot=args.receipt_snapshot,
                        receipt_snapshot_sha256=args.receipt_snapshot_sha256)
    elif args.command == "compare-inventory":
        result = compare(
            args.previous,
            args.current,
            now=datetime.now(UTC),
            max_age_days=args.max_age_days,
            owner=args.owner,
            previous_completion=args.previous_completion,
            current_completion=args.current_completion,
        )
        write_report(args.output, result)
    else:
        db = connect(args.index, readonly=True, scope=args.scope)
        try:
            params = (args.collection,) if args.collection else ()
            where = " WHERE collection_id=?" if args.collection else ""
            if args.command == "coverage":
                result = coverage(db, args.collection)
            elif args.command == "validate":
                result = {
                    "integrity": db.execute("PRAGMA integrity_check").fetchone()[0],
                    "foreignKeys": [list(r) for r in db.execute("PRAGMA foreign_key_check")],
                    "generation": json.loads(
                        db.execute("SELECT value FROM metadata WHERE key='generation'").fetchone()[0]
                    ),
                }
            elif args.command == "query":
                if not 1 <= args.limit <= 5000 or args.offset < 0:
                    parser.error("limit must be 1..5000 and offset must be nonnegative")
                clauses = ["collection_id=?"] if args.collection else []
                query_params = list(params)
                if args.sha256:
                    clauses.append("sha256=?")
                    query_params.append("sha256:" + args.sha256.removeprefix("sha256:"))
                if args.name is not None:
                    clauses.append("name LIKE ? ESCAPE '\\'")
                    query_params.append("%" + args.name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
                for field, value in [("native_id", args.native_id), ("source_version", args.source_version)]:
                    if value is not None:
                        # Content can identify several native records inside one
                        # retained response; those values stay in field evidence.
                        clauses.append(
                            f"(json_extract(data,'$.mapping.{field}')=? OR EXISTS("
                            "SELECT 1 FROM json_each(files.data,'$.mapping.evidence') e "
                            "WHERE json_extract(e.value,'$.origin')='content' "
                            "AND json_extract(e.value,'$.target')=? AND json_extract(e.value,'$.value')=?))"
                        )
                        query_params.extend((value, field, value))
                if args.native_id is not None and args.source_version is not None:
                    db.create_function("identity_version_matches", 3, _identity_version_matches, deterministic=True)
                    clauses.append("identity_version_matches(data,?,?)")
                    query_params.extend((args.native_id, args.source_version))
                if args.period is not None:
                    clauses.append("EXISTS(SELECT 1 FROM raw_periods p WHERE p.file_id=files.id AND p.label=?)")
                    query_params.append(args.period)
                where = " WHERE " + " AND ".join(clauses) if clauses else ""
                result = [
                    dict(r)
                    for r in db.execute(
                        "SELECT * FROM files" + where + " ORDER BY id LIMIT ? OFFSET ?",
                        (*query_params, args.limit, args.offset),
                    )
                ]
            elif args.command == "holdings":
                from .raw_archive import holdings

                result = holdings(db, args.collection)
            elif args.command == "gap-report":
                result = [
                    dict(r)
                    for r in db.execute(
                        "SELECT collection_id,status,count(*) AS items FROM files"
                        + where
                        + " GROUP BY collection_id,status ORDER BY collection_id,status",
                        params,
                    )
                ]
            else:
                inventory_by_route = {}
                for route_id, status, count in db.execute(
                    "SELECT route_id,status,count(*) FROM files WHERE route_id IS NOT NULL GROUP BY route_id,status"
                ):
                    inventory_by_route.setdefault(route_id, {})[status] = count
                relationships_by_route = {}
                policies_by_route = {}
                for kind, data in db.execute(
                    "SELECT kind,data FROM evidence WHERE kind IN ('relationships','planned') ORDER BY id"
                ):
                    evidence = json.loads(data)
                    if kind == "relationships":
                        for route_id in {evidence.get("from_id"), evidence.get("to_id")} - {None}:
                            relationships_by_route.setdefault(route_id, []).append(evidence)
                    elif evidence.get("route_id"):
                        policies_by_route.setdefault(evidence["route_id"], []).append(evidence)
                result = []
                for row in db.execute("SELECT * FROM routes" + where + " ORDER BY id", params):
                    route = json.loads(row["data"])
                    qualified = [
                        json.loads(r[0])
                        for r in db.execute(
                            "SELECT data FROM evidence WHERE kind='periods' AND collection_id=?",
                            (row["collection_id"],),
                        )
                    ]
                    periods = (route.get("periods", []) + [p for p in qualified if p.get("route_id") == row["id"]]) or [
                        {"kind": "unknown", "label": "Unknown", "basis": "no-qualified-period-assertion"}
                    ]
                    for period in periods:
                        relationships = relationships_by_route.get(row["id"], [])
                        result.append(
                            {
                                "collection_id": row["collection_id"],
                                "route_id": row["id"],
                                "period": period,
                                "route": route,
                                "comparison": (
                                    "Scoped relationship evidence recorded; apply only within each stated scope"
                                    if relationships
                                    else "unresolved; no qualified cross-route identity mapping"
                                ),
                                "relationships": relationships,
                                "selection_policies": policies_by_route.get(row["id"], []),
                                "inventory": inventory_by_route.get(row["id"], {}),
                                "holdings_assignment": "Only explicit route_id bindings count; unassigned collection holdings remain unresolved",
                            }
                        )
        finally:
            db.close()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
