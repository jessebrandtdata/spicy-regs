"""Cloudflare R2 Data Catalog (Apache Iceberg) connector.

The internal, write-side table format for the ETL. The catalog is **R2 Data
Catalog** — a managed Iceberg REST catalog built into the same R2 bucket the
project already publishes Parquet to, so no separate metastore (Glue/Nessie/
Postgres) has to be stood up.

DuckDB's ``iceberg`` and ``httpfs`` extensions provide catalog access. Version
1.5.3 or later supports the atomic MERGE used for paired writes.
The current upsert implementation uses a checked atomic MERGE, keeping the row with
the most recent ``modify_date`` for each primary key.

This module is the "Iceberg load" stage only, mirroring the thin-wrapper style
of :mod:`spicy_regs.sources.r2`:

* :func:`merge_and_export` — ensure the table exists, MERGE the per-agency
  staging Parquet in, then export a public ``{name}.parquet`` snapshot so the
  no-credentials CLI / MCP read path keeps working (the "dual model").
* :func:`merge_comments` upserts staged comments without rebuilding public files.
  :func:`export_public_comments` builds the compatible mirror and index together
  at the end of a successful ingestion sweep.

Credentials are read from the environment, alongside the existing ``R2_*`` vars:

* ``R2_CATALOG_URI``        — the Iceberg REST catalog endpoint (catalog-uri)
* ``R2_CATALOG_WAREHOUSE``  — the warehouse name
* ``R2_CATALOG_TOKEN``      — an R2 API token with R2 + data-catalog permissions
* ``R2_CATALOG_NAMESPACE``  — Iceberg namespace/schema (optional, default ``default``)
"""

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from loguru import logger

from spicy_regs.schemas import RecordType
from spicy_regs.duckdb_settings import ExportResources

if TYPE_CHECKING:
    import polars as pl

# DuckDB alias the attached catalog is addressed by (``<alias>.<namespace>.<table>``).
_CATALOG_ALIAS = "reg_catalog"

# Required environment variables for the catalog connection.
_REQUIRED_ENV = ("R2_CATALOG_URI", "R2_CATALOG_WAREHOUSE", "R2_CATALOG_TOKEN")


def is_configured() -> bool:
    """True when every credential needed to reach the catalog is present."""
    return all(getenv(var) for var in _REQUIRED_ENV)


def _namespace() -> str:
    # `or "default"` (not getenv's default arg) so an env var set to an empty
    # string — e.g. a GitHub Actions `${{ secrets.R2_CATALOG_NAMESPACE }}` that
    # resolves to "" when the secret is unset — still falls back to "default".
    return getenv("R2_CATALOG_NAMESPACE") or "default"


def _schema_ref() -> str:
    """Quoted ``alias."namespace"`` reference (the default namespace is a keyword)."""
    return f'{_CATALOG_ALIAS}."{_namespace()}"'


def _sql_str(value: str) -> str:
    """Escape a value for inlining inside a single-quoted SQL literal."""
    return value.replace("'", "''")


def _connect():
    """Open a DuckDB connection with the R2 Data Catalog attached.

    ``CREATE SECRET`` / ``ATTACH`` do not accept bind parameters, so the
    credentials are inlined with single-quote escaping. The token never leaves
    this process — it is read from the environment, used to attach, and the
    connection is closed by the caller.
    """
    import duckdb

    if not is_configured():
        missing = [var for var in _REQUIRED_ENV if not getenv(var)]
        raise RuntimeError("R2 Data Catalog is not configured; missing env var(s): " + ", ".join(missing))

    uri = getenv("R2_CATALOG_URI", "")
    warehouse = getenv("R2_CATALOG_WAREHOUSE", "")
    token = getenv("R2_CATALOG_TOKEN", "")

    con = duckdb.connect()
    # avro is installed explicitly before iceberg: DuckDB 1.5's iceberg extension
    # pulls in `avro` to read Iceberg manifests and otherwise auto-installs it
    # lazily during LOAD, a nested install that fails in sandboxes without a
    # writable home directory. Provisioning it on the top-level path avoids that.
    try:
        con.execute("INSTALL avro; LOAD avro;")
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN '{_sql_str(token)}');")
        con.execute(f"ATTACH '{_sql_str(warehouse)}' AS {_CATALOG_ALIAS} (TYPE ICEBERG, ENDPOINT '{_sql_str(uri)}');")
    except Exception:
        con.close()
        raise
    return con


CATALOG_LOCK_ENV = "SPICY_REGS_CATALOG_LOCK"
_warned_unlocked: set[str] = set()


def _warn_unlocked_write(record_type: RecordType, operation: str) -> None:
    """Warn once per table when this process writes the catalog without the workflow lock.

    Workflows in the ``comments-catalog-write`` concurrency group set
    ``SPICY_REGS_CATALOG_LOCK``. Any other write (rows or schema) can land
    while the ETL or mirror job exports, and that export then refuses with
    "Catalog changed during export" (ETL 36351853866). The write proceeds.
    """
    if getenv(CATALOG_LOCK_ENV) or record_type.name in _warned_unlocked:
        return
    _warned_unlocked.add(record_type.name)
    logger.warning(
        "{}: writing catalog table {} without {} (the comments-catalog-write group). A concurrent ETL or "
        "mirror export will refuse with 'Catalog changed during export'. Run catalog writes "
        "only when no ETL or mirror run holds comments-catalog-write.",
        operation, record_type.name, CATALOG_LOCK_ENV,
    )


def _catalog_namespace(record_type):
    from .regulatory_catalog import namespace, supports
    if not supports(record_type):
        raise ValueError(f'Unsupported regulatory catalog dataset: {record_type.name}')
    return namespace()


def _qualified(record_type: RecordType) -> str:
    from .regulatory_catalog import qualified
    _catalog_namespace(record_type)
    return qualified(record_type)


def _ensure_table(con, record_type: RecordType) -> None:
    """Initialize the declared native subject and receipt tables."""
    from .regulatory_catalog import ensure_native
    ensure_native(con, record_type)




def _column_types(con, record_type: RecordType) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f"DESCRIBE {_qualified(record_type)}").fetchall()}


def _connect_for_table(record_type: RecordType):
    """Open one connection and initialize its native dataset if it is new."""
    _warn_unlocked_write(record_type, "prepare for write")
    con = _connect()
    try:
        _ensure_table(con, record_type)
    except BaseException:
        con.close()
        raise
    return con


def _staging_files(staging_dir: Path, record_type: RecordType) -> list[Path]:
    """Per-agency staging Parquet files for this record type (see write_staging)."""
    staging_type_dir = staging_dir / record_type.name
    if not staging_type_dir.exists():
        return []
    return sorted(staging_type_dir.glob("*.parquet"))


@contextmanager
def _transaction(con) -> Iterator[None]:
    """Commit every enclosed write together; roll back even on interruption."""
    con.execute("BEGIN")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        try:
            con.execute("ROLLBACK")
        except Exception as rollback_exc:
            logger.warning("iceberg: ROLLBACK failed after an aborted write: {}", rollback_exc)
        raise


def _scope_predicate(record_type: RecordType, scope: Mapping[str, str] | None, alias: str = "") -> str:
    """Render column-equals-literal constraints as a trusted SQL predicate (``TRUE`` when unscoped)."""
    if not scope:
        return "TRUE"
    unknown = sorted(set(scope) - set(record_type.schema))
    if unknown:
        raise ValueError(f"Catalog replacement scope names unknown columns: {', '.join(unknown)}")
    return " AND ".join(f"{alias}\"{column}\" = '{_sql_str(value)}'" for column, value in scope.items())


def replace_rows(
    con, record_type: RecordType, source: str, *, expected_prior: str | None = None,
    scope: Mapping[str, str] | None = None, expected_snapshot: "CatalogSnapshot | None" = None,
) -> None:
    """Atomically replace selected unique keys, validating values before commit.

    ``source`` must be a self-contained temp table, never a projection over the
    live table (see :func:`upsert_comment_text`). Shared by the ETL upsert
    (:func:`_merge`), the text fills and the comment source repair.
    Read-modify-write callers pass a self-contained ``expected_prior``; its rows
    and absences must still match inside the write transaction. Direct
    replacements may omit it.

    ``expected_snapshot`` refuses inside the transaction unless the table is still
    at that snapshot, so nothing committed since the caller looked is overwritten
    (the comment fill's undo passes the snapshot its operator reviewed).

    ``scope`` (column → value, e.g. one ``agency_code``) restricts every catalog
    read and the MERGE match to rows with those values; every source row must
    carry them. A same-key row outside the scope is neither read nor replaced.

    Inside the transaction the target rows for the source keys are copied once;
    the prior and duplicate checks read that copy. Only the MERGE and its
    readback read the catalog table again, so an unscoped call scans it three
    times, not four.
    """
    from .regulatory_catalog import replace_native
    _warn_unlocked_write(record_type, "replace rows")
    return replace_native(con, record_type, source, expected_prior=expected_prior,
                          scope=scope, expected_snapshot=expected_snapshot)


def _merge(con, staging_files: list[Path], record_type: RecordType) -> int:
    """Row-level upsert of the staged rows into the Iceberg table.

    Uses a single MERGE with transactional readback (:func:`replace_rows`).

    Mirrors the dedup semantics of ``transforms.merge_staging_files``: collapse
    the staging rows to one per key (latest ``modify_date`` wins), keep only the
    keys whose incoming row is brand-new or strictly newer than the table's, then
    replace exactly those keys. Deleting by key (not by agency) is essential — a
    since-year-filtered run stages only a slice, so an agency-wide delete would
    drop the rows it isn't re-inserting. ``modify_date`` is an ISO-8601 string,
    so the lexical ``>`` comparison orders chronologically.

    Older DELETE/INSERT writes could leave physical duplicates: on the R2 Data
    Catalog a DELETE did not reliably remove the prior row (f70e2e1, 903f28a).
    MERGE support requires DuckDB >=1.5.3; this path refuses affected prior
    duplicates rather than guessing which historical row to keep. Existing
    duplicates require a fresh qualified source build; the live runtime never
    chooses a winner among corrupted stored rows. The in-memory tests cannot exercise the Iceberg engine;
    ``scripts/probe_catalog_replace.py`` runs this write path against a
    throwaway catalog table in the integration workflow.

    Table reads per call: the prior below (outside the transaction), then the
    in-transaction copy, the MERGE and its readback in :func:`replace_rows`.
    """
    key = record_type.dedup_key
    from .regulatory_catalog import processing_table
    tbl = _qualified(record_type)
    # Temp names are per-record-type so a dockets + comments run on one
    # connection can't collide.
    staged = f"_staged_{record_type.name}"
    winners = f"_winners_{record_type.name}"
    prior = f"_prior_{record_type.name}"

    files_sql = ", ".join(f"'{_sql_str(str(p))}'" for p in staging_files)
    source = f"read_parquet([{files_sql}], union_by_name=true)"
    from .regulatory_catalog import _validate_source_columns
    _validate_source_columns(con, source, record_type.name)

    # 1. Collapse staging to one row per key (latest modify_date wins).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {staged} AS
        SELECT *
        FROM {source}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "{key}"
            ORDER BY modify_date DESC NULLS LAST
        ) = 1;
        """
    )
    tbl = processing_table(con, record_type, where=f'"{key}" IN (SELECT "{key}" FROM {staged})')
    con.execute(f'CREATE OR REPLACE TEMP TABLE {prior} AS SELECT * FROM {tbl} '
                f'WHERE "{key}" IN (SELECT "{key}" FROM {staged})')
    # 2. Keep only rows that should win over the table: a new key, or one whose
    #    incoming modify_date is strictly newer (matching the old MERGE guard).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {winners} AS
        SELECT s.*
        FROM {staged} s
        LEFT JOIN {prior} t ON t."{key}" = s."{key}"
        WHERE t."{key}" IS NULL
           OR t.modify_date IS NULL
           OR s.modify_date > t.modify_date;
        """
    )
    changed = con.execute(f"SELECT count(*) FROM {winners}").fetchone()[0]
    try:
        # Avoid empty catalog commits: the snapshot is also the mirror's input identity.
        from .regulatory_catalog import replace_native
        rejected = f'(SELECT * FROM {source} EXCEPT ALL SELECT * FROM {winners})'
        replace_native(con, record_type, winners, expected_prior=prior, rejected_source=rejected)
        return changed
    finally:
        con.execute(f"DROP TABLE IF EXISTS {staged};")
        con.execute(f"DROP TABLE IF EXISTS {winners};")
        con.execute(f"DROP TABLE IF EXISTS {prior};")
        con.execute(f"DROP TABLE IF EXISTS {tbl}")


def _export_parquet(con, record_type: RecordType, output_dir: Path) -> Path:
    """Export a checked native pair and its byte-identical subject file."""
    import shutil
    from uuid import uuid4
    from . import regulatory_catalog as native
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = native.export_pair(con, record_type, output_dir / '.catalog-pairs' / record_type.name,
                                  generation_id='catalog-' + uuid4().hex)
    output = output_dir / f'{record_type.name}.parquet'
    shutil.copyfile(selected.subjects[0], output)
    return output


def _build_comments_index(con, record_type: RecordType, output_dir: Path, *, source_sql: str | None = None) -> Path:
    """Rebuild ``comments_index.parquet`` from the catalog comments table.

    The index is the small per-``(agency_code, docket_id, year, month)`` row-count
    artifact that ``build_feed_summary`` / ``build_agency_rollups`` read instead
    of scanning the full comments table. This is the published index's only
    builder: it is derived straight from the table with the schema
    (agency_code, docket_id, year, month, row_count).

    ``year`` / ``month`` come from ``posted_date``; ``docket_id`` is trimmed of
    stray quotes. NULL docket IDs and dates retain NULL groups. Written atomically
    via a temp file so a crashed rebuild can't leave a half-written index in place.
    """
    from spicy_regs.transforms.comment_partitions import validate_comment_coordinates

    source_sql = source_sql or f"SELECT * FROM {_qualified(record_type)}"
    validate_comment_coordinates(con, source_sql)
    index_file = output_dir / "comments_index.parquet"
    tmp_file = index_file.with_suffix(".tmp.parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                agency_code,
                TRIM(docket_id, '"') AS docket_id,
                EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS year,
                EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS month,
                CAST(COUNT(*) AS BIGINT) AS row_count
            FROM ({source_sql})
            GROUP BY 1, 2, 3, 4
        ) TO '{_sql_str(str(tmp_file))}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    tmp_file.replace(index_file)
    return index_file


_REQUIRED_S3_ENV = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")


def create_s3_secret(con) -> None:
    """Register R2's S3 API as a DuckDB secret so bucket objects can be read/globbed.

    Reads over the public HTTPS URL can't list directories (the whole reason the
    catalog cutover exists), so source Parquet is read via the S3 API, which does
    support listing/globbing. Shared by the catalog seeding scripts.
    """
    missing = [v for v in _REQUIRED_S3_ENV if not getenv(v)]
    if missing:
        raise RuntimeError("Missing R2 S3 env var(s): " + ", ".join(missing))
    # R2_ENDPOINT is a full URL for boto3; DuckDB wants the bare host + USE_SSL.
    endpoint = getenv("R2_ENDPOINT", "")
    host = urlparse(endpoint).netloc or endpoint.replace("https://", "").replace("http://", "")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET r2_s3_secret (
            TYPE S3,
            KEY_ID '{_sql_str(getenv("R2_ACCESS_KEY_ID", ""))}',
            SECRET '{_sql_str(getenv("R2_SECRET_ACCESS_KEY", ""))}',
            ENDPOINT '{_sql_str(host)}',
            URL_STYLE 'path',
            USE_SSL true,
            REGION 'auto'
        );
        """
    )


def upsert_comment_text(con, record_type: RecordType, agency: str, updates: "pl.DataFrame | Path") -> None:
    """Upsert filled ``text_content`` / ``text_extraction_status`` for one agency.

    Shared helper for the durable text-fill paths (derived-data backfill and PDF
    enrichment). ``updates`` is a polars DataFrame, or a Parquet file read without
    loading it into Python, with columns ``comment_id, _new_text, _new_status``
    and optionally ``_new_pdf_results`` (both fill paths supply it: PDF attempts,
    or the derived-text provenance). Every row whose ``comment_id`` matches
    the agency's rows gets ``text_content`` / ``text_extraction_status`` refreshed
    (``COALESCE`` keeps the existing value when the incoming column is NULL). It
    uses the checked atomic replacement (:func:`replace_rows`) with an
    ``agency_code`` scope: the prior read, the in-transaction copy, the MERGE
    match and the readback all carry the agency predicate, so Iceberg can skip
    files whose ``agency_code`` bounds exclude it instead of reading the whole
    tens-of-millions-row table for each agency. A same ``comment_id`` under
    another agency is neither read nor replaced. No-ops on an empty frame; the
    caller is expected to have handled that case already.

    CRITICAL — self-contained temp table: the MERGE reads from an independent
    ``_uct_replacement`` temp table (a full snapshot of the affected rows with the
    requested columns overridden in place), **never** from a projection over the live
    catalog table. An earlier version projected the overrides straight off
    ``{tbl} t JOIN updates`` in the INSERT's SELECT; on the R2 Data Catalog that
    dropped column writes — ``text_content`` landed but ``text_extraction_status``
    came back NULL, so incremental re-runs kept re-selecting already-filled rows.
    Snapshotting into a self-contained temp table removes the live-table reference
    from the write path and fixes it (see PR #117). Plain DuckDB does not reproduce
    the catalog behavior, so this invariant is verified by a scoped catalog run,
    not the unit test — keep the ``_uct_replacement`` indirection intact.
    """
    if isinstance(updates, Path):
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _uct_updates AS SELECT * FROM read_parquet('{_sql_str(str(updates))}');"
        )
    else:
        con.register("_uct_updates_src", updates.to_arrow())
        try:
            con.execute("CREATE OR REPLACE TEMP TABLE _uct_updates AS SELECT * FROM _uct_updates_src;")
        finally:
            con.unregister("_uct_updates_src")
    if not con.execute("SELECT count(*) FROM _uct_updates").fetchone()[0]:
        con.execute("DROP TABLE _uct_updates;")
        return

    from .regulatory_catalog import processing_table
    tbl = processing_table(con, record_type, where=f"agency_code = '{_sql_str(agency)}' AND comment_id IN (SELECT comment_id FROM _uct_updates)")
    ag = _sql_str(agency)
    col_list = ", ".join(f'"{c}"' for c in record_type.schema)
    update_columns = {row[0] for row in con.execute("DESCRIBE _uct_updates").fetchall()}
    pdf_assignment = (
        ", pdf_extraction_results_json = COALESCE(u._new_pdf_results, r.pdf_extraction_results_json)"
        if "_new_pdf_results" in update_columns
        else ""
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _uct_prior AS
        SELECT {col_list} FROM {tbl}
        WHERE agency_code = '{ag}' AND comment_id IN (SELECT comment_id FROM _uct_updates);
        """
    )
    con.execute("CREATE OR REPLACE TEMP TABLE _uct_replacement AS SELECT * FROM _uct_prior")
    con.execute(
        f"""
        UPDATE _uct_replacement AS r
        SET text_content = COALESCE(u._new_text, r.text_content),
            text_extraction_status = COALESCE(u._new_status, r.text_extraction_status){pdf_assignment}
        FROM _uct_updates u
        WHERE r.comment_id = u.comment_id;
        """
    )
    replace_rows(con, record_type, "_uct_replacement", expected_prior="_uct_prior", scope={"agency_code": agency})
    con.execute("DROP TABLE IF EXISTS _uct_updates;")
    con.execute("DROP TABLE IF EXISTS _uct_replacement;")
    con.execute("DROP TABLE IF EXISTS _uct_prior;")
    con.execute(f"DROP TABLE {tbl}")


def merge_comments(staging_dir: Path, record_type: RecordType) -> int:
    """Upsert staged comments; final mirror publication owns the index recount."""
    staging_files = _staging_files(staging_dir, record_type)
    if not staging_files:
        return 0
    from spicy_regs.transforms.comment_partitions import validate_staged_comments

    validate_staged_comments(staging_dir)
    con = _connect_for_table(record_type)
    try:
        changed = _merge(con, staging_files, record_type)
        logger.info("iceberg: merged {:,} winning {} rows", changed, record_type.name)
        return changed
    finally:
        con.close()


@dataclass(frozen=True)
class CatalogSnapshot:
    """Exact source version, including identity across table recreation."""

    table_uuid: str
    snapshot_id: int
    schema_id: int


@dataclass(frozen=True)
class CatalogPairSnapshot(CatalogSnapshot):
    """Subject identity plus the shared receipt table's independently changing identity."""

    receipts: CatalogSnapshot


def _selected_catalog_namespace(con, record_type: RecordType) -> str:
    """Only a checked native initialization establishes read authority."""
    from .regulatory_catalog import require_initialized
    require_initialized(con, record_type)
    return _catalog_namespace(record_type)


def _table_metadata(con, record_type: RecordType, *, namespace: str | None = None) -> dict:
    selected_namespace = namespace if namespace is not None else _selected_catalog_namespace(con, record_type)
    return _catalog_metadata(con, selected_namespace, record_type.name)


def _catalog_metadata(con, namespace: str, table: str) -> dict:
    raw = con.execute("SELECT metadata FROM iceberg_load_table_response(?)", [
        f"{_CATALOG_ALIAS}.{namespace}.{table}",
    ]).fetchone()[0]
    return json.loads(raw) if isinstance(raw, str) else raw


def _receipt_metadata(con, record_type: RecordType, *, namespace: str | None = None) -> dict:
    selected_namespace = namespace if namespace is not None else _selected_catalog_namespace(con, record_type)
    return _catalog_metadata(con, selected_namespace, "etl_receipts")


def _current_snapshot(metadata: dict) -> CatalogSnapshot:
    return CatalogSnapshot(metadata["table-uuid"], int(metadata["current-snapshot-id"]),
                           int(metadata["current-schema-id"]))


def _read_snapshot(con, record_type: RecordType, *, namespace: str | None = None) -> CatalogSnapshot:
    snapshot = _current_snapshot(_table_metadata(con, record_type, namespace=namespace))
    if not snapshot.table_uuid or snapshot.snapshot_id < 0:
        raise RuntimeError("Comments catalog has no usable snapshot")
    return snapshot


def _read_pair_snapshot(con, record_type: RecordType, *, namespace: str | None = None) -> CatalogPairSnapshot:
    """Pin both tables; an explicit ``namespace`` skips the initialization check (a rebuild reads an uninitialized pair)."""
    subject = _read_snapshot(con, record_type, namespace=namespace)
    receipts = _current_snapshot(_receipt_metadata(con, record_type, namespace=namespace))
    if not receipts.table_uuid or receipts.snapshot_id < 0:
        raise RuntimeError("Catalog receipts have no usable snapshot")
    return CatalogPairSnapshot(subject.table_uuid, subject.snapshot_id, subject.schema_id, receipts)


def catalog_snapshot(record_type: RecordType) -> CatalogPairSnapshot:
    """Pin subject and receipt metadata in one transaction without preparing tables."""
    con = _connect()
    try:
        with _transaction(con):
            return _read_pair_snapshot(con, record_type)
    finally:
        con.close()


def _compacted_only(metadata: dict, snapshot: CatalogSnapshot) -> bool:
    """True when the current snapshot descends from ``snapshot`` through ``replace`` commits alone.

    Iceberg's ``replace`` rewrites data and delete files without changing the
    table's rows; R2's managed compaction commits one about hourly (ETL 36760140108
    and 36827148539 refused on it). Any other operation, a recreated table, a
    schema change or a current snapshot outside ``snapshot``'s descendants is a change.
    """
    current = _current_snapshot(metadata)
    if (current.table_uuid, current.schema_id) != (snapshot.table_uuid, snapshot.schema_id):
        return False
    parents = {int(s["snapshot-id"]): s for s in metadata.get("snapshots", [])}
    step = current.snapshot_id
    for _ in range(len(parents)):  # a chain visits each snapshot once; a parent cycle must not spin forever
        if step == snapshot.snapshot_id:
            return True
        commit = parents.get(step)
        if commit is None or commit.get("summary", {}).get("operation") != "replace" \
                or commit.get("parent-snapshot-id") is None:
            return False
        step = int(commit["parent-snapshot-id"])
    return step == snapshot.snapshot_id


def compacted_since(record_type: RecordType, snapshot: CatalogPairSnapshot, *,
                    declared_schema: bool = False) -> tuple[bool, bool]:
    """Whether the subject and the receipt table each moved past ``snapshot`` by compaction alone.

    Reads table metadata only. ``snapshot`` was pinned under the initialization check, and a pair reached through
    ``replace`` commits alone keeps that table and initialization receipt, so the check's scan of every receipt row
    (78 s on the 26.5M-row table, 2026-11-08) proves nothing more here. The declared policy is code and can move
    without a commit: ``declared_schema`` also refuses a subject schema that differs from it, by the rule
    :func:`regulatory_catalog.require_initialized` applies.
    """
    from .regulatory_catalog import schema_difference
    namespace = _catalog_namespace(record_type)
    con = _connect()
    try:
        with _transaction(con):
            if declared_schema and (difference := schema_difference(con, record_type)):
                raise ValueError("Native catalog schema differs from declared policy: " + difference)
            return (_compacted_only(_table_metadata(con, record_type, namespace=namespace), snapshot),
                    _compacted_only(_receipt_metadata(con, record_type, namespace=namespace), snapshot.receipts))
    finally:
        con.close()


def rows_unchanged_since(record_type: RecordType, snapshot: CatalogPairSnapshot) -> bool:
    """Require unchanged subject and receipt rows, allowing compaction of either table."""
    return all(compacted_since(record_type, snapshot))


def _snapshot_query(record_type: RecordType, snapshot: CatalogSnapshot, *, namespace: str | None = None) -> str:
    table = (_qualified(record_type) if namespace is None
             else f'{_CATALOG_ALIAS}."{namespace}"."{record_type.name}"')
    return f"SELECT * FROM {table} AT (VERSION => {int(snapshot.snapshot_id)})"


def export_public_comments(
    output_dir: Path, record_type: RecordType, *, memory_limit: str = ExportResources.memory,
    threads: int = ExportResources.threads,
    snapshot: CatalogPairSnapshot | None = None, resources: ExportResources | None = None,
) -> dict[str, Path]:
    """Scan one pinned snapshot into agencies; sort once, assemble, then recount.

    Every payload read from the catalog includes its Iceberg deletes. Remaining
    work reads local files. Anonymous readers retain their existing paths/schema.
    """
    import duckdb
    from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies

    resources = resources or ExportResources(memory=memory_limit, threads=threads)
    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="comments-export-", dir=output_dir) as work:
        work_dir = Path(work)
        con = _connect()
        try:
            from . import regulatory_catalog as native
            from spicy_regs.transforms.regulations_receipts import policy
            from uuid import uuid4
            native.require_initialized(con, record_type)
            current = _read_pair_snapshot(con, record_type)
            if snapshot is not None and current != snapshot:
                raise RuntimeError("Catalog snapshot changed before export; retry")
            resources.configure(con, work_dir / "spill")
            sidecar = output_dir / '.catalog-pairs' / record_type.name
            selected = native.export_pair(con, record_type, sidecar,
                                          generation_id='catalog-' + uuid4().hex, snapshot=current)
            columns = policy(record_type.name).subject_schema.names
            source = f"SELECT * FROM read_parquet('{_sql_str(str(selected.subjects[0]))}')"
            stage_comment_agencies(con, source, work_dir / "staging", resources=resources)
        finally:
            con.close()
        partitions = sort_comment_agencies(work_dir / "staging", output_dir, resources=resources)
        with duckdb.connect() as con:
            resources.configure(con, work_dir / "spill")
            monolith = assemble_comments(con, partitions, output_dir, columns, resources=resources)
            con.from_parquet(str(monolith)).create_view("comments_export")
            index_file = _build_comments_index(con, record_type, output_dir,
                                               source_sql="SELECT * FROM comments_export")
        result = {"comments": monolith, "index": index_file, "partitions": partitions}
        result.update(receipts=selected.receipts, generation=sidecar / "generation.json")
        return result


def check_comments_against_indexes(record_type: RecordType, indexes: Mapping[str, Sequence[str]], *, agency: str | None = None,
                                   limit: int = 100, threads: int = 8) -> list[str]:
    """Raw catalog IDs and agency/docket/month counts against each labelled public index's file URLs.

    Remote Parquet and Iceberg scans wait on one ranged GET per column chunk; parallel requests, not CPU, set their
    pace (the full catalog check took 53.6 s at 2 threads and 18.8 s at 8, 2026-11-08).
    """
    from spicy_regs.comments_health import check_comments
    from spicy_regs.duckdb_settings import load_public_http
    from .publication import parquet_scan
    from .regulatory_catalog import require_initialized

    where = f" WHERE agency_code = '{_sql_str(agency)}'" if agency else ""
    con = _connect()
    try:
        load_public_http(con)
        con.execute(f"SET memory_limit='4GB'; SET threads={int(threads)}; SET preserve_insertion_order=false")
        require_initialized(con, record_type)
        return check_comments(con, f"SELECT * FROM {_qualified(record_type)}{where}",
                              {label: f"SELECT * FROM {parquet_scan(urls)}{where}" for label, urls in indexes.items()},
                              limit=limit)
    finally:
        con.close()


def audit_duplicates(con, record_type: RecordType) -> list[tuple[str, int, int]]:
    """Per-agency (agency_code, rows, distinct_keys) where rows exceed distinct keys.

    A read-only duplication report over the catalog table: any agency whose row
    count is above its distinct ``dedup_key`` count carries duplicate rows. Sorted
    by the number of duplicate rows, worst first. Empty when the table is clean.
    """
    from .regulatory_catalog import require_initialized
    require_initialized(con, record_type)
    key = record_type.dedup_key
    tbl = _qualified(record_type)
    rows = con.execute(
        f"""
        SELECT agency_code,
               count(*) AS rows,
               count(DISTINCT "{key}") AS distinct_keys
        FROM {tbl}
        WHERE agency_code IS NOT NULL
        GROUP BY agency_code
        HAVING count(*) > count(DISTINCT "{key}")
        ORDER BY count(*) - count(DISTINCT "{key}") DESC
        """
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def merge_and_export(staging_dir: Path, output_dir: Path, record_type: RecordType) -> Path | None:
    """Upsert staged rows into the catalog table, then export the public Parquet.

    Returns the path to the exported ``{name}.parquet`` (so the pipeline can
    publish it via the existing R2 upload), or ``None`` when there was nothing
    staged for this record type.
    """
    staging_files = _staging_files(staging_dir, record_type)
    if not staging_files:
        logger.info("iceberg: no staging files for {}; skipping merge", record_type.name)
        return None

    con = _connect_for_table(record_type)
    try:
        logger.info(
            "iceberg: MERGE {} staging file(s) into {}",
            len(staging_files),
            _qualified(record_type),
        )
        _merge(con, staging_files, record_type)
        total = con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]
        logger.info("iceberg: {} now holds {:,} rows", record_type.name, total)
        out_file = _export_parquet(con, record_type, output_dir)
        logger.info("iceberg: exported public snapshot to {}", out_file)
        return out_file
    finally:
        con.close()
