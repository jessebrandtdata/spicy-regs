"""Private citation processing rows from the same selected native ETL receipts."""
from __future__ import annotations

from collections.abc import Mapping
import json
import hashlib
import threading
import weakref
from contextlib import contextmanager

from spicy_regs.runtime_bounds import bounded_connection, checkpoint

from spicy_regs.fec_receipt_adapter import receipt_owner

INPUT_RECORD = "_spicy_citation_inputs"
PROCESSING_PREFIX = "_spicy_citation_processing_"
SOURCE_DIGEST_PREFIX = "_spicy_citation_digests_"
PROCESSING_TABLES = ("document_citations", "document_citation_reads", "house_activity_reports", "budget_volumes")
_initialization_locks = weakref.WeakKeyDictionary()
_registry_lock = threading.Lock()


@contextmanager
def _bounded_lock(lock):
    while not lock.acquire(timeout=.05):
        checkpoint()
    try:
        checkpoint()
        yield
    finally:
        lock.release()


def _has_inputs(cursor):
    import duckdb

    try:
        cursor.execute(f"SELECT snapshot FROM {INPUT_RECORD}").fetchone()
    except duckdb.CatalogException:
        return False
    return True


def _copy_inputs(source, destination, record):
    """Transfer verified private rows; the caller commits the readiness record last."""
    names = dict.fromkeys((*record["tables"].values(), *record["sources"].values()))
    temporary = "_spicy_citation_transfer_batch"
    for name in names:
        checkpoint()
        if not name.startswith((PROCESSING_PREFIX, SOURCE_DIGEST_PREFIX)):
            raise ValueError("Citation input binding names a nonprivate table")
        quoted = '"' + name.replace('"', '""') + '"'
        reader = source.execute(f"SELECT * FROM {quoted}").to_arrow_reader(batch_size=2000)
        import pyarrow as pa

        destination.register(temporary, pa.Table.from_batches([], schema=reader.schema))
        try:
            destination.execute(f'CREATE TABLE {quoted} AS SELECT * FROM "{temporary}"')
            for batch in reader:
                checkpoint()
                destination.register(temporary, pa.Table.from_batches([batch]))
                destination.execute(f'INSERT INTO {quoted} SELECT * FROM "{temporary}"')
        finally:
            destination.unregister(temporary)
            reader.close()
        source.execute(f"DROP TABLE {quoted}")
    destination.execute(f"CREATE TABLE {INPUT_RECORD} (snapshot VARCHAR)")
    destination.execute(f"INSERT INTO {INPUT_RECORD} VALUES (?)", [json.dumps(record)])


def ensure_citation_inputs(connection, cursor, index, base_url, available, *,
                           local_directory=None, local_selection=None, memory_limit=None, temp_directory=None):
    """Prepare on demand without changing the serving connection's locked file access.

    A private database validates downloaded sidecars and native receipts. Only
    verified Arrow batches cross into serving, atomically. Locks retain neither
    connections nor source readers; a failed request may be retried explicitly.
    """
    import duckdb
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter

    with _registry_lock:
        lock = _initialization_locks.setdefault(connection, threading.Lock())
    with _bounded_lock(lock), bounded_connection(cursor):
        if _has_inputs(cursor):
            return
        selected = local_selection or {}
        with duckdb.connect(config={"threads": 2, "allow_persistent_secrets": False}) as private:
            with bounded_connection(private):
                if memory_limit is not None:
                    private.execute("SET memory_limit=?", [memory_limit])
                if temp_directory is not None:
                    private.execute("SET temp_directory=?", [temp_directory])
                adapter = ReceiptAdapter(
                    private, index, base_url, local_directory=local_directory,
                    local_receipts=(selected.get("receipt_members", {}) if local_selection is not None else None),
                    local_native=selected.get("native", {}),
                )
                install_citation_inputs(private, adapter, available)
                record = selected_citation_inputs(private)
                cursor.execute("BEGIN TRANSACTION")
                try:
                    _copy_inputs(private, cursor, record)
                    cursor.execute("COMMIT")
                except BaseException:
                    cursor.execute("ROLLBACK")
                    raise


def install_citation_inputs(connection, adapter, available):
    """Bind verified source rows in the trusted validation database.

    The public subject tables keep native columns. Only citation tool code can
    use these regular in-memory tables; request cursors share them and public
    SQL cannot name the private prefix.
    """
    from spicy_regs.citation_resolution import SOURCE_TABLES

    def selected(table):
        return table in adapter.local_native or receipt_owner(adapter.index, table) is not None

    if "document_citations" not in available or not selected("document_citations"):
        return
    import pyarrow as pa
    from spicy_regs.legislative_documents import field_registry, recorded_subject, native_document_key

    registry = field_registry()
    tables = {}
    pins = {}
    for table in PROCESSING_TABLES:
        if not selected(table) or (table != "document_citation_reads" and table not in available):
            continue
        schema = pa.schema([(field["name"], pa.string()) for field in registry[table]["fields"]])

        court_keys = {}

        def original(row, dataset=table, court_keys=court_keys):
            raw = row.get("raw_source_row")
            if not isinstance(raw, Mapping):
                raise ValueError(f"{dataset}: citation source input is missing from the selected receipt")
            mapped = recorded_subject(dataset, raw, row)
            if mapped is not None and any(row.get(name) != value for name, value in mapped.items()):
                raise ValueError(f"{dataset}: citation source input differs from its selected native subject")
            result = dict(raw)
            if dataset == "document_citations":
                result["document_key"] = native_document_key(raw.get("document_kind"), row["document_key"])
            elif dataset == "document_citation_reads":
                result["document_key"] = native_document_key(raw.get("document_kind"), raw.get("document_key"))
            if raw.get("document_kind") == "court_opinion_derived_pdf":
                key, spelling = result["document_key"], raw["document_key"]
                if key in court_keys and court_keys[key] != spelling:
                    raise ValueError("Historical citation keys converge on one native scope")
                court_keys[key] = spelling
            return result

        tables[table] = adapter.restore_originals(table, schema, original, prefix=PROCESSING_PREFIX)
        owner = receipt_owner(adapter.index, table)
        native = adapter.local_native.get(table)
        pins[table] = (
            {"status": "native_selected", "generation_id": native["generation_id"]}
            if native is not None else
            {"status": "published", "family": owner[0], "artifact_digest": owner[1]["artifactDigest"],
             "generation_id": owner[1]["etlReceipts"]["generationId"],
             "receipt_sha256": owner[1]["etlReceipts"]["sha256"]}
        )
    # Literal held fields also pass the exact subject/receipt join. Keep only
    # keys and checked digests so large text bodies are not duplicated in RAM.
    from spicy_regs.citation_sources import TEXT_SOURCES
    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.subject_catalog import descriptors
    sources = {}
    for table in sorted({spec.table for spec in TEXT_SOURCES.values()} & set(available)):
        if not selected(table):
            continue
        subject_policy = DatasetPolicy.from_descriptor(descriptors()[table])
        specs = [spec for spec in TEXT_SOURCES.values() if spec.table == table]
        keys = tuple(dict.fromkeys(name for spec in specs for name in spec.keys))
        text_fields = tuple(dict.fromkeys(spec.field for spec in specs))
        fields = (*keys, *text_fields)
        schema = pa.schema([subject_policy.subject_schema.field(name) for name in keys] +
                           [pa.field("sha256_" + name, pa.string()) for name in text_fields])

        def held_digests(row, dataset=table, names=fields, key_fields=keys, texts=text_fields,
                         native_names=set(subject_policy.subject_schema.names)):
            missing = set(names) - native_names
            # Congress capture text such as a reconstructed Record entry stays
            # in source_fields; native legal/body fields use their subject value.
            original = row.get("source_fields") if missing and dataset == "house_communications" else {}
            if missing and not isinstance(original, Mapping):
                raise ValueError(f"{dataset}: held text is missing its selected source receipt")
            if missing - set(original):
                raise ValueError(f"{dataset}: held text has no declared native or receipt field")
            values = {name: row.get(name) if name in native_names else original[name] for name in names}
            return ({name: values[name] for name in key_fields} |
                    {"sha256_" + name: "sha256:" + hashlib.sha256(values[name].encode("utf-8")).hexdigest()
                     if values[name] is not None else None for name in texts})

        sources[table] = adapter.restore_originals(table, schema, held_digests, prefix=SOURCE_DIGEST_PREFIX)
    record = {"tables": tables, "publication": pins, "sources": sources,
              "selected_sources": sorted(set(sources) | (set(tables) & set(SOURCE_TABLES.values())))}
    connection.execute(f"CREATE TABLE {INPUT_RECORD} (snapshot VARCHAR)")
    connection.execute(f"INSERT INTO {INPUT_RECORD} VALUES (?)", [json.dumps(record)])


def selected_citation_inputs(cursor):
    """Only a verified private binding can supply processing evidence to the tool."""
    import duckdb

    try:
        value = cursor.execute(f"SELECT snapshot FROM {INPUT_RECORD}").fetchone()
    except duckdb.CatalogException as error:
        raise ValueError("Citation resolution requires selected native ETL receipts") from error
    if value is None:
        raise ValueError("Citation resolution is missing its selected ETL receipt binding")
    return json.loads(value[0])
