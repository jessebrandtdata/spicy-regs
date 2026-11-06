"""Publish mutable citation fixture inputs through the real native receipt writer."""
from pathlib import Path
from tempfile import TemporaryDirectory

from spicy_regs.citation_receipts import INPUT_RECORD, PROCESSING_PREFIX, SOURCE_DIGEST_PREFIX, PROCESSING_TABLES, install_citation_inputs
from spicy_regs.etl_receipts import ReceiptContext, write_dataset
from spicy_regs.fec_receipt_adapter import ReceiptAdapter
from spicy_regs.govinfo_revisions import DATED_TABLES
from spicy_regs.legislative_documents import field_registry
from spicy_regs.legislative_receipts import mapped_record, policy

DIGEST = "sha256:" + "1" * 64
OTHER_DIGEST = "sha256:" + "2" * 64
SECOND_DIGEST = "sha256:" + "3" * 64
FIXTURE_REVISION = "2026-09-18T12:00:00Z"


def prepare_citation_inputs(connection):
    """Capture this test's current staging rows before invoking the citation tool.

    Staging tables remain editable for pagination and changed-input tests. The
    tool's private inputs are newly validated, immutable native subjects plus
    exact ETL receipts; production has no old-schema reader fallback.
    """
    available = [name for name, in connection.execute("SHOW TABLES").fetchall()]
    for name in available:
        if name == INPUT_RECORD or name.startswith((PROCESSING_PREFIX, SOURCE_DIGEST_PREFIX)):
            connection.execute(f'DROP TABLE "{name}"')
    registry = field_registry()
    context = ReceiptContext("citation-fixture", "fixture-attempt", "native-fixture", [
        {"source_id": "fixture-source", "sha256": "a" * 64}])
    native = {}
    with TemporaryDirectory(prefix="citation-fixture-") as directory:
        for table in (*PROCESSING_TABLES, "comments"):
            if table not in available:
                continue
            cursor = connection.execute(f'SELECT * FROM "{table}"')
            names = [column[0] for column in cursor.description]
            source = [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
            # These synthetic staging tables predate dated package keys. Give
            # them one explicit fixture revision; an explicitly supplied null
            # or invalid timestamp still reaches the normal refusal path.
            if table in DATED_TABLES and "last_modified" not in names:
                source = [row | {"last_modified": FIXTURE_REVISION} for row in source]
            if table == "comments":
                from spicy_regs.transforms.regulations_receipts import policy as regulations_policy
                from spicy_regs.transforms.regulations_shape import shape_record
                dataset_policy = regulations_policy(table)
                mapped = [shape_record(table, row) for row in source]
            else:
                dataset_policy = policy(table)
                columns = [field["name"] for field in registry[table]["fields"]]
                source = [{column: None if row.get(column) is None else str(row[column]) for column in columns}
                          for row in source]
                mapped = [mapped_record(table, row) for row in source]
            subject, receipts = write_dataset(
                [(row, context) for row in mapped], Path(directory) / table, dataset_policy)
            native[table] = {"subjects": [str(subject)] if subject else [],
                             "receipts": str(receipts), "generation_id": context.generation_id}
        adapter = ReceiptAdapter(connection, {"families": {}}, "", local_native=native)
        install_citation_inputs(connection, adapter, available)
    return connection
