"""SQL that reproduces the receipt writer's exact encoding and digests, a column at a time, and a validator built on it.

``etl_receipts.exact_json`` encodes one Python value. These build the DuckDB expression that yields the same text for a
column of an Arrow type, so a table's identities, versions and receipt ids come from one scan instead of one call per
row (2026-10-05: 26M comments took about 20 hours by row, and a million of them 82 seconds this way).

The row functions stay the reference. DuckDB spells one class of text differently: a control character with no short
JSON escape is written ``\\u001B`` where Python writes ``\\u001b``. Strings containing the unit separator used by bill
paths receive an exact spelling here; :func:`needs_reference_sql` leaves other unproven escapes to the row writer.

:func:`validate_bundle` is ``etl_receipts.validate_receipt_bundle`` over whole columns. SQL may only prove a row good:
a row it cannot prove is read again by the row reader and decided by the row functions themselves.
"""

from __future__ import annotations

import io
import json
import sqlite3
from dataclasses import dataclass
from contextvars import ContextVar
from hashlib import sha256
from itertools import batched
from os import getenv
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack, closing, contextmanager, nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.runtime_bounds import bounded_connection, checkpoint
from spicy_regs.etl_receipts import (
    RECEIPT_ROW_GROUP,
    RECEIPT_SCHEMA, open_receipt_writer,
    DatasetPolicy,
    ParquetInput,
    _bundle_policies,
    _load_receipts,
    _parquet,
    _rows,
    _subjects,
    _unpack,
    subject_identity,
    receipt_policies,
    validate_publisher_generation,
)

#: An upper-case hex letter in a control character's escape: the backslash run before ``u`` is odd, so it is an escape
#: and not a doubled literal backslash followed by the letters.
_UPPER_HEX_ESCAPE = r"(^|[^\\])(\\\\)*\\u00[01][A-F]"
#: A JSON string as ``json.loads`` reads one: no raw control character, and only the escapes JSON defines.
_STRING = r'"(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"'
#: The encoded values with nothing inside them. An integer is held to 18 digits, below where ``int`` refuses a text.
#: A UTC datetime ``datetime.fromisoformat`` certainly reads: years 1000-9999, days to the 28th, so no calendar date is invalid.
_DATETIME = (r'\["datetime","[1-9][0-9]{3}-(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])'
             r'T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{6})?\+00:00"\]')
_SCALAR = (r'\["null",null\]|\["bool",(?:true|false)\]|\["int",-?(?:0|[1-9][0-9]{0,17})\]|\["str",' + _STRING + r"\]|"
           + _DATETIME)
#: A list or a mapping whose members have each already been replaced by the ``chr(1)`` that stands for one value.
_NESTED = (
    r'\["list",\[(?:\x01(?:,\x01)*)?\]\]|\["dict",\[(?:\[' + _STRING + r",\x01\](?:,\[" + _STRING + r",\x01\])*)?\]\]"
)
#: One member of a mapping whose value is replaced; the key is taken only where its text is its JSON spelling.
_MEMBER = r'\["([^"\\\x00-\x1f]*)",\x01\]'
#: How many levels of list and mapping the proof follows. A deeper value is the row reader's to decide.
_DEPTH = 8
#: ``ReceiptContext``'s rule for a witness digest.
_WITNESS_SHA256 = r"(?:sha256:)?[0-9a-f]{64}"
#: What :func:`exact_json_sql` spells for a value it does not own. It holds an upper-case hex control escape, which
#: :func:`needs_reference_sql` matches, so a writer or validator that checks its encoded subject routes the row.
_ROUTE = '["route","\\u001B"]'
_BATCH = 2_000
#: Target logical Arrow bytes per opt-in INSERT; one larger row stays intact.
_INSERT_BYTES = 16 * 1024 * 1024


class BulkValidationError(RuntimeError):
    """Bulk execution failed; retrying the complete bundle by row is unsafe."""


class BulkInputError(BulkValidationError, ValueError):
    """The input data itself failed bulk execution, for example a date32 outside Python's date range.

    It is a ``ValueError`` as the row validator's own refusals are, so writers and the conversion command keep
    reporting invalid input as a refusal. Every other bulk failure (memory, storage, I/O) is not one.
    """


class NotBulkEligible(Exception):
    """SQL has no proven way to decide this bundle, so ``validate_receipt_bundle`` must."""


@dataclass(frozen=True)
class BulkLimits:
    """Explicit limits for one conversion; ordinary readers keep their defaults."""

    threads: int
    memory_limit: str
    require_bulk: bool = False


_limits: ContextVar[BulkLimits | None] = ContextVar("spicy_bulk_limits", default=None)


@contextmanager
def bulk_limits(*, threads: int, memory_limit: str, require_bulk: bool = False):
    if type(threads) is not int or threads < 1 or not isinstance(memory_limit, str) or not memory_limit.strip():
        raise ValueError("Bulk limits need positive threads and an explicit memory limit")
    token = _limits.set(BulkLimits(threads, memory_limit, require_bulk))
    try:
        yield
    finally:
        _limits.reset(token)


def bulk_failure(action: str, error: Exception) -> BulkValidationError:
    """``error`` as a failure that never retries by row: invalid input data, or an operational one."""
    invalid = isinstance(
        error, (ValueError, pa.ArrowTypeError, duckdb.DataError, duckdb.IntegrityError, duckdb.InvalidInputException)
    )
    return (BulkInputError if invalid else BulkValidationError)(f"{action}; no row fallback: {error}")


def mapper_table(rows, schema: pa.Schema, what: str) -> pa.Table:
    """``rows`` from the shared mapper as an Arrow table; one the schema cannot hold is the row reader's to decide."""
    try:
        return pa.Table.from_pylist(rows, schema=schema)
    except (ValueError, TypeError, OverflowError) as error:  # ArrowInvalid and ArrowTypeError; not memory or I/O
        raise NotBulkEligible(f"{what} does not fit its schema; the row reader decides") from error


def reference_fallback(error: NotBulkEligible, route: str = "admission") -> None:
    """Refuse a mapped route's wholesale fallback when its caller or runner requires bulk work.

    The environment applies in worker threads and subprocesses too. It does not
    change resource limits, unmapped eligibility, or sparse row decisions inside a bulk route.
    """
    required = getenv("SPICY_REGS_REQUIRE_BULK", "false")
    if required not in {"true", "false"}:
        raise ValueError("SPICY_REGS_REQUIRE_BULK must be 'true' or 'false'; reference fallback disabled") from error
    limits = _limits.get()
    if required == "true" or (limits is not None and limits.require_bulk):
        raise ValueError(f"Required bulk {route} failed; reference fallback disabled: {error}") from error


def log_row_fallback(route: str, error: NotBulkEligible, *, dataset: str, rows: int | None = None) -> None:
    """A bulk route handed ``dataset``'s work to the (slow) row path: refuse it where bulk is required, else warn.

    Every such route is visible: a bundle that silently validated by row held 15.9M lobbying rows at 2.8k rows/s.
    """
    reference_fallback(error, route)
    from loguru import logger

    logger.warning("{} route=row dataset={}{} reason={}", route, dataset, "" if rows is None else f" rows={rows}", error)


def log_row_eligibility(route: str, *, dataset: str, rows: int, reason: str) -> None:
    """Warn for a dataset with no bulk mapping; ``require_bulk`` governs mapped-route fallbacks only."""
    from loguru import logger

    logger.warning("{} route=row dataset={} rows={} reason={}", route, dataset, rows, reason)


@dataclass(frozen=True)
class _RetainedScan:
    """Custody-bound completed inserts; all remaining admission checks still run."""

    database: Path
    database_states: Mapping[Path, list[int]]
    input_states: Mapping[Path, list[int]]
    unproven_subjects: Path
    ordinal_state: list[int]
    subject_rows: int
    receipt_rows: int
    insert_sql_sha256: str


def _retained_source15_scan(evidence, subject_paths, receipt_paths, earlier, generation_id):
    """Qualify only the closed source15 inserts, without asserting admission."""
    from spicy_regs.local_data import file_signature

    path, expected_sha256, current_capture_pin = evidence
    if not isinstance(path, Path) or not path.is_absolute():
        raise ValueError("Retained scan evidence requires an absolute regular file")

    def read_pin(pin):
        member = Path(pin["path"])
        before = file_signature(member)
        if sha256(member.read_bytes()).hexdigest() != pin["sha256"]:
            raise ValueError("Retained scan evidence changed")
        value = json.loads(member.read_text())
        if file_signature(member) != before:
            raise ValueError("Retained scan evidence changed during verification")
        return value

    proof = read_pin({"path": str(path), "sha256": expected_sha256})
    if proof["format"] != "comments-retained-source15-scan/1":
        raise ValueError("Expected the explicit source15 scan recovery")
    fixed = {
        "terminal": "8d0747061c6be820ff2f9b20778416ab2ae14070854aefbe32dd7b55a486f719",
        "launch": "adb06786c772078ba5abac27f961d85c134f8d4c79f58af101d2d0edce24783c",
        "assessment": "3b24d33cee6ee3f05960b39846382d6c7d64e2a896aa830964cbc2010a2d8832",
        "compatibility": "274428e79632cad55e6d48611d3a8a6ea96267dc57dddbaee40d85e66e59f999",
    }
    if any(proof[key]["sha256"] != digest for key, digest in fixed.items()):
        raise ValueError("Retained scan must use the exact closed source15 evidence")
    terminal, launch, assessment = (read_pin(proof[key]) for key in ("terminal", "launch", "assessment"))
    compatibility = read_pin(proof["compatibility"])
    if launch["capturedInput"]["sha256"] != "9d2195a26c197da500cbf003a442febb0cb9806d91301b2cd2676d7e67c06b1d":
        raise ValueError("Retained source15 capture descriptor differs")
    old_capture = read_pin(launch["capturedInput"])
    current_capture = read_pin(current_capture_pin)
    if dict(old_capture, validationRevision=current_capture["validationRevision"]) != current_capture:
        raise ValueError("Retained source15 capture differs from the current selected descriptor")
    revision = "3da6088cbad6e89944047dfb6d9bb4c3cf3a26a3"
    expected_inputs = {Path(member["path"]) for member in old_capture["members"].values()}
    if (set((*subject_paths, *receipt_paths)) != expected_inputs
            or old_capture["metadata"]["generation_id"] != generation_id
            or old_capture["metadata"]["policy"] != earlier.descriptor()
            or old_capture["validationRevision"] != revision
            or launch["source"]["checkout"] != revision or launch["source"]["main"] != revision
            or launch["source"]["uncommitted"] or launch["source"]["untracked"]
            or launch["helperSha256"] != "0997b0ffe84ed7e7574b783c6a2f21c174a2cdb117bbe03517c32e509f73a5f2"
            or launch["bulkSha256"] != "90ac01c016325c41b9947bd11982d0d184732b27d61f32d961e8e7b543b265a6"
            or terminal["freshLaunchProofSha256"] != proof["launch"]["sha256"]
            or terminal["status"] != "FAILED_PRIVATE_COMMENTS_PREPARATION_SUPERVISION"
            or terminal["exitCode"] != -15 or terminal["resourceBreach"] != "host disk reserve"
            or terminal["remainingLiveOwnedProcesses"] != [] or any(terminal["ownedGroups"].values())
            or terminal["processClosureError"] or terminal["supervisionError"]
            or terminal["cleanupInspectionErrors"] or terminal["published"] is not False
            or assessment["status"] != "STOPPED_SOURCE15_METADATA_AND_RECOVERY_ASSESSMENT"
            or assessment["databaseSignaturesBefore"] != assessment["databaseSignaturesAfter"]
            or compatibility["source15"] != revision
            or compatibility["source15BulkSha256"] != launch["bulkSha256"]
            or compatibility["insertSqlSha256"] != proof["insertSqlSha256"]
            or compatibility["policy"] != earlier.descriptor()
            or proof["insertSqlSha256"] != insert_sql_sha256(earlier)):
        raise ValueError("Source15 scan, closure, inputs or compatible SQL differ")
    database_states = {}
    for held in assessment["databaseSignaturesAfter"]:
        member = Path(held["path"])
        expected = [held[key] for key in ("device", "inode", "byteSize", "mtimeNs", "ctimeNs")]
        if file_signature(member) != expected:
            raise ValueError("Retained source15 database custody changed")
        database_states[member] = expected
    database = next(member for member in database_states if member.name == "work.duckdb")
    ordinals = database.parent / "unproven-subjects.parquet"
    held = next(value for value in assessment["files"].values() if value["path"] == str(ordinals))
    ordinal_state = [held[key] for key in ("device", "inode", "byteSize", "mtimeNs", "ctimeNs")]
    with pq.ParquetFile(ordinals) as source:
        reference_rows = source.metadata.num_rows
    if (file_signature(ordinals) != ordinal_state
            or reference_rows != 295
            or assessment["databaseReadOnlyMetadata"]["tables"] != ["receipts", "subjects"]
            or assessment["databaseReadOnlyMetadata"]["rowCountsFromStorageMetadata"]["receipts"]["sumSegmentRows"] != 26_418_079
            or assessment["databaseReadOnlyMetadata"]["rowCountsFromStorageMetadata"]["subjects"]["sumSegmentRows"] != 26_418_373):
        raise ValueError("Retained source15 completed inserts differ")
    inputs = {Path(old_capture["members"][name]["path"]):
              [state[key] for key in ("device", "inode", "byteSize", "mtimeNs", "ctimeNs")]
              for name, state in ((name, member["custody"]) for name, member in old_capture["members"].items())}
    return _RetainedScan(database, database_states, inputs, ordinals, ordinal_state,
                        26_418_078, 26_418_079, proof["insertSqlSha256"])


def insert_sql_sha256(policy):
    """Bind retained keys to the same receipt and subject insert expressions."""
    expressions = [_receipt_insert_sql({policy.dataset: policy}, False), _subject_insert_sql(policy, "n")]
    return sha256(json.dumps(expressions, ensure_ascii=False).encode()).hexdigest()


@contextmanager
def bulk_connection(directory: Path | None = None, *, threads: int = 4, memory_limit: str = "4GB"):
    """A private disk-backed DuckDB with bounded CPU, RAM and spill.

    Each operation owns its files. A second statement must never run while a
    result from this connection is still being consumed.
    """
    limits = _limits.get()
    if limits is not None:
        threads, memory_limit = limits.threads, limits.memory_limit
    with TemporaryDirectory(prefix="etl-bulk-", dir=directory) as temp:
        root = Path(temp)
        with closing(duckdb.connect(str(root / "work.duckdb"), config={
            "threads": threads, "memory_limit": memory_limit, "max_temp_directory_size": "32GB",
            "temp_directory": str(root / "spill"),
        })) as con, bounded_connection(con):
            # Timestamp spellings and parses must not depend on the host's zone.
            con.execute("SET TimeZone = 'UTC'")
            yield con, root


_GROUP = RECEIPT_ROW_GROUP
#: The table a :func:`write_split` query reads: the source's batch, with ``file_row_number`` as ``read_parquet`` gives it.
SOURCE_BATCH = "source_batch"


def with_insertions(kept: pa.Table, mask: list[bool], insertions: Mapping[int, dict | None]) -> pa.Table:
    """Merge the row functions' rows (at their batch positions; None leaves a row out) into the SQL-proven ``kept`` ones."""
    positions = [index for index, flag in enumerate(mask) if flag]
    replacements = [(index, row) for index, row in insertions.items() if row is not None]
    if not replacements:
        return kept
    added = pa.Table.from_pylist([row for _, row in replacements], schema=kept.schema)
    positions.extend(index for index, _ in replacements)
    order = sorted(range(len(positions)), key=positions.__getitem__)
    return pa.concat_tables([kept, added]).take(order)


def new_receipt_writer(path: Path) -> pq.ParquetWriter:
    """Open a receipt writer; its caller owns closing the shared member."""
    return open_receipt_writer(path)


def write_split(
    con: duckdb.DuckDBPyConnection,
    query: str,
    *,
    source: Path | pa.Table,
    batch_rows: int,
    expected_rows: int,
    subject_schema: pa.Schema,
    subject_path: Path,
    receipt_path: Path,
    row_attempt: Callable[[pa.Table, int, dict], tuple[dict | None, dict]],
    label: str,
    first_receipts: Sequence[Mapping] = (),
    last_receipts: Iterable[Mapping] = (),
    last_refusals: Iterable[Mapping] = (),
    refusals_last: bool = False,
    segments: Callable[[pa.Table], Iterable[pa.Table]] | None = None,
    isolate: str | None = None,
    subject_columns: Sequence[str] | None = None,
    receipt_writer: pq.ParquetWriter | None = None,
    subject_writer: pq.ParquetWriter | None = None,
) -> int:
    """Write a subject file and a receipt file from one SQL query over ``source``; return how many rows the row code decided.

    ``source`` is a Parquet file or an in-memory Arrow table (a caller that already holds the rows, sorted, need not
    write them out to be read back).

    ``query`` reads :data:`SOURCE_BATCH` (the source's rows with a ``file_row_number`` column, as ``read_parquet(...,
    file_row_number = true)`` gives them) and yields the subject columns, the receipt columns, ``_ordinal`` and
    ``_reference``, plus ``_accepted`` where a row can be proven receipted without a subject. The source is fed to
    DuckDB as ordered registered batches, because a parallel ``read_parquet`` stream was seen to emit a later row group
    first (the order relies on registering one table per query; the ordinal guard below stays). A row marked
    ``_reference`` is given to ``row_attempt(batch, index, source_row)``, the family's own row function, with the row as
    the source file holds it (never decoded from the query's encoded text, which spells a value SQL does not own as a
    marker), and merged back in place; with ``refusals_last`` its failure receipt goes after normal accepted/observed
    records, as the row writers order them. ``first_receipts`` open the receipt file and ``last_receipts`` follow the rows.
    ``last_refusals`` follows the routed refusals, retaining the order of separately rejected updates.
    ``receipt_writer`` borrows an already open family member; this call flushes its rows but does not close it.
    ``subject_writer`` likewise borrows an open subject member (``subject_path`` is then not written), so a caller
    that owns the member's layout (compression level, statistics, several sources into one member) writes it once.
    Each segment's subjects are one ``write_table`` call, so ``segments`` also bounds the subject row groups.
    ``subject_columns`` optionally names query aliases in subject-schema order when a native field name collides with
    a receipt field; aliases are renamed before routed subjects are inserted.
    ``segments`` may split a batch; a row whose boolean ``isolate`` column is true must be alone in its segment and is
    written as a receipt row group of its own, so a reader fetching a neighbour does not load it.

    No receipt is validated here: the generation's admission is the one validation. A DuckDB or Arrow failure raises
    :func:`bulk_failure`, never a row retry. Rows arriving out of order, or fewer than ``expected_rows``, raise
    ``RuntimeError``: a stream cut short leaves a bundle that is consistent with itself.
    """
    if subject_columns is not None and len(subject_columns) != len(subject_schema):
        raise ValueError("subject_columns must name one query column per subject field")
    selected_subjects = subject_schema.names if subject_columns is None else list(subject_columns)
    seen, routed, piece_start = 0, 0, 0
    piece: pa.RecordBatch | None = None
    pending = pa.Table.from_pylist(list(first_receipts), schema=RECEIPT_SCHEMA)
    with ExitStack() as stack:
        scratch = Path(stack.enter_context(TemporaryDirectory(dir=Path(receipt_path).parent, prefix=".split-")))
        subjects = subject_writer if subject_writer is not None else stack.enter_context(
            pq.ParquetWriter(subject_path, subject_schema, compression="zstd"))
        receipts = receipt_writer if receipt_writer is not None else stack.enter_context(new_receipt_writer(receipt_path))
        refused = stack.enter_context(open_receipt_writer(scratch / "refused.parquet"))

        def emit(table: pa.Table, final: bool = False) -> None:
            nonlocal pending
            pending = pa.concat_tables([pending, table])
            whole = pending.num_rows if final else pending.num_rows - pending.num_rows % _GROUP
            if whole:
                receipts.write_table(pending.slice(0, whole), row_group_size=_GROUP)
                pending = pending.slice(whole)

        def part(table: pa.Table) -> int:
            names = table.column_names
            flags = table["_reference"].to_pylist()
            proven = [not flag for flag in flags]
            accepted = table["_accepted"].to_pylist() if "_accepted" in names else proven
            written = table.filter(pa.array(proven)).select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA)
            kept = table.filter(pa.array(accepted)).select(selected_subjects).rename_columns(subject_schema.names).cast(subject_schema)
            decided_subjects, decided_receipts, refusals = {}, {}, []
            for index in (index for index, flag in enumerate(flags) if flag):
                assert piece is not None
                offset = table["_ordinal"][index].as_py() - piece_start
                subject, receipt = row_attempt(table, index, piece.slice(offset, 1).to_pylist()[0])
                decided_subjects[index] = subject
                if refusals_last and receipt["outcome"] not in {"accepted", "observed"}:
                    refusals.append(receipt)
                else:
                    decided_receipts[index] = receipt
            if refusals:
                refused.write_table(pa.Table.from_pylist(refusals, schema=RECEIPT_SCHEMA))
            if decided_subjects:
                written = with_insertions(written, proven, decided_receipts)
                kept = with_insertions(kept, accepted, decided_subjects)
            if kept.num_rows:
                subjects.write_table(kept)
            alone = isolate is not None and any(table[isolate].to_pylist())
            if alone:
                emit(RECEIPT_SCHEMA.empty_table(), final=True)
            emit(written, final=alone)
            return len(decided_subjects)

        try:
            with nullcontext(source) if isinstance(source, pa.Table) else pq.ParquetFile(source) as original:
                start = 0
                pieces = (original.to_batches(max_chunksize=batch_rows) if isinstance(original, pa.Table)
                          else original.iter_batches(batch_size=batch_rows))
                for piece in pieces:
                    piece_start = start
                    checkpoint()
                    piece.validate(full=True)
                    numbered = pa.Table.from_batches([piece]).append_column(
                        "file_row_number", pa.array(range(start, start + piece.num_rows), pa.int64()))
                    start += piece.num_rows
                    con.register(SOURCE_BATCH, numbered)
                    try:
                        for batch in con.execute(query).to_arrow_reader(batch_rows):
                            table = pa.Table.from_batches([batch])
                            table.validate(full=True)
                            ordinals = table["_ordinal"].to_pylist()
                            if ordinals != list(range(seen, seen + len(ordinals))):
                                raise RuntimeError(f"{label}: source rows did not arrive in file order")
                            seen += len(ordinals)
                            for each in (table,) if segments is None else segments(table):
                                routed += part(each)
                    finally:
                        con.unregister(SOURCE_BATCH)
            for receipt_batch in batched(last_receipts, _GROUP):
                emit(pa.Table.from_pylist(list(receipt_batch), schema=RECEIPT_SCHEMA))
            refused.close()
            with pq.ParquetFile(scratch / "refused.parquet") as late:
                if isolate is not None:
                    # The deferred file does not retain route flags. Preserve the
                    # isolation guarantee for every deferred failure on this route.
                    emit(RECEIPT_SCHEMA.empty_table(), final=True)
                    for batch in late.iter_batches(batch_size=1):
                        receipts.write_batch(batch)
                else:
                    for batch in late.iter_batches(batch_size=_GROUP):
                        emit(pa.Table.from_batches([batch]))
            for receipt_batch in batched(last_refusals, _GROUP):
                emit(pa.Table.from_pylist(list(receipt_batch), schema=RECEIPT_SCHEMA))
            emit(RECEIPT_SCHEMA.empty_table(), final=True)
        except (duckdb.Error, pa.ArrowException) as error:
            raise bulk_failure(f"Bulk {label} write failed", error) from error
    if seen != expected_rows:
        raise RuntimeError(f"{label}: read {seen} of the source's {expected_rows} rows")
    return routed


def _ordinal_file(con, query: str, parameters, path: Path) -> Iterator[int]:
    """Finish the SQL query before streaming its bounded ordinal batches."""
    con.execute(f"COPY ({query}) TO {_literal(str(path))} (FORMAT PARQUET)", parameters)
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=_BATCH):
            checkpoint()
            yield from batch.column(0).to_pylist()


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def exact_json_sql(expression: str, dtype: pa.DataType) -> str:
    """SQL text equal to ``exact_json`` of ``expression``'s value: the tagged encoding with keys in sorted order.

    Covers strings, integers, booleans, date32 in Python's date range, fixed-point decimal128 scales 0 through 6
    and decimal128(38,18) values whose absolute value is at least 0.000001,
    timestamp[us, UTC] in years 1000 through 9998, finite normal float32/float64 (zero included), and structs and
    lists of them whose DuckDB text equals Python's. A value this spells only as :data:`_ROUTE` (a timestamp outside
    those years, a subnormal, infinite or NaN float) matches :func:`needs_reference_sql`, so its row goes to the row
    functions, which decide it. Any other type refuses, so its table stays with the row writer until a test here proves
    its spelling.
    """
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        # Split the actual unit separator before JSON encoding. Replacing its
        # serialized escape would also change literal backslash-u001F text.
        # Encoding each piece keeps quotes/backslashes exact, including empty
        # pieces around adjacent separators; other controls remain unproven.
        part = "CAST(to_json(_control_part) AS VARCHAR)"
        joined = (f"array_to_string(list_transform(string_split({expression}, chr(31)), "
                  f"_control_part -> substr({part}, 2, length({part}) - 2)), {_literal(r'\u001f')})")
        text = (f"CASE WHEN contains({expression}, chr(31)) THEN '\"' || {joined} || '\"' "
                f"ELSE CAST(to_json({expression}) AS VARCHAR) END")
        body = f"'[\"str\",' || {text} || ']'"
    elif pa.types.is_integer(dtype):
        body = f"'[\"int\",' || CAST({expression} AS VARCHAR) || ']'"
    elif pa.types.is_boolean(dtype):
        body = f"'[\"bool\",' || CASE WHEN {expression} THEN 'true' ELSE 'false' END || ']'"
    elif pa.types.is_date32(dtype):
        # Arrow date32 and DuckDB DATE exceed Python date's range; those values
        # cannot be represented by the reference encoder and must refuse.
        # Within that range %Y is four digits, matching date.isoformat().
        text = f"strftime({expression}, '%Y-%m-%d')"
        body = (
            f"CASE WHEN isfinite({expression}) AND {expression} BETWEEN DATE '0001-01-01' AND DATE '9999-12-31' "
            f"THEN '[\"date\",' || CAST(to_json({text}) AS VARCHAR) || ']' "
            "ELSE error('date32 outside Python date range') END"
        )
    elif pa.types.is_decimal128(dtype) and (0 <= dtype.scale <= 6 or (dtype.precision, dtype.scale) == (38, 18)):
        # Arrow's Decimal and DuckDB both retain fixed-point spelling at these
        # scales; the additional scorecard type needs the value guard below.
        # Widen precision without changing scale: DECIMAL(1,1) otherwise writes
        # '.0' in DuckDB while Arrow's Decimal spells it '0.0'.
        text = f"CAST(CAST({expression} AS DECIMAL(38,{dtype.scale})) AS VARCHAR)"
        body = f"'[\"decimal\",' || CAST(to_json({text}) AS VARCHAR) || ']'"
        if dtype.scale == 18:
            # Smaller values, including zero, use Decimal's exponent spelling.
            # Keep those with the reference, including its context's E/e choice.
            body = (f"CASE WHEN abs({expression}) >= CAST('0.000001' AS DECIMAL(38,18)) "
                    f"THEN {body} ELSE {_literal(_ROUTE)} END")
    elif pa.types.is_timestamp(dtype) and dtype.unit == "us" and dtype.tz == "UTC":
        # ``datetime.isoformat()``: seconds always, microseconds only when nonzero, then the offset. Years outside
        # 1000-9998 are left to the row functions (``%Y`` is not four digits there, and Python's range ends at 9999).
        utc = f"timezone('UTC', {expression})"
        text = (f"strftime({utc}, '%Y-%m-%dT%H:%M:%S') || CASE WHEN strftime({utc}, '%f') = '000000' THEN '' "
                f"ELSE '.' || strftime({utc}, '%f') END || '+00:00'")
        body = (f"CASE WHEN isfinite({expression}) AND year({utc}) BETWEEN 1000 AND 9998 "
                f"THEN '[\"datetime\",' || CAST(to_json({text}) AS VARCHAR) || ']' ELSE {_literal(_ROUTE)} END")
    elif pa.types.is_float64(dtype) or pa.types.is_float32(dtype):
        body = _float_sql(expression)
    elif pa.types.is_struct(dtype):
        body = record_json_sql(((field.name, field.type) for field in dtype), qualifier=expression + ".")
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        item = exact_json_sql("item", dtype.value_type)
        body = f"'[\"list\",[' || array_to_string(list_transform({expression}, item -> {item}), ',') || ']]'"
    else:
        raise NotImplementedError(f"No proven SQL spelling for {dtype}")
    return f"CASE WHEN {expression} IS NULL THEN '[\"null\",null]' ELSE {body} END"


def _float_sql(expression: str) -> str:
    """``["float", float.hex()]`` for a finite double, from its binary exponent and 52-bit mantissa.

    ``log2`` may be off by one at a power of two, so the exponent is corrected against exact powers of two. A
    subnormal, infinite or NaN value is :data:`_ROUTE`: ``_pack`` refuses the last two and the first is not worth a
    spelling.
    """
    v = f"CAST({expression} AS DOUBLE)"
    normal = "(isfinite(_v) AND abs(_v) >= 2.2250738585072014e-308)"
    sign = "CASE WHEN starts_with(CAST(_v AS VARCHAR), '-') THEN '-' ELSE '' END"
    significand = (
        "lpad(lower(to_hex(CAST((_a / pow(2.0, _e) - 1.0) * 4503599627370496.0 AS BIGINT))), 13, '0')"
    )
    exponent = "CASE WHEN _e >= 0 THEN '+' ELSE '-' END || CAST(abs(_e) AS VARCHAR)"
    spelled = (
        f"list_transform([CASE WHEN {normal} THEN abs(_v) ELSE 1.0 END], _a -> "
        f"list_transform([CAST(floor(log2(_a)) AS INTEGER)], _g -> "
        "list_transform([CASE WHEN _a < pow(2.0, _g) THEN _g - 1 WHEN _a >= pow(2.0, _g + 1) THEN _g + 1 ELSE _g END], "
        f"_e -> {sign} || '0x1.' || {significand} || 'p' || {exponent})[1])[1])[1]"
    )
    return (
        f"list_transform([{v}], _v -> CASE WHEN _v = 0 THEN '[\"float\",\"' || {sign} || '0x0.0p+0\"]' "
        f"WHEN {normal} THEN '[\"float\",\"' || {spelled} || '\"]' ELSE {_literal(_ROUTE)} END)[1]"
    )


def _concat_sql(parts: list[str]) -> str:
    """Balance nonempty ordered fragments, retaining ``||`` and its NULL propagation."""
    while len(parts) > 1:
        parts = [f"({parts[i]} || {parts[i + 1]})" if i + 1 < len(parts) else parts[i]
                 for i in range(0, len(parts), 2)]
    return parts[0]


def record_json_sql(fields: Iterable[tuple[str, pa.DataType]], *, qualifier: str = "") -> str:
    """SQL text equal to ``exact_json`` of a mapping of the named columns (or of a struct's fields).

    DuckDB matches a name without regard to case, so two fields that differ only by case would both read the first
    one; they refuse like an unproven type.
    """
    fields = list(fields)
    if len({name.casefold() for name, _ in fields}) != len(fields):
        raise NotImplementedError("No proven SQL spelling for field names that differ only by case")
    if not fields:
        return "'[\"dict\",[]]'"
    parts = ["'[\"dict\",['"]
    for index, (name, dtype) in enumerate(sorted(fields, key=lambda field: field[0])):
        if index:
            parts.append("','")
        parts.extend((
            _literal("[" + json.dumps(name, ensure_ascii=False) + ","),
            exact_json_sql(qualifier + '"' + name.replace('"', '""') + '"', dtype),
            "']'",
        ))
    parts.append("']]'")
    return _concat_sql(parts)


def digest_sql(text: str) -> str:
    """SQL equal to ``etl_receipts._digest`` of a value whose exact encoding is ``text``."""
    return f"'sha256:' || sha256({text})"


def needs_reference_sql(text: str) -> str:
    """Whether an encoding built here differs from the reference's: it holds an upper-case hex control escape."""
    return f"regexp_matches({text}, {_literal(_UPPER_HEX_ESCAPE)})"


def identity_sql(policy: DatasetPolicy) -> tuple[str, str, str]:
    """SQL for ``subject_identity`` over a table of the policy's subject columns: record id, version, identity text."""
    types = {field.name: field.type for field in policy.subject_schema}
    pairs = " || ',' || ".join(
        _literal('["list",[["str",' + json.dumps(key, ensure_ascii=False) + "],")
        + " || "
        + exact_json_sql('"' + key.replace('"', '""') + '"', types[key])
        + " || ']]'"
        for key in policy.identity_fields
    )
    identity = "'[\"list\",[' || " + pairs + " || ']]'"
    head = _literal('["list",[["str",' + json.dumps(policy.dataset, ensure_ascii=False) + "],")
    subject = record_json_sql((field.name, field.type) for field in policy.subject_schema)
    return (
        digest_sql(f"{head} || {identity} || ']]'"),
        digest_sql(f"{head} || {subject} || ']]'"),
        identity,
    )


def receipt_json_sql() -> str:
    """SQL text of a receipt row without its id: what ``receipt_id`` is the digest of."""
    return record_json_sql((field.name, field.type) for field in RECEIPT_SCHEMA if field.name != "receipt_id")


def reduced_sql(text: str) -> str:
    """``text`` with each value of the tagged encoding, to ``_DEPTH`` levels, replaced by ``chr(1)``.

    Every replacement stands for a text ``_unpack(json.loads(...))`` returns from without raising, so a text that
    reduces to one ``chr(1)`` is one such value. That holds only for a text with no ``chr(1)`` of its own, which the
    caller checks. A float, decimal, date, bytes or other datetime value is never replaced: its row goes to the reference.
    """
    reduced = f"regexp_replace({text}, {_literal(_SCALAR)}, chr(1), 'g')"
    for _ in range(_DEPTH):
        reduced = f"regexp_replace({reduced}, {_literal(_NESTED)}, chr(1), 'g')"
    return reduced


def decodes_sql(text: str) -> str:
    """Whether SQL proves ``_unpack(json.loads(text))`` returns. False or NULL proves nothing either way."""
    return f"(NOT contains({text}, chr(1)) AND {reduced_sql(text)} = chr(1))"


def mapping_keys_sql(text: str) -> str:
    """The keys of the mapping ``text`` decodes to, or NULL where SQL cannot prove it decodes to one.

    A mapping is its members between a fixed head and tail, so only the members are reduced. A key spelled with an
    escape is not proven: its text would have to be decoded as ``json.loads`` decodes it.
    """
    members = reduced_sql(f"substr({text}, 10, length({text}) - 11)")
    return (
        f"CASE WHEN NOT contains({text}, chr(1)) AND length({text}) >= 11 AND starts_with({text}, '[\"dict\",[')"
        f" AND ends_with({text}, ']]') THEN list_transform([{members}], members -> CASE WHEN"
        f" regexp_full_match(members, {_literal(f'(?:{_MEMBER}(?:,{_MEMBER})*)?')})"
        f" THEN regexp_extract_all(members, {_literal(_MEMBER)}, 1) END)[1] END"
    )


def _names_sql(names: Iterable[str]) -> str:
    return "[" + ", ".join(_literal(name) for name in names) + "]::VARCHAR[]"


def _by_dataset_sql(policies: Mapping[str, DatasetPolicy], each: Callable[[DatasetPolicy], str], other: str) -> str:
    """``each`` policy's expression for a receipt of its dataset, and ``other`` for a dataset with no policy."""
    cases = " ".join(
        f"WHEN {_literal(name)} THEN CASE policy_version "
        + " ".join(f"WHEN {_literal(p.policy_version)} THEN {each(p)}" for p in receipt_policies(policy))
        + f" ELSE {other} END" for name, policy in policies.items()
    )
    return f"CASE dataset {cases} ELSE {other} END" if cases else other


def _receipt_insert_sql(policies: Mapping[str, DatasetPolicy], scoped: bool, *, retain_processing: bool = False) -> str:
    """Keep each receipt's join fields and whether SQL proved every check ``_load_receipts`` makes of one row.

    The conditions follow that function's order. One that is false or NULL leaves the row unproven; nothing here
    refuses a row. ``scoped`` leaves out the receipts ``select_receipts`` would not have copied.
    """
    witness = (
        "coalesce(w.source_id <> '', false) AND (coalesce(w.sha256 <> '', false) OR coalesce(w.body_version <> '', false))"
        f" AND (w.sha256 IS NULL OR regexp_full_match(w.sha256, {_literal(_WITNESS_SHA256)}))"
    )
    failed = _by_dataset_sql(policies, lambda policy: _names_sql(sorted(policy.input_fields)), "NULL")
    passed = _by_dataset_sql(policies, lambda policy: _names_sql(policy.receipt_fields), "NULL")
    proven = " AND ".join(
        (
            _by_dataset_sql(policies, lambda policy: f"policy_version = {_literal(policy.policy_version)}", "false"),
            f"receipt_id = {digest_sql('body')} AND NOT {needs_reference_sql('body')}",
            decodes_sql("diagnostic_json"),
            "generation_id <> '' AND attempt_id <> '' AND processor <> ''",
            f"len(witnesses) > 0 AND list_bool_and(list_transform(witnesses, w -> {witness}))",
            "CASE WHEN outcome = 'accepted' THEN subject_version IS NOT NULL AND record_id <> '' AND identity_json <> ''"
            f" AND {_by_dataset_sql(policies, lambda policy: 'false' if policy.receipt_only else 'true', 'false')}"
            " WHEN outcome IN ('rejected', 'refused', 'error', 'observed') THEN subject_version IS NULL ELSE false END",
            f"list_has_all(CASE WHEN outcome IN ('rejected', 'refused', 'error') THEN {failed} ELSE {passed} END, fields)",
        )
    )
    selected = f" WHERE dataset IN ({', '.join(map(_literal, policies)) or 'NULL'})" if scoped else ""
    return (
        "INSERT INTO receipts SELECT n, dataset, generation_id, receipt_id, outcome,"
        " CASE WHEN outcome = 'accepted' THEN record_id END, CASE WHEN outcome = 'accepted' THEN subject_version END,"
        f" CASE WHEN outcome = 'accepted' THEN identity_json END, ({proven}) IS NOT TRUE"
        + (", processing_json, diagnostic_json" if retain_processing else "") + " FROM (SELECT *,"
        f" {receipt_json_sql()} AS body, {mapping_keys_sql('processing_json')} AS fields FROM source{selected})"
    )


def _subject_insert_sql(policy: DatasetPolicy, number: str) -> str:
    """Keep each subject's identity as ``subject_identity`` gives it, and whether the row functions must give it instead.

    They must where the encoding holds an escape SQL spells differently, and where an identity field is NULL without
    leave, which ``subject_identity`` refuses.
    """
    record_id, version, identity = identity_sql(policy)
    return (
        f'INSERT INTO subjects SELECT "{number}", {_literal(policy.dataset)}, {record_id}, {version}, {identity},'
        f" ({_subject_reference_sql(policy)}) IS NOT FALSE FROM source"
    )


def _subject_reference_sql(policy: DatasetPolicy) -> str:
    """Subjects whose exact encoding or null validation belongs to the row reference."""
    subject = record_json_sql((field.name, field.type) for field in policy.subject_schema)
    missing = " OR ".join(
        '"' + key.replace('"', '""') + '" IS NULL'
        for key in dict.fromkeys([*(field.name for field in policy.subject_schema if not field.nullable),
                                  *(key for key in policy.identity_fields if key not in policy.nullable_identity_fields)])
    )
    return f"{needs_reference_sql(subject)} OR {missing or 'false'}"


def _numbered(
    parquet: pq.ParquetFile, start: int, number: str, wanted: frozenset[str] | None, read: list[int]
) -> pa.RecordBatchReader:
    """The file's rows as the row reader decodes them, with each row's number from ``start`` in a column ``number``.

    Every batch is validated first: DuckDB hashes an Arrow string without checking that it is UTF-8, where the row
    reader refuses to decode one that is not. ``wanted`` names the datasets a scoped read keeps; a batch holding none
    of them is still decoded and validated, as ``select_receipts`` would have read it, and then left out. ``read``
    counts the rows decoded and the rows SQL is to keep, so the caller can hold DuckDB to both.
    """
    schema = parquet.schema_arrow.append(pa.field(number, pa.int64()))

    def batches() -> Iterator[pa.RecordBatch]:
        first = start
        for batch in parquet.iter_batches(batch_size=_BATCH):
            checkpoint()
            batch.validate(full=True)
            last = first + batch.num_rows
            kept = batch.num_rows
            if wanted is not None:
                held = batch.column("dataset").value_counts().to_pylist()
                kept = sum(pair["counts"] for pair in held if pair["values"] in wanted)
            read[0] += batch.num_rows
            read[1] += kept
            if kept:
                yield pa.RecordBatch.from_arrays([*batch.columns, pa.arange(first, last)], schema=schema)
            first = last

    return pa.RecordBatchReader.from_batches(schema, batches())


Span = tuple[int, int, ParquetInput]


def _insert_batches(batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
    """Ordered zero-copy slices; backing row-group buffers and one wide row can exceed the target."""
    pending = [batch]
    while pending:
        piece = pending.pop()
        if piece.num_rows > 1 and piece.nbytes > _INSERT_BYTES:
            middle = piece.num_rows // 2
            pending.extend((piece.slice(middle), piece.slice(0, middle)))
        else:
            yield piece


def _scan(
    con: duckdb.DuckDBPyConnection,
    paths: Iterable[ParquetInput],
    start: int,
    insert: str,
    *,
    number: str = "n",
    wanted: frozenset[str] | None = None,
    bounded_insert: bool = False,
) -> list[Span]:
    """Insert each file's rows with global ordinals and retain its row range and input.

    A stream that ends early loses rows without an error, so the rows decoded and the rows inserted are both counted.
    ``bounded_insert`` finishes each small INSERT before the next slice; decoding,
    validation and global ordinals still come from the same numbered reader.
    """
    spans = []
    for path in paths:
        with _parquet(path) as parquet:
            read = [0, 0]
            with closing(_numbered(parquet, start, number, wanted, read)) as numbered:
                sources = (
                    (pa.Table.from_batches([piece]) for batch in numbered for piece in _insert_batches(batch))
                    if bounded_insert else (numbered,)
                )
                inserted = 0
                for source in sources:
                    checkpoint()
                    con.register("source", source)
                    [(added,)] = con.execute(insert).fetchall()
                    con.unregister("source")
                    inserted += added
            if read != [parquet.metadata.num_rows, inserted]:
                raise BulkValidationError("DuckDB did not take every row the reader decoded; no row fallback")
            spans.append((start, start + parquet.metadata.num_rows, path))
            start += parquet.metadata.num_rows
    return spans


def _readable(
    paths: Sequence[ParquetInput], schema: pa.Schema, differs: str
) -> tuple[Sequence[ParquetInput], Exception | None, bool]:
    """The inputs before the first the row validator stops at for its schema, and what it raises there."""
    for index, path in enumerate(paths):
        try:
            with _parquet(path) as parquet:
                if parquet.schema_arrow.equals(schema):
                    continue
        except Exception as error:
            return paths[:index], error, False
        return paths[:index], ValueError(differs), True
    return paths, None, False


def _held(spans: Iterable[Span], numbers: Iterable[int]) -> Iterator[tuple[list[int], pa.Table]]:
    """The rows with these ascending numbers, read again as the row reader decodes them, a row group at a time.

    A row group is read once however many of its rows are asked for, and only when one is, so this costs one row
    group's decode for each row at worst and the row reader's own pass at most.
    """
    numbers = iter(numbers)
    wanted = next(numbers, None)
    for first, stop, path in spans:
        if wanted is None or wanted >= stop:
            continue
        with _parquet(path) as parquet:
            for group in range(parquet.num_row_groups):
                stop_group = first + parquet.metadata.row_group(group).num_rows
                if wanted is not None and wanted < stop_group:
                    for batch in parquet.iter_batches(batch_size=_BATCH, row_groups=[group]):
                        last = first + batch.num_rows
                        picked = []
                        while wanted is not None and wanted < last:
                            picked.append(wanted)
                            wanted = next(numbers, None)
                        if picked:
                            yield picked, pa.Table.from_batches([batch]).take([number - first for number in picked])
                        first = last
                first = stop_group


def _member(table: pa.Table) -> Callable[[], io.BytesIO]:
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return lambda: io.BytesIO(sink.getvalue())


def _repeated_sql(key: str, where: str) -> str:
    """The first row that repeats an earlier row's ``key``: where the row loader's unique index refuses an insert."""
    return (
        f"SELECT min(n) FROM (SELECT n, row_number() OVER (PARTITION BY {key} ORDER BY n) AS turn FROM receipts"
        f" WHERE {where} AND ({key}) IN (SELECT {key} FROM receipts WHERE {where} GROUP BY ALL HAVING count(*) > 1))"
        " WHERE turn > 1"
    )


def _check_receipts(
    con: duckdb.DuckDBPyConnection,
    temp: Path,
    receipt_paths: Sequence[ParquetInput],
    policies: Mapping[str, DatasetPolicy],
    generation_id: str | None,
    scoped: bool,
    *,
    retain_processing: bool = False,
    bounded_insert: bool = False,
) -> None:
    """``_load_receipts`` over whole columns: raise what it would at the first row, or schema, it refuses."""
    validate_publisher_generation(generation_id)
    paths, stopped, schema_mismatch = _readable(receipt_paths, RECEIPT_SCHEMA, "Receipt schema differs from the shared schema")
    if scoped and stopped is not None:
        if schema_mismatch:
            raise NotBulkEligible("select_receipts decides a receipt file that is not the shared schema")
        raise bulk_failure("Bulk receipt input could not be read", stopped) from stopped
    con.execute(
        "CREATE TABLE receipts (n BIGINT, dataset VARCHAR, generation_id VARCHAR, receipt_id VARCHAR, outcome VARCHAR,"
        " record_id VARCHAR, subject_version VARCHAR, identity_json VARCHAR, unproven BOOLEAN"
        + (", processing_json VARCHAR, diagnostic_json VARCHAR" if retain_processing else "") + ")"
    )
    wanted = frozenset(policies) if scoped else None
    spans = _scan(con, paths, 0, _receipt_insert_sql(policies, scoped, retain_processing=retain_processing),
                  wanted=wanted, bounded_insert=bounded_insert)

    repeats = (
        con.execute(_repeated_sql("receipt_id", "true")).fetchall()[0][0],
        con.execute(_repeated_sql("dataset, record_id", "outcome = 'accepted' AND record_id IS NOT NULL")).fetchall()[0][0],
    )
    # No row after the first repeated receipt can change the row loader's refusal.
    last = min((number for number in repeats if number is not None), default=None)
    # Read to the end before anything else runs on this database: a second statement would end the stream.
    unproven = "SELECT n FROM receipts WHERE unproven AND n <= coalesce(?, n) ORDER BY n"
    numbers = _ordinal_file(con, unproven, [last], temp / "unproven-receipts.parquet")
    with closing(sqlite3.connect(str(temp / "reference.db"))) as reference:
        _load_receipts(
            reference,
            (_member(table) for _, table in _held(spans, numbers)),
            policies,
            generation_id,
            retain_processing=False,
        )
    if last is not None:
        raise ValueError("Duplicate or ambiguous receipt join")
    if stopped is not None:
        raise stopped


def _check_subjects(
    con: duckdb.DuckDBPyConnection, subjects: Mapping[str, Sequence[ParquetInput]], policies: Mapping[str, DatasetPolicy], temp: Path,
    *, bounded_insert: bool = False,
) -> None:
    """``_joined_subjects`` over whole columns: raise what it would at the first subject, or schema, it refuses."""
    con.execute(
        "CREATE TABLE subjects (n BIGINT, dataset VARCHAR, record_id VARCHAR, version VARCHAR, identity VARCHAR,"
        " unproven BOOLEAN)"
    )
    start, stopped, before = 0, None, None
    for dataset, paths in subjects.items():
        policy = policies[dataset]
        if policy.receipt_only and paths:
            stopped = ValueError("Processing-only dataset cannot publish a subject table")
            break
        if not paths:
            continue
        number = "n"
        while number in {name.casefold() for name in policy.subject_schema.names}:
            number += "_"
        paths, stopped, _ = _readable(paths, policy.subject_schema, f"Subject schema differs from policy: {dataset}")
        spans = _scan(con, paths, start, _subject_insert_sql(policy, number), number=number,
                      bounded_insert=bounded_insert)
        start = spans[-1][1] if spans else start
        unproven = "SELECT n FROM subjects WHERE unproven AND dataset = ? ORDER BY n"
        numbers = _ordinal_file(con, unproven, [dataset], temp / "unproven-subjects.parquet")
        for picked, table in _held(spans, numbers):
            given = []
            for held, raw in zip(picked, table.to_pylist()):
                try:
                    try:
                        subject = _subjects(policy, [raw])[0]
                    except ValueError as error:
                        raise NotBulkEligible("The row normalizer must decide subject validation order") from error
                    given.append((held, dataset, *subject_identity(policy, subject), False))
                except ValueError as error:
                    # The row validator raises this only if every subject before it has its receipt.
                    stopped, before = error, held
                    break
            if given:
                con.executemany("INSERT INTO subjects VALUES (?, ?, ?, ?, ?, ?)", given)
            if before is not None:
                break
        if stopped is not None:
            break
    _check_subject_matches(con, stopped=stopped, before=before)


def _check_subject_matches(con, *, stopped=None, before=None):
    """Finish the wide join once, then check and replay its narrow ordinal map."""
    joined = (
        "FROM (SELECT * FROM subjects WHERE NOT unproven AND n < coalesce(?, n + 1)) AS s"
        " LEFT JOIN (SELECT * FROM receipts WHERE outcome = 'accepted') AS r ON s.dataset = r.dataset"
        " AND s.record_id = r.record_id AND s.version = r.subject_version AND s.identity = r.identity_json"
    )
    con.execute(f"CREATE TABLE matched_subject_receipts AS SELECT s.n, s.dataset, r.n AS receipt {joined}", [before])
    # Sorting after the join releases its wide key state. Ordered row groups let
    # replay select a source batch without rescanning the complete map each time.
    con.execute("CREATE TABLE subject_receipts AS SELECT * FROM matched_subject_receipts ORDER BY n")
    con.execute("DROP TABLE matched_subject_receipts")
    [(total, matched, used)] = con.execute(
        "SELECT count(*), count(receipt), count(DISTINCT receipt) FROM subject_receipts"
    ).fetchall()
    if not total == matched == used:
        # The first subject with no accepted receipt, or whose receipt an earlier subject already took.
        [(dataset,)] = con.execute(
            "SELECT arg_min(dataset, n) FROM (SELECT n, dataset, receipt,"
            " row_number() OVER (PARTITION BY receipt ORDER BY n) AS turn FROM subject_receipts)"
            " WHERE receipt IS NULL OR turn > 1",
        ).fetchall()
        raise ValueError(f"Missing, ambiguous or reused subject receipt: {dataset}")
    if stopped is not None:
        raise stopped
    if [(used,)] != con.execute("SELECT count(*) FROM receipts WHERE outcome = 'accepted'").fetchall():
        raise ValueError("Accepted receipt has no matching subject")


def _nested_nullable(dtype: pa.DataType) -> bool:
    """Whether nested nulls need no extra validation beyond Arrow decoding."""
    if pa.types.is_struct(dtype):
        return all(field.nullable and _nested_nullable(field.type) for field in dtype)
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return dtype.value_field.nullable and _nested_nullable(dtype.value_type)
    return True


def _check_retained_scan(con, scan, subjects, receipt_paths, policy, generation_id):
    """Reuse exact closed scan tables read-only, then complete admission here."""
    from spicy_regs.local_data import file_signature

    def check_custody():
        for path, expected in {**scan.database_states, **scan.input_states,
                               scan.unproven_subjects: scan.ordinal_state}.items():
            if file_signature(path) != expected:
                raise ValueError("Retained scan custody changed")

    if (not isinstance(scan, _RetainedScan) or policy.receipt_only
            or len(subjects[policy.dataset]) != 1 or len(receipt_paths) != 1
            or set(scan.input_states) != set((*subjects[policy.dataset], *receipt_paths))
            or set(scan.database_states) != {scan.database, Path(str(scan.database) + ".wal")}
            or scan.insert_sql_sha256 != insert_sql_sha256(policy)):
        raise ValueError("Retained scan differs from the selected reader inputs or SQL")
    check_custody()
    validate_publisher_generation(generation_id)
    paths, stopped, _ = _readable(receipt_paths, RECEIPT_SCHEMA, "Receipt schema differs from the shared schema")
    if stopped is not None:
        raise stopped
    subject_paths, stopped, _ = _readable(subjects[policy.dataset], policy.subject_schema,
                                      f"Subject schema differs from policy: {policy.dataset}")
    if stopped is not None:
        raise stopped
    with _parquet(paths[0]) as source:
        if source.metadata.num_rows != scan.receipt_rows:
            raise ValueError("Retained receipt population differs")
    with _parquet(subject_paths[0]) as source:
        if source.metadata.num_rows != scan.subject_rows:
            raise ValueError("Retained subject population differs")
    con.execute(f"ATTACH {_literal(str(scan.database))} AS retained_scan (READ_ONLY)")
    con.execute("CREATE VIEW receipts AS SELECT * FROM retained_scan.receipts")
    con.execute("CREATE VIEW subjects AS SELECT * FROM retained_scan.subjects")
    with pq.ParquetFile(scan.unproven_subjects) as source:
        if source.schema_arrow.names != ["n"]:
            raise ValueError("Retained reference ordinals differ")
        numbers = source.read().column("n").to_pylist()
    if numbers != sorted(set(numbers)) or any(type(n) is not int or not 0 <= n < scan.subject_rows for n in numbers):
        raise ValueError("Retained reference ordinals differ")
    [(rows, unique, first, last, unproven)] = con.execute(
        "SELECT count(*), count(DISTINCT n), min(n), max(n), count(*) FILTER (WHERE unproven) FROM receipts"
    ).fetchall()
    if (rows, unique, first, last, unproven) != (scan.receipt_rows, scan.receipt_rows, 0, scan.receipt_rows - 1, 0):
        raise ValueError("Retained receipt scan is incomplete or requires the reference validator")
    for key, where in (("receipt_id", "true"),
                       ("dataset, record_id", "outcome = 'accepted' AND record_id IS NOT NULL")):
        if con.execute(_repeated_sql(key, where)).fetchall() != [(None,)]:
            raise ValueError("Duplicate or ambiguous receipt join")
    [(rows, unique, first, last)] = con.execute(
        "SELECT count(*), count(DISTINCT n), min(n), max(n) FROM subjects"
    ).fetchall()
    if (rows, unique, first, last) != (scan.subject_rows + len(numbers), scan.subject_rows, 0, scan.subject_rows - 1):
        raise ValueError("Retained subject scan or reference corrections are incomplete")
    if con.execute("SELECT n FROM subjects WHERE unproven ORDER BY n").fetchall() != [(n,) for n in numbers]:
        raise ValueError("Retained subject reference ordinals differ")
    expected = []
    for picked, table in _held([(0, scan.subject_rows, subject_paths[0])], numbers):
        for n, raw in zip(picked, table.to_pylist()):
            subject = _subjects(policy, [raw])[0]
            expected.append((n, policy.dataset, *subject_identity(policy, subject), False))
    actual = con.execute("SELECT * FROM subjects WHERE NOT unproven AND n IN (SELECT unnest(?)) ORDER BY n",
                         [numbers]).fetchall()
    if actual != expected:
        raise ValueError("Retained subject reference corrections differ")
    check_custody()
    _check_subject_matches(con)
    check_custody()


def validate_bundle(
    subjects: Mapping[str, Sequence[ParquetInput]],
    receipt_paths: Sequence[ParquetInput],
    policies: Sequence[DatasetPolicy],
    *,
    generation_id: str | None = None,
    scoped: bool = False,
) -> None:
    """``validate_receipt_bundle`` in bulk: accept and refuse the same bundles, raising what it raises.

    Each file is read once, through the row reader's own Parquet decoder, and each row is encoded, hashed and checked
    once in DuckDB. A row SQL cannot prove (an escape it spells differently, a value type it does not follow, anything
    malformed) is read again and given to ``_load_receipts`` or ``subject_identity``, so every refusal of one row is
    the row code's own. The joins the row validator makes through a SQLite index are set operations over a DuckDB
    file in a temporary directory, which spills there under memory pressure.

    Raises :class:`NotBulkEligible` only for unsupported semantics. DuckDB execution failures raise
    :class:`BulkValidationError` with their original cause and never request a full row retry.

    ``scoped`` ignores receipts of datasets with no policy here. Selected receipts keep source-file order.
    With multiple policies this can report a different first fault than dataset-grouped row selection; acceptance
    and refusal are unchanged. Single-policy diagnostics retain row-reader ordering.
    """
    with _validated_bundle(subjects, receipt_paths, policies, generation_id=generation_id, scoped=scoped):
        pass


@contextmanager
def _validated_bundle(subjects, receipt_paths, policies, *, generation_id, scoped=False, retain_processing=False,
                      threads=4, memory_limit="4GB", bounded_insert=False, retained_scan=None):
    """Keep the maintained admission tables alive for replay only after complete admission."""
    # Explicit conversion limits bound encoding work as well as DuckDB storage.
    # Reuse the same byte-sized inserts for writer validation and later replay.
    bounded_insert = bounded_insert or _limits.get() is not None
    registered = _bundle_policies(subjects, receipt_paths, policies)
    try:
        for policy in registered.values():
            if not policy.receipt_only and subjects[policy.dataset]:
                if not all(_nested_nullable(field.type) for field in policy.subject_schema):
                    raise NotBulkEligible("Nested nonnullable fields require the row normalizer")
                _subject_insert_sql(policy, "n")
    except NotImplementedError as error:
        raise NotBulkEligible(str(error)) from error
    with bulk_connection(threads=threads, memory_limit=memory_limit) as (con, temp):
        try:
            con.execute("SET preserve_insertion_order = false")
            if retained_scan is None:
                _check_receipts(con, Path(temp), receipt_paths, registered, generation_id, scoped,
                                retain_processing=retain_processing, bounded_insert=bounded_insert)
                _check_subjects(con, subjects, registered, temp, bounded_insert=bounded_insert)
            else:
                if len(registered) != 1 or scoped or retain_processing or not bounded_insert:
                    raise ValueError("Retained scans require one ordinal-processing selection")
                held_policy = next(iter(registered.values()))
                retained_scan = _retained_source15_scan(retained_scan, subjects[held_policy.dataset],
                                                        receipt_paths, held_policy, generation_id)
                _check_retained_scan(con, retained_scan, subjects, receipt_paths, held_policy, generation_id)
        except duckdb.Error as error:
            raise bulk_failure("Bulk receipt validation failed", error) from error
        try:
            yield con
        finally:
            if retained_scan is not None:
                from spicy_regs.local_data import file_signature
                if any(file_signature(path) != state for path, state in retained_scan.database_states.items()):
                    raise ValueError("Retained scan database custody changed during replay")


def read_with_receipts(subject_paths, receipt_paths, policy, *, generation_id, processing_by_ordinal=False):
    """Public reads always perform complete admission of the original inputs."""
    yield from _read_with_receipts(subject_paths, receipt_paths, policy, generation_id=generation_id,
                                  processing_by_ordinal=processing_by_ordinal)


def _read_promotion_rows(subject_paths, receipt_paths, policy, *, generation_id, evidence=None, include_prior=False):
    """Helper-only replay, optionally carrying the admitted receipt for inheritance."""
    yield from _read_with_receipts(subject_paths, receipt_paths, policy, generation_id=generation_id,
                                  processing_by_ordinal=True, retained_scan=evidence, include_prior=include_prior)


def _read_with_receipts(subject_paths, receipt_paths, policy, *, generation_id, processing_by_ordinal=False,
                        retained_scan=None, include_prior=False):
    """Replay completely admitted subjects in source order, with the exact stored processing JSON.

    Admission uses the same SQL checks and row-reference fallbacks as validate_bundle.
    Replay never falls back after yielding: any truncated stream or changed input fails
    the read, so atomic processing writers cannot accept an incomplete population.
    ``processing_by_ordinal`` keeps only admitted keys/ordinals in DuckDB and
    gathers processing from guarded local receipts by bounded source row groups.
    Groups can be reread across batches; this trades those reads for payload storage.
    """
    if not receipt_paths:
        raise NotBulkEligible("The row reader decides absent receipt inputs")
    if type(processing_by_ordinal) is not bool:
        raise ValueError("processing_by_ordinal must be boolean")
    if retained_scan is not None and not processing_by_ordinal:
        raise ValueError("Retained scans require guarded ordinal processing")
    if include_prior and not processing_by_ordinal:
        raise ValueError("Promotion receipt replay requires guarded ordinals")
    states = {}
    if processing_by_ordinal:
        from spicy_regs.local_data import file_signature
        if any(not isinstance(path, Path) for path in (*subject_paths, *receipt_paths)):
            raise NotBulkEligible("Ordinal processing replay requires regular local Path inputs")
        subject_paths = tuple(path.absolute() for path in subject_paths)
        receipt_paths = tuple(path.absolute() for path in receipt_paths)
        states = {path: file_signature(path) for path in (*subject_paths, *receipt_paths)}

    def check_sources():
        if not states:
            return
        checkpoint()
        if any(file_signature(path) != state for path, state in states.items()):
            raise ValueError("Ordinal processing source changed after selection")

    with _validated_bundle(
        {policy.dataset: subject_paths}, receipt_paths, [policy],
        generation_id=generation_id, retain_processing=not processing_by_ordinal,
        # Finish small INSERTs for wide receipts within the selected reader's budgets.
        threads=1 if processing_by_ordinal else 4,
        memory_limit="8GB" if processing_by_ordinal else "4GB",
        bounded_insert=processing_by_ordinal,
        retained_scan=retained_scan,
    ) as con, ExitStack() as files:
        check_sources()
        processing_rows = None
        if processing_by_ordinal:
            from spicy_regs.receipt_history import _take_prior_rows
            priors, file_ends, group_ends, offset = [], [], [], 0
            for path in receipt_paths:
                parquet = files.enter_context(_parquet(path))
                priors.append(parquet)
                ends, total = [], 0
                for group in range(parquet.metadata.num_row_groups):
                    total += parquet.metadata.row_group(group).num_rows
                    ends.append(total)
                group_ends.append(ends)
                offset += parquet.metadata.num_rows
                file_ends.append(offset)
            schema = RECEIPT_SCHEMA if include_prior else pa.schema([RECEIPT_SCHEMA.field("processing_json")])

            def processing_rows(ordinals):
                check_sources()
                table, positions = _take_prior_rows(priors, file_ends, group_ends, ordinals, schema)
                if positions != list(range(len(ordinals))):
                    raise ValueError("Ordinal processing replay omitted a selected receipt")
                values = table.to_pylist() if include_prior else table.column("processing_json").to_pylist()
                check_sources()
                return values

        check_sources()
        yield from _replay_admitted_subjects(con, subject_paths, policy, processing_rows=processing_rows,
                                           include_prior=include_prior)
        check_sources()


def _replay_admitted_subjects(con, subject_paths, policy, *, start=0, processing_rows=None, include_prior=False):
    """Shared replay over already completely admitted state; no second admission."""
    if not subject_paths:
        return
    number = "n"
    while number in {field.name.casefold() for field in policy.subject_schema}:
        number += "_"
    fields = ["\"" + field.name.replace('"', '""') + "\"" for field in policy.subject_schema]
    subject_sql = "struct_pack(" + ", ".join(f"{name} := source.{name}" for name in fields) + ")"
    record_id, version, identity = identity_sql(policy)
    aliases = []
    occupied = {field.name.casefold() for field in policy.subject_schema} | {number}
    for name in ("_replay_id", "_replay_version", "_replay_identity", "_replay_reference"):
        while name in occupied:
            name += "_"
        aliases.append(name)
        occupied.add(name)
    key_name, version_name, identity_name, reference_name = aliases
    same = (f'CASE WHEN source."{reference_name}" IS NOT FALSE THEN NULL ELSE'
            f' source."{key_name}" = s.record_id AND source."{version_name}" = s.version'
            f' AND source."{identity_name}" = s.identity END')

    def replay(first, stop):
        count = 0
        processing = "m.receipt" if processing_rows is not None else "r.processing_json"
        query = (
            f'SELECT {subject_sql}, {processing}, {same}, s.record_id, s.version, s.identity'
            f' FROM (SELECT *, {record_id} AS "{key_name}", {version} AS "{version_name}",'
            f' {identity} AS "{identity_name}", {_subject_reference_sql(policy)} AS "{reference_name}"'
            f' FROM source) source JOIN (SELECT * FROM subjects WHERE NOT unproven AND n >= ? AND n < ?) s'
            f' ON source."{number}" = s.n'
            ' JOIN (SELECT * FROM subject_receipts WHERE n >= ? AND n < ?) m ON s.n = m.n'
            + (" JOIN receipts r ON r.n = m.receipt" if processing_rows is None else "")
            + f' ORDER BY source."{number}"'
        )
        with closing(con.execute(query, [first, stop, first, stop]).to_arrow_reader(_BATCH)) as reader:
            for batch in reader:
                subjects = _subjects(policy, batch.column(0).to_pylist())
                processing_values = batch.column(1).to_pylist()
                if processing_rows is not None:
                    processing_values = processing_rows(processing_values)
                priors = processing_values if include_prior else [None] * len(processing_values)
                if include_prior:
                    processing_values = [receipt["processing_json"] for receipt in processing_values]
                for subject, processing, prior, matched, key, expected_version, expected_identity in zip(
                    subjects, processing_values, priors, *(batch.column(i).to_pylist() for i in range(2, 6))
                ):
                    if matched is False or matched is None and subject_identity(policy, subject) != (key, expected_version, expected_identity):
                        raise ValueError(f"Missing, ambiguous or reused subject receipt: {policy.dataset}")
                    count += 1
                    row = subject | _unpack(json.loads(processing))
                    yield (row, prior) if include_prior else row
        return count

    for path in subject_paths:
        with _parquet(path) as parquet:
            if not parquet.schema_arrow.equals(policy.subject_schema):
                raise ValueError(f"Subject schema differs from policy: {policy.dataset}")
            read = [0, 0]
            count = 0
            with closing(_numbered(parquet, start, number, None, read)) as numbered:
                # Finish each explicit-limit byte slice before encoding the next.
                # Ordinal processing keeps its existing batch replay outside that context.
                limited = _limits.get() is not None
                batch_replay = limited or processing_rows is not None
                sources = (
                    (pa.Table.from_batches([piece]) for batch in numbered for piece in _insert_batches(batch))
                    if limited else (pa.Table.from_batches([batch]) for batch in numbered)
                    if batch_replay else (numbered,)
                )
                for source in sources:
                    con.register("source", source)
                    try:
                        first = start + count
                        stop = first + source.num_rows if batch_replay else start + parquet.metadata.num_rows
                        added = yield from replay(first, stop)
                        if batch_replay and added != source.num_rows:
                            raise ValueError("Receipt replay did not reconstruct every source batch")
                        count += added
                    finally:
                        con.unregister("source")
            if read != [parquet.metadata.num_rows] * 2 or count != parquet.metadata.num_rows:
                raise ValueError("Receipt replay did not reconstruct every subject row")
            start += count


def read_attempts(receipt_paths, policy, *, generation_id, outcomes):
    """Admit every receipt before replaying its exact fields, without rebuilding the row index."""
    with bulk_connection() as (con, temp):
        try:
            _check_receipts(con, temp, receipt_paths, {policy.dataset: policy}, generation_id, False)
        except duckdb.Error as error:
            raise bulk_failure("Bulk receipt read failed", error) from error
        for path in receipt_paths:
            for receipt in _rows(path):
                if outcomes is None or receipt["outcome"] in outcomes:
                    yield {
                        **receipt,
                        "processing_fields": _unpack(json.loads(receipt["processing_json"])),
                        "diagnostics": _unpack(json.loads(receipt["diagnostic_json"])),
                    }
