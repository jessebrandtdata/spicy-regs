"""One bounded set comparison carries immutable receipt occurrences in file order."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from collections.abc import Callable, Mapping, Sequence
from bisect import bisect_right
from contextlib import ExitStack
from dataclasses import dataclass


def crs_revision_identity_migrations(policy, subject_mapper):
    """Map only the registered CRS /1 keys to their retained, dated /2 revision.

    The family's existing mapper interprets the raw date. The previous receipt
    remains immutable; its alias is used only to find a migration predecessor.
    """
    from spicy_regs.earlier_receipt_policies import earlier_policies
    from spicy_regs.etl_receipts import decode_exact_json, subject_identity

    registered = {
        "bill_publisher_summaries": (("legislative-documents/2",), "raw_source_row"),
        "crs_reports": (("government-sources/2", "government-sources/3"), "raw_record"),
    }
    migration = registered.get(policy.dataset)
    if migration is None or policy.policy_version not in migration[0]:
        return {}
    previous = next(p for p in earlier_policies(policy) if p.identity_fields != policy.identity_fields)

    def record_id(processing_json, prior_record_id):
        source = decode_exact_json(processing_json)[migration[1]]
        subject = subject_mapper(source)
        if subject_identity(previous, subject)[0] != prior_record_id:
            raise ValueError("CRS revision migration differs from the prior receipt identity")
        return subject_identity(policy, subject)[0]

    return {(policy.dataset, previous.policy_version): record_id}


def _migrated_record_keys(record_id):
    """Decode only old-policy accepted inputs, in bounded Arrow batches."""
    import pyarrow as pa

    def derive(processing, identities):
        return pa.array([record_id(raw, identity) for raw, identity in
                         zip(processing.to_pylist(), identities.to_pylist(), strict=True)], type=pa.string())

    return derive


@dataclass(frozen=True)
class RefusalIdentity:
    """Explicit raw natural key shared by a family's accepted and refused inputs.

    Values must match exactly, including their lossless type tags. A changed key
    spelling or missing field cannot resolve a refusal. Null is a present value.
    ``refused_raw_field`` defaults to the accepted input's container name.
    An explicit ``accepted_raw_field=None`` uses the accepted declared identity;
    the caller must supply the refused raw container and identical key names.
    ``refused_identity`` may derive those declared keys through the family's
    maintained identity function. Only unmatched refused inputs are decoded;
    missing or invalid keys stay unresolved, and retained receipts never change.
    """

    fields: tuple[str, ...]
    accepted_raw_field: str | None
    refused_raw_field: str | None = None
    refused_identity: Callable[[Mapping], Mapping | None] | None = None

    def __post_init__(self):
        if (not self.fields or len(set(self.fields)) != len(self.fields)
                or any(not isinstance(field, str) or not field for field in self.fields)
                or (self.accepted_raw_field is not None
                    and (not isinstance(self.accepted_raw_field, str) or not self.accepted_raw_field))
                or (self.accepted_raw_field is None and self.refused_raw_field is None)
                or (self.refused_raw_field is not None
                    and (not isinstance(self.refused_raw_field, str) or not self.refused_raw_field))):
            raise ValueError("Refusal resolution needs explicit raw input and natural-key fields")


def _derived_refusal_keys(identity: RefusalIdentity):
    """Bounded Arrow UDF for a family's explicit refusal identity derivation."""
    import pyarrow as pa
    from spicy_regs.etl_receipts import decode_exact_json, exact_json

    def derive(texts):
        keys = []
        for text in texts.to_pylist():
            try:
                processing = decode_exact_json(text)
                raw = processing[identity.refused_raw_field or identity.accepted_raw_field]
                assert identity.refused_identity is not None
                values = identity.refused_identity(raw) if isinstance(raw, Mapping) else None
                key = ("[" + ",".join(exact_json(values[field]) for field in identity.fields) + "]"
                       if isinstance(values, Mapping) and all(field in values for field in identity.fields) else None)
            except (ValueError, TypeError, KeyError):
                key = None
            keys.append(key)
        return pa.array(keys, type=pa.string())

    return derive


def _raw_identity_sql(column: str, raw_field: str, fields: tuple[str, ...]) -> str:
    """Extract exact tagged key values; absent containers/fields yield SQL NULL."""
    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    def item(value, key):
        return ("json_extract(list_extract(list_filter(json_extract(" + value
                + ", '$[1]')::JSON[], entry -> json_extract_string(entry, '$[0]')="
                + literal(key) + "), 1), '$[1]')")

    raw = item(column, raw_field)
    entries = (f"CASE WHEN json_extract_string({raw}, '$[0]')='dict' "
               f"THEN json_extract({raw}, '$[1]')::JSON[] ELSE []::JSON[] END")
    values = [(f"json_extract(list_extract(list_filter(({entries}), entry -> "
               f"json_extract_string(entry, '$[0]')={literal(field)}), 1), '$[1]')") for field in fields]
    present = " AND ".join(f"{value} IS NOT NULL" for value in values)
    return (f"CASE WHEN {present} "
            f"THEN CAST(to_json(list_value({', '.join(values)})) AS VARCHAR) END")


def _pending_refusals(con, identities: Mapping[str, RefusalIdentity], *, current_source: str = "current") -> None:
    """Keep unmatched prior refusals unless a current accepted input proves resolution.

    Matching operates only on narrow natural keys. No raw identity is guessed:
    families opt in with the exact containers and fields their receipts retain.
    """
    con.execute("""CREATE TABLE pending_refusals AS SELECT ordinal FROM prior
        WHERE outcome='refused' AND ordinal NOT IN
        (SELECT prior_ordinal FROM matches WHERE prior_ordinal IS NOT NULL)""")
    if not con.execute("SELECT count(*) FROM pending_refusals").fetchone()[0]:
        return
    for name, condition in (("prior", "ordinal IN (SELECT ordinal FROM pending_refusals)"),
                            ("current", "outcome='accepted'")):
        source = current_source if name == "current" else name
        parts: list[str] = [f"SELECT ordinal,dataset,'declared' AS kind,identity_json AS value FROM {source} "
                           f"WHERE {condition} AND record_id IS NOT NULL AND identity_json IS NOT NULL"]
        derived_functions = []
        for index, (dataset, identity) in enumerate(identities.items()):
            raw_field = (identity.refused_raw_field or identity.accepted_raw_field
                         if name == "prior" else identity.accepted_raw_field)
            if name == "prior" and identity.refused_identity is not None:
                function = f"derived_refusal_key_{index}"
                con.create_function(function, _derived_refusal_keys(identity), ["VARCHAR"], "VARCHAR",
                                    type="arrow", null_handling="special")
                derived_functions.append(function)
                # Apply DuckDB's same JSON spelling to the callback's exact tags
                # and the accepted key; no retained receipt is rewritten.
                key = f"CAST(to_json(CAST({function}(processing_json) AS JSON)) AS VARCHAR)"
            elif raw_field is None:
                # Declared identity_json is an exact list of [field, value]
                # pairs. Repackage only its existing tagged values so the raw
                # comparison preserves types and never normalizes a key.
                entries = "json_extract(identity_json, '$[1]')::JSON[]"
                values = [
                    "json_extract(list_extract(list_filter(" + entries
                    + ", entry -> json_extract_string(entry, '$[1][0][1]')='"
                    + field.replace("'", "''") + "'), 1), '$[1][1]')"
                    for field in identity.fields
                ]
                key = ("CASE WHEN " + " AND ".join(f"{value} IS NOT NULL" for value in values)
                       + " THEN CAST(to_json(list_value(" + ", ".join(values) + ")) AS VARCHAR) END")
            else:
                key = _raw_identity_sql("processing_json", raw_field, identity.fields)
            dataset_sql = "'" + dataset.replace("'", "''") + "'"
            parts.append(f"SELECT ordinal,dataset,'raw' AS kind,{key} AS value FROM {source} "
                         f"WHERE {condition} AND dataset={dataset_sql}")
        try:
            con.execute(f"CREATE TABLE {name}_resolution_keys AS " + " UNION ALL ".join(parts))
        finally:
            for function in derived_functions:
                con.remove_function(function)
    con.execute("""DELETE FROM pending_refusals WHERE ordinal IN (
        SELECT p.ordinal FROM prior_resolution_keys p JOIN current_resolution_keys c
        ON p.dataset=c.dataset AND p.kind=c.kind AND p.value=c.value)""")
    con.execute("DROP TABLE prior_resolution_keys")
    con.execute("DROP TABLE current_resolution_keys")


def _processing_key(column: str) -> str:
    """A narrow candidate key; matching raw values are checked before output."""
    return f"unhex(sha256({column}))"


def _take_prior_rows(priors, file_ends, group_ends, ordinals, schema):
    """Gather one output batch, reading each required prior row group once."""
    import pyarrow as pa

    requests = {}
    for position, ordinal in enumerate(ordinals):
        if ordinal is None:
            continue
        file = bisect_right(file_ends, ordinal)
        if ordinal < 0 or file == len(priors):
            raise ValueError("Receipt history selected an invalid prior ordinal")
        local = ordinal - (file_ends[file - 1] if file else 0)
        group = bisect_right(group_ends[file], local)
        row = local - (group_ends[file][group - 1] if group else 0)
        requests.setdefault((file, group), []).append((position, row))
    chunks, positions = [], []
    for (file, group), rows in requests.items():
        source = priors[file].read_row_group(group, columns=schema.names, use_threads=False)
        chunks.append(source.take(pa.array([row for _, row in rows], type=pa.int64())))
        del source
        positions.extend(position for position, _ in rows)
    if not chunks:
        return pa.Table.from_batches([], schema=schema), []
    order = sorted(range(len(positions)), key=positions.__getitem__)
    return pa.concat_tables(chunks).take(pa.array(order, type=pa.int64())), sorted(positions)


def history_query_threads(value: int) -> int:
    """Validate DuckDB's positive BIGINT thread setting before creating private state."""
    if type(value) is not int or not 1 <= value <= (1 << 63) - 1:
        raise ValueError("History query threads must be a positive BIGINT integer")
    return value


def carry_receipt_history(current_path: Path, prior_paths: Sequence[Path], destination: Path, **options):
    """Time :func:`_carry_receipt_history`, which this only calls (``receipt-phase carry``)."""
    from spicy_regs.phase_log import phase_log

    with phase_log("carry", priors=len(prior_paths)):
        return _carry_receipt_history(current_path, prior_paths, destination, **options)


def _carry_receipt_history(
    current_path: Path, prior_paths: Sequence[Path], destination: Path, *,
    refusal_identities: Mapping[str, RefusalIdentity] | None = None,
    identity_migrations: Mapping[tuple[str, str], Callable[[str, str], str]] | None = None,
    resolution_receipts: Path | None = None,
    threads: int = 1,
    report_settings: Callable[[dict[str, int | str]], None] | None = None,
) -> Path:
    """Keep identical attempts exactly; reference only the direct accepted predecessor on change.

    Inputs are candidate current receipts and an already validated selected prior bundle.
    Unmatched old accepted rows and observations disappear. Unresolved prior
    refusals follow current receipts unchanged, in prior occurrence order.
    A current accepted receipt resolves a refusal with the same declared identity,
    or the exact raw natural key explicitly supplied in ``refusal_identities``.
    With ``resolution_receipts``, that current row's (dataset, record_id) must also
    be accepted in this already-admitted fresh receipt file. Use it when current
    includes unchanged held rows; those alone cannot prove a fresh success.
    Missing or changed key spellings remain unresolved. Duplicate nonaccepted
    occurrences match FIFO, so repeated refusals do not accumulate copies.
    ``report_settings`` receives the effective limits from this query connection;
    ``threads`` defaults to one and is validated before any private state is created.
    Narrow matching keys spill to private disk. Full prior rows are gathered by
    Parquet ordinal in output batches; Python only decodes changed predecessors.
    Registered ``identity_migrations`` derive a new key from an old receipt's
    retained input. They always create a current receipt with original evidence
    and predecessor references, even when the input and subject are unchanged.
    """
    threads = history_query_threads(threads)
    import duckdb
    import duckdb.func
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    from spicy_regs.etl_receipts import (
        RECEIPT_SCHEMA, open_receipt_writer, ReceiptContext, _digest, decode_exact_json, exact_json, inherit_receipt,
    )

    def compute(name, *args):
        # Arrow generates these kernels at import time; use its typed entry point.
        return pc.call_function(name, list(args))

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        TemporaryDirectory(prefix="receipt-history-", dir=destination.parent) as temporary,
        duckdb.connect(str(Path(temporary) / "history.duckdb")) as con,
        ExitStack() as files,
    ):
        con.execute("SET threads=?", [threads])
        con.execute("SET memory_limit='1GB'")
        con.execute("SET max_temp_directory_size='32GB'")
        con.execute("SET temp_directory=?", [str(Path(temporary) / "spill")])
        # Ordinals define occurrence and final output order explicitly. Avoid
        # extra import/export buffers while the wide history spills to disk.
        con.execute("SET preserve_insertion_order=false")
        if report_settings is not None:
            effective = con.execute(
                "SELECT current_setting('threads'), current_setting('memory_limit'), "
                "current_setting('max_temp_directory_size')"
            ).fetchone()
            assert effective is not None
            report_settings(dict(zip(
                ["historyQueryThreads", "historyQueryMemory", "historyQuerySpill"], effective
            )))

        def quoted(path):
            return "'" + str(path).replace("'", "''") + "'"

        con.execute(
            "CREATE VIEW current AS SELECT * EXCLUDE(file_row_number), file_row_number AS ordinal "
            f"FROM read_parquet({quoted(current_path)}, file_row_number=true, hive_partitioning=false)"
        )
        offset, parts, priors, file_ends, group_ends = 0, [], [], [], []
        for path in prior_paths:
            parquet = files.enter_context(pq.ParquetFile(path))
            priors.append(parquet)
            count = parquet.metadata.num_rows
            ends, total = [], 0
            for group in range(parquet.metadata.num_row_groups):
                total += parquet.metadata.row_group(group).num_rows
                ends.append(total)
            group_ends.append(ends)
            parts.append(
                f"SELECT * EXCLUDE(file_row_number), file_row_number+{offset} AS ordinal "
                f"FROM read_parquet({quoted(path)}, file_row_number=true, hive_partitioning=false)"
            )
            offset += count
            file_ends.append(offset)
        con.execute(
            "CREATE VIEW prior AS " + (" UNION ALL ".join(parts) if parts else "SELECT * FROM current WHERE false")
        )
        duplicate = con.execute(
            "SELECT EXISTS(SELECT 1 FROM prior WHERE outcome='accepted' GROUP BY dataset,record_id HAVING count(*)>1)"
        ).fetchone()
        assert duplicate is not None
        if duplicate[0]:
            raise ValueError("Conflicting selected prior receipts")
        # One narrow table per side, built straight from the view: every text identity is a 32-byte digest (hex ids
        # and versions would be 71 characters each), and accepted-only columns are not stored twice. The workspace
        # was ~543 B/row; it is private disk, so keep it small. Candidate keys are checked against the raw values below.
        # Kept receipts are not decoded, so check the one embedded history that can be wrong: payloads that older
        # inheritance retained by digest. Only such legacy receipts mention it; a carried receipt never does.
        histories = con.execute(
            "SELECT diagnostic_json FROM prior WHERE contains(diagnostic_json,'retained_processing')"
        ).to_arrow_reader(batch_size=2000)
        for batch in histories:
            for text in batch.column(0).to_pylist():
                retained = decode_exact_json(text).get("retained_processing", {})
                if not isinstance(retained, dict) or any(_digest(value) != key for key, value in retained.items()):
                    raise ValueError("Retained processing digest differs")
        for name in ("current", "prior"):
            con.execute(
                f"CREATE TABLE {name}_ranked AS SELECT *, row_number() OVER(PARTITION BY dataset,outcome,"
                "processing_key, record_key, version_key, diagnostic_key ORDER BY ordinal) AS occurrence FROM ("
                "SELECT ordinal, dataset, outcome, "
                f"{_processing_key('processing_json')} AS processing_key, "
                "CASE WHEN outcome='accepted' THEN unhex(sha256(record_id)) ELSE ''::BLOB END AS record_key, "
                "CASE WHEN outcome='accepted' THEN unhex(sha256(subject_version)) ELSE ''::BLOB END AS version_key, "
                f"CASE WHEN outcome='accepted' THEN ''::BLOB ELSE {_processing_key('diagnostic_json')} END AS diagnostic_key "
                f"FROM {name})"
            )
        con.execute("CREATE TABLE migrated_keys (ordinal BIGINT, dataset VARCHAR, record_key BLOB)")
        for index, ((dataset, policy_version), record_id) in enumerate((identity_migrations or {}).items()):
            function = f"migrated_record_key_{index}"
            con.create_function(function, _migrated_record_keys(record_id), ["VARCHAR", "VARCHAR"], "VARCHAR",
                                type=duckdb.func.PythonUDFType.ARROW,
                                null_handling=duckdb.func.FunctionNullHandling.SPECIAL)
            try:
                con.execute(
                    f"INSERT INTO migrated_keys SELECT ordinal,dataset,unhex(sha256({function}(processing_json,record_id))) "
                    "FROM prior WHERE dataset=? AND policy_version=? AND outcome='accepted'",
                    [dataset, policy_version],
                )
            finally:
                con.remove_function(function)
        # Outcome separates the branches, so ignored fields use fixed keys.
        # Ordinary equality preserves null refusal and admits a hash join
        # instead of an OR join that compares every current/prior pair.
        # A changed accepted row finds its predecessor by identity alone; a non-accepted row's empty record_key matches nothing.
        con.execute("""CREATE TABLE matches AS SELECT c.ordinal AS current_ordinal,p.ordinal AS prior_ordinal,
            CASE WHEN p.ordinal IS NULL AND c.outcome='accepted'
                THEN coalesce(predecessor.ordinal,migration.ordinal) END AS predecessor_ordinal,
            p.ordinal IS NULL AND predecessor.ordinal IS NULL AND migration.ordinal IS NOT NULL AS migrated
            FROM current_ranked c LEFT JOIN prior_ranked p ON c.dataset=p.dataset AND c.outcome=p.outcome
            AND c.processing_key=p.processing_key AND c.occurrence=p.occurrence
            AND c.record_key=p.record_key AND c.version_key=p.version_key AND c.diagnostic_key=p.diagnostic_key
            LEFT JOIN prior_ranked predecessor ON c.dataset=predecessor.dataset AND c.record_key=predecessor.record_key
            AND predecessor.outcome='accepted'
            LEFT JOIN migrated_keys migration ON c.dataset=migration.dataset AND c.record_key=migration.record_key""")
        for name in ("current_ranked", "prior_ranked", "migrated_keys"):
            con.execute(f"DROP TABLE {name}")
        resolution_source = "current"
        if resolution_receipts is not None:
            # Read only fresh acceptance keys. A semijoin does not duplicate current rows or copy their payloads.
            con.execute(
                "CREATE VIEW resolving_current AS SELECT c.* FROM current c SEMI JOIN "
                f"read_parquet({quoted(resolution_receipts)}, hive_partitioning=false) fresh "
                "ON c.dataset=fresh.dataset AND c.record_id=fresh.record_id AND fresh.outcome='accepted'"
            )
            resolution_source = "resolving_current"
        _pending_refusals(con, refusal_identities or {}, current_source=resolution_source)
        # Only ordinals are sorted. Joining/sorting literal JSON and full rows
        # can exhaust the connection's memory even with spilling enabled.
        reader = con.execute("SELECT * FROM matches ORDER BY current_ordinal").to_arrow_reader(batch_size=2000)
        target = Path(temporary) / "receipts.parquet"
        with (
            pq.ParquetFile(current_path) as current,
            open_receipt_writer(target) as writer,
        ):
            expected_count = current.metadata.num_rows
            written = 0
            for batch in current.iter_batches(batch_size=2000, columns=RECEIPT_SCHEMA.names, use_threads=False):
                try:
                    mapping = reader.read_next_batch()
                except StopIteration as error:
                    raise ValueError("Receipt history changed current receipt count") from error
                if mapping.num_rows != batch.num_rows:
                    raise ValueError("Receipt history changed current receipt count")
                expected_order = pa.array(range(written, written + batch.num_rows), type=pa.int64())
                if not pc.call_function(
                    "all", [pc.call_function("equal", [mapping.column("current_ordinal"), expected_order])]
                ).as_py():
                    raise ValueError("Receipt history changed current receipt order")
                table = pa.Table.from_batches([batch])
                ordinals = compute("coalesce", mapping.column("prior_ordinal"), mapping.column("predecessor_ordinal"))
                prior, positions = _take_prior_rows(priors, file_ends, group_ends, ordinals.to_pylist(), RECEIPT_SCHEMA)
                if positions:
                    selected = table.take(pa.array(positions, type=pa.int64()))
                    retained = pc.take(
                        compute("is_valid", mapping.column("prior_ordinal")), pa.array(positions, type=pa.int64())
                    )

                    def distinct(left, right):
                        equal = pc.fill_null(compute("equal", left, right), False)
                        return compute(
                            "invert",
                            compute("or", equal, compute("and", compute("is_null", left), compute("is_null", right))),
                        )

                    mismatch = compute(
                        "and",
                        retained,
                        compute(
                            "or",
                            distinct(selected["processing_json"], prior["processing_json"]),
                            compute(
                                "and",
                                pc.fill_null(compute("not_equal", selected["outcome"], "accepted"), False),
                                distinct(selected["diagnostic_json"], prior["diagnostic_json"]),
                            ),
                        ),
                    )
                    if compute("any", mismatch).as_py():
                        raise ValueError("Receipt history candidate keys differ from exact values")
                    indices = list(range(batch.num_rows))
                    for i, position in enumerate(positions):
                        if retained[i].as_py():
                            indices[position] = batch.num_rows + i
                    table = pa.concat_tables([table, prior]).take(pa.array(indices, type=pa.int64()))
                changed = [
                    position for position in positions if mapping.column("predecessor_ordinal")[position].is_valid
                ]
                if changed:
                    updates = table.take(pa.array(changed, type=pa.int64())).to_pylist()
                    predecessors = prior.filter(
                        pc.take(
                            compute("is_valid", mapping.column("predecessor_ordinal")),
                            pa.array(positions, type=pa.int64()),
                        )
                    )
                    migrating = compute("any", mapping.column("migrated")).as_py()
                    prior_columns = (RECEIPT_SCHEMA.names if migrating else
                                     ["receipt_id", "generation_id", "processing_json"])
                    diagnostics, identities, witnesses = [], [], []
                    for position, row, predecessor in zip(
                        changed, updates, predecessors.select(prior_columns).to_pylist(), strict=True,
                    ):
                        values = decode_exact_json(row["diagnostic_json"])
                        values = {
                            k: v
                            for k, v in values.items()
                            if k not in {"prior_receipts", "retained_processing", "prior_receipt"}
                        }
                        values["prior_receipt"] = {
                            "receipt_id": predecessor["receipt_id"],
                            "generation_id": predecessor["generation_id"],
                            "processing_sha256": _digest(decode_exact_json(predecessor["processing_json"])),
                        }
                        if mapping.column("migrated")[position].as_py():
                            context = inherit_receipt(ReceiptContext(
                                row["generation_id"], row["attempt_id"], row["processor"], row["witnesses"], values,
                            ), predecessor)
                            values = dict(context.diagnostics)
                            values["prior_receipt"].update({
                                "policy_version": predecessor["policy_version"], "record_id": predecessor["record_id"],
                            })
                            row["witnesses"] = list(context.witnesses)
                        row["diagnostic_json"] = exact_json(values)
                        diagnostics.append(row["diagnostic_json"])
                        witnesses.append(row["witnesses"])
                        identities.append(_digest({k: v for k, v in row.items() if k != "receipt_id"}))
                    # Replace only changed accepted rows. Every unchanged row stays Arrow, with no Python decoding.
                    columns = [("diagnostic_json", diagnostics), ("receipt_id", identities)]
                    if migrating:
                        columns.append(("witnesses", witnesses))
                    for name, values in columns:
                        by_position = dict(zip(changed, values))
                        replacements = pa.array([by_position.get(i) for i in range(batch.num_rows)],
                                                type=RECEIPT_SCHEMA.field(name).type)
                        table = table.set_column(
                            table.schema.get_field_index(name),
                            name,
                            pc.call_function(
                                "if_else",
                                [compute("is_valid", mapping.column("predecessor_ordinal")), replacements, table[name]],
                            ),
                        )
                writer.write_table(table.select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA))
                written += batch.num_rows
            if written != expected_count:
                raise ValueError("Receipt history changed current receipt count")
            if next(reader, None) is not None:
                raise ValueError("Receipt history changed current receipt count")
            for batch in con.execute("SELECT ordinal FROM pending_refusals ORDER BY ordinal").to_arrow_reader(2000):
                retained, positions = _take_prior_rows(
                    priors, file_ends, group_ends, batch.column(0).to_pylist(), RECEIPT_SCHEMA)
                if positions != list(range(batch.num_rows)):
                    raise ValueError("Receipt history lost an unresolved refusal")
                writer.write_table(retained)
        target.replace(destination)
    return destination
