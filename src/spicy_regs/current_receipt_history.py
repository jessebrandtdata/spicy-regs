"""Bounded lookups retain the maintained writers' exact receipt inheritance."""
from pathlib import Path
from collections.abc import Sequence
from contextlib import ExitStack


def inherit_current_receipts(current: Path, priors: Sequence[Path], destination: Path, *, inherit_observations: bool = True,
                             require_unique_prior: bool = False) -> Path:
    """Batch the disk lookup; preserve the current row writer's inheritance exactly.

    Callers admit both input bundles first. Matching accepted identity inherits
    regardless of whether values changed. Matching observations inherit in prior
    file order, with repeated receipt identities ignored.
    Failed attempts remain fresh. No prior row replaces a current receipt.
    ``require_unique_prior`` leaves any identity with more than one accepted prior to the row writer, as
    :class:`NotBulkEligible`; without it, differing receipts for one identity are a ``ValueError``.
    SQL sorts ordinal pairs; full payloads are bounded by an output batch and
    the largest prior row group. Prior groups may be reread across batches.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.etl_bulk import NotBulkEligible, bulk_connection, _literal
    from spicy_regs.receipt_history import _take_prior_rows
    from spicy_regs.etl_receipts import (RECEIPT_SCHEMA, open_receipt_writer, ReceiptContext, decode_exact_json,
                                        exact_json, inherit_receipt, _digest as receipt_digest)
    with bulk_connection(destination.parent) as (con, _), ExitStack() as files:
        # File ordinals and explicit ORDER BY clauses define every required order.
        con.execute("SET threads=1; SET memory_limit='1GB'; SET preserve_insertion_order=false")
        parts, offset, parquet_priors, file_ends, group_ends = [], 0, [], [], []
        for prior in priors:
            parquet = files.enter_context(pq.ParquetFile(prior))
            parquet_priors.append(parquet)
            ends, total = [], 0
            for group in range(parquet.metadata.num_row_groups):
                total += parquet.metadata.row_group(group).num_rows
                ends.append(total)
            group_ends.append(ends)
            literal = _literal(str(prior))
            parts.append(f"SELECT * EXCLUDE(file_row_number), file_row_number+{offset} AS ordinal "
                         f"FROM read_parquet({literal}, file_row_number=true, hive_partitioning=false)")
            offset += parquet.metadata.num_rows
            file_ends.append(offset)
        if not parts:
            raise ValueError("Prior inheritance requires an admitted prior")
        con.execute("CREATE VIEW prior AS " + " UNION ALL ".join(parts))
        conflicts = "count(*)>1" if require_unique_prior else "count(DISTINCT receipt_id)>1"
        conflicting = con.execute("SELECT EXISTS(SELECT 1 FROM prior WHERE outcome='accepted' "
                                  f"GROUP BY dataset,record_id HAVING {conflicts})").fetchone()
        if conflicting == (True,):
            if require_unique_prior:
                raise NotBulkEligible("Ambiguous selected prior receipts require the original row writer")
            raise ValueError("Conflicting selected prior receipts")
        con.execute("CREATE TABLE accepted_keys AS SELECT dataset,record_id,min(ordinal) AS ordinal "
                    "FROM prior WHERE outcome='accepted' GROUP BY dataset,record_id")
        if inherit_observations:
            observed = con.execute("SELECT * FROM read_parquet(?) WHERE outcome='observed'", [str(current)]).to_arrow_table()
            if observed.num_rows != 1:
                raise ValueError("Current inheritance requires one fresh metadata observation")
            metadata = observed.to_pylist()[0]
            metadata_context = ReceiptContext(metadata['generation_id'], metadata['attempt_id'], metadata['processor'],
                                              metadata['witnesses'], decode_exact_json(metadata['diagnostic_json']))
            prior_metadata = con.execute("SELECT * EXCLUDE(ordinal) FROM prior WHERE outcome='observed' "
                                        "AND dataset=? AND processing_json=? ORDER BY ordinal",
                                        [metadata['dataset'], metadata['processing_json']]).to_arrow_reader(batch_size=2000)
            seen = set()
            for batch in prior_metadata:
                for row in batch.to_pylist():
                    if row['receipt_id'] not in seen:
                        metadata_context = inherit_receipt(metadata_context, row)
                        seen.add(row['receipt_id'])
            metadata['witnesses'] = list(metadata_context.witnesses)
            metadata['diagnostic_json'] = exact_json(metadata_context.diagnostics)
            metadata['receipt_id'] = receipt_digest({k: v for k, v in metadata.items() if k != 'receipt_id'})
        # Put acceptance inside an equality key so DuckDB can hash the join.
        # Nonaccepted rows get NULL and retain their unmatched current receipt.
        query = """SELECT c.file_row_number AS current_ordinal, k.ordinal AS prior_ordinal
            FROM read_parquet(?,file_row_number=true,hive_partitioning=false) c
            LEFT JOIN accepted_keys k ON CASE WHEN c.outcome='accepted' THEN c.dataset END=k.dataset AND c.record_id=k.record_id
            ORDER BY current_ordinal"""
        # Only ordinals cross the corpus-wide join/sort. Gather complete prior
        # payloads for each output batch with the maintained row-group reader.
        # All lookup statements finish before opening the streaming result.
        reader = con.execute(query, [str(current)]).to_arrow_reader(batch_size=2000)
        count = 0
        with pq.ParquetFile(current) as source, \
                open_receipt_writer(destination) as writer:
            expected_count = source.metadata.num_rows
            for batch in source.iter_batches(batch_size=2000, columns=RECEIPT_SCHEMA.names, use_threads=False):
                try:
                    mapping = reader.read_next_batch()
                except StopIteration as error:
                    raise ValueError("Prior inheritance changed current receipt count") from error
                if mapping.num_rows != batch.num_rows:
                    raise ValueError("Prior inheritance changed current receipt count")
                if mapping.column('current_ordinal').to_pylist() != list(range(count, count + batch.num_rows)):
                    raise ValueError("Prior inheritance changed current receipt order")
                prior, positions = _take_prior_rows(
                    parquet_priors, file_ends, group_ends,
                    mapping.column('prior_ordinal').to_pylist(), RECEIPT_SCHEMA,
                )
                prior_rows = dict(zip(positions, prior.to_pylist(), strict=True))
                result = []
                for position, row in enumerate(batch.to_pylist()):
                    count += 1
                    prior = prior_rows.get(position)
                    matches = [prior] if prior is not None else []
                    if inherit_observations and row['outcome'] == 'observed':
                        row = dict(metadata)
                    if matches:
                        context = ReceiptContext(row['generation_id'], row['attempt_id'], row['processor'],
                                                 row['witnesses'], decode_exact_json(row['diagnostic_json']))
                        for match in matches:
                            context = inherit_receipt(context, match)
                        row['witnesses'] = list(context.witnesses)
                        row['diagnostic_json'] = exact_json(context.diagnostics)
                        row['receipt_id'] = receipt_digest({k: v for k, v in row.items() if k != 'receipt_id'})
                    result.append(row)
                writer.write_table(pa.Table.from_pylist(result, schema=RECEIPT_SCHEMA))
        if count != expected_count or next(reader, None) is not None:
            raise ValueError("Prior inheritance changed current receipt count")
    return destination
