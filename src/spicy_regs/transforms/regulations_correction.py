"""Explicit source rereads: correct mapped fields without erasing enrichment."""

from collections.abc import Sequence

import duckdb


ENRICHMENT_COLUMNS = frozenset({"text_content", "text_extraction_status", "pdf_extraction_results_json"})


def correction_query(
    con: duckdb.DuckDBPyConnection,
    *,
    fresh_sql: str,
    prior_sql: str,
    columns: Sequence[str],
    key: str,
) -> str:
    """Validate an explicit reread and return its source-recency merge query.

    Fresh source fields, including NULL, win at an equal source instant. An
    older or undated reread cannot displace a dated newer prior. Both undated
    rows permit the explicit correction. Only independently produced text and
    extraction fields fall back to the prior, and only together: a fresh row
    with neither text nor status keeps the prior's text, status and results,
    and any other fresh row replaces all three, so text never pairs with
    another fill's status or provenance. Source facts never fall back.
    Conflicting/duplicate input identities and unorderable dates refuse before
    output replacement, so an incomplete correction remains retryable.
    """
    for name, query in (("_correction_fresh", fresh_sql), ("_correction_prior", prior_sql)):
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS {query}")
        bad = con.execute(f'SELECT count(*) FROM {name} WHERE "{key}" IS NULL OR trim("{key}") = \'\'').fetchone()
        duplicates = con.execute(f'SELECT count(*) - count(DISTINCT "{key}") FROM {name}').fetchone()
        if (bad and bad[0]) or (duplicates and duplicates[0]):
            raise ValueError(f"source correction requires distinct nonblank {key} in {name}")

    # Unrelated legacy rows do not have to acquire a new date spelling just to
    # survive a bounded repair. Only fresh and matched prior dates are compared.
    bad_dates = con.execute(f"""
        SELECT count(*) FROM (
            SELECT modify_date FROM _correction_fresh
            UNION ALL
            SELECT p.modify_date FROM _correction_prior p
            JOIN _correction_fresh f USING ("{key}")
        ) WHERE modify_date IS NOT NULL
          AND try_cast(modify_date AS TIMESTAMPTZ) IS NULL
    """).fetchone()
    if bad_dates and bad_dates[0]:
        raise ValueError("source correction cannot order a non-NULL modify_date")

    wins = f'''f."{key}" IS NOT NULL AND (
        p."{key}" IS NULL OR p.modify_date IS NULL OR
        try_cast(f.modify_date AS TIMESTAMPTZ) >= try_cast(p.modify_date AS TIMESTAMPTZ)
    )'''
    unfilled = 'f."text_content" IS NULL AND f."text_extraction_status" IS NULL'
    projection = []
    for column in columns:
        fresh = f'f."{column}"'
        if column in ENRICHMENT_COLUMNS:
            fresh = f'CASE WHEN {unfilled} THEN p."{column}" ELSE {fresh} END'
        projection.append(f'CASE WHEN {wins} THEN {fresh} ELSE p."{column}" END AS "{column}"')
    return f'''SELECT {", ".join(projection)} FROM _correction_fresh f
               FULL OUTER JOIN _correction_prior p ON f."{key}" = p."{key}"'''


def correct_receipted_dataset(prior, fresh, destination, *, generation_id: str):
    """Local correction using receipt-qualified status and exact source recency.

    Both inputs must be complete selected subject/receipt pairs. The existing
    correction rule keeps text, status and per-URL results together; its duplicate
    and unorderable-date refusals still occur before the new output is exposed.
    """
    from hashlib import file_digest
    from pathlib import Path
    from tempfile import TemporaryDirectory

    from spicy_regs.etl_receipts import ReceiptContext
    from spicy_regs.transforms.regulations_receipts import materialize_internal, write_records
    from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS

    if prior.dataset != fresh.dataset or prior.dataset not in ("documents", "comments"):
        raise ValueError("Correction inputs must name the same documents/comments dataset")
    dataset = prior.dataset
    witnesses = []
    for selected in (prior, fresh):
        for path in (*selected.subjects, selected.receipts):
            with path.open("rb") as body:
                digest = file_digest(body, "sha256").hexdigest()
            witnesses.append(
                {
                    "source_id": f"{dataset}:{path.name}",
                    "source_uri": None,
                    "sha256": digest,
                    "locator": None,
                    "body_version": selected.generation_id,
                }
            )
    with TemporaryDirectory(prefix="regulations-correction-") as temp, duckdb.connect() as con:
        paths = [
            materialize_internal(selected, Path(temp) / f"{role}.parquet")
            for role, selected in [("prior", prior), ("fresh", fresh)]
        ]
        for role, path in zip(("prior_input", "fresh_input"), paths, strict=True):
            con.from_parquet(str(path)).create_view(role)
        query = correction_query(
            con,
            fresh_sql="SELECT * FROM fresh_input",
            prior_sql="SELECT * FROM prior_input",
            columns=[c for c, _ in SOURCE_COLUMNS[dataset]],
            key="document_id" if dataset == "documents" else "comment_id",
        )

        def records():
            ordinal = 0
            for batch in con.execute(query).to_arrow_reader(2000):
                for row in batch.to_pylist():
                    yield (
                        row,
                        ReceiptContext(
                            generation_id,
                            f"correction:{ordinal}",
                            "spicy-regs:regulations-correction-v1",
                            witnesses,
                            {"prior_generation_id": prior.generation_id, "fresh_generation_id": fresh.generation_id},
                        ),
                    )
                    ordinal += 1

        if dataset == "documents":
            def fresh_failures():
                import pyarrow as pa
                import pyarrow.compute as pc
                import pyarrow.parquet as pq

                with pq.ParquetFile(fresh.receipts) as source:
                    for batch in source.iter_batches(batch_size=2000):
                        selected = pc.call_function("and", [
                            pc.call_function("equal", [batch.column("dataset"), dataset]),
                            pc.call_function("is_in", [batch.column("outcome")],
                                             pc.SetLookupOptions(pa.array(["refused", "rejected", "error"]))),
                        ])
                        yield from batch.filter(selected).to_pylist()

            # The old selected row is the predecessor. Both input files remain
            # witnessed; fresh failures are current attempts, not old history
            # that an unchanged held subject could incorrectly resolve.
            return write_records(dataset, records(), destination, prior_receipts=[prior.receipts],
                                 additional_failures=fresh_failures(), resolution_receipts=fresh.receipts)
        return write_records(dataset, records(), destination, prior_receipts=[prior.receipts, fresh.receipts])
