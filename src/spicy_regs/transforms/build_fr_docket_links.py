"""Transform: build ``fr_docket_links.parquet``, the Federal Register ↔ docket link table.

One row per document–docket pair a dated Federal Register record states, in either of its two statements, carrying
the display columns the docket page needs, sorted by ``docket_id`` so ``WHERE docket_id = ?`` prunes row groups
instead of scanning (it replaced the ``docket_ids_json LIKE '%"<id>"%'`` full scan the docket page ran on every load):

- the labels the document prints: each element of ``docket_ids_json``, verbatim as ``docket_id``, one row per array
  occurrence (``docket_source_ordinal`` its position). The explosion keeps the old ``LIKE``'s matching semantics,
  including the quirk where a few array elements join two ids;
- the Register's regulations.gov link: ``regulations_dot_gov_docket_id``, never a docket read off a document id, as
  a row of its own only where no printed label carries it.

``link_source`` says which statement carries the pair: ``printed``, ``regulations_dot_gov_info`` or ``both``, where a
printed label is the Register's docket id or SpicyDocs' normalizer reads that id from it ("Docket No. FAA-2007-29334").
It is not part of a row's identity, so a pair both statements carry is one row, not two. A held Register row read
before the link was requested states no link (NULL), and adds none.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

# A docket's rows are fetched by range from the group that holds them.
from spicy_regs.transforms.regulations_shape import ROW_GROUP_ROWS
#: The link's own columns; every other published column is the Register's, carried for display.
OWN = ("docket_id", "docket_source_ordinal", "link_source", "normalized_docket_candidates")


def build_fr_docket_links(output_dir: Path) -> Path:
    """Build ``fr_docket_links.parquet`` from the published Register table: each pair either statement carries.

    Written in the published columns and order (``regulations_shape.subject_schema``), lists native, ordered by
    docket and then date descending; within one docket and date the order is the document's and the label's.
    """
    import duckdb
    import duckdb.func
    from spicy_docs.interpretation import identifier_shapes

    from spicy_regs.transforms.regulations_shape import subject_schema

    fr_file = output_dir / "federal_register.parquet"
    if not fr_file.exists():
        raise FileNotFoundError(f"federal_register.parquet not found in {output_dir}")
    schema = subject_schema("fr_docket_links")
    display = ", ".join(f"fr.{name}" for name in schema.names if name not in OWN)

    logger.info("Building FR docket links via DuckDB...")
    out_file = output_dir / "fr_docket_links.parquet"
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    query = f"""
        WITH fr AS (SELECT *, regulations_dot_gov_docket_id AS _stated FROM read_parquet('{fr_file}')),
        printed AS (
            SELECT link.docket_id, link.ordinality - 1 AS docket_source_ordinal,
                   normalize_docket_candidates(link.docket_id) AS normalized_docket_candidates, fr._stated, {display}
            FROM fr, UNNEST(fr.docket_ids) WITH ORDINALITY AS link(docket_id, ordinality)
            WHERE link.docket_id IS NOT NULL AND TRIM(link.docket_id) <> ''
        ),
        tagged AS (
            SELECT *, CASE WHEN _stated = docket_id OR list_contains(normalized_docket_candidates, _stated)
                           THEN 'both' ELSE 'printed' END AS link_source
            FROM printed
        )
        SELECT {", ".join(schema.names)} FROM tagged
        UNION ALL BY NAME
        SELECT fr._stated AS docket_id, CAST(NULL AS BIGINT) AS docket_source_ordinal,
               normalize_docket_candidates(fr._stated) AS normalized_docket_candidates, {display},
               'regulations_dot_gov_info' AS link_source
        FROM fr ANTI JOIN (SELECT document_number, publication_date FROM tagged WHERE link_source = 'both') carried
             USING (document_number, publication_date)
        WHERE fr._stated IS NOT NULL
        ORDER BY docket_id, publication_date DESC, document_number, docket_source_ordinal NULLS LAST
    """
    with duckdb.connect() as con:
        con.create_function("normalize_docket_candidates", identifier_shapes.normalize_docket_references,
                            ["VARCHAR"], "VARCHAR[]", null_handling=duckdb.func.FunctionNullHandling.SPECIAL)
        con.execute("SET memory_limit='4GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute(f"SET temp_directory='{spill_dir}'")
        with pq.ParquetWriter(out_file, schema, compression="zstd") as writer:
            for batch in con.execute(query).to_arrow_reader(50_000):
                writer.write_table(pa.Table.from_batches([batch]).cast(schema), row_group_size=ROW_GROUP_ROWS)

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FR docket links: {:,} rows", rows)
    return out_file
