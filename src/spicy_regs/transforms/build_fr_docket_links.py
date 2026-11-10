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

import hashlib
import inspect
import json
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

#: The Register columns each link row carries for display, after the link's own.
DISPLAY_COLUMNS = (
    "document_number", "title", "abstract", "document_type", "subtype", "publication_date", "effective_on",
    "comments_close_on", "signing_date", "agency_slugs", "docket_ids_json", "regulation_id_numbers_json", "html_url",
    "pdf_url", "executive_order_number",
)
#: The published columns, in order; ``link_source`` is appended last so every earlier column keeps its position.
LINK_COLUMNS = (
    "docket_id", "docket_source_ordinal", "normalized_docket_candidates_json", "docket_normalization_rule",
    *DISPLAY_COLUMNS, "link_source",
)


def build_fr_docket_links(output_dir: Path) -> Path:
    """Build ``fr_docket_links.parquet``: each document–docket pair either statement carries, with display columns."""
    import duckdb
    import duckdb.func
    from spicy_docs.interpretation import identifier_shapes

    rule = "spicy_docs.normalize_docket_references@sha256:" + hashlib.sha256(
        Path(inspect.getfile(identifier_shapes)).read_bytes()
    ).hexdigest()

    fr_file = output_dir / "federal_register.parquet"
    if not fr_file.exists():
        raise FileNotFoundError(f"federal_register.parquet not found in {output_dir}")

    held = set(pq.read_schema(fr_file).names)
    # The published table holds each list natively; this builder's rows still state them as the receipt writer
    # takes them, so a published table is read back into that text here (ss-vse3, until the links leave receipts).
    published = ("" if "docket_ids_json" in held else
                 " REPLACE (array_to_string(agency_slugs, ',') AS agency_slugs),"
                 " CAST(to_json(docket_ids) AS VARCHAR) AS docket_ids_json,"
                 " CAST(to_json(regulation_id_numbers) AS VARCHAR) AS regulation_id_numbers_json")
    if published and (missing := sorted(set(DISPLAY_COLUMNS) - held - {"docket_ids_json", "regulation_id_numbers_json"})):
        # A generation published before regulations-native-v3 kept html_url and pdf_url in its receipts.
        raise ValueError(
            f"The pinned federal_register generation predates its promoted columns ({', '.join(missing)}), which "
            "the links carry: the federal_register job must publish first (rollup-federal-register.yml runs it "
            "before this one; a standalone fr_docket_links run cannot go ahead of it)")

    logger.info("Building FR docket links via DuckDB...")

    out_file = output_dir / "fr_docket_links.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.create_function("normalize_docket_candidates", lambda value: json.dumps(
        identifier_shapes.normalize_docket_references(value)), ["VARCHAR"], "VARCHAR", null_handling=duckdb.func.FunctionNullHandling.SPECIAL)
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    # Carries the columns the docket page's FR section renders (normalizeFRRow
    # rebuilds `docket_ids` from docket_ids_json, so keep that column). Sorted by
    # docket_id with a small row-group size so per-docket lookups prune.
    stated = "regulations_dot_gov_docket_id" if "regulations_dot_gov_docket_id" in held else "CAST(NULL AS VARCHAR)"
    display = ", ".join(f"fr.{column}" for column in DISPLAY_COLUMNS)
    query = f"""
    COPY (
        WITH fr AS (SELECT *{published}, {stated} AS _stated FROM read_parquet('{fr_file}')),
        printed AS (
            SELECT link.docket_id, link.ordinality - 1 AS docket_source_ordinal,
                   normalize_docket_candidates(link.docket_id) AS normalized_docket_candidates_json,
                   '{rule}' AS docket_normalization_rule, fr._stated, {display}
            FROM fr, UNNEST(CAST(json_extract(fr.docket_ids_json, '$') AS VARCHAR[])) WITH ORDINALITY AS link(docket_id, ordinality)
            WHERE fr.docket_ids_json IS NOT NULL AND link.docket_id IS NOT NULL AND TRIM(link.docket_id) <> ''
        ),
        tagged AS (
            SELECT *, CASE WHEN _stated = docket_id
                             OR list_contains(CAST(json_extract(normalized_docket_candidates_json, '$') AS VARCHAR[]), _stated)
                           THEN 'both' ELSE 'printed' END AS link_source
            FROM printed
        )
        SELECT {", ".join(LINK_COLUMNS)} FROM tagged
        UNION ALL BY NAME
        SELECT fr._stated AS docket_id, CAST(NULL AS BIGINT) AS docket_source_ordinal,
               normalize_docket_candidates(fr._stated) AS normalized_docket_candidates_json,
               '{rule}' AS docket_normalization_rule, {display},
               'regulations_dot_gov_info' AS link_source
        FROM fr ANTI JOIN (SELECT document_number, publication_date FROM tagged WHERE link_source = 'both') carried
             USING (document_number, publication_date)
        WHERE fr._stated IS NOT NULL
        ORDER BY docket_id, publication_date DESC
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """
    con.execute(query)
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FR docket links: {:,} rows", rows)

    return out_file
