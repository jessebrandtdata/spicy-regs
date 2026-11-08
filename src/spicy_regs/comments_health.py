"""Read-only checks shared by comments publication and operational monitoring."""

from __future__ import annotations

from collections.abc import Mapping
from uuid import uuid4


def check_comments(con, comments: str, index: str | Mapping[str, str], *, limit: int = 20) -> list[str]:
    """Check raw IDs and every agency/docket/month count, including NULL dockets/dates.

    The arguments are caller-owned SQL relations, never user SQL. Use the raw
    catalog or public files: a consumer's deduplicating view hides corruption.
    ``index`` may name several indexes by label; the rows are counted once and
    each index must equal the count.
    """
    rows, ids, missing_coordinates = con.execute(f"""
        SELECT count(*), count(DISTINCT comment_id),
               count(*) FILTER (WHERE comment_id IS NULL OR agency_code IS NULL)
        FROM ({comments})
    """).fetchone()
    errors = []
    if rows != ids or missing_coordinates:
        errors.append(f"comments: {rows:,} rows, {ids:,} unique IDs, {missing_coordinates:,} incomplete identities")
    indexes = {"": index} if isinstance(index, str) else index
    actual = f"comments_health_actual_{uuid4().hex}"
    con.execute(f"""
        CREATE TEMP TABLE {actual} AS
        SELECT agency_code, trim(docket_id, '"') AS docket_id,
               extract(year FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS year,
               extract(month FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS month,
               count(*) AS row_count
        FROM ({comments}) GROUP BY 1, 2, 3, 4
    """)
    try:
        for label, sql in indexes.items():
            mismatches = con.execute(f"""
                WITH expected AS (
                    SELECT agency_code, docket_id, year, month, sum(row_count) AS row_count
                    FROM ({sql}) GROUP BY 1, 2, 3, 4
                )
                SELECT coalesce(e.agency_code, a.agency_code), coalesce(e.docket_id, a.docket_id),
                       coalesce(e.year, a.year), coalesce(e.month, a.month), e.row_count, a.row_count
                FROM expected e FULL OUTER JOIN {actual} a
                  ON e.agency_code IS NOT DISTINCT FROM a.agency_code
                 AND e.docket_id IS NOT DISTINCT FROM a.docket_id
                 AND e.year IS NOT DISTINCT FROM a.year
                 AND e.month IS NOT DISTINCT FROM a.month
                WHERE e.row_count IS DISTINCT FROM a.row_count
                ORDER BY 1, 2, 3, 4 LIMIT {int(limit)}
            """).fetchall()
            name = f"{label} index" if label else "index"
            errors.extend(f"comments coverage {agency}/{docket}/{year}-{month}: {name}={expected}, actual={found}"
                          for agency, docket, year, month, expected, found in mismatches)
    finally:
        con.execute(f"DROP TABLE IF EXISTS {actual}")
    return errors


def check_retained_ids(con, previous: str, candidate: str) -> list[str]:
    """Refuse loss of previously published identities before replacing the mirror."""
    missing = con.execute(f"""
        SELECT count(*) FROM (
            SELECT comment_id FROM ({previous})
            EXCEPT SELECT comment_id FROM ({candidate})
        )
    """).fetchone()[0]
    return [f"comments export would discard {missing:,} previously published IDs"] if missing else []
