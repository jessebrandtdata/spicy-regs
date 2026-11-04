# Spicy Regs Data Dictionary

This is the schema reference for the **Spicy Regs** dataset — an open mirror of
[regulations.gov](https://www.regulations.gov) federal regulatory data,
published as Apache Parquet on a public Cloudflare R2 bucket.

It documents the tables supported by this checkout, column by column. The schema
is generated from code, and the offline check keeps those declarations and their
descriptions aligned. Availability and the schema actually served must be checked
against public artifacts separately. A local run or `measured_on` date does not
establish publication. See the [publication observation](publication-status.md).

## Where the data comes from

```
regulations.gov  →  Mirrulations S3 mirror  →  Spicy Regs ETL  →  Parquet on R2
                                                                   (data.{{ SPICYREGS_DOMAIN }})
```

The ETL flattens the raw regulations.gov JSON into a handful of flat tables and
publishes them, plus small pre-computed rollups, to `{{ R2_PUBLIC_URL }}`.
Alongside them it ingests a set of **complementary federal data sources** — the
Federal Register, the Unified Agenda, Congress.gov, the CFR, SAM.gov, lobbying
disclosures, the FEC, USASpending, federal-court litigation, and GAO/CRS reports
— so the rulemaking lifecycle, the organizations that engage in it, and its
downstream context can all be queried from one place.

## The tables

When published, a table is available as `{{ R2_PUBLIC_URL }}/<name>.parquet` and
is queryable through the MCP server (`list_sources` / `describe_table` /
`query_sql`).

### Core regulations.gov tables

| Table | Grain | Key |
| --- | --- | --- |
| [`dockets`](tables/dockets.md) | one row per docket | `docket_id` |
| [`documents`](tables/documents.md) | one row per document | `document_id` |
| [`comments`](tables/comments.md) | one row per public comment | `comment_id` |
| [`comments_index`](tables/comments_index.md) | one row per agency, docket and posting month | — |

### Rollups (pre-aggregated views of the core tables)

| Table | Grain |
| --- | --- |
| [`feed_summary`](tables/feed_summary.md) | one row per docket |
| [`agency_stats`](tables/agency_stats.md) | one row per agency |
| [`agency_monthly_volume`](tables/agency_monthly_volume.md) | one row per agency / month / document type |

### Rulemaking lifecycle (external sources)

| Table | Grain | Key |
| --- | --- | --- |
| [`federal_register`](tables/federal_register.md) | one row per dated Federal Register record | `document_number`, `publication_date` |
| [`unified_agenda`](tables/unified_agenda.md) | one row per RIN per agenda edition | `rin` |
| [`congress_bills`](tables/congress_bills.md) | one row per bill | `bill_id` |
| [`cfr_sections`](tables/cfr_sections.md) | one row per CFR granule | `granule_id` |
| [`fcc_proceedings`](tables/fcc_proceedings.md) | one row per FCC proceeding (docket) | `name` |
| [`fcc_filings`](tables/fcc_filings.md) | one row per FCC ECFS filing (comment) | `id_submission` |

### Rulemaking dataset (derived, one snapshot)

These tables publish together as one snapshot under
`materialized/rulemaking/latest.json`, not at `<name>.parquet`; the MCP server
reads the snapshot that pointer names.

| Table | Grain | Key |
| --- | --- | --- |
| [`rule_targets`](tables/rule_targets.md) | one docket, CFR and RIN edge per evidence class | `docket_id` + `cfr_ref` + `rin` + `source` |
| [`proceedings`](tables/proceedings.md) | one rulemaking proceeding | `proceeding_id` |
| [`regulatory_agenda_items`](tables/regulatory_agenda_items.md) | one agenda item per RIN | `agenda_item_id` |
| [`agenda_item_proceedings`](tables/agenda_item_proceedings.md) | one evidence link from an agenda item to a proceeding | `relationship_id` |
| [`comment_periods`](tables/comment_periods.md) | one continuous public-comment interval | `comment_period_id` |
| [`rulemaking_lifecycles`](tables/rulemaking_lifecycles.md) | one lifecycle per docketed proceeding | `proceeding_id` |
| [`lifecycle_events`](tables/lifecycle_events.md) | one stage event per document of a proceeding | `proceeding_id` + `document_id` |
| [`agency_lifecycle_stats`](tables/agency_lifecycle_stats.md) | one agency (or all) and stratum | `agency_code` + `stratum` |

### Organizations & influence

| Table | Grain | Key |
| --- | --- | --- |
| [`sam_entities`](tables/sam_entities.md) | one row per SAM-registered entity | `uei` |
| [`lobbying_filings`](tables/lobbying_filings.md) | one row per LDA filing | `filing_uuid` |
| [`fec_committees`](tables/fec_committees.md) | one row per FEC committee / PAC | `committee_id` |
| [`org_committee_links`](tables/org_committee_links.md) | one row per (commenter org name, FEC committee) match | `organization` + `committee_id` |

For selected native FEC records, collection coverage and reported relationships,
see [FEC integration](fec-integration.md) and the
[R2 delivery checklist](fec-delivery-plan.md). The
[coverage limits and backlog](fec-gaps.md) and
[coverage census](research/fec-coverage-2026-09-21.md) preserve source gaps and
optional expansion separately from the delivered selection.

For the complete fork delivery, see the [generation plan](fork-generation.md)
and [local reuse inventory](research/local-data-reuse-2026-09-21.md), which cover
all rollups and distinguish usable inputs from incomplete or defective outputs.

### Outcomes & context

| Table | Grain | Key |
| --- | --- | --- |
| [`usaspending_recipients`](tables/usaspending_recipients.md) | one row per federal-award recipient | `recipient_id` |
| [`court_dockets`](tables/court_dockets.md) | one row per federal-court docket | `cl_docket_id` |
| [`gao_reports`](tables/gao_reports.md) | one row per GAO report | `report_id` |
| [`gao_recommendations`](tables/gao_recommendations.md) | one row per GAO recommendation per agency, kept once it closes | `recommendation_id` |
| [`crs_reports`](tables/crs_reports.md) | one row per CRS report | `report_id` |

## How the tables relate

The three core tables form a simple hierarchy keyed by id:

```
dockets (docket_id)
  └── documents (document_id, docket_id →)
  └── comments  (comment_id,  docket_id →)
```

- `documents.docket_id` and `comments.docket_id` reference `dockets.docket_id`.
- `agency_code` appears on every table and is the join key for the agency rollups.
- The rollups (`comments_index`, `feed_summary`, `agency_stats`,
  `agency_monthly_volume`) are pre-aggregated views built from the three core
  tables so consumers don't have to scan the tens-of-millions-of-rows comments
  dataset.

The complementary sources are **reference tables** rather than strict children
of `dockets`; they join to the corpus (and to each other) on a few shared keys:

- **RIN** (Regulation Identifier Number) links `unified_agenda` (the planned
  action) to `federal_register.regulation_id_numbers_json` (the published rule).
- **CFR citations** link `cfr_sections` to `federal_register.cfr_references_json`
  and `unified_agenda.cfr_references_json` (the codified text a rule amends).
- **Docket IDs** in `federal_register.docket_ids_json` tie FR documents back to
  regulations.gov dockets.
- **UEI** (Unique Entity ID) links `sam_entities` and `usaspending_recipients`,
  and is the anchor for resolving commenter/organization names to a canonical
  entity.
- **Organization name** bridges the softer influence sources — `lobbying_filings`
  (registrant/client), `fec_committees`, and comment filers — where no shared id
  exists. For `fec_committees` that bridge is now materialized:
  [`org_committee_links`](tables/org_committee_links.md) resolves
  `comments.organization` to `committee_id` once, with `match_method` /
  `confidence` on every row, so consumers stop re-inventing the normalization.
- **`agency_code` / agency name** appears across nearly every table.

> Coverage notes: `sam_entities` covers the active public registry (~885K rows;
> chunked ingestion walks SAM's bulk extract by `registrationDate` year window
> across runs), `lobbying_filings` covers 2024-onward, `usaspending_recipients`
> is the top ~100K recipients by award amount, `org_committee_links` is bounded
> by the ~0.08% of comments that carry an `organization` value at all, and
> `gao_reports` holds GovInfo's closed 1989–2008 GAO archive, GAO's
> recent-items RSS window (it grows as the daily job runs), and, for each year
> or month a walk of GAO's own Month in Review has finished, that listing.
> Each table page notes its own scope.

## How to query it

=== "AI assistant (MCP)"

    The hosted MCP server exposes `list_sources`, `describe_table`, and
    `query_sql`. `list_sources` names each subject's tables and the declared
    outputs with no available view; `list_sources(subject)` gives that
    subject's tables with their labels, coverage and pinned rows. `describe_table` includes actual columns, field
    meanings, declared identifiers and coverage caveats; a listed definition
    does not establish that its data is published. Add
    `https://mcp.{{ SPICYREGS_DOMAIN }}/mcp` as a connector, or run it locally:

    ```bash
    claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp
    ```

=== "CLI"

    ```bash
    uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download
    uv run spicy-regs stats
    ```

=== "DuckDB (SQL)"

    ```sql
    INSTALL httpfs; LOAD httpfs;
    SELECT agency_code, COUNT(*) AS dockets
    FROM read_parquet('{{ R2_PUBLIC_URL }}/dockets.parquet')
    GROUP BY agency_code
    ORDER BY dockets DESC
    LIMIT 20;
    ```

=== "Python"

    ```python
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(
        "SELECT agency_code, docket_count "
        "FROM read_parquet('{{ R2_PUBLIC_URL }}/agency_stats.parquet') "
        "ORDER BY docket_count DESC LIMIT 20"
    ).df()   # -> pandas DataFrame
    ```

    See **[Querying with Python](querying-python.md)** for a full walkthrough,
    including cross-source joins (RIN, UEI) and working with the large
    `comments` table.

!!! note "Keeping this current"
    Column names and types are the source of truth in code
    (`RECORD_TYPES` for the core tables, the spicy-docs contracts for the hosted
    tables, each transform's own declaration for the rest, read by
    `expected_schemas()` in `data_dictionary.py`). The
    prose lives in `data_dictionary/descriptions.yaml`. Run
    `uv run spicy-regs-dict generate` to rebuild the table pages, and
    `uv run spicy-regs-dict check` to verify the two are in sync — the same
    check runs in CI on every pull request.
