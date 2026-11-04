"""Check a published source family through public files and generic hosted MCP.

Expected counts come from the qualified preparation, never a fixed publisher
list. Exact public member pins establish equality to the admitted generation;
hosted counts and attributed values independently check the consumer route.
"""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import file_digest, sha256
import json
from pathlib import Path

import duckdb
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
import pyarrow.parquet as pq

from spicy_regs.scorecards.operations.results import query_rows, write
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.scorecards.etl import SOURCE_NAMES
from spicy_regs.scorecards.inputs import stage_member
from spicy_regs.sources import publication
from spicy_regs import receipt_lookup
from spicy_regs.mcp_server import _jsonify


_ATTRIBUTION_FIELDS = """p.publisher_id,p.name AS publisher,c.scorecard_id,
c.title AS scorecard,m.member_name,metric.name AS metric,r.value_text AS published_value,
CAST(r.value_number AS VARCHAR) AS numeric_value,p.scorecard_index_url AS publisher_url"""
_ATTRIBUTION_FROM = """FROM scorecard_member_ratings r JOIN scorecards c USING(scorecard_id)
JOIN scorecard_publishers p USING(publisher_id)
JOIN scorecard_members m ON m.scorecard_id=r.scorecard_id
 AND m.publisher_member_key=r.publisher_member_key
JOIN scorecard_metrics metric ON metric.scorecard_id=r.scorecard_id AND metric.metric_id=r.metric_id"""
ATTRIBUTION = f"""SELECT {_ATTRIBUTION_FIELDS}
{_ATTRIBUTION_FROM}
QUALIFY row_number() OVER(PARTITION BY p.publisher_id ORDER BY c.scorecard_id,
 r.publisher_member_key,r.metric_id)=1 ORDER BY p.publisher_id"""


def adoption_scopes(prepared):
    scopes = prepared.get("accepted_scopes")
    if (not isinstance(scopes, list) or not scopes or len(scopes) > 500
            or any(not isinstance(scope, str) or not scope for scope in scopes) or len(set(scopes)) != len(scopes)):
        raise ValueError("Source-adoption readback requires exact distinct accepted scopes (at most 500)")
    return sorted(scopes)


def local_rows(connection, sql):
    result = connection.execute(sql)
    columns = [column[0] for column in result.description]
    return [_jsonify(dict(zip(columns, row, strict=True))) for row in result.fetchall()]


def public_files(args, prepared, authenticated):
    index = publication.load_index(args.public_url)
    write(args.output / "public-index.json", index)
    family = index["families"]["scorecards"]
    if family != authenticated["family_entry"] or family["artifactDigest"] != prepared["generation"]["artifactDigest"]:
        raise ValueError("Public source pointer differs from the authenticated qualified generation")
    tables = family["tables"]
    from spicy_regs.scorecards.etl import admitted_read_policies
    selected = admitted_read_policies(tuple(prepared["counts"]), columns={
        key.removesuffix(".parquet"): value["columns"] for key, value in tables.items()})
    if set(tables) != {name + ".parquet" for name, policy in selected.items() if not policy.receipt_only}:
        raise ValueError("Public source table membership differs from the qualified preparation")
    paths, downloads = {}, []
    target = args.output / "members"
    target.mkdir()
    for key in tables:
        member = publication.single_member(index, key)
        paths[key.removesuffix(".parquet")] = target / key
        downloads.append((member, target / key))
    receipts = publication.receipt_members(index, dataset="scorecards")
    if len(receipts) != 1:
        raise ValueError("Expected one generation-bound source receipt member")
    paths["etl_receipts"] = target / "etl_receipts.parquet"
    downloads.append((receipts[0], paths["etl_receipts"]))

    def download(entry):
        member, path = entry
        stage_member(member, path, local_inputs=getattr(args, "local_inputs", ()),
                     fetch=lambda selected, target: publication.fetch_member(args.public_url, selected, target, timeout=120))
        rows = pq.ParquetFile(path).metadata.num_rows
        if rows != member.rows:
            raise ValueError("Public Parquet footer count differs from its pin")
        return dict(key=member.key, sha256=member.sha256, bytes=member.byte_size, rows=rows,
                    origin="retained_verified" if path.is_symlink() else "downloaded")

    with ThreadPoolExecutor(max_workers=4) as executor:
        members = list(executor.map(download, downloads))
    connection = duckdb.connect()
    try:
        ExportResources(memory=getattr(args, "memory_limit", "1GB"), threads=1).configure(connection, args.output / "spill")
        connection.execute("SET max_temp_directory_size='32GB'")
        for name, path in paths.items():
            connection.read_parquet(str(path)).create_view(name)
        for name, expected in prepared["counts"].items():
            sql = (
                "SELECT count(*) AS rows FROM etl_receipts WHERE dataset='scorecard_snapshots' AND outcome='observed'"
                if selected[name].receipt_only
                else f'SELECT count(*) AS rows FROM "{name}"'
            )
            if local_rows(connection, sql) != [{"rows": expected}]:
                raise ValueError("Public source count differs from preparation: " + name)
        # The container pins the member; carried receipts retain their original generation_id.
        candidate = Path(prepared["generation_directory"]) / family["etlReceipts"]["key"]
        with candidate.open("rb") as stream:
            digest = "sha256:" + file_digest(stream, "sha256").hexdigest()
        if candidate.stat().st_size != family["etlReceipts"]["byteSize"] or digest != family["etlReceipts"]["sha256"]:
            raise ValueError("Local candidate receipts differ from the admitted public member")
        connection.read_parquet(str(candidate)).create_view("_candidate_scorecard_receipts")
        sql = (
            "SELECT generation_id,dataset,outcome,count(*) AS rows FROM {} "
            "GROUP BY generation_id,dataset,outcome ORDER BY generation_id,dataset,outcome"
        )
        if local_rows(connection, sql.format("etl_receipts")) != local_rows(
            connection, sql.format("_candidate_scorecard_receipts")
        ):
            raise ValueError("Public source receipt origins or outcome counts differ from the pinned candidate")
        write(args.output / "public-files.json", dict(status="passed", generation=prepared["generation"], members=members))
    except BaseException:
        connection.close()
        raise
    return connection, family


async def hosted(args, connection, family):
    expected_pin = family["artifactDigest"]
    calls = []
    async with streamable_http_client(args.mcp_url) as (read, send):
        async with ClientSession(read, send, read_timeout_seconds=300) as session:
            await session.initialize()

            async def call(tool, arguments, label):
                result = await session.call_tool(tool, arguments)
                payload = result.model_dump(by_alias=True, mode="json")
                path = args.output / (label + ".json")
                write(path, payload)
                if payload.get("isError"):
                    raise ValueError("Hosted readback refused " + label)
                body = payload.get("structuredContent")
                if body is None:
                    body = json.loads(next(row["text"] for row in payload["content"] if row["type"] == "text"))
                if body.get("error"):
                    raise ValueError("Hosted readback returned an error for " + label)
                if tool == "query_sql":
                    if body.get("truncated") or body.get("truncated_cells") or not body.get("publication"):
                        raise ValueError("Hosted reply is truncated or lacks publication pins")
                    if any(pin.get("artifact_digest") != expected_pin for pin in body["publication"].values()):
                        raise ValueError("Hosted query uses a different source generation")
                    actual = query_rows(body)
                    if actual != local_rows(connection, arguments["sql"]):
                        raise ValueError("Hosted rows differ from the downloaded qualified members: " + label)
                calls.append(dict(tool=tool, label=label, response_sha256=sha256(path.read_bytes()).hexdigest()))
                return body

            expected_tables = {key.removesuffix(".parquet") for key in family["tables"]}
            listed = await call("list_sources", {}, "list_sources")
            listed_names = {row if isinstance(row, str) else row["table"] for row in listed.get("tables", [])}
            for group in listed.get("subjects", []):
                if expected_tables <= listed_names:
                    break
                # Counts-only servers need a page; older full replies have entries
                # and the names default has strings. Keep all supported readers.
                if "tables" not in group:
                    page = await call("list_sources", {"subject": group["subject"]}, "list_sources_" + group["subject"])
                    group = page["subjects"][0]
                listed_names.update(row if isinstance(row, str) else row["table"] for row in group["tables"])
            if not expected_tables <= listed_names:
                raise ValueError("Hosted source tables are not all listed")
            described = await call("describe_table", {"table": "scorecard_member_ratings"}, "describe_ratings")
            if described["publication"].get("artifact_digest") != expected_pin:
                raise ValueError("Hosted source description has not refreshed")
            actual_columns = [[row["column_name"], row["column_type"]] for row in described["columns"]]
            if actual_columns != family["tables"]["scorecard_member_ratings.parquet"]["columns"]:
                raise ValueError("Hosted rating columns differ from the published native schema")
            for name in sorted(expected_tables):
                key = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
                sql = f'SELECT "{key}",count(*) AS rows FROM "{name}" GROUP BY "{key}" ORDER BY "{key}"'
                await call("query_sql", {"sql": sql, "max_rows": 500}, name + "_scope_counts")
            if getattr(args, "mode", "full") == "source-adoption":
                scopes = adoption_scopes({"accepted_scopes": args.accepted_scopes})
                selected = ",".join("'" + scope.replace("'", "''") + "'" for scope in scopes)
                native = await call("query_sql", {
                    "sql": f"SELECT * FROM scorecards WHERE scorecard_id IN ({selected}) ORDER BY scorecard_id",
                    "max_rows": 500,
                }, "accepted_scorecards")
                if [row["scorecard_id"] for row in query_rows(native)] != scopes:
                    raise ValueError("Native rows do not cover every accepted scorecard")
                rating_scopes = local_rows(connection,
                    f"SELECT DISTINCT scorecard_id FROM scorecard_member_ratings WHERE scorecard_id IN ({selected}) "
                    "ORDER BY scorecard_id")
                native_columns = {name for name, _ in family["tables"]["scorecard_member_ratings.parquet"]["columns"]}
                source_fields = "".join(",r." + field for field in ("capture_id", "source_url", "source_path")
                                        if field in native_columns)
                sql = f"""SELECT {_ATTRIBUTION_FIELDS}{source_fields}
{_ATTRIBUTION_FROM}
WHERE c.scorecard_id IN ({selected})
QUALIFY row_number() OVER(PARTITION BY c.scorecard_id ORDER BY r.publisher_member_key,r.metric_id)=1
ORDER BY c.scorecard_id"""
                sampled = await call("query_sql", {"sql": sql, "max_rows": 500}, "accepted_scope_native_values")
                if [row["scorecard_id"] for row in query_rows(sampled)] != [row["scorecard_id"] for row in rating_scopes]:
                    raise ValueError("Native attributed samples do not cover accepted scorecards with ratings")
                return calls
            await call("query_sql", {"sql": ATTRIBUTION, "max_rows": 500}, "attributed_ratings")
            # An exact-key sample checks native values and source fields using
            # the serving layer's maintained receipt lookup, including read states.
            table = "scorecard_member_ratings"
            keys = ("scorecard_id", "metric_id", "publisher_member_key")
            sample = local_rows(connection,
                "SELECT * FROM scorecard_member_ratings ORDER BY length(CAST(value_number AS VARCHAR)) DESC NULLS LAST, "
                + ",".join(keys) + " LIMIT 1")
            if sample:
                key = {name: sample[0][name] for name in keys}
                where = " AND ".join(name + "='" + value.replace("'", "''") + "'" for name, value in key.items())
                await call("query_sql", {"sql": "SELECT * FROM " + table + " WHERE " + where, "max_rows": 2},
                           "rating_native_values")
                native_columns = {name for name, _ in family["tables"][table + ".parquet"]["columns"]}
                fields = [field for field in ("source_url", "source_path", "capture_id") if field not in native_columns]
                # Current native rows hold their source fields directly; the
                # receipt still checks the original numeric lexeme/read state.
                if not fields:
                    fields = ["value_number"]
                index = {"families": {"scorecards": family}}
                local = {"selected_tables": [table], "receipt_members": {
                    family["prefix"] + "/" + family["etlReceipts"]["key"]:
                        str(args.output / "members/etl_receipts.parquet")}}
                expected = receipt_lookup.read_fields(
                    connection, table=table, keys=[key], fields=fields, index=index, local=local,
                    base_url=args.public_url, entry=described, plain=_jsonify)
                actual = await call("read_receipt_fields", {"table": table, "keys": [key], "fields": fields},
                                    "rating_source_fields")
                if any(actual.get(name) != expected[name] for name in ("table", "identity_fields", "receipts", "keys")):
                    raise ValueError("Hosted rating source fields or read states differ from the pinned public member")
            receipt_description = await call("describe_table", {"table": "etl_receipts"}, "describe_receipts")
            if not receipt_description.get("available"):
                raise ValueError("Hosted shared ETL receipt table is unavailable")
            # Other families share this view; include every origin of this family's source datasets.
            if set(family["etlReceipts"]["datasets"]) != set(SOURCE_NAMES):
                raise ValueError("Source receipt datasets differ from the complete scorecard family")
            datasets = ",".join("'" + name + "'" for name in sorted(SOURCE_NAMES))
            sql = (
                "SELECT generation_id,dataset,outcome,count(*) AS rows FROM etl_receipts "
                f"WHERE dataset IN ({datasets}) GROUP BY generation_id,dataset,outcome ORDER BY generation_id,dataset,outcome"
            )
            # A shared receipt query legitimately returns other family pins.
            result = await session.call_tool("query_sql", {"sql": sql, "max_rows": 500})
            payload = result.model_dump(by_alias=True, mode="json")
            path = args.output / "source_receipt_counts.json"
            write(path, payload)
            if payload.get("isError"):
                raise ValueError("Hosted source receipt query was refused")
            body = payload.get("structuredContent")
            if body is None:
                body = json.loads(next(row["text"] for row in payload["content"] if row["type"] == "text"))
            expected_receipt_pin = {
                "artifact_digest": expected_pin,
                "generation_id": family["etlReceipts"]["generationId"],
                "sha256": family["etlReceipts"]["sha256"],
                "rows": family["etlReceipts"]["rows"],
                "datasets": family["etlReceipts"]["datasets"],
            }
            actual_receipt_pin = (
                body.get("publication", {}).get("etl_receipts", {}).get("families", {}).get("scorecards", {})
            )
            if any(actual_receipt_pin.get(key) != value for key, value in expected_receipt_pin.items()):
                raise ValueError("Hosted shared receipts do not pin the selected scorecards family")
            if body.get("truncated") or query_rows(body) != local_rows(connection, sql):
                raise ValueError("Hosted source receipt outcomes differ from the pinned public member")
            calls.append(
                dict(
                    tool="query_sql",
                    label="source_receipt_counts",
                    response_sha256=sha256(path.read_bytes()).hexdigest(),
                )
            )
    return calls


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--publication-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--public-url", default="https://data.spicygov.ai")
    parser.add_argument("--mcp-url", default="https://mcp.spicygov.ai/mcp")
    parser.add_argument("--memory-limit", default="1GB")
    parser.add_argument("--mode", choices=("full", "source-adoption"), default="full",
                        help="Source adoption checks accepted native scopes without hosted receipt-serving diagnostics")
    parser.add_argument("--local-inputs", type=Path, action="append", default=[],
                        help="Reuse exact pinned members from these retained directories before downloading")
    args = parser.parse_args(argv)
    if args.output.exists():
        raise ValueError("Retain prior observations; choose a fresh readback directory")
    args.output.mkdir(parents=True)
    preparation_bytes = args.preparation.read_bytes()
    prepared = json.loads(preparation_bytes)
    args.accepted_scopes = adoption_scopes(prepared) if args.mode == "source-adoption" else []
    authenticated = json.loads(args.publication_receipt.read_bytes())
    if (
        authenticated["status"] != "published_authenticated_readback"
        or authenticated["preparation_sha256"] != sha256(preparation_bytes).hexdigest()
    ):
        raise ValueError("Publication receipt does not admit this qualified preparation")
    status = "public_files_pending"
    connection = None
    try:
        connection, family = public_files(args, prepared, authenticated)
        status = "public_files_verified_hosted_pending"
        calls = asyncio.run(hosted(args, connection, family))
        write(
            args.output / "summary.json",
            dict(
                status="passed",
                observed_at=datetime.now(UTC).isoformat(),
                generation=prepared["generation"],
                preparation_sha256=sha256(preparation_bytes).hexdigest(),
                publication_receipt_sha256=sha256(args.publication_receipt.read_bytes()).hexdigest(),
                counts=prepared["counts"],
                calls=calls,
                mode=args.mode,
                receipt_serving="passed" if args.mode == "full" else "not_checked",
                accepted_scopes=args.accepted_scopes,
            ),
        )
        print(json.dumps(dict(status="passed", generation=prepared["generation"])))
    except Exception as error:
        write(args.output / "failure.json", dict(status=status, error_type=type(error).__name__, reason=str(error)))
        raise
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
