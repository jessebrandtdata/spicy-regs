# Container image for the Spicy Regs MCP server (deployed to Cloudflare
# Containers; also runs anywhere that serves an $PORT HTTP container).
#
# Runs the CANONICAL server (src/spicy_regs/mcp_server.py:build_app) directly —
# not the Vercel copy. The server needs only the runtime packages below, never
# the other ETL deps (boto3, polars), so we install the package with --no-deps
# and add just those, keeping the image small and cold starts fast. This is why
# the container can be the single source of truth once Vercel is retired.
FROM python:3.12-slim

# duckdb needs no system libs; keep the image minimal. Unbuffered stdout so the
# platform captures logs live.
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Runtime deps only, each at uv.lock's version: pip resolves the roots below with
# the lock as constraints, and lock_pins.py check fails the build if any installed
# package differs from the lock or is not in it. Round 6 found the image running
# 22 packages the tests never ran (pydantic 2.13, starlette 1.7 against the
# lock's 2.12.5 and 0.52.1), because only mcp and duckdb were pinned; the server's
# argument errors and its HTTP retry schedule come from these packages.
# uvicorn serves the ASGI app; its [standard] extras (uvloop, httptools, websockets,
# watchfiles) are not in the lock, so the image runs uvicorn's default asyncio
# loop and h11, as the tests do. loguru: the publication index reader the server
# imports to resolve fork generations logs through it (found by the 2026-09-25
# local smoke). httpx: that reader's client, which mcp 2 (on httpx2) no longer
# brings. pytz: DuckDB imports it to return a TIMESTAMPTZ value (found by the
# 2026-09-28 persona re-run). pyarrow: a connection that selects a native
# document_citations or a native campaign-finance view input restores rows from
# receipts with the receipt writer's own code (citation_receipts,
# fec_receipt_adapter, etl_receipts), which imports it inside the call, as does
# read_receipt_fields (receipt_lookup). Without
# it every request failed once such a family published (found 2026-10-05 by
# running those calls with only this image's packages); about 117 MB installed
# beside the 93 MB the others take. The lock and the check stay in the image,
# so the binding step can rerun the check inside the pushed image.
COPY uv.lock deploy/cloudflare/lock_pins.py deploy/cloudflare/runtime_readers.py /app/lock/
RUN python /app/lock/lock_pins.py constraints /app/lock/uv.lock > /app/lock/constraints.txt \
    && pip install -c /app/lock/constraints.txt mcp duckdb uvicorn loguru httpx pytz pyarrow && pip check

# Install the package code without its declared (ETL) dependencies. mcp_server
# imports only the local _icon module plus mcp/duckdb, so this is sufficient and
# keeps boto3/polars/etc out of the image (so pip check runs before it).
COPY pyproject.toml README.md ./
COPY src ./src
COPY packaging ./packaging
COPY data_dictionary ./data_dictionary
RUN pip install --no-deps . && python /app/lock/lock_pins.py check /app/lock/uv.lock
# Exercise native receipt selection before checking the other runtime imports.
RUN python /app/lock/runtime_readers.py
# Fail the build, not the first request, when a module the server imports at
# request time needs a package this image does not install. pyarrow,
# etl_receipts and etl_policy_registry are imported inside calls, which
# importing the calling module does not run, so they are named here.
RUN python -c "import spicy_regs.mcp_server, spicy_regs.sources.publication, spicy_regs.local_data; import pyarrow, spicy_regs.etl_receipts, spicy_regs.etl_policy_registry, spicy_regs.fec_receipt_adapter, spicy_regs.citation_receipts, spicy_regs.receipt_lookup; import spicy_regs.subject_catalog; assert spicy_regs.subject_catalog.policies(); import spicy_regs.relationship_views, spicy_regs.citation_resolution, spicy_regs.acquisition_queue; from spicy_regs.vocabulary_mapping import lookup_agency; assert lookup_agency('regulations.gov:agency', 'OPM')['status'] == 'reviewed_mapping'; import duckdb; duckdb.sql(\"SELECT TIMESTAMPTZ '2026-01-01 00:00:00+00'\").fetchall()"

# The platform sends traffic to $PORT (default 8080). build_app() mounts the MCP
# endpoint at /mcp; --factory because build_app is a callable returning the app.
ENV PORT=8080
EXPOSE 8080
# Shared with the Worker entrypoint override, including when an old image starts
# during rollout. The guard execs the server, preserving PID 1 stop signals.
COPY deploy/cloudflare/start_reader.py /app/lock/start_reader.py
CMD ["python", "/app/lock/start_reader.py"]
