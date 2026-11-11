"""Publish the compatible comments mirror once per verified catalog snapshot."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import json
from os import environ, getenv
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.comments_health import check_comments, check_retained_ids
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, r2, publication
from spicy_regs.pipelines.comments_generation import TABLES, build_comments_generation
from spicy_regs.transforms.partition_comments import agency_comments

FORMAT_VERSION = 1
RECEIPT_KEY = "comments-publication.json"
MIN_EXPECTED_ROWS = 1_000_000
_AGENCY_KEY = re.compile(r"comments/agency/agency_code=[A-Za-z0-9_-]+/part-0\.parquet\Z")


# The next two check the one-shot v1 promotion's prepared pair (scripts/prepare_comments_selected_export.py and the
# remote staging in comments_generation). That route published receipts, which the mirror no longer does (ss-juz8);
# they leave with it once a receipts-free publication has run.
def _check_prepared_snapshot(reader, captured_snapshot):
    """Observe live state; allow only equality or unchanged rows in both tables."""
    observed_snapshot = reader.snapshot()
    if (observed_snapshot != captured_snapshot
            and reader.rows_unchanged_since(captured_snapshot) is not True):
        raise RuntimeError("Selected comments pair changed since capture")
    return observed_snapshot


def _validate_prepared_export(output_dir, result, snapshot):
    """Bind canonical checked/sealed/uploaded members to one derived manifest."""
    canonical = {
        "comments": output_dir / "comments.parquet",
        "index": output_dir / "comments_index.parquet",
        "partitions": output_dir / "comments" / "agency",
        "receipts": output_dir / ".catalog-pairs" / "comments" / "etl_receipts.parquet",
        "generation": output_dir / ".catalog-pairs" / "comments" / "generation.json",
        "manifest": output_dir / "comments-prepared-export.json",
    }
    if set(result) != set(canonical):
        raise ValueError("Prepared comments members differ from the canonical export")
    for name, path in canonical.items():
        if (Path(result[name]).absolute() != path.absolute()
                or not path.resolve().is_relative_to(output_dir.resolve())):
            raise ValueError("Prepared comments member is outside its canonical output path")
    manifest = json.loads(canonical["manifest"].read_text())
    if manifest.get("purpose") not in {None, "native-generation"}:
        raise ValueError("Prepared comments publication purpose differs")
    native_only = manifest.get("purpose") == "native-generation"
    if native_only and canonical["partitions"].exists():
        raise ValueError("Native-only preparation must not contain legacy agency files")
    derivation = json.loads(canonical["generation"].read_text())
    if (manifest.get("format") != "comments-prepared-export/1"
            or manifest.get("source") != asdict(snapshot)
            or manifest.get("derivation") != derivation
            or derivation.get("dataset") != "comments"
            or derivation.get("snapshot") != asdict(snapshot)
            or derivation.get("rows") != 26_418_078):
        raise ValueError("Prepared comments derivation differs from the full selected pair")
    from spicy_regs.transforms.regulations_receipts import policy
    from spicy_regs.etl_receipts import receipt_policies
    current = policy("comments")
    earlier = [p for p in receipt_policies(current) if p.policy_version == "regulations-native-v1"]
    original = derivation.get("original_pair", {})
    if (len(earlier) != 1 or original.get("policy") != earlier[0].descriptor()
            or derivation.get("current_policy") != current.descriptor()
            or original.get("metadata", {}).get("snapshot") != asdict(snapshot)):
        raise ValueError("Prepared comments policies or original pair differ")
    files = [canonical[name] for name in ("comments", "index", "receipts", "generation")]
    files += sorted(canonical["partitions"].glob("agency_code=*/part-0.parquet"))
    expected = {path.relative_to(output_dir).as_posix(): path for path in files}
    if set(manifest.get("members", {})) != set(expected):
        raise ValueError("Prepared comments manifest membership differs")
    for name, path in expected.items():
        if not path.resolve().is_relative_to(output_dir.resolve()):
            raise ValueError("Prepared comments member resolves outside the output")
        actual = publication.file_identity(path)
        if manifest["members"][name] != {
            "sha256": actual["sha256"].removeprefix("sha256:"), "byteSize": actual["bytes"]
        }:
            raise ValueError("Prepared comments bytes differ from their manifest")
    if pq.ParquetFile(canonical["comments"]).metadata.num_rows != 26_418_078:
        raise ValueError("Prepared comments population is incomplete")
    return native_only


@dataclass(frozen=True)
class PublicPredecessor:
    etag: str
    agencies: frozenset[str]


def validate_export(output_dir: Path, previous_url: str, *,
                    resources: ExportResources | None = None, candidate_builder=None) -> PublicPredecessor:
    """Keep every prior ID; refuse a moving predecessor and inconsistent coverage."""
    resources = resources or ExportResources()
    before = publication.public_object_version(previous_url)
    if before is None:
        raise RuntimeError("Prior comments mirror is missing; cannot verify its population")
    with TemporaryDirectory(prefix="comments-check-", dir=output_dir) as spill, duckdb.connect() as con:
        resources.configure(con, Path(spill))
        if candidate_builder is None:
            con.from_parquet(str(output_dir / "comments.parquet")).create_view("candidate")
            con.from_parquet(str(output_dir / "comments_index.parquet")).create_view("candidate_index")
        else:
            # The remote writer supplies narrow, pinned candidate relations;
            # every predecessor and health check below remains identical.
            candidate_builder(con)
        con.from_parquet(previous_url).create_view("previous")
        errors = check_comments(con, "SELECT * FROM candidate", "SELECT * FROM candidate_index")
        errors += check_retained_ids(con, "SELECT * FROM previous", "SELECT * FROM candidate")
        if errors:
            raise RuntimeError("Refusing comments publication: " + "; ".join(errors))
        agencies = frozenset(row[0] for row in con.execute("SELECT DISTINCT agency_code FROM previous").fetchall())
        if any(not isinstance(a, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", a) for a in agencies):
            raise RuntimeError("Prior comments mirror has invalid agency codes")
    if publication.public_object_version(previous_url) != before:
        raise RuntimeError("Prior comments mirror changed during validation; retry on a stable predecessor")
    return PublicPredecessor(before["etag"], agencies)


def _retain_empty_agencies(result: dict[str, Path], predecessor: PublicPredecessor, receipt: dict | None) -> None:
    """Clear old fixed agency URLs when their last row moves to another agency."""
    agencies = set(predecessor.agencies)
    # Carry forward earlier empty replacements as well as the current parent.
    if isinstance(receipt, dict) and isinstance(receipt.get("files"), dict):
        agencies.update(key.split("agency_code=", 1)[1].split("/", 1)[0]
                        for key in receipt["files"] if _AGENCY_KEY.fullmatch(key))
    schema = pq.ParquetFile(result["comments"]).schema_arrow
    schema = pa.schema([field for field in schema if field.name != "agency_code"])
    for agency in sorted(agencies):
        path = result["partitions"] / f"agency_code={agency}" / "part-0.parquet"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_batches([], schema=schema), path, compression="zstd")


@contextmanager
def _agency_files_may_shrink():
    """Lift ``r2``'s shrink guard for the fixed agency files alone, restoring the caller's setting after.

    An agency file may legitimately shrink: re-sorted and re-compressed it can be several times smaller while
    holding more rows (ACF 19.7 MB to 6.0 MB), and one whose last row moved agency is emptied. The monolith and
    the generation's members are not exempt, so the bypass is no longer the workflow's environment (ss-juz8). The
    guard reads only ``R2_ALLOW_SHRINK``; an ``allow_shrink`` argument on ``upload_file``, ``preflight_uploads``
    and ``upload_comment_partitions`` would replace this.
    """
    held = environ.get("R2_ALLOW_SHRINK")
    environ["R2_ALLOW_SHRINK"] = "1"
    try:
        yield
    finally:
        if held is None:
            environ.pop("R2_ALLOW_SHRINK", None)
        else:
            environ["R2_ALLOW_SHRINK"] = held


def check_receipt_rows(output_dir: Path, files: dict[str, dict], exported_rows: int) -> None:
    """Refuse a receipt whose ``rows`` the published files do not bear out.

    The server states ``rows`` from the receipt without opening a file, so each must be the sum of its own file's row
    groups; the monolith's must be the count the export validated; and the agency files must sum to it, the rule
    :func:`_receipt_matches` holds a published receipt to. It runs on the local files before any upload, so a refusal
    leaves the public files and their receipt describing the same generation.
    """
    errors = []
    for key, item in files.items():
        metadata = pq.ParquetFile(output_dir / key).metadata
        grouped = sum(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups))
        if item["rows"] != grouped:
            errors.append(f"{key} states {item['rows']:,} rows and its row groups hold {grouped:,}")
    if files["comments.parquet"]["rows"] != exported_rows:
        errors.append(f"comments.parquet states {files['comments.parquet']['rows']:,} rows; the export validated "
                      f"{exported_rows:,}")
    agencies = sum(item["rows"] for key, item in files.items() if _AGENCY_KEY.fullmatch(key))
    if agencies != exported_rows:
        errors.append(f"the agency files hold {agencies:,} rows; the export validated {exported_rows:,}")
    if errors:
        raise RuntimeError("Refusing the comments receipt: " + "; ".join(errors))


def receipt_files(receipt: dict | None) -> dict[str, dict] | None:
    """The receipt's file descriptors when they meet the published-receipt rules, else ``None``.

    Each names a fixed mirror key with a stored version and the digest the publisher read back; the agency files
    hold the monolith's rows. A receipt failing these rules binds nothing.
    """
    files = receipt.get("files") if isinstance(receipt, dict) else None
    if not isinstance(files, dict) or not {"comments.parquet", "comments_index.parquet"} < files.keys():
        return None
    for key, item in files.items():
        if key not in {"comments.parquet", "comments_index.parquet"} and not _AGENCY_KEY.fullmatch(key):
            return None
        if not publication.comments_file_record(item) or item["bytes"] == 0:
            return None
    if files["comments.parquet"]["rows"] < MIN_EXPECTED_ROWS or files["comments_index.parquet"]["rows"] < 1:
        return None
    if sum(item["rows"] for key, item in files.items() if _AGENCY_KEY.fullmatch(key)) != files["comments.parquet"]["rows"]:
        return None
    return files


def receipt_source(receipt: dict | None) -> iceberg.CatalogPairSnapshot | None:
    """The catalog pair a receipt was exported from; ``None`` for a receipt that predates pair snapshots."""
    source = receipt.get("source") if isinstance(receipt, dict) else None
    if not isinstance(source, dict):
        return None
    try:
        receipts = iceberg.CatalogSnapshot(**source["receipts"])
        return iceberg.CatalogPairSnapshot(source["table_uuid"], int(source["snapshot_id"]), int(source["schema_id"]),
                                           receipts)
    except (KeyError, TypeError, ValueError):
        return None


def _receipt_matches(receipt: dict | None, snapshot: iceberg.CatalogPairSnapshot, base_url: str) -> bool:
    """A successful receipt plus unchanged public/storage versions permits a no-op.

    The comments table's identity is compared, not the shared receipt table's: nothing of that table is published
    (ss-juz8), so a receipt-only commit leaves the publication current.
    """
    if not isinstance(receipt, dict) or receipt.get("format_version") != FORMAT_VERSION:
        return False
    source = receipt_source(receipt)
    if source is None or source.subject() != snapshot.subject():
        return False
    generation = receipt.get("generation")
    if not isinstance(generation, dict):
        return False
    if publication.current_index(base_url)["families"].get("comments") != generation:
        return False
    files = receipt_files(receipt)
    if files is None:
        return False

    def stored(entry: tuple[str, dict]) -> bool:
        key, item = entry
        return r2.object_version(key) == {name: item[name] for name in ("etag", "bytes")}

    with ThreadPoolExecutor(max_workers=8) as pool:
        if not all(list(pool.map(stored, files.items()))):
            return False
    return not publication.public_version_drift({f"{base_url}/{key}": item for key, item in files.items()})


def publish_comments_mirror(output_dir: Path, *, resources: ExportResources | None = None,
                            skip_upload: bool = False, force: bool = False) -> bool:
    """Build/verify/publish; return False only for a verified unchanged publication.

    The caller holds comments-catalog-write for the whole operation. Fixed public
    keys are not an atomic generation. Advance the receipt only after all files
    upload and their public bytes match; any interruption stays retryable.

    The generation carries no ETL receipts (ss-juz8). Both tables' installed policies must be marked receipts
    retired (``subject_catalog.RECEIPTS_RETIRED``), the one owner of that (ss-ag1j): admission and publication then
    accept the generation and refuse receipts for it. The marker is asked first, through the registry's own rule,
    so an unmarked or half-marked family refuses in seconds, not after the export.
    """
    resources = resources or ExportResources()
    rt = RECORD_TYPES["comments"]
    from spicy_regs.etl_policy_registry import require_registered_receipts
    try:
        require_registered_receipts(set(TABLES), None)
    except ValueError as error:
        raise RuntimeError("The installed comments policies still require ETL receipts; mark both receipts_retired "
                           "before publishing receipts-free comments (ss-juz8)") from error
    if not skip_upload and not getenv("R2_ACCESS_KEY_ID"):
        raise RuntimeError("R2 upload credentials are required to publish the comments mirror")
    snapshot = iceberg.catalog_snapshot(rt)
    base_url = resolve_r2_base_url().rstrip("/")
    receipt = None
    if not skip_upload:
        receipt = None if force else r2.read_json_object(RECEIPT_KEY)
        if not force and _receipt_matches(receipt, snapshot, base_url):
            logger.info("Comments snapshot {} already published; verified object versions, skipping build", snapshot.snapshot_id)
            return False

    prior_index = publication.current_index(base_url) if not skip_upload else publication.empty_index()
    result = iceberg.export_public_comments(output_dir, rt, resources=resources, snapshot=snapshot)
    n_rows = pq.ParquetFile(result["comments"]).metadata.num_rows
    if n_rows < MIN_EXPECTED_ROWS:
        raise RuntimeError(f"Comments export has {n_rows:,} rows, below the {MIN_EXPECTED_ROWS:,} safety floor")
    previous_url = f"{base_url}/comments.parquet"
    predecessor = validate_export(output_dir, previous_url, resources=resources)
    _retain_empty_agencies(result, predecessor, receipt)
    agency_files = sorted(result["partitions"].glob("agency_code=*/part-0.parquet"))
    with TemporaryDirectory(prefix="comments-check-", dir=output_dir) as spill, duckdb.connect() as con:
        resources.configure(con, Path(spill))
        agency_comments(con, agency_files).create_view("partitions")
        con.from_parquet(str(result["index"])).create_view("candidate_index")
        con.from_parquet(str(result["comments"])).create_view("candidate")
        errors = check_comments(con, "SELECT * FROM partitions", "SELECT * FROM candidate_index")
        errors += check_retained_ids(con, "SELECT * FROM candidate", "SELECT * FROM partitions")
        if errors:
            raise RuntimeError("Invalid comments partitions: " + "; ".join(errors))
    build = {
        "format_version": FORMAT_VERSION, "source": asdict(snapshot),
        "resources": asdict(resources), "rows": n_rows, "predecessor_etag": predecessor.etag,
    }
    (output_dir / "comments-build.json").write_text(json.dumps(build, indent=2, sort_keys=True) + "\n")
    generation_directory = build_comments_generation(output_dir, result)
    if skip_upload:
        logger.info("Verified local comments generation in {}", generation_directory)
        return True

    # Compaction may move the table past its pin without changing a row; any other commit refuses.
    if not iceberg.rows_unchanged_since(rt, snapshot):
        raise RuntimeError("Catalog changed during export; refusing publication")
    current = publication.public_object_version(previous_url)
    if current is None or current["etag"] != predecessor.etag:
        raise RuntimeError("Prior comments mirror changed before publication; refusing overwrite")
    files = [result["comments"], *agency_files, result["index"]]
    rows = {path.relative_to(output_dir).as_posix(): {"rows": pq.ParquetFile(path).metadata.num_rows} for path in files}
    check_receipt_rows(output_dir, rows, n_rows)
    # Every planned file is guarded before the first byte lands; only the agency files (and the index uploaded
    # with them) may shrink, the monolith here and the generation's members at publication may not.
    r2.preflight_uploads(output_dir, [result["comments"]])
    with _agency_files_may_shrink():
        r2.preflight_uploads(output_dir, [*agency_files, result["index"]])
    r2.upload_file(result["comments"], remote_key="comments.parquet")
    with _agency_files_may_shrink():
        r2.upload_comment_partitions(output_dir, agency_files)

    def verify(path: Path) -> tuple[str, dict]:
        key = path.relative_to(output_dir).as_posix()
        return key, {**r2.verify_public_file(path, key, base_url), **rows[key]}

    with ThreadPoolExecutor(max_workers=4) as pool:
        descriptors = dict(pool.map(verify, files))
    published_index = publication.publish_generation(
        generation_directory, client=r2.get_r2_client(), bucket=getenv("R2_BUCKET_NAME", "spicy-regs"),
        prior_index=prior_index)
    receipt = {"format_version": FORMAT_VERSION, "source": asdict(snapshot), "files": descriptors,
               "generation": published_index["families"]["comments"]}
    receipt_path = output_dir / RECEIPT_KEY
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    r2.upload_file(receipt_path, remote_key=RECEIPT_KEY, cache_control="no-cache, must-revalidate")
    r2.verify_public_file(receipt_path, RECEIPT_KEY, base_url)
    logger.info("Published and read back comments snapshot {}", snapshot.snapshot_id)
    return True
