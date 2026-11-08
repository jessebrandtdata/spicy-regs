"""Retry failed attachment-text reads without replaying raw comment ingestion."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock

import polars as pl
import pyarrow as pa
from loguru import logger
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.schemas import RECORD_TYPES
from spicy_regs.sources import iceberg
from spicy_regs.transforms.comment_text_updates import write_text_updates
from spicy_regs.transforms.derived_text_pool import DerivedTextPool, TextResult

PENDING_TEXT_FILE = "pending_comment_text.parquet"
TEXT_RULE_VERSION = "mirrulations-derived-v1"
_COORDINATES = ["agency_code", "docket_id", "comment_id"]
_COLUMNS = [*_COORDINATES, "posted_date", "text_content", "text_extraction_status"]
_SCHEMA = pa.schema(
    [
        *(
            (name, pa.string())
            for name in [
                *_COORDINATES,
                "posted_date",
                "phase",
                "reason",
                "source_json",
                "rule_version",
                "attempted_at",
            ]
        ),
        ("attempts", pa.int64()),
    ]
)


class PendingCommentText:
    """Persist unresolved text work only after the comment data has committed.

    This checkpoint is separate from the raw-key manifest. Source coordinates,
    failed selection (including ETags when listed), rule version and attempts
    explain each retry. Only failures enter it; valid missing/blank reads do not.
    A fresh run lists again so a changed extraction can recover from a 412.
    """

    def __init__(self, output_dir: Path, *, index=None) -> None:
        from spicy_regs.pipelines.regulatory_publication import restore_checkpoint

        self.path = output_dir / ".processing" / PENDING_TEXT_FILE
        self.rows = {
            row["comment_id"]: row for row in restore_checkpoint(output_dir, "pending_comment_text", index=index)
        }
        self._retry = dict(self.rows)
        self._lock = Lock()

    @classmethod
    def from_receipts(cls, receipt: Path, *, generation_id: str, output_dir: Path):
        """Restore a selected checkpoint without a remote or legacy fallback."""
        from spicy_regs.transforms.regulations_checkpoints import read_checkpoint

        instance = cls.__new__(cls)
        instance.path = output_dir / ".processing" / PENDING_TEXT_FILE
        instance.rows = {
            row["comment_id"]: row
            for row in read_checkpoint("pending_comment_text", receipt, generation_id=generation_id)
        }
        instance._retry = dict(instance.rows)
        instance._lock = Lock()
        return instance

    def save_receipts(self, destination: Path, context) -> Path:
        """Save the active retry set, including successful empty retirement."""
        from spicy_regs.transforms.regulations_checkpoints import write_checkpoint

        with self._lock:
            return write_checkpoint("pending_comment_text", self.rows.values(), destination, context)

    def observe(self, result: TextResult, *, retry: bool = False) -> None:
        row = result.record
        identity = row.get("comment_id")
        if not identity:
            return
        with self._lock:
            # Do not immediately repeat a failure in this run. Inline success
            # needs a persisted-row check before retiring a prior failure: an
            # older raw row can lose the metadata merge. This also protects a
            # crash after a chunk checkpoint but before the final text retry.
            if result.error is not None or retry:
                self._retry.pop(identity, None)
            if result.error is None:
                if retry:
                    self.rows.pop(identity, None)
                elif identity in self.rows:
                    self._retry[identity] = self.rows[identity]
                return
            previous = self.rows.get(identity, {})
            self.rows[identity] = {
                **{column: row.get(column) for column in [*_COORDINATES, "posted_date"]},
                "phase": result.error.phase,
                "reason": scrub_credential(str(result.error), ""),
                "source_json": result.error.source_json,
                "rule_version": TEXT_RULE_VERSION,
                "attempted_at": datetime.now(UTC).isoformat(),
                "attempts": previous.get("attempts", 0) + 1,
            }

    def save(self) -> None:
        from spicy_regs.pipelines.regulatory_publication import finish_checkpoints, restore_checkpoint

        root = self.path.parent.parent
        with self._lock:
            finish_checkpoints(
                root,
                {
                    "pending_comment_text": list(self.rows.values()),
                    "failed_keys": restore_checkpoint(root, "failed_keys"),
                },
                publish=False,
            )

    def retry(
        self, pool: DerivedTextPool, output_dir: Path, agencies: list[str], *, before_write: Callable[[], None] = lambda: None,
    ) -> None:
        """Retry prior failures still untouched by ingestion in the catalog; preserve current metadata.

        Re-read only the existing row's coordinates and text state. Rows already
        filled by another path are resolved without fetching or overwriting them.
        Missing/moved rows remain pending until their source ingestion resolves.
        ``before_write`` runs before each catalog upsert (the ETL commits the originals it read).
        """
        rows = [row for row in self._retry.values() if row["agency_code"] in agencies]
        if not rows:
            return
        logger.info("Retrying {} prior comment-text failures independently of raw keys", len(rows))
        pending = pl.from_dicts(rows, schema={name: pl.String for name in _COORDINATES})
        with TemporaryDirectory(dir=output_dir, prefix=".text-retry-") as staging:
            self._retry_catalog(pool, pending, Path(staging) / "updates.parquet", before_write)

    def _updates(self, pool: DerivedTextPool, candidates: pl.DataFrame, updates: Path) -> dict[str, int]:
        # No attachment gate: the retained failure proves the original row was
        # eligible. The current persisted text/status protects completed work.
        return write_text_updates(
            pool.map(
                candidates.unique(subset="comment_id").iter_rows(named=True),
                select=lambda row: row["text_content"] is None and not row["text_extraction_status"],
            ),
            updates,
            observe=lambda result: self.observe(result, retry=True),
        )

    def _retry_catalog(
        self, pool: DerivedTextPool, pending: pl.DataFrame, updates: Path, before_write: Callable[[], None],
    ) -> None:
        record_type = RECORD_TYPES["comments"]
        con = iceberg._connect_for_table(record_type)
        try:
            con.register("_text_pending", pending.to_arrow())
            from spicy_regs.sources.regulatory_catalog import processing_table

            prior = processing_table(con, record_type, where="comment_id IN (SELECT comment_id FROM _text_pending)")
            for agency in pending["agency_code"].unique().sort():
                candidates = con.execute(
                    f"""
                    SELECT {", ".join("r." + column for column in _COLUMNS)}
                    FROM {prior} r JOIN _text_pending p
                      ON r.agency_code = p.agency_code AND r.comment_id = p.comment_id
                     AND r.docket_id IS NOT DISTINCT FROM p.docket_id
                    WHERE r.agency_code = ?
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.comment_id ORDER BY r.modify_date DESC NULLS LAST) = 1
                """,
                    [agency],
                ).pl()
                stats = self._updates(pool, candidates, updates)
                if stats["derived"]:
                    before_write()
                    iceberg.upsert_comment_text(con, record_type, agency, updates)
        finally:
            con.close()
