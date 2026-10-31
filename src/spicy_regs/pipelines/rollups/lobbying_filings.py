"""Rollup pipeline: lobbying_filings.parquet (Senate LDA REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_lobbying_filings``.
``LDA_FILING_YEAR`` reads one whole filing year instead of the posted-date
window, the unit of the history backfill. ``LDA_POOL_PASSES`` raises the walks pooled before
a tied query refuses (``build_lobbying_filings._iter_filings``).

``run-rollup-lobbying-rebuild`` builds the whole family once from retained pages instead: the latest capture of each
filing, no prior and no request. ``LDA_RETAINED_MANIFEST`` names the page manifest and
``LDA_RETAINED_MANIFEST_SHA256`` pins its bytes; the first generation under a new lobbying_filings policy is this one.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import date_env, int_env, text_env
from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.government import GovernmentReceiptRollup
from spicy_regs.transforms.build_lobbying_filings import build_lobbying_filings, rebuild_lobbying_filings


class LobbyingFilingsRollup(GovernmentReceiptRollup):
    """Senate Lobbying Disclosure Act filings ingested from lda.gov (key optional)."""

    name: ClassVar[str] = "lobbying-filings"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = (
        "lobbying_filings.parquet", "lobbying_activities.parquet", "lobbying_activity_lobbyists.parquet",
        "lobbying_registrants.parquet", "lobbying_clients.parquet",
    )
    # Decision 47: the activities and their lobbyists join the family as their own tables; Q12 (2026-10-30): the
    # registrant and client records, one row per LDA id as last captured.
    added_tables: ClassVar[tuple[str, ...]] = (
        "lobbying_activities.parquet", "lobbying_activity_lobbyists.parquet",
        "lobbying_registrants.parquet", "lobbying_clients.parquet",
    )

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_lobbying_filings(
            output_dir,
            evidence=self.source_evidence, receipt_generation_id=self.receipt_generation_id,
            since=date_env("LDA_SINCE"),
            until=date_env("LDA_UNTIL"),
            filing_year=int_env("LDA_FILING_YEAR"),
            max_passes=int_env("LDA_POOL_PASSES"),
        )


class LobbyingRetainedRebuild(LobbyingFilingsRollup):
    """The whole lobbying family rebuilt from retained LDA pages named by a pinned manifest."""

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        manifest, digest = text_env("LDA_RETAINED_MANIFEST"), text_env("LDA_RETAINED_MANIFEST_SHA256")
        if manifest is None or digest is None:
            raise ValueError("A retained rebuild needs LDA_RETAINED_MANIFEST and LDA_RETAINED_MANIFEST_SHA256")
        return rebuild_lobbying_filings(
            output_dir, manifest=Path(manifest), manifest_sha256=digest,
            evidence=self.source_evidence, receipt_generation_id=self.receipt_generation_id,
        )


app = make_rollup_app(LobbyingFilingsRollup)
rebuild_app = make_rollup_app(LobbyingRetainedRebuild)

if __name__ == "__main__":
    app()
