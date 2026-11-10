"""Rollup pipeline: court_opinion_clusters.parquet (CourtListener bulk ingest).

Like ``courtlistener``, this rollup *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the bulk read, the search
catch-up, and the incremental merge with the prior published table all happen
inside ``build_court_opinion_clusters``. A run that finds a new export streams
two dumps, not one: the 2.3 GiB ``opinion-clusters`` dump plus the 4.67 GiB
``dockets`` dump, because the cluster dump says nothing about which court
decided — that lives on the docket (cached by dump date when run locally). A
run whose published table already holds the newest export streams neither: it
only adds the clusters created since.
"""

from datetime import date
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.court_receipts import CourtReceiptRollup as RollupPipeline
from spicy_regs.sources.courtlistener import DEFAULT_MAX_REQUESTS, DEFAULT_MAX_WAIT_SECONDS
from spicy_regs.transforms.build_court_opinion_clusters import build_court_opinion_clusters


class CourtOpinionClustersRollup(RollupPipeline):
    """Federal and state court decisions (opinion clusters) from CourtListener bulk data."""

    name: ClassVar[str] = "court-opinion-clusters"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "court_opinion_clusters.parquet"
    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        return build_court_opinion_clusters(output_dir, evidence=self.source_evidence)


app = make_rollup_app(CourtOpinionClustersRollup)


@app.command(name="catch-up")
def catch_up(
    *,
    output_dir: Path,
    prior_directory: Path,
    expected_prior: str,
    source_edition: date,
    source_edition_evidence: Path,
    source_edition_sha256: str,
    courts_local_file: Path,
    courts_sha256: str,
    search_through: int,
    retained_search_directory: Path,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    max_wait_seconds: float = DEFAULT_MAX_WAIT_SECONDS,
    skip_upload: bool = True,
) -> None:
    """Resume a bounded same-edition Search addition; see docs/court-search-recovery.md.

    Select the same local prior, source-edition evidence, courts original and
    retained directory on every attempt. This never refreshes historical rows
    or acquires a newer bulk edition; use the default command for those cases.
    """
    from dotenv import load_dotenv
    from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget
    from spicy_regs.transforms.catch_up_court_opinion_clusters import (
        catch_up_court_opinion_clusters,
        check_catch_up_directories,
    )

    # The rollup opens its output evidence before calling build.
    check_catch_up_directories(prior_directory, output_dir, retained_search_directory)
    load_dotenv()
    allocation = CourtListenerTraversalBudget(max_requests, max_wait_seconds)

    class SelectedCatchup(CourtOpinionClustersRollup):
        def build(self, output_dir: Path) -> Path:
            if self.source_evidence is None:
                raise ValueError("Court catch-up requires captured source evidence")
            return catch_up_court_opinion_clusters(
                output_dir, prior_directory=prior_directory, expected_prior=expected_prior,
                source_edition=source_edition, source_edition_evidence=source_edition_evidence,
                source_edition_sha256=source_edition_sha256, courts_local_file=courts_local_file,
                courts_sha256=courts_sha256, search_through=search_through, search_budget=allocation,
                retained_search_directory=retained_search_directory, evidence=self.source_evidence,
            )

    SelectedCatchup(output_dir=output_dir, skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
