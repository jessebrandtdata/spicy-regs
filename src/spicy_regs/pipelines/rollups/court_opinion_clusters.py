"""Rollup pipeline: court_opinion_clusters.parquet (CourtListener bulk ingest plus a daily search catch-up).

Like ``courtlistener``, this rollup *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty. Each run does one of two
things, decided from the published generation's journal before anything is
downloaded (docs/court-search-recovery.md):

* The publisher lists an export newer than the one the table holds, or the
  journal does not say which it holds: ``build_court_opinion_clusters`` rebuilds
  from the export and merges it over the prior. That streams two dumps, not
  one: the 2.3 GiB ``opinion-clusters`` dump plus the 4.67 GiB ``dockets``
  dump, because the cluster dump says nothing about which court decided — that
  lives on the docket (cached by dump date when run locally). It makes no
  Search request, so no quota refusal can discard it.
* Otherwise ``daily_catch_up`` appends one day's share of the clusters created
  since, above the id the table already reaches, without restoring the prior.
  A day with no quota or no backlog publishes nothing and succeeds.
  ``COURT_CLUSTERS_MAX_REQUESTS`` lowers one run's Search requests (2 to 105),
  for a dispatch that proves the route with a small spend.
"""

import os
from datetime import date
from pathlib import Path
from typing import ClassVar

from loguru import logger

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.court_receipts import CourtReceiptRollup as RollupPipeline
from spicy_regs.sources import r2
from spicy_regs.sources.courtlistener import DEFAULT_MAX_REQUESTS, DEFAULT_MAX_WAIT_SECONDS
from spicy_regs.transforms.build_court_opinion_clusters import DATASET, build_court_opinion_clusters


#: A dispatch's cap on one catch-up's Search requests; blank is the normal day (``daily_catch_up`` checks the range).
MAX_REQUESTS_ENV = "COURT_CLUSTERS_MAX_REQUESTS"


class CourtOpinionClustersRollup(RollupPipeline):
    """Federal and state court decisions (opinion clusters) from CourtListener bulk data."""

    name: ClassVar[str] = "court-opinion-clusters"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "court_opinion_clusters.parquet"
    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        from spicy_docs.sources.courtlistener.bulk import latest_dump_date, list_bulk_dumps

        from spicy_regs.transforms.catch_up_court_opinion_clusters import daily_catch_up, held_selection

        evidence = self.source_evidence
        held = held_selection(evidence)
        newest = latest_dump_date(list_bulk_dumps(), DATASET) if held else None
        if evidence is None or held is None or (newest is not None and newest > held[0]):
            logger.info("Opinion clusters: bulk rebuild ({})", "the published journal names no export" if held is None
                        else f"export {newest} is newer than the {held[0]} one held")
            return build_court_opinion_clusters(output_dir, evidence=evidence)
        edition, above = held
        logger.info("Opinion clusters: daily catch-up above id {:,}; the {} export is the newest listed", above, edition)
        stated = os.environ.get(MAX_REQUESTS_ENV, "").strip()
        if stated and not (stated.isascii() and stated.isdecimal()):
            raise ValueError(f"{MAX_REQUESTS_ENV} must be a whole number of requests, got {stated!r}")
        if not self.skip_upload:
            # A run that could not publish would spend the day's quota for nothing.
            r2.require_credentials("Court catch-up publication")
        candidate, self.deferred_failure = daily_catch_up(
            output_dir, evidence=evidence, source_edition=edition, above=above,
            max_requests=int(stated) if stated else None)
        return candidate

    def _run_tables(self, output_dir: Path) -> None:
        from spicy_regs.transforms.catch_up_court_opinion_clusters import NothingToCatchUp

        try:
            super()._run_tables(output_dir)
        except NothingToCatchUp as nothing:
            if self.source_evidence is None:
                raise RuntimeError("Court catch-up no-op requires initialized source evidence") from nothing
            self.source_evidence.seal(outcome="no-op")
            logger.info("Opinion clusters: nothing to do ({}); the published generation is unchanged", nothing)


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
