"""Hosted refreshes of the source families that were refreshed by hand.

Each rollup acquires through its family pipeline's own capture functions with the run's source evidence, so
every downloaded byte is in that evidence and is uploaded to R2 before the family pointer moves: a hosted
runner has no shared raw archive, and its evidence is where its downloads are kept. A run that finds nothing
the published generation lacks raises :class:`Unchanged` and publishes nothing.
"""
from __future__ import annotations

import shutil
from datetime import UTC, date, datetime, timedelta
from os import getenv
from pathlib import Path

from spicy_regs.pipelines import (
    administration_policy, executive_orders, grants_opportunities, house_disbursements, ofac, treasury_fiscal,
)
from spicy_regs.pipelines.raw_releases import (
    journal_snapshot_releases, published_before, restore_originals, snapshot_readers,
)
from spicy_regs.pipelines.rollups.base import RollupPipeline, Unchanged, make_rollup_app
from spicy_regs.schemas.house_disbursements import PARTITION_COLUMNS as HOUSE_PARTITIONS, TABLES as HOUSE_TABLES
from spicy_regs.sources import r2, retained_collections
from spicy_regs.transforms import build_executive_orders, build_grants_opportunities, build_ofac, build_treasury_fiscal

#: How many days back a Grants.gov run looks for the newest daily export before it reports the source as stalled.
GRANTS_LOOKBACK_DAYS = 7


class SourceRefreshRollup(RollupPipeline):
    """A rollup whose build acquires its own source and reads its own published generation."""

    retain_source_evidence = True
    #: An injected HTTP transport for the source requests; ``None`` reaches the publisher.
    transport = None

    @property
    def evidence(self):
        """The run's source evidence, which ``run`` opens before it calls ``build``."""
        if self.source_evidence is None:
            raise RuntimeError(f"{self.name}: build needs the source evidence run() opens")
        return self.source_evidence

    def published_tables(self, directory: Path) -> list[Path]:
        """This family's published tables, each fetched against its pin into ``directory``."""
        directory.mkdir()
        paths = [directory / key for key in (self.outputs or (self.output,))]
        for path in paths:
            if not r2.download(path.name, path):
                raise RuntimeError(f"{self.name}: published {path.name} is required to continue the family")
        return paths


class OfacSanctionsRollup(SourceRefreshRollup):
    """Publish the current OFAC SDN and consolidated exports as the family's two snapshots."""

    name = "ofac-sanctions"
    outputs = tuple(f"{table}.parquet" for table in build_ofac.SCHEMAS)

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        from spicy_docs.sources.ofac_profile import OFAC_PROFILE, iter_retained_pages

        evidence = self.evidence
        scopes = ofac.capture(evidence, ("sdn", "consolidated"), transport=self.transport)
        if all(published_before(evidence, ofac.STAGE, scope["capture"]["response_sha256"]) for scope in scopes):
            raise Unchanged("both exports are the published ones")
        readers, _ = snapshot_readers(OFAC_PROFILE, iter_retained_pages, scopes, output_dir / "source-releases",
                                      store=evidence.store)
        journal_snapshot_releases(evidence, readers, ofac.RELEASE_EVENT)
        return tuple(build_ofac.build_ofac(readers, output_dir))


class GrantsOpportunitiesRollup(SourceRefreshRollup):
    """Publish the newest Grants.gov daily export as the family's one snapshot."""

    name = "grants-opportunities"
    output = build_grants_opportunities.OUTPUT

    def newest_export(self, today: date) -> str | None:
        """The newest export date the publisher holds within the lookback, newest first."""
        days = (today - timedelta(days=back) for back in range(GRANTS_LOOKBACK_DAYS))
        return next((day.isoformat() for day in days
                     if grants_opportunities.stated_version(day.isoformat(), transport=self.transport)), None)

    def build(self, output_dir: Path) -> Path:
        from spicy_docs.sources.grants_gov_profile import GRANTS_GOV_PROFILE, iter_retained_pages

        evidence = self.evidence
        newest = self.newest_export(datetime.now(UTC).date())
        if newest is None:
            raise RuntimeError(f"Grants.gov holds no daily export from the last {GRANTS_LOOKBACK_DAYS} days")
        published = evidence.inherited_event(grants_opportunities.RELEASE_EVENT)
        if published and newest <= published["collection_outcome"]["requestedScope"]["snapshot_date"]:
            raise Unchanged(f"the newest export, {newest}, is published")
        scopes = grants_opportunities.capture(evidence, [newest], transport=self.transport)
        readers, _ = snapshot_readers(GRANTS_GOV_PROFILE, iter_retained_pages, scopes,
                                      output_dir / "source-releases", store=evidence.store)
        journal_snapshot_releases(evidence, readers, grants_opportunities.RELEASE_EVENT)
        return build_grants_opportunities.build_grants_opportunities(readers, output_dir)


class AdministrationPolicyRollup(SourceRefreshRollup):
    """Rebuild administration-policy statements from the community archive's current commit."""

    name = "administration-policy"
    outputs = tuple(f"{table}.parquet" for table in retained_collections.SCHEMAS)
    receipt_policies = tuple(retained_collections.POLICIES.values())

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.evidence
        statements = next(path for path in self.published_tables(output_dir / "published")
                          if path.stem == administration_policy.STATEMENTS)
        published = administration_policy.published_sources(statements)
        commit = administration_policy.head_commit()
        evidence.event("administration-policy-head", remote=administration_policy.REMOTE, commit=commit)
        selection = administration_policy.capture(evidence, commit, (), published=published,
                                                  transport=self.transport)
        if all(source["commit"] == published[source["administration"]]["commit"] for source in selection["sources"]):
            raise Unchanged(f"no archive file differs at {commit[:12]}")
        built = administration_policy.candidate(selection, output_dir, evidence, store=evidence.store, retain=False)
        # The builder states its own generation identity in every receipt it wrote.
        self.receipt_generation_id = built["generation"]
        shutil.copyfile(output_dir / "candidate" / "etl_receipts.parquet", output_dir / "etl_receipts.parquet")
        return tuple(output_dir / "candidate" / built["outputs"][table]["path"]
                     for table in retained_collections.SCHEMAS)


class TreasuryFiscalRollup(SourceRefreshRollup):
    """Add each newly listed Debt to the Penny day to the published Treasury generation.

    The published rows name every capture behind them, and published evidence holds those bytes, so a run
    needs no capture file from an earlier one. A new monthly statement needs its reviewed estimate entries
    and is added by hand.
    """

    name = "treasury-fiscal"
    outputs = tuple(f"{table}.parquet" for table in build_treasury_fiscal.SCHEMAS)

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.evidence
        published = treasury_fiscal.published_selection(self.published_tables(output_dir / "published"))
        debt_days = [scope["selection"]["record_date"] for scope in published["releases"]
                     if scope["selection"]["dataset"] == "debt_to_penny"]
        listed = treasury_fiscal.debt_dates_after(evidence, date.fromisoformat(max(debt_days)),
                                                  transport=self.transport)
        if not listed:
            raise Unchanged(f"no debt date is listed after {max(debt_days)}")
        added = treasury_fiscal.capture(evidence, [("debt_to_penny", day) for day in listed], [],
                                        transport=self.transport)
        held = [capture["response_sha256"] for scope in published["releases"] for capture in scope["captures"]]
        held += [report["sha256"] for report in published["reports"].values()]
        restore_originals(evidence.store, held, getenv("R2_PUBLIC_URL", r2.VERIFIED_PUBLIC_URL),
                          max_bytes=treasury_fiscal.MAX_REPORT_BYTES)
        selection = treasury_fiscal.merge_selections([published, added])
        readers, inputs = treasury_fiscal.selected_inputs(selection, output_dir / "source-releases",
                                                          store=evidence.store)
        files, _ = treasury_fiscal.retain_and_build(readers, output_dir, evidence, **inputs)
        return tuple(files)


class ExecutiveOrdersRollup(SourceRefreshRollup):
    """Publish every executive order the Federal Register API serves, keeping the published rows' bodies."""

    name = "executive-orders"
    output = build_executive_orders.OUTPUT
    #: An injected listing fetch (URL to bytes); ``None`` uses the Federal Register owner's client.
    fetch = None

    def build(self, output_dir: Path) -> Path:
        evidence = self.evidence
        (table,) = self.published_tables(output_dir / "published")
        prior = executive_orders.prior_rows(table)
        selection = executive_orders.capture(evidence, prior=prior, results=output_dir / "body-captures",
                                             through=datetime.now(UTC).date(), fetch=self.fetch,
                                             transport=self.transport)
        if "bodies" not in selection:
            raise Unchanged(f"the API lists the {len(prior)} published orders unchanged")
        return executive_orders.retain_and_build(selection, output_dir, evidence, prior=prior)


class HouseDisbursementsRollup(SourceRefreshRollup):
    """Rebuild the House ledger when the House lists a CSV the published generation lacks.

    The published originals are restored from R2 evidence (0.92 GB for 2016 to mid-2026) rather than downloaded
    from the House again; every published quarter is normalized again with the new ones.
    """

    name = house_disbursements.FAMILY
    outputs = tuple(f"{table}.parquet" for table in HOUSE_TABLES.values())
    partitioned = {key: HOUSE_PARTITIONS for key in outputs}

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        from spicy_docs.sources.house_disbursements import HouseDisbursementsBudget

        evidence = self.evidence
        listed = house_disbursements.listed_links(evidence, transport=self.transport)
        # The published rows, not the prior evidence's journal, say which files are published: a journal event read
        # back here would have to be handed on by every native conversion (native_conversion.INHERITED_EVENTS).
        for key in self.outputs:
            r2.download_members(key, output_dir / "published")
        files = house_disbursements.published_files(output_dir / "published")
        added = house_disbursements.additions(listed, files)
        if not added:
            raise Unchanged("the House lists no CSV the published generation lacks")
        restore_originals(evidence.store, [item["source_sha256"] for item in files],
                          getenv("R2_PUBLIC_URL", r2.VERIFIED_PUBLIC_URL), max_bytes=HouseDisbursementsBudget().max_bytes,
                          transport=self.transport)
        return house_disbursements.refresh(files, added, output_dir, store=evidence.store, evidence=evidence,
                                           transport=self.transport)


ofac_sanctions_app = make_rollup_app(OfacSanctionsRollup)
grants_opportunities_app = make_rollup_app(GrantsOpportunitiesRollup)
administration_policy_app = make_rollup_app(AdministrationPolicyRollup)
treasury_fiscal_app = make_rollup_app(TreasuryFiscalRollup)
executive_orders_app = make_rollup_app(ExecutiveOrdersRollup)
house_disbursements_app = make_rollup_app(HouseDisbursementsRollup)
