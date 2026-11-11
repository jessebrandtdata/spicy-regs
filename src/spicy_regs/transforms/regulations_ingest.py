"""Local receipt-bound entry points for retained regulatory source ingestion."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from hashlib import file_digest
from pathlib import Path
from tempfile import TemporaryDirectory

from spicy_regs.etl_receipts import ReceiptContext
from spicy_regs.transforms.regulations_receipts import ReceiptInput, build_local_generation, materialize_internal


def _build(dataset, destination, *, generation_id, witnesses, prior, prior_filename, build):
    if prior is not None and prior.dataset != dataset:
        raise ValueError("Prior dataset differs from the source producer")
    # Check retained-source pins before the producer runs, including empty reads.
    ReceiptContext(generation_id, "source-build", "spicy-regs:regulations-ingest-v1", witnesses)
    pins = list(witnesses)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="regulations-ingest-", dir=destination.parent) as temp:
        work = Path(temp)
        if prior is not None:
            materialize_internal(prior, work / prior_filename)
            for path in (*prior.subjects, prior.receipts):
                with path.open("rb") as body:
                    digest = file_digest(body, "sha256").hexdigest()
                pins.append(
                    {
                        "source_id": f"{dataset}:{path.name}",
                        "source_uri": None,
                        "sha256": digest,
                        "locator": None,
                        "body_version": prior.generation_id,
                    }
                )
        output = build(work)
        return build_local_generation(
            {dataset: output},
            destination,
            generation_id=generation_id,
            family=dataset.replace("_", "-"),
            witnesses=pins,
            include_source_witness=False,
        )


def unified_agenda_generation(
    records_by_edition: Mapping[str, Iterable[dict]],
    destination: Path,
    *,
    generation_id: str,
    witnesses: Sequence[Mapping],
    prior: ReceiptInput | None = None,
    source_only: bool = False,
    source_receipts: list[dict] | None = None,
):
    """Use the edition merge with literal timetable/legal-authority structures.

    ``source_only`` replays the selected retained editions with no prior (``build_unified_agenda`` refuses one),
    the only way a column added to the reader reaches every held edition; ``source_receipts`` collects the
    readers' input pins and repairs into the subject's metadata.
    """
    from spicy_regs.transforms.build_unified_agenda import build_unified_agenda

    return _build(
        "unified_agenda",
        destination,
        generation_id=generation_id,
        witnesses=witnesses,
        prior=prior,
        prior_filename="_ua_prior.parquet",
        build=lambda work: build_unified_agenda(
            work,
            editions=tuple(records_by_edition),
            records=records_by_edition.__getitem__,
            download_prior=lambda *_: False,
            source_only=source_only,
            source_receipts=source_receipts,
        ),
    )


def cfr_sections_generation(
    granules: Iterable[dict],
    destination: Path,
    *,
    acquirer,
    generation_id: str,
    witnesses: Sequence[Mapping],
    prior: ReceiptInput | None = None,
    replace_all: bool = False,
):
    """Keep annual-volume placement, failure preservation and source update dates.

    ``acquirer`` supplies retained volume bytes through the existing source API;
    callers must not supply a live network client for a local-only replay.
    """
    from spicy_regs.transforms.build_cfr_sections import build_cfr_sections

    return _build(
        "cfr_sections",
        destination,
        generation_id=generation_id,
        witnesses=witnesses,
        prior=prior,
        prior_filename="_cfr_prior.parquet",
        build=lambda work: build_cfr_sections(
            work, granules=granules, acquirer=acquirer, replace_all=replace_all, download_prior=lambda *_: False
        ),
    )
