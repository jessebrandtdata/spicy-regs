"""Select and build House ledger/summary generations from complete pinned normalizations.

``select`` writes the selection behind a published generation, read from its own rows, plus each CSV the
House's pages list that it lacks, and the ``--reuse`` map that reads the published originals from the raw
archive instead of downloading them again. ``build`` makes an ordinary admitted local generation from a
normalization; publication is a separate operation. The refresh order is in docs/local-family-refresh.md.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.pipelines.disbursement_generation import build_tables
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, open_raw, raw_original
from spicy_regs.schemas.house_disbursements import FAMILY, PARTITION_COLUMNS, SCHEMAS, TABLES
from spicy_regs.transforms.build_house_disbursements import build_house_disbursements

#: Selection IDs as the published generation names them: ``house-2026-q2-detail``.
_ID = "house-{year}-q{quarter}-{kind}"
#: The keys a ``congressional-disbursement-selection/1`` file declares.
_SELECTED = ("id", "chamber", "format", "kind", "url", "period_start", "period_end")


def published_files(generation: Path) -> list[dict]:
    """The selection entry of each source file behind a published generation's rows, with its digest and encoding.

    Every row names its source file, URL, digest, report period and the encoding its normalization read, so the
    generation rebuilds from these and the raw archive alone.
    """
    import pyarrow.dataset as ds

    files = []
    columns = ["source_file_id", "source_url", "source_sha256", "report_period_start", "report_period_end",
               "source_encoding"]
    for kind, table in TABLES.items():
        rows = ds.dataset(generation / table, format="parquet").to_table(columns=columns)
        for identity, url, sha256, start, end, encoding in sorted({tuple(row.values()) for row in rows.to_pylist()}):
            files.append({"id": identity, "chamber": "house", "format": "csv", "kind": kind, "url": url,
                          "period_start": start.isoformat(), "period_end": end.isoformat(),
                          "source_sha256": sha256, "encoding": encoding})
    if len({item["id"] for item in files}) != len(files) or len({item["url"] for item in files}) != len(files):
        raise ValueError("Published rows give one source file two URLs, periods or encodings")
    return files


def reuse_map(files: list[dict], store) -> dict:
    """``acquire --reuse``: each published original read from ``store`` instead of downloaded again; absent raises."""
    reuse = {}
    for item in files:
        original = raw_original(store, item["source_sha256"], None)
        reuse[item["url"]] = {"path": str(original), "sha256": item["source_sha256"],
                              "byte_size": original.stat().st_size, "encoding": item["encoding"]}
    return reuse


def listed_links(evidence, *, transport=None) -> list[tuple]:
    """``(page, index, link)`` for every CSV link the House's current and archive pages list.

    Both pages are captured through ``evidence`` into the raw archive before they are read.
    """
    from spicy_docs.sources.house_disbursements_index import (
        HouseDisbursementsIndexAcquirer, HouseDisbursementsIndexBudget,
    )

    budget = HouseDisbursementsIndexBudget()
    retained = evidence.transport(transport, stage="house-disbursements-index", max_bytes=budget.max_bytes)
    with HouseDisbursementsIndexAcquirer(budget=budget, transport=retained) as acquirer:
        pages = [(page, acquirer.acquire_index(selection=page)[0]) for page in ("current", "archive")]
    return [(page, index, link) for page, index in pages for link in index.links if link.duplicate_of is None]


def additions(listed: list[tuple], published: list[dict]) -> list[dict]:
    """Each ``listed`` CSV that ``published`` lacks, as a selection entry.

    A listed revision of a published quarter refuses: which original replaces which is a reviewed decision, not a
    selection rule.
    """
    known = {item["url"] for item in published}
    ids = {item["id"]: item["url"] for item in published}
    added = []
    for page, index, link in listed:
        if link.url in known:
            continue
        if not link.period_start or not link.period_end or link.period_issues:
            raise ValueError(f"The House lists {link.url} without a clean quarter: {link.period_issues}")
        start = link.period_start
        identity = _ID.format(year=start[:4], quarter=(int(start[5:7]) - 1) // 3 + 1, kind=link.kind)
        if identity in ids:
            raise ValueError(f"The House lists {link.url} for {identity}, which is published from "
                             f"{ids[identity]}; select the revision by hand")
        ids[identity] = link.url
        known.add(link.url)
        added.append({
            "id": identity, "chamber": "house", "format": "csv", "kind": link.kind, "url": link.url,
            "period_start": link.period_start, "period_end": link.period_end,
            "publisher": "House Chief Administrative Officer", "listing_url": index.index_url,
            "listing_sha256": index.source_sha256,
            "listing_occurrence": {"page": page, "link_number": link.link_number, "group_number": link.group_number,
                                   "report_label": link.report_label, "listing_text": link.listing_text},
        })
    return added


def write_selection(files: list[dict], added: list[dict], reuse: dict, output_dir: Path) -> Path:
    """Write ``selection.json`` (published files, then the added ones) and ``reuse.json``; returns the selection."""
    from spicy_regs.pipelines.congressional_disbursements import SCHEMA

    selected = [{key: item[key] for key in _SELECTED} for item in files] + added
    selection = output_dir / "selection.json"
    selection.write_text(json.dumps({"schema": SCHEMA, "files": selected, "scope": (
        "The published generation's files and each CSV the House lists beside them")}, indent=2, ensure_ascii=False))
    (output_dir / "reuse.json").write_text(json.dumps(reuse, indent=2, sort_keys=True) + "\n")
    return selection


def select(generation: Path, output_dir: Path, *, archive_root: Path | None = None, transport=None) -> dict:
    """Write ``selection.json`` and ``reuse.json`` for the next build into ``output_dir``; return what was added."""
    from spicy_regs.source_evidence import acquisition_evidence

    output_dir.mkdir(parents=True, exist_ok=False)
    files = published_files(generation)
    reuse = reuse_map(files, open_raw(archive_root))
    with acquisition_evidence(FAMILY, output_dir=output_dir) as evidence:
        added = additions(listed_links(evidence, transport=transport), files)
    write_selection(files, added, reuse, output_dir)
    return {"published": len(files), "added": [item["id"] for item in added]}


def refresh(files: list[dict], added: list[dict], output_dir: Path, *, store, evidence,
            transport=None) -> tuple[Path, ...]:
    """Acquire, normalize and convert ``files`` plus ``added`` into ``output_dir``; returns the table directories.

    The hosted refresh's whole path, the hand one's three commands. The acquisition's own journal, which holds the
    new files' HTTP receipts, is retained in ``evidence`` so a runner's scratch archive does not take it along.
    """
    from spicy_regs.pipelines.congressional_disbursements import acquire_selection, normalize_selection
    from spicy_regs.source_evidence import JOURNAL

    (output_dir / "select").mkdir(parents=True)
    reuse = reuse_map(files, store)
    selection = write_selection(files, added, reuse, output_dir / "select")
    acquired = acquire_selection(selection, output_dir / "acquired", reuse=reuse, archive_root=store.root,
                                 transport=transport)
    evidence.retain_file(Path(acquired["last_evidence_directory"]) / "artifact" / JOURNAL,
                         stage="house-acquisition-journal")
    normalize_selection(output_dir / "acquired" / "acquisition.json", output_dir / "normalized")
    return build_house_disbursements(output_dir / "normalized" / "manifest.json", output_dir, evidence=evidence)


def build(manifest: Path, output_dir: Path, *, read_index: Path | None = None):
    """Create an ordinary admitted local generation; publication remains a separate operation."""
    normalization = json.loads(manifest.read_text())
    acquisition = json.loads(Path(normalization["acquisition"]).read_text())
    return build_tables(
        manifest,
        output_dir,
        family=FAMILY,
        tables=tuple(TABLES.values()),
        schemas=SCHEMAS,
        partitions=PARTITION_COLUMNS,
        archive_root=Path(acquisition["archive_root"]),
        builder=build_house_disbursements,
        read_index=read_index,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    selecting = commands.add_parser("select", help="Write the next build's selection and reuse map")
    selecting.add_argument("--generation", required=True, type=Path, help="A local copy of the published generation")
    selecting.add_argument("--output-dir", required=True, type=Path, help="A fresh directory")
    selecting.add_argument("--archive-root", type=Path, help="Raw archive holding the published originals")
    building = commands.add_parser("build", help="Build a generation from a complete normalization")
    building.add_argument("--normalization", required=True, type=Path)
    building.add_argument("--output", required=True, type=Path)
    building.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    args = parser.parse_args()
    if args.command == "select":
        print(json.dumps(select(args.generation, args.output_dir, archive_root=args.archive_root)))
        return
    artifact, evidence = build(args.normalization, args.output, read_index=args.read_index)
    print(
        json.dumps(
            {
                "directory": str(args.output / "generation"),
                "source_evidence": str(evidence),
                "logical_id": artifact.pin.logical_id,
                "artifact_digest": artifact.pin.artifact_digest,
            }
        )
    )


if __name__ == "__main__":
    main()
