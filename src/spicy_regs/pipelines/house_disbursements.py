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


def published_selection(generation: Path, store) -> tuple[list[dict], dict]:
    """The selected files behind a published generation's rows, and the reuse map of their originals in ``store``.

    Every row names its source file, URL, digest, report period and the encoding its normalization read, so the
    generation rebuilds from this and the raw archive alone. An original absent from ``store`` raises.
    """
    import pyarrow.dataset as ds

    files, reuse = [], {}
    columns = ["source_file_id", "source_url", "source_sha256", "report_period_start", "report_period_end",
               "source_encoding"]
    for kind, table in TABLES.items():
        rows = ds.dataset(generation / table, format="parquet").to_table(columns=columns)
        for identity, url, sha256, start, end, encoding in sorted({tuple(row.values()) for row in rows.to_pylist()}):
            files.append({"id": identity, "chamber": "house", "format": "csv", "kind": kind, "url": url,
                          "period_start": start.isoformat(), "period_end": end.isoformat()})
            original = raw_original(store, sha256, None)
            if url in reuse:
                raise ValueError(f"Published rows name {url} twice")
            reuse[url] = {"path": str(original), "sha256": sha256, "byte_size": original.stat().st_size,
                          "encoding": encoding}
    if len({item["id"] for item in files}) != len(files):
        raise ValueError("Published rows give one source file ID two URLs or periods")
    return files, reuse


def listed_additions(evidence, published: list[dict], *, transport=None) -> list[dict]:
    """Each CSV the House's current and archive pages list that ``published`` lacks, as selection entries.

    Both pages are captured through ``evidence`` into the raw archive before they are read. A listed revision of
    a published quarter refuses: which original replaces which is a reviewed decision, not a selection rule.
    """
    from spicy_docs.sources.house_disbursements_index import (
        HouseDisbursementsIndexAcquirer, HouseDisbursementsIndexBudget,
    )

    budget = HouseDisbursementsIndexBudget()
    known = {item["url"] for item in published}
    ids = {item["id"]: item["url"] for item in published}
    added = []
    retained = evidence.transport(transport, stage="house-disbursements-index", max_bytes=budget.max_bytes)
    with HouseDisbursementsIndexAcquirer(budget=budget, transport=retained) as acquirer:
        for page in ("current", "archive"):
            index, _capture = acquirer.acquire_index(selection=page)
            for link in index.links:
                if link.url in known or link.duplicate_of is not None:
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
                    "listing_occurrence": {"page": page, "link_number": link.link_number,
                                           "group_number": link.group_number, "report_label": link.report_label,
                                           "listing_text": link.listing_text},
                })
    return added


def select(generation: Path, output_dir: Path, *, archive_root: Path | None = None, transport=None) -> dict:
    """Write ``selection.json`` and ``reuse.json`` for the next build into ``output_dir``; return what was added."""
    from spicy_regs.pipelines.congressional_disbursements import SCHEMA
    from spicy_regs.source_evidence import acquisition_evidence

    output_dir.mkdir(parents=True, exist_ok=False)
    published, reuse = published_selection(generation, open_raw(archive_root))
    with acquisition_evidence(FAMILY, output_dir=output_dir) as evidence:
        added = listed_additions(evidence, published, transport=transport)
    selection = {"schema": SCHEMA, "scope": "The published generation's files and each CSV the House lists beside them",
                 "files": published + added}
    (output_dir / "selection.json").write_text(json.dumps(selection, indent=2, ensure_ascii=False) + "\n")
    (output_dir / "reuse.json").write_text(json.dumps(reuse, indent=2, sort_keys=True) + "\n")
    return {"published": len(published), "added": [item["id"] for item in added]}


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
