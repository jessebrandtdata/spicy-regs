"""The Unified Agenda and CFR sections tables across the move off per-row receipts (ss-vse3), on a cut of real data.

``fixtures/agenda_cfr_receipt_switch`` (``manifest.json`` names every source): for each table, rows of its live
generation (unified-agenda ab8eb298…, 2026-11-10; cfr-sections 897008e4…, 2026-11-09) with their receipts exactly as
published, the run's observed input record included, and ``night.json``, the next night's input read from those
receipts' text rows with labelled edits.

Each night is built twice from the same input. The receipt route restores the prior through its receipts
(``materialize_internal``) and writes the table with ``write_held_dataset``, as ``SubjectReceiptRollup`` did; the
plain route reads the prior back from the published table (``RegulationsTableRollup``), with no receipts file to
open. The two tables must be the same (``receipt_switch.table_differences``: schema, rows both ways, key at every
position).

What these cannot see: the whole tables (the full-size runs are in docs/agenda-cfr-receipt-archive.md), a night
that reads a whole edition or lists every CFR package, and Parquet layout beyond rows, order and row-group size.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.receipt_switch import receipt_files, table_differences

FIXTURE = Path(__file__).parent / "fixtures/agenda_cfr_receipt_switch"
MANIFEST = json.loads((FIXTURE / "manifest.json").read_text())
KEYS = {"unified_agenda": ["rin", "agenda_edition"], "cfr_sections": ["granule_id"]}


def _night(dataset):
    return json.loads((FIXTURE / dataset / "night.json").read_text())


def _published(dataset, tmp_path, *, marker=None):
    """The live cut as the plain route is handed it: the table alone, its receipts left behind."""
    held = tmp_path / "published" / f"{dataset}.parquet"
    held.parent.mkdir(parents=True, exist_ok=True)
    table = pq.read_table(FIXTURE / dataset / f"{dataset}.parquet")
    pq.write_table(table.replace_schema_metadata(marker), held, row_group_size=2000)
    return held


def _serve(path):
    def download(key, target):
        assert key == path.name
        shutil.copyfile(path, target)
        return True
    return download


def _receipt_route(dataset, tmp_path, builder, **kwargs):
    """The night as the receipt route built it: prior restored through the receipts, table by the held writer."""
    from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal, write_held_dataset

    work = tmp_path / "receipt-route"
    work.mkdir()
    selected = ReceiptInput(dataset, (FIXTURE / dataset / f"{dataset}.parquet",),
                            FIXTURE / dataset / "etl_receipts.parquet", MANIFEST[dataset]["generationId"])
    restored = materialize_internal(selected, work / "restored.parquet")
    text = builder(work, download_prior=_serve(restored.rename(restored.with_name(f"{dataset}.parquet"))), **kwargs)
    subject, _ = write_held_dataset(dataset, text, work / "held", generation_id="night")
    return subject


class NoVolumes:
    """A volume client for a night that must read none: every listed package is unchanged or holds no section."""

    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def acquire_annual(self, selection, **_):
        from spicy_docs.sources.cfr.models import CfrSourceError

        self.calls.append(selection)
        raise CfrSourceError(f"no volume in this test: {selection}")


def _plain_route(rollup, tmp_path, prior, builder, **kwargs):
    out = tmp_path / "plain-route"
    out.mkdir()
    table = rollup(output_dir=out).build_table(out, builder, download_prior=_serve(prior), **kwargs)
    assert receipt_files(out) == [], "the plain route writes no receipts file"
    return table


def test_the_unified_agenda_night_is_the_receipt_routes_night(tmp_path):
    from spicy_regs.pipelines.rollups.unified_agenda import UnifiedAgendaRollup
    from spicy_regs.transforms.build_unified_agenda import build_unified_agenda

    night = _night("unified_agenda")
    inputs = {"editions": ("202510",), "records": lambda edition: night}
    expected = _receipt_route("unified_agenda", tmp_path, build_unified_agenda, **inputs)
    actual = _plain_route(UnifiedAgendaRollup, tmp_path, _published("unified_agenda", tmp_path),
                          build_unified_agenda, **inputs)
    assert table_differences(actual, expected, key=KEYS["unified_agenda"]) == []
    rows = {(row["rin"], row["agenda_edition"]): row for row in pq.read_table(actual).to_pylist()}
    labelled = MANIFEST["unified_agenda"]["night"]
    assert len(rows) == MANIFEST["unified_agenda"]["rows"] + 1
    assert rows[labelled["edited_title"], "202510"]["title"].endswith("(edited for the switch test)")
    assert (labelled["added"], "202510") in rows
    # Not in the night's read: the row stands as published, read back and mapped again.
    published = {(row["rin"], row["agenda_edition"]): row
                 for row in pq.read_table(FIXTURE / "unified_agenda/unified_agenda.parquet").to_pylist()}
    assert rows[labelled["not_reread"], "202510"] == published[labelled["not_reread"], "202510"]
    assert {row["related_rins"] is None for row in rows.values()} == {True, False}


@pytest.mark.parametrize("night", [1, 2])
def test_the_cfr_night_is_the_receipt_routes_night_once_the_table_carries_its_marker(tmp_path, night):
    """Night 2 on: the table carries the placement marker, and only the changed package is re-shaped.

    Night 1 reads a table the receipt route published, whose marker its receipts alone held: every package with a
    section is placed again, as ``replace_all`` would (each a volume download; none here, so each keeps its rows and
    the output stays unmarked until a night places them all).
    """
    from spicy_regs.pipelines.rollups.cfr_sections import CfrSectionsRollup
    from spicy_regs.transforms.build_cfr_sections import PLACEMENT_MARKER, build_cfr_sections

    granules = _night("cfr_sections")
    expected = _receipt_route("cfr_sections", tmp_path, build_cfr_sections, granules=granules, acquirer=NoVolumes())
    assert pq.read_schema(expected).metadata is None, "the receipt route's table never held the marker"
    volumes = NoVolumes()
    prior = _published("cfr_sections", tmp_path, marker=PLACEMENT_MARKER if night == 2 else None)
    actual = _plain_route(CfrSectionsRollup, tmp_path, prior, build_cfr_sections, granules=granules, acquirer=volumes)
    assert table_differences(actual, expected, key=KEYS["cfr_sections"]) == []
    [(key, value)] = PLACEMENT_MARKER.items()
    marker = (pq.read_schema(actual).metadata or {}).get(key.encode())
    if night == 2:
        assert volumes.calls == []
        assert marker == value.encode()
        # The next night reads it back with the prior.
        from spicy_regs.transforms.regulations_shape import published_as_source

        text = published_as_source("cfr_sections", actual, tmp_path / "text.parquet")
        assert pq.read_schema(text).metadata == {key.encode(): value.encode()}
    else:
        assert {selection.package_id for selection in volumes.calls} == {"CFR-2025-title50-vol2",
                                                                          "CFR-2025-title50-vol6"}
        assert marker is None
    rows = {row["granule_id"]: row for row in pq.read_table(actual).to_pylist()}
    edited = MANIFEST["cfr_sections"]["night"]["edited_heading"]
    assert rows[edited]["heading"].endswith("(edited for the switch test)")
    assert rows[edited]["last_modified"] == MANIFEST["cfr_sections"]["night"]["lastModified"]


def test_the_page_urls_are_their_identities(tmp_path):
    """Each receipt's ``url`` (receipt-only, never published) is the builder's template of the row's identity."""
    from spicy_regs.etl_receipts import read_with_receipts
    from spicy_regs.transforms.regulations_receipts import policy

    pages = {
        "unified_agenda": lambda row: ("https://www.reginfo.gov/public/do/eAgendaViewRule"
                                       f"?pubId={row['agenda_edition']}&RIN={row['rin']}"),
        "cfr_sections": lambda row: f"https://www.govinfo.gov/app/details/{row['package_id']}/{row['granule_id']}",
    }
    for dataset, page in pages.items():
        from spicy_regs.etl_receipts import select_receipts

        scoped = select_receipts(FIXTURE / dataset / "etl_receipts.parquet", tmp_path / f"{dataset}.parquet",
                                 dataset=dataset)
        rows = list(read_with_receipts([FIXTURE / dataset / f"{dataset}.parquet"], [scoped], policy(dataset),
                                       generation_id=MANIFEST[dataset]["generationId"]))
        assert len(rows) == MANIFEST[dataset]["rows"]
        assert all(row["url"] == row["raw_conversion_inputs"]["url"] == page(row) for row in rows)


def test_a_table_whose_receipts_are_not_retired_cannot_take_the_plain_route():
    from spicy_regs.pipelines.rollups.regulations_tables import RegulationsTableRollup

    docket = type("DocketsTable", (RegulationsTableRollup,), {"name": "x", "output": "dockets.parquet", "build": None})
    with pytest.raises(ValueError, match="dockets still publishes receipts"):
        docket().build_table(Path("unused"), lambda *_, **__: Path("unused"))


def test_the_edition_file_is_retained_as_read(tmp_path, monkeypatch):
    """Each edition the reader fetches is kept as source evidence, with the editions' manifest in the journal."""
    import hashlib

    import httpx

    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.sources.unified_agenda import UnifiedAgendaReader
    from spicy_regs.transforms import build_unified_agenda as builder
    from tests.test_unified_agenda import _FIXTURE_XML

    transport = httpx.MockTransport(lambda request: httpx.Response(
        200, stream=httpx.ByteStream(_FIXTURE_XML), headers={"content-type": "application/xml"}))
    monkeypatch.setattr(builder, "UnifiedAgendaReader", lambda **kwargs: UnifiedAgendaReader(transport=transport,
                                                                                             **kwargs))
    evidence = CaptureEvidence(tmp_path / "audit", "unified-agenda")
    builder.build_unified_agenda(tmp_path, editions=("202510",), download_prior=lambda *_: False, evidence=evidence)
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    [capture] = [event for event in journal if event["event"] == "capture"]
    digest = hashlib.sha256(_FIXTURE_XML).hexdigest()
    assert (capture["stage"], capture["edition"], capture["sha256"]) == (
        "unified-agenda-edition", "202510", "sha256:" + digest)
    [manifest] = [event for event in journal if event["event"] == "unified-agenda-editions"]
    assert [(item["edition"], item["input_sha256"]) for item in manifest["editions"]] == [("202510", digest)]


@pytest.mark.parametrize("dataset", ["unified_agenda", "cfr_sections"])
def test_the_hosted_job_merges_at_the_pinned_table_and_publishes_no_receipts(tmp_path, monkeypatch, remote, dataset):
    """Hosted mode against the fake bucket, on the night the switch merges: the live generation is the receipt
    route's (the fixture's pair, published as main published it), pinned in the archive; the job reads its table
    alone, publishes the night's table without receipts, and that table is the receipt route's night."""
    import functools

    from spicy_regs import subject_catalog
    from spicy_regs.generations import build_generation
    from spicy_regs.sources import publication
    from spicy_regs.transforms.regulations_receipts import policy
    from tests.test_federal_register_switch import published, stored
    from tests.test_receipts_retired import _pin, pin_archive

    family = MANIFEST[dataset]["family"]
    index = stored(remote, monkeypatch)
    with monkeypatch.context() as as_published:
        as_published.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset())
        build_generation(tmp_path / "prior-generation", family=family,
                         files=[FIXTURE / dataset / f"{dataset}.parquet"], expected_keys=[f"{dataset}.parquet"],
                         receipt_path=FIXTURE / dataset / "etl_receipts.parquet", receipt_policies=[policy(dataset)],
                         receipt_generation_id=MANIFEST[dataset]["generationId"])
        publication.publish_generation(tmp_path / "prior-generation", client=remote, bucket="spicy-regs",
                                       prior_index=publication.empty_index())
    before = index()["families"][family]
    assert before["etlReceipts"]["datasets"] == [dataset]
    pin_archive(monkeypatch, tmp_path, {dataset: _pin(family, before["artifactDigest"])})
    if dataset == "unified_agenda":
        from spicy_regs.pipelines.rollups import unified_agenda as module
        from spicy_regs.transforms.build_unified_agenda import build_unified_agenda as builder

        night = _night(dataset)
        inputs = {"editions": ("202510",), "records": lambda edition: night}
        rollup = module.UnifiedAgendaRollup
    else:
        from spicy_regs.pipelines.rollups import cfr_sections as module
        from spicy_regs.transforms.build_cfr_sections import build_cfr_sections as builder

        # The live table carries no placement marker, so this first night asks for every sectioned package's volume.
        inputs = {"granules": _night(dataset), "acquirer": NoVolumes()}
        rollup = module.CfrSectionsRollup
    expected = _receipt_route(dataset, tmp_path, builder, **inputs)
    monkeypatch.setattr(module, builder.__name__, functools.partial(builder, **inputs))
    rollup(output_dir=tmp_path / "night", skip_upload=False).run()
    entry = index()["families"][family]
    assert "etlReceipts" not in entry
    assert table_differences(published(tmp_path / "night") / f"{dataset}.parquet", expected,
                             key=KEYS[dataset]) == []
