"""Receipt-writing local entrypoint for the existing FEC registry/history builders.

Mapped tables use private staging; only the admitted native subject and shared receipts leave the build directory.
Raw acquisition evidence never lives in staging. The producers that acquire (the committee walk and the two bulk
master histories) get one durable ``capture_dir``, resolved here and nowhere else: the ``capture_dir`` argument, else
a non-empty ``FEC_CAPTURE_DIR``, else ``<output_dir>.inputs/raw``. It survives a successful build, a parser failure
and a mapping failure. A history table can instead rebuild offline from a retained ``selection`` (no HTTP), and a
committee walk can continue a refused attempt with ``resume``. Every acquiring bundle names the originals it used in
``<output_dir>.inputs/acquisition.json``: the route, the raw directory, and for a rebuild the selection's path and
SHA-256, so the bundle can be traced to them later.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from .fec_identity_receipts import IdentityReceiptWriter, read_identity_rows
from .fec_master_history import write_json

BUILDERS = {
    "fec_committees": ("build_fec_committees", "build_fec_committees"),
    "fec_candidate_history": ("build_fec_candidate_history", "build_fec_candidate_history"),
    "fec_committee_history": ("build_fec_committee_history", "build_fec_committee_history"),
    "fec_source_catalog": ("build_fec_source_catalog", "build_fec_source_catalog"),
    "org_committee_links": ("build_org_committee_links", "build_org_committee_links"),
}
#: Producers that read from a publisher and so need durable raw storage.
ACQUIRING = frozenset({"fec_committees", "fec_candidate_history", "fec_committee_history"})
#: Producers that can rebuild from a retained master selection.
HISTORIES = frozenset({"fec_candidate_history", "fec_committee_history"})


def build_fec_identity_rollup(
    table, output_dir, *, generation_id, inputs=(), prior_bundle=None, prior_generation_id=None, **builder_options
):
    """Run one named maintained producer and write its native receipt bundle.

    Input paths must be explicit local files. A caller may supply a fully local
    existing builder configuration; this function does not enable publication.
    Arguments are validated before anything is written, so a refused call leaves
    no custody directory behind.
    """
    if table not in BUILDERS:
        raise ValueError("Unsupported FEC identity rollup")
    selection, resume = builder_options.get("selection"), builder_options.get("resume")
    if selection is not None and table not in HISTORIES:
        raise ValueError("selection rebuilds only the candidate and committee history tables")
    if resume is not None and table != "fec_committees":
        raise ValueError("resume continues only a committee walk")
    if selection is not None and (builder_options.get("capture_dir") is not None or builder_options.get("evidence")):
        raise ValueError("A selection rebuild acquires nothing; pass neither capture_dir nor evidence")
    if table == "fec_committees":
        if prior_bundle is None and not builder_options.get("full_walk"):
            raise ValueError(
                "Committee increment requires a selected prior receipt bundle; first build requires full_walk"
            )
        if prior_bundle is not None and not prior_generation_id:
            raise ValueError("Committee prior requires its selected receipt generation")
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise FileExistsError(output_dir)
    # Raw custody belongs beside the bundle, never in the staging directory below.
    evidence_dir = output_dir.with_name(output_dir.name + ".inputs")
    acquisition: dict | None = None
    if selection is not None:
        path = Path(selection).resolve()
        try:
            with path.open("rb") as stream:
                digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        except OSError:
            raise ValueError(f"FEC master selection is missing or unreadable: {selection}") from None
        builder_options["selection"] = path
        acquisition = {"route": "selection", "selection": {"path": str(path), "sha256": digest}}
    elif table in ACQUIRING:
        capture_dir = Path(
            builder_options.get("capture_dir") or os.environ.get("FEC_CAPTURE_DIR") or evidence_dir / "raw"
        ).resolve()
        if capture_dir.is_relative_to(output_dir.resolve()):
            raise ValueError("FEC acquisition storage must not be inside the receipt bundle")
        builder_options["capture_dir"] = capture_dir
        acquisition = {"route": "refresh", "capture_dir": str(capture_dir)}
        if resume is not None:
            attempt = Path(resume).resolve()
            if not (attempt / "checkpoint.json").is_file():
                raise ValueError(f"{resume} is not a retained committee attempt (no checkpoint.json)")
            builder_options["resume"] = attempt
            acquisition = {"route": "resume", "capture_dir": str(capture_dir), "resume": str(attempt)}
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(exist_ok=False)
    if acquisition is not None:
        write_json(evidence_dir / "acquisition.json", acquisition)
    with TemporaryDirectory(prefix=".fec-identity-producer-", dir=output_dir.parent) as temporary:
        stage = Path(temporary)
        # Copy caller-selected local inputs; producers must never rewrite them.
        for path in inputs:
            path = Path(path)
            if path.is_symlink() or not path.is_file():
                raise ValueError("Expected a selected local input file")
            if (stage / path.name).exists():
                raise ValueError("Duplicate input basename")
            shutil.copyfile(path, stage / path.name)
        module, function = BUILDERS[table]
        if table == "fec_committees":
            from .build_fec_committees import COLUMNS, _SCHEMA
            from .parquet_rows import write_rows

            prior_path = stage / "_fec_prior.parquet"
            if prior_path.exists():
                raise ValueError("Committee prior must come from an admitted receipt bundle")
            if prior_bundle is None:
                prior_rows = ()
            else:

                def reconstructed():
                    for row in read_identity_rows(prior_bundle, table, generation_id=prior_generation_id):
                        originals = row["conversion_inputs"]
                        yield {name: originals[name] for name in COLUMNS}

                prior_rows = reconstructed()
            # An explicit empty full-walk prior also prevents an unqualified remote fallback.
            write_rows(prior_rows, prior_path, _SCHEMA)
        built = getattr(importlib.import_module("spicy_regs.transforms." + module), function)(stage, **builder_options)
        with built.open("rb") as stream:
            digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        # The mapped input must remain available for receipt witnesses after staging ends.
        retained = evidence_dir / built.name
        shutil.copyfile(built, retained)
        witness = dict(source_id=table, source_uri=None, sha256=digest, locator=None, body_version=None)
        prior_receipts = () if prior_bundle is None else (Path(prior_bundle) / "etl_receipts.parquet",)
        with IdentityReceiptWriter(output_dir, generation_id=generation_id, tables=[table],
                                   prior_receipts=prior_receipts) as writer:
            with pq.ParquetFile(built) as source:
                for batch in source.iter_batches(batch_size=512, use_threads=False):
                    for row in batch.to_pylist():
                        writer.emit(table, row, input_witness=witness)
    return output_dir
