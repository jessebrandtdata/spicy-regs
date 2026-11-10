"""Convert one old-shape rollup family to native subjects and receipts, once, through that family's own writer.

A scheduled rollup refuses a published family that carries no ETL receipts (``selected_generations``), so a family
whose builder reads its own prior cannot convert itself. This command does it explicitly, one named family at a time:
the family's retained published tables stand in for its builder's output, its maintained writer splits them into
subjects and receipts, and the standard admission publishes them. No scheduled path gains an old-shape reader.

The receipt it writes keeps the captured index entry, and is saved before the pointer write is attempted, so
``--rollback`` can put the pointer back whatever became of the process. Without ``--publish`` nothing is written
outside the work directory. Runbook: ``docs/native-family-conversion.md``.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib
import io
import json
import os
import pkgutil
import re
import shutil
import subprocess
import sys
import time
import tomllib
import zipfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import distribution, version
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import pyarrow.parquet as pq

from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.sources import publication, r2

RECEIPT = "conversion.json"
SCRIPT = "scripts/convert_family_to_native.py"
NOTHING_PUBLISHED = "nothing was published; the family's pointer is unchanged"
#: Journal events a later run reads back from its prior (``CaptureEvidence.inherited_event``). A conversion reads no
#: source, so it journals each one again unchanged; otherwise the next run would treat every held row as never read.
INHERITED_EVENTS = (
    "congress-index-selection", "scorecard-source-attempt", "crs-discovery", "committee-discovery",
    "catchup-selection", "export-selection",
)
#: The receipt outcomes of a row or observation that converted; any other outcome stops the conversion.
CONVERTED_OUTCOMES = frozenset({"accepted", "observed"})
#: The shape of a sealed prepared receipt's digest, as the preparation prints it for the operator.
RECEIPT_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class ConversionRefused(RuntimeError):
    """The conversion stopped. ``state`` says what the bucket holds now, in words an operator can act on."""

    def __init__(self, message: str, *, state: str = NOTHING_PUBLISHED):
        super().__init__(message)
        self.state = state


def _wheel_state(root: Path, locked: Mapping) -> dict:
    """The pinned SpicyDocs wheel by digest: the lock's, the vendored file's, and installed files that differ from it."""
    wheels = locked.get("wheels", [])
    state = {"locked_sha256": wheels[0]["hash"] if len(wheels) == 1 else None, "file_sha256": None,
             "installed_differs": None}
    wheel = root / locked.get("source", {}).get("path", "")
    if state["locked_sha256"] is None or not wheel.is_file():
        return state  # Not a vendored wheel: there are no bytes here to hold the installed files against.
    with wheel.open("rb") as stream:
        state["file_sha256"] = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
    installed, differs = distribution("spicy-docs"), []
    with zipfile.ZipFile(wheel) as archive:
        record = next(name for name in archive.namelist() if name.endswith(".dist-info/RECORD"))
        for name, digest, _size in csv.reader(io.TextIOWrapper(archive.open(record), "utf-8")):
            if not digest or ".data/" in name:
                continue
            algorithm, _, expected = digest.partition("=")
            target = Path(str(installed.locate_file(name)))
            actual = (base64.urlsafe_b64encode(hashlib.new(algorithm, target.read_bytes()).digest()).rstrip(b"=").decode()
                      if target.is_file() else None)
            if actual != expected:
                differs.append(name)
    state["installed_differs"] = differs[:20]
    return state


def source_state(remote: str) -> dict:
    """What this process would convert with: the checkout, the remote's main, and the SpicyDocs wheel beside its pin."""
    root = Path(__file__).resolve().parents[2]

    def git(*arguments: str) -> str:
        try:
            return subprocess.run(["git", "-C", str(root), *arguments], check=True, capture_output=True, text=True,
                                  env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).stdout.strip()
        except subprocess.CalledProcessError as failure:
            raise ConversionRefused(f"git {arguments[0]} failed: {failure.stderr.strip()[:300]}") from failure

    url = git("remote", "get-url", remote)
    if not re.match(r"(https://|ssh://|git@)", url):
        raise ConversionRefused(f"--remote {remote} is {url!r}, not a hosted repository: main must be read from "
                                "where it is published, not from a local path")
    listed = git("ls-remote", url, "refs/heads/main").split()
    if not listed:
        raise ConversionRefused(f"--remote {remote} has no main branch")
    locked = next(package for package in tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))["package"]
                  if package["name"] == "spicy-docs")
    return {
        "checkout": git("rev-parse", "HEAD"),
        "uncommitted": git("status", "--porcelain", "--untracked-files=no").splitlines(),
        # Every module under src/ can be imported and every declaration there is read, tracked or not.
        "untracked": [name for name in git("ls-files", "--others", "--", "src", "scripts").splitlines()
                      if "__pycache__/" not in name and not name.endswith(".pyc")],
        "remote": re.sub(r"//[^/@]*@", "//", url),
        "main": listed[0],
        "spicy_docs": version("spicy-docs"),
        "pinned_spicy_docs": locked["version"],
        "spicy_docs_wheel": _wheel_state(root, locked),
    }


def _runtime_problems(state: Mapping, spicy_docs: str) -> list[str]:
    """The same clean checkout and exact installed-wheel checks for every path."""
    problems = []
    if state["uncommitted"]:
        problems.append(f"the checkout has uncommitted changes: {', '.join(state['uncommitted'][:5])}")
    if state["untracked"]:
        problems.append(f"the checkout has untracked files the command could load: {', '.join(state['untracked'][:5])}")
    if state["spicy_docs"] != spicy_docs or state["pinned_spicy_docs"] != spicy_docs:
        problems.append(f"SpicyDocs is {state['spicy_docs']} installed and {state['pinned_spicy_docs']} pinned, "
                        f"not the {spicy_docs} this run was started for")
    wheel = state["spicy_docs_wheel"]
    if wheel["locked_sha256"] is None or wheel["file_sha256"] != wheel["locked_sha256"]:
        problems.append(f"the SpicyDocs wheel in this checkout is {wheel['file_sha256']}, and uv.lock pins "
                        f"{wheel['locked_sha256']}")
    elif wheel["installed_differs"]:
        problems.append("the installed SpicyDocs differs from the pinned wheel in "
                        + ", ".join(wheel["installed_differs"][:5]))
    return problems


def _local_state(state: Mapping) -> dict:
    """The parts of a source state this process loaded from: everything but the hosted main."""
    return {key: value for key, value in state.items() if key != "main"}


def _refuse_moved(state: Mapping, *, main: str, spicy_docs: str) -> None:
    """Field policies and the writer come from main and its pinned wheel; converting from anything else is refused."""
    problems = []
    if len(main) < 8 or not state["main"].startswith(main):
        problems.append(f"main is {state['main'][:12]}, not the {main[:12]} this run was started for")
    if state["checkout"] != state["main"]:
        problems.append(f"this checkout is {state['checkout'][:12]}, not main {state['main'][:12]}")
    problems.extend(_runtime_problems(state, spicy_docs))
    if problems:
        raise ConversionRefused("Main or the pinned wheel moved: " + "; ".join(problems))


def _historical_producer(root: Path, revision: str, publisher: str, *, same_lock: bool = True) -> tuple[str, Mapping]:
    """Read retained Git blobs, with the same path ordering and bytes as generations.source_digest.

    ``same_lock=False`` drops the identical-``uv.lock`` requirement, for a producer whose loaded code the caller
    holds to the publisher's byte for byte instead (:mod:`spicy_regs.code_closure`).
    """
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ConversionRefused("--expect-producer requires a full lowercase Git commit")

    def git(*arguments: str) -> bytes:
        try:
            return subprocess.run(["git", "-C", str(root), *arguments], check=True, capture_output=True,
                                  env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).stdout
        except subprocess.CalledProcessError as failure:
            raise ConversionRefused(f"Prepared producer history cannot be verified ({arguments[0]}): "
                                    + failure.stderr.decode(errors="replace").strip()[:300]) from failure

    if git("cat-file", "-t", revision).strip() != b"commit":
        raise ConversionRefused("Prepared producer pin is not a commit in this repository")
    git("merge-base", "--is-ancestor", revision, publisher)
    historical_lock = git("show", f"{revision}:uv.lock")
    if same_lock and historical_lock != git("show", f"{publisher}:uv.lock"):
        raise ConversionRefused("Prepared producer and publisher require the same locked runtime")
    locked = next((package for package in tomllib.loads(historical_lock.decode())["package"]
                   if package["name"] == "spicy-docs"), None)
    if locked is None:
        raise ConversionRefused("Prepared producer has no locked SpicyDocs runtime")
    wheels = locked.get("wheels", [])
    if len(wheels) != 1 or not locked.get("source", {}).get("path"):
        raise ConversionRefused("Prepared producer has no single locked vendored SpicyDocs wheel")
    wheel = git("show", f"{revision}:{locked['source']['path']}")
    if "sha256:" + hashlib.sha256(wheel).hexdigest() != wheels[0]["hash"]:
        raise ConversionRefused("Prepared producer wheel bytes differ from its lock")
    digest = hashlib.sha256()
    prefix = "src/spicy_regs/"
    files = []
    for entry in git("ls-tree", "-r", "-z", revision, "--", prefix).split(b"\0"):
        if not entry:
            continue
        properties, name = entry.split(b"\t", 1)
        path = Path(name.decode().removeprefix(prefix))
        if path.suffix != ".py" or "__pycache__" in path.parts:
            continue
        if properties.split()[0] not in (b"100644", b"100755"):
            raise ConversionRefused("Prepared producer Python source is not a regular Git blob")
        files.append(path)
    if not files:
        raise ConversionRefused("Prepared producer Python source is unavailable")
    for path in sorted(files):
        digest.update(path.as_posix().encode() + b"\0")
        digest.update(git("show", f"{revision}:{prefix}{path.as_posix()}"))
    return "urn:spicy-regs:implementation:sha256:" + digest.hexdigest(), locked


def _checkpoint_ancestor(previous: str, observed: str, *, root: Path | None = None) -> None:
    """Both full commits must form a forward path in retained hosted history."""
    if any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value) for value in (previous, observed)):
        raise ConversionRefused("Checkpoint hosted history requires exact full commits")
    root = root or Path(__file__).resolve().parents[2]
    checked = subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", previous, observed],
        capture_output=True, text=True, env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"})
    if checked.returncode != 0:
        raise ConversionRefused("Checkpoint hosted main rewound, diverged or has unavailable ancestry")


def _checkpoint_producer(started: Mapping, producer: str, spicy_docs: str) -> dict:
    """Admit only a frozen clean producer, never substitute a fabricated main state."""
    if not re.fullmatch(r"[0-9a-f]{40}", producer) or started["checkout"] != producer:
        raise ConversionRefused("Checkpoint requires its exact frozen producer checkout")
    problems = _runtime_problems(started, spicy_docs)
    if problems:
        raise ConversionRefused("Checkpoint producer or pinned wheel moved: " + "; ".join(problems))
    if not re.fullmatch(r"[0-9a-f]{40}", started["main"]):
        raise ConversionRefused("Checkpoint requires an exact observed hosted main")
    from spicy_regs.generations import implementation_id
    identity, locked = _historical_producer(Path(__file__).resolve().parents[2], producer, started["main"])
    if (identity != implementation_id() or locked["version"] != started["pinned_spicy_docs"]
            or locked["wheels"][0]["hash"] != started["spicy_docs_wheel"]["locked_sha256"]):
        raise ConversionRefused("Checkpoint producer code or locked runtime differs")
    return {"commit": producer, "implementationId": identity, "lockedSpicyDocs": locked}


def _prepared_implementation(source: Mapping, publisher: Mapping, expected_producer: str | None,
                             closure: Mapping | None = None) -> tuple[str, bool]:
    """Keep the sealed producer distinct from the current publisher; return its identity and whether code rebound it.

    An earlier producer needs its full commit, ancestry, its wheel matching its own lock, and its Git blobs hashing to
    the generation's implementation. Then either the whole locked runtime is the publisher's (identical ``uv.lock``
    and installed wheel), or, for a seal that recorded its loaded code, that code is byte-identical here
    (:mod:`spicy_regs.code_closure`): a lock or wheel move that changed nothing the preparation ran no longer refuses.
    """
    from spicy_regs import code_closure
    from spicy_regs.generations import implementation_id

    # Preserve same-revision callers. Reusing an earlier producer always needs an explicit full pin and Git proof.
    if expected_producer is None:
        if source != publisher:
            raise ConversionRefused("An earlier prepared producer requires --expect-producer")
        return implementation_id(), False
    if (source["checkout"] != expected_producer or source["main"] != expected_producer
            or source["remote"] != publisher["remote"]):
        raise ConversionRefused("Prepared producer pin or repository differs from its sealed source")
    rebind = closure is not None and expected_producer != publisher["checkout"]
    _refuse_moved(source, main=expected_producer, spicy_docs=source["spicy_docs"] if rebind else publisher["spicy_docs"])
    identity, locked = _historical_producer(Path(__file__).resolve().parents[2], expected_producer,
                                          publisher["checkout"], same_lock=not rebind)
    if (locked["version"] != source["pinned_spicy_docs"]
            or locked["wheels"][0]["hash"] != source["spicy_docs_wheel"]["locked_sha256"]
            or not rebind and source["spicy_docs_wheel"] != publisher["spicy_docs_wheel"]):
        raise ConversionRefused("Prepared producer runtime differs from the current installed locked wheel")
    if rebind and (changed := code_closure.differences(closure)):
        raise ConversionRefused(f"Prepared producer's loaded code differs from this publisher's ({len(changed)}): "
                                + ", ".join(changed[:10]))
    return identity, rebind


@dataclass(frozen=True)
class _Built:
    """One admitted generation, its local original comparisons, and a byte-exact public read-back."""

    generation: Path
    policies: Mapping[str, str]
    compare_original: Callable[[str, Path], dict]
    read_published: Callable[[Mapping, Any, str], dict]
    evidence: tuple[Path, ...] = ()
    added_tables: frozenset[str] = frozenset()
    receipt_only_tables: frozenset[str] = frozenset()
    read_operation: ConversionReadOperation | None = None
    check_originals: Callable[[], None] | None = None


def _rollup_class(family: str, base: type | None = None):
    """The scheduled rollup that owns ``family``: by default a subject/receipt one, or any subclass of ``base``."""
    import spicy_regs.pipelines.rollups as rollups
    from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup

    for module in pkgutil.iter_modules(rollups.__path__):
        for value in vars(importlib.import_module(f"{rollups.__name__}.{module.name}")).values():
            if (isinstance(value, type) and issubclass(value, base or SubjectReceiptRollup)
                    and value.__dict__.get("name") == family and value.publication_family is None):
                return value
    return None


def _regulatory_base_class(family: str):
    from spicy_regs.pipelines.rollups.regulatory_base import (
        CommentAttributesFamily, DocketAttributesFamily, DocketsFamily, DocumentAttributesFamily, DocumentsFamily,
    )

    return {cls.name: cls for cls in (DocketsFamily, DocumentsFamily, DocketAttributesFamily,
                                      DocumentAttributesFamily, CommentAttributesFamily)}.get(family)


def _replaced_evidence(base: str, entry: Mapping) -> tuple[dict, list[dict]]:
    """The generation ``entry`` names, and the journal events its next run would have read back (:data:`INHERITED_EVENTS`).

    Reads only pinned roots, manifests and the journal, so it runs before any table is fetched or hashed. Evidence
    written before ``evidence_version`` 2 is left with its old generation: nothing is handed on, and the conversion
    writes current evidence. Any other evidence that is not current completed-build evidence refuses here, not after
    the family's tables are held. The conversion's own ``CaptureEvidence`` still records the earlier pin, so nothing
    on a conversion path may call ``inherited_event``: it would refuse after the downloads.
    """
    from spicy_regs.source_evidence import INPUT_ROLE

    _, replaced = publication.load_family_root(base, entry)
    pin = next((item for item in replaced["inputs"] if item.get("role") == INPUT_ROLE), None)
    if pin is None:
        return replaced, []
    journal = publication.load_evidence_journal(base, pin, predating_ok=True).splitlines()
    return replaced, [
        {name: value for name, value in row.items() if name != "recorded_at"}
        for row in map(json.loads, journal)
        if row.get("event") in INHERITED_EVENTS
    ]


def _journal(evidence, events: Sequence[Mapping]) -> None:
    for row in events:
        evidence.event(row["event"], **{name: value for name, value in row.items() if name != "event"})


def _table_files(path: Path) -> dict[str, Path]:
    """The physical layout of one held table; directories never imply extra Hive columns."""
    if path.is_symlink():
        raise ValueError(f"A retained table cannot be a symlink: {path}")
    if path.is_file():
        return {path.name: path}
    if not path.is_dir():
        raise ValueError(f"A retained table is missing: {path}")
    members = sorted(member for member in path.rglob("*") if member.is_symlink() or not member.is_dir())
    if any(member.is_symlink() or not member.is_file() or member.suffix != ".parquet" for member in members):
        raise ValueError(f"A retained split table has an unexpected member: {path}")
    return {member.relative_to(path).as_posix(): member for member in members}


def _table_rows(path: Path) -> int:
    return sum(pq.ParquetFile(member).metadata.num_rows for member in _table_files(path).values())


def _retain_table(base: str, captured: Mapping, key: str, directory: Path,
                  original_pins: dict | None = None, retained_members: Mapping[str, str] | None = None) -> Path:
    """Fetch every pinned member without collapsing its partition or changing its physical schema."""
    from spicy_regs.generations import _table_info
    from spicy_regs.local_data import file_signature

    descriptor = publication.table_descriptor(captured, key)
    if descriptor is None:
        raise ConversionRefused(f"{key}: no captured table descriptor")
    split = "members" in descriptor
    table_path = directory / (key.removesuffix(".parquet") if split else key)
    seen, signatures = set(), {}
    for member in publication.table_members(captured, key):
        relative = Path(member.key)
        if relative.is_absolute() or ".." in relative.parts or publication.member_table(member.key) != key:
            raise ConversionRefused(f"{key}: member lies outside its captured table: {member.key}")
        if member.key in seen:
            raise ConversionRefused(f"{key}: duplicate captured member: {member.key}")
        seen.add(member.key)
        target = directory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if retained_members is not None:
            from spicy_regs.local_data import file_signature
            if member.path not in retained_members:
                raise ConversionRefused(f"{member.path}: missing from explicit retained member selection; no download attempted")
            local = Path(retained_members[member.path])
            if local.is_symlink() or not local.is_file():
                raise ConversionRefused("Retained selected member must be a regular original file")
            held = file_signature(local)
            if local.stat().st_size != member.byte_size or _file_digest(local) != member.sha256 or file_signature(local) != held:
                raise ConversionRefused("Retained selected member differs from captured bytes")
            os.link(local, target)
        elif not publication.fetch_member(base, member, target, member.path):
            raise ConversionRefused(f"{member.path} is not readable")
        before = file_signature(target)
        observed = _table_info(target)
        if file_signature(target) != before:
            raise ConversionRefused(f"{key}: fetched member changed during its physical check")
        name = member.key.removeprefix(key.removesuffix(".parquet") + "/") if split else member.key
        signatures[name] = before
        if observed["rows"] != member.rows or observed["columns"] != descriptor["columns"]:
            raise ConversionRefused(f"{member.path}: retained physical schema or row count differs from its pin")
    if _table_rows(table_path) != descriptor["rows"]:
        raise ConversionRefused(f"{key}: retained member rows differ from the captured table")
    if _original_states({key: table_path}) != {key: signatures}:
        raise ConversionRefused(f"{key}: fetched member layout changed")
    if original_pins is not None:
        original_pins[key] = signatures
    return table_path


def _convert_rollup(cls, old: Mapping, retained: Mapping[str, Path], base: str, work: Path,
                    handed_on: Sequence[Mapping], read_operation: ConversionReadOperation,
                    original_pins: Mapping) -> _Built:
    """Run the family's rollup with its builder replaced by "return the retained tables"; everything after is its own."""
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    if set(cls.source_outputs) != set(old["tables"]):
        raise ConversionRefused(
            f"{cls.name}: its rollup writes {sorted(cls.source_outputs)} and the published family holds "
            f"{sorted(old['tables'])}; convert only a family whose table set main still writes")
    # A rollup's own constructor arguments and inputs feed only its builder, which is replaced below.
    pipeline = cls.__new__(cls)
    RollupPipeline.__init__(pipeline, output_dir=work / "build", skip_upload=True)
    pipeline.inputs = ()

    def retained_tables(directory: Path, **_) -> tuple[Path, ...]:
        copied = []
        for key in cls.source_outputs:
            source = retained[key]
            target = directory / source.name
            if source.is_dir():
                shutil.copytree(source, target)
            else:
                shutil.copyfile(source, target)
            copied.append(target)
        return tuple(copied)

    def build(output_dir: Path):
        if pipeline.source_evidence:  # inherited from the replaced generation just before the build
            _journal(pipeline.source_evidence, handed_on)
        return pipeline.build_receipts(output_dir, retained_tables)

    pipeline.build = build
    pipeline.run(read_operation=read_operation)
    (generation,) = (work / "build" / "generations").iterdir()

    return _native_built(generation, retained, base,
                         (pipeline.source_evidence.artifact_dir,) if pipeline.source_evidence else (), read_operation, original_pins)


def _native_built(generation: Path, retained: Mapping[str, Path], base: str,
                  evidence: tuple[Path, ...], read_operation: ConversionReadOperation,
                  original_pins: Mapping | None = None, *, semantic: bool = True) -> _Built:
    """Use the exact admitted policies and one family reader for every comparison.

    ``semantic=False`` builds on a byte-only admission (sealed publication): it has no reader, so it cannot compare
    originals; its restoration evidence is the seal's.
    """
    artifact, reader, _ = read_operation.admit_generation(generation, semantic=semantic)
    spec = artifact.root["spec"]
    policies = read_operation.policies(generation)
    receipt_only = frozenset(name + ".parquet" for name, policy in policies.items() if policy.receipt_only)
    added = frozenset(spec["tables"]) - set(retained)
    pinned_originals = _original_states(retained) if original_pins is None else original_pins

    def check_originals(key=None):
        selected = retained if key is None else {key: retained[key]}
        if _original_states(selected) != {name: pinned_originals[name] for name in selected}:
            raise ConversionRefused("Retained originals changed during this conversion operation")

    check_originals()

    def compare_original(dataset: str, original: Path) -> dict:
        if reader is None:
            raise ConversionRefused("A byte-only admission cannot compare originals; take the full verification")
        check_originals(dataset + ".parquet")
        # Only maintained base-family conversion explicitly restores the captured source schema.
        schema = ({"source_schema": pq.read_schema(original)} if _regulatory_base_class(spec["family"]) is not None
                  else {})
        checked = reader.compare_original(dataset, original, **schema)
        check_originals(dataset + ".parquet")
        return checked

    def read_published(index: Mapping, client, bucket: str) -> dict:
        # Local proof is finished: release the admitted reader's processing state before checking the public bytes.
        check_originals()
        read_operation.release_generation(generation)
        verified = read_operation.verify_public_generation(base, index, spec["family"], generation, client, bucket)
        check_originals()
        return {"public_bytes": verified}

    return _Built(generation, {name: policy.policy_version for name, policy in policies.items()},
                  compare_original, read_published, evidence, added, receipt_only,
                  read_operation=read_operation, check_originals=check_originals)


def _convert_fec(family, old, captured, retained, base, work, handed_on,
                 retain_evidence, read_operation, original_pins, *, filing_associations=None,
                 read_snapshot=None, observation_generation=None, check_resources=None, observation_rebinding=None,
                 observation_checkpoint=None, observation_source_pin=None, observation_hosted_main=None):
    from spicy_regs.fec_native_conversion import build_retained_family
    evidence = None
    if retain_evidence:
        from spicy_regs.source_evidence import CaptureEvidence
        (work / "build").mkdir(parents=True, exist_ok=True)
        evidence = CaptureEvidence(work / "build", family)
        evidence.inherit(dict(captured), public_url=base)
        _journal(evidence, handed_on)
    try:
        generation = build_retained_family(family, old, captured, retained, work,
            operation=read_operation, inputs=evidence.inputs() if evidence else (),
            filing_associations=filing_associations, read_snapshot=read_snapshot,
            observation_generation=observation_generation, check_resources=check_resources,
            observation_rebinding=observation_rebinding, observation_checkpoint=observation_checkpoint,
            observation_source_pin=observation_source_pin, observation_hosted_main=observation_hosted_main)
    except BaseException as error:
        if evidence:
            evidence.finish(error)
        raise
    if evidence:
        evidence.finish()
    return _native_built(generation, retained, base,
        (evidence.artifact_dir,) if evidence else (), read_operation, original_pins)


def _conversion_snapshot(captured, receipt, operation, *, return_rebinding=False):
    """Keep captured prior separate from a locally admitted staged observations parent."""
    staged = receipt.get("staged_observations")
    if staged is None:
        return (captured, None) if return_rebinding else captured
    if receipt["family"] != "fec-query":
        raise ConversionRefused("Only FEC query may select staged observations")
    from spicy_regs.fec_native_conversion import staged_snapshot
    generation = Path(staged["directory"])
    artifact, reader, _ = operation.admit_generation(generation, expected_pin=staged["pin"])
    parent_work = generation.parent.parent
    parent_receipt = staged.get("preparation")
    if parent_receipt is None:
        parent_receipt = json.loads((parent_work / RECEIPT).read_bytes())
        staged["preparation"] = parent_receipt
    seal = parent_receipt.get("prepared", {})
    if (parent_receipt.get("family") != "fec-observations"
            or seal.get("format") != "spicy-regs-native-conversion-prepared"
            or seal.get("receiptDigest") != _prepared_digest(parent_receipt)
            or seal.get("generationPin") != artifact.pin.as_dict()
            or parent_receipt.get("published") is not None
            or any(key in parent_receipt for key in ("refused", "publish_attempt", "rollback_attempt"))
            or seal.get("capturedIndexSha256") != _file_digest(parent_work / "captured-publication.v2.json")):
        raise ConversionRefused("Staged observations require their untouched official preparation seal")
    parent_capture = publication.parse_index((parent_work / "captured-publication.v2.json").read_bytes())
    if parent_capture != captured or parent_receipt["captured"]["entry"] != captured["families"]["fec-observations"]:
        raise ConversionRefused("Staged observations differ from the complete captured source prior")
    originals, _ = _retained_inputs(parent_capture, parent_receipt["captured"]["entry"], parent_work)
    for key, original in originals.items():
        if _has_differences(reader.compare_original(key.removesuffix(".parquet"), original)):
            raise ConversionRefused("Staged observations do not restore exact captured original values")
    snapshot = staged_snapshot(captured, generation, operation)
    if return_rebinding:
        from spicy_regs.fec_native_conversion import ObservationRebinding
        return snapshot, ObservationRebinding(captured['families']['fec-observations']['artifactDigest'],
            artifact.pin.artifact_digest)
    return snapshot


def _convert_court(family: str, old: Mapping, captured: Mapping, retained: Mapping[str, Path], base: str,
                   work: Path, handed_on: Sequence[Mapping], retain_evidence: bool,
                   read_operation: ConversionReadOperation, original_pins: Mapping) -> _Built:
    """Court tables: the shared court writer over each retained table, then the court generation admission."""
    from spicy_regs.court_receipts import (
        build_court_generation, finish_court_output,
    )
    evidence = None
    if retain_evidence:
        # As a scheduled run does: this generation names the one it replaces and one evidence artifact of its own.
        from spicy_regs.source_evidence import CaptureEvidence

        (work / "build").mkdir(parents=True, exist_ok=True)
        evidence = CaptureEvidence(work / "build", family)
        evidence.inherit(dict(captured), public_url=base)
        _journal(evidence, handed_on)
    generation_id = uuid4().hex
    subjects = {}
    for key in old["tables"]:
        (member,) = publication.table_members(captured, key)
        # The witness names the immutable published object, not this machine's copy of it.
        witness = {"source_id": member.path, "source_uri": None, "sha256": member.sha256, "locator": None,
                   "body_version": None}
        subjects[key.removesuffix(".parquet")] = finish_court_output(
            key.removesuffix(".parquet"), retained[key], work / "court", witnesses=[witness], generation_id=generation_id)
    generation = work / "build" / "generation"
    try:
        build_court_generation(generation, family=family, files=list(subjects.values()),
                               publication_status="complete-family", read_snapshot=captured,
                               inputs=evidence.inputs() if evidence else (), read_operation=read_operation)
    except BaseException as error:
        if evidence:
            evidence.finish(error)
        raise
    if evidence:
        evidence.finish()

    return _native_built(generation, retained, base,
                        (evidence.artifact_dir,) if evidence else (), read_operation, original_pins)


def _convert_regulatory_base(cls, captured: Mapping, retained: Mapping[str, Path], base: str,
                             work: Path, handed_on: Sequence[Mapping], retain_evidence: bool,
                   read_operation: ConversionReadOperation, original_pins: Mapping) -> _Built:
    from spicy_regs.generations import build_generation
    from spicy_regs.transforms.regulations_receipts import policy, write_held_dataset

    dataset = cls.output.removesuffix(".parquet")
    generation_id = uuid4().hex
    evidence = None
    if retain_evidence:
        from spicy_regs.source_evidence import CaptureEvidence

        (work / "build").mkdir(parents=True, exist_ok=True)
        evidence = CaptureEvidence(work / "build", cls.name)
        evidence.inherit(dict(captured), public_url=base)
        _journal(evidence, handed_on)
    try:
        (member,) = publication.table_members(captured, cls.output)
        witness = {"source_id": member.path, "source_uri": None, "sha256": member.sha256,
                   "locator": None, "body_version": None}
        subject, receipts = write_held_dataset(
            dataset, retained[cls.output], work / "regulatory-native", generation_id=generation_id,
            witnesses=[witness], bulk=True,
        )
        owner = cls.__new__(cls)
        owner.validate_native_output(subject)
        generation = work / "build" / "generation"
        build_generation(
            generation, family=cls.name, files=[subject], expected_keys=[cls.output],
            schemas=owner.generation_schemas(), read_snapshot=captured,
            inputs=evidence.inputs() if evidence else (), receipt_path=receipts,
            receipt_policies=[policy(dataset)], receipt_generation_id=generation_id, read_operation=read_operation,
        )
    except BaseException as error:
        if evidence:
            evidence.finish(error)
        raise
    if evidence:
        evidence.finish()
    return _native_built(generation, retained, base,
                                  (evidence.artifact_dir,) if evidence else (), read_operation, original_pins)


def _differences(restored: Path, retained: Path) -> dict:
    """How the rows a native read restores differ from the retained table: columns, types, and rows each way."""
    import duckdb

    def literal(path: Path) -> str:
        return "'" + str(path).replace("'", "''") + "'"

    with duckdb.connect() as con:
        described = [dict((name, kind) for name, kind, *_ in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({literal(path)}, hive_partitioning=false)").fetchall())
                     for path in (restored, retained)]
        shared = [name for name in described[1] if name in described[0]]
        # A column of one type on both sides is compared as that type. Text is only the common ground for a column
        # whose type changed, which is reported and refused anyway; as text, ['a, b'] and ['a', 'b'] are one value.
        quoted = {name: '"' + name.replace('"', '""') + '"' for name in shared}
        text = ", ".join(quoted[name] if described[0][name] == described[1][name]
                         else f"CAST({quoted[name]} AS VARCHAR) AS {quoted[name]}" for name in shared) or "NULL"
        either = [con.execute(f"SELECT count(*) FROM (SELECT {text} FROM read_parquet({literal(a)}, hive_partitioning=false)"
                              f" EXCEPT ALL SELECT {text} FROM read_parquet({literal(b)}, hive_partitioning=false))")
                  .fetchall()[0][0]
                  for a, b in ((restored, retained), (retained, restored))]
    metadata = [pq.read_schema(path).metadata or {} for path in (restored, retained)]
    return {
        "metadata_changes": sorted(key.decode(errors="replace") for key in metadata[0].keys() | metadata[1].keys()
                                   if metadata[0].get(key) != metadata[1].get(key)),
        "columns_only_in_restored": sorted(set(described[0]) - set(described[1])),
        "columns_only_in_retained": sorted(set(described[1]) - set(described[0])),
        "type_changes": {name: [described[1][name], described[0][name]] for name in shared
                         if described[0][name] != described[1][name]},
        "rows_only_in_restored": either[0],
        "rows_only_in_retained": either[1],
    }


def _table_differences(restored: Path, retained: Path) -> dict:
    """Compare each exact member, including empty files and metadata, rather than only a table-wide row union."""
    if restored.is_dir() != retained.is_dir():
        raise ValueError("Restored table changed between a single file and a split directory")
    if not retained.is_dir():
        return _differences(restored, retained)
    actual, original = _table_files(restored), _table_files(retained)
    members = {name: _differences(actual[name], original[name]) for name in sorted(actual.keys() & original.keys())}
    for name, check in members.items():
        check["schema_changed"] = not pq.read_schema(actual[name]).equals(pq.read_schema(original[name]), check_metadata=True)
    only_actual, only_original = sorted(actual.keys() - original.keys()), sorted(original.keys() - actual.keys())
    return {
        "members": members, "members_only_in_restored": only_actual, "members_only_in_retained": only_original,
        "schema_changes": [name for name, check in members.items() if check["schema_changed"]],
        **{field: [f"{name}:{value}" for name, check in members.items() for value in check[field]]
           for field in ("metadata_changes", "columns_only_in_restored", "columns_only_in_retained")},
        "type_changes": {f"{name}:{column}": change for name, check in members.items()
                         for column, change in check["type_changes"].items()},
        "rows_only_in_restored": sum(check["rows_only_in_restored"] for check in members.values())
        + sum(_table_rows(actual[name]) for name in only_actual),
        "rows_only_in_retained": sum(check["rows_only_in_retained"] for check in members.values())
        + sum(_table_rows(original[name]) for name in only_original),
    }


def _has_differences(check: Mapping) -> bool:
    return any(check.get(field) for field in (
        "members_only_in_restored", "members_only_in_retained", "schema_changes", "metadata_changes", "type_changes",
        "columns_only_in_restored", "columns_only_in_retained", "rows_only_in_restored", "rows_only_in_retained"))


def _target(expected: str | None, action: str) -> dict:
    """The bucket and endpoint this run writes to, which must be the bucket the operator named."""
    try:
        r2.require_credentials(action)
    except RuntimeError as missing:
        raise ConversionRefused(str(missing)) from missing
    bucket, endpoint = os.getenv("R2_BUCKET_NAME"), os.getenv("R2_ENDPOINT")
    if not bucket or not endpoint:
        raise ConversionRefused("R2_BUCKET_NAME and R2_ENDPOINT must both be set: no bucket is assumed")
    if bucket != expected:
        raise ConversionRefused(f"this run would write to bucket {bucket}, not the --expect-bucket {expected}")
    target = {"bucket": bucket, "endpoint_host": urlsplit(endpoint).hostname}
    print(f"{action}: bucket {bucket} at {target['endpoint_host']}", flush=True)
    return target


def _same_generation(entry: Mapping | None, other: Mapping | None) -> bool:
    def named(value):
        return None if value is None else {key: item for key, item in value.items() if key != "publishedAt"}

    return named(entry) == named(other)


def _published_state(family: str, entry: Mapping | None, receipt_path: Path, bucket: str) -> str:
    return (f"{family} IS published as {(entry or {}).get('artifactDigest')}. Roll back with: {SCRIPT} --rollback "
            f"{receipt_path} --expect-bucket {bucket} (and the same --env-file)")


def _uncertain_state(action: str, receipt_path: Path, bucket: str | None) -> str:
    return (f"a {action} was attempted and its result is not known. "
            f"{SCRIPT} --rollback {receipt_path} --expect-bucket {bucket} reads the stored index and "
            "restores the captured entry if this conversion's generation is what it names")


def _write_receipt(path: Path, receipt: Mapping) -> None:
    """Keep the previous complete rollback record until its replacement is written and synced."""
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(receipt, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _prepared_digest(receipt: Mapping) -> str:
    value = dict(receipt)
    value["prepared"] = {key: item for key, item in value["prepared"].items() if key != "receiptDigest"}
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _resume_unsealed(source: Mapping) -> str:
    """How to finish a preparation that refused to seal: resume on current main, pinned to the code that prepared it."""
    return ("the receipt stays unsealed: resume it with --resume-generation from a clean checkout of current main and "
            f"--expect-producer {source['checkout']} (the preparation's checkout)")


def _seal_prepared(receipt: dict, built: _Built, work: Path, identity: Mapping) -> None:
    """Seal a qualified preparation: its pin, captured index, evidence, and the identity its admission ran under.

    The admission key binds the semantic admission and restoration this process just completed to this validator
    identity, so a publisher under the same key may rely on them (:func:`publish_prepared`). The receipt digest
    covers all of it; the operator carries it to publication from this run's output, never from the receipt.
    ``identity`` is the one taken when the run started: the process ran the modules it imported then, so a
    checkout, pull or SpicyDocs re-sync on disk since then refuses the seal rather than naming code that did not run.
    The receipt stays unsealed and resumable.
    ``closure`` records the code this process loaded (:mod:`spicy_regs.code_closure`); a publisher on a later main
    may rebind the seal on it (:func:`_prepared_implementation`). It is hashed from disk at seal; the
    start-identity check covers its module bytes but not SpicyRegs data files other than ``.json`` and ``.xsd``.
    """
    from spicy_regs import code_closure
    from spicy_regs.generations import admission_key, validation_identity

    if built.read_operation is None:
        raise ConversionRefused("Preparation seal requires its active admitted operation")
    if validation_identity() != identity:
        raise ConversionRefused("The validator code on disk changed during this preparation; "
                                + _resume_unsealed(receipt["source"]))
    if built.check_originals:
        built.check_originals()
    admitted, _ = built.read_operation.verified_source(built.generation)
    artifact = admitted.root
    pin = {key: artifact[key] for key in ("logicalId", "artifactDigest")}
    receipt["prepared"] = {
        "format": "spicy-regs-native-conversion-prepared", "version": 2,
        "generationPin": pin,
        "validationIdentity": dict(identity),
        "admissionKey": admission_key(pin, identity),
        "closure": code_closure.record(),
        "capturedIndexSha256": _file_digest(work / "captured-publication.v2.json"),
        "evidenceDirectories": [path.resolve().relative_to(work.resolve()).as_posix() for path in built.evidence],
    }
    receipt["prepared"]["receiptDigest"] = _prepared_digest(receipt)


def _prepared_path(work: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = work / path
    if path.is_symlink() or not path.resolve().is_relative_to(work.resolve()) or not path.exists():
        raise ConversionRefused("Prepared input is missing or outside its original work directory")
    return path


def _prepared_refusal_state(receipt_path: Path) -> str:
    return ("This invocation made no new publication attempt. The saved receipt does not establish the current "
            f"stored pointer. Reconcile any previous publication outcome read-only using {receipt_path} before "
            "another write.")


def _original_states(retained: Mapping[str, Path]) -> dict:
    from spicy_regs.local_data import file_signature

    return {key: {name: file_signature(path) for name, path in _table_files(table).items()}
            for key, table in retained.items()}


def _retained_inputs(captured: Mapping, old: Mapping, work: Path) -> tuple[dict[str, Path], dict]:
    """Recheck retained bytes against publisher pins; no cached proof substitutes for these checks."""
    from spicy_regs.generations import _table_info
    from spicy_regs.local_data import file_signature

    retained, signatures = {}, {}
    for key, descriptor in old["tables"].items():
        path = _prepared_path(work, "retained/" + (key.removesuffix(".parquet") if "members" in descriptor else key))
        members = publication.table_members(captured, key)
        expected = {member.key.removeprefix(key.removesuffix(".parquet") + "/") if path.is_dir() else member.key
                    for member in members}
        if set(_table_files(path)) != expected:
            raise ConversionRefused(f"{key}: retained member layout changed")
        signatures[key] = {}
        for member in members:
            target = _prepared_path(work, "retained/" + member.key)
            before = file_signature(target)
            if target.stat().st_size != member.byte_size or _file_digest(target) != member.sha256:
                raise ConversionRefused(f"{key}: retained member bytes changed")
            observed = _table_info(target)
            if observed["columns"] != descriptor["columns"] or observed["rows"] != member.rows:
                raise ConversionRefused(f"{key}: retained member schema or rows changed")
            if file_signature(target) != before:
                raise ConversionRefused(f"{key}: retained member changed during its byte check")
            name = member.key.removeprefix(key.removesuffix(".parquet") + "/") if path.is_dir() else member.key
            signatures[key][name] = before
        retained[key] = path
    if _original_states(retained) != signatures:
        raise ConversionRefused("Retained originals changed during admission")
    return retained, signatures


def _check_prepared_membership(family: str, old: Mapping, spec: Mapping) -> None:
    """Qualify actual historical outputs against maintained family/partition declarations."""
    from spicy_regs.court_receipts import POLICIES as court_policies

    regulatory = _regulatory_base_class(family)
    from spicy_regs.fec_native_conversion import FAMILIES, check_membership, published_outputs
    # Captured tables by the key the prepared generation publishes them under: FEC retires
    # some captured tables and renames rebuilt ones (ss-j4vz B10, Q1, Q2).
    outputs = published_outputs(family, old["tables"]) if family in FAMILIES else old["tables"]
    if family in FAMILIES:
        check_membership(family, old)
        allowed_added = frozenset()
    elif regulatory is not None:
        if set(old["tables"]) != {regulatory.output} or "members" in old["tables"][regulatory.output]:
            raise ConversionRefused("Prepared regulatory family membership differs")
        allowed_added = frozenset()
    elif all(key.removesuffix(".parquet") in court_policies for key in old["tables"]):
        if any("members" in table for table in old["tables"].values()):
            raise ConversionRefused("Prepared court family has unsupported split members")
        allowed_added = frozenset()
    elif (cls := _rollup_class(family)) is not None:
        if set(cls.source_outputs) != set(old["tables"]) or any(
                ("members" in table) != (key in cls.partitioned) or "members" in table
                and tuple(table["partitionColumns"]) != tuple(cls.partitioned[key])
                for key, table in old["tables"].items()):
            raise ConversionRefused("Prepared family membership or partition declaration differs")
        allowed_added = frozenset(cls.added_tables)
    else:
        raise ConversionRefused("Prepared family has no maintained converter reader")
    descriptors = {value["dataset"]: value for value in spec["etlReceipts"]["policies"]}
    if family in FAMILIES and set(descriptors) != {key.removesuffix(".parquet") for key in outputs}:
        raise ConversionRefused("Prepared FEC receipt membership differs from its complete registered siblings")
    receipt_only = {name + ".parquet" for name, value in descriptors.items() if value["receipt_only"]}
    expected = set(outputs) - receipt_only
    if not expected <= set(spec["tables"]) or set(spec["tables"]) - expected - allowed_added:
        raise ConversionRefused("Prepared subject membership differs from the family's declared outputs")
    if not {key.removesuffix(".parquet") for key in outputs} <= set(descriptors):
        raise ConversionRefused("Prepared original output has no admitted receipt policy")
    for key, table in outputs.items():
        if key in receipt_only:
            continue
        actual = spec["tables"][key]
        if ("members" in table) != ("partitionColumns" in actual) or (
                "members" in table and table["partitionColumns"] != actual["partitionColumns"]):
            raise ConversionRefused("Prepared native partition declaration differs from captured inputs")


def _prepared_evidence(generation_root: Mapping, family: str, evidence: tuple[Path, ...]) -> None:
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE, verify_evidence

    admitted = [verify_evidence(path) for path in evidence]
    if ([{"role": INPUT_ROLE, **item.pin.as_dict()} for item in admitted]
            != [item for item in generation_root["inputs"] if item["role"] == INPUT_ROLE]
            or any(item.root["spec"]["family"] != family or item.root["inputs"] != [
                value for value in generation_root["inputs"] if value["role"] == PRIOR_ROLE] for item in admitted)):
        raise ConversionRefused("Prepared source evidence differs from the generation")


def _receipt_producer_implementation(receipt: Mapping, now: Mapping, expected_producer: str | None) -> tuple[str, bool]:
    """The sealed producer's implementation identity, and whether its loaded code (not its lock) rebound it here.

    A resumed or checkpointed receipt names a ``producer`` whose writer ran in another process than the seal, so its
    seal's loaded code does not cover the writer: it keeps the identical-lock rule.
    """
    producer = receipt.get("producer")
    if producer is None:
        return _prepared_implementation(receipt["source"], now, expected_producer, receipt["prepared"].get("closure"))
    if expected_producer is None or producer.get("commit") != expected_producer:
        raise ConversionRefused("Resumed producer requires its exact --expect-producer commit")
    admission = receipt.get("checkpoint_admission")
    if admission is not None:
        source = receipt["source"]
        if (receipt["family"] != "fec-observations"
                or source["checkout"] != expected_producer or source["remote"] != now["remote"]
                or admission != {"observed_remote_main": source["main"], "remote": source["remote"],
                                 "producer_commit": expected_producer}
                or _runtime_problems(source, now["spicy_docs"])
                or source["spicy_docs_wheel"] != now["spicy_docs_wheel"]):
            raise ConversionRefused("Checkpoint prepared source or observed history differs")
        _checkpoint_ancestor(expected_producer, source["main"])
        _checkpoint_ancestor(source["main"], now["checkout"])
    identity, locked = _historical_producer(Path(__file__).resolve().parents[2], expected_producer, now["checkout"])
    if (producer != {"commit": expected_producer, "implementationId": identity, "lockedSpicyDocs": locked}
            or locked["version"] != now["pinned_spicy_docs"]
            or locked["wheels"][0]["hash"] != now["spicy_docs_wheel"]["locked_sha256"]):
        raise ConversionRefused("Resumed producer source or locked runtime differs")
    return identity, False


def resume_prepared(generation: Path, *, work: Path, allowed: Sequence[str], expected_main: str,
                    expected_spicy_docs: str, expected_producer: str, remote: str = "origin",
                    state: Callable[[str], dict] | None = None) -> dict:
    """Finish a retained writer's complete generation; never rerun its writer or publish it."""
    from spicy_regs.generations import implementation_id, validation_identity
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    from spicy_regs.source_evidence import INPUT_ROLE

    state = state or source_state
    work = work.resolve()
    try:
        now = state(remote)
        validator = validation_identity()
        _refuse_moved(now, main=expected_main, spicy_docs=expected_spicy_docs)
        base = (os.getenv("R2_PUBLIC_URL") or "").rstrip("/")
        if not base:
            raise ConversionRefused("R2_PUBLIC_URL is not set")
        receipt_path = work / RECEIPT
        previous = json.loads(receipt_path.read_bytes()) if receipt_path.exists() else {}
        if receipt_path.is_symlink() or any(key in previous for key in (
                "prepared", "refused", "publish_attempt", "rollback_attempt", "rolled_back")) or previous.get("published"):
            raise ConversionRefused("Resume requires an unsealed generation with no publication attempt")
        captured = publication.parse_index(_prepared_path(work, "captured-publication.v2.json").read_bytes())
        generation = _prepared_path(work, str(generation))
        with ConversionReadOperation() as operation:
            artifact, _, _ = operation.admit_generation(generation)
            spec = artifact.root["spec"]
            family = spec["family"]
            if family not in allowed:
                raise ConversionRefused(f"{family} is not in this run's allow-list")
            old = captured["families"][family]
            if (spec["publicationStatus"] != "complete-family" or spec["readSnapshot"] != _conversion_snapshot(captured, previous, operation)
                    or "etlReceipts" in old or previous.get("family", family) != family
                    or previous.get("captured", {}).get("entry", old) != old):
                raise ConversionRefused("Resume generation does not bind its captured complete prior")
            replaced, handed_on = _replaced_evidence(base, old)
            identity, locked = _historical_producer(Path(__file__).resolve().parents[2], expected_producer, now["checkout"])
            if (artifact.root["producer"]["implementationId"] != identity
                    or artifact.root["producer"]["verifierImplementationId"] != identity
                    or locked["version"] != now["pinned_spicy_docs"]
                    or locked["wheels"][0]["hash"] != now["spicy_docs_wheel"]["locked_sha256"]
                    or any(version(name) != value for name, value in spec["packages"].items())):
                raise ConversionRefused("Retained producer source or installed locked runtime differs")
            _check_prepared_membership(family, old, spec)
            retained, original_pins = _retained_inputs(captured, old, work)
            pins = {item["artifactDigest"] for item in artifact.root["inputs"] if item["role"] == INPUT_ROLE}
            evidence = tuple(path.parent for path in sorted((work / "build" / "source-evidence").glob("*/artifact/artifact.json"))
                             if json.loads(path.read_bytes()).get("artifactDigest") in pins)
            _prepared_evidence(artifact.root, family, evidence)
            built = _native_built(generation, retained, base, evidence, operation, original_pins)
            scheduled = _regulatory_base_class(family) or _rollup_class(family, RollupPipeline)
            retains = bool(replaced["inputs"]) or bool(scheduled and scheduled.retain_source_evidence)
            proof, problems, _ = _qualify_conversion(family, built, old, retained, replaced, retains, handed_on)
            if problems:
                raise ConversionRefused(f"Retained generation does not qualify: {problems}")
            if state(remote) != now:
                raise ConversionRefused("Source or installed wheel moved during resume verification")
            receipt = {"family": family, "started_at": _now(), "source": now, "public_url": base,
                       "target": None, "captured": {"entry": old}, "published": None,
                       "producer": {"commit": expected_producer, "implementationId": identity, "lockedSpicyDocs": locked},
                       "preparation_verifier": {"source": now, "implementationId": implementation_id()}, **proof,
                       **({"staged_observations": previous["staged_observations"]} if "staged_observations" in previous else {})}
            operation.verified_source(generation)
            _seal_prepared(receipt, built, work, validator)
            _write_receipt(receipt_path, receipt)
            return receipt
    except (KeyError, TypeError, ValueError, OSError) as failure:
        raise ConversionRefused(f"Retained conversion cannot be verified: {type(failure).__name__}: {failure}") from failure


def _sealed_admission(receipt: Mapping, full_verification: bool, closure_bound: bool) -> bool:
    """Whether this publisher may rely on the seal's semantic admission and restoration instead of repeating them.

    A version-2 seal qualifies under this process's admission key (the key an owner process uses in memory:
    generations.admission_key), or when ``closure_bound``: everything the sealing process loaded, validators
    included, is byte-identical here. Staged FEC observations keep the full path: their parent generation's
    admission and restoration are not part of this seal.
    """
    from spicy_regs.generations import admission_key, validation_identity

    sealed = receipt["prepared"]
    return (not full_verification and sealed["version"] == 2 and "staged_observations" not in receipt
            and (closure_bound or sealed["admissionKey"] == admission_key(sealed["generationPin"],
                                                                         validation_identity())))


def publish_prepared(receipt_path: Path, *, allowed: Sequence[str], expected_main: str, expected_spicy_docs: str,
                     expect_bucket: str, expected_receipt_digest: str, remote: str = "origin",
                     state: Callable[[str], dict] | None = None, expected_producer: str | None = None,
                     full_verification: bool = False,
                     read_operation: ConversionReadOperation | None = None) -> dict:
    """Publish one completed official dry run after rechecking its bytes against the seal the operator names.

    ``expected_receipt_digest`` comes from the preparation run's own output, never from the receipt it covers: it
    is what makes the seal's recorded proof the operator's, not the receipt's word for itself. Every member and
    retained original is hashed again, and every byte, structural, membership, lineage and policy check runs. Row
    admission and original restoration are repeated only when the seal's admission key differs from this process's
    (other code, runtime or platform), for a version-1 seal, for staged FEC observations, or with
    ``full_verification``. The source writer never runs here. Separate field, server and coverage qualifications in
    the release package must name this receipt's exact artifact.
    """
    if read_operation is None:
        with ConversionReadOperation() as operation:
            return publish_prepared(receipt_path, allowed=allowed, expected_main=expected_main,
                                    expected_spicy_docs=expected_spicy_docs, expect_bucket=expect_bucket,
                                    expected_receipt_digest=expected_receipt_digest, remote=remote, state=state,
                                    expected_producer=expected_producer, full_verification=full_verification,
                                    read_operation=operation)
    state = state or source_state
    verified = _verify_prepared(receipt_path, allowed=allowed, expected_main=expected_main,
                                expected_spicy_docs=expected_spicy_docs, expect_bucket=expect_bucket,
                                expected_receipt_digest=expected_receipt_digest, remote=remote, state=state,
                                expected_producer=expected_producer, full_verification=full_verification,
                                read_operation=read_operation)
    return _publish_conversion(verified.receipt, verified.built, verified.captured, receipt_path.parent,
                               expected_main=expected_main, expected_spicy_docs=expected_spicy_docs, remote=remote,
                               state=state, exact_prior=True, publisher_source=verified.publisher,
                               conversion_proof=verified.conversion_proof, verification=verified.verification)


@dataclass(frozen=True)
class _VerifiedPrepared:
    """A prepared receipt that passed every pre-publication check in this operation, ready for the pointer write."""

    receipt: dict
    built: _Built
    captured: Mapping
    publisher: Mapping
    conversion_proof: publication.NativeConversionProof | None
    verification: Mapping


def _verify_prepared(receipt_path: Path, *, allowed: Sequence[str], expected_main: str, expected_spicy_docs: str,
                     expect_bucket: str, expected_receipt_digest: str, remote: str, state: Callable[[str], dict],
                     expected_producer: str | None, full_verification: bool,
                     read_operation: ConversionReadOperation) -> _VerifiedPrepared:
    """Every check :func:`publish_prepared` makes before it writes; nothing here writes to the bucket or receipt."""
    work = receipt_path.parent
    started = time.monotonic()
    try:
        now = state(remote)
        _refuse_moved(now, main=expected_main, spicy_docs=expected_spicy_docs)
        if not RECEIPT_DIGEST.fullmatch(expected_receipt_digest or ""):
            raise ConversionRefused("--expect-receipt-digest must be the sha256: digest the preparation printed")
        if receipt_path.name != RECEIPT or receipt_path.is_symlink() or not receipt_path.is_file():
            raise ConversionRefused("--publish-prepared needs an existing official conversion.json")
        receipt = json.loads(receipt_path.read_bytes())
        sealed = receipt.get("prepared", {})
        if (sealed.get("format") != "spicy-regs-native-conversion-prepared" or sealed.get("version") not in (1, 2)
                or sealed.get("receiptDigest") != _prepared_digest(receipt)
                or "published" not in receipt or receipt["published"] is not None
                or any(key in receipt for key in ("refused", "publish_attempt", "rollback_attempt", "rolled_back"))):
            raise ConversionRefused("Prepared receipt is incomplete, altered, refused, or already attempted")
        if sealed["receiptDigest"] != expected_receipt_digest:
            raise ConversionRefused("Prepared receipt is not the one the operator named: its digest differs from "
                                    "--expect-receipt-digest")
        family, base = receipt["family"], receipt["public_url"]
        if family not in allowed:
            raise ConversionRefused(f"{family} is not in this run's allow-list")
        producer_implementation, closure_bound = _receipt_producer_implementation(receipt, now, expected_producer)
        if base != (os.getenv("R2_PUBLIC_URL") or "").rstrip("/"):
            raise ConversionRefused("Prepared public URL differs from R2_PUBLIC_URL")
        capture_path = _prepared_path(work, "captured-publication.v2.json")
        if _file_digest(capture_path) != sealed["capturedIndexSha256"]:
            raise ConversionRefused("Prepared publication snapshot changed")
        captured = publication.parse_index(capture_path.read_bytes())
        old = captured["families"][family]
        if old != receipt["captured"]["entry"] or "etlReceipts" in old:
            raise ConversionRefused("Prepared captured family differs or is already native")
        target = _target(expect_bucket, "Publishing a prepared conversion")
        if publication.stored_family(r2.get_r2_client(), target["bucket"], family) != old:
            raise ConversionRefused("Prepared family's complete captured entry changed; nothing was uploaded")
        replaced, handed_on = _replaced_evidence(base, old)
        retained, original_pins = _retained_inputs(captured, old, work)
        generation = _prepared_path(work, receipt["generation"]["directory"])
        semantic = not _sealed_admission(receipt, full_verification, closure_bound)
        artifact, _, _ = read_operation.admit_generation(generation, semantic=semantic)
        spec = artifact.root["spec"]
        if (artifact.pin.as_dict() != sealed["generationPin"]
                or artifact.pin.artifact_digest != receipt["generation"]["artifactDigest"]
                or spec["family"] != family or spec["publicationStatus"] != "complete-family"
                or spec["readSnapshot"] != _conversion_snapshot(captured, receipt, read_operation)
                or spec["etlReceipts"]["generationId"] != receipt["generation"]["receiptGenerationId"]
                or artifact.root["producer"]["implementationId"] != producer_implementation
                or artifact.root["producer"]["verifierImplementationId"] != producer_implementation
                # A closure-bound producer's SpicyDocs is held by the bytes it loaded, not its release string.
                or any(version(name) != value for name, value in spec["packages"].items()
                       if not (closure_bound and name == "spicy-docs"))):
            raise ConversionRefused("Prepared generation, captured inputs, or implementation differs")
        evidence = tuple(_prepared_path(work, value) for value in sealed["evidenceDirectories"])
        _prepared_evidence(artifact.root, family, evidence)
        from spicy_regs.pipelines.rollups.base import RollupPipeline

        _check_prepared_membership(family, old, spec)
        built = _native_built(generation, retained, base, evidence, read_operation, original_pins, semantic=semantic)
        scheduled = _regulatory_base_class(family) or _rollup_class(family, RollupPipeline)
        retains = bool(replaced["inputs"]) or bool(scheduled and scheduled.retain_source_evidence)
        proof, problems, conversion_proof = _qualify_conversion(
            family, built, old, retained, replaced, retains, handed_on, sealed=None if semantic else receipt
        )
        if problems or any(receipt.get(key) != value for key, value in proof.items()):
            raise ConversionRefused(f"Prepared restoration proof differs: {problems}")
        verification = {"mode": "full" if semantic else "sealed-admission", "rebound_by_loaded_code": closure_bound,
                        "seconds": round(time.monotonic() - started, 1)}
    except (ConversionRefused, publication.PublicationError) as failure:
        raise ConversionRefused(str(failure), state=_prepared_refusal_state(receipt_path)) from failure
    except (KeyError, TypeError, ValueError, OSError) as failure:
        raise ConversionRefused(f"Prepared conversion cannot be verified: {type(failure).__name__}: {failure}",
                                state=_prepared_refusal_state(receipt_path)) from failure
    receipt["target"] = target
    return _VerifiedPrepared(receipt, built, captured, now, conversion_proof, verification)


def convert(family: str, *, allowed: Sequence[str], work: Path, expected_main: str, expected_spicy_docs: str,
            publish: bool = False, remote: str = "origin", state: Callable[[str], dict] | None = None,
            expect_bucket: str | None = None, read_operation: ConversionReadOperation | None = None,
            filing_associations=None, staged_observations: Path | None = None,
            retained_members: Mapping[str, str] | None = None,
            check_resources: Callable[[], None] | None = None, observation_checkpoint: Path | None = None) -> dict:
    """Convert ``family`` in ``work``; with ``publish``, move its pointer and read the result back anonymously."""
    if read_operation is None:
        try:
            with ConversionReadOperation() as operation:
                return convert(family, allowed=allowed, work=work, expected_main=expected_main,
                               expected_spicy_docs=expected_spicy_docs, publish=publish, remote=remote,
                               state=state, expect_bucket=expect_bucket, read_operation=operation,
                               filing_associations=filing_associations, staged_observations=staged_observations,
                               retained_members=retained_members, check_resources=check_resources,
                               observation_checkpoint=observation_checkpoint)
        except ValueError as failure:
            receipt_path = work / RECEIPT
            attempted = receipt_path.is_file() and json.loads(receipt_path.read_bytes()).get("publish_attempt")
            failure_state = _uncertain_state("publish", receipt_path, expect_bucket) if attempted else NOTHING_PUBLISHED
            raise ConversionRefused(f"{family}: {failure}", state=failure_state) from failure
    if check_resources is None:
        from spicy_regs.runtime_bounds import checkpoint
        check_resources = checkpoint
    check_resources()
    if observation_checkpoint is not None and (family != 'fec-observations' or publish):
        raise ConversionRefused('Observation checkpoints belong only to local fec-observations preparation')
    if observation_checkpoint is not None and retained_members is None:
        raise ConversionRefused('Observation checkpoints require an explicit retained member map; no downloads')
    if family not in allowed:
        raise ConversionRefused(f"{family} is not in this run's allow-list ({', '.join(sorted(allowed)) or 'empty'})")
    from spicy_regs.generations import validation_identity

    work = work.resolve()
    state = state or source_state
    started = state(remote)
    validator = validation_identity()
    checkpoint_producer = None
    if observation_checkpoint is not None:
        checkpoint_producer = _checkpoint_producer(started, expected_main, expected_spicy_docs)
    else:
        _refuse_moved(started, main=expected_main, spicy_docs=expected_spicy_docs)
    base = (os.getenv("R2_PUBLIC_URL") or "").rstrip("/")
    if not base:
        raise ConversionRefused("R2_PUBLIC_URL is not set: the published family is read from it")
    # A run that cannot publish says so before it converts anything.
    target = _target(expect_bucket, "Publishing a converted family") if publish else None
    if work.exists() and any(work.iterdir()):
        raise ConversionRefused(f"{work} is not empty; every conversion keeps its own receipt and build")
    work.mkdir(parents=True, exist_ok=True)
    receipt: dict = {"family": family, "started_at": _now(), "source": started, "public_url": base, "target": target}
    if checkpoint_producer is not None:
        receipt["producer"] = checkpoint_producer
        receipt["checkpoint_admission"] = {"observed_remote_main": started["main"], "remote": started["remote"],
                                           "producer_commit": checkpoint_producer["commit"]}
    pointer = f"{base}/{publication.INDEX_V2_KEY}"
    before = r2.public_object_version(pointer)
    with publication.snapshot(base) as captured:
        if before is None or r2.public_object_version(pointer) != before:
            raise ConversionRefused("The publication index moved while it was being captured; run again")
        old = captured["families"].get(family)
        if old is None:
            raise ConversionRefused(f"{family} is not a published family")
        if "etlReceipts" in old:
            raise ConversionRefused(f"{family} already carries ETL receipts: it is native")
        (work / "captured-publication.v2.json").write_text(json.dumps(captured, sort_keys=True), encoding="utf-8")
        receipt["captured"] = {"etag": before["etag"], "bytes": before["bytes"], "entry": old}
        from spicy_regs.fec_native_conversion import FAMILIES, check_membership
        if family in FAMILIES:
            check_membership(family, old)
        # A family whose generations carry source evidence keeps carrying it: the converted generation names the
        # one it replaces and hands on what that one's journal held for the next run. That evidence is read before
        # any table, so evidence that is not current completed-build evidence refuses before gigabytes are
        # downloaded; evidence from before version 2 stays with its generation and hands nothing on.
        replaced, handed_on = _replaced_evidence(base, old)
        original_pins: dict = {}
        retained = {key: _retain_table(base, captured, key, work / "retained", original_pins, retained_members) for key in old["tables"]}
        if _original_states(retained) != original_pins:
            raise ConversionRefused("Retained originals changed during collection")
        from spicy_regs.court_receipts import POLICIES as court_policies
        from spicy_regs.pipelines.rollups.base import RollupPipeline

        cls = None
        from spicy_regs.fec_native_conversion import FAMILIES, check_membership
        fec = family in FAMILIES
        if staged_observations is not None:
            artifact, _, _ = read_operation.admit_generation(staged_observations)
            receipt["staged_observations"] = {"directory": str(staged_observations.resolve()), "pin": artifact.pin.as_dict()}
        read_snapshot, observation_rebinding = _conversion_snapshot(captured, receipt, read_operation, return_rebinding=True)
        regulatory = _regulatory_base_class(family)
        if regulatory is not None and (set(old["tables"]) != {regulatory.output}
                                       or "members" in old["tables"][regulatory.output]):
            raise ConversionRefused("Regulatory family must contain its one maintained unsplit output")
        if fec:
            from spicy_regs.fec_native_conversion import converted_tables

            check_membership(family, old)
            claimed = {key.removesuffix(".parquet") for key in converted_tables(family, old["tables"])}
        elif regulatory is not None:
            from spicy_regs.transforms.regulations_receipts import policy

            claimed = {policy(regulatory.output.removesuffix(".parquet")).dataset}
        elif all(key.removesuffix(".parquet") in court_policies for key in old["tables"]):
            if any(path.is_dir() for path in retained.values()):
                raise ConversionRefused(f"{family}: the court converter has no split-table writer")
            claimed = {key.removesuffix(".parquet") for key in old["tables"]}
        elif (cls := _rollup_class(family)) is not None:
            for key, table in old["tables"].items():
                if ("members" in table) != (key in cls.partitioned) or (
                    "members" in table and tuple(table["partitionColumns"]) != tuple(cls.partitioned[key])
                ):
                    raise ConversionRefused(f"{family}: {key} partition layout differs from its maintained writer")
            claimed = {policy.dataset for policy in cls.receipt_policies}
        else:
            raise ConversionRefused(f"{family} has no subject/receipt rollup or court writer to convert it through")
        # The index gives every dataset an entry lists one owning family; publication would refuse a second. A
        # shared log is listed by none.
        from spicy_regs.subject_catalog import shared_receipt_logs

        claimed -= shared_receipt_logs()
        for other, entry in captured["families"].items():
            owned = {key.removesuffix(".parquet") for key in entry["tables"]} | set(entry.get("etlReceipts", {}).get("datasets", ()))
            if other != family and (shared := sorted(claimed & owned)):
                raise ConversionRefused(f"{family}: its receipts would hold {shared}, which already belongs to family {other}")
        scheduled = _regulatory_base_class(family) or _rollup_class(family, RollupPipeline)
        retains = bool(replaced["inputs"]) or bool(scheduled and scheduled.retain_source_evidence)
        _write_receipt(work / RECEIPT, receipt)
        elapsed = time.monotonic()
        try:
            if fec:
                built = _convert_fec(family, old, captured, retained, base, work, handed_on, retains,
                    read_operation, original_pins, filing_associations=filing_associations, read_snapshot=read_snapshot,
                    observation_generation=staged_observations, check_resources=check_resources,
                    observation_rebinding=observation_rebinding, observation_checkpoint=observation_checkpoint,
                    observation_source_pin={name: value for name, value in started.items() if name != "main"},
                    observation_hosted_main=started["main"] if observation_checkpoint is not None else None)
            elif regulatory is not None:
                built = _convert_regulatory_base(regulatory, captured, retained, base, work, handed_on, retains, read_operation, original_pins)
            else:
                built = (_convert_court(family, old, captured, retained, base, work, handed_on, retains, read_operation, original_pins) if cls is None
                         else _convert_rollup(cls, old, retained, base, work, handed_on, read_operation, original_pins))
        except ValueError as refusal:  # how the writers refuse a row or a family they cannot classify
            raise ConversionRefused(f"{family}: {refusal}") from refusal
        receipt["convert_seconds"] = round(time.monotonic() - elapsed, 1)

    proof, problems, conversion_proof = _qualify_conversion(family, built, old, retained, replaced, retains, handed_on)
    receipt.update(proof)

    def save() -> dict:
        _write_receipt(work / RECEIPT, receipt)
        return receipt

    if problems:
        receipt["refused"] = problems
        save()
        raise ConversionRefused(f"{family}: " + "; ".join(problems))
    if not publish or target is None:
        receipt["published"] = None
        try:
            # Hosted main may move during a long preparation; the checkout and wheel this process loaded may not.
            if _local_state(state(remote)) != _local_state(started):
                raise ConversionRefused("The checkout or installed SpicyDocs moved during this preparation; "
                                        + _resume_unsealed(started))
            _seal_prepared(receipt, built, work, validator)
        except ConversionRefused:
            save()
            raise
        return save()

    return _publish_conversion(receipt, built, captured, work, expected_main=expected_main,
                               expected_spicy_docs=expected_spicy_docs, remote=remote, state=state,
                               conversion_proof=conversion_proof)


#: Fields of a sealed ``tables`` entry derived from the bytes here, not from the reader's restoration.
_RECOUNTED = ("retained_rows", "subject_rows")


def _qualify_conversion(family: str, built: _Built, old: Mapping, retained: Mapping[str, Path],
                        replaced: Mapping, retains: bool, handed_on: Sequence[Mapping],
                        sealed: Mapping | None = None,
                        ) -> tuple[dict, list[str], publication.NativeConversionProof | None]:
    """Qualify ``built`` against its retained originals; return the receipt's proof fields, problems, and the proof.

    The reader part (receipt outcomes, original restoration) comes from the admitted reader,
    or, with ``sealed``, from that prepared receipt, whose semantic admission ran under this process's admission key.
    Everything else (subject counts, membership, physical tables, lineage, handed-on journal) is recomputed here
    either way, so the caller's comparison with the seal still checks it.
    """
    if built.read_operation is None:
        raise ConversionRefused("Conversion qualification requires its active admitted reader")
    artifact = built.read_operation.verified_source(built.generation)[0].root
    tables, problems = {}, []
    from spicy_regs.fec_native_conversion import REBUILT_TABLES, RETIRED_TABLES, converted_tables
    from spicy_regs.transforms.fec_original_tables import expected_rows
    retired, rebuilt = RETIRED_TABLES.get(family, frozenset()), REBUILT_TABLES.get(family, {})
    if sealed is None:
        _, reader, _ = built.read_operation.admit_generation(built.generation)
        outcomes = reader.outcome_counts()

        def restore(dataset: str, original: Path) -> dict:
            if dataset != "fec_record_evidence" or not (retired or rebuilt):
                return built.compare_original(dataset, original)
            # Evidence targeting retired or rebuilt tables is not carried; compare what is.
            from tempfile import TemporaryDirectory

            from spicy_regs.fec_native_conversion import referenced_evidence
            with TemporaryDirectory(dir=built.generation.parent.parent, prefix="qualify-evidence-") as held:
                kept = Path(held) / f"{dataset}.parquet"
                dropped = referenced_evidence(original, kept, family)
                return built.compare_original(dataset, kept) | {"unreferenced_evidence_rows": dropped}
    else:
        outcomes = dict(sealed["receipt_outcomes"])

        def restore(dataset: str, _original: Path) -> dict:
            return {name: value for name, value in sealed["tables"][dataset].items() if name not in _RECOUNTED}
    if unconverted := {name: count for name, count in outcomes.items() if name not in CONVERTED_OUTCOMES and count}:
        problems.append(f"receipts hold {unconverted}; every retained row must convert")
    for key, table in old["tables"].items():
        dataset = key.removesuffix(".parquet")
        subject = built.generation / (dataset if "members" in table else key)
        if key in retired:
            # Owner-retired table (ss-j4vz B10, Q2): the captured prior holds it, the generation omits it.
            tables[dataset] = {"retired": True, "retained_rows": table["rows"]}
            continue
        if key in rebuilt:
            # Built from its retained originals (ss-j4vz B7-B9), not converted from the captured rows,
            # so the captured row count is no witness. The committed selection states how many rows its
            # admitted originals hold; any other count, zero included, refuses.
            dataset = rebuilt[key]
            subject = built.generation / f"{dataset}.parquet"
            rows = _table_rows(subject) if subject.exists() else 0
            # Originals the build did not admit stay countable here, by reason (receipt status, body boundaries).
            listed = built.generation.parent / "rebuilt" / dataset / f"{dataset}.refusals.json"
            refused: dict[str, int] = {}
            for _, reason in (json.loads(listed.read_text()) if listed.exists() else ()):
                refused[reason] = refused.get(reason, 0) + 1
            tables[dataset] = {"rebuilt_from_originals": True, "captured_table": key.removesuffix(".parquet"),
                               "retained_rows": table["rows"], "subject_rows": rows,
                               "expected_rows": expected_rows(dataset), "refused_originals": refused}
            if rows != expected_rows(dataset) or rows == 0:
                problems.append(f"{dataset} holds {rows} rows; its selected originals hold {expected_rows(dataset)}")
            continue
        try:
            check = restore(dataset, retained[key]) | {"retained_rows": table["rows"]}
            dropped = check.get("unreferenced_evidence_rows", 0)
        except ValueError as unreadable:
            problems.append(f"{dataset} does not restore: {unreadable}")
            continue
        if subject.exists():
            check["subject_rows"] = _table_rows(subject)
            if subject.is_dir():
                source_members, subject_members = _table_files(retained[key]), _table_files(subject)
                if source_members.keys() != subject_members.keys() or any(
                    _table_rows(subject_members[name]) != _table_rows(source_members[name]) for name in source_members
                    if name in subject_members
                ):
                    problems.append(f"{key}: native subject members differ from the retained partition layout or counts")
        elif key not in built.receipt_only_tables:
            problems.append(f"{key} is neither a subject table nor a declared receipt-only table")
        tables[dataset] = check
        if check.get("subject_rows", table["rows"]) + dropped != table["rows"] or _has_differences(check):
            problems.append(f"{dataset} does not restore to its retained table: {check}")
    expected = (converted_tables(family, old["tables"]) | built.added_tables) - built.receipt_only_tables
    if (subjects := set(artifact["spec"]["tables"])) != expected:
        problems.append(f"the generation holds {sorted(subjects)}; publication expects {sorted(expected)}")
    # Keep the original physical-file guard as well as the logical membership check.
    auxiliary = {"etl_receipts.parquet"}
    physical = {publication.member_table(path.relative_to(built.generation).as_posix())
                for path in built.generation.rglob("*.parquet") if path.relative_to(built.generation).as_posix() not in auxiliary}
    if physical != subjects:
        problems.append(f"the generation's physical tables {sorted(physical)} differ from its declared tables {sorted(subjects)}")
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE

    lineage = {"replaced_generation_inputs": replaced["inputs"], "inputs": artifact["inputs"], "handed_on_events": len(handed_on)}
    if retains:
        prior = {"role": PRIOR_ROLE, "logicalId": old["logicalId"], "artifactDigest": old["artifactDigest"]}
        if prior not in artifact["inputs"] or sum(item["role"] == INPUT_ROLE for item in artifact["inputs"]) != 1:
            problems.append(f"{family} retains source evidence and the converted generation names {artifact['inputs']}: "
                            "it must name the replaced generation and one evidence artifact")
        journaled = [{name: value for name, value in row.items() if name != "recorded_at"}
                     for directory in built.evidence
                     for row in map(json.loads, (directory / "journal.jsonl").read_text().splitlines())
                     if row.get("event") in INHERITED_EVENTS]
        if journaled != list(handed_on):
            problems.append(f"{len(handed_on)} journal events the next run reads back were not all handed on")
    from rulespec_artifacts import canonical_json_bytes

    conversion_proof = (None if problems else publication.NativeConversionProof(
        artifact["logicalId"], artifact["artifactDigest"], canonical_json_bytes(old), frozenset(old["tables"])))
    return {
        "policy_versions": dict(built.policies),
        "receipt_outcomes": dict(outcomes),
        "tables": tables,
        "lineage": lineage,
        "generation": {"directory": str(built.generation), "artifactDigest": artifact["artifactDigest"],
                       "receiptGenerationId": artifact["spec"]["etlReceipts"]["generationId"]},
    }, problems, conversion_proof


def _publish_conversion(receipt: dict, built: _Built, captured: Mapping, work: Path, *,
                        expected_main: str, expected_spicy_docs: str, remote: str, state: Callable[[str], dict],
                        exact_prior: bool = False, publisher_source: Mapping | None = None,
                        conversion_proof: publication.NativeConversionProof | None = None,
                        verification: Mapping | None = None) -> dict:
    exact_prior = exact_prior or conversion_proof is not None
    family, base, target = (receipt[name] for name in ("family", "public_url", "target"))
    started = publisher_source if publisher_source is not None else receipt["source"]
    old = receipt["captured"]["entry"]
    artifact = json.loads((built.generation / "artifact.json").read_bytes())

    def save() -> dict:
        _write_receipt(work / RECEIPT, receipt)
        return receipt

    try:
        _refuse_moved(now := state(remote), main=expected_main, spicy_docs=expected_spicy_docs)
        if now != started:
            raise ConversionRefused("Main or the pinned wheel moved while the family converted")
        client, bucket = r2.get_r2_client(), target["bucket"]
    except (ConversionRefused, publication.PublicationError) as failure:
        if exact_prior:
            raise ConversionRefused(str(failure), state=_prepared_refusal_state(work / RECEIPT)) from failure
        raise
    # The rollback file exists before the pointer can move: it names the captured entry and the generation intended.
    from spicy_regs.generations import implementation_id

    attempt: dict = {"at": _now(), "outcome": "attempted", "error": None,
                     "publisher": {"source": now, "implementationId": implementation_id()},
                     **({"verification": dict(verification)} if verification is not None else {})}
    receipt |= {"published": None, "publish_attempt": attempt}
    save()
    try:
        if built.read_operation is None:
            raise ConversionRefused("Conversion publication requires its active verified read operation")
        if built.check_originals:
            built.check_originals()
        admitted, source = built.read_operation.verified_source(built.generation)
        index = publication._publish_verified_generation(
            admitted, source, client=client, bucket=bucket, prior_index=captured, evidence_directories=built.evidence,
            added_tables=built.added_tables, receipt_only_tables=built.receipt_only_tables, exact_prior=exact_prior,
            conversion_proof=conversion_proof, local=built.generation,
            upload_member=lambda prefix, key: publication._put_immutable(
                client, bucket, prefix + "/" + key, built.generation / key))
        entry = index["families"][family]
    except Exception as failure:
        # The publish call did not return. A conditional write whose response was lost has still moved the pointer,
        # so the stored index, read with credentials, says which of three states holds.
        reason = f"{type(failure).__name__}: {failure}"
        try:
            entry = publication.stored_family(client, bucket, family)
        except Exception as unreadable:
            attempt |= {"error": reason, "verification_error": f"{type(unreadable).__name__}: {unreadable}"}
            save()
            raise ConversionRefused(
                f"{family}: {reason}; stored index could not be read ({attempt['verification_error']})",
                state=_uncertain_state("publish", work / RECEIPT, bucket)) from unreadable
        if exact_prior and entry != old and _same_generation(entry, old):
            attempt |= {"outcome": "captured entry changed", "error": reason, "entry": entry}
            save()
            raise ConversionRefused(
                f"{family}: {reason}", state=f"{family}'s captured entry changed; this conversion's generation "
                "was not published by this attempt. No automatic rollback was performed") from failure
        if _same_generation(entry, old):
            attempt |= {"outcome": "not moved", "error": reason}
            save()
            raise ConversionRefused(f"{family}: {reason}") from failure
        if (entry or {}).get("artifactDigest") != artifact["artifactDigest"]:
            attempt |= {"outcome": "moved elsewhere", "error": reason, "entry": entry}
            save()
            raise ConversionRefused(
                f"{family}: {reason}", state=f"{family} now names {(entry or {}).get('artifactDigest')}, which is "
                "neither the captured generation nor this conversion's: another writer published it, and this "
                "receipt rolls nothing back") from failure
        attempt |= {"outcome": "moved to this conversion's generation", "error": reason}
        receipt["published"] = {"entry": entry}
        save()
    else:
        attempt["outcome"] = "published"
        if (mirror := getattr(index, "mirror", None)) is not None:
            attempt["mirror"] = mirror  # reported, never a refusal: the pointer has already moved
    receipt["published"] = {"entry": entry}
    save()
    published = _published_state(family, entry, work / RECEIPT, bucket)
    try:
        from spicy_regs.sources.cloudflare import purge_urls

        purge_urls([f"{base}/{publication.INDEX_V2_KEY}"])
        read_index = publication.current_index(base)
        entry = read_index["families"].get(family) or {}
        read = built.read_published(read_index, client, bucket)
        # Past any cached snapshot: the public pointer must still name the entry whose bytes were just verified.
        if publication.load_index(base)["families"].get(family) != entry:
            raise ConversionRefused("Public family pointer moved during conversion read-back")
    except Exception as failure:
        raise ConversionRefused(f"{family} could not be read back ({type(failure).__name__}: {failure})",
                                state=published) from failure
    receipt["read_back"] = {
        "artifactDigest": entry.get("artifactDigest"),
        "etl_receipts_rows": entry.get("etlReceipts", {}).get("rows"),
        "table_rows": {key: table["rows"] for key, table in entry.get("tables", {}).items()},
        **read,
    }
    save()
    # Source rows were compared with the captured publication before admission, and the retained originals were
    # held to the captured entry's counts then (_retained_inputs); comparing them again here could only agree.
    # Added status tables have no old counterpart: read them back against the
    # exact admitted generation, rather than indexing an absent old table.
    expected_tables = artifact["spec"]["tables"]
    wrong = sorted(set(entry.get("tables", {})) ^ set(expected_tables))
    wrong += [key for key, table in entry.get("tables", {}).items()
              if key in expected_tables and table["rows"] != expected_tables[key]["rows"]]
    if entry.get("artifactDigest") != artifact["artifactDigest"] or "etlReceipts" not in entry or wrong:
        raise ConversionRefused(f"{family} does not read back as converted ({wrong or entry.get('artifactDigest')})",
                                state=published)
    return receipt


def rollback(receipt_path: Path, *, discard_newer: bool = False, expect_bucket: str | None = None) -> dict:
    """Point the family back at the entry the conversion captured, if it names what the conversion published.

    The receipt is enough from the moment a publish was attempted, whether or not the process saw the write land.
    The stored index decides: the captured generation (nothing to do), this conversion's (restore), or another
    writer's (refused unless ``discard_newer``).
    """
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if not receipt.get("publish_attempt") and not receipt.get("published"):
        raise ConversionRefused(f"{receipt_path} records no publish attempt: there is nothing to roll back")
    target = _target(expect_bucket, "Rolling back a converted family")
    if receipt.get("target") and receipt["target"] != target:
        raise ConversionRefused(f"{receipt_path} was written to {receipt['target']}, not {target}")
    family, captured = receipt["family"], receipt["captured"]["entry"]
    client = r2.get_r2_client()
    stored = publication.stored_family(client, target["bucket"], family)
    ours = (stored or {}).get("artifactDigest") == receipt["generation"]["artifactDigest"]
    if not ours and not _same_generation(stored, captured) and not discard_newer:
        raise ConversionRefused(
            f"{family} names {(stored or {}).get('artifactDigest')}, neither the captured generation nor this "
            "conversion's", state=f"{family} is unchanged by this rollback; --discard-newer would replace that generation")
    attempt = {"at": _now(), "outcome": "attempted", "found_entry": stored,
               "discarded_newer": discard_newer}
    receipt["rollback_attempt"] = attempt

    def save() -> None:
        _write_receipt(receipt_path, receipt)

    save()
    try:
        publication.restore_family(client, target["bucket"], family, captured,
                                   expected=None if discard_newer else stored)
    except Exception as failure:
        attempt["error"] = f"{type(failure).__name__}: {failure}"
        try:
            observed = publication.stored_family(client, target["bucket"], family)
        except Exception as unreadable:
            attempt["verification_error"] = f"{type(unreadable).__name__}: {unreadable}"
            save()
            raise ConversionRefused(
                f"{family}: rollback failed ({attempt['error']}); stored index could not be read "
                f"({attempt['verification_error']})",
                state=_uncertain_state("rollback", receipt_path, expect_bucket)) from unreadable
        if observed != captured:
            attempt.update(outcome="not restored", entry=observed)
            save()
            raise ConversionRefused(
                f"{family}: rollback failed ({attempt['error']})",
                state=f"{family} names {(observed or {}).get('artifactDigest')} in the stored index; "
                      f"retry {SCRIPT} --rollback {receipt_path} --expect-bucket {expect_bucket}") from failure
        attempt["outcome"] = "restored after lost response"
    else:
        attempt["outcome"] = "restored"
    save()
    from spicy_regs.sources.cloudflare import purge_urls

    base = receipt["public_url"]
    try:
        purge_urls([f"{base}/{publication.INDEX_V2_KEY}"])
        entry = publication.current_index(base)["families"].get(family)
    except Exception as failure:
        raise ConversionRefused(
            f"{family}: rollback read-back failed ({type(failure).__name__}: {failure})",
            state=f"{family}'s stored v2 entry was restored; public read-back is unverified. "
            f"Retry {SCRIPT} --rollback {receipt_path} --expect-bucket {expect_bucket}",
        ) from failure
    receipt["rolled_back"] = {"at": _now(), "entry": entry, "discarded_newer": discard_newer,
                              "found": "this conversion's generation" if ours else
                                       "the captured generation" if _same_generation(stored, captured) else "another generation"}
    save()
    if entry != captured:
        raise ConversionRefused(f"{family} does not read back as its captured entry after the rollback",
                                state=f"{family} names {(entry or {}).get('artifactDigest')} when read publicly")
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("family", nargs="?", help="The one published family to convert")
    parser.add_argument("--allow", action="append", default=[], metavar="FAMILY[,FAMILY...]",
                        help="This run's approved families; a family outside the list is refused")
    parser.add_argument("--work", type=Path, help="A new directory for this family's build and receipt")
    parser.add_argument("--filing-associations", type=Path, help="Explicit admitted FEC filing/definition selection JSON")
    parser.add_argument("--staged-observations", type=Path, help="Complete prepared observations generation for FEC query")
    parser.add_argument("--retained-member-map", type=Path, help="Exact captured object path to retained local file map; missing members refuse downloads")
    parser.add_argument("--observation-checkpoint", type=Path,
                        help="Reuse closed consecutive observation row groups; keep each invocation's --work new")
    parser.add_argument("--bulk-threads", type=int, help="DuckDB threads for this initial conversion")
    parser.add_argument("--bulk-memory-limit", help="DuckDB memory limit for this initial conversion")
    parser.add_argument("--require-bulk", action="store_true", help="Refuse SQL admission failure instead of building a reference index")
    parser.add_argument("--expect-main", help="The main commit this run was started for; with --observation-checkpoint, the exact frozen producer commit")
    parser.add_argument("--expect-spicy-docs", help="The SpicyDocs version main pinned when this run was started")
    parser.add_argument("--remote", default="origin", help="The git remote whose main is checked (default: origin)")
    parser.add_argument("--publish", action="store_true", help="Move the family's pointer; without it nothing is uploaded")
    parser.add_argument("--publish-prepared", type=Path, metavar="CONVERSION_JSON",
                        help="Recheck and publish one completed official dry run without rerunning its writer")
    parser.add_argument("--resume-generation", type=Path, metavar="GENERATION",
                        help="Verify and seal a retained complete generation without restarting its writer")
    parser.add_argument("--expect-producer", help="Full retained producer commit for preparation or publication")
    parser.add_argument("--expect-receipt-digest", metavar="SHA256",
                        help="With --publish-prepared: the 'prepared receipt digest' line the preparation printed "
                             "(its log or job output), never a value read from the receipt itself")
    parser.add_argument("--full-verification", action="store_true",
                        help="With --publish-prepared: repeat row admission and restoration even under the seal's "
                             "admission key")
    parser.add_argument("--expect-bucket", help="The bucket --publish or --rollback may write to; any other is refused")
    parser.add_argument("--rollback", type=Path, metavar="RECEIPT", help="Restore the entry a conversion receipt captured")
    parser.add_argument("--discard-newer", action="store_true",
                        help="With --rollback: also discard a generation published after the conversion")
    parser.add_argument("--env-file", type=Path, help="Read R2_* settings from this file; nothing is loaded implicitly")
    args = parser.parse_args(argv)
    if args.observation_checkpoint and (args.family != 'fec-observations' or args.publish or args.rollback
                                        or args.resume_generation or args.publish_prepared):
        parser.error('--observation-checkpoint belongs only to local fec-observations preparation')
    if args.observation_checkpoint and not args.retained_member_map:
        parser.error('--observation-checkpoint requires --retained-member-map; no downloads')
    if args.rollback and (args.family or args.allow or args.work or args.expect_main or args.expect_spicy_docs or args.publish_prepared
                          or args.resume_generation or args.expect_producer or args.publish or args.remote != "origin"):
        parser.error("--rollback takes only --expect-bucket, --env-file and --discard-newer")
    if args.expect_producer and not (args.publish_prepared or args.resume_generation):
        parser.error("--expect-producer belongs to --publish-prepared or --resume-generation")
    if args.resume_generation and (args.family or args.publish or args.publish_prepared):
        parser.error("--resume-generation cannot rebuild or publish a family")
    if args.resume_generation and not (args.work and args.allow and args.expect_main
                                       and args.expect_spicy_docs and args.expect_producer):
        parser.error("--resume-generation needs --work, --allow, --expect-main, --expect-spicy-docs and --expect-producer")
    if args.publish_prepared and (args.family or args.work or args.publish):
        parser.error("--publish-prepared cannot rebuild a family; omit FAMILY, --work and --publish")
    if bool(args.publish_prepared) != bool(args.expect_receipt_digest) or (
            args.full_verification and not args.publish_prepared):
        parser.error("--publish-prepared needs --expect-receipt-digest, and both it and --full-verification "
                     "belong only to --publish-prepared")
    if args.expect_receipt_digest and not RECEIPT_DIGEST.fullmatch(args.expect_receipt_digest):
        parser.error("--expect-receipt-digest must be sha256: followed by 64 lowercase hex digits")
    if (args.publish or args.publish_prepared or args.rollback) and not args.expect_bucket:
        parser.error("publication and rollback need --expect-bucket: the bucket this run may write to")
    if args.discard_newer and not args.rollback:
        parser.error("--discard-newer belongs to --rollback")
    if (args.filing_associations or args.staged_observations or args.retained_member_map) and (
            args.rollback or args.resume_generation or args.publish_prepared):
        parser.error("FEC selections and retained member map belong only to an initial conversion")
    if (args.filing_associations or args.staged_observations) and args.family != "fec-query":
        parser.error("Filing associations and staged observations belong only to fec-query")
    if args.bulk_threads is not None and args.bulk_threads < 1:
        parser.error("--bulk-threads must be positive")
    if (args.require_bulk or args.bulk_threads is not None or args.bulk_memory_limit is not None) and (
            args.rollback or args.resume_generation or args.publish_prepared):
        parser.error("Bulk limits belong only to an initial conversion")
    entered_prepared = False
    try:
        if args.env_file:
            from dotenv import dotenv_values

            settings = {name: value for name, value in dotenv_values(args.env_file).items() if value is not None}
            if not settings:
                parser.error(f"{args.env_file} holds no settings")
            if differing := sorted(name for name, value in settings.items() if os.environ.get(name, value) != value):
                raise ConversionRefused(f"{', '.join(differing)} in the environment differ from {args.env_file}; "
                                        "unset them or correct the file, so the write target is the one written down")
            os.environ.update(settings)
        if args.rollback:
            done = rollback(args.rollback, discard_newer=args.discard_newer, expect_bucket=args.expect_bucket)
            print(f"{done['family']}: found {done['rolled_back']['found']}; the pointer names "
                  f"{done['rolled_back']['entry']['artifactDigest']}")
            return 0
        if args.resume_generation:
            done = resume_prepared(
                args.resume_generation, work=args.work,
                allowed=[name for value in args.allow for name in value.split(",") if name],
                expected_main=args.expect_main, expected_spicy_docs=args.expect_spicy_docs,
                expected_producer=args.expect_producer, remote=args.remote)
        elif args.publish_prepared:
            if not (args.allow and args.expect_main and args.expect_spicy_docs):
                parser.error("--publish-prepared needs --allow, --expect-main and --expect-spicy-docs")
            entered_prepared = True
            done = publish_prepared(
                args.publish_prepared, allowed=[name for value in args.allow for name in value.split(",") if name],
                expected_main=args.expect_main, expected_spicy_docs=args.expect_spicy_docs,
                remote=args.remote, expect_bucket=args.expect_bucket, expected_producer=args.expect_producer,
                expected_receipt_digest=args.expect_receipt_digest, full_verification=args.full_verification)
        elif not (args.family and args.work and args.expect_main and args.expect_spicy_docs):
            parser.error("a conversion needs FAMILY, --allow, --work, --expect-main and --expect-spicy-docs")
        else:
            from spicy_regs.fec_native_conversion import load_filing_associations
            from contextlib import nullcontext
            from spicy_regs.etl_bulk import bulk_limits

            limits = (bulk_limits(threads=args.bulk_threads or 4, memory_limit=args.bulk_memory_limit if args.bulk_memory_limit is not None else "4GB",
                                 require_bulk=args.require_bulk)
                      if args.require_bulk or args.bulk_threads is not None or args.bulk_memory_limit is not None
                      else nullcontext())
            with limits:
                done = convert(
                    args.family, allowed=[name for value in args.allow for name in value.split(",") if name], work=args.work,
                    expected_main=args.expect_main, expected_spicy_docs=args.expect_spicy_docs, publish=args.publish,
                    remote=args.remote, expect_bucket=args.expect_bucket,
                    filing_associations=load_filing_associations(args.filing_associations) if args.filing_associations else None,
                    staged_observations=args.staged_observations,
                    retained_members=json.loads(args.retained_member_map.read_text()) if args.retained_member_map else None,
                    observation_checkpoint=args.observation_checkpoint)
    except (ConversionRefused, publication.PublicationError) as refusal:
        print(f"REFUSED: {refusal}", file=sys.stderr)
        result_state = (_prepared_refusal_state(args.publish_prepared) if args.publish_prepared and not entered_prepared
                        else getattr(refusal, "state", NOTHING_PUBLISHED))
        print(f"STATE: {result_state}", file=sys.stderr)
        return 1
    except (Exception, KeyboardInterrupt) as failure:
        # Anything else is not a refusal the command chose, so it does not claim to know the bucket's state.
        operation_receipt = args.rollback or args.publish_prepared or (args.work / RECEIPT if args.work else None)
        attempted = operation_receipt and operation_receipt.exists() and json.loads(
            operation_receipt.read_text(encoding="utf-8")).get("rollback_attempt" if args.rollback else "publish_attempt")
        print(f"FAILED: {type(failure).__name__}: {failure}", file=sys.stderr)
        print("STATE: " + (_uncertain_state("rollback" if args.rollback else "publish", operation_receipt, args.expect_bucket)
                           if attempted else NOTHING_PUBLISHED), file=sys.stderr)
        return 130 if isinstance(failure, KeyboardInterrupt) else 2
    for name, table in done["tables"].items():
        print(f"{name}: {table['retained_rows']:,} retained rows restore exactly")
    print(f"{done['family']}: receipts {done['receipt_outcomes']}, generation {done['generation']['artifactDigest']}")
    print(f"published and read back; receipt {args.publish_prepared or args.work / RECEIPT}" if done["published"]
          else f"not published (no --publish); receipt {args.work / RECEIPT}")
    if not done["published"] and "prepared" in done:
        # The operator's publication channel: --expect-receipt-digest is copied from here, not from the receipt.
        print(f"prepared receipt digest {done['prepared']['receiptDigest']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
