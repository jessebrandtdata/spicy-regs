"""Complete rollup artifacts, verified with Rulespec before publication.

The platform library owns manifests, byte identity and membership. This module
only binds a declared set of Parquet outputs to their observed schema/counts.
It does not qualify source coverage or interpreted fields.
"""

from __future__ import annotations

import functools
import hashlib
import json
import platform
import re
import shutil
import stat
import sys
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from importlib.metadata import version
from pathlib import Path
from time import monotonic

import duckdb
import pyarrow.parquet as pq
from loguru import logger

KIND = "spicy-regs-rollup-generation"
MANIFEST = "members.json"
_PART_NAME = re.compile(r"part-\d{6}\.parquet\Z")


# Only the producing owner path uses these completed checks. Independent readers
# always call verify_generation/verify_generation_source and perform full admission.
_OWNER_ADMISSIONS: OrderedDict[str, None] = OrderedDict()


def validation_identity() -> dict:
    """Exact validator/policy sources and runtime; restart owner processes after code changes.

    A prepared conversion seals it, so a publisher with the same identity can trust the seal's semantic admission
    instead of repeating it: SpicyDocs is held by its code bytes, not only its release string, and the platform
    by system and machine (not the OS point release, which would refuse a minor update for no reason).
    """
    import rulespec_artifacts

    return {
        "spicy_regs": _package_tree(Path(__file__).parent),
        "rulespec_artifacts": _package_tree(Path(rulespec_artifacts.__file__).parent),
        "spicy_docs_code": spicy_docs_code(),
        "runtime": sys.version,
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "packages": {name: version(name) for name in ("spicy-regs", "spicy-docs", "rulespec-artifacts", "duckdb", "pyarrow")},
    }


def admission_key(pin: Mapping, validator: Mapping) -> str:
    """The one key under which a completed semantic admission of ``pin`` may stand in for another.

    In memory for this owner process (:func:`admit_generation_for_publication`) and in a prepared conversion's seal.
    """
    return hashlib.sha256(json.dumps([dict(pin), dict(validator)], sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def admit_generation_for_publication(directory: Path, *, expected_pin=None):
    """Renew bytes and reuse only this owner's completed local semantic check.

    Misses check exact bytes both before and after semantic admission. Hits
    perform fresh artifact admission once; producer metadata never creates a hit.
    Independent verify_generation/verify_generation_source always remain deep.
    """
    from rulespec_artifacts import LocalMemberSource, admit_artifact
    from spicy_regs.etl_policy_registry import require_registered_receipts

    validator = validation_identity()
    source = LocalMemberSource(directory)
    artifact = admit_artifact(source, expected_pin=expected_pin)
    identity = admission_key(artifact.pin.as_dict(), validator)
    require_registered_receipts(artifact.root["spec"]["tables"], artifact.root["spec"].get("etlReceipts"))
    if identity not in _OWNER_ADMISSIONS:
        from spicy_regs.etl_receipts import validate_receipt_bundle

        try:
            _verify_admitted_generation(artifact, source, lambda key: _table_info(directory / key),
                                        validate_receipt_bundle)
        except BaseException:
            close = getattr(artifact.local_member_states, "close", None)
            if close is not None:
                close()
            raise
        admit_artifact(source, expected_pin=artifact.pin)
    if validation_identity() != validator:
        raise ValueError("Generation validator changed during admission")
    _OWNER_ADMISSIONS[identity] = None
    _OWNER_ADMISSIONS.move_to_end(identity)
    while len(_OWNER_ADMISSIONS) > 8:
        _OWNER_ADMISSIONS.popitem(last=False)
    return artifact


def _table_info(path: Path) -> dict:
    """Observed columns and row count for one Parquet file; decoding all pages catches a body/footer mismatch.

    Hive partitioning is off: a split member's ``col=value`` directory must not add a column its file lacks.
    """
    info = _table_footer_info(path)
    # A readable footer does not establish readable data pages. Decode every
    # column in bounded batches before qualifying these bytes for publication.
    with pq.ParquetFile(path) as parquet:
        rows = sum(batch.num_rows for batch in parquet.iter_batches(batch_size=2000))
        if rows != parquet.metadata.num_rows:
            raise ValueError(f"Parquet body/footer row mismatch: {path.name}")
    return info


def _table_footer_info(path: Path) -> dict:
    """Describe a footer; complete body decoding must be owned by the caller's admission."""
    with duckdb.connect() as con:
        columns = con.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)",
                              [str(path)]).fetchall()
    return {"columns": [[row[0], row[1]] for row in columns], "rows": pq.read_metadata(path).num_rows}


def _check_partition(path: Path, partition: Mapping[str, str]) -> None:
    """Every row of a split member holds the partition values its key names, compared as text."""
    selected = ", ".join('CAST("' + column.replace('"', '""') + '" AS VARCHAR)' for column in partition)
    with duckdb.connect() as con:
        found = con.execute(f"SELECT DISTINCT {selected} FROM read_parquet(?, hive_partitioning = false)",
                            [str(path)]).fetchall()
    if any(row != tuple(partition.values()) for row in found):
        raise ValueError(f"Split member rows differ from the partition its key names: {path.name}")


def _package_tree(root: Path) -> str:
    """SHA-256 over every ``.py`` and package data file (:func:`spicy_regs.code_closure.is_data_file`) under ``root``."""
    from spicy_regs.code_closure import is_data_file

    digest = hashlib.sha256()
    for path in sorted(
        p for p in root.rglob("*")
        if (p.is_file() and p.suffix == ".py" and "__pycache__" not in p.parts) or is_data_file(p)
    ):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def source_digest(root: Path, suffixes: tuple[str, ...] = (".py",)) -> str:
    """SHA-256 over every file under ``root`` with one of ``suffixes``: its relative path, then its bytes."""
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.suffix in suffixes and "__pycache__" not in p.parts):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


@functools.cache
def spicy_docs_code() -> str:
    """Digest of the installed SpicyDocs' code and data (.py, .json, .xsd): what reads, derives and shapes a source.

    A re-read keyed on it, not on the release string, skips a version-only release (0.39.2
    changed one README line against 0.39.1). The whole package is digested, not the modules a
    caller imports, because a missed transitive module would leave stale rows silently; a
    release touching only another source still re-reads, which costs requests, not correctness.
    """
    import spicy_docs

    return source_digest(Path(spicy_docs.__file__).parent, (".py", ".json", ".xsd"))


def implementation_id() -> str:
    """Content digest of this package's Python sources, recorded in every generation root."""
    return "urn:spicy-regs:implementation:sha256:" + source_digest(Path(__file__).parent)


def verify_generation(directory: Path, *, expected_pin=None):
    """Hash every member and reconcile actual Parquet counts/shape; fail closed."""
    from rulespec_artifacts import LocalMemberSource

    return verify_generation_source(
        LocalMemberSource(directory), lambda key: _table_info(directory / key), expected_pin=expected_pin
    )


def verify_generation_source(source, table_info: Callable[[str], dict], *, expected_pin=None):
    """Apply the same artifact and table checks to local or pinned remote bytes."""
    from spicy_regs.etl_receipts import validate_receipt_bundle

    return _verify_generation_source(source, table_info, expected_pin=expected_pin,
                                     admit_receipts=validate_receipt_bundle)


def _verify_generation_source(source, table_info, *, expected_pin, admit_receipts, semantic=True):
    """Same product checks with admission retained only by an explicit conversion owner.

    ``semantic=False`` keeps every byte and structural check but leaves row content to ``admit_receipts`` and skips
    the receipt key index's row check: for a caller holding an earlier semantic admission under the same
    :func:`admission_key`.
    """
    from rulespec_artifacts import admit_artifact

    artifact = admit_artifact(source, expected_pin=expected_pin)
    try:
        return _verify_admitted_generation(artifact, source, table_info, admit_receipts, semantic=semantic)
    except BaseException:
        if artifact.local_member_states is not None:
            close = getattr(artifact.local_member_states, 'close', None)
            if close is not None:
                close()
        raise


def _verify_admitted_generation(artifact, source, table_info, admit_receipts, *, semantic=True):
    from rulespec_artifacts import iter_member_descriptors

    root = artifact.root
    if root["kind"] != KIND:
        raise ValueError("Not a SpicyRegs rollup generation")
    spec = root["spec"]
    if set(spec) - {"parents", "etlReceipts"} != {"family", "tables", "packages", "readSnapshot", "carriedForward", "publicationStatus"}:
        raise ValueError("Invalid rollup generation specification")
    if spec["publicationStatus"] not in {"complete-family", "local-partial"}:
        raise ValueError("Invalid generation publication status")
    tables = spec["tables"]
    from spicy_regs.sources.publication import member_table, parse_index, table_descriptor, table_entries
    from rulespec_artifacts import canonical_json_bytes

    snapshot = spec["readSnapshot"]
    if snapshot:
        parse_index(canonical_json_bytes(snapshot))
    _check_parents(spec.get("parents", {}), snapshot)
    carried = spec["carriedForward"]
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE

    inputs = root["inputs"]
    evidence = [item for item in inputs if item["role"] == INPUT_ROLE]
    prior_inputs = [item for item in inputs if item["role"] == PRIOR_ROLE]
    if inputs:
        prior = snapshot.get("families", {}).get(spec["family"])
        expected_prior = ([{"role": PRIOR_ROLE, "logicalId": prior["logicalId"],
                            "artifactDigest": prior["artifactDigest"]}] if prior else [])
        if len(evidence) != 1 or prior_inputs != expected_prior or len(inputs) != 1 + len(prior_inputs):
            raise ValueError("Generation evidence lineage differs from its captured prior")
    if not isinstance(carried, dict) or not set(carried) <= set(tables):
        raise ValueError("Invalid carried-forward declarations")
    if any("partitionColumns" in tables[key] for key in carried):
        raise ValueError("A split table is carried forward member by member at publication, not declared whole")
    members = list(iter_member_descriptors(artifact, source))
    receipt_spec = spec.get("etlReceipts")
    receipt_members = [member for member in members if member.object_key == "etl_receipts.parquet"]
    if bool(receipt_spec) != bool(receipt_members) or len(receipt_members) > 1:
        raise ValueError("Generation receipt membership differs from its declaration")
    table_members_only = [member for member in members if member.object_key != "etl_receipts.parquet"]
    if tables or table_members_only:
        table_entries(tables, table_members_only)
    elif not receipt_spec:
        raise ValueError("Generation has neither subjects nor receipts")
    if receipt_spec:
        if receipt_members[0].record_count != receipt_spec['rows']:
            raise ValueError('Generation receipt member count differs from receipt declaration')
        _verify_receipts(source, tables, table_members_only, receipt_spec, table_info, admit_receipts)
    for member in table_members_only:
        key = member.object_key
        if key is None:
            raise ValueError("Generation members must be plain Parquet filenames")
        table = tables[member_table(key)]
        if "partitionColumns" in table:
            actual = table_info(key)
            if actual["columns"] != table["columns"] or member.record_count != actual["rows"]:
                raise ValueError(f"Generation table shape/count mismatch: {key}")
            continue
        if Path(key).name != key or not key.endswith(".parquet"):
            raise ValueError("Generation members must be plain Parquet filenames")
        actual = table_info(key)
        if actual != tables[key] or member.record_count != actual["rows"]:
            raise ValueError(f"Generation table shape/count mismatch: {key}")
        if key in carried:
            prior = snapshot.get("families", {}).get(spec["family"])
            descriptor = table_descriptor(snapshot, key) if prior else None
            if (
                prior is None
                or carried[key] != prior["artifactDigest"]
                or descriptor is None
                or descriptor["sha256"] != member.sha256
                or descriptor["byteSize"] != member.byte_size
            ):
                raise ValueError(f"Carried-forward bytes differ from their captured generation: {key}")
    return artifact


_PARENT_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


def _check_parents(parents, snapshot: Mapping) -> None:
    """Each parent names its bytes or storage version; a managed one agrees with the captured index."""
    from spicy_regs.sources.publication import table_owner, table_pin

    if not isinstance(parents, dict):
        raise ValueError("Invalid generation parents")
    for key, parent in parents.items():
        fields = set(parent) - {"family", "artifactDigest"}
        size = parent.get("byteSize")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError(f"Invalid parent size: {key}")
        if fields == {"sha256", "byteSize"}:
            if not _PARENT_DIGEST.fullmatch(str(parent["sha256"])):
                raise ValueError(f"Invalid parent digest: {key}")
        elif fields == {"tableDescriptorDigest", "byteSize"}:
            if ("family" not in parent
                    or not _PARENT_DIGEST.fullmatch(str(parent["tableDescriptorDigest"]))):
                raise ValueError(f"Invalid split-table parent digest: {key}")
        elif fields != {"etag", "byteSize"} or not isinstance(parent["etag"], str) or not parent["etag"]:
            raise ValueError(f"Invalid parent version: {key}")
        if ("family" in parent) != ("artifactDigest" in parent):
            raise ValueError(f"Parent family and generation must be stated together: {key}")
        if "family" in parent:
            owner = table_owner(snapshot or {"families": {}}, key)
            if (owner is None or owner[0] != parent["family"]
                    or owner[1]["artifactDigest"] != parent["artifactDigest"]
                    or owner[1]["tables"][key]["byteSize"] != size
                    or ("sha256" in parent or "tableDescriptorDigest" in parent)
                    and table_pin(snapshot, key) != parent):
                raise ValueError(f"Parent differs from its captured family: {key}")


def output_keys(files: Sequence[Path], partitioned: Mapping[str, Sequence[str]]) -> list[str]:
    """Each output's table key: a split table's directory ``<name>`` is ``<name>.parquet``, any other file its name."""
    return [f"{path.name}.parquet" if f"{path.name}.parquet" in partitioned else path.name for path in files]


def build_generation(
    directory: Path,
    *,
    family: str,
    files: Sequence[Path],
    expected_keys: Sequence[str],
    schemas: Mapping[str, list[tuple[str, str]]] | None = None,
    read_snapshot: Mapping | None = None,
    carried_forward: Mapping[str, str] | None = None,
    publication_status: str = "complete-family",
    inputs=(),
    parents: Mapping[str, Mapping] | None = None,
    partitioned: Mapping[str, Sequence[str]] | None = None,
    receipt_path: Path | None = None,
    receipt_policies: Sequence | None = None,
    receipt_generation_id: str | None = None,
    adopt_owned_receipt: bool = False,
    owned_output_root: Path | None = None,
    read_operation=None,
    canonical_root: Path | None = None,
):
    """Snapshot exactly one declared family into a new immutable artifact.

    ``parents`` records each table of another family the build read: the digest
    and size of its bytes, or the storage version of an input read in place,
    plus the family and generation when a managed family published it. The
    family's own prior is its prior-generation input, never a parent; a parent
    of the generation's own family is refused here, while the verifier still
    admits the roots published that way before this rule.

    ``partitioned`` maps a table stored as several files to its partition
    columns. Its entry in ``files`` is a directory named for the table holding
    ``<col>=<value>/.../part-NNNNNN.parquet`` in column order; every row of a
    member must hold its key's values, and every member the same columns.

    A successful empty table is a Parquet file with zero rows. An omitted
    output is a failure, never an empty success. No root is written until the
    complete set and all declared schemas have been inspected.

    ``adopt_owned_receipt`` moves a caller-owned temporary receipt into this
    generation instead of copying it. It requires a regular file on the same
    filesystem. After the move, this directory retains the receipt even if
    admission fails; callers must preserve the directory for recovery. Retained
    source receipts use the default copy behavior.

    ``owned_output_root`` opts table outputs into the same move behavior. All
    inputs must be single-link regular files beneath that explicit build root,
    without symlink ancestors, on the same filesystem as the destination. A
    failed build retains moved files in this directory and remaining files at
    their prior owned paths; no retained source may be passed as owned output.

    """
    from rulespec_artifacts import (
        LocalMemberSource,
        describe_member,
    )

    if any(parent.get("family") == family for parent in (parents or {}).values()):
        raise ValueError("A generation's own prior is its prior-generation input, not a parent")
    expected, partitioned = set(expected_keys), dict(partitioned or {})
    names = output_keys(files, partitioned)
    if adopt_owned_receipt and receipt_path is None:
        raise ValueError("Owned receipt adoption requires a receipt file")
    if (not expected and receipt_path is None) or len(expected) != len(expected_keys) or len(set(names)) != len(names) or set(names) != expected:
        raise ValueError("Build outputs differ from the declared complete family")
    if any(Path(key).name != key or not key.endswith(".parquet") for key in expected):
        raise ValueError("Output keys must be plain Parquet filenames")
    if owned_output_root is not None:
        _validate_owned_outputs(owned_output_root, directory, files,
            receipt_path if adopt_owned_receipt else None, partitioned)
    preparation_started = monotonic()
    logger.info("Generation {}: preparing members (copy or adopt, then hash)", family)
    directory.mkdir(parents=True, exist_ok=False)
    # Both final admissions (the read operation's and admit_generation_for_publication) decode every page.
    table_info = _table_footer_info
    tables, rows = {}, {}
    for key, path in sorted(zip(names, files)):
        if key in partitioned:
            columns = list(partitioned[key])
            tables[key] = {**_copy_split(path, directory, columns, rows, table_info=table_info, adopt=owned_output_root is not None), "partitionColumns": columns}
        else:
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"Output is not a regular file: {path.name}")
            if owned_output_root is not None:
                path.rename(directory / key)
            else:
                shutil.copyfile(path, directory / key)
            tables[key] = table_info(directory / key)
            rows[key] = tables[key]["rows"]
        declared = (schemas or {}).get(Path(key).stem)
        if declared is not None and tables[key]["columns"] != [list(column) for column in declared]:
            raise ValueError(f"Output differs from the declared schema: {key}")
    receipt_spec = None
    if receipt_path is not None:
        from spicy_regs.etl_receipts import RECEIPT_KEY
        if not receipt_policies or not receipt_generation_id or RECEIPT_KEY in tables:
            raise ValueError("Receipt admission needs policies and a generation identity")
        if adopt_owned_receipt:
            source_stat = receipt_path.lstat()
            if not stat.S_ISREG(source_stat.st_mode):
                raise ValueError("Owned receipt adoption requires a regular file without a symlink")
            if source_stat.st_dev != directory.stat().st_dev:
                raise ValueError("Owned receipt adoption requires the same filesystem")
            receipt_path.rename(directory / RECEIPT_KEY)
        else:
            shutil.copyfile(receipt_path, directory / RECEIPT_KEY)
        receipt_spec = {"key": RECEIPT_KEY, "generationId": receipt_generation_id,
                        "policies": [policy.descriptor() for policy in receipt_policies],
                        **table_info(directory / RECEIPT_KEY)}
        rows[RECEIPT_KEY] = receipt_spec["rows"]
    elif receipt_policies is not None or receipt_generation_id is not None:
        raise ValueError("Receipt admission cannot omit the shared receipt member")
    from spicy_regs.etl_policy_registry import require_registered_receipts
    require_registered_receipts(tables, receipt_spec)
    source = LocalMemberSource(directory)
    members = [
        describe_member(
            source,
            object_key=key,
            role="table",
            media_type="application/vnd.apache.parquet",
            record_count=count,
        )
        for key, count in sorted(rows.items())
    ]
    _write_generation_metadata(
        directory, family=family, tables=tables, members=members, read_snapshot=read_snapshot,
        carried_forward=carried_forward, publication_status=publication_status, inputs=inputs,
        parents=parents, etl_receipts=receipt_spec,
    )
    logger.info("Generation {}: preparation completed in {:.1f}s", family, monotonic() - preparation_started)
    admission_started = monotonic()
    logger.info("Generation {}: admission started", family)
    if read_operation is not None:
        from rulespec_artifacts import ArtifactPin, parse_admitted_json, publish_directory_no_replace
        if canonical_root is not None:
            root = parse_admitted_json((directory / "artifact.json").read_bytes())
            pin = ArtifactPin(root["logicalId"], root["artifactDigest"])
            destination = canonical_root / pin.artifact_digest.removeprefix("sha256:")
            if not destination.exists():
                publish_directory_no_replace(directory, destination)
            directory = destination
        else:
            pin = None
        artifact = read_operation.admit_generation(directory, expected_pin=pin)[0]
    else:
        if canonical_root is not None:
            raise ValueError("Canonical adoption requires an owned conversion read operation")
        artifact = admit_generation_for_publication(directory)
    logger.info("Generation {}: admission completed in {:.1f}s", family, monotonic() - admission_started)
    return artifact


def _validate_owned_outputs(root, destination, files, receipt, partitioned):
    """Adoption is restricted to unaliased, freshly owned local output files."""
    root, destination = Path(root).absolute(), Path(destination).absolute()
    if any('..' in path.parts for path in (root, destination, *map(Path, files), *([] if receipt is None else [Path(receipt)]))):
        raise ValueError('Owned output adoption refuses parent traversal')
    def no_symlinks(path):
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('Owned output adoption refuses symlink paths or ancestors')
    no_symlinks(root)
    no_symlinks(destination)
    if not root.is_dir() or not destination.is_relative_to(root) or destination == root:
        raise ValueError('Owned output adoption requires a separate destination inside its build root')
    existing_destination = next(p for p in (destination, *destination.parents) if p.exists())
    if existing_destination.stat().st_dev != root.stat().st_dev:
        raise ValueError('Owned output destination must be on the same filesystem')
    seen = set()
    for path in [*files, *([] if receipt is None else [receipt])]:
        path = Path(path).absolute()
        no_symlinks(path)
        if not path.is_relative_to(root) or path == root or destination.is_relative_to(path) or path.is_relative_to(destination):
            raise ValueError('Owned output is outside build root or aliases its destination')
        members = list(path.rglob('*')) if path.is_dir() else [path]
        if path.is_dir() and path.name + '.parquet' not in partitioned:
            raise ValueError('Owned output directory is not a declared split table')
        for member in members:
            no_symlinks(member)
            info = member.lstat()
            if stat.S_ISDIR(info.st_mode):
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_dev != root.stat().st_dev:
                raise ValueError('Owned output adoption requires single-link regular files on the same filesystem')
            identity = (info.st_dev, info.st_ino)
            if identity in seen:
                raise ValueError('Owned output sources alias one another')
            seen.add(identity)
            if path.is_dir():
                labels = member.relative_to(path).parts[:-1]
                if not _PART_NAME.fullmatch(member.name) or [label.partition('=')[0] for label in labels] != list(partitioned[path.name + '.parquet']) or not all('=' in label for label in labels):
                    raise ValueError('Owned split output contains an unexpected member')


def _copy_split(path: Path, directory: Path, columns: list[str], rows: dict[str, int], *, table_info=_table_info, adopt=False) -> dict:
    """Copy one split table's members into ``directory`` under ``<table>/``; return its columns and summed rows.

    Each member's row count is recorded in ``rows`` by its object key.
    """
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"A split output is not a directory: {path.name}")
    found = sorted(member for member in path.rglob("*") if member.is_symlink() or not member.is_dir())
    if not found:
        raise ValueError(f"A split output has no members: {path.name}")
    info, total = None, 0
    for member in found:
        relative = member.relative_to(path).as_posix()
        partition = relative.split("/")[:-1]
        if (member.is_symlink() or not member.is_file() or not _PART_NAME.fullmatch(member.name)
                or [part.partition("=")[0] for part in partition] != columns or not all("=" in p for p in partition)):
            raise ValueError(f"A split member is not <col>=<value>/.../part-NNNNNN.parquet: {path.name}/{relative}")
        key = f"{path.name}/{relative}"
        (directory / key).parent.mkdir(parents=True, exist_ok=True)
        if adopt:
            member.rename(directory / key)
        else:
            shutil.copyfile(member, directory / key)
        observed = table_info(directory / key)
        if info is not None and observed["columns"] != info["columns"]:
            raise ValueError(f"Members of a split table differ in columns: {key}")
        _check_partition(directory / key, dict(part.split("=", 1) for part in partition))
        info, rows[key] = observed, observed["rows"]
        total += observed["rows"]
    assert info is not None
    return {"columns": info["columns"], "rows": total}


def _write_generation_metadata(
    directory: Path, *, family: str, tables: Mapping, members,
    read_snapshot: Mapping | None = None, carried_forward: Mapping[str, str] | None = None,
    publication_status: str = "complete-family", inputs=(), extra_packages: Sequence[str] = (),
    parents: Mapping[str, Mapping] | None = None,
    etl_receipts: Mapping | None = None,
):
    """Write the existing manifest/root format; callers must then verify bytes."""
    from rulespec_artifacts import Producer, build_artifact_root, canonical_json_bytes, write_member_manifest

    with (directory / MANIFEST).open("wb") as stream:
        manifest = write_member_manifest(
            stream, scope_kind="global", scope_id=family, object_key=MANIFEST, members=members
        )
    implementation = implementation_id()
    root = build_artifact_root(
        kind=KIND,
        spec={
            "family": family,
            "tables": tables,
            "packages": {
                name: version(name) for name in ("spicy-regs", "spicy-docs", "rulespec-artifacts", *extra_packages)
            },
            "readSnapshot": dict(read_snapshot or {}),
            "carriedForward": dict(carried_forward or {}),
            "publicationStatus": publication_status,
            **({"etlReceipts": dict(etl_receipts)} if etl_receipts is not None else {}),
            **({"parents": {key: dict(value) for key, value in parents.items()}} if parents else {}),
        },
        producer=Producer("spicy-regs", implementation, "urn:spicy-regs:rollup-verifier", "1", implementation),
        manifests=[manifest],
        inputs=inputs,
    )
    (directory / "artifact.json").write_bytes(canonical_json_bytes(root))


def _verify_receipts(source, tables, members, receipt_spec, table_info, admit_receipts):
    """Recheck all row joins through the same local or pinned remote byte source."""
    from spicy_regs.etl_receipts import DatasetPolicy, RECEIPT_KEY
    from spicy_regs.sources.publication import member_table

    if ({"key", "generationId", "policies", "columns", "rows"} != set(receipt_spec)
            or receipt_spec["key"] != RECEIPT_KEY or not isinstance(receipt_spec["generationId"], str)
            or not receipt_spec["generationId"]):
        raise ValueError("Invalid generation receipt declaration")
    if table_info(RECEIPT_KEY) != {key: receipt_spec[key] for key in ("columns", "rows")}:
        raise ValueError("Receipt member differs from its declared shape/count")
    policies = [DatasetPolicy.from_descriptor(value) for value in receipt_spec["policies"]]
    if {p.dataset + ".parquet" for p in policies if not p.receipt_only} != set(tables):
        raise ValueError("Receipt policies must classify every subject output in a generation")
    subjects = {p.dataset: [] for p in policies}
    for member in members:
        subjects[member_table(member.object_key).removesuffix(".parquet")].append(
            lambda key=member.object_key: source.open(key))
    admit_receipts(subjects, [lambda: source.open(RECEIPT_KEY)], policies,
                   generation_id=receipt_spec["generationId"])
