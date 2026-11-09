"""Resolve one local download selection without fetching remote data."""

from __future__ import annotations

import json
from os import stat_result
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from spicy_regs.runtime_bounds import checkpoint, stream_sha256
from spicy_regs.sources.publication import empty_index, parse_index, table_descriptor, table_members, receipt_members, table_owner

_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")


@dataclass(frozen=True)
class LocalSelection:
    """One resolved local download: its files by name, publication index, and whether it is a batch.

    A split table's entry in ``files`` is its directory; ``split`` holds its member files.
    """

    directory: Path
    files: dict[str, tuple[Path, str]]
    publication: dict
    is_download: bool
    split: dict[str, tuple[Path, ...]] = field(default_factory=dict)

    native: dict = field(default_factory=dict)
    native_signatures: dict = field(default_factory=dict)
    docspec: dict = field(default_factory=dict)
    docspec_signatures: dict = field(default_factory=dict)
    docspec_configuration: dict = field(default_factory=dict)

    @property
    def receipts(self) -> dict[str, Path]:
        if self.native:
            return {str(value.receipts): value.receipts for value in self.native.values()}
        return {member.path: self.directory / receipt_local_key(member)
                for member in selected_receipt_members(self.publication, self.files)}

    def paths(self, name: str) -> tuple[Path, ...]:
        """Every local file of table ``name``: a split table's members in index order, else its one file."""
        return self.split[name] if name in self.split else (self.files[name][0],)


def receipt_local_key(member) -> Path:
    """Family and generation stay in the local key, avoiding shared-name collisions."""
    return Path(".etl-receipts") / member.path


def selected_receipt_members(index: Mapping, selected) -> tuple:
    members = {}
    for name in selected:
        owner = table_owner(index, name + ".parquet")
        if owner and "etlReceipts" in owner[1]:
            for member in receipt_members(index, dataset=name):
                members[member.path] = member
    return tuple(members.values())


def selection_record(index: Mapping, name: str) -> dict:
    """What a download records for table ``name``: its file's path (a split table's member paths) and status."""
    members = table_members(index, f"{name}.parquet")
    split = "members" in (table_descriptor(index, f"{name}.parquet") or {})
    return {"key": [member.path for member in members] if split else members[0].path,
            "status": "managed" if members[0].sha256 is not None else "legacy-unversioned"}


def _unique_pairs(pairs):
    """JSON object hook that refuses a metadata document repeating a key."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeError("Current download metadata repeats a key")
        result[key] = value
    return result


def local_selection(output_dir: Path, *, admit_rows=True) -> LocalSelection:
    """Capture current once; a download batch exposes only its explicit selection.

    A native build root uses its immutable saved selection. A directory without
    a saved selection remains an explicit, unqualified local Parquet workspace.
    """
    root = output_dir.expanduser().absolute()
    configured = os.environ.get("SPICY_REGS_DOCSPEC_EXPORTS")
    if configured:
        configuration = Path(configured)
        if not configuration.is_absolute() or configuration.is_symlink() or not configuration.is_file():
            raise RuntimeError("DocSpec binding must name an explicit regular configuration file")
        if any((root / marker).exists() or (root / marker).is_symlink()
               for marker in ("current", "download.json", ".native-state/selection.json")):
            raise RuntimeError("DocSpec exports require a separate explicit local directory")
        from spicy_regs.docspec_exports import local_exports

        return local_exports(root.resolve(strict=True), configuration)

    if (root / ".native-state" / "selection.json").exists():
        if (root / "current").exists() or (root / "current").is_symlink() or (root / "download.json").exists():
            raise RuntimeError("Ambiguous local selection: use the explicit download batch or a separate native build root")
        return _native_selection(root, admit_rows=admit_rows)
    current = root if root.name == "current" and root.is_symlink() else root / "current"
    if current.is_symlink():
        directory = current.resolve(strict=True)
        if directory.parent != (current.parent / "download-runs").resolve():
            raise RuntimeError("Current download points outside download-runs")
    elif current.exists():
        raise RuntimeError("Current download must be a symlink")
    else:
        directory = root.resolve(strict=True)
    metadata_path = directory / "download.json"
    is_download = metadata_path.is_file()
    if current.is_symlink() and not is_download:
        raise RuntimeError("Current download is missing download.json")
    index = empty_index()
    result, split = {}, {}
    if is_download:
        metadata = json.loads(metadata_path.read_text(), object_pairs_hook=_unique_pairs)
        if metadata.get("version") != 1 or metadata.get("status") != "complete":
            raise RuntimeError("Current download is incomplete")
        index = parse_index(json.dumps(metadata["publication"]).encode())
        selected = metadata.get("selected")
        if not isinstance(selected, dict) or not selected:
            raise RuntimeError("Current download has no selected tables")
        for name, selection in selected.items():
            if not _NAME.fullmatch(name):
                raise RuntimeError("Current download contains an invalid table name")
            if selection != selection_record(index, name):
                raise RuntimeError(f"Current download selection differs from its publication snapshot: {name}")
            if isinstance(selection["key"], list):
                split[name] = tuple(directory / member.key for member in table_members(index, f"{name}.parquet"))
                _assert_exactly(directory / name, split[name])
                result[name] = (directory / name, selection["status"])
                continue
            path = directory / f"{name}.parquet"
            if path.is_symlink() or not path.is_file():
                raise RuntimeError(f"Current download is missing a regular {name}.parquet")
            result[name] = (path, selection["status"])
    if not is_download:
        for path in sorted(root.glob("*.parquet")):
            if _NAME.fullmatch(path.stem) and path.is_file():
                result.setdefault(path.stem, (path.resolve(), "local-unmanaged"))
    selection = LocalSelection(directory, result, index, is_download, split)
    for path in selection.receipts.values():
        file_signature(path)
    return selection



def _native_selection(root: Path, *, admit_rows=True) -> LocalSelection:
    from tempfile import TemporaryDirectory
    from spicy_regs.selected_generations import SelectedInputs
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.etl_receipts import (select_receipts, selected_subject_policy, validate_receipt_bundle,
                                         validate_publisher_generation)

    policies = installed_policies()
    native, visible, parts, signatures = {}, {}, {}, {}
    receipt_versions = {}
    with TemporaryDirectory(prefix="local-native-selection-") as temporary:
        inputs = SelectedInputs(root, temporary, public_url="")
        for name in inputs.local:
            if name not in policies:
                raise ValueError(f"Selected native dataset has no installed policy: {name}")
            selected = inputs.select(name)
            validate_publisher_generation(selected.generation_id)
            native[name] = selected
            paths = (*selected.subjects, selected.receipts)
            for path in paths:
                signatures[str(path)] = file_signature(path)
            checkpoint()
            policy = selected_subject_policy(policies[name], selected.subjects)
            if not selected.subjects:
                from spicy_regs.etl_receipts import receipt_policies
                import pyarrow.parquet as pq
                historical = {p.policy_version: p for p in receipt_policies(policy) if p.receipt_only}
                if historical and not policy.receipt_only:
                    # Classify historical receipt-only data without replaying
                    # payloads during discovery. Reuse one narrow projection for
                    # all datasets sharing the same selected receipt file.
                    receipt_path = str(selected.receipts)
                    if receipt_path not in receipt_versions:
                        versions_by_dataset = {}
                        with pq.ParquetFile(selected.receipts) as parquet:
                            for batch in parquet.iter_batches(columns=["dataset", "policy_version"]):
                                checkpoint()
                                for dataset, version in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist(), strict=True):
                                    versions_by_dataset.setdefault(dataset, set()).add(version)
                        receipt_versions[receipt_path] = versions_by_dataset
                    versions = receipt_versions[receipt_path].get(name, set())
                    if len(versions) != 1:
                        raise ValueError("No-subject selection requires one exact receipt policy")
                    version, = versions
                    if version in historical:
                        policy = historical[version]
                    elif version != policy.policy_version:
                        raise ValueError("No-subject selection has an unsupported receipt policy")
                    else:
                        raise ValueError("Selected current main dataset has no physical subject member")
            if admit_rows:
                scoped = select_receipts(selected.receipts, Path(temporary) / f"{name}.parquet", dataset=name)
                validate_receipt_bundle({name: selected.subjects}, [scoped], [policy],
                                        generation_id=selected.generation_id)
            if not policy.receipt_only:
                # Empty multipart tables have no invented member or partition.
                visible[name] = (selected.subjects[0] if selected.subjects else root, "native-selected")
                parts[name] = selected.subjects
        # Recheck pins against the same captured pointer after validation.
        inputs.cache.clear()
        for name in native:
            inputs.select(name)
        assert_local_members_unchanged(signatures)
    return LocalSelection(root, visible, empty_index(), False, parts, native, signatures)

def _assert_exactly(directory: Path, files: tuple[Path, ...]) -> None:
    """A split table's directory holds exactly its member files, each a regular file, and no symlink anywhere."""
    if directory.is_symlink() or not directory.is_dir():
        raise RuntimeError(f"Current download is missing a regular {directory.name}/ directory")
    found = {path for path in directory.rglob("*") if path.is_symlink() or not path.is_dir()}
    if found != set(files) or any(path.is_symlink() or not path.is_file() for path in files):
        raise RuntimeError(f"Current download {directory.name}/ differs from its published members")


def file_state_from_stat(stat: stat_result) -> tuple[int, int, int, int, int, int]:
    """Bind a checked file descriptor to its exact ordinary local state, including mode."""
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_mode)


def file_signature(path: Path) -> list[int]:
    """Detect ordinary replacement/mutation; this is not a cryptographic pin."""
    stat = path.lstat()
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"Local download member is no longer a regular file: {path.name}")
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def verify_local_members(selection: LocalSelection) -> dict[str, list[int]]:
    """Rehash managed bytes once, every member file of a split table included; return a change guard per file."""
    checkpoint()
    signatures = {}
    if selection.docspec:
        assert_local_members_unchanged(selection.docspec_signatures)
        return dict(selection.docspec_signatures)
    if selection.native:
        assert_local_members_unchanged(selection.native_signatures)
        return dict(selection.native_signatures)
    if not selection.is_download:
        return signatures
    for name, (_, status) in selection.files.items():
        checkpoint()
        pins = table_members(selection.publication, f"{name}.parquet") if status == "managed" else (None,)
        for path, member in zip(selection.paths(name), pins, strict=True):
            before = file_signature(path)
            if member is not None:
                if member.sha256 is None:
                    raise RuntimeError(f"Local download member has no generation pin: {name}")
                with path.open("rb") as stream:
                    digest = "sha256:" + stream_sha256(stream)
                if before[2] != member.byte_size or digest != member.sha256:
                    raise RuntimeError(f"Local download member differs from its generation pin: {name}")
            if file_signature(path) != before:
                raise RuntimeError(f"Local download member changed during verification: {name}")
            signatures[str(path)] = before
    for member in selected_receipt_members(selection.publication, selection.files):
        path = selection.directory / receipt_local_key(member)
        before = file_signature(path)
        with path.open("rb") as stream:
            digest = "sha256:" + stream_sha256(stream)
        if before[2] != member.byte_size or digest != member.sha256:
            raise RuntimeError("Local receipt member differs from its generation pin")
        if file_signature(path) != before:
            raise RuntimeError("Local receipt member changed during verification")
        signatures[str(path)] = before
    return signatures


def assert_local_members_unchanged(signatures: dict[str, list[int]]) -> None:
    """Raise when a member taken signatures from has since changed or vanished."""
    for name, expected in signatures.items():
        try:
            actual = file_signature(Path(name))
        except OSError as exc:
            raise RuntimeError(f"Local download member unavailable: {name}") from exc
        if actual != expected:
            raise RuntimeError(f"Local download member changed after verification: {name}")
