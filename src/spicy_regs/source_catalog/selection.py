"""Select a prepared local catalog and public export with guarded rollback."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile

from .exporter import export, validate_public
from .storage import digest


def require(ok, message):
    if not ok:
        raise ValueError(message)


def plain(path):
    path = path.absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)), f"Linked path: {path}")
    return path.resolve()


def identity(path):
    value = plain(path).stat()
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def check_identities(expected, message):
    """Identify the first changed input without weakening the full identity check."""
    fields = ("device", "inode", "bytes", "mtime_ns", "ctime_ns")
    for path, before in expected.items():
        old = dict(zip(fields, before))
        try:
            after = identity(path)
        except (OSError, ValueError) as error:
            raise ValueError(f"{message}: {path}; expected={old}; current unavailable: {error}") from error
        if after != before:
            raise ValueError(f"{message}: {path}; expected={old}; current={dict(zip(fields, after))}")


def read(path):
    return json.loads(plain(path).read_bytes())


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


@contextmanager
def selection_lock(cache):
    path = plain(cache / ".selection.lock")
    with path.open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("Another local catalog selection is running") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def metadata(index):
    require(not any(index.parent.glob(index.name + "-*")), f"Index has active sidecars: {index}")
    with sqlite3.connect(index.as_uri() + "?mode=ro", uri=True) as db:
        require(db.execute("PRAGMA journal_mode").fetchone()[0] != "wal", "Checkpoint the catalog before selection")
        return {key: json.loads(value) for key, value in db.execute("SELECT key,value FROM metadata")}


def inventory_pins(snapshot_sha256, receipt_paths):
    paths = sorted(receipt_paths)
    return {
        "raw_snapshot_sha256": snapshot_sha256.removeprefix("sha256:"),
        "raw_acquisition_receipts": len(paths),
        "raw_acquisition_receipt_set_sha256": hashlib.sha256(("\n".join(paths) + "\n").encode()).hexdigest(),
    }


def raw_selection(raw_root):
    paths = sorted(str(path.relative_to(raw_root)) for path in (raw_root / "acquisitions/sha256").glob("*/*"))
    for name in paths:
        require(plain(raw_root / name).is_file(), f"Invalid acquisition receipt: {name}")
    return inventory_pins(digest(plain(raw_root / "snapshot.json")), paths)


def index_inventory(db, raw_root):
    snapshot = db.execute("SELECT sha256,bytes FROM inputs WHERE path=?", (str(raw_root / "snapshot.json"),)).fetchone()
    require(snapshot is not None, "Catalog does not pin the raw snapshot")
    receipt_paths = [str(Path(path).relative_to(raw_root)) for (path,) in db.execute("SELECT path FROM inputs")
                     if Path(path).is_relative_to(raw_root / "acquisitions/sha256")]
    return inventory_pins(snapshot[0], receipt_paths), snapshot[1]


def public_members(root, generation, *, decode=True):
    """Read and pin one public document at a time, following only contained references."""
    members, identities = [], {}
    public_version = None
    prefix = "generations/" + generation.removeprefix("sha256:") + "/"

    def member_bytes(name):
        relative = Path(name)
        require(not relative.is_absolute() and ".." not in relative.parts, f"Invalid public member: {name}")
        require(name == "manifest.json" or name.startswith(prefix), f"Public member outside selected generation: {name}")
        path = plain(root / relative)
        require(path not in identities and path.is_file(), f"Missing or repeated public member: {name}")
        before = identity(path)
        body = path.read_bytes()
        require(identity(path) == before, f"Public member changed while reading: {name}")
        identities[path] = before
        members.append({"path": name, "sha256": "sha256:" + hashlib.sha256(body).hexdigest(), "bytes": len(body)})
        return body

    def member(name, format_name):
        value = json.loads(member_bytes(name))
        validate_public(value)
        require(value.get("audience") != "internal", "Internal export cannot be selected for public use")
        require(value.get("format") == format_name and value.get("version") in (1, 2, 3)
                and (public_version is None or value["version"] == public_version)
                and value.get("generation") == generation, f"Invalid public format or generation: {name}")
        return value

    manifest = member("manifest.json", "spicy-source-catalog")
    public_version = manifest["version"]
    collections = set()
    for summary in manifest["collections"]:
        require(summary["id"] not in collections, "Repeated public collection")
        collections.add(summary["id"])
        detail = member(summary["detailPath"], "spicy-source-collection")
        detail_summary = detail["collection"] if public_version == 1 else {
            key: value for key, value in detail["collection"].items() if key != "periods"
        }
        require(detail_summary == summary, "Public collection differs from manifest")
        def file_inventory(inventory, inventory_kind):
            rows = 0
            require(type(inventory["total"]) is int and inventory["total"] >= 0, "Invalid public file total")
            for page in inventory["pages"]:
                require(type(page["rows"]) is int and 0 <= page["rows"] <= 5000, "Invalid public file page size")
                if public_version == 3:
                    from .file_inventory import MAX_BYTES, SCHEMA_ID, validate
                    require(set(page) == {"path", "format", "schema", "rows", "bytes", "sha256"}
                            and page["format"] == "parquet" and page["schema"] == SCHEMA_ID,
                            "Invalid native public file descriptor")
                    require(type(page["bytes"]) is int and 0 < page["bytes"] <= MAX_BYTES,
                            "Invalid native public file byte size")
                    body = member_bytes(page["path"])
                    require(len(body) == page["bytes"] and "sha256:" + hashlib.sha256(body).hexdigest() == page["sha256"],
                            "Native public file pin differs")
                    if decode:
                        records = validate(body, generation=generation, dataset_id=summary["id"], inventory_kind=inventory_kind)
                        require(len(records) == page["rows"], "Public file page differs from collection")
                    rows += page["rows"]
                else:
                    value = member(page["path"], "spicy-source-files")
                    require(value["collectionId"] == summary["id"] and len(value["files"]) == page["rows"],
                            "Public file page differs from collection")
                    rows += len(value["files"])
            require(rows == inventory["total"], "Public file total differs")
            return rows

        require(file_inventory(detail["files"], "active") == summary["holdings"]["aliases"], "Public file total differs")
        if "retainedFiles" in detail:
            file_inventory(detail["retainedFiles"], "attention")
    require(len(collections) == manifest["totals"]["collections"], "Public collection total differs")
    generation_root = plain(root / prefix)
    actual = set()
    directories = [generation_root]
    for path in generation_root.rglob("*"):
        plain(path)
        if path.is_file():
            actual.add(path)
        elif path.is_dir():
            directories.append(path)
    require(actual == set(identities) - {root / "manifest.json"}, "Unreferenced or missing public generation members")
    identities.update((path, identity(path)) for path in directories)
    return sorted(members, key=lambda item: item["path"]), identities


def select(candidate: Path, reconciliation: Path, staged_public: Path, cache: Path, public: Path,
           raw_root: Path, *, expected_generation: str, allow_inventory_change: bool = False,
           receipt_snapshot: Path | None = None, receipt_snapshot_sha256: str | None = None) -> dict:
    """Replace an existing local selection, optionally accepting reviewed new raw inventory.

    Builders own SQLite integrity checks. Selection checks their matching report,
    hashes the candidate once, and validates each staged public member. Pause local
    readers/writers during the switch: separate filesystem replacements are not a
    multi-file transaction. Backups remain available after success or interruption.
    A reviewed inventory change accepts only the candidate-pinned snapshot and
    receipt set currently held by the raw archive; the previous controls must still agree.
    """
    candidate, reconciliation, staged_public, cache, public, raw_root = map(
        plain, (candidate, reconciliation, staged_public, cache, public, raw_root))
    for path in (candidate, reconciliation, staged_public, cache, public):
        require(not path.is_relative_to(raw_root), "Catalog selection must stay outside the raw archive")
    require(candidate != cache / "index.sqlite" and staged_public != public, "Selection requires separate candidate outputs")
    require(cache.is_dir() and public.is_dir(), "An existing local selection is required")
    with selection_lock(cache):
        return _select(candidate, reconciliation, staged_public, cache, public, raw_root, expected_generation,
                       allow_inventory_change, receipt_snapshot, receipt_snapshot_sha256)


def _select(candidate, reconciliation, staged_public, cache, public, raw_root, expected_generation,
            allow_inventory_change, receipt_snapshot=None, receipt_snapshot_sha256=None):
    selected_index = cache / "index.sqlite"
    targets = [selected_index, cache / "index.sqlite.reconciliation.json", cache / "reconciliation.json",
               cache / "selection.json", public / "manifest.json", cache / "current.json"]
    baseline = {path: identity(path) for path in targets}
    current, selected = read(cache / "current.json"), read(cache / "selection.json")
    require(current["generation"] == selected["generation"] == expected_generation, "Selected generation changed")
    require(current.get("index") == "index.sqlite" and current.get("selection") == "selection.json",
            "Unsupported selected catalog paths")
    require(Path(current["public_manifest"]).resolve() == public / "manifest.json", "Selected public root differs")
    require(digest(public / "manifest.json").removeprefix("sha256:") == current["public_manifest_sha256"],
            "Selected public manifest pin differs")
    require(read(public / "manifest.json")["generation"] == expected_generation, "Selected public generation differs")
    prior_digest = current.get('index_sha256')
    require(isinstance(prior_digest, str) and bool(re.fullmatch(r'[a-f0-9]{64}', prior_digest)),
            'Selected index has no recorded byte identity')
    if receipt_snapshot is not None or receipt_snapshot_sha256 is not None:
        from .maintenance import check_receipt_snapshot

        qualified, _ = check_receipt_snapshot(receipt_snapshot, receipt_snapshot_sha256,
                                               index=selected_index, expected_generation=expected_generation)
        require(Path(qualified['rawRoot']) == raw_root, 'Receipt qualification uses another raw archive')
        require(qualified['baseIndex']['sha256'] == 'sha256:' + prior_digest,
                'Receipt qualification differs from selected index identity')
    else:
        require(digest(selected_index).removeprefix('sha256:') == prior_digest,
                'Selected index bytes differ from their recorded identity')
    previous_metadata = metadata(selected_index)
    require(previous_metadata["generation"] == expected_generation, "Selected index generation differs")
    require(read(targets[1]) == read(targets[2]) and read(targets[1])["generation"] == expected_generation,
            "Selected reconciliation reports differ")
    raw = raw_selection(raw_root)
    previous_raw = {key: selected.get(key) for key in raw}
    require(all(current.get(key) == value for key, value in previous_raw.items()),
            "Selected raw inventory controls differ")
    require(all(isinstance(previous_raw[key], str) and re.fullmatch(r"[a-f0-9]{64}", previous_raw[key])
                for key in ("raw_snapshot_sha256", "raw_acquisition_receipt_set_sha256"))
            and isinstance(previous_raw["raw_acquisition_receipts"], int)
            and not isinstance(previous_raw["raw_acquisition_receipts"], bool)
            and previous_raw["raw_acquisition_receipts"] >= 0, "Selected raw inventory pins are invalid")
    with sqlite3.connect(selected_index.as_uri() + "?mode=ro", uri=True) as db:
        require(index_inventory(db, raw_root)[0] == previous_raw, "Selected index raw inventory differs from controls")
    require(allow_inventory_change or previous_raw == raw,
            "Raw inventory changed; review the candidate and use --allow-inventory-change")
    candidate_identity, report_identity = identity(candidate), identity(reconciliation)
    report, candidate_metadata = read(reconciliation), metadata(candidate)
    with sqlite3.connect(candidate.as_uri() + "?mode=ro", uri=True) as db:
        candidate_raw, snapshot_bytes = index_inventory(db, raw_root)
        require(candidate_raw["raw_snapshot_sha256"] == raw["raw_snapshot_sha256"]
                and snapshot_bytes == (raw_root / "snapshot.json").stat().st_size,
                "Candidate does not pin the current raw snapshot")
        require(candidate_raw == raw, "Candidate does not pin the current receipt set")
        current_tables = db.execute("SELECT count(*) FROM tables WHERE json_extract(data,'$.publication.scope')='current'").fetchone()[0]
    generation = candidate_metadata["generation"]
    require(bool(re.fullmatch(r"sha256:[a-f0-9]{64}", generation)) and generation != expected_generation,
            "Candidate must have a new catalog generation")
    require(report.get("status") == "validated" and report.get("generation") == generation,
            "Candidate reconciliation is not validated for this generation")
    require(report.get('baseGeneration', expected_generation) == expected_generation
            or expected_generation in report.get('ancestry', []), 'Candidate belongs to another selected base')
    require(report in (candidate_metadata.get("reconciliation"), candidate_metadata.get("publication_reconciliation")),
            "Candidate reconciliation differs from index metadata")
    if previous_metadata.get("publication_reconciliation"):
        require(current_tables > 0
                and candidate_metadata.get("publication_reconciliation", {}).get("currentTables") == current_tables,
                "Candidate dropped selected publication accounting")
    from .export_completion import check_references, load as load_export_completion

    admitted = load_export_completion(cache, candidate, staged_public, generation)
    if admitted is not None:
        completion, staged_identities = admitted
        members = completion['members']
        index_sha256 = completion['indexSha256'].removeprefix('sha256:')
    else:
        members, staged_identities = public_members(staged_public, generation)
        staged_manifest = read(staged_public / "manifest.json")
        version = staged_manifest["version"]
        layout = {}
        for summary in staged_manifest["collections"]:
            detail = read(staged_public / summary["detailPath"])
            for field, kind in (("files", "active"), ("retainedFiles", "attention")):
                inventory = detail.get(field)
                if inventory is None:
                    continue
                pages = inventory["pages"]
                provenance = set()
                if version in (1, 2):
                    for page in pages:
                        value = read(staged_public / page["path"])
                        provenance.update("provenance" in row for row in value["files"])
                    require(len(provenance) <= 1, "Mixed legacy provenance layouts")
                    require(version == 1 or provenance != {False}, "Version 2 provenance is missing")
                layout[(summary["id"], kind)] = {"rows": [page["rows"] for page in pages],
                                                  "provenance": next(iter(provenance), True)}
        # Unadmitted or transferred exports retain canonical replay. A record
        # supplied alongside staging cannot stand in for native completion.
        expected = {item["path"]: item for item in members}
        emitted = set()

        def compare(path, body):
            name = str(path.relative_to(staged_public))
            item = expected.get(name)
            if item is None or name in emitted:
                raise ValueError(f"Staged public member differs from candidate export: {name}")
            if path.suffix == ".json":
                require(json.loads(body) == read(path), f"Staged public member differs from candidate export: {name}")
            else:
                require(len(body) == item["bytes"] and "sha256:" + hashlib.sha256(body).hexdigest() == item["sha256"],
                        f"Staged public member differs from candidate export: {name}")
            emitted.add(name)

        export(candidate, staged_public, emit=compare, public_version=version, page_layout=layout)
        require(emitted == set(expected), "Staged public members differ from candidate export")
        index_sha256 = digest(candidate).removeprefix("sha256:")
    generation_name = generation.removeprefix("sha256:")
    source_generation = staged_public / "generations" / generation_name
    destination_generation = plain(public / "generations" / generation_name)
    require(not destination_generation.exists(), "Public generation already exists")
    require(candidate.stat().st_dev == cache.stat().st_dev
            and source_generation.stat().st_dev == public.stat().st_dev,
            "Candidate and staged public generation must share their destination filesystems")

    def guards():
        check_identities(baseline, "Selected files changed during validation")
        check_identities({candidate: candidate_identity, reconciliation: report_identity}, "Candidate changed during validation")
        check_identities(staged_identities, "Staged public files changed")
        if admitted is not None:
            check_references(completion["references"])
        if receipt_snapshot is not None:
            # Validation/export can outlive the initial custody check. Recheck
            # raw inputs here without rebinding the hard-linked selected index.
            qualified, _ = check_receipt_snapshot(receipt_snapshot, receipt_snapshot_sha256)
            require(Path(qualified['rawRoot']) == raw_root, 'Receipt qualification uses another raw archive')
        require(raw_selection(raw_root) == raw, "Raw inventory changed during validation")
        metadata(selected_index)
        metadata(candidate)

    guards()
    backup = Path(tempfile.mkdtemp(prefix="selection-backup-", dir=cache))
    saved = []
    try:
        for number, target in enumerate(targets):
            previous = backup / str(number)
            if target == selected_index:
                os.link(target, previous)
            else:
                shutil.copy2(target, previous)
            saved.append((target, previous))
        # Creating the backup hard link changes ctime, but not selected content.
        linked_identity = identity(selected_index)
        require(linked_identity[:-1] == baseline[selected_index][:-1], "Selected index changed while backing up")
        baseline[selected_index] = linked_identity
        manifest_pin = next(item for item in members if item["path"] == "manifest.json")
        selection = {"format": "spicygov-local-catalog-selection", "version": 1,
                     "generation": generation, **raw, "files": members}
        refreshed = {**current, "generation": generation, **raw, "index_sha256": index_sha256,
                     "public_manifest_sha256": manifest_pin["sha256"].removeprefix("sha256:"),
                     "refreshed_at": datetime.now(UTC).isoformat(),
                     "scope": report.get("scope", "Local source catalog selection; no new archive audit or deployment claim.")}
        prepared = [(targets[1], report), (targets[2], report), (targets[3], selection), (targets[5], refreshed)]
        replacements = []
        for number, (target, value) in enumerate(prepared):
            path = backup / f"prepared-{number}.json"
            write(path, value)
            replacements.append((path, target))
        moves = [(source_generation, destination_generation), (candidate, selected_index),
                 *replacements[:-1], (staged_public / "manifest.json", public / "manifest.json"), replacements[-1]]
        write(backup / "recovery.json", {"previousGeneration": expected_generation, "generation": generation,
              "saved": [{"target": str(target), "backup": str(previous)} for target, previous in saved],
              "moves": [{"source": str(source), "destination": str(target)} for source, target in moves],
              "previousRawInventory": previous_raw, "rawInventory": raw})
        guards()
    except BaseException:
        shutil.rmtree(backup)
        raise
    moved = []
    try:
        for source, target in moves:
            os.replace(source, target)
            moved.append((source, target))
        require(metadata(selected_index)["generation"] == read(cache / "current.json")["generation"]
                == read(cache / "selection.json")["generation"] == read(public / "manifest.json")["generation"] == generation,
                "Selected generation readback differs")
    except BaseException as error:
        failures = []
        for source, target in reversed(moved):
            try:
                os.replace(target, source)
            except OSError as failure:
                failures.append(f"{target} -> {source}: {failure}")
        for target, previous in saved:
            try:
                temporary = target.parent / (".restore-" + backup.name + "-" + previous.name)
                if target == selected_index:
                    os.link(previous, temporary)
                else:
                    shutil.copy2(previous, temporary)
                os.replace(temporary, target)
            except OSError as failure:
                failures.append(f"Restore {target}: {failure}")
        if failures:
            write(backup / "rollback-errors.json", failures)
            raise RuntimeError(f"Selection failed; recover saved files from {backup}") from error
        raise
    return {"status": "selected", "generation": generation, "previousGeneration": expected_generation,
            "publicMembers": len(members), "backup": str(backup),
            "inventoryChanged": previous_raw != raw, "previousRawInventory": previous_raw, "rawInventory": raw}
