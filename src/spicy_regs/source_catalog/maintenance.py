"""Refresh reviewed catalog metadata over unchanged raw holdings into a candidate."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from .importer import Importer, encoded
from .publication import _clone_index, import_publications
from .raw_archive import classify_retained_objects, holdings, remap_object_datasets, retained_classification_candidates
from .raw_classification import classification_inputs
from .scope import install_scope
from .selection import check_identities, identity, index_inventory, metadata, plain, raw_selection, require
from .storage import Inputs, connect, digest, schema, stable


def receipt_inventory(root):
    """Inventory names and custody without opening receipt or original payloads."""
    result = {}
    for path in sorted(root.glob('acquisitions/sha256/*/*')):
        relative = str(path.relative_to(root))
        require(bool(re.fullmatch(r'acquisitions/sha256/([0-9a-f]{2})/([0-9a-f]{64})', relative))
                and path.parent.name == path.name[:2], 'Invalid acquisition receipt key')
        custody = list(identity(path))
        require(stat.S_ISREG(path.stat().st_mode), 'Acquisition receipt must be a regular file')
        result[relative] = {'sha256': 'sha256:' + path.name, 'bytes': custody[2], 'custody': custody}
    return result


def freeze_receipts(index, output, *, expected_generation, expected_index_sha256=None):
    """Authenticate one base and retain its receipt admission under exact custody.

    Legacy indexes have accepted receipt hashes but no custody completion. Their
    first qualification hashes those receipts once. Later runs reuse that result
    only while their full file identity matches. This is not a deep raw-object
    audit: inherited originals retain the central archive's immutable custody.
    """
    from .selection import inventory_pins, write

    index, output = plain(index), plain(output)
    require(not output.exists(), 'Receipt snapshot requires a new output')
    before = identity(index)
    meta = metadata(index)
    require(meta['generation'] == expected_generation, 'Selected base catalog generation changed')
    index_digest = digest(index)
    if expected_index_sha256 is not None:
        require(index_digest.removeprefix('sha256:') == expected_index_sha256.removeprefix('sha256:'),
                'Selected index bytes differ from their recorded identity')
    with closing(connect(index, readonly=True, scope='all')) as db:
        roots = {Path(row[0]).parent for row in db.execute("SELECT path FROM inputs WHERE path LIKE '%/snapshot.json'")
                 if (Path(row[0]).parent / 'objects/sha256').is_dir()}
        require(len(roots) == 1, 'Receipt qualification requires one central raw archive')
        root = plain(roots.pop())
        require(not output.is_relative_to(root), 'Receipt snapshot must stay outside the raw archive')
        accepted = {str(Path(p).relative_to(root)): {'sha256': sha, 'bytes': size}
                    for p, sha, size in db.execute('SELECT path,sha256,bytes FROM inputs')
                    if Path(p).is_relative_to(root / 'acquisitions/sha256')}
        pinned_raw, _ = index_inventory(db, root)
    current = receipt_inventory(root)
    require(accepted.keys() <= current.keys(), 'Previously indexed acquisition receipts disappeared')
    require(digest(root / 'snapshot.json').removeprefix('sha256:') == pinned_raw['raw_snapshot_sha256'],
            'Raw inventory changed; rebuild holdings before refreshing metadata')
    prior = meta.get('receipt_admission', {})
    reusable = prior.get('receipts', {}) if prior.get('version') == 1 else {}
    reads = logical_bytes = reused = 0
    for relative, pin in accepted.items():
        now = current[relative]
        require({k: now[k] for k in ('sha256', 'bytes')} == pin,
                'Previously indexed acquisition receipt differs from its accepted pin')
        if reusable.get(relative) == now:
            reused += 1
        else:
            require(digest(root / relative) == pin['sha256'], 'Previously indexed acquisition receipt changed')
            reads += 1
            logical_bytes += pin['bytes']
        require(list(identity(root / relative)) == now['custody'], 'Receipt changed during qualification')
    snapshot = {'format': 'spicy-source-receipt-snapshot', 'version': 1,
                'baseGeneration': expected_generation,
                'baseIndex': {'sha256': index_digest, 'bytes': before[2], 'custody': list(before)},
                'rawRoot': str(root), 'rawRootIdentity': list(identity(root)[:2]),
                'rawInventory': inventory_pins(pinned_raw['raw_snapshot_sha256'], current),
                'receipts': current,
                'work': {'indexBytesHashed': before[2], 'inheritedReceiptReads': reads,
                         'inheritedReceiptBytesHashed': logical_bytes, 'inheritedReceiptReused': reused,
                         'inheritedOriginalPayloadOpens': 0}}
    check_identities({index: before}, 'Base catalog changed during receipt qualification')
    after = receipt_inventory(root)
    require(current.keys() <= after.keys() and all(after[key] == pin for key, pin in current.items()),
            'Frozen receipts changed during qualification')
    require(digest(root / 'snapshot.json').removeprefix('sha256:') == pinned_raw['raw_snapshot_sha256'],
            'Raw snapshot changed during qualification')
    output.parent.mkdir(parents=True, exist_ok=True)
    write(output, snapshot)
    return snapshot


def check_receipt_snapshot(path, expected_sha256, *, index=None, expected_generation=None):
    """Reuse a completed, caller-pinned qualification under unchanged file custody."""
    from .selection import inventory_pins, read

    require(path is not None and expected_sha256 is not None, 'Receipt snapshot requires its caller-pinned SHA-256')
    path = plain(path)
    require(digest(path) == 'sha256:' + expected_sha256.removeprefix('sha256:'), 'Receipt snapshot pin differs')
    value = read(path)
    require(value.get('format') == 'spicy-source-receipt-snapshot' and value.get('version') == 1,
            'Unsupported receipt snapshot')
    root = plain(Path(value['rawRoot']))
    require(list(identity(root)[:2]) == value['rawRootIdentity'], 'Raw archive custody changed')
    require(digest(root / 'snapshot.json').removeprefix('sha256:') == value['rawInventory']['raw_snapshot_sha256'],
            'Raw snapshot changed since qualification')
    current = receipt_inventory(root)
    require(value['receipts'].keys() <= current.keys(), 'Frozen acquisition receipts disappeared')
    require(all(current[key] == pin for key, pin in value['receipts'].items()), 'Frozen receipt custody changed')
    require(inventory_pins(value['rawInventory']['raw_snapshot_sha256'], value['receipts']) == value['rawInventory'],
            'Frozen receipt inventory differs from its pins')
    if index is not None:
        require(list(identity(index)) == value['baseIndex']['custody'], 'Qualified base catalog custody changed')
        require(metadata(index)['generation'] == value['baseGeneration'] == expected_generation,
                'Qualified base catalog generation differs')
    return value, sorted(current.keys() - value['receipts'].keys())


def configured_metadata(db, configuration, inputs, base_generation):
    """Apply the importer's configured IDs while retaining superseded row values."""
    normalized = sqlite3.connect(":memory:")
    try:
        schema(normalized)
        Importer(normalized, inputs, configuration)
        changed = {}
        explicit_collections = {entry["id"] for entry in configuration.get("collections", [])}
        for table in ("collections", "routes", "evidence"):
            rows = list(normalized.execute(f"SELECT * FROM {table} ORDER BY id"))
            columns = [item[1] for item in normalized.execute(f"PRAGMA table_info({table})")]
            updates = ",".join(f"{column}=excluded.{column}" for column in columns[1:])
            changed[table] = 0
            for row in rows:
                previous = db.execute(f"SELECT * FROM main.{table} WHERE id=?", (row[0],)).fetchone()
                if table == "collections" and previous is not None:
                    if row[0] not in explicit_collections:
                        continue
                    row = (row[0], row[1], encoded({**json.loads(previous[2]), **json.loads(row[2])}))
                if previous is not None and tuple(previous) == row:
                    continue
                if previous is not None:
                    # Rows from acquisitions and discovery remain unchanged.
                    # An explicitly configured ID may supersede metadata, while
                    # its previous value remains inspectable as history.
                    prior = {"table": table, "row": list(previous), "baseGeneration": base_generation}
                    history_id = "configured-metadata-history:" + stable(prior)
                    cid = previous[0] if table == "collections" else previous[1]
                    db.execute("INSERT OR IGNORE INTO main.evidence VALUES(?,?,?,?)", (
                        history_id, cid, "configured_metadata_history", encoded(prior),
                    ))
                placeholders = ",".join("?" for _ in row)
                db.execute(f"INSERT INTO main.{table} VALUES({placeholders}) ON CONFLICT(id) DO UPDATE SET {updates}", row)
                changed[table] += 1
        if remapped := remap_object_datasets(db, configuration):
            changed["object_datasets"] = remapped
        return changed
    finally:
        normalized.close()


def refresh_metadata(index: Path, output: Path, *, config: Path | None = None,
                     publication_reconciliation: Path | None = None, expected_generation: str,
                     include_acquisitions: bool = False, refresh_content: tuple[str, ...] = (),
                     reuse_unchanged: bool = False, refresh_object_list: Path | None = None,
                     receipt_snapshot: Path | None = None, receipt_snapshot_sha256: str | None = None) -> dict:
    """Clone a pinned raw catalog, refresh metadata, and leave selection separate.

    Base input pins retain their original meaning. Phase inputs identify the code,
    registry and optional publication evidence read by this bounded update. This
    default operation does not replay acquisition manifests. The optional
    acquisition mode adds committed receipts over the unchanged base snapshot;
    named content readers refresh only when their interpretation inputs change.
    Neither mode writes the raw archive.
    """
    index, output = plain(index), plain(output)
    require(index != output, "Metadata refresh requires a separate candidate")
    source_identity = identity(index)
    source_metadata = metadata(index)
    require(source_metadata["generation"] == expected_generation, "Selected base catalog generation changed")
    frozen = None
    if receipt_snapshot is not None or receipt_snapshot_sha256 is not None:
        require(include_acquisitions, 'Frozen receipt inputs require acquisition reconciliation')
        frozen, _ = check_receipt_snapshot(receipt_snapshot, receipt_snapshot_sha256,
                                            index=index, expected_generation=expected_generation)
    source = connect(index, readonly=True, scope="all")
    candidate = report_candidate = db = None
    installed = []
    try:
        source.execute("BEGIN")
        roots = {Path(row[0]).parent for row in source.execute("SELECT path FROM inputs WHERE path LIKE '%/snapshot.json'")
                 if (Path(row[0]).parent / "objects/sha256").is_dir()}
        require(len(roots) == 1, "Metadata refresh requires one pinned central raw archive")
        raw_root = plain(roots.pop())
        require(not output.is_relative_to(raw_root), "The rebuildable catalog must live outside the raw archive")
        output.parent.mkdir(parents=True, exist_ok=True)
        report_path = output.with_suffix(output.suffix + ".reconciliation.json")
        require(not output.exists() and not report_path.exists(), "Metadata refresh requires new candidate outputs")
        raw = frozen['rawInventory'] if frozen is not None else raw_selection(raw_root)
        if frozen is not None:
            require(str(raw_root) == frozen['rawRoot'], 'Receipt snapshot names another raw archive')

        def check_inventory():
            if frozen is None:
                require(raw_selection(raw_root) == raw, 'Raw inventory changed during metadata refresh')
                return []
            _, added = check_receipt_snapshot(receipt_snapshot, receipt_snapshot_sha256,
                                              index=index, expected_generation=expected_generation)
            return added
        indexed_raw, snapshot_bytes = index_inventory(source, raw_root)
        require((include_acquisitions or indexed_raw == raw)
                and indexed_raw['raw_snapshot_sha256'] == raw['raw_snapshot_sha256']
                and snapshot_bytes == (raw_root / "snapshot.json").stat().st_size,
                "Raw inventory changed; rebuild holdings before refreshing metadata")
        prior_receipts = {row[0]: {"sha256": row[1], "bytes": row[2]}
                          for row in source.execute("SELECT path,sha256,bytes FROM inputs")
                          if Path(row[0]).is_relative_to(raw_root / 'acquisitions/sha256')}
        if include_acquisitions:
            require(prior_receipts.keys() <= {str(p.resolve()) for p in raw_root.glob('acquisitions/sha256/*/*')},
                    "Previously indexed acquisition receipts disappeared")
        require(include_acquisitions or not refresh_content, "Content refresh requires acquisition reconciliation")
        require(not source_metadata.get("publication_reconciliation") or publication_reconciliation is not None,
                "Publication-aware metadata refresh requires --publication-reconciliation")
        inputs = Inputs(raw_root)
        for code in (*Path(__file__).parent.glob("*.py"), *classification_inputs()):
            inputs.pin(code)
        config = plain(config or Path(__file__).parent / "config/registry.v1.json")
        require(not config.is_relative_to(raw_root), "Catalog configuration must stay outside the raw archive")
        configuration = inputs.json(config)
        require(isinstance(configuration, dict), "Catalog configuration must be an object")
        readers = dict(source_metadata.get('reconciliation', {}).get('inputRefresh', {}).get('contentReaders', {}))
        partial_readers = dict(source_metadata.get('reconciliation', {}).get('inputRefresh', {}).get('partialContentReaders', {}))
        requested_readers = {}
        cohort = None
        cohort_pin = None
        if refresh_object_list is not None:
            require(include_acquisitions and refresh_content, "Object replay requires named content refresh collections")
            require(len(set(refresh_content)) == 1, "Object replay requires exactly one named collection")
            path = inputs.pin(plain(refresh_object_list))
            cohort = tuple(path.read_text().splitlines())
            require(bool(cohort) and len(set(cohort)) == len(cohort)
                    and all(re.fullmatch(r"sha256:[0-9a-f]{64}", sha) for sha in cohort),
                    "Object list must contain unique canonical SHA-256 identities, one per line")
            cohort = tuple(sorted(cohort))
            placeholders = ','.join('?' for _ in refresh_content)
            require(all(source.execute("SELECT 1 FROM files WHERE sha256=? AND collection_id IN ("
                        + placeholders + ") LIMIT 1", (sha, *refresh_content)).fetchone() for sha in cohort),
                    "Every replay object must exist in a named collection in the selected catalog")
            cohort_pin = {'objects': len(cohort), 'objectSetSha256': 'sha256:' + hashlib.sha256(''.join(sha+'\n' for sha in cohort).encode()).hexdigest()}
        if include_acquisitions:
            from .raw_content import SUPPORTED_DATASETS, reader_inputs
            import spicy_docs.storage.raw_archive as archive_reader
            inputs.pin(Path(archive_reader.__file__))
            require(set(refresh_content) <= SUPPORTED_DATASETS, "Unsupported content refresh collection")
            for cid in dict.fromkeys(refresh_content):
                dependencies = [inputs.pins[str(inputs.pin(path))] for path in reader_inputs(cid)]
                requested_readers[cid] = stable(cid, inputs.pins[str(Path(__file__).parent / 'raw_content.py')],
                    inputs.pins[str(Path(__file__).parent / 'raw_mapping.py')], inputs.pins[str(config)], dependencies)
        if publication_reconciliation is not None:
            inputs.pin(plain(publication_reconciliation))
        previous_pins = source_metadata.get('reconciliation', {}).get('phaseInputs', {})
        previous_interpretation = {path: pin for path, pin in previous_pins.items()
                                   if not Path(path).is_relative_to(raw_root)}
        def reader_current(cid, reader):
            if readers.get(cid) == reader:
                return True
            if cohort is None:
                return False
            assert cohort_pin is not None
            return partial_readers.get(cid) == {**cohort_pin, 'reader': reader}

        if (reuse_unchanged and previous_interpretation and indexed_raw == raw
                and all(previous_interpretation.get(path) == pin for path, pin in inputs.pins.items())
                and all(reader_current(cid, value) for cid, value in requested_readers.items())):
            # Reuse only this owner's accepted generation under unchanged file
            # custody and interpretation inputs. Raw bodies are immutable and
            # their deep audit is a separate operation.
            compatible = True
            for path, pin in previous_interpretation.items():
                if not Path(path).is_file() or inputs.pins[str(inputs.pin(Path(path)))] != pin:
                    compatible = False
                    break
            if compatible:
                inputs.check()
                check_identities({index: source_identity}, "Base catalog changed during unchanged check")
                added = check_inventory()
                return {'format': 'spicy-source-metadata-refresh', 'version': 1, 'status': 'unchanged',
                        'generation': expected_generation, 'rawInventory': raw,
                        'freshness': {'status': 'pending-catch-up' if added else 'current', 'addedReceiptKeys': added},
                        'scope': 'Accepted selected generation reused; no clone, raw payload read, validation or export.'}
        descriptor, temporary = tempfile.mkstemp(prefix=output.name + ".", suffix=".tmp", dir=output.parent)
        os.close(descriptor)
        candidate = Path(temporary)
        method = _clone_index(index, candidate, source)
        check_identities({index: source_identity}, 'Base catalog changed while cloning metadata candidate')
        require(metadata(candidate)['generation'] == expected_generation, 'Cloned metadata base generation differs')
        db = connect(candidate)
        changed = configured_metadata(db, configuration, inputs, expected_generation)
        delta = None
        if include_acquisitions:
            from .raw_archive import RawImporter
            importer = RawImporter(db, inputs, configuration)
            importer.prepare_metadata()
            refreshed = 0
            for cid, reader in requested_readers.items():
                if not reader_current(cid, reader):
                    refreshed += importer.refresh_content((cid,), objects=cohort)
                if cohort is None:
                    readers[cid] = reader
                    partial_readers.pop(cid, None)
                else:
                    # This reader was applied only to the named byte identities.
                    # Retain the family-wide reader pin for all other originals.
                    assert cohort_pin is not None
                    partial_readers[cid] = {**cohort_pin, 'reader': reader}
            reviews = importer.import_receipts(skip=prior_receipts, **({'receipt_snapshot': frozen} if frozen is not None else {}))
            importer.finish_reviews(reviews)
            for path, pin in inputs.pins.items():
                if Path(path).is_relative_to(raw_root / 'acquisitions/sha256') and path not in prior_receipts:
                    db.execute('INSERT INTO inputs VALUES(?,?,?)', (path, pin['sha256'], pin['bytes']))
            if frozen is None:
                raw = raw_selection(raw_root)
            require(index_inventory(db, raw_root)[0] == raw, 'Acquisition receipt inventory changed during refresh')
            if frozen is not None:
                db.execute("INSERT OR REPLACE INTO metadata VALUES('receipt_admission',?)",
                           (encoded({'version': 1, 'receipts': frozen['receipts']}),))
            delta = {'newReceipts': raw['raw_acquisition_receipts'] - len(prior_receipts),
                     'refreshedFileLocations': refreshed, 'contentReaders': readers,
                     'partialContentReaders': partial_readers,
                     'metadataExtraction': dict(importer.metadata_counts),
                     'baseInventoryReplayed': False}
        classification = classify_retained_objects(db, raw_root, retained_classification_candidates(db, configuration))
        publication = None
        if publication_reconciliation is not None:
            publication = import_publications(db, inputs, publication_reconciliation)
        policy = configuration.get("catalog_scope", source_metadata.get("source_scope"))
        if policy is not None:
            install_scope(db, policy)
        measured_holdings = holdings(db)
        measured_holdings["inventory_scope"] = "all-custody; includes supporting evidence, excludes receipt-only"
        measured_counts = {
            "files": db.execute("SELECT count(*) FROM main.files").fetchone()[0],
            "collections": db.execute("SELECT count(*) FROM main.collections").fetchone()[0],
        }
        db.commit()
        active_holdings = None
        if policy is not None:
            with closing(connect(candidate, readonly=True, scope="active")) as scoped:
                active_holdings = holdings(scoped)
            active_holdings["inventory_scope"] = "active; applies reviewed source scope and usable-status filters"
        quick_check_tables = ("collections", "routes", "metadata", "tables", "lineage")
        foreign_key_tables = ("routes", "tables", "lineage")
        if include_acquisitions:
            quick_check_tables += ('files', 'objects', 'raw_objects', 'raw_periods', 'inputs', 'evidence')
            foreign_key_tables += ('files', 'raw_objects', 'raw_periods', 'evidence')
        check_identities({index: source_identity}, "Base catalog changed during metadata refresh")
        check_inventory()
        generation = "sha256:" + stable("source-catalog-metadata-refresh", 1, expected_generation, inputs.pins,
                                        changed, classification, publication, delta)
        report = {"format": "spicy-source-metadata-refresh", "version": 1, "status": "validated",
                  "baseGeneration": expected_generation, "generation": generation, "copyMethod": method,
                  "ancestry": list(dict.fromkeys([*source_metadata.get('refresh_ancestry', []), expected_generation])),
                  "phaseInputs": inputs.pins, "configuredMetadata": changed,
                  "retainedClassification": classification, "rawInventory": raw,
                  "raw_archive": measured_holdings, **measured_counts,
                  "validation": {"sqliteQuickCheckTables": list(quick_check_tables),
                                 "foreignKeyCheckTables": list(foreign_key_tables),
                                 "configuredReferencesEnforced": True, "fullInheritedIndexScan": False},
                  "scope": "Metadata refresh over unchanged raw holdings; no acquisition, full raw audit or deployment."}
        if frozen is not None:
            added = check_inventory()
            report['freshness'] = {'status': 'pending-catch-up' if added else 'current', 'addedReceiptKeys': added}
            report['receiptSnapshot'] = {'path': str(plain(receipt_snapshot)), 'sha256': receipt_snapshot_sha256,
                                         'qualificationWork': frozen['work']}
        if delta is not None:
            report.update(format='spicy-source-input-refresh', inputRefresh=delta,
                          scope='Retained base inventory plus committed receipt additions and named content refreshes; no downloads or full archive audit.')
            report['validation']['fullInheritedIndexScan'] = True
        if active_holdings is not None:
            report["active_holdings"] = active_holdings
        if publication is not None:
            report["publication"] = publication
            db.execute("INSERT OR REPLACE INTO metadata VALUES('publication_reconciliation',?)", (encoded({
                "format": "spicy-publication-reconciliation-result", "version": 1, "generation": generation,
                "phaseInputs": inputs.pins, **publication,
            }),))
        for key, value in (("generation", generation), ("generatedAt", datetime.now(UTC).isoformat()),
                           ("reconciliation", report), ('refresh_ancestry', report['ancestry'])):
            db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)", (key, encoded(value)))
        db.commit()
        for table in quick_check_tables:
            require([row[0] for row in db.execute(f"PRAGMA quick_check('{table}')")] == ["ok"],
                    f"Metadata refresh SQLite check failed: {table}")
        for table in foreign_key_tables:
            require(not db.execute(f"PRAGMA foreign_key_check('{table}')").fetchall(),
                    f"Metadata refresh references failed validation: {table}")
        db.close()
        db = None
        descriptor, temporary_report = tempfile.mkstemp(prefix=report_path.name + ".", suffix=".tmp", dir=output.parent)
        report_candidate = Path(temporary_report)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(json.dumps(report, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        with candidate.open("rb") as stream:
            os.fsync(stream.fileno())
        inputs.check()
        check_identities({index: source_identity}, "Base catalog changed during metadata refresh")
        check_inventory()
        # Exclusive links publish only new candidate paths. A concurrent output
        # creator is never overwritten; selected files remain untouched.
        for temporary_path, destination in ((candidate, output), (report_candidate, report_path)):
            os.link(temporary_path, destination)
            installed.append(destination)
        directory = os.open(output.parent, os.O_RDONLY)
        try:
            try:
                os.fsync(directory)
            except OSError as error:
                if error.errno not in {errno.EINVAL, errno.ENOTSUP}:
                    raise
        finally:
            os.close(directory)
        return report
    except BaseException:
        for path in installed:
            path.unlink()
        raise
    finally:
        if db is not None:
            db.close()
        source.close()
        if candidate is not None:
            candidate.unlink(missing_ok=True)
        if report_candidate is not None:
            report_candidate.unlink(missing_ok=True)
