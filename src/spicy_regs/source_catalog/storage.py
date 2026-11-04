"""Disposable SQLite catalog and pinned durable input readers."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

VERSION = 1


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def stable(*parts: object) -> str:
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


def connect(path: Path, *, readonly: bool = False, scope: str = "active", cache_mib: int = 256) -> sqlite3.Connection:
    # Match the importer budget for readers too: the SQLite default cache is
    # too small for the catalog's identifier indexes and causes repeated I/O.
    # This is a per-connection ceiling, allocated only as pages are touched.
    if isinstance(cache_mib, bool) or not isinstance(cache_mib, int) or cache_mib < 1:
        raise ValueError("Catalog page cache must be a positive integer in MiB")
    db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) if readonly else sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute(f"PRAGMA cache_size=-{cache_mib * 1024}")
    if readonly:
        from .scope import select_scope

        try:
            select_scope(db, scope)
        except BaseException:
            db.close()
            raise
    return db


def schema(db: sqlite3.Connection) -> None:
    db.executescript("""
    PRAGMA user_version=1;
    CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE collections(id TEXT PRIMARY KEY, title TEXT NOT NULL, data TEXT NOT NULL);
    CREATE TABLE routes(id TEXT PRIMARY KEY, collection_id TEXT REFERENCES collections, data TEXT NOT NULL);
    CREATE TABLE objects(sha256 TEXT PRIMARY KEY, bytes INTEGER NOT NULL);
    CREATE TABLE files(id TEXT PRIMARY KEY, collection_id TEXT REFERENCES collections, route_id TEXT,
      name TEXT NOT NULL, status TEXT NOT NULL, sha256 TEXT REFERENCES objects, bytes INTEGER,
      local_path TEXT, data TEXT NOT NULL);
    CREATE INDEX files_collection ON files(collection_id,id);
    CREATE INDEX files_holdings ON files(collection_id,status,sha256,id);
    CREATE INDEX files_sha ON files(sha256);
    CREATE TABLE evidence(id TEXT PRIMARY KEY, collection_id TEXT REFERENCES collections, kind TEXT, data TEXT NOT NULL);
    CREATE INDEX evidence_collection ON evidence(collection_id,kind);
    CREATE TABLE tables(id TEXT, collection_id TEXT REFERENCES collections, family TEXT, data TEXT NOT NULL,
      PRIMARY KEY(id,collection_id));
    CREATE TABLE lineage(id TEXT PRIMARY KEY, collection_id TEXT REFERENCES collections, data TEXT NOT NULL);
    CREATE TABLE inputs(path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL);
    """)


def summary_index(db: sqlite3.Connection) -> bool:
    """Migrate old candidates once; retain the separate ordered-export index."""
    present = db.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name='files_holdings'").fetchone()
    db.execute('CREATE INDEX IF NOT EXISTS files_holdings ON files(collection_id,status,sha256,id)')
    return present is None


class Inputs:
    """Pin each read before parsing and check the same bytes before activation."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.pins: dict[str, dict] = {}

    def pin(self, path: Path) -> Path:
        path = path.resolve()
        if str(path) not in self.pins:
            self.pins[str(path)] = {"sha256": digest(path), "bytes": path.stat().st_size}
        return path

    def json(self, path: Path) -> Any:
        path = self.pin(path)
        with gzip.open(path, "rt") if path.suffix == ".gz" else path.open() as stream:
            return json.load(stream)

    def check(self) -> None:
        for path, pin in self.pins.items():
            if digest(Path(path)) != pin["sha256"]:
                raise RuntimeError(f"Input changed during build: {path}")


def stored_path(root: Path, reference: str, sha: str | None = None) -> Path | None:
    """Resolve original or gzip storage; match the retained corpus object reader."""
    paths = [root / reference]
    if sha:
        identity = sha.removeprefix("sha256:")
        paths.append(root / "raw/objects/sha256" / identity[:2] / identity)
        paths.append(root / "source-evidence/blobs/sha256" / identity)
    for path in paths:
        for candidate in (path, path.with_suffix(path.suffix + ".gz")):
            if candidate.is_file() and not candidate.is_symlink():
                return candidate
    return None


def array_items(inputs: Inputs, path: Path, key: str):
    """Parse one top-level array with strict separators and whole-document validation."""
    path = inputs.pin(path)
    decoder = json.JSONDecoder()
    with gzip.open(path, "rt") if path.suffix == ".gz" else path.open() as stream:
        buffer = ""
        ended = False

        def fill():
            nonlocal buffer, ended
            chunk = stream.read(65536)
            buffer += chunk
            ended = not chunk

        def trim():
            nonlocal buffer
            while True:
                buffer = buffer.lstrip()
                if buffer or ended:
                    return
                fill()

        def character(expected):
            nonlocal buffer
            trim()
            if not buffer.startswith(expected):
                raise ValueError(f"Expected {expected!r} in {path}")
            buffer = buffer[len(expected) :]

        def value():
            nonlocal buffer
            trim()
            while True:
                try:
                    result, end = decoder.raw_decode(buffer)
                except json.JSONDecodeError:
                    if ended:
                        raise ValueError(f"Malformed JSON in {path}") from None
                    fill()
                    continue
                if end == len(buffer) and not ended:
                    fill()
                    continue
                buffer = buffer[end:]
                return result

        character("{")
        keys = set()
        found = False
        trim()
        if buffer.startswith("}"):
            character("}")
        else:
            while True:
                name = value()
                if not isinstance(name, str) or name in keys:
                    raise ValueError(f"Invalid or duplicate root key in {path}")
                keys.add(name)
                character(":")
                if name == key:
                    found = True
                    character("[")
                    trim()
                    if buffer.startswith("]"):
                        character("]")
                    else:
                        while True:
                            yield value()
                            trim()
                            if buffer.startswith("]"):
                                character("]")
                                break
                            character(",")
                else:
                    value()
                trim()
                if buffer.startswith("}"):
                    character("}")
                    break
                character(",")
        trim()
        if buffer or not ended:
            raise ValueError(f"Trailing content in {path}")
        if not found:
            raise ValueError(f"Missing top-level {key} array in {path}")
