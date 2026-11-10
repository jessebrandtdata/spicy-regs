"""Find local paths in what a publish would upload: generations, evidence artifacts and input proofs (ss-81bv)."""

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

LOCAL_ROOTS = ("/Users/", "/private/", "/home/", "/tmp/")
#: A receipt table up to this size is read whole; above it, and for every other table, only its key-value metadata.
#: 0 for now: receipts still name build folders (legislative file states' ``source_path``, carried witness URIs),
#: which the evidence fix of ss-81bv does not change. Raise it to 8 MiB with the receipts fix.
WHOLE_RECEIPTS = 0


def _text(path: Path, *, whole: bool = True) -> str:
    """What a published file says; Parquet is decoded first, as a compressed page hides strings from a byte search."""
    if path.suffix == ".parquet":
        try:
            if whole:
                table = pq.read_table(path)
                return json.dumps([table.to_pylist(), str(table.schema.metadata)], default=str)
            return str(pq.read_metadata(path).metadata)
        except (pa.ArrowInvalid, OSError):
            pass  # A test's deliberately damaged table: its bytes are all there is to read.
    # The builtin open, not Path.open: a test may still have Path.open patched when this guard runs.
    with open(path, "rb") as stream:
        return stream.read().decode(errors="replace")


def local_paths(*directories: Path, also: tuple[str, ...] = ()) -> list[tuple[str, str]]:
    """Each (file, marker) where a published file names a local path.

    Retained source blobs are the publisher's own bytes and are not read.
    """
    found = []
    for directory in directories:
        for path in sorted(item for item in directory.rglob("*") if item.is_file()):
            relative = path.relative_to(directory)
            if relative.parts[0] == "blobs":
                continue
            text = _text(path)
            found += [(str(relative), marker) for marker in (*LOCAL_ROOTS, *also) if marker in text]
    return found


def published_files(root: Path) -> Iterator[tuple[Path, bool]]:
    """Each file under ``root`` that a publish uploads and that can name a path, and whether Parquet is read whole.

    An evidence artifact's journal, root and member manifest, and the JSON inputs its build retained for itself
    (``blobs/sha256``; a central-raw original is the publisher's and lives elsewhere); a generation's root, member
    manifest and Parquet members; and an input proof under ``generation-inputs/``.
    """
    for directory, _, names in os.walk(root):
        here = Path(directory)
        if "journal.jsonl" in names:
            controls = ("journal.jsonl", "artifact.json", "members.json")
            yield from ((here / name, False) for name in controls if name in names)
            blobs = here / "blobs" / "sha256"
            for blob in sorted(blobs.iterdir()) if blobs.is_dir() else ():
                with open(blob, "rb") as stream:
                    if stream.read(64).lstrip()[:1] in (b"{", b"["):
                        yield blob, False
        elif "artifact.json" in names and "members.json" in names:
            yield here / "artifact.json", False
            yield here / "members.json", False
            for table in sorted(here.rglob("*.parquet")):
                yield table, table.name == "etl_receipts.parquet" and table.stat().st_size <= WHOLE_RECEIPTS
        if "inputs.json" in names and "generation-inputs" in here.parts:
            yield here / "inputs.json", False


def machine_paths(tmp_path: Path) -> dict[str, str]:
    """This run's own folders by label: the test's temporary folder (as given and resolved), the checkout, home."""
    folders = {
        "temporary folder": str(tmp_path),
        "resolved temporary folder": str(tmp_path.resolve()),
        "checkout": str(Path(__file__).resolve().parents[1]),
        "home folder": str(Path.home()),
    }
    if folders["resolved temporary folder"].startswith("/private/"):
        folders["temporary folder without /private"] = folders["resolved temporary folder"].removeprefix("/private")
    # A home of "/" or "/root" in a container would match every path-shaped string.
    return {label: folder for label, folder in folders.items() if len(Path(folder).parts) >= 3}


def published_machine_paths(root: Path, folders: dict[str, str]) -> list[tuple[str, str]]:
    """Each (published file, folder label) where what a publish uploads from ``root`` names one of ``folders``.

    Labels, not the folders themselves, so a failure message never repeats a home folder.
    """
    found = []
    for path, whole in published_files(root):
        text = _text(path, whole=whole)
        found += [(str(path.relative_to(root)), label) for label, folder in folders.items() if folder in text]
    return found
