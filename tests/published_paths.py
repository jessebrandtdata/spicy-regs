"""Find local absolute paths in what a publish would upload: a generation and its evidence artifacts."""

import json
from pathlib import Path

import pyarrow.parquet as pq

LOCAL_ROOTS = ("/Users/", "/private/", "/home/", "/tmp/")


def local_paths(*directories: Path, also: tuple[str, ...] = ()) -> list[tuple[str, str]]:
    """Each (file, marker) where a published file names a local path.

    Parquet is decoded first, because a compressed page hides its strings from a byte search. Retained
    source blobs are the publisher's own bytes and are not read.
    """
    found = []
    for directory in directories:
        for path in sorted(item for item in directory.rglob("*") if item.is_file()):
            relative = path.relative_to(directory)
            if relative.parts[0] == "blobs":
                continue
            if path.suffix == ".parquet":
                table = pq.read_table(path)
                text = json.dumps([table.to_pylist(), str(table.schema.metadata)], default=str)
            else:
                text = path.read_text()
            found += [(str(relative), marker) for marker in (*LOCAL_ROOTS, *also) if marker in text]
    return found
