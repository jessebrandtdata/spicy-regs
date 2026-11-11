"""Incremental-processing manifest: which source keys have we already seen?

A :class:`Manifest` is the one piece of state that makes the ETL incremental.
It plays two composable roles:

* **Membership test** for a :class:`~spicy_regs.sources.base.Reader` — passed as
  ``processed_keys``, it answers ``key in manifest`` so already-processed files
  are skipped.
* **Accumulator** — :meth:`record` collects the keys seen this run (thread-safe,
  so agencies can be processed in parallel) and :meth:`save` appends them back to
  the persisted manifest.

Membership is backed by a Bloom filter loaded from ``manifest.parquet`` (fetched
from R2 when not present locally; with neither, :class:`MissingManifestError`
unless the caller allows a fresh start). Bloom hashing is deterministic, so a false
positive is *sticky*: the same never-seen key tests positive on every run and is
skipped until a ``--full-refresh`` rebuilds the filter. At the 1e-7 rate over
~30M keys the expected count is a handful of keys total — an accepted tradeoff
for the memory saving over an exact set, not "picked up next run".
"""

from collections.abc import Container, Iterable
from hashlib import md5, sha1
from math import log
from pathlib import Path
from threading import Lock
from time import monotonic

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources.r2 import download_from_r2

MANIFEST_FILE = "manifest.parquet"
#: The persisted manifest's one column, written by :func:`save_manifest`. The first manifest was
#: seeded once (2026-09-24) from the ids the published base tables held.
MANIFEST_SCHEMA = pa.schema([("key", pa.large_string())])


class MissingManifestError(RuntimeError):
    """No manifest locally or on R2, and the caller did not allow a fresh start.

    An empty manifest makes every key in the mirror new. On the fork that
    happened silently: each scheduled batch logged "starting fresh", re-read its
    agencies' whole history at ~280 keys/s and was cancelled at 60 minutes with
    nothing persisted (run 35828391248, 2026-09-23). So a run starts empty only
    when it asks to (``allow_fresh_start``).
    """


# ---------------------------------------------------------------------------
# Bloom filter — stdlib-only, about 126 MB for 30M keys at 1e-7 FP rate.
#
# A false positive skips a genuinely new file *permanently*: the hashes are
# deterministic, so the same key collides on every run — it is not "picked up
# next run", only by a ``--full-refresh`` that rebuilds the filter. At 1e-7 over
# ~30M keys the expected count is ~3 keys total, the accepted cost of replacing a
# Python set with a compact bit array sized by the usual Bloom-filter formula.
# ---------------------------------------------------------------------------


class BloomFilter:
    """Memory-efficient probabilistic set membership using a bit array."""

    __slots__ = ("_bits", "_nbits", "_k")

    def __init__(self, capacity: int, fp_rate: float = 1e-7) -> None:
        if capacity < 1 or not 0 < fp_rate < 1:
            raise ValueError("Bloom capacity must be positive and false-positive rate must be in (0, 1)")
        self._nbits = max(1, int(-capacity * log(fp_rate) / (log(2) ** 2)))
        self._k = max(1, int((self._nbits / capacity) * log(2)))
        # bytearray has the same width on every platform; unsigned long does not.
        self._bits = bytearray((self._nbits + 7) // 8)

    def _hashes(self, key: str) -> list[int]:
        kb = key.encode()
        h1 = int.from_bytes(md5(kb).digest()[:8], "little")
        h2 = int.from_bytes(sha1(kb).digest()[:8], "little")
        return [(h1 + i * h2) % self._nbits for i in range(self._k)]

    def add(self, key: str) -> None:
        for pos in self._hashes(key):
            self._bits[pos >> 3] |= 1 << (pos & 7)

    def __contains__(self, key: str) -> bool:
        for pos in self._hashes(key):
            if not (self._bits[pos >> 3] & (1 << (pos & 7))):
                return False
        return True

    @property
    def size_bytes(self) -> int:
        return len(self._bits)


def save_manifest(output_dir: Path, new_keys: set[str]) -> None:
    """Append new keys to the existing manifest Parquet file.

    Reads the old manifest (if any) in streaming batches, writes those
    plus the new keys to a temp file, then replaces the original.
    This avoids loading the full 27M-key manifest into memory.
    """
    manifest_file = output_dir / MANIFEST_FILE
    temp_file = output_dir / "manifest_new.parquet"

    existing_rows = 0
    with pq.ParquetWriter(temp_file, MANIFEST_SCHEMA, compression="zstd") as writer:
        # Stream existing manifest rows
        if manifest_file.exists():
            pf = pq.ParquetFile(manifest_file)
            for batch in pf.iter_batches(batch_size=500_000, columns=["key"]):
                table = pa.Table.from_batches([batch]).cast(MANIFEST_SCHEMA)
                writer.write_table(table)
                existing_rows += batch.num_rows

        # Append new keys
        new_table = pa.table({"key": list(new_keys)}).cast(MANIFEST_SCHEMA)
        writer.write_table(new_table)

    temp_file.replace(manifest_file)

    total = existing_rows + len(new_keys)
    logger.info("Saved manifest: {:,} keys ({:,} existing + {:,} new)", total, existing_rows, len(new_keys))


class Manifest:
    """Tracks processed source keys for incremental ETL runs.

    ``processed`` is anything supporting ``key in processed`` (a Bloom filter
    for a loaded manifest, or a set for the empty/bootstrap case).
    """

    def __init__(self, processed: Container[str]) -> None:
        self._processed = processed
        self._new_keys: set[str] = set()
        self._committed_keys: set[str] = set()
        self._lock = Lock()

    @classmethod
    def empty(cls) -> "Manifest":
        """A manifest that has seen nothing — every key is treated as new."""
        return cls(set())

    @classmethod
    def load(cls, output_dir: Path, *, allow_fresh_start: bool = False) -> "Manifest":
        """Build from ``manifest.parquet`` (fetched from R2 if not local).

        With none available (first run, R2 not configured, or never seeded) this
        raises :class:`MissingManifestError` unless ``allow_fresh_start`` asks for
        an empty manifest.
        """
        manifest_file = output_dir / MANIFEST_FILE
        if not manifest_file.exists() and not download_from_r2(MANIFEST_FILE, manifest_file):
            if not allow_fresh_start:
                raise MissingManifestError(
                    f"No {MANIFEST_FILE} in {output_dir} or on R2: every source key would count as new. "
                    "Restore its retained acquisition state, or allow a fresh start "
                    "(--allow-fresh-start) for a deliberate bootstrap."
                )
            logger.warning("No manifest found — fresh start allowed: every source key is new")
            return cls.empty()

        pf = pq.ParquetFile(manifest_file)
        key_count = pf.metadata.num_rows
        # Size the filter to the keys actually loaded (with modest headroom);
        # new keys from this run are tracked separately in ``_new_keys``.
        bloom = BloomFilter(capacity=max(key_count + key_count // 10, 1000))
        logger.info("Loading manifest: {:,} source keys", key_count)
        loaded = 0
        reported = monotonic()
        for batch in pf.iter_batches(batch_size=500_000, columns=["key"]):
            for key in batch.column("key").to_pylist():
                bloom.add(key)
            loaded += batch.num_rows
            if monotonic() - reported >= 30:
                logger.info("Loading manifest: {:,}/{:,} source keys", loaded, key_count)
                reported = monotonic()
        logger.info("Loaded manifest: {:,} keys (~{:.0f} MB)", key_count, bloom.size_bytes / 1_048_576)
        return cls(bloom)

    @property
    def inherits_nothing(self) -> bool:
        """True for a full refresh or fresh start: no prior run's keys are skipped."""
        return isinstance(self._processed, (set, frozenset)) and not self._processed

    def __contains__(self, key: str) -> bool:
        return key in self._new_keys or key in self._committed_keys or key in self._processed

    def record(self, keys: Iterable[str]) -> None:
        """Mark ``keys`` as processed this run (thread-safe)."""
        with self._lock:
            self._new_keys.update(key for key in keys if key not in self)

    @property
    def recorded(self) -> int:
        """Keys recorded since load, saved or not: unchanged across a pass means it retired nothing."""
        with self._lock:
            return len(self._new_keys) + len(self._committed_keys)

    @property
    def new_keys(self) -> set[str]:
        """Keys awaiting commit (a copy, safe to iterate)."""
        with self._lock:
            return set(self._new_keys)

    def save(self, output_dir: Path) -> None:
        """Commit pending keys once, retaining membership for later local batches.

        An exact set tracks keys added after load without filling the Bloom
        filter beyond its sized capacity. Failed writes keep their pending keys.
        """
        with self._lock:
            if self._new_keys:
                save_manifest(output_dir, self._new_keys)
                self._committed_keys.update(self._new_keys)
                self._new_keys.clear()
