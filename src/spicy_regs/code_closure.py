"""The code a process loaded, recorded so a sealed preparation can be rebound to a later main by what it ran.

A prepared conversion is sealed in the process that ran its writer, admission and restoration. :func:`record` keeps
the bytes of every first-party module in ``sys.modules`` (SpicyRegs, SpicyDocs, Rulespec artifacts), the bytes of
every first-party data file whether or not it was read (DuckDB and pyarrow read package files Python never sees, and
a cached read made before recording would be invisible to any tracer), the versions of every third-party distribution
loaded, Python and the platform. :func:`differences` re-hashes the same set in the current process.

``generations.spicy_docs_code`` digests the whole SpicyDocs package rather than the modules a caller imports,
"because a missed transitive module would leave stale rows silently". ``sys.modules`` holds every module imported,
transitively included, so no imported module is missed here; code run without import (a file exec'd, a subprocess)
would be, and no conversion path does that. On 2026-10-30 a comment-attributes seal loaded 67 SpicyRegs and 40
SpicyDocs modules (fixture conversion, measured in ss-5tt8), against 212 and 134 before the transforms facade and the
rollup scan were removed from that path.
"""

from __future__ import annotations

import hashlib
import importlib
import platform
import sys
from collections.abc import Mapping
from importlib.metadata import PackageNotFoundError, packages_distributions, version
from pathlib import Path

#: First-party packages whose bytes are recorded; every other loaded package is recorded by distribution version.
ROOTS = ("spicy_regs", "spicy_docs", "rulespec_artifacts")


def _root(package: str) -> Path:
    return Path(str(importlib.import_module(package).__file__)).parent.resolve()


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def is_data_file(path: Path) -> bool:
    """Whether ``path`` is a non-Python data file in an installed package tree."""
    return path.is_file() and path.suffix not in (".py", ".pyc") and "__pycache__" not in path.parts


def _data_files() -> dict[str, str]:
    files = {}
    for package in ROOTS:
        root = _root(package)
        for path in sorted(root.rglob("*")):
            if is_data_file(path):
                files[f"{package}/{path.relative_to(root).as_posix()}"] = _digest(path)
    return files


def _runtime() -> dict:
    return {"python": sys.version, "platform": {"system": platform.system(), "machine": platform.machine()}}


def record() -> dict:
    """The loaded first-party module bytes, all first-party data bytes, loaded distributions, Python and platform."""
    modules, tops = {}, set()
    for name, module in list(sys.modules.items()):
        top = name.partition(".")[0]
        tops.add(top)
        path = getattr(module, "__file__", None)
        if top in ROOTS and path:
            source = Path(path).resolve()
            modules[f"{top}/{source.relative_to(_root(top)).as_posix()}"] = _digest(source)
    owners = packages_distributions()
    distributions = {name: version(name) for top in sorted(tops - set(ROOTS)) for name in owners.get(top, ())}
    return {"modules": dict(sorted(modules.items())), "data": _data_files(),
            "distributions": dict(sorted(distributions.items())), **_runtime()}


def differences(recorded: Mapping) -> list[str]:
    """What differs between ``recorded`` and this process's installed code, named; empty when byte-identical."""
    found = []
    for key, digest in recorded["modules"].items():
        package, _, relative = key.partition("/")
        path = _root(package) / relative
        if not path.is_file() or _digest(path) != digest:
            found.append(f"module {key}")
    data = _data_files()
    found += [f"data {key}" for key in sorted(data.keys() | recorded["data"].keys())
              if data.get(key) != recorded["data"].get(key)]
    for name, held in recorded["distributions"].items():
        try:
            current = version(name)
        except PackageNotFoundError:
            current = None
        if current != held:
            found.append(f"distribution {name} {held} -> {current}")
    found += [f"runtime {key}" for key, value in _runtime().items() if recorded.get(key) != value]
    return found
