"""Locate the maintained dictionary inputs in a checkout or an installed package."""
from pathlib import Path

INPUTS = Path(__file__).resolve().parent
_project = INPUTS.parents[2]
if INPUTS.parent == _project / "src" / "spicy_regs" and (_project / "pyproject.toml").is_file():
    INPUTS = _project / "data_dictionary"
