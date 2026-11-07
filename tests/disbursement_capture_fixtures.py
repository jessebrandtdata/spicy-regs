"""Relocate retained capture fixtures without changing their evidence bytes."""

import json
from pathlib import Path

from spicy_regs.pipelines.congressional_disbursement_pages import pin


def selected_capture(fixture, tmp_path):
    source = json.loads((fixture / "selection.json").read_text())["files"][0]
    item = source["structured_observations"][0]
    for key in ("result", "image", "page_record", "request", "response", "plan", "qualification"):
        artifact = item[key]
        artifact["path"] = str((fixture / artifact["sha256"].removeprefix("sha256:")).resolve())
    for artifact in item.get("evidence", []):
        artifact["path"] = str((fixture / artifact["sha256"].removeprefix("sha256:")).resolve())
    qualification = json.loads(Path(item["qualification"]["path"]).read_text())
    for artifact in qualification["review_evidence"] + [
        a for rule in qualification["corrections"] for a in rule["review_evidence"]
    ]:
        artifact["path"] = str((fixture / artifact["sha256"].removeprefix("sha256:")).resolve())
    target = tmp_path / "qualification.json"
    target.write_text(json.dumps(qualification))
    item["qualification"] = pin(target)
    return source, item
