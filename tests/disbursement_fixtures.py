"""Use retained PDF excerpts through real acquisition and page-reading stages."""

import json

import httpx

from spicy_regs.pipelines.congressional_disbursement_pages import read_pages
from spicy_regs.pipelines.congressional_disbursements import acquire_selection


def read_pdf_fixtures(tmp_path, chamber, sources, *, profile="ledger"):
    originals = {source["url"]: source["fixture"].read_bytes() for source in sources}
    selection = {
        "schema": "congressional-disbursement-selection/1",
        "files": [
            {"chamber": chamber, "format": "pdf", "kind": "full", **{k: v for k, v in source.items() if k != "fixture"}}
            for source in sources
        ],
    }
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(selection))
    acquisition = acquire_selection(
        path,
        tmp_path / "acquired",
        archive_root=tmp_path / "raw",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, content=originals[str(request.url)], headers={"Content-Type": "application/pdf"}
            )
        ),
    )
    qualified = tmp_path / "qualified.json"
    qualified.write_text(json.dumps({**selection, "files": acquisition["files"]}))
    read_pages(qualified, tmp_path / "reading", chamber=chamber, archive_root=tmp_path / "raw", profile=profile)
    return tmp_path / "reading/manifest.json"
