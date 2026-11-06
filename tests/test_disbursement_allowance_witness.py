"""Retained native lines distinguish two amounts inside one original table cell."""

from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from spicy_regs.pipelines.disbursement_page_profiles import validate_page_content

FIXTURES = Path(__file__).parent / "fixtures/disbursement_summaries"


def _page():
    provenance = json.loads((FIXTURES / "allowance-merged-page-provenance.json").read_text())
    body = (FIXTURES / provenance["fixture"]).read_bytes()
    assert "sha256:" + hashlib.sha256(body).hexdigest() == provenance["fixture_sha256"]
    return json.loads(gzip.decompress(body))


def test_allowance_host_accepts_distinct_native_amounts_without_inventing_a_border():
    page = _page()
    before = deepcopy(page)
    validate_page_content(page, "allowance-summary")
    assert page == before
    assert len(page["records"]) == 31
    assert page["tables"][0]["cells"][9][3] == "$1,518,721.00\n$884,492.00"
    assert page["tables"][0]["cells"][10][3] is None
    assert page["tables"][0]["cell_boxes"][10][3] is None
    assert [page["records"][i]["values"]["allowance_amount"] for i in (9, 10)] == [
        "1518721.00",
        "884492.00",
    ]
    assert [page["records"][i]["cell_origins"]["allowance_amount"]["native_block"] for i in (9, 10)] == [
        42,
        46,
    ]


@pytest.mark.parametrize(
    "failure", ["wrong_cent", "reused_witness", "invented_border", "duplicate_row", "missing_witness"]
)
def test_allowance_host_rejects_wrong_merged_amount_associations(failure):
    page = _page()
    row = page["records"][10]
    if failure == "wrong_cent":
        row["values"]["allowance_amount"] = "884492.01"
    elif failure == "reused_witness":
        row["cell_origins"]["allowance_amount"]["native_block"] = 42
    elif failure == "invented_border":
        table = page["tables"][0]
        table["cell_boxes"][10][3] = {
            **table["cell_boxes"][9][3],
            "y0": table["cell_boxes"][10][0]["y0"],
            "y1": table["cell_boxes"][10][0]["y1"],
        }
    elif failure == "duplicate_row":
        page["records"].insert(11, deepcopy(row))
    else:
        row.pop("cell_origins")
    with pytest.raises(ValueError):
        validate_page_content(page, "allowance-summary")
