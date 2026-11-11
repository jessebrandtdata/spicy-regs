"""A native input refusal must happen before any potentially large downloads."""

from copy import deepcopy
import sys

import pytest

from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecard_analysis import (
    SOURCE_TABLES, OFFICIAL_TABLES, COMPARISON_INPUTS, analysis_input_entries,
)
from tests.test_scorecard_analysis import _inputs, _serve

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def fixture_owner(index, key):
    selected = publication.table_owner(index, key)
    assert selected is not None
    owner = selected[1]
    assert isinstance(owner, dict)
    return owner


@pytest.mark.parametrize("name", (*SOURCE_TABLES, *OFFICIAL_TABLES, *COMPARISON_INPUTS))
def test_missing_native_input_refuses_before_any_subject_or_receipt_download(tmp_path, monkeypatch, name):
    store, index = _inputs(tmp_path)
    owner = fixture_owner(index, name + ".parquet")
    owner["etlReceipts"]["datasets"].remove(name)
    calls = _serve(monkeypatch, store)
    output = tmp_path / "refused"
    output.mkdir()
    rollup = ScorecardAnalysisRollup()
    with pytest.raises(publication.PublicationError, match="requires native receipts for:.*" + name):
        rollup._prime(output, index)
    assert calls == []
    assert list(output.iterdir()) == []
    assert not hasattr(rollup, "_scorecard_input_paths")


def test_mixed_scorecard_generations_refuse_before_any_download(tmp_path, monkeypatch):
    store, index = _inputs(tmp_path)
    source = index["families"]["scorecards"]
    split = deepcopy(source)
    split["tables"] = {"scorecard_items.parquet": source["tables"].pop("scorecard_items.parquet")}
    split["artifactDigest"] = "sha256:" + "1" * 64
    index["families"]["different-scorecard-generation"] = split
    calls = _serve(monkeypatch, store)
    with pytest.raises(publication.PublicationError, match="source inputs must share one generation"):
        ScorecardAnalysisRollup()._prime(tmp_path, index)
    assert calls == []


def test_preflight_requires_a_selected_receipt_generation(tmp_path):
    _, index = _inputs(tmp_path)
    fixture_owner(index, "members.parquet")["etlReceipts"]["generationId"] = ""
    with pytest.raises(publication.PublicationError, match="selected native receipt generation: members"):
        analysis_input_entries(index)


def test_preparation_script_uses_same_preflight_before_object_download(tmp_path, monkeypatch):
    from spicy_regs.scorecards.operations import analysis as module
    _, index = _inputs(tmp_path)
    fixture_owner(index, "members.parquet").pop("etlReceipts")
    client = object()
    monkeypatch.setattr(module.r2, "get_r2_client", lambda: client)
    monkeypatch.setattr(module.publication, "_stored_index", lambda *args: (index, None))
    monkeypatch.setattr(module, "load_dotenv", lambda *args: None)
    monkeypatch.setattr(sys, "argv", ["prepare_analysis.py", "--output", str(tmp_path / "refused")])
    with pytest.raises(publication.PublicationError, match="requires native receipts for: members, member_terms"):
        module.main()
    assert list((tmp_path / "refused").iterdir()) == []
