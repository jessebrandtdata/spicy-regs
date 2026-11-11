"""The four regulatory summaries build and publish without receipts; their generation pins the parents it read.

ss-nxew: hosted dry run 42727716619 built ``discovery_signals`` and admission then refused it ("Registered datasets
require ETL receipts"), because 64141879 installed a field policy for each summary while its rollup writes no
receipts. Each is rebuilt whole from published parents, so no policy is installed for them
(``regulations_receipts.PARENT_PINNED_SUMMARIES``).

What these cannot see: the hosted runner, the bucket and the live index. The parents are fixtures of a few rows held
as local files, so no parent here is a managed family's pin and ``_assert_parents_current`` has nothing to hold. The
prior family is the fake store's own first publication, which has the live entries' shape (one table, no
``etlReceipts``) but not their sizes, so the shrink guard is not exercised against the live bytes.
"""

import hashlib
import importlib
import pkgutil
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import spicy_regs.pipelines.rollups as rollups
from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.agency_monthly_volume import AgencyMonthlyVolumeRollup
from spicy_regs.pipelines.rollups.agency_stats import AgencyStatsRollup
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.pipelines.rollups.discovery_signals import DiscoverySignalsRollup
from spicy_regs.pipelines.rollups.feed_summary import FeedSummaryRollup
from spicy_regs.pipelines.rollups.regulatory_base import _BaseTableFamily
from spicy_regs.sources import publication as pub
from spicy_regs.sources import r2
from spicy_regs.transforms.regulations_receipts import PARENT_PINNED_SUMMARIES
from tests.generation_fakes import Store
from tests.test_removed_postings import _inputs, _spike_documents
from tests.test_rollup_base import _setup_env

BASE_TABLES = {"dockets.parquet", "comments_index.parquet", "documents.parquet"}
#: Each summary, the rows the fixture gives it, and the base tables it reads. The fixture is FNA's two dockets
#: (tests/test_removed_postings.py): one agency, two dockets, two document types in one month; the spike fixture
#: gives discovery its one agency.
SUMMARIES = [
    (AgencyStatsRollup, 1, BASE_TABLES),
    (FeedSummaryRollup, 2, BASE_TABLES),
    (AgencyMonthlyVolumeRollup, 2, {"documents.parquet"}),
    (DiscoverySignalsRollup, 1, {"documents.parquet"}),
]
each_summary = pytest.mark.parametrize(("rollup", "rows", "base_tables"), SUMMARIES, ids=[s[0].name for s in SUMMARIES])


def _parents(path, rollup, *, status=True):
    path.mkdir(parents=True, exist_ok=True)
    _inputs(path, status=status)
    if rollup is DiscoverySignalsRollup:
        _spike_documents(path)


def _sha256(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


@each_summary
def test_a_dry_run_builds_a_generation_admission_accepts(tmp_path, rollup, rows, base_tables):
    """The hosted dry run's path: upload skipped, the table built, then ``build_generation`` admits it."""
    _parents(tmp_path, rollup)
    rollup(output_dir=tmp_path, skip_upload=True).run()

    [generation] = (tmp_path / "generations").iterdir()
    spec = verify_generation(generation).root["spec"]
    assert (spec["family"], spec["publicationStatus"]) == (rollup.name, "complete-family")
    assert {key: table["rows"] for key, table in spec["tables"].items()} == {rollup.output: rows}
    assert "etlReceipts" not in spec
    # Provenance is the parents: each base table read, at the digest of the bytes read, and the comments file the
    # two comment-counting summaries read for a removed posting.
    assert set(spec["parents"]) == base_tables | set(rollup.remote_inputs)
    assert {key: spec["parents"][key]["sha256"] for key in base_tables} == {
        key: _sha256(tmp_path / key) for key in base_tables}


@each_summary
def test_a_summary_publishes_over_a_prior_that_has_no_receipts(tmp_path, monkeypatch, rollup, rows, base_tables):
    """As live: the family holds one table and no ``etlReceipts``, and the next generation replaces it alike."""
    source, store = tmp_path / "published", Store()
    # No removed posting, so no comment is read: the comments file is pinned by its stored version alone.
    _parents(source, rollup, status=False)
    _setup_env(monkeypatch)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(r2, "download", lambda key, path: bool(shutil.copyfile(source / key, path)))
    monkeypatch.setattr(r2, "public_object_version", lambda url: {"etag": '"comments-1"', "bytes": 1})
    monkeypatch.setattr(pub, "load_index", lambda url: pub.parse_index(store.objects[pub.INDEX_V2_KEY])
                        if pub.INDEX_V2_KEY in store.objects else pub.empty_index())

    def publish(directory):
        rollup(output_dir=tmp_path / directory, skip_upload=False).run()
        return pub.parse_index(store.objects[pub.INDEX_V2_KEY])["families"][rollup.name]

    first, second = publish("first"), publish("second")
    assert first["artifactDigest"] != second["artifactDigest"]
    for entry in (first, second):
        assert {key: table["rows"] for key, table in entry["tables"].items()} == {rollup.output: rows}
        assert "etlReceipts" not in entry
    spec = pub.family_root(store.objects[second["prefix"] + "/artifact.json"], second)["spec"]
    assert spec["readSnapshot"]["families"][rollup.name] == first
    assert {key: set(parent) for key, parent in spec["parents"].items()} == {
        **{key: {"sha256", "byteSize"} for key in base_tables},
        **{key: {"etag", "byteSize"} for key in rollup.remote_inputs},
    }


def test_the_summaries_are_the_four_named_and_none_has_an_installed_policy():
    assert PARENT_PINNED_SUMMARIES == {"agency_monthly_volume", "agency_stats", "discovery_signals", "feed_summary"}
    assert {rollup.output.removesuffix(".parquet") for rollup, _, _ in SUMMARIES} == PARENT_PINNED_SUMMARIES
    assert not PARENT_PINNED_SUMMARIES & installed_policies().keys()


#: The base table families, each with the dataset it publishes. They declare no policy: each states it while it
#: builds, assigning it on the class (``_BaseTableFamily.validate_native_output``), so the class attribute of these
#: five differs before and after a build in the same process. Nothing below reads it to recognise them.
BASE_FAMILIES = {
    "dockets": "dockets", "documents": "documents", "docket-attributes": "docket_attributes",
    "document-attributes": "document_attributes", "comment-attributes": "comment_attributes",
}


def _scheduled_rollups():
    """Every rollup class the package defines; a test module's stand-in class is no scheduled writer."""
    for module in pkgutil.iter_modules(rollups.__path__):
        importlib.import_module(f"{rollups.__name__}.{module.name}")

    def subclasses(cls):
        for subclass in cls.__subclasses__():
            yield from (subclass, *subclasses(subclass))

    return {cls for cls in subclasses(RollupPipeline)
            if cls.__module__.startswith(rollups.__name__ + ".") and "name" in vars(cls)}


def test_a_rollup_with_no_receipt_route_publishes_no_table_with_an_installed_policy():
    """The refusal of ss-nxew, held for every scheduled rollup before a hosted run meets it.

    An installed policy makes receipts mandatory at admission, unless the table's receipts are retired (its policy
    is then only its schema declaration). A rollup has a receipt route when its class declares policies, when it
    passes them itself (``generation_kwargs``, the court families), or when it is a base table family, recognised by
    its base class whether or not one has built yet (``BASE_FAMILIES``).
    """
    from spicy_regs.subject_catalog import retired_receipt_datasets

    installed, no_route = installed_policies(), {}
    assert {rollup for rollup, _, _ in SUMMARIES} <= _scheduled_rollups()
    for cls in _scheduled_rollups():
        if (issubclass(cls, _BaseTableFamily) or not cls.generation_tables or cls.receipt_policies
                or cls.generation_kwargs is not RollupPipeline.generation_kwargs):
            continue
        tables = [key.removesuffix(".parquet") for key in cls.outputs or (cls.output,)]
        if registered := sorted((installed.keys() - retired_receipt_datasets()) & set(tables)):
            no_route[cls.name] = registered
    assert no_route == {}
    assert {cls.name: cls.output.removesuffix(".parquet") for cls in _scheduled_rollups()
            if issubclass(cls, _BaseTableFamily)} == BASE_FAMILIES


@pytest.mark.parametrize("family", sorted(BASE_FAMILIES))
def test_a_base_family_states_its_installed_policy_when_it_builds(tmp_path, monkeypatch, family):
    """The route the guard above takes on trust, held for each of the five whatever was built before."""
    [cls] = [rollup for rollup in _scheduled_rollups() if rollup.name == family]
    monkeypatch.setattr(cls, "receipt_policies", ())  # the build assigns on the class; this undoes it afterwards
    owner = cls.__new__(cls)
    pq.write_table(pa.table({owner.key(): ["A-1"]}), tmp_path / cls.output)
    owner.validate_native_output(tmp_path / cls.output)
    [stated] = owner.receipt_policies
    assert stated.descriptor() == installed_policies()[BASE_FAMILIES[family]].descriptor()
