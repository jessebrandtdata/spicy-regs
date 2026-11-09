"""Shared pytest fixtures for pipeline tests; the autouse env fixture strips pipeline environment variables.

Tests marked ``integration`` are exempt.
"""

import os
import shutil
from pathlib import Path

import polars as pl
import pytest

_ISOLATED_PREFIXES = ("R2_", "SPICYREGS_", "SPICY_REGS_", "CLOUDFLARE_", "AWS_", "FEC_")
_ISOLATED_NAMES = frozenset({"AGENCIES", "API_GOV", "DATA_GOV_API_KEY", "SAM_API_KEY", "LDA_API_KEY", "COURTLISTENER_API_TOKEN",
                             "SPICYGOV_PUBLICATION_MIRROR",
                             # Paid proxies: a walled-fetch ladder must never spend a developer's credits in a test.
                             "ZYTE_TOKEN", "FIRECRAWL_API_KEY"})


@pytest.fixture(autouse=True)
def isolate_env(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Delete pipeline environment variables before each non-``integration`` test."""
    if "integration" in request.keywords:
        return
    for name in list(os.environ):
        if name.startswith(_ISOLATED_PREFIXES) or name in _ISOLATED_NAMES:
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "central-raw"))


class _NoBulkBillText:
    """Every BILLS folder lists nothing, so the per-package route answers for every printing.

    The bill family builds its keyless bulk bill-text acquirer by default;
    this stands in for it so no test reaches GovInfo, and a test of the bulk
    route passes its own ``bills_acquirer``.
    """

    def __init__(self, **_arguments) -> None:
        self.listings: list[tuple[int, int, str]] = []

    def list_folder(self, congress: int, session: int, bill_type: str):
        from types import SimpleNamespace

        self.listings.append((congress, session, bill_type))
        link = f"https://www.govinfo.gov/bulkdata/BILLS/{congress}/{session}/{bill_type}/BILLS-{congress}-{session}-{bill_type}.zip"
        return SimpleNamespace(listing=SimpleNamespace(members={}, zip_entry=SimpleNamespace(link=link)))

    def acquire(self, listing, *, keep=None):
        raise AssertionError("an empty listing names nothing to download")


@pytest.fixture(autouse=True)
def fresh_owner_admissions() -> None:
    """Start every test with no process-wide owner admission, so a pin reused across tests never skips a check."""
    from spicy_regs import generations

    generations._OWNER_ADMISSIONS.clear()


@pytest.fixture(autouse=True)
def no_bulk_bill_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the bill family's default bulk bill-text acquirer off the network (see ``_NoBulkBillText``)."""
    monkeypatch.setattr("spicy_regs.transforms.build_bill_family.BulkBillsAcquirer", _NoBulkBillText, raising=False)


class _NoBillSum:
    """The bill family's default BILLSUM acquirer, off the network: every folder reads as refused.

    So a run keeps its published BILLSUM rows and retained entries, as it does when GovInfo refuses; a test of
    the supplement passes its own ``summary_acquirer``.
    """

    def __init__(self, **_arguments) -> None:
        pass

    def list_folder(self, congress: int, bill_type: str):
        from spicy_docs.sources.congress.bill_status import BillSourceError

        raise BillSourceError(f"no BILLSUM folder in a hermetic test ({congress} {bill_type})")

    def acquire(self, listing):
        raise AssertionError("a refused listing names nothing to download")


@pytest.fixture(autouse=True)
def no_billsum(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the bill family's default BILLSUM acquirer off the network (see ``_NoBillSum``)."""
    monkeypatch.setattr("spicy_regs.transforms.build_bill_family.BillSumAcquirer", _NoBillSum, raising=False)


class _NoCboFeed:
    """The bill family's default CBO feed acquirer, off the network: every feed reads as unavailable.

    So a run keeps its prior feed rows, as it does when cbo.gov refuses; a test of the feed route passes its own
    ``cbo_acquirer``.
    """

    def __init__(self, **_arguments) -> None:
        pass

    def acquire_per_congress_feed(self, congress: int, **_arguments):
        from spicy_docs.sources.cbo import CboSourceError

        raise CboSourceError(f"no CBO feed in a hermetic test ({congress})")


@pytest.fixture(autouse=True)
def no_cbo_feed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the bill family's default CBO feed acquirer off the network (see ``_NoCboFeed``)."""
    monkeypatch.setattr("spicy_regs.transforms.bill_family_cbo.CboAcquirer", _NoCboFeed, raising=False)


def _no_generation_root(base_url, entry):
    from spicy_regs.sources.publication import PublicationError

    raise PublicationError(f"no generation root in a hermetic test ({entry.get('prefix')})")


@pytest.fixture(autouse=True)
def no_generation_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the MCP server's lazy generation-root reads off the network: every root reads as unavailable.

    A test of the lineage surface passes its own reader; ``read_pinned_root`` itself is tested through a fake GET.
    """
    monkeypatch.setattr("spicy_regs.sources.publication.read_pinned_root", _no_generation_root)


class _ObjectsListingClient:
    """ListObjectsV2 pages over a fake resource's ``objects.filter``, so its refusals and records still apply."""

    def __init__(self, resource) -> None:
        self._resource = resource

    def get_paginator(self, name: str) -> "_ObjectsListingClient":
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str, Delimiter: str | None = None, StartAfter: str | None = None):
        keys = sorted(obj.key for obj in self._resource.Bucket(Bucket).objects.filter(Prefix=Prefix))
        if Delimiter is None:
            yield {"Contents": [{"Key": key} for key in keys if StartAfter is None or key > StartAfter]}
            return
        rests = (key[len(Prefix) :] for key in keys)
        prefixes = sorted({Prefix + rest.split(Delimiter)[0] + Delimiter for rest in rests if Delimiter in rest})
        yield {"CommonPrefixes": [{"Prefix": prefix} for prefix in prefixes]}


def _no_archive_network(url: str) -> bytes:
    from spicy_regs.archive_release import ArchiveReleaseError

    if url.startswith(("https://", "http://")):
        raise ArchiveReleaseError(f"no archive release in a hermetic test ({url})")
    with open(url, "rb") as stream:
        return stream.read()


@pytest.fixture(autouse=True)
def no_archive_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the MCP's archive binding off the network: a remote release reads as unavailable, a local path reads.

    An unscoped remote connection then reports the archive unavailable; a test of the binding points
    ``archive_release.ORIGIN`` at a fixture release directory.
    """
    from spicy_regs import archive_release

    monkeypatch.setattr(archive_release, "_get", _no_archive_network)
    archive_release.read_manifest.cache_clear()
    archive_release._read_schema.cache_clear()


@pytest.fixture(autouse=True)
def list_mirrulations_through_fake_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    """SpicyDocs lists Mirrulations with its own botocore client; fake resources list through their objects."""
    from spicy_docs.sources import mirrulations

    monkeypatch.setattr(mirrulations, "listing_client", _ObjectsListingClient)


@pytest.fixture
def remote(monkeypatch: pytest.MonkeyPatch):
    """A fake bucket whose publication index is read back from its own stored pointer."""
    from spicy_regs.sources import publication as pub, r2
    from tests.generation_fakes import Store

    store = Store()
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "fake")
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)

    def load_index(url):
        raw = store.objects.get(pub.INDEX_V2_KEY)
        return pub.empty_index() if raw is None else pub.parse_index(raw)

    monkeypatch.setattr(pub, "load_index", load_index)
    return store


@pytest.fixture
def tmp_output(tmp_path: Path) -> Path:
    """Return a temporary output directory."""
    out = tmp_path / "output"
    out.mkdir()
    return out


@pytest.fixture
def ample_disk_space(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give tiny materialization fixtures deterministic capacity on CI runners."""
    monkeypatch.setattr(
        shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(total=2**41, used=2**40, free=2**40)
    )


@pytest.fixture
def sample_dockets() -> list[dict]:
    """Three docket records across EPA and FDA, one with a null abstract and RIN."""
    return [
        {
            "docket_id": "EPA-2024-0001",
            "agency_code": "EPA",
            "title": "Clean Air Standards",
            "docket_type": "Rulemaking",
            "modify_date": "2024-06-15",
            "abstract": "Proposed rule for clean air",
            "rin": "2060-AG12",
        },
        {
            "docket_id": "FDA-2024-0010",
            "agency_code": "FDA",
            "title": "Drug Labeling",
            "docket_type": "Rulemaking",
            "modify_date": "2024-05-01",
            "abstract": "Updated drug labeling requirements",
            "rin": "0910-AH34",
        },
        {
            "docket_id": "EPA-2024-0002",
            "agency_code": "EPA",
            "title": "Water Quality",
            "docket_type": "Nonrulemaking",
            "modify_date": "2024-07-20",
            "abstract": None,
            "rin": None,
        },
    ]


@pytest.fixture
def sample_comments() -> list[dict]:
    """Four comment records across three dockets, mixing individual, organizational, and missing attribution."""
    return [
        {"comment_id": "C-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA", "first_name": "Ada", "last_name": "Lovelace", "organization": None, "category": "Individual", "title": "Support", "comment": "I support this", "document_type": "Public Comment", "posted_date": "2024-06-20", "modify_date": "2024-06-20", "receive_date": "2024-06-20", "attachments_json": None},
        {"comment_id": "C-002", "docket_id": "EPA-2024-0001", "agency_code": "EPA", "first_name": None, "last_name": None, "organization": "Sierra Club", "category": "Organization", "title": "Oppose", "comment": "I oppose this", "document_type": "Public Comment", "posted_date": "2024-06-21", "modify_date": "2024-06-21", "receive_date": "2024-06-21", "attachments_json": None},
        {"comment_id": "C-003", "docket_id": "FDA-2024-0010", "agency_code": "FDA", "first_name": "Grace", "last_name": "Hopper", "organization": None, "category": None, "title": "Question", "comment": "What about X?", "document_type": "Public Comment", "posted_date": "2024-05-10", "modify_date": "2024-05-10", "receive_date": "2024-05-10", "attachments_json": None},
        {"comment_id": "C-004", "docket_id": "EPA-2024-0002", "agency_code": "EPA", "first_name": None, "last_name": None, "organization": None, "category": None, "title": "Feedback", "comment": "More data needed", "document_type": "Public Comment", "posted_date": "2024-07-25", "modify_date": "2024-07-25", "receive_date": "2024-07-25", "attachments_json": None},
    ]


@pytest.fixture
def sample_documents() -> list[dict]:
    """Two document records: one proposed rule and one withdrawn notice carrying an additional RIN."""
    return [
        {"document_id": "D-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA", "title": "Proposed Rule", "document_type": "Proposed Rule", "posted_date": "2024-06-01", "modify_date": "2024-06-01", "comment_start_date": "2024-06-01", "comment_end_date": "2024-07-01", "file_url": None, "attachments_json": None, "fr_doc_num": None, "withdrawn": "false", "reason_withdrawn": None, "additional_rins": None, "text_content": None, "text_extraction_status": None},
        {"document_id": "D-002", "docket_id": "FDA-2024-0010", "agency_code": "FDA", "title": "Notice", "document_type": "Notice", "posted_date": "2024-04-15", "modify_date": "2024-04-15", "comment_start_date": "2024-04-15", "comment_end_date": "2024-05-15", "file_url": None, "attachments_json": None, "fr_doc_num": "2025-13790", "withdrawn": "true", "reason_withdrawn": "Superseded by revised proposal", "additional_rins": "[\"0910-AH35\"]", "text_content": None, "text_extraction_status": None},
    ]


DOCKET_SCHEMA = {
    "docket_id": pl.Utf8,
    "agency_code": pl.Utf8,
    "title": pl.Utf8,
    "docket_type": pl.Utf8,
    "modify_date": pl.Utf8,
    "abstract": pl.Utf8,
    "rin": pl.Utf8,
}

COMMENT_SCHEMA = {
    "comment_id": pl.Utf8,
    "docket_id": pl.Utf8,
    "agency_code": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "organization": pl.Utf8,
    "category": pl.Utf8,
    "title": pl.Utf8,
    "comment": pl.Utf8,
    "document_type": pl.Utf8,
    "posted_date": pl.Utf8,
    "modify_date": pl.Utf8,
    "receive_date": pl.Utf8,
    "attachments_json": pl.Utf8,
    "text_content": pl.Utf8,
    "text_extraction_status": pl.Utf8,
}

DOCUMENT_SCHEMA = {
    "document_id": pl.Utf8,
    "docket_id": pl.Utf8,
    "agency_code": pl.Utf8,
    "title": pl.Utf8,
    "document_type": pl.Utf8,
    "posted_date": pl.Utf8,
    "modify_date": pl.Utf8,
    "comment_start_date": pl.Utf8,
    "comment_end_date": pl.Utf8,
    "file_url": pl.Utf8,
    "attachments_json": pl.Utf8,
    "fr_doc_num": pl.Utf8,
    "withdrawn": pl.Utf8,
    "reason_withdrawn": pl.Utf8,
    "additional_rins": pl.Utf8,
    "text_content": pl.Utf8,
    "text_extraction_status": pl.Utf8,
}


def write_parquet_from_dicts(path: Path, records: list[dict], schema: dict) -> None:
    """Write ``records`` to ``path`` as a zstd-compressed Parquet file under ``schema``."""
    df = pl.DataFrame(records, schema=schema)
    df.write_parquet(path, compression="zstd")


@pytest.fixture(scope="session")
def installed_httpfs() -> Path:
    """Reuse matching signed httpfs; a cold runner provisions it once through DuckDB.

    A warm cache never downloads. A cold offline runner fails at this ordinary
    dependency-install boundary, rather than inside a sealed private HOME.
    """
    import duckdb

    with duckdb.connect() as con:
        row = con.execute("SELECT install_path FROM duckdb_extensions() "
                          "WHERE extension_name='httpfs' AND installed").fetchone()
        if row is None or not Path(row[0]).is_file():
            con.execute('INSTALL httpfs')
            row = con.execute("SELECT install_path FROM duckdb_extensions() "
                              "WHERE extension_name='httpfs' AND installed").fetchone()
        assert row and Path(row[0]).is_file(), "DuckDB did not install a matching httpfs"
        path = Path(row[0]).resolve()
        assert con.execute("SELECT current_setting('allow_unsigned_extensions')").fetchone() == (False,)
        con.execute("LOAD '" + str(path).replace("'", "''") + "'")
    return path
