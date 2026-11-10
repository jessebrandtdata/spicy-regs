"""The Federal Register table across the move away from per-row receipts (ss-vse3), on a cut of real data.

``fixtures/federal_register_switch`` (``manifest.json`` names every source):

- ``prior/``: 164 rows of the generation published 2026-10-30 (f0ce213e…, ``regulations-native-v1``) with their
  receipts, exactly as published: 36 of the 221 documents of 2026-10-29/30, 120 rows of one 2009 row group, and
  eight edge rows chosen by hand (below).
- ``pages/``: two cuts of that run's one retained API page (blob 985bbeaa…), each re-wrapped with its own count.
  ``night-2`` carries one labelled edit, document 2026-22019's relayed comment count 0 -> 3, so a re-read row changes.
- ``expected/``: the public tables the receipt route published from them (the Register table after each night, and
  the docket links built from each), written by this test's own route at the commit that promoted the columns,
  and held there by ``test_the_receipt_route_...``.

What this cannot see: the live prior is ``regulations-native-v2`` (three more columns than this v1 cut; the v2 pair
is covered by a synthetic row in tests/test_regulations_receipts.py), 164 rows say nothing about a million, and the
pages are cut, so no window is split and no page repeats a document.
"""

import json
import shutil
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

FIXTURE = Path(__file__).parent / "fixtures/federal_register_switch"
MANIFEST = json.loads((FIXTURE / "manifest.json").read_text())
PROMOTED = ("html_url", "pdf_url", "body_html_url", "source_sha256", "regulations_dot_gov_info_json")


def serve(monkeypatch, tmp_path, night):
    """Answer every Register request with one retained page, through the real traversal and evidence owner."""
    from spicy_docs.transport import acquisition

    raw = (FIXTURE / f"pages/{night}.json").read_bytes()

    @contextmanager
    def fetcher(_):
        yield lambda url: raw

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw-source-originals"))
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("FR_SINCE", raising=False)


def published(root, before=()):
    """The one generation ``root`` gained since ``before``."""
    [generation] = [path for path in (root / "generations").iterdir() if path not in before]
    return generation


def assert_links(table, register):
    """The link rows a Register table states, checked on rows read by hand from the fixture."""
    links = pq.read_table(table).to_pylist()
    assert pq.read_schema(table).names[-2:] == ["link_source", "normalized_docket_candidates_json"]
    assert [row["docket_id"] for row in links] == sorted(row["docket_id"] for row in links)
    documents = {(row["document_number"], row["publication_date"]): row for row in pq.read_table(register).to_pylist()}
    printed = sum(len([label for label in row["docket_ids"] or [] if label and label.strip()])
                  for row in documents.values())
    assert sum(row["link_source"] != "regulations_dot_gov_info" for row in links) == printed
    assert {(row["document_number"], row["publication_date"]) for row in links} <= set(documents)
    # 2026-22078 prints "Docket Number USCG-2026-1075" and the Register links regulations.gov docket USCG-2026-1075:
    # one row, both statements, and the normalizer's reading of the printed label is public.
    [link] = [row for row in links if row["document_number"] == "2026-22078"]
    assert (link["docket_id"], link["docket_source_ordinal"], link["link_source"]) == (
        "Docket Number USCG-2026-1075", 0, "both")
    assert json.loads(link["normalized_docket_candidates_json"]) == ["USCG-2026-1075"]


def rows(path):
    return {(row["document_number"], row["publication_date"]): row for row in pq.read_table(path).to_pylist()}


def assert_same_table(actual, expected):
    """Schema, row count, row order on the key, and every value both ways."""
    assert pq.read_schema(actual).equals(pq.read_schema(expected))
    order = "a.docket_id" if "docket_id" in pq.read_schema(expected).names else "NULL"
    one, other = (f"read_parquet('{path}', file_row_number=true)" for path in (actual, expected))
    names = ", ".join(f'"{name}"' for name in pq.read_schema(expected).names)
    counts = duckdb.sql(f"SELECT (SELECT count(*) FROM {one}), (SELECT count(*) FROM {other})").fetchone()
    assert counts[0] == counts[1]
    for left, right in ((one, other), (other, one)):
        assert duckdb.sql(f"SELECT count(*) FROM (SELECT {names} FROM {left} EXCEPT ALL SELECT {names} FROM {right})"
                          ).fetchone() == (0,)
    assert duckdb.sql(f"""SELECT count(*) FROM {one} a FULL JOIN {other} b USING (file_row_number)
                          WHERE a.document_number IS DISTINCT FROM b.document_number
                             OR a.publication_date IS DISTINCT FROM b.publication_date
                             OR {order} IS DISTINCT FROM {order.replace("a.", "b.")}""").fetchone() == (0,)


def assert_edge_rows(table, *, night):
    """Hand-checked against the fixture's receipts and pages, not against the route under test."""
    held = rows(table)
    assert list(held) == sorted(held, key=lambda key: (-int(key[1].replace("-", "")), key[0]))
    # One number on two dates stays two records, each with its own page on federalregister.gov.
    first, second = held["2015-25354", "2015-10-06"], held["2015-25354", "2015-10-13"]
    assert first["html_url"].startswith("https://www.federalregister.gov/documents/2015/10/06/2015-25354/")
    assert second["html_url"].startswith("https://www.federalregister.gov/documents/2015/10/13/2015-25354/")
    assert first["title"] != second["title"] or first["html_url"] != second["html_url"]
    # 1994: the Register serves no PDF, and says so with null; the other URLs are there.
    for number in ("94-31430", "94-31579"):
        row = held[number, "1994-12-30"]
        assert row["pdf_url"] is None and row["html_url"] and row["body_html_url"].endswith(f"/{number}.html")
    # Read since the regulations.gov statement was first requested, and the Register states nothing: '{}', not NULL.
    for number in ("2026-19936", "2026-19937"):
        assert held[number, "2026-09-30"]["regulations_dot_gov_info_json"] == "{}"
    # The regulatory plan's own URL is stated nowhere else: 2026-22078 is never re-read, 2026-22042 is on night 2.
    for number, plan in (("2026-22078", "1625-AA00/safety-zone-regulations"),
                         ("2026-22042", "2120-AA66/airspace-actions")):
        row = held[number, "2026-10-29"]
        assert json.loads(row["regulations_dot_gov_info_json"])["regulatory_plan"]["html_url"] == (
            "https://www.federalregister.gov/regulations/" + plan)
        assert row["regulations_dot_gov_regulatory_plan_title"]
    # Every row has the Register's URLs. The pointer names the page a row was last read from, and only that.
    assert all(row["html_url"] and row["body_html_url"] and row["regulations_dot_gov_info_json"] for row in held.values())
    pages = {name: MANIFEST["pages"][name]["sha256"] for name in ("night-1", "night-2")}
    read = {name: {(d["document_number"], d["publication_date"])
                   for d in json.loads((FIXTURE / f"pages/{name}.json").read_bytes())["results"]}
            for name in pages}
    for key, row in held.items():
        expected = (pages["night-2"] if night == 2 and key in read["night-2"] else
                    pages["night-1"] if key in read["night-1"] else None)
        assert row["source_sha256"] == expected, key
    edited = held[MANIFEST["edited"]["document_number"], MANIFEST["edited"]["publication_date"]]
    assert edited["regulations_dot_gov_comments_count"] == (3 if night == 2 else 0)
    assert len(held) == MANIFEST["prior"]["rows"] + (6 if night == 1 else 13)


def test_the_receipt_route_fills_the_promoted_columns_from_an_earlier_policy_prior(tmp_path, monkeypatch):
    """Step 1's nightly: the prior restores through its receipts although its policy is earlier (no replay marker),
    one run gives every row the values its receipts held, and the next run carries them. ``expected/`` is this."""
    from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup
    from spicy_regs.pipelines.rollups.fr_docket_links import FrDocketLinksRollup
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection

    root = tmp_path / "output"
    prior = tmp_path / "prior"
    shutil.copytree(FIXTURE / "prior", prior)
    assert not set(PROMOTED) & set(pq.read_schema(prior / "federal_register.parquet").names)
    remember_selection(root, [SelectedDataset("federal_register", (prior / "federal_register.parquet",),
                                              prior / "etl_receipts.parquet", MANIFEST["prior"]["generationId"])])
    # The selected prior is carried, never replay-required: the bill-family freeze of ss-aj7t cannot happen here.
    selected = SelectedPriors(tmp_path / "probe", root=root, public_url="")
    assert selected.get("federal_register") is not None and not selected.replaced

    serve(monkeypatch, tmp_path, "night-1")
    FederalRegisterRollup(output_dir=root).run()
    first = published(root)
    assert (first / "etl_receipts.parquet").is_file()
    assert pq.read_schema(first / "federal_register.parquet").names[-5:] == list(PROMOTED)
    assert_edge_rows(first / "federal_register.parquet", night=1)
    assert_same_table(first / "federal_register.parquet", FIXTURE / "expected/night-1.parquet")
    # The links read the published Register table; nothing of the Register's is restored through receipts.
    with monkeypatch.context() as links_only:
        links_only.setattr(SelectedPriors, "get", lambda self, dataset: pytest.fail(f"restored {dataset}"))
        FrDocketLinksRollup(output_dir=root).run()
    first_links = published(root, before=(first,))
    assert_links(first_links / "fr_docket_links.parquet", first / "federal_register.parquet")
    assert_same_table(first_links / "fr_docket_links.parquet", FIXTURE / "expected/links-night-1.parquet")

    serve(monkeypatch, tmp_path, "night-2")
    FederalRegisterRollup(output_dir=root).run()
    second = published(root, before=(first, first_links))
    assert_edge_rows(second / "federal_register.parquet", night=2)
    assert_same_table(second / "federal_register.parquet", FIXTURE / "expected/night-2.parquet")
    FrDocketLinksRollup(output_dir=root).run()
    second_links = published(root, before=(first, first_links, second))
    assert_links(second_links / "fr_docket_links.parquet", second / "federal_register.parquet")
    assert_same_table(second_links / "fr_docket_links.parquet", FIXTURE / "expected/links-night-2.parquet")


def stored(remote, monkeypatch):
    """Serve the fake bucket's own objects to the pinned readers; admission and publication run for real."""
    import hashlib

    from spicy_regs.sources import publication, r2

    def index(_=None):
        raw = remote.objects.get(publication.INDEX_V2_KEY)
        return publication.empty_index() if raw is None else publication.parse_index(raw)

    def fetch(base, member, target, label=None, **_):
        raw = remote.objects[member.path]
        assert "sha256:" + hashlib.sha256(raw).hexdigest() == member.sha256
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        return True

    def get(url, *, allow_missing=False):
        return remote.objects.get(url.removeprefix("https://example.test/"))

    monkeypatch.setattr(publication, "current_index", index)
    monkeypatch.setattr(publication, "fetch_member", fetch)
    monkeypatch.setattr(publication, "_bounded_get", get)
    monkeypatch.setattr(r2, "require_credentials", lambda _: None)
    return index


def publish_prior(remote, monkeypatch, tmp_path):
    """Put the fixture's pair in the bucket as main published it: under the policy it was written with."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.etl_receipts import receipt_policies
    from spicy_regs.generations import build_generation
    from spicy_regs.sources import publication
    from spicy_regs.transforms.regulations_receipts import policy

    schema = pq.read_schema(FIXTURE / "prior/federal_register.parquet")
    [earlier] = [p for p in receipt_policies(policy("federal_register")) if p.subject_schema.equals(schema)]
    assert earlier.policy_version == MANIFEST["prior"]["policy_version"]
    directory = tmp_path / "prior-generation"
    with monkeypatch.context() as as_published:
        as_published.setattr(etl_policy_registry, "installed_policies", lambda: {"federal_register": earlier})
        build_generation(
            directory, family="federal-register", files=[FIXTURE / "prior/federal_register.parquet"],
            expected_keys=["federal_register.parquet"], receipt_path=FIXTURE / "prior/etl_receipts.parquet",
            receipt_policies=[earlier], receipt_generation_id=MANIFEST["prior"]["generationId"])
        publication.publish_generation(directory, client=remote, bucket="spicy-regs",
                                       prior_index=publication.empty_index())


def test_publication_accepts_the_promoted_columns_over_the_earlier_policy_prior(tmp_path, monkeypatch, remote):
    """The step-1 generation against a prior of the live shape: fewer columns, receipts owned, an earlier policy.
    Its columns differ, so the shrink guard's rows-and-columns rule does not apply and the ordinary byte ratio
    holds (the table grows here, as it will live: the promoted columns were 44 MB of receipts)."""
    from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup

    index = stored(remote, monkeypatch)
    publish_prior(remote, monkeypatch, tmp_path)
    before = index()["families"]["federal-register"]
    assert before["etlReceipts"]["datasets"] == ["federal_register"]
    serve(monkeypatch, tmp_path, "night-1")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    FederalRegisterRollup(output_dir=tmp_path / "output", skip_upload=False).run()
    after = index()["families"]["federal-register"]
    assert after["artifactDigest"] != before["artifactDigest"]
    assert after["etlReceipts"]["datasets"] == ["federal_register"]
    table, prior = after["tables"]["federal_register.parquet"], before["tables"]["federal_register.parquet"]
    # Every published column is kept. This v1 cut also gains v2's three; the promoted five are the last.
    assert [column for column in table["columns"] if column in prior["columns"]] == prior["columns"]
    assert {name for name, _ in table["columns"]} - {name for name, _ in prior["columns"]} == {
        "president_json", "disposition_notes", "executive_order_notes", *PROMOTED}
    assert [name for name, _ in table["columns"][-5:]] == list(PROMOTED)
    assert (table["rows"], prior["rows"]) == (170, 164) and table["byteSize"] > prior["byteSize"]
    assert_same_table(published(tmp_path / "output") / "federal_register.parquet", FIXTURE / "expected/night-1.parquet")


def test_the_rulemaking_stages_read_the_published_table_as_they_read_the_restored_one(tmp_path):
    """The materializer's text shape of the published table against the rows the fixture's receipts restore.

    Compared on the columns the stages declare (``source_column_requirements``), for the 146 rows the first night does
    not re-read. The lists are compared parsed: the published table keeps an agency's five identifying fields and a
    CFR reference's three, not the Register's ``url``, ``json_url`` or ``citation_url``, and spells a part as text.
    """
    from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline, stage_text
    from spicy_regs.transforms.regulations_receipts import ReceiptInput, read_internal

    restored = {(row["document_number"], row["publication_date"]): row for row in read_internal(ReceiptInput(
        "federal_register", (FIXTURE / "prior/federal_register.parquet",), FIXTURE / "prior/etl_receipts.parquet",
        MANIFEST["prior"]["generationId"]))}
    staged = tmp_path / "federal_register.parquet"
    shutil.copyfile(FIXTURE / "expected/night-1.parquet", staged)
    stage_text("federal_register", staged)
    text = rows(staged)
    columns = RulemakingDatasetPipeline(output_dir=tmp_path).source_column_requirements()["federal_register.parquet"]
    assert set(columns) <= set(pq.read_schema(staged).names)
    carried = [key for key, row in rows(FIXTURE / "expected/night-1.parquet").items() if row["source_sha256"] is None]
    assert len(carried) == 146 and set(carried) <= set(restored)
    kept = {"agencies_json": ("id", "name", "raw_name", "parent_id", "slug"),
            "cfr_references_json": ("title", "part", "chapter")}

    def stated(name, value):
        if not name.endswith("_json") or value is None:
            return value
        parsed = json.loads(value)
        if name in kept:
            parsed = [{field: (str(item[field]) if field in ("part", "chapter") and item.get(field) is not None
                               else item.get(field)) for field in kept[name]} for item in parsed]
        return parsed

    for key in carried:
        assert {name: stated(name, text[key][name]) for name in columns} == {
            name: stated(name, restored[key][name]) for name in columns}, key
    stage_text("federal_register", staged)  # already text: left alone
    assert rows(staged) == text
