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

import hashlib
import json
import shutil
from contextlib import contextmanager
from pathlib import Path

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
    assert pq.read_schema(table).names[-2:] == ["link_source", "normalized_docket_candidates"]
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
    assert link["normalized_docket_candidates"] == ["USCG-2026-1075"]
    # A label the normalizer reads no regulations.gov docket from holds an empty list, never NULL: 2026-22032
    # prints the antidumping case number A-570-893.
    [case] = [row for row in links if row["document_number"] == "2026-22032" and row["docket_id"] == "A-570-893"]
    assert case["normalized_docket_candidates"] == [] and case["link_source"] == "printed"
    assert all(row["normalized_docket_candidates"] is not None for row in links)


def rows(path):
    return {(row["document_number"], row["publication_date"]): row for row in pq.read_table(path).to_pylist()}


#: The v1 prior lacks every column that left receipts; carried, its rows would publish NULL for them.
V1_REFUSAL = (r"lacks \['html_url', 'pdf_url', 'body_html_url', 'source_sha256', 'regulations_dot_gov_info_json'\], "
              r"which a generation before 2026-11-10 held only in its receipts")
#: A Register row's identity, and a link row's: a printed label may repeat, so its position is part of it.
KEY = ("document_number", "publication_date")
LINK_KEY = ("docket_id", *KEY, "docket_source_ordinal")


def assert_same_table(actual, expected):
    """Schema, row count, every value both ways and the key at every position (``receipt_switch``)."""
    from spicy_regs.receipt_switch import table_differences

    key = LINK_KEY if "docket_id" in pq.read_schema(expected).names else KEY
    assert table_differences(actual, expected, key=key) == []


def prior_receipt_copies(table, *, reread):
    """``table``'s promoted values against the 2026-10-30 receipts' copies, for the prior rows ``reread`` misses."""
    import operator

    from spicy_regs.etl_receipts import receipt_policies
    from spicy_regs.receipt_switch import promoted_differences
    from spicy_regs.transforms.regulations_receipts import policy

    prior = FIXTURE / "prior/federal_register.parquet"
    [earlier] = [p for p in receipt_policies(policy("federal_register"))
                 if p.subject_schema.equals(pq.read_schema(prior))]
    read = {(d["document_number"], d["publication_date"])
            for name in reread for d in json.loads((FIXTURE / f"pages/{name}.json").read_bytes())["results"]}
    carried = set(rows(prior)) - read
    assert len(carried) > 100
    copies = {name: operator.itemgetter(name) for name in (
        "html_url", "pdf_url", "body_html_url", "regulations_dot_gov_info_json")}
    return promoted_differences(table, prior, FIXTURE / "prior/etl_receipts.parquet", earlier,
                                generation_id=MANIFEST["prior"]["generationId"], key=KEY, copies=copies, among=carried)


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


def journal_events(root, family, name):
    """Every event ``name`` in the journal ``family``'s generation published with its inputs under ``root``."""
    [journal] = (root / "generation-inputs" / family).glob("*/evidence-0/journal.jsonl")
    return [event for line in journal.read_text().splitlines() if (event := json.loads(line)).get("event") == name]


def test_the_plain_route_publishes_what_the_receipt_route_published(tmp_path, monkeypatch):
    """Step 3 on the fixture, through ``receipt_switch``: the prior is the public table the receipt route published
    after night 1 (``expected/night-1``), handed over with no receipts file at all; night 2's page through the plain
    route equals the table the receipt route published from the same inputs, and so do the links built from it."""
    from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup
    from spicy_regs.pipelines.rollups.fr_docket_links import FrDocketLinksRollup
    from spicy_regs.receipt_switch import receipt_files, table_differences

    root = tmp_path / "output"
    root.mkdir()
    shutil.copyfile(FIXTURE / "expected/night-1.parquet", root / "federal_register.parquet")
    serve(monkeypatch, tmp_path, "night-2")
    FederalRegisterRollup(output_dir=root).run()
    register = published(root)
    assert table_differences(register / "federal_register.parquet", FIXTURE / "expected/night-2.parquet",
                             key=KEY) == []
    assert_edge_rows(register / "federal_register.parquet", night=2)
    # Rows neither night re-read carry the values the 2026-10-30 receipts held, read through their admission.
    assert prior_receipt_copies(register / "federal_register.parquet", reread=("night-1", "night-2")) == []
    # The journal names the page read and counts the merge.
    [pages] = journal_events(root, "federal-register", "federal-register-pages")
    # The stand-in fetcher answers every request with the one page, so each page entry names it.
    assert {page["sha256"] for page in pages["pages"]} == {MANIFEST["pages"]["night-2"]["sha256"]}
    assert MANIFEST["pages"]["night-2"]["records"] in [page["records"] for page in pages["pages"]]
    [merge] = journal_events(root, "federal-register", "federal-register-merge")
    assert {key: merge[key] for key in ("prior_rows", "read_rows", "new_rows", "rows")} == {
        "prior_rows": 170, "read_rows": MANIFEST["pages"]["night-2"]["records"], "new_rows": 7, "rows": 177}

    FrDocketLinksRollup(output_dir=root).run()
    links = published(root, before=(register,))
    assert table_differences(links / "fr_docket_links.parquet", FIXTURE / "expected/links-night-2.parquet",
                             key=LINK_KEY) == []
    assert_links(links / "fr_docket_links.parquet", register / "federal_register.parquet")
    parents = json.loads((links / "artifact.json").read_text())["spec"]["parents"]
    assert parents["federal_register.parquet"]["sha256"] == "sha256:" + hashlib.sha256(
        (register / "federal_register.parquet").read_bytes()).hexdigest()
    # Neither generation states receipts, and no receipts file was written or fetched anywhere under the run.
    for generation in (register, links):
        assert "etlReceipts" not in json.loads((generation / "artifact.json").read_text())["spec"]
    assert receipt_files(root) == []


def test_the_plain_route_refuses_a_prior_published_before_the_promoted_columns(tmp_path, monkeypatch):
    """Step 3 merges only after step 1's night: carried, such a prior would publish NULL where receipts held values."""
    from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup

    root = tmp_path / "output"
    root.mkdir()
    shutil.copyfile(FIXTURE / "prior/federal_register.parquet", root / "federal_register.parquet")
    serve(monkeypatch, tmp_path, "night-1")
    with pytest.raises(ValueError, match=V1_REFUSAL):
        FederalRegisterRollup(output_dir=root).run()
    assert not (root / "generations").exists() or not list((root / "generations").iterdir())


def stored(remote, monkeypatch):
    """Serve the fake bucket's own objects to the pinned readers; admission and publication run for real."""
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
    from spicy_regs import etl_policy_registry, subject_catalog
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
        as_published.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset())
        build_generation(
            directory, family="federal-register", files=[FIXTURE / "prior/federal_register.parquet"],
            expected_keys=["federal_register.parquet"], receipt_path=FIXTURE / "prior/etl_receipts.parquet",
            receipt_policies=[earlier], receipt_generation_id=MANIFEST["prior"]["generationId"])
        publication.publish_generation(directory, client=remote, bucket="spicy-regs",
                                       prior_index=publication.empty_index())


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


def test_receipts_leave_a_published_table_only_by_its_retired_marker_and_never_return(tmp_path, monkeypatch, remote):
    """The installed policy's ``receipts_retired`` marker at publication: refused unmarked, accepted marked, one-way.

    The generations are the fixture's tables published directly through ``publish_generation``, a path that states
    nothing about retirement: publication reads the marker itself, as every other path now does (ss-ag1j).
    """
    from spicy_regs import subject_catalog
    from spicy_regs.etl_receipts import receipt_policies
    from spicy_regs.generations import build_generation
    from spicy_regs.sources import publication
    from spicy_regs.transforms.regulations_receipts import policy

    index = stored(remote, monkeypatch)
    publish_prior(remote, monkeypatch, tmp_path)

    def publish(name, table, *, receipts=None, policies=None):
        (tmp_path / name).with_suffix(".table").mkdir()
        table = Path(shutil.copyfile(table, (tmp_path / name).with_suffix(".table") / "federal_register.parquet"))
        build_generation(tmp_path / name, family="federal-register", files=[table],
                         expected_keys=["federal_register.parquet"], receipt_path=receipts,
                         receipt_policies=policies if receipts else None,
                         receipt_generation_id=MANIFEST["prior"]["generationId"] if receipts else None)
        return publication.publish_generation(tmp_path / name, client=remote, bucket="spicy-regs", prior_index=index())

    before = index()["families"]["federal-register"]
    # Unmarked, a generation without receipts is refused before it is even built: the policy still requires them.
    with monkeypatch.context() as unmarked:
        unmarked.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset())
        with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
            publish("unmarked", FIXTURE / "expected/night-1.parquet")
    assert index()["families"]["federal-register"] == before

    # Marked, as its installed policy file now is; the archive pins the generation its receipts leave.
    assert "federal_register" in subject_catalog.retired_receipt_datasets()
    from tests.test_receipts_retired import _pin, pin_archive

    pin_archive(monkeypatch, tmp_path, {"federal_register": _pin("federal-register", before["artifactDigest"])})
    publish("retired", FIXTURE / "expected/night-1.parquet")
    after = index()["families"]["federal-register"]
    assert "etlReceipts" not in after and after["tables"]["federal_register.parquet"]["rows"] == 170
    # The archive is untouched: the prior generation's receipt member is still stored under its own prefix.
    assert before["prefix"] + "/etl_receipts.parquet" in remote.objects
    # Inert from here on: the next generation needs nothing.
    publish("next", FIXTURE / "expected/night-2.parquet")
    assert index()["families"]["federal-register"]["tables"]["federal_register.parquet"]["rows"] == 177

    # Receipts for a retired table are refused for as long as it is marked, before anything is uploaded.
    schema = pq.read_schema(FIXTURE / "prior/federal_register.parquet")
    [earlier] = [p for p in receipt_policies(policy("federal_register")) if p.subject_schema.equals(schema)]
    with pytest.raises(ValueError, match=r"Receipts are retired for \['federal_register'\]"):
        publish("again", FIXTURE / "prior/federal_register.parquet", receipts=FIXTURE / "prior/etl_receipts.parquet",
                policies=[earlier])


def test_a_retirement_covers_only_the_tables_it_marks(tmp_path, monkeypatch):
    """A family that owns two receipt datasets and retires one may not drop the other with it.

    Nor can it keep receipts for the other alone today: admission requires a receipt member to classify every
    table beside it (``generations._verify_receipts``), so the tables of one family leave receipts together."""
    import pyarrow as pa

    from spicy_regs import etl_policy_registry, subject_catalog
    from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
    from spicy_regs.generations import build_generation
    from spicy_regs.sources.publication import PublicationError, empty_index, publish_generation
    from tests.generation_fakes import Store

    store, schema = Store(), pa.schema([("id", pa.string())])
    policies = [DatasetPolicy(name, schema, ("id",), ("held",)) for name in ("kept_table", "retired_table")]
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {p.dataset: p for p in policies})
    held = ReceiptContext("build-1", "row", "test", [{"source_id": "s", "body_version": "1"}])
    pairs = [write_dataset([({"id": "1", "held": "x"}, held)], tmp_path / p.dataset, p) for p in policies]
    receipts = combine_receipts([receipt for _, receipt in pairs], tmp_path / "etl_receipts.parquet")
    tables = [subject for subject, _ in pairs if subject is not None]
    assert len(tables) == len(policies)
    keys = [p.dataset + ".parquet" for p in policies]
    build_generation(tmp_path / "first", family="two", files=tables, expected_keys=keys, receipt_path=receipts,
                     receipt_policies=policies, receipt_generation_id="build-1")
    index = publish_generation(tmp_path / "first", client=store, bucket="b", prior_index=empty_index())
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset({"retired_table"}))
    from tests.test_receipts_retired import _pin, pin_archive

    pin_archive(monkeypatch, tmp_path, {"retired_table": _pin("two", index["families"]["two"]["artifactDigest"])})
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {"retired_table": policies[1]})
    build_generation(tmp_path / "second", family="two", files=tables, expected_keys=keys)
    with pytest.raises(PublicationError, match="drops required ETL receipts"):
        publish_generation(tmp_path / "second", client=store, bucket="b", prior_index=index)
    with pytest.raises(ValueError, match="Receipt policies must classify every subject output"):
        build_generation(tmp_path / "third", family="two", files=tables, expected_keys=keys, receipt_path=pairs[0][1],
                         receipt_policies=policies[:1], receipt_generation_id="build-1")


def test_both_hosted_jobs_merge_at_the_pinned_public_table_and_publish_no_receipts(tmp_path, monkeypatch, remote):
    """Hosted mode against the fake bucket, on the night the switch merges. The live prior is the receipt route's
    last generation; the Register job refuses one that predates the promoted columns and publishes nothing, then
    over the promoted table it publishes night 2 without receipts, and the links job pins that table as its parent."""
    from spicy_regs.generations import build_generation
    from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup
    from spicy_regs.pipelines.rollups.fr_docket_links import FrDocketLinksRollup
    from spicy_regs.receipt_switch import table_differences
    from spicy_regs.sources import publication

    index = stored(remote, monkeypatch)
    publish_prior(remote, monkeypatch, tmp_path)
    before = index()["families"]["federal-register"]
    serve(monkeypatch, tmp_path, "night-1")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    with pytest.raises(ValueError, match=V1_REFUSAL):
        FederalRegisterRollup(output_dir=tmp_path / "early", skip_upload=False).run()
    assert index()["families"]["federal-register"] == before

    # Step 1's night published the promoted table; this one is the first without receipts.
    (tmp_path / "night-1").mkdir()
    shutil.copyfile(FIXTURE / "expected/night-1.parquet", tmp_path / "night-1/federal_register.parquet")
    build_generation(tmp_path / "promoted", family="federal-register",
                     files=[tmp_path / "night-1/federal_register.parquet"], expected_keys=["federal_register.parquet"])
    from tests.test_receipts_retired import _pin, pin_archive

    # The receipts leave here: the archive must pin the generation they leave in, the live one (ss-ag1j review).
    pin_archive(monkeypatch, tmp_path, {"federal_register": _pin("federal-register", before["artifactDigest"])})
    publication.publish_generation(tmp_path / "promoted", client=remote, bucket="spicy-regs", prior_index=index())
    serve(monkeypatch, tmp_path, "night-2")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    FederalRegisterRollup(output_dir=tmp_path / "register", skip_upload=False).run()
    register = index()["families"]["federal-register"]
    assert "etlReceipts" not in register and register["tables"]["federal_register.parquet"]["rows"] == 177
    assert table_differences(published(tmp_path / "register") / "federal_register.parquet",
                             FIXTURE / "expected/night-2.parquet", key=KEY) == []
    FrDocketLinksRollup(output_dir=tmp_path / "links", skip_upload=False).run()
    links = index()["families"]["fr-docket-links"]
    assert "etlReceipts" not in links
    root = publication.family_root(remote.objects[links["prefix"] + "/artifact.json"], links)
    table = register["tables"]["federal_register.parquet"]
    assert root["spec"]["parents"] == {"federal_register.parquet": {
        "sha256": table["sha256"], "byteSize": table["byteSize"],
        "family": "federal-register", "artifactDigest": register["artifactDigest"]}}
    assert table_differences(published(tmp_path / "links") / "fr_docket_links.parquet",
                             FIXTURE / "expected/links-night-2.parquet", key=LINK_KEY) == []
