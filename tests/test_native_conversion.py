"""The one-time family conversion: what it proves before it publishes, what it refuses, and how it rolls back."""

import json
from contextlib import contextmanager
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import canonical_json_bytes

from spicy_regs import native_conversion as conversion
from spicy_regs.sources import publication, r2
from tests.regulatory_publication_fakes import install

BASE = "https://test.invalid"
MAIN = "0f" * 20
WHEEL = "0.56.0"
PINNED = "sha256:" + "ab" * 32
PINNED_WHEEL: dict = {"locked_sha256": PINNED, "file_sha256": PINNED, "installed_differs": []}
STATE: dict = {"checkout": MAIN, "uncommitted": [], "untracked": [], "remote": "git@example.invalid:spicy-regs.git",
               "main": MAIN, "spicy_docs": WHEEL, "pinned_spicy_docs": WHEEL, "spicy_docs_wheel": PINNED_WHEEL}
BUCKET = "spicy-regs"

CFR = [
    {"granule_id": "CFR-2025-title1-vol1-sec1-1", "part_granule": "false", "package_id": "CFR-2025-title1-vol1",
     "cfr_ref": "1-1.1", "title": "1", "part": "1", "section": "1.1", "heading": "Definitions.",
     "structure_level": "section", "edition_year": "2025", "last_modified": "2025-06-01T00:00:00Z",
     "url": "https://www.govinfo.gov/app/details/CFR-2025-title1-vol1/CFR-2025-title1-vol1-sec1-1"},
    {"granule_id": "CFR-2025-title1-vol1-part2", "part_granule": "true", "package_id": "CFR-2025-title1-vol1",
     "cfr_ref": None, "title": "1", "part": "2", "section": None, "heading": "", "structure_level": "part",
     "edition_year": "2025", "last_modified": None, "url": None},
]
DOCKET = {
    "cl_docket_id": "73613631", "case_name": "County v. Department", "case_name_full": "", "court_id": "dcd",
    "court": "District Court, District of Columbia", "court_citation_string": "D.D.C.", "docket_number": "1:26-cv-02460",
    "date_filed": "2026-07-14", "date_terminated": None, "date_argued": None, "nature_of_suit": "899",
    "cause": "05:551 Administrative Procedure Act", "jurisdiction_type": "U.S. Government Defendant",
    "jury_demand": "None", "assigned_to": "A Judge", "referred_to": None, "parties_json": '["County", null, ""]',
    "attorneys_json": None, "firms_json": "[]", "pacer_case_id": "294455", "date_created": "2026-07-14T17:38:04Z",
    "absolute_url": "https://www.courtlistener.com/docket/73613631/county-v-department/", "case_type": "cv",
}


@pytest.fixture
def bucket(monkeypatch):
    """The shared in-memory bucket, also answering the public reads the conversion makes outside the index."""
    store = install(monkeypatch)
    fetch = publication.fetch_member
    # The public API accepts an omitted diagnostic label; the shared older fixture requires one.
    def fetch_member(base, member, target, label=None):
        return fetch(base, member, target, label)
    monkeypatch.setattr(publication, "fetch_member", fetch_member)
    monkeypatch.setenv("R2_BUCKET_NAME", BUCKET)
    monkeypatch.setenv("R2_ENDPOINT", "https://account.r2.invalid")

    def stored(url):
        return url.removeprefix(BASE + "/")

    monkeypatch.setattr(r2, "public_object_version", lambda url: (
        {"etag": store._etag(stored(url)), "bytes": len(store.objects[stored(url)])} if stored(url) in store.objects else None))
    monkeypatch.setattr(publication, "_bounded_get", lambda url, **_: store.objects.get(stored(url)))
    real_stream = httpx.stream

    @contextmanager
    def stream(method, url, **options):
        """Anonymous public GETs of the stored bytes; read-back verifies them through the real streaming code."""
        if not url.startswith(BASE + "/"):
            with real_stream(method, url, **options) as response:
                yield response
            return
        raw = store.objects.get(stored(url))
        yield httpx.Response(404 if raw is None else 200, content=raw or b"", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx, "stream", stream)
    return store


def publish_old(store, monkeypatch, directory, family, tables):
    """Publish ``tables`` as they were published before receipts existed: admitted, indexed, no ``etlReceipts``."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation

    files = []
    for name, rows in tables.items():
        files.append(directory / "old" / f"{name}.parquet")
        files[-1].parent.mkdir(parents=True, exist_ok=True)
        table = rows if isinstance(rows, pa.Table) else pa.Table.from_pylist(
            rows, schema=pa.schema([(column, pa.string()) for column in rows[0]]))
        pq.write_table(table, files[-1])
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(directory / "old-generation" / family, family=family, files=files,
                         expected_keys=[path.name for path in files])
        index = publication.publish_generation(directory / "old-generation" / family, client=store,
                                               bucket="spicy-regs", prior_index=publication.current_index(BASE))
    return index["families"][family]


#: The operator's channel in these tests: the digest each preparation returned, by receipt path. It is never read
#: back from the receipt it covers, so a receipt changed after preparation cannot vouch for itself.
PREPARED: dict = {}


def prepared_digest(receipt_path):
    return PREPARED[Path(receipt_path).resolve()]


def convert(family, work, **options):
    options.setdefault("allowed", [family])
    options.setdefault("state", lambda remote: dict(STATE))
    options.setdefault("expect_bucket", BUCKET)
    done = conversion.convert(family, work=work, expected_main=MAIN, expected_spicy_docs=WHEEL, **options)
    if done.get("prepared"):
        PREPARED[(Path(work) / conversion.RECEIPT).resolve()] = done["prepared"]["receiptDigest"]
    return done


def refused_read(tmp_path, dataset):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    with pytest.raises(ValueError, match="selected native input requires one ETL receipt generation"):
        SelectedPriors(tmp_path / "refused" / dataset, public_url=BASE).get(dataset)


def test_a_rollup_family_converts_publishes_and_reads_back(tmp_path, monkeypatch, bucket):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    refused_read(tmp_path, "cfr_sections")

    receipt = convert("cfr-sections", tmp_path / "work", publish=True)

    entry = publication.current_index(BASE)["families"]["cfr-sections"]
    assert entry == receipt["published"]["entry"] and entry["etlReceipts"]["datasets"] == ["cfr_sections"]
    assert entry["tables"]["cfr_sections.parquet"]["rows"] == 2
    assert receipt["captured"]["entry"] == old and "etlReceipts" not in old
    assert receipt["policy_versions"] == {"cfr_sections": "regulations-native-v1"}
    assert receipt["tables"]["cfr_sections"] == {
        "retained_rows": 2, "subject_rows": 2, "rows_only_in_restored": 0, "rows_only_in_retained": 0,
        "columns_only_in_restored": [], "columns_only_in_retained": [], "type_changes": {}, "metadata_changes": []}
    assert public_rows(receipt) == {"cfr_sections": 2}
    assert json.loads((tmp_path / "work" / conversion.RECEIPT).read_text()) == receipt
    # The read every scheduled run makes now restores exactly the rows the builder last wrote.
    restored = pq.read_table(SelectedPriors(tmp_path / "after", public_url=BASE).get("cfr_sections")).to_pylist()
    assert sorted(restored, key=lambda row: row["granule_id"]) == sorted(CFR, key=lambda row: row["granule_id"])
    assert pq.read_schema(receipt["generation"]["directory"] + "/cfr_sections.parquet").field("part_granule").type == pa.bool_()


BILL_SUBJECTS = [
    {"bill_id": "107-hconres-1", "policy_area": "Congress", "carrier": "congress-api",
     "subjects_json": '["Congressional sessions", "House of Representatives", "Senate"]', "subject_count": "3",
     "enriched_at": "2026-09-24T21:22:53+00:00"},
    {"bill_id": "119-hr-1", "policy_area": None, "subjects_json": "[]", "subject_count": "0", "carrier": "billstatus",
     "enriched_at": "2026-09-25T21:22:53+00:00"},
]


def test_bill_subjects_converts_so_its_scheduled_rollup_can_read_its_prior(tmp_path, monkeypatch, bucket):
    """The nightly has failed since 2026-10-04: congress_bills until 10-06, then bill_subjects, which has no receipts.

    The live family carries evidence with no evidence_version key. Its one conversion must still proceed, hand nothing
    on, publish current-version evidence, and leave a prior the scheduled rollup reads back as the rows it last wrote.
    """
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.source_evidence import EVIDENCE_VERSION

    publish_old_with_evidence(tmp_path, monkeypatch, bucket, "bill-subjects", "bill_subjects", BILL_SUBJECTS,
                              unversioned=True)
    old = stored(bucket, "bill-subjects")
    assert "evidence_version" not in evidence_root(bucket, published_evidence_pin(bucket, "bill-subjects"))["spec"]
    refused_read(tmp_path, "bill_subjects")

    receipt = convert("bill-subjects", tmp_path / "work", publish=True)

    entry = stored(bucket, "bill-subjects")
    assert entry == receipt["published"]["entry"] and "etlReceipts" not in old
    assert "bill_subjects" in entry["etlReceipts"]["datasets"] and entry["tables"]["bill_subjects.parquet"]["rows"] == 2
    assert public_rows(receipt)["bill_subjects"] == 2 and receipt["lineage"]["handed_on_events"] == 0
    new_pin = published_evidence_pin(bucket, "bill-subjects")
    assert evidence_root(bucket, new_pin)["spec"]["evidence_version"] == EVIDENCE_VERSION
    assert publication.load_evidence_journal(BASE, new_pin)
    restored = pq.read_table(SelectedPriors(tmp_path / "after", public_url=BASE).get("bill_subjects")).to_pylist()
    assert sorted(restored, key=lambda row: row["bill_id"]) == BILL_SUBJECTS


def test_without_publish_nothing_leaves_the_work_directory(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    written = list(bucket.writes)

    receipt = convert("cfr-sections", tmp_path / "work")

    assert receipt["published"] is None and "read_back" not in receipt
    assert receipt["tables"]["cfr_sections"]["rows_only_in_retained"] == 0
    assert bucket.writes == written
    refused_read(tmp_path, "cfr_sections")


def test_document_attributes_cli_converts_typed_rows_without_publication(tmp_path, monkeypatch, bucket):
    """An attribute family's typed rows through the converter (comment-attributes ran here until its receipts were
    retired, ss-vse3: a retired table never converts back into receipts, as the next test shows)."""
    from spicy_regs import regulations_bulk
    from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
    from spicy_regs.transforms.regulations_shape import TYPES

    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS["document_attributes"]])
    table = pa.Table.from_pylist([
        {"document_id": "D-1", "page_count": 3, "open_for_comment": False, "city": "é😀"},
        {"document_id": "D-2", "page_count": 0, "open_for_comment": True, "address1": "escape\x1b"},
        {"document_id": "D-3"},
    ], schema=schema)
    old = publish_old(bucket, monkeypatch, tmp_path, "document-attributes", {"document_attributes": table})
    written = list(bucket.writes)
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    monkeypatch.setenv("SPICY_REGS_REQUIRE_BULK", "true")
    routes = []
    writer = regulations_bulk.write_held_dataset

    def observed(*args, **kwargs):
        routes.append(args[0])
        return writer(*args, **kwargs)

    monkeypatch.setattr(regulations_bulk, "write_held_dataset", observed)
    work = tmp_path / "attributes"
    assert conversion.main(["document-attributes", "--allow", "document-attributes", "--work", str(work),
                            "--expect-main", MAIN, "--expect-spicy-docs", WHEEL]) == 0
    receipt = json.loads((work / conversion.RECEIPT).read_text())
    assert routes == ["document_attributes"]
    assert receipt["captured"]["entry"] == old
    assert receipt["published"] is None and bucket.writes == written
    assert receipt["tables"]["document_attributes"] == {
        "retained_rows": 3, "subject_rows": 3, "rows_only_in_restored": 0, "rows_only_in_retained": 0,
        "columns_only_in_restored": [], "columns_only_in_retained": [], "type_changes": {}, "metadata_changes": []}


def test_a_retired_attribute_table_never_converts_into_receipts(tmp_path, monkeypatch, bucket):
    from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
    from spicy_regs.transforms.regulations_shape import TYPES

    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS["comment_attributes"]])
    old = publish_old(bucket, monkeypatch, tmp_path, "comment-attributes",
                      {"comment_attributes": pa.Table.from_pylist([{"comment_id": "C-1"}], schema=schema)})
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match=r"Receipts are retired for \['comment_attributes'\]"):
        convert("comment-attributes", tmp_path / "work", publish=True)
    assert bucket.writes == written
    assert publication.current_index(BASE)["families"]["comment-attributes"] == old


def test_a_court_family_converts_through_the_court_writer(tmp_path, monkeypatch, bucket):
    from spicy_regs.court_receipts import prior_receipt_selection, restore_processing_input

    old = publish_old(bucket, monkeypatch, tmp_path, "courtlistener", {"court_dockets": [DOCKET]})

    receipt = convert("courtlistener", tmp_path / "work", publish=True)

    entry = publication.current_index(BASE)["families"]["courtlistener"]
    assert entry["etlReceipts"]["datasets"] == ["court_dockets"] and receipt["policy_versions"] == {"court_dockets": "courts/1"}
    assert public_rows(receipt) == {"court_dockets": 1}
    (member,) = publication.table_members(publication.current_index(BASE), "court_dockets.parquet")
    subject = tmp_path / "published" / "court_dockets.parquet"
    subject.parent.mkdir()
    publication.fetch_member(BASE, member, subject, member.path)
    assert pq.read_table(subject).to_pylist()[0]["parties"] == ["County", None, ""]
    receipts, generation = prior_receipt_selection(subject, dataset="court_dockets")
    restored = restore_processing_input(subject, tmp_path / "restored.parquet", dataset="court_dockets",
                                        schema=pa.schema([(name, pa.string()) for name in DOCKET]),
                                        receipt_path=receipts, generation_id=generation)
    assert pq.read_table(restored).to_pylist() == [DOCKET]
    # Each receipt's witness is the published object it was converted from, not a path on the converting machine.
    (witness,) = pq.read_table(receipts)["witnesses"][0].as_py()
    (retained,) = publication.table_members({"families": {"courtlistener": old}}, "court_dockets.parquet")
    assert (witness["source_id"], witness["sha256"], witness["source_uri"]) == (retained.path, retained.sha256, None)


@pytest.mark.parametrize("state, message", [
    ({"main": "1e" * 20}, "main is 1e1e1e1e1e1e, not the 0f0f0f0f0f0f this run was started for"),
    ({"checkout": "2d" * 20}, "this checkout is 2d2d2d2d2d2d, not main 0f0f0f0f0f0f"),
    ({"uncommitted": [" M src/spicy_regs/etl_receipts.py"]}, "uncommitted changes"),
    ({"spicy_docs": "0.55.0"}, "SpicyDocs is 0.55.0 installed and 0.56.0 pinned"),
    ({"spicy_docs": "0.57.0", "pinned_spicy_docs": "0.57.0"}, "not the 0.56.0 this run was started for"),
])
def test_a_moved_main_or_wheel_refuses_before_anything_is_read(tmp_path, monkeypatch, bucket, state, message):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    with pytest.raises(conversion.ConversionRefused, match=message):
        convert("cfr-sections", tmp_path / "work", publish=True, state=lambda remote: STATE | state)
    assert not (tmp_path / "work").exists()


def test_main_moving_during_the_conversion_refuses_the_publication(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    states = iter([dict(STATE), STATE | {"main": "1e" * 20}])
    with pytest.raises(conversion.ConversionRefused, match="Main or the pinned wheel moved"):
        convert("cfr-sections", tmp_path / "work", publish=True, state=lambda remote: next(states))
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old


def test_only_a_named_old_shape_family_with_a_writer_converts(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    publish_old(bucket, monkeypatch, tmp_path, "unmapped-fixture", {"unmapped_fixture": [{"id": "C-1"}]})
    written = list(bucket.writes)
    for family, options, message in [
        ("cfr-sections", {"allowed": ["amendments", "treaties"]}, "not in this run's allow-list"),
        ("nominations", {}, "nominations is not a published family"),
        ("unmapped-fixture", {}, "no subject/receipt rollup or court writer"),
    ]:
        with pytest.raises(conversion.ConversionRefused, match=message):
            convert(family, tmp_path / family, publish=True, **options)
    assert bucket.writes == written

    convert("cfr-sections", tmp_path / "first", publish=True)
    with pytest.raises(conversion.ConversionRefused, match="already carries ETL receipts"):
        convert("cfr-sections", tmp_path / "second", publish=True)
    with pytest.raises(conversion.ConversionRefused, match="is not empty"):
        convert("cfr-sections", tmp_path / "first", publish=True)


def test_families_of_one_source_convert_in_turn_and_roll_back_alone(tmp_path, monkeypatch, bucket):
    """Every Congress.gov family's receipts hold ``congress_acquisition``; it is a shared log, so no entry claims it.

    Before logs were shared the first such family took the dataset and publication refused the second ("already
    belongs to family"). Found rehearsing the small families, 2026-10-05.
    """
    from spicy_regs.congress_subjects import INPUT_COLUMNS
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    treaty = dict.fromkeys(INPUT_COLUMNS["treaties"]) | {"treaty_id": "119-1", "congress_received": "119"}
    nomination = dict.fromkeys(INPUT_COLUMNS["nominations"]) | {"citation": "PN1-119", "congress": "119"}
    old = publish_old(bucket, monkeypatch, tmp_path, "treaties", {"treaties": [treaty]})
    publish_old(bucket, monkeypatch, tmp_path, "nominations", {"nominations": [nomination]})

    receipts = {family: convert(family, tmp_path / family, publish=True) for family in ("treaties", "nominations")}

    families = publication.current_index(BASE)["families"]
    for family in receipts:
        entry = families[family]
        expected_datasets = [family, "nominations_detail_reads"] if family == "nominations" else [family]
        assert entry == receipts[family]["published"]["entry"]
        assert entry["etlReceipts"]["datasets"] == expected_datasets
        held = pq.read_table(receipts[family]["generation"]["directory"] + "/etl_receipts.parquet")
        assert set(held["generation_id"].to_pylist()) == {entry["etlReceipts"]["generationId"]}
        assert set(held["dataset"].to_pylist()) == {family, "congress_acquisition"}
    # Each family's next scheduled run restores its own rows from its own receipt member.
    for family, row in (("treaties", treaty), ("nominations", nomination)):
        assert pq.read_table(SelectedPriors(tmp_path / "after" / family, public_url=BASE).get(family)).to_pylist() == [row]

    conversion.rollback(tmp_path / "treaties" / conversion.RECEIPT, expect_bucket=BUCKET)
    families = publication.current_index(BASE)["families"]
    assert families["treaties"] == old and families["nominations"] == receipts["nominations"]["published"]["entry"]
    refused_read(tmp_path, "treaties")
    assert pq.read_table(SelectedPriors(tmp_path / "kept", public_url=BASE).get("nominations")).to_pylist() == [nomination]


def test_a_receipt_dataset_another_family_owns_refuses_before_converting(tmp_path, monkeypatch, bucket):
    """A dataset an entry lists has one owner; publication would refuse a second, which a dry run never reaches."""
    from spicy_regs.congress_receipts import policy
    from spicy_regs.pipelines.rollups.congress_index import TreatiesRollup

    publish_old(bucket, monkeypatch, tmp_path, "nominations", {"nominations": [{"citation": "PN1-119"}]})
    old = publish_old(bucket, monkeypatch, tmp_path, "treaties", {"treaties": [{"treaty_id": "119-1"}]})
    # Stands for any dataset without the shared-log marker that a second family's rollup comes to declare.
    monkeypatch.setattr(TreatiesRollup, "receipt_policies", (*TreatiesRollup.receipt_policies, policy("nominations")))

    with pytest.raises(conversion.ConversionRefused,
                       match=r"would hold \['nominations'\], which already belongs to family nominations"):
        convert("treaties", tmp_path / "treaties")
    assert not (tmp_path / "treaties" / "build").exists()
    assert publication.current_index(BASE)["families"]["treaties"] == old


def test_a_row_the_writer_refuses_stops_the_conversion(tmp_path, monkeypatch, bucket):
    unnamed = CFR[0] | {"granule_id": None}
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": [CFR[1], unnamed]})
    with pytest.raises(conversion.ConversionRefused, match="cfr-sections: Subject conversion refused"):
        convert("cfr-sections", tmp_path / "rollup", publish=True)
    courts = publish_old(bucket, monkeypatch, tmp_path, "courtlistener",
                         {"court_dockets": [DOCKET, DOCKET | {"cl_docket_id": "9", "parties_json": "not a list"}]})
    with pytest.raises(conversion.ConversionRefused, match=r"receipts hold \{'refused': 1\}"):
        convert("courtlistener", tmp_path / "court", publish=True)
    assert json.loads((tmp_path / "court" / conversion.RECEIPT).read_text())["receipt_outcomes"] == {"accepted": 1, "refused": 1}
    families = publication.current_index(BASE)["families"]
    assert (families["cfr-sections"], families["courtlistener"]) == (old, courts)


def test_rollback_restores_the_captured_entry_and_keeps_the_converted_objects(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    receipt = convert("cfr-sections", tmp_path / "work", publish=True)
    converted = receipt["published"]["entry"]

    rolled = conversion.rollback(tmp_path / "work" / conversion.RECEIPT, expect_bucket=BUCKET)

    assert publication.current_index(BASE)["families"]["cfr-sections"] == old == rolled["rolled_back"]["entry"]
    assert f"{converted['prefix']}/etl_receipts.parquet" in bucket.objects
    refused_read(tmp_path, "cfr_sections")
    # Rolling back again changes nothing, and a dry receipt has nothing to roll back.
    writes = list(bucket.writes)
    conversion.rollback(tmp_path / "work" / conversion.RECEIPT, expect_bucket=BUCKET)
    assert bucket.writes == writes
    convert("cfr-sections", tmp_path / "dry")
    with pytest.raises(conversion.ConversionRefused, match="records no publish attempt"):
        conversion.rollback(tmp_path / "dry" / conversion.RECEIPT, expect_bucket=BUCKET)


def test_rollback_does_not_discard_a_later_generation_unasked(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    convert("cfr-sections", tmp_path / "work", publish=True)
    index = publication.current_index(BASE)
    later = index["families"]["cfr-sections"] | {"artifactDigest": "sha256:" + "c" * 64, "prefix": "generations/cfr-sections/" + "c" * 64}
    bucket.objects[publication.INDEX_V2_KEY] = canonical_json_bytes(index | {"families": index["families"] | {"cfr-sections": later}})

    with pytest.raises(conversion.ConversionRefused, match="neither the captured generation nor this conversion's"):
        conversion.rollback(tmp_path / "work" / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"]["cfr-sections"] == later
    conversion.rollback(tmp_path / "work" / conversion.RECEIPT, discard_newer=True, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old
    # An earlier generation whose table is gone cannot be pointed at.
    del bucket.objects[f"{old['prefix']}/cfr_sections.parquet"]
    with pytest.raises(publication.PublicationError, match="cfr_sections.parquet is no longer stored as published"):
        publication.restore_family(bucket, "spicy-regs", "cfr-sections", old, expected=None)


def test_the_command_line_names_one_family_and_its_allow_list(tmp_path, monkeypatch, bucket, capsys):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    common = ["--work", str(tmp_path / "work"), "--expect-main", MAIN, "--expect-spicy-docs", WHEEL]

    assert conversion.main(["cfr-sections", "--allow", "amendments,treaties", *common]) == 1
    assert "REFUSED: cfr-sections is not in this run's allow-list (amendments, treaties)" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        conversion.main(["cfr-sections", "--allow", "cfr-sections", "--work", str(tmp_path / "work")])
    assert conversion.main(["cfr-sections", "--allow", "amendments,cfr-sections", *common]) == 0
    assert "not published (no --publish)" in capsys.readouterr().out
    assert "etlReceipts" not in publication.current_index(BASE)["families"]["cfr-sections"]


V2 = publication.INDEX_V2_KEY
COMMON = ["--allow", "cfr-sections", "--expect-main", MAIN, "--expect-spicy-docs", WHEEL, "--remote", "fork"]


def public_rows(receipt):
    """Row counts the public pointer names after read-back, by dataset."""
    return {key.removesuffix(".parquet"): rows for key, rows in receipt["read_back"]["table_rows"].items()}


def stored(store, family="cfr-sections"):
    return publication.parse_index(store.objects[V2])["families"][family]


def test_a_pointer_write_whose_response_was_lost_is_found_and_finished(tmp_path, monkeypatch, bucket, capsys):
    """The conditional PUT lands, the client's retry answers 412, and publication raises: the family IS converted."""
    from tests.generation_fakes import error

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    real, lost = bucket.put_object, []

    def put_object(**kwargs):
        result = real(**kwargs)
        if kwargs["Key"] == V2 and not lost:
            lost.append(kwargs["Key"])
            raise error("PreconditionFailed")
        return result

    monkeypatch.setattr(bucket, "put_object", put_object)
    work = tmp_path / "work"
    assert conversion.main(["cfr-sections", *COMMON, "--work", str(work), "--publish", "--expect-bucket", BUCKET]) == 0

    receipt = json.loads((work / conversion.RECEIPT).read_text())
    assert lost and stored(bucket) == receipt["published"]["entry"] != old
    assert receipt["publish_attempt"]["outcome"] == "moved to this conversion's generation"
    assert "Family changed since the build read its inputs" in receipt["publish_attempt"]["error"]
    assert public_rows(receipt) == {"cfr_sections": 2}
    assert "published and read back" in capsys.readouterr().out


def test_lost_publish_response_followed_by_recovery_failure_reports_actual_uncertainty(
    tmp_path, monkeypatch, bucket, capsys
):
    from tests.generation_fakes import error

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    real, lost = bucket.put_object, []

    def put_object(**kwargs):
        result = real(**kwargs)
        if kwargs["Key"] == V2 and not lost:
            lost.append(True)
            raise error("PreconditionFailed")
        return result

    def fail(*args, **kwargs):
        raise publication.PublicationError("recovery failed")

    work = tmp_path / "work"
    with monkeypatch.context() as recovery:
        recovery.setattr(bucket, "put_object", put_object)
        recovery.setattr(publication, "stored_family", fail)
        assert conversion.main(["cfr-sections", *COMMON, "--work", str(work), "--publish", "--expect-bucket", BUCKET]) == 1
    report = capsys.readouterr().err
    assert conversion.NOTHING_PUBLISHED not in report
    assert "result is not known" in report
    receipt = json.loads((work / conversion.RECEIPT).read_text())
    assert stored(bucket)["artifactDigest"] == receipt["generation"]["artifactDigest"]
    assert receipt["captured"]["entry"] == old
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert stored(bucket) == old


@pytest.mark.parametrize("failed_step", ["write", "replace"])
def test_interrupted_receipt_rewrite_keeps_previous_complete_record(tmp_path, monkeypatch, failed_step):
    path = tmp_path / conversion.RECEIPT
    original = {"captured": {"entry": {"artifactDigest": "retained"}}, "publish_attempt": {"outcome": "attempted"}}
    conversion._write_receipt(path, original)
    before = path.read_bytes()

    def fail(*args, **kwargs):
        if failed_step == "write":
            args[1].write('{"captured":')
        raise KeyboardInterrupt()

    if failed_step == "write":
        monkeypatch.setattr(conversion.json, "dump", fail)
    else:
        monkeypatch.setattr(conversion.os, "replace", fail)
    with pytest.raises(KeyboardInterrupt):
        conversion._write_receipt(path, {**original, "published": {"entry": {"artifactDigest": "new"}}})
    assert path.read_bytes() == before and json.loads(path.read_text()) == original
    assert list(tmp_path.iterdir()) == [path]


def test_receipt_replace_failure_after_publish_preserves_rollback_record(tmp_path, monkeypatch, bucket, capsys):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    work = tmp_path / "work"
    path = work / conversion.RECEIPT
    real = conversion.os.replace

    def fail_after_publish(source, target):
        if target == path and json.loads(source.read_text()).get("published"):
            raise OSError("interrupted receipt replacement")
        return real(source, target)

    with monkeypatch.context() as interrupted:
        interrupted.setattr(conversion.os, "replace", fail_after_publish)
        assert conversion.main(["cfr-sections", *COMMON, "--work", str(work), "--publish", "--expect-bucket", BUCKET]) == 2
    report = capsys.readouterr().err
    assert conversion.NOTHING_PUBLISHED not in report and "result is not known" in report
    receipt = json.loads(path.read_text())
    assert receipt["captured"]["entry"] == old and receipt["published"] is None
    assert receipt["generation"]["artifactDigest"] == stored(bucket)["artifactDigest"]
    conversion.rollback(path, expect_bucket=BUCKET)
    assert stored(bucket) == old


def test_a_process_stopped_after_the_pointer_write_leaves_a_receipt_that_rolls_back(tmp_path, monkeypatch, bucket, capsys):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    work = tmp_path / "work"
    put_pointer = publication._put_pointer

    def stop_after_the_pointer_moved(client, bucket_name, key, raw, etag):
        moved = put_pointer(client, bucket_name, key, raw, etag)
        if key == V2:
            raise KeyboardInterrupt()
        return moved

    with monkeypatch.context() as stopped:
        stopped.setattr(publication, "_put_pointer", stop_after_the_pointer_moved)
        assert conversion.main(["cfr-sections", *COMMON, "--work", str(work), "--publish", "--expect-bucket", BUCKET]) == 130
    report = capsys.readouterr().err
    assert "a publish was attempted and its result is not known" in report
    assert f"--rollback {work / conversion.RECEIPT} --expect-bucket {BUCKET}" in report
    receipt = json.loads((work / conversion.RECEIPT).read_text())
    assert receipt["publish_attempt"]["outcome"] == "attempted" and receipt["published"] is None
    assert stored(bucket)["artifactDigest"] == receipt["generation"]["artifactDigest"]  # the pointer did move

    assert conversion.main(["--rollback", str(work / conversion.RECEIPT), "--expect-bucket", BUCKET]) == 0
    assert stored(bucket) == old and "found this conversion's generation" in capsys.readouterr().out


def test_a_refused_pointer_write_reports_the_state_it_left(tmp_path, monkeypatch, bucket, capsys):
    """Another writer moves the family between capture and publish: refused, and the report names what is stored."""
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    other = old | {"artifactDigest": "sha256:" + "c" * 64, "prefix": "generations/cfr-sections/" + "c" * 64}
    real = publication._publish_verified_generation

    def publish(*arguments, **options):
        index = publication.parse_index(bucket.objects[V2])
        bucket.objects[V2] = canonical_json_bytes({**index, "version": 2, "families": index["families"] | {"cfr-sections": other}})
        return real(*arguments, **options)

    monkeypatch.setattr(publication, "_publish_verified_generation", publish)
    work = tmp_path / "work"
    assert conversion.main(["cfr-sections", *COMMON, "--work", str(work), "--publish", "--expect-bucket", BUCKET]) == 1
    report = capsys.readouterr().err
    assert "REFUSED: cfr-sections: PublicationError: Family changed since the build read its inputs" in report
    assert "STATE: cfr-sections now names sha256:" + "c" * 64 in report and "rolls nothing back" in report
    assert json.loads((work / conversion.RECEIPT).read_text())["publish_attempt"]["outcome"] == "moved elsewhere"
    assert stored(bucket) == other
    # The same refusal with the family untouched says nothing was published.
    bucket.objects[V2] = canonical_json_bytes({"format": publication.parse_index(bucket.objects[V2])["format"], "version": 2,
                                               "families": {"cfr-sections": old}})
    monkeypatch.setattr(publication, "_publish_verified_generation",
                        lambda *_a, **_k: (_ for _ in ()).throw(publication.PublicationError("Refusing to shrink")))
    assert conversion.main(["cfr-sections", *COMMON, "--work", str(tmp_path / "again"), "--publish", "--expect-bucket", BUCKET]) == 1
    report = capsys.readouterr().err
    assert "REFUSED: cfr-sections: PublicationError: Refusing to shrink" in report and f"STATE: {conversion.NOTHING_PUBLISHED}" in report


@pytest.mark.parametrize("call", [1, 2], ids=["writer-validation", "generation-admission"])
@pytest.mark.parametrize(
    "failure, code, report",
    [
        (duckdb.InvalidInputException("date32 outside Python date range"), 1, "REFUSED: cfr-sections: "),
        (duckdb.OutOfMemoryException("injected"), 2, "FAILED: BulkValidationError: "),
    ],
    ids=["invalid-input", "operational"],
)
def test_a_duckdb_failure_in_bulk_admission_reports_invalid_input_as_a_refusal(
    tmp_path, monkeypatch, bucket, capsys, call, failure, code, report
):
    """Invalid input data stays a refusal (exit 1) and an operational failure does not (exit 2); nothing is published."""
    from spicy_regs import etl_bulk

    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    written, check, seen = list(bucket.writes), etl_bulk._check_receipts, []

    def check_receipts(*args, **kwargs):
        seen.append(call)
        if len(seen) == call:
            raise failure
        return check(*args, **kwargs)

    monkeypatch.setattr(etl_bulk, "_check_receipts", check_receipts)
    arguments = ["cfr-sections", *COMMON, "--work", str(tmp_path / "work"), "--publish", "--expect-bucket", BUCKET]
    assert conversion.main(arguments) == code
    error = capsys.readouterr().err
    assert report in error and "no row fallback: " in error and f"STATE: {conversion.NOTHING_PUBLISHED}" in error
    assert len(seen) == call and bucket.writes == written


def test_the_write_target_is_named_checked_first_and_recorded(tmp_path, monkeypatch, bucket, capsys):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    run = ["cfr-sections", *COMMON, "--publish"]
    with pytest.raises(SystemExit):
        conversion.main([*run, "--work", str(tmp_path / "unnamed")])
    assert conversion.main([*run, "--work", str(tmp_path / "other"), "--expect-bucket", "another-bucket"]) == 1
    assert "would write to bucket spicy-regs, not the --expect-bucket another-bucket" in capsys.readouterr().err
    assert not (tmp_path / "other").exists()  # refused before anything was converted
    # A setting exported in the shell that the env-file contradicts is refused by name, never by value.
    env = tmp_path / "r2.env"
    env.write_text("R2_BUCKET_NAME=some-other-bucket\nR2_ENDPOINT=https://account.r2.invalid\n")
    assert conversion.main([*run, "--work", str(tmp_path / "env"), "--expect-bucket", BUCKET, "--env-file", str(env)]) == 1
    report = capsys.readouterr().err
    assert "R2_BUCKET_NAME in the environment differ from" in report and "some-other-bucket" not in report
    monkeypatch.setattr(r2, "require_credentials", lambda action: (_ for _ in ()).throw(RuntimeError(f"{action} requires R2 credentials")))
    assert conversion.main([*run, "--work", str(tmp_path / "keyless"), "--expect-bucket", BUCKET]) == 1
    assert "requires R2 credentials" in capsys.readouterr().err and not (tmp_path / "keyless").exists()
    monkeypatch.undo()
    with pytest.raises(SystemExit):  # a rollback takes no conversion arguments
        conversion.main(["cfr-sections", "--rollback", str(tmp_path / "x.json"), "--expect-bucket", BUCKET])


def test_the_published_receipt_names_its_target(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    receipt = convert("cfr-sections", tmp_path / "work", publish=True)
    assert receipt["target"] == {"bucket": BUCKET, "endpoint_host": "account.r2.invalid"}
    monkeypatch.setenv("R2_ENDPOINT", "https://elsewhere.r2.invalid")
    with pytest.raises(conversion.ConversionRefused, match="was written to"):
        conversion.rollback(tmp_path / "work" / conversion.RECEIPT, expect_bucket=BUCKET)


def test_a_checkout_that_could_load_something_else_is_refused():
    def refused(**changed):
        with pytest.raises(conversion.ConversionRefused) as refusal:
            conversion._refuse_moved(STATE | changed, main=MAIN, spicy_docs=WHEEL)
        return str(refusal.value)

    conversion._refuse_moved(STATE, main=MAIN, spicy_docs=WHEEL)
    assert "untracked files the command could load: src/spicy_regs/pipelines/rollups/extra.py" in refused(
        untracked=["src/spicy_regs/pipelines/rollups/extra.py"])
    wheel = PINNED_WHEEL
    assert "uv.lock pins" in refused(spicy_docs_wheel=wheel | {"file_sha256": "sha256:" + "cd" * 32})
    assert "uv.lock pins" in refused(spicy_docs_wheel=wheel | {"locked_sha256": None, "file_sha256": None})
    assert "differs from the pinned wheel in spicy_docs/reader.py" in refused(
        spicy_docs_wheel=wheel | {"installed_differs": ["spicy_docs/reader.py"]})


def test_main_is_read_from_a_hosted_remote_and_the_installed_wheel_is_the_pinned_one(monkeypatch):
    import subprocess

    with pytest.raises(conversion.ConversionRefused, match="git remote failed"):
        conversion.source_state("no-such-remote")
    real = subprocess.run

    def local(command, **options):
        if command[3:5] == ["remote", "get-url"]:
            return subprocess.CompletedProcess(command, 0, stdout="/tmp/a-clone\n", stderr="")
        return real(command, **options)

    monkeypatch.setattr(subprocess, "run", local)
    with pytest.raises(conversion.ConversionRefused, match="not a hosted repository"):
        conversion.source_state("anything")
    monkeypatch.undo()
    # This checkout's own lock, vendored wheel and installed files agree; the check re-hashes the installed files.
    from pathlib import Path
    import tomllib

    root = Path(conversion.__file__).resolve().parents[2]
    locked = next(p for p in tomllib.loads((root / "uv.lock").read_text())["package"] if p["name"] == "spicy-docs")
    state = conversion._wheel_state(root, locked)
    assert state["locked_sha256"] == state["file_sha256"] and state["installed_differs"] == []
    assert conversion._wheel_state(root, locked | {"source": {"registry": "https://example.invalid"}})["file_sha256"] is None


GROUP = {"cl_docket_id": "1", "parent_cl_docket_id": "2", "confidence_tier": "exact", "edition": "2026-06-30",
         "rule_version": "v1", "match_basis": "docket_number", "group_size": "3"}


def test_declared_native_court_conversion_keeps_original_processing_types(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, "court-docket-groups", {"court_docket_groups": [GROUP]})
    receipt = convert("court-docket-groups", tmp_path / "work", publish=True)
    check = receipt["tables"]["court_docket_groups"]
    assert check["type_changes"] == {} and check["rows_only_in_retained"] == 0
    subject = pq.read_table(receipt["generation"]["directory"] + "/court_docket_groups.parquet")
    assert subject["group_size"].type == pa.int64() and subject["group_size"].to_pylist() == [3]
    assert public_rows(receipt) == {"court_docket_groups": 1}


def test_file_metadata_a_later_run_reads_must_survive(tmp_path, monkeypatch, bucket):
    """The CFR placement marker is file metadata the next CFR run depends on; any metadata change is a difference."""
    import importlib

    cfr = importlib.import_module("spicy_regs.transforms.build_cfr_sections")  # the package exports a function by this name
    table = pa.Table.from_pylist(CFR, schema=pa.schema([(column, pa.string()) for column in CFR[0]]))
    marked, plain = tmp_path / "marked.parquet", tmp_path / "plain.parquet"
    pq.write_table(table.replace_schema_metadata(dict(cfr.PLACEMENT_MARKER)), marked)
    pq.write_table(table, plain)
    assert conversion._differences(marked, marked)["metadata_changes"] == []
    assert conversion._differences(plain, marked)["metadata_changes"] == sorted(cfr.PLACEMENT_MARKER)
    # Through the converter: the marker the published table carries is on the table the next run restores.
    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation

    path = tmp_path / "old" / "cfr_sections.parquet"
    path.parent.mkdir()
    pq.write_table(table.replace_schema_metadata(dict(cfr.PLACEMENT_MARKER)), path)
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(tmp_path / "old-generation", family="cfr-sections", files=[path], expected_keys=[path.name])
        publication.publish_generation(tmp_path / "old-generation", client=bucket, bucket=BUCKET,
                                       prior_index=publication.current_index(BASE))
    assert convert("cfr-sections", tmp_path / "work")["tables"]["cfr_sections"]["metadata_changes"] == []


def test_nested_values_are_compared_as_values_not_as_text(tmp_path):
    """Cast to text, DuckDB prints ['a, b'] and ['a', 'b'] alike; one difference each way must be found."""
    one, two = tmp_path / "one.parquet", tmp_path / "two.parquet"
    pq.write_table(pa.table({"names": pa.array([["a, b"]], pa.list_(pa.string()))}), one)
    pq.write_table(pa.table({"names": pa.array([["a", "b"]], pa.list_(pa.string()))}), two)
    check = conversion._differences(one, two)
    assert (check["rows_only_in_restored"], check["rows_only_in_retained"], check["type_changes"]) == (1, 1, {})


def test_a_rejected_row_stops_the_conversion_like_a_refused_one():
    assert conversion.CONVERTED_OUTCOMES == {"accepted", "observed"}
    from spicy_regs.etl_receipts import OUTCOMES

    assert OUTCOMES - conversion.CONVERTED_OUTCOMES == {"rejected", "refused", "error"}


def test_every_journal_event_a_run_reads_back_is_handed_on():
    """A builder that starts reading another event back from its prior must be added to the conversion's list."""
    import re
    from pathlib import Path

    source = Path(conversion.__file__).parent
    read_back = {name for path in source.rglob("*.py") for name in re.findall(r'inherited_event\(\s*"([^"]+)"', path.read_text())}
    assert read_back == set(conversion.INHERITED_EVENTS)


def test_a_converted_family_keeps_its_evidence_lineage(tmp_path, monkeypatch, bucket):
    """The converted generation names the one it replaces and hands on what the next run reads from its journal."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE, CaptureEvidence

    def old_with_evidence(family, name, rows, **journaled):
        path = tmp_path / family / f"{name}.parquet"
        path.parent.mkdir()
        pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(column, pa.string()) for column in rows[0]])), path)
        evidence = CaptureEvidence(tmp_path / family, family)
        evidence.inherit(publication.current_index(BASE), public_url=BASE)
        for event, fields in journaled.items():
            evidence.event(event.replace("_", "-"), **fields)
        with monkeypatch.context() as patch:
            patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
            build_generation(tmp_path / family / "generation", family=family, files=[path], expected_keys=[path.name],
                             inputs=evidence.inputs(), read_snapshot=publication.current_index(BASE))
            index = publication.publish_generation(tmp_path / family / "generation", client=bucket, bucket=BUCKET,
                                                   prior_index=publication.current_index(BASE),
                                                   evidence_directories=(evidence.artifact_dir,))
        evidence.finish()
        return index["families"][family]

    def next_run(family):
        evidence = CaptureEvidence(tmp_path / "next" / family, family)
        evidence.inherit(publication.current_index(BASE), public_url=BASE)
        lineage = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()][-1]
        return evidence, lineage["evidence_status"]

    remainder = {"table": "treaties", "unevidenced": [["119-1"]], "shape_version": 2}
    crs_discovery = {"cursor": "2026-11-05T13:00:00+00:00", "since": "2026-11-02", "mode": "refresh"}
    old = {"courtlistener": old_with_evidence("courtlistener", "court_dockets", [DOCKET]),
           "treaties": old_with_evidence("treaties", "treaties", [{"treaty_id": "119-1"}],
                                         congress_index_selection=remainder, crs_discovery=crs_discovery)}
    for family in old:
        receipt = convert(family, tmp_path / "work" / family, publish=True)
        entry = publication.current_index(BASE)["families"][family]
        root = json.loads(bucket.objects[entry["prefix"] + "/artifact.json"])
        assert {"role": PRIOR_ROLE, "logicalId": old[family]["logicalId"], "artifactDigest": old[family]["artifactDigest"]} in root["inputs"]
        assert [item["role"] for item in root["inputs"]].count(INPUT_ROLE) == 1 and receipt["lineage"]["inputs"] == root["inputs"]
        evidence, status = next_run(family)
        assert status == "Inherited pins; prior source coverage is not requalified by this run."
        if family == "treaties":
            assert receipt["lineage"]["handed_on_events"] == 2
            handed = evidence.inherited_event("congress-index-selection", table="treaties")
            assert handed is not None and handed["unevidenced"] == [["119-1"]] and handed["shape_version"] == 2
            # The data-independent copy path retains the CRS cursor for its actual
            # discovery consumer; conversion must not replace it with its own time.
            from spicy_regs.transforms.build_crs_reports import _discovery_state
            state = _discovery_state(evidence)
            assert {name: value for name, value in state.items() if name not in {"event", "recorded_at"}} == crs_discovery


def publish_old_with_evidence(
    tmp_path,
    monkeypatch,
    store,
    family="cfr-sections",
    table="cfr_sections",
    rows=CFR,
    *,
    evidence_version=None,
    unversioned=False,
    outcome="build-complete",
    **journaled,
):
    """Publish ``family`` as it was before receipts, carrying source evidence of ``evidence_version`` (default: current).

    ``journaled`` events (``name_with_underscores=fields``) are written to the evidence journal; an ``outcome`` other
    than ``build-complete`` publishes evidence the way no live family has it, to test the refusal.
    """
    from spicy_regs import etl_policy_registry, source_evidence
    from spicy_regs.generations import build_generation

    directory = tmp_path / family
    directory.mkdir()
    path = directory / f"{table}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(column, pa.string()) for column in rows[0]])), path)
    with monkeypatch.context() as patch:
        # Everything published here, including the evidence, is written the way the live pre-v2 families were.
        if evidence_version is not None:
            patch.setattr(source_evidence, "EVIDENCE_VERSION", evidence_version)
        if unversioned:
            # Publication verifies what it publishes at the version it believes current; let the old shape through.
            patch.setattr(source_evidence, "_require_current_version", lambda spec: None)
            # The live bill-subjects, lobbying-filings and FEC evidence carries no evidence_version key at all.
            build = source_evidence.build_artifact_root

            def without_version(*, spec, **rest):
                return build(spec={k: v for k, v in spec.items() if k != "evidence_version"}, **rest)

            patch.setattr(source_evidence, "build_artifact_root", without_version)
        if outcome != "build-complete":
            patch.setattr(source_evidence, "require_current_evidence_root", lambda root: None)
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        evidence = source_evidence.CaptureEvidence(directory, family)
        evidence.inherit(publication.current_index(BASE), public_url=BASE)
        for event, fields in journaled.items():
            evidence.event(event.replace("_", "-"), **fields)
        evidence.seal(outcome=outcome)
        build_generation(
            directory / "generation",
            family=family,
            files=[path],
            expected_keys=[path.name],
            inputs=evidence.inputs(),
            read_snapshot=publication.current_index(BASE),
        )
        publication.publish_generation(
            directory / "generation",
            client=store,
            bucket=BUCKET,
            prior_index=publication.current_index(BASE),
            evidence_directories=(evidence.artifact_dir,),
        )
    evidence.finish()


def count_fetches(monkeypatch):
    """The key of every table member a conversion downloads, in order."""
    fetch, fetched = publication.fetch_member, []

    def counting(base, member, *rest):
        fetched.append(member.key)
        return fetch(base, member, *rest)

    monkeypatch.setattr(publication, "fetch_member", counting)
    return fetched


def test_a_family_with_current_evidence_downloads_its_tables_and_converts(tmp_path, monkeypatch, bucket):
    """The control for the refusal below: the same family, with evidence this runtime reads, fetches and converts."""
    publish_old_with_evidence(tmp_path, monkeypatch, bucket, "courtlistener", "court_dockets", [DOCKET])
    fetched = count_fetches(monkeypatch)

    receipt = convert("courtlistener", tmp_path / "work")

    assert fetched == ["court_dockets.parquet"] and receipt["lineage"]["inputs"]


def test_a_family_whose_evidence_is_not_a_completed_build_is_refused_before_any_download(
    tmp_path, monkeypatch, bucket, capsys
):
    """Current-format evidence of an unfinished build cannot be inherited: exit 1, nothing downloaded or published.

    The refusal comes from the evidence alone, so it precedes the first table download (fec-query is 54 tables).
    """
    publish_old_with_evidence(tmp_path, monkeypatch, bucket, outcome="failed")
    fetched = count_fetches(monkeypatch)
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    written = list(bucket.writes)
    arguments = ["cfr-sections", *COMMON, "--work", str(tmp_path / "work"), "--publish", "--expect-bucket", BUCKET]
    assert conversion.main(arguments) == 1
    error = capsys.readouterr().err
    assert "REFUSED:" in error and "Inherited source evidence refused: Only completed-build evidence" in error
    assert f"STATE: {conversion.NOTHING_PUBLISHED}" in error and bucket.writes == written
    assert fetched == [] and not (tmp_path / "work" / "retained").exists()


def evidence_root(store, pin):
    prefix = f"{publication.EVIDENCE_PREFIX}/{pin['artifactDigest'].removeprefix('sha256:')}"
    return json.loads(store.objects[f"{prefix}/artifact.json"])


def published_evidence_pin(store, family):
    from spicy_regs.source_evidence import INPUT_ROLE

    entry = stored(store, family)
    (pin,) = [
        item
        for item in json.loads(store.objects[entry["prefix"] + "/artifact.json"])["inputs"]
        if item["role"] == INPUT_ROLE
    ]
    return pin


@pytest.mark.parametrize("version", ["absent", 1])
def test_a_family_whose_evidence_predates_version_2_hands_nothing_on_and_gets_current_evidence(
    tmp_path, monkeypatch, bucket, capsys, version
):
    """Evidence with no evidence_version (as published today) or version 1 stays with its generation: no refusal, no events inherited, v2 written."""
    from spicy_regs.source_evidence import CaptureEvidence

    remainder = {"table": "treaties", "unevidenced": [["119-1"]], "shape_version": 2}
    publish_old_with_evidence(
        tmp_path,
        monkeypatch,
        bucket,
        "treaties",
        "treaties",
        [{"treaty_id": "119-1"}],
        evidence_version=None if version == "absent" else version,
        unversioned=version == "absent",
        congress_index_selection=remainder,
    )
    old_pin = published_evidence_pin(bucket, "treaties")
    assert evidence_root(bucket, old_pin)["spec"].get("evidence_version") == (None if version == "absent" else 1)
    held = {key: value for key, value in bucket.objects.items() if key.startswith(publication.EVIDENCE_PREFIX)}
    monkeypatch.setattr(conversion, "source_state", lambda remote: dict(STATE))
    arguments = [
        *("treaties", "--allow", "treaties", "--expect-main", MAIN, "--expect-spicy-docs", WHEEL, "--remote", "fork"),
        *("--work", str(tmp_path / "work"), "--publish", "--expect-bucket", BUCKET),
    ]

    assert conversion.main(arguments) == 0

    assert "published and read back" in capsys.readouterr().out
    receipt = json.loads((tmp_path / "work" / conversion.RECEIPT).read_text())
    assert receipt["lineage"]["handed_on_events"] == 0
    new_pin = published_evidence_pin(bucket, "treaties")
    new_evidence = evidence_root(bucket, new_pin)
    assert new_pin != old_pin and new_evidence["spec"]["evidence_version"] == 2
    assert new_evidence["spec"]["outcome"] == "build-complete"
    assert all(bucket.objects[key] == value for key, value in held.items())  # the old evidence is untouched
    # The next scheduled run reads the new evidence; it holds none of the earlier generation's journal.
    evidence = CaptureEvidence(tmp_path / "next", "treaties")
    evidence.inherit(publication.current_index(BASE), public_url=BASE)
    assert evidence.inherited_event("congress-index-selection", table="treaties") is None


def test_rollback_recovers_a_lost_response(tmp_path, monkeypatch, bucket):
    from tests.generation_fakes import error

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    convert("cfr-sections", tmp_path / "work", publish=True)
    real, lost = bucket.put_object, []

    def put_object(**kwargs):
        result = real(**kwargs)
        if kwargs["Key"] == V2 and not lost:
            lost.append(True)
            raise error("InternalError")
        return result

    monkeypatch.setattr(bucket, "put_object", put_object)
    path = tmp_path / "work" / conversion.RECEIPT
    receipt = conversion.rollback(path, expect_bucket=BUCKET)
    assert receipt["rollback_attempt"]["outcome"] == "restored after lost response"
    assert receipt["rolled_back"]["entry"] == stored(bucket) == old


def test_interrupted_rollback_reports_the_attempt_and_can_be_retried(tmp_path, monkeypatch, bucket, capsys):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    convert("cfr-sections", tmp_path / "work", publish=True)
    real = bucket.put_object

    def put_object(**kwargs):
        result = real(**kwargs)
        if kwargs["Key"] == V2:
            raise KeyboardInterrupt()
        return result

    monkeypatch.setattr(bucket, "put_object", put_object)
    path = tmp_path / "work" / conversion.RECEIPT
    assert conversion.main(["--rollback", str(path), "--expect-bucket", BUCKET]) == 130
    report = capsys.readouterr().err
    assert "a rollback was attempted" in report and conversion.NOTHING_PUBLISHED not in report
    assert stored(bucket) == old and json.loads(path.read_text())["rollback_attempt"]["outcome"] == "attempted"
    monkeypatch.setattr(bucket, "put_object", real)
    assert conversion.main(["--rollback", str(path), "--expect-bucket", BUCKET]) == 0


def test_rollback_verification_failure_reports_uncertainty_after_saved_attempt(tmp_path, monkeypatch, bucket, capsys):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    convert("cfr-sections", tmp_path / "work", publish=True)
    path = tmp_path / "work" / conversion.RECEIPT
    restore, read = publication.restore_family, publication.stored_family
    reads = []

    def lost(*args, **kwargs):
        restore(*args, **kwargs)
        raise publication.PublicationError("lost restore response")

    def unavailable(*args, **kwargs):
        reads.append(True)
        if len(reads) > 1:
            raise publication.PublicationError("stored index unavailable")
        return read(*args, **kwargs)

    with monkeypatch.context() as recovery:
        recovery.setattr(publication, "restore_family", lost)
        recovery.setattr(publication, "stored_family", unavailable)
        assert conversion.main(["--rollback", str(path), "--expect-bucket", BUCKET]) == 1
    report = capsys.readouterr().err
    assert "a rollback was attempted and its result is not known" in report and conversion.NOTHING_PUBLISHED not in report
    assert stored(bucket) == old
    receipt = json.loads(path.read_text())
    assert receipt["captured"]["entry"] == old and receipt["rollback_attempt"]["verification_error"]
    conversion.rollback(path, expect_bucket=BUCKET)
    assert stored(bucket) == old


@pytest.mark.parametrize('family,tables', [('cfr-sections', {'cfr_sections': CFR}),
                                         ('courtlistener', {'court_dockets': [DOCKET]})])
def test_anonymous_readback_uses_one_index_for_subjects_and_receipts(tmp_path, monkeypatch, bucket, family, tables):
    from spicy_regs.sources import cloudflare

    publish_old(bucket, monkeypatch, tmp_path, family, tables)
    real, reads = publication.current_index, []
    reading_back = []
    monkeypatch.setattr(cloudflare, 'purge_urls', lambda *_: reading_back.append(True))

    def current_index(*args, **kwargs):
        if reading_back:
            reads.append(True)
            assert len(reads) == 1, 'read-back must not select a different generation per dataset/member'
        return real(*args, **kwargs)

    monkeypatch.setattr(publication, 'current_index', current_index)
    receipt = convert(family, tmp_path / 'work', publish=True)
    assert reads == [True]
    assert receipt['read_back']['artifactDigest'] == receipt['generation']['artifactDigest']


def publish_split_sections(tmp_path, monkeypatch, bucket):
    """Two Congress partitions, repeated bodies and an empty member, through the real legislative writer."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation
    from spicy_regs.legislative_documents import field_registry
    from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup

    class SectionsRollup(SubjectReceiptRollup):
        name = "bill-family"
        inputs = ()
        outputs = ("bill_sections.parquet",)
        partitioned = {"bill_sections.parquet": ("congress",)}

        def build(self, output_dir):
            raise AssertionError("The conversion must not acquire new source data")

    monkeypatch.setattr(conversion, "_rollup_class", lambda family, base=None: SectionsRollup)
    directory = tmp_path / "old" / "bill_sections"
    fields = [f["name"] for f in field_registry()["bill_sections"]["fields"]]
    schema = pa.schema([(name, pa.string()) for name in fields])
    expected = {}
    for congress, part, sequences in [(118, 0, [1, 2]), (119, 0, [1]), (119, 1, [])]:
        relative = f"congress={congress}/part-{part:06d}.parquet"
        path = directory / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [dict.fromkeys(fields) | {"bill_id": f"hr1-{congress}", "version_code": "ih", "source": "govinfo",
                "congress": str(congress), "seq": str(seq), "body": "The same literal body."} for seq in sequences]
        table = pa.Table.from_pylist(rows, schema=schema.with_metadata({b"checkpoint": relative.encode()}))
        pq.write_table(table, path)
        expected[relative] = table
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(tmp_path / "old-generation", family="bill-family", files=[directory],
                         expected_keys=["bill_sections.parquet"], partitioned=SectionsRollup.partitioned)
        index = publication.publish_generation(tmp_path / "old-generation", client=bucket, bucket=BUCKET,
                                               prior_index=publication.current_index(BASE))
    return index["families"]["bill-family"], expected


def test_split_legislative_members_convert_and_restore_without_collapsing_partitions(tmp_path, monkeypatch, bucket):
    from pathlib import Path

    from spicy_regs.legislative_documents import printing_id
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    old, expected = publish_split_sections(tmp_path, monkeypatch, bucket)
    receipt = convert("bill-family", tmp_path / "work", publish=True)
    entry = receipt["published"]["entry"]
    table = entry["tables"]["bill_sections.parquet"]
    assert table["partitionColumns"] == ["congress"]
    assert [(m["key"], m["rows"]) for m in table["members"]] == [
        (m["key"], m["rows"]) for m in old["tables"]["bill_sections.parquet"]["members"]]
    assert public_rows(receipt)["bill_sections"] == 3
    restored = SelectedPriors(tmp_path / "after", public_url=BASE).get("bill_sections")
    assert restored.is_dir()
    assert {p.relative_to(restored).as_posix() for p in restored.rglob("*.parquet")} == set(expected)
    for relative, original in expected.items():
        actual = pq.ParquetFile(restored / relative).read()
        assert actual.equals(original, check_metadata=True)
        native = pq.ParquetFile(Path(receipt["generation"]["directory"]) / "bill_sections" / relative).read()
        assert "source" not in native.schema.names and "printing_id" in native.schema.names
        for row in native.to_pylist():
            assert row["printing_id"] == printing_id(row["bill_id"], row["version_code"], "govinfo")
    assert set(receipt["tables"]["bill_sections"]["members"]) == set(expected)
    conversion.rollback(tmp_path / "work" / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"]["bill-family"] == old


@pytest.mark.parametrize("damage", [
    "metadata", "field-metadata", "nullability", "column-order", "row-order", "drop-empty", "extra-empty",
    "move-rows", "type", "column", "layout",
])
def test_split_restoration_must_preserve_each_member_before_publication(tmp_path, monkeypatch, bucket, damage):
    """An unchanged union/count is insufficient: partition membership and each footer belong to the source."""
    from spicy_regs.conversion_reads import _FamilyReader

    old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    retained_first = tmp_path / "old/bill_sections/congress=118/part-000000.parquet"
    real = _FamilyReader.compare_original

    def broken(reader, dataset, original, **kwargs):
        def damage_input():
            restored = original
            first = restored / "congress=118/part-000000.parquet"
            second = restored / "congress=119/part-000000.parquet"
            empty = restored / "congress=119/part-000001.parquet"
            table = pq.ParquetFile(first).read()
            if damage == "metadata":
                pq.write_table(table.replace_schema_metadata({b"checkpoint": b"changed"}), first)
            elif damage in {"field-metadata", "nullability"}:
                fields = list(table.schema)
                fields[0] = (fields[0].with_metadata({b"source-note": b"changed"}) if damage == "field-metadata"
                             else fields[0].with_nullable(False))
                pq.write_table(table.cast(pa.schema(fields, metadata=table.schema.metadata)), first)
            elif damage == "column-order":
                pq.write_table(table.select(list(reversed(table.column_names))), first)
            elif damage == "row-order":
                pq.write_table(table.take(list(reversed(range(table.num_rows)))), first)
                # Count, schema, metadata and multiset stay identical; only source order changes.
                assert not conversion._has_differences(conversion._differences(first, retained_first))
            elif damage == "drop-empty":
                empty.unlink()
            elif damage == "extra-empty":
                pq.write_table(pq.ParquetFile(empty).read(), empty.with_name("part-000002.parquet"))
            elif damage == "move-rows":
                # Move one row each way, preserving both file counts and the table-wide multiset.
                other = pq.ParquetFile(second).read()
                pq.write_table(pa.concat_tables([table.slice(1), other]).replace_schema_metadata(table.schema.metadata), first)
                pq.write_table(table.slice(0, 1).replace_schema_metadata(other.schema.metadata), second)
            elif damage == "type":
                column = table.schema.get_field_index("seq")
                pq.write_table(table.set_column(column, "seq", table["seq"].cast(pa.int64())), first)
            elif damage == "column":
                pq.write_table(table.drop(["source"]), first)
            elif damage == "layout":
                return first
            return restored

        return real(reader, dataset, damage_input(), **kwargs)

    monkeypatch.setattr(_FamilyReader, "compare_original", broken)
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="does not restore") as refusal:
        convert("bill-family", tmp_path / "work", publish=True)
    assert refusal.value.state == conversion.NOTHING_PUBLISHED
    assert bucket.writes == written
    assert publication.current_index(BASE)["families"]["bill-family"] == old


@pytest.mark.parametrize("partitioned", [{}, {"bill_sections.parquet": ("bill_id",)}])
def test_split_layout_must_match_the_current_family_writer(tmp_path, monkeypatch, bucket, partitioned):
    publish_split_sections(tmp_path, monkeypatch, bucket)
    cls = conversion._rollup_class("bill-family")
    monkeypatch.setattr(cls, "partitioned", partitioned)
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="partition layout differs"):
        convert("bill-family", tmp_path / "work", publish=True)
    assert bucket.writes == written
    assert not (tmp_path / "work" / "build").exists()


@pytest.mark.parametrize("damage", ["missing", "flipped"])
def test_split_public_readback_checks_empty_member_and_reports_published_state(tmp_path, monkeypatch, bucket, damage):
    """A public member that is absent or differs by one byte after upload fails read-back and keeps the rollback."""
    from spicy_regs.conversion_reads import ConversionReadOperation

    old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    real = ConversionReadOperation.verify_public_generation

    def damaged(operation, base, index, family, directory, *storage):
        entry = index["families"][family]
        empty = next(member for member in entry["tables"]["bill_sections.parquet"]["members"] if member["rows"] == 0)
        key = f"{entry['prefix']}/{empty['key']}"
        if damage == "missing":
            del bucket.objects[key]
        else:
            raw = bucket.objects[key]
            bucket.objects[key] = bytes([raw[0] ^ 1]) + raw[1:]
        return real(operation, base, index, family, directory, *storage)

    monkeypatch.setattr(ConversionReadOperation, "verify_public_generation", damaged)
    with pytest.raises(conversion.ConversionRefused, match="could not be read back") as refusal:
        convert("bill-family", tmp_path / "work", publish=True)
    assert "IS published" in refusal.value.state
    receipt = json.loads((tmp_path / "work" / conversion.RECEIPT).read_text())
    assert receipt["publish_attempt"]["outcome"] == "published" and receipt["published"] and "read_back" not in receipt
    assert not (tmp_path / "work" / "read-back").exists()
    assert publication.current_index(BASE)["families"]["bill-family"] != old
    conversion.rollback(tmp_path / "work" / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)["families"]["bill-family"] == old


@pytest.mark.parametrize("damage", ["rows", "columns", "unavailable"])
def test_each_retained_split_member_must_match_its_captured_descriptor(tmp_path, monkeypatch, bucket, damage):
    from copy import deepcopy

    old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    captured = deepcopy(publication.current_index(BASE))
    table = captured["families"]["bill-family"]["tables"]["bill_sections.parquet"]
    if damage == "rows":
        table["members"][0]["rows"] += 1
        table["rows"] += 1
    elif damage == "columns":
        table["columns"] = [column for column in table["columns"] if column[0] != "source"]
    else:
        member = publication.table_members(captured, "bill_sections.parquet")[0]
        del bucket.objects[member.path]
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="schema or row count|not readable"):
        conversion._retain_table(BASE, captured, "bill_sections.parquet", tmp_path / "retained")
    assert bucket.writes == written
    assert publication.current_index(BASE)["families"]["bill-family"] == old


def test_split_support_does_not_authorize_an_incomplete_family(tmp_path, monkeypatch, bucket):
    from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup

    old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    monkeypatch.setattr(conversion, "_rollup_class", lambda family, base=None: BillFamilyRollup)
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="convert only a family whose table set main still writes"):
        convert("bill-family", tmp_path / "work", publish=True)
    assert bucket.writes == written
    assert publication.current_index(BASE)["families"]["bill-family"] == old


def test_split_support_still_refuses_unlisted_physical_tables(tmp_path, monkeypatch, bucket):
    old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    real = conversion._convert_rollup

    def extra(*args, **kwargs):
        built = real(*args, **kwargs)
        pq.write_table(pa.table({"unexpected": ["row"]}), built.generation / "unlisted.parquet")
        return built

    monkeypatch.setattr(conversion, "_convert_rollup", extra)
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="member changed after admission"):
        convert("bill-family", tmp_path / "work", publish=True)
    assert bucket.writes == written
    assert publication.current_index(BASE)["families"]["bill-family"] == old


@pytest.mark.parametrize("family,read_dataset", [
    ("committee-reports", "committee_report_reads"),
    ("native-legal-references", "native_legal_reference_reads"),
])
def test_checkpoint_source_rows_convert_and_restore_in_their_declared_main_shape(
        tmp_path, monkeypatch, bucket, family, read_dataset):
    """The complete family includes checkpoint reads even when no subject row was produced."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation
    from spicy_regs.legislative_documents import field_registry
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from tests.test_legislative_receipts import retained, row

    monkeypatch.delenv("R2_ALLOW_SHRINK", raising=False)
    cls = conversion._rollup_class(family)
    reads_key = read_dataset + ".parquet"
    assert reads_key in cls.source_outputs
    processing_only = read_dataset == "committee_report_reads"
    assert (reads_key not in cls.outputs) == processing_only
    assert (reads_key in cls.receipt_only_tables) == processing_only
    assert next(p for p in cls.receipt_policies if p.dataset == read_dataset).receipt_only == processing_only
    reads = (row(read_dataset, package_id="CRPT-119hrpt1", outcome="complete", rule_version="r1")
             if read_dataset == "committee_report_reads" else row(
                 read_dataset, scope_id="scope", source_family="uscode", source_record_key="record",
                 read_status="complete", occurrence_count="0", source_bytes="0", rule_version="r1"))
    discovery = {"cursor": "2026-11-05T13:00:00+00:00", "since": "2026-11-04T13:00:00Z", "mode": "refresh"}
    evidence = None
    if processing_only:
        from spicy_regs.source_evidence import CaptureEvidence
        from spicy_regs.transforms.committee_report_reads import RULE_VERSIONS

        reads.update(last_modified="2026-11-03T12:00:00Z", observed_at="2026-11-05T13:00:00+00:00",
                     rule_version=RULE_VERSIONS["CRPT"])
        source_reads = [reads, reads | {"package_id": "CRPT-119hrpt2", "outcome": "pending"},
                        reads | {"package_id": "CRPT-119hrpt3", "outcome": "refused"}]
        evidence = CaptureEvidence(tmp_path / "source-evidence", family)
        evidence.inherit(publication.current_index(BASE), public_url=BASE)
        evidence.event("committee-discovery", **discovery)
    else:
        source_reads = [reads, reads | {"scope_id": "second-scope"}]
    files = []
    for key in cls.source_outputs:
        dataset = key.removesuffix(".parquet")
        assert dataset in field_registry()
        files.append(retained(tmp_path / "old", dataset, source_reads if dataset == read_dataset else [],
                              {b"checkpoint": b"preserve exact original metadata"}))
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(tmp_path / "old-generation" / family, family=family, files=files,
                         expected_keys=list(cls.source_outputs), inputs=evidence.inputs() if evidence else (),
                         read_snapshot=publication.current_index(BASE))
        old = publication.publish_generation(tmp_path / "old-generation" / family, client=bucket,
                                             bucket=BUCKET, prior_index=publication.current_index(BASE),
                                             evidence_directories=(evidence.artifact_dir,) if evidence else ())["families"][family]
    if evidence:
        evidence.finish()
    prepared = convert(family, tmp_path / "work", publish=False)
    assert prepared["published"] is None
    assert prepared["captured"]["entry"] == old
    assert prepared["tables"][read_dataset]["retained_rows"] == len(source_reads)
    assert not any(prepared["tables"][read_dataset][name] for name in
                   ("type_changes", "metadata_changes", "rows_only_in_restored", "rows_only_in_retained"))
    conversion.publish_prepared(tmp_path / "work" / conversion.RECEIPT, allowed=[family], expected_main=MAIN,
                                expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE),
                                expected_receipt_digest=prepared["prepared"]["receiptDigest"])
    current = publication.current_index(BASE)
    entry = current["families"][family]
    assert (reads_key not in entry["tables"]) == processing_only
    assert read_dataset in entry["etlReceipts"]["datasets"]
    restored = SelectedPriors(tmp_path / "anonymous", index=current, public_url=BASE).get(read_dataset)
    assert pq.read_table(restored).to_pylist() == source_reads
    assert pq.read_schema(restored).equals(pq.read_schema(tmp_path / "old" / reads_key), check_metadata=True)
    if processing_only:
        from spicy_regs.transforms.build_committee_reports import _settled, _since
        from spicy_regs.transforms.committee_report_reads import prior_reads

        assert prepared["lineage"]["handed_on_events"] == 1
        next_evidence = CaptureEvidence(tmp_path / "next", family)
        next_evidence.inherit(current, public_url=BASE)
        handed = next_evidence.inherited_event("committee-discovery")
        assert handed is not None
        assert {name: value for name, value in handed.items() if name not in {"event", "recorded_at"}} == discovery
        monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
        assert _since(evidence=next_evidence) == "2026-11-04T13:00:00Z"
        states = prior_reads(restored, {})
        assert _settled(states["CRPT-119hrpt1"], "CRPT")
        assert {key: row["outcome"] for key, row in states.items() if not _settled(row, "CRPT")} == {
            "CRPT-119hrpt2": "pending", "CRPT-119hrpt3": "refused",
        }


def test_conversion_compares_all_datasets_with_one_reader_per_actual_generation(tmp_path, monkeypatch, bucket):
    """Every dataset uses the admitted stream once; read-back hashes the public bytes without another admission."""
    from contextlib import contextmanager

    from spicy_regs import conversion_reads
    from spicy_regs.congress_subjects import INPUT_COLUMNS
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors, SubjectReceiptRollup

    class TwoDatasetRollup(SubjectReceiptRollup):
        name = "bill-family"
        inputs = ()
        outputs = ("treaties.parquet", "nominations.parquet")

        def build(self, output_dir):
            pytest.fail("Conversion must reuse retained source inputs")

    monkeypatch.setattr(conversion, "_rollup_class", lambda family, base=None: TwoDatasetRollup)
    originals = {
        "treaties": [dict.fromkeys(INPUT_COLUMNS["treaties"]) | {"treaty_id": "119-1", "congress_received": "119"}],
        "nominations": [dict.fromkeys(INPUT_COLUMNS["nominations"]) | {"citation": "PN1-119", "congress": "119"}],
    }
    publish_old(bucket, monkeypatch, tmp_path, "bill-family", originals)
    admissions, comparisons, active = [], [], []
    open_reader = conversion_reads._open_family_reader
    compare_original = conversion_reads._FamilyReader.compare_original

    @contextmanager
    def recorded_reader(*args, **kwargs):
        assert not active, "Release the local reader before admitting the anonymous generation"
        with open_reader(*args, **kwargs) as reader:
            admissions.append(reader)
            active.append(reader)
            try:
                yield reader
            finally:
                active.pop()

    def compare(reader, dataset, original, **kwargs):
        comparisons.append((reader, dataset))
        return compare_original(reader, dataset, original, **kwargs)

    def materialized(*args, **kwargs):
        pytest.fail("Conversion must compare the complete admitted stream without a restored Parquet or EXCEPT ALL")

    monkeypatch.setattr(conversion_reads, "_open_family_reader", recorded_reader)
    monkeypatch.setattr(conversion_reads._FamilyReader, "compare_original", compare)
    monkeypatch.setattr(SelectedPriors, "get", materialized)
    monkeypatch.setattr(conversion, "_differences", materialized)
    monkeypatch.setattr(conversion, "_table_differences", materialized)
    receipt = convert("bill-family", tmp_path / "work", publish=True)

    assert len(admissions) == 1, "One receipt admission; public bytes are held to it by digest, not admitted again"
    assert sorted(dataset for reader, dataset in comparisons) == sorted(originals)
    assert not active
    assert public_rows(receipt).items() >= {name: len(rows) for name, rows in originals.items()}.items()


@pytest.mark.parametrize("interruption", ["absent", "unsealed"])
def test_resume_proves_retained_generation_without_rebuilding_or_relabeling_the_producer(
    tmp_path, monkeypatch, bucket, interruption,
):
    from pathlib import Path

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    prepared = convert("cfr-sections", tmp_path / "work")
    work = tmp_path / "work"
    receipt_path = work / conversion.RECEIPT
    generation = Path(prepared["generation"]["directory"])
    artifact = json.loads((generation / "artifact.json").read_bytes())
    identity = artifact["producer"]["implementationId"]
    original_bytes = {path.relative_to(generation): path.read_bytes() for path in generation.rglob("*") if path.is_file()}
    if interruption == "absent":
        receipt_path.unlink()
    else:
        for name in ("prepared", "tables", "policy_versions", "generation", "lineage", "receipt_outcomes", "published"):
            prepared.pop(name, None)
        receipt_path.write_text(json.dumps(prepared))
    verifier = STATE | {"checkout": "ef" * 20, "main": "ef" * 20}
    histories = []

    def historical(root, producer, publisher):
        histories.append((producer, publisher))
        return identity, {"version": WHEEL, "wheels": [{"hash": PINNED_WHEEL["locked_sha256"]}]}

    def rebuild(*args, **kwargs):
        pytest.fail("Resuming a complete generation must not rerun its writer")

    monkeypatch.setattr(conversion, "_historical_producer", historical)
    monkeypatch.setattr(conversion, "_convert_rollup", rebuild)
    before_writes = list(bucket.writes)
    resumed = conversion.resume_prepared(
        generation, work=work, allowed=["cfr-sections"], expected_main=verifier["main"],
        expected_spicy_docs=WHEEL, expected_producer=MAIN, state=lambda remote: dict(verifier),
    )

    assert histories == [(MAIN, verifier["main"])]
    assert resumed["producer"]["commit"] == MAIN
    assert resumed["producer"]["implementationId"] == identity
    assert resumed["source"] == verifier
    assert resumed["source"]["checkout"] != resumed["producer"]["commit"]
    assert resumed["captured"]["entry"] == old
    assert resumed["tables"]["cfr_sections"] == {
        "retained_rows": 2, "subject_rows": 2, "rows_only_in_restored": 0, "rows_only_in_retained": 0,
        "columns_only_in_restored": [], "columns_only_in_retained": [], "type_changes": {}, "metadata_changes": [],
    }
    assert resumed["prepared"]["receiptDigest"] == conversion._prepared_digest(resumed)
    assert resumed["published"] is None and bucket.writes == before_writes
    assert {path.relative_to(generation): path.read_bytes() for path in generation.rglob("*") if path.is_file()} == original_bytes
    assert json.loads(receipt_path.read_bytes()) == resumed


@pytest.mark.parametrize("damage", ["producer", "wheel", "missing-member", "retained-values", "prior", "attempted"])
def test_resume_refuses_changed_proof_inputs_without_sealing_or_writing_the_bucket(tmp_path, monkeypatch, bucket, damage):
    from pathlib import Path

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    prepared = convert("cfr-sections", tmp_path / "work")
    work = tmp_path / "work"
    generation = Path(prepared["generation"]["directory"])
    artifact = json.loads((generation / "artifact.json").read_bytes())
    identity = artifact["producer"]["implementationId"]
    locked = {"version": WHEEL, "wheels": [{"hash": PINNED_WHEEL["locked_sha256"]}]}
    receipt_path = work / conversion.RECEIPT
    receipt_path.unlink()
    if damage == "producer":
        identity += "-different-source"
    elif damage == "wheel":
        locked["wheels"][0]["hash"] = "sha256:" + "cd" * 32
    elif damage == "missing-member":
        (generation / "etl_receipts.parquet").unlink()
    elif damage == "retained-values":
        retained = work / "retained/cfr_sections.parquet"
        rows = pq.ParquetFile(retained).read()
        heading = rows.schema.get_field_index("heading")
        pq.write_table(rows.set_column(heading, "heading", pa.array(["Changed", ""])), retained)
    elif damage == "prior":
        captured_path = work / "captured-publication.v2.json"
        captured = json.loads(captured_path.read_bytes())
        captured["families"]["cfr-sections"]["publishedAt"] = "2000-01-01T00:00:00Z"
        captured_path.write_text(json.dumps(captured))
    else:
        receipt_path.write_text(json.dumps({"publish_attempt": {"outcome": "attempted"}}))
    previous = receipt_path.read_bytes() if receipt_path.exists() else None
    writes = list(bucket.writes)
    monkeypatch.setattr(conversion, "_historical_producer", lambda *_: (identity, locked))

    def rebuild(*args, **kwargs):
        pytest.fail("An invalid resume must never restart its writer")

    monkeypatch.setattr(conversion, "_convert_rollup", rebuild)
    message = {
        "producer": "producer source or installed locked runtime differs",
        "wheel": "producer source or installed locked runtime differs",
        "missing-member": "member|absent|missing|not found",
        "retained-values": "retained member bytes changed",
        "prior": "bind its captured complete prior",
        "attempted": "unsealed generation with no publication attempt",
    }[damage]
    with pytest.raises(conversion.ConversionRefused, match=message):
        conversion.resume_prepared(generation, work=work, allowed=["cfr-sections"], expected_main=MAIN,
                                   expected_spicy_docs=WHEEL, expected_producer=MAIN, state=lambda remote: dict(STATE))
    assert (receipt_path.read_bytes() if receipt_path.exists() else None) == previous
    assert bucket.writes == writes
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old


def test_historical_bill_membership_does_not_require_or_synthesize_new_navigation_status_tables(tmp_path, monkeypatch, bucket):
    """Check completed writer metadata against current optional additions without altering the retained artifact."""
    from copy import deepcopy
    from pathlib import Path

    old, _ = publish_split_sections(tmp_path, monkeypatch, bucket)
    prepared = convert("bill-family", tmp_path / "work")
    generation = Path(prepared["generation"]["directory"])
    spec = json.loads((generation / "artifact.json").read_bytes())["spec"]
    # Model the earlier metadata shape only. Complete historical bytes are checked by resume/admission tests.
    status = "bill_family_document_file_outcomes"
    spec["tables"].pop(status + ".parquet")
    spec["etlReceipts"]["policies"] = [policy for policy in spec["etlReceipts"]["policies"]
                                      if policy["dataset"] != status]
    unchanged = deepcopy(spec)

    conversion._check_prepared_membership("bill-family", old, spec)
    assert spec == unchanged
    assert status + ".parquet" not in spec["tables"]
    assert status not in {policy["dataset"] for policy in spec["etlReceipts"]["policies"]}

    missing_original = deepcopy(spec)
    missing_original["tables"].pop("bill_sections.parquet")
    with pytest.raises(conversion.ConversionRefused, match="subject membership differs"):
        conversion._check_prepared_membership("bill-family", old, missing_original)
    unexpected = deepcopy(spec)
    unexpected["tables"]["unexpected.parquet"] = {}
    with pytest.raises(conversion.ConversionRefused, match="subject membership differs"):
        conversion._check_prepared_membership("bill-family", old, unexpected)
    wrong_partition = deepcopy(old)
    wrong_partition["tables"]["bill_sections.parquet"]["partitionColumns"] = ["bill_id"]
    with pytest.raises(conversion.ConversionRefused, match="partition declaration differs"):
        conversion._check_prepared_membership("bill-family", wrong_partition, spec)


def test_original_changing_during_writer_refuses_without_rehashing_or_publication(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    build = conversion._convert_rollup

    def changed(*args, **kwargs):
        original = args[2]["cfr_sections.parquet"]
        table = pq.ParquetFile(original).read()
        heading = table.schema.get_field_index("heading")
        pq.write_table(table.set_column(heading, "heading", pa.array(["Modified source", ""])), original)
        return build(*args, **kwargs)

    def rehash(*args, **kwargs):
        pytest.fail("An original changed after its verified fetch must refuse through the held state guard")

    monkeypatch.setattr(conversion, "_convert_rollup", changed)
    monkeypatch.setattr(conversion, "_file_digest", rehash)
    writes = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="Retained originals changed"):
        convert("cfr-sections", tmp_path / "work", publish=True)
    assert bucket.writes == writes
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old
    receipt = tmp_path / "work" / conversion.RECEIPT
    assert not receipt.exists() or "prepared" not in json.loads(receipt.read_bytes())


def test_operation_exit_failure_after_publication_reports_uncertainty(tmp_path, monkeypatch, bucket):
    from spicy_regs.conversion_reads import ConversionReadOperation

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    exit_operation = ConversionReadOperation.__exit__

    def changed(operation, *args):
        exit_operation(operation, *args)
        raise ValueError("Generation state changed during operation exit")

    monkeypatch.setattr(ConversionReadOperation, "__exit__", changed)
    with pytest.raises(conversion.ConversionRefused, match="Generation state changed during operation exit") as refusal:
        convert("cfr-sections", tmp_path / "work", publish=True)
    assert "result is not known" in refusal.value.state
    assert conversion.NOTHING_PUBLISHED not in refusal.value.state
    stored_entry = publication.current_index(BASE)["families"]["cfr-sections"]
    receipt = json.loads((tmp_path / "work" / conversion.RECEIPT).read_bytes())
    assert stored_entry != old
    assert stored_entry == receipt["published"]["entry"]
    assert receipt["publish_attempt"]


@pytest.mark.parametrize("difference", ["field-order", "nullability", "field-metadata"])
def test_rollup_conversion_checks_the_actual_maintained_restoration_schema(tmp_path, monkeypatch, bucket, difference):
    """A schema the scheduled reader cannot reproduce must refuse conversion, even when values still match."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.generations import build_generation
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    table = pa.Table.from_pylist(CFR, schema=pa.schema([(column, pa.string()) for column in CFR[0]]))
    if difference == "field-order":
        table = table.select(list(reversed(table.column_names)))
    else:
        fields = list(table.schema)
        fields[0] = (fields[0].with_nullable(False) if difference == "nullability"
                     else fields[0].with_metadata({b"source-note": b"retain this field metadata"}))
        table = table.cast(pa.schema(fields))
    original = tmp_path / "old/cfr_sections.parquet"
    original.parent.mkdir()
    pq.write_table(table, original)
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(tmp_path / "old-generation", family="cfr-sections", files=[original], expected_keys=[original.name])
        index = publication.publish_generation(tmp_path / "old-generation", client=bucket, bucket=BUCKET,
                                               prior_index=publication.current_index(BASE))
    old = index["families"]["cfr-sections"]
    writes = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="schema"):
        convert("cfr-sections", tmp_path / "work", publish=True)

    # Establish the expectation through the maintained scheduled reader, not a copied schema definition.
    restored = SelectedPriors(tmp_path / "scheduled", root=tmp_path / "work/build", public_url="").get("cfr_sections")
    assert not pq.read_schema(restored).equals(pq.read_schema(original), check_metadata=True)
    assert bucket.writes == writes
    assert publication.current_index(BASE)["families"]["cfr-sections"] == old


def test_court_conversion_refuses_file_metadata_the_maintained_restore_cannot_reproduce(tmp_path, monkeypatch, bucket):
    from spicy_regs import etl_policy_registry
    from spicy_regs.court_receipts import restore_processing_input
    from spicy_regs.generations import build_generation

    table = pa.Table.from_pylist([DOCKET], schema=pa.schema([(column, pa.string()) for column in DOCKET]))
    table = table.replace_schema_metadata({b"source-note": b"keep this court file metadata"})
    original = tmp_path / "old/court_dockets.parquet"
    original.parent.mkdir()
    pq.write_table(table, original)
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "require_registered_receipts", lambda *_: None)
        build_generation(tmp_path / "old-generation", family="courtlistener", files=[original], expected_keys=[original.name])
        index = publication.publish_generation(tmp_path / "old-generation", client=bucket, bucket=BUCKET,
                                               prior_index=publication.current_index(BASE))
    old = index["families"]["courtlistener"]
    writes = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match="schema|metadata"):
        convert("courtlistener", tmp_path / "work", publish=True)

    generation = tmp_path / "work/build/generation"
    artifact = json.loads((generation / "artifact.json").read_bytes())
    (tmp_path / "scheduled").mkdir()
    restored = restore_processing_input(
        generation / original.name, tmp_path / "scheduled/court_dockets.parquet", dataset="court_dockets",
        schema=table.schema.remove_metadata(), receipt_path=generation / "etl_receipts.parquet",
        generation_id=artifact["spec"]["etlReceipts"]["generationId"],
    )
    assert not pq.read_schema(restored).equals(pq.read_schema(original), check_metadata=True)
    assert bucket.writes == writes
    assert publication.current_index(BASE)["families"]["courtlistener"] == old
